//! `OwnedErc20` — Phase 5 follow-up: ERC20 transfers involving a given
//! owner, fetched **RPC-efficiently** as two narrow per-topic histories
//! merged in position order.
//!
//! Instead of fetching every Transfer on a token and filtering for the
//! owner client-side, this opens two [`EvmLogHistory`] instances against
//! the same [`LogStore`]:
//!
//! * **out**: `topics = [Transfer, owner_topic, None, None]` — outgoing
//!   transfers (owner = `from`).
//! * **in**:  `topics = [Transfer, None, owner_topic, None]` — incoming
//!   transfers (owner = `to`).
//!
//! Each history's `eth_getLogs` calls carry the owner topic, so the node
//! filters server-side and only the relevant logs come over the wire.
//! [`OwnedErc20::transfers`] merges them via [`merge_hist_stores`] keyed
//! by `(block_number, log_index)` and returns a
//! `HistStore<MergedItem<EvmEvent>, MergedPos>` whose
//! `item.source_key` is `"in"` or `"out"` — that's the direction.
//!
//! ## Re-entry
//!
//! Both inner histories share the same `Arc<LogStore>`, but each writes
//! into its own filter-hash-scoped slot (logs are keyed by filter, scanned
//! ranges and reorg state too). Restarting picks up cached state per
//! filter so cold-start is paid once per `(owner, token, direction)` tuple.

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::providers::Provider;

use crate::arch::{HistStore, HistoryError};
use crate::evm::{EvmEvent, EvmLogHistory, LogStore, TRANSFER_TOPIC};
use crate::merge::{merge_hist_stores, ErasedHistStore, MergedItem, MergedPos};

/// Source key for outgoing transfers (`owner == from`).
pub const OUT_KEY: &str = "out";
/// Source key for incoming transfers (`owner == to`).
pub const IN_KEY: &str = "in";

/// Pad a 20-byte address to a 32-byte topic by left-padding with 12 zero
/// bytes — the EVM convention for indexed `address` event parameters.
fn address_to_topic(addr: [u8; 20]) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[12..].copy_from_slice(&addr);
    t
}

/// ERC20 transfers from-or-to a fixed owner address on a single token.
pub struct OwnedErc20<P: Provider<Ethereum> + Clone + Send + Sync + 'static> {
    owner: [u8; 20],
    token_address: [u8; 20],
    out_history: Arc<EvmLogHistory<P>>,
    in_history: Arc<EvmLogHistory<P>>,
}

impl<P> OwnedErc20<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    /// Open both per-topic histories. Each shares `store` but uses its own
    /// filter (different `topics`), so their scanned-range and log
    /// bookkeeping stays separate.
    pub async fn new(
        provider: P,
        store: Arc<LogStore>,
        owner: [u8; 20],
        token_address: [u8; 20],
    ) -> Result<Self, HistoryError> {
        Self::with_options(provider, store, owner, token_address, 1000, 12).await
    }

    pub async fn with_options(
        provider: P,
        store: Arc<LogStore>,
        owner: [u8; 20],
        token_address: [u8; 20],
        chunk_size: u64,
        block_time: u64,
    ) -> Result<Self, HistoryError> {
        let owner_topic = address_to_topic(owner);
        let out_topics: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), Some(owner_topic), None, None];
        let in_topics: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), None, Some(owner_topic), None];

        let out_history = EvmLogHistory::with_options(
            provider.clone(),
            store.clone(),
            token_address,
            out_topics,
            chunk_size,
            block_time,
        )
        .await?;
        let in_history = EvmLogHistory::with_options(
            provider,
            store,
            token_address,
            in_topics,
            chunk_size,
            block_time,
        )
        .await?;

        Ok(Self {
            owner,
            token_address,
            out_history,
            in_history,
        })
    }

    pub fn owner(&self) -> &[u8; 20] {
        &self.owner
    }

    pub fn token_address(&self) -> &[u8; 20] {
        &self.token_address
    }

    /// Update both inner histories' `latest_store` from the chain. Required
    /// before iterating [`transfers`](Self::transfers) without an active
    /// subscription: the merged store's `end` is built from the inner
    /// stores' tips, which start at [`EvmPosition::earliest`] and only
    /// advance via [`EvmLogHistory::update_tip`] or
    /// [`EvmLogHistory::subscribe`].
    ///
    /// Returns the new tip block number (same on both sides for a single
    /// chain). Cheap — one `eth_blockNumber` round-trip per inner history.
    pub async fn update_tips(&self) -> Result<u64, HistoryError> {
        let (out_tip, in_tip) =
            tokio::try_join!(self.out_history.update_tip(), self.in_history.update_tip())?;
        // Both sides hit the same chain and should agree, but return the
        // outgoing-side number for callers that just want a single u64.
        let _ = in_tip;
        Ok(out_tip)
    }

    /// Outgoing (`from == owner`) history. Useful if a consumer needs the
    /// pre-merge stream — e.g. for follow-mode subscription wiring.
    pub fn out_history(&self) -> &Arc<EvmLogHistory<P>> {
        &self.out_history
    }

    /// Incoming (`to == owner`) history.
    pub fn in_history(&self) -> &Arc<EvmLogHistory<P>> {
        &self.in_history
    }

    /// Merged in/out transfer stream. Yielded items carry the direction
    /// on `MergedItem::source_key` ([`OUT_KEY`] or [`IN_KEY`]).
    ///
    /// Items are ordered by `(block_number, log_index)`. Cross-direction
    /// ties are impossible (a single log only has one `from` and one
    /// `to`), so ordering is total.
    pub fn transfers(&self) -> HistStore<MergedItem<EvmEvent>, MergedPos> {
        let mut sources: BTreeMap<String, ErasedHistStore<EvmEvent>> = BTreeMap::new();
        sources.insert(
            OUT_KEY.into(),
            ErasedHistStore::new(self.out_history.hist_store()),
        );
        sources.insert(
            IN_KEY.into(),
            ErasedHistStore::new(self.in_history.hist_store()),
        );
        merge_hist_stores(sources, |a, b| {
            (a.value.block_number, a.value.log_index)
                .cmp(&(b.value.block_number, b.value.log_index))
        })
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_to_topic_left_pads_with_zero() {
        let addr = [0xAB; 20];
        let topic = address_to_topic(addr);
        // First 12 bytes zero, last 20 bytes the address.
        assert_eq!(&topic[..12], &[0u8; 12]);
        assert_eq!(&topic[12..], &addr);
    }

    #[test]
    fn keys_are_distinct() {
        // Locked-in convention: source_key values that downstream code
        // pattern-matches on.
        assert_eq!(OUT_KEY, "out");
        assert_eq!(IN_KEY, "in");
        assert_ne!(OUT_KEY, IN_KEY);
    }
}
