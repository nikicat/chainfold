//! `OwnedErc20` — Phase 5 follow-up: ERC20 transfers involving a given
//! owner, fetched **RPC-efficiently** as two narrow per-topic histories
//! merged in position order.
//!
//! Instead of fetching every Transfer on a token and filtering for the
//! owner client-side, this opens two [`EvmLogHistory`] instances against
//! the same [`LogCache`]:
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
//! Both inner histories share the same `Arc<LogCache>`, but each writes
//! into its own filter-hash-scoped slot (logs are keyed by filter, scanned
//! ranges and reorg state too). Restarting picks up cached state per
//! filter so cold-start is paid once per `(owner, token, direction)` tuple.

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;

use crate::arch::{HistStore, HistoryError};
use crate::evm::{ChainProvider, EvmEvent, EvmLogHistory, LogCache, TRANSFER_TOPIC, Topic};
use crate::merge::{ErasedHistStore, MergedItem, MergedPos, SourceKey, merge_hist_stores};

/// Direction of an owned-ERC20 transfer relative to the tracked owner.
///
/// Surfaces downstream as the merge `source_key` ([`MergedItem::source_key`]).
/// The merge layer is generically keyed on `String`, so this is the typed
/// source of truth that converts at that boundary via
/// [`as_str`](Self::as_str) / [`from_key`](Self::from_key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    /// Outgoing — `owner == from`.
    Out,
    /// Incoming — `owner == to`.
    In,
}

impl TransferDirection {
    /// The merge `source_key` wire string for this direction.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
        }
    }

    /// Parse a merge `source_key` back into a direction. `None` for any key
    /// not produced by [`OwnedErc20::transfers`].
    pub fn from_key(key: &str) -> Option<Self> {
        match key {
            _ if key == Self::Out.as_str() => Some(Self::Out),
            _ if key == Self::In.as_str() => Some(Self::In),
            _ => None,
        }
    }
}

/// Source key for outgoing transfers (`owner == from`).
pub const OUT_KEY: &str = TransferDirection::Out.as_str();
/// Source key for incoming transfers (`owner == to`).
pub const IN_KEY: &str = TransferDirection::In.as_str();

/// The topic for an indexed `address` event parameter — the address
/// left-padded with 12 zero bytes (see [`Topic::from`]`(Address)`).
fn address_to_topic(addr: Address) -> Topic {
    addr.into()
}

/// ERC20 transfers from-or-to a fixed owner address on a single token.
pub struct OwnedErc20<P: Provider<Ethereum> + Clone + Send + Sync + 'static> {
    owner: Address,
    token_address: Address,
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
    /// Open both per-topic histories with default chunk size / block time.
    /// The chain id rides in the [`ChainProvider`]; `OwnedErc20` just forwards
    /// it to the inner histories and never talks to the RPC itself.
    pub async fn new(
        provider: ChainProvider<P>,
        store: Arc<LogCache>,
        owner: Address,
        token_address: Address,
    ) -> Result<Self, HistoryError> {
        Self::with_options(provider, store, owner, token_address, 1000, 12).await
    }

    pub async fn with_options(
        provider: ChainProvider<P>,
        store: Arc<LogCache>,
        owner: Address,
        token_address: Address,
        chunk_size: u64,
        block_time: u64,
    ) -> Result<Self, HistoryError> {
        let owner_topic = address_to_topic(owner);
        let out_topics: [Option<Topic>; 4] = [Some(TRANSFER_TOPIC), Some(owner_topic), None, None];
        let in_topics: [Option<Topic>; 4] = [Some(TRANSFER_TOPIC), None, Some(owner_topic), None];

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

    pub fn owner(&self) -> Address {
        self.owner
    }

    pub fn token_address(&self) -> Address {
        self.token_address
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
        let mut sources: BTreeMap<SourceKey, ErasedHistStore<EvmEvent>> = BTreeMap::new();
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
        let addr = Address::repeat_byte(0xAB);
        let topic = address_to_topic(addr);
        // First 12 bytes zero, last 20 bytes the address.
        assert_eq!(&topic.as_slice()[..12], &[0u8; 12]);
        assert_eq!(&topic.as_slice()[12..], addr.as_slice());
    }

    #[test]
    fn keys_are_distinct() {
        // Locked-in convention: source_key values that downstream code
        // pattern-matches on.
        assert_eq!(OUT_KEY, "out");
        assert_eq!(IN_KEY, "in");
        assert_ne!(OUT_KEY, IN_KEY);
    }

    #[test]
    fn direction_key_round_trips() {
        for dir in [TransferDirection::Out, TransferDirection::In] {
            assert_eq!(TransferDirection::from_key(dir.as_str()), Some(dir));
        }
        assert_eq!(TransferDirection::from_key("sideways"), None);
        // The exported consts agree with the enum's wire form.
        assert_eq!(TransferDirection::Out.as_str(), OUT_KEY);
        assert_eq!(TransferDirection::In.as_str(), IN_KEY);
    }
}
