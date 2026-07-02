//! `OwnedErc20` — ERC20 transfers involving a given owner, fetched
//! **RPC-efficiently** as two narrow per-topic histories merged in
//! position order.
//!
//! Instead of fetching every Transfer on a token and filtering for the
//! owner client-side, this opens two [`EvmChain::logs`] stores:
//!
//! * **out**: `topics = [Transfer, owner_topic, None, None]` — outgoing
//!   transfers (owner = `from`).
//! * **in**:  `topics = [Transfer, None, owner_topic, None]` — incoming
//!   transfers (owner = `to`).
//!
//! Each store's `eth_getLogs` calls carry the owner topic, so the node
//! filters server-side and only the relevant logs come over the wire.
//! [`OwnedErc20::transfers`] merges them via the ordered
//! [`merge_hist_stores`] — both filters share the chain's
//! `(block_number, log_index)` position space, so the chain's own order is
//! the stream order and no comparator is needed. Yields are
//! `MergedItem<EvmEvent, EvmPosition>` whose `source_key` is `"in"` or
//! `"out"` — that's the direction.
//!
//! ## Self-transfers
//!
//! A transfer with `from == to == owner` is one log matched by both
//! filters, so it surfaces twice — once per direction — at the identical
//! [`EvmPosition`]. Each copy's anchor carries its direction tag
//! ([`OrderedPos`]`::bound`), so a resume anchored *between* the two copies
//! is exact: it replays the second copy and nothing else. (Before the
//! ordered merge this was a known resume edge; the tagged anchor closed it.)
//!
//! ## Re-entry
//!
//! Both inner stores share the chain's [`LogCache`](crate::evm::LogCache),
//! but each writes into its own filter-hash-scoped slot (logs are keyed by
//! filter, scanned ranges and reorg state too). Restarting picks up cached
//! state per filter so cold-start is paid once per
//! `(owner, token, direction)` tuple.

use std::collections::BTreeMap;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;

use crate::arch::HistStore;
use crate::evm::{EvmChain, EvmEvent, EvmPosition, Finality, LogFilter, TRANSFER_TOPIC, Topic};
use crate::merge::{MergedItem, OrderedPos, SourceKey, merge_hist_stores};

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
///
/// Not generic over the provider: [`EvmChain::logs`] hands back type-erased
/// [`HistStore`]s, and those (plus their wiring, which lives and dies with
/// them) are all this needs to hold.
pub struct OwnedErc20 {
    owner: Address,
    token_address: Address,
    out_transfers: HistStore<EvmEvent, EvmPosition>,
    in_transfers: HistStore<EvmEvent, EvmPosition>,
}

impl OwnedErc20 {
    /// Open both per-topic log stores on `chain`. Synchronous and RPC-free —
    /// the chain already knows its head; cached state materializes lazily.
    pub fn new<P>(chain: &EvmChain<P>, owner: Address, token_address: Address) -> Self
    where
        P: Provider<Ethereum> + Clone + Send + Sync + 'static,
    {
        let owner_topic = address_to_topic(owner);
        let out_filter = LogFilter::new(
            token_address,
            [Some(TRANSFER_TOPIC), Some(owner_topic), None, None],
        );
        let in_filter = LogFilter::new(
            token_address,
            [Some(TRANSFER_TOPIC), None, Some(owner_topic), None],
        );

        Self {
            owner,
            token_address,
            out_transfers: chain.logs(&out_filter, Finality::Latest),
            in_transfers: chain.logs(&in_filter, Finality::Latest),
        }
    }

    pub fn owner(&self) -> Address {
        self.owner
    }

    pub fn token_address(&self) -> Address {
        self.token_address
    }

    /// Outgoing (`from == owner`) store. Useful if a consumer needs the
    /// pre-merge stream.
    pub fn out_transfers(&self) -> &HistStore<EvmEvent, EvmPosition> {
        &self.out_transfers
    }

    /// Incoming (`to == owner`) store.
    pub fn in_transfers(&self) -> &HistStore<EvmEvent, EvmPosition> {
        &self.in_transfers
    }

    /// Merged in/out transfer stream, ordered by `(block_number,
    /// log_index)`. Yielded items carry the direction on
    /// `MergedItem::source_key` ([`OUT_KEY`] or [`IN_KEY`]).
    ///
    /// Anchor a window from a plain [`EvmPosition`] with
    /// `OrderedPos::from(pos)`; saved item anchors resume exactly, including
    /// between the two copies of a self-transfer (see the module docs).
    pub fn transfers(
        &self,
    ) -> HistStore<MergedItem<EvmEvent, EvmPosition>, OrderedPos<EvmPosition>> {
        let mut sources: BTreeMap<SourceKey, HistStore<EvmEvent, EvmPosition>> = BTreeMap::new();
        sources.insert(OUT_KEY.into(), self.out_transfers.clone());
        sources.insert(IN_KEY.into(), self.in_transfers.clone());
        merge_hist_stores(sources)
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
