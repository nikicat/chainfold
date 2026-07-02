//! Construction options and public value types for
//! [`EvmChain`](super::EvmChain).

use std::time::Duration;

use alloy::primitives::Address;
use alloy::rpc::types::Filter;

use crate::evm::types::{BlockHash, BlockNumber, ChunkSize, Topic};

/// Default chunk size for `eth_getLogs` gap fills.
pub const DEFAULT_CHUNK_SIZE: ChunkSize = ChunkSize::new(1000);

/// Assumed block time for
/// [`HistStore::time_pos`](crate::arch::HistStore::time_pos) estimation.
pub const DEFAULT_BLOCK_TIME: Duration = Duration::from_secs(12);

/// Choice of live-tracking transport.
#[derive(Clone, Debug)]
pub enum SubscribeTransport {
    /// Native `eth_subscribe` via a pubsub-capable provider (websocket /
    /// ipc). Sub-second latency. Provider must be built with `connect_ws`
    /// or similar.
    Ws,
    /// HTTP poll fallback. `interval` controls the head-poll cadence.
    Poll { interval: Duration },
}

impl SubscribeTransport {
    /// Convenience: `Poll { interval: 2s }`.
    pub fn poll_default() -> Self {
        Self::Poll {
            interval: Duration::from_secs(2),
        }
    }
}

/// Chain-wide finality tier (see `docs/ARCHITECTURE.md`, "three finality
/// levels"). Until Phase 7 wires `best`/`safe` producers into the head
/// clock, all tiers resolve to `Latest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Finality {
    /// Chain head as announced (may reorg).
    Latest,
    /// Mined + confidence heuristic. **Currently mirrors `Latest`.**
    Best,
    /// Finalized. **Currently mirrors `Latest`.**
    Safe,
}

/// A single EVM log filter: contract address + the four topic slots. The
/// unit [`EvmChain::logs`](super::EvmChain::logs) is keyed by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFilter {
    pub address: Address,
    pub topics: [Option<Topic>; 4],
}

impl LogFilter {
    /// Filter on `address` with no topic constraints.
    pub fn address(address: Address) -> Self {
        Self {
            address,
            topics: [None; 4],
        }
    }

    /// Filter on `address` + topic constraints.
    pub fn new(address: Address, topics: [Option<Topic>; 4]) -> Self {
        Self { address, topics }
    }

    /// The alloy [`Filter`] equivalent (no block bounds).
    pub(super) fn to_alloy(&self) -> Filter {
        super::rpc::apply_topics(Filter::new().address(self.address), &self.topics)
    }
}

impl std::fmt::Display for LogFilter {
    /// `address[topic0,topic1,topic2,topic3]` with `*` for open slots.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}[", self.address)?;
        for (i, t) in self.topics.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            match t {
                Some(t) => write!(f, "{t}")?,
                None => f.write_str("*")?,
            }
        }
        f.write_str("]")
    }
}

/// One observation of the chain head. The header behind `hash` is always
/// persisted to the cache before the event is published.
#[derive(Clone, Copy, Debug)]
pub struct HeadEvent {
    pub number: BlockNumber,
    pub hash: BlockHash,
}

/// Construction options for [`EvmChain`](super::EvmChain).
#[derive(Clone, Debug)]
pub struct EvmChainConfig {
    /// `eth_getLogs` gap-fill chunk size.
    pub chunk_size: ChunkSize,
    /// Assumed time per block, for
    /// [`HistStore::time_pos`](crate::arch::HistStore::time_pos) estimation.
    pub block_time: Duration,
    /// `Some(transport)` starts the head clock at construction and gives
    /// every log store live wiring. `None` builds a one-shot chain: the
    /// head only moves via [`EvmChain::refresh_head`](super::EvmChain::refresh_head).
    pub follow: Option<SubscribeTransport>,
}

impl Default for EvmChainConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_time: DEFAULT_BLOCK_TIME,
            follow: None,
        }
    }
}
