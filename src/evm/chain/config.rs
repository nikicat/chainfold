//! Construction options for [`EvmChain`](super::EvmChain).

use alloy::network::Ethereum;
use alloy::providers::RootProvider;

use crate::chain::ChunkSize;
use std::time::Duration;

/// Default chunk size for `eth_getLogs` gap fills.
pub const DEFAULT_CHUNK_SIZE: ChunkSize = ChunkSize::new(1000);

/// Assumed block time for
/// [`HistStore::time_pos`](crate::arch::HistStore::time_pos) estimation.
pub const DEFAULT_BLOCK_TIME: Duration = Duration::from_secs(12);

/// Choice of head-tracking transport.
#[derive(Clone, Debug)]
pub enum SubscribeTransport {
    /// Native `eth_subscribe` over a dedicated pubsub connection
    /// (websocket / ipc) that carries **subscriptions only** — `newHeads`
    /// and the per-filter log pushes. Every fetch (backfills, header
    /// fills, archival queries) stays on the chain's main provider:
    /// hosted providers route http through load balancers and recommend
    /// it for archival work, and ws-routed queries can bill higher — so
    /// the subscriber never serves a request the http side could.
    Ws {
        /// The pubsub-capable connection `eth_subscribe` rides.
        subscriber: RootProvider<Ethereum>,
    },
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

/// Construction options for [`EvmChain`](super::EvmChain).
#[derive(Clone, Debug)]
pub struct EvmChainConfig {
    /// `eth_getLogs` gap-fill chunk size.
    pub chunk_size: ChunkSize,
    /// Assumed time per block, for
    /// [`HistStore::time_pos`](crate::arch::HistStore::time_pos) estimation.
    pub block_time: Duration,
    /// Head-tracking transport. Tracking starts lazily — exactly once, on
    /// the first demand for a tip (a consumer awaiting a snapshot, or a
    /// time-position estimate resolving its anchor) — and then runs for
    /// the life of the chain and its stores. Construction itself performs
    /// no RPC and spawns nothing.
    pub transport: SubscribeTransport,
}

impl Default for EvmChainConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_time: DEFAULT_BLOCK_TIME,
            transport: SubscribeTransport::poll_default(),
        }
    }
}
