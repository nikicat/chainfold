//! [`EvmChain`] — the per-chain client entity. Owns the provider, the
//! durable [`LogCache`], **one** demand-started head clock, and vends the
//! per-source histories: logs ([`logs`](EvmChain::logs) /
//! [`logs_hist`](EvmChain::logs_hist)) and blocks
//! ([`blocks`](EvmChain::blocks) / [`blocks_hist`](EvmChain::blocks_hist)).
//!
//! ## Head clock
//!
//! The chain runs at most one head-tracking task (`newHeads` over WS, or an
//! `eth_blockNumber` poll), regardless of how many stores exist. Tracking
//! is **demand-started**: construction performs no RPC and spawns nothing;
//! the first demand for a tip (a consumer awaiting a snapshot, or a
//! time-position estimate resolving its anchor) starts the clock exactly
//! once, and it then runs for the life of the chain and its stores.
//!
//! Every head the **ws** clock publishes has its block header persisted to
//! the cache *before* the head event goes out — the headers-present
//! invariant that lets [`LogSpec::resolve`](super::LogSpec)
//! fill push-delivered timestamps synchronously. Poll heads carry no
//! header claim: in poll mode every log arrives through the fetcher, which
//! ensures its own headers.
//!
//! Explicitly bounded histories ([`EvmChain::blocks_hist`] /
//! [`EvmChain::logs_hist`]) never demand the head at all, so a
//! fully-cached walk is completely offline.
//!
//! Finality is a chain-wide fact: the store constructors take a
//! [`Finality`] tier. Until the finality phase wires real `best`/`safe`
//! producers, all tiers mirror `Latest`.
//!
//! Split across submodules — one per source, plus the chain-wide plumbing:
//! - [`logs`] — the logs source: [`LogFilter`], the `eth_getLogs` fetcher,
//!   ws push wiring, store + bounded hist.
//! - [`blocks`] — the blocks source: batched `eth_getBlockByNumber`
//!   fetcher, store + bounded hist.
//! - [`config`] — [`EvmChainConfig`], [`SubscribeTransport`].
//! - [`head_clock`] — the EVM [`HeadClock`] sessions (ws / poll).
//!
//! The demand-started clock owner ([`HeadSeeder`]), the head channel, the
//! per-store `follow_head` wiring, [`Finality`], and the wall-clock anchor
//! machinery are shared scaffolding in [`crate::chain`].

use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::sync::OnceCell;

use crate::arch::{HistoryError, Timestamp};
use crate::chain::{
    BlockNumber, BlockRange, BlockTimeSource, ChunkSize, HeadClock, HeadSeeder, TimeAnchor,
    TimeLocator,
};
use crate::evm::cache::LogCache;
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::ChainId;
use crate::share::Share;

mod blocks;
mod config;
mod fetch;
mod head_clock;
mod logs;
#[cfg(test)]
mod testkit;

pub use config::{DEFAULT_BLOCK_TIME, DEFAULT_CHUNK_SIZE, EvmChainConfig, SubscribeTransport};
pub use logs::LogFilter;

// ============================================================
// EvmChain
// ============================================================

pub struct EvmChain<P> {
    provider: ChainProvider<P>,
    chunk_size: ChunkSize,
    block_time: Duration,
    /// Head-tracking transport; also steers the per-filter log wiring
    /// (ws push session vs. head-driven fetch).
    transport: SubscribeTransport,

    /// Shared head machinery: the head channel plus the demand-started
    /// clock (an [`EvmHeadClock`](head_clock::EvmHeadClock) session under
    /// the shared backoff runner). Every store holds it as its
    /// [`TipSeed`](crate::range_index::TipSeed), so tracking (and the
    /// stores it drives) outlives the chain handle itself.
    seeder: Arc<HeadSeeder>,

    /// Wall-clock → block-number estimation anchor, resolved lazily on the
    /// first `time_pos` / [`Self::estimate_block`] call.
    anchor: Arc<OnceCell<TimeAnchor>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmChain<P> {
    /// Create the chain entity. Construction does nothing observable: no
    /// RPC, no tasks. Head tracking over `config.transport` starts on the
    /// first demand for a tip (see the module docs). The durable cache
    /// rides in the [`ChainProvider`].
    pub fn new(provider: ChainProvider<P>, config: EvmChainConfig) -> Self {
        tracing::debug!(
            chain_id = %provider.chain_id(),
            transport = ?config.transport,
            chunk_size = %config.chunk_size,
            "EvmChain created"
        );

        let clock = head_clock::EvmHeadClock::new(provider.clone(), config.transport.clone());
        Self {
            seeder: Arc::new(HeadSeeder::new(Arc::new(clock) as Arc<dyn HeadClock>)),
            provider,
            chunk_size: config.chunk_size,
            block_time: config.block_time,
            transport: config.transport,
            anchor: Arc::new(OnceCell::new()),
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.provider.chain_id()
    }

    /// The shared durable cache.
    pub fn cache(&self) -> &Arc<LogCache> {
        self.provider.log_cache()
    }

    /// The underlying provider (chain-id-carrying).
    pub fn provider(&self) -> &ChainProvider<P> {
        &self.provider
    }

    /// The first block minted at/after wall-clock `time` (clamped to the
    /// head observed when the anchor resolved). The nominal block cadence
    /// seeds only an initial guess, refined against real block timestamps
    /// by the shared [`TimeLocator`] — cache-first, so warm searches are
    /// offline. The first call demands the head (starting tracking if
    /// needed).
    pub async fn estimate_block(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        self.locator().locate(time).await
    }

    fn locator(&self) -> TimeLocator {
        TimeLocator::new(
            self.seeder.share(),
            Arc::new(EvmBlockTimeSource {
                provider: self.provider.clone(),
            }),
            self.anchor.share(),
            self.block_time,
        )
    }
}

/// The EVM [`BlockTimeSource`]: block `n`'s timestamp, cache-first across
/// both durable tables — `cached_block_headers`, then `cached_blocks` (the
/// blocks history stores full rows the header writers never see) — before
/// the singleflight [`ChainProvider::ensure_block_header`] fetch.
struct EvmBlockTimeSource<P> {
    provider: ChainProvider<P>,
}

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> BlockTimeSource
    for EvmBlockTimeSource<P>
{
    async fn block_time_at(&self, n: BlockNumber) -> Result<Timestamp, HistoryError> {
        let cache = self.provider.log_cache();
        if let Some(block) = cache
            .get_blocks_in_range(BlockRange::new(n, n))
            .map_err(|e| HistoryError::internal(format!("get_blocks_in_range: {e}")))?
            .first()
        {
            return Ok(block.timestamp);
        }
        // ensure_block_header is itself cache-first (headers table) and
        // singleflights the fetch on a miss.
        Ok(self.provider.ensure_block_header(n).await?.timestamp)
    }
}

// ============================================================
// Tests — chain-wide concerns (per-source tests live with the source)
// ============================================================

#[cfg(test)]
mod tests {
    use super::testkit;
    use super::testkit::{GENESIS, TEST_CHAIN, make_chain, offline_chain, ts, warm_cache};
    use super::*;
    use crate::chain::Finality;
    use crate::evm::types::EvmPosition;

    /// Construction (and opening stores) is completely offline: the mock
    /// provider has no responses queued, so any RPC would fail loudly.
    #[tokio::test]
    async fn construction_and_store_open_are_offline() {
        let (cache, _) = warm_cache();
        let chain = offline_chain(cache);
        // Opening stores is side-effect-free — nothing demands the head.
        let _logs = chain.logs(&LogFilter::address(testkit::ADDR), Finality::Latest);
        let _blocks = chain.blocks(Finality::Latest);
    }

    /// The anchor resolves lazily: the first estimate demands the head
    /// (the clock's first poll consumes the one queued `eth_blockNumber`)
    /// and reads the cached header; afterwards it is pure arithmetic.
    #[tokio::test]
    async fn estimate_block_resolves_anchor_on_demand() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        // Anchor: HEAD @ ts(2400); block_time = 12s. 1200 s earlier
        // → 100 blocks before the anchor.
        assert_eq!(
            chain.estimate_block(ts(1200)).await.unwrap(),
            BlockNumber::new(100)
        );
        // Second call: no head demand, no header read — the queue is
        // empty, so an RPC here would error.
        assert_eq!(
            chain.estimate_block(GENESIS).await.unwrap(),
            BlockNumber::ZERO
        );
    }

    /// `chain_id` stays on `EvmChain`, never on positions.
    #[tokio::test]
    async fn chain_id_is_on_chain_not_position() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        assert_eq!(chain.chain_id(), TEST_CHAIN);
        // EvmPosition has only block + log_index — no chain field.
        let p = EvmPosition::new(100, 0);
        let s = format!("{p:?}");
        assert!(
            !s.contains("chain"),
            "EvmPosition Debug should not mention chain: {s}"
        );
    }
}
