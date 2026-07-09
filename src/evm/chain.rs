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
//! [`Finality`] tier. Until Phase 7 wires real `best`/`safe` producers,
//! all tiers mirror `Latest`.
//!
//! Split across submodules — one per source, plus the chain-wide plumbing:
//! - [`logs`] — the logs source: [`LogFilter`], the `eth_getLogs` fetcher,
//!   ws push wiring, store + bounded hist.
//! - [`blocks`] — the blocks source: batched `eth_getBlockByNumber`
//!   fetcher, store + bounded hist.
//! - [`config`] — [`EvmChainConfig`], [`SubscribeTransport`], [`Finality`].
//! - [`head_clock`] — the chain-wide head-tracking task.
//! - [`wiring`] — the shared per-store task driving `on_bump` off the
//!   head channel.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::sync::{OnceCell, watch};
use tokio_util::task::AbortOnDropHandle;

use crate::arch::{HistoryError, Timestamp};
use crate::chain::{BlockNumber, ChunkSize};
use crate::evm::cache::LogCache;
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::ChainId;
use crate::range_index::TipSeed;
use crate::shared::Shared;

mod blocks;
mod config;
mod head_clock;
mod logs;
#[cfg(test)]
mod testkit;
mod wiring;

pub use config::{
    DEFAULT_BLOCK_TIME, DEFAULT_CHUNK_SIZE, EvmChainConfig, Finality, SubscribeTransport,
};
pub use logs::LogFilter;

/// Reconnect backoff bounds shared by the head clock and the ws wiring.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

// ============================================================
// EvmChain
// ============================================================

pub struct EvmChain<P> {
    provider: ChainProvider<P>,
    chunk_size: ChunkSize,
    block_time: Duration,

    /// Shared head machinery: the head channel plus the demand-started
    /// clock. Every store holds it as its [`TipSeed`], so tracking (and
    /// the stores it drives) outlives the chain handle itself.
    seeder: Arc<HeadSeeder<P>>,

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

        Self {
            seeder: Arc::new(HeadSeeder {
                provider: provider.clone(),
                transport: config.transport,
                head: HeadCell::new(),
                clock: OnceLock::new(),
            }),
            provider,
            chunk_size: config.chunk_size,
            block_time: config.block_time,
            anchor: Arc::new(OnceCell::new()),
        }
    }

    pub fn chain_id(&self) -> ChainId {
        self.provider.chain_id()
    }

    /// The shared durable cache.
    pub fn cache(&self) -> &Shared<LogCache> {
        self.provider.log_cache()
    }

    /// The underlying provider (chain-id-carrying).
    pub fn provider(&self) -> &ChainProvider<P> {
        &self.provider
    }

    /// Estimate the block mined around wall-clock `time`, from the lazily
    /// resolved anchor + the configured block time. The first call demands
    /// the head (starting tracking if needed) and reads one header
    /// (cache-first); afterwards it is pure arithmetic.
    pub async fn estimate_block(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        let anchor = self.lazy_anchor().get().await?;
        Ok(estimate_block_from_time(
            anchor.block,
            anchor.time,
            self.block_time,
            time,
        ))
    }

    fn lazy_anchor(&self) -> LazyAnchor<P> {
        LazyAnchor {
            seeder: self.seeder.clone(),
            cell: self.anchor.clone(),
        }
    }
}

// ============================================================
// Head machinery
// ============================================================

/// One observation of the chain head, as published on the internal head
/// channel by the clock. Heads published by the **ws** clock have their
/// block header persisted to the cache before the event goes out (the
/// headers-present invariant for push-delivered logs); poll heads carry no
/// header claim. Consumers needing the hash read it from the cache by
/// number (`ensure_block_header`).
#[derive(Clone, Copy, Debug)]
struct HeadEvent {
    number: BlockNumber,
}

/// The chain-wide head channel: `None` until the first observation;
/// publishes are monotonic. The chain-level analog of the store's
/// unset-until-seeded tip.
#[derive(Clone)]
struct HeadCell {
    chan: watch::Sender<Option<HeadEvent>>,
}

impl HeadCell {
    fn new() -> Self {
        Self {
            chan: watch::channel(None).0,
        }
    }

    fn subscribe(&self) -> watch::Receiver<Option<HeadEvent>> {
        self.chan.subscribe()
    }

    /// Monotonic publish: a no-op unless `number` advances past the
    /// current head.
    fn publish(&self, number: BlockNumber) {
        self.chan.send_if_modified(|cur| match cur {
            Some(c) if c.number >= number => false,
            _ => {
                *cur = Some(HeadEvent { number });
                true
            }
        });
    }

    /// Await the first head observation — purely reactive, no fetching.
    async fn observed(&self) -> BlockNumber {
        let mut rx = self.subscribe();
        loop {
            if let Some(h) = *rx.borrow_and_update() {
                return h.number;
            }
            // `self` holds the sender — the channel cannot close.
            rx.changed().await.expect("head sender lives with the cell");
        }
    }
}

/// Chain-side [`TipSeed`]: the first demand for a tip starts head tracking
/// over the configured transport, exactly once; observed heads flow
/// through the [`HeadCell`] to every store's wiring. Tracking runs until
/// the chain and all its stores drop (no stop-on-idle yet).
struct HeadSeeder<P> {
    provider: ChainProvider<P>,
    transport: SubscribeTransport,
    head: HeadCell,
    clock: OnceLock<AbortOnDropHandle<()>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> TipSeed for HeadSeeder<P> {
    fn request(&self) {
        self.clock.get_or_init(|| {
            AbortOnDropHandle::new(tokio::spawn(head_clock::run(
                self.provider.clone(),
                self.transport.clone(),
                self.head.clone(),
            )))
        });
    }
}

// ============================================================
// Wall-clock → block estimation
// ============================================================

/// Anchor for wall-clock → block estimation: a recent `(block, timestamp)`
/// observation. Anchored at a recent head rather than genesis because some
/// chains use timestamp 0 there.
#[derive(Clone, Copy)]
struct TimeAnchor {
    block: BlockNumber,
    time: Timestamp,
}

/// Lazy shared resolver for the chain's [`TimeAnchor`]: demands the head
/// (starting tracking if needed), awaits the observation reactively, then
/// reads the header cache-first — at most one resolution per chain.
struct LazyAnchor<P> {
    seeder: Arc<HeadSeeder<P>>,
    cell: Arc<OnceCell<TimeAnchor>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> LazyAnchor<P> {
    async fn get(&self) -> Result<TimeAnchor, HistoryError> {
        self.cell
            .get_or_try_init(|| async {
                self.seeder.request();
                let block = self.seeder.head.observed().await;
                let header = self.seeder.provider.ensure_block_header(block).await?;
                Ok(TimeAnchor {
                    block,
                    time: header.timestamp,
                })
            })
            .await
            .copied()
    }
}

fn estimate_block_from_time(
    anchor_block: BlockNumber,
    anchor_time: Timestamp,
    block_time: Duration,
    time: Timestamp,
) -> BlockNumber {
    let block_time = block_time.as_secs().max(1);
    if time >= anchor_time {
        anchor_block + (time.get() - anchor_time.get()) / block_time
    } else {
        anchor_block.saturating_sub((anchor_time.get() - time.get()) / block_time)
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
