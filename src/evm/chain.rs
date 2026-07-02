//! [`EvmChain`] — the per-chain client entity. Owns the provider, the
//! durable [`LogCache`], **one** head clock, and vends per-filter log
//! [`HistStore`]s backed by [`RangeIndexStore<LogSpec>`].
//!
//! ## Head clock
//!
//! The chain runs at most one head-tracking task (`newHeads` over WS, or an
//! `eth_blockNumber` poll), regardless of how many log stores exist. Every
//! head it observes has its block header persisted to the cache *before*
//! the head event is published — the headers-present invariant that lets
//! [`LogSpec::resolve`](super::range_index::LogSpec) fill timestamps
//! synchronously.
//!
//! Finality is a chain-wide fact: [`EvmChain::head`] takes a [`Finality`]
//! tier. Until Phase 7 wires real `best`/`safe` producers, all tiers mirror
//! `Latest`.
//!
//! ## Per-source log delivery
//!
//! Log delivery stays **per source** (= per filter): one chain-wide log
//! subscription cannot express `topic1=owner OR topic2=owner` without
//! refetching everything client-side. Each store created by
//! [`EvmChain::logs`] gets its own wiring task (see [`wiring`]), tied to
//! the store's lifetime.
//!
//! Split across submodules:
//! - [`config`] — [`EvmChainConfig`], [`SubscribeTransport`], [`Finality`],
//!   [`LogFilter`], [`HeadEvent`].
//! - [`head_clock`] — the chain-wide head-tracking task.
//! - [`wiring`] — the per-store tasks driving `on_events` / `on_bump`.
//! - [`rpc`] — provider helpers shared by all of the above (header
//!   ensure, gap-fill fetcher).

use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use crate::arch::{HistStore, HistoryError, Store, Timestamp};
use crate::evm::cache::{LogCache, compute_filter_hash};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::range_index::{LogSpec, RangeIndex, RangeIndexStore, TimePosFn};
use crate::evm::types::{BlockNumber, ChainId, ChunkSize, EvmEvent, EvmPosition, LogIdx};
use crate::shared::Shared;

mod config;
mod head_clock;
mod rpc;
mod wiring;

pub use config::{
    DEFAULT_BLOCK_TIME, DEFAULT_CHUNK_SIZE, EvmChainConfig, Finality, HeadEvent, LogFilter,
    SubscribeTransport,
};

/// Reconnect backoff bounds shared by the head clock and the ws wiring.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

// ============================================================
// EvmChain
// ============================================================

pub struct EvmChain<P> {
    provider: ChainProvider<P>,
    chunk_size: ChunkSize,
    follow: Option<SubscribeTransport>,

    /// The single head clock output. Holding only the sender: `.borrow()`
    /// reads, `.subscribe()` mints receivers for wiring tasks and
    /// [`Self::head`] stores.
    head: watch::Sender<HeadEvent>,

    // Wall-clock → block-number estimation. Anchored at the construction
    // head rather than genesis because some chains use timestamp 0 there.
    anchor_block: BlockNumber,
    anchor_time: Timestamp,
    block_time: Duration,

    /// Head-clock task (follow mode only); aborted when the chain drops.
    _head_clock: Option<AbortOnDropHandle<()>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmChain<P> {
    /// Connect the chain entity: fetches the current head once (one
    /// `eth_blockNumber` + a cache-first header fetch — per *chain*, not
    /// per filter), seeds the head clock with it, and, in follow mode,
    /// spawns the head-tracking task. The durable cache rides in the
    /// [`ChainProvider`].
    pub async fn new(
        provider: ChainProvider<P>,
        config: EvmChainConfig,
    ) -> Result<Self, HistoryError> {
        let head_number = provider.get_block_number().await?;
        let header = provider.ensure_block_header(head_number).await?;

        let (head, _) = watch::channel(HeadEvent {
            number: head_number,
            hash: header.hash,
        });

        let clock = config.follow.as_ref().map(|transport| {
            AbortOnDropHandle::new(tokio::spawn(head_clock::run(
                provider.clone(),
                transport.clone(),
                head.clone(),
            )))
        });

        tracing::debug!(
            chain_id = %provider.chain_id(),
            head = head_number.get(),
            follow = ?config.follow,
            chunk_size = %config.chunk_size,
            "EvmChain created"
        );

        Ok(Self {
            provider,
            chunk_size: config.chunk_size,
            follow: config.follow,
            head,
            anchor_block: head_number,
            anchor_time: Timestamp::new(header.timestamp),
            block_time: config.block_time,
            _head_clock: clock,
        })
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

    /// Reactive chain head for a finality tier. **All tiers currently
    /// mirror `Latest`** — `best`/`safe` producers land with Phase 7.
    pub fn head(&self, tier: Finality) -> Store<HeadEvent> {
        let _ = tier; // Latest-only until Phase 7.
        Store::new(self.head.subscribe())
    }

    /// One-shot head poll: fetch the latest block number and, if it
    /// advanced, publish it (persisting the header first). Drives every
    /// log store's tip in non-follow mode. Returns the (possibly
    /// unchanged) head number.
    pub async fn refresh_head(&self) -> Result<BlockNumber, HistoryError> {
        let n = self.provider.get_block_number().await?;
        if n > self.head.borrow().number {
            let header = self.provider.ensure_block_header(n).await?;
            self.head.send_replace(HeadEvent {
                number: n,
                hash: header.hash,
            });
        }
        Ok(self.head.borrow().number)
    }

    /// Open a log history for `filter` at `tier`. Cheap and synchronous —
    /// no RPC; cached state materializes lazily, gaps fill on demand. The
    /// store's tip is seeded from the current head and then driven by this
    /// chain's head clock (plus, in ws follow mode, a narrow per-filter
    /// log subscription). All background wiring dies with the returned
    /// store.
    pub fn logs(&self, filter: &LogFilter, tier: Finality) -> HistStore<EvmEvent, EvmPosition> {
        let _ = tier; // Latest-only until Phase 7.

        let filter_hash = compute_filter_hash(&filter.address, &filter.topics);
        let spec = LogSpec::new(self.cache().share(), filter_hash);
        let fetcher = rpc::build_fetcher(self.provider.clone(), filter.address, filter.topics);
        let index = Arc::new(RangeIndex::with_chunk_size(spec, fetcher, self.chunk_size));
        let store = RangeIndexStore::new(index, self.time_pos_fn());

        // Seed the tip so consumers can iterate immediately in one-shot
        // mode; the wiring below advances it from there.
        let head = *self.head.borrow();
        store.on_bump(tip_pos(head.number));

        let wiring = match &self.follow {
            None => tokio::spawn(wiring::mirror_head(
                self.head.subscribe(),
                Arc::downgrade(&store),
            )),
            Some(SubscribeTransport::Poll { .. }) => tokio::spawn(wiring::poll_follow(
                self.head.subscribe(),
                Arc::downgrade(&store),
                head.number,
            )),
            Some(SubscribeTransport::Ws) => {
                // Live pages persist via the async drain (never SQLite on
                // the delivery path).
                store.index().attach_drain();
                tokio::spawn(wiring::ws_follow(
                    self.provider.clone(),
                    filter.to_alloy(),
                    self.head.subscribe(),
                    Arc::downgrade(&store),
                    head.number,
                ))
            }
        };
        store.attach_wiring(AbortOnDropHandle::new(wiring));

        tracing::debug!(
            chain_id = %self.provider.chain_id(),
            filter = %filter,
            filter_hash = %format!("0x{}", hex::encode(&filter_hash.as_slice()[..8])),
            "log store opened"
        );

        store.hist_store()
    }

    fn time_pos_fn(&self) -> TimePosFn<EvmPosition> {
        let (anchor_block, anchor_time, block_time) =
            (self.anchor_block, self.anchor_time, self.block_time);
        Arc::new(move |time: Timestamp| {
            let block = estimate_block_from_time(anchor_block, anchor_time, block_time, time);
            // The cut "just before `block`" — iteration after this anchor
            // starts with the first log in `block`.
            Ok(match block.checked_sub(1) {
                None => EvmPosition::earliest(),
                Some(prev) => EvmPosition {
                    block: prev,
                    log_index: LogIdx::MAX,
                },
            })
        })
    }
}

/// The end-of-block tip position for a head block: a `Hist` ending here
/// iterates every log up to and including the last one in `n`.
fn tip_pos(n: BlockNumber) -> EvmPosition {
    EvmPosition {
        block: n,
        log_index: LogIdx::MAX,
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
