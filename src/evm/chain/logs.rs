//! The logs source: per-filter log histories over
//! [`RangeIndexStore<LogSpec>`] — [`LogFilter`], the reactive store
//! ([`EvmChain::logs`]) with its ws push wiring, and the bounded hist
//! ([`EvmChain::logs_hist`]). The gap-fill fetcher is the adaptive
//! slicing executor in [`fetch`](super::fetch).
//!
//! ## Per-source log delivery
//!
//! Log delivery stays **per source** (= per filter): one chain-wide log
//! subscription cannot express `topic1=owner OR topic2=owner` without
//! refetching everything client-side. Each store created by
//! [`EvmChain::logs`] gets its own wiring task, tied to the store's
//! lifetime.
//!
//! ## Why ws coverage and log delivery share one session
//!
//! A head advance is recorded as an *empty coverage page* — the claim
//! "these blocks were scanned; no logs matched". That claim is only sound
//! while this filter's log subscription is actually delivering. Splitting
//! the two (heads healthy, logs dead) would persist false coverage that a
//! later gap-fill trusts and skips. So [`ws_log_session`] interleaves both
//! in one `select!`; when the log subscription drops, coverage stops too,
//! and [`ws_follow`] fetch-fills `(last_tip, head]` before trusting push
//! delivery again.

use std::sync::{Arc, Weak};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::Filter;
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_util::task::AbortOnDropHandle;

use super::EvmChain;
use super::fetch::LogFetcher;
use crate::arch::{Hist, HistStore, HistoryError, Timestamp};
use crate::chain::{BlockNumber, BlockRange};
use crate::chain::{Finality, HeadEvent, INITIAL_BACKOFF, MAX_BACKOFF, TimeLocator, follow_head};
use crate::evm::cache::compute_filter_hash;
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::{BlockHash, EvmEvent, EvmPosition, LogIdx, Topic};
use crate::evm::{LogPage, LogSpec};
use crate::range_index::{RangeIndex, RangeIndexStore, RangeSpec, TimePos, TipSeed};
use crate::share::Share;

// ============================================================
// LogFilter
// ============================================================

/// A single EVM log filter: contract address + the four topic slots. The
/// unit [`EvmChain::logs`] is keyed by.
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
    fn to_alloy(&self) -> Filter {
        apply_topics(Filter::new().address(self.address), &self.topics)
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

// ============================================================
// EvmChain — logs source
// ============================================================

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmChain<P> {
    /// Open a log history for `filter` at `tier`. Cheap, synchronous and
    /// side-effect-free — no RPC; cached state materializes lazily, gaps
    /// fill on demand. The store's tip is driven by this chain's head
    /// clock (plus, on a ws transport, a narrow per-filter log
    /// subscription); the first consumer awaiting a snapshot demand-starts
    /// the clock. All background wiring dies with the returned store.
    pub fn logs(&self, filter: &LogFilter, tier: Finality) -> HistStore<EvmEvent, EvmPosition> {
        let _ = tier; // Latest-only until the finality phase.

        let store = self.log_store(filter, Some(self.seeder.share() as Arc<dyn TipSeed>));

        let head_rx = self.seeder.head().subscribe();
        let wiring = match &self.transport {
            super::SubscribeTransport::Poll { .. } => {
                tokio::spawn(follow_head(head_rx, Arc::downgrade(&store)))
            }
            super::SubscribeTransport::Ws { subscriber } => {
                // Live pages persist via the async drain (never SQLite on
                // the delivery path).
                store.index().attach_drain();
                tokio::spawn(ws_follow(
                    self.provider().clone(),
                    subscriber.clone(),
                    filter.to_alloy(),
                    head_rx,
                    Arc::downgrade(&store),
                ))
            }
        };
        store.attach_wiring(AbortOnDropHandle::new(wiring));

        tracing::debug!(
            chain_id = %self.chain_id(),
            filter = %filter,
            "log store opened"
        );

        store.hist_store()
    }

    /// Open a finite log history for `filter` over the explicit inclusive
    /// block `range`. Head-free: nothing is fetched at construction, the
    /// clock is never demanded, and a fully-cached walk performs no RPC at
    /// all; gaps fill on demand through the chunked `eth_getLogs` fetcher.
    /// A range extending beyond the real chain head fails loudly at
    /// iteration rather than clamping.
    pub fn logs_hist(&self, filter: &LogFilter, range: BlockRange) -> Hist<EvmEvent, EvmPosition> {
        let store = self.log_store(filter, None);

        tracing::debug!(
            chain_id = %self.chain_id(),
            filter = %filter,
            %range,
            "bounded log hist opened"
        );

        Hist::new(store, log_cut_before(range.from), tip_pos(range.to))
    }

    /// The store both entry points share: spec + fetcher + reactive layer.
    fn log_store(
        &self,
        filter: &LogFilter,
        tip_seed: Option<Arc<dyn TipSeed>>,
    ) -> Arc<RangeIndexStore<LogSpec>> {
        let filter_hash = compute_filter_hash(&filter.address, &filter.topics);
        let spec = LogSpec::new(self.cache().share(), filter_hash);
        let fetcher = Arc::new(LogFetcher::new(
            self.provider().clone(),
            filter.to_alloy(),
            self.chunk_size,
        ));
        let index = Arc::new(RangeIndex::with_chunk_size(spec, fetcher, self.chunk_size));
        RangeIndexStore::new(
            index,
            Arc::new(LogTimePos {
                locator: self.locator(),
            }),
            tip_seed,
        )
    }
}

// ============================================================
// Positions
// ============================================================

/// The end-of-block tip position for a head block: a `Hist` ending here
/// iterates every log up to and including the last one in `n`.
pub(super) fn tip_pos(n: BlockNumber) -> EvmPosition {
    LogSpec::head_pos(n)
}

/// The log-space cut "just before `block`": iteration after this cut
/// starts with the first log in `block`.
fn log_cut_before(block: BlockNumber) -> EvmPosition {
    match block.checked_sub(1) {
        None => EvmPosition::earliest(),
        Some(prev) => EvmPosition {
            block: prev,
            log_index: LogIdx::MAX,
        },
    }
}

/// [`TimePos`] for the log position space: the cut "just before" the
/// estimated block, so iteration after the anchor starts with that block's
/// first log.
struct LogTimePos {
    locator: TimeLocator,
}

#[async_trait::async_trait]
impl TimePos<EvmPosition> for LogTimePos {
    async fn time_pos(&self, time: Timestamp) -> Result<EvmPosition, HistoryError> {
        Ok(log_cut_before(self.locator.locate(time).await?))
    }
}

// ============================================================
// Filter plumbing
// ============================================================

/// Apply `topics[0..3]` to an alloy [`Filter`] in the conventional slots
/// (`event_signature` / `topic1` / `topic2` / `topic3`). Skips slots that
/// are `None` (matches anything in that position).
fn apply_topics(mut filter: Filter, topics: &[Option<Topic>; 4]) -> Filter {
    if let Some(t) = topics[0] {
        filter = filter.event_signature(t.b256());
    }
    if let Some(t) = topics[1] {
        filter = filter.topic1(t.b256());
    }
    if let Some(t) = topics[2] {
        filter = filter.topic2(t.b256());
    }
    if let Some(t) = topics[3] {
        filter = filter.topic3(t.b256());
    }
    filter
}

// ============================================================
// Ws wiring
// ============================================================

type WeakLogStore = Weak<RangeIndexStore<LogSpec>>;

/// Ws log wiring: run [`ws_log_session`]s forever, gap-filling after
/// every disconnect *before* re-entering push delivery, with exponential
/// backoff on transport errors. Holds only a [`Weak`] store reference
/// (the store owns the task via `AbortOnDropHandle`). The `subscriber`
/// connection carries only the `eth_subscribe` traffic; fetches (the
/// reconnect gap-fill, header ensures) ride the main `provider`.
async fn ws_follow<P>(
    provider: ChainProvider<P>,
    subscriber: RootProvider<Ethereum>,
    filter: Filter,
    head: watch::Receiver<Option<HeadEvent>>,
    store: WeakLogStore,
) where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut last_tip: Option<BlockNumber> = None;
    let mut first = true;
    loop {
        if !first {
            // Reconnect gap-fill: fetch everything push delivery may have
            // missed while the session was down. Must succeed before any
            // new empty coverage page is recorded — retry on failure
            // rather than falling through to the session.
            let Some(s) = store.upgrade() else { return };
            let current = (*head.borrow()).map(|h| h.number);
            if let (Some(tip), Some(current)) = (last_tip, current)
                && current > tip
            {
                let range = BlockRange::new(tip + 1, current);
                if let Err(e) = s.index().share().ensure_cached(range).await {
                    tracing::warn!(
                        %range,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "reconnect gap-fill failed, retrying"
                    );
                    drop(s);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                s.on_bump(tip_pos(current));
                last_tip = Some(current);
            }
        }
        first = false;

        match ws_log_session(
            &provider,
            &subscriber,
            &filter,
            head.clone(),
            &store,
            &mut last_tip,
        )
        .await
        {
            Ok(()) => return, // store or chain dropped
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "log subscription error, will reconnect"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// One ws session: narrow log pushes interleaved with head-driven empty
/// coverage. `Ok(())` = orderly shutdown (store/chain gone); `Err` =
/// transport failure, the caller gap-fills and reconnects. Subscribes on
/// the dedicated `subscriber`; header ensures ride the main `provider`.
async fn ws_log_session<P>(
    provider: &ChainProvider<P>,
    subscriber: &RootProvider<Ethereum>,
    filter: &Filter,
    mut head: watch::Receiver<Option<HeadEvent>>,
    store: &WeakLogStore,
    last_tip: &mut Option<BlockNumber>,
) -> Result<(), HistoryError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let sub = subscriber
        .subscribe_logs(filter)
        .await
        .map_err(|e| HistoryError::internal(format!("subscribe_logs: {e}")))?;
    let mut logs = sub.into_stream();

    loop {
        tokio::select! {
            log = logs.next() => {
                let Some(log) = log else {
                    return Err(HistoryError::internal("logs subscription closed"));
                };
                let Some(s) = store.upgrade() else { return Ok(()) };
                // Headers-present invariant: resolve() reads the timestamp
                // from the cached header, synchronously. The head clock
                // usually beat us to it; fall back to one RPC when the log
                // outruns its head event.
                if log.block_timestamp.is_none()
                    && let Some(bn) = log.block_number
                {
                    provider.ensure_block_header(BlockNumber::new(bn)).await?;
                }
                let bn = BlockNumber::new(log.block_number.unwrap_or(0));
                let bh = log.block_hash.map_or(BlockHash::ZERO, BlockHash::new);
                // Single-block page covering exactly this block. Late
                // arrivals relative to head coverage are absorbed by the
                // tree's late-item rules.
                s.on_events(&LogPage {
                    range: BlockRange::new(bn, bn),
                    items: vec![log],
                    end_block_hash: bh,
                });
                *last_tip = Some(last_tip.map_or(bn, |t| t.max(bn)));
            }

            changed = head.changed() => {
                if changed.is_err() {
                    // Head channel closed — chain and every other holder
                    // gone. Keep the store serving cached state, stop the
                    // live wiring.
                    return Ok(());
                }
                let Some(h) = *head.borrow_and_update() else { continue };
                if last_tip.is_some_and(|t| h.number <= t) {
                    continue;
                }
                let Some(s) = store.upgrade() else { return Ok(()) };
                match *last_tip {
                    // First observation with no baseline: no coverage
                    // claim possible — just advance the tip; gaps fill
                    // lazily on iteration.
                    None => s.on_bump(tip_pos(h.number)),
                    Some(tip) => {
                        // Empty coverage page for (tip, h]: "scanned, no
                        // logs (yet)". Sound only because this session's
                        // log subscription is live — see the module docs.
                        // The ws clock persisted this head's header before
                        // publishing, so this resolves from the cache.
                        let header = provider.ensure_block_header(h.number).await?;
                        s.on_events(&LogPage {
                            range: BlockRange::new(tip + 1, h.number),
                            items: vec![],
                            end_block_hash: header.hash,
                        });
                    }
                }
                *last_tip = Some(h.number);
            }
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::TryStreamExt;
    use tokio::time::timeout;
    use tracing::subscriber::set_default;
    use tracing_subscriber::prelude::*;

    use super::super::testkit::{
        ADDR, HEAD, RpcCounter, make_chain, offline_chain, ts, warm_cache,
    };
    use super::*;
    use crate::arch::{Hist, Position, Positioned};
    use crate::cache::ScanPartition;
    use crate::evm::cache::{LogCache, compute_filter_hash};
    use crate::evm::types::{BlockHash, LogIdx};

    fn log_store<P>(chain: &EvmChain<P>) -> HistStore<EvmEvent, EvmPosition>
    where
        P: Provider<Ethereum> + Clone + Send + Sync + 'static,
    {
        chain.logs(&LogFilter::address(ADDR), Finality::Latest)
    }

    /// Iterate a Hist into a Vec<Positioned> with no in-flight fetcher.
    async fn collect(hist: Hist<EvmEvent, EvmPosition>) -> Vec<Positioned<EvmEvent, EvmPosition>> {
        hist.iter().try_collect().await.unwrap()
    }

    #[tokio::test]
    async fn iter_walks_all_logs_in_window() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        // The first consumer await demand-starts the clock, whose first
        // poll observes the head and seeds the store's tip.
        let hist = log_store(&chain).next().await;
        assert_eq!(*hist.end(), tip_pos(HEAD));

        let items = collect(hist).await;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].item.position(), EvmPosition::new(100, 0));
        assert_eq!(items[1].item.position(), EvmPosition::new(100, 1));
        assert_eq!(items[2].item.position(), EvmPosition::new(101, 0));
    }

    #[tokio::test]
    async fn split_clamps_anchor_to_window() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let hist = log_store(&chain).next().await;

        // Split past end ⇒ head = full, tail = empty.
        let (head, tail) = hist.split(EvmPosition::new(9999, 0));
        assert_eq!(collect(head).await.len(), 3);
        assert!(tail.is_empty());

        // Split before start ⇒ head = empty, tail = full.
        let (head, tail) = hist.split(EvmPosition::new(0, 0));
        assert!(head.is_empty());
        assert_eq!(collect(tail).await.len(), 3);

        // Split between logs.
        let (head, tail) = hist.split(EvmPosition::new(100, 0));
        let head_items = collect(head).await;
        let tail_items = collect(tail).await;
        assert_eq!(head_items.len(), 1);
        assert_eq!(head_items[0].item.position(), EvmPosition::new(100, 0));
        assert_eq!(tail_items.len(), 2);
        assert_eq!(tail_items[0].item.position(), EvmPosition::new(100, 1));
    }

    #[tokio::test]
    async fn iter_empty_window_yields_nothing() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let hist = log_store(&chain).next().await;
        // Zero-width window via two splits at the same cut.
        let cut = EvmPosition::new(150, 0);
        let (_, tail) = hist.split(cut);
        let (empty, _) = tail.split(cut);
        assert!(empty.is_empty());
        assert!(collect(empty).await.is_empty());
    }

    #[tokio::test]
    async fn iter_picks_up_timestamp_from_store() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let items = collect(log_store(&chain).next().await).await;
        // Headers inserted by warm_cache: block 100 @ ts(1200), 101 @ ts(1212).
        assert_eq!(items[0].item.timestamp, ts(1200));
        assert_eq!(items[2].item.timestamp, ts(1212));
    }

    /// When a log carries its own `block_timestamp`, resolve stamps the event
    /// from it and never reads a block header — proven by leaving no header
    /// cached for that block and no `eth_getBlockByNumber` queued on the mock
    /// provider (the sync resolve would error on the missing header).
    #[tokio::test]
    async fn iter_uses_log_block_timestamp_without_header_fetch() {
        use super::super::testkit::{BH_0, BH_200, GENESIS, insert_header, make_log};

        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &super::super::testkit::TOPICS);
        insert_header(&cache, BlockNumber::ZERO, BH_0, GENESIS);
        insert_header(&cache, HEAD, BH_200, ts(2400));
        // A log at block 150 WITH a timestamp and deliberately NO header for 150.
        let block_150 = BlockNumber::new(150);
        let mut log = make_log(block_150, LogIdx::ZERO, BlockHash::repeat_byte(0x9A));
        log.block_timestamp = Some(ts(1800).get());
        cache.insert_logs(&fh, std::slice::from_ref(&log)).unwrap();
        cache
            .coverage()
            .record_scanned_range(
                &ScanPartition::Filter(fh),
                BlockRange::new(BlockNumber::ZERO, HEAD),
                BH_200.into(),
            )
            .unwrap();

        let chain = make_chain(cache);
        let hist = log_store(&chain).next().await;
        let (_, tail) = hist.split(tip_pos(block_150 - 1));
        let (window, _) = tail.split(tip_pos(block_150));
        let items = collect(window).await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].item.timestamp, ts(1800));
    }

    #[tokio::test]
    async fn position_marshal_round_trips_via_store_load() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let store = log_store(&chain);

        let p = EvmPosition::new(123_456_789, 42);
        let value = p.marshal();
        // Deserialization lives on the store, not the position.
        let restored = store.load(value).expect("load");
        assert_eq!(restored, p);
    }

    #[tokio::test]
    async fn time_pos_estimates_block() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let store = log_store(&chain);
        // Anchor: HEAD @ ts(2400); block_time = 12s. 1200 s earlier
        // → 100 blocks before the anchor → the cut "just before block 100".
        // Resolving the anchor demands the head (the queued response).
        let pos = store.time_pos(ts(1200)).await.unwrap();
        assert_eq!(pos, tip_pos(BlockNumber::new(99)));
    }

    #[tokio::test]
    async fn hist_store_next_uses_current_head() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let hist = log_store(&chain).next().await;
        assert_eq!(*hist.start(), EvmPosition::earliest());
        assert_eq!(*hist.end(), tip_pos(HEAD));
    }

    /// The head demand is chain-wide singleflight: two stores awaiting
    /// concurrently start ONE clock, whose single queued observation
    /// resolves both (a second fetch would error on the empty mock queue).
    #[tokio::test]
    async fn concurrent_next_demands_head_once() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache); // exactly one queued head answer
        let a = log_store(&chain);
        let b = log_store(&chain);
        let (ha, hb) = tokio::join!(a.next(), b.next());
        assert_eq!(*ha.end(), tip_pos(HEAD));
        assert_eq!(*hb.end(), tip_pos(HEAD));
    }

    /// With the head unobservable (dead RPC), `next()` waits rather than
    /// fabricate a snapshot — the store's unset-tip contract.
    #[tokio::test]
    async fn next_waits_while_head_unobservable() {
        let (cache, _) = warm_cache();
        let chain = offline_chain(cache);
        let store = log_store(&chain);
        let pending = timeout(Duration::from_millis(100), store.next()).await;
        assert!(
            pending.is_err(),
            "next() must wait until a head is observed"
        );
    }

    /// A fully-cached bounded walk is completely offline: the mock
    /// provider has NO responses queued, so any RPC — head demand
    /// included — fails the test loudly.
    #[tokio::test]
    async fn logs_hist_fully_cached_walk_is_offline() {
        let (cache, _) = warm_cache();
        let chain = offline_chain(cache);
        let hist = chain.logs_hist(
            &LogFilter::address(ADDR),
            BlockRange::new(BlockNumber::ZERO, HEAD),
        );
        assert_eq!(*hist.start(), EvmPosition::earliest());
        assert_eq!(*hist.end(), tip_pos(HEAD));
        let items = collect(hist).await;
        assert_eq!(items.len(), 3);
    }

    /// Guardrail for the "minimize RPC" guarantee: a fully-cached walk must
    /// issue **zero** `eth_getLogs`. Counts the observability contract's RPC
    /// spans rather than timing, so a refactor that reintroduces a network
    /// round-trip on the warm path fails here, loudly.
    #[tokio::test]
    async fn warm_walk_issues_zero_get_logs() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache);
        let hist = log_store(&chain).next().await;

        // Install the counter only around the walk — the head observation
        // happens before this and isn't counted.
        let counter = RpcCounter::new("eth_getLogs");
        let guard = set_default(tracing_subscriber::registry().with(counter.clone()));
        let items = collect(hist).await;
        drop(guard);

        assert_eq!(items.len(), 3);
        assert_eq!(
            counter.count(),
            0,
            "a fully-cached walk must not issue eth_getLogs"
        );
    }
}
