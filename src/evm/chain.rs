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

use crate::arch::{HistStore, HistoryError, Signal, Timestamp};
use crate::evm::cache::{LogCache, compute_filter_hash};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::range_index::{
    BlockSpec, LogSpec, RangeIndex, RangeIndexStore, RangeSpec, TimePosFn,
};
use crate::evm::types::{BlockNumber, ChainId, ChunkSize, EvmBlock, EvmEvent, EvmPosition, LogIdx};
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
            anchor_time: header.timestamp,
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
    pub fn head(&self, tier: Finality) -> Signal<HeadEvent> {
        let _ = tier; // Latest-only until Phase 7.
        Signal::new(self.head.subscribe())
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

    /// Open the chain's block history at `tier`. Cheap and synchronous —
    /// no RPC; cached blocks materialize lazily, gaps fill on demand
    /// (batched `eth_getBlockByNumber`). The store's tip is seeded from
    /// the current head and then driven by this chain's head clock.
    ///
    /// In follow mode blocks always arrive via eager gap-fill on each head
    /// advance — there is no push delivery path (`newHeads` payloads lack
    /// the transaction count), so ws and poll transports behave
    /// identically here. All background wiring dies with the returned
    /// store.
    pub fn blocks(&self, tier: Finality) -> HistStore<EvmBlock, BlockNumber> {
        let _ = tier; // Latest-only until Phase 7.

        let spec = BlockSpec::new(self.cache().share());
        let fetcher = rpc::build_block_fetcher(self.provider.clone());
        let index = Arc::new(RangeIndex::with_chunk_size(spec, fetcher, self.chunk_size));
        let store = RangeIndexStore::new(index, self.block_time_pos_fn());

        // Seed the tip so consumers can iterate immediately in one-shot
        // mode; the wiring below advances it from there.
        let head = *self.head.borrow();
        store.on_bump(head.number);

        let wiring = match &self.follow {
            None => tokio::spawn(wiring::mirror_head::<BlockSpec>(
                self.head.subscribe(),
                Arc::downgrade(&store),
            )),
            Some(_) => tokio::spawn(wiring::poll_follow::<BlockSpec>(
                self.head.subscribe(),
                Arc::downgrade(&store),
                head.number,
            )),
        };
        store.attach_wiring(AbortOnDropHandle::new(wiring));

        tracing::debug!(chain_id = %self.provider.chain_id(), "block store opened");

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

    fn block_time_pos_fn(&self) -> TimePosFn<BlockNumber> {
        let (anchor_block, anchor_time, block_time) =
            (self.anchor_block, self.anchor_time, self.block_time);
        Arc::new(move |time: Timestamp| {
            let block = estimate_block_from_time(anchor_block, anchor_time, block_time, time);
            // The cut "just before `block`" — iteration after this anchor
            // starts at `block` itself (ZERO doubles as the lower-bound
            // sentinel, keeping genesis included).
            Ok(block.checked_sub(1).unwrap_or(BlockNumber::ZERO))
        })
    }
}

/// The end-of-block tip position for a head block: a `Hist` ending here
/// iterates every log up to and including the last one in `n`.
fn tip_pos(n: BlockNumber) -> EvmPosition {
    LogSpec::head_pos(n)
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
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::{Hist, Position, Positioned};
    use crate::evm::cache::{ScanPartition, compute_filter_hash};
    use crate::evm::types::{BlockHash, BlockRange, FilterHash, LogIdx, Topic};
    use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData, U64, U256};
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::Log;
    use alloy::transports::mock::Asserter;
    use futures_util::TryStreamExt;

    const ADDR: Address = Address::repeat_byte(0x42);
    const TOPICS: [Option<Topic>; 4] = [None; 4];
    const GENESIS: Timestamp = Timestamp::new(1_600_000_000);
    const TEST_CHAIN: ChainId = ChainId::new(1);
    /// The mock chain head — the one `eth_blockNumber` answer.
    const HEAD: BlockNumber = BlockNumber::new(200);

    const BH_0: BlockHash = BlockHash::ZERO;
    const BH_100: BlockHash = BlockHash::repeat_byte(0xA0);
    const BH_101: BlockHash = BlockHash::repeat_byte(0xA1);
    const BH_200: BlockHash = BlockHash::repeat_byte(0xA2);

    /// `GENESIS` shifted by `secs` — headers are stamped on a 12 s cadence.
    fn ts(secs: u64) -> Timestamp {
        GENESIS + secs
    }

    fn make_log(block: BlockNumber, log_index: LogIdx, block_hash: BlockHash) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);

        Log {
            inner: LogInner {
                address: ADDR,
                data: log_data,
            },
            block_hash: Some(block_hash.b256()),
            block_number: Some(block.get()),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xCC; 32])),
            transaction_index: Some(0),
            log_index: Some(log_index.get() as u64),
            removed: false,
        }
    }

    /// Insert a header row.
    fn insert_header(cache: &LogCache, block: BlockNumber, hash: BlockHash, time: Timestamp) {
        // Parent hash is irrelevant to these tests — resolve only reads
        // (hash, timestamp).
        cache
            .insert_block_header(block, &hash, &BlockHash::ZERO, time)
            .unwrap();
    }

    fn warm_cache() -> (Shared<LogCache>, FilterHash) {
        let cache = Shared::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        insert_header(&cache, BlockNumber::ZERO, BH_0, GENESIS);
        insert_header(&cache, BlockNumber::new(100), BH_100, ts(1200));
        insert_header(&cache, BlockNumber::new(101), BH_101, ts(1212));
        insert_header(&cache, HEAD, BH_200, ts(2400));

        let logs = vec![
            make_log(BlockNumber::new(100), LogIdx::ZERO, BH_100),
            make_log(BlockNumber::new(100), LogIdx::new(1), BH_100),
            make_log(BlockNumber::new(101), LogIdx::ZERO, BH_101),
        ];
        cache.insert_logs(&fh, &logs).unwrap();
        cache
            .record_scanned_range(
                &ScanPartition::Filter(fh),
                BlockRange::new(BlockNumber::ZERO, HEAD),
                &BH_200,
            )
            .unwrap();
        (cache, fh)
    }

    /// One-shot chain over a mock provider: the only RPC is the head
    /// `eth_blockNumber` at construction (the anchor header for [`HEAD`]
    /// is served by the warm cache).
    async fn make_chain(
        cache: Shared<LogCache>,
    ) -> EvmChain<impl Provider<Ethereum> + Clone + 'static> {
        let asserter = Asserter::new();
        asserter.push_success(&U64::from(HEAD.get()));
        let provider = ProviderBuilder::default().connect_mocked_client(asserter);
        let provider = ChainProvider::new(provider, TEST_CHAIN, cache);
        EvmChain::new(
            provider,
            EvmChainConfig {
                chunk_size: ChunkSize::new(100),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

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
        let chain = make_chain(cache).await;
        // The store's tip is seeded from the chain head at logs().
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
        let chain = make_chain(cache).await;
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
        let chain = make_chain(cache).await;
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
        let chain = make_chain(cache).await;
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
        let cache = Shared::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);
        // Anchor header (HEAD) so construction is fully cache-served.
        insert_header(&cache, BlockNumber::ZERO, BH_0, GENESIS);
        insert_header(&cache, HEAD, BH_200, ts(2400));
        // A log at block 150 WITH a timestamp and deliberately NO header for 150.
        let block_150 = BlockNumber::new(150);
        let mut log = make_log(block_150, LogIdx::ZERO, BlockHash::repeat_byte(0x9A));
        log.block_timestamp = Some(ts(1800).get());
        cache.insert_logs(&fh, std::slice::from_ref(&log)).unwrap();
        cache
            .record_scanned_range(
                &ScanPartition::Filter(fh),
                BlockRange::new(BlockNumber::ZERO, HEAD),
                &BH_200,
            )
            .unwrap();

        let chain = make_chain(cache).await;
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
        let chain = make_chain(cache).await;
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
        let chain = make_chain(cache).await;
        let store = log_store(&chain);
        // Anchor: HEAD @ ts(2400); block_time = 12s. 1200 s earlier
        // → 100 blocks before the anchor → the cut "just before block 100".
        let pos = store.time_pos(ts(1200)).await.unwrap();
        assert_eq!(pos, tip_pos(BlockNumber::new(99)));
    }

    #[tokio::test]
    async fn hist_store_next_uses_current_head() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache).await;
        let hist = log_store(&chain).next().await;
        assert_eq!(*hist.start(), EvmPosition::earliest());
        assert_eq!(*hist.end(), tip_pos(HEAD));
        // The chain-level head store agrees.
        assert_eq!(chain.head(Finality::Latest).get().number, HEAD);
    }

    /// `chain_id` stays on `EvmChain`, never on positions.
    #[tokio::test]
    async fn chain_id_is_on_chain_not_position() {
        let (cache, _) = warm_cache();
        let chain = make_chain(cache).await;
        assert_eq!(chain.chain_id(), TEST_CHAIN);
        // EvmPosition has only block + log_index — no chain field.
        let p = EvmPosition::new(100, 0);
        let s = format!("{p:?}");
        assert!(
            !s.contains("chain"),
            "EvmPosition Debug should not mention chain: {s}"
        );
    }

    /// Guardrail for the "minimize RPC" guarantee: a fully-cached walk must
    /// issue **zero** `eth_getLogs`. Counts the observability contract's RPC
    /// spans rather than timing, so a refactor that reintroduces a network
    /// round-trip on the warm path fails here, loudly.
    #[tokio::test]
    async fn warm_walk_issues_zero_get_logs() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use tracing::Subscriber;
        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Id};
        use tracing::subscriber::set_default;
        use tracing_subscriber::layer::{Context, Layer};
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::registry::LookupSpan;

        use crate::evm::observe::{RPC_TARGET, RpcMethod};

        #[derive(Clone, Default)]
        struct GetLogsCounter(StdArc<AtomicUsize>);

        impl<S> Layer<S> for GetLogsCounter
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
                if attrs.metadata().target() != RPC_TARGET {
                    return;
                }
                #[derive(Default)]
                struct MethodField(Option<String>);
                impl Visit for MethodField {
                    fn record_str(&mut self, field: &Field, value: &str) {
                        if field.name() == "method" {
                            self.0 = Some(value.to_owned());
                        }
                    }
                    fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
                }
                let mut found = MethodField::default();
                attrs.record(&mut found);
                if found.0.as_deref() == Some(RpcMethod::GetLogs.as_str()) {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let (cache, _) = warm_cache();
        let chain = make_chain(cache).await;
        let hist = log_store(&chain).next().await;

        // Install the counter only around the walk — the chain's setup RPC
        // (block_number) happens before this and isn't counted.
        let counter = GetLogsCounter::default();
        let guard = set_default(tracing_subscriber::registry().with(counter.clone()));
        let items = collect(hist).await;
        drop(guard);

        assert_eq!(items.len(), 3);
        assert_eq!(
            counter.0.load(Ordering::Relaxed),
            0,
            "a fully-cached walk must not issue eth_getLogs"
        );
    }

    // ============================================================
    // Blocks history
    // ============================================================

    use crate::evm::types::{EvmBlock, Gas, GasPrice, TxCount};

    fn test_block(n: u64) -> EvmBlock {
        EvmBlock {
            number: BlockNumber::new(n),
            // Unique per height (offset so genesis isn't ZERO).
            hash: BlockHash::new(B256::from(U256::from(n + 1))),
            parent_hash: BlockHash::new(B256::from(U256::from(n))),
            timestamp: ts(n * 12),
            tx_count: TxCount::new(n as u32),
            gas_used: Gas::new(15_000_000),
            gas_limit: Gas::new(30_000_000),
            base_fee: Some(GasPrice::new(1_000_000_000)),
            miner: Address::repeat_byte(0x99),
        }
    }

    /// Warm cache for the blocks source: anchor headers for construction +
    /// dense `[0, HEAD]` block coverage under the blocks partition.
    fn warm_blocks_cache() -> Shared<LogCache> {
        let cache = Shared::new(LogCache::open_in_memory().unwrap());
        insert_header(&cache, BlockNumber::ZERO, BH_0, GENESIS);
        insert_header(&cache, HEAD, BH_200, ts(2400));

        let blocks: Vec<EvmBlock> = (0..=HEAD.get()).map(test_block).collect();
        let end_hash = blocks.last().unwrap().hash;
        cache.insert_blocks(&blocks).unwrap();
        cache
            .record_scanned_range(
                &ScanPartition::Blocks,
                BlockRange::new(BlockNumber::ZERO, HEAD),
                &end_hash,
            )
            .unwrap();
        cache
    }

    #[tokio::test]
    async fn blocks_tip_seeded_from_head() {
        let chain = make_chain(warm_blocks_cache()).await;
        let hist = chain.blocks(Finality::Latest).next().await;
        assert_eq!(*hist.end(), HEAD);
    }

    #[tokio::test]
    async fn blocks_iter_walks_dense_window_including_genesis() {
        let chain = make_chain(warm_blocks_cache()).await;
        let hist = chain.blocks(Finality::Latest).next().await;

        let items: Vec<Positioned<EvmBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        assert_eq!(items.len(), HEAD.get() as usize + 1, "dense incl. genesis");
        assert_eq!(items[0].pos, BlockNumber::ZERO);
        assert_eq!(items.last().unwrap().pos, HEAD);
        // Items carry the rich fields, positioned at their own number.
        let b100 = &items[100];
        assert_eq!(b100.item.number, BlockNumber::new(100));
        assert_eq!(b100.item.tx_count, TxCount::new(100));
        assert_eq!(b100.item.timestamp, ts(1200));
    }

    #[tokio::test]
    async fn blocks_time_pos_cuts_just_before_estimated_block() {
        let chain = make_chain(warm_blocks_cache()).await;
        let store = chain.blocks(Finality::Latest);
        // 12 s cadence anchored at (HEAD, ts(2400)): ts(1200) → block 100,
        // cut just before it = 99 (iteration after the anchor starts at 100).
        assert_eq!(
            store.time_pos(ts(1200)).await.unwrap(),
            BlockNumber::new(99)
        );
        // Before-anchor underflow clamps to the ZERO sentinel.
        assert_eq!(
            store.time_pos(GENESIS).await.unwrap(),
            BlockNumber::ZERO,
            "estimate at/below genesis clamps to the lower-bound sentinel"
        );
    }

    /// Same guard as [`warm_walk_issues_zero_get_logs`], for the blocks
    /// source: a fully-cached walk must not issue `eth_getBlockByNumber`.
    #[tokio::test]
    async fn blocks_warm_walk_issues_zero_get_block_by_number() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use tracing::Subscriber;
        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Id};
        use tracing::subscriber::set_default;
        use tracing_subscriber::layer::{Context, Layer};
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::registry::LookupSpan;

        use crate::evm::observe::{RPC_TARGET, RpcMethod};

        #[derive(Clone, Default)]
        struct GetBlockCounter(StdArc<AtomicUsize>);

        impl<S> Layer<S> for GetBlockCounter
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
                if attrs.metadata().target() != RPC_TARGET {
                    return;
                }
                #[derive(Default)]
                struct MethodField(Option<String>);
                impl Visit for MethodField {
                    fn record_str(&mut self, field: &Field, value: &str) {
                        if field.name() == "method" {
                            self.0 = Some(value.to_owned());
                        }
                    }
                    fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
                }
                let mut found = MethodField::default();
                attrs.record(&mut found);
                if found.0.as_deref() == Some(RpcMethod::GetBlockByNumber.as_str()) {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let chain = make_chain(warm_blocks_cache()).await;
        let hist = chain.blocks(Finality::Latest).next().await;

        let counter = GetBlockCounter::default();
        let guard = set_default(tracing_subscriber::registry().with(counter.clone()));
        let items: Vec<Positioned<EvmBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        drop(guard);

        assert_eq!(items.len(), HEAD.get() as usize + 1);
        assert_eq!(
            counter.0.load(Ordering::Relaxed),
            0,
            "a fully-cached walk must not issue eth_getBlockByNumber"
        );
    }
}
