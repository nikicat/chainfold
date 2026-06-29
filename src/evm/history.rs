//! `EvmLogHistory` — [`HistStoreBackend`] over an alloy [`Provider`],
//! [`LogCache`], and [`LogIndex`].
//!
//! Each instance is scoped to a single log filter (address + topics) on a
//! single chain. The chain identity stays on the history (`chain_id()`) —
//! [`EvmPosition`] itself carries only `(block, log_index)` so that
//! generic combinators (`Hist::split`, future `MergedHistStore`, etc.) can
//! reason about cuts without knowing anything EVM-specific.
//!
//! ## Tip tracking and live events
//!
//! Construction does not start any background work. Call
//! [`subscribe`](EvmLogHistory::subscribe) to spawn a background task that
//! delivers fresh log pages and tip updates:
//!
//! * Each delivered page is fed to [`LogIndex::insert_page`] (in-memory
//!   tree update synchronous) and drained asynchronously to [`LogCache`].
//! * Tip advances bump `latest_store`, which drives
//!   [`HistStore::snapshots`](crate::arch::HistStore::snapshots).
//! * On reconnect after a transport error the subscriber fills the gap via
//!   [`LogIndex::ensure_cached`]; duplicates are absorbed by the late-item
//!   rules.
//!
//! Without an active subscription, [`HistStore::snapshots`] still yields the
//! current snapshot but will not advance until a tip update arrives —
//! callers can also drive the tip manually via [`Self::update_tip`].

use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use futures_util::StreamExt;
use futures_util::stream::{empty, once, poll_fn};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::arch::{
    AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError,
    ItemStream, Positioned, Store, Timestamp,
};
use crate::evm::cache::{BlockHeader, LogCache, compute_filter_hash};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::log_index::{BlockPos, Fetcher, LogIndex, LogPage};
use crate::evm::observe::{CacheLayer, CacheOutcome, RpcMethod, cache_event, rpc_span};
use crate::evm::subscriber::{EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks};
use crate::evm::types::{BlockNumber, ChainId, EvmEvent, EvmPosition, FilterHash, LogIdx, Topic};

fn fmt_topic(t: Option<&Topic>) -> String {
    match t {
        Some(b) => format!("{b}"),
        None => "*".into(),
    }
}

/// Default chunk size for `eth_getLogs` calls (in blocks).
const DEFAULT_CHUNK_SIZE: u64 = 1000;

/// Assumed block time in seconds for [`HistStore::time_pos`] estimation.
///
/// Public so callers that go through [`EvmLogHistory::with_options`] (to set a
/// custom chunk size) can pass the same default instead of a bare literal.
pub const DEFAULT_BLOCK_TIME: u64 = 12;

// ============================================================
// EvmLogHistory
// ============================================================

pub struct EvmLogHistory<P> {
    provider: ChainProvider<P>,
    store: Arc<LogCache>,
    index: Arc<LogIndex>,
    filter_hash: FilterHash,
    address: Address,
    topics: [Option<Topic>; 4],
    chunk_size: u64,

    // Reactive tip stores. `best`/`safe` are stubs whose watch channels
    // are closed at construction — they emit the initial `earliest`
    // position once and stay frozen there until finality wiring lands.
    latest_store: Store<EvmPosition>,
    best_store: Store<EvmPosition>,
    safe_store: Store<EvmPosition>,
    latest_tx: watch::Sender<EvmPosition>,

    // Wall-clock → block-number estimation. Anchored at a recent block
    // rather than genesis because some chains use timestamp 0 at genesis.
    anchor_block: u64,
    anchor_timestamp: u64,
    block_time: u64,

    // Self-reference so we can hand out Arc<dyn HistBackend>.
    self_ref: OnceLock<Weak<Self>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmLogHistory<P> {
    /// Convenience constructor with default chunk size / block time. The
    /// chain id rides in the [`ChainProvider`]; build one with
    /// [`ChainProvider::new`] (known) or [`ChainProvider::discover`] (fetch).
    pub async fn new(
        provider: ChainProvider<P>,
        store: Arc<LogCache>,
        address: Address,
        topics: [Option<Topic>; 4],
    ) -> Result<Arc<Self>, HistoryError> {
        Self::with_options(
            provider,
            store,
            address,
            topics,
            DEFAULT_CHUNK_SIZE,
            DEFAULT_BLOCK_TIME,
        )
        .await
    }

    pub async fn with_options(
        provider: ChainProvider<P>,
        store: Arc<LogCache>,
        address: Address,
        topics: [Option<Topic>; 4],
        chunk_size: u64,
        block_time: u64,
    ) -> Result<Arc<Self>, HistoryError> {
        let filter_hash = compute_filter_hash(&address, &topics);
        let chain_id = provider.chain_id();

        let anchor_block = provider
            .get_block_number()
            .instrument(rpc_span(RpcMethod::BlockNumber))
            .await
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))?;
        let anchor_timestamp = ensure_block_header_via(&provider, &store, anchor_block)
            .await?
            .timestamp;

        let earliest = EvmPosition::earliest();
        let (latest_tx, latest_rx) = watch::channel(earliest);
        // Stubs: the sender drops at end of this inner scope, closing the
        // channel. Receivers still answer `get()` with `earliest`; their
        // subscribe() yields the initial value once and then ends.
        let best_rx = {
            let (_, rx) = watch::channel(earliest);
            rx
        };
        let safe_rx = {
            let (_, rx) = watch::channel(earliest);
            rx
        };

        let fetcher = build_fetcher(provider.clone(), store.clone(), address, topics);
        let index = Arc::new(LogIndex::with_chunk_size(
            store.clone(),
            filter_hash,
            fetcher,
            chunk_size,
        ));

        let history = Arc::new(Self {
            provider,
            store,
            index,
            filter_hash,
            address,
            topics,
            chunk_size,
            latest_store: Store::new(latest_rx),
            best_store: Store::new(best_rx),
            safe_store: Store::new(safe_rx),
            latest_tx,
            anchor_block,
            anchor_timestamp,
            block_time,
            self_ref: OnceLock::new(),
        });
        let _ = history.self_ref.set(Arc::downgrade(&history));

        tracing::debug!(
            chain_id = %chain_id,
            address = %format!("{address:#x}"),
            topic0 = %fmt_topic(topics[0].as_ref()),
            topic1 = %fmt_topic(topics[1].as_ref()),
            topic2 = %fmt_topic(topics[2].as_ref()),
            topic3 = %fmt_topic(topics[3].as_ref()),
            chunk_size,
            filter_hash = %format!("0x{}", hex::encode(&filter_hash.as_slice()[..8])),
            "EvmLogHistory created"
        );

        Ok(history)
    }

    /// Wrap this history as a public [`HistStore`].
    pub fn hist_store(self: &Arc<Self>) -> HistStore<EvmEvent, EvmPosition> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<_, _>>)
    }

    pub fn chain_id(&self) -> ChainId {
        self.provider.chain_id()
    }

    pub fn filter_hash(&self) -> FilterHash {
        self.filter_hash
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn latest_store(&self) -> &Store<EvmPosition> {
        &self.latest_store
    }

    /// Underlying [`LogIndex`]. Tests + manual cache-priming callers.
    pub fn index(&self) -> &Arc<LogIndex> {
        &self.index
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref
            .get()
            .expect("self_ref not initialized")
            .upgrade()
            .expect("EvmLogHistory dropped while still in use")
    }

    // --------------------------------------------------------
    // Tip tracking
    // --------------------------------------------------------

    /// Fetch the latest block from RPC and bump `latest_store`. Returns
    /// the new tip block number.
    pub async fn update_tip(&self) -> Result<u64, HistoryError> {
        let block_number = self.latest_block_number().await?;
        self.update_tip_to(block_number).await?;
        Ok(block_number)
    }

    /// Internal: bump the latest tip store to `block_number`. The position
    /// records the *end-of-block* cut (`log_index = u32::MAX`) so a `Hist`
    /// with this end iterates every log up to and including the last one
    /// in that block.
    async fn update_tip_to(&self, block_number: u64) -> Result<(), HistoryError> {
        // We don't need the header here — chain identity and timestamps
        // live elsewhere — but we touch the cache so later iter() calls
        // for the tip block don't have to round-trip.
        let _ = ensure_block_header_via(&self.provider, &self.store, block_number).await?;
        let pos = EvmPosition::new(block_number, u32::MAX);
        let prev = self.latest_store.get().block;
        let _ = self.latest_tx.send(pos);
        if block_number != prev.get() {
            tracing::debug!(block_number, prev = prev.get(), "tip updated");
        }
        Ok(())
    }

    /// Subscribe to push-based tip + log delivery. See module-level docs.
    pub async fn subscribe(
        self: &Arc<Self>,
        transport: SubscribeTransport,
    ) -> Result<EvmLogSubscriber, HistoryError> {
        self.index.attach_drain();

        let start_block = self.update_tip().await?;

        let filter = self.build_filter(None, None);

        let index = self.index.clone();
        let on_page: Arc<dyn Fn(LogPage) + Send + Sync> =
            Arc::new(move |page: LogPage| index.insert_page(&page));

        let history = Arc::clone(self);
        let on_tip = Arc::new(move |bn: u64| {
            let history = history.clone();
            Box::pin(async move {
                if let Err(e) = history.update_tip_to(bn).await {
                    tracing::warn!(block_number = bn, error = %e, "update_tip");
                }
            }) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        });

        let history = Arc::clone(self);
        let on_reconnect = Arc::new(move |last_tip: u64| {
            let history = history.clone();
            Box::pin(async move {
                if let Err(e) = history.gap_fill_after_reconnect(last_tip).await {
                    tracing::warn!(last_tip, error = %e, "reconnect gap-fill");
                }
            }) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        });

        let callbacks = SubscriberCallbacks {
            on_page,
            on_tip,
            on_reconnect,
        };

        Ok(EvmLogSubscriber::start(
            self.provider.clone(),
            filter,
            transport,
            start_block,
            callbacks,
        ))
    }

    async fn gap_fill_after_reconnect(&self, last_tip: u64) -> Result<(), HistoryError> {
        let current = self.latest_block_number().await?;
        if current <= last_tip {
            return Ok(());
        }
        tracing::info!(from = last_tip + 1, to = current, "reconnect gap-fill");
        self.index.ensure_cached(last_tip + 1, current).await
    }

    fn build_filter(&self, from: Option<u64>, to: Option<u64>) -> Filter {
        let mut filter = Filter::new().address(self.address);
        if let Some(f) = from {
            filter = filter.from_block(f);
        }
        if let Some(t) = to {
            filter = filter.to_block(t);
        }
        apply_topics(filter, &self.topics)
    }

    async fn latest_block_number(&self) -> Result<u64, HistoryError> {
        let n = self
            .provider
            .get_block_number()
            .instrument(rpc_span(RpcMethod::BlockNumber))
            .await
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))?;
        Ok(n)
    }

    fn estimate_block_from_time(&self, time: Timestamp) -> u64 {
        // Drop to the raw seconds for the block-time arithmetic; the anchor
        // timestamp and block_time are u64 internals on the same scale.
        let time = time.get();
        if time >= self.anchor_timestamp {
            let delta_secs = time - self.anchor_timestamp;
            self.anchor_block + delta_secs / self.block_time
        } else {
            let delta_secs = self.anchor_timestamp - time;
            let delta_blocks = delta_secs / self.block_time;
            self.anchor_block.saturating_sub(delta_blocks)
        }
    }
}

// ============================================================
// Position <-> BlockPos bridging
// ============================================================

fn evm_to_block_pos(p: EvmPosition) -> BlockPos {
    if p == EvmPosition::earliest() {
        BlockPos::Bottom
    } else {
        BlockPos::At {
            block: p.block,
            log_index: p.log_index,
        }
    }
}

// ============================================================
// HistBackend impl
// ============================================================

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> HistBackend<EvmEvent, EvmPosition>
    for EvmLogHistory<P>
{
    fn iter(&self, start: EvmPosition, end: EvmPosition) -> ItemStream<EvmEvent, EvmPosition> {
        if end <= start {
            return Box::pin(empty());
        }

        // Materialize any persisted scanned ranges covering this window
        // into the in-memory tree, so items() can answer from cache
        // instead of falling through to the fetcher.
        let from_block = start.block;
        let to_block = end.block;
        if let Err(e) = self
            .index
            .ensure_window_loaded(from_block.get(), to_block.get())
        {
            return Box::pin(once(async move { Err(e) }));
        }

        let start_pos = evm_to_block_pos(start);
        let end_pos = evm_to_block_pos(end);
        let log_stream = self.index.clone().items(start_pos, end_pos);
        let provider = self.provider.clone();
        let store = self.store.clone();

        Box::pin(log_stream.then(move |result| {
            let provider = provider.clone();
            let store = store.clone();
            async move {
                match result {
                    Ok(log) => {
                        let mut event = log_to_evm_event(&log);
                        // Only round-trip to a block header when the log didn't
                        // already carry its timestamp.
                        if log.block_timestamp.is_none() {
                            let bn = log.block_number.unwrap_or(0);
                            event.timestamp = Timestamp::new(
                                ensure_block_header_via(&provider, &store, bn)
                                    .await?
                                    .timestamp,
                            );
                        }
                        let pos = event.position();
                        Ok(Positioned::new(event, pos))
                    }
                    Err(e) => Err(e),
                }
            }
        }))
    }
}

// ============================================================
// HistStoreBackend impl
// ============================================================

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> HistStoreBackend<EvmEvent, EvmPosition>
    for EvmLogHistory<P>
{
    fn snapshots(&self) -> HistStream<EvmEvent, EvmPosition> {
        let backend: Arc<dyn HistBackend<_, _>> = self.arc_self();
        let mut latest_sub = self.latest_store.subscribe();
        Box::pin(poll_fn(move |cx| {
            latest_sub.as_mut().poll_next(cx).map(|tip_opt| {
                tip_opt.map(|tip| Hist::new(backend.clone(), EvmPosition::earliest(), tip))
            })
        }))
    }

    async fn next(&self) -> Hist<EvmEvent, EvmPosition> {
        let backend: Arc<dyn HistBackend<_, _>> = self.arc_self();
        let tip = self.latest_store.get();
        Hist::new(backend, EvmPosition::earliest(), tip)
    }

    fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<EvmEvent, EvmPosition>) + Send + Sync>,
    ) -> AsyncDisposable {
        let backend: Arc<dyn HistBackend<_, _>> = self.arc_self();
        let mut stream = self.latest_store.subscribe();
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel2.cancelled() => break,
                    tip = stream.next() => {
                        let Some(tip) = tip else { break };
                        let hist = Hist::new(backend.clone(), EvmPosition::earliest(), tip);
                        cb(hist);
                    }
                }
            }
        });
        AsyncDisposable::new(cancel, handle)
    }

    async fn time_pos(&self, time: Timestamp) -> Result<EvmPosition, HistoryError> {
        let block = self.estimate_block_from_time(time);
        // Return the cut "just before block B" — items iterated after this
        // anchor start with the first log in block B.
        if block == 0 {
            Ok(EvmPosition::earliest())
        } else {
            Ok(EvmPosition::new(block - 1, u32::MAX))
        }
    }

    fn latest(&self) -> &Store<EvmPosition> {
        &self.latest_store
    }
    fn best(&self) -> &Store<EvmPosition> {
        &self.best_store
    }
    fn safe(&self) -> &Store<EvmPosition> {
        &self.safe_store
    }
}

// ============================================================
// Free helpers
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

fn log_to_evm_event(log: &Log) -> EvmEvent {
    EvmEvent {
        contract_address: log.address(),
        block_number: BlockNumber::new(log.block_number.unwrap_or(0)),
        // Provider-supplied per-log timestamp when present; otherwise 0, and
        // the caller fills it from the (cached) block header.
        timestamp: Timestamp::new(log.block_timestamp.unwrap_or(0)),
        block_hash: log.block_hash.unwrap_or(B256::ZERO),
        log_index: LogIdx::new(log.log_index.unwrap_or(0) as u32),
        tx_hash: log.transaction_hash.unwrap_or(B256::ZERO),
        topics: log.topics().iter().copied().map(Topic::new).collect(),
        data: log.data().data.to_vec(),
    }
}

/// Cache-first block-header lookup. Falls back to RPC and persists.
pub(crate) async fn ensure_block_header_via<P>(
    provider: &P,
    store: &LogCache,
    block_number: u64,
) -> Result<BlockHeader, HistoryError>
where
    P: Provider<Ethereum>,
{
    if let Some(header) = store
        .get_block_header(block_number)
        .map_err(|e| HistoryError::internal(e.to_string()))?
    {
        cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
        return Ok(header);
    }
    cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);

    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .into_future()
        .instrument(rpc_span(RpcMethod::GetBlockByNumber))
        .await
        .map_err(|e| HistoryError::internal(format!("get_block failed: {e}")))?
        .ok_or_else(|| HistoryError::internal(format!("block {block_number} not found")))?;

    let hash = block.header.hash;
    let parent_hash = block.header.parent_hash;
    let timestamp = block.header.timestamp;

    store
        .insert_block_header(block_number, &hash, &parent_hash, timestamp)
        .map_err(|e| HistoryError::internal(e.to_string()))?;

    Ok(BlockHeader {
        height: block_number,
        hash,
        parent_hash,
        timestamp,
    })
}

fn build_fetcher<P>(
    provider: P,
    store: Arc<LogCache>,
    address: Address,
    topics: [Option<Topic>; 4],
) -> Fetcher
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    Arc::new(move |from: BlockNumber, to: BlockNumber| {
        let provider = provider.clone();
        let store = store.clone();
        Box::pin(async move {
            let (from_u, to_u) = (from.get(), to.get());
            let filter = Filter::new()
                .address(address)
                .from_block(from_u)
                .to_block(to_u);
            let filter = apply_topics(filter, &topics);
            tracing::debug!(
                from = from_u,
                to = to_u,
                blocks = to_u - from_u + 1,
                "eth_getLogs"
            );
            let logs = provider
                .get_logs(&filter)
                .instrument(rpc_span(RpcMethod::GetLogs))
                .await
                .map_err(|e| HistoryError::internal(format!("eth_getLogs failed: {e}")))?;
            tracing::debug!(
                from = from_u,
                to = to_u,
                num_logs = logs.len(),
                "eth_getLogs returned"
            );

            let end_header = ensure_block_header_via(&provider, &store, to_u).await?;
            for log in &logs {
                // Logs that carry their own timestamp need no header — the
                // query path reads it straight off the persisted log.
                if log.block_timestamp.is_some() {
                    continue;
                }
                if let Some(bn) = log.block_number {
                    let _ = ensure_block_header_via(&provider, &store, bn).await;
                }
            }

            Ok(LogPage {
                from_block: from,
                to_block: to,
                items: logs,
                end_block_hash: end_header.hash,
            })
        })
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Position;
    use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData, U64};
    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;
    use futures_util::TryStreamExt;

    const ADDR: Address = Address::repeat_byte(0x42);
    const TOPICS: [Option<Topic>; 4] = [None; 4];
    const GENESIS_TS: u64 = 1_600_000_000;
    const TEST_CHAIN: u64 = 1;

    const BH_0: B256 = B256::ZERO;
    const BH_100: B256 = B256::repeat_byte(0xA0);
    const BH_101: B256 = B256::repeat_byte(0xA1);
    const BH_200: B256 = B256::repeat_byte(0xA2);

    fn make_log(block_num: u64, log_idx: u64, block_hash: B256) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);

        Log {
            inner: LogInner {
                address: ADDR,
                data: log_data,
            },
            block_hash: Some(block_hash),
            block_number: Some(block_num),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xCC; 32])),
            transaction_index: Some(0),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    fn warm_cache() -> (Arc<LogCache>, FilterHash) {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        cache
            .insert_block_header(0, &BH_0, &B256::ZERO, GENESIS_TS)
            .unwrap();
        cache
            .insert_block_header(100, &BH_100, &BH_0, GENESIS_TS + 1200)
            .unwrap();
        cache
            .insert_block_header(101, &BH_101, &BH_100, GENESIS_TS + 1212)
            .unwrap();
        cache
            .insert_block_header(200, &BH_200, &BH_101, GENESIS_TS + 2400)
            .unwrap();

        let logs = vec![
            make_log(100, 0, BH_100),
            make_log(100, 1, BH_100),
            make_log(101, 0, BH_101),
        ];
        cache.insert_logs(&fh, &logs).unwrap();
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();
        (cache, fh)
    }

    async fn make_history(
        store: Arc<LogCache>,
    ) -> Arc<EvmLogHistory<impl Provider<Ethereum> + Clone + 'static>> {
        let asserter = Asserter::new();
        // Only get_block_number for the anchor now — chain id rides in the
        // ChainProvider, so no get_chain_id round-trip. The anchor block-header
        // lookup hits the cache (warm_cache has block 200).
        asserter.push_success(&U64::from(200u64));
        let provider = ProviderBuilder::default().connect_mocked_client(asserter);
        let provider = ChainProvider::new(provider, ChainId::new(TEST_CHAIN));
        EvmLogHistory::with_options(provider, store, ADDR, TOPICS, 100, DEFAULT_BLOCK_TIME)
            .await
            .unwrap()
    }

    /// Iterate a Hist into a Vec<Positioned> with no in-flight fetcher.
    async fn collect(hist: Hist<EvmEvent, EvmPosition>) -> Vec<Positioned<EvmEvent, EvmPosition>> {
        hist.iter().try_collect().await.unwrap()
    }

    #[tokio::test]
    async fn iter_walks_all_logs_in_window() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let end = EvmPosition::new(200, u32::MAX);
        let hist = Hist::new(backend, EvmPosition::earliest(), end);

        let items = collect(hist).await;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].item.position(), EvmPosition::new(100, 0));
        assert_eq!(items[1].item.position(), EvmPosition::new(100, 1));
        assert_eq!(items[2].item.position(), EvmPosition::new(101, 0));
    }

    #[tokio::test]
    async fn split_clamps_anchor_to_window() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let end = EvmPosition::new(200, u32::MAX);
        let hist = Hist::new(backend, EvmPosition::earliest(), end);

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
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let pos = EvmPosition::new(150, 0);
        let hist = Hist::new(backend, pos, pos);
        assert!(hist.is_empty());
        assert!(collect(hist).await.is_empty());
    }

    #[tokio::test]
    async fn iter_picks_up_timestamp_from_store() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let end = EvmPosition::new(200, u32::MAX);
        let hist = Hist::new(backend, EvmPosition::earliest(), end);
        let items = collect(hist).await;
        // block 100 header inserted with GENESIS_TS + 1200
        assert_eq!(items[0].item.timestamp, Timestamp::new(GENESIS_TS + 1200));
        assert_eq!(items[2].item.timestamp, Timestamp::new(GENESIS_TS + 1212));
    }

    /// When a log carries its own `block_timestamp`, the walk stamps the event
    /// from it and never fetches the block header — proven by leaving no
    /// header cached for that block and no `get_block_by_number` queued on the
    /// mock provider, so a header fetch would error the walk.
    #[tokio::test]
    async fn iter_uses_log_block_timestamp_without_header_fetch() {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);
        // Anchor header (block 200) so construction is fully cache-served.
        cache
            .insert_block_header(0, &BH_0, &B256::ZERO, GENESIS_TS)
            .unwrap();
        cache
            .insert_block_header(200, &BH_200, &BH_101, GENESIS_TS + 2400)
            .unwrap();
        // A log at block 150 WITH a timestamp and deliberately NO header for 150.
        let mut log = make_log(150, 0, B256::repeat_byte(0x9A));
        log.block_timestamp = Some(GENESIS_TS + 1800);
        cache.insert_logs(&fh, std::slice::from_ref(&log)).unwrap();
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();

        let history = make_history(cache).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let hist = Hist::new(
            backend,
            EvmPosition::new(149, u32::MAX),
            EvmPosition::new(150, u32::MAX),
        );
        let items = collect(hist).await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].item.timestamp, Timestamp::new(GENESIS_TS + 1800));
    }

    #[tokio::test]
    async fn position_marshal_round_trip() {
        let p = EvmPosition::new(123_456_789, 42);
        let bytes = p.marshal();
        let restored = EvmPosition::unmarshal(&bytes).unwrap();
        assert_eq!(restored, p);
    }

    #[tokio::test]
    async fn time_pos_estimates_block() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let hs = history.hist_store();
        // anchor block 200 @ GENESIS_TS + 2400; block_time = 12
        // 100 blocks before anchor → block 100 → time_pos returns cut "just before 100"
        let pos = hs
            .time_pos(Timestamp::new(GENESIS_TS + 1200))
            .await
            .unwrap();
        assert_eq!(pos.block, BlockNumber::new(99));
        assert_eq!(pos.log_index, LogIdx::MAX);
    }

    #[tokio::test]
    async fn hist_store_next_uses_current_tip() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        // Bootstrap latest_store by simulating a tip update.
        let _ = history.latest_tx.send(EvmPosition::new(200, u32::MAX));
        let hs = history.hist_store();
        let hist = hs.next().await;
        assert_eq!(*hist.start(), EvmPosition::earliest());
        assert_eq!(*hist.end(), EvmPosition::new(200, u32::MAX));
    }

    /// `chain_id` stays on `EvmLogHistory`, never on positions.
    #[tokio::test]
    async fn chain_id_is_on_history_not_position() {
        let (store, _) = warm_cache();
        let history = make_history(store).await;
        assert_eq!(history.chain_id(), ChainId::new(TEST_CHAIN));
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

        let (store, _) = warm_cache();
        let history = make_history(store).await;
        let backend: Arc<dyn HistBackend<_, _>> = history.clone();
        let end = EvmPosition::new(200, u32::MAX);
        let hist = Hist::new(backend, EvmPosition::earliest(), end);

        // Install the counter only around the walk — make_history's setup RPCs
        // (chain_id, block_number) happen before this and aren't counted.
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

    // Suppress dead-code warnings on unused fixtures during phase work.
    #[allow(dead_code)]
    fn _unused_warmup() {
        let _ = (BH_0, BH_100, BH_101, BH_200);
    }
}
