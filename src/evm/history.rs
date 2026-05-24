//! `EvmLogHistory` — [`HistStoreBackend`] over an alloy [`Provider`],
//! [`LogStore`], and [`LogIndex`].
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
//!   tree update synchronous) and drained asynchronously to [`LogStore`].
//! * Tip advances bump `latest_store`, which drives
//!   [`HistStore::snapshots`](crate::arch::HistStore::snapshots).
//! * On reconnect after a transport error the subscriber fills the gap via
//!   [`LogIndex::ensure_cached`]; duplicates are absorbed by the late-item
//!   rules.
//!
//! Without an active subscription, [`HistStore::snapshots`] still yields the
//! current snapshot but will not advance until a tip update arrives —
//! callers can also drive the tip manually via [`Self::poll_tip`].

use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use futures_util::stream::{empty, once, poll_fn};
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::arch::evm::{EvmEvent, EvmPosition};
use crate::arch::{
    AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError,
    ItemStream, Positioned, Store, Timestamp,
};
use crate::evm::log_index::{BlockPos, Fetcher, LogIndex, LogPage};
use crate::evm::log_store::{compute_filter_hash, BlockHeader, LogStore};
use crate::evm::subscriber::{EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks};

fn fmt_topic(t: Option<&[u8; 32]>) -> String {
    match t {
        Some(b) => format!("0x{}", hex::encode(b)),
        None => "*".into(),
    }
}

/// Default chunk size for `eth_getLogs` calls (in blocks).
const DEFAULT_CHUNK_SIZE: u64 = 1000;

/// Assumed block time in seconds for [`HistStore::time_pos`] estimation.
const DEFAULT_BLOCK_TIME: u64 = 12;

// ============================================================
// EvmLogHistory
// ============================================================

pub struct EvmLogHistory<P> {
    provider: P,
    store: Arc<LogStore>,
    index: Arc<LogIndex>,
    filter_hash: [u8; 32],
    address: [u8; 20],
    topics: [Option<[u8; 32]>; 4],
    chunk_size: u64,
    chain_id: u64,

    // Reactive tip stores. `best`/`safe` are stubs that mirror `latest`
    // until Phase 7 wires confirmation depth + finality polling.
    latest_store: Store<EvmPosition>,
    best_store: Store<EvmPosition>,
    safe_store: Store<EvmPosition>,
    latest_tx: watch::Sender<EvmPosition>,
    #[allow(dead_code)]
    best_tx: watch::Sender<EvmPosition>,
    #[allow(dead_code)]
    safe_tx: watch::Sender<EvmPosition>,

    // Wall-clock → block-number estimation. Anchored at a recent block
    // rather than genesis because some chains use timestamp 0 at genesis.
    anchor_block: u64,
    anchor_timestamp: u64,
    block_time: u64,

    // Self-reference so we can hand out Arc<dyn HistBackend>.
    self_ref: OnceLock<Weak<Self>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmLogHistory<P> {
    pub async fn new(
        provider: P,
        store: Arc<LogStore>,
        address: [u8; 20],
        topics: [Option<[u8; 32]>; 4],
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
        provider: P,
        store: Arc<LogStore>,
        address: [u8; 20],
        topics: [Option<[u8; 32]>; 4],
        chunk_size: u64,
        block_time: u64,
    ) -> Result<Arc<Self>, HistoryError> {
        let filter_hash = compute_filter_hash(&address, &topics);

        let chain_id = provider
            .get_chain_id()
            .await
            .map_err(|e| HistoryError::internal(format!("get_chain_id failed: {e}")))?;
        let anchor_block = provider
            .get_block_number()
            .await
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))?;
        let anchor_timestamp = ensure_block_header_via(&provider, &store, anchor_block)
            .await?
            .timestamp;

        let earliest = EvmPosition::earliest();
        let (latest_tx, latest_rx) = watch::channel(earliest);
        let (best_tx, best_rx) = watch::channel(earliest);
        let (safe_tx, safe_rx) = watch::channel(earliest);

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
            chain_id,
            latest_store: Store::new(latest_rx),
            best_store: Store::new(best_rx),
            safe_store: Store::new(safe_rx),
            latest_tx,
            best_tx,
            safe_tx,
            anchor_block,
            anchor_timestamp,
            block_time,
            self_ref: OnceLock::new(),
        });
        let _ = history.self_ref.set(Arc::downgrade(&history));

        tracing::debug!(
            chain_id,
            address = %format!("0x{}", hex::encode(address)),
            topic0 = %fmt_topic(topics[0].as_ref()),
            topic1 = %fmt_topic(topics[1].as_ref()),
            topic2 = %fmt_topic(topics[2].as_ref()),
            topic3 = %fmt_topic(topics[3].as_ref()),
            chunk_size,
            filter_hash = %format!("0x{}", hex::encode(&filter_hash[..8])),
            "EvmLogHistory created"
        );

        Ok(history)
    }

    /// Wrap this history as a public [`HistStore`].
    pub fn hist_store(self: &Arc<Self>) -> HistStore<EvmEvent, EvmPosition> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<_, _>>)
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn filter_hash(&self) -> &[u8; 32] {
        &self.filter_hash
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn latest_store(&self) -> &Store<EvmPosition> {
        &self.latest_store
    }

    /// Underlying [`LogStore`]. Used by erc20 metadata/balance helpers.
    pub fn store(&self) -> &Arc<LogStore> {
        &self.store
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
    pub async fn poll_tip(&self) -> Result<u64, HistoryError> {
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
        if block_number != prev {
            tracing::debug!(block_number, prev, "tip updated");
        }
        Ok(())
    }

    /// Subscribe to push-based tip + log delivery. See module-level docs.
    pub async fn subscribe(
        self: &Arc<Self>,
        transport: SubscribeTransport,
    ) -> Result<EvmLogSubscriber, HistoryError> {
        self.index.attach_drain();

        let start_block = self.poll_tip().await?;

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
        let mut filter = Filter::new().address(Address::from(self.address));
        if let Some(f) = from {
            filter = filter.from_block(f);
        }
        if let Some(t) = to {
            filter = filter.to_block(t);
        }
        apply_topics(filter, &self.topics)
    }

    async fn latest_block_number(&self) -> Result<u64, HistoryError> {
        tracing::debug!("eth_blockNumber");
        let n = self
            .provider
            .get_block_number()
            .await
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))?;
        tracing::debug!(block_number = n, "eth_blockNumber returned");
        Ok(n)
    }

    fn estimate_block_from_time(&self, time: Timestamp) -> u64 {
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
        if let Err(e) = self.index.ensure_window_loaded(from_block, to_block) {
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
                        let bn = log.block_number.unwrap_or(0);
                        let header = ensure_block_header_via(&provider, &store, bn).await?;
                        let mut event = log_to_evm_event(&log);
                        event.timestamp = header.timestamp;
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
fn apply_topics(mut filter: Filter, topics: &[Option<[u8; 32]>; 4]) -> Filter {
    if let Some(t) = topics[0] {
        filter = filter.event_signature(B256::from(t));
    }
    if let Some(t) = topics[1] {
        filter = filter.topic1(B256::from(t));
    }
    if let Some(t) = topics[2] {
        filter = filter.topic2(B256::from(t));
    }
    if let Some(t) = topics[3] {
        filter = filter.topic3(B256::from(t));
    }
    filter
}

fn log_to_evm_event(log: &Log) -> EvmEvent {
    let addr: [u8; 20] = log.address().0 .0;
    let block_hash: [u8; 32] = log.block_hash.map_or([0; 32], |h| h.0);
    let tx_hash: [u8; 32] = log.transaction_hash.map_or([0; 32], |h| h.0);

    EvmEvent {
        contract_address: addr,
        block_number: log.block_number.unwrap_or(0),
        timestamp: 0,
        block_hash,
        log_index: log.log_index.unwrap_or(0) as u32,
        tx_hash,
        topics: log.topics().iter().map(|t| t.0).collect(),
        data: log.data().data.to_vec(),
    }
}

/// Cache-first block-header lookup. Falls back to RPC and persists.
pub(crate) async fn ensure_block_header_via<P>(
    provider: &P,
    store: &LogStore,
    block_number: u64,
) -> Result<BlockHeader, HistoryError>
where
    P: Provider<Ethereum>,
{
    if let Some(header) = store
        .get_block_header(block_number)
        .map_err(|e| HistoryError::internal(e.to_string()))?
    {
        tracing::trace!(block_number, "block header cache hit");
        return Ok(header);
    }

    tracing::debug!(block_number, "eth_getBlockByNumber");

    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .map_err(|e| HistoryError::internal(format!("get_block failed: {e}")))?
        .ok_or_else(|| HistoryError::internal(format!("block {block_number} not found")))?;

    let hash: [u8; 32] = block.header.hash.0;
    let parent_hash: [u8; 32] = block.header.parent_hash.0;
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
    store: Arc<LogStore>,
    address: [u8; 20],
    topics: [Option<[u8; 32]>; 4],
) -> Fetcher
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    Arc::new(move |from, to| {
        let provider = provider.clone();
        let store = store.clone();
        Box::pin(async move {
            let filter = Filter::new()
                .address(Address::from(address))
                .from_block(from)
                .to_block(to);
            let filter = apply_topics(filter, &topics);
            tracing::debug!(from, to, blocks = to - from + 1, "eth_getLogs");
            let logs = provider
                .get_logs(&filter)
                .await
                .map_err(|e| HistoryError::internal(format!("eth_getLogs failed: {e}")))?;
            tracing::debug!(from, to, num_logs = logs.len(), "eth_getLogs returned");

            let end_header = ensure_block_header_via(&provider, &store, to).await?;
            for log in &logs {
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
    use alloy::primitives::{Address, Bytes, Log as LogInner, LogData, B256, U64};
    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;
    use futures_util::TryStreamExt;

    const ADDR: [u8; 20] = [0x42; 20];
    const TOPICS: [Option<[u8; 32]>; 4] = [None; 4];
    const GENESIS_TS: u64 = 1_600_000_000;
    const TEST_CHAIN: u64 = 1;

    const BH_0: [u8; 32] = [0x00; 32];
    const BH_100: [u8; 32] = [0xA0; 32];
    const BH_101: [u8; 32] = [0xA1; 32];
    const BH_200: [u8; 32] = [0xA2; 32];

    fn make_log(block_num: u64, log_idx: u64, block_hash: [u8; 32]) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);

        Log {
            inner: LogInner {
                address: Address::from(ADDR),
                data: log_data,
            },
            block_hash: Some(B256::from(block_hash)),
            block_number: Some(block_num),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xCC; 32])),
            transaction_index: Some(0),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    fn warm_cache() -> (Arc<LogStore>, [u8; 32]) {
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        cache
            .insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS)
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
        store: Arc<LogStore>,
    ) -> Arc<EvmLogHistory<impl Provider<Ethereum> + Clone + 'static>> {
        let asserter = Asserter::new();
        // get_chain_id, get_block_number for anchor
        asserter.push_success(&U64::from(TEST_CHAIN));
        asserter.push_success(&U64::from(200u64));
        // anchor block-header lookup hits the cache (warm_cache has block 200).
        let provider = ProviderBuilder::default().connect_mocked_client(asserter);
        EvmLogHistory::with_options(provider, store, ADDR, TOPICS, 100, 12)
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
        assert_eq!(items[0].item.timestamp, GENESIS_TS + 1200);
        assert_eq!(items[2].item.timestamp, GENESIS_TS + 1212);
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
        let pos = hs.time_pos(GENESIS_TS + 1200).await.unwrap();
        assert_eq!(pos.block, 99);
        assert_eq!(pos.log_index, u32::MAX);
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
        assert_eq!(history.chain_id(), TEST_CHAIN);
        // EvmPosition has only block + log_index — no chain field.
        let p = EvmPosition::new(100, 0);
        let s = format!("{p:?}");
        assert!(
            !s.contains("chain"),
            "EvmPosition Debug should not mention chain: {s}"
        );
    }

    // Suppress dead-code warnings on unused fixtures during phase work.
    #[allow(dead_code)]
    fn _unused_warmup() {
        let _ = (BH_0, BH_100, BH_101, BH_200);
    }
}
