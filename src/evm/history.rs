//! Concrete `HistoryBackend<EvmEvent, EvmPosition>` backed by an alloy
//! Provider and [`LogCache`].
//!
//! This is the first concrete history implementation. It fetches EVM logs
//! from an RPC provider, caches them in SQLite via `LogCache`, and serves
//! navigation queries from the cache.
//!
//! ## Tip tracking
//!
//! Call [`start_tip_tracking`](EvmLogHistory::start_tip_tracking) to spawn
//! a background task that polls the chain head and updates the reactive
//! [`Store`]s. Without it, `after()` with no cutoff returns `None` at the
//! tip instead of blocking.

use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use tokio::task::JoinHandle;

use crate::arch::evm::{EvmEvent, EvmPosition};
use crate::arch::{
    Cursor, HistoryBackend, HistoryError, SearchEvent, SearchStream, Side, Store,
    Timestamp,
};
use crate::evm::log_cache_legacy::{BlockHeader, LogCache};

/// Default chunk size for `eth_getLogs` calls (in blocks).
const DEFAULT_CHUNK_SIZE: u64 = 1000;

/// Assumed block time in seconds for `at(time)` estimation.
const DEFAULT_BLOCK_TIME: u64 = 12;

/// Default tip polling interval.
const DEFAULT_TIP_POLL_INTERVAL: Duration = Duration::from_secs(2);

// ============================================================
// EvmLogHistory
// ============================================================

/// A [`HistoryBackend`] for EVM log events.
///
/// Generic over `P`: any alloy provider (direct RPC, CachingProvider, etc.).
/// Each instance is scoped to a single log filter (address + topics).
pub struct EvmLogHistory<P> {
    provider: P,
    cache: Arc<LogCache>,
    filter_hash: [u8; 32],
    address: [u8; 20],
    topics: [Option<[u8; 32]>; 4],
    chunk_size: u64,
    chain_id: u64,

    // Reactive tip stores
    latest_store: Store<EvmPosition>,
    best_store: Store<EvmPosition>,
    safe_store: Store<EvmPosition>,

    // Senders to update tip stores
    latest_tx: tokio::sync::watch::Sender<EvmPosition>,
    best_tx: tokio::sync::watch::Sender<EvmPosition>,
    safe_tx: tokio::sync::watch::Sender<EvmPosition>,

    // For at(time) estimation — uses a recent anchor block instead of genesis
    // because some chains (e.g. Ethereum mainnet) have genesis timestamp 0.
    anchor_block: u64,
    anchor_timestamp: u64,
    block_time: u64,

    // Self-reference so Cursors can hold Arc<dyn HistoryBackend>
    self_ref: OnceLock<Weak<Self>>,
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmLogHistory<P> {
    /// Create a new history. Returns `Arc<Self>` because cursors need a
    /// backend reference.
    ///
    /// Fetches genesis block timestamp from RPC for `at(time)` estimation.
    pub async fn new(
        provider: P,
        cache: Arc<LogCache>,
        address: [u8; 20],
        topics: [Option<[u8; 32]>; 4],
    ) -> Result<Arc<Self>, HistoryError> {
        Self::with_options(provider, cache, address, topics, DEFAULT_CHUNK_SIZE, DEFAULT_BLOCK_TIME).await
    }

    /// Create with explicit chunk size and block time.
    pub async fn with_options(
        provider: P,
        cache: Arc<LogCache>,
        address: [u8; 20],
        topics: [Option<[u8; 32]>; 4],
        chunk_size: u64,
        block_time: u64,
    ) -> Result<Arc<Self>, HistoryError> {
        let filter_hash = crate::evm::log_cache_legacy::compute_filter_hash(&address, &topics);

        // Fetch chain_id and a recent anchor block for at() estimation.
        let chain_id = provider.get_chain_id().await
            .map_err(|e| HistoryError::internal(format!("get_chain_id failed: {e}")))?;
        let anchor_block = provider.get_block_number().await
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))?;
        let anchor_timestamp = Self::fetch_block_timestamp(&provider, &cache, anchor_block).await?;

        let (latest_tx, latest_rx) = tokio::sync::watch::channel(EvmPosition::earliest(chain_id));
        let (best_tx, best_rx) = tokio::sync::watch::channel(EvmPosition::earliest(chain_id));
        let (safe_tx, safe_rx) = tokio::sync::watch::channel(EvmPosition::earliest(chain_id));

        let history = Arc::new(Self {
            provider,
            cache,
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

        // Store weak self-reference for cursor creation.
        let _ = history.self_ref.set(Arc::downgrade(&history));

        fn fmt_topic(t: &Option<[u8; 32]>) -> String {
            match t {
                Some(b) => format!("0x{}", alloy::hex::encode(b)),
                None => "*".into(),
            }
        }
        tracing::debug!(
            chain_id,
            address = %format!("0x{}", alloy::hex::encode(address)),
            topic0 = %fmt_topic(&topics[0]),
            topic1 = %fmt_topic(&topics[1]),
            topic2 = %fmt_topic(&topics[2]),
            topic3 = %fmt_topic(&topics[3]),
            chunk_size,
            filter_hash = %format!("0x{}", alloy::hex::encode(&filter_hash[..8])),
            "EvmLogHistory created"
        );

        Ok(history)
    }

    /// Create a [`History`](crate::arch::History) wrapping this backend.
    pub fn history(self: &Arc<Self>) -> crate::arch::History<EvmEvent, EvmPosition> {
        crate::arch::History::new(Arc::clone(self) as Arc<dyn HistoryBackend<EvmEvent, EvmPosition>>)
    }

    /// Get a strong Arc<Self> from the stored weak ref.
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

    /// Fetch the latest block from RPC and update the `latest` store.
    ///
    /// Returns the new tip block number.
    pub async fn poll_tip(&self) -> Result<u64, HistoryError> {
        let block_number = self.latest_block_number().await?;
        let header = self.ensure_block_header(block_number).await?;

        let pos = EvmPosition {
            chain_id: self.chain_id,
            block_number,
            log_index: u32::MAX, // after all logs in this block
            block_hash: header.hash,
            timestamp: header.timestamp,
            side: Side::After,
        };

        let prev = self.latest_store.get().block_number;
        let _ = self.latest_tx.send(pos);
        if block_number != prev {
            tracing::debug!(block_number, prev, "tip updated");
        }
        Ok(block_number)
    }

    /// Spawn a background task that polls the chain head at `interval`
    /// and updates the `latest` store. Returns a `JoinHandle` that can
    /// be used to cancel it.
    ///
    /// Without this, `after()` with no cutoff returns `None` at the tip
    /// instead of blocking for new events.
    pub fn start_tip_tracking(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        self.start_tip_tracking_with_cancel(interval, tokio_util::sync::CancellationToken::new())
    }

    /// Same as [`start_tip_tracking`](Self::start_tip_tracking) but accepts
    /// an external cancellation token.
    pub fn start_tip_tracking_with_cancel(
        self: &Arc<Self>,
        interval: Duration,
        cancel: tokio_util::sync::CancellationToken,
    ) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {
                        if let Err(e) = this.poll_tip().await {
                            tracing::warn!("tip poll failed: {e}");
                        }
                    }
                }
            }
        })
    }

    /// Wait for the latest store to advance past `current_block`.
    async fn wait_for_tip_advance(&self, current_block: u64) -> Result<u64, HistoryError> {
        let mut rx = self.latest_tx.subscribe();
        loop {
            rx.changed().await.map_err(|_| {
                HistoryError::internal("tip tracking channel closed — call start_tip_tracking()")
            })?;
            let pos = rx.borrow_and_update().clone();
            if pos.block_number > current_block {
                return Ok(pos.block_number);
            }
        }
    }

    // --------------------------------------------------------
    // Cache management
    // --------------------------------------------------------

    /// Ensure the cache covers [start_block, end_block] for our filter.
    /// Fetches any missing sub-ranges from RPC.
    async fn ensure_cached(&self, start_block: u64, end_block: u64) -> Result<(), HistoryError> {
        let scanned = self.cache.get_scanned_ranges(&self.filter_hash)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        let gaps = crate::evm::log_cache_legacy::compute_gaps((start_block, end_block), &scanned);

        if gaps.is_empty() {
            tracing::trace!(start_block, end_block, "cache hit");
        } else {
            tracing::debug!(start_block, end_block, num_gaps = gaps.len(), "cache miss, fetching gaps");
        }

        for (gap_start, gap_end) in gaps {
            self.fetch_gap(gap_start, gap_end).await?;
        }

        Ok(())
    }

    /// Fetch logs for a single gap range, chunking if necessary.
    async fn fetch_gap(&self, start: u64, end: u64) -> Result<(), HistoryError> {
        let mut cursor = start;
        while cursor <= end {
            let chunk_end = cursor.saturating_add(self.chunk_size - 1).min(end);
            self.fetch_chunk(cursor, chunk_end).await?;
            if chunk_end >= end {
                break;
            }
            cursor = chunk_end + 1;
        }
        Ok(())
    }

    /// Fetch a single chunk of logs from RPC and insert into cache.
    async fn fetch_chunk(&self, from: u64, to: u64) -> Result<(), HistoryError> {
        let mut filter = Filter::new()
            .address(alloy::primitives::Address::from(self.address))
            .from_block(from)
            .to_block(to);

        if let Some(t) = self.topics[0] {
            filter = filter.event_signature(alloy::primitives::B256::from(t));
        }
        if let Some(t) = self.topics[1] {
            filter = filter.topic1(alloy::primitives::B256::from(t));
        }
        if let Some(t) = self.topics[2] {
            filter = filter.topic2(alloy::primitives::B256::from(t));
        }
        if let Some(t) = self.topics[3] {
            filter = filter.topic3(alloy::primitives::B256::from(t));
        }

        tracing::debug!(from, to, blocks = to - from + 1, "eth_getLogs");

        let logs = self.provider.get_logs(&filter).await
            .map_err(|e| HistoryError::internal(format!("eth_getLogs failed: {e}")))?;

        tracing::debug!(from, to, num_logs = logs.len(), "eth_getLogs returned");

        self.cache.insert_logs(&self.filter_hash, &logs)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        // Record scanned range. Need the end block's hash.
        let end_header = self.ensure_block_header(to).await?;
        self.cache
            .record_scanned_range(&self.filter_hash, from, to, &end_header.hash)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        // Cache block headers for blocks that have logs.
        for log in &logs {
            if let Some(bn) = log.block_number {
                let _ = self.ensure_block_header(bn).await;
            }
        }

        Ok(())
    }

    /// Get block header, cache-first with RPC fallback.
    async fn ensure_block_header(&self, block_number: u64) -> Result<BlockHeader, HistoryError> {
        if let Some(header) = self.cache.get_block_header(block_number)
            .map_err(|e| HistoryError::internal(e.to_string()))?
        {
            tracing::trace!(block_number, "block header cache hit");
            return Ok(header);
        }

        tracing::debug!(block_number, "eth_getBlockByNumber");

        let block = self.provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|e| HistoryError::internal(format!("get_block failed: {e}")))?
            .ok_or_else(|| HistoryError::internal(format!("block {block_number} not found")))?;

        let hash: [u8; 32] = block.header.hash.0;
        let parent_hash: [u8; 32] = block.header.parent_hash.0;
        let timestamp = block.header.timestamp;

        self.cache
            .insert_block_header(block_number, &hash, &parent_hash, timestamp)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        Ok(BlockHeader {
            height: block_number,
            hash,
            parent_hash,
            timestamp,
        })
    }

    /// Fetch block timestamp (with caching). Static — used by constructor.
    async fn fetch_block_timestamp(
        provider: &P,
        cache: &LogCache,
        block_number: u64,
    ) -> Result<u64, HistoryError> {
        if let Some(header) = cache.get_block_header(block_number)
            .map_err(|e| HistoryError::internal(e.to_string()))?
        {
            tracing::trace!(block_number, "genesis block header cache hit");
            return Ok(header.timestamp);
        }

        tracing::debug!(block_number, "eth_getBlockByNumber (genesis)");

        let block = provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(|e| HistoryError::internal(format!("get_block failed: {e}")))?
            .ok_or_else(|| HistoryError::internal(format!("block {block_number} not found")))?;

        let hash: [u8; 32] = block.header.hash.0;
        let parent_hash: [u8; 32] = block.header.parent_hash.0;
        let timestamp = block.header.timestamp;

        cache
            .insert_block_header(block_number, &hash, &parent_hash, timestamp)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        Ok(timestamp)
    }

    // --------------------------------------------------------
    // Log → domain type conversions
    // --------------------------------------------------------

    fn log_to_evm_event(log: &alloy::rpc::types::Log) -> EvmEvent {
        let addr: [u8; 20] = log.address().0 .0;
        let block_hash: [u8; 32] = log.block_hash.map(|h| h.0).unwrap_or([0; 32]);
        let tx_hash: [u8; 32] = log.transaction_hash.map(|h| h.0).unwrap_or([0; 32]);

        EvmEvent {
            contract_address: addr,
            block_number: log.block_number.unwrap_or(0),
            timestamp: 0, // filled in by caller
            block_hash,
            log_index: log.log_index.unwrap_or(0) as u32,
            tx_hash,
            topics: log.topics().iter().map(|t| t.0).collect(),
            data: log.data().data.to_vec(),
        }
    }

    async fn log_to_position(
        &self,
        log: &alloy::rpc::types::Log,
        side: Side,
    ) -> Result<EvmPosition, HistoryError> {
        let block_number = log.block_number.unwrap_or(0);
        let header = self.ensure_block_header(block_number).await?;

        Ok(EvmPosition {
            chain_id: self.chain_id,
            block_number,
            log_index: log.log_index.unwrap_or(0) as u32,
            block_hash: header.hash,
            timestamp: header.timestamp,
            side,
        })
    }

    async fn log_to_cursor(
        &self,
        log: &alloy::rpc::types::Log,
    ) -> Result<Cursor<EvmEvent, EvmPosition>, HistoryError> {
        let before = self.log_to_position(log, Side::Before).await?;
        let after = self.log_to_position(log, Side::After).await?;

        let mut event = Self::log_to_evm_event(log);
        event.timestamp = before.timestamp;

        Ok(Cursor::new(event, before, after, self.arc_self()))
    }

    // --------------------------------------------------------
    // Navigation helpers
    // --------------------------------------------------------

    async fn latest_block_number(&self) -> Result<u64, HistoryError> {
        tracing::debug!("eth_blockNumber");
        let n = self.provider
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

    fn pos_before(a: &EvmPosition, b: &EvmPosition) -> bool {
        (a.block_number, a.log_index) < (b.block_number, b.log_index)
    }

    fn pos_equiv(a: &EvmPosition, b: &EvmPosition) -> bool {
        // earliest sentinels
        if a.block_number == 0 && a.log_index == 0 && a.side == Side::Before
            && b.block_number == 0 && b.log_index == 0 && b.side == Side::Before
        {
            return true;
        }
        // never sentinels
        if a.block_number == u64::MAX && b.block_number == u64::MAX {
            return true;
        }
        // Same coordinate, same side
        a.block_number == b.block_number && a.log_index == b.log_index && a.side == b.side
    }

    fn search_coords(pos: &EvmPosition) -> (u64, u32) {
        (pos.block_number, pos.log_index)
    }

    /// Search forward for first matching log starting from `position`,
    /// up to `max_block`. Returns the log if found.
    fn search_cache_forward(
        &self,
        position: &EvmPosition,
        start_block: u64,
        end_block: u64,
        is_first_chunk: bool,
    ) -> Result<Option<alloy::rpc::types::Log>, HistoryError> {
        let log = if is_first_chunk {
            let (block, idx) = Self::search_coords(position);
            match position.side {
                Side::Before => self.cache
                    .first_log_at_or_after(&self.filter_hash, block, idx),
                Side::After => self.cache
                    .first_log_after(&self.filter_hash, block, idx),
            }
        } else {
            self.cache
                .first_log_at_or_after(&self.filter_hash, start_block, 0)
        };
        // Discard results beyond the current cached chunk — the range
        // (end_block+1..) may not have been fetched yet.
        let log = log.map_err(|e| HistoryError::internal(e.to_string()))?;
        Ok(log.filter(|l| l.block_number.unwrap_or(0) <= end_block))
    }

    // --------------------------------------------------------
    // Search stream tasks
    // --------------------------------------------------------

    /// Type-annotated Ok for search event channel sends.
    fn search_ok(
        event: SearchEvent<EvmEvent, EvmPosition>,
    ) -> Result<SearchEvent<EvmEvent, EvmPosition>, HistoryError> {
        Ok(event)
    }

    /// Wrap an mpsc receiver as a [`SearchStream`].
    fn rx_to_stream(
        rx: tokio::sync::mpsc::Receiver<
            Result<SearchEvent<EvmEvent, EvmPosition>, HistoryError>,
        >,
    ) -> SearchStream<EvmEvent, EvmPosition> {
        Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    /// Forward search task. Sends [`SearchEvent`]s to `tx`.
    ///
    /// Returns `Ok(())` on completion or cancellation. Returns `Err` on
    /// RPC/cache errors — caller sends the error to the channel.
    async fn run_search_after(
        &self,
        position: &EvmPosition,
        cutoff: Option<&EvmPosition>,
        tx: &tokio::sync::mpsc::Sender<Result<SearchEvent<EvmEvent, EvmPosition>, HistoryError>>,
    ) -> Result<(), HistoryError> {
        let (start_block, _) = Self::search_coords(position);
        let has_cutoff = cutoff.is_some();

        let mut max_block = match cutoff {
            Some(c) => c.block_number,
            None => self.latest_block_number().await?,
        };

        let mut search_from = start_block;

        loop {
            let mut chunk_start = search_from;
            while chunk_start <= max_block {
                let chunk_end = (chunk_start + self.chunk_size - 1).min(max_block);
                self.ensure_cached(chunk_start, chunk_end).await?;

                let is_first = chunk_start == start_block;
                if let Some(log) = self.search_cache_forward(position, chunk_start, chunk_end, is_first)? {
                    let (lb, li) = (
                        log.block_number.unwrap_or(0),
                        log.log_index.unwrap_or(0) as u32,
                    );
                    if let Some(c) = cutoff {
                        if (lb, li) > (c.block_number, c.log_index) {
                            return Ok(()); // Past cutoff
                        }
                    }
                    let cursor = self.log_to_cursor(&log).await?;
                    let _ = tx.send(Self::search_ok(SearchEvent::Found(cursor))).await;
                    return Ok(());
                }

                // Chunk scanned, nothing found — report progress
                let scanned = EvmPosition {
                    chain_id: self.chain_id,
                    block_number: chunk_end,
                    log_index: u32::MAX,
                    block_hash: [0; 32],
                    timestamp: 0,
                    side: Side::After,
                };
                if tx.send(Self::search_ok(SearchEvent::Scanned(scanned))).await.is_err() {
                    return Ok(()); // Receiver dropped = cancelled
                }

                chunk_start = chunk_end + 1;
            }

            // Don't re-scan on next tip advance
            search_from = max_block + 1;

            if has_cutoff {
                return Ok(()); // Bounded search complete
            }

            // Unbounded — wait for new blocks
            max_block = self.wait_for_tip_advance(max_block).await?;
        }
    }

    /// Backward search task. Sends [`SearchEvent`]s to `tx`.
    async fn run_search_before(
        &self,
        position: &EvmPosition,
        cutoff: Option<&EvmPosition>,
        tx: &tokio::sync::mpsc::Sender<Result<SearchEvent<EvmEvent, EvmPosition>, HistoryError>>,
    ) -> Result<(), HistoryError> {
        let (block, idx) = Self::search_coords(position);
        let min_block = match cutoff {
            Some(c) => c.block_number,
            None => 0,
        };

        if block == 0 && idx == 0 && position.side == Side::Before {
            return Ok(()); // At earliest
        }

        // Cap search_end: u64::MAX (never sentinel) overflows SQL i64 casts.
        let mut search_end = if block == u64::MAX {
            let tip = self.latest_store.get();
            if tip.block_number > 0 && tip.block_number < u64::MAX {
                tip.block_number
            } else {
                let ranges = self.cache.get_scanned_ranges(&self.filter_hash)
                    .map_err(|e| HistoryError::internal(e.to_string()))?;
                match ranges.last() {
                    Some(r) => r.1,
                    None => return Ok(()),
                }
            }
        } else {
            block
        };

        loop {
            let search_start = if search_end > self.chunk_size {
                (search_end - self.chunk_size + 1).max(min_block)
            } else {
                min_block
            };

            self.ensure_cached(search_start, search_end).await?;

            let log = if search_end == block {
                match position.side {
                    Side::After => self.cache
                        .last_log_at_or_before(&self.filter_hash, block, idx)
                        .map_err(|e| HistoryError::internal(e.to_string()))?,
                    Side::Before => self.cache
                        .last_log_before(&self.filter_hash, block, idx)
                        .map_err(|e| HistoryError::internal(e.to_string()))?,
                }
            } else {
                self.cache
                    .last_log_at_or_before(&self.filter_hash, search_end, u32::MAX)
                    .map_err(|e| HistoryError::internal(e.to_string()))?
            };
            // Discard results before the current cached chunk.
            let log = log.filter(|l| l.block_number.unwrap_or(0) >= search_start);

            if let Some(log) = log {
                let (lb, li) = (
                    log.block_number.unwrap_or(0),
                    log.log_index.unwrap_or(0) as u32,
                );
                if let Some(c) = cutoff {
                    if (lb, li) < (c.block_number, c.log_index) {
                        return Ok(());
                    }
                }
                let cursor = self.log_to_cursor(&log).await?;
                let _ = tx.send(Self::search_ok(SearchEvent::Found(cursor))).await;
                return Ok(());
            }

            // Chunk scanned backward, nothing found — report progress
            let scanned = EvmPosition {
                chain_id: self.chain_id,
                block_number: search_start,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::Before,
            };
            if tx.send(Self::search_ok(SearchEvent::Scanned(scanned))).await.is_err() {
                return Ok(()); // Cancelled
            }

            if search_start <= min_block {
                return Ok(()); // Reached the beginning
            }
            search_end = search_start - 1;
        }
    }
}

// ============================================================
// HistoryBackend implementation
// ============================================================

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> HistoryBackend<EvmEvent, EvmPosition>
    for EvmLogHistory<P>
{
    fn latest(&self) -> &Store<EvmPosition> {
        &self.latest_store
    }

    fn best(&self) -> &Store<EvmPosition> {
        &self.best_store
    }

    fn safe(&self) -> &Store<EvmPosition> {
        &self.safe_store
    }

    fn source_id(&self) -> u64 {
        self.chain_id
    }

    fn earliest(&self) -> EvmPosition {
        EvmPosition::earliest(self.chain_id)
    }

    fn never(&self) -> EvmPosition {
        EvmPosition::never(self.chain_id)
    }

    fn at(&self, time: Timestamp) -> EvmPosition {
        let block = self.estimate_block_from_time(time);
        EvmPosition {
            chain_id: self.chain_id,
            block_number: block,
            log_index: 0,
            block_hash: [0; 32],
            timestamp: time,
            side: Side::Before,
        }
    }

    async fn is_equiv(
        &self,
        a: &EvmPosition,
        b: &EvmPosition,
    ) -> Result<bool, HistoryError> {
        Ok(Self::pos_equiv(a, b))
    }

    async fn lca(
        &self,
        a: &EvmPosition,
        b: &EvmPosition,
    ) -> Result<EvmPosition, HistoryError> {
        if Self::pos_before(a, b) || Self::pos_equiv(a, b) {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    }

    fn search_after(
        &self,
        position: &EvmPosition,
        cutoff: Option<&EvmPosition>,
    ) -> SearchStream<EvmEvent, EvmPosition> {
        let pos = position.clone();
        let cut = cutoff.cloned();
        let this = self.arc_self();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            if let Err(e) = this.run_search_after(&pos, cut.as_ref(), &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        Self::rx_to_stream(rx)
    }

    fn search_before(
        &self,
        position: &EvmPosition,
        cutoff: Option<&EvmPosition>,
    ) -> SearchStream<EvmEvent, EvmPosition> {
        let pos = position.clone();
        let cut = cutoff.cloned();
        let this = self.arc_self();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            if let Err(e) = this.run_search_before(&pos, cut.as_ref(), &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        Self::rx_to_stream(rx)
    }

    async fn is_after(
        &self,
        cursor_pos: &EvmPosition,
        p: &EvmPosition,
    ) -> Result<bool, HistoryError> {
        Ok((cursor_pos.block_number, cursor_pos.log_index) > (p.block_number, p.log_index))
    }

    async fn is_before(
        &self,
        cursor_pos: &EvmPosition,
        p: &EvmPosition,
    ) -> Result<bool, HistoryError> {
        Ok((cursor_pos.block_number, cursor_pos.log_index) < (p.block_number, p.log_index))
    }

    fn estimate_scan_count(&self, from: &EvmPosition, to: &EvmPosition) -> u64 {
        if from.block_number >= to.block_number {
            return 0;
        }
        let scanned = self
            .cache
            .get_scanned_ranges(&self.filter_hash)
            .unwrap_or_default();
        let gaps =
            crate::evm::log_cache_legacy::compute_gaps((from.block_number, to.block_number), &scanned);
        gaps.iter()
            .map(|&(s, e)| (e - s) / self.chunk_size + 1)
            .sum()
    }

    fn scan_chunk_before(&self, target: &EvmPosition) -> EvmPosition {
        EvmPosition {
            block_number: target.block_number.saturating_sub(self.chunk_size),
            log_index: 0,
            side: Side::Before,
            ..target.clone()
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Position;
    use crate::evm::log_cache_legacy::compute_filter_hash;
    use alloy::primitives::{Address, Bytes, LogData, B256};
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::types::Log;
    use alloy::transports::mock::Asserter;

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
            inner: alloy::primitives::Log {
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

    /// Pre-populate cache with genesis + test blocks/logs, scanned ranges.
    /// Returns (cache, filter_hash).
    fn warm_cache() -> (Arc<LogCache>, [u8; 32]) {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        // Block headers
        cache.insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS).unwrap();
        cache.insert_block_header(100, &BH_100, &BH_0, GENESIS_TS + 1200).unwrap();
        cache.insert_block_header(101, &BH_101, &BH_100, GENESIS_TS + 1212).unwrap();
        cache.insert_block_header(200, &BH_200, &BH_101, GENESIS_TS + 2400).unwrap();

        // Logs: block 100 has 2 logs (idx 0,1), block 101 has 1 log (idx 0)
        let logs = vec![
            make_log(100, 0, BH_100),
            make_log(100, 1, BH_100),
            make_log(101, 0, BH_101),
        ];
        cache.insert_logs(&fh, &logs).unwrap();

        // Mark range as fully scanned
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();

        (cache, fh)
    }

    /// Build an EvmLogHistory backed by the warm cache. No RPC calls needed
    /// for construction (genesis header is in cache) or bounded queries
    /// (scanned ranges cover [0, 200]).
    async fn make_history(
        cache: Arc<LogCache>,
    ) -> Arc<EvmLogHistory<impl Provider<Ethereum> + Clone + Send + Sync + 'static>> {
        let asserter = Asserter::new();
        // Constructor calls: get_chain_id(), get_block_number() (anchor)
        asserter.push_success(&alloy::primitives::U64::from(TEST_CHAIN));
        asserter.push_success(&alloy::primitives::U64::from(200u64));
        // fetch_block_timestamp(200) hits cache (warm_cache has block 200 header)
        let provider = ProviderBuilder::default().connect_mocked_client(asserter);
        EvmLogHistory::with_options(provider, cache, ADDR, TOPICS, 100, 12)
            .await
            .unwrap()
    }

    /// Cutoff at block 200 — keeps all queries within the warm cache.
    fn cutoff_200() -> EvmPosition {
        EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: 200,
            log_index: u32::MAX,
            block_hash: BH_200,
            timestamp: 0,
            side: Side::After,
        }
    }

    // -- Navigation: after() --

    #[tokio::test]
    async fn after_earliest_returns_first_log() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        let c = h.after(&EvmPosition::earliest(TEST_CHAIN), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 100);
        assert_eq!(c.value().log_index, 0);
    }

    #[tokio::test]
    async fn after_side_before_is_inclusive() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        // Position Before (100, 0) → should return the log at (100, 0)
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: BH_100, timestamp: 0, side: Side::Before,
        };
        let c = h.after(&pos, Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 100);
        assert_eq!(c.value().log_index, 0);
    }

    #[tokio::test]
    async fn after_side_after_is_exclusive() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        // Position After (100, 0) → should skip (100, 0), return (100, 1)
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: BH_100, timestamp: 0, side: Side::After,
        };
        let c = h.after(&pos, Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 100);
        assert_eq!(c.value().log_index, 1);
    }

    #[tokio::test]
    async fn after_returns_none_past_last_log() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        // After the last log (101, 0)
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 101, log_index: 0,
            block_hash: BH_101, timestamp: 0, side: Side::After,
        };
        assert!(h.after(&pos, Some(&cutoff)).await.unwrap().is_none());
    }

    // -- Navigation: before() --

    #[tokio::test]
    async fn before_never_returns_last_log() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        let c = h.before(&EvmPosition::never(TEST_CHAIN), None).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 101);
        assert_eq!(c.value().log_index, 0);
    }

    #[tokio::test]
    async fn before_side_after_is_inclusive() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        // Position After (100, 1) → should return the log at (100, 1)
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 1,
            block_hash: BH_100, timestamp: 0, side: Side::After,
        };
        let c = h.before(&pos, None).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 100);
        assert_eq!(c.value().log_index, 1);
    }

    #[tokio::test]
    async fn before_side_before_is_exclusive() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        // Position Before (100, 1) → should return (100, 0)
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 1,
            block_hash: BH_100, timestamp: 0, side: Side::Before,
        };
        let c = h.before(&pos, None).await.unwrap().unwrap();
        assert_eq!(c.value().block_number, 100);
        assert_eq!(c.value().log_index, 0);
    }

    #[tokio::test]
    async fn before_earliest_returns_none() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        assert!(h.before(&EvmPosition::earliest(TEST_CHAIN), None).await.unwrap().is_none());
    }

    // -- Cursor navigation --

    #[tokio::test]
    async fn cursor_next_walks_forward() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        let c0 = h.after(&EvmPosition::earliest(TEST_CHAIN), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!((c0.value().block_number, c0.value().log_index), (100, 0));

        // next() calls backend.after(c.after, None) — use cutoff variant via direct call
        let c1 = h.after(c0.after(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!((c1.value().block_number, c1.value().log_index), (100, 1));

        let c2 = h.after(c1.after(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!((c2.value().block_number, c2.value().log_index), (101, 0));

        assert!(h.after(c2.after(), Some(&cutoff)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cursor_prev_walks_backward() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        let c2 = h.before(&EvmPosition::never(TEST_CHAIN), None).await.unwrap().unwrap();
        assert_eq!((c2.value().block_number, c2.value().log_index), (101, 0));

        let c1 = h.before(c2.before(), None).await.unwrap().unwrap();
        assert_eq!((c1.value().block_number, c1.value().log_index), (100, 1));

        let c0 = h.before(c1.before(), None).await.unwrap().unwrap();
        assert_eq!((c0.value().block_number, c0.value().log_index), (100, 0));

        assert!(h.before(c0.before(), None).await.unwrap().is_none());
    }

    // -- Position ops --

    #[tokio::test]
    async fn is_equiv_same_position() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        let a = EvmPosition::earliest(TEST_CHAIN);
        assert!(h.is_equiv(&a, &a).await.unwrap());

        let b = EvmPosition::never(TEST_CHAIN);
        assert!(h.is_equiv(&b, &b).await.unwrap());
    }

    #[tokio::test]
    async fn lca_returns_earlier() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        let a = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: BH_100, timestamp: 0, side: Side::After,
        };
        let b = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 101, log_index: 0,
            block_hash: BH_101, timestamp: 0, side: Side::After,
        };
        let lca = h.lca(&a, &b).await.unwrap();
        assert_eq!(lca.block_number, 100);
    }

    #[tokio::test]
    async fn at_estimates_block_from_time() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;

        // anchor = block 200 @ GENESIS_TS + 2400, block_time = 12
        // GENESIS_TS + 1200 is 1200s before anchor → 100 blocks before → block 100
        let pos = h.at(GENESIS_TS + 1200);
        assert_eq!(pos.block_number, 100);

        // GENESIS_TS + 3600 is 1200s after anchor → 100 blocks after → block 300
        let pos = h.at(GENESIS_TS + 3600);
        assert_eq!(pos.block_number, 300);
    }

    // -- Marshal round-trip --

    #[tokio::test]
    async fn position_marshal_round_trip() {
        let (cache, _) = warm_cache();
        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        let c = h.after(&EvmPosition::earliest(TEST_CHAIN), Some(&cutoff)).await.unwrap().unwrap();
        let bytes = c.before().marshal();
        let restored = EvmPosition::unmarshal(&bytes).unwrap();
        assert_eq!(restored.block_number, c.before().block_number);
        assert_eq!(restored.log_index, c.before().log_index);
        assert_eq!(restored.side, c.before().side);
    }

    // -- Tip tracking --

    #[tokio::test]
    async fn tip_tracking_blocks_then_resolves() {
        let (cache, fh) = warm_cache();

        // Add a log at block 300 that will be "discovered" after tip advance
        let bh_300: [u8; 32] = [0xA3; 32];
        cache.insert_block_header(300, &bh_300, &BH_200, GENESIS_TS + 3600).unwrap();
        cache.insert_logs(&fh, &[make_log(300, 0, bh_300)]).unwrap();
        cache.record_scanned_range(&fh, 201, 300, &bh_300).unwrap();

        // Build history with a mock that returns chain_id, anchor block, then block 200 for after()
        let asserter = Asserter::new();
        // Constructor calls: get_chain_id(), get_block_number() (anchor)
        asserter.push_success(&alloy::primitives::U64::from(TEST_CHAIN));
        asserter.push_success(&alloy::primitives::U64::from(200u64));
        // fetch_block_timestamp(200) hits cache (warm_cache has block 200 header)
        // after() without cutoff calls get_block_number() — push response
        asserter.push_success(&alloy::primitives::U64::from(200u64));
        let provider = ProviderBuilder::default().connect_mocked_client(asserter);
        let h = EvmLogHistory::with_options(provider, cache, ADDR, TOPICS, 100, 12)
            .await
            .unwrap();

        let cutoff = cutoff_200();

        // Verify nothing after (101, 0) within cutoff
        let last = h.after(&EvmPosition {
            chain_id: TEST_CHAIN, block_number: 101, log_index: 0,
            block_hash: BH_101, timestamp: 0, side: Side::After,
        }, Some(&cutoff)).await.unwrap();
        assert!(last.is_none());

        // Call after() without cutoff — first searches up to block 200 (from
        // mock get_block_number), finds nothing, then blocks waiting for tip.
        let h2 = h.clone();
        let pos = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 101, log_index: 0,
            block_hash: BH_101, timestamp: 0, side: Side::After,
        };

        let handle = tokio::spawn(async move {
            h2.after(&pos, None).await
        });

        // Let spawned task reach the wait point
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Simulate tip advance
        let _ = h.latest_tx.send(EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: 300,
            log_index: u32::MAX,
            block_hash: bh_300,
            timestamp: GENESIS_TS + 3600,
            side: Side::After,
        });

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("timed out waiting for after()")
            .expect("task panicked");

        let cursor = result.unwrap().unwrap();
        assert_eq!(cursor.value().block_number, 300);
        assert_eq!(cursor.value().log_index, 0);
    }

    // -- Stale cache does not skip uncached ranges --

    #[tokio::test]
    async fn stale_cached_log_beyond_chunk_not_returned() {
        // Regression: search_cache_forward was unbounded — a log cached by a
        // previous session far beyond chunk_end was returned as Found, causing
        // the search to skip uncached intermediate blocks and miss events.
        //
        // Setup: blocks 0-200 fully scanned (no RPC needed), NO logs in that
        // range. A stale log at block 500 from a previous session. Search
        // from 0 with cutoff at 200.
        //
        // Before the fix: chunk [0,99] unbounded query finds 500. Since
        // 500 > cutoff 200, the search reports "past cutoff" and returns
        // None — without ever scanning chunks [100,199].
        //
        // After the fix: chunk [0,99] bounded query finds nothing (500 > 99).
        // The search continues through [100,199], finds nothing, and
        // correctly returns None.
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        cache.insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS).unwrap();
        cache.insert_block_header(200, &BH_200, &BH_0, GENESIS_TS + 2400).unwrap();
        let bh_500: [u8; 32] = [0x55; 32];
        cache.insert_block_header(500, &bh_500, &BH_200, GENESIS_TS + 6000).unwrap();

        // Fully scanned 0-200 with no logs.
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();

        // Stale log at 500 from a previous session.
        cache.insert_logs(&fh, &[make_log(500, 0, bh_500)]).unwrap();
        cache.record_scanned_range(&fh, 500, 500, &bh_500).unwrap();

        let h = make_history(cache).await;
        let history = h.history();

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        // Collect all Scanned events from the search stream to verify the
        // search actually walks through all chunks instead of short-circuiting.
        let mut scanned_blocks = vec![];
        let mut stream = history.search_after(
            &EvmPosition::earliest(TEST_CHAIN), Some(&cutoff),
        );
        use futures_util::StreamExt;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                SearchEvent::Scanned(pos) => scanned_blocks.push(pos.block_number),
                SearchEvent::Found(_) => panic!("should not find stale log at 500"),
            }
        }

        // With chunk_size=100 and range 0-200, we expect chunks [0,99], [100,199],
        // and [200,200] (the cutoff block itself).
        assert_eq!(scanned_blocks, vec![99, 199, 200],
            "search must scan all chunks, not short-circuit on stale log at 500");
    }

    #[tokio::test]
    async fn stale_cached_log_before_chunk_not_returned_in_reverse() {
        // Same bug in reverse: last_log_at_or_before was unbounded and could
        // find a stale log from far before the current chunk's lower bound.
        //
        // Setup: blocks 0-200 fully scanned. Log at 150. Stale log at block
        // 10 from a different scanned range. Search backward from 200.
        //
        // First backward chunk is [101,200]. The log at 150 is within it.
        // Before the fix, the unbounded backward query from search_end=200
        // could return 10 (if 150 didn't exist) skipping uncached ranges.
        // We test the simpler case: log at 150 should be found, not 10.
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let fh = compute_filter_hash(&ADDR, &TOPICS);

        cache.insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS).unwrap();
        let bh_10: [u8; 32] = [0x10; 32];
        cache.insert_block_header(10, &bh_10, &BH_0, GENESIS_TS + 120).unwrap();
        let bh_150: [u8; 32] = [0x15; 32];
        cache.insert_block_header(150, &bh_150, &bh_10, GENESIS_TS + 1800).unwrap();
        cache.insert_block_header(200, &BH_200, &bh_150, GENESIS_TS + 2400).unwrap();

        // Fully scanned 0-200.
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();
        cache.insert_logs(&fh, &[make_log(150, 0, bh_150)]).unwrap();

        let h = make_history(cache).await;
        let history = h.history();

        let cursor = history.before(&EvmPosition::never(TEST_CHAIN), None)
            .await.unwrap().unwrap();
        assert_eq!(cursor.value().block_number, 150,
            "should find log at 150, not jump to stale cached log at 10");
    }

    // -- Empty history --

    #[tokio::test]
    async fn empty_history_returns_none() {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        cache.insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS).unwrap();
        cache.insert_block_header(200, &BH_200, &BH_0, GENESIS_TS + 2400).unwrap();

        let fh = compute_filter_hash(&ADDR, &TOPICS);
        // Scanned but no logs
        cache.record_scanned_range(&fh, 0, 200, &BH_200).unwrap();

        let h = make_history(cache).await;
        let cutoff = cutoff_200();

        assert!(h.after(&EvmPosition::earliest(TEST_CHAIN), Some(&cutoff)).await.unwrap().is_none());
        assert!(h.before(&EvmPosition::never(TEST_CHAIN), None).await.unwrap().is_none());
    }

    // -- estimate_scan_count --

    #[tokio::test]
    async fn estimate_scan_count_cached() {
        // Fully scanned range → 0 scans needed
        let (cache, _) = warm_cache(); // scanned [0, 200]
        let h = make_history(cache).await;
        let from = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 0, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        let to = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        assert_eq!(h.estimate_scan_count(&from, &to), 0);
    }

    #[tokio::test]
    async fn estimate_scan_count_uncached() {
        // No scanned ranges → ceil(range/chunk)
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        cache.insert_block_header(0, &BH_0, &[0; 32], GENESIS_TS).unwrap();
        cache.insert_block_header(200, &BH_200, &BH_0, GENESIS_TS + 2400).unwrap();

        let h = make_history(cache).await;
        // chunk_size is 100 (make_history uses with_options(..., 100, 12))
        // range is 0..2000 → one gap (0, 2000), (2000-0)/100 + 1 = 21
        let from = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 0, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        let to = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 2000, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        assert_eq!(h.estimate_scan_count(&from, &to), 21);
    }

    #[tokio::test]
    async fn estimate_scan_count_gaps() {
        // Partial coverage: scanned [0,200], gap [201, 1500]
        let (cache, _fh) = warm_cache(); // scanned [0, 200]
        let h = make_history(cache).await;
        let from = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 0, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        let to = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 1500, log_index: 0,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        // Gap: (201, 1500), width = 1299, chunk_size=100 → 1299/100 + 1 = 13
        assert_eq!(h.estimate_scan_count(&from, &to), 13);
    }
}
