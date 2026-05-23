//! In-memory range tree backed by [`LogStore`].
//!
//! Phase 1b of the refactor (see `refactor.md`). Translates the reference implementation's
//! `RangeTree` (history.ts:758–951) to Rust: a `BTreeMap` keyed by the
//! *end* position of each [`Range`], with the same singleflight / late-item
//! semantics. Items themselves live in [`LogStore`]; the tree only holds
//! [`LogRef`]s, so a long-running history's resident memory is bounded by
//! the number of distinct positions (Phase 7 will add eviction).
//!
//! Lazy loading: rather than slurping every `scanned_range` from
//! [`LogStore`] on first access, the tree materializes per-window on
//! demand via [`LogIndex::ensure_window_loaded`]. This caps startup cost
//! at the cost of one bookkeeping `Vec<(u64, u64)>` of windows already
//! pulled in.
//!
//! This module owns: types, page insertion (four late-item cases),
//! lazy-load of scanned ranges, the fetcher-driven singleflight, the
//! `items()` async stream, and the cached-only navigation methods
//! (`first_at_or_after`, `first_after`, `last_at_or_before`,
//! `last_before`). Wiring into `EvmLogHistory` lands in Phase 1c.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use alloy::rpc::types::Log;
use futures_core::Stream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use super::log_store::LogStore;
use crate::arch::HistoryError;

// ============================================================
// Fetcher
// ============================================================

/// Fills a gap on demand. Returns the [`LogPage`] for the requested
/// inclusive block range `[from_block, to_block]`.
///
/// In production this wraps `eth_getLogs`; tests pass a closure to make
/// `LogIndex` pure-Rust testable.
pub type Fetcher = Arc<
    dyn Fn(u64, u64) -> Pin<Box<dyn Future<Output = Result<LogPage, HistoryError>> + Send>>
        + Send
        + Sync,
>;

/// Build a [`Fetcher`] that always errors. Useful in tests that should
/// never trigger a fetch — if the fetcher is invoked the test fails
/// with a clear message.
#[cfg(test)]
pub(crate) fn never_fetch() -> Fetcher {
    Arc::new(|_, _| {
        Box::pin(async {
            Err(HistoryError::internal(
                "never_fetch fetcher was invoked: an unexpected gap fill was triggered",
            ))
        })
    })
}

// ============================================================
// BlockPos — position type for the tree
// ============================================================

/// Position within an EVM filter's event stream.
///
/// `Bottom` is the universal lower bound used as `Range.start` for ranges
/// covering the earliest blocks (avoids the underflow problem when a
/// scanned range starts at block 0).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum BlockPos {
    /// Strictly below every real `(block, log_index)`.
    Bottom,
    /// Concrete `(block, log_index)` coordinate.
    At { block: u64, log_index: u32 },
}

impl BlockPos {
    /// The position *just before* the first log in `block`. Used as the
    /// exclusive lower bound for a block-level scanned range whose start
    /// is `block`.
    pub fn before_block(block: u64) -> Self {
        match block.checked_sub(1) {
            Some(prev) => BlockPos::At {
                block: prev,
                log_index: u32::MAX,
            },
            None => BlockPos::Bottom,
        }
    }

    /// The position *at the end* of `block` — strictly greater than every
    /// possible log in that block. Used as the inclusive upper bound for
    /// a block-level scanned range whose end is `block`.
    pub fn end_of_block(block: u64) -> Self {
        BlockPos::At {
            block,
            log_index: u32::MAX,
        }
    }

    /// Convert a concrete (block, log_index) pair to a position.
    pub fn at(block: u64, log_index: u32) -> Self {
        BlockPos::At { block, log_index }
    }

    /// The block number this position sits in, if concrete. `None` for
    /// [`Bottom`].
    pub fn block(&self) -> Option<u64> {
        match self {
            BlockPos::Bottom => None,
            BlockPos::At { block, .. } => Some(*block),
        }
    }
}

// ============================================================
// LogRef — what the tree stores in lieu of full Logs
// ============================================================

/// Pointer into [`LogStore`] identifying a single cached log.
///
/// Includes `block_hash` so reorg-aware resolution can reject references
/// into blocks that have since been canonicalized away.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct LogRef {
    pub block_number: u64,
    pub log_index: u32,
    pub block_hash: [u8; 32],
}

impl LogRef {
    pub fn pos(&self) -> BlockPos {
        BlockPos::At {
            block: self.block_number,
            log_index: self.log_index,
        }
    }
}

// ============================================================
// Range — one entry in the tree
// ============================================================

/// One slice of the position-line. Covers items at positions in
/// `(start, end]`; `item` (if present) lives at `end`.
///
/// `Range::empty` (`item == None`) is the gap tombstone: "we scanned
/// `(start, end]` and found nothing." Preserved across compaction so
/// gap detection remains O(1).
///
/// `fetch` is set while a sibling task is filling this range from the
/// fetcher — peer iterators clone the [`Notify`] and `await` it instead
/// of refetching.
struct Range {
    start: BlockPos,
    end: BlockPos,
    item: Option<LogRef>,
    fetch: Option<Arc<Notify>>,
}

impl Range {
    fn empty(start: BlockPos, end: BlockPos) -> Self {
        debug_assert!(start <= end, "empty range with end < start");
        Self {
            start,
            end,
            item: None,
            fetch: None,
        }
    }

    fn filled(start: BlockPos, end: BlockPos, item: LogRef) -> Self {
        debug_assert!(start <= end, "filled range with end < start");
        debug_assert_eq!(item.pos(), end, "filled item must sit at range end");
        Self {
            start,
            end,
            item: Some(item),
            fetch: None,
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.item.is_none()
    }
}

// ============================================================
// LogPage — input to insert_page
// ============================================================

/// A page of logs covering `(from_block, to_block]` from one
/// `eth_getLogs` call (or a subscription delivery).
///
/// `end_block_hash` lets the consumer record the matching scanned-range
/// entry in [`LogStore::record_scanned_range`].
#[derive(Clone)]
pub struct LogPage {
    pub from_block: u64,
    pub to_block: u64,
    pub items: Vec<Log>,
    pub end_block_hash: [u8; 32],
}

// ============================================================
// LogIndex
// ============================================================

/// Per-filter in-memory range tree on top of [`LogStore`].
///
/// One instance per `(LogStore, filter_hash)` pair. Cheap to construct
/// — actual `scanned_ranges` rows are pulled into the tree on demand by
/// [`Self::ensure_window_loaded`].
pub struct LogIndex {
    store: Arc<LogStore>,
    filter_hash: [u8; 32],
    fetcher: Fetcher,
    /// Aligned scanned-range chunk size (blocks). Determines the
    /// granularity at which gaps get fetched and the alignment of
    /// `scanned_ranges` rows recorded in [`LogStore`].
    chunk_size: u64,
    tree: Mutex<BTreeMap<BlockPos, Range>>,
    /// Block-level windows already materialized into the tree from
    /// `LogStore::get_scanned_ranges`. Tracked so a second visit to the
    /// same window does not re-slurp.
    loaded_windows: Mutex<Vec<(u64, u64)>>,
    /// Optional async drain. When attached, every page passed to
    /// [`Self::insert_page`] is cloned onto the channel and persisted to
    /// [`LogStore`] by a background task. None means the index is purely
    /// in-memory (tests, or the legacy fetcher-driven path which writes
    /// inline in [`Self::finalize_gap_fill`]).
    drain_tx: Mutex<Option<UnboundedSender<LogPage>>>,
}

impl LogIndex {
    /// Construct a fresh, empty index over `store` for `filter_hash`.
    /// Aligned chunk size defaults to `1000` blocks via [`Self::with_chunk_size`].
    pub fn new(store: Arc<LogStore>, filter_hash: [u8; 32], fetcher: Fetcher) -> Self {
        Self::with_chunk_size(store, filter_hash, fetcher, 1000)
    }

    /// Like [`Self::new`] but with an explicit aligned chunk size.
    pub fn with_chunk_size(
        store: Arc<LogStore>,
        filter_hash: [u8; 32],
        fetcher: Fetcher,
        chunk_size: u64,
    ) -> Self {
        assert!(chunk_size > 0, "chunk_size must be positive");
        Self {
            store,
            filter_hash,
            fetcher,
            chunk_size,
            tree: Mutex::new(BTreeMap::new()),
            loaded_windows: Mutex::new(Vec::new()),
            drain_tx: Mutex::new(None),
        }
    }

    /// Spawn a background task that persists every page passed to
    /// [`Self::insert_page`] to [`LogStore`]. Returns `false` (and skips
    /// spawning) if a drain is already attached.
    ///
    /// Items are written via [`LogStore::insert_logs`]; the scanned range
    /// is recorded via [`LogStore::record_scanned_range`]. Failures are
    /// logged but never propagated — the in-memory tree remains the source
    /// of truth on the live-event path.
    pub fn attach_drain(&self) -> bool {
        let mut drain = self.drain_tx.lock().unwrap();
        if drain.is_some() {
            return false;
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<LogPage>();
        let store = self.store.clone();
        let filter_hash = self.filter_hash;
        tokio::spawn(async move {
            while let Some(page) = rx.recv().await {
                if !page.items.is_empty() {
                    if let Err(e) = store.insert_logs(&filter_hash, &page.items) {
                        tracing::warn!(error = %e, "drain insert_logs");
                    }
                }
                if let Err(e) = store.record_scanned_range(
                    &filter_hash,
                    page.from_block,
                    page.to_block,
                    &page.end_block_hash,
                ) {
                    tracing::warn!(error = %e, "drain record_scanned_range");
                }
            }
        });
        *drain = Some(tx);
        true
    }

    /// Whether a drain task is currently attached. Test helper.
    #[cfg(test)]
    pub(crate) fn has_drain(&self) -> bool {
        self.drain_tx.lock().unwrap().is_some()
    }

    /// Filter hash this index serves.
    pub fn filter_hash(&self) -> &[u8; 32] {
        &self.filter_hash
    }

    /// Aligned chunk size in blocks.
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Number of ranges currently in the tree. Test helper.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tree.lock().unwrap().len()
    }

    /// Whether the block window `[from, to]` has been materialized.
    /// Test helper.
    #[cfg(test)]
    pub(crate) fn window_loaded(&self, from: u64, to: u64) -> bool {
        self.loaded_windows
            .lock()
            .unwrap()
            .iter()
            .any(|&(a, b)| a <= from && b >= to)
    }

    // --------------------------------------------------------
    // Page insertion (the reference RangeTree.addPage + addGapItems)
    // --------------------------------------------------------

    /// Install a freshly fetched page into the tree. Items in `page.items`
    /// MUST be in ascending `(block_number, log_index)` order. Items
    /// strictly past the existing tail are appended; items at-or-before
    /// the tail go through the late-item path (the four cases in
    /// [`Self::insert_late_item`]).
    ///
    /// Mirrors the reference `RangeTree.addPage` (history.ts:773–804).
    pub fn insert_page(&self, page: &LogPage) {
        assert_increasing(&page.items);

        let mut tree = self.tree.lock().unwrap();
        let page_start = BlockPos::before_block(page.from_block);
        let page_end = BlockPos::end_of_block(page.to_block);

        // Tail = last range currently in the tree (max by key = max by end).
        let tail_end = tree.keys().next_back().copied();

        let mut new_ranges: Vec<Range> = Vec::new();
        let mut range_start: BlockPos = page_start;
        // Predecessor used for late items that chain together.
        let mut late_prev: BlockPos = page_start;

        for log in &page.items {
            let item = log_ref_from(log).expect("page items must have block_hash");
            let item_pos = item.pos();

            let strictly_past_tail = tail_end.map(|t| item_pos > t).unwrap_or(true);
            let strictly_before_tail = tail_end.map(|t| item_pos < t).unwrap_or(false);
            // Equal-to-tail items go through addPage's "no-op for prev tracking"
            // branch — the reference keeps them out of insertLateItem because
            // a range already ends there.

            if strictly_past_tail {
                new_ranges.push(Range::filled(range_start, item_pos, item));
            } else if strictly_before_tail {
                Self::insert_late_item(&mut tree, item, late_prev);
            }
            range_start = item_pos;
            late_prev = item_pos;
        }

        // Final empty range if there's an unfilled tail past the last new item.
        if page_end > range_start {
            // Only append if the new tail extends past the existing tree tail.
            let beyond_tail = tail_end.map(|t| page_end > t).unwrap_or(true);
            if beyond_tail {
                let start = if let Some(t) = tail_end {
                    if range_start <= t {
                        t
                    } else {
                        range_start
                    }
                } else {
                    range_start
                };
                if page_end > start {
                    new_ranges.push(Range::empty(start, page_end));
                }
            }
        }

        for r in new_ranges {
            tree.insert(r.end, r);
        }
        drop(tree);

        // Async drain to LogStore — see attach_drain. Cloning the page is
        // necessary because `page: &LogPage` is borrowed; the drain task
        // owns its copy. In-memory tree update above is the source of truth
        // for navigation; persistence is best-effort.
        if let Some(tx) = self.drain_tx.lock().unwrap().as_ref() {
            let _ = tx.send(page.clone());
        }
    }

    /// Translate of `RangeTree.insertLateItem` (reference history.ts:828–842).
    ///
    /// Four cases over the range whose end is the next key `>= item.pos`:
    /// 1. None / range.start > item.pos → item sits in a gap →
    ///    insert `(rangeStart, item.pos, item)`.
    /// 2. range.end == item.pos and range filled → duplicate, no-op.
    /// 3. range.end == item.pos and range empty → upgrade to filled.
    /// 4. range.start <= item.pos < range.end → split the range around
    ///    `item.pos`, preserving the original tail.
    ///
    /// A range with a live `fetch` notify is left alone — the in-flight
    /// fetcher will deliver authoritative state.
    fn insert_late_item(tree: &mut BTreeMap<BlockPos, Range>, item: LogRef, range_start: BlockPos) {
        let item_pos = item.pos();
        let existing_key = tree.range(item_pos..).next().map(|(k, _)| *k);
        let existing = existing_key.and_then(|k| tree.get(&k));

        // Case 1: no covering range → new range in the gap.
        let Some(existing) = existing else {
            tree.insert(item_pos, Range::filled(range_start, item_pos, item));
            return;
        };

        if existing.fetch.is_some() {
            return; // Range mid-fetch; the fetcher will deliver.
        }

        let existing_start = existing.start;
        let existing_end = existing.end;
        let existing_filled_item = existing.item;

        if existing_end == item_pos {
            match existing_filled_item {
                Some(_) => {
                    // Case 2: duplicate.
                }
                None => {
                    // Case 3: upgrade empty range with the new item.
                    tree.insert(item_pos, Range::filled(existing_start, existing_end, item));
                }
            }
            return;
        }

        if existing_start <= item_pos {
            // Case 4: split around item.pos.
            tree.remove(&existing_end);
            tree.insert(item_pos, Range::filled(existing_start, item_pos, item));
            // Right half preserves the original tail item.
            let right = if let Some(orig) = existing_filled_item {
                Range::filled(item_pos, existing_end, orig)
            } else {
                Range::empty(item_pos, existing_end)
            };
            tree.insert(existing_end, right);
            return;
        }

        // existing.start > item.pos → item in a gap before existing.
        tree.insert(item_pos, Range::filled(range_start, item_pos, item));
    }

    // --------------------------------------------------------
    // Lazy per-window load
    // --------------------------------------------------------

    /// Materialize any `scanned_ranges` rows from [`LogStore`] that overlap
    /// the requested block window into the in-memory tree, if not already
    /// pulled. Idempotent.
    ///
    /// The window is widened to align with [`Self::chunk_size`] so peer
    /// processes observing the same filter at the same chunk granularity
    /// see the same tree shape.
    pub fn ensure_window_loaded(&self, from_block: u64, to_block: u64) -> Result<(), HistoryError> {
        let (aligned_from, aligned_to) = align_window(from_block, to_block, self.chunk_size);

        {
            let loaded = self.loaded_windows.lock().unwrap();
            if loaded
                .iter()
                .any(|&(a, b)| a <= aligned_from && b >= aligned_to)
            {
                return Ok(());
            }
        }

        let scanned = self
            .store
            .get_scanned_ranges(&self.filter_hash)
            .map_err(|e| HistoryError::internal(format!("get_scanned_ranges: {e}")))?;

        // For each scanned range overlapping the aligned window, materialize
        // its tree entries — one filled Range per cached log, plus an
        // empty tail Range if the scanned region extends past the last log.
        let mut tree = self.tree.lock().unwrap();
        for (s_start, s_end) in scanned {
            if s_end < aligned_from || s_start > aligned_to {
                continue;
            }
            // Skip if any range with key in (Bottom, end_of_block(s_end)] is
            // already present and matches — naive check via the end key.
            let scanned_end_pos = BlockPos::end_of_block(s_end);
            if tree.contains_key(&scanned_end_pos) {
                // Either an empty tail or a filled-at-end-of-block range is
                // already there. Treat as loaded.
                continue;
            }

            let logs = self
                .store
                .get_logs_in_range(&self.filter_hash, s_start, s_end)
                .map_err(|e| HistoryError::internal(format!("get_logs_in_range: {e}")))?;

            let mut prev_pos = BlockPos::before_block(s_start);
            for log in &logs {
                let Some(item) = log_ref_from(log) else {
                    continue;
                };
                let pos = item.pos();
                tree.insert(pos, Range::filled(prev_pos, pos, item));
                prev_pos = pos;
            }
            if scanned_end_pos > prev_pos {
                tree.insert(scanned_end_pos, Range::empty(prev_pos, scanned_end_pos));
            }
        }
        drop(tree);

        self.loaded_windows
            .lock()
            .unwrap()
            .push((aligned_from, aligned_to));
        Ok(())
    }

    // --------------------------------------------------------
    // Cached-only navigation (no fetching)
    // --------------------------------------------------------

    /// First cached log at or after `(block, log_index)`. Pure tree
    /// lookup: never triggers a fetch and never blocks on a peer's fetch.
    /// Empty (tombstone) ranges are skipped. Returns `None` once the
    /// search exits a contiguous cached region (i.e. hits a gap or the
    /// end of the tree).
    pub fn first_at_or_after(
        &self,
        block: u64,
        log_index: u32,
    ) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate(NavDir::ForwardInclusive, block, log_index)
    }

    /// First cached log strictly after `(block, log_index)`.
    pub fn first_after(&self, block: u64, log_index: u32) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate(NavDir::ForwardExclusive, block, log_index)
    }

    /// Last cached log at or before `(block, log_index)`.
    pub fn last_at_or_before(
        &self,
        block: u64,
        log_index: u32,
    ) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate(NavDir::BackwardInclusive, block, log_index)
    }

    /// Last cached log strictly before `(block, log_index)`.
    pub fn last_before(&self, block: u64, log_index: u32) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate(NavDir::BackwardExclusive, block, log_index)
    }

    fn cached_navigate(
        &self,
        dir: NavDir,
        block: u64,
        log_index: u32,
    ) -> Result<Option<Log>, HistoryError> {
        let pivot = BlockPos::At { block, log_index };
        let log_ref_opt = {
            let tree = self.tree.lock().unwrap();
            match dir {
                NavDir::ForwardInclusive | NavDir::ForwardExclusive => {
                    let from = match dir {
                        NavDir::ForwardInclusive => Bound::Included(pivot),
                        NavDir::ForwardExclusive => Bound::Excluded(pivot),
                        _ => unreachable!(),
                    };
                    let mut last_end: Option<BlockPos> = None;
                    let mut found = None;
                    for (&_key, r) in tree.range((from, Bound::Unbounded)) {
                        // Tombstone range — skip but track contiguity.
                        if r.item.is_none() {
                            // Detect a gap between the previous range's end
                            // and this range's start.
                            if let Some(prev_end) = last_end {
                                if r.start > prev_end {
                                    // Gap — cached region ends here.
                                    break;
                                }
                            } else {
                                // First range we examined. Its `start` should
                                // sit below our pivot for the scan to be
                                // contiguous from the pivot onward.
                                if r.start > pivot {
                                    break;
                                }
                            }
                            last_end = Some(r.end);
                            continue;
                        }
                        // Filled range. Item sits at r.end.
                        // The first filled range whose end satisfies our
                        // direction is the answer.
                        let satisfies = match dir {
                            NavDir::ForwardInclusive => r.end >= pivot,
                            NavDir::ForwardExclusive => r.end > pivot,
                            _ => unreachable!(),
                        };
                        if !satisfies {
                            last_end = Some(r.end);
                            continue;
                        }
                        // Contiguity check: the range we're about to yield
                        // must abut the previous cached region or contain
                        // our pivot.
                        if let Some(prev_end) = last_end {
                            if r.start > prev_end {
                                break;
                            }
                        } else if r.start > pivot {
                            break;
                        }
                        found = Some(r.item.expect("filled"));
                        break;
                    }
                    let _ = last_end;
                    found
                }
                NavDir::BackwardInclusive | NavDir::BackwardExclusive => {
                    let upper = match dir {
                        NavDir::BackwardInclusive => Bound::Included(pivot),
                        NavDir::BackwardExclusive => Bound::Excluded(pivot),
                        _ => unreachable!(),
                    };
                    let mut last_start: Option<BlockPos> = None;
                    let mut found = None;
                    for (&_key, r) in tree.range((Bound::Unbounded, upper)).rev() {
                        if r.item.is_none() {
                            if let Some(prev_start) = last_start {
                                if r.end < prev_start {
                                    break;
                                }
                            } else if r.end < pivot {
                                // Cached region doesn't reach our pivot.
                                break;
                            }
                            last_start = Some(r.start);
                            continue;
                        }
                        let satisfies = match dir {
                            NavDir::BackwardInclusive => r.end <= pivot,
                            NavDir::BackwardExclusive => r.end < pivot,
                            _ => unreachable!(),
                        };
                        if !satisfies {
                            last_start = Some(r.start);
                            continue;
                        }
                        if let Some(prev_start) = last_start {
                            if r.end < prev_start {
                                break;
                            }
                        }
                        found = Some(r.item.expect("filled"));
                        break;
                    }
                    found
                }
            }
        };
        match log_ref_opt {
            Some(lr) => self.resolve(&lr).map(Some),
            None => Ok(None),
        }
    }

    /// Resolve a [`LogRef`] to a full [`Log`] via [`LogStore`]. Returns an
    /// internal error if the cached block has since been marked reorged —
    /// consumers detect that condition by comparing `EvmEvent::block_hash`
    /// against what the store currently reports (see module docs on
    /// consumer-owned reorg recovery).
    fn resolve(&self, lr: &LogRef) -> Result<Log, HistoryError> {
        match self
            .store
            .get_log_by_ref(&self.filter_hash, &lr.block_hash, lr.log_index)
            .map_err(|e| HistoryError::internal(format!("get_log_by_ref: {e}")))?
        {
            Some(log) => Ok(log),
            None => Err(HistoryError::internal(format!(
                "log_ref points to reorged block {:?} (log_index {})",
                lr.block_hash, lr.log_index
            ))),
        }
    }

    // --------------------------------------------------------
    // ensure_cached + singleflight
    // --------------------------------------------------------

    /// Fill any gaps in `[from_block, to_block]` by calling the fetcher.
    /// Concurrent callers share work via per-range [`Notify`] sentinels —
    /// the first arrival at a gap installs the notify, fetches, drops the
    /// notify, and notifies waiters; later arrivals see the notify, wait
    /// on it, and re-walk.
    pub async fn ensure_cached(&self, from_block: u64, to_block: u64) -> Result<(), HistoryError> {
        self.ensure_window_loaded(from_block, to_block)?;

        let from_pos = BlockPos::before_block(from_block);
        let to_pos = BlockPos::end_of_block(to_block);
        loop {
            let action = self.find_next_gap_or_wait(from_pos, to_pos);
            match action {
                NextStep::Done => return Ok(()),
                NextStep::Wait(notify) => {
                    notify.notified().await;
                }
                NextStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    // We hold the in-tree fetch notify; fetch outside the lock.
                    let (block_from, block_to) = gap_block_range(start_pos, end_pos);
                    let fetch_result = (self.fetcher)(block_from, block_to).await;
                    // Always remove the placeholder + notify on completion.
                    self.finalize_gap_fill(start_pos, end_pos, fetch_result.as_ref().ok())?;
                    notify.notify_waiters();
                    fetch_result?;
                }
            }
        }
    }

    /// Walk the tree (under lock) from `from` toward `to` looking for the
    /// next gap, in-flight fetch, or completion. Returns an action without
    /// holding any locks across an await.
    fn find_next_gap_or_wait(&self, from: BlockPos, to: BlockPos) -> NextStep {
        if from >= to {
            return NextStep::Done;
        }
        let mut tree = self.tree.lock().unwrap();
        let mut cursor = from;
        loop {
            let next = tree
                .range((Bound::Excluded(cursor), Bound::Unbounded))
                .next()
                .map(|(_, r)| (r.start, r.end, r.fetch.clone()));
            match next {
                None => {
                    // No range past cursor — trailing gap (cursor, to].
                    let notify = Arc::new(Notify::new());
                    let placeholder = Range {
                        start: cursor,
                        end: to,
                        item: None,
                        fetch: Some(notify.clone()),
                    };
                    tree.insert(to, placeholder);
                    return NextStep::FillGap {
                        start_pos: cursor,
                        end_pos: to,
                        notify,
                    };
                }
                Some((start, _end, Some(notify))) => {
                    if start > cursor {
                        // Gap before the in-flight range — fill the gap (we win the race).
                        let gap_end = if start < to { start } else { to };
                        let our_notify = Arc::new(Notify::new());
                        let placeholder = Range {
                            start: cursor,
                            end: gap_end,
                            item: None,
                            fetch: Some(our_notify.clone()),
                        };
                        tree.insert(gap_end, placeholder);
                        return NextStep::FillGap {
                            start_pos: cursor,
                            end_pos: gap_end,
                            notify: our_notify,
                        };
                    }
                    // We're inside an in-flight range — wait for it. After
                    // waking we'll re-walk from `cursor`, so the range's end
                    // doesn't need to be threaded out of the closure.
                    return NextStep::Wait(notify);
                }
                Some((start, end, None)) => {
                    if start > cursor {
                        // Gap before this range.
                        let gap_end = if start < to { start } else { to };
                        let our_notify = Arc::new(Notify::new());
                        let placeholder = Range {
                            start: cursor,
                            end: gap_end,
                            item: None,
                            fetch: Some(our_notify.clone()),
                        };
                        tree.insert(gap_end, placeholder);
                        return NextStep::FillGap {
                            start_pos: cursor,
                            end_pos: gap_end,
                            notify: our_notify,
                        };
                    }
                    // No gap; range is fully cached. Advance and continue.
                    if end >= to {
                        return NextStep::Done;
                    }
                    cursor = end;
                }
            }
        }
    }

    /// Remove the placeholder fetch range and insert any fetched logs as
    /// proper Range entries. Called after the fetcher returns (success or
    /// failure). On failure we just remove the placeholder so a subsequent
    /// caller retries.
    fn finalize_gap_fill(
        &self,
        start_pos: BlockPos,
        end_pos: BlockPos,
        page: Option<&LogPage>,
    ) -> Result<(), HistoryError> {
        let mut tree = self.tree.lock().unwrap();
        // Drop placeholder.
        tree.remove(&end_pos);
        let Some(page) = page else { return Ok(()) };
        // Translate page items into Ranges chained from start_pos to end_pos.
        let mut prev = start_pos;
        for log in &page.items {
            let Some(item) = log_ref_from(log) else {
                continue;
            };
            let pos = item.pos();
            if pos <= prev || pos > end_pos {
                continue;
            }
            tree.insert(pos, Range::filled(prev, pos, item));
            prev = pos;
        }
        if end_pos > prev {
            tree.insert(end_pos, Range::empty(prev, end_pos));
        }
        // Persist to LogStore so the next process sees the same shape.
        drop(tree);
        if !page.items.is_empty() {
            self.store
                .insert_logs(&self.filter_hash, &page.items)
                .map_err(|e| HistoryError::internal(format!("insert_logs: {e}")))?;
        }
        self.store
            .record_scanned_range(
                &self.filter_hash,
                page.from_block,
                page.to_block,
                &page.end_block_hash,
            )
            .map_err(|e| HistoryError::internal(format!("record_scanned_range: {e}")))?;
        Ok(())
    }

    // --------------------------------------------------------
    // items() async stream
    // --------------------------------------------------------

    /// Yield every cached and fetched-on-demand log whose position is
    /// in `(from, to]`, in ascending order. Closes when `to` is reached.
    ///
    /// Mirrors the reference `RangeTree.items` (history.ts:872–915): gaps in
    /// the tree trigger a fetcher call on the consuming task; in-flight
    /// fetches block on the notify. Cancellation is by dropping the
    /// stream (which drops the Arc<Self> capture).
    pub fn items(
        self: Arc<Self>,
        from: BlockPos,
        to: BlockPos,
    ) -> Pin<Box<dyn Stream<Item = Result<Log, HistoryError>> + Send>> {
        use futures_util::stream::unfold;
        let state = ItemsState {
            index: self,
            cursor: from,
            to,
            done: false,
        };
        Box::pin(unfold(state, |mut state| async move {
            if state.done || state.cursor >= state.to {
                return None;
            }
            match state.index.clone().next_yield(state.cursor, state.to).await {
                Ok(Some((log, advanced))) => {
                    state.cursor = advanced;
                    Some((Ok(log), state))
                }
                Ok(None) => None,
                Err(e) => {
                    state.done = true;
                    Some((Err(e), state))
                }
            }
        }))
    }

    /// Internal helper for [`Self::items`] — walks the tree until it can
    /// yield exactly one item or determines there's nothing left in
    /// `(from, to]`. Same dispatch as [`Self::find_next_gap_or_wait`] but
    /// also yields filled items.
    async fn next_yield(
        self: Arc<Self>,
        mut from: BlockPos,
        to: BlockPos,
    ) -> Result<Option<(Log, BlockPos)>, HistoryError> {
        loop {
            if from >= to {
                return Ok(None);
            }
            let snap = self.snapshot_walk(from, to);
            match snap {
                WalkStep::Yield(item, next_from) => {
                    let log = self.resolve(&item)?;
                    return Ok(Some((log, next_from)));
                }
                WalkStep::Skip(next_from) => {
                    from = next_from;
                }
                WalkStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    let (block_from, block_to) = gap_block_range(start_pos, end_pos);
                    let result = (self.fetcher)(block_from, block_to).await;
                    self.finalize_gap_fill(start_pos, end_pos, result.as_ref().ok())?;
                    notify.notify_waiters();
                    result?;
                    // Tree changed; restart from current cursor.
                }
                WalkStep::Wait(notify) => {
                    notify.notified().await;
                }
                WalkStep::Done => return Ok(None),
            }
        }
    }

    /// One step of the items() walk performed entirely under the tree
    /// mutex. Side effect: installs a fetch placeholder + notify when
    /// returning [`WalkStep::FillGap`].
    fn snapshot_walk(&self, from: BlockPos, to: BlockPos) -> WalkStep {
        let mut tree = self.tree.lock().unwrap();
        let next = tree
            .range((Bound::Excluded(from), Bound::Unbounded))
            .next()
            .map(|(_, r)| (r.start, r.end, r.fetch.clone(), r.item));
        let Some((start, end, fetch_opt, item_opt)) = next else {
            // No range past cursor — trailing gap.
            if to <= from {
                return WalkStep::Done;
            }
            let notify = Arc::new(Notify::new());
            tree.insert(
                to,
                Range {
                    start: from,
                    end: to,
                    item: None,
                    fetch: Some(notify.clone()),
                },
            );
            return WalkStep::FillGap {
                start_pos: from,
                end_pos: to,
                notify,
            };
        };
        if let Some(notify) = fetch_opt {
            if start > from {
                // Gap to fill before the in-flight range.
                let gap_end = if start < to { start } else { to };
                let our_notify = Arc::new(Notify::new());
                tree.insert(
                    gap_end,
                    Range {
                        start: from,
                        end: gap_end,
                        item: None,
                        fetch: Some(our_notify.clone()),
                    },
                );
                return WalkStep::FillGap {
                    start_pos: from,
                    end_pos: gap_end,
                    notify: our_notify,
                };
            }
            return WalkStep::Wait(notify);
        }
        if start > from {
            // Gap.
            let gap_end = if start < to { start } else { to };
            let our_notify = Arc::new(Notify::new());
            tree.insert(
                gap_end,
                Range {
                    start: from,
                    end: gap_end,
                    item: None,
                    fetch: Some(our_notify.clone()),
                },
            );
            return WalkStep::FillGap {
                start_pos: from,
                end_pos: gap_end,
                notify: our_notify,
            };
        }
        // start <= from. Use this range.
        if end > to {
            // Range extends past our cap. If item lies past `to`, done.
            // Filled at end > to means item is outside our window.
            return WalkStep::Done;
        }
        match item_opt {
            Some(item) => WalkStep::Yield(item, end),
            None => WalkStep::Skip(end),
        }
    }
}

// ============================================================
// Private supporting types
// ============================================================

#[derive(Copy, Clone)]
enum NavDir {
    ForwardInclusive,
    ForwardExclusive,
    BackwardInclusive,
    BackwardExclusive,
}

enum NextStep {
    Done,
    FillGap {
        start_pos: BlockPos,
        end_pos: BlockPos,
        notify: Arc<Notify>,
    },
    Wait(Arc<Notify>),
}

enum WalkStep {
    Yield(LogRef, BlockPos),
    Skip(BlockPos),
    FillGap {
        start_pos: BlockPos,
        end_pos: BlockPos,
        notify: Arc<Notify>,
    },
    Wait(Arc<Notify>),
    Done,
}

struct ItemsState {
    index: Arc<LogIndex>,
    cursor: BlockPos,
    to: BlockPos,
    done: bool,
}

/// Convert a position-level gap `(start, end]` to inclusive block-level
/// bounds `(from_block, to_block)` suitable for the fetcher. If `start`
/// is at the end of a block, advance past that block. If `start` is
/// `Bottom`, start from block 0.
fn gap_block_range(start: BlockPos, end: BlockPos) -> (u64, u64) {
    let from_block = match start {
        BlockPos::Bottom => 0,
        BlockPos::At { block, log_index } => {
            if log_index == u32::MAX {
                block.saturating_add(1)
            } else {
                block
            }
        }
    };
    let to_block = match end {
        BlockPos::Bottom => 0,
        BlockPos::At { block, .. } => block,
    };
    (from_block, to_block)
}

// ============================================================
// Free helpers
// ============================================================

/// Build a [`LogRef`] from an alloy [`Log`]. Returns `None` for logs
/// missing a block hash (pending / mempool logs are not cacheable and
/// MUST NOT enter the tree).
fn log_ref_from(log: &Log) -> Option<LogRef> {
    let block_hash = log.block_hash?;
    Some(LogRef {
        block_number: log.block_number?,
        log_index: log.log_index? as u32,
        block_hash: block_hash.0,
    })
}

/// Align `[from, to]` outward to a multiple of `chunk` so peer processes
/// scanning the same filter at the same chunk size hit identical
/// scanned-range boundaries.
fn align_window(from: u64, to: u64, chunk: u64) -> (u64, u64) {
    let aligned_from = (from / chunk) * chunk;
    let aligned_to = (to / chunk + 1) * chunk - 1;
    (aligned_from, aligned_to)
}

fn assert_increasing(logs: &[Log]) {
    let mut prev: Option<(u64, u64)> = None;
    for log in logs {
        let cur = (log.block_number.unwrap_or(0), log.log_index.unwrap_or(0));
        if let Some(p) = prev {
            assert!(
                cur > p,
                "LogPage items must be in ascending (block, log_index) order"
            );
        }
        prev = Some(cur);
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, Bytes, LogData, B256};

    const FH: [u8; 32] = [0xAA; 32];
    const HASH_A: [u8; 32] = [0xBB; 32];
    const HASH_B: [u8; 32] = [0xBC; 32];

    fn mk_log(block: u64, idx: u64, block_hash: [u8; 32]) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01]);
        let log_data = LogData::new_unchecked(vec![topic0], data);
        Log {
            inner: alloy::primitives::Log {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(B256::from(block_hash)),
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(idx),
            removed: false,
        }
    }

    fn mk_index() -> LogIndex {
        LogIndex::new(
            Arc::new(LogStore::open_in_memory().unwrap()),
            FH,
            never_fetch(),
        )
    }

    // -- BlockPos ordering --

    #[test]
    fn block_pos_bottom_below_everything() {
        assert!(BlockPos::Bottom < BlockPos::at(0, 0));
        assert!(BlockPos::Bottom < BlockPos::end_of_block(0));
        assert!(BlockPos::before_block(0) == BlockPos::Bottom);
        assert!(
            BlockPos::before_block(1)
                == BlockPos::At {
                    block: 0,
                    log_index: u32::MAX,
                }
        );
    }

    // -- insert_page basic shapes --

    #[test]
    fn insert_range_in_gap_creates_three_entries() {
        let idx = mk_index();
        // First page: blocks [100, 200] with two logs at (150, 0) and (180, 0).
        let page = LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 0, HASH_B)],
            end_block_hash: HASH_B,
        };
        idx.insert_page(&page);
        // Expect: filled@150, filled@180, empty tail @ end_of_block(200) = 3 entries.
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn insert_range_at_existing_end_extends_not_duplicates() {
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before = idx.len();
        // Second page covering [200, 300] with one log at (250, 0).
        idx.insert_page(&LogPage {
            from_block: 201,
            to_block: 300,
            items: vec![mk_log(250, 0, HASH_B)],
            end_block_hash: HASH_B,
        });
        // Expect new filled@250 + new empty-tail past 250 = 2 more entries.
        assert_eq!(idx.len(), before + 2);
    }

    // -- late_item insertion (4 cases) --

    #[test]
    fn late_item_inside_existing_range_splits_it() {
        let idx = mk_index();
        // Set up: page with single log at (200, 0) covering [100, 300].
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 300,
            items: vec![mk_log(200, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        // Now a late item arrives at (250, 0) — inside the tail empty
        // range (200, 300]. Should split into filled@250 + empty(250, 300].
        let mut tree = idx.tree.lock().unwrap();
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 250,
                log_index: 0,
                block_hash: HASH_B,
            },
            BlockPos::at(200, 0),
        );
        // before_len had: filled@(200, 0), empty(end_of_block(300)). After split:
        // filled@(200, 0), filled@(250, 0), empty(end_of_block(300)) — net +1.
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(250, 0)));
        assert!(tree.contains_key(&BlockPos::end_of_block(300)));
    }

    #[test]
    fn late_item_at_empty_range_end_upgrades_to_filled() {
        let idx = mk_index();
        // Page with no items so tree gets a single empty range ending at
        // end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![],
            end_block_hash: HASH_A,
        });
        assert_eq!(idx.len(), 1);
        let target_pos = BlockPos::end_of_block(200);
        {
            let tree = idx.tree.lock().unwrap();
            assert!(tree.get(&target_pos).unwrap().is_empty());
        }
        let mut tree = idx.tree.lock().unwrap();
        // Late item lands exactly at the empty range's end position.
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 200,
                log_index: u32::MAX,
                block_hash: HASH_B,
            },
            BlockPos::at(150, 0),
        );
        // Count unchanged; the empty was upgraded in-place.
        assert_eq!(tree.len(), 1);
        let r = tree.get(&target_pos).unwrap();
        assert!(!r.is_empty());
    }

    #[test]
    fn late_item_duplicate_is_ignored() {
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        // Same (block, log_index) as existing filled range end.
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 150,
                log_index: 0,
                block_hash: HASH_B, // different hash — still ignored
            },
            BlockPos::at(100, 0),
        );
        assert_eq!(tree.len(), before_len);
    }

    #[test]
    fn late_item_in_gap_inserts_filled_range() {
        let idx = mk_index();
        // First page [100, 200] with one log — gives filled@(150, 0)
        // and empty-tail at end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        // Insert a late item at (90, 0) — in a true gap before everything.
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 90,
                log_index: 0,
                block_hash: HASH_B,
            },
            BlockPos::before_block(90),
        );
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(90, 0)));
    }

    // -- lazy per-window load --

    #[test]
    fn is_loaded_lazy_first_access_only() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        // Pre-populate the store as if a previous process had scanned [100, 200]
        // with one log at (150, 0).
        store.insert_logs(&FH, &[mk_log(150, 0, HASH_A)]).unwrap();
        store.record_scanned_range(&FH, 100, 200, &HASH_A).unwrap();

        let idx = LogIndex::with_chunk_size(store, FH, never_fetch(), 1000);
        assert_eq!(idx.len(), 0, "tree starts empty");

        idx.ensure_window_loaded(100, 200).unwrap();
        assert!(idx.window_loaded(100, 200));
        assert!(
            idx.len() >= 2,
            "expected at least filled@150 + empty-tail entries"
        );

        let count_after_first_load = idx.len();
        idx.ensure_window_loaded(100, 200).unwrap();
        assert_eq!(
            idx.len(),
            count_after_first_load,
            "second ensure_window_loaded must be a no-op"
        );
    }

    #[test]
    fn align_window_rounds_outward() {
        assert_eq!(align_window(100, 200, 1000), (0, 999));
        assert_eq!(align_window(1500, 2500, 1000), (1000, 2999));
        assert_eq!(align_window(0, 0, 1000), (0, 999));
    }

    // -- prune tombstone forward-compat (Phase 7) --

    #[test]
    fn prune_keeps_empty_range_tombstone() {
        // Forward-compat smoke: an empty range survives a "drop the LogRef"
        // operation (which Phase 7 will apply when pruning items). Today we
        // model this with insert_page over a no-log scan.
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![],
            end_block_hash: HASH_A,
        });
        // The empty range IS the tombstone — gap detection downstream
        // depends on its presence.
        assert_eq!(idx.len(), 1);
        let tree = idx.tree.lock().unwrap();
        let r = tree.get(&BlockPos::end_of_block(200)).unwrap();
        assert!(r.is_empty(), "empty range survives as a tombstone");
        assert_eq!(r.start, BlockPos::before_block(100));
        assert_eq!(r.end, BlockPos::end_of_block(200));
    }

    // -- navigation (cached-only) --

    #[tokio::test]
    async fn first_at_or_after_finds_next_cached_log() {
        let idx = Arc::new(mk_index());
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        });
        // We need the logs to exist in LogStore for resolve() to find them.
        idx.store
            .insert_logs(&FH, &[mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)])
            .unwrap();

        let log = idx.first_at_or_after(140, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));

        let log = idx.first_at_or_after(150, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));

        let log = idx.first_at_or_after(150, 1).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        // After the last cached item: empty-tail range is contiguous up to
        // block 200, but there are no items past 180; we return None.
        let none = idx.first_at_or_after(201, 0).unwrap();
        assert!(none.is_none(), "past cached region returns None");
    }

    #[tokio::test]
    async fn last_at_or_before_finds_prev_cached_log() {
        let idx = Arc::new(mk_index());
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        });
        idx.store
            .insert_logs(&FH, &[mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)])
            .unwrap();

        let log = idx.last_at_or_before(200, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        let log = idx.last_at_or_before(180, 2).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        let log = idx.last_before(180, 2).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));
    }

    // -- items() with fetcher --

    #[tokio::test]
    async fn items_walks_filled_ranges_yields_in_order() {
        use futures_util::StreamExt;
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let logs = vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)];
        store.insert_logs(&FH, &logs).unwrap();
        let idx = Arc::new(LogIndex::new(store, FH, never_fetch()));
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: logs,
            end_block_hash: HASH_B,
        });
        let mut stream = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let a = stream.next().await.unwrap().unwrap();
        assert_eq!(a.block_number, Some(150));
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(b.block_number, Some(180));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn items_into_uncached_gap_calls_store_and_caches() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log(150, 0, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(LogIndex::new(store.clone(), FH, fetcher));

        // First walk fetches.
        let mut stream = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.block_number, Some(150));
        drop(stream);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second walk over the same range should NOT refetch — the gap has
        // been backfilled both in the in-memory tree and in LogStore.
        let mut stream2 = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let first2 = stream2.next().await.unwrap().unwrap();
        assert_eq!(first2.block_number, Some(150));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no refetch on warm tree");

        // And the persisted scanned_ranges row is there.
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert!(!ranges.is_empty(), "scanned range was persisted");
    }

    // -- singleflight --

    #[tokio::test]
    async fn concurrent_searches_in_same_gap_fire_one_fetch() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    // Give peers time to enter the wait state.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log(150, 0, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(LogIndex::new(store, FH, fetcher));

        // Spawn 5 concurrent walkers over the same uncached gap.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let idx = idx.clone();
            handles.push(tokio::spawn(async move {
                let mut s = idx.items(BlockPos::before_block(100), BlockPos::end_of_block(200));
                s.next().await.map(|r| r.map(|log| log.block_number))
            }));
        }
        for h in handles {
            let result = h.await.unwrap();
            let block = result.unwrap().unwrap();
            assert_eq!(block, Some(150));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly one fetch served 5 concurrent walkers"
        );
    }

    // -- attach_drain: async sqlite persistence --

    #[tokio::test]
    async fn insert_page_drains_to_store_asynchronously() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let idx = LogIndex::new(store.clone(), FH, never_fetch());
        assert!(idx.attach_drain(), "first attach succeeds");
        assert!(!idx.attach_drain(), "second attach is a no-op");

        let page = LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        };

        // The synchronous tree update must complete before the drain runs.
        // Right after insert_page returns, the tree has the entries but the
        // store has not been written yet (drain task hasn't been polled).
        idx.insert_page(&page);
        assert_eq!(idx.len(), 3, "tree updated synchronously");

        // Yield so the drain task can run.
        for _ in 0..10 {
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let logs = store.get_logs_in_range(&FH, 100, 200).unwrap();
            if logs.len() == 2 {
                break;
            }
        }

        let logs = store.get_logs_in_range(&FH, 100, 200).unwrap();
        assert_eq!(logs.len(), 2, "drain wrote both logs to the store");
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200)], "drain recorded the scanned range");
    }

    #[tokio::test]
    async fn insert_page_without_drain_does_not_touch_store() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let idx = LogIndex::new(store.clone(), FH, never_fetch());
        assert!(!idx.has_drain());

        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });

        // Give any background work a chance to run.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            store.get_logs_in_range(&FH, 100, 200).unwrap().len(),
            0,
            "no drain attached — store remains empty"
        );
        assert!(
            store.get_scanned_ranges(&FH).unwrap().is_empty(),
            "no drain attached — scanned ranges remain empty"
        );
    }
}
