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
//! Scope of this commit (Phase 1b scaffold): types, page insertion (the
//! four late-item cases), and lazy-load of scanned ranges. The
//! `items()` async generator, `first_*` / `last_*` navigation methods,
//! and the fetcher-driven singleflight follow in a subsequent commit
//! before the Phase 1b review checkpoint.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use alloy::rpc::types::Log;
use tokio::sync::Notify;

use super::log_store::LogStore;
use crate::arch::HistoryError;

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
    /// Aligned scanned-range chunk size (blocks). Determines the
    /// granularity at which gaps get fetched and the alignment of
    /// `scanned_ranges` rows recorded in [`LogStore`].
    chunk_size: u64,
    tree: Mutex<BTreeMap<BlockPos, Range>>,
    /// Block-level windows already materialized into the tree from
    /// `LogStore::get_scanned_ranges`. Tracked so a second visit to the
    /// same window does not re-slurp.
    loaded_windows: Mutex<Vec<(u64, u64)>>,
}

impl LogIndex {
    /// Construct a fresh, empty index over `store` for `filter_hash`.
    /// Aligned chunk size defaults to `1000` blocks via [`Self::with_chunk_size`].
    pub fn new(store: Arc<LogStore>, filter_hash: [u8; 32]) -> Self {
        Self::with_chunk_size(store, filter_hash, 1000)
    }

    /// Like [`Self::new`] but with an explicit aligned chunk size.
    pub fn with_chunk_size(
        store: Arc<LogStore>,
        filter_hash: [u8; 32],
        chunk_size: u64,
    ) -> Self {
        assert!(chunk_size > 0, "chunk_size must be positive");
        Self {
            store,
            filter_hash,
            chunk_size,
            tree: Mutex::new(BTreeMap::new()),
            loaded_windows: Mutex::new(Vec::new()),
        }
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

            let strictly_past_tail =
                tail_end.map(|t| item_pos > t).unwrap_or(true);
            let strictly_before_tail =
                tail_end.map(|t| item_pos < t).unwrap_or(false);
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
    fn insert_late_item(
        tree: &mut BTreeMap<BlockPos, Range>,
        item: LogRef,
        range_start: BlockPos,
    ) {
        let item_pos = item.pos();
        let existing_key = tree
            .range(item_pos..)
            .next()
            .map(|(k, _)| *k);
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
            tree.insert(
                item_pos,
                Range::filled(existing_start, item_pos, item),
            );
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
        LogIndex::new(Arc::new(LogStore::open_in_memory().unwrap()), FH)
    }

    // -- BlockPos ordering --

    #[test]
    fn block_pos_bottom_below_everything() {
        assert!(BlockPos::Bottom < BlockPos::at(0, 0));
        assert!(BlockPos::Bottom < BlockPos::end_of_block(0));
        assert!(BlockPos::before_block(0) == BlockPos::Bottom);
        assert!(BlockPos::before_block(1) == BlockPos::At {
            block: 0,
            log_index: u32::MAX,
        });
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
        store
            .record_scanned_range(&FH, 100, 200, &HASH_A)
            .unwrap();

        let idx = LogIndex::with_chunk_size(store, FH, 1000);
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
}
