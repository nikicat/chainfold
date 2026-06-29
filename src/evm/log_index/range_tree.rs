//! The [`Range`] tree-entry type and the operations that mutate the tree:
//! page insertion (with the four late-item cases), the lazy per-window load
//! of scanned ranges from `LogCache`, and the gap placeholder helper.

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy::rpc::types::Log;
use tokio::sync::Notify;

use super::LogIndex;
use super::block_pos::{BlockPos, LogPage, LogRef, log_ref_from};
use crate::arch::HistoryError;
use crate::evm::types::BlockRange;

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
pub(super) struct Range {
    pub(super) start: BlockPos,
    pub(super) end: BlockPos,
    pub(super) item: Option<LogRef>,
    pub(super) fetch: Option<Arc<Notify>>,
}

impl Range {
    pub(super) fn empty(start: BlockPos, end: BlockPos) -> Self {
        debug_assert!(start <= end, "empty range with end < start");
        Self {
            start,
            end,
            item: None,
            fetch: None,
        }
    }

    pub(super) fn filled(start: BlockPos, end: BlockPos, item: LogRef) -> Self {
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
    pub(super) fn is_empty(&self) -> bool {
        self.item.is_none()
    }
}

impl LogIndex {
    // --------------------------------------------------------
    // Page insertion (addPage + addGapItems)
    // --------------------------------------------------------

    /// Install a freshly fetched page into the tree. Items in `page.items`
    /// MUST be in ascending `(block_number, log_index)` order. Items
    /// strictly past the existing tail are appended; items at-or-before
    /// the tail go through the late-item path (the four cases in
    /// [`Self::insert_late_item`]).
    pub fn insert_page(&self, page: &LogPage) {
        assert_increasing(&page.items);

        let mut tree = self.tree.lock().unwrap();
        let page_start = BlockPos::before_block(page.from_block.get());
        let page_end = BlockPos::end_of_block(page.to_block.get());

        // Tail = last range currently in the tree (max by key = max by end).
        let tail_end = tree.keys().next_back().copied();

        let mut new_ranges: Vec<Range> = Vec::new();
        let mut range_start: BlockPos = page_start;
        // Predecessor used for late items that chain together.
        let mut late_prev: BlockPos = page_start;

        for log in &page.items {
            let item = log_ref_from(log).expect("page items must have block_hash");
            let item_pos = item.pos();

            let strictly_past_tail = tail_end.is_none_or(|t| item_pos > t);
            let strictly_before_tail = tail_end.is_some_and(|t| item_pos < t);
            // Equal-to-tail items take the no-op branch — they aren't
            // routed through insert_late_item because a range already ends
            // at that position.

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
            let beyond_tail = tail_end.is_none_or(|t| page_end > t);
            if beyond_tail {
                let start = if let Some(t) = tail_end {
                    if range_start <= t { t } else { range_start }
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

        // Async drain to LogCache — see attach_drain. Cloning the page is
        // necessary because `page: &LogPage` is borrowed; the drain task
        // owns its copy. In-memory tree update above is the source of truth
        // for navigation; persistence is best-effort.
        if let Some(tx) = self.drain_tx.lock().unwrap().as_ref() {
            let _ = tx.send(page.clone());
        }
    }

    /// Insert an item that arrived strictly before the current tail.
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
    pub(super) fn insert_late_item(
        tree: &mut BTreeMap<BlockPos, Range>,
        item: LogRef,
        range_start: BlockPos,
    ) {
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

    /// Materialize any `scanned_ranges` rows from `LogCache` that overlap
    /// the requested block window into the in-memory tree, if not already
    /// pulled. Idempotent.
    ///
    /// The window is widened to align with [`Self::chunk_size`] so peer
    /// processes observing the same filter at the same chunk granularity
    /// see the same tree shape.
    pub fn ensure_window_loaded(&self, from_block: u64, to_block: u64) -> Result<(), HistoryError> {
        let window = align_window(from_block, to_block, self.chunk_size);
        let (aligned_from, aligned_to) = window.as_u64();

        {
            let loaded = self.loaded_windows.lock().unwrap();
            if loaded
                .iter()
                .any(|w| w.from <= window.from && w.to >= window.to)
            {
                return Ok(());
            }
        }

        // Scanned ranges come straight off SQLite as raw `(u64, u64)` rows.
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

        self.loaded_windows.lock().unwrap().push(window);
        Ok(())
    }
}

/// Install an in-flight fetch placeholder at `end` covering `(start, end]`
/// and return its [`Notify`]. Caller fills `(start, end]` via the fetcher
/// and replaces the placeholder in `finalize_gap_fill`.
pub(super) fn install_gap_placeholder(
    tree: &mut BTreeMap<BlockPos, Range>,
    start: BlockPos,
    end: BlockPos,
) -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    tree.insert(
        end,
        Range {
            start,
            end,
            item: None,
            fetch: Some(notify.clone()),
        },
    );
    notify
}

/// Align `[from, to]` outward to a multiple of `chunk` so peer processes
/// scanning the same filter at the same chunk size hit identical
/// scanned-range boundaries.
pub(super) fn align_window(from: u64, to: u64, chunk: u64) -> BlockRange {
    let aligned_from = (from / chunk) * chunk;
    let aligned_to = (to / chunk + 1) * chunk - 1;
    BlockRange::from_u64(aligned_from, aligned_to)
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
