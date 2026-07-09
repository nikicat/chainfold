//! The [`Range`] tree-entry type and the operations that mutate the tree:
//! page insertion (with the four late-item cases), the lazy per-window load
//! of scanned ranges from the durable store, and the gap placeholder helper.
//!
//! All generic over a [`RangeSpec`](super::spec::RangeSpec) `S`.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Notify;

use super::RangeIndex;
use super::spec::{IndexPos, LoadedSpan, RangeSpec};
use crate::arch::HistoryError;
use crate::chain::{BlockRange, ChunkSize};

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
pub(super) struct Range<S: RangeSpec> {
    pub(super) start: S::Pos,
    pub(super) end: S::Pos,
    pub(super) item: Option<S::Item>,
    pub(super) fetch: Option<Arc<Notify>>,
}

impl<S: RangeSpec> Range<S> {
    pub(super) fn empty(start: S::Pos, end: S::Pos) -> Self {
        debug_assert!(start <= end, "empty range with end < start");
        Self {
            start,
            end,
            item: None,
            fetch: None,
        }
    }

    pub(super) fn filled(start: S::Pos, end: S::Pos, item: S::Item) -> Self {
        debug_assert!(start <= end, "filled range with end < start");
        debug_assert_eq!(S::item_pos(&item), end, "filled item must sit at range end");
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

impl<S: RangeSpec> RangeIndex<S> {
    // --------------------------------------------------------
    // Page insertion (addPage + addGapItems)
    // --------------------------------------------------------

    /// Install a freshly fetched page into the tree. Items the page carries
    /// (via [`RangeSpec::page_items`]) MUST be in ascending position order.
    /// Items strictly past the existing tail are appended; items at-or-before
    /// the tail go through the late-item path (the four cases in
    /// [`Self::insert_late_item`]).
    pub fn insert_page(&self, page: &S::Page) {
        let items = S::page_items(page);
        assert_increasing::<S>(&items);

        let mut tree = self.tree.lock().unwrap();
        let page_bounds = S::page_bounds(page);
        let page_start = S::Pos::before_block(page_bounds.from);
        let page_end = S::Pos::end_of_block(page_bounds.to);

        // Tail = last range currently in the tree (max by key = max by end).
        let tail_end = tree.keys().next_back().copied();

        let mut new_ranges: Vec<Range<S>> = Vec::new();
        let mut range_start = page_start;
        // Predecessor used for late items that chain together.
        let mut late_prev = page_start;

        for item in &items {
            let item_pos = S::item_pos(item);

            let strictly_past_tail = tail_end.is_none_or(|t| item_pos > t);
            let strictly_before_tail = tail_end.is_some_and(|t| item_pos < t);
            // Equal-to-tail items take the no-op branch — they aren't
            // routed through insert_late_item because a range already ends
            // at that position.

            if strictly_past_tail {
                new_ranges.push(Range::filled(range_start, item_pos, *item));
            } else if strictly_before_tail {
                Self::insert_late_item(&mut tree, *item, late_prev);
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

        // Async drain to the durable store — see attach_drain. Cloning the
        // page is necessary because `page: &S::Page` is borrowed; the drain
        // task owns its copy. In-memory tree update above is the source of
        // truth for navigation; persistence is best-effort.
        if let Some(tx) = self.drain_tx.lock().unwrap().as_ref() {
            let _ = tx.send(page.clone());
        }
    }

    /// Insert an item that arrived strictly before the current tail.
    ///
    /// Four cases over the range whose end is the next key `>= item_pos`:
    /// 1. None / `range.start > item_pos` → item sits in a gap →
    ///    insert `(range_start, item_pos, item)`.
    /// 2. `range.end == item_pos` and range filled → duplicate, no-op.
    /// 3. `range.end == item_pos` and range empty → upgrade to filled.
    /// 4. `range.start <= item_pos < range.end` → split the range around
    ///    `item_pos`, preserving the original tail.
    ///
    /// A range with a live `fetch` notify is left alone — the in-flight
    /// fetcher will deliver authoritative state.
    pub(super) fn insert_late_item(
        tree: &mut BTreeMap<S::Pos, Range<S>>,
        item: S::Item,
        range_start: S::Pos,
    ) {
        let item_pos = S::item_pos(&item);
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
            // Case 4: split around item_pos.
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

        // existing.start > item_pos → item in a gap before existing.
        tree.insert(item_pos, Range::filled(range_start, item_pos, item));
    }

    // --------------------------------------------------------
    // Lazy per-window load
    // --------------------------------------------------------

    /// Materialize any cached spans from the durable store that overlap the
    /// requested block window into the in-memory tree, if not already
    /// pulled. Idempotent.
    ///
    /// The window is widened to align with [`Self::chunk_size`] so peer
    /// processes observing the same source at the same chunk granularity
    /// see the same tree shape.
    pub fn ensure_window_loaded(&self, range: BlockRange) -> Result<(), HistoryError> {
        let window = align_window(range, self.chunk_size);

        {
            let loaded = self.loaded_windows.lock().unwrap();
            if loaded
                .iter()
                .any(|w| w.from <= window.from && w.to >= window.to)
            {
                return Ok(());
            }
        }

        let spans = self.spec.load_window(window)?;
        let mut tree = self.tree.lock().unwrap();
        for LoadedSpan {
            range: span_range,
            items,
        } in spans
        {
            let span_start = S::Pos::before_block(span_range.from);
            let span_end = S::Pos::end_of_block(span_range.to);
            // Skip if a range ending at this span's end is already present —
            // either an empty tail or a filled-at-end-of-block range is
            // already there. Treat as loaded.
            if tree.contains_key(&span_end) {
                continue;
            }

            let mut prev_pos = span_start;
            for item in &items {
                let pos = S::item_pos(item);
                tree.insert(pos, Range::filled(prev_pos, pos, *item));
                prev_pos = pos;
            }
            if span_end > prev_pos {
                tree.insert(span_end, Range::empty(prev_pos, span_end));
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
pub(super) fn install_gap_placeholder<S: RangeSpec>(
    tree: &mut BTreeMap<S::Pos, Range<S>>,
    start: S::Pos,
    end: S::Pos,
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

/// Align `[range.from, range.to]` outward to a multiple of `chunk` so peer
/// processes scanning the same source at the same chunk size hit identical
/// scanned-range boundaries.
///
/// The alignment math drops to `u64` at this seam ([`ChunkSize`] and
/// [`BlockNumber`](crate::evm::types::BlockNumber) don't do division).
pub(super) fn align_window(range: BlockRange, chunk: ChunkSize) -> BlockRange {
    let (from, to, chunk) = (range.from.get(), range.to.get(), chunk.get());
    let aligned_from = (from / chunk) * chunk;
    let aligned_to = (to / chunk + 1) * chunk - 1;
    BlockRange::from_u64(aligned_from, aligned_to)
}

fn assert_increasing<S: RangeSpec>(items: &[S::Item]) {
    let mut prev: Option<S::Pos> = None;
    for item in items {
        let cur = S::item_pos(item);
        if let Some(p) = prev {
            assert!(cur > p, "page items must be in ascending position order");
        }
        prev = Some(cur);
    }
}
