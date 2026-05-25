//! Fetcher-driven gap filling: the [`Fetcher`] type, `ensure_cached`, the
//! `items()` async stream, and the singleflight `walk_step` shared by both.
//! This is the only path that calls out to `eth_getLogs` (via the fetcher)
//! and the only path that writes fetched pages back into [`LogStore`].

use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

use alloy::rpc::types::Log;
use futures_core::Stream;
use tokio::sync::Notify;

use super::LogIndex;
use super::block_pos::{BlockPos, LogPage, LogRef, log_ref_from};
use super::range_tree::{Range, install_gap_placeholder};
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

impl LogIndex {
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
        let mut cursor = from_pos;
        loop {
            match self.walk_step(cursor, to_pos) {
                WalkStep::Done => return Ok(()),
                // Gap-fill path ignores the boundary item; just advance past
                // the cached range. `walk_step`'s top guard turns the final
                // `cursor >= to` into `Done` on the next iteration.
                WalkStep::Cached { end, .. } => cursor = end,
                WalkStep::Wait(notify) => {
                    notify.notified().await;
                }
                WalkStep::FillGap {
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
    /// Gaps in the tree trigger a fetcher call on the consuming task;
    /// in-flight fetches block on the per-range `Notify`. Cancellation is
    /// by dropping the stream (which drops the `Arc<Self>` capture).
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
    /// `(from, to]`. Shares [`Self::walk_step`] with `ensure_cached`; the
    /// difference is this path acts on the `Cached` boundary item.
    async fn next_yield(
        self: Arc<Self>,
        mut from: BlockPos,
        to: BlockPos,
    ) -> Result<Option<(Log, BlockPos)>, HistoryError> {
        loop {
            match self.walk_step(from, to) {
                WalkStep::Done => return Ok(None),
                WalkStep::Cached { end, item } => {
                    // A range extending past `to` carries a boundary item
                    // outside our window — nothing left to yield.
                    if end > to {
                        return Ok(None);
                    }
                    match item {
                        Some(item) => {
                            let log = self.resolve(&item)?;
                            return Ok(Some((log, end)));
                        }
                        // Empty (no-log) range — advance and keep walking.
                        None => from = end,
                    }
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
            }
        }
    }

    /// One step of walking the range tree from `cursor` toward `to`,
    /// performed entirely under the tree mutex — no locks are held across an
    /// await. Classifies the position just past `cursor` as completion, a
    /// cached range, an in-flight fetch to wait on, or a gap to fill. On
    /// [`WalkStep::FillGap`] it installs a placeholder + notify as a side
    /// effect. Callers loop, advancing `cursor` until they reach `to`.
    fn walk_step(&self, cursor: BlockPos, to: BlockPos) -> WalkStep {
        if cursor >= to {
            return WalkStep::Done;
        }
        let mut tree = self.tree.lock().unwrap();
        let next = tree
            .range((Bound::Excluded(cursor), Bound::Unbounded))
            .next()
            .map(|(_, r)| (r.start, r.end, r.fetch.clone(), r.item));
        let Some((start, end, fetch_opt, item_opt)) = next else {
            // No range past cursor — trailing gap (cursor, to].
            let notify = install_gap_placeholder(&mut tree, cursor, to);
            return WalkStep::FillGap {
                start_pos: cursor,
                end_pos: to,
                notify,
            };
        };
        if start > cursor {
            // Gap before the next range (cached or in-flight) — fill it. If
            // we beat an in-flight neighbour here, we own the gap placeholder.
            let gap_end = start.min(to);
            let notify = install_gap_placeholder(&mut tree, cursor, gap_end);
            return WalkStep::FillGap {
                start_pos: cursor,
                end_pos: gap_end,
                notify,
            };
        }
        // start <= cursor: cursor sits inside this range.
        if let Some(notify) = fetch_opt {
            // In-flight fetch covers us — wait, then re-walk from `cursor`.
            return WalkStep::Wait(notify);
        }
        WalkStep::Cached {
            end,
            item: item_opt,
        }
    }
}

// ============================================================
// Private supporting types
// ============================================================

/// One step of [`LogIndex::walk_step`]. Both `ensure_cached` (gap-fill only)
/// and `next_yield` (streaming) drive the same walk; they diverge only in how
/// they treat the `Cached` arm — the former advances past it, the latter
/// yields its boundary `item`.
enum WalkStep {
    /// Coverage reached `to`; nothing left in range.
    Done,
    /// A cached range covers `(cursor, end]`. `item` is its boundary log
    /// (`None` for an empty range).
    Cached { end: BlockPos, item: Option<LogRef> },
    /// A gap `(start_pos, end_pos]` to fill via the fetcher; a placeholder
    /// guarded by `notify` is already installed in the tree.
    FillGap {
        start_pos: BlockPos,
        end_pos: BlockPos,
        notify: Arc<Notify>,
    },
    /// An in-flight fetch covers the cursor; wait on `notify`, then re-walk.
    Wait(Arc<Notify>),
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
