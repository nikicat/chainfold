//! Fetcher-driven gap filling: `ensure_cached`, the `items()` async stream,
//! and the singleflight `walk_step` shared by both. This is the only path
//! that calls the fetcher and the only path that writes fetched pages back
//! to the durable store (via [`RangeSpec::persist`]).
//!
//! All generic over a [`RangeSpec`](super::spec::RangeSpec) `S`.

use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use tokio::sync::Notify;

use super::RangeIndex;
use super::range_tree::{Range, install_gap_placeholder};
#[cfg(test)]
use super::spec::Fetcher;
use super::spec::{IndexPos, RangeSpec};
use crate::arch::{HistoryError, Positioned};
use crate::chain::BlockRange;
use crate::observe::{CacheLayer, CacheOutcome, cache_event};

/// The public positioned value [`RangeIndex::items`] yields for a spec `S`.
type Yielded<S> = Positioned<<S as RangeSpec>::Event, <S as RangeSpec>::PubPos>;

/// Build a [`Fetcher`] that always errors. Useful in tests that should
/// never trigger a fetch — if the fetcher is invoked the test fails
/// with a clear message.
#[cfg(test)]
#[allow(dead_code)] // consumed by the feature-gated spec tests
pub(crate) fn never_fetch<S: RangeSpec>() -> Fetcher<S::Page> {
    Arc::new(|_, _| {
        Box::pin(async {
            Err(HistoryError::internal(
                "never_fetch fetcher was invoked: an unexpected gap fill was triggered",
            ))
        })
    })
}

impl<S: RangeSpec> RangeIndex<S> {
    // --------------------------------------------------------
    // ensure_cached + singleflight
    // --------------------------------------------------------

    /// Fill any gaps in `[range.from, range.to]` by calling the fetcher.
    /// Concurrent callers share work via per-range [`Notify`] sentinels —
    /// the first arrival at a gap installs the notify, fetches, drops the
    /// notify, and notifies waiters; later arrivals see the notify, wait
    /// on it, and re-walk.
    pub async fn ensure_cached(&self, range: BlockRange) -> Result<(), HistoryError> {
        self.ensure_window_loaded(range)?;

        let from_pos = S::Pos::before_block(range.from);
        let to_pos = S::Pos::end_of_block(range.to);
        let mut cursor = from_pos;
        loop {
            match self.walk_step(cursor, to_pos) {
                WalkStep::Done => return Ok(()),
                // Gap-fill path ignores the boundary item; just advance past
                // the cached range. `walk_step`'s top guard turns the final
                // `cursor >= to` into `Done` on the next iteration.
                WalkStep::Cached { end, .. } => cursor = end,
                WalkStep::Wait(notify) => {
                    // Coalesced onto a peer's in-flight fetch — an RPC saved.
                    cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
                    notify.notified().await;
                }
                WalkStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    // We hold the in-tree fetch notify; fetch outside the lock.
                    let bounds = S::Pos::fetch_bounds(start_pos, end_pos);
                    let fetch_result = (self.fetcher)(bounds.from, bounds.to).await;
                    // Always remove the placeholder + notify on completion.
                    self.finalize_gap_fill(start_pos, end_pos, fetch_result.as_ref().ok())?;
                    notify.notify_waiters();
                    fetch_result?;
                }
            }
        }
    }

    /// Remove the placeholder fetch range and insert any fetched items as
    /// proper Range entries. Called after the fetcher returns (success or
    /// failure). On failure we just remove the placeholder so a subsequent
    /// caller retries.
    fn finalize_gap_fill(
        &self,
        start_pos: S::Pos,
        end_pos: S::Pos,
        page: Option<&S::Page>,
    ) -> Result<(), HistoryError> {
        let mut tree = self.tree.lock().unwrap();
        // Drop placeholder.
        tree.remove(&end_pos);
        let Some(page) = page else {
            return Ok(());
        };
        let items = S::page_items(page);
        // Translate page items into Ranges chained from start_pos to end_pos.
        let mut prev = start_pos;
        for item in &items {
            let pos = S::item_pos(item);
            if pos <= prev || pos > end_pos {
                continue;
            }
            tree.insert(pos, Range::filled(prev, pos, *item));
            prev = pos;
        }
        if end_pos > prev {
            tree.insert(end_pos, Range::empty(prev, end_pos));
        }
        // Persist so the next process sees the same shape.
        drop(tree);
        self.spec.persist(page)
    }

    // --------------------------------------------------------
    // items() async stream
    // --------------------------------------------------------

    /// Yield every cached and fetched-on-demand item whose position is
    /// in `(from, to]`, in ascending order. Closes when `to` is reached.
    ///
    /// Gaps in the tree trigger a fetcher call on the consuming task;
    /// in-flight fetches block on the per-range `Notify`. Cancellation is
    /// by dropping the stream (which drops the `Arc<Self>` capture).
    pub fn items(
        self: Arc<Self>,
        from: S::Pos,
        to: S::Pos,
    ) -> Pin<Box<dyn Stream<Item = Result<Yielded<S>, HistoryError>> + Send>> {
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
                Ok(Some((full, advanced))) => {
                    state.cursor = advanced;
                    Some((Ok(full), state))
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
        mut from: S::Pos,
        to: S::Pos,
    ) -> Result<Option<(Yielded<S>, S::Pos)>, HistoryError> {
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
                            let full = self.resolve(&item)?;
                            return Ok(Some((full, end)));
                        }
                        // Empty (no-item) range — advance and keep walking.
                        None => from = end,
                    }
                }
                WalkStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    let bounds = S::Pos::fetch_bounds(start_pos, end_pos);
                    let result = (self.fetcher)(bounds.from, bounds.to).await;
                    self.finalize_gap_fill(start_pos, end_pos, result.as_ref().ok())?;
                    notify.notify_waiters();
                    result?;
                    // Tree changed; restart from current cursor.
                }
                WalkStep::Wait(notify) => {
                    // Coalesced onto a peer's in-flight fetch — an RPC saved.
                    cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
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
    fn walk_step(&self, cursor: S::Pos, to: S::Pos) -> WalkStep<S> {
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

/// One step of [`RangeIndex::walk_step`]. Both `ensure_cached` (gap-fill only)
/// and `next_yield` (streaming) drive the same walk; they diverge only in how
/// they treat the `Cached` arm — the former advances past it, the latter
/// yields its boundary `item`.
enum WalkStep<S: RangeSpec> {
    /// Coverage reached `to`; nothing left in range.
    Done,
    /// A cached range covers `(cursor, end]`. `item` is its boundary item
    /// (`None` for an empty range).
    Cached { end: S::Pos, item: Option<S::Item> },
    /// A gap `(start_pos, end_pos]` to fill via the fetcher; a placeholder
    /// guarded by `notify` is already installed in the tree.
    FillGap {
        start_pos: S::Pos,
        end_pos: S::Pos,
        notify: Arc<Notify>,
    },
    /// An in-flight fetch covers the cursor; wait on `notify`, then re-walk.
    Wait(Arc<Notify>),
}

struct ItemsState<S: RangeSpec> {
    index: Arc<RangeIndex<S>>,
    cursor: S::Pos,
    to: S::Pos,
    done: bool,
}
