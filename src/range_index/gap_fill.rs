//! Fetcher-driven gap filling: `ensure_cached`, the `items()` async stream,
//! and the singleflight `walk_step` shared by both. This is the only path
//! that calls the fetcher and the only path that writes fetched pages back
//! to the durable store (via [`RangeSpec::persist`]).
//!
//! A gap is filled by **one** [`PageFetcher::fetch`] stream whose pages
//! finalize incrementally: each page's items enter the tree, the page
//! persists, the in-flight placeholder shrinks from the left, and waiters
//! are woken — so consumers see the delivered prefix while later pages are
//! still in flight. The walker that installed the placeholder owns the
//! stream ([`ActiveFill`]); dropping it mid-fill releases the remainder.
//!
//! All generic over a [`RangeSpec`](super::spec::RangeSpec) `S`.

use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;

use super::RangeIndex;
use super::range_tree::{Range, install_gap_placeholder, shrink_placeholder};
use super::spec::{IndexPos, PageStream, RangeSpec};
#[cfg(test)]
use super::spec::{PageFetcher, SinglePageFetcher};
use crate::arch::{HistoryError, Positioned};
use crate::chain::BlockRange;
use crate::observe::{CacheLayer, CacheOutcome, cache_event};
use crate::share::Share;

/// The public positioned value [`RangeIndex::items`] yields for a spec `S`.
type Yielded<S> = Positioned<<S as RangeSpec>::Event, <S as RangeSpec>::PubPos>;

/// Build a [`PageFetcher`] that always errors. Useful in tests that should
/// never trigger a fetch — if the fetcher is invoked the test fails
/// with a clear message.
#[cfg(test)]
pub(crate) fn never_fetch<S: RangeSpec>() -> Arc<dyn PageFetcher<S::Page>> {
    Arc::new(SinglePageFetcher(|_range: BlockRange| async {
        Err::<S::Page, _>(HistoryError::internal(
            "never_fetch fetcher was invoked: an unexpected gap fill was triggered",
        ))
    }))
}

impl<S: RangeSpec> RangeIndex<S> {
    // --------------------------------------------------------
    // ensure_cached + singleflight
    // --------------------------------------------------------

    /// Fill any gaps in `[range.from, range.to]` by calling the fetcher.
    /// Concurrent callers share work via per-range [`Notify`] sentinels —
    /// the first arrival at a gap installs the notify, drives the page
    /// stream to completion, and notifies waiters per page; later arrivals
    /// see the notify, wait on it, and re-walk.
    pub async fn ensure_cached(self: Arc<Self>, range: BlockRange) -> Result<(), HistoryError> {
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
                    self.wait_for_progress(cursor, &notify).await;
                }
                WalkStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    // We hold the in-tree placeholder; drive the whole
                    // stream before continuing the walk.
                    let mut fill = ActiveFill::start(self.share(), start_pos, end_pos, notify);
                    while let FillStep::PageLanded = fill.drive_one().await? {}
                }
            }
        }
    }

    /// Wait on a peer's in-flight fetch. Registers interest *before*
    /// re-checking that the placeholder still covers the position past
    /// `cursor`: [`Notify::notify_waiters`] stores no permit, so a
    /// notification firing between the walk (which cloned the notify) and
    /// the await here would otherwise be lost — with per-page wakeups that
    /// window is hot. Registration-first makes progress either visible to
    /// the re-check or delivered to the registered future.
    async fn wait_for_progress(&self, cursor: S::Pos, notify: &Arc<Notify>) {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.still_in_flight(cursor, notify) {
            return; // Progress already happened — re-walk.
        }
        notified.await;
    }

    /// Read-only probe: does the range covering the position just past
    /// `cursor` still carry this same in-flight [`Notify`]?
    fn still_in_flight(&self, cursor: S::Pos, notify: &Arc<Notify>) -> bool {
        let tree = self.tree.lock().unwrap();
        tree.range((Bound::Excluded(cursor), Bound::Unbounded))
            .next()
            .and_then(|(_, r)| r.fetch.as_ref())
            .is_some_and(|n| Arc::ptr_eq(n, notify))
    }

    /// Remove the placeholder at `end_pos` iff it still carries `notify` —
    /// identity-checked so an unrelated range later keyed at the same
    /// position is never clobbered — then wake waiters.
    fn abort_fill(&self, end_pos: S::Pos, notify: &Arc<Notify>) {
        let mut tree = self.tree.lock().unwrap();
        let ours = tree
            .get(&end_pos)
            .and_then(|r| r.fetch.as_ref())
            .is_some_and(|n| Arc::ptr_eq(n, notify));
        if ours {
            tree.remove(&end_pos);
        }
        drop(tree);
        notify.notify_waiters();
    }

    // --------------------------------------------------------
    // items() async stream
    // --------------------------------------------------------

    /// Yield every cached and fetched-on-demand item whose position is
    /// in `(from, to]`, in ascending order. Closes when `to` is reached.
    ///
    /// Gaps in the tree trigger a fetcher stream driven on the consuming
    /// task — items from a delivered page yield while later pages are still
    /// in flight; peers of an in-flight fetch block on the per-range
    /// `Notify`. Cancellation is by dropping the stream (which drops the
    /// in-flight fill, releasing its placeholder).
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
            fill: None,
            done: false,
        };
        Box::pin(unfold(state, |mut state| async move {
            match state.next_yield().await {
                Ok(Some(full)) => Some((Ok(full), state)),
                Ok(None) => None,
                Err(e) => {
                    state.done = true;
                    Some((Err(e), state))
                }
            }
        }))
    }

    /// One step of walking the range tree from `cursor` toward `to`,
    /// performed entirely under the tree mutex — no locks are held across an
    /// await. Classifies the position just past `cursor` as completion, a
    /// cached range, an in-flight fetch to wait on, or a gap to fill. On
    /// [`WalkStep::FillGap`] it installs a placeholder + notify as a side
    /// effect. Callers loop, advancing `cursor` until they reach `to`.
    ///
    /// Gaps are capped at [`Self::chunk_size`]-aligned boundaries via
    /// [`Self::cap_gap`], so one fetcher stream never spans a chunk boundary;
    /// the loop advances into the remainder chunk by chunk.
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
            let gap_end = self.cap_gap(cursor, to);
            let notify = install_gap_placeholder(&mut tree, cursor, gap_end);
            return WalkStep::FillGap {
                start_pos: cursor,
                end_pos: gap_end,
                notify,
            };
        };
        if start > cursor {
            // Gap before the next range (cached or in-flight) — fill it. If
            // we beat an in-flight neighbour here, we own the gap placeholder.
            let gap_end = self.cap_gap(cursor, start.min(to));
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

    /// Cap the gap `(cursor, gap_end]` at the end of its first
    /// [`Self::chunk_size`]-aligned chunk, in block space. The start is
    /// never rounded down — everything before `cursor` is already covered,
    /// and refetching it would break the fetched-at-most-once guarantee.
    /// After a partial leading chunk, every subsequent fetch lands exactly
    /// on aligned boundaries, so peers at the same chunk size share cache
    /// entries.
    fn cap_gap(&self, cursor: S::Pos, gap_end: S::Pos) -> S::Pos {
        let bounds = S::Pos::fetch_bounds(cursor, gap_end);
        let chunk_end = self.chunk_size.chunk_end(bounds.from);
        if chunk_end < bounds.to {
            S::Pos::end_of_block(chunk_end)
        } else {
            gap_end
        }
    }
}

// ============================================================
// ActiveFill — one in-flight streaming gap fill
// ============================================================

/// An in-flight streaming gap fill, owned by the walker that installed the
/// placeholder. [`Self::drive_one`] awaits the next page and finalizes it;
/// waiters observe progress through the placeholder's [`Notify`], which
/// stays the same `Arc` across shrinks so nobody is orphaned mid-fill.
/// Dropping this mid-stream removes the placeholder remainder and wakes
/// waiters — a cancelled walker can never strand the gap.
struct ActiveFill<S: RangeSpec> {
    index: Arc<RangeIndex<S>>,
    stream: PageStream<S::Page>,
    /// Left edge of the unfilled remainder; advances per page.
    cursor: S::Pos,
    end_pos: S::Pos,
    notify: Arc<Notify>,
    /// The placeholder is gone (final page landed, or aborted).
    done: bool,
}

/// One [`ActiveFill::drive_one`] outcome.
enum FillStep {
    /// A page landed and was finalized; the tree changed, more pages remain.
    PageLanded,
    /// The stream covered the whole gap; the placeholder is gone.
    Finished,
}

impl<S: RangeSpec> ActiveFill<S> {
    /// Open the fetcher stream for the gap `(start_pos, end_pos]` whose
    /// placeholder (guarded by `notify`) the caller just installed.
    fn start(
        index: Arc<RangeIndex<S>>,
        start_pos: S::Pos,
        end_pos: S::Pos,
        notify: Arc<Notify>,
    ) -> Self {
        let bounds = S::Pos::fetch_bounds(start_pos, end_pos);
        let stream = index.fetcher.fetch(bounds);
        Self {
            index,
            stream,
            cursor: start_pos,
            end_pos,
            notify,
            done: false,
        }
    }

    /// Await the next page and finalize it. On a fetch error, a persist
    /// error, or a stream ending short of the gap, the fill aborts: the
    /// remaining placeholder is removed, waiters are woken, and the error
    /// surfaces — the finalized prefix stands.
    async fn drive_one(&mut self) -> Result<FillStep, HistoryError> {
        debug_assert!(!self.done, "drive_one called after the fill finished");
        match self.stream.next().await {
            Some(Ok(page)) => self.finalize_page(&page),
            Some(Err(e)) => {
                self.abort();
                Err(e)
            }
            None => {
                // Contract: a clean end is only valid after a page reaching
                // the gap end. Ending short would leave a suffix gap the
                // very next walk re-fetches — an infinite loop against a
                // deterministic short-streaming fetcher. Fail loudly.
                self.abort();
                Err(HistoryError::internal(format!(
                    "page stream ended before covering its gap \
                     (at {:?}, gap ends {:?})",
                    self.cursor, self.end_pos
                )))
            }
        }
    }

    /// Insert one delivered page: shrink (or remove) the placeholder,
    /// chain the page's items from the running cursor, stamp the empty
    /// tail, persist inline, and wake waiters.
    fn finalize_page(&mut self, page: &S::Page) -> Result<FillStep, HistoryError> {
        let bounds = S::page_bounds(page);
        let mut page_end = S::Pos::end_of_block(bounds.to);
        if page_end > self.end_pos {
            debug_assert!(false, "page {bounds:?} extends past the gap end");
            page_end = self.end_pos;
        }

        let mut tree = self.index.tree.lock().unwrap();
        // Placeholder bookkeeping first: on the final page the empty-tail /
        // item inserts below share its key, so it must be gone before they
        // land; on an interior page the same `Notify` must survive the
        // shrink so registered waiters stay attached.
        if page_end < self.end_pos {
            shrink_placeholder::<S>(&mut tree, self.end_pos, page_end);
        } else {
            tree.remove(&self.end_pos);
            self.done = true;
        }
        let items = S::page_items(page);
        let mut prev = self.cursor;
        for item in &items {
            let pos = S::item_pos(item);
            if pos > page_end {
                // Unlike items at-or-before the cursor (legitimate on the
                // first page of a mid-block gap), an item past the page's
                // own bounds is always a fetcher contract violation.
                debug_assert!(false, "page item at {pos:?} beyond the page bounds");
                continue;
            }
            if pos <= prev {
                continue;
            }
            tree.insert(pos, Range::filled(prev, pos, *item));
            prev = pos;
        }
        if page_end > prev {
            tree.insert(page_end, Range::empty(prev, page_end));
        }
        drop(tree);

        // Persist so the next process sees the same shape. A failure aborts
        // the remainder — but waiters are still woken (via abort), and the
        // in-memory prefix stands.
        let persisted = self.index.spec.persist(page);
        if let Err(e) = persisted {
            self.abort();
            return Err(e);
        }
        self.notify.notify_waiters();
        self.cursor = page_end;
        if self.done {
            Ok(FillStep::Finished)
        } else {
            Ok(FillStep::PageLanded)
        }
    }

    /// Release the placeholder remainder (if any) and wake waiters.
    /// Idempotent on the tree; always notifies so no error path can strand
    /// a registered waiter.
    fn abort(&mut self) {
        if self.done {
            self.notify.notify_waiters();
        } else {
            self.index.abort_fill(self.end_pos, &self.notify);
            self.done = true;
        }
    }
}

impl<S: RangeSpec> Drop for ActiveFill<S> {
    fn drop(&mut self) {
        if !self.done {
            self.index.abort_fill(self.end_pos, &self.notify);
        }
    }
}

// ============================================================
// Private supporting types
// ============================================================

/// One step of [`RangeIndex::walk_step`]. Both `ensure_cached` (gap-fill only)
/// and the `items()` walk drive the same step; they diverge only in how
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

/// The `items()` walk state: the running cursor plus, when this walker owns
/// a gap fill, the in-flight [`ActiveFill`] driven between yields.
struct ItemsState<S: RangeSpec> {
    index: Arc<RangeIndex<S>>,
    cursor: S::Pos,
    to: S::Pos,
    fill: Option<ActiveFill<S>>,
    done: bool,
}

impl<S: RangeSpec> ItemsState<S> {
    /// Walk until exactly one item can be yielded or the window is
    /// exhausted. An owned gap fill is driven one page at a time between
    /// walk steps, so items from a delivered page yield while later pages
    /// are still in flight.
    async fn next_yield(&mut self) -> Result<Option<Yielded<S>>, HistoryError> {
        loop {
            if self.done || self.cursor >= self.to {
                return Ok(None);
            }
            match self.index.walk_step(self.cursor, self.to) {
                WalkStep::Done => return Ok(None),
                WalkStep::Cached { end, item } => {
                    // A range extending past `to` carries a boundary item
                    // outside our window — nothing left to yield.
                    if end > self.to {
                        return Ok(None);
                    }
                    match item {
                        Some(item) => {
                            let full = self.index.resolve(&item)?;
                            self.cursor = end;
                            return Ok(Some(full));
                        }
                        // Empty (no-item) range — advance and keep walking.
                        None => self.cursor = end,
                    }
                }
                WalkStep::FillGap {
                    start_pos,
                    end_pos,
                    notify,
                } => {
                    // Pages are contiguous, so a new gap can only appear
                    // once the previous fill finished (which cleared it).
                    debug_assert!(self.fill.is_none(), "second gap while a fill is in flight");
                    self.fill = Some(ActiveFill::start(
                        self.index.share(),
                        start_pos,
                        end_pos,
                        notify,
                    ));
                    // Next walk step lands inside the placeholder and
                    // drives it via the arm below.
                }
                WalkStep::Wait(notify) => {
                    let ours = self
                        .fill
                        .as_mut()
                        .filter(|f| Arc::ptr_eq(&f.notify, &notify));
                    if let Some(fill) = ours {
                        match fill.drive_one().await {
                            Ok(FillStep::PageLanded) => {}
                            Ok(FillStep::Finished) => self.fill = None,
                            Err(e) => {
                                self.fill = None;
                                return Err(e);
                            }
                        }
                    } else {
                        // Coalesced onto a peer's in-flight fetch — an RPC
                        // saved.
                        cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
                        self.index.wait_for_progress(self.cursor, &notify).await;
                    }
                }
            }
        }
    }
}
