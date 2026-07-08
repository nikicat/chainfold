//! chainfold — public API.
//!
//! Source-agnostic core: a stream of items is exposed as a sequence of
//! [`Hist`] snapshots produced by a [`HistStore`]. Each [`Hist`] is finite,
//! immutable, and iterates items in the half-open interval `(start, end]`.
//! [`HistStore::snapshots`] yields a fresh [`Hist`] each time the source's tip
//! advances; consumers resume by [`split`](Hist::split)ing at a saved
//! position.
//!
//! ## Layering
//!
//! * **Generic core** (this module) knows nothing about EVM, Solana,
//!   Bitcoin, CEXes, or any other concrete source. Tests for the core use a
//!   synthetic position type, not [`crate::evm::EvmPosition`].
//! * **Source modules** ([`crate::evm`], future `crate::solana`, …) provide
//!   concrete [`HistStoreBackend`] implementations and source-specific
//!   event/position types.
//!
//! ## Convention
//!
//! Items in a [`Hist`] are at positions in `(start, end]` — strictly after
//! `start`, at-or-before `end`. A position therefore represents the
//! *post-cut* of an item: the place "just after" it. Save the position of
//! the last item you processed as your resume anchor; [`Hist::split`] at
//! that anchor yields `(empty_prefix, remaining_tail)`.

use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::unfold;
use tokio::sync::watch;

// ============================================================
// Core type aliases
// ============================================================

/// A Unix timestamp in seconds.
///
/// A source-agnostic wall-clock instant — the unit [`HistStore::time_pos`],
/// [`act_and_wait`], and the matcher time-anchors speak. A newtype rather than
/// a bare `u64` so a timestamp can't be silently swapped with a block number or
/// any other count. Convert to/from `u64` at the chrono / RPC / `SQLite`
/// boundary via [`Timestamp::new`] / [`Timestamp::get`].
///
/// Arithmetic is *affine*, like `BlockNumber`'s: an instant + a seconds
/// offset is another instant ([`Add`](std::ops::Add)); the distance between
/// two instants is a `u64` of seconds, computed via [`Timestamp::get`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Wrap seconds-since-epoch.
    pub const fn new(secs: u64) -> Self {
        Self(secs)
    }

    /// The inner seconds-since-epoch, for the chrono / RPC / `SQLite` boundary.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(secs: u64) -> Self {
        Self(secs)
    }
}

impl std::ops::Add<u64> for Timestamp {
    type Output = Self;
    fn add(self, secs: u64) -> Self {
        Self(self.0 + secs)
    }
}

/// Boxed async stream.
pub type Subscription<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// Stream of items yielded by [`Hist::iter`].
pub type ItemStream<T, P> = Subscription<Result<Positioned<T, P>, HistoryError>>;

/// Stream of snapshots yielded by [`HistStore::snapshots`].
pub type HistStream<T, P> = Subscription<Hist<T, P>>;

// ============================================================
// HistoryError
// ============================================================

/// Errors returned by [`Hist`] and [`HistStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// Storage, serialization, or other internal error.
    #[error(transparent)]
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct InternalMsg(String);

impl HistoryError {
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(Box::new(InternalMsg(msg.into())))
    }
}

// ============================================================
// Position
// ============================================================

/// Totally-ordered, serializable cut point in a single source's stream.
///
/// A position is the gap *between* (or at the boundary of) items — never an
/// item itself. The natural lower bound (analogous to "before any item") is
/// the type's own [`Default`] value; e.g. EVM uses `EvmPosition { block: 0,
/// log_index: 0 }`. This sentinel is what the vector-clock interleave
/// ([`crate::merge::interleave_hist_stores`]) substitutes when a saved
/// [`crate::merge::MergedPos`] lacks an entry for a newly-added source.
///
/// **Source-local.** Positions from different sources are not comparable
/// and the trait does not provide source identity — that concern lives on
/// [`HistStore`]. Sources sharing one position space merge via
/// [`crate::merge::merge_hist_stores`] on `P`'s own order; disjoint-space
/// interleave wraps per-source positions in a keyed
/// [`crate::merge::MergedPos`] and never compares them across sources.
pub trait Position: Clone + Default + Eq + Ord + std::fmt::Debug + Send + Sync + 'static {
    /// Serialize to a structured [`serde_json::Value`] for persistence.
    ///
    /// A *structured* value (not opaque bytes) so composite positions nest
    /// trivially — `MergedPos` is a JSON object of its sources' values,
    /// `MatcherAnchors` a `{src, dst}` object — with no hand-rolled framing.
    /// Dump it to JSON or any other serde format at the storage boundary.
    ///
    /// Deserialization is deliberately **not** on this trait — it lives on the
    /// owning store as [`HistStore::load`]. A position can only serialize
    /// *itself* (a behaviour of a concrete instance), whereas `load` must
    /// exist *before* any instance does and, for heterogeneous merge, needs
    /// the source identities the store holds. Mirrors the reference
    /// `history.ts`'s `pos.serialize()` / `store.load(json)` split.
    fn marshal(&self) -> serde_json::Value;
}

// ============================================================
// Positioned<T, P>
// ============================================================

/// An item paired with its position in the source stream.
///
/// Yielded by [`Hist::iter`]. The position is the *post-cut* of the item:
/// `hist.split(item.pos)` yields `(prefix_including_item, rest)`.
#[derive(Clone, Debug)]
pub struct Positioned<T, P: Position> {
    pub item: T,
    pub pos: P,
}

impl<T, P: Position> Positioned<T, P> {
    pub fn new(item: T, pos: P) -> Self {
        Self { item, pos }
    }
}

// ============================================================
// Signal
// ============================================================

/// A reactive value: the current value plus a stream of its changes.
///
/// A thin newtype over [`tokio::sync::watch::Receiver`] — "current value +
/// change notifications" is exactly what `watch` provides. The load-bearing
/// half is the synchronous [`get`](Signal::get) read; [`subscribe`](Signal::subscribe)
/// adds the change stream. Named `Signal` (à la SolidJS/Leptos), not `Store`, to keep
/// the reactive *value* distinct from the `*Store` types (`HistStore`,
/// `LogCache`'s stores, `AnchorStore`, `RangeIndexStore`), which are backing
/// stores, not reactive values.
pub struct Signal<T: Clone + Send + Sync> {
    rx: watch::Receiver<T>,
}

impl<T: Clone + Send + Sync> Clone for Signal<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Signal<T> {
    pub fn new(rx: watch::Receiver<T>) -> Self {
        Self { rx }
    }

    /// Current value (non-blocking snapshot).
    pub fn get(&self) -> T {
        self.rx.borrow().clone()
    }

    /// Stream of value changes. The first item is always the current value.
    pub fn subscribe(&self) -> Subscription<T> {
        let rx = self.rx.clone();
        Box::pin(unfold((rx, true), |(mut rx, first)| async move {
            if first {
                let val = rx.borrow_and_update().clone();
                Some((val, (rx, false)))
            } else {
                rx.changed().await.ok()?;
                let val = rx.borrow_and_update().clone();
                Some((val, (rx, false)))
            }
        }))
    }
}

// ============================================================
// Hist
// ============================================================

/// Finite, immutable snapshot of a source's stream.
///
/// Iterates items in `(start, end]`. The snapshot is fixed at construction
/// — even if new items arrive at the source, this `Hist` continues to
/// reflect the same window. Get a fresh window via
/// [`HistStore::next`]/[`HistStore::snapshots`].
pub struct Hist<T, P: Position> {
    backend: Arc<dyn HistBackend<T, P>>,
    start: P,
    end: P,
}

impl<T, P: Position> Clone for Hist<T, P> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
        }
    }
}

impl<T: Send + Sync + 'static, P: Position> Hist<T, P> {
    pub fn new(backend: Arc<dyn HistBackend<T, P>>, start: P, end: P) -> Self {
        debug_assert!(start <= end, "Hist::new: start must be <= end");
        Self {
            backend,
            start,
            end,
        }
    }

    /// Exclusive lower bound — items in this Hist are at positions `> start`.
    pub fn start(&self) -> &P {
        &self.start
    }

    /// Inclusive upper bound — the most recent item's position (or `start`
    /// for an empty Hist).
    pub fn end(&self) -> &P {
        &self.end
    }

    /// Underlying backend. Combinators wrap this to build proxy histories
    /// without going through [`HistStore`].
    pub fn backend(&self) -> &Arc<dyn HistBackend<T, P>> {
        &self.backend
    }

    /// True if `start == end` (no items in this snapshot).
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// Stream items in `(start, end]` in ascending position order.
    ///
    /// Item gaps cached by the backend are returned immediately; uncached
    /// gaps are filled on demand. Drop the stream to cancel any in-flight
    /// fetches.
    pub fn iter(&self) -> ItemStream<T, P> {
        self.backend.iter(self.start.clone(), self.end.clone())
    }

    /// Split into `(head, tail)` at position `p`.
    ///
    /// * `head` iterates `(start, min(p, end)]`.
    /// * `tail` iterates `(max(p, start), end]`.
    ///
    /// If `p < start` the head is empty and tail == self. If `p > end` the
    /// head == self and tail is empty. Always returns two valid (possibly
    /// empty) snapshots; never widens the window.
    ///
    /// Clamping is delegated to [`HistBackend::clamp_pos`] so that backends
    /// whose [`Position`] has non-canonical [`Ord`] (e.g. the keyed
    /// [`crate::merge::MergedPos`]) can substitute component-wise clamping.
    pub fn split(&self, p: P) -> (Self, Self) {
        let clamped = self.backend.clamp_pos(p, &self.start, &self.end);
        let head = Self {
            backend: self.backend.clone(),
            start: self.start.clone(),
            end: clamped.clone(),
        };
        let tail = Self {
            backend: self.backend.clone(),
            start: clamped,
            end: self.end.clone(),
        };
        (head, tail)
    }
}

// ============================================================
// HistStore
// ============================================================

/// Reactive sequence of [`Hist`] snapshots — the live "current view" of a
/// source's stream.
///
/// Yields a fresh [`Hist`] whenever the source's tip advances. Each new
/// snapshot's `end` is the latest known position; consumers process
/// `(saved_anchor, new_end]` and save the new anchor.
pub struct HistStore<T, P: Position> {
    backend: Arc<dyn HistStoreBackend<T, P>>,
}

impl<T, P: Position> Clone for HistStore<T, P> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<T: Send + Sync + 'static, P: Position> HistStore<T, P> {
    pub fn new(backend: Arc<dyn HistStoreBackend<T, P>>) -> Self {
        Self { backend }
    }

    /// Stream of snapshots: first item is the current snapshot, subsequent
    /// items are emitted as the tip advances.
    pub fn snapshots(&self) -> HistStream<T, P> {
        self.backend.snapshots()
    }

    /// Current snapshot, snapped to the latest known tip.
    pub async fn next(&self) -> Hist<T, P> {
        self.backend.next().await
    }

    /// Estimate a position at a wall-clock time. Used as a cut point in
    /// [`Hist::split`] when resuming from a time instead of an anchor.
    pub async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        self.backend.time_pos(time).await
    }

    /// Reconstruct a position from a [`serde_json::Value`] produced by
    /// [`Position::marshal`].
    ///
    /// The store — which knows the concrete `P` (and, for merge, the source
    /// identities) — owns deserialization. Resume by loading a saved anchor
    /// here, then [`Hist::split`]ting at it. Returns `None` on invalid input.
    pub fn load(&self, value: serde_json::Value) -> Option<P> {
        self.backend.load(value)
    }
}

// ============================================================
// Backend traits (implemented per source)
// ============================================================

/// Backing implementation for [`Hist`]. Implemented per source.
pub trait HistBackend<T: Send + Sync + 'static, P: Position>: Send + Sync {
    /// Stream items in `(start, end]` ascending.
    fn iter(&self, start: P, end: P) -> ItemStream<T, P>;

    /// Clamp `p` to lie within `[start, end]`. Default implementation uses
    /// [`Ord`] on `P`. Override for positions whose canonical `Ord` does
    /// not reflect the window-containment semantics needed by
    /// [`Hist::split`] — e.g. the keyed [`crate::merge::MergedPos`] has a
    /// lexicographic total order over its per-source components but clamps
    /// each component separately to honor per-source containment.
    fn clamp_pos(&self, p: P, start: &P, end: &P) -> P {
        if &p < start {
            start.clone()
        } else if &p > end {
            end.clone()
        } else {
            p
        }
    }
}

/// Backing implementation for [`HistStore`]. Implemented per source.
#[async_trait::async_trait]
pub trait HistStoreBackend<T: Send + Sync + 'static, P: Position>: Send + Sync {
    /// Yield a fresh [`Hist`] each time the source's tip advances; first
    /// yield is the current snapshot.
    fn snapshots(&self) -> HistStream<T, P>;

    /// Current snapshot.
    async fn next(&self) -> Hist<T, P>;

    /// Estimate a position at a wall-clock time.
    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError>;

    /// Deserialize a position from a [`Position::marshal`] value. The store
    /// owns deserialization because it knows the concrete `P` (and, for
    /// merge, the per-source identities). Returns `None` on invalid input.
    fn load(&self, value: serde_json::Value) -> Option<P>;
}

// ============================================================
// watch_hist / act_and_wait
// ============================================================

/// Yield every item arriving after `start`, across snapshot boundaries.
///
/// Each fresh snapshot from `store.snapshots()` is split at the running anchor;
/// only the tail (post-anchor items) is iterated. The anchor advances as
/// items are emitted, so a snapshot that arrives during iteration just
/// extends the window — items are never duplicated.
pub fn watch_hist<T: Send + Sync + 'static, P: Position>(
    store: &HistStore<T, P>,
    start: P,
) -> ItemStream<T, P> {
    let snapshots = store.snapshots();
    Box::pin(unfold(
        WatchState {
            snapshots,
            anchor: start,
            current: None,
        },
        watch_step,
    ))
}

struct WatchState<T: Send + Sync + 'static, P: Position> {
    snapshots: HistStream<T, P>,
    anchor: P,
    current: Option<ItemStream<T, P>>,
}

async fn watch_step<T: Send + Sync + 'static, P: Position>(
    mut state: WatchState<T, P>,
) -> Option<(Result<Positioned<T, P>, HistoryError>, WatchState<T, P>)> {
    loop {
        if state.current.is_none() {
            let hist = state.snapshots.next().await?;
            let (_, tail) = hist.split(state.anchor.clone());
            state.current = Some(tail.iter());
        }
        let stream = state.current.as_mut().unwrap();
        match stream.next().await {
            Some(Ok(item)) => {
                state.anchor = item.pos.clone();
                return Some((Ok(item), state));
            }
            Some(Err(e)) => return Some((Err(e), state)),
            None => {
                state.current = None;
            }
        }
    }
}

/// Snapshot the tip, run `op`, then wait for the first item satisfying
/// `predicate` that arrives at-or-after the snapshot.
///
/// Race-safe: items that appeared between snapshot and op-completion are
/// still observed because the snapshot defines the lower bound, not the
/// op's start time.
pub async fn act_and_wait<T, P, R, OpFut, OpFn, Pred>(
    store: &HistStore<T, P>,
    op: OpFn,
    predicate: Pred,
) -> Result<(Positioned<T, P>, R), HistoryError>
where
    T: Send + Sync + 'static,
    P: Position,
    OpFn: FnOnce() -> OpFut,
    OpFut: std::future::Future<Output = R>,
    Pred: Fn(&T, &R) -> bool + Send + Sync,
{
    let snapshot = store.next().await;
    let anchor = snapshot.end().clone();
    let result = op().await;

    let mut stream = watch_hist(store, anchor);
    while let Some(next) = stream.next().await {
        let item = next?;
        if predicate(&item.item, &result) {
            return Ok((item, result));
        }
    }
    Err(HistoryError::internal(
        "act_and_wait: stream ended without match",
    ))
}
