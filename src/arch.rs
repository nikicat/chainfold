//! chainfold — public API.
//!
//! Source-agnostic core: a stream of items is exposed as a sequence of
//! [`Hist`] snapshots produced by a [`HistStore`]. Each [`Hist`] is finite,
//! immutable, and iterates items in the half-open interval `(start, end]`.
//! [`HistStore::gen`] yields a fresh [`Hist`] each time the source's tip
//! advances; consumers resume by [`split`](Hist::split)ing at a saved
//! position.
//!
//! ## Layering
//!
//! * **Generic core** (this module) knows nothing about EVM, Solana,
//!   Bitcoin, CEXes, or any other concrete source. Tests for the core use a
//!   synthetic position type, not [`evm::EvmPosition`].
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

// ============================================================
// Core type aliases
// ============================================================

/// Unix timestamp in seconds.
pub type Timestamp = u64;

/// Boxed async stream.
pub type Subscription<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// Stream of items yielded by [`Hist::iter`].
pub type ItemStream<T, P> = Subscription<Result<Positioned<T, P>, HistoryError>>;

/// Stream of snapshots yielded by [`HistStore::gen`].
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
/// the type's own `Default` value when that makes sense; sources document
/// their own convention (e.g. EVM uses `EvmPosition { block: 0, log_index:
/// 0 }`).
///
/// **Source-local.** Positions from different sources are not comparable
/// and the trait does not provide source identity — that concern lives on
/// [`HistStore`]. Heterogeneous merge (planned) wraps per-source positions
/// in a keyed structure, never compares them directly.
pub trait Position:
    Clone + Eq + Ord + std::fmt::Debug + Send + Sync + 'static
{
    /// Serialize for persistence. Reconstruct with [`unmarshal`](Self::unmarshal).
    fn marshal(&self) -> Vec<u8>;

    /// Deserialize a previously marshalled position. Returns `None` on
    /// invalid data.
    fn unmarshal(bytes: &[u8]) -> Option<Self>
    where
        Self: Sized;
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
// Store
// ============================================================

/// Reactive value that updates as upstream state changes.
///
/// Inspired by Svelte/Zustand stores.
pub struct Store<T: Clone + Send + Sync> {
    rx: tokio::sync::watch::Receiver<T>,
}

impl<T: Clone + Send + Sync> Clone for Store<T> {
    fn clone(&self) -> Self {
        Self { rx: self.rx.clone() }
    }
}

impl<T: Clone + Send + Sync + 'static> Store<T> {
    pub fn new(rx: tokio::sync::watch::Receiver<T>) -> Self {
        Self { rx }
    }

    /// Current value (non-blocking snapshot).
    pub fn get(&self) -> T {
        self.rx.borrow().clone()
    }

    /// Stream of value changes. The first item is always the current value.
    pub fn subscribe(&self) -> Subscription<T> {
        let rx = self.rx.clone();
        Box::pin(futures_util::stream::unfold(
            (rx, true),
            |(mut rx, first)| async move {
                if first {
                    let val = rx.borrow_and_update().clone();
                    Some((val, (rx, false)))
                } else {
                    rx.changed().await.ok()?;
                    let val = rx.borrow_and_update().clone();
                    Some((val, (rx, false)))
                }
            },
        ))
    }
}

// ============================================================
// AsyncDisposable
// ============================================================

/// Handle that cancels a background task on drop.
///
/// Returned by [`HistStore::subscribe`] and similar push-based APIs.
pub struct AsyncDisposable {
    cancel: tokio_util::sync::CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl AsyncDisposable {
    pub fn new(
        cancel: tokio_util::sync::CancellationToken,
        handle: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            cancel,
            handle: Some(handle),
        }
    }
}

impl Drop for AsyncDisposable {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(h) = self.handle.take() {
            h.abort();
        }
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
/// [`HistStore::next`]/[`HistStore::gen`].
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
        Self { backend, start, end }
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
    pub fn split(&self, p: P) -> (Self, Self) {
        let clamped = if p < self.start {
            self.start.clone()
        } else if p > self.end {
            self.end.clone()
        } else {
            p
        };
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

    /// Position just after the last item in `items`, or `None` if empty.
    ///
    /// Convenience for the "save resume anchor after a batch" pattern.
    pub fn pos_after(items: &[Positioned<T, P>]) -> Option<P> {
        items.last().map(|p| p.pos.clone())
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
        Self { backend: self.backend.clone() }
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

    /// Push-based variant of [`snapshots`](Self::snapshots) — invokes `cb`
    /// with each new snapshot. Drop the returned handle to cancel.
    pub fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<T, P>) + Send + Sync>,
    ) -> AsyncDisposable {
        self.backend.subscribe(cb)
    }

    /// Estimate a position at a wall-clock time. Used as a cut point in
    /// [`Hist::split`] when resuming from a time instead of an anchor.
    pub async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        self.backend.time_pos(time).await
    }

    /// Reactive tip stores (best-effort; sources without finality wiring
    /// fall back to `latest`).
    pub fn latest(&self) -> &Store<P> {
        self.backend.latest()
    }
    pub fn best(&self) -> &Store<P> {
        self.backend.best()
    }
    pub fn safe(&self) -> &Store<P> {
        self.backend.safe()
    }
}

// ============================================================
// Backend traits (implemented per source)
// ============================================================

/// Backing implementation for [`Hist`]. Implemented per source.
pub trait HistBackend<T: Send + Sync + 'static, P: Position>: Send + Sync {
    /// Stream items in `(start, end]` ascending.
    fn iter(&self, start: P, end: P) -> ItemStream<T, P>;
}

/// Backing implementation for [`HistStore`]. Implemented per source.
#[async_trait::async_trait]
pub trait HistStoreBackend<T: Send + Sync + 'static, P: Position>:
    Send + Sync
{
    /// Yield a fresh [`Hist`] each time the source's tip advances; first
    /// yield is the current snapshot.
    fn snapshots(&self) -> HistStream<T, P>;

    /// Current snapshot.
    async fn next(&self) -> Hist<T, P>;

    /// Push-based variant of [`snapshots`](Self::snapshots).
    fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<T, P>) + Send + Sync>,
    ) -> AsyncDisposable;

    /// Estimate a position at a wall-clock time.
    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError>;

    fn latest(&self) -> &Store<P>;
    fn best(&self) -> &Store<P>;
    fn safe(&self) -> &Store<P>;
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
    Box::pin(futures_util::stream::unfold(
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
    Err(HistoryError::internal("act_and_wait: stream ended without match"))
}

// ============================================================
// EVM types (concrete source — see src/evm/* for impl)
// ============================================================

pub mod evm {
    use super::*;

    // --------------------------------------------------------
    // EvmPosition — totally ordered (block, log_index)
    // --------------------------------------------------------

    /// Position in an EVM log stream. Totally ordered: `(block, log_index)`
    /// lexicographically.
    ///
    /// Items in a [`Hist<_, EvmPosition>`](super::Hist) are at positions in
    /// `(start, end]`. The lower-bound sentinel for "before all logs on
    /// this chain" is `EvmPosition { block: 0, log_index: 0 }` — no real
    /// EVM chain has a log at genesis-block / index-0 (genesis carries no
    /// transactions on any production chain).
    ///
    /// Layer note: this struct carries *only* the coordinates needed to
    /// totally-order events on one chain. Chain identity lives on the
    /// [`EvmLogHistory`](crate::evm::EvmLogHistory) producing the stream;
    /// block hashes live in [`LogStore`](crate::evm::LogStore); timestamps
    /// live on [`EvmEvent`].
    #[derive(
        Debug,
        Clone,
        Copy,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        Hash,
        Default,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct EvmPosition {
        pub block: u64,
        pub log_index: u32,
    }

    impl EvmPosition {
        pub const fn new(block: u64, log_index: u32) -> Self {
            Self { block, log_index }
        }

        /// Lower-bound sentinel: strictly less than every real log position.
        pub const fn earliest() -> Self {
            Self { block: 0, log_index: 0 }
        }
    }

    impl Position for EvmPosition {
        fn marshal(&self) -> Vec<u8> {
            postcard::to_allocvec(self).expect("EvmPosition serialization cannot fail")
        }

        fn unmarshal(bytes: &[u8]) -> Option<Self> {
            postcard::from_bytes(bytes).ok()
        }
    }

    // --------------------------------------------------------
    // Raw EVM event
    // --------------------------------------------------------

    /// A single log event. Carries its own (block, log_index, block_hash,
    /// timestamp) so consumers don't need to round-trip the store for them.
    #[derive(Debug, Clone)]
    pub struct EvmEvent {
        pub contract_address: [u8; 20],
        pub block_number: u64,
        pub timestamp: Timestamp,
        pub block_hash: [u8; 32],
        pub log_index: u32,
        pub tx_hash: [u8; 32],
        pub topics: Vec<[u8; 32]>,
        pub data: Vec<u8>,
    }

    impl EvmEvent {
        pub fn position(&self) -> EvmPosition {
            EvmPosition { block: self.block_number, log_index: self.log_index }
        }
    }

    // --------------------------------------------------------
    // Block / Transaction
    // --------------------------------------------------------

    #[derive(Debug, Clone)]
    pub struct EvmBlock {
        pub number: u64,
        pub hash: [u8; 32],
        pub parent_hash: [u8; 32],
        pub timestamp: Timestamp,
    }

    #[derive(Debug, Clone)]
    pub struct EvmTransaction {
        pub hash: [u8; 32],
        pub from: [u8; 20],
        pub to: Option<[u8; 20]>,
        pub value: u128,
    }

    // --------------------------------------------------------
    // Asset / Amount
    // --------------------------------------------------------

    pub trait Asset: Send + Sync {
        fn symbol(&self) -> &str;
        fn name(&self) -> &str;
    }

    /// Asset-aware fixed-point amount.
    #[derive(Clone)]
    pub struct Amount {
        pub raw: u128,
        pub decimals: u8,
        pub asset: Arc<dyn Asset>,
    }

    impl std::fmt::Debug for Amount {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Amount({} {})", self, self.asset.symbol())
        }
    }

    impl std::fmt::Display for Amount {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if self.decimals == 0 {
                return write!(f, "{}", self.raw);
            }
            let divisor = 10u128.pow(self.decimals as u32);
            let whole = self.raw / divisor;
            let frac = self.raw % divisor;
            write!(
                f,
                "{}.{:0>width$}",
                whole,
                frac,
                width = self.decimals as usize
            )
        }
    }

    // --------------------------------------------------------
    // ERC20 events
    // --------------------------------------------------------

    #[derive(Debug, Clone)]
    pub struct Erc20Transfer {
        pub event: EvmEvent,
        pub token: Arc<Erc20>,
        pub from: [u8; 20],
        pub to: [u8; 20],
        pub amount: Amount,
    }

    #[derive(Debug, Clone)]
    pub struct Erc20Mint {
        pub event: EvmEvent,
        pub token: Arc<Erc20>,
        pub to: [u8; 20],
        pub amount: Amount,
    }

    #[derive(Debug, Clone)]
    pub struct Erc20Burn {
        pub event: EvmEvent,
        pub token: Arc<Erc20>,
        pub from: [u8; 20],
        pub amount: Amount,
    }

    // --------------------------------------------------------
    // Erc20 / OwnedErc20 (metadata only — backends live in src/evm)
    // --------------------------------------------------------

    #[derive(Debug)]
    pub struct Erc20 {
        pub address: [u8; 20],
        pub symbol: String,
        pub name: String,
        pub decimals: u8,
    }

    impl Asset for Erc20 {
        fn symbol(&self) -> &str {
            &self.symbol
        }
        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Reduced state: a running account balance.
    #[derive(Clone)]
    pub struct Erc20Balance {
        pub token: Arc<Erc20>,
        pub owner: [u8; 20],
        pub balance: Amount,
    }
}
