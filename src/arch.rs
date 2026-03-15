//! chainfold — public API.
//!
//! Idiomatic Rust translation of `arch.md` pseudocode.
//! Implementation-side traits (Source, EventStore, Reducer) live in
//! [`provider`](crate::provider).

use std::pin::Pin;
use std::sync::Arc;

use futures_util::StreamExt;

// ============================================================
// Core type aliases
// ============================================================

/// Unix timestamp in seconds.
pub type Timestamp = u64;

/// Boxed async stream, used by [`Store::subscribe`].
pub type Subscription<T> = Pin<Box<dyn futures_core::Stream<Item = T> + Send>>;

// ============================================================
// SearchEvent
// ============================================================

/// Progress update from a [`search_after`](HistoryBackend::search_after) or
/// [`search_before`](HistoryBackend::search_before) stream.
pub enum SearchEvent<T: Send + Sync + 'static, P: Position> {
    /// Source has scanned up to this position with no event found yet.
    /// Emitted after each chunk (eth_getLogs response, new block, etc.).
    Scanned(P),
    /// First matching event found. Definitive — no closer event exists
    /// in this source. Stream ends after this.
    Found(Cursor<T, P>),
}

/// Progressive search stream. Emits [`SearchEvent`]s as scanning progresses.
///
/// **Cancellation**: drop the stream to stop the search. Sources detect
/// the dropped receiver and stop fetching.
pub type SearchStream<T, P> =
    Pin<Box<dyn futures_core::Stream<Item = Result<SearchEvent<T, P>, HistoryError>> + Send>>;

// ============================================================
// Side
// ============================================================

/// Whether a position sits just before or just after an event coordinate.
///
/// Between two adjacent cursors there are always two distinct positions:
/// the [`After`](Side::After) of the first cursor and the [`Before`](Side::Before)
/// of the second. These two positions are *equivalent*
/// ([`History::is_equiv`]) even though they are not equal.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Side {
    /// Just before the event at this coordinate.
    Before,
    /// Just after the event at this coordinate.
    After,
}

// ============================================================
// HistoryError
// ============================================================

/// Errors returned by [`History`] and [`Cursor`] operations.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// The referenced position belongs to a fork that is no longer canonical.
    #[error("chain reorg: position no longer on canonical chain")]
    ChainReorg,

    /// Storage, serialization, or other internal error.
    #[error(transparent)]
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct InternalMsg(String);

impl HistoryError {
    pub(crate) fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(Box::new(InternalMsg(msg.into())))
    }
}

// ============================================================
// Position
// ============================================================

/// A separator in a [`History`] — divides it into "before" and "after".
///
/// Does **not** point to a specific event (that's [`Cursor`]'s job).
/// An empty history has positions ([`earliest`](History::earliest),
/// [`never`](History::never)) but no cursors.
///
/// Positions are **not globally ordered**. During reorgs, positions on
/// different branches are incomparable. Use [`History::is_equiv`] and
/// [`History::lca`] for comparison.
///
/// Positions are bound to the [`History`] that created them — using a
/// position from one history on another is undefined.
pub trait Position: Clone + Send + Sync + 'static {
    /// Identifies which source (chain, exchange, etc.) this position belongs to.
    fn source_id(&self) -> u64;

    /// Serialize for persistence. Reconstruct with [`unmarshal`](Self::unmarshal).
    fn marshal(&self) -> Vec<u8>;

    /// Deserialize a previously marshalled position. Returns `None` on
    /// invalid data.
    fn unmarshal(data: &[u8]) -> Option<Self>;

    /// Which side of an event coordinate this position sits on.
    fn side(&self) -> Side;

    /// Same coordinates, different side.
    fn with_side(&self, side: Side) -> Self;
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
    pub(crate) fn new(rx: tokio::sync::watch::Receiver<T>) -> Self {
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
// Cursor
// ============================================================

/// Points to a specific item in a [`History`].
///
/// Each cursor has two bounding positions: [`before`](Self::before) and
/// [`after`](Self::after). Navigate with [`prev`](Self::prev) /
/// [`next`](Self::next).
///
/// ```text
///   ... ─ [cursor_prev] ─ (before) ─ [THIS CURSOR] ─ (after) ─ [cursor_next] ─ ...
/// ```
pub struct Cursor<T, P: Position> {
    value: Arc<T>,
    before: P,
    after: P,
    backend: Arc<dyn HistoryBackend<T, P>>,
}

impl<T, P: Position> Clone for Cursor<T, P> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            before: self.before.clone(),
            after: self.after.clone(),
            backend: self.backend.clone(),
        }
    }
}

impl<T: Send + Sync + 'static, P: Position> Cursor<T, P> {
    pub(crate) fn new(
        value: T,
        before: P,
        after: P,
        backend: Arc<dyn HistoryBackend<T, P>>,
    ) -> Self {
        Self {
            value: Arc::new(value),
            before,
            after,
            backend,
        }
    }

    /// Re-wrap this cursor to point at a different backend.
    ///
    /// Used by MergedHistory to make source cursors navigate via the
    /// merged backend instead of the original source.
    pub(crate) fn with_backend(self, backend: Arc<dyn HistoryBackend<T, P>>) -> Self {
        Self { backend, ..self }
    }

    /// The event or state at this position.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Position just before this item.
    ///
    /// `history.after(cursor.before(), None)` returns a cursor pointing
    /// to the same item.
    pub fn before(&self) -> &P {
        &self.before
    }

    /// Position just after this item.
    ///
    /// `history.before(cursor.after(), None)` returns a cursor pointing
    /// to the same item.
    pub fn after(&self) -> &P {
        &self.after
    }

    /// Previous item in the history.
    ///
    /// Shorthand for `history.before(self.before(), None)`.
    /// Returns `None` at the beginning of history.
    pub async fn prev(&self) -> Result<Option<Cursor<T, P>>, HistoryError> {
        self.backend.before(&self.before, None).await
    }

    /// Next item in the history.
    ///
    /// Shorthand for `history.after(self.after(), None)`.
    /// **Blocks at the tip** waiting for new events on live histories.
    /// Returns `None` only for static/closed histories.
    pub async fn next(&self) -> Result<Option<Cursor<T, P>>, HistoryError> {
        self.backend.after(&self.after, None).await
    }

    /// True if this cursor comes after position `p` on the canonical chain.
    ///
    /// Returns `false` if `p` is on a different branch.
    pub async fn is_after(&self, p: &P) -> Result<bool, HistoryError> {
        self.backend.is_after(&self.after, p).await
    }

    /// True if this cursor comes before position `p` on the canonical chain.
    ///
    /// Returns `false` if `p` is on a different branch.
    pub async fn is_before(&self, p: &P) -> Result<bool, HistoryError> {
        self.backend.is_before(&self.before, p).await
    }
}

// ============================================================
// History
// ============================================================

/// Lazy, cached, bidirectional stream of events or states.
///
/// The central abstraction of chainfold. All variants — source-backed,
/// reduced, merged — share this interface.
///
/// ```ignore
/// // Reorg-safe processing loop (see arch.md for full example):
/// let pos = transfers.unmarshal(&load_anchor())?;
/// let lca = transfers.lca(&pos, &transfers.safe().get()).await?;
///
/// // Rollback reorged cursors
/// let mut curs = transfers.before(&pos, None).await?;
/// while let Some(c) = curs {
///     if transfers.is_equiv(c.before(), &lca).await? { break; }
///     revert(c.value());
///     curs = c.prev().await?;
/// }
///
/// // Replay forward
/// let mut curs = transfers.after(&lca, None).await?;
/// while let Some(c) = curs {
///     process(c.value());
///     save_anchor(&c.after().marshal());
///     curs = c.next().await?;
/// }
/// ```
pub struct History<T, P: Position> {
    inner: Arc<dyn HistoryBackend<T, P>>,
}

impl<T, P: Position> Clone for History<T, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Send + Sync + 'static, P: Position> History<T, P> {
    pub(crate) fn new(backend: Arc<dyn HistoryBackend<T, P>>) -> Self {
        Self { inner: backend }
    }

    // -- Reactive tip positions --------------------------------

    /// Updates as new events arrive (includes mempool/unconfirmed).
    pub fn latest(&self) -> &Store<P> {
        self.inner.latest()
    }

    /// Updates as events are mined into blocks.
    pub fn best(&self) -> &Store<P> {
        self.inner.best()
    }

    /// Updates as blocks finalize (no further reorgs possible).
    pub fn safe(&self) -> &Store<P> {
        self.inner.safe()
    }

    // -- Source identity ----------------------------------------

    /// Identifies which source this history belongs to.
    pub fn source_id(&self) -> u64 {
        self.inner.source_id()
    }

    // -- Sentinel positions ------------------------------------

    /// Beginning-of-time sentinel.
    ///
    /// `history.after(&history.earliest(), None)` returns the first cursor.
    pub fn earliest(&self) -> P {
        self.inner.earliest()
    }

    /// Past-the-end sentinel. No cursor ever comes after this position.
    pub fn never(&self) -> P {
        self.inner.never()
    }

    // -- Position creation -------------------------------------

    /// Approximate position at a wall-clock time on the canonical chain.
    ///
    /// Useful as a cutoff for [`after`](Self::after) / [`before`](Self::before).
    /// Does not need to be exact — navigation resolves against actual data.
    pub fn at(&self, time: Timestamp) -> P {
        self.inner.at(time)
    }

    // -- Comparison --------------------------------------------

    /// True if `a` and `b` are equivalent positions.
    ///
    /// Two positions are equivalent when they sit in the same gap:
    /// navigating forward or backward from either yields the same cursor
    /// (or `None` at boundaries).
    pub async fn is_equiv(&self, a: &P, b: &P) -> Result<bool, HistoryError> {
        self.inner.is_equiv(a, b).await
    }

    /// Latest common ancestor of two positions.
    ///
    /// Always exists because [`earliest`](Self::earliest) is an ancestor of
    /// every position. Useful for reorg recovery: roll back to the LCA,
    /// then replay forward on the canonical chain.
    pub async fn lca(&self, a: &P, b: &P) -> Result<P, HistoryError> {
        self.inner.lca(a, b).await
    }

    // -- Navigation --------------------------------------------

    /// Progressive forward search stream.
    ///
    /// Emits [`SearchEvent::Scanned`] as chunks are scanned, then
    /// [`SearchEvent::Found`] when an event is located. Drop the stream
    /// to cancel.
    pub fn search_after(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P> {
        self.inner.search_after(position, cutoff)
    }

    /// Progressive backward search stream.
    pub fn search_before(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P> {
        self.inner.search_before(position, cutoff)
    }

    /// First cursor after `position`, or `None` if none exists before `cutoff`.
    ///
    /// `cutoff = None` means unbounded — **blocks at the tip** for live
    /// histories, waiting for new events.
    pub async fn after(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<T, P>>, HistoryError> {
        self.inner.after(position, cutoff).await
    }

    /// Last cursor before `position`, or `None` if none exists after `cutoff`.
    ///
    /// `cutoff = None` means unbounded (back to [`earliest`](Self::earliest)).
    pub async fn before(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<T, P>>, HistoryError> {
        self.inner.before(position, cutoff).await
    }

    // -- Combinators -------------------------------------------

    /// Merge N histories (same event and position type) into one
    /// interleaved history ordered by `pos_cmp`.
    ///
    /// Events from all sources appear in the merged output. Duplicates
    /// (same position in multiple sources) are naturally deduplicated
    /// because all sources share the same position space.
    ///
    /// ```ignore
    /// let merged = History::merge(vec![incoming, outgoing], |a, b| {
    ///     (a.block_number, a.log_index).cmp(&(b.block_number, b.log_index))
    /// });
    /// ```
    pub fn merge(
        sources: Vec<Self>,
        pos_cmp: fn(&P, &P) -> std::cmp::Ordering,
    ) -> Self {
        crate::merge::MergedHistory::into_history(sources, pos_cmp)
    }

    // -- Serialization -----------------------------------------

    /// Deserialize a position previously saved with [`Position::marshal`].
    pub fn unmarshal(&self, data: &[u8]) -> Result<P, HistoryError> {
        P::unmarshal(data).ok_or_else(|| HistoryError::internal("invalid position data"))
    }
}

// ============================================================
// HistoryBackend (internal)
// ============================================================

/// Internal trait backing [`History`] and [`Cursor`] operations.
///
/// Not part of the public API. Implemented by source-backed histories,
/// reducer-derived histories, and merged histories.
///
/// ## Required: `search_after` / `search_before`
///
/// These return a [`SearchStream`] that progressively reports scanning
/// progress and eventually yields a result. They are **not** async —
/// they synchronously return a stream. The actual work happens when
/// the stream is polled.
///
/// ## Provided: `after` / `before`
///
/// Default implementations that consume the search stream to completion,
/// returning only the final `Found` cursor (or `None`).
#[async_trait::async_trait]
pub(crate) trait HistoryBackend<T: Send + Sync + 'static, P: Position>: Send + Sync {
    fn source_id(&self) -> u64;
    fn earliest(&self) -> P;
    fn never(&self) -> P;

    fn latest(&self) -> &Store<P>;
    fn best(&self) -> &Store<P>;
    fn safe(&self) -> &Store<P>;

    fn at(&self, time: Timestamp) -> P;

    async fn is_equiv(&self, a: &P, b: &P) -> Result<bool, HistoryError>;
    async fn lca(&self, a: &P, b: &P) -> Result<P, HistoryError>;

    /// Progressive forward search. Returns a stream of [`SearchEvent`]s.
    ///
    /// The stream emits `Scanned(pos)` as each chunk is scanned, then
    /// `Found(cursor)` when the first matching event is located. The stream
    /// ends after `Found` or when the search space is exhausted.
    ///
    /// `cutoff = None` means unbounded — the stream should block at the
    /// tip waiting for new events (for live histories).
    fn search_after(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P>;

    /// Progressive backward search. Same contract as [`search_after`] but
    /// scans in reverse.
    fn search_before(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P>;

    /// First cursor after `position`. Default impl consumes [`search_after`].
    async fn after(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<T, P>>, HistoryError> {
        let mut stream = self.search_after(position, cutoff);
        while let Some(event) = stream.next().await {
            match event? {
                SearchEvent::Scanned(_) => continue,
                SearchEvent::Found(cursor) => return Ok(Some(cursor)),
            }
        }
        Ok(None)
    }

    /// Last cursor before `position`. Default impl consumes [`search_before`].
    async fn before(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<T, P>>, HistoryError> {
        let mut stream = self.search_before(position, cutoff);
        while let Some(event) = stream.next().await {
            match event? {
                SearchEvent::Scanned(_) => continue,
                SearchEvent::Found(cursor) => return Ok(Some(cursor)),
            }
        }
        Ok(None)
    }

    async fn is_after(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError>;
    async fn is_before(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError>;
}

// ============================================================
// EVM types
// ============================================================

pub mod evm {
    use super::*;

    // --------------------------------------------------------
    // EvmPosition
    // --------------------------------------------------------

    /// Position in an EVM event stream.
    ///
    /// `block_hash` disambiguates forks (same `block_number` + `log_index`
    /// can exist on different chains during a reorg). `timestamp` is
    /// carried for convenience ([`time()`](Self::time)).
    #[derive(Debug, Clone, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct EvmPosition {
        pub chain_id: u64,
        pub block_number: u64,
        pub log_index: u32,
        pub block_hash: [u8; 32],
        pub timestamp: Timestamp,
        pub side: Side,
    }

    impl EvmPosition {
        /// Block timestamp at this position.
        pub fn time(&self) -> Timestamp {
            self.timestamp
        }

        /// Beginning-of-time sentinel for this chain.
        pub fn earliest(chain_id: u64) -> Self {
            Self {
                chain_id,
                block_number: 0,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::Before,
            }
        }

        /// Past-the-end sentinel for this chain.
        pub fn never(chain_id: u64) -> Self {
            Self {
                chain_id,
                block_number: u64::MAX,
                log_index: u32::MAX,
                block_hash: [0xff; 32],
                timestamp: u64::MAX,
                side: Side::After,
            }
        }
    }

    impl Position for EvmPosition {
        fn source_id(&self) -> u64 {
            self.chain_id
        }

        fn marshal(&self) -> Vec<u8> {
            postcard::to_allocvec(self).expect("EvmPosition serialization cannot fail")
        }

        fn unmarshal(data: &[u8]) -> Option<Self> {
            postcard::from_bytes(data).ok()
        }

        fn side(&self) -> Side {
            self.side
        }

        fn with_side(&self, side: Side) -> Self {
            Self { side, ..self.clone() }
        }
    }

    /// Convenience aliases.
    pub type EvmCursor<T> = Cursor<T, EvmPosition>;
    pub type EvmHistory<T> = History<T, EvmPosition>;

    // --------------------------------------------------------
    // Raw EVM event
    // --------------------------------------------------------

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
    // EvmChain
    // --------------------------------------------------------

    /// Entry point for an EVM-compatible blockchain.
    pub struct EvmChain {
        _private: (),
    }

    impl EvmChain {
        pub fn blocks(&self) -> &EvmHistory<EvmBlock> {
            todo!()
        }

        pub fn transactions(&self) -> &EvmHistory<EvmTransaction> {
            todo!()
        }

        pub async fn contract(
            &self,
            _address: [u8; 20],
        ) -> Result<Arc<EvmContract>, HistoryError> {
            todo!()
        }

        pub async fn erc20(&self, _address: [u8; 20]) -> Result<Arc<Erc20>, HistoryError> {
            todo!()
        }
    }

    // --------------------------------------------------------
    // EvmContract
    // --------------------------------------------------------

    pub struct EvmContract {
        pub address: [u8; 20],
        _chain: Arc<EvmChain>,
    }

    impl std::fmt::Debug for EvmContract {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("EvmContract")
                .field("address", &self.address)
                .finish()
        }
    }

    impl EvmContract {
        pub fn states(&self) -> &EvmHistory<Vec<u8>> {
            todo!()
        }

        pub fn created_at(&self) -> &EvmPosition {
            todo!()
        }
    }

    // --------------------------------------------------------
    // Asset / Amount
    // --------------------------------------------------------

    pub trait Asset: Send + Sync {
        fn symbol(&self) -> &str;
        fn name(&self) -> &str;
    }

    /// Asset-aware fixed-point amount.
    ///
    /// Carries an [`Asset`] reference so that arithmetic can enforce
    /// same-asset operations at runtime.
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
    // Erc20
    // --------------------------------------------------------

    #[derive(Debug)]
    pub struct Erc20 {
        pub contract: EvmContract,
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

    impl Erc20 {
        pub fn transfers(
            &self,
            _from: Option<[u8; 20]>,
            _to: Option<[u8; 20]>,
        ) -> EvmHistory<Erc20Transfer> {
            todo!()
        }

        pub fn mints(&self, _to: [u8; 20]) -> EvmHistory<Erc20Mint> {
            todo!()
        }

        pub fn burns(&self, _from: [u8; 20]) -> EvmHistory<Erc20Burn> {
            todo!()
        }

        /// Returns the singleton `OwnedErc20` for this owner.
        pub fn owned_by(self: &Arc<Self>, _owner: [u8; 20]) -> OwnedErc20 {
            todo!()
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
    // OwnedErc20
    // --------------------------------------------------------

    /// ERC20 events and states scoped to a specific account.
    pub struct OwnedErc20 {
        pub token: Arc<Erc20>,
        pub owner: [u8; 20],
    }

    impl OwnedErc20 {
        /// All transfers involving this account (in + out combined).
        pub fn transfers(&self) -> EvmHistory<Erc20Transfer> {
            todo!()
        }

        pub fn mints(&self) -> EvmHistory<Erc20Mint> {
            todo!()
        }

        pub fn burns(&self) -> EvmHistory<Erc20Burn> {
            todo!()
        }

        /// Balance history — reduced from transfer/mint/burn events.
        pub fn balances(&self) -> EvmHistory<Erc20Balance> {
            todo!()
        }
    }

    // --------------------------------------------------------
    // Erc20Balance (reduced state)
    // --------------------------------------------------------

    #[derive(Clone)]
    pub struct Erc20Balance {
        pub token: Arc<Erc20>,
        pub owner: [u8; 20],
        pub balance: Amount,
    }
}
