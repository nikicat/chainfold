//! Implementation-side building blocks for chainfold backends.
//!
//! Currently provides [`PositionedEvent`] (event + position pair) and
//! [`Reducer`] (pure state fold). Traits for Source and EventStore are
//! intentionally deferred until concrete implementations (EVM, BTC)
//! reveal the right abstractions.
//!
//! See [`arch`](crate::arch) for the public consumer API.

use crate::arch::Position;

// ============================================================
// PositionedEvent
// ============================================================

/// An event paired with its position in the stream.
///
/// Used by concrete source implementations and cache layers to pass
/// events with their coordinates. The history backend creates
/// [`Cursor`](crate::arch::Cursor) before/after positions from
/// `position` using [`Position::with_side`].
#[derive(Debug, Clone)]
pub struct PositionedEvent<P, T> {
    pub position: P,
    pub data: T,
}

impl<P: Position, T> PositionedEvent<P, T> {
    pub fn new(position: P, data: T) -> Self {
        Self { position, data }
    }
}

// ============================================================
// Reducer
// ============================================================

/// Pure state fold: `(State, Event) -> State`.
///
/// Used to derive a state [`History`](crate::arch::History) from an
/// event history. No I/O, no side effects.
///
/// `initial()` returns the genesis state (before any events). For
/// backfilling from a non-genesis position, the orchestrator provides
/// the starting state — the reducer doesn't need to know where it
/// came from.
///
/// ```ignore
/// struct BalanceReducer;
///
/// impl Reducer for BalanceReducer {
///     type Event = Erc20Transfer;
///     type State = u128;
///
///     fn initial(&self) -> u128 { 0 }
///
///     fn apply(&self, balance: u128, transfer: &Erc20Transfer) -> u128 {
///         // simplified — real impl checks from/to
///         balance + transfer.amount.raw
///     }
/// }
/// ```
pub trait Reducer: Send + Sync + 'static {
    type Event;
    type State: Clone + Send + Sync;

    /// Starting state before any events (genesis).
    fn initial(&self) -> Self::State;

    /// Apply one event, producing the next state.
    fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State;
}
