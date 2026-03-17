//! Implementation-side building blocks for chainfold backends.
//!
//! Currently provides [`PositionedEvent`] (event + position pair) and
//! [`Reducer`] (state fold with external state fetching). Traits for
//! Source and EventStore are intentionally deferred until concrete
//! implementations (EVM, BTC) reveal the right abstractions.
//!
//! See [`arch`](crate::arch) for the public consumer API.

use crate::arch::{HistoryError, Position};

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

/// State fold with external state fetching: `(State, Event) -> State`.
///
/// Used by [`ReducedHistory`](crate::reduce::ReducedHistory) to derive
/// a state history from an event history. `apply` is pure; `fetch_state`
/// may do I/O (e.g. RPC call) to obtain ground-truth state at an
/// arbitrary position.
///
/// No `initial()` — `fetch_state(earliest())` replaces it. A reducer
/// that can't fetch state at an arbitrary position can't be used
/// meaningfully; replaying from genesis is never acceptable.
///
/// ```ignore
/// struct BalanceReducer { ... }
///
/// #[async_trait::async_trait]
/// impl Reducer<EvmPosition> for BalanceReducer {
///     type Event = EvmEvent;
///     type State = U256;
///
///     fn apply(&self, balance: U256, event: &EvmEvent) -> U256 {
///         // parse transfer, add/subtract
///         balance
///     }
///
///     async fn fetch_state(&self, pos: &EvmPosition) -> Result<U256, HistoryError> {
///         // eth_call balanceOf at pos.block_number
///         Ok(U256::ZERO)
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Reducer<P: Position>: Send + Sync + 'static {
    type Event: Clone + Send + Sync + 'static;
    type State: Clone + Send + Sync + 'static;

    /// Apply one event, producing the next state.
    fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State;

    /// Fetch state at a position from an external source (e.g., RPC call).
    /// Used when no nearby cached state exists.
    async fn fetch_state(&self, position: &P) -> Result<Self::State, HistoryError>;

    /// Find the closest externally-cached state at or before `pos`.
    /// Returns None if no cached state exists outside the in-memory cache.
    /// Used to find efficient replay starting points across sessions.
    fn find_cached_state_floor(&self, _pos: &P) -> Option<(P, Self::State)> {
        None
    }
}
