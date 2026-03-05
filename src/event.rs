use crate::cursor::ChainCursor;

/// Library-owned envelope wrapping a user event payload.
///
/// Event carries the cursor (ordering position) and the user's event payload.
/// Block context (block hash, height, parent hash) lives on Block<C, E>, not here.
/// Events are always accessed through their containing Block.
///
/// Generic parameters:
/// - C: ChainCursor — the cursor type for this source (EvmCursor, BtcCursor, CexCursor)
/// - E — the user-defined event payload type
///
/// C is listed first as it is the more constrained type (bound by ChainCursor).
#[derive(Debug, Clone)]
pub struct Event<C: ChainCursor, E> {
    /// Position in the event stream. Uniquely identifies this event's place in the chain order.
    pub cursor: C,

    /// The user's event payload. Opaque to the library.
    pub data: E,
}
