use crate::cursor::ChainCursor;

/// Library-owned envelope wrapping a user event payload.
///
/// The library adds ordering metadata (cursor) and optional chain metadata (block_hash).
/// The user's event type E is opaque to the library — no Serialize bound is required on E
/// in Phase 1, as events flow as native Rust types without library-level serialization.
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

    /// Block hash at the cursor position. None for CEX sources (no concept of blocks).
    /// Used for reorg detection in later phases.
    pub block_hash: Option<[u8; 32]>,
}
