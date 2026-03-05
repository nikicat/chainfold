pub mod recovery;

use crate::cursor::ChainCursor;

/// Bridges typed events/state to opaque byte representation for storage.
///
/// Client provides encode/decode for event payloads and state only.
/// Cursor serialization is library-internal (postcard).
/// No cursor_to_tuple/tuple_to_cursor — cursors stored as postcard bytes.
///
/// Note: encode_event/decode_event operate on the event PAYLOAD (Self::Event),
/// not the full Event<C, E> wrapper. The cursor is serialized separately by
/// the library. This is the key simplification from the old Codec.
pub trait Codec: Send + Sync + 'static {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type State: Clone + Send + 'static;

    /// Serialize event payload to bytes.
    fn encode_event(&self, event: &Self::Event) -> Vec<u8>;

    /// Deserialize event payload from bytes. Returns None on corrupt/incompatible data.
    fn decode_event(&self, payload: &[u8]) -> Option<Self::Event>;

    /// Serialize state to opaque bytes for snapshot storage.
    fn encode_state(&self, state: &Self::State) -> Vec<u8>;

    /// Deserialize state from opaque bytes. Returns None on corrupt/incompatible.
    fn decode_state(&self, bytes: &[u8]) -> Option<Self::State>;
}
