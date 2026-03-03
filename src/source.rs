use crate::cursor::ChainCursor;
use crate::event::Event;

/// The single trait that all chain adapters implement.
///
/// EventSource combines three operations:
/// - `fetch_state`: retrieve initial state snapshot at a cursor position (cold start)
/// - `fetch_events`: retrieve historical events in a cursor range [from, to)
/// - `subscribe`: subscribe to live events from a cursor position onwards
///
/// Using `#[async_trait]` for Phase 1 ergonomics. The macro boxes futures on the heap
/// (~10-50ns overhead per call), acceptable for Phase 1. Migration to `trait_variant`
/// for zero-cost dispatch is deferred to Phase 3 hot path optimization.
///
/// The trait requires `Send + Sync` on the implementor, enabling use with `tokio::spawn`.
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    /// The cursor type for this source. Determines ordering semantics.
    type Cursor: ChainCursor;

    /// The user's event payload type. Opaque to the library.
    type Event: Send + 'static;

    /// The error type for this source. Implementations choose their own error type;
    /// callers can use SourceError as a type-erased wrapper if needed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch the initial state snapshot at a specific cursor position.
    ///
    /// Returns opaque bytes — the library does not interpret them. The caller owns
    /// serialization and deserialization of the snapshot. On cold start, the library
    /// calls this once and passes the bytes to the client's restore function.
    async fn fetch_state(&self, at: &Self::Cursor) -> Result<Vec<u8>, Self::Error>;

    /// Fetch events in the half-open range [from, to).
    ///
    /// Returns events in cursor order. The library relies on this invariant for
    /// correct state reconstruction. Implementations must not return events
    /// outside the specified range.
    async fn fetch_events(
        &self,
        from: &Self::Cursor,
        to: &Self::Cursor,
    ) -> Result<Vec<Event<Self::Cursor, Self::Event>>, Self::Error>;

    /// Subscribe to live events starting from the given cursor.
    ///
    /// Returns a boxed stream of events. The implementor owns the connection lifecycle
    /// (WebSocket, polling loop, etc.). The stream yields events in cursor order.
    ///
    /// `Box<dyn Stream>` is used because `async_trait` does not support `impl Trait`
    /// in return position. This is the accepted tradeoff for Phase 1 ergonomics.
    async fn subscribe(
        &self,
        from: &Self::Cursor,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Event<Self::Cursor, Self::Event>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    >;
}
