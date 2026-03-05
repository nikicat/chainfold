use crate::block::Block;
use crate::cursor::ChainCursor;

/// The single trait that all chain adapters implement.
///
/// EventSource combines three operations:
/// - `fetch_state`: retrieve initial state snapshot at a block height (cold start)
/// - `fetch_blocks`: retrieve historical blocks in a height range [from, to)
/// - `subscribe`: subscribe to live blocks from a height position onwards
///
/// Using `#[async_trait]` for ergonomics. The macro boxes futures on the heap
/// (~10-50ns overhead per call), acceptable for this phase.
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

    /// Fetch the initial state snapshot.
    ///
    /// Returns opaque bytes — the library does not interpret them. The caller owns
    /// serialization and deserialization of the snapshot. On cold start, the library
    /// calls this once and passes the bytes to the client's restore function.
    async fn fetch_state(&self, at: &Self::Cursor) -> Result<Vec<u8>, Self::Error>;

    /// Fetch blocks in the half-open height range [from_height, to_height).
    ///
    /// Returns blocks in height order. The library relies on this invariant for
    /// correct state reconstruction. Implementations must not return blocks
    /// outside the specified range.
    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<Self::Cursor, Self::Event>>, Self::Error>;

    /// Subscribe to live blocks starting from the given height.
    ///
    /// Returns a boxed stream of blocks. The implementor owns the connection lifecycle
    /// (WebSocket, polling loop, etc.). The stream yields blocks in height order.
    ///
    /// `Box<dyn Stream>` is used because `async_trait` does not support `impl Trait`
    /// in return position.
    async fn subscribe(
        &self,
        from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<Self::Cursor, Self::Event>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    >;
}
