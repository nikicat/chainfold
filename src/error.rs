/// Generic error wrapper for EventSource method failures.
///
/// Adapters wrap their chain-specific errors into SourceError via the From impl.
/// The library never inspects the inner error — it propagates it to the caller.
///
/// The `Box<dyn Error + Send + Sync>` bound ensures SourceError itself is Send + Sync,
/// which is required for use across async boundaries.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct SourceError(#[from] pub Box<dyn std::error::Error + Send + Sync>);
