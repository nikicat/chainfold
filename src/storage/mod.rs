/// Storage abstraction for persisting events, snapshots, anchors, and coverage ranges.
///
/// The Storage trait is synchronous — callers in async contexts use `tokio::task::spawn_blocking`
/// to bridge to async. This avoids the version-pinning issues with tokio-rusqlite and keeps
/// the storage layer simple and testable.
///
/// Cursors are represented as `(i64, i64)` tuples (major, minor). The coordinator maps
/// concrete ChainCursor types to/from this canonical INTEGER pair representation.
pub mod memory;
pub mod schema;
pub mod sqlite;

pub use memory::InMemoryStorage;
pub use sqlite::SqliteStorage;

// ============================================================
// Core types
// ============================================================

/// A persisted event payload with opaque bytes.
///
/// The payload is serialized externally by the coordinator (using postcard).
/// Storage treats it as opaque bytes.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub source_id: String,
    pub cursor_major: i64,
    pub cursor_minor: i64,
    pub payload: Vec<u8>,
}

/// A persisted state machine snapshot.
///
/// Snapshots enable fast recovery: replay only from the nearest snapshot
/// rather than replaying all events from genesis.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub source_id: String,
    pub cursor_major: i64,
    pub cursor_minor: i64,
    pub payload: Vec<u8>,
    /// Unix epoch seconds when this snapshot was taken.
    pub created_at: i64,
}

/// A named cursor bookmark for a source.
///
/// Anchors track positions like "finalized_head" or "last_processed".
/// They support upsert — writing to the same (source_id, name) updates the cursor.
#[derive(Debug, Clone)]
pub struct Anchor {
    pub source_id: String,
    pub name: String,
    pub cursor_major: i64,
    pub cursor_minor: i64,
}

// ============================================================
// Storage trait
// ============================================================

/// Sync storage trait. Implement for any durable or in-memory backend.
///
/// All methods are synchronous. Use `tokio::task::spawn_blocking` to call from async contexts.
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    // ---- Events ----

    /// Write events to storage. Returns the count of rows actually inserted.
    ///
    /// Duplicate events (same source_id + cursor_major + cursor_minor) are silently dropped.
    /// The returned count reflects actual inserts, not the batch size — critical for
    /// accurate snapshot triggering.
    fn write_events(&self, source_id: &str, events: &[StoredEvent]) -> Result<usize, Self::Error>;

    /// Query events in the cursor range [from, to) — inclusive start, exclusive end.
    ///
    /// Results are ordered by (cursor_major, cursor_minor) ascending.
    fn events_in_range(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<Vec<StoredEvent>, Self::Error>;

    // ---- Snapshots ----

    /// Persist a snapshot. Multiple snapshots per source are allowed.
    fn write_snapshot(&self, snapshot: &Snapshot) -> Result<(), Self::Error>;

    /// Return the most recent snapshot for a source, or None if none exists.
    fn latest_snapshot(&self, source_id: &str) -> Result<Option<Snapshot>, Self::Error>;

    /// Return all snapshots at or before the given cursor, newest first.
    fn snapshots_before(
        &self,
        source_id: &str,
        cursor: (i64, i64),
    ) -> Result<Vec<Snapshot>, Self::Error>;

    // ---- Anchors ----

    /// Write or update an anchor. Upserts on (source_id, name).
    fn write_anchor(&self, anchor: &Anchor) -> Result<(), Self::Error>;

    /// Read the named anchor for a source, or None if not set.
    fn read_anchor(&self, source_id: &str, name: &str) -> Result<Option<Anchor>, Self::Error>;

    // ---- Coverage ----

    /// Record that the range [from, to) has been fetched and stored.
    ///
    /// Adjacent and overlapping intervals are merged into a single interval.
    fn add_coverage(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<(), Self::Error>;

    /// Return true if [from, to) is fully covered by persisted intervals.
    fn is_fully_cached(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<bool, Self::Error>;

    /// Return the highest range_end across all coverage intervals for this source.
    fn cached_tip(&self, source_id: &str) -> Result<Option<(i64, i64)>, Self::Error>;
}
