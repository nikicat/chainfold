pub mod schema;

// memory.rs is implemented in Plan 02-02.
// sqlite.rs is implemented in Plan 02-03.

/// Block DAG storage trait. All methods are synchronous.
/// Implementations wrap with Mutex<Connection> for thread safety.
///
/// The block DAG model stores blocks (with parent pointers) rather than a flat event log.
/// Canonical chain tracking is done via the canonical_tip table: one row per source_id
/// pointing to the current canonical head block_hash.
///
/// Recovery reads the canonical chain from a snapshot forward to the tip, replaying
/// events without re-fetching them from the source.
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Write a block (metadata + events) atomically. Returns true if the block
    /// was new (inserted), false if it was a duplicate (block_hash already exists).
    /// Updates canonical_tip to this block's hash in the same transaction.
    ///
    /// Deduplication is at block granularity: if block_hash exists, skip entirely.
    fn write_block(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        parent_hash: &[u8; 32],
        height: u64,
        events: &[(&[u8], &[u8])], // (cursor_bytes, payload_bytes) pairs
    ) -> Result<bool, Self::Error>;

    /// Get the canonical tip block hash for a source, or None if no blocks stored.
    fn get_canonical_tip(&self, source_id: &str) -> Result<Option<[u8; 32]>, Self::Error>;

    /// Get the height of a block by its hash, or None if the block doesn't exist.
    fn get_block_height(&self, block_hash: &[u8; 32]) -> Result<Option<u64>, Self::Error>;

    /// Write a state snapshot associated with a block hash.
    fn write_snapshot(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        payload: &[u8],
    ) -> Result<(), Self::Error>;

    /// Walk parent pointers from tip_hash backward until a block with a snapshot
    /// is found. Returns (block_hash, snapshot_payload) or None if no snapshot exists
    /// on the canonical chain (including genesis reached).
    fn find_nearest_snapshot(
        &self,
        source_id: &str,
        tip_hash: &[u8; 32],
    ) -> Result<Option<([u8; 32], Vec<u8>)>, Self::Error>;

    /// Collect block hashes on the canonical chain from from_hash (exclusive) to
    /// to_hash (inclusive), in forward order (oldest to newest).
    /// Used for replay: collect the path, then load events for each block.
    fn collect_canonical_chain(
        &self,
        from_hash: &[u8; 32],
        to_hash: &[u8; 32],
    ) -> Result<Vec<[u8; 32]>, Self::Error>;

    /// Load events for a specific block, in insertion order.
    /// Returns (cursor_bytes, payload_bytes) pairs.
    fn load_block_events(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>;
}
