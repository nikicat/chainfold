pub mod memory;
pub mod schema;
pub mod sqlite;

pub use memory::InMemoryStorage;
pub use sqlite::SqliteStorage;

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

    /// Get the parent hash of a block.
    ///
    /// Returns `Some(parent_hash)` if the block exists. For genesis blocks, the
    /// parent is the all-zeros sentinel `[0u8; 32]` — this is still returned as
    /// `Some([0u8; 32])`. Returns `None` only if the block is not in storage.
    fn get_parent_hash(&self, block_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, Self::Error>;

    /// Explicitly set the canonical tip for a source to the given block hash.
    ///
    /// This is an upsert: overwrites the existing tip if one exists, or creates
    /// the entry if no tip has been set yet. Used during reorg handling to update
    /// the canonical tip without writing a new block.
    fn set_canonical_tip(&self, source_id: &str, block_hash: &[u8; 32]) -> Result<(), Self::Error>;
}
