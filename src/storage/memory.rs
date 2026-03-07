/// In-memory storage implementation for testing.
///
/// Implements the block DAG Storage trait without filesystem I/O.
/// Provides identical semantics to SqliteStorage:
/// - Block-level deduplication (block_hash is the dedup key)
/// - Canonical tip tracking per source_id
/// - Parent pointer traversal for snapshot finding
/// - Forward-ordered chain collection
///
/// Cloning InMemoryStorage produces a new handle to the same underlying
/// state (Arc<Mutex<Inner>>) — intentional for test harnesses that need
/// to share storage between an engine instance and an inspector.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::Storage;

// ============================================================
// Error type
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageError {
    #[error("internal lock poisoned")]
    Poisoned,
}

// ============================================================
// Internal state
// ============================================================

#[derive(Debug, Clone)]
struct StoredBlock {
    parent_hash: [u8; 32],
    height: u64,
    /// (cursor_bytes, payload_bytes) pairs in insertion order
    events: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Default)]
struct Inner {
    /// block_hash -> StoredBlock (stores all blocks regardless of source)
    blocks: HashMap<[u8; 32], StoredBlock>,
    /// source_id -> canonical tip block_hash
    canonical_tips: HashMap<String, [u8; 32]>,
    /// (source_id, block_hash) -> snapshot payload bytes
    snapshots: HashMap<(String, [u8; 32]), Vec<u8>>,
}

// ============================================================
// InMemoryStorage
// ============================================================

/// In-memory storage backed by `Arc<Mutex<Inner>>`.
///
/// Thread-safe: all methods lock the mutex for the duration of the operation.
#[derive(Clone)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<Inner>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

// The all-zeros hash is the genesis sentinel — it signals "no parent".
const ZERO_HASH: [u8; 32] = [0u8; 32];

impl Storage for InMemoryStorage {
    type Error = MemoryStorageError;

    fn write_block(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        parent_hash: &[u8; 32],
        height: u64,
        events: &[(&[u8], &[u8])],
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;

        // Block-level deduplication: if block_hash already exists, skip entirely.
        if inner.blocks.contains_key(block_hash) {
            return Ok(false);
        }

        let stored = StoredBlock {
            parent_hash: *parent_hash,
            height,
            events: events
                .iter()
                .map(|(c, p)| (c.to_vec(), p.to_vec()))
                .collect(),
        };

        inner.blocks.insert(*block_hash, stored);
        inner.canonical_tips.insert(source_id.to_string(), *block_hash);

        Ok(true)
    }

    fn get_canonical_tip(&self, source_id: &str) -> Result<Option<[u8; 32]>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        Ok(inner.canonical_tips.get(source_id).copied())
    }

    fn get_block_height(&self, block_hash: &[u8; 32]) -> Result<Option<u64>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        Ok(inner.blocks.get(block_hash).map(|b| b.height))
    }

    fn write_snapshot(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        payload: &[u8],
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        inner
            .snapshots
            .insert((source_id.to_string(), *block_hash), payload.to_vec());
        Ok(())
    }

    fn find_nearest_snapshot(
        &self,
        source_id: &str,
        tip_hash: &[u8; 32],
    ) -> Result<Option<([u8; 32], Vec<u8>)>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;

        let mut current = *tip_hash;
        loop {
            // Check if there's a snapshot at the current block.
            let key = (source_id.to_string(), current);
            if let Some(payload) = inner.snapshots.get(&key) {
                return Ok(Some((current, payload.clone())));
            }

            // Walk to parent.
            match inner.blocks.get(&current) {
                Some(block) => {
                    let parent = block.parent_hash;
                    // Genesis: parent is all-zeros — no further ancestors.
                    if parent == ZERO_HASH {
                        return Ok(None);
                    }
                    current = parent;
                }
                // Block not found — chain is broken or we've walked past genesis.
                None => return Ok(None),
            }
        }
    }

    fn collect_canonical_chain(
        &self,
        from_hash: &[u8; 32],
        to_hash: &[u8; 32],
    ) -> Result<Vec<[u8; 32]>, Self::Error> {
        // Same hash: empty range.
        if from_hash == to_hash {
            return Ok(vec![]);
        }

        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;

        // Walk backward from to_hash until we reach from_hash, collecting hashes.
        // from_hash is excluded; to_hash is included.
        let mut chain: Vec<[u8; 32]> = Vec::new();
        let mut current = *to_hash;

        loop {
            chain.push(current);

            if current == *from_hash {
                // We've reached from_hash — but since it's exclusive, pop it.
                chain.pop();
                break;
            }

            match inner.blocks.get(&current) {
                Some(block) => {
                    let parent = block.parent_hash;
                    if parent == ZERO_HASH {
                        // Reached genesis without hitting from_hash.
                        // This means from_hash isn't on the ancestor chain.
                        // Return what we have (caller is responsible for valid inputs).
                        break;
                    }
                    current = parent;
                }
                None => break,
            }
        }

        // Reverse so the order is from oldest (exclusive) to newest (inclusive).
        chain.reverse();
        Ok(chain)
    }

    fn load_block_events(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        match inner.blocks.get(block_hash) {
            Some(block) => Ok(block.events.clone()),
            None => Ok(vec![]),
        }
    }

    fn get_parent_hash(&self, block_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        Ok(inner.blocks.get(block_hash).map(|b| b.parent_hash))
    }

    fn set_canonical_tip(&self, source_id: &str, block_hash: &[u8; 32]) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        inner.canonical_tips.insert(source_id.to_string(), *block_hash);
        Ok(())
    }

    fn load_events_by_height_range(
        &self,
        _source_id: &str,
        _from_height: u64,
        _to_height: u64,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        todo!("load_events_by_height_range not yet implemented")
    }
}
