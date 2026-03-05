/// SQLite-backed storage implementation for the block DAG model.
///
/// Uses WAL mode for concurrent reads, NORMAL synchronous for performance,
/// foreign_keys ON for referential integrity, and busy_timeout for contention handling.
///
/// All methods are synchronous — callers in async contexts must use
/// `tokio::task::spawn_blocking`.
use rusqlite::{Connection, OpenFlags};
use std::sync::Mutex;

use super::Storage;
use crate::storage::schema::migrations;

// ============================================================
// Error type
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
}

// ============================================================
// SqliteStorage
// ============================================================

/// SQLite-backed block DAG storage.
///
/// Wraps `Mutex<Connection>` because `rusqlite::Connection` is not `Sync`.
/// The `Storage` trait requires `Send + Sync + 'static`, so the mutex provides
/// interior mutability with exclusive access.
pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at the given path.
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let mut conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory SQLite database. Useful for tests.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

/// Configure pragma settings on a new connection.
fn configure_connection(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
}

// All-zeros hash is the genesis sentinel — signals "no parent".
const ZERO_HASH: [u8; 32] = [0u8; 32];

// ============================================================
// Storage trait implementation
// ============================================================

impl Storage for SqliteStorage {
    type Error = StorageError;

    fn write_block(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        parent_hash: &[u8; 32],
        height: u64,
        events: &[(&[u8], &[u8])],
    ) -> Result<bool, Self::Error> {
        let conn = self.conn.lock().unwrap();

        // Block-level dedup: if block_hash already exists, skip entirely.
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = ?1)",
            rusqlite::params![&block_hash[..]],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(false);
        }

        // All three writes (block + events + canonical_tip) happen atomically.
        let tx = conn.unchecked_transaction()?;

        // Insert block metadata.
        tx.execute(
            "INSERT INTO blocks (hash, parent_hash, height, source_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![&block_hash[..], &parent_hash[..], height as i64, source_id],
        )?;

        // Insert events in order.
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO events (block_hash, cursor, payload) VALUES (?1, ?2, ?3)",
            )?;
            for (cursor_bytes, payload_bytes) in events {
                stmt.execute(rusqlite::params![
                    &block_hash[..],
                    *cursor_bytes,
                    *payload_bytes
                ])?;
            }
        }

        // Upsert canonical_tip: one row per source_id, always points to the latest block.
        tx.execute(
            "INSERT INTO canonical_tip (source_id, block_hash) VALUES (?1, ?2)
             ON CONFLICT(source_id) DO UPDATE SET block_hash = excluded.block_hash",
            rusqlite::params![source_id, &block_hash[..]],
        )?;

        tx.commit()?;
        Ok(true)
    }

    fn get_canonical_tip(&self, source_id: &str) -> Result<Option<[u8; 32]>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_hash FROM canonical_tip WHERE source_id = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![source_id], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(arr)
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn get_block_height(&self, block_hash: &[u8; 32]) -> Result<Option<u64>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare_cached("SELECT height FROM blocks WHERE hash = ?1")?;
        let mut rows = stmt.query_map(rusqlite::params![&block_hash[..]], |row| {
            let h: i64 = row.get(0)?;
            Ok(h as u64)
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn write_snapshot(
        &self,
        source_id: &str,
        block_hash: &[u8; 32],
        payload: &[u8],
    ) -> Result<(), Self::Error> {
        let conn = self.conn.lock().unwrap();
        // INSERT OR REPLACE because PK is (source_id, block_hash) — same block can have
        // its snapshot overwritten (e.g., from recovery), but this is a no-op in practice.
        conn.execute(
            "INSERT OR REPLACE INTO snapshots (source_id, block_hash, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![source_id, &block_hash[..], payload],
        )?;
        Ok(())
    }

    fn find_nearest_snapshot(
        &self,
        source_id: &str,
        tip_hash: &[u8; 32],
    ) -> Result<Option<([u8; 32], Vec<u8>)>, Self::Error> {
        let conn = self.conn.lock().unwrap();

        let mut current = *tip_hash;
        loop {
            // Check for snapshot at current block.
            {
                let mut stmt = conn.prepare_cached(
                    "SELECT payload FROM snapshots WHERE source_id = ?1 AND block_hash = ?2",
                )?;
                let mut rows = stmt.query_map(
                    rusqlite::params![source_id, &current[..]],
                    |row| {
                        let payload: Vec<u8> = row.get(0)?;
                        Ok(payload)
                    },
                )?;
                if let Some(row) = rows.next() {
                    return Ok(Some((current, row?)));
                }
            }

            // Walk to parent via blocks table.
            let parent: Option<[u8; 32]> = {
                let mut stmt = conn.prepare_cached(
                    "SELECT parent_hash FROM blocks WHERE hash = ?1",
                )?;
                let mut rows = stmt.query_map(rusqlite::params![&current[..]], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    Ok(arr)
                })?;
                rows.next().map(|r| r).transpose()?
            };

            match parent {
                None => return Ok(None), // Block not found — broken chain.
                Some(p) if p == ZERO_HASH => return Ok(None), // Genesis reached.
                Some(p) => current = p,
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

        let conn = self.conn.lock().unwrap();

        // Walk backward from to_hash until from_hash is reached (exclusive).
        let mut chain: Vec<[u8; 32]> = Vec::new();
        let mut current = *to_hash;

        loop {
            chain.push(current);

            if current == *from_hash {
                // Reached from_hash — it's exclusive, so pop it.
                chain.pop();
                break;
            }

            let parent: Option<[u8; 32]> = {
                let mut stmt = conn.prepare_cached(
                    "SELECT parent_hash FROM blocks WHERE hash = ?1",
                )?;
                let mut rows = stmt.query_map(rusqlite::params![&current[..]], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    Ok(arr)
                })?;
                rows.next().map(|r| r).transpose()?
            };

            match parent {
                None => break,                            // Block not in DB.
                Some(p) if p == ZERO_HASH => break,       // Reached genesis without finding from_hash.
                Some(p) => current = p,
            }
        }

        // Reverse to get forward order: oldest -> newest.
        chain.reverse();
        Ok(chain)
    }

    fn load_block_events(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT cursor, payload FROM events WHERE block_hash = ?1 ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![&block_hash[..]], |row| {
            let cursor: Vec<u8> = row.get(0)?;
            let payload: Vec<u8> = row.get(1)?;
            Ok((cursor, payload))
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn get_parent_hash(&self, block_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare_cached("SELECT parent_hash FROM blocks WHERE hash = ?1")?;
        let mut rows = stmt.query_map(rusqlite::params![&block_hash[..]], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Ok(arr)
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn set_canonical_tip(&self, source_id: &str, block_hash: &[u8; 32]) -> Result<(), Self::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO canonical_tip (source_id, block_hash) VALUES (?1, ?2)
             ON CONFLICT(source_id) DO UPDATE SET block_hash = excluded.block_hash",
            rusqlite::params![source_id, &block_hash[..]],
        )?;
        Ok(())
    }
}
