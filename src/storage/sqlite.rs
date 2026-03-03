/// SQLite-backed storage implementation.
///
/// Uses WAL mode for concurrent reads, NORMAL synchronous for performance,
/// and a UNIQUE constraint on events for deduplication via INSERT OR IGNORE.
///
/// All methods are synchronous — callers in async contexts must use
/// `tokio::task::spawn_blocking`.
use rusqlite::{Connection, OpenFlags};
use std::sync::Mutex;

use super::{Anchor, Snapshot, Storage, StoredEvent};
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

pub struct SqliteStorage {
    /// Mutex wrapping Connection because rusqlite::Connection is not Sync.
    /// The Storage trait requires Send + Sync, so we use Mutex<Connection>.
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

    /// Open an in-memory SQLite database (useful for tests).
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

// ============================================================
// Storage trait implementation
// ============================================================

impl Storage for SqliteStorage {
    type Error = StorageError;

    fn write_events(&self, _source_id: &str, events: &[StoredEvent]) -> Result<usize, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        let mut total_inserted = 0usize;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO events (source_id, cursor_major, cursor_minor, payload)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for ev in events {
                stmt.execute(rusqlite::params![
                    ev.source_id,
                    ev.cursor_major,
                    ev.cursor_minor,
                    ev.payload
                ])?;
                // changes() returns number of rows actually modified (0 if IGNORE fired)
                total_inserted += conn.changes() as usize;
            }
        }
        tx.commit()?;
        Ok(total_inserted)
    }

    fn events_in_range(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        // Use explicit tuple comparison expanded to major/minor because SQLite
        // does not support (a, b) >= (c, d) syntax in all versions.
        let mut stmt = conn.prepare_cached(
            "SELECT source_id, cursor_major, cursor_minor, payload
             FROM events
             WHERE source_id = ?1
               AND (cursor_major > ?2 OR (cursor_major = ?2 AND cursor_minor >= ?3))
               AND (cursor_major < ?4 OR (cursor_major = ?4 AND cursor_minor < ?5))
             ORDER BY cursor_major ASC, cursor_minor ASC",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![source_id, from.0, from.1, to.0, to.1],
            |row| {
                Ok(StoredEvent {
                    source_id: row.get(0)?,
                    cursor_major: row.get(1)?,
                    cursor_minor: row.get(2)?,
                    payload: row.get(3)?,
                })
            },
        )?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn write_snapshot(&self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO snapshots (source_id, cursor_major, cursor_minor, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                snapshot.source_id,
                snapshot.cursor_major,
                snapshot.cursor_minor,
                snapshot.payload,
                snapshot.created_at
            ],
        )?;
        Ok(())
    }

    fn latest_snapshot(&self, source_id: &str) -> Result<Option<Snapshot>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT source_id, cursor_major, cursor_minor, payload, created_at
             FROM snapshots
             WHERE source_id = ?1
             ORDER BY cursor_major DESC, cursor_minor DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![source_id], |row| {
            Ok(Snapshot {
                source_id: row.get(0)?,
                cursor_major: row.get(1)?,
                cursor_minor: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn snapshots_before(
        &self,
        source_id: &str,
        cursor: (i64, i64),
    ) -> Result<Vec<Snapshot>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT source_id, cursor_major, cursor_minor, payload, created_at
             FROM snapshots
             WHERE source_id = ?1
               AND (cursor_major < ?2 OR (cursor_major = ?2 AND cursor_minor <= ?3))
             ORDER BY cursor_major DESC, cursor_minor DESC",
        )?;
        let rows = stmt.query_map(rusqlite::params![source_id, cursor.0, cursor.1], |row| {
            Ok(Snapshot {
                source_id: row.get(0)?,
                cursor_major: row.get(1)?,
                cursor_minor: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn write_anchor(&self, anchor: &Anchor) -> Result<(), Self::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO anchors (source_id, name, cursor_major, cursor_minor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(source_id, name)
             DO UPDATE SET cursor_major = excluded.cursor_major,
                           cursor_minor = excluded.cursor_minor",
            rusqlite::params![anchor.source_id, anchor.name, anchor.cursor_major, anchor.cursor_minor],
        )?;
        Ok(())
    }

    fn read_anchor(&self, source_id: &str, name: &str) -> Result<Option<Anchor>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT source_id, name, cursor_major, cursor_minor
             FROM anchors
             WHERE source_id = ?1 AND name = ?2",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![source_id, name], |row| {
            Ok(Anchor {
                source_id: row.get(0)?,
                name: row.get(1)?,
                cursor_major: row.get(2)?,
                cursor_minor: row.get(3)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    fn add_coverage(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<(), Self::Error> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        // Find all existing intervals that overlap or touch [from, to).
        // An interval [a, b) overlaps or is adjacent to [from, to) when:
        //   a <= to AND b >= from  (using tuple comparison expanded for SQLite)
        let overlapping = {
            let mut stmt = tx.prepare_cached(
                "SELECT id, range_start_major, range_start_minor, range_end_major, range_end_minor
                 FROM coverage
                 WHERE source_id = ?1
                   AND (range_start_major < ?4 OR (range_start_major = ?4 AND range_start_minor <= ?5))
                   AND (range_end_major > ?2 OR (range_end_major = ?2 AND range_end_minor >= ?3))",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![source_id, from.0, from.1, to.0, to.1],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?;
            let mut result: Vec<(i64, i64, i64, i64, i64)> = Vec::new();
            for r in rows {
                result.push(r?);
            }
            result
        };

        // Compute merged interval: union of all overlapping intervals + new interval
        let mut merged_start = from;
        let mut merged_end = to;

        let mut ids_to_delete: Vec<i64> = Vec::new();
        for (id, start_major, start_minor, end_major, end_minor) in &overlapping {
            ids_to_delete.push(*id);
            let start = (*start_major, *start_minor);
            let end = (*end_major, *end_minor);
            if start < merged_start {
                merged_start = start;
            }
            if end > merged_end {
                merged_end = end;
            }
        }

        // Delete overlapping intervals
        for id in &ids_to_delete {
            tx.execute("DELETE FROM coverage WHERE id = ?1", rusqlite::params![id])?;
        }

        // Insert merged interval
        tx.execute(
            "INSERT INTO coverage (source_id, range_start_major, range_start_minor, range_end_major, range_end_minor)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                source_id,
                merged_start.0,
                merged_start.1,
                merged_end.0,
                merged_end.1
            ],
        )?;

        tx.commit()?;
        Ok(())
    }

    fn is_fully_cached(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<bool, Self::Error> {
        let conn = self.conn.lock().unwrap();
        // A single cached interval covers [from, to) if:
        //   range_start <= from AND range_end >= to
        let mut stmt = conn.prepare_cached(
            "SELECT COUNT(*) FROM coverage
             WHERE source_id = ?1
               AND (range_start_major < ?2 OR (range_start_major = ?2 AND range_start_minor <= ?3))
               AND (range_end_major > ?4 OR (range_end_major = ?4 AND range_end_minor >= ?5))",
        )?;
        let count: i64 = stmt.query_row(
            rusqlite::params![source_id, from.0, from.1, to.0, to.1],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn cached_tip(&self, source_id: &str) -> Result<Option<(i64, i64)>, Self::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT range_end_major, range_end_minor
             FROM coverage
             WHERE source_id = ?1
             ORDER BY range_end_major DESC, range_end_minor DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![source_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }
}
