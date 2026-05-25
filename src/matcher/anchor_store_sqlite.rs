//! SQLite-backed [`AnchorStore`].
//!
//! Lives in its own submodule so the parent [`matcher`](super) module stays
//! free of `rusqlite` at the surface. `SQLite` is a generic persistence backend
//! here — nothing source-specific — so this is *not* gated behind the `evm`
//! feature; it sits beside [`InMemoryAnchorStore`](super::InMemoryAnchorStore)
//! as the durable sibling.

use std::sync::Arc;

use tokio::task::spawn_blocking;

use crate::arch::HistoryError;

use super::AnchorStore;

/// SQLite-backed [`AnchorStore`]. Opens (or creates) a 2-column kv table
/// at the given path. Owns its own DB file — independent of the EVM cache.
pub struct SqliteAnchorStore {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
}

impl SqliteAnchorStore {
    pub fn open(path: &str) -> Result<Self, HistoryError> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| HistoryError::internal(format!("sqlite open: {e}")))?;
        conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS matcher_anchors (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            ",
        )
        .map_err(|e| HistoryError::internal(format!("sqlite create: {e}")))?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }
}

#[async_trait::async_trait]
impl AnchorStore for SqliteAnchorStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, HistoryError> {
        let key = key.to_owned();
        let conn = self.conn.clone();
        spawn_blocking(move || -> Result<Option<Vec<u8>>, HistoryError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare_cached("SELECT value FROM matcher_anchors WHERE key = ?1")
                .map_err(|e| HistoryError::internal(format!("sqlite prep: {e}")))?;
            use rusqlite::OptionalExtension;
            let row = stmt
                .query_row([&key], |r| r.get::<_, Vec<u8>>(0))
                .optional()
                .map_err(|e| HistoryError::internal(format!("sqlite query: {e}")))?;
            Ok(row)
        })
        .await
        .map_err(|e| HistoryError::internal(format!("blocking join: {e}")))?
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), HistoryError> {
        let key = key.to_owned();
        let conn = self.conn.clone();
        spawn_blocking(move || -> Result<(), HistoryError> {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT INTO matcher_anchors (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![&key, &value],
            )
            .map_err(|e| HistoryError::internal(format!("sqlite insert: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| HistoryError::internal(format!("blocking join: {e}")))?
    }
}
