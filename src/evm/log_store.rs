//! Durable SQLite-backed store for EVM log cache, scanned-range tracking,
//! block headers, and ERC20 sidecar tables.
//!
//! Phase 1 of the refactor (see `refactor.md`) pushes navigation queries
//! out of this layer into the in-memory [`LogIndex`](super::log_index).
//! `LogStore` therefore exposes only the narrow durable surface — bulk
//! read/write, scanned-range bookkeeping, and reorg invalidation — with
//! no `first_log_*` / `last_log_*` navigation helpers.
//!
//! Schema lives here; [`super::log_cache_legacy`] is a thin facade that
//! borrows these migrations and adds the deprecated navigation methods
//! while old callers are being rewired.

use std::sync::Mutex;

use alloy::primitives::{Address, Bytes, LogData, B256, U256};
use alloy::rpc::types::Log;
use rusqlite::Connection;

// ============================================================
// Versioning
// ============================================================

/// Bump this when cache schema or semantics change.
/// The version is appended to the DB filename so stale caches are bypassed.
pub const CACHE_VERSION: u32 = 3;

/// Inserts the cache version before the file extension.
/// `"./foo.db"` → `"./foo.v3.db"`, `"./foo"` → `"./foo.v3"`
pub fn versioned_path(base: &str) -> String {
    match base.rsplit_once('.') {
        Some((stem, ext)) => format!("{}.v{}.{}", stem, CACHE_VERSION, ext),
        None => format!("{}.v{}", base, CACHE_VERSION),
    }
}

// ============================================================
// Error
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration: {0}")]
    Migration(#[from] rusqlite_migration::Error),
}

// ============================================================
// Schema
// ============================================================

pub(crate) fn migrations() -> rusqlite_migration::Migrations<'static> {
    rusqlite_migration::Migrations::new(vec![
        rusqlite_migration::M::up(
            "
        -- Raw EVM log entries returned by eth_getLogs, keyed by filter.
        -- Each row is a 1:1 copy of an alloy Log struct (minus 'removed').
        -- filter_hash: BLAKE3(address ++ topic0..3) — groups logs by the
        --   eth_getLogs filter that produced them. Different filters for the
        --   same contract (e.g. Transfer vs Approval) get separate partitions.
        -- Uniqueness is (filter_hash, block_hash, log_index) — not block_number —
        --   so that the same log_index on different forks (reorgs) can coexist.
        --   Reorged logs remain in the table and are filtered out at query time
        --   via the reorged_blocks table.
        CREATE TABLE cached_logs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_hash       BLOB    NOT NULL,
            block_number      INTEGER NOT NULL,
            log_index         INTEGER NOT NULL,
            address           BLOB    NOT NULL,
            topic0            BLOB,
            topic1            BLOB,
            topic2            BLOB,
            topic3            BLOB,
            data              BLOB    NOT NULL,
            block_hash        BLOB    NOT NULL,
            transaction_hash  BLOB    NOT NULL,
            transaction_index INTEGER NOT NULL,
            UNIQUE(filter_hash, block_hash, log_index)
        );
        -- Index for range queries by block_number. May return logs from
        -- reorged blocks — caller joins against reorged_blocks to filter.
        CREATE INDEX idx_cached_logs_filter_block
            ON cached_logs(filter_hash, block_number, log_index);

        -- Block hashes that have been reorged out of the canonical chain.
        -- Small table — typically empty or a handful of entries.
        -- block_number is stored for efficient range-based cleanup.
        -- Logs from these blocks are excluded from all cache queries.
        CREATE TABLE reorged_blocks (
            block_hash   BLOB    NOT NULL PRIMARY KEY,
            block_number INTEGER NOT NULL
        );

        -- Tracks which block ranges have been scanned (via eth_getLogs) for
        -- a given filter_hash, even if zero logs were returned.
        -- Used to compute gaps: when the user requests [A, B], we diff
        -- against scanned_ranges to find sub-ranges we haven't fetched yet.
        -- Ranges are inclusive [start_block, end_block].
        -- end_block_hash identifies the block at end_block — if it appears
        --   in reorged_blocks, this range must be rescanned.
        -- Ranges are NOT eagerly merged on insert. Only finalized ranges
        --   should be compacted (via compact_ranges) to prevent a single
        --   reorg at the tip from invalidating a huge merged range.
        CREATE TABLE scanned_ranges (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_hash     BLOB    NOT NULL,
            start_block     INTEGER NOT NULL,
            end_block       INTEGER NOT NULL,
            end_block_hash  BLOB    NOT NULL
        );
        CREATE INDEX idx_scanned_ranges_filter
            ON scanned_ranges(filter_hash, start_block, end_block);

        -- Block headers cached from eth_getBlockByNumber.
        -- Shared across all filters (not partitioned by filter_hash).
        CREATE TABLE cached_block_headers (
            height       INTEGER PRIMARY KEY,
            hash         BLOB    NOT NULL,
            parent_hash  BLOB    NOT NULL,
            timestamp    INTEGER NOT NULL
        );
        ",
        ),
        rusqlite_migration::M::up(
            "
        CREATE TABLE erc20_metadata (
            address    BLOB    NOT NULL,
            chain_id   INTEGER NOT NULL,
            name       TEXT    NOT NULL,
            symbol     TEXT    NOT NULL,
            decimals   INTEGER NOT NULL,
            PRIMARY KEY (address, chain_id)
        );
        ",
        ),
        rusqlite_migration::M::up(
            "
        CREATE TABLE erc20_balances (
            chain_id     INTEGER NOT NULL,
            address      BLOB    NOT NULL,
            owner        BLOB    NOT NULL,
            block_number INTEGER NOT NULL,
            balance      BLOB    NOT NULL,
            PRIMARY KEY (chain_id, address, owner, block_number)
        );
        ",
        ),
        rusqlite_migration::M::up(
            "
        -- Resolved (event name) → (canonical signature, topic0) for a given
        -- contract on a given chain. Populated by the signature resolver
        -- (Sourcify → Etherscan) so subsequent lookups are zero-network.
        CREATE TABLE event_signatures (
            chain_id    INTEGER NOT NULL,
            address     BLOB    NOT NULL,
            event_name  TEXT    NOT NULL,
            signature   TEXT    NOT NULL,
            topic0      BLOB    NOT NULL,
            PRIMARY KEY (chain_id, address, event_name)
        );
        ",
        ),
    ])
}

pub(crate) fn configure_connection(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
}

// ============================================================
// Helpers
// ============================================================

/// Compute a deterministic BLAKE3 hash of (address, topic0-3).
///
/// Null topics use a 0xFF sentinel (all-ones, 32 bytes) to distinguish
/// "no topic filter" from any real topic value.
pub fn compute_filter_hash(address: &[u8; 20], topics: &[Option<[u8; 32]>; 4]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(address);
    let sentinel = [0xFF_u8; 32];
    for topic in topics {
        match topic {
            Some(t) => hasher.update(t),
            None => hasher.update(&sentinel),
        };
    }
    *hasher.finalize().as_bytes()
}

/// Compute uncovered sub-ranges within `requested` given sorted, non-overlapping `scanned` ranges.
///
/// Both `requested` and each entry in `scanned` are inclusive `[start, end]` ranges.
pub fn compute_gaps(requested: (u64, u64), scanned: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();
    let mut cursor = requested.0;
    for &(start, end) in scanned {
        if start > requested.1 {
            break;
        }
        if end < cursor {
            continue;
        }
        if start > cursor {
            gaps.push((cursor, start - 1));
        }
        cursor = cursor.max(end + 1);
        if cursor > requested.1 {
            break;
        }
    }
    if cursor <= requested.1 {
        gaps.push((cursor, requested.1));
    }
    gaps
}

// ============================================================
// LogStore
// ============================================================

/// A cached block header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub height: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: u64,
}

/// Cached ERC20 token metadata (name, symbol, decimals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedErc20Metadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

/// Durable SQLite-backed log + range + header + erc20 store.
///
/// Self-contained SQLite database. WAL mode supports concurrent readers.
/// Logs are append-only; reorgs are handled by marking blocks in
/// `reorged_blocks` and filtering them out at query time.
///
/// Phase 1 boundary: navigation queries (first/last log) live in
/// [`LogIndex`](super::log_index), not here.
pub struct LogStore {
    conn: Mutex<Connection>,
}

impl LogStore {
    /// Open a log store at the given path. Creates DB and runs migrations if needed.
    pub fn open(path: &str) -> Result<Self, CacheError> {
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory log store. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // --------------------------------------------------------
    // Logs
    // --------------------------------------------------------

    /// Insert logs. Deduplicates by (filter_hash, block_hash, log_index).
    /// Logs without a block_hash (pending/mempool) are silently skipped.
    pub fn insert_logs(&self, filter_hash: &[u8; 32], logs: &[Log]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO cached_logs
                 (filter_hash, block_number, log_index, address, topic0, topic1, topic2, topic3,
                  data, block_hash, transaction_hash, transaction_index)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )?;
            for log in logs {
                let block_hash = match log.block_hash {
                    Some(h) => h,
                    None => continue,
                };
                let topics = log.topics();
                stmt.execute(rusqlite::params![
                    &filter_hash[..],
                    log.block_number.unwrap_or(0) as i64,
                    log.log_index.unwrap_or(0) as i64,
                    log.address().as_slice(),
                    topics.first().map(|t| t.as_slice().to_vec()),
                    topics.get(1).map(|t| t.as_slice().to_vec()),
                    topics.get(2).map(|t| t.as_slice().to_vec()),
                    topics.get(3).map(|t| t.as_slice().to_vec()),
                    log.data().data.as_ref(),
                    block_hash.as_slice(),
                    log.transaction_hash.map(|h| h.0.to_vec()),
                    log.transaction_index.unwrap_or(0) as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Resolve a single cached log by its `(block_hash, log_index)` identity
    /// under `filter_hash`. Does NOT consult `reorged_blocks` — the caller
    /// (typically `LogIndex`) is expected to never request a reference into
    /// a reorged block.
    pub fn get_log_by_ref(
        &self,
        filter_hash: &[u8; 32],
        block_hash: &[u8; 32],
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1 AND block_hash = ?2 AND log_index = ?3
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![&filter_hash[..], &block_hash[..], log_index as i64],
            |row| row_to_log(row),
        )?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// All cached, non-reorged logs for `filter_hash` in `[from_block, to_block]`,
    /// ordered by (block_number, log_index).
    pub fn get_logs_in_range(
        &self,
        filter_hash: &[u8; 32],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1 AND block_number >= ?2 AND block_number <= ?3
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC",
        )?;
        let logs = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], from_block as i64, to_block as i64],
                |row| row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }

    // --------------------------------------------------------
    // Scanned ranges
    // --------------------------------------------------------

    /// Record a scanned block range. Does NOT merge — call [`Self::compact_ranges`]
    /// separately for finalized ranges.
    pub fn record_scanned_range(
        &self,
        filter_hash: &[u8; 32],
        start: u64,
        end: u64,
        end_block_hash: &[u8; 32],
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO scanned_ranges (filter_hash, start_block, end_block, end_block_hash)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                &filter_hash[..],
                start as i64,
                end as i64,
                &end_block_hash[..],
            ],
        )?;
        Ok(())
    }

    /// All valid scanned ranges for `filter_hash`, sorted by start_block.
    /// Excludes ranges whose `end_block_hash` is in `reorged_blocks`.
    pub fn get_scanned_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<Vec<(u64, u64)>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT start_block, end_block FROM scanned_ranges
             WHERE filter_hash = ?1
               AND end_block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY start_block ASC",
        )?;
        let ranges = stmt
            .query_map(rusqlite::params![&filter_hash[..]], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ranges)
    }

    /// Merge adjacent/overlapping scanned ranges that are entirely within
    /// `[0, up_to_block]`. Only call this for finalized block heights.
    ///
    /// After compaction, the merged range's `end_block_hash` is taken from
    /// the constituent range with the highest `end_block`.
    pub fn compact_ranges(
        &self,
        filter_hash: &[u8; 32],
        up_to_block: u64,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        let ranges: Vec<(i64, u64, u64, Vec<u8>)> = {
            let mut stmt = tx.prepare(
                "SELECT id, start_block, end_block, end_block_hash FROM scanned_ranges
                 WHERE filter_hash = ?1 AND end_block <= ?2
                   AND end_block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
                 ORDER BY start_block ASC",
            )?;
            let result: Vec<(i64, u64, u64, Vec<u8>)> = stmt
                .query_map(
                    rusqlite::params![&filter_hash[..], up_to_block as i64],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get::<_, i64>(1)? as u64,
                            row.get::<_, i64>(2)? as u64,
                            row.get::<_, Vec<u8>>(3)?,
                        ))
                    },
                )?
                .collect::<Result<_, _>>()?;
            result
        };

        if ranges.len() < 2 {
            return Ok(());
        }

        let mut merged: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        let mut ids_to_delete: Vec<i64> = Vec::new();
        for (id, start, end, end_hash) in &ranges {
            ids_to_delete.push(*id);
            if let Some(last) = merged.last_mut() {
                if last.1 + 1 >= *start {
                    if *end > last.1 {
                        last.1 = *end;
                        last.2 = end_hash.clone();
                    }
                    continue;
                }
            }
            merged.push((*start, *end, end_hash.clone()));
        }

        if merged.len() < ranges.len() {
            for id in &ids_to_delete {
                tx.execute("DELETE FROM scanned_ranges WHERE id = ?1", [id])?;
            }
            for (start, end, end_hash) in &merged {
                tx.execute(
                    "INSERT INTO scanned_ranges (filter_hash, start_block, end_block, end_block_hash)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![&filter_hash[..], *start as i64, *end as i64, &end_hash[..]],
                )?;
            }
            tx.commit()?;
        }

        Ok(())
    }

    // --------------------------------------------------------
    // Reorg handling
    // --------------------------------------------------------

    /// Mark blocks as reorged. Logs from these blocks will be excluded from
    /// queries. Scanned ranges ending at these blocks become invalid.
    pub fn mark_reorged(&self, blocks: &[([u8; 32], u64)]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO reorged_blocks (block_hash, block_number) VALUES (?1, ?2)",
            )?;
            for (hash, number) in blocks {
                stmt.execute(rusqlite::params![&hash[..], *number as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete scanned ranges whose `end_block_hash` is in `reorged_blocks`.
    /// Call after [`Self::mark_reorged`] so gaps are recomputed for affected
    /// block ranges.
    pub fn invalidate_reorged_ranges(&self, filter_hash: &[u8; 32]) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM scanned_ranges
             WHERE filter_hash = ?1
               AND end_block_hash IN (SELECT block_hash FROM reorged_blocks)",
            rusqlite::params![&filter_hash[..]],
        )?;
        Ok(deleted)
    }

    /// Remove entries from `reorged_blocks` below the finality boundary.
    /// Once a block is finalized, it can't be reorged so the tombstone is
    /// safe to drop.
    pub fn cleanup_reorged_before(&self, block_number: u64) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM reorged_blocks WHERE block_number < ?1",
            rusqlite::params![block_number as i64],
        )?;
        Ok(deleted)
    }

    // --------------------------------------------------------
    // Block headers
    // --------------------------------------------------------

    /// Insert or replace a cached block header.
    pub fn insert_block_header(
        &self,
        height: u64,
        hash: &[u8; 32],
        parent_hash: &[u8; 32],
        timestamp: u64,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO cached_block_headers (height, hash, parent_hash, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![height as i64, &hash[..], &parent_hash[..], timestamp as i64],
        )?;
        Ok(())
    }

    /// Get a cached block header by height.
    pub fn get_block_header(&self, height: u64) -> Result<Option<BlockHeader>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT hash, parent_hash, timestamp FROM cached_block_headers WHERE height = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![height as i64], |row| {
            let hash_bytes: Vec<u8> = row.get(0)?;
            let parent_bytes: Vec<u8> = row.get(1)?;
            let timestamp: i64 = row.get(2)?;

            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hash_bytes);
            let mut parent_hash = [0u8; 32];
            parent_hash.copy_from_slice(&parent_bytes);

            Ok(BlockHeader {
                height,
                hash,
                parent_hash,
                timestamp: timestamp as u64,
            })
        })?;

        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    // --------------------------------------------------------
    // ERC20 metadata
    // --------------------------------------------------------

    pub fn get_erc20_metadata(
        &self,
        address: &[u8; 20],
        chain_id: u64,
    ) -> Result<Option<CachedErc20Metadata>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT name, symbol, decimals FROM erc20_metadata
             WHERE address = ?1 AND chain_id = ?2",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![&address[..], chain_id as i64], |row| {
            Ok(CachedErc20Metadata {
                name: row.get(0)?,
                symbol: row.get(1)?,
                decimals: row.get::<_, i64>(2)? as u8,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn insert_erc20_metadata(
        &self,
        address: &[u8; 20],
        chain_id: u64,
        name: &str,
        symbol: &str,
        decimals: u8,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO erc20_metadata (address, chain_id, name, symbol, decimals)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![&address[..], chain_id as i64, name, symbol, decimals as i64],
        )?;
        Ok(())
    }

    // --------------------------------------------------------
    // ERC20 balances
    // --------------------------------------------------------

    pub fn get_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
    ) -> Result<Option<U256>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT balance FROM erc20_balances
             WHERE chain_id = ?1 AND address = ?2 AND owner = ?3 AND block_number = ?4",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![
                chain_id as i64,
                &address[..],
                &owner[..],
                block_number as i64
            ],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(bytes)
            },
        )?;
        match rows.next() {
            Some(Ok(bytes)) if bytes.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Some(U256::from_be_bytes(arr)))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn get_erc20_balance_floor(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        max_block: u64,
    ) -> Result<Option<(u64, U256)>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, balance FROM erc20_balances
             WHERE chain_id = ?1 AND address = ?2 AND owner = ?3 AND block_number <= ?4
             ORDER BY block_number DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![chain_id as i64, &address[..], &owner[..], max_block as i64],
            |row| {
                let block: i64 = row.get(0)?;
                let balance: Vec<u8> = row.get(1)?;
                Ok((block as u64, balance))
            },
        )?;
        match rows.next() {
            Some(Ok((block, bytes))) if bytes.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Some((block, U256::from_be_bytes(arr))))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn insert_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
        balance: U256,
    ) -> Result<(), CacheError> {
        let bytes: [u8; 32] = balance.to_be_bytes();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO erc20_balances (chain_id, address, owner, block_number, balance)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                chain_id as i64,
                &address[..],
                &owner[..],
                block_number as i64,
                &bytes[..]
            ],
        )?;
        Ok(())
    }

    // --------------------------------------------------------
    // Event signatures (resolver cache)
    // --------------------------------------------------------

    pub fn get_event_signature(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        event_name: &str,
    ) -> Result<Option<(String, [u8; 32])>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT signature, topic0 FROM event_signatures
             WHERE chain_id = ?1 AND address = ?2 AND event_name = ?3",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![chain_id as i64, &address[..], event_name],
            |row| {
                let sig: String = row.get(0)?;
                let topic: Vec<u8> = row.get(1)?;
                Ok((sig, topic))
            },
        )?;
        match rows.next() {
            Some(Ok((sig, topic))) if topic.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&topic);
                Ok(Some((sig, arr)))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn insert_event_signature(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        event_name: &str,
        signature: &str,
        topic0: &[u8; 32],
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO event_signatures
             (chain_id, address, event_name, signature, topic0)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                chain_id as i64,
                &address[..],
                event_name,
                signature,
                &topic0[..]
            ],
        )?;
        Ok(())
    }
}

// ============================================================
// Row decoding (also reused by the legacy navigation methods)
// ============================================================

pub(crate) fn row_to_log(row: &rusqlite::Row) -> Result<Log, rusqlite::Error> {
    let block_number: i64 = row.get(0)?;
    let log_index: i64 = row.get(1)?;
    let address_bytes: Vec<u8> = row.get(2)?;
    let topic0: Option<Vec<u8>> = row.get(3)?;
    let topic1: Option<Vec<u8>> = row.get(4)?;
    let topic2: Option<Vec<u8>> = row.get(5)?;
    let topic3: Option<Vec<u8>> = row.get(6)?;
    let data: Vec<u8> = row.get(7)?;
    let block_hash: Vec<u8> = row.get(8)?;
    let tx_hash: Option<Vec<u8>> = row.get(9)?;
    let tx_index: i64 = row.get(10)?;

    let mut topics = Vec::new();
    for t in [topic0, topic1, topic2, topic3] {
        if let Some(bytes) = t {
            topics.push(B256::from_slice(&bytes));
        }
    }

    let log_data = LogData::new_unchecked(topics, Bytes::from(data));
    let mut addr_arr = [0u8; 20];
    addr_arr.copy_from_slice(&address_bytes);

    Ok(Log {
        inner: alloy::primitives::Log {
            address: Address::from(addr_arr),
            data: log_data,
        },
        block_hash: Some(B256::from_slice(&block_hash)),
        block_number: Some(block_number as u64),
        block_timestamp: None,
        transaction_hash: tx_hash.map(|b| B256::from_slice(&b)),
        transaction_index: Some(tx_index as u64),
        log_index: Some(log_index as u64),
        removed: false,
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    const FH: [u8; 32] = [0xAA; 32];
    const BLOCK_HASH_A: [u8; 32] = [0xBB; 32];
    const BLOCK_HASH_B: [u8; 32] = [0xBC; 32];
    const BLOCK_HASH_C: [u8; 32] = [0xBD; 32];

    fn make_test_log(block_num: u64, log_idx: u64, block_hash: [u8; 32]) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);

        Log {
            inner: alloy::primitives::Log {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(B256::from(block_hash)),
            block_number: Some(block_num),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    // -- compute_filter_hash --

    #[test]
    fn filter_hash_deterministic() {
        let addr = [0x42_u8; 20];
        let topics = [Some([0xAA_u8; 32]), None, None, None];
        assert_eq!(
            compute_filter_hash(&addr, &topics),
            compute_filter_hash(&addr, &topics)
        );
    }

    #[test]
    fn filter_hash_different_address() {
        let topics = [None; 4];
        assert_ne!(
            compute_filter_hash(&[0x01u8; 20], &topics),
            compute_filter_hash(&[0x02u8; 20], &topics)
        );
    }

    #[test]
    fn filter_hash_none_vs_some() {
        let addr = [0x42_u8; 20];
        assert_ne!(
            compute_filter_hash(&addr, &[None, None, None, None]),
            compute_filter_hash(&addr, &[Some([0x00u8; 32]), None, None, None])
        );
    }

    // -- logs --

    #[test]
    fn logs_round_trip() {
        let store = LogStore::open_in_memory().unwrap();
        let log = make_test_log(100, 0, BLOCK_HASH_A);
        store.insert_logs(&FH, &[log.clone()]).unwrap();
        let retrieved = store.get_logs_in_range(&FH, 100, 100).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, log.block_number);
        assert_eq!(retrieved[0].log_index, log.log_index);
    }

    #[test]
    fn logs_skip_pending_without_block_hash() {
        let store = LogStore::open_in_memory().unwrap();
        let mut pending = make_test_log(100, 0, BLOCK_HASH_A);
        pending.block_hash = None;
        store.insert_logs(&FH, &[pending]).unwrap();
        assert!(store.get_logs_in_range(&FH, 100, 100).unwrap().is_empty());
    }

    #[test]
    fn logs_same_block_number_different_hash_coexist() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(100, 0, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        let retrieved = store.get_logs_in_range(&FH, 100, 100).unwrap();
        assert_eq!(retrieved.len(), 2);
    }

    #[test]
    fn logs_reorged_blocks_filtered_out() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(101, 0, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, 100)]).unwrap();
        let retrieved = store.get_logs_in_range(&FH, 100, 101).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, Some(101));
    }

    // -- scanned ranges --

    #[test]
    fn range_records_without_merging() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B)
            .unwrap();
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200), (201, 300)]);
    }

    #[test]
    fn range_reorged_end_block_excluded() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, 200)]).unwrap();
        assert!(store.get_scanned_ranges(&FH).unwrap().is_empty());
    }

    #[test]
    fn invalidate_reorged_ranges_deletes() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, 200)]).unwrap();
        let deleted = store.invalidate_reorged_ranges(&FH).unwrap();
        assert_eq!(deleted, 1);
        // After physical deletion, get_scanned_ranges no longer filters via the reorged join.
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(201, 300)]);
    }

    #[test]
    fn compact_merges_finalized_ranges() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 199, &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&FH, 200, 299, &BLOCK_HASH_B)
            .unwrap();
        store
            .record_scanned_range(&FH, 300, 399, &BLOCK_HASH_C)
            .unwrap();
        store.compact_ranges(&FH, 399).unwrap();
        assert_eq!(store.get_scanned_ranges(&FH).unwrap(), vec![(100, 399)]);
    }

    #[test]
    fn compact_preserves_end_block_hash_of_highest() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 199, &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&FH, 200, 299, &BLOCK_HASH_B)
            .unwrap();
        store.compact_ranges(&FH, 299).unwrap();
        // Mark the highest range's end hash reorged; should excise the merged range.
        store.mark_reorged(&[(BLOCK_HASH_B, 299)]).unwrap();
        assert!(store.get_scanned_ranges(&FH).unwrap().is_empty());
    }

    #[test]
    fn compact_skips_reorged_ranges() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 199, &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&FH, 200, 299, &BLOCK_HASH_B)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, 199)]).unwrap();
        store.compact_ranges(&FH, 299).unwrap();
        // The reorged range is left alone (filtered out of visible ranges); the
        // non-reorged range stays as-is since it's the only compactable entry.
        assert_eq!(store.get_scanned_ranges(&FH).unwrap(), vec![(200, 299)]);
    }

    // -- compute_gaps --

    #[test]
    fn gaps_no_scanned_returns_full_range() {
        assert_eq!(compute_gaps((100, 200), &[]), vec![(100, 200)]);
    }

    #[test]
    fn gaps_fully_covered_returns_empty() {
        assert_eq!(
            compute_gaps((100, 200), &[(50, 250)]),
            Vec::<(u64, u64)>::new()
        );
    }

    #[test]
    fn gaps_partial_coverage() {
        assert_eq!(
            compute_gaps((100, 300), &[(150, 200)]),
            vec![(100, 149), (201, 300)]
        );
    }

    // -- block headers --

    #[test]
    fn block_header_round_trip() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .insert_block_header(100, &BLOCK_HASH_A, &BLOCK_HASH_B, 1_700_000_000)
            .unwrap();
        let h = store.get_block_header(100).unwrap().unwrap();
        assert_eq!(h.height, 100);
        assert_eq!(h.hash, BLOCK_HASH_A);
        assert_eq!(h.parent_hash, BLOCK_HASH_B);
        assert_eq!(h.timestamp, 1_700_000_000);
    }

    // -- erc20 metadata --

    #[test]
    fn erc20_metadata_round_trip() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        store
            .insert_erc20_metadata(&addr, 1, "Wrapped Ether", "WETH", 18)
            .unwrap();
        let m = store.get_erc20_metadata(&addr, 1).unwrap().unwrap();
        assert_eq!(m.name, "Wrapped Ether");
        assert_eq!(m.symbol, "WETH");
        assert_eq!(m.decimals, 18);
    }

    #[test]
    fn erc20_metadata_different_chains() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        store.insert_erc20_metadata(&addr, 1, "A", "A", 18).unwrap();
        store.insert_erc20_metadata(&addr, 10, "B", "B", 6).unwrap();
        assert_eq!(
            store.get_erc20_metadata(&addr, 1).unwrap().unwrap().name,
            "A"
        );
        assert_eq!(
            store.get_erc20_metadata(&addr, 10).unwrap().unwrap().name,
            "B"
        );
    }

    #[test]
    fn erc20_metadata_upsert() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        store
            .insert_erc20_metadata(&addr, 1, "Old", "OLD", 18)
            .unwrap();
        store
            .insert_erc20_metadata(&addr, 1, "New", "NEW", 6)
            .unwrap();
        let m = store.get_erc20_metadata(&addr, 1).unwrap().unwrap();
        assert_eq!(m.name, "New");
        assert_eq!(m.symbol, "NEW");
        assert_eq!(m.decimals, 6);
    }

    // -- erc20 balances --

    #[test]
    fn erc20_balance_round_trip() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x55u8; 20];
        store
            .insert_erc20_balance(1, &addr, &owner, 100, U256::from(1_000_000_u64))
            .unwrap();
        let b = store
            .get_erc20_balance(1, &addr, &owner, 100)
            .unwrap()
            .unwrap();
        assert_eq!(b, U256::from(1_000_000_u64));
    }

    #[test]
    fn erc20_balance_different_blocks() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x55u8; 20];
        store
            .insert_erc20_balance(1, &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(1, &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        assert_eq!(
            store
                .get_erc20_balance(1, &addr, &owner, 100)
                .unwrap()
                .unwrap(),
            U256::from(1u64)
        );
        assert_eq!(
            store
                .get_erc20_balance(1, &addr, &owner, 200)
                .unwrap()
                .unwrap(),
            U256::from(2u64)
        );
    }

    #[test]
    fn erc20_balance_upsert() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x55u8; 20];
        store
            .insert_erc20_balance(1, &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(1, &addr, &owner, 100, U256::from(2u64))
            .unwrap();
        assert_eq!(
            store
                .get_erc20_balance(1, &addr, &owner, 100)
                .unwrap()
                .unwrap(),
            U256::from(2u64)
        );
    }

    #[test]
    fn erc20_balance_floor_lookup() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x55u8; 20];
        store
            .insert_erc20_balance(1, &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(1, &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        let (block, balance) = store
            .get_erc20_balance_floor(1, &addr, &owner, 150)
            .unwrap()
            .unwrap();
        assert_eq!(block, 100);
        assert_eq!(balance, U256::from(1u64));
    }

    #[test]
    fn erc20_balance_floor_none() {
        let store = LogStore::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x55u8; 20];
        store
            .insert_erc20_balance(1, &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        assert!(store
            .get_erc20_balance_floor(1, &addr, &owner, 100)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cleanup_reorged_before() {
        let store = LogStore::open_in_memory().unwrap();
        store
            .mark_reorged(&[
                (BLOCK_HASH_A, 100),
                (BLOCK_HASH_B, 200),
                (BLOCK_HASH_C, 300),
            ])
            .unwrap();
        let deleted = store.cleanup_reorged_before(250).unwrap();
        assert_eq!(deleted, 2);
    }
}
