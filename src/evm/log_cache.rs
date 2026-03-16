//! EVM log cache with scanned-range tracking.
//!
//! Stores raw `eth_getLogs` results keyed by BLAKE3 hash of (address, topic0-3).
//! Tracks scanned block ranges (even when zero logs returned) and uses gap
//! computation to identify uncovered sub-ranges.
//!
//! Reorg handling: logs are append-only. Reorged blocks are tracked in a
//! separate table and excluded from queries. Scanned ranges that reference
//! reorged blocks are invalidated (deleted). Physical cleanup of reorged
//! logs is a separate background concern.

/// Bump this when cache schema or semantics change.
/// The version is appended to the DB filename so stale caches are bypassed.
pub const CACHE_VERSION: u32 = 3;

/// Inserts the cache version before the file extension.
/// `"./foo.db"` → `"./foo.v2.db"`, `"./foo"` → `"./foo.v2"`
pub fn versioned_path(base: &str) -> String {
    match base.rsplit_once('.') {
        Some((stem, ext)) => format!("{}.v{}.{}", stem, CACHE_VERSION, ext),
        None => format!("{}.v{}", base, CACHE_VERSION),
    }
}

use std::sync::Mutex;

use alloy::primitives::{Address, Bytes, LogData, B256};
use alloy::rpc::types::Log;
use rusqlite::Connection;

// ============================================================
// Error type
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

fn migrations() -> rusqlite_migration::Migrations<'static> {
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
        -- Used to resolve block_number -> (hash, parent_hash, timestamp)
        -- without an RPC call, e.g. for building EvmPosition from a log
        -- or for reorg detection via parent_hash chain validation.
        CREATE TABLE cached_block_headers (
            height       INTEGER PRIMARY KEY,
            hash         BLOB    NOT NULL,  -- 32-byte block hash
            parent_hash  BLOB    NOT NULL,  -- 32-byte parent block hash
            timestamp    INTEGER NOT NULL    -- unix seconds
        );
        ",
        ),
        rusqlite_migration::M::up(
            "
        -- Immutable ERC20 token metadata cached from on-chain eth_call.
        -- Composite PK (address, chain_id) so the same token address on
        -- different chains gets separate rows.
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
        -- Point-in-time ERC20 balanceOf results cached from eth_call.
        -- Each row is a self-contained snapshot: no range tracking.
        -- balance is stored as big-endian 32 bytes (U256).
        CREATE TABLE erc20_balances (
            chain_id     INTEGER NOT NULL,
            address      BLOB    NOT NULL,  -- token contract (20 bytes)
            owner        BLOB    NOT NULL,  -- account (20 bytes)
            block_number INTEGER NOT NULL,
            balance      BLOB    NOT NULL,  -- U256 big-endian 32 bytes
            PRIMARY KEY (chain_id, address, owner, block_number)
        );
        ",
        ),
    ])
}

fn configure_connection(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )
}

// ============================================================
// Filter hashing
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

// ============================================================
// Gap computation
// ============================================================

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
// LogCache
// ============================================================

/// Cache for raw EVM log data and scanned-range tracking.
///
/// Self-contained SQLite database. WAL mode supports concurrent readers.
/// Logs are append-only; reorgs are handled by marking blocks in
/// `reorged_blocks` and filtering them out at query time.
pub struct LogCache {
    conn: Mutex<Connection>,
}

impl LogCache {
    /// Open a log cache at the given path. Creates DB and runs migrations if needed.
    pub fn open(path: &str) -> Result<Self, CacheError> {
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory log cache. Useful for tests.
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

    /// Insert logs into the cache.
    ///
    /// Deduplicates by (filter_hash, block_hash, log_index). Logs without
    /// a block_hash are silently skipped (pending/mempool logs aren't cacheable).
    pub fn insert_logs(
        &self,
        filter_hash: &[u8; 32],
        logs: &[Log],
    ) -> Result<(), CacheError> {
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
                    None => continue, // skip pending logs
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

    /// Get cached logs for a filter hash within [from_block, to_block].
    ///
    /// Excludes logs from reorged blocks. Ordered by (block_number, log_index).
    pub fn get_cached_logs(
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
                |row| Self::row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }

    /// First cached log after (block_number, log_index), excluding reorged blocks.
    pub fn first_log_after(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number > ?2 OR (block_number = ?2 AND log_index > ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| Self::row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// Last cached log before (block_number, log_index), excluding reorged blocks.
    pub fn last_log_before(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number < ?2 OR (block_number = ?2 AND log_index < ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number DESC, log_index DESC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| Self::row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// First cached log at or after (block_number, log_index), excluding reorged.
    ///
    /// Unlike [`first_log_after`] this is inclusive: a log at exactly
    /// (block_number, log_index) matches.
    pub fn first_log_at_or_after(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number > ?2 OR (block_number = ?2 AND log_index >= ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| Self::row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// Last cached log at or before (block_number, log_index), excluding reorged.
    ///
    /// Unlike [`last_log_before`] this is inclusive: a log at exactly
    /// (block_number, log_index) matches.
    pub fn last_log_at_or_before(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number < ?2 OR (block_number = ?2 AND log_index <= ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number DESC, log_index DESC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| Self::row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    // --------------------------------------------------------
    // Scanned ranges
    // --------------------------------------------------------

    /// Record a scanned block range. Does NOT merge — call [`compact_ranges`]
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

    /// Get all valid scanned ranges for a filter hash, sorted by start_block.
    ///
    /// Excludes ranges whose end_block_hash is in reorged_blocks.
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
    /// [0, `up_to_block`]. Only call this for finalized block heights.
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

        // Fetch compactable ranges (entirely within finalized boundary,
        // not reorged), sorted by start_block.
        let ranges: Vec<(i64, u64, u64, Vec<u8>)> = {
            let mut stmt = tx.prepare(
                "SELECT id, start_block, end_block, end_block_hash FROM scanned_ranges
                 WHERE filter_hash = ?1 AND end_block <= ?2
                   AND end_block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
                 ORDER BY start_block ASC",
            )?;
            let result = stmt
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

        // Greedy merge: walk sorted ranges, merge adjacent/overlapping.
        let mut merged: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        let mut ids_to_delete: Vec<i64> = Vec::new();

        for (id, start, end, end_hash) in &ranges {
            ids_to_delete.push(*id);
            if let Some(last) = merged.last_mut() {
                // Adjacent (last.end + 1 >= start) or overlapping
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

        // Only rewrite if merging actually reduced the count.
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
    pub fn mark_reorged(
        &self,
        blocks: &[([u8; 32], u64)], // (block_hash, block_number)
    ) -> Result<(), CacheError> {
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

    /// Delete scanned ranges whose end_block_hash is in reorged_blocks.
    ///
    /// Call after [`mark_reorged`] to ensure gaps are recomputed for
    /// affected block ranges.
    pub fn invalidate_reorged_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM scanned_ranges
             WHERE filter_hash = ?1
               AND end_block_hash IN (SELECT block_hash FROM reorged_blocks)",
            rusqlite::params![&filter_hash[..]],
        )?;
        Ok(deleted)
    }

    /// Remove entries from reorged_blocks that are below the finality
    /// boundary. Once a block is finalized, we know it can't be reorged
    /// and can physically clean up.
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
    pub fn get_block_header(
        &self,
        height: u64,
    ) -> Result<Option<BlockHeader>, CacheError> {
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

    /// Get cached ERC20 metadata for a token address on a specific chain.
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
        let mut rows = stmt.query_map(
            rusqlite::params![&address[..], chain_id as i64],
            |row| {
                Ok(CachedErc20Metadata {
                    name: row.get(0)?,
                    symbol: row.get(1)?,
                    decimals: row.get::<_, i64>(2)? as u8,
                })
            },
        )?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Insert or replace cached ERC20 metadata.
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

    /// Get a cached ERC20 balance at a specific block.
    pub fn get_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
    ) -> Result<Option<[u8; 32]>, CacheError> {
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
                let mut balance = [0u8; 32];
                if bytes.len() == 32 {
                    balance.copy_from_slice(&bytes);
                }
                Ok(balance)
            },
        )?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Insert or replace a cached ERC20 balance at a specific block.
    pub fn insert_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
        balance: &[u8; 32],
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO erc20_balances (chain_id, address, owner, block_number, balance)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                chain_id as i64,
                &address[..],
                &owner[..],
                block_number as i64,
                &balance[..]
            ],
        )?;
        Ok(())
    }

    // --------------------------------------------------------
    // Internal
    // --------------------------------------------------------

    fn row_to_log(row: &rusqlite::Row) -> Result<Log, rusqlite::Error> {
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
}

// ============================================================
// BlockHeader
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
            transaction_hash: Some(B256::from([0xCC; 32])),
            transaction_index: Some(0),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    // -- filter hash --

    #[test]
    fn filter_hash_deterministic() {
        let addr = [0x42u8; 20];
        let topics = [Some([0xAAu8; 32]), None, None, None];
        assert_eq!(
            compute_filter_hash(&addr, &topics),
            compute_filter_hash(&addr, &topics),
        );
    }

    #[test]
    fn filter_hash_different_address() {
        let topics = [None; 4];
        assert_ne!(
            compute_filter_hash(&[0x01u8; 20], &topics),
            compute_filter_hash(&[0x02u8; 20], &topics),
        );
    }

    #[test]
    fn filter_hash_none_vs_some() {
        let addr = [0x42u8; 20];
        assert_ne!(
            compute_filter_hash(&addr, &[None, None, None, None]),
            compute_filter_hash(&addr, &[Some([0x00u8; 32]), None, None, None]),
        );
    }

    // -- logs --

    #[test]
    fn logs_round_trip() {
        let cache = LogCache::open_in_memory().unwrap();
        let logs = vec![
            make_test_log(100, 0, BLOCK_HASH_A),
            make_test_log(100, 1, BLOCK_HASH_A),
            make_test_log(101, 0, BLOCK_HASH_B),
        ];
        cache.insert_logs(&FH, &logs).unwrap();
        let retrieved = cache.get_cached_logs(&FH, 100, 101).unwrap();
        assert_eq!(retrieved.len(), 3);
        assert_eq!(retrieved[0].block_number, Some(100));
        assert_eq!(retrieved[0].log_index, Some(0));
        assert_eq!(retrieved[2].block_number, Some(101));
    }

    #[test]
    fn logs_skip_pending_without_block_hash() {
        let cache = LogCache::open_in_memory().unwrap();
        let mut pending = make_test_log(100, 0, BLOCK_HASH_A);
        pending.block_hash = None;
        cache.insert_logs(&FH, &[pending]).unwrap();
        assert!(cache.get_cached_logs(&FH, 100, 100).unwrap().is_empty());
    }

    #[test]
    fn logs_same_block_number_different_hash_coexist() {
        let cache = LogCache::open_in_memory().unwrap();
        // Same block_number=100, log_index=0, but different block hashes (fork)
        let log_a = make_test_log(100, 0, BLOCK_HASH_A);
        let log_b = make_test_log(100, 0, BLOCK_HASH_B);
        cache.insert_logs(&FH, &[log_a, log_b]).unwrap();
        // Both visible (neither is reorged yet)
        let logs = cache.get_cached_logs(&FH, 100, 100).unwrap();
        assert_eq!(logs.len(), 2);
    }

    #[test]
    fn logs_reorged_blocks_filtered_out() {
        let cache = LogCache::open_in_memory().unwrap();
        cache
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(100, 0, BLOCK_HASH_B), // fork
                ],
            )
            .unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 100)]).unwrap();

        let logs = cache.get_cached_logs(&FH, 100, 100).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].block_hash, Some(B256::from(BLOCK_HASH_A)));
    }

    #[test]
    fn first_log_after_excludes_reorged() {
        let cache = LogCache::open_in_memory().unwrap();
        cache
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(101, 0, BLOCK_HASH_B), // will be reorged
                    make_test_log(101, 0, BLOCK_HASH_C), // canonical replacement
                ],
            )
            .unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 101)]).unwrap();

        let next = cache.first_log_after(&FH, 100, 0).unwrap().unwrap();
        assert_eq!(next.block_hash, Some(B256::from(BLOCK_HASH_C)));
    }

    #[test]
    fn last_log_before_excludes_reorged() {
        let cache = LogCache::open_in_memory().unwrap();
        cache
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A), // will be reorged
                    make_test_log(100, 0, BLOCK_HASH_B), // canonical
                    make_test_log(101, 0, BLOCK_HASH_C),
                ],
            )
            .unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_A, 100)]).unwrap();

        let prev = cache.last_log_before(&FH, 101, 0).unwrap().unwrap();
        assert_eq!(prev.block_hash, Some(B256::from(BLOCK_HASH_B)));
    }

    // -- scanned ranges --

    #[test]
    fn range_records_without_merging() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        // Two separate rows, not merged
        let ranges = cache.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200), (201, 300)]);
    }

    #[test]
    fn range_reorged_end_block_excluded() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 300)]).unwrap();

        let ranges = cache.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200)]);
    }

    #[test]
    fn invalidate_reorged_ranges_deletes() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 300)]).unwrap();

        let deleted = cache.invalidate_reorged_ranges(&FH).unwrap();
        assert_eq!(deleted, 1);

        // After invalidation, even without the filter, only one range exists
        let conn = cache.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM scanned_ranges WHERE filter_hash = ?1",
                rusqlite::params![&FH[..]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn compact_merges_finalized_ranges() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        cache.record_scanned_range(&FH, 301, 400, &BLOCK_HASH_C).unwrap();
        // Only compact up to block 300 (simulate finality boundary)
        cache.compact_ranges(&FH, 300).unwrap();

        let ranges = cache.get_scanned_ranges(&FH).unwrap();
        // [100,200] + [201,300] merged → [100,300], plus [301,400] untouched
        assert_eq!(ranges, vec![(100, 300), (301, 400)]);
    }

    #[test]
    fn compact_preserves_end_block_hash_of_highest() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        cache.compact_ranges(&FH, 300).unwrap();

        // Verify the merged range has BLOCK_HASH_B as end_block_hash
        let conn = cache.conn.lock().unwrap();
        let hash: Vec<u8> = conn
            .query_row(
                "SELECT end_block_hash FROM scanned_ranges WHERE filter_hash = ?1",
                rusqlite::params![&FH[..]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(&hash[..], &BLOCK_HASH_B[..]);
    }

    #[test]
    fn compact_skips_reorged_ranges() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A).unwrap();
        cache.record_scanned_range(&FH, 201, 300, &BLOCK_HASH_B).unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 300)]).unwrap();

        cache.compact_ranges(&FH, 300).unwrap();
        // Only one valid range, nothing to merge
        let ranges = cache.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200)]);
    }

    // -- gaps --

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
        let cache = LogCache::open_in_memory().unwrap();
        cache.insert_block_header(42, &[0xAA; 32], &[0xBB; 32], 1700000000).unwrap();
        let h = cache.get_block_header(42).unwrap().unwrap();
        assert_eq!(h.height, 42);
        assert_eq!(h.hash, [0xAA; 32]);
        assert_eq!(h.parent_hash, [0xBB; 32]);
        assert_eq!(h.timestamp, 1700000000);
    }

    // -- erc20 metadata --

    #[test]
    fn erc20_metadata_round_trip() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        cache
            .insert_erc20_metadata(&addr, 1, "USD Coin", "USDC", 6)
            .unwrap();
        let meta = cache.get_erc20_metadata(&addr, 1).unwrap().unwrap();
        assert_eq!(meta.name, "USD Coin");
        assert_eq!(meta.symbol, "USDC");
        assert_eq!(meta.decimals, 6);
    }

    #[test]
    fn erc20_metadata_different_chains() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        cache
            .insert_erc20_metadata(&addr, 1, "USD Coin", "USDC", 6)
            .unwrap();
        cache
            .insert_erc20_metadata(&addr, 137, "USD Coin (PoS)", "USDC", 6)
            .unwrap();
        let eth = cache.get_erc20_metadata(&addr, 1).unwrap().unwrap();
        let poly = cache.get_erc20_metadata(&addr, 137).unwrap().unwrap();
        assert_eq!(eth.name, "USD Coin");
        assert_eq!(poly.name, "USD Coin (PoS)");
        // Non-existent chain returns None
        assert!(cache.get_erc20_metadata(&addr, 42161).unwrap().is_none());
    }

    #[test]
    fn erc20_metadata_upsert() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        cache
            .insert_erc20_metadata(&addr, 1, "Old Name", "OLD", 18)
            .unwrap();
        cache
            .insert_erc20_metadata(&addr, 1, "New Name", "NEW", 18)
            .unwrap();
        let meta = cache.get_erc20_metadata(&addr, 1).unwrap().unwrap();
        assert_eq!(meta.name, "New Name");
        assert_eq!(meta.symbol, "NEW");
    }

    // -- erc20 balances --

    #[test]
    fn erc20_balance_round_trip() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x01u8; 20];
        let mut balance = [0u8; 32];
        balance[31] = 100; // U256(100)
        cache.insert_erc20_balance(1, &addr, &owner, 1000, &balance).unwrap();
        let got = cache.get_erc20_balance(1, &addr, &owner, 1000).unwrap().unwrap();
        assert_eq!(got, balance);
    }

    #[test]
    fn erc20_balance_different_blocks() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x01u8; 20];
        let mut bal_a = [0u8; 32];
        bal_a[31] = 100;
        let mut bal_b = [0u8; 32];
        bal_b[31] = 200;
        cache.insert_erc20_balance(1, &addr, &owner, 1000, &bal_a).unwrap();
        cache.insert_erc20_balance(1, &addr, &owner, 2000, &bal_b).unwrap();
        assert_eq!(cache.get_erc20_balance(1, &addr, &owner, 1000).unwrap().unwrap(), bal_a);
        assert_eq!(cache.get_erc20_balance(1, &addr, &owner, 2000).unwrap().unwrap(), bal_b);
        assert!(cache.get_erc20_balance(1, &addr, &owner, 1500).unwrap().is_none());
    }

    #[test]
    fn erc20_balance_upsert() {
        let cache = LogCache::open_in_memory().unwrap();
        let addr = [0x42u8; 20];
        let owner = [0x01u8; 20];
        let mut bal_a = [0u8; 32];
        bal_a[31] = 100;
        let mut bal_b = [0u8; 32];
        bal_b[31] = 200;
        cache.insert_erc20_balance(1, &addr, &owner, 1000, &bal_a).unwrap();
        cache.insert_erc20_balance(1, &addr, &owner, 1000, &bal_b).unwrap();
        assert_eq!(cache.get_erc20_balance(1, &addr, &owner, 1000).unwrap().unwrap(), bal_b);
    }

    // -- cleanup --

    #[test]
    fn cleanup_reorged_before() {
        let cache = LogCache::open_in_memory().unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_A, 100), (BLOCK_HASH_B, 200)]).unwrap();
        let deleted = cache.cleanup_reorged_before(150).unwrap();
        assert_eq!(deleted, 1);
        // BLOCK_HASH_B still present
        let logs = vec![make_test_log(200, 0, BLOCK_HASH_B)];
        cache.insert_logs(&FH, &logs).unwrap();
        assert!(cache.get_cached_logs(&FH, 200, 200).unwrap().is_empty());
    }
}
