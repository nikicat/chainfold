//! [`LogCache`] — the durable EVM log cache.
//!
//! Owns the reorg-coherent set of tables: raw logs (`cached_logs`),
//! scanned-range bookkeeping (`scanned_ranges`), reorg tombstones
//! (`reorged_blocks`), and cached block headers (`cached_block_headers`).
//!
//! Exposes only the narrow durable surface — bulk read/write, scanned-range
//! bookkeeping, reorg invalidation, and header lookup. Navigation queries
//! (first/last log) live in the in-memory [`LogIndex`](crate::evm::LogIndex).

use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
use alloy::rpc::types::Log;

use super::{CacheError, SharedConn};
use crate::evm::observe::{SqliteOp, sqlite_span};

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
// Types
// ============================================================

/// A cached block header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub height: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: u64,
}

// ============================================================
// LogCache
// ============================================================

/// Durable SQLite-backed log + range + reorg + header cache.
///
/// A cheap handle over the shared connection vended by
/// [`CacheDb`](super::CacheDb). Logs are append-only; reorgs are handled by
/// marking blocks in `reorged_blocks` and filtering them out at query time.
///
/// Navigation queries (first/last log) live in
/// [`LogIndex`](crate::evm::LogIndex), not here.
pub struct LogCache {
    conn: SharedConn,
}

impl LogCache {
    pub(super) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
    }

    /// Open a standalone log cache at the given path. Creates the DB and runs
    /// migrations if needed. Equivalent to `CacheDb::open(path)?.logs()`.
    pub fn open(path: &str) -> Result<Self, CacheError> {
        Ok(super::CacheDb::open(path)?.logs())
    }

    /// Open a standalone in-memory log cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(super::CacheDb::open_in_memory()?.logs())
    }

    // --------------------------------------------------------
    // Logs
    // --------------------------------------------------------

    /// Insert logs. Deduplicates by (`filter_hash`, `block_hash`, `log_index`).
    /// Logs without a `block_hash` (pending/mempool) are silently skipped.
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
                let Some(block_hash) = log.block_hash else {
                    continue;
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
            row_to_log,
        )?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// All cached, non-reorged logs for `filter_hash` in `[from_block, to_block]`,
    /// ordered by (`block_number`, `log_index`).
    pub fn get_logs_in_range(
        &self,
        filter_hash: &[u8; 32],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, CacheError> {
        let _span = sqlite_span(SqliteOp::GetLogsInRange).entered();
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
                row_to_log,
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

    /// All valid scanned ranges for `filter_hash`, sorted by `start_block`.
    /// Excludes ranges whose `end_block_hash` is in `reorged_blocks`.
    pub fn get_scanned_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<Vec<(u64, u64)>, CacheError> {
        let _span = sqlite_span(SqliteOp::GetScannedRanges).entered();
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
            if let Some(last) = merged.last_mut()
                && last.1 + 1 >= *start
            {
                if *end > last.1 {
                    last.1 = *end;
                    last.2.clone_from(end_hash);
                }
                continue;
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
}

// ============================================================
// Row decoding
// ============================================================

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
    for bytes in [topic0, topic1, topic2, topic3].into_iter().flatten() {
        topics.push(B256::from_slice(&bytes));
    }

    let log_data = LogData::new_unchecked(topics, Bytes::from(data));
    let mut addr_arr = [0u8; 20];
    addr_arr.copy_from_slice(&address_bytes);

    Ok(Log {
        inner: LogInner {
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
            inner: LogInner {
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
        let store = LogCache::open_in_memory().unwrap();
        let log = make_test_log(100, 0, BLOCK_HASH_A);
        store.insert_logs(&FH, std::slice::from_ref(&log)).unwrap();
        let retrieved = store.get_logs_in_range(&FH, 100, 100).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, log.block_number);
        assert_eq!(retrieved[0].log_index, log.log_index);
    }

    #[test]
    fn logs_skip_pending_without_block_hash() {
        let store = LogCache::open_in_memory().unwrap();
        let mut pending = make_test_log(100, 0, BLOCK_HASH_A);
        pending.block_hash = None;
        store.insert_logs(&FH, &[pending]).unwrap();
        assert!(store.get_logs_in_range(&FH, 100, 100).unwrap().is_empty());
    }

    #[test]
    fn logs_same_block_number_different_hash_coexist() {
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&FH, 100, 200, &BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, 200)]).unwrap();
        assert!(store.get_scanned_ranges(&FH).unwrap().is_empty());
    }

    #[test]
    fn invalidate_reorged_ranges_deletes() {
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
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
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_block_header(100, &BLOCK_HASH_A, &BLOCK_HASH_B, 1_700_000_000)
            .unwrap();
        let h = store.get_block_header(100).unwrap().unwrap();
        assert_eq!(h.height, 100);
        assert_eq!(h.hash, BLOCK_HASH_A);
        assert_eq!(h.parent_hash, BLOCK_HASH_B);
        assert_eq!(h.timestamp, 1_700_000_000);
    }

    // -- reorg cleanup --

    #[test]
    fn cleanup_reorged_before() {
        let store = LogCache::open_in_memory().unwrap();
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
