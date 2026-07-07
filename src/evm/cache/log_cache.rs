//! [`LogCache`] — the durable EVM log cache.
//!
//! Owns the reorg-coherent set of tables: raw logs (`cached_logs`),
//! scanned-range bookkeeping (`scanned_ranges`), reorg tombstones
//! (`reorged_blocks`), and cached block headers (`cached_block_headers`).
//!
//! Exposes only the narrow durable surface — bulk read/write, scanned-range
//! bookkeeping, reorg invalidation, and header lookup. Navigation queries
//! (first/last log) live in the in-memory [`LogIndex`](crate::evm::LogIndex).

use std::sync::LazyLock;

use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
use alloy::rpc::types::Log;
use rusqlite::types::{ToSql, ToSqlOutput};

use super::{CacheError, SharedConn, col_address, col_b256, col_b256_opt};
use crate::arch::Timestamp;
use crate::evm::observe::{SqliteOp, sqlite_span};
use crate::evm::types::{BlockHash, BlockNumber, BlockRange, FilterHash, LogIdx, Topic};

// ============================================================
// Helpers
// ============================================================

/// Compute a deterministic BLAKE3 hash of (address, topic0-3).
///
/// Null topics use a 0xFF sentinel (all-ones, 32 bytes) to distinguish
/// "no topic filter" from any real topic value.
pub fn compute_filter_hash(address: &Address, topics: &[Option<Topic>; 4]) -> FilterHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(address.as_slice());
    let sentinel = [0xFF_u8; 32];
    for topic in topics {
        match topic {
            Some(t) => hasher.update(t.as_slice()),
            None => hasher.update(&sentinel),
        };
    }
    FilterHash::new(*hasher.finalize().as_bytes())
}

/// Compute uncovered sub-ranges within `requested` given sorted, non-overlapping `scanned` ranges.
///
/// Both `requested` and each entry in `scanned` are inclusive block ranges.
/// The set arithmetic stays in raw `u64` internally; only the boundary is typed.
pub fn compute_gaps(requested: BlockRange, scanned: &[BlockRange]) -> Vec<BlockRange> {
    let (req_from, req_to) = requested.as_u64();
    let mut gaps = Vec::new();
    let mut cursor = req_from;
    for &range in scanned {
        let (start, end) = range.as_u64();
        if start > req_to {
            break;
        }
        if end < cursor {
            continue;
        }
        if start > cursor {
            gaps.push(BlockRange::from_u64(cursor, start - 1));
        }
        cursor = cursor.max(end + 1);
        if cursor > req_to {
            break;
        }
    }
    if cursor <= req_to {
        gaps.push(BlockRange::from_u64(cursor, req_to));
    }
    gaps
}

// ============================================================
// Types
// ============================================================

/// A cached block header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub height: BlockNumber,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: Timestamp,
}

/// Partition key for `scanned_ranges` rows: which source's coverage a range
/// belongs to. A closed enum rather than a bare [`FilterHash`] — the blocks
/// history's coverage is not the hash of any filter, and pretending it is
/// would be a type fib.
///
/// [`Blocks`](Self::Blocks) encodes to a fixed derived blob
/// (`BLAKE3("chainfold:blocks")`) in the same BLOB column real filter hashes
/// use; collision with a genuine filter hash is a 2^-256 event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPartition {
    /// Coverage for one log filter's `eth_getLogs` scans.
    Filter(FilterHash),
    /// Coverage for the chain's block history (the blocks source).
    Blocks,
}

static BLOCKS_PARTITION_BLOB: LazyLock<[u8; 32]> =
    LazyLock::new(|| *blake3::hash(b"chainfold:blocks").as_bytes());

impl ScanPartition {
    /// The raw 32 bytes this partition binds as in `scanned_ranges`.
    fn as_blob(&self) -> &[u8; 32] {
        match self {
            Self::Filter(fh) => fh.as_bytes(),
            Self::Blocks => &BLOCKS_PARTITION_BLOB,
        }
    }
}

/// `ScanPartition` persists as its 32-byte blob in the `filter_hash` BLOB
/// column of `scanned_ranges`. Lives here (the persistence layer) like the
/// [`FilterHash`]/[`ChainId`](crate::evm::types::ChainId) `ToSql` impls in
/// [`cache`](super).
impl ToSql for ScanPartition {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(&self.as_blob()[..]))
    }
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
    pub fn insert_logs(&self, filter_hash: &FilterHash, logs: &[Log]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO cached_logs
                 (filter_hash, block_number, log_index, address, topic0, topic1, topic2, topic3,
                  data, block_hash, transaction_hash, transaction_index, block_timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )?;
            for log in logs {
                let Some(block_hash) = log.block_hash else {
                    continue;
                };
                let topics = log.topics();
                stmt.execute(rusqlite::params![
                    filter_hash,
                    log.block_number.unwrap_or(0),
                    log.log_index.unwrap_or(0),
                    log.address().as_slice(),
                    topics.first().map(|t| t.as_slice().to_vec()),
                    topics.get(1).map(|t| t.as_slice().to_vec()),
                    topics.get(2).map(|t| t.as_slice().to_vec()),
                    topics.get(3).map(|t| t.as_slice().to_vec()),
                    log.data().data.as_ref(),
                    block_hash.as_slice(),
                    log.transaction_hash.map(|h| h.0.to_vec()),
                    log.transaction_index.unwrap_or(0),
                    log.block_timestamp,
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
        filter_hash: &FilterHash,
        block_hash: &BlockHash,
        log_index: LogIdx,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index, block_timestamp
             FROM cached_logs
             WHERE filter_hash = ?1 AND block_hash = ?2 AND log_index = ?3
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![filter_hash, block_hash, log_index],
            row_to_log,
        )?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// All cached, non-reorged logs for `filter_hash` with block heights in
    /// `range` (inclusive), ordered by (`block_number`, `log_index`).
    pub fn get_logs_in_range(
        &self,
        filter_hash: &FilterHash,
        range: BlockRange,
    ) -> Result<Vec<Log>, CacheError> {
        let _span = sqlite_span(SqliteOp::GetLogsInRange).entered();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index, block_timestamp
             FROM cached_logs
             WHERE filter_hash = ?1 AND block_number >= ?2 AND block_number <= ?3
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC",
        )?;
        let logs = stmt
            .query_map(
                rusqlite::params![filter_hash, range.from, range.to],
                row_to_log,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }

    // --------------------------------------------------------
    // Scanned ranges
    // --------------------------------------------------------

    /// Record a scanned block range (inclusive). Does NOT merge — call
    /// [`Self::compact_ranges`] separately for finalized ranges.
    pub fn record_scanned_range(
        &self,
        partition: &ScanPartition,
        range: BlockRange,
        end_block_hash: &BlockHash,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO scanned_ranges (filter_hash, start_block, end_block, end_block_hash)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![partition, range.from, range.to, end_block_hash],
        )?;
        Ok(())
    }

    /// All valid scanned ranges for `partition`, sorted by start.
    /// Excludes ranges whose `end_block_hash` is in `reorged_blocks`.
    pub fn get_scanned_ranges(
        &self,
        partition: &ScanPartition,
    ) -> Result<Vec<BlockRange>, CacheError> {
        let _span = sqlite_span(SqliteOp::GetScannedRanges).entered();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT start_block, end_block FROM scanned_ranges
             WHERE filter_hash = ?1
               AND end_block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY start_block ASC",
        )?;
        let ranges = stmt
            .query_map(rusqlite::params![partition], |row| {
                Ok(BlockRange::new(
                    row.get("start_block")?,
                    row.get("end_block")?,
                ))
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
        partition: &ScanPartition,
        up_to_block: BlockNumber,
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
                .query_map(rusqlite::params![partition, up_to_block], |row| {
                    Ok((
                        row.get("id")?,
                        row.get("start_block")?,
                        row.get("end_block")?,
                        row.get("end_block_hash")?,
                    ))
                })?
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
                    rusqlite::params![partition, start, end, &end_hash[..]],
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
    pub fn mark_reorged(&self, blocks: &[(BlockHash, BlockNumber)]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO reorged_blocks (block_hash, block_number) VALUES (?1, ?2)",
            )?;
            for (hash, number) in blocks {
                stmt.execute(rusqlite::params![hash, number])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete scanned ranges whose `end_block_hash` is in `reorged_blocks`.
    /// Call after [`Self::mark_reorged`] so gaps are recomputed for affected
    /// block ranges.
    pub fn invalidate_reorged_ranges(
        &self,
        partition: &ScanPartition,
    ) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM scanned_ranges
             WHERE filter_hash = ?1
               AND end_block_hash IN (SELECT block_hash FROM reorged_blocks)",
            rusqlite::params![partition],
        )?;
        Ok(deleted)
    }

    /// Remove entries from `reorged_blocks` below the finality boundary.
    /// Once a block is finalized, it can't be reorged so the tombstone is
    /// safe to drop.
    pub fn cleanup_reorged_before(&self, block_number: BlockNumber) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM reorged_blocks WHERE block_number < ?1",
            rusqlite::params![block_number],
        )?;
        Ok(deleted)
    }

    // --------------------------------------------------------
    // Block headers
    // --------------------------------------------------------

    /// Insert or replace a cached block header.
    pub fn insert_block_header(
        &self,
        height: BlockNumber,
        hash: &BlockHash,
        parent_hash: &BlockHash,
        timestamp: Timestamp,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO cached_block_headers (height, hash, parent_hash, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![height, hash, parent_hash, timestamp],
        )?;
        Ok(())
    }

    /// Get a cached block header by height.
    pub fn get_block_header(&self, height: BlockNumber) -> Result<Option<BlockHeader>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT hash, parent_hash, timestamp FROM cached_block_headers WHERE height = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![height], |row| {
            Ok(BlockHeader {
                height,
                hash: row.get("hash")?,
                parent_hash: row.get("parent_hash")?,
                timestamp: row.get("timestamp")?,
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
//
// Columns are read by name, not index — a swapped column in the SELECT list
// then fails loudly instead of silently decoding into the wrong field.

fn row_to_log(row: &rusqlite::Row) -> Result<Log, rusqlite::Error> {
    let topics: Vec<B256> = [
        col_b256_opt(row, "topic0")?,
        col_b256_opt(row, "topic1")?,
        col_b256_opt(row, "topic2")?,
        col_b256_opt(row, "topic3")?,
    ]
    .into_iter()
    .flatten()
    .collect();
    let data: Vec<u8> = row.get("data")?;

    Ok(Log {
        inner: LogInner {
            address: col_address(row, "address")?,
            data: LogData::new_unchecked(topics, Bytes::from(data)),
        },
        block_hash: Some(col_b256(row, "block_hash")?),
        block_number: Some(row.get("block_number")?),
        block_timestamp: row.get("block_timestamp")?,
        transaction_hash: col_b256_opt(row, "transaction_hash")?,
        transaction_index: Some(row.get("transaction_index")?),
        log_index: Some(row.get("log_index")?),
        removed: false,
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    const FH: FilterHash = FilterHash::new([0xAA; 32]);
    const SP: ScanPartition = ScanPartition::Filter(FH);
    const BLOCK_HASH_A: BlockHash = BlockHash::repeat_byte(0xBB);
    const BLOCK_HASH_B: BlockHash = BlockHash::repeat_byte(0xBC);
    const BLOCK_HASH_C: BlockHash = BlockHash::repeat_byte(0xBD);

    /// Shorthand [`BlockNumber`] constructor.
    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    /// Base for fixture timestamps — [`ts`]`(offset)` = `TS_BASE + offset`.
    const TS_BASE: Timestamp = Timestamp::new(1_700_000_000);

    fn ts(offset: u64) -> Timestamp {
        TS_BASE + offset
    }

    fn make_test_log(block: BlockNumber, log_idx: LogIdx, block_hash: BlockHash) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);

        Log {
            inner: LogInner {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(block_hash.b256()),
            block_number: Some(block.get()),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(u64::from(log_idx.get())),
            removed: false,
        }
    }

    // -- compute_filter_hash --

    #[test]
    fn filter_hash_deterministic() {
        let addr = Address::repeat_byte(0x42);
        let topics = [Some(Topic::new(B256::repeat_byte(0xAA))), None, None, None];
        assert_eq!(
            compute_filter_hash(&addr, &topics),
            compute_filter_hash(&addr, &topics)
        );
    }

    #[test]
    fn filter_hash_different_address() {
        let topics = [None; 4];
        assert_ne!(
            compute_filter_hash(&Address::repeat_byte(0x01), &topics),
            compute_filter_hash(&Address::repeat_byte(0x02), &topics)
        );
    }

    #[test]
    fn filter_hash_none_vs_some() {
        let addr = Address::repeat_byte(0x42);
        assert_ne!(
            compute_filter_hash(&addr, &[None, None, None, None]),
            compute_filter_hash(&addr, &[Some(Topic::new(B256::ZERO)), None, None, None])
        );
    }

    // -- logs --

    #[test]
    fn logs_round_trip() {
        let store = LogCache::open_in_memory().unwrap();
        let log = make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_A);
        store.insert_logs(&FH, std::slice::from_ref(&log)).unwrap();
        let retrieved = store.get_logs_in_range(&FH, br(100, 100)).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, log.block_number);
        assert_eq!(retrieved[0].log_index, log.log_index);
    }

    #[test]
    fn logs_preserve_block_timestamp() {
        // The block_timestamp column survives the SQLite round-trip when the
        // provider supplied it, and stays NULL/None when it didn't.
        let store = LogCache::open_in_memory().unwrap();
        let mut with_ts = make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_A);
        with_ts.block_timestamp = Some(1_700_000_123);
        let without_ts = make_test_log(bn(101), LogIdx::ZERO, BLOCK_HASH_B); // block_timestamp: None
        store.insert_logs(&FH, &[with_ts, without_ts]).unwrap();
        let got = store.get_logs_in_range(&FH, br(100, 101)).unwrap();
        assert_eq!(got[0].block_timestamp, Some(1_700_000_123));
        assert_eq!(got[1].block_timestamp, None);
    }

    #[test]
    fn logs_skip_pending_without_block_hash() {
        let store = LogCache::open_in_memory().unwrap();
        let mut pending = make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_A);
        pending.block_hash = None;
        store.insert_logs(&FH, &[pending]).unwrap();
        assert!(
            store
                .get_logs_in_range(&FH, br(100, 100))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn logs_same_block_number_different_hash_coexist() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_logs(
                &FH,
                &[
                    make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_A),
                    make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        let retrieved = store.get_logs_in_range(&FH, br(100, 100)).unwrap();
        assert_eq!(retrieved.len(), 2);
    }

    #[test]
    fn logs_reorged_blocks_filtered_out() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_logs(
                &FH,
                &[
                    make_test_log(bn(100), LogIdx::ZERO, BLOCK_HASH_A),
                    make_test_log(bn(101), LogIdx::ZERO, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(100))]).unwrap();
        let retrieved = store.get_logs_in_range(&FH, br(100, 101)).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, Some(101));
    }

    // -- scanned ranges --

    #[test]
    fn range_records_without_merging() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 200), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(201, 300), &BLOCK_HASH_B)
            .unwrap();
        let ranges = store.get_scanned_ranges(&SP).unwrap();
        assert_eq!(ranges, vec![br(100, 200), br(201, 300)]);
    }

    #[test]
    fn range_reorged_end_block_excluded() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 200), &BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(200))]).unwrap();
        assert!(store.get_scanned_ranges(&SP).unwrap().is_empty());
    }

    #[test]
    fn invalidate_reorged_ranges_deletes() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 200), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(201, 300), &BLOCK_HASH_B)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(200))]).unwrap();
        let deleted = store.invalidate_reorged_ranges(&SP).unwrap();
        assert_eq!(deleted, 1);
        // After physical deletion, get_scanned_ranges no longer filters via the reorged join.
        let ranges = store.get_scanned_ranges(&SP).unwrap();
        assert_eq!(ranges, vec![br(201, 300)]);
    }

    #[test]
    fn compact_merges_finalized_ranges() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 199), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), &BLOCK_HASH_B)
            .unwrap();
        store
            .record_scanned_range(&SP, br(300, 399), &BLOCK_HASH_C)
            .unwrap();
        store.compact_ranges(&SP, bn(399)).unwrap();
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(100, 399)]);
    }

    #[test]
    fn compact_preserves_end_block_hash_of_highest() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 199), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), &BLOCK_HASH_B)
            .unwrap();
        store.compact_ranges(&SP, bn(299)).unwrap();
        // Mark the highest range's end hash reorged; should excise the merged range.
        store.mark_reorged(&[(BLOCK_HASH_B, bn(299))]).unwrap();
        assert!(store.get_scanned_ranges(&SP).unwrap().is_empty());
    }

    #[test]
    fn compact_skips_reorged_ranges() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&SP, br(100, 199), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), &BLOCK_HASH_B)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(199))]).unwrap();
        store.compact_ranges(&SP, bn(299)).unwrap();
        // The reorged range is left alone (filtered out of visible ranges); the
        // non-reorged range stays as-is since it's the only compactable entry.
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(200, 299)]);
    }

    // -- compute_gaps --

    fn br(from: u64, to: u64) -> BlockRange {
        BlockRange::from_u64(from, to)
    }

    #[test]
    fn gaps_no_scanned_returns_full_range() {
        assert_eq!(compute_gaps(br(100, 200), &[]), vec![br(100, 200)]);
    }

    #[test]
    fn gaps_fully_covered_returns_empty() {
        assert_eq!(
            compute_gaps(br(100, 200), &[br(50, 250)]),
            Vec::<BlockRange>::new()
        );
    }

    #[test]
    fn gaps_partial_coverage() {
        assert_eq!(
            compute_gaps(br(100, 300), &[br(150, 200)]),
            vec![br(100, 149), br(201, 300)]
        );
    }

    // -- blocks scan partition --

    #[test]
    fn blocks_scan_partition_disjoint_from_filters() {
        // Coverage recorded under the Blocks partition must not leak into any
        // filter's partition, and vice versa — they share one table.
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&ScanPartition::Blocks, br(100, 200), &BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(300, 400), &BLOCK_HASH_B)
            .unwrap();
        assert_eq!(
            store.get_scanned_ranges(&ScanPartition::Blocks).unwrap(),
            vec![br(100, 200)]
        );
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(300, 400)]);
    }

    #[test]
    fn blocks_scanned_range_reorged_end_hash_excluded() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .record_scanned_range(&ScanPartition::Blocks, br(100, 200), &BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(200))]).unwrap();
        assert!(
            store
                .get_scanned_ranges(&ScanPartition::Blocks)
                .unwrap()
                .is_empty()
        );
    }

    // -- block headers --

    #[test]
    fn block_header_round_trip() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_block_header(bn(100), &BLOCK_HASH_A, &BLOCK_HASH_B, ts(0))
            .unwrap();
        let h = store.get_block_header(bn(100)).unwrap().unwrap();
        assert_eq!(h.height, bn(100));
        assert_eq!(h.hash, BLOCK_HASH_A);
        assert_eq!(h.parent_hash, BLOCK_HASH_B);
        assert_eq!(h.timestamp, ts(0));
    }

    // -- reorg cleanup --

    #[test]
    fn cleanup_reorged_before() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .mark_reorged(&[
                (BLOCK_HASH_A, bn(100)),
                (BLOCK_HASH_B, bn(200)),
                (BLOCK_HASH_C, bn(300)),
            ])
            .unwrap();
        let deleted = store.cleanup_reorged_before(bn(250)).unwrap();
        assert_eq!(deleted, 2);
    }
}
