//! [`LogCache`] — the durable EVM log cache.
//!
//! Owns the EVM payload tables: raw logs (`cached_logs`), full blocks
//! (`cached_blocks`), and cached block headers (`cached_block_headers`).
//! The cross-family scanned-range/reorg bookkeeping lives in the shared
//! [`Coverage`](crate::cache::Coverage) view, reachable via
//! [`LogCache::coverage`].
//!
//! Exposes only the narrow durable surface — bulk read/write and header
//! lookup. Navigation queries (first/last log) live in the in-memory
//! [`LogIndex`](crate::evm::LogIndex).

use std::path::Path;

use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
use alloy::rpc::types::Log;

use super::{col_address, col_b256, col_b256_opt};
use crate::arch::Timestamp;
use crate::cache::{CacheDb, CacheError, Coverage, FilterHash, SharedConn, sqlite_span};
use crate::chain::{BlockNumber, BlockRange};
use crate::evm::types::{BlockHash, EvmBlock, LogIdx, Topic};
use crate::share::Share;

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

// ============================================================
// LogCache
// ============================================================

/// Durable SQLite-backed log + block + header cache.
///
/// A cheap handle over the shared connection vended by
/// [`CacheDb`](crate::cache::CacheDb). Logs are append-only; reorgs are
/// handled by tombstoning blocks via the shared [`Coverage`] view and
/// filtering them out at query time.
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        Ok(CacheDb::open(path)?.logs())
    }

    /// Open a standalone in-memory log cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(CacheDb::open_in_memory()?.logs())
    }

    /// The shared cross-family coverage view (scanned ranges + reorg
    /// tombstones) over this cache's connection.
    pub fn coverage(&self) -> Coverage {
        Coverage::from_conn(self.conn.share())
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
        let _span =
            sqlite_span!("get_logs_in_range", filter = ?filter_hash, range = %range).entered();
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

    // --------------------------------------------------------
    // Blocks (the blocks-history source)
    // --------------------------------------------------------

    /// Insert full blocks, replacing by height — a reorg refetch overwrites
    /// the stale fork row in place. Written only by the blocks fetch path
    /// (see the `cached_blocks` schema comment).
    pub fn insert_blocks(&self, blocks: &[EvmBlock]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO cached_blocks
                 (height, hash, parent_hash, timestamp, tx_count, gas_used, gas_limit,
                  base_fee, miner)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for b in blocks {
                stmt.execute(rusqlite::params![
                    b.number,
                    b.hash,
                    b.parent_hash,
                    b.timestamp,
                    b.tx_count,
                    b.gas_used,
                    b.gas_limit,
                    b.base_fee,
                    b.miner.as_slice(),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// All cached, non-reorged blocks with heights in `range` (inclusive),
    /// ordered by height.
    pub fn get_blocks_in_range(&self, range: BlockRange) -> Result<Vec<EvmBlock>, CacheError> {
        let _span = sqlite_span!("get_blocks_in_range", range = %range).entered();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT height, hash, parent_hash, timestamp, tx_count, gas_used, gas_limit,
                    base_fee, miner
             FROM cached_blocks
             WHERE height >= ?1 AND height <= ?2
               AND hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY height ASC",
        )?;
        let blocks = stmt
            .query_map(rusqlite::params![range.from, range.to], row_to_evm_block)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(blocks)
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

fn row_to_evm_block(row: &rusqlite::Row) -> Result<EvmBlock, rusqlite::Error> {
    Ok(EvmBlock {
        number: row.get("height")?,
        hash: row.get("hash")?,
        parent_hash: row.get("parent_hash")?,
        timestamp: row.get("timestamp")?,
        tx_count: row.get("tx_count")?,
        gas_used: row.get("gas_used")?,
        gas_limit: row.get("gas_limit")?,
        base_fee: row.get("base_fee")?,
        miner: col_address(row, "miner")?,
    })
}

// ============================================================
// Tests
// ============================================================

/// Minimal `cached_logs` fixture. Module-level (not inside `mod tests`) so
/// the parent [`cache`](super) test module can seed logs through
/// [`LogCache::insert_logs`] too.
#[cfg(test)]
pub(super) fn make_test_log(block: BlockNumber, log_idx: LogIdx, block_hash: BlockHash) -> Log {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::TxCount;

    const FH: FilterHash = FilterHash::new([0xAA; 32]);
    const BLOCK_HASH_A: BlockHash = BlockHash::repeat_byte(0xBB);
    const BLOCK_HASH_B: BlockHash = BlockHash::repeat_byte(0xBC);

    /// Shorthand [`BlockNumber`] constructor.
    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    /// Base for fixture timestamps — [`ts`]`(offset)` = `TS_BASE + offset`.
    const TS_BASE: Timestamp = Timestamp::new(1_700_000_000);

    fn ts(offset: u64) -> Timestamp {
        TS_BASE + offset
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
        store
            .coverage()
            .mark_reorged(&[(BLOCK_HASH_A.into(), bn(100))])
            .unwrap();
        let retrieved = store.get_logs_in_range(&FH, br(100, 101)).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, Some(101));
    }

    fn br(from: u64, to: u64) -> BlockRange {
        BlockRange::from_u64(from, to)
    }

    // -- blocks (cached_blocks) --

    use crate::arch::Timestamp;
    use crate::evm::types::{Gas, GasPrice};

    fn make_test_block(
        number: BlockNumber,
        hash: BlockHash,
        base_fee: Option<GasPrice>,
    ) -> EvmBlock {
        EvmBlock {
            number,
            hash,
            parent_hash: BlockHash::repeat_byte(0x11),
            timestamp: ts(number.get()),
            tx_count: TxCount::new(42),
            gas_used: Gas::new(14_900_000),
            gas_limit: Gas::new(30_000_000),
            base_fee,
            miner: Address::repeat_byte(0x99),
        }
    }

    #[test]
    fn blocks_round_trip_all_fields() {
        let store = LogCache::open_in_memory().unwrap();
        let with_fee = make_test_block(bn(100), BLOCK_HASH_A, Some(GasPrice::new(2_410_000_000)));
        let pre_1559 = make_test_block(bn(101), BLOCK_HASH_B, None);
        store.insert_blocks(&[with_fee, pre_1559]).unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 101))
            .unwrap();
        assert_eq!(got, vec![with_fee, pre_1559]);
    }

    #[test]
    fn blocks_replace_by_height() {
        // A reorg refetch at the same height overwrites the stale fork row.
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_blocks(&[make_test_block(bn(100), BLOCK_HASH_A, None)])
            .unwrap();
        let refetched = make_test_block(bn(100), BLOCK_HASH_B, None);
        store.insert_blocks(&[refetched]).unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 100))
            .unwrap();
        assert_eq!(got, vec![refetched]);
    }

    #[test]
    fn blocks_reorged_hash_filtered_out() {
        let store = LogCache::open_in_memory().unwrap();
        store
            .insert_blocks(&[
                make_test_block(bn(100), BLOCK_HASH_A, None),
                make_test_block(bn(101), BLOCK_HASH_B, None),
            ])
            .unwrap();
        store
            .coverage()
            .mark_reorged(&[(BLOCK_HASH_A.into(), bn(100))])
            .unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 101))
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].number, bn(101));
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
}
