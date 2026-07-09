//! [`BtcBlockCache`] — the durable Bitcoin block cache.
//!
//! Owns the Bitcoin payload table (`cached_btc_blocks`); the cross-family
//! scanned-range/reorg bookkeeping lives in the shared
//! [`Coverage`](crate::cache::Coverage) view, reachable via
//! [`BtcBlockCache::coverage`]. The Bitcoin analog of the EVM `LogCache`'s
//! blocks surface.

use std::path::Path;

use super::types::{BtcBlock, BtcBlockHash, CompactTarget};
use crate::cache::{CacheDb, CacheError, Coverage, SharedConn, sqlite_span};
use crate::chain::{BlockNumber, BlockRange};
use crate::share::Share;

/// `BtcBlockHash` persists as its 32 raw bytes (RPC/display order) in a
/// `BLOB` column; `CompactTarget` as its exact `u32` bits in an `INTEGER`
/// column. In the persistence layer, like every domain-newtype bind — the
/// types themselves stay `SQLite`-free.
mod sql_binds {
    use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

    use super::{BtcBlockHash, CompactTarget};

    impl ToSql for BtcBlockHash {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::from(self.as_slice()))
        }
    }

    impl FromSql for BtcBlockHash {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            <[u8; 32]>::column_result(value).map(Self::new)
        }
    }

    impl ToSql for CompactTarget {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::from(i64::from(self.get())))
        }
    }

    impl FromSql for CompactTarget {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            u32::column_result(value).map(Self::new)
        }
    }
}

/// Durable SQLite-backed Bitcoin block cache.
///
/// A cheap handle over the shared connection vended by
/// [`CacheDb`](crate::cache::CacheDb). Rows are height-keyed and replaced
/// in place on reorg refetch; reorgs are handled by tombstoning blocks via
/// the shared [`Coverage`] view and filtering them out at query time.
pub struct BtcBlockCache {
    conn: SharedConn,
}

impl BtcBlockCache {
    pub(super) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
    }

    /// Open a standalone Bitcoin block cache at the given path. Creates the
    /// DB and runs migrations if needed. Equivalent to
    /// `CacheDb::open(path)?.btc_blocks()`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        Ok(CacheDb::open(path)?.btc_blocks())
    }

    /// Open a standalone in-memory Bitcoin block cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(CacheDb::open_in_memory()?.btc_blocks())
    }

    /// The shared cross-family coverage view (scanned ranges + reorg
    /// tombstones) over this cache's connection.
    pub fn coverage(&self) -> Coverage {
        Coverage::from_conn(self.conn.share())
    }

    /// Insert full blocks, replacing by height — a reorg refetch overwrites
    /// the stale fork row in place. Written only by the blocks fetch path
    /// (see the `cached_btc_blocks` schema comment).
    pub fn insert_blocks(&self, blocks: &[BtcBlock]) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO cached_btc_blocks
                 (height, hash, prev_hash, time, tx_count, bits)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for b in blocks {
                stmt.execute(rusqlite::params![
                    b.height,
                    b.hash,
                    b.prev_hash,
                    b.time,
                    b.tx_count,
                    b.bits,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// All cached, non-reorged blocks with heights in `range` (inclusive),
    /// ordered by height.
    pub fn get_blocks_in_range(&self, range: BlockRange) -> Result<Vec<BtcBlock>, CacheError> {
        let _span = sqlite_span!("get_btc_blocks_in_range").entered();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT height, hash, prev_hash, time, tx_count, bits
             FROM cached_btc_blocks
             WHERE height >= ?1 AND height <= ?2
               AND hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY height ASC",
        )?;
        let blocks = stmt
            .query_map(rusqlite::params![range.from, range.to], row_to_btc_block)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(blocks)
    }

    /// One cached, non-reorged block by height — the time-anchor read.
    pub fn get_block_at(&self, height: BlockNumber) -> Result<Option<BtcBlock>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT height, hash, prev_hash, time, tx_count, bits
             FROM cached_btc_blocks
             WHERE height = ?1
               AND hash NOT IN (SELECT block_hash FROM reorged_blocks)",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![height], row_to_btc_block)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }
}

/// The Bitcoin view over the shared cache file. Living here — not in
/// [`crate::cache`] — so the shared layer never names a family.
impl CacheDb {
    /// Bitcoin block cache view (`cached_btc_blocks` + shared coverage).
    pub fn btc_blocks(&self) -> BtcBlockCache {
        BtcBlockCache::from_conn(self.shared_conn())
    }
}

// Columns are read by name, not index — a swapped column in the SELECT list
// then fails loudly instead of silently decoding into the wrong field.
fn row_to_btc_block(row: &rusqlite::Row) -> Result<BtcBlock, rusqlite::Error> {
    Ok(BtcBlock {
        height: row.get("height")?,
        hash: row.get("hash")?,
        prev_hash: row.get("prev_hash")?,
        time: row.get("time")?,
        tx_count: row.get("tx_count")?,
        bits: row.get("bits")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Timestamp;
    use crate::cache::ScanPartition;
    use crate::chain::TxCount;

    const HASH_A: BtcBlockHash = BtcBlockHash::repeat_byte(0xBB);
    const HASH_B: BtcBlockHash = BtcBlockHash::repeat_byte(0xBC);

    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    fn mk_block(height: u64, hash: BtcBlockHash) -> BtcBlock {
        BtcBlock {
            height: bn(height),
            hash,
            prev_hash: BtcBlockHash::repeat_byte(0x11),
            time: Timestamp::new(1_700_000_000 + height * 600),
            tx_count: TxCount::new(2_000),
            bits: CompactTarget::new(0x1703_2f14),
        }
    }

    #[test]
    fn blocks_round_trip_all_fields() {
        let store = BtcBlockCache::open_in_memory().unwrap();
        let a = mk_block(100, HASH_A);
        let b = mk_block(101, HASH_B);
        store.insert_blocks(&[a, b]).unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 101))
            .unwrap();
        assert_eq!(got, vec![a, b]);
        assert_eq!(store.get_block_at(bn(100)).unwrap(), Some(a));
        assert_eq!(store.get_block_at(bn(102)).unwrap(), None);
    }

    #[test]
    fn blocks_replace_by_height() {
        // A reorg refetch at the same height overwrites the stale fork row.
        let store = BtcBlockCache::open_in_memory().unwrap();
        store.insert_blocks(&[mk_block(100, HASH_A)]).unwrap();
        let refetched = mk_block(100, HASH_B);
        store.insert_blocks(&[refetched]).unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 100))
            .unwrap();
        assert_eq!(got, vec![refetched]);
    }

    #[test]
    fn blocks_reorged_hash_filtered_out() {
        let store = BtcBlockCache::open_in_memory().unwrap();
        store
            .insert_blocks(&[mk_block(100, HASH_A), mk_block(101, HASH_B)])
            .unwrap();
        store
            .coverage()
            .mark_reorged(&[(HASH_A.into(), bn(100))])
            .unwrap();
        let got = store
            .get_blocks_in_range(BlockRange::from_u64(100, 101))
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].height, bn(101));
        assert_eq!(store.get_block_at(bn(100)).unwrap(), None);
    }

    #[test]
    fn coverage_rides_the_btc_partition() {
        // The view's coverage handle writes under BtcBlocks without
        // touching the EVM partitions (three-way disjointness is pinned in
        // the coverage tests; this is the view-level smoke).
        let store = BtcBlockCache::open_in_memory().unwrap();
        store
            .coverage()
            .record_scanned_range(
                &ScanPartition::BtcBlocks,
                BlockRange::from_u64(0, 100),
                HASH_A.into(),
            )
            .unwrap();
        assert_eq!(
            store
                .coverage()
                .get_scanned_ranges(&ScanPartition::BtcBlocks)
                .unwrap(),
            vec![BlockRange::from_u64(0, 100)]
        );
        assert!(
            store
                .coverage()
                .get_scanned_ranges(&ScanPartition::Blocks)
                .unwrap()
                .is_empty()
        );
    }
}
