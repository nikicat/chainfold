//! [`Coverage`] — cross-family scan-coverage bookkeeping.
//!
//! Owns the two tables every source family shares: `scanned_ranges` (which
//! block ranges have been fetched, partitioned per [`ScanPartition`]) and
//! `reorged_blocks` (tombstones for blocks reorged out of the canonical
//! chain). Every family's durable view embeds this one for its coverage
//! claims; the per-family payload tables (`cached_logs`, `cached_blocks`,
//! `cached_btc_blocks`, …) stay in the family modules.
//!
//! Hashes cross this seam as the opaque [`CoverageHash`]: the coverage
//! tables only ever compare hashes for identity, so the per-source hash
//! newtypes (with their source-specific display conventions) convert via
//! `From` exactly at the persist boundary and never leak in.

use std::sync::LazyLock;

use rusqlite::types::{ToSql, ToSqlOutput};

use super::{CacheError, SharedConn, sqlite_span};
use crate::chain::{BlockNumber, BlockRange};

// ============================================================
// CoverageHash
// ============================================================

/// An opaque 32-byte block identity in the coverage tables (`end_block_hash`
/// in `scanned_ranges`, `block_hash` in `reorged_blocks`).
///
/// Deliberately *not* a per-source hash type: the coverage layer only ever
/// stores and compares these bytes, so source display conventions (EVM `0x`
/// hex, Bitcoin byte-reversed hex) don't apply here. Each source defines
/// `From<ItsBlockHash> for CoverageHash` next to its own hash type, and the
/// conversion happens exactly where a page is persisted or a reorg is
/// recorded. No `FromSql` — coverage hashes are compared in SQL, never read
/// back out.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CoverageHash([u8; 32]);

impl CoverageHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 bytes, for hashing and `SQLite` binds.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for CoverageHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A short prefix is enough to correlate logs; the full 32 bytes are
        // rarely useful in diagnostics.
        write!(f, "CoverageHash(0x{})", hex::encode(&self.0[..8]))
    }
}

/// `CoverageHash` persists as its 32 raw bytes in a `BLOB` column. Lives in
/// the persistence layer so the type itself stays `SQLite`-free.
impl ToSql for CoverageHash {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(&self.0[..]))
    }
}

// ============================================================
// FilterHash
// ============================================================

/// A chainfold cache **partition key**: the BLAKE3 hash of a log filter's
/// `(address, topic0..3)`. A newtype over `[u8; 32]`, *not* a block/tx hash
/// alias — a filter hash never appears on-chain, so confusing it with an
/// on-chain hash would be a category error.
///
/// Scopes every cached log, scanned range, and in-memory tree to one filter.
/// Compute one with `compute_filter_hash` (in the EVM cache — the hashing of
/// an EVM filter is a source concern; the *key type* lives here because the
/// shared `scanned_ranges` partitioning speaks it). It binds to `SQLite` as
/// a 32-byte BLOB via a `ToSql` impl in this persistence layer, keeping the
/// type itself persistence-free.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterHash([u8; 32]);

impl FilterHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 bytes — for hashing and the cache layer's `SQLite` binds.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Byte slice view, for `SQLite` binds and `hex` formatting.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for FilterHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A short prefix is enough to correlate logs; the full 32 bytes are
        // rarely useful in diagnostics.
        write!(f, "FilterHash(0x{})", hex::encode(&self.0[..8]))
    }
}

/// `FilterHash` persists as its 32 raw bytes in a `BLOB` column. No
/// `FromSql` — a filter hash is always supplied by the caller, never read
/// back from a row.
impl ToSql for FilterHash {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}

// ============================================================
// ScanPartition
// ============================================================

/// Partition key for `scanned_ranges` rows: which source's coverage a range
/// belongs to. A closed enum rather than a bare [`FilterHash`] — a block
/// history's coverage is not the hash of any filter, and pretending it is
/// would be a type fib.
///
/// The non-filter variants encode to fixed derived blobs
/// (`BLAKE3("chainfold:…")`) in the same BLOB column real filter hashes
/// use; collision with a genuine filter hash is a 2^-256 event. Each source
/// family gets its own variant — the table is shared per cache file, so two
/// families' block coverage must not collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPartition {
    /// Coverage for one EVM log filter's `eth_getLogs` scans.
    Filter(FilterHash),
    /// Coverage for the EVM chain's block history (the blocks source).
    Blocks,
    /// Coverage for the Bitcoin chain's block history.
    BtcBlocks,
}

static BLOCKS_PARTITION_BLOB: LazyLock<[u8; 32]> =
    LazyLock::new(|| *blake3::hash(b"chainfold:blocks").as_bytes());

static BTC_BLOCKS_PARTITION_BLOB: LazyLock<[u8; 32]> =
    LazyLock::new(|| *blake3::hash(b"chainfold:bitcoin:blocks").as_bytes());

impl ScanPartition {
    /// The raw 32 bytes this partition binds as in `scanned_ranges`.
    fn as_blob(&self) -> &[u8; 32] {
        match self {
            Self::Filter(fh) => fh.as_bytes(),
            Self::Blocks => &BLOCKS_PARTITION_BLOB,
            Self::BtcBlocks => &BTC_BLOCKS_PARTITION_BLOB,
        }
    }
}

/// `ScanPartition` persists as its 32-byte blob in the `filter_hash` BLOB
/// column of `scanned_ranges`. Lives here (the persistence layer) like the
/// [`FilterHash`]/[`CoverageHash`] `ToSql` impls above.
impl ToSql for ScanPartition {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(&self.as_blob()[..]))
    }
}

// ============================================================
// Gap computation
// ============================================================

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
// Coverage
// ============================================================

/// Durable scan-coverage view: scanned-range bookkeeping and reorg
/// tombstones, shared by every source family over the one cache file.
///
/// A cheap handle over the shared connection vended by
/// [`CacheDb`](super::CacheDb) (or a family view's `.coverage()` accessor).
/// Ranges are append-only; reorgs are handled by tombstoning block hashes in
/// `reorged_blocks` — ranges whose `end_block_hash` is tombstoned stop
/// counting as coverage at query time.
#[derive(Clone)]
pub struct Coverage {
    conn: SharedConn,
}

impl Coverage {
    pub(crate) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
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
        end_block_hash: CoverageHash,
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
        let _span = sqlite_span!("get_scanned_ranges").entered();
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

    /// Mark blocks as reorged. Payload rows from these blocks will be
    /// excluded from queries. Scanned ranges ending at these blocks become
    /// invalid.
    pub fn mark_reorged(&self, blocks: &[(CoverageHash, BlockNumber)]) -> Result<(), CacheError> {
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
    ///
    /// TODO(family-scoped tombstones): `reorged_blocks` is shared by every
    /// family in the cache file, and heights are only comparable within one
    /// family — a cleanup driven by one family's finality would also drop
    /// the other family's tombstones at numerically lower heights. No
    /// production caller exists yet (Phase 7 wires finality-driven cleanup);
    /// partition the table by family before wiring this up.
    pub fn cleanup_reorged_before(&self, block_number: BlockNumber) -> Result<usize, CacheError> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM reorged_blocks WHERE block_number < ?1",
            rusqlite::params![block_number],
        )?;
        Ok(deleted)
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheDb;

    const FH: FilterHash = FilterHash::new([0xAA; 32]);
    const SP: ScanPartition = ScanPartition::Filter(FH);
    const BLOCK_HASH_A: CoverageHash = CoverageHash::new([0xBB; 32]);
    const BLOCK_HASH_B: CoverageHash = CoverageHash::new([0xBC; 32]);
    const BLOCK_HASH_C: CoverageHash = CoverageHash::new([0xBD; 32]);

    /// Shorthand [`BlockNumber`] constructor.
    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    fn br(from: u64, to: u64) -> BlockRange {
        BlockRange::from_u64(from, to)
    }

    fn coverage() -> Coverage {
        CacheDb::open_in_memory().unwrap().coverage()
    }

    // -- scanned ranges --

    #[test]
    fn range_records_without_merging() {
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 200), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(201, 300), BLOCK_HASH_B)
            .unwrap();
        let ranges = store.get_scanned_ranges(&SP).unwrap();
        assert_eq!(ranges, vec![br(100, 200), br(201, 300)]);
    }

    #[test]
    fn range_reorged_end_block_excluded() {
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 200), BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(200))]).unwrap();
        assert!(store.get_scanned_ranges(&SP).unwrap().is_empty());
    }

    #[test]
    fn invalidate_reorged_ranges_deletes() {
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 200), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(201, 300), BLOCK_HASH_B)
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
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 199), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), BLOCK_HASH_B)
            .unwrap();
        store
            .record_scanned_range(&SP, br(300, 399), BLOCK_HASH_C)
            .unwrap();
        store.compact_ranges(&SP, bn(399)).unwrap();
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(100, 399)]);
    }

    #[test]
    fn compact_preserves_end_block_hash_of_highest() {
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 199), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), BLOCK_HASH_B)
            .unwrap();
        store.compact_ranges(&SP, bn(299)).unwrap();
        // Mark the highest range's end hash reorged; should excise the merged range.
        store.mark_reorged(&[(BLOCK_HASH_B, bn(299))]).unwrap();
        assert!(store.get_scanned_ranges(&SP).unwrap().is_empty());
    }

    #[test]
    fn compact_skips_reorged_ranges() {
        let store = coverage();
        store
            .record_scanned_range(&SP, br(100, 199), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&SP, br(200, 299), BLOCK_HASH_B)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(199))]).unwrap();
        store.compact_ranges(&SP, bn(299)).unwrap();
        // The reorged range is left alone (filtered out of visible ranges); the
        // non-reorged range stays as-is since it's the only compactable entry.
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(200, 299)]);
    }

    // -- partitions --

    #[test]
    fn scan_partitions_are_mutually_disjoint() {
        // Coverage recorded under any partition must not leak into another —
        // filters, EVM blocks, and Bitcoin blocks all share one table (and,
        // in the one shared DB file, one row space).
        let store = coverage();
        store
            .record_scanned_range(&ScanPartition::Blocks, br(100, 200), BLOCK_HASH_A)
            .unwrap();
        store
            .record_scanned_range(&ScanPartition::BtcBlocks, br(500, 600), BLOCK_HASH_C)
            .unwrap();
        store
            .record_scanned_range(&SP, br(300, 400), BLOCK_HASH_B)
            .unwrap();
        assert_eq!(
            store.get_scanned_ranges(&ScanPartition::Blocks).unwrap(),
            vec![br(100, 200)]
        );
        assert_eq!(
            store.get_scanned_ranges(&ScanPartition::BtcBlocks).unwrap(),
            vec![br(500, 600)]
        );
        assert_eq!(store.get_scanned_ranges(&SP).unwrap(), vec![br(300, 400)]);
    }

    #[test]
    fn blocks_scanned_range_reorged_end_hash_excluded() {
        let store = coverage();
        store
            .record_scanned_range(&ScanPartition::Blocks, br(100, 200), BLOCK_HASH_A)
            .unwrap();
        store.mark_reorged(&[(BLOCK_HASH_A, bn(200))]).unwrap();
        assert!(
            store
                .get_scanned_ranges(&ScanPartition::Blocks)
                .unwrap()
                .is_empty()
        );
    }

    // -- compute_gaps --

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

    // -- reorg cleanup --

    #[test]
    fn cleanup_reorged_before() {
        let store = coverage();
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
