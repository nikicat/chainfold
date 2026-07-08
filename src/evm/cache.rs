//! Durable `SQLite` cache for the EVM source.
//!
//! One on-disk `SQLite` file backs several independent concerns. Rather than
//! a single god-object owning every table, the file is opened once by
//! [`CacheDb`], which then vends narrow per-concern handles that all share
//! the same connection:
//!
//! - [`LogCache`] — the EVM log cache proper: raw logs, scanned-range
//!   bookkeeping, reorg tombstones, and cached block headers.
//! - [`Erc20Cache`] — ERC20 token metadata + historical balances.
//! - [`EventSignatureCache`] — resolved `(event name) → signature/topic0/abi`
//!   rows for the signature resolver.
//!
//! All handles serialize through one [`Mutex<Connection>`]; migrations and
//! the [`CACHE_VERSION`] are unified because there is a single file.
//!
//! Navigation queries (first/last log) live in the in-memory
//! [`LogIndex`](super::LogIndex), not here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use bytesize::ByteSize;
use rusqlite::Connection;
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

use crate::arch::Timestamp;
use crate::evm::types::{
    BlockHash, BlockNumber, ChainId, FilterHash, Gas, GasPrice, LogIdx, TxCount, TxHash,
};

mod erc20_cache;
mod event_signature_cache;
mod log_cache;

pub use erc20_cache::{CachedErc20Metadata, Erc20Cache};
pub use event_signature_cache::{EventSignatureCache, EventSignatureRow};
pub use log_cache::{BlockHeader, LogCache, ScanPartition, compute_filter_hash, compute_gaps};

/// `ChainId` persists as its underlying `u64` in an `INTEGER` column. Defining
/// `ToSql` here — in the persistence layer, not on the domain type in
/// [`types`](crate::evm::types) — keeps `ChainId` free of any `SQLite`
/// dependency while letting the caches bind it directly, with no `.get() as
/// i64` cast at every call site. Read-back isn't needed (no query selects the
/// chain id into a `ChainId`), so there's deliberately no `FromSql`.
impl ToSql for ChainId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.get() as i64))
    }
}

/// `FilterHash` persists as its 32 raw bytes in a `BLOB` column. Like
/// [`ChainId`]'s, this `ToSql` lives in the persistence layer so the domain
/// type stays `SQLite`-free, and the caches bind it directly instead of
/// reaching for `&fh.as_slice()[..]` at every call site. No `FromSql` — a
/// filter hash is always supplied by the caller, never read back from a row.
impl ToSql for FilterHash {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}

// ============================================================
// Typed column binds/reads
// ============================================================
//
// Every numeric domain newtype binds and reads as its underlying integer, so
// query call sites carry no `as i64` / `as u64` casts — the (checked)
// conversion lives here, once per type. Like the `ToSql` impls above, these
// live in the persistence layer so the domain types stay `SQLite`-free.
// `FromSql` delegates to rusqlite's range-checked `u64`/`u32` readers, so a
// negative or oversized column value errors instead of wrapping.

macro_rules! sql_newtype_u64 {
    ($($ty:ty),+ $(,)?) => {$(
        impl ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(self.get() as i64))
            }
        }
        impl FromSql for $ty {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                u64::column_result(value).map(Self::new)
            }
        }
    )+};
}

macro_rules! sql_newtype_u32 {
    ($($ty:ty),+ $(,)?) => {$(
        impl ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(i64::from(self.get())))
            }
        }
        impl FromSql for $ty {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                u32::column_result(value).map(Self::new)
            }
        }
    )+};
}

macro_rules! sql_newtype_b256 {
    ($($ty:ty),+ $(,)?) => {$(
        impl ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(self.as_slice()))
            }
        }
        impl FromSql for $ty {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                <[u8; 32]>::column_result(value).map(|arr| Self::new(B256::from(arr)))
            }
        }
    )+};
}

sql_newtype_u64!(BlockNumber, Timestamp, Gas, GasPrice);
sql_newtype_u32!(LogIdx, TxCount);
sql_newtype_b256!(BlockHash, TxHash);

/// Read a 32-byte BLOB column (block/tx hash, topic) as a [`B256`]. A helper
/// rather than a `FromSql` impl — both the trait and the type are foreign
/// (orphan rule). Errors on a wrong-sized blob via rusqlite's fixed-size
/// array reader.
pub(crate) fn col_b256(row: &rusqlite::Row<'_>, col: &str) -> rusqlite::Result<B256> {
    row.get::<_, [u8; 32]>(col).map(B256::from)
}

/// [`col_b256`] for a nullable column.
pub(crate) fn col_b256_opt(row: &rusqlite::Row<'_>, col: &str) -> rusqlite::Result<Option<B256>> {
    row.get::<_, Option<[u8; 32]>>(col)
        .map(|opt| opt.map(B256::from))
}

/// Read a 20-byte BLOB column as an [`Address`]. See [`col_b256`].
pub(crate) fn col_address(row: &rusqlite::Row<'_>, col: &str) -> rusqlite::Result<Address> {
    row.get::<_, [u8; 20]>(col).map(Address::from)
}

// ============================================================
// Versioning
// ============================================================

/// Bump this when cache schema or semantics change.
/// The version is appended to the DB filename so stale caches are bypassed.
pub const CACHE_VERSION: u32 = 3;

/// Inserts the cache version before the file extension.
/// `"./foo.db"` → `"./foo.v3.db"`, `"./foo"` → `"./foo.v3"`.
///
/// Operates on the file-name component only, so dots elsewhere in the path
/// (e.g. a `my.dir/` directory) are never mistaken for the extension.
pub fn versioned_path(base: &Path) -> PathBuf {
    let Some(stem) = base.file_stem() else {
        // No file name to version (empty path or `..`) — nothing sensible to
        // do; hand the path back for `Connection::open` to reject.
        return base.to_path_buf();
    };
    let mut name = stem.to_os_string();
    name.push(format!(".v{CACHE_VERSION}"));
    if let Some(ext) = base.extension() {
        name.push(".");
        name.push(ext);
    }
    base.with_file_name(name)
}

// ============================================================
// Tables & sections
// ============================================================

/// One purgeable section of the cache — the unit `chainfold cache purge`
/// selects. `History` bundles logs AND blocks because `scanned_ranges` is
/// shared between them (partitioned by filter hash, including the synthetic
/// blocks partition — see [`ScanPartition`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSection {
    /// Log/block scan history: cached logs and blocks, scanned-range
    /// bookkeeping, reorg tombstones, and block headers.
    History,
    /// ERC20 token metadata + historical balances.
    Erc20,
    /// Resolved event signatures.
    Signatures,
}

impl CacheSection {
    pub const ALL: [Self; 3] = [Self::History, Self::Erc20, Self::Signatures];

    /// Section name as surfaced by the CLI (matches the purge flag names).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Erc20 => "erc20",
            Self::Signatures => "signatures",
        }
    }
}

/// The closed set of tables in the cache file, grouped by section. A
/// migration adding a table (see `migrations()` below) must extend
/// [`Self::ALL`] and [`Self::section`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTable {
    CachedLogs,
    ReorgedBlocks,
    ScannedRanges,
    CachedBlockHeaders,
    CachedBlocks,
    Erc20Metadata,
    Erc20Balances,
    EventSignatures,
}

impl CacheTable {
    pub const ALL: [Self; 8] = [
        Self::CachedLogs,
        Self::ReorgedBlocks,
        Self::ScannedRanges,
        Self::CachedBlockHeaders,
        Self::CachedBlocks,
        Self::Erc20Metadata,
        Self::Erc20Balances,
        Self::EventSignatures,
    ];

    /// The SQL table name. Identifiers can't be bound as parameters, so
    /// queries interpolate this — always from this closed enum, never from a
    /// caller-supplied string.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::CachedLogs => "cached_logs",
            Self::ReorgedBlocks => "reorged_blocks",
            Self::ScannedRanges => "scanned_ranges",
            Self::CachedBlockHeaders => "cached_block_headers",
            Self::CachedBlocks => "cached_blocks",
            Self::Erc20Metadata => "erc20_metadata",
            Self::Erc20Balances => "erc20_balances",
            Self::EventSignatures => "event_signatures",
        }
    }

    /// Which purge section this table belongs to.
    #[must_use]
    pub fn section(self) -> CacheSection {
        match self {
            Self::CachedLogs
            | Self::ReorgedBlocks
            | Self::ScannedRanges
            | Self::CachedBlockHeaders
            | Self::CachedBlocks => CacheSection::History,
            Self::Erc20Metadata | Self::Erc20Balances => CacheSection::Erc20,
            Self::EventSignatures => CacheSection::Signatures,
        }
    }
}

/// Row count for one table — a [`CacheDb::purge`] report entry (rows
/// deleted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCount {
    pub table: CacheTable,
    pub rows: u64,
}

/// Size snapshot of one table — a [`CacheDb::table_stats`] entry. `bytes`
/// is the table's b-tree pages *including its indexes* (via the `dbstat`
/// vtab), so the per-table figures do not sum to the file size: freelist,
/// schema, and `sqlite_sequence` pages belong to no table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStat {
    pub table: CacheTable,
    pub rows: u64,
    pub bytes: ByteSize,
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
// Shared connection + opener
// ============================================================

/// Shared `SQLite` connection. Every per-concern cache holds a clone of this
/// `Arc` and serializes its statements through the single `Mutex`.
pub(crate) type SharedConn = Arc<Mutex<Connection>>;

/// Owns one opened-and-migrated `SQLite` cache file and hands out the
/// per-concern cache views ([`LogCache`], [`Erc20Cache`],
/// [`EventSignatureCache`]). All vended handles share this connection, so a
/// single open is enough for the whole process.
pub struct CacheDb {
    conn: SharedConn,
}

impl CacheDb {
    /// Open a cache at the given path. Creates the DB and runs migrations if
    /// needed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        let _span =
            tracing::span!(target: "chainfold::setup", tracing::Level::DEBUG, "db_open").entered();
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Log cache view (logs, scanned ranges, reorg tombstones, block headers).
    pub fn logs(&self) -> LogCache {
        LogCache::from_conn(self.conn.clone())
    }

    /// ERC20 metadata + balance view.
    pub fn erc20(&self) -> Erc20Cache {
        Erc20Cache::from_conn(self.conn.clone())
    }

    /// Event-signature resolver cache view.
    pub fn signatures(&self) -> EventSignatureCache {
        EventSignatureCache::from_conn(self.conn.clone())
    }

    /// Row count + byte size for every table, in [`CacheTable::ALL`] order.
    ///
    /// Bytes come from the `dbstat` virtual table (rusqlite's bundled
    /// `SQLite` compiles with `SQLITE_ENABLE_DBSTAT_VTAB`), attributing each
    /// index's pages to its table via `sqlite_schema.tbl_name`. Deliberately
    /// no *file* size: an in-memory cache has no file, and the on-disk
    /// footprint (main file + WAL/SHM sidecars) is a filesystem concern the
    /// caller measures itself.
    pub fn table_stats(&self) -> Result<Vec<TableStat>, CacheError> {
        let conn = self.conn.lock().unwrap();
        CacheTable::ALL
            .iter()
            .map(|&table| {
                let rows = conn
                    .prepare_cached(&format!("SELECT COUNT(*) FROM {}", table.name()))?
                    .query_row([], |row| row.get(0))?;
                let bytes: u64 = conn
                    .prepare_cached(
                        "SELECT COALESCE(SUM(st.pgsize), 0)
                         FROM dbstat st JOIN sqlite_schema m ON st.name = m.name
                         WHERE m.tbl_name = ?1",
                    )?
                    .query_row([table.name()], |row| row.get(0))?;
                Ok(TableStat {
                    table,
                    rows,
                    bytes: ByteSize::b(bytes),
                })
            })
            .collect()
    }

    /// Delete every row of every table belonging to one of `sections`, in
    /// one transaction. Returns the deleted count per in-scope table. Freed
    /// pages are not returned to the filesystem until [`Self::vacuum`].
    pub fn purge(&self, sections: &[CacheSection]) -> Result<Vec<TableCount>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        let mut report = Vec::new();
        for &table in &CacheTable::ALL {
            if !sections.contains(&table.section()) {
                continue;
            }
            let rows = tx.execute(&format!("DELETE FROM {}", table.name()), [])? as u64;
            report.push(TableCount { table, rows });
        }
        tx.commit()?;
        Ok(report)
    }

    /// Rebuild the DB file to reclaim space freed by deletes, then truncate
    /// the WAL — the rebuilt pages land there, so without the checkpoint the
    /// on-disk footprint wouldn't actually shrink. VACUUM cannot run inside
    /// a transaction; [`Self::purge`] commits before returning, and the
    /// mutex guarantees no statement is in flight on this connection.
    pub fn vacuum(&self) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }
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
        rusqlite_migration::M::up(
            "
        -- Matched ABI event item JSON (the {\"type\":\"event\",...} object)
        -- needed for typed log decoding. Nullable for rows written before
        -- the resolver started populating it.
        ALTER TABLE event_signatures ADD COLUMN abi_json TEXT;
        ",
        ),
        rusqlite_migration::M::up(
            "
        -- Per-log block timestamp, when the provider includes it on the
        -- eth_getLogs response (a non-standard but common extension). Lets the
        -- query path stamp EvmEvent.timestamp directly, skipping the
        -- eth_getBlockByNumber header fetch. Nullable: most providers omit it,
        -- and rows written before this column existed have NULL — in both
        -- cases the reader falls back to the cached block header.
        ALTER TABLE cached_logs ADD COLUMN block_timestamp INTEGER;
        ",
        ),
        rusqlite_migration::M::up(
            "
        -- Full cached blocks for the blocks history (BlockSpec). Distinct
        -- from cached_block_headers, whose three INSERT-OR-REPLACE writers
        -- (head clock, ensure_block_header, log fetcher) only know the four
        -- header fields and would wipe the rich columns: rows here are
        -- written only by the blocks fetch path. Height-keyed — a reorg
        -- refetch overwrites the stale fork row in place; until then the
        -- stale row is filtered at query time via reorged_blocks. Coverage
        -- lives in scanned_ranges under the blocks ScanPartition.
        CREATE TABLE cached_blocks (
            height       INTEGER PRIMARY KEY,
            hash         BLOB    NOT NULL,
            parent_hash  BLOB    NOT NULL,
            timestamp    INTEGER NOT NULL,
            tx_count     INTEGER NOT NULL,
            gas_used     INTEGER NOT NULL,
            gas_limit    INTEGER NOT NULL,
            base_fee     INTEGER,            -- NULL pre-EIP-1559
            miner        BLOB    NOT NULL
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evm::ChainId;
    use alloy::primitives::Address;

    #[test]
    fn versioned_path_inserts_version_before_extension() {
        assert_eq!(
            versioned_path(Path::new("./foo.db")),
            PathBuf::from("./foo.v3.db")
        );
        // No extension → version is appended.
        assert_eq!(versioned_path(Path::new("foo")), PathBuf::from("foo.v3"));
        // A dot in a directory name is not an extension separator.
        assert_eq!(
            versioned_path(Path::new("./my.dir/chainfold.db")),
            PathBuf::from("./my.dir/chainfold.v3.db")
        );
    }

    #[test]
    fn cache_db_vends_views_over_one_connection() {
        // Writing through one view and reading through another that came from
        // the same CacheDb proves they share a single underlying connection.
        let db = CacheDb::open_in_memory().unwrap();
        let erc20 = db.erc20();
        let addr = Address::repeat_byte(0x42);
        erc20
            .insert_erc20_metadata(&addr, ChainId::new(1), "X", "X", 18)
            .unwrap();
        assert!(
            db.erc20()
                .get_erc20_metadata(&addr, ChainId::new(1))
                .unwrap()
                .is_some()
        );
        // Distinct concern on the same file.
        let logs = db.logs();
        assert!(
            logs.get_scanned_ranges(&ScanPartition::Filter(FilterHash::new([0u8; 32])))
                .unwrap()
                .is_empty()
        );
    }

    // -- table_counts / purge / vacuum --

    use crate::evm::types::{BlockRange, EvmBlock, Topic};
    use alloy::primitives::U256;

    /// Seed one row into every table, through the public cache APIs only.
    fn seed_all_tables(db: &CacheDb) {
        let fh = FilterHash::new([0xAA; 32]);
        let bh = BlockHash::repeat_byte(0xBB);
        let parent = BlockHash::repeat_byte(0x11);
        let bn = BlockNumber::new(100);
        let ts = Timestamp::new(1_700_000_000);

        let logs = db.logs();
        logs.insert_logs(&fh, &[log_cache::make_test_log(bn, LogIdx::new(0), bh)])
            .unwrap();
        logs.record_scanned_range(&ScanPartition::Filter(fh), BlockRange::new(bn, bn), &bh)
            .unwrap();
        logs.mark_reorged(&[(BlockHash::repeat_byte(0xBC), bn)])
            .unwrap();
        logs.insert_block_header(bn, &bh, &parent, ts).unwrap();
        logs.insert_blocks(&[EvmBlock {
            number: bn,
            hash: bh,
            parent_hash: parent,
            timestamp: ts,
            tx_count: TxCount::new(1),
            gas_used: Gas::new(21_000),
            gas_limit: Gas::new(30_000_000),
            base_fee: None,
            miner: Address::repeat_byte(0x99),
        }])
        .unwrap();

        let addr = Address::repeat_byte(0x42);
        let erc20 = db.erc20();
        erc20
            .insert_erc20_metadata(&addr, ChainId::new(1), "X", "X", 18)
            .unwrap();
        erc20
            .insert_erc20_balance(
                ChainId::new(1),
                &addr,
                &Address::repeat_byte(0x43),
                100,
                U256::from(1),
            )
            .unwrap();

        db.signatures()
            .insert_event_signature(
                ChainId::new(1),
                &addr,
                "Transfer",
                "Transfer(address,address,uint256)",
                Topic::new(B256::repeat_byte(0xDD)),
                None,
            )
            .unwrap();
    }

    #[test]
    fn table_stats_zero_rows_on_fresh_db() {
        let db = CacheDb::open_in_memory().unwrap();
        let stats = db.table_stats().unwrap();
        assert_eq!(
            stats.iter().map(|s| s.table).collect::<Vec<_>>(),
            CacheTable::ALL.to_vec()
        );
        assert!(stats.iter().all(|s| s.rows == 0));
        // Even an empty table owns its root b-tree page — dbstat is wired up.
        assert!(stats.iter().all(|s| s.bytes > ByteSize::b(0)));
    }

    #[test]
    fn purge_section_clears_only_its_tables() {
        let db = CacheDb::open_in_memory().unwrap();
        seed_all_tables(&db);
        assert!(db.table_stats().unwrap().iter().all(|s| s.rows == 1));

        let report = db.purge(&[CacheSection::Erc20]).unwrap();
        // The report covers exactly the in-scope tables...
        assert_eq!(
            report,
            vec![
                TableCount {
                    table: CacheTable::Erc20Metadata,
                    rows: 1,
                },
                TableCount {
                    table: CacheTable::Erc20Balances,
                    rows: 1,
                },
            ]
        );
        // ...which are now empty, while every other section is untouched.
        for stat in db.table_stats().unwrap() {
            let expected = u64::from(stat.table.section() != CacheSection::Erc20);
            assert_eq!(stat.rows, expected, "{}", stat.table.name());
        }
    }

    #[test]
    fn purge_all_sections_clears_everything() {
        let db = CacheDb::open_in_memory().unwrap();
        seed_all_tables(&db);
        let report = db.purge(&CacheSection::ALL).unwrap();
        assert_eq!(report.len(), CacheTable::ALL.len());
        assert!(db.table_stats().unwrap().iter().all(|s| s.rows == 0));
    }

    #[test]
    fn vacuum_runs_on_shared_connection() {
        // Pins the invariant that nothing holds a transaction or in-flight
        // statement when vacuum() takes the connection — VACUUM errors
        // inside a transaction.
        let db = CacheDb::open_in_memory().unwrap();
        seed_all_tables(&db);
        db.purge(&CacheSection::ALL).unwrap();
        db.vacuum().unwrap();
    }
}
