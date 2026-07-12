//! Durable `SQLite` cache shared by every source family.
//!
//! One on-disk `SQLite` file backs several independent concerns. Rather than
//! a single god-object owning every table, the file is opened once by
//! [`CacheDb`], which then vends narrow per-concern handles that all share
//! the same connection:
//!
//! - [`Coverage`] — the cross-family scan-coverage bookkeeping: scanned
//!   ranges (partitioned per [`ScanPartition`]) and reorg tombstones.
//! - Per-family views, vended by `impl CacheDb` blocks in each family's
//!   module (`LogCache`/`Erc20Cache`/`EventSignatureCache` for EVM, the
//!   blocks cache for Bitcoin) — the shared layer never names a family.
//!
//! All handles serialize through one [`Mutex<Connection>`]; migrations and
//! the [`CACHE_VERSION`] are unified because there is a single file. The
//! schema is likewise unified: every family's tables are created in every
//! cache file (an empty table costs one b-tree page), so a single migration
//! list serves all features.
//!
//! One DB file scopes one chain: the history tables carry no chain/network
//! column, so pointing two chains at the same file mixes their histories.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytesize::ByteSize;
use rusqlite::Connection;

use crate::arch::Timestamp;
use crate::chain::{BlockNumber, TxCount};
use crate::share::Share;

mod coverage;

pub use coverage::{Coverage, CoverageHash, FilterHash, ScanPartition, compute_gaps};

// ============================================================
// Typed column binds/reads
// ============================================================
//
// Every numeric domain newtype binds and reads as its underlying integer, so
// query call sites carry no `as i64` / `as u64` casts — the (checked)
// conversion lives here, once per type. These live in the persistence layer
// so the domain types stay `SQLite`-free. `FromSql` delegates to rusqlite's
// range-checked `u64`/`u32` readers, so a negative or oversized column value
// errors instead of wrapping. The macros are `pub(crate)` so each family's
// cache module applies them to its own newtypes.

macro_rules! sql_newtype_u64 {
    ($($ty:ty),+ $(,)?) => {$(
        impl rusqlite::types::ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                Ok(rusqlite::types::ToSqlOutput::from(self.get() as i64))
            }
        }
        impl rusqlite::types::FromSql for $ty {
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                u64::column_result(value).map(Self::new)
            }
        }
    )+};
}

macro_rules! sql_newtype_u32 {
    ($($ty:ty),+ $(,)?) => {$(
        impl rusqlite::types::ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                Ok(rusqlite::types::ToSqlOutput::from(i64::from(self.get())))
            }
        }
        impl rusqlite::types::FromSql for $ty {
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                u32::column_result(value).map(Self::new)
            }
        }
    )+};
}

// Consumed by the per-family cache modules, which are feature-gated — a
// core-only build has no user, hence the allow.
#[allow(unused_imports)]
pub(crate) use {sql_newtype_u32, sql_newtype_u64};

sql_newtype_u64!(BlockNumber, Timestamp);
sql_newtype_u32!(TxCount);

/// `SQLite`-read span (target `chainfold::sqlite`): the name is the query's
/// row label in a stats report. A macro, not a fn — `tracing` span names are
/// static callsite metadata, so each needs its own literal. Trailing fields
/// (query bounds, partition) pass through so the span log records carry
/// them.
macro_rules! sqlite_span {
    ($name:literal $(, $($fields:tt)+)?) => {
        tracing::span!(target: "chainfold::sqlite", tracing::Level::DEBUG, $name $(, $($fields)+)?)
    };
}

pub(crate) use sqlite_span;

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
/// selects. `History` bundles logs AND blocks (both families') because
/// `scanned_ranges` and `reorged_blocks` are shared between them
/// (partitioned per [`ScanPartition`]) — purging one family's rows while
/// keeping shared coverage would be incoherent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSection {
    /// Log/block scan history: cached logs and blocks, scanned-range
    /// bookkeeping, reorg tombstones, and block headers.
    History,
    /// ERC20 token metadata + historical balances.
    Erc20,
    /// Resolved event signatures.
    Signatures,
    /// The curated token list (symbol → address entries + fetch metadata).
    Tokens,
}

impl CacheSection {
    pub const ALL: &[Self] = &[Self::History, Self::Erc20, Self::Signatures, Self::Tokens];

    /// Section name as surfaced by the CLI (matches the purge flag names).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Erc20 => "erc20",
            Self::Signatures => "signatures",
            Self::Tokens => "tokens",
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
    CachedBtcBlocks,
    Erc20Metadata,
    Erc20Balances,
    EventSignatures,
    TokenList,
    TokenListMeta,
}

impl CacheTable {
    pub const ALL: &[Self] = &[
        Self::CachedLogs,
        Self::ReorgedBlocks,
        Self::ScannedRanges,
        Self::CachedBlockHeaders,
        Self::CachedBlocks,
        Self::CachedBtcBlocks,
        Self::Erc20Metadata,
        Self::Erc20Balances,
        Self::EventSignatures,
        Self::TokenList,
        Self::TokenListMeta,
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
            Self::CachedBtcBlocks => "cached_btc_blocks",
            Self::Erc20Metadata => "erc20_metadata",
            Self::Erc20Balances => "erc20_balances",
            Self::EventSignatures => "event_signatures",
            Self::TokenList => "token_list",
            Self::TokenListMeta => "token_list_meta",
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
            | Self::CachedBlocks
            | Self::CachedBtcBlocks => CacheSection::History,
            Self::Erc20Metadata | Self::Erc20Balances => CacheSection::Erc20,
            Self::EventSignatures => CacheSection::Signatures,
            Self::TokenList | Self::TokenListMeta => CacheSection::Tokens,
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
/// per-concern cache views ([`Coverage`] here; each family's views via
/// `impl CacheDb` blocks in the family module). All vended handles share
/// this connection, so a single open is enough for the whole process.
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

    /// The shared connection, for the per-family `impl CacheDb` vend blocks
    /// (feature-gated — a core-only build has no caller, hence the allow).
    #[allow(dead_code)]
    pub(crate) fn shared_conn(&self) -> SharedConn {
        self.conn.share()
    }

    /// Cross-family scan-coverage view (scanned ranges + reorg tombstones).
    pub fn coverage(&self) -> Coverage {
        Coverage::from_conn(self.conn.share())
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
        for &table in CacheTable::ALL {
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
        rusqlite_migration::M::up(
            "
        -- Bitcoin block history (BtcBlockSpec) — the Bitcoin analog of
        -- cached_blocks. Height-keyed INSERT OR REPLACE (a reorg refetch
        -- overwrites the stale fork row in place; until then the stale row
        -- is filtered at query time via reorged_blocks). Coverage lives in
        -- scanned_ranges under the BtcBlocks ScanPartition; reorg
        -- tombstones share reorged_blocks (the hash spaces are disjoint).
        -- Like cached_blocks, no network column: one DB file scopes one
        -- chain.
        CREATE TABLE cached_btc_blocks (
            height    INTEGER PRIMARY KEY,
            hash      BLOB    NOT NULL,
            prev_hash BLOB    NOT NULL,
            time      INTEGER NOT NULL,
            tx_count  INTEGER NOT NULL,
            bits      INTEGER NOT NULL
        );
        ",
        ),
        rusqlite_migration::M::up(
            "
        -- Curated token-list entries (the Token Lists standard,
        -- tokens.uniswap.org) for symbol → address resolution. Replaced
        -- wholesale on refresh; token_list_meta records where the entries
        -- came from and when (the staleness check). Symbol lookups match
        -- case-insensitively, hence the NOCASE index.
        CREATE TABLE token_list (
            chain_id INTEGER NOT NULL,
            address  BLOB    NOT NULL,
            symbol   TEXT    NOT NULL,
            name     TEXT    NOT NULL,
            decimals INTEGER NOT NULL,
            PRIMARY KEY (chain_id, address)
        );
        CREATE INDEX idx_token_list_symbol
            ON token_list(chain_id, symbol COLLATE NOCASE);

        -- One row (id=1): provenance + freshness of the token_list snapshot.
        CREATE TABLE token_list_meta (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            url         TEXT    NOT NULL,
            name        TEXT    NOT NULL,
            version     TEXT    NOT NULL,
            fetched_at  INTEGER NOT NULL
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
}
