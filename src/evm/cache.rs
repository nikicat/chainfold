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

use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
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
/// `"./foo.db"` → `"./foo.v3.db"`, `"./foo"` → `"./foo.v3"`
pub fn versioned_path(base: &str) -> String {
    match base.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}.v{CACHE_VERSION}.{ext}"),
        None => format!("{base}.v{CACHE_VERSION}"),
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
    pub fn open(path: &str) -> Result<Self, CacheError> {
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
        assert_eq!(versioned_path("./foo.db"), "./foo.v3.db");
        // No extension (and no other '.') → version is appended.
        assert_eq!(versioned_path("foo"), "foo.v3");
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
}
