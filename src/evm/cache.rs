//! EVM views over the shared `SQLite` cache.
//!
//! The cache file itself — open/migrate, purge/stats, and the cross-family
//! [`Coverage`](crate::cache::Coverage) bookkeeping — lives in
//! [`crate::cache`]. This module holds what is EVM about it:
//!
//! - [`LogCache`] — the EVM log cache proper: raw logs, cached blocks and
//!   block headers (plus a [`coverage()`](LogCache::coverage) accessor for
//!   the shared scanned-range/reorg bookkeeping).
//! - [`Erc20Cache`] — ERC20 token metadata + historical balances.
//! - [`EventSignatureCache`] — resolved `(event name) → signature/topic0/abi`
//!   rows for the signature resolver.
//! - [`TokenListCache`] — the curated token-list snapshot for
//!   symbol → address resolution.
//! - The `SQLite` binds for the EVM domain newtypes, and the `impl CacheDb`
//!   block vending the four views.
//!
//! Navigation queries (first/last log) live in the in-memory
//! [`LogIndex`](super::LogIndex), not here.

use alloy::primitives::{Address, B256};

use crate::cache::{CacheDb, CoverageHash, sql_newtype_u32, sql_newtype_u64};
use crate::evm::types::{BlockHash, ChainId, Gas, GasPrice, LogIdx, TokenSymbol, TxHash};
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

mod erc20_cache;
mod event_signature_cache;
mod log_cache;
mod token_list_cache;

pub use erc20_cache::{CachedErc20Metadata, Erc20Cache};
pub use event_signature_cache::{EventSignatureCache, EventSignatureRow};
pub use log_cache::{BlockHeader, LogCache, compute_filter_hash};
pub use token_list_cache::{TokenListCache, TokenListEntry, TokenListMeta, TokenListVersion};

// The EVM numeric newtypes bind/read via the shared cache macros (see
// `crate::cache` — the checked conversion lives there, once per width).
// Defining the binds here — in the persistence layer, not on the domain
// types in [`types`](crate::evm::types) — keeps those types free of any
// `SQLite` dependency while letting the caches bind them directly, with no
// `.get() as i64` cast at every call site. `ChainId` reads back too: the
// token-list rows are chain-keyed and `tokens list` selects across chains.
sql_newtype_u64!(Gas, GasPrice, ChainId);
sql_newtype_u32!(LogIdx);

/// `TokenSymbol` persists as its raw string in a `TEXT` column (compared
/// under `COLLATE NOCASE` by the symbol queries). See the numeric-newtype
/// note above for why the binds live here.
impl ToSql for TokenSymbol {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_str()))
    }
}

impl FromSql for TokenSymbol {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        String::column_result(value).map(Self::new)
    }
}

macro_rules! sql_newtype_b256 {
    ($($ty:ty),+ $(,)?) => {$(
        impl ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(self.as_slice()))
            }
        }
        impl rusqlite::types::FromSql for $ty {
            fn column_result(
                value: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                <[u8; 32]>::column_result(value).map(|arr| Self::new(B256::from(arr)))
            }
        }
    )+};
}

sql_newtype_b256!(BlockHash, TxHash);

/// An EVM block hash enters the shared coverage tables as an opaque
/// [`CoverageHash`] — the conversion point where the EVM hash type stops and
/// the family-agnostic persistence identity begins.
impl From<BlockHash> for CoverageHash {
    fn from(hash: BlockHash) -> Self {
        Self::new(hash.b256().0)
    }
}

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

/// The EVM views over the shared cache file. Living here — not in
/// [`crate::cache`] — so the shared layer never names a family.
impl CacheDb {
    /// Log cache view (logs, cached blocks, block headers + shared coverage).
    pub fn logs(&self) -> LogCache {
        LogCache::from_conn(self.shared_conn())
    }

    /// ERC20 metadata + balance view.
    pub fn erc20(&self) -> Erc20Cache {
        Erc20Cache::from_conn(self.shared_conn())
    }

    /// Event-signature resolver cache view.
    pub fn signatures(&self) -> EventSignatureCache {
        EventSignatureCache::from_conn(self.shared_conn())
    }

    /// Token-list snapshot view (symbol → address resolution).
    pub fn token_list(&self) -> TokenListCache {
        TokenListCache::from_conn(self.shared_conn())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Timestamp;
    use crate::cache::{CacheSection, CacheTable, FilterHash, ScanPartition, TableCount};
    use crate::chain::{BlockNumber, BlockRange, TxCount};
    use crate::evm::types::{EvmBlock, Topic};
    use alloy::primitives::U256;

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
        assert!(
            db.coverage()
                .get_scanned_ranges(&ScanPartition::Filter(FilterHash::new([0u8; 32])))
                .unwrap()
                .is_empty()
        );
    }

    // -- table_counts / purge / vacuum --

    /// Rows [`seed_all_tables`] leaves in `table` — 1 everywhere, except
    /// the Bitcoin table when that family isn't compiled in.
    fn seeded_rows(table: CacheTable) -> u64 {
        #[cfg(not(feature = "bitcoin"))]
        if table == CacheTable::CachedBtcBlocks {
            return 0;
        }
        let _ = table;
        1
    }

    /// Seed one row into every table (every compiled family's), through the
    /// public cache APIs only.
    fn seed_all_tables(db: &CacheDb) {
        let fh = FilterHash::new([0xAA; 32]);
        let bh = BlockHash::repeat_byte(0xBB);
        let parent = BlockHash::repeat_byte(0x11);
        let bn = BlockNumber::new(100);
        let ts = Timestamp::new(1_700_000_000);

        let logs = db.logs();
        logs.insert_logs(&fh, &[log_cache::make_test_log(bn, LogIdx::new(0), bh)])
            .unwrap();
        let coverage = db.coverage();
        coverage
            .record_scanned_range(
                &ScanPartition::Filter(fh),
                BlockRange::new(bn, bn),
                bh.into(),
            )
            .unwrap();
        coverage
            .mark_reorged(&[(BlockHash::repeat_byte(0xBC).into(), bn)])
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

        db.token_list()
            .replace(
                &[TokenListEntry {
                    chain_id: ChainId::new(1),
                    address: addr,
                    symbol: TokenSymbol::new("X"),
                    name: "X Coin".to_owned(),
                    decimals: 18,
                }],
                &TokenListMeta {
                    url: "https://example.test/list".parse().expect("static URL"),
                    name: "Test List".to_owned(),
                    version: TokenListVersion {
                        major: 1,
                        minor: 0,
                        patch: 0,
                    },
                    fetched_at: ts,
                },
            )
            .unwrap();

        #[cfg(feature = "bitcoin")]
        {
            use crate::bitcoin::{BtcBlock, BtcBlockHash, CompactTarget};
            db.btc_blocks()
                .insert_blocks(&[BtcBlock {
                    height: bn,
                    hash: BtcBlockHash::repeat_byte(0xEB),
                    prev_hash: BtcBlockHash::ZERO,
                    time: ts,
                    tx_count: TxCount::new(1),
                    bits: CompactTarget::new(0x1d00_ffff),
                }])
                .unwrap();
        }
    }

    #[test]
    fn purge_section_clears_only_its_tables() {
        let db = CacheDb::open_in_memory().unwrap();
        seed_all_tables(&db);
        assert!(
            db.table_stats()
                .unwrap()
                .iter()
                .all(|s| s.rows == seeded_rows(s.table))
        );

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
            let expected = if stat.table.section() == CacheSection::Erc20 {
                0
            } else {
                seeded_rows(stat.table)
            };
            assert_eq!(stat.rows, expected, "{}", stat.table.name());
        }
    }

    #[test]
    fn purge_all_sections_clears_everything() {
        let db = CacheDb::open_in_memory().unwrap();
        seed_all_tables(&db);
        let report = db.purge(CacheSection::ALL).unwrap();
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
        db.purge(CacheSection::ALL).unwrap();
        db.vacuum().unwrap();
    }
}
