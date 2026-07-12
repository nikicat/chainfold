//! [`TokenListCache`] — durable snapshot of a curated token list
//! (symbol → address entries), for resolving user-typed symbols like `USDC`.
//!
//! A cheap handle over the shared connection vended by
//! [`CacheDb`](super::CacheDb). The snapshot is replaced wholesale by the
//! resolver in [`tokenlist`](crate::evm::tokenlist); [`TokenListMeta`]
//! records where it came from and when (the staleness check).

use std::path::Path;

use alloy::primitives::Address;
use reqwest::Url;
use rusqlite::types::Type;

use crate::arch::Timestamp;
use crate::cache::{CacheDb, CacheError, SharedConn};
use crate::evm::cache::col_address;
use crate::evm::types::{ChainId, TokenSymbol};

/// One token-list entry: a symbol's contract address on one chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenListEntry {
    pub chain_id: ChainId,
    pub address: Address,
    pub symbol: TokenSymbol,
    pub name: String,
    pub decimals: u8,
}

/// A token list's own version — the Token Lists standard's
/// `version: {major, minor, patch}` object, deserialized as-is from the
/// wire. Renders (and persists) as `major.minor.patch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct TokenListVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl std::fmt::Display for TokenListVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl std::str::FromStr for TokenListVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(3, '.').map(str::parse::<u64>);
        let mut next = || {
            parts
                .next()
                .ok_or_else(|| format!("not a major.minor.patch version: {s:?}"))?
                .map_err(|e| format!("not a major.minor.patch version: {s:?} ({e})"))
        };
        Ok(Self {
            major: next()?,
            minor: next()?,
            patch: next()?,
        })
    }
}

/// Provenance + freshness of the cached snapshot (single row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenListMeta {
    /// Where the list was fetched from.
    pub url: Url,
    /// The list's display name (e.g. `Uniswap Labs Default`).
    pub name: String,
    /// The list's own version.
    pub version: TokenListVersion,
    /// When the snapshot was fetched (seconds since epoch).
    pub fetched_at: Timestamp,
}

/// Durable SQLite-backed token-list snapshot.
pub struct TokenListCache {
    conn: SharedConn,
}

impl TokenListCache {
    pub(super) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
    }

    /// Open a standalone token-list cache at the given path. Creates the DB
    /// and runs migrations if needed. Equivalent to
    /// `CacheDb::open(path)?.token_list()`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        Ok(CacheDb::open(path)?.token_list())
    }

    /// Open a standalone in-memory token-list cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(CacheDb::open_in_memory()?.token_list())
    }

    /// Replace the whole snapshot (entries + meta) in one transaction — a
    /// refresh either lands completely or not at all.
    pub fn replace(
        &self,
        entries: &[TokenListEntry],
        meta: &TokenListMeta,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM token_list", [])?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO token_list (chain_id, address, symbol, name, decimals)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for e in entries {
                stmt.execute(rusqlite::params![
                    e.chain_id,
                    e.address.as_slice(),
                    e.symbol,
                    e.name,
                    e.decimals
                ])?;
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO token_list_meta (id, url, name, version, fetched_at)
             VALUES (1, ?1, ?2, ?3, ?4)",
            rusqlite::params![
                meta.url.as_str(),
                meta.name,
                meta.version.to_string(),
                meta.fetched_at
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// The snapshot's provenance row, `None` before the first refresh.
    pub fn meta(&self) -> Result<Option<TokenListMeta>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT url, name, version, fetched_at FROM token_list_meta WHERE id = 1",
        )?;
        let mut rows = stmt.query_map([], read_meta)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Entries whose symbol matches `symbol` case-insensitively on
    /// `chain_id`, ordered by address for determinism. Usually 0 or 1 hits;
    /// >1 means the symbol is ambiguous within the list.
    pub fn find(
        &self,
        chain_id: ChainId,
        symbol: &TokenSymbol,
    ) -> Result<Vec<TokenListEntry>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT chain_id, address, symbol, name, decimals FROM token_list
             WHERE chain_id = ?1 AND symbol = ?2 COLLATE NOCASE
             ORDER BY address",
        )?;
        let rows = stmt.query_map(rusqlite::params![chain_id, symbol], read_entry)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Every cached entry, optionally narrowed to one chain — backs
    /// `tokens list`. Ordered by (chain, symbol, address).
    pub fn list(&self, chain_id: Option<ChainId>) -> Result<Vec<TokenListEntry>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT chain_id, address, symbol, name, decimals FROM token_list
             WHERE ?1 IS NULL OR chain_id = ?1
             ORDER BY chain_id, symbol COLLATE NOCASE, address",
        )?;
        let rows = stmt.query_map(rusqlite::params![chain_id], read_entry)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

fn read_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<TokenListEntry> {
    Ok(TokenListEntry {
        chain_id: row.get("chain_id")?,
        address: col_address(row, "address")?,
        symbol: row.get("symbol")?,
        name: row.get("name")?,
        decimals: row.get("decimals")?,
    })
}

/// Parse a TEXT column back into its domain type. A value that fails to
/// parse is a corrupt row — error loudly (the message names the column and
/// how to recover).
fn parse_col<T>(column: &str, raw: &str) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    raw.parse().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Text,
            format!("token_list_meta.{column} {raw:?}: {e} (purge with `cache purge --tokens`)")
                .into(),
        )
    })
}

fn read_meta(row: &rusqlite::Row<'_>) -> rusqlite::Result<TokenListMeta> {
    Ok(TokenListMeta {
        url: parse_col("url", &row.get::<_, String>("url")?)?,
        name: row.get("name")?,
        version: parse_col("version", &row.get::<_, String>("version")?)?,
        fetched_at: row.get("fetched_at")?,
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(chain: u64, byte: u8, symbol: &str) -> TokenListEntry {
        TokenListEntry {
            chain_id: ChainId::new(chain),
            address: Address::repeat_byte(byte),
            symbol: TokenSymbol::new(symbol),
            name: format!("{symbol} Coin"),
            decimals: 6,
        }
    }

    fn meta(url: &str) -> TokenListMeta {
        TokenListMeta {
            url: url.parse().expect("test URL parses"),
            name: "Test List".to_owned(),
            version: TokenListVersion {
                major: 1,
                minor: 2,
                patch: 3,
            },
            fetched_at: Timestamp::new(1_700_000_000),
        }
    }

    fn sym(s: &str) -> TokenSymbol {
        TokenSymbol::new(s)
    }

    #[test]
    fn empty_cache_has_no_meta_and_no_entries() {
        let store = TokenListCache::open_in_memory().unwrap();
        assert!(store.meta().unwrap().is_none());
        assert!(store.list(None).unwrap().is_empty());
        assert!(
            store
                .find(ChainId::new(1), &sym("USDC"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn replace_round_trips_entries_and_meta() {
        let store = TokenListCache::open_in_memory().unwrap();
        let entries = vec![entry(1, 0x11, "USDC"), entry(10, 0x22, "USDC")];
        store
            .replace(&entries, &meta("https://example.test/list"))
            .unwrap();

        assert_eq!(
            store.meta().unwrap().unwrap(),
            meta("https://example.test/list")
        );
        assert_eq!(store.list(None).unwrap(), entries);
        assert_eq!(
            store.find(ChainId::new(1), &sym("USDC")).unwrap(),
            vec![entry(1, 0x11, "USDC")]
        );
    }

    #[test]
    fn replace_swaps_the_whole_snapshot() {
        let store = TokenListCache::open_in_memory().unwrap();
        store
            .replace(&[entry(1, 0x11, "OLD")], &meta("https://old.test"))
            .unwrap();
        store
            .replace(&[entry(1, 0x22, "NEW")], &meta("https://new.test"))
            .unwrap();

        assert_eq!(store.list(None).unwrap(), vec![entry(1, 0x22, "NEW")]);
        assert!(store.find(ChainId::new(1), &sym("OLD")).unwrap().is_empty());
        assert_eq!(
            store.meta().unwrap().unwrap().url.as_str(),
            "https://new.test/"
        );
    }

    #[test]
    fn find_matches_case_insensitively() {
        let store = TokenListCache::open_in_memory().unwrap();
        store
            .replace(&[entry(1, 0x11, "USDC")], &meta("https://example.test"))
            .unwrap();
        for query in ["USDC", "usdc", "Usdc"] {
            assert_eq!(
                store.find(ChainId::new(1), &sym(query)).unwrap().len(),
                1,
                "query {query:?}"
            );
        }
    }

    #[test]
    fn find_is_chain_scoped() {
        let store = TokenListCache::open_in_memory().unwrap();
        store
            .replace(
                &[entry(1, 0x11, "USDC"), entry(10, 0x22, "USDC")],
                &meta("https://example.test"),
            )
            .unwrap();
        assert_eq!(
            store.find(ChainId::new(10), &sym("usdc")).unwrap(),
            vec![entry(10, 0x22, "USDC")]
        );
        assert!(
            store
                .find(ChainId::new(137), &sym("usdc"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn find_returns_all_same_chain_collisions() {
        // Two entries share a symbol (differing case) on one chain — both
        // must surface so the resolver can report the ambiguity.
        let store = TokenListCache::open_in_memory().unwrap();
        store
            .replace(
                &[entry(1, 0x11, "LIT"), entry(1, 0x22, "lit")],
                &meta("https://example.test"),
            )
            .unwrap();
        assert_eq!(store.find(ChainId::new(1), &sym("lit")).unwrap().len(), 2);
    }

    #[test]
    fn list_filters_by_chain() {
        let store = TokenListCache::open_in_memory().unwrap();
        store
            .replace(
                &[entry(1, 0x11, "USDC"), entry(10, 0x22, "WBTC")],
                &meta("https://example.test"),
            )
            .unwrap();
        assert_eq!(
            store.list(Some(ChainId::new(10))).unwrap(),
            vec![entry(10, 0x22, "WBTC")]
        );
        assert_eq!(store.list(None).unwrap().len(), 2);
    }
}
