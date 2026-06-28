//! [`Erc20Cache`] — durable cache of ERC20 token metadata and historical
//! balances.
//!
//! A cheap handle over the shared connection vended by
//! [`CacheDb`](super::CacheDb). Independent of the log cache — populated by
//! the ERC20 metadata/balance helpers, not the log-history path.

use alloy::primitives::{Address, U256};

use super::{CacheError, SharedConn};
use crate::evm::types::ChainId;

/// Cached ERC20 token metadata (name, symbol, decimals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedErc20Metadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

/// Durable SQLite-backed ERC20 metadata + balance cache.
pub struct Erc20Cache {
    conn: SharedConn,
}

impl Erc20Cache {
    pub(super) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
    }

    /// Open a standalone ERC20 cache at the given path. Creates the DB and
    /// runs migrations if needed. Equivalent to `CacheDb::open(path)?.erc20()`.
    pub fn open(path: &str) -> Result<Self, CacheError> {
        Ok(super::CacheDb::open(path)?.erc20())
    }

    /// Open a standalone in-memory ERC20 cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(super::CacheDb::open_in_memory()?.erc20())
    }

    // --------------------------------------------------------
    // Metadata
    // --------------------------------------------------------

    pub fn get_erc20_metadata(
        &self,
        address: &Address,
        chain_id: ChainId,
    ) -> Result<Option<CachedErc20Metadata>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT name, symbol, decimals FROM erc20_metadata
             WHERE address = ?1 AND chain_id = ?2",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![address.as_slice(), chain_id], |row| {
            Ok(CachedErc20Metadata {
                name: row.get(0)?,
                symbol: row.get(1)?,
                decimals: row.get::<_, i64>(2)? as u8,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn insert_erc20_metadata(
        &self,
        address: &Address,
        chain_id: ChainId,
        name: &str,
        symbol: &str,
        decimals: u8,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO erc20_metadata (address, chain_id, name, symbol, decimals)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![address.as_slice(), chain_id, name, symbol, decimals as i64],
        )?;
        Ok(())
    }

    // --------------------------------------------------------
    // Balances
    // --------------------------------------------------------

    pub fn get_erc20_balance(
        &self,
        chain_id: ChainId,
        address: &Address,
        owner: &Address,
        block_number: u64,
    ) -> Result<Option<U256>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT balance FROM erc20_balances
             WHERE chain_id = ?1 AND address = ?2 AND owner = ?3 AND block_number = ?4",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![
                chain_id,
                address.as_slice(),
                owner.as_slice(),
                block_number as i64
            ],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(bytes)
            },
        )?;
        match rows.next() {
            Some(Ok(bytes)) if bytes.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Some(U256::from_be_bytes(arr)))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn get_erc20_balance_floor(
        &self,
        chain_id: ChainId,
        address: &Address,
        owner: &Address,
        max_block: u64,
    ) -> Result<Option<(u64, U256)>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, balance FROM erc20_balances
             WHERE chain_id = ?1 AND address = ?2 AND owner = ?3 AND block_number <= ?4
             ORDER BY block_number DESC LIMIT 1",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![
                chain_id,
                address.as_slice(),
                owner.as_slice(),
                max_block as i64
            ],
            |row| {
                let block: i64 = row.get(0)?;
                let balance: Vec<u8> = row.get(1)?;
                Ok((block as u64, balance))
            },
        )?;
        match rows.next() {
            Some(Ok((block, bytes))) if bytes.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(Some((block, U256::from_be_bytes(arr))))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn insert_erc20_balance(
        &self,
        chain_id: ChainId,
        address: &Address,
        owner: &Address,
        block_number: u64,
        balance: U256,
    ) -> Result<(), CacheError> {
        let bytes: [u8; 32] = balance.to_be_bytes();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO erc20_balances (chain_id, address, owner, block_number, balance)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                chain_id,
                address.as_slice(),
                owner.as_slice(),
                block_number as i64,
                &bytes[..]
            ],
        )?;
        Ok(())
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- metadata --

    #[test]
    fn erc20_metadata_round_trip() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        store
            .insert_erc20_metadata(&addr, ChainId::new(1), "Wrapped Ether", "WETH", 18)
            .unwrap();
        let m = store
            .get_erc20_metadata(&addr, ChainId::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(m.name, "Wrapped Ether");
        assert_eq!(m.symbol, "WETH");
        assert_eq!(m.decimals, 18);
    }

    #[test]
    fn erc20_metadata_different_chains() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        store
            .insert_erc20_metadata(&addr, ChainId::new(1), "A", "A", 18)
            .unwrap();
        store
            .insert_erc20_metadata(&addr, ChainId::new(10), "B", "B", 6)
            .unwrap();
        assert_eq!(
            store
                .get_erc20_metadata(&addr, ChainId::new(1))
                .unwrap()
                .unwrap()
                .name,
            "A"
        );
        assert_eq!(
            store
                .get_erc20_metadata(&addr, ChainId::new(10))
                .unwrap()
                .unwrap()
                .name,
            "B"
        );
    }

    #[test]
    fn erc20_metadata_upsert() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        store
            .insert_erc20_metadata(&addr, ChainId::new(1), "Old", "OLD", 18)
            .unwrap();
        store
            .insert_erc20_metadata(&addr, ChainId::new(1), "New", "NEW", 6)
            .unwrap();
        let m = store
            .get_erc20_metadata(&addr, ChainId::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(m.name, "New");
        assert_eq!(m.symbol, "NEW");
        assert_eq!(m.decimals, 6);
    }

    // -- balances --

    #[test]
    fn erc20_balance_round_trip() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x55);
        store
            .insert_erc20_balance(
                ChainId::new(1),
                &addr,
                &owner,
                100,
                U256::from(1_000_000_u64),
            )
            .unwrap();
        let b = store
            .get_erc20_balance(ChainId::new(1), &addr, &owner, 100)
            .unwrap()
            .unwrap();
        assert_eq!(b, U256::from(1_000_000_u64));
    }

    #[test]
    fn erc20_balance_different_blocks() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x55);
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        assert_eq!(
            store
                .get_erc20_balance(ChainId::new(1), &addr, &owner, 100)
                .unwrap()
                .unwrap(),
            U256::from(1u64)
        );
        assert_eq!(
            store
                .get_erc20_balance(ChainId::new(1), &addr, &owner, 200)
                .unwrap()
                .unwrap(),
            U256::from(2u64)
        );
    }

    #[test]
    fn erc20_balance_upsert() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x55);
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 100, U256::from(2u64))
            .unwrap();
        assert_eq!(
            store
                .get_erc20_balance(ChainId::new(1), &addr, &owner, 100)
                .unwrap()
                .unwrap(),
            U256::from(2u64)
        );
    }

    #[test]
    fn erc20_balance_floor_lookup() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x55);
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 100, U256::from(1u64))
            .unwrap();
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        let (block, balance) = store
            .get_erc20_balance_floor(ChainId::new(1), &addr, &owner, 150)
            .unwrap()
            .unwrap();
        assert_eq!(block, 100);
        assert_eq!(balance, U256::from(1u64));
    }

    #[test]
    fn erc20_balance_floor_none() {
        let store = Erc20Cache::open_in_memory().unwrap();
        let addr = Address::repeat_byte(0x42);
        let owner = Address::repeat_byte(0x55);
        store
            .insert_erc20_balance(ChainId::new(1), &addr, &owner, 200, U256::from(2u64))
            .unwrap();
        assert!(
            store
                .get_erc20_balance_floor(ChainId::new(1), &addr, &owner, 100)
                .unwrap()
                .is_none()
        );
    }
}
