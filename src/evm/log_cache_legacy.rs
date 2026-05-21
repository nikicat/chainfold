//! Backwards-compatibility facade for the `LogCache` API.
//!
//! `LogCache` is a thin wrapper around [`LogStore`](super::log_store::LogStore)
//! that keeps the four deprecated navigation methods (`first_log_after`,
//! `last_log_before`, `first_log_at_or_after`, `last_log_at_or_before`)
//! alive until Phase 1c rewires `EvmLogHistory` onto the new
//! `LogIndex`. Schema and primary surface live in `log_store`; this
//! module just forwards.
//!
//! New code should depend on `LogStore` + `LogIndex`. Do not add anything
//! here.

#![allow(deprecated)]

use alloy::primitives::U256;
use alloy::rpc::types::Log;

use super::log_store::{row_to_log, LogStore};

// Re-export helpers and types that downstream code may still import
// via this module. Most direct imports moved over to `log_store` in
// Phase 1c; the re-exports here keep the legacy path valid until the
// module is fully removed.
pub use super::log_store::{BlockHeader, CacheError, CachedErc20Metadata};

/// Legacy cache surface. Wraps [`LogStore`] and adds navigation queries
/// that have been moved to `LogIndex` in Phase 1.
#[deprecated(
    note = "use `LogStore` + `LogIndex` directly; LogCache is a transitional facade and will be removed after Phase 3"
)]
pub struct LogCache {
    inner: std::sync::Arc<LogStore>,
}

impl LogCache {
    pub fn open(path: &str) -> Result<Self, CacheError> {
        Ok(Self {
            inner: std::sync::Arc::new(LogStore::open(path)?),
        })
    }

    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(Self {
            inner: std::sync::Arc::new(LogStore::open_in_memory()?),
        })
    }

    /// Borrow the underlying [`LogStore`]. Used by `EvmLogHistory` to wire
    /// a `LogIndex` over the same backing storage.
    pub fn store(&self) -> &std::sync::Arc<LogStore> {
        &self.inner
    }

    // --------------------------------------------------------
    // Forward the slim API verbatim
    // --------------------------------------------------------

    pub fn insert_logs(&self, filter_hash: &[u8; 32], logs: &[Log]) -> Result<(), CacheError> {
        self.inner.insert_logs(filter_hash, logs)
    }

    /// Legacy alias for [`LogStore::get_logs_in_range`].
    pub fn get_cached_logs(
        &self,
        filter_hash: &[u8; 32],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, CacheError> {
        self.inner.get_logs_in_range(filter_hash, from_block, to_block)
    }

    pub fn record_scanned_range(
        &self,
        filter_hash: &[u8; 32],
        start: u64,
        end: u64,
        end_block_hash: &[u8; 32],
    ) -> Result<(), CacheError> {
        self.inner
            .record_scanned_range(filter_hash, start, end, end_block_hash)
    }

    pub fn get_scanned_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<Vec<(u64, u64)>, CacheError> {
        self.inner.get_scanned_ranges(filter_hash)
    }

    pub fn compact_ranges(
        &self,
        filter_hash: &[u8; 32],
        up_to_block: u64,
    ) -> Result<(), CacheError> {
        self.inner.compact_ranges(filter_hash, up_to_block)
    }

    pub fn mark_reorged(&self, blocks: &[([u8; 32], u64)]) -> Result<(), CacheError> {
        self.inner.mark_reorged(blocks)
    }

    pub fn invalidate_reorged_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<usize, CacheError> {
        self.inner.invalidate_reorged_ranges(filter_hash)
    }

    pub fn cleanup_reorged_before(&self, block_number: u64) -> Result<usize, CacheError> {
        self.inner.cleanup_reorged_before(block_number)
    }

    pub fn insert_block_header(
        &self,
        height: u64,
        hash: &[u8; 32],
        parent_hash: &[u8; 32],
        timestamp: u64,
    ) -> Result<(), CacheError> {
        self.inner
            .insert_block_header(height, hash, parent_hash, timestamp)
    }

    pub fn get_block_header(&self, height: u64) -> Result<Option<BlockHeader>, CacheError> {
        self.inner.get_block_header(height)
    }

    pub fn get_erc20_metadata(
        &self,
        address: &[u8; 20],
        chain_id: u64,
    ) -> Result<Option<CachedErc20Metadata>, CacheError> {
        self.inner.get_erc20_metadata(address, chain_id)
    }

    pub fn insert_erc20_metadata(
        &self,
        address: &[u8; 20],
        chain_id: u64,
        name: &str,
        symbol: &str,
        decimals: u8,
    ) -> Result<(), CacheError> {
        self.inner
            .insert_erc20_metadata(address, chain_id, name, symbol, decimals)
    }

    pub fn get_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
    ) -> Result<Option<U256>, CacheError> {
        self.inner
            .get_erc20_balance(chain_id, address, owner, block_number)
    }

    pub fn get_erc20_balance_floor(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        max_block: u64,
    ) -> Result<Option<(u64, U256)>, CacheError> {
        self.inner
            .get_erc20_balance_floor(chain_id, address, owner, max_block)
    }

    pub fn insert_erc20_balance(
        &self,
        chain_id: u64,
        address: &[u8; 20],
        owner: &[u8; 20],
        block_number: u64,
        balance: U256,
    ) -> Result<(), CacheError> {
        self.inner
            .insert_erc20_balance(chain_id, address, owner, block_number, balance)
    }

    // --------------------------------------------------------
    // Deprecated navigation methods (move to LogIndex in Phase 1b)
    // --------------------------------------------------------

    /// First cached log after (block_number, log_index), excluding reorged.
    #[deprecated(note = "moved to LogIndex in Phase 1; legacy only for EvmLogHistory until Phase 1c")]
    pub fn first_log_after(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.inner.connection().lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number > ?2 OR (block_number = ?2 AND log_index > ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// Last cached log before (block_number, log_index), excluding reorged.
    #[deprecated(note = "moved to LogIndex in Phase 1; legacy only for EvmLogHistory until Phase 1c")]
    pub fn last_log_before(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.inner.connection().lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number < ?2 OR (block_number = ?2 AND log_index < ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number DESC, log_index DESC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// First cached log at or after (block_number, log_index), excluding reorged.
    #[deprecated(note = "moved to LogIndex in Phase 1; legacy only for EvmLogHistory until Phase 1c")]
    pub fn first_log_at_or_after(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.inner.connection().lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number > ?2 OR (block_number = ?2 AND log_index >= ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number ASC, log_index ASC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }

    /// Last cached log at or before (block_number, log_index), excluding reorged.
    #[deprecated(note = "moved to LogIndex in Phase 1; legacy only for EvmLogHistory until Phase 1c")]
    pub fn last_log_at_or_before(
        &self,
        filter_hash: &[u8; 32],
        block_number: u64,
        log_index: u32,
    ) -> Result<Option<Log>, CacheError> {
        let conn = self.inner.connection().lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index
             FROM cached_logs
             WHERE filter_hash = ?1
               AND (block_number < ?2 OR (block_number = ?2 AND log_index <= ?3))
               AND block_hash NOT IN (SELECT block_hash FROM reorged_blocks)
             ORDER BY block_number DESC, log_index DESC
             LIMIT 1",
        )?;
        let logs: Vec<Log> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], block_number as i64, log_index as i64],
                |row| row_to_log(row),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs.into_iter().next())
    }
}

// ============================================================
// Tests for the deprecated navigation surface
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, Bytes, LogData, B256};

    const FH: [u8; 32] = [0xAA; 32];
    const BLOCK_HASH_A: [u8; 32] = [0xBB; 32];
    const BLOCK_HASH_B: [u8; 32] = [0xBC; 32];

    fn make_test_log(block_num: u64, log_idx: u64, block_hash: [u8; 32]) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01, 0x02]);
        let log_data = LogData::new_unchecked(vec![topic0], data);
        Log {
            inner: alloy::primitives::Log {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(B256::from(block_hash)),
            block_number: Some(block_num),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    #[test]
    fn first_log_after_excludes_reorged() {
        let cache = LogCache::open_in_memory().unwrap();
        cache
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(200, 0, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_A, 100)]).unwrap();
        let log = cache.first_log_after(&FH, 0, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(200));
    }

    #[test]
    fn last_log_before_excludes_reorged() {
        let cache = LogCache::open_in_memory().unwrap();
        cache
            .insert_logs(
                &FH,
                &[
                    make_test_log(100, 0, BLOCK_HASH_A),
                    make_test_log(200, 0, BLOCK_HASH_B),
                ],
            )
            .unwrap();
        cache.mark_reorged(&[(BLOCK_HASH_B, 200)]).unwrap();
        let log = cache.last_log_before(&FH, 999, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(100));
    }
}
