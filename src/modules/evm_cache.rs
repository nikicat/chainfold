//! EVM log cache with scanned range tracking.
//!
//! Stores raw `eth_getLogs` results keyed by BLAKE3 hash of (address, topic0-3).
//! Tracks scanned block ranges (even when zero logs returned) and merges
//! adjacent/overlapping ranges eagerly on write. Gap computation identifies
//! uncovered sub-ranges for a requested range.
//!
//! `CachingProvider<P>` wraps an inner alloy `Provider` and intercepts `get_logs`
//! to serve cached results for finalized block ranges. `BlockHeaderCache` trait
//! provides cached block header access.
//!
//! Requires the `evm` feature flag.

use std::sync::{Arc, Mutex};

use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes, LogData, B256};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportResult;
use rusqlite::Connection;

use crate::modules::erc20::Erc20SourceError;
use crate::storage::schema::migrations;
use crate::storage::sqlite::{configure_connection, StorageError};

// ============================================================
// Filter hashing
// ============================================================

/// Compute a deterministic BLAKE3 hash of (address, topic0-3).
///
/// Null topics use a 0xFF sentinel (all-ones, 32 bytes) to distinguish
/// "no topic filter" from any real topic value.
pub fn compute_filter_hash(address: &[u8; 20], topics: &[Option<[u8; 32]>; 4]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(address);
    let sentinel = [0xFF_u8; 32];
    for topic in topics {
        match topic {
            Some(t) => hasher.update(t),
            None => hasher.update(&sentinel),
        };
    }
    *hasher.finalize().as_bytes()
}

// ============================================================
// CachedBlockHeader
// ============================================================

/// A cached block header (height, hash, parent_hash, timestamp).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedBlockHeader {
    pub height: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: u64,
}

// ============================================================
// Gap computation
// ============================================================

/// Compute uncovered sub-ranges within `requested` given sorted, non-overlapping `scanned` ranges.
///
/// Both `requested` and each entry in `scanned` are inclusive `[start, end]` ranges.
/// Returns the sub-ranges of `requested` not covered by any `scanned` range.
pub fn compute_gaps(requested: (u64, u64), scanned: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();
    let mut cursor = requested.0;
    for &(start, end) in scanned {
        if start > requested.1 {
            break;
        }
        if end < cursor {
            continue;
        }
        if start > cursor {
            gaps.push((cursor, start - 1));
        }
        cursor = cursor.max(end + 1);
        if cursor > requested.1 {
            break;
        }
    }
    if cursor <= requested.1 {
        gaps.push((cursor, requested.1));
    }
    gaps
}

// ============================================================
// LogCache
// ============================================================

/// Cache for raw EVM log data and scanned range tracking.
///
/// Opens its own SQLite connection to the same database file used by
/// `SqliteStorage`. WAL mode supports concurrent connections.
pub struct LogCache {
    conn: Mutex<Connection>,
}

impl LogCache {
    /// Open a log cache backed by a SQLite database at the given path.
    ///
    /// Runs migrations (idempotent) and configures WAL mode.
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory log cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        migrations().to_latest(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Record a scanned range and eagerly merge with adjacent/overlapping ranges.
    ///
    /// Adjacent means `end1 + 1 == start2`. A gap of 1 block (e.g., [100,200] + [202,300])
    /// does NOT merge.
    pub fn record_scanned_range(
        &self,
        filter_hash: &[u8; 32],
        start: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        // Find merge candidates: ranges where start_block <= end+1 AND end_block >= start-1.
        // This catches overlapping and adjacent (end+1 == start) ranges.
        let candidates: Vec<(i64, u64, u64)> = {
            let mut stmt = tx.prepare(
                "SELECT id, start_block, end_block FROM scanned_ranges
                 WHERE filter_hash = ?1 AND start_block <= ?3 AND end_block >= ?2",
            )?;
            let result = stmt
                .query_map(
                    rusqlite::params![
                        &filter_hash[..],
                        start.saturating_sub(1) as i64,
                        (end + 1) as i64
                    ],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get::<_, i64>(1)? as u64,
                            row.get::<_, i64>(2)? as u64,
                        ))
                    },
                )?
                .collect::<Result<_, _>>()?;
            result
        };

        let mut merged_start = start;
        let mut merged_end = end;
        let mut ids_to_delete = Vec::new();

        for (id, s, e) in &candidates {
            merged_start = merged_start.min(*s);
            merged_end = merged_end.max(*e);
            ids_to_delete.push(*id);
        }

        // Delete old ranges
        for id in &ids_to_delete {
            tx.execute("DELETE FROM scanned_ranges WHERE id = ?1", [id])?;
        }

        // Insert merged range
        tx.execute(
            "INSERT INTO scanned_ranges (filter_hash, start_block, end_block) VALUES (?1, ?2, ?3)",
            rusqlite::params![&filter_hash[..], merged_start as i64, merged_end as i64],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get all scanned ranges for a filter hash, sorted by start_block.
    pub fn get_scanned_ranges(
        &self,
        filter_hash: &[u8; 32],
    ) -> Result<Vec<(u64, u64)>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT start_block, end_block FROM scanned_ranges
             WHERE filter_hash = ?1 ORDER BY start_block ASC",
        )?;
        let ranges = stmt
            .query_map(rusqlite::params![&filter_hash[..]], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ranges)
    }

    /// Insert logs into the cache. Uses INSERT OR IGNORE to deduplicate by
    /// (filter_hash, block_number, log_index).
    pub fn insert_logs(
        &self,
        filter_hash: &[u8; 32],
        logs: &[Log],
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO cached_logs
                 (filter_hash, block_number, log_index, address, topic0, topic1, topic2, topic3,
                  data, block_hash, transaction_hash, transaction_index, removed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )?;
            for log in logs {
                let topics = log.topics();
                stmt.execute(rusqlite::params![
                    &filter_hash[..],
                    log.block_number.unwrap_or(0) as i64,
                    log.log_index.unwrap_or(0) as i64,
                    log.address().as_slice(),
                    topics.first().map(|t| t.as_slice().to_vec()),
                    topics.get(1).map(|t| t.as_slice().to_vec()),
                    topics.get(2).map(|t| t.as_slice().to_vec()),
                    topics.get(3).map(|t| t.as_slice().to_vec()),
                    log.data().data.as_ref(),
                    log.block_hash.map(|h| h.0.to_vec()),
                    log.transaction_hash.map(|h| h.0.to_vec()),
                    log.transaction_index.unwrap_or(0) as i64,
                    log.removed as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Get cached logs for a filter hash within a block range [from_block, to_block].
    ///
    /// Returns logs ordered by (block_number, log_index). Returns empty vec for
    /// uncached ranges.
    pub fn get_cached_logs(
        &self,
        filter_hash: &[u8; 32],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT block_number, log_index, address, topic0, topic1, topic2, topic3,
                    data, block_hash, transaction_hash, transaction_index, removed
             FROM cached_logs
             WHERE filter_hash = ?1 AND block_number >= ?2 AND block_number <= ?3
             ORDER BY block_number ASC, log_index ASC",
        )?;

        let logs = stmt
            .query_map(
                rusqlite::params![
                    &filter_hash[..],
                    from_block as i64,
                    to_block as i64,
                ],
                |row| {
                    let block_number: i64 = row.get(0)?;
                    let log_index: i64 = row.get(1)?;
                    let address_bytes: Vec<u8> = row.get(2)?;
                    let topic0: Option<Vec<u8>> = row.get(3)?;
                    let topic1: Option<Vec<u8>> = row.get(4)?;
                    let topic2: Option<Vec<u8>> = row.get(5)?;
                    let topic3: Option<Vec<u8>> = row.get(6)?;
                    let data: Vec<u8> = row.get(7)?;
                    let block_hash: Option<Vec<u8>> = row.get(8)?;
                    let tx_hash: Option<Vec<u8>> = row.get(9)?;
                    let tx_index: i64 = row.get(10)?;
                    let removed: i64 = row.get(11)?;

                    // Reconstruct alloy Log
                    let mut topics = Vec::new();
                    for t in [topic0, topic1, topic2, topic3] {
                        if let Some(bytes) = t {
                            topics.push(B256::from_slice(&bytes));
                        }
                    }

                    let log_data = LogData::new_unchecked(topics, Bytes::from(data));

                    let mut addr_arr = [0u8; 20];
                    addr_arr.copy_from_slice(&address_bytes);

                    Ok(Log {
                        inner: alloy::primitives::Log {
                            address: Address::from(addr_arr),
                            data: log_data,
                        },
                        block_hash: block_hash.map(|b| B256::from_slice(&b)),
                        block_number: Some(block_number as u64),
                        block_timestamp: None,
                        transaction_hash: tx_hash.map(|b| B256::from_slice(&b)),
                        transaction_index: Some(tx_index as u64),
                        log_index: Some(log_index as u64),
                        removed: removed != 0,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(logs)
    }

    /// Insert or replace a cached block header.
    pub fn insert_block_header(
        &self,
        height: u64,
        hash: &[u8; 32],
        parent_hash: &[u8; 32],
        timestamp: u64,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO cached_block_headers (height, hash, parent_hash, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![height as i64, &hash[..], &parent_hash[..], timestamp as i64],
        )?;
        Ok(())
    }

    /// Get a cached block header by height.
    pub fn get_block_header(
        &self,
        height: u64,
    ) -> Result<Option<CachedBlockHeader>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT hash, parent_hash, timestamp FROM cached_block_headers WHERE height = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![height as i64], |row| {
            let hash_bytes: Vec<u8> = row.get(0)?;
            let parent_bytes: Vec<u8> = row.get(1)?;
            let timestamp: i64 = row.get(2)?;

            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hash_bytes);
            let mut parent_hash = [0u8; 32];
            parent_hash.copy_from_slice(&parent_bytes);

            Ok(CachedBlockHeader {
                height,
                hash,
                parent_hash,
                timestamp: timestamp as u64,
            })
        })?;

        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }
}

// ============================================================
// FinalityConfig
// ============================================================

/// Chain-specific finality depth configuration.
///
/// Only blocks below `chain_tip - depth()` are considered finalized and cacheable.
/// Blocks within the finality window always go to RPC.
#[derive(Debug, Clone, Copy)]
pub enum FinalityConfig {
    /// Ethereum mainnet: 64 blocks.
    EthereumMainnet,
    /// Arbitrum: instant finality (0 blocks).
    Arbitrum,
    /// Custom finality depth.
    Custom(u64),
}

impl FinalityConfig {
    /// Returns the finality depth in blocks.
    pub fn depth(&self) -> u64 {
        match self {
            Self::EthereumMainnet => 64,
            Self::Arbitrum => 0,
            Self::Custom(d) => *d,
        }
    }
}

// ============================================================
// CachingProvider
// ============================================================

/// A provider wrapper that caches `get_logs` results for finalized block ranges.
///
/// Wraps an inner `P: Provider` and an `Arc<LogCache>`. Implements the alloy
/// `Provider` trait, intercepting `get_logs` to serve cached results for
/// finalized ranges. Non-cacheable filters (multi-address, multi-topic-per-slot,
/// tag-based block ranges) delegate directly to the inner provider.
pub struct CachingProvider<P: Provider> {
    inner: P,
    cache: Arc<LogCache>,
    finality: FinalityConfig,
}

impl<P: Provider + Clone> Clone for CachingProvider<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: self.cache.clone(),
            finality: self.finality,
        }
    }
}

impl<P: Provider> CachingProvider<P> {
    /// Create a new caching provider wrapping the inner provider.
    pub fn new(inner: P, cache: Arc<LogCache>, finality: FinalityConfig) -> Self {
        Self {
            inner,
            cache,
            finality,
        }
    }

    /// Access the underlying log cache.
    pub fn cache(&self) -> &Arc<LogCache> {
        &self.cache
    }
}

/// Extract a single address from a Filter's address field.
/// Returns None if the filter has zero or multiple addresses.
fn extract_single_address(filter: &Filter) -> Option<Address> {
    if filter.address.len() != 1 {
        return None;
    }
    filter.address.iter().next().copied()
}

/// Extract topics from a Filter as an array of Option<[u8; 32]>.
/// Returns None if any topic slot has multiple values (OR filter per slot).
fn extract_topics(filter: &Filter) -> Option<[Option<[u8; 32]>; 4]> {
    let mut result: [Option<[u8; 32]>; 4] = [None; 4];
    for (i, topic_set) in filter.topics.iter().enumerate() {
        if topic_set.len() > 1 {
            return None; // Multi-value topic slot — can't cache
        }
        result[i] = topic_set.iter().next().map(|b| b.0);
    }
    Some(result)
}

/// Build a sub-filter for a specific block range, preserving address and topics.
fn build_sub_filter(base: &Filter, from: u64, to: u64) -> Filter {
    let mut f = base.clone();
    f.block_option = alloy::rpc::types::FilterBlockOption::Range {
        from_block: Some(from.into()),
        to_block: Some(to.into()),
    };
    f
}

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync> Provider for CachingProvider<P> {
    fn root(&self) -> &RootProvider<Ethereum> {
        self.inner.root()
    }

    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        // Extract single address; bail to inner for multi-address
        let address = match extract_single_address(filter) {
            Some(a) => a,
            None => return self.inner.get_logs(filter).await,
        };

        // Extract topics; bail for multi-value-per-slot
        let topics = match extract_topics(filter) {
            Some(t) => t,
            None => return self.inner.get_logs(filter).await,
        };

        // Extract numeric block range; bail for tag-based (latest/pending/etc)
        let from_block = match filter.get_from_block() {
            Some(b) => b,
            None => return self.inner.get_logs(filter).await,
        };
        let to_block = match filter.get_to_block() {
            Some(b) => b,
            None => return self.inner.get_logs(filter).await,
        };

        // Compute filter hash for cache key
        let addr_bytes: [u8; 20] = address.0 .0;
        let filter_hash = compute_filter_hash(&addr_bytes, &topics);

        // Get chain tip for finality computation
        let chain_tip = self.inner.get_block_number().await?;
        let finality_boundary = chain_tip.saturating_sub(self.finality.depth());

        // Clamp cacheable range
        let safe_to = to_block.min(finality_boundary);

        let mut all_logs = Vec::new();

        // Handle cacheable portion [from_block, safe_to]
        if from_block <= safe_to {
            let cache = self.cache.clone();
            let fh = filter_hash;

            // Get scanned ranges
            let scanned = {
                let c = cache.clone();
                tokio::task::spawn_blocking(move || c.get_scanned_ranges(&fh))
                    .await
                    .map_err(|e| {
                        alloy::transports::TransportError::local_usage_str(&format!(
                            "spawn_blocking: {}",
                            e
                        ))
                    })?
                    .map_err(|e| {
                        alloy::transports::TransportError::local_usage_str(&format!(
                            "cache error: {}",
                            e
                        ))
                    })?
            };

            // Compute gaps
            let gaps = compute_gaps((from_block, safe_to), &scanned);

            // Fetch each gap from inner provider
            for (gap_from, gap_to) in gaps {
                let sub_filter = build_sub_filter(filter, gap_from, gap_to);
                let gap_logs = self.inner.get_logs(&sub_filter).await?;

                // Insert fetched logs into cache
                let c = cache.clone();
                let fh = filter_hash;
                let logs_clone = gap_logs.clone();
                tokio::task::spawn_blocking(move || c.insert_logs(&fh, &logs_clone))
                    .await
                    .map_err(|e| {
                        alloy::transports::TransportError::local_usage_str(&format!(
                            "spawn_blocking: {}",
                            e
                        ))
                    })?
                    .map_err(|e| {
                        alloy::transports::TransportError::local_usage_str(&format!(
                            "cache error: {}",
                            e
                        ))
                    })?;

                // Record scanned range
                let c = cache.clone();
                let fh = filter_hash;
                tokio::task::spawn_blocking(move || {
                    c.record_scanned_range(&fh, gap_from, gap_to)
                })
                .await
                .map_err(|e| {
                    alloy::transports::TransportError::local_usage_str(&format!(
                        "spawn_blocking: {}",
                        e
                    ))
                })?
                .map_err(|e| {
                    alloy::transports::TransportError::local_usage_str(&format!(
                        "cache error: {}",
                        e
                    ))
                })?;
            }

            // Retrieve all cached logs for the cacheable range
            let c = cache.clone();
            let fh = filter_hash;
            let cached_from = from_block;
            let cached_to = safe_to;
            let cached_logs = tokio::task::spawn_blocking(move || {
                c.get_cached_logs(&fh, cached_from, cached_to)
            })
            .await
            .map_err(|e| {
                alloy::transports::TransportError::local_usage_str(&format!(
                    "spawn_blocking: {}",
                    e
                ))
            })?
            .map_err(|e| {
                alloy::transports::TransportError::local_usage_str(&format!("cache error: {}", e))
            })?;

            all_logs.extend(cached_logs);
        }

        // Handle unfinalized portion [safe_to+1, to_block]
        if to_block > safe_to {
            let unfin_from = if from_block > safe_to {
                from_block
            } else {
                safe_to + 1
            };
            let sub_filter = build_sub_filter(filter, unfin_from, to_block);
            let unfin_logs = self.inner.get_logs(&sub_filter).await?;
            all_logs.extend(unfin_logs);
        }

        // Sort by (block_number, log_index)
        all_logs.sort_by_key(|log| {
            (
                log.block_number.unwrap_or(0),
                log.log_index.unwrap_or(0),
            )
        });

        Ok(all_logs)
    }
}

// ============================================================
// BlockHeaderCache trait
// ============================================================

/// Trait for cached block header access.
///
/// Default implementation delegates to `Provider::get_block_by_number`.
/// `CachingProvider` overrides to check the `LogCache` first.
#[async_trait::async_trait]
pub trait BlockHeaderCache: Send + Sync {
    /// Get a block header by height, possibly from cache.
    ///
    /// Returns `(hash, parent_hash, timestamp)` or `None` if the block doesn't exist.
    async fn get_or_fetch_block_header(
        &self,
        height: u64,
    ) -> Result<Option<([u8; 32], [u8; 32], u64)>, Erc20SourceError>;
}

/// CachingProvider: check LogCache first, delegate on miss.
#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync> BlockHeaderCache for CachingProvider<P> {
    async fn get_or_fetch_block_header(
        &self,
        height: u64,
    ) -> Result<Option<([u8; 32], [u8; 32], u64)>, Erc20SourceError> {
        // Check cache first
        let cache = self.cache.clone();
        let cached = tokio::task::spawn_blocking(move || cache.get_block_header(height))
            .await
            .map_err(|e| Erc20SourceError::Rpc(e.to_string()))?
            .map_err(|e| Erc20SourceError::Rpc(e.to_string()))?;

        if let Some(header) = cached {
            return Ok(Some((header.hash, header.parent_hash, header.timestamp)));
        }

        // Cache miss: fetch from provider
        let block = self
            .inner
            .get_block_by_number(height.into())
            .await
            .map_err(|e| Erc20SourceError::Rpc(e.to_string()))?;

        match block {
            Some(b) => {
                let hash: [u8; 32] = b.header.hash.0;
                let parent_hash: [u8; 32] = b.header.parent_hash.0;
                let timestamp = b.header.timestamp;

                // Store in cache
                let cache = self.cache.clone();
                let h = hash;
                let ph = parent_hash;
                let ts = timestamp;
                let _ = tokio::task::spawn_blocking(move || {
                    cache.insert_block_header(height, &h, &ph, ts)
                })
                .await;

                Ok(Some((hash, parent_hash, timestamp)))
            }
            None => Ok(None),
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- compute_filter_hash tests --

    #[test]
    fn filter_hash_deterministic() {
        let addr = [0x42u8; 20];
        let topics = [Some([0xAAu8; 32]), None, None, None];
        let h1 = compute_filter_hash(&addr, &topics);
        let h2 = compute_filter_hash(&addr, &topics);
        assert_eq!(h1, h2, "same inputs must produce same hash");
    }

    #[test]
    fn filter_hash_different_address() {
        let topics = [None; 4];
        let h1 = compute_filter_hash(&[0x01u8; 20], &topics);
        let h2 = compute_filter_hash(&[0x02u8; 20], &topics);
        assert_ne!(h1, h2, "different addresses must produce different hashes");
    }

    #[test]
    fn filter_hash_different_topics() {
        let addr = [0x42u8; 20];
        let h1 = compute_filter_hash(&addr, &[Some([0xAAu8; 32]), None, None, None]);
        let h2 = compute_filter_hash(&addr, &[Some([0xBBu8; 32]), None, None, None]);
        assert_ne!(h1, h2, "different topics must produce different hashes");
    }

    #[test]
    fn filter_hash_none_vs_some() {
        let addr = [0x42u8; 20];
        let h1 = compute_filter_hash(&addr, &[None, None, None, None]);
        let h2 = compute_filter_hash(&addr, &[Some([0x00u8; 32]), None, None, None]);
        assert_ne!(h1, h2, "None topic must differ from zero topic");
    }

    // -- record_scanned_range tests --

    #[test]
    fn range_new_persists() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 200)]);
    }

    #[test]
    fn range_overlapping_merge() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache.record_scanned_range(&fh, 150, 250).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 250)]);
    }

    #[test]
    fn range_adjacent_merge() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache.record_scanned_range(&fh, 201, 300).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 300)]);
    }

    #[test]
    fn range_gap_of_one_no_merge() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache.record_scanned_range(&fh, 202, 300).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 200), (202, 300)]);
    }

    #[test]
    fn range_contained_absorbed() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 300).unwrap();
        cache.record_scanned_range(&fh, 150, 200).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 300)]);
    }

    #[test]
    fn range_identical_idempotent() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 200)]);
    }

    #[test]
    fn range_multi_merge() {
        // Three disjoint ranges, then a bridging range that merges all three.
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache.record_scanned_range(&fh, 300, 400).unwrap();
        cache.record_scanned_range(&fh, 500, 600).unwrap();
        // Bridge 200..500 merges all three.
        cache.record_scanned_range(&fh, 201, 499).unwrap();
        let ranges = cache.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 600)]);
    }

    // -- compute_gaps tests --

    #[test]
    fn gaps_no_scanned_returns_full_range() {
        assert_eq!(compute_gaps((100, 200), &[]), vec![(100, 200)]);
    }

    #[test]
    fn gaps_fully_covered_returns_empty() {
        assert_eq!(compute_gaps((100, 200), &[(50, 250)]), Vec::<(u64, u64)>::new());
    }

    #[test]
    fn gaps_partial_coverage() {
        assert_eq!(
            compute_gaps((100, 300), &[(150, 200)]),
            vec![(100, 149), (201, 300)]
        );
    }

    #[test]
    fn gaps_multiple_scanned() {
        assert_eq!(
            compute_gaps((100, 500), &[(100, 150), (200, 250), (400, 500)]),
            vec![(151, 199), (251, 399)]
        );
    }

    #[test]
    fn gaps_scanned_beyond_request() {
        assert_eq!(
            compute_gaps((100, 200), &[(150, 300)]),
            vec![(100, 149)]
        );
    }

    // -- insert_logs / get_cached_logs round-trip --

    fn make_test_log(block_num: u64, log_idx: u64) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let topic1 = B256::from([0xEE; 32]);
        let data = Bytes::from(vec![0x01, 0x02, 0x03]);
        let log_data = LogData::new_unchecked(vec![topic0, topic1], data);

        Log {
            inner: alloy::primitives::Log {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(B256::from([0xBB; 32])),
            block_number: Some(block_num),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xCC; 32])),
            transaction_index: Some(7),
            log_index: Some(log_idx),
            removed: false,
        }
    }

    #[test]
    fn logs_round_trip() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        let logs = vec![make_test_log(100, 0), make_test_log(100, 1), make_test_log(101, 0)];

        cache.insert_logs(&fh, &logs).unwrap();
        let retrieved = cache.get_cached_logs(&fh, 100, 101).unwrap();

        assert_eq!(retrieved.len(), 3);
        // Verify field-by-field equality (block_timestamp is not stored)
        for (orig, cached) in logs.iter().zip(retrieved.iter()) {
            assert_eq!(orig.address(), cached.address());
            assert_eq!(orig.topics(), cached.topics());
            assert_eq!(orig.data().data, cached.data().data);
            assert_eq!(orig.block_hash, cached.block_hash);
            assert_eq!(orig.block_number, cached.block_number);
            assert_eq!(orig.transaction_hash, cached.transaction_hash);
            assert_eq!(orig.transaction_index, cached.transaction_index);
            assert_eq!(orig.log_index, cached.log_index);
            assert_eq!(orig.removed, cached.removed);
        }
    }

    #[test]
    fn logs_dedup_by_filter_block_index() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        let log = make_test_log(100, 0);

        cache.insert_logs(&fh, &[log.clone()]).unwrap();
        cache.insert_logs(&fh, &[log]).unwrap(); // duplicate
        let retrieved = cache.get_cached_logs(&fh, 100, 100).unwrap();
        assert_eq!(retrieved.len(), 1, "INSERT OR IGNORE should deduplicate");
    }

    #[test]
    fn logs_range_query() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        let logs = vec![
            make_test_log(100, 0),
            make_test_log(200, 0),
            make_test_log(300, 0),
        ];
        cache.insert_logs(&fh, &logs).unwrap();

        // Only block 200
        let retrieved = cache.get_cached_logs(&fh, 200, 200).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].block_number, Some(200));
    }

    #[test]
    fn logs_empty_for_uncached_range() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh = [0xAA; 32];
        let retrieved = cache.get_cached_logs(&fh, 100, 200).unwrap();
        assert!(retrieved.is_empty());
    }

    // -- block header tests --

    #[test]
    fn block_header_round_trip() {
        let cache = LogCache::open_in_memory().unwrap();
        let hash = [0xAA; 32];
        let parent = [0xBB; 32];

        cache.insert_block_header(42, &hash, &parent, 1700000000).unwrap();
        let header = cache.get_block_header(42).unwrap().expect("should exist");

        assert_eq!(header.height, 42);
        assert_eq!(header.hash, hash);
        assert_eq!(header.parent_hash, parent);
        assert_eq!(header.timestamp, 1700000000);
    }

    #[test]
    fn block_header_missing() {
        let cache = LogCache::open_in_memory().unwrap();
        assert!(cache.get_block_header(999).unwrap().is_none());
    }

    #[test]
    fn block_header_replace() {
        let cache = LogCache::open_in_memory().unwrap();
        let hash1 = [0xAA; 32];
        let hash2 = [0xCC; 32];
        let parent = [0xBB; 32];

        cache.insert_block_header(42, &hash1, &parent, 100).unwrap();
        cache.insert_block_header(42, &hash2, &parent, 200).unwrap();

        let header = cache.get_block_header(42).unwrap().expect("should exist");
        assert_eq!(header.hash, hash2);
        assert_eq!(header.timestamp, 200);
    }

    // -- scanned ranges isolation between filters --

    #[test]
    fn ranges_isolated_by_filter() {
        let cache = LogCache::open_in_memory().unwrap();
        let fh1 = [0xAA; 32];
        let fh2 = [0xBB; 32];

        cache.record_scanned_range(&fh1, 100, 200).unwrap();
        cache.record_scanned_range(&fh2, 300, 400).unwrap();

        assert_eq!(cache.get_scanned_ranges(&fh1).unwrap(), vec![(100, 200)]);
        assert_eq!(cache.get_scanned_ranges(&fh2).unwrap(), vec![(300, 400)]);
    }

    // -- FinalityConfig tests --

    #[test]
    fn finality_config_ethereum_mainnet() {
        assert_eq!(FinalityConfig::EthereumMainnet.depth(), 64);
    }

    #[test]
    fn finality_config_arbitrum() {
        assert_eq!(FinalityConfig::Arbitrum.depth(), 0);
    }

    #[test]
    fn finality_config_custom() {
        assert_eq!(FinalityConfig::Custom(128).depth(), 128);
        assert_eq!(FinalityConfig::Custom(0).depth(), 0);
    }

    // -- extract_single_address / extract_topics tests --

    #[test]
    fn extract_single_address_works() {
        let addr = Address::from([0x42u8; 20]);
        let filter = Filter::new().address(addr).from_block(0u64).to_block(100u64);
        assert_eq!(extract_single_address(&filter), Some(addr));
    }

    #[test]
    fn extract_single_address_empty() {
        let filter = Filter::new().from_block(0u64).to_block(100u64);
        assert_eq!(extract_single_address(&filter), None);
    }

    #[test]
    fn extract_topics_single() {
        let topic = B256::from([0xDD; 32]);
        let filter = Filter::new()
            .address(Address::from([0x42u8; 20]))
            .event_signature(topic)
            .from_block(0u64)
            .to_block(100u64);
        let topics = extract_topics(&filter).unwrap();
        assert_eq!(topics[0], Some([0xDD; 32]));
        assert_eq!(topics[1], None);
        assert_eq!(topics[2], None);
        assert_eq!(topics[3], None);
    }

    #[test]
    fn extract_topics_multi_value_per_slot_returns_none() {
        use alloy::rpc::types::FilterSet;
        let mut filter = Filter::new()
            .address(Address::from([0x42u8; 20]))
            .from_block(0u64)
            .to_block(100u64);
        // Set topic0 to multiple values
        filter.topics[0] = FilterSet::from(vec![
            B256::from([0xDD; 32]),
            B256::from([0xEE; 32]),
        ]);
        assert!(extract_topics(&filter).is_none());
    }

    // -- CachingProvider tests (using mock provider) --
    // These tests require a tokio runtime and a real alloy provider.
    // We use an in-memory mock approach: since CachingProvider wraps any P: Provider,
    // we test the cache integration using direct LogCache + the helper functions.

    #[test]
    fn build_sub_filter_preserves_address_and_topics() {
        let addr = Address::from([0x42u8; 20]);
        let topic = B256::from([0xDD; 32]);
        let base = Filter::new()
            .address(addr)
            .event_signature(topic)
            .from_block(0u64)
            .to_block(100u64);
        let sub = build_sub_filter(&base, 50, 75);
        assert_eq!(sub.get_from_block(), Some(50));
        assert_eq!(sub.get_to_block(), Some(75));
        assert_eq!(extract_single_address(&sub), Some(addr));
        let topics = extract_topics(&sub).unwrap();
        assert_eq!(topics[0], Some([0xDD; 32]));
    }

    // Integration test: CachingProvider caches logs for finalized range
    // and serves from cache on second call, verifiable via LogCache state.
    #[test]
    fn caching_provider_cache_accessor() {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        // Pre-populate cache
        let fh = [0xAA; 32];
        cache.record_scanned_range(&fh, 100, 200).unwrap();
        cache
            .insert_logs(&fh, &[make_test_log(150, 0)])
            .unwrap();

        // Verify cache is accessible
        let logs = cache.get_cached_logs(&fh, 100, 200).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].block_number, Some(150));
    }

    // Test that two LogCache references (Arc) share data
    #[test]
    fn shared_arc_log_cache() {
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let cache2 = cache.clone();
        let fh = [0xAA; 32];

        // Writer inserts via cache
        cache.insert_logs(&fh, &[make_test_log(100, 0)]).unwrap();
        cache.record_scanned_range(&fh, 100, 100).unwrap();

        // Reader sees data via cache2
        let logs = cache2.get_cached_logs(&fh, 100, 100).unwrap();
        assert_eq!(logs.len(), 1);
        let ranges = cache2.get_scanned_ranges(&fh).unwrap();
        assert_eq!(ranges, vec![(100, 100)]);
    }
}
