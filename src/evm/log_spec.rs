//! [`LogSpec`] — the [`RangeSpec`] for EVM logs.
//!
//! Holds a shared [`LogCache`] handle and the filter hash it serves. The tree
//! stores lightweight [`LogRef`]s; [`resolve`](RangeSpec::resolve) reconstructs
//! the full [`EvmEvent`] from the cache **synchronously** — the block header
//! it needs for the timestamp must already be cached (the fetcher and the live
//! path pre-persist headers; see the headers-present invariant in the R3 plan).
use std::sync::Arc;

use alloy::rpc::types::Log;

use super::block_pos::{BlockPos, LogPage, LogRef, log_ref_from};
use crate::arch::{HistoryError, Positioned, Timestamp};
use crate::cache::{FilterHash, ScanPartition};
use crate::chain::{BlockNumber, BlockRange};
use crate::evm::cache::LogCache;
use crate::evm::types::{BlockHash, EvmEvent, EvmPosition, LogIdx, Topic, TxHash};
use crate::range_index::{LoadedSpan, RangeSpec};

/// [`RangeSpec`] for a single EVM log filter over a [`LogCache`].
#[derive(Clone)]
pub struct LogSpec {
    store: Arc<LogCache>,
    filter_hash: FilterHash,
}

impl LogSpec {
    pub fn new(store: Arc<LogCache>, filter_hash: FilterHash) -> Self {
        Self { store, filter_hash }
    }
}

impl RangeSpec for LogSpec {
    type Pos = BlockPos;
    type Item = LogRef;
    type Event = EvmEvent;
    type PubPos = EvmPosition;
    type Page = LogPage;

    fn item_pos(item: &LogRef) -> BlockPos {
        item.pos()
    }

    fn page_items(page: &LogPage) -> Vec<LogRef> {
        // Pending logs without a block hash are un-cacheable and dropped.
        page.items.iter().filter_map(log_ref_from).collect()
    }

    fn page_bounds(page: &LogPage) -> BlockRange {
        page.range
    }

    fn resolve(&self, item: &LogRef) -> Result<Positioned<EvmEvent, EvmPosition>, HistoryError> {
        let log = self
            .store
            .get_log_by_ref(&self.filter_hash, &item.block_hash, item.log_index)
            .map_err(|e| HistoryError::internal(format!("get_log_by_ref: {e}")))?
            .ok_or_else(|| {
                HistoryError::internal(format!(
                    "log_ref points to reorged block {:?} (log_index {})",
                    item.block_hash, item.log_index
                ))
            })?;

        let mut event = log_to_evm_event(&log);
        // Sync-only timestamp fill: prefer the log's own timestamp; otherwise
        // read the (must-be-cached) block header. Never RPC here.
        if log.block_timestamp.is_none() {
            let bn = BlockNumber::new(log.block_number.unwrap_or(0));
            let header = self
                .store
                .get_block_header(bn)
                .map_err(|e| HistoryError::internal(format!("get_block_header: {e}")))?
                .ok_or_else(|| {
                    HistoryError::internal(format!(
                        "block header {bn} not cached (headers-present invariant violated)"
                    ))
                })?;
            event.timestamp = header.timestamp;
        }
        let pos = event.position();
        Ok(Positioned::new(event, pos))
    }

    fn persist(&self, page: &LogPage) -> Result<(), HistoryError> {
        if !page.items.is_empty() {
            self.store
                .insert_logs(&self.filter_hash, &page.items)
                .map_err(|e| HistoryError::internal(format!("insert_logs: {e}")))?;
        }
        self.store
            .coverage()
            .record_scanned_range(
                &ScanPartition::Filter(self.filter_hash),
                page.range,
                page.end_block_hash.into(),
            )
            .map_err(|e| HistoryError::internal(format!("record_scanned_range: {e}")))
    }

    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<LogRef>>, HistoryError> {
        let scanned = self
            .store
            .coverage()
            .get_scanned_ranges(&ScanPartition::Filter(self.filter_hash))
            .map_err(|e| HistoryError::internal(format!("get_scanned_ranges: {e}")))?;

        let mut spans = Vec::new();
        for scanned_range in scanned {
            if scanned_range.to < window.from || scanned_range.from > window.to {
                continue;
            }
            let logs = self
                .store
                .get_logs_in_range(&self.filter_hash, scanned_range)
                .map_err(|e| HistoryError::internal(format!("get_logs_in_range: {e}")))?;
            let items: Vec<LogRef> = logs.iter().filter_map(log_ref_from).collect();
            spans.push(LoadedSpan {
                range: scanned_range,
                items,
            });
        }
        Ok(spans)
    }

    fn load_pos(&self, value: serde_json::Value) -> Option<EvmPosition> {
        EvmPosition::from_value(value)
    }

    fn index_pos(p: &EvmPosition) -> BlockPos {
        if *p == EvmPosition::earliest() {
            BlockPos::Bottom
        } else {
            BlockPos::At {
                block: p.block,
                log_index: p.log_index,
            }
        }
    }

    fn head_pos(n: BlockNumber) -> EvmPosition {
        // End-of-block cut: a `Hist` ending here iterates every log up to
        // and including the last one in `n`.
        EvmPosition {
            block: n,
            log_index: LogIdx::MAX,
        }
    }

    fn window(from: &EvmPosition, to: &EvmPosition) -> BlockRange {
        BlockRange::new(from.block, to.block)
    }
}

/// Build an [`EvmEvent`] from an alloy [`Log`]. Provider-supplied per-log
/// timestamp when present; otherwise `0`, and [`LogSpec::resolve`] fills it
/// from the cached block header.
fn log_to_evm_event(log: &Log) -> EvmEvent {
    EvmEvent {
        contract_address: log.address(),
        block_number: BlockNumber::new(log.block_number.unwrap_or(0)),
        timestamp: Timestamp::new(log.block_timestamp.unwrap_or(0)),
        block_hash: log.block_hash.map_or(BlockHash::ZERO, BlockHash::new),
        log_index: LogIdx::new(log.log_index.unwrap_or(0) as u32),
        tx_hash: log.transaction_hash.map_or(TxHash::ZERO, TxHash::new),
        topics: log.topics().iter().copied().map(Topic::new).collect(),
        data: log.data().data.to_vec(),
    }
}

// ============================================================
// Tests — LogSpec resolution semantics (the EVM-specific parts of the
// generic walk tests, which now run over the synthetic MockSpec).
// ============================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
    use futures_util::StreamExt;

    use super::*;
    use crate::range_index::{IndexPos, RangeIndex, never_fetch};
    use crate::share::Share;

    const FH: FilterHash = FilterHash::new([0xAA; 32]);
    const HASH_A: BlockHash = BlockHash::repeat_byte(0xBB);
    const HASH_B: BlockHash = BlockHash::repeat_byte(0xBC);
    /// Base for fixture block timestamps (`TS_BASE + block`).
    const TS_BASE: Timestamp = Timestamp::new(1_700_000_000);

    fn bn(n: u64) -> BlockNumber {
        BlockNumber::from(n)
    }

    fn mk_log(block: BlockNumber, idx: LogIdx, block_hash: BlockHash) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01]);
        let log_data = LogData::new_unchecked(vec![topic0], data);
        Log {
            inner: LogInner {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(block_hash.b256()),
            block_number: Some(block.get()),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(u64::from(idx.get())),
            removed: false,
        }
    }

    fn mk_cache() -> Arc<LogCache> {
        Arc::new(LogCache::open_in_memory().unwrap())
    }

    /// A walk resolves each `LogRef` through the cache: timestamps come from
    /// the (must-be-cached) block headers — the headers-present invariant —
    /// and positions map to `(block, log_index)`.
    #[tokio::test]
    async fn resolve_fills_timestamp_from_cached_header() {
        let store = mk_cache();
        let logs = vec![
            mk_log(bn(150), LogIdx::ZERO, HASH_A),
            mk_log(bn(180), LogIdx::new(2), HASH_B),
        ];
        store.insert_logs(&FH, &logs).unwrap();
        // Fixture logs carry no block_timestamp, so resolution exercises
        // the headers-present path: both block headers must be cached.
        store
            .insert_block_header(bn(150), &HASH_A, &BlockHash::ZERO, TS_BASE + 150)
            .unwrap();
        store
            .insert_block_header(bn(180), &HASH_B, &HASH_A, TS_BASE + 180)
            .unwrap();
        let idx = Arc::new(RangeIndex::new(
            LogSpec::new(store, FH),
            never_fetch::<LogSpec>(),
        ));
        idx.insert_page(&LogPage {
            range: BlockRange::from_u64(100, 200),
            items: logs,
            end_block_hash: HASH_B,
        });
        let mut stream = idx.share().items(
            BlockPos::before_block(bn(100)),
            BlockPos::end_of_block(bn(200)),
        );
        let a = stream.next().await.unwrap().unwrap();
        assert_eq!(a.item.block_number, bn(150));
        assert_eq!(a.item.timestamp, TS_BASE + 150);
        assert_eq!(a.pos, EvmPosition::new(150, 0));
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(b.item.block_number, bn(180));
        assert_eq!(b.item.timestamp, TS_BASE + 180);
        assert_eq!(b.pos, EvmPosition::new(180, 2));
        assert!(stream.next().await.is_none());
    }

    /// A provider-supplied per-log `block_timestamp` short-circuits the
    /// header read — no header needs to be cached.
    #[tokio::test]
    async fn resolve_prefers_per_log_timestamp() {
        let store = mk_cache();
        let mut log = mk_log(bn(150), LogIdx::ZERO, HASH_A);
        log.block_timestamp = Some((TS_BASE + 150).get());
        store.insert_logs(&FH, std::slice::from_ref(&log)).unwrap();
        let idx = Arc::new(RangeIndex::new(
            LogSpec::new(store, FH),
            never_fetch::<LogSpec>(),
        ));
        idx.insert_page(&LogPage {
            range: BlockRange::from_u64(100, 200),
            items: vec![log],
            end_block_hash: HASH_A,
        });
        let got = idx
            .share()
            .items(
                BlockPos::before_block(bn(100)),
                BlockPos::end_of_block(bn(200)),
            )
            .next()
            .await
            .unwrap()
            .unwrap();
        // No header was cached — the per-log timestamp served resolution.
        assert_eq!(got.item.timestamp, TS_BASE + 150);
    }

    /// Pending logs (no block hash) are un-cacheable and dropped from pages.
    #[test]
    fn page_items_drop_pending_logs() {
        let mut pending = mk_log(bn(150), LogIdx::ZERO, HASH_A);
        pending.block_hash = None;
        let page = LogPage {
            range: BlockRange::from_u64(100, 200),
            items: vec![pending, mk_log(bn(160), LogIdx::ZERO, HASH_B)],
            end_block_hash: HASH_B,
        };
        let items = LogSpec::page_items(&page);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].block_number, bn(160));
    }
}
