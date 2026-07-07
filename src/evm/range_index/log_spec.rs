//! [`LogSpec`] — the [`RangeSpec`] for EVM logs.
//!
//! Holds a shared [`LogCache`] handle and the filter hash it serves. The tree
//! stores lightweight [`LogRef`]s; [`resolve`](RangeSpec::resolve) reconstructs
//! the full [`EvmEvent`] from the cache **synchronously** — the block header
//! it needs for the timestamp must already be cached (the fetcher and the live
//! path pre-persist headers; see the headers-present invariant in the R3 plan).

use alloy::rpc::types::Log;

use super::block_pos::{BlockPos, LogPage, LogRef, log_ref_from};
use super::spec::{LoadedSpan, RangeSpec};
use crate::arch::{HistoryError, Positioned, Timestamp};
use crate::evm::cache::{LogCache, ScanPartition};
use crate::evm::types::{
    BlockHash, BlockNumber, BlockRange, EvmEvent, EvmPosition, FilterHash, LogIdx, Topic, TxHash,
};
use crate::shared::Shared;

/// [`RangeSpec`] for a single EVM log filter over a [`LogCache`].
#[derive(Clone)]
pub struct LogSpec {
    store: Shared<LogCache>,
    filter_hash: FilterHash,
}

impl LogSpec {
    pub fn new(store: Shared<LogCache>, filter_hash: FilterHash) -> Self {
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
        BlockRange::new(page.from_block, page.to_block)
    }

    fn page_tip(page: &LogPage) -> EvmPosition {
        // End-of-block cut: a Hist ending here iterates every log up to and
        // including the last one in `to_block`.
        EvmPosition::new(page.to_block.get(), u32::MAX)
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
            .record_scanned_range(
                &ScanPartition::Filter(self.filter_hash),
                BlockRange::new(page.from_block, page.to_block),
                &page.end_block_hash,
            )
            .map_err(|e| HistoryError::internal(format!("record_scanned_range: {e}")))
    }

    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<LogRef>>, HistoryError> {
        let scanned = self
            .store
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
