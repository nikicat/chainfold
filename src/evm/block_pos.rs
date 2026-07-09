//! Position and pointer data types for the log range tree.
//!
//! [`BlockPos`] is the key type ordering the tree; [`LogRef`] is the
//! lightweight pointer the tree stores in lieu of full `Log`s; [`LogPage`]
//! is the unit of input to page insertion and gap fills.
//!
//! [`BlockPos`] implements [`IndexPos`](crate::range_index::IndexPos) so a
//! [`RangeIndex<LogSpec>`](super::RangeIndex) can map a position-gap back to
//! the inclusive block range an `eth_getLogs` fetch needs.

use alloy::rpc::types::Log;

use crate::chain::{BlockNumber, BlockRange};
use crate::evm::types::{BlockHash, LogIdx};
use crate::range_index::IndexPos;

// ============================================================
// BlockPos — position type for the tree
// ============================================================

/// Position within an EVM filter's event stream.
///
/// `Bottom` is the universal lower bound used as `Range.start` for ranges
/// covering the earliest blocks (avoids the underflow problem when a
/// scanned range starts at block 0).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum BlockPos {
    /// Strictly below every real `(block, log_index)`.
    Bottom,
    /// Concrete `(block, log_index)` coordinate.
    At {
        block: BlockNumber,
        log_index: LogIdx,
    },
}

impl BlockPos {
    /// Convert a concrete (block, `log_index`) pair to a position.
    pub fn at(block: BlockNumber, log_index: LogIdx) -> Self {
        BlockPos::At { block, log_index }
    }

    /// The block number this position sits in, if concrete. `None` for
    /// [`Bottom`].
    pub fn block(&self) -> Option<BlockNumber> {
        match self {
            BlockPos::Bottom => None,
            BlockPos::At { block, .. } => Some(*block),
        }
    }
}

impl IndexPos for BlockPos {
    fn bottom() -> Self {
        BlockPos::Bottom
    }

    fn before_block(block: BlockNumber) -> Self {
        match block.checked_sub(1) {
            Some(prev) => BlockPos::At {
                block: prev,
                log_index: LogIdx::MAX,
            },
            None => BlockPos::Bottom,
        }
    }

    fn end_of_block(block: BlockNumber) -> Self {
        BlockPos::At {
            block,
            log_index: LogIdx::MAX,
        }
    }

    fn fetch_bounds(start: Self, end: Self) -> BlockRange {
        let from = match start {
            BlockPos::Bottom => BlockNumber::ZERO,
            BlockPos::At { block, log_index } => {
                if log_index == LogIdx::MAX {
                    block.saturating_add(1)
                } else {
                    block
                }
            }
        };
        let to = match end {
            BlockPos::Bottom => BlockNumber::ZERO,
            BlockPos::At { block, .. } => block,
        };
        BlockRange::new(from, to)
    }
}

// ============================================================
// LogRef — what the tree stores in lieu of full Logs
// ============================================================

/// Pointer into `LogCache` identifying a single cached log.
///
/// Includes `block_hash` so reorg-aware resolution can reject references
/// into blocks that have since been canonicalized away.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct LogRef {
    pub block_number: BlockNumber,
    pub log_index: LogIdx,
    pub block_hash: BlockHash,
}

impl LogRef {
    pub fn pos(&self) -> BlockPos {
        BlockPos::At {
            block: self.block_number,
            log_index: self.log_index,
        }
    }
}

// ============================================================
// LogPage — input to insert_page
// ============================================================

/// A page of logs covering the inclusive block range
/// `[from_block, to_block]` from one `eth_getLogs` call (or a subscription
/// delivery). In position space that is `(before_block(from_block),
/// end_of_block(to_block)]`.
///
/// `end_block_hash` lets the consumer record the matching scanned-range
/// entry in `LogCache::record_scanned_range`.
#[derive(Clone)]
pub struct LogPage {
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
    pub items: Vec<Log>,
    pub end_block_hash: BlockHash,
}

/// Build a [`LogRef`] from an alloy [`Log`]. Returns `None` for logs
/// missing a block hash (pending / mempool logs are not cacheable and
/// MUST NOT enter the tree).
pub(super) fn log_ref_from(log: &Log) -> Option<LogRef> {
    let block_hash = BlockHash::new(log.block_hash?);
    Some(LogRef {
        block_number: BlockNumber::new(log.block_number?),
        log_index: LogIdx::new(log.log_index? as u32),
        block_hash,
    })
}
