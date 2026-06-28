//! Position and pointer data types for the range tree.
//!
//! [`BlockPos`] is the key type ordering the tree; [`LogRef`] is the
//! lightweight pointer the tree stores in lieu of full `Log`s; [`LogPage`]
//! is the unit of input to page insertion and gap fills.

use alloy::primitives::B256;
use alloy::rpc::types::Log;

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
    At { block: u64, log_index: u32 },
}

impl BlockPos {
    /// The position *just before* the first log in `block`. Used as the
    /// exclusive lower bound for a block-level scanned range whose start
    /// is `block`.
    pub fn before_block(block: u64) -> Self {
        match block.checked_sub(1) {
            Some(prev) => BlockPos::At {
                block: prev,
                log_index: u32::MAX,
            },
            None => BlockPos::Bottom,
        }
    }

    /// The position *at the end* of `block` — strictly greater than every
    /// possible log in that block. Used as the inclusive upper bound for
    /// a block-level scanned range whose end is `block`.
    pub fn end_of_block(block: u64) -> Self {
        BlockPos::At {
            block,
            log_index: u32::MAX,
        }
    }

    /// Convert a concrete (block, `log_index`) pair to a position.
    pub fn at(block: u64, log_index: u32) -> Self {
        BlockPos::At { block, log_index }
    }

    /// The block number this position sits in, if concrete. `None` for
    /// [`Bottom`].
    pub fn block(&self) -> Option<u64> {
        match self {
            BlockPos::Bottom => None,
            BlockPos::At { block, .. } => Some(*block),
        }
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
    pub block_number: u64,
    pub log_index: u32,
    pub block_hash: B256,
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

/// A page of logs covering `(from_block, to_block]` from one
/// `eth_getLogs` call (or a subscription delivery).
///
/// `end_block_hash` lets the consumer record the matching scanned-range
/// entry in `LogCache::record_scanned_range`.
#[derive(Clone)]
pub struct LogPage {
    pub from_block: u64,
    pub to_block: u64,
    pub items: Vec<Log>,
    pub end_block_hash: B256,
}

/// Build a [`LogRef`] from an alloy [`Log`]. Returns `None` for logs
/// missing a block hash (pending / mempool logs are not cacheable and
/// MUST NOT enter the tree).
pub(super) fn log_ref_from(log: &Log) -> Option<LogRef> {
    let block_hash = log.block_hash?;
    Some(LogRef {
        block_number: log.block_number?,
        log_index: log.log_index? as u32,
        block_hash,
    })
}
