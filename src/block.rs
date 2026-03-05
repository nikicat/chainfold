use crate::cursor::ChainCursor;
use crate::event::Event;

/// A block in the chain DAG, containing metadata and an ordered list of events.
///
/// Block is the first-class unit of storage in the block DAG model. Each block
/// has a hash, a parent hash (for chain linkage), a height, and the events that
/// occurred in it.
///
/// Generic parameters:
/// - C: ChainCursor — the cursor type for this source (EvmCursor, BtcCursor, etc.)
/// - E — the user-defined event payload type
///
/// CEX sources (no block concept) produce synthetic single-event blocks to fit the model.
#[derive(Debug, Clone)]
pub struct Block<C: ChainCursor, E> {
    /// Chain-native hash of this block. 32 bytes.
    pub block_hash: [u8; 32],
    /// Hash of the parent block. All-zeros ([0u8; 32]) for genesis.
    pub parent_hash: [u8; 32],
    /// Block height (0-indexed from genesis).
    pub height: u64,
    /// Events in this block, in cursor order.
    pub events: Vec<Event<C, E>>,
}
