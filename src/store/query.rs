//! Historical query API: reconstruct_at() and EventRangeIterator.
//!
//! - `reconstruct_at()`: reconstruct state at any historical block height
//!   using a 4-tier cost hierarchy (snapshot → cached replay → fetch → fetch_state).
//! - `EventRangeIterator`: lazy paginated iterator over events in a height range.

use std::sync::Arc;

use crate::coordinator::Codec;
use crate::coordinator::EngineError;
use crate::cursor::ChainCursor;
use crate::event::Event;
use crate::machine::StateMachine;
use crate::storage::Storage;

/// Reconstruct state at a given block height using the cheapest available path.
///
/// Cost hierarchy (tiers):
/// 1. Exact snapshot at target block — return directly (0 replay)
/// 2. Nearest snapshot + cached event replay to target (0 RPC)
/// 3. No snapshot — replay from genesis with initial_state (0 RPC, full replay)
///
/// Note: Tier 3 (fetch missing events from source) and Tier 4 (fetch_state)
/// require an async EventSource reference. This synchronous version covers
/// tiers 1-3 using only storage. An async version with RPC fallback can be
/// added later.
pub fn reconstruct_at<SM, S, C>(
    _storage: &S,
    _codec: &C,
    _sm: &SM,
    _initial_state: &SM::State,
    _source_id: &str,
    _target_height: u64,
) -> Result<SM::State, EngineError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    todo!("reconstruct_at not yet implemented")
}

/// Lazy paginated iterator over events in a block height range.
///
/// Loads events in pages from storage via `load_events_by_height_range`,
/// decodes them via the Codec, and yields `(Cursor, Event)` pairs.
/// The storage lock is released between page loads.
pub struct EventRangeIterator<S: Storage, C: Codec> {
    _storage: Arc<S>,
    _codec: C,
    _source_id: String,
    _from_height: u64,
    _to_height: u64,
    _page_size: usize,
    _offset: usize,
    _buffer: Vec<(C::Cursor, C::Event)>,
    _buffer_pos: usize,
    _exhausted: bool,
}

impl<S: Storage, C: Codec> EventRangeIterator<S, C> {
    pub fn new(
        _storage: Arc<S>,
        _codec: C,
        _source_id: String,
        _from_height: u64,
        _to_height: u64,
        _page_size: usize,
    ) -> Self {
        todo!("EventRangeIterator::new not yet implemented")
    }
}

impl<S: Storage, C: Codec> Iterator for EventRangeIterator<S, C> {
    type Item = (C::Cursor, C::Event);

    fn next(&mut self) -> Option<Self::Item> {
        todo!("EventRangeIterator::next not yet implemented")
    }
}
