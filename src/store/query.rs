//! Historical query API: reconstruct_at() and EventRangeIterator.
//!
//! - `reconstruct_at()`: reconstruct state at any historical block height
//!   using a multi-tier cost hierarchy (snapshot -> cached replay -> full replay).
//! - `EventRangeIterator`: lazy paginated iterator over events in a height range.

use std::sync::Arc;

use crate::coordinator::Codec;
use crate::coordinator::EngineError;
use crate::event::Event;
use crate::machine::StateMachine;
use crate::storage::Storage;

/// The all-zeros hash is the genesis sentinel.
const ZERO_HASH: [u8; 32] = [0u8; 32];

/// Reconstruct state at a given block height using the cheapest available path.
///
/// Cost hierarchy (tiers):
/// 1. Exact snapshot at target block — decode and return directly
/// 2. Nearest snapshot before target + replay cached events to target
/// 3. No snapshot — replay all events from genesis using initial_state
///
/// The caller provides `initial_state` as the genesis state for tier 3.
///
/// Returns `Err` on storage errors or if the target height is not on the
/// canonical chain.
pub fn reconstruct_at<SM, S, C>(
    storage: &S,
    codec: &C,
    sm: &SM,
    initial_state: &SM::State,
    source_id: &str,
    target_height: u64,
) -> Result<SM::State, EngineError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    // Get canonical tip.
    let tip_hash = storage
        .get_canonical_tip(source_id)
        .map_err(|e| EngineError::Storage(Box::new(e)))?
        .ok_or_else(|| EngineError::Storage("no canonical tip".into()))?;

    // Find the block hash at target_height by walking canonical chain backward from tip.
    let target_hash = find_block_at_height(storage, &tip_hash, target_height)?;

    // Try to find the nearest snapshot at or before target_hash on the canonical chain.
    let snapshot = storage
        .find_nearest_snapshot(source_id, &target_hash)
        .map_err(|e| EngineError::Storage(Box::new(e)))?;

    match snapshot {
        Some((snap_hash, snap_payload)) => {
            // Decode snapshot state.
            let mut state = codec
                .decode_state(&snap_payload)
                .ok_or(EngineError::DecodeState)?;

            if snap_hash == target_hash {
                // Tier 1: exact snapshot at target — return directly.
                return Ok(state);
            }

            // Tier 2: replay from snapshot to target.
            let chain = storage
                .collect_canonical_chain(&snap_hash, &target_hash)
                .map_err(|e| EngineError::Storage(Box::new(e)))?;

            for block_hash in chain {
                let events = load_and_decode_events::<SM, S, C>(storage, codec, &block_hash)?;
                for event in &events {
                    state = sm.apply(state, event);
                }
            }

            Ok(state)
        }
        None => {
            // Tier 3: no snapshot — replay from genesis.
            let mut state = initial_state.clone();

            let chain = storage
                .collect_canonical_chain(&ZERO_HASH, &target_hash)
                .map_err(|e| EngineError::Storage(Box::new(e)))?;

            for block_hash in chain {
                let events = load_and_decode_events::<SM, S, C>(storage, codec, &block_hash)?;
                for event in &events {
                    state = sm.apply(state, event);
                }
            }

            Ok(state)
        }
    }
}

/// Walk backward from `tip_hash` to find the block at `target_height`.
fn find_block_at_height<S: Storage>(
    storage: &S,
    tip_hash: &[u8; 32],
    target_height: u64,
) -> Result<[u8; 32], EngineError> {
    let mut current = *tip_hash;
    loop {
        let height = storage
            .get_block_height(&current)
            .map_err(|e| EngineError::Storage(Box::new(e)))?
            .ok_or_else(|| EngineError::Storage("block not found during height walk".into()))?;

        if height == target_height {
            return Ok(current);
        }

        if height == 0 || height < target_height {
            return Err(EngineError::Storage(
                format!(
                    "target height {} not found on canonical chain (reached height {})",
                    target_height, height
                )
                .into(),
            ));
        }

        let parent = storage
            .get_parent_hash(&current)
            .map_err(|e| EngineError::Storage(Box::new(e)))?
            .ok_or_else(|| EngineError::Storage("parent not found during height walk".into()))?;

        if parent == ZERO_HASH {
            return Err(EngineError::Storage(
                format!(
                    "reached genesis without finding target height {}",
                    target_height
                )
                .into(),
            ));
        }

        current = parent;
    }
}

/// Load events for a block and decode them via Codec.
fn load_and_decode_events<SM, S, C>(
    storage: &S,
    codec: &C,
    block_hash: &[u8; 32],
) -> Result<Vec<Event<SM::Cursor, SM::Event>>, EngineError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let stored = storage
        .load_block_events(block_hash)
        .map_err(|e| EngineError::Storage(Box::new(e)))?;

    let events: Vec<Event<SM::Cursor, SM::Event>> = stored
        .iter()
        .filter_map(|(cursor_bytes, payload_bytes)| {
            let cursor: SM::Cursor = postcard::from_bytes(cursor_bytes).ok()?;
            let data: SM::Event = codec.decode_event(payload_bytes)?;
            Some(Event { cursor, data })
        })
        .collect();

    Ok(events)
}

/// Lazy paginated iterator over events in a block height range.
///
/// Loads events in pages from storage via `load_events_by_height_range`,
/// decodes them via the Codec, and yields `(Cursor, Event)` pairs.
/// The storage lock is released between page loads so concurrent operations
/// are not blocked.
pub struct EventRangeIterator<S: Storage, C: Codec> {
    storage: Arc<S>,
    codec: C,
    source_id: String,
    from_height: u64,
    to_height: u64,
    page_size: usize,
    offset: usize,
    /// Buffer stored in reverse order so we can pop() from the back (O(1)).
    buffer: Vec<(C::Cursor, C::Event)>,
    exhausted: bool,
}

impl<S: Storage, C: Codec> EventRangeIterator<S, C> {
    /// Create a new iterator over events in `[from_height, to_height]`.
    ///
    /// Events are loaded in pages of `page_size` from storage. Each page load
    /// acquires and releases the storage lock independently.
    pub fn new(
        storage: Arc<S>,
        codec: C,
        source_id: String,
        from_height: u64,
        to_height: u64,
        page_size: usize,
    ) -> Self {
        Self {
            storage,
            codec,
            source_id,
            from_height,
            to_height,
            page_size,
            offset: 0,
            buffer: Vec::new(),
            exhausted: false,
        }
    }

    /// Load the next page of events from storage into the buffer.
    /// Buffer is stored reversed so pop() yields items in forward order.
    fn load_next_page(&mut self) -> bool {
        let raw_events = match self.storage.load_events_by_height_range(
            &self.source_id,
            self.from_height,
            self.to_height,
            self.page_size,
            self.offset,
        ) {
            Ok(events) => events,
            Err(_) => {
                self.exhausted = true;
                return false;
            }
        };

        if raw_events.is_empty() {
            self.exhausted = true;
            return false;
        }

        self.offset += raw_events.len();
        self.buffer.clear();

        for (cursor_bytes, payload_bytes) in &raw_events {
            if let (Some(cursor), Some(event)) = (
                postcard::from_bytes::<C::Cursor>(cursor_bytes).ok(),
                self.codec.decode_event(payload_bytes),
            ) {
                self.buffer.push((cursor, event));
            }
        }

        // Reverse so pop() yields in forward (height-ascending) order.
        self.buffer.reverse();

        !self.buffer.is_empty()
    }
}

impl<S: Storage, C: Codec> Iterator for EventRangeIterator<S, C> {
    type Item = (C::Cursor, C::Event);

    fn next(&mut self) -> Option<Self::Item> {
        // If buffer has items, pop from back (which is the next in forward order).
        if let Some(item) = self.buffer.pop() {
            return Some(item);
        }

        if self.exhausted {
            return None;
        }

        // Load next page.
        if !self.load_next_page() {
            return None;
        }

        self.buffer.pop()
    }
}
