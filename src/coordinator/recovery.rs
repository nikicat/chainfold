/// Recovery pipeline for the Engine coordinator.
///
/// Sequence: load snapshot -> replay cached events -> gap fetch -> ready
///
/// See Task 2 for full implementation.
use crate::coordinator::{Codec, Engine};
use crate::machine::StateMachine;
use crate::source::EventSource;
use crate::storage::Storage;

// ============================================================
// RecoveryError
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync>),
    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("no valid snapshot found and fetch_state failed")]
    NoValidState,
    #[error("codec error: failed to decode event")]
    DecodeEvent,
}

// ============================================================
// recover — main entry point
// ============================================================

/// Run the full recovery pipeline:
///
/// 1. Load latest valid snapshot (with fallback to older snapshots or fetch_state)
/// 2. Replay cached events from snapshot cursor to cached tip (no storage writes)
/// 3. Fetch gap from cached tip to live_tip via EventSource (with storage writes)
///
/// On success, the Engine is in a state consistent with `live_tip`.
pub async fn recover<SM, S, ES, C>(
    engine: &mut Engine<SM, S, C>,
    source: &ES,
    live_tip: &SM::Cursor,
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    // Step 1: Load snapshot and rebuild ReducerCore with its state
    let snapshot_cursor_tuple = load_snapshot(engine, source).await?;

    // Step 2: Replay cached events from snapshot_cursor to cached_tip
    let cached_tip_tuple = replay_cached_events(engine, snapshot_cursor_tuple)?;

    // Step 3: Fetch gap from cached tip to live_tip
    let gap_start_tuple = cached_tip_tuple.unwrap_or(snapshot_cursor_tuple);
    fetch_gap(engine, source, gap_start_tuple, live_tip).await?;

    Ok(())
}

// ============================================================
// Step 1: Load snapshot
// ============================================================

/// Load the latest valid snapshot into the engine's ReducerCore.
///
/// Returns the cursor tuple of the loaded snapshot (the start point for event replay).
///
/// Fallback order:
/// 1. Latest snapshot — try codec.decode_state
/// 2. Older snapshots (snapshots_before) — try each in order
/// 3. EventSource::fetch_state at genesis cursor (0, 0)
async fn load_snapshot<SM, S, ES, C>(
    engine: &mut Engine<SM, S, C>,
    source: &ES,
) -> Result<(i64, i64), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let source_id = engine.source_id().to_string();

    // Try latest snapshot first
    let latest = engine
        .storage()
        .latest_snapshot(&source_id)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    if let Some(snap) = &latest {
        if let Some(state) = engine.codec().decode_state(&snap.payload) {
            let cursor_tuple = (snap.cursor_major, snap.cursor_minor);
            engine.replace_state(state);
            return Ok(cursor_tuple);
        }

        // Latest snapshot corrupt — try older ones
        let cursor = (snap.cursor_major, snap.cursor_minor);
        let older = engine
            .storage()
            .snapshots_before(&source_id, cursor)
            .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

        for older_snap in &older {
            if let Some(state) = engine.codec().decode_state(&older_snap.payload) {
                let cursor_tuple = (older_snap.cursor_major, older_snap.cursor_minor);
                engine.replace_state(state);
                return Ok(cursor_tuple);
            }
        }
    }

    // No valid snapshots — fetch initial state from source
    let genesis_cursor = engine.codec().tuple_to_cursor(0, 0);
    let state_bytes = source
        .fetch_state(&genesis_cursor)
        .await
        .map_err(|e| RecoveryError::Source(Box::new(e)))?;

    let state = engine
        .codec()
        .decode_state(&state_bytes)
        .ok_or(RecoveryError::NoValidState)?;

    engine.replace_state(state);
    Ok((0, 0))
}

// ============================================================
// Step 2: Replay cached events
// ============================================================

/// Replay events from storage between snapshot_cursor and cached_tip.
///
/// Returns the cursor tuple of the last replayed event, or None if no cached events exist.
/// CRITICAL: This calls replay_from_storage — no storage writes occur.
fn replay_cached_events<SM, S, C>(
    engine: &mut Engine<SM, S, C>,
    from_cursor: (i64, i64),
) -> Result<Option<(i64, i64)>, RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let source_id = engine.source_id().to_string();

    // Get the cached tip to know our range
    let cached_tip = engine
        .storage()
        .cached_tip(&source_id)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    let Some(tip) = cached_tip else {
        return Ok(None);
    };

    if tip <= from_cursor {
        return Ok(None);
    }

    // Fetch events in range [from_cursor, tip)
    let stored_events = engine
        .storage()
        .events_in_range(&source_id, from_cursor, tip)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    if stored_events.is_empty() {
        return Ok(None);
    }

    let last_cursor = stored_events
        .last()
        .map(|e| (e.cursor_major, e.cursor_minor));

    // Replay without storage writes
    engine.replay_from_storage(&stored_events);

    Ok(last_cursor)
}

// ============================================================
// Step 3: Fetch gap
// ============================================================

/// Fetch events in the gap [gap_start, live_tip) from the EventSource.
///
/// Calls fetch_events in a loop with advancing cursors until caught up.
/// Each chunk is passed to engine.process_events — writes to storage and applies to state.
async fn fetch_gap<SM, S, ES, C>(
    engine: &mut Engine<SM, S, C>,
    source: &ES,
    gap_start: (i64, i64),
    live_tip: &SM::Cursor,
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let live_tip_tuple = engine.codec().cursor_to_tuple(live_tip);

    if gap_start >= live_tip_tuple {
        // No gap to fetch
        return Ok(());
    }

    let mut current_start = gap_start;

    loop {
        let from_cursor = engine.codec().tuple_to_cursor(current_start.0, current_start.1);

        let chunk = source
            .fetch_events(&from_cursor, live_tip)
            .await
            .map_err(|e| RecoveryError::Source(Box::new(e)))?;

        if chunk.is_empty() {
            break; // gap closed
        }

        // Advance cursor to just past the last event in the chunk
        let last = chunk.last().unwrap(); // safe: chunk is non-empty
        let last_tuple = engine.codec().cursor_to_tuple(&last.cursor);

        engine
            .process_events(&chunk)
            .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

        // Next iteration starts just after the last fetched event
        current_start = (last_tuple.0, last_tuple.1 + 1);

        if current_start >= live_tip_tuple {
            break; // caught up
        }
    }

    Ok(())
}
