// Recovery pipeline for block DAG model.
//
// Implements the three-phase crash recovery:
// 1. Warm recovery (tip + snapshot): load snapshot, replay forward, fetch gap
// 2. Warm recovery (tip, no snapshot): replay all stored blocks from genesis, fetch gap
// 3. Cold start (empty DB): engine has initial state from construction, fetch all blocks

use crate::coordinator::{Codec, Engine};
use crate::event::Event;
use crate::machine::StateMachine;
use crate::source::EventSource;
use crate::storage::Storage;

// ============================================================
// Error type
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync>),
    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("codec error: failed to decode state from snapshot")]
    DecodeState,
}

// ============================================================
// Public API
// ============================================================

/// Run the full block DAG recovery pipeline.
///
/// Sequence:
/// 1. Read canonical_tip from storage
/// 2. If tip exists (warm recovery):
///    a. Find nearest snapshot — load state if found, else keep initial state
///    b. Replay stored events from snapshot block (or genesis) to tip
///    c. Fetch gap blocks from source if tip is behind live_tip_height
/// 3. If no tip (cold start):
///    - Engine already holds initial state from construction
///    - Fetch all blocks from source for height range [0, live_tip_height]
///
/// After recovery, Engine state is consistent with live_tip_height.
///
/// Note: The Engine must be created with the correct initial state before calling
/// recover. Recovery does NOT call EventSource::fetch_state — the caller provides
/// initial state via Engine construction.
pub async fn recover<SM, S, ES, C>(
    engine: &mut Engine<SM, S, C>,
    source: &ES,
    live_tip_height: u64,
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let source_id = engine.source_id().to_string();

    // Step 1: Read canonical tip.
    let tip_hash = engine
        .storage()
        .get_canonical_tip(&source_id)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    match tip_hash {
        None => {
            // Cold start: empty DB. Engine already has initial state from construction.
            // Fetch all blocks from source and process them through Engine.
            fetch_gap_blocks(engine, source, 0, live_tip_height).await?;
        }
        Some(tip) => {
            // Warm recovery: we have stored blocks.
            let tip_height = engine
                .storage()
                .get_block_height(&tip)
                .map_err(|e| RecoveryError::Storage(Box::new(e)))?
                .unwrap_or(0);

            // Step 2: Try to find a snapshot on the canonical chain.
            let snapshot_result = engine
                .storage()
                .find_nearest_snapshot(&source_id, &tip)
                .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

            match snapshot_result {
                Some((snap_hash, snap_payload)) => {
                    // Load snapshot state into engine (replaces initial state).
                    let state = engine
                        .codec()
                        .decode_state(&snap_payload)
                        .ok_or(RecoveryError::DecodeState)?;
                    engine.replace_state(state);

                    // Replay events for blocks AFTER the snapshot, up to the tip.
                    // If snap_hash == tip, no replay needed (snapshot IS the tip).
                    if snap_hash != tip {
                        replay_chain(engine, &snap_hash, &tip)?;
                    }
                }
                None => {
                    // No snapshot: engine has initial state from construction.
                    // Replay ALL stored blocks from genesis to tip.
                    replay_full_chain(engine, &tip)?;
                }
            }

            // Step 3: Fetch gap blocks from source if behind live tip.
            if tip_height < live_tip_height {
                fetch_gap_blocks(engine, source, tip_height + 1, live_tip_height).await?;
            }
        }
    }

    Ok(())
}

// ============================================================
// Internal helpers
// ============================================================

/// Replay stored events for blocks on the canonical chain from snap_hash (exclusive)
/// to tip_hash (inclusive), in forward order.
///
/// Called after loading a snapshot — replays the "tail" of the chain from snapshot
/// to tip so the state machine is caught up to the canonical tip.
fn replay_chain<SM, S, C>(
    engine: &mut Engine<SM, S, C>,
    from_hash: &[u8; 32],
    to_hash: &[u8; 32],
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    let chain = engine
        .storage()
        .collect_canonical_chain(from_hash, to_hash)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    for block_hash in chain {
        replay_block_events(engine, &block_hash)?;
    }
    Ok(())
}

/// Replay all blocks from genesis to tip_hash (no snapshot available).
///
/// Uses the all-zeros genesis sentinel ([0u8; 32]) as the exclusive lower bound
/// for collect_canonical_chain. Since genesis blocks have parent_hash = [0u8; 32],
/// the walk will stop when it reaches genesis's parent (the sentinel), and genesis
/// itself will be included in the result. This correctly replays all stored blocks.
fn replay_full_chain<SM, S, C>(
    engine: &mut Engine<SM, S, C>,
    tip_hash: &[u8; 32],
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    // [0u8; 32] = genesis sentinel (exclusive lower bound).
    // collect_canonical_chain([0;32], tip) walks from tip backward to genesis,
    // reverses to forward order. Genesis IS included because its parent [0;32]
    // equals the sentinel and the walk terminates on the sentinel.
    let all_zeros = [0u8; 32];
    let chain = engine
        .storage()
        .collect_canonical_chain(&all_zeros, tip_hash)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    for block_hash in chain {
        replay_block_events(engine, &block_hash)?;
    }
    Ok(())
}

/// Decode and replay events for a single block through the engine WITHOUT writing to storage.
///
/// Loads raw (cursor_bytes, payload_bytes) from storage, decodes each event via Codec,
/// and calls engine.replay_events — which applies events to the state machine without
/// triggering storage writes or snapshot counters.
fn replay_block_events<SM, S, C>(
    engine: &mut Engine<SM, S, C>,
    block_hash: &[u8; 32],
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    // Load raw bytes first (immutable borrow of engine released after collect).
    let stored_events = engine
        .storage()
        .load_block_events(block_hash)
        .map_err(|e| RecoveryError::Storage(Box::new(e)))?;

    // Decode events from stored bytes.
    // cursor_bytes: serialized via postcard (library-internal format).
    // payload_bytes: serialized via Codec (client-provided).
    let events: Vec<Event<SM::Cursor, SM::Event>> = stored_events
        .iter()
        .filter_map(|(cursor_bytes, payload_bytes)| {
            let cursor: SM::Cursor = postcard::from_bytes(cursor_bytes).ok()?;
            let data: SM::Event = engine.codec().decode_event(payload_bytes)?;
            Some(Event { cursor, data })
        })
        .collect();

    // Replay through ReducerCore (state advance, NO storage write).
    engine.replay_events(&events);
    Ok(())
}

/// Fetch gap blocks from source and process them through the engine's write-first pipeline.
///
/// Fetches blocks in the inclusive height range [from_height, to_height].
/// Uses chunked pagination: each call to fetch_blocks may return fewer blocks than
/// requested; the loop advances by the last returned block's actual height + 1 to
/// avoid gaps or duplicates (per gap-stitching requirement INGEST-02).
///
/// Each fetched block is processed through engine.process_block, which:
/// - Writes block + events to storage (write-first invariant)
/// - Applies events to the state machine
/// - Triggers a snapshot if the snapshot threshold is crossed
///
/// Blocks already in storage (duplicates) are silently skipped by process_block.
///
/// pub(crate): exposed so Engine::run() in Plan 03 can call this directly.
pub(crate) async fn fetch_gap_blocks<SM, S, ES, C>(
    engine: &mut Engine<SM, S, C>,
    source: &ES,
    from_height: u64,
    to_height: u64,
) -> Result<(), RecoveryError>
where
    SM: StateMachine,
    S: Storage,
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    if from_height > to_height {
        return Ok(());
    }

    let mut current_height = from_height;

    loop {
        if current_height > to_height {
            break;
        }

        // fetch_blocks is half-open [from, to) so we pass to_height + 1.
        let blocks = source
            .fetch_blocks(current_height, to_height + 1)
            .await
            .map_err(|e| RecoveryError::Source(Box::new(e)))?;

        if blocks.is_empty() {
            // Source has no more blocks in this range — gap is fully fetched.
            break;
        }

        for block in &blocks {
            engine
                .process_block(block)
                .map_err(|e| RecoveryError::Storage(Box::new(e)))?;
        }

        // Advance by the last block's actual height + 1, not by block count.
        // This is critical: blocks may not be sequential by index in the slice.
        let last_height = blocks.last().unwrap().height;
        current_height = last_height + 1;
    }

    Ok(())
}
