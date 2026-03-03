/// Engine coordinator — owns ReducerCore + Storage, orchestrates write-first event processing,
/// periodic snapshot creation, and the crash-recovery pipeline.
///
/// # Write-first invariant
///
/// `process_events` writes to Storage BEFORE applying to ReducerCore. This ensures that
/// on crash, stored events can be replayed without loss even if the in-memory state was
/// not persisted.
///
/// # Two distinct code paths
///
/// - `process_events`: write to storage THEN apply to ReducerCore (for new live events)
/// - `replay_stored_events`: apply to ReducerCore ONLY, no storage write (for recovery replay)
///
/// Mixing these would violate either the write-first invariant or the no-double-write
/// invariant for recovery. Keep them separate.
pub mod recovery;

use crate::cursor::ChainCursor;
use crate::event::Event;
use crate::machine::{ReducerCore, StateMachine};
use crate::storage::{Anchor, Snapshot, Storage, StoredEvent};
use tokio::sync::watch;

// ============================================================
// Codec trait — bridges typed events/state to opaque storage bytes
// ============================================================

/// Bridges between the generic typed domain (SM::Event, SM::State, SM::Cursor)
/// and the opaque byte representation used by Storage.
///
/// The Codec is provided by the client alongside their StateMachine implementation.
/// The Engine does not know how to serialize or deserialize events or state —
/// all of that is delegated to the Codec.
pub trait Codec: Send + Sync + 'static {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type State: Clone + Send + 'static;

    /// Convert a typed Event to storage representation: (cursor_major, cursor_minor, payload_bytes).
    fn encode_event(&self, event: &Event<Self::Cursor, Self::Event>) -> (i64, i64, Vec<u8>);

    /// Convert a StoredEvent back to a typed Event. Returns None for corrupt/incompatible payloads.
    fn decode_event(
        &self,
        stored: &StoredEvent,
    ) -> Option<Event<Self::Cursor, Self::Event>>;

    /// Serialize state to opaque bytes for snapshot storage.
    fn encode_state(&self, state: &Self::State) -> Vec<u8>;

    /// Deserialize state from opaque bytes. Returns None for corrupt/incompatible snapshots.
    fn decode_state(&self, bytes: &[u8]) -> Option<Self::State>;

    /// Convert a cursor to (major, minor) i64 tuple for storage.
    fn cursor_to_tuple(&self, cursor: &Self::Cursor) -> (i64, i64);

    /// Convert (major, minor) i64 tuple back to a cursor.
    fn tuple_to_cursor(&self, major: i64, minor: i64) -> Self::Cursor;
}

// ============================================================
// EngineConfig
// ============================================================

/// Configuration for the Engine coordinator.
pub struct EngineConfig {
    /// Logical source identifier used as a key in all Storage calls.
    pub source_id: String,
    /// Create a snapshot after this many distinct (non-duplicate) events are inserted.
    pub snapshot_every_n: usize,
}

// ============================================================
// Engine
// ============================================================

/// Coordinator that owns ReducerCore + Storage + Codec.
///
/// Drives state forward with the write-first pipeline:
/// 1. Write events to Storage (dedup via INSERT OR IGNORE)
/// 2. Update anchor to last event's cursor
/// 3. Update coverage interval
/// 4. Apply events to ReducerCore (state advances)
/// 5. Trigger snapshot if distinct insert count crosses threshold
pub struct Engine<SM, S, C>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    core: ReducerCore<SM>,
    storage: S,
    codec: C,
    config: EngineConfig,
    /// Running count of distinct events inserted since last snapshot.
    events_since_snapshot: usize,
}

impl<SM, S, C> Engine<SM, S, C>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    /// Create a new Engine with the given components.
    pub fn new(core: ReducerCore<SM>, storage: S, codec: C, config: EngineConfig) -> Self {
        Self {
            core,
            storage,
            codec,
            config,
            events_since_snapshot: 0,
        }
    }

    /// Process a batch of new events using the write-first pipeline.
    ///
    /// Returns the number of events actually inserted into storage (duplicates are dropped).
    ///
    /// Steps:
    /// 1. Encode events to StoredEvents via codec
    /// 2. Write to storage (dedup applied at storage boundary)
    /// 3. Update anchor to last event's cursor
    /// 4. Update coverage interval for the batch range
    /// 5. Apply each event to ReducerCore (state advances)
    /// 6. Increment events_since_snapshot by actual insert count
    /// 7. Trigger snapshot if threshold crossed
    pub fn process_events(
        &mut self,
        events: &[Event<SM::Cursor, SM::Event>],
    ) -> Result<usize, S::Error> {
        if events.is_empty() {
            return Ok(0);
        }

        // Step 1: Encode events to storage representation
        let stored_events: Vec<StoredEvent> = events
            .iter()
            .map(|e| {
                let (major, minor, payload) = self.codec.encode_event(e);
                StoredEvent {
                    source_id: self.config.source_id.clone(),
                    cursor_major: major,
                    cursor_minor: minor,
                    payload,
                }
            })
            .collect();

        // Step 2: Write to storage — returns actual insert count (dups dropped)
        let inserted = self.storage.write_events(&self.config.source_id, &stored_events)?;

        // Step 3: Update anchor to last event's cursor
        let last_event = events.last().unwrap(); // safe: events is non-empty
        let (last_major, last_minor, _) = self.codec.encode_event(last_event);
        let anchor = Anchor {
            source_id: self.config.source_id.clone(),
            name: "last_processed".to_string(),
            cursor_major: last_major,
            cursor_minor: last_minor,
        };
        self.storage.write_anchor(&anchor)?;

        // Step 4: Update coverage interval for the batch range
        let first_event = events.first().unwrap(); // safe: events is non-empty
        let (first_major, first_minor, _) = self.codec.encode_event(first_event);
        // Coverage range: [first_cursor, just-past-last_cursor)
        // We use last_cursor as the end of the coverage range (exclusive end = last + epsilon)
        // For simplicity we use (last_major, last_minor + 1) as the exclusive end
        let coverage_end = (last_major, last_minor + 1);
        self.storage.add_coverage(
            &self.config.source_id,
            (first_major, first_minor),
            coverage_end,
        )?;

        // Step 5: Apply each event to ReducerCore (AFTER storage write — write-first invariant)
        for event in events {
            self.core.apply(event);
        }

        // Step 6: Accumulate insert count for snapshot triggering
        self.events_since_snapshot += inserted;

        // Step 7: Trigger snapshot if threshold crossed
        if inserted > 0 && self.events_since_snapshot >= self.config.snapshot_every_n {
            self.create_snapshot(last_major, last_minor)?;
            self.events_since_snapshot = 0;
        }

        Ok(inserted)
    }

    /// Replay stored events through ReducerCore WITHOUT writing to storage.
    ///
    /// Used exclusively during the recovery pipeline to restore in-memory state
    /// from already-persisted events. Must not be called for new live events.
    pub fn replay_stored_events(&mut self, events: &[Event<SM::Cursor, SM::Event>]) {
        for event in events {
            self.core.apply(event);
        }
    }

    /// Replay stored events from raw StoredEvents (decoded via codec).
    ///
    /// Convenience method used by the recovery pipeline which has StoredEvents from storage.
    /// Skips events that fail to decode (logs nothing — caller is responsible for error handling).
    pub fn replay_from_storage(&mut self, stored: &[StoredEvent]) {
        for se in stored {
            if let Some(event) = self.codec.decode_event(se) {
                self.core.apply(&event);
            }
        }
    }

    /// Subscribe to state changes via a watch::Receiver.
    pub fn subscribe(&self) -> watch::Receiver<SM::State> {
        self.core.subscribe()
    }

    /// Get the current state (clones the state).
    pub fn state(&self) -> SM::State {
        self.core.subscribe().borrow().clone()
    }

    /// Get a reference to the underlying storage.
    ///
    /// Used in tests to inspect storage state directly.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get a reference to the codec.
    pub fn codec(&self) -> &C {
        &self.codec
    }

    /// Get the source_id from config.
    pub fn source_id(&self) -> &str {
        &self.config.source_id
    }

    /// Replace the engine's current state with the given value.
    ///
    /// Used exclusively by the recovery pipeline to restore state from a snapshot.
    /// Broadcasts the new state to all watch::Receiver subscribers immediately.
    pub fn replace_state(&mut self, state: SM::State) {
        self.core.reset_state(state);
    }

    // ---- Private helpers ----

    fn create_snapshot(
        &mut self,
        cursor_major: i64,
        cursor_minor: i64,
    ) -> Result<(), S::Error> {
        let state = self.state();
        let payload = self.codec.encode_state(&state);
        let snapshot = Snapshot {
            source_id: self.config.source_id.clone(),
            cursor_major,
            cursor_minor,
            payload,
            created_at: current_unix_epoch(),
        };
        self.storage.write_snapshot(&snapshot)
    }
}

/// Return current Unix epoch as i64 seconds.
fn current_unix_epoch() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
