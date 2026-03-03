/// Tests for the Engine coordinator — write-first event processing, snapshot triggering,
/// anchor updates, deduplication counting, and state accessibility via watch::Receiver.
use std::sync::Arc;

use chainfold::coordinator::{Codec, Engine, EngineConfig};
use chainfold::cursor::CexCursor;
use chainfold::event::Event;
use chainfold::machine::{ReducerCore, StateMachine};
use chainfold::storage::{InMemoryStorage, Storage, StoredEvent};

// ============================================================
// Mock state machine: sums event values
// ============================================================

struct CounterMachine;

impl StateMachine for CounterMachine {
    type State = u64;
    type Event = u64; // event payload is a value to add
    type Cursor = CexCursor;

    fn apply(&self, state: u64, event: &Event<CexCursor, u64>) -> u64 {
        state + event.data
    }
}

// ============================================================
// Mock codec: CexCursor <-> (major=seq_id, minor=0), event payload = u64 LE bytes
// ============================================================

struct CounterCodec;

impl Codec for CounterCodec {
    type Cursor = CexCursor;
    type Event = u64;
    type State = u64;

    fn encode_event(&self, event: &Event<CexCursor, u64>) -> (i64, i64, Vec<u8>) {
        let major = event.cursor.seq_id as i64;
        let minor = 0i64;
        let payload = event.data.to_le_bytes().to_vec();
        (major, minor, payload)
    }

    fn decode_event(&self, stored: &StoredEvent) -> Option<Event<CexCursor, u64>> {
        let bytes: [u8; 8] = stored.payload.as_slice().try_into().ok()?;
        let data = u64::from_le_bytes(bytes);
        let cursor = CexCursor {
            seq_id: stored.cursor_major as u64,
            timestamp_ms: 0,
        };
        Some(Event {
            cursor,
            data,
            block_hash: None,
        })
    }

    fn encode_state(&self, state: &u64) -> Vec<u8> {
        state.to_le_bytes().to_vec()
    }

    fn decode_state(&self, bytes: &[u8]) -> Option<u64> {
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(u64::from_le_bytes(arr))
    }

    fn cursor_to_tuple(&self, cursor: &CexCursor) -> (i64, i64) {
        (cursor.seq_id as i64, 0)
    }

    fn tuple_to_cursor(&self, major: i64, _minor: i64) -> CexCursor {
        CexCursor {
            seq_id: major as u64,
            timestamp_ms: 0,
        }
    }
}

// ============================================================
// Helper: make a batch of events with values 1..=n starting at seq_id `start`
// ============================================================
fn make_events(start: u64, count: u64) -> Vec<Event<CexCursor, u64>> {
    (0..count)
        .map(|i| Event {
            cursor: CexCursor {
                seq_id: start + i,
                timestamp_ms: 0,
            },
            data: 1u64, // each event contributes 1
            block_hash: None,
        })
        .collect()
}

// ============================================================
// Test: write-first invariant — events appear in storage before state changes
// ============================================================
#[test]
fn test_write_first_invariant() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 100,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    let events = make_events(0, 3);
    let inserted = engine.process_events(&events).unwrap();

    // All 3 events stored
    assert_eq!(inserted, 3);

    // Verify storage has all 3 events
    let stored = engine.storage().events_in_range(
        "test_src",
        (0, 0),
        (100, 0),
    ).unwrap();
    assert_eq!(stored.len(), 3, "events must be in storage");

    // State should reflect the 3 events (each adds 1)
    assert_eq!(engine.state(), 3u64);
}

// ============================================================
// Test: actual insert count for snapshot triggering (not batch size)
// ============================================================
#[test]
fn test_insert_count_for_snapshot() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 5,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    // Send 3 new events + 2 duplicates (seq_ids 0,1 repeated)
    let events_batch1 = make_events(0, 3); // seq 0,1,2 — all new, insert count = 3
    engine.process_events(&events_batch1).unwrap();
    // No snapshot yet (3 < 5)
    assert!(engine.storage().latest_snapshot("test_src").unwrap().is_none());

    // Send overlapping batch: seq 1,2 (duplicates) + seq 3,4 (new) = 2 inserts
    let events_batch2 = make_events(1, 4); // seq 1,2,3,4 — 1,2 are dups, 3,4 are new
    engine.process_events(&events_batch2).unwrap();
    // Total inserted: 3 + 2 = 5 — snapshot should be triggered
    assert!(
        engine.storage().latest_snapshot("test_src").unwrap().is_some(),
        "snapshot should be triggered after 5 actual inserts"
    );
}

// ============================================================
// Test: snapshot created after exactly N distinct events
// ============================================================
#[test]
fn test_snapshot_after_n_events() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 5,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    // Process 4 events — no snapshot
    engine.process_events(&make_events(0, 4)).unwrap();
    assert!(
        engine.storage().latest_snapshot("test_src").unwrap().is_none(),
        "no snapshot before N events"
    );

    // Process 1 more — snapshot triggered
    engine.process_events(&make_events(4, 1)).unwrap();
    assert!(
        engine.storage().latest_snapshot("test_src").unwrap().is_some(),
        "snapshot triggered at exactly N events"
    );
}

// ============================================================
// Test: no snapshot when all events in batch are duplicates
// ============================================================
#[test]
fn test_no_snapshot_on_all_duplicates() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 3,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    // First pass: insert 2 events
    engine.process_events(&make_events(0, 2)).unwrap();
    assert!(engine.storage().latest_snapshot("test_src").unwrap().is_none());

    // Second pass: same 2 events again — all duplicates, no snapshot
    engine.process_events(&make_events(0, 2)).unwrap();
    assert!(
        engine.storage().latest_snapshot("test_src").unwrap().is_none(),
        "no snapshot when all events are duplicates (insert count = 0)"
    );
}

// ============================================================
// Test: anchor updated with last event's cursor
// ============================================================
#[test]
fn test_anchor_updated_atomically() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 100,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    engine.process_events(&make_events(0, 5)).unwrap();

    let anchor = engine.storage()
        .read_anchor("test_src", "last_processed")
        .unwrap();
    assert!(anchor.is_some(), "anchor must be set after processing events");
    let anchor = anchor.unwrap();
    // Last event is seq_id 4
    assert_eq!(anchor.cursor_major, 4, "anchor at last event's cursor");
    assert_eq!(anchor.cursor_minor, 0);
}

// ============================================================
// Test: state accessible via watch::Receiver
// ============================================================
#[test]
fn test_state_accessible_via_watch() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 100,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    let rx = engine.subscribe();
    assert_eq!(*rx.borrow(), 0u64);

    engine.process_events(&make_events(0, 7)).unwrap();
    assert_eq!(*rx.borrow(), 7u64, "watch receiver reflects state after processing");
    assert_eq!(engine.state(), 7u64);
}

// ============================================================
// Test: replay_stored_events does NOT write to storage
// ============================================================
#[test]
fn test_replay_does_not_write_to_storage() {
    let storage = InMemoryStorage::new();
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let codec = CounterCodec;
    let config = EngineConfig {
        source_id: "test_src".to_string(),
        snapshot_every_n: 100,
    };

    let mut engine = Engine::new(core, storage, codec, config);

    // First write some events normally
    engine.process_events(&make_events(0, 3)).unwrap();
    let count_after_write = engine
        .storage()
        .events_in_range("test_src", (0, 0), (100, 0))
        .unwrap()
        .len();
    assert_eq!(count_after_write, 3);

    // Now replay the same events — should NOT add more storage entries
    let events = make_events(0, 3);
    engine.replay_stored_events(&events);

    let count_after_replay = engine
        .storage()
        .events_in_range("test_src", (0, 0), (100, 0))
        .unwrap()
        .len();
    assert_eq!(
        count_after_replay, count_after_write,
        "replay must not write to storage"
    );

    // But state should advance (each replay adds to sum)
    assert_eq!(engine.state(), 6u64, "replay advances state");
}
