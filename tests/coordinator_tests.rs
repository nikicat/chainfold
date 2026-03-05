/// Integration tests for the Engine coordinator.
///
/// Tests the write-first pipeline: receive Block -> write to storage -> apply events
/// to ReducerCore -> trigger snapshot if threshold crossed.
///
/// Uses a simple CounterMachine that sums u64 event values.
use std::sync::Arc;

use chainfold::storage::InMemoryStorage;
use chainfold::storage::Storage;
use chainfold::{Block, Codec, Engine, EngineConfig, EngineError, EngineStatus, ReorgEvent, Event, ReducerCore, StateMachine};

// ============================================================
// Test helpers: CounterMachine + TestCodec
// ============================================================

/// A minimal cursor for tests: just a u64 sequence number.
/// Uses u64's derived Ord, Clone, Serialize, Deserialize via serde.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct TestCursor(u64);

/// A simple state machine that sums u64 event values.
struct CounterMachine;

impl StateMachine for CounterMachine {
    type State = u64;
    type Event = u64;
    type Cursor = TestCursor;

    fn apply(&self, state: u64, event: &Event<TestCursor, u64>) -> u64 {
        state + event.data
    }
}

/// A codec that serializes u64 events and state as little-endian bytes.
struct TestCodec;

impl Codec for TestCodec {
    type Cursor = TestCursor;
    type Event = u64;
    type State = u64;

    fn encode_event(&self, event: &u64) -> Vec<u8> {
        event.to_le_bytes().to_vec()
    }

    fn decode_event(&self, payload: &[u8]) -> Option<u64> {
        if payload.len() == 8 {
            Some(u64::from_le_bytes(payload.try_into().unwrap()))
        } else {
            None
        }
    }

    fn encode_state(&self, state: &u64) -> Vec<u8> {
        state.to_le_bytes().to_vec()
    }

    fn decode_state(&self, bytes: &[u8]) -> Option<u64> {
        if bytes.len() == 8 {
            Some(u64::from_le_bytes(bytes.try_into().unwrap()))
        } else {
            None
        }
    }
}

/// Create a block hash where byte[0] = n and the rest are 0.
fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// Genesis hash: all-zeros (no parent).
const GENESIS: [u8; 32] = [0u8; 32];

/// Build a test block with the given events (each event value contributes to state sum).
fn make_block(
    hash: [u8; 32],
    parent: [u8; 32],
    height: u64,
    event_values: &[u64],
) -> Block<TestCursor, u64> {
    let events: Vec<Event<TestCursor, u64>> = event_values
        .iter()
        .enumerate()
        .map(|(i, &v)| Event {
            cursor: TestCursor(i as u64),
            data: v,
        })
        .collect();
    Block {
        block_hash: hash,
        parent_hash: parent,
        height,
        events,
    }
}

/// Create a default engine with InMemoryStorage and snapshot_every_n = 1000 (no snapshots by default).
fn make_engine(snapshot_every_n: usize) -> Engine<CounterMachine, InMemoryStorage, TestCodec> {
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = InMemoryStorage::new();
    let config = EngineConfig::new("test-source", snapshot_every_n);
    Engine::new(core, storage, TestCodec, config)
}

// ============================================================
// Tests
// ============================================================

#[test]
fn test_process_block_writes_and_applies() {
    let mut engine = make_engine(1000);

    let hash_a = make_hash(1);
    let block = make_block(hash_a, GENESIS, 0, &[10, 20, 30]);

    let is_new = engine.process_block(&block).expect("process_block");
    assert!(is_new, "first block should be new");

    // State should reflect sum of events: 10 + 20 + 30 = 60
    assert_eq!(engine.state(), 60u64, "state should equal sum of event values");

    // Canonical tip must be updated in storage.
    let tip = engine
        .storage()
        .get_canonical_tip("test-source")
        .expect("get_canonical_tip");
    assert_eq!(tip, Some(hash_a), "canonical tip should be updated");
}

#[test]
fn test_process_block_duplicate_returns_false() {
    let mut engine = make_engine(1000);

    let hash_a = make_hash(1);
    let block = make_block(hash_a, GENESIS, 0, &[5, 5]);

    let first = engine.process_block(&block).expect("first process");
    assert!(first);
    assert_eq!(engine.state(), 10u64);

    // Process same block again — should be a no-op.
    let second = engine.process_block(&block).expect("second process");
    assert!(!second, "duplicate block should return false");

    // State must not change after duplicate.
    assert_eq!(engine.state(), 10u64, "state must not change on duplicate");
}

#[test]
fn test_process_block_write_first_invariant() {
    // This test verifies that after process_block returns, BOTH storage and state
    // have been updated (write-first means storage is written before state is advanced).
    let mut engine = make_engine(1000);

    let hash_a = make_hash(1);
    let block = make_block(hash_a, GENESIS, 0, &[42]);

    engine.process_block(&block).expect("process_block");

    // Storage must contain the block.
    let height = engine
        .storage()
        .get_block_height(&hash_a)
        .expect("get_block_height");
    assert_eq!(height, Some(0u64), "block must be in storage");

    // Events must be in storage.
    let stored_events = engine
        .storage()
        .load_block_events(&hash_a)
        .expect("load_block_events");
    assert_eq!(stored_events.len(), 1, "events must be stored");

    // State must also be advanced.
    assert_eq!(engine.state(), 42u64, "state must be advanced after write");
}

#[test]
fn test_snapshot_every_n_blocks() {
    // snapshot_every_n = 3: snapshot after blocks 3 and 6.
    let mut engine = make_engine(3);

    let hashes: Vec<[u8; 32]> = (1u8..=6).map(make_hash).collect();
    let parents: Vec<[u8; 32]> = std::iter::once(GENESIS)
        .chain(hashes.iter().copied())
        .take(6)
        .collect();

    for i in 0..6 {
        let block = make_block(hashes[i], parents[i], i as u64, &[1]);
        engine.process_block(&block).expect("process_block");
    }

    // After 6 blocks with snapshot_every_n=3, snapshots should exist at blocks 3 and 6.
    let snap_at_3 = engine
        .storage()
        .find_nearest_snapshot("test-source", &hashes[2]) // hash[2] = block 3
        .expect("find_nearest_snapshot at 3");
    assert!(snap_at_3.is_some(), "snapshot should exist after 3 blocks");

    let snap_at_6 = engine
        .storage()
        .find_nearest_snapshot("test-source", &hashes[5]) // hash[5] = block 6
        .expect("find_nearest_snapshot at 6");
    assert!(snap_at_6.is_some(), "snapshot should exist after 6 blocks");
}

#[test]
fn test_snapshot_not_triggered_by_duplicates() {
    // snapshot_every_n = 2: snapshot triggered after 2 NEW distinct blocks.
    // If block A is processed twice, it counts as 1, not 2.
    let mut engine = make_engine(2);

    let hash_a = make_hash(1);
    let hash_b = make_hash(2);

    let block_a = make_block(hash_a, GENESIS, 0, &[1]);
    let block_b = make_block(hash_b, hash_a, 1, &[1]);

    // Process A (count = 1).
    engine.process_block(&block_a).expect("process block A");

    // Process A again (duplicate — count stays at 1).
    let dup = engine.process_block(&block_a).expect("process block A dup");
    assert!(!dup, "duplicate should return false");

    // No snapshot yet (count = 1 < 2).
    let snap_before_b = engine
        .storage()
        .find_nearest_snapshot("test-source", &hash_a)
        .expect("find_nearest_snapshot");
    assert!(snap_before_b.is_none(), "no snapshot should exist after only 1 distinct block");

    // Process B (count = 2 >= threshold) — snapshot should trigger.
    engine.process_block(&block_b).expect("process block B");

    let snap_after_b = engine
        .storage()
        .find_nearest_snapshot("test-source", &hash_b)
        .expect("find_nearest_snapshot after B");
    assert!(snap_after_b.is_some(), "snapshot should be triggered after 2nd distinct block");
}

#[test]
fn test_engine_state_and_subscribe() {
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = InMemoryStorage::new();
    let config = EngineConfig::new("test-source", 100);
    let mut engine = Engine::new(core, storage, TestCodec, config);

    // Subscribe before processing blocks.
    let rx = engine.subscribe();

    // Initial state is 0.
    assert_eq!(*rx.borrow(), 0u64);
    assert_eq!(engine.state(), 0u64);

    // Process a block.
    let hash_a = make_hash(1);
    let block = make_block(hash_a, GENESIS, 0, &[7, 3]);
    engine.process_block(&block).expect("process_block");

    // State should be 10.
    assert_eq!(engine.state(), 10u64);
    // Receiver should see the update.
    assert_eq!(*rx.borrow(), 10u64);
}

#[test]
fn test_engine_with_in_memory_storage() {
    // Full pipeline test: 5 blocks, verify final state and canonical tip.
    let mut engine = make_engine(1000);

    let hashes: Vec<[u8; 32]> = (1u8..=5).map(make_hash).collect();
    let parents: Vec<[u8; 32]> = std::iter::once(GENESIS)
        .chain(hashes.iter().copied())
        .take(5)
        .collect();

    let values = [1u64, 2, 3, 4, 5];
    for i in 0..5 {
        let block = make_block(hashes[i], parents[i], i as u64, &[values[i]]);
        let is_new = engine.process_block(&block).expect("process_block");
        assert!(is_new, "block {} should be new", i);
    }

    // Final state = sum of all values = 1+2+3+4+5 = 15
    assert_eq!(engine.state(), 15u64, "final state should be sum of all event values");

    // Canonical tip should be the last block.
    let tip = engine
        .storage()
        .get_canonical_tip("test-source")
        .expect("get_canonical_tip");
    assert_eq!(tip, Some(hashes[4]), "canonical tip should be the last block");
}

// ============================================================
// Phase 3: New type tests — EngineConfig::new, ReorgEvent, EngineStatus, EngineError
// ============================================================

#[test]
fn test_engine_config_new_constructor() {
    let config = EngineConfig::new("my-source", 50);
    assert_eq!(config.source_id, "my-source");
    assert_eq!(config.snapshot_every_n, 50);
    assert_eq!(config.max_retries, 5, "default max_retries should be 5");
    assert_eq!(config.initial_delay_ms, 1000, "default initial_delay_ms should be 1000");
    assert_eq!(config.max_delay_ms, 30_000, "default max_delay_ms should be 30000");
    assert_eq!(config.batch_size, 64, "default batch_size should be 64");
}

#[test]
fn test_reorg_event_fields() {
    let ancestor = make_hash(10);
    let old_tip = make_hash(11);
    let new_tip = make_hash(20);

    let event = ReorgEvent {
        common_ancestor: ancestor,
        old_tip,
        new_tip,
        depth: 3,
    };

    assert_eq!(event.common_ancestor, ancestor);
    assert_eq!(event.old_tip, old_tip);
    assert_eq!(event.new_tip, new_tip);
    assert_eq!(event.depth, 3);
}

#[test]
fn test_engine_status_variants() {
    // Verify all variants can be constructed and compared via Debug (no PartialEq needed).
    let disconnected = EngineStatus::Disconnected;
    let reconnecting = EngineStatus::Reconnecting(2);
    let reconnected = EngineStatus::Reconnected;
    let gap_filling = EngineStatus::GapFilling;

    // Just verify they're debuggable (compile-time check of variant existence).
    let _ = format!("{:?}", disconnected);
    let _ = format!("{:?}", reconnecting);
    let _ = format!("{:?}", reconnected);
    let _ = format!("{:?}", gap_filling);

    // Verify Reconnecting carries the attempt count.
    if let EngineStatus::Reconnecting(n) = reconnecting {
        assert_eq!(n, 2, "attempt number should be 2");
    } else {
        panic!("expected Reconnecting variant");
    }
}

#[test]
fn test_engine_error_variants() {
    use std::io;

    // Storage variant
    let storage_err: EngineError = EngineError::Storage(
        Box::new(io::Error::new(io::ErrorKind::Other, "disk full"))
    );
    assert!(storage_err.to_string().contains("storage error"));

    // Source variant
    let source_err: EngineError = EngineError::Source(
        Box::new(io::Error::new(io::ErrorKind::ConnectionReset, "disconnected"))
    );
    assert!(source_err.to_string().contains("source error"));

    // DecodeState variant
    let decode_err = EngineError::DecodeState;
    assert!(decode_err.to_string().contains("decode"));

    // ReconnectExhausted variant
    let reconnect_err = EngineError::ReconnectExhausted(
        5,
        Box::new(io::Error::new(io::ErrorKind::TimedOut, "timeout"))
    );
    assert!(reconnect_err.to_string().contains("5"));

    // ReorgWithoutAncestor variant
    let reorg_err = EngineError::ReorgWithoutAncestor;
    assert!(reorg_err.to_string().contains("ancestor"));
}

#[test]
fn test_engine_error_is_send_sync() {
    // Compile-time check: EngineError must be Send + Sync.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EngineError>();
}

#[test]
fn test_engine_config_new_used_in_engine() {
    // Verify EngineConfig::new() can be used to construct an Engine.
    let sm = Arc::new(CounterMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = InMemoryStorage::new();
    let config = EngineConfig::new("test-source", 100);
    let _engine: Engine<CounterMachine, InMemoryStorage, TestCodec> =
        Engine::new(core, storage, TestCodec, config);
    // If this compiles and runs, the constructor works.
}
