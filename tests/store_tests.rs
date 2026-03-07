/// Tests for Store trait and ReducedStore.
///
/// Covers:
/// - ReducedStore::get() returns initial state
/// - ReducedStore::get() returns updated state after Sender changes
/// - ReducedStore::subscribe() delivers state changes via changed().await
/// - Multiple subscribers see the same state updates (fan-out)
/// - Engine with Arc<InMemoryStorage> processes blocks and store.get() reflects state
/// - OnChange pattern: subscribe() + changed().await loop receives state transitions
use std::sync::Arc;

use chainfold::storage::InMemoryStorage;
use chainfold::store::{ReducedStore, Store};
use chainfold::{Block, Codec, Engine, EngineConfig, Event, ReducerCore, StateMachine};

// ============================================================
// Test infrastructure (same as other test files)
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct TestCursor(u64);

struct SumMachine;

impl StateMachine for SumMachine {
    type State = u64;
    type Event = u64;
    type Cursor = TestCursor;

    fn apply(&self, state: u64, event: &Event<TestCursor, u64>) -> u64 {
        state + event.data
    }
}

struct SumCodec;

impl Codec for SumCodec {
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

fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

const GENESIS: [u8; 32] = [0u8; 32];

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

// ============================================================
// Tests
// ============================================================

/// ReducedStore::get() returns initial state after construction.
#[test]
fn test_reduced_store_get_initial_state() {
    let (sender, _rx) = tokio::sync::watch::channel(42u64);
    let sender = Arc::new(sender);
    let store = ReducedStore::new(sender);
    assert_eq!(store.get(), 42u64, "get() should return initial state");
}

/// ReducedStore::get() returns updated state after watch::Sender sends new value.
#[test]
fn test_reduced_store_get_updated_state() {
    let (sender, _rx) = tokio::sync::watch::channel(0u64);
    let sender = Arc::new(sender);
    let store = ReducedStore::new(sender.clone());

    // Update via sender
    sender.send_modify(|s| *s = 99);

    assert_eq!(store.get(), 99u64, "get() should reflect updated state");
}

/// ReducedStore::subscribe() returns Receiver that sees state changes via changed().await.
#[tokio::test]
async fn test_reduced_store_subscribe_sees_changes() {
    let (sender, _rx) = tokio::sync::watch::channel(0u64);
    let sender = Arc::new(sender);
    let store = ReducedStore::new(sender.clone());

    let mut rx = store.subscribe();

    // Send a new value
    sender.send_modify(|s| *s = 50);

    // changed().await should complete since value was updated
    rx.changed().await.expect("changed() should succeed");
    assert_eq!(*rx.borrow(), 50u64, "subscriber should see new value");
}

/// Multiple subscribers see the same state updates (fan-out).
#[tokio::test]
async fn test_reduced_store_multiple_subscribers() {
    let (sender, _rx) = tokio::sync::watch::channel(0u64);
    let sender = Arc::new(sender);
    let store = ReducedStore::new(sender.clone());

    let mut rx1 = store.subscribe();
    let mut rx2 = store.subscribe();

    sender.send_modify(|s| *s = 77);

    rx1.changed().await.expect("rx1 changed");
    rx2.changed().await.expect("rx2 changed");

    assert_eq!(*rx1.borrow(), 77u64, "rx1 should see 77");
    assert_eq!(*rx2.borrow(), 77u64, "rx2 should see 77");
}

/// Engine with Arc<InMemoryStorage> processes blocks and store.get() reflects updated state.
#[test]
fn test_engine_store_reflects_state() {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = Arc::new(InMemoryStorage::new());
    let config = EngineConfig::new("test-source", 1000);
    let mut engine = Engine::new(core, storage, SumCodec, config);

    // Get the store BEFORE processing blocks
    let store = engine.reduced_store();
    assert_eq!(store.get(), 0u64, "store should show initial state");

    // Process a block
    let block = make_block(make_hash(1), GENESIS, 0, &[10, 20, 30]);
    engine.process_block(&block).expect("process_block");

    // Store should reflect updated state
    assert_eq!(store.get(), 60u64, "store should reflect state after block processing");
}

/// OnChange pattern: subscribe() + changed().await loop receives each state transition.
#[tokio::test]
async fn test_on_change_pattern() {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = Arc::new(InMemoryStorage::new());
    let config = EngineConfig::new("test-source", 1000);
    let mut engine = Engine::new(core, storage, SumCodec, config);

    let store = engine.reduced_store();
    let mut rx = store.subscribe();

    // Process block A
    let block_a = make_block(make_hash(1), GENESIS, 0, &[10]);
    engine.process_block(&block_a).expect("process block A");

    rx.changed().await.expect("should see block A");
    assert_eq!(*rx.borrow(), 10u64, "state after block A");

    // Process block B
    let block_b = make_block(make_hash(2), make_hash(1), 1, &[20]);
    engine.process_block(&block_b).expect("process block B");

    rx.changed().await.expect("should see block B");
    assert_eq!(*rx.borrow(), 30u64, "state after block B");
}
