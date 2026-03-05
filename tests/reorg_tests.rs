/// Integration tests for reorg detection and state re-derivation.
///
/// Tests the Engine's `process_or_reorg` path:
/// - Single-block reorgs (depth=1)
/// - Multi-block reorgs (depth>1)
/// - No reorg on normal extension
/// - ReorgEvent field correctness
/// - on_reorg callback fires BEFORE state changes
/// - Old branch blocks preserved in storage (DAG model)
/// - State after reorg matches clean processing (deterministic)
/// - Reorg with snapshot near fork (fast path)
/// - Reorg without snapshot (full replay from genesis)
use std::sync::Arc;

use chainfold::storage::{InMemoryStorage, Storage};
use chainfold::{Block, Codec, Engine, EngineConfig, Event, ReorgEvent, ReducerCore, StateMachine};

// ============================================================
// Test infrastructure
// ============================================================

/// Minimal cursor: u64 sequence number.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct TestCursor(u64);

/// State machine that sums u64 event values.
struct SumMachine;

impl StateMachine for SumMachine {
    type State = u64;
    type Event = u64;
    type Cursor = TestCursor;

    fn apply(&self, state: u64, event: &Event<TestCursor, u64>) -> u64 {
        state + event.data
    }
}

/// Codec: u64 events and state as little-endian 8 bytes.
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

/// Create a block hash where byte[0] = n and the rest are 0.
fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// Genesis sentinel: all-zeros (no parent).
const GENESIS_PARENT: [u8; 32] = [0u8; 32];

/// Build a block with a specific hash byte, parent byte, height, and event value.
/// parent_byte = 0 means use GENESIS_PARENT (all-zeros).
fn make_block(hash_byte: u8, parent_byte: u8, height: u64, value: u64) -> Block<TestCursor, u64> {
    Block {
        block_hash: make_hash(hash_byte),
        parent_hash: if parent_byte == 0 {
            GENESIS_PARENT
        } else {
            make_hash(parent_byte)
        },
        height,
        events: vec![Event {
            cursor: TestCursor(0),
            data: value,
        }],
    }
}

/// Create a default engine with InMemoryStorage.
fn make_engine(snapshot_every_n: usize) -> Engine<SumMachine, InMemoryStorage, SumCodec> {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = InMemoryStorage::new();
    let config = EngineConfig::new("test-source", snapshot_every_n);
    Engine::new(core, storage, SumCodec, config)
}

// ============================================================
// Tests
// ============================================================

/// Single-block reorg:
/// Chain A -> B -> C, then D arrives with parent=B (skipping C).
/// Engine detects reorg, finds ancestor B, re-derives state to B, processes D.
/// Final state = A.val + B.val + D.val.
#[test]
fn test_single_block_reorg() {
    let mut engine = make_engine(1000);

    // Build chain: A(h=0,val=10) -> B(h=1,val=20) -> C(h=2,val=30)
    let block_a = make_block(1, 0, 0, 10); // hash=1, parent=GENESIS
    let block_b = make_block(2, 1, 1, 20); // hash=2, parent=1
    let block_c = make_block(3, 2, 2, 30); // hash=3, parent=2

    // Process the main chain via process_block (no reorg).
    let mut no_reorg = |_: ReorgEvent| panic!("should not trigger reorg on normal blocks");
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("process A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("process B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("process C");

    // State = 10 + 20 + 30 = 60
    assert_eq!(engine.state(), 60u64, "state before reorg should be 60");

    // Canonical tip should be C (hash=3).
    let tip = engine.storage().get_canonical_tip("test-source").expect("get_canonical_tip");
    assert_eq!(tip, Some(make_hash(3)), "tip should be C before reorg");

    // Block D arrives: hash=4, parent=B (not C!) — this is a reorg.
    let block_d = make_block(4, 2, 2, 100); // hash=4, parent=2 (B)

    let mut reorg_events: Vec<ReorgEvent> = Vec::new();
    let result = engine.process_or_reorg(&block_d, &mut |e| reorg_events.push(e.clone()));
    result.expect("process_or_reorg with reorg");

    // on_reorg should have been called exactly once.
    assert_eq!(reorg_events.len(), 1, "on_reorg should fire exactly once");
    let event = &reorg_events[0];

    // ReorgEvent: common_ancestor=B(hash=2), old_tip=C(hash=3), new_tip=D(hash=4), depth=1.
    assert_eq!(event.common_ancestor, make_hash(2), "common_ancestor should be B");
    assert_eq!(event.old_tip, make_hash(3), "old_tip should be C");
    assert_eq!(event.new_tip, make_hash(4), "new_tip should be D");
    assert_eq!(event.depth, 1, "depth should be 1");

    // State = A.val + B.val + D.val = 10 + 20 + 100 = 130 (C excluded).
    assert_eq!(engine.state(), 130u64, "state after reorg should exclude C and include D");

    // Canonical tip should now be D (hash=4).
    let new_tip = engine.storage().get_canonical_tip("test-source").expect("get_canonical_tip");
    assert_eq!(new_tip, Some(make_hash(4)), "canonical tip should be D after reorg");

    // Old block C (hash=3) should still be in storage — DAG nodes are never deleted.
    let c_height = engine.storage().get_block_height(&make_hash(3)).expect("get_block_height for C");
    assert!(c_height.is_some(), "block C should still be in storage after reorg");
}

/// Multi-block reorg:
/// Chain A(h=0) -> B(h=1) -> C(h=2) -> D(h=3) -> E(h=4).
/// Block F arrives: parent=B (hash=2), h=2, val=500.
/// Engine rolls back to B (depth = 3: E->D->C rolled back).
/// State = A.val + B.val + F.val.
#[test]
fn test_multi_block_reorg() {
    let mut engine = make_engine(1000);

    // Build chain: A -> B -> C -> D -> E
    let block_a = make_block(1, 0, 0, 10);  // hash=1, parent=GENESIS, val=10
    let block_b = make_block(2, 1, 1, 20);  // hash=2, parent=1, val=20
    let block_c = make_block(3, 2, 2, 30);  // hash=3, parent=2, val=30
    let block_d = make_block(4, 3, 3, 40);  // hash=4, parent=3, val=40
    let block_e = make_block(5, 4, 4, 50);  // hash=5, parent=4, val=50

    let mut no_reorg = |_: ReorgEvent| panic!("should not trigger reorg on main chain");
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("process A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("process B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("process C");
    engine.process_or_reorg(&block_d, &mut no_reorg).expect("process D");
    engine.process_or_reorg(&block_e, &mut no_reorg).expect("process E");

    // State = 10+20+30+40+50 = 150
    assert_eq!(engine.state(), 150u64, "state before reorg");

    // Block F arrives: parent=B (depth 3 reorg, rolling back C/D/E).
    let block_f = make_block(6, 2, 2, 500); // hash=6, parent=2 (B), val=500

    let mut reorg_events: Vec<ReorgEvent> = Vec::new();
    engine
        .process_or_reorg(&block_f, &mut |e| reorg_events.push(e.clone()))
        .expect("multi-block reorg");

    // Depth = E.height - B.height = 4 - 1 = 3.
    assert_eq!(reorg_events.len(), 1, "on_reorg should fire once");
    assert_eq!(reorg_events[0].depth, 3, "depth should be 3");
    assert_eq!(reorg_events[0].common_ancestor, make_hash(2), "common_ancestor should be B");
    assert_eq!(reorg_events[0].old_tip, make_hash(5), "old_tip should be E");
    assert_eq!(reorg_events[0].new_tip, make_hash(6), "new_tip should be F");

    // State = A.val + B.val + F.val = 10 + 20 + 500 = 530
    assert_eq!(engine.state(), 530u64, "state after multi-block reorg");
}

/// Normal extension (no reorg):
/// Process A -> B. Block C arrives with parent=B (correct extension).
/// on_reorg should NOT be called.
/// State = A.val + B.val + C.val.
#[test]
fn test_no_reorg_on_normal_extension() {
    let mut engine = make_engine(1000);

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30); // parent=B — correct extension

    let mut reorg_count = 0usize;
    engine.process_or_reorg(&block_a, &mut |_| reorg_count += 1).expect("A");
    engine.process_or_reorg(&block_b, &mut |_| reorg_count += 1).expect("B");
    engine.process_or_reorg(&block_c, &mut |_| reorg_count += 1).expect("C");

    assert_eq!(reorg_count, 0, "on_reorg must NOT be called on normal extension");
    assert_eq!(engine.state(), 60u64, "state should be 10+20+30=60");
}

/// Verify all four ReorgEvent fields are populated correctly.
#[test]
fn test_reorg_event_fields_explicit() {
    let mut engine = make_engine(1000);

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30);

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("C");

    // D has parent=B — triggers reorg.
    let block_d = make_block(4, 2, 2, 100);

    let mut captured_event: Option<ReorgEvent> = None;
    engine
        .process_or_reorg(&block_d, &mut |e| captured_event = Some(e))
        .expect("reorg to D");

    let evt = captured_event.expect("event should have been captured");
    assert_eq!(evt.common_ancestor, make_hash(2), "common_ancestor = B");
    assert_eq!(evt.old_tip, make_hash(3), "old_tip = C");
    assert_eq!(evt.new_tip, make_hash(4), "new_tip = D");
    assert_eq!(evt.depth, 1, "depth = 1 (C rolled back)");
}

/// on_reorg callback fires BEFORE state changes.
///
/// The callback receives the ReorgEvent with old_tip = C (hash=3).
/// This proves the callback was invoked with the OLD state context —
/// the engine calls on_reorg before re-deriving state from the fork point.
///
/// We additionally verify: state AFTER process_or_reorg reflects new branch.
#[test]
fn test_reorg_callback_fires_before_state_change() {
    let mut engine = make_engine(1000);

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30);

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("C");

    // State before reorg = 60.
    assert_eq!(engine.state(), 60u64);

    // D triggers reorg (parent=B).
    let block_d = make_block(4, 2, 2, 100);

    let mut old_tip_in_callback: Option<[u8; 32]> = None;
    engine
        .process_or_reorg(&block_d, &mut |e| {
            // The callback receives old_tip = C (hash=3).
            // This proves it fired BEFORE state was changed to the new branch.
            old_tip_in_callback = Some(e.old_tip);
        })
        .expect("reorg to D");

    // Callback was called with old_tip = C.
    assert_eq!(
        old_tip_in_callback,
        Some(make_hash(3)),
        "callback should see old_tip = C (called before state re-derivation)"
    );

    // After process_or_reorg, state reflects new branch: A + B + D = 130.
    assert_eq!(engine.state(), 130u64, "post-reorg state should be 130");
}

/// State after reorg matches clean processing of the new branch.
///
/// Clean: process A -> B -> D (no C ever processed).
/// Reorg: process A -> B -> C, then reorg to D (parent=B).
/// Both must produce the same final state.
#[test]
fn test_reorg_state_matches_clean_processing() {
    // Clean path: A -> B -> D
    let mut clean_engine = make_engine(1000);
    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_d = make_block(4, 2, 2, 100);

    let mut no_reorg = |_: ReorgEvent| {};
    clean_engine.process_or_reorg(&block_a, &mut no_reorg).expect("A clean");
    clean_engine.process_or_reorg(&block_b, &mut no_reorg).expect("B clean");
    clean_engine.process_or_reorg(&block_d, &mut no_reorg).expect("D clean");
    let state_clean = clean_engine.state();

    // Reorg path: A -> B -> C, then reorg to D.
    let mut reorg_engine = make_engine(1000);
    let block_c = make_block(3, 2, 2, 30);

    reorg_engine.process_or_reorg(&block_a, &mut |_| {}).expect("A reorg path");
    reorg_engine.process_or_reorg(&block_b, &mut |_| {}).expect("B reorg path");
    reorg_engine.process_or_reorg(&block_c, &mut |_| {}).expect("C reorg path");
    reorg_engine.process_or_reorg(&block_d, &mut |_| {}).expect("D via reorg");

    let state_reorg = reorg_engine.state();

    assert_eq!(
        state_clean, state_reorg,
        "state after reorg must match clean processing of the new branch"
    );
}

/// Reorg with snapshot near the fork point.
///
/// Engine with snapshot_every_n = 2.
/// Process A(h=0) -> B(h=1) [snapshot at B] -> C(h=2) -> D(h=3) [snapshot at D].
/// Block E arrives: parent=B (reorg at height 1, depth=2).
///
/// Re-derivation should:
/// 1. Find snapshot at B (the fork point IS the snapshot).
/// 2. Load snapshot at B (no replay needed — snapshot equals ancestor).
/// 3. Process E. State = snapshot-at-B + E.val.
#[test]
fn test_reorg_with_snapshot_near_fork() {
    let mut engine = make_engine(2); // snapshot every 2 blocks

    let block_a = make_block(1, 0, 0, 10); // h=0, val=10
    let block_b = make_block(2, 1, 1, 20); // h=1, val=20  [snapshot after B = 2nd block]
    let block_c = make_block(3, 2, 2, 30); // h=2, val=30
    let block_d = make_block(4, 3, 3, 40); // h=3, val=40  [snapshot after D = 4th block]

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("B");

    // Snapshot should exist at B (2nd block with snapshot_every_n=2).
    let snap_at_b = engine
        .storage()
        .find_nearest_snapshot("test-source", &make_hash(2))
        .expect("find_nearest_snapshot at B");
    assert!(snap_at_b.is_some(), "snapshot should exist at B");

    engine.process_or_reorg(&block_c, &mut no_reorg).expect("C");
    engine.process_or_reorg(&block_d, &mut no_reorg).expect("D");

    // State = 10+20+30+40 = 100
    assert_eq!(engine.state(), 100u64);

    // Block E arrives: parent=B (reorg, depth=2 — rolls back C and D).
    let block_e = make_block(5, 2, 2, 200); // hash=5, parent=B(hash=2), val=200

    let mut reorg_events: Vec<ReorgEvent> = Vec::new();
    engine
        .process_or_reorg(&block_e, &mut |e| reorg_events.push(e.clone()))
        .expect("reorg to E");

    // Reorg detected with depth=2.
    assert_eq!(reorg_events.len(), 1);
    assert_eq!(reorg_events[0].depth, 2, "depth should be 2 (D and C rolled back)");
    assert_eq!(reorg_events[0].common_ancestor, make_hash(2), "common_ancestor = B");

    // State = snapshot-at-B(=30) + E.val(200) = 230
    // Snapshot at B captures state after A+B = 10+20 = 30.
    assert_eq!(engine.state(), 230u64, "state = snapshot-at-B(30) + E.val(200)");

    // Canonical tip = E.
    let tip = engine.storage().get_canonical_tip("test-source").expect("get_canonical_tip");
    assert_eq!(tip, Some(make_hash(5)), "canonical tip should be E");
}

/// Reorg without any snapshot (full replay from genesis).
///
/// Engine with snapshot_every_n = 1000 (no snapshots ever created).
/// Process A(h=0) -> B(h=1) -> C(h=2).
/// Block D arrives: parent=A (reorg to height 0, depth=2).
///
/// Re-derivation: no snapshot found. Engine replays from genesis (initial_state=0):
/// - Resets to initial_state (0)
/// - Replays A's events (only A, since ancestor IS A and D's parent_hash is A)
/// - Processes D. State = A.val + D.val.
///
/// Note: ancestor is A (hash=1). After reorg, process_block(D) extends from A -> D.
/// So state = initial(0) + replay_up_to_ancestor(A=10) + D.val(99) = 109.
#[test]
fn test_reorg_without_snapshot() {
    let mut engine = make_engine(1000); // no snapshots

    let block_a = make_block(1, 0, 0, 10); // hash=1, parent=GENESIS, val=10
    let block_b = make_block(2, 1, 1, 20); // hash=2, parent=1, val=20
    let block_c = make_block(3, 2, 2, 30); // hash=3, parent=2, val=30

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("C");

    // Confirm no snapshots.
    let snap = engine
        .storage()
        .find_nearest_snapshot("test-source", &make_hash(3))
        .expect("find_nearest_snapshot");
    assert!(snap.is_none(), "no snapshot should exist");

    // D arrives: parent=A (reorg: rolls back B and C, depth=2).
    let block_d = make_block(4, 1, 1, 99); // hash=4, parent=A(hash=1), val=99

    let mut reorg_events: Vec<ReorgEvent> = Vec::new();
    engine
        .process_or_reorg(&block_d, &mut |e| reorg_events.push(e.clone()))
        .expect("reorg without snapshot");

    assert_eq!(reorg_events.len(), 1);
    assert_eq!(reorg_events[0].depth, 2, "depth should be 2");
    assert_eq!(reorg_events[0].common_ancestor, make_hash(1), "common_ancestor = A");

    // State: initial(0) + replay A(10) + D.val(99) = 109
    assert_eq!(engine.state(), 109u64, "state = 0 + A.val(10) + D.val(99) = 109");
}

/// Old branch blocks are preserved in storage after reorg (DAG model).
///
/// Process A -> B -> C. Reorg to D (parent=B).
/// Block C (hash=3) must still be accessible in storage.
#[test]
fn test_old_branch_blocks_preserved() {
    let mut engine = make_engine(1000);

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30);
    let block_d = make_block(4, 2, 2, 100); // parent=B, triggers reorg

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_a, &mut no_reorg).expect("A");
    engine.process_or_reorg(&block_b, &mut no_reorg).expect("B");
    engine.process_or_reorg(&block_c, &mut no_reorg).expect("C");

    engine.process_or_reorg(&block_d, &mut |_| {}).expect("D via reorg");

    // Block C still in storage — height accessible.
    let c_height = engine
        .storage()
        .get_block_height(&make_hash(3))
        .expect("get_block_height for C");
    assert!(c_height.is_some(), "block C height should still be in storage");
    assert_eq!(c_height.unwrap(), 2, "block C height should be 2");

    // Block C events still loadable.
    let c_events = engine
        .storage()
        .load_block_events(&make_hash(3))
        .expect("load_block_events for C");
    assert_eq!(c_events.len(), 1, "block C should have 1 event in storage");

    // New canonical tip is D.
    let tip = engine
        .storage()
        .get_canonical_tip("test-source")
        .expect("get_canonical_tip");
    assert_eq!(tip, Some(make_hash(4)), "canonical tip should be D");
}

/// process_or_reorg handles the first block correctly (no canonical tip yet).
/// First block should be processed normally with no reorg triggered.
#[test]
fn test_process_or_reorg_first_block_no_reorg() {
    let mut engine = make_engine(1000);

    let block_a = make_block(1, 0, 0, 42);

    let mut reorg_count = 0usize;
    engine
        .process_or_reorg(&block_a, &mut |_| reorg_count += 1)
        .expect("first block");

    assert_eq!(reorg_count, 0, "no reorg on first block");
    assert_eq!(engine.state(), 42u64);

    let tip = engine.storage().get_canonical_tip("test-source").expect("get_canonical_tip");
    assert_eq!(tip, Some(make_hash(1)));
}
