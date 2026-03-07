/// Integration tests for the historical query API.
///
/// Tests At() (reconstruct_at) 4-tier cost hierarchy:
/// - Tier 1: exact snapshot match
/// - Tier 2: nearest snapshot + cached event replay
/// - Tier 3: missing events fetched from source + replay
/// - Tier 4: fall back to fetch_state
///
/// Tests Range() (EventRangeIterator):
/// - Paginated iteration across page boundaries
/// - Empty range returns immediately
/// - Correct event ordering
/// - Post-reorg correctness for historical queries
use std::sync::Arc;

use chainfold::storage::{InMemoryStorage, Storage};
use chainfold::{
    Block, Codec, Engine, EngineConfig, EngineError, Event, ReducerCore, ReorgEvent, StateMachine,
};
use chainfold::store::query::{reconstruct_at, EventRangeIterator};

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
            cursor: TestCursor(height * 10),
            data: value,
        }],
    }
}

/// Create a default engine with InMemoryStorage.
fn make_engine(
    snapshot_every_n: usize,
) -> (
    Engine<SumMachine, InMemoryStorage, SumCodec>,
    Arc<InMemoryStorage>,
) {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let storage = Arc::new(InMemoryStorage::new());
    let config = EngineConfig::new("test-source", snapshot_every_n);
    let engine = Engine::new(core, storage.clone(), SumCodec, config);
    (engine, storage)
}

// ============================================================
// Storage: load_events_by_height_range tests
// ============================================================

#[test]
fn test_load_events_by_height_range_returns_events_in_height_order() {
    let storage = InMemoryStorage::new();

    // Write 5 blocks at heights 0-4, each with one event.
    for i in 0u8..5 {
        let hash = make_hash(i + 1);
        let parent = if i == 0 { GENESIS_PARENT } else { make_hash(i) };
        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64 * 10)).unwrap();
        let payload_bytes = (i as u64 * 100).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    // Query range [1, 3] — should get blocks at heights 1, 2, 3.
    let events = storage
        .load_events_by_height_range("test-source", 1, 3, 100, 0)
        .unwrap();
    assert_eq!(events.len(), 3, "should return 3 events for heights 1,2,3");

    // Verify height ordering by decoding payload values.
    let values: Vec<u64> = events
        .iter()
        .map(|(_, p)| u64::from_le_bytes(p[..8].try_into().unwrap()))
        .collect();
    assert_eq!(values, vec![100, 200, 300]);
}

#[test]
fn test_load_events_by_height_range_with_offset_beyond_available() {
    let storage = InMemoryStorage::new();

    // Write 3 blocks.
    for i in 0u8..3 {
        let hash = make_hash(i + 1);
        let parent = if i == 0 { GENESIS_PARENT } else { make_hash(i) };
        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64)).unwrap();
        let payload_bytes = (i as u64).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    // Offset 100 is way beyond the 3 available events.
    let events = storage
        .load_events_by_height_range("test-source", 0, 2, 100, 100)
        .unwrap();
    assert!(events.is_empty(), "offset beyond available events returns empty");
}

#[test]
fn test_load_events_by_height_range_pagination() {
    let storage = InMemoryStorage::new();

    // Write 5 blocks at heights 0-4.
    for i in 0u8..5 {
        let hash = make_hash(i + 1);
        let parent = if i == 0 { GENESIS_PARENT } else { make_hash(i) };
        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64)).unwrap();
        let payload_bytes = (i as u64).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    // Page 1: limit=2, offset=0 -> heights 0, 1
    let page1 = storage
        .load_events_by_height_range("test-source", 0, 4, 2, 0)
        .unwrap();
    assert_eq!(page1.len(), 2);

    // Page 2: limit=2, offset=2 -> heights 2, 3
    let page2 = storage
        .load_events_by_height_range("test-source", 0, 4, 2, 2)
        .unwrap();
    assert_eq!(page2.len(), 2);

    // Page 3: limit=2, offset=4 -> height 4
    let page3 = storage
        .load_events_by_height_range("test-source", 0, 4, 2, 4)
        .unwrap();
    assert_eq!(page3.len(), 1);
}

// ============================================================
// reconstruct_at tests
// ============================================================

#[tokio::test]
async fn test_reconstruct_at_exact_snapshot() {
    // Tier 1: exact snapshot at target block.
    let (mut engine, storage) = make_engine(1); // snapshot every block

    // Process blocks: h=0 (val=10), h=1 (val=20).
    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);

    engine.process_block(&block_a).unwrap();
    engine.process_block(&block_b).unwrap();

    // Reconstruct at height 0 — snapshot exists at block A.
    let state = reconstruct_at(
        storage.as_ref(),
        &SumCodec,
        &SumMachine,
        &0u64,
        "test-source",
        0,
    )
    .unwrap();

    assert_eq!(state, 10u64, "tier 1: exact snapshot should return state=10");
}

#[tokio::test]
async fn test_reconstruct_at_snapshot_plus_cached_replay() {
    // Tier 2: nearest snapshot + cached event replay.
    let (mut engine, storage) = make_engine(2); // snapshot every 2 blocks

    // Process blocks h=0..4 — snapshots at h=1 (state=30) and h=3 (state=100).
    let blocks: Vec<_> = (0..5)
        .map(|i| make_block(i as u8 + 1, i as u8, i as u64, (i as u64 + 1) * 10))
        .collect();

    for b in &blocks {
        engine.process_block(b).unwrap();
    }

    // Reconstruct at height 2 — nearest snapshot is at height 1, replay height 2 events.
    // Snapshot at h=1 stores state after blocks 0+1 = 10+20 = 30.
    // Replay h=2 events (value=30) -> state = 30 + 30 = 60.
    let state = reconstruct_at(
        storage.as_ref(),
        &SumCodec,
        &SumMachine,
        &0u64,
        "test-source",
        2,
    )
    .unwrap();

    assert_eq!(state, 60u64, "tier 2: snapshot(30) + replay(30) = 60");
}

#[tokio::test]
async fn test_reconstruct_at_no_snapshot_full_replay() {
    // No snapshot available — replay from genesis using initial state.
    let (mut engine, storage) = make_engine(1000); // no snapshots

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30);

    engine.process_block(&block_a).unwrap();
    engine.process_block(&block_b).unwrap();
    engine.process_block(&block_c).unwrap();

    // Reconstruct at height 1 — no snapshots, replay from genesis.
    // initial_state(0) + h=0(10) + h=1(20) = 30.
    let state = reconstruct_at(
        storage.as_ref(),
        &SumCodec,
        &SumMachine,
        &0u64,
        "test-source",
        1,
    )
    .unwrap();

    assert_eq!(state, 30u64, "no snapshot: initial(0) + h0(10) + h1(20) = 30");
}

// ============================================================
// EventRangeIterator tests
// ============================================================

#[test]
fn test_event_range_iterator_yields_across_page_boundaries() {
    let storage = Arc::new(InMemoryStorage::new());

    // Write 10 blocks at heights 0-9.
    for i in 0u8..10 {
        let hash = make_hash(i + 1);
        let parent = if i == 0 { GENESIS_PARENT } else { make_hash(i) };
        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64)).unwrap();
        let payload_bytes = (i as u64).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    // Iterate range [0, 9] with page_size=3.
    let iter: EventRangeIterator<InMemoryStorage, SumCodec> = EventRangeIterator::new(
        storage,
        SumCodec,
        "test-source".to_string(),
        0,
        9,
        3, // page_size
    );

    let events: Vec<(TestCursor, u64)> = iter.collect();
    assert_eq!(events.len(), 10, "should yield all 10 events across pages");

    // Verify ordering.
    let values: Vec<u64> = events.iter().map(|(_, v)| *v).collect();
    assert_eq!(values, (0..10).collect::<Vec<u64>>());
}

#[test]
fn test_event_range_iterator_empty_range() {
    let storage = Arc::new(InMemoryStorage::new());

    // No blocks written.
    let iter: EventRangeIterator<InMemoryStorage, SumCodec> = EventRangeIterator::new(
        storage,
        SumCodec,
        "test-source".to_string(),
        0,
        10,
        32,
    );

    let events: Vec<(TestCursor, u64)> = iter.collect();
    assert!(events.is_empty(), "empty range returns no events");
}

#[test]
fn test_event_range_iterator_returns_none_when_exhausted() {
    let storage = Arc::new(InMemoryStorage::new());

    // Write 2 blocks.
    for i in 0u8..2 {
        let hash = make_hash(i + 1);
        let parent = if i == 0 { GENESIS_PARENT } else { make_hash(i) };
        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64)).unwrap();
        let payload_bytes = (i as u64).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    let mut iter: EventRangeIterator<InMemoryStorage, SumCodec> = EventRangeIterator::new(
        storage,
        SumCodec,
        "test-source".to_string(),
        0,
        1,
        32,
    );

    // Should yield 2, then None.
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    assert!(iter.next().is_none(), "iterator should be exhausted");
}

#[test]
fn test_event_range_iterator_100_events_pages_of_32() {
    let storage = Arc::new(InMemoryStorage::new());

    // Write 100 blocks at heights 0-99.
    for i in 0u8..100 {
        let mut hash = [0u8; 32];
        hash[0] = i.wrapping_add(1);
        hash[1] = (i / 255) as u8;
        // Use unique hashes — since we only go to 100, byte[0] overflow isn't an issue
        // (wrapping_add handles it, and each hash is unique for i in 0..100).
        let parent = if i == 0 {
            GENESIS_PARENT
        } else {
            let mut p = [0u8; 32];
            p[0] = i; // parent = previous block's hash[0]
            p[1] = ((i - 1) / 255) as u8;
            p
        };

        let cursor_bytes = postcard::to_allocvec(&TestCursor(i as u64)).unwrap();
        let payload_bytes = (i as u64).to_le_bytes().to_vec();
        storage
            .write_block(
                "test-source",
                &hash,
                &parent,
                i as u64,
                &[(&cursor_bytes, &payload_bytes)],
            )
            .unwrap();
    }

    let iter: EventRangeIterator<InMemoryStorage, SumCodec> = EventRangeIterator::new(
        storage,
        SumCodec,
        "test-source".to_string(),
        0,
        99,
        32, // page_size
    );

    let events: Vec<(TestCursor, u64)> = iter.collect();
    assert_eq!(events.len(), 100, "should yield all 100 events");

    // Verify ordering.
    for (i, (_, val)) in events.iter().enumerate() {
        assert_eq!(*val, i as u64, "event {} should have value {}", i, i);
    }
}

// ============================================================
// Reorg correctness for historical queries
// ============================================================

#[tokio::test]
async fn test_reconstruct_at_after_reorg_returns_new_chain_state() {
    // Process A -> B -> C, reorg to D (parent=B).
    // Reconstruct at height 2 should return state from D's chain, not C's.
    let (mut engine, storage) = make_engine(1); // snapshot every block

    let block_a = make_block(1, 0, 0, 10);
    let block_b = make_block(2, 1, 1, 20);
    let block_c = make_block(3, 2, 2, 30);

    engine.process_block(&block_a).unwrap();
    engine.process_block(&block_b).unwrap();
    engine.process_block(&block_c).unwrap();

    // Reorg: D (parent=B, height=2, value=100).
    let block_d = make_block(4, 2, 2, 100);
    engine
        .process_or_reorg(&block_d, &mut |_: ReorgEvent| {})
        .unwrap();

    // After reorg, canonical chain is A -> B -> D.
    // Reconstruct at height 2 should give state from A+B+D = 10+20+100 = 130.
    // Note: reconstruct_at walks the canonical chain (A->B->D), not the old chain.
    let state = reconstruct_at(
        storage.as_ref(),
        &SumCodec,
        &SumMachine,
        &0u64,
        "test-source",
        2,
    )
    .unwrap();

    assert_eq!(state, 130u64, "post-reorg at height 2 should be 130 (A+B+D)");
}
