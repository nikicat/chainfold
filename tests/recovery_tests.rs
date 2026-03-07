/// Integration tests for the block DAG crash recovery pipeline.
///
/// Tests the three recovery scenarios:
/// 1. Warm recovery with snapshot: load snapshot, replay forward, fetch gap
/// 2. Warm recovery without snapshot: replay all stored blocks from genesis
/// 3. Cold start: empty DB, fetch all blocks from source
///
/// Also tests: chunked gap fetching (pagination), deduplication during gap fetch,
/// snapshot triggering during gap fetch, and deterministic recovery (state matches
/// sequential processing).
use std::sync::Arc;

use chainfold::coordinator::recovery::recover;
use chainfold::storage::{InMemoryStorage, Storage};
use chainfold::{Block, Codec, Engine, EngineConfig, Event, ReducerCore, StateMachine};

// ============================================================
// Test infrastructure: SumMachine + SumCodec
// ============================================================

/// Minimal cursor: u64 sequence number.
/// Satisfies ChainCursor bounds (Ord, Clone, Serialize, Deserialize, Send, Sync, 'static).
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

// ============================================================
// MockBlockSource
// ============================================================

/// Mock EventSource that returns blocks from an in-memory Vec.
///
/// Supports optional chunked pagination: if `page_size` is Some(n), fetch_blocks
/// returns at most n blocks per call, enabling testing of chunked gap fetch logic.
struct MockBlockSource {
    /// All available blocks, sorted by height.
    blocks: Vec<Block<TestCursor, u64>>,
    /// If Some(n), returns at most n blocks per fetch_blocks call.
    page_size: Option<usize>,
}

impl MockBlockSource {
    fn new(blocks: Vec<Block<TestCursor, u64>>) -> Self {
        Self {
            blocks,
            page_size: None,
        }
    }

    fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = Some(page_size);
        self
    }
}

#[async_trait::async_trait]
impl chainfold::EventSource for MockBlockSource {
    type Cursor = TestCursor;
    type Event = u64;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &TestCursor) -> Result<Vec<u8>, Self::Error> {
        // Return encoded initial state = 0u64
        Ok(0u64.to_le_bytes().to_vec())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<TestCursor, u64>>, Self::Error> {
        // Filter to [from_height, to_height) range.
        let filtered: Vec<Block<TestCursor, u64>> = self
            .blocks
            .iter()
            .filter(|b| b.height >= from_height && b.height < to_height)
            .cloned()
            .collect();

        // Apply pagination if configured.
        let result = match self.page_size {
            Some(n) => filtered.into_iter().take(n).collect(),
            None => filtered,
        };

        Ok(result)
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<TestCursor, u64>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        Ok(Box::new(futures_util::stream::empty()))
    }
}

// ============================================================
// Helper functions
// ============================================================

/// Create a hash where byte[0] = n and the rest are 0.
fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// The genesis sentinel (all-zeros: no parent).
const GENESIS_PARENT: [u8; 32] = [0u8; 32];

/// Build a linear chain of n blocks.
///
/// Block at index i:
/// - block_hash: [i+1, 0, 0, ...] (1-indexed to avoid collision with GENESIS_PARENT)
/// - parent_hash: [i, 0, 0, ...] (genesis has parent = all-zeros)
/// - height: i
/// - events: single event with value (i+1) (so state after all n blocks = sum(1..=n))
fn make_chain(n: usize) -> Vec<Block<TestCursor, u64>> {
    (0..n)
        .map(|i| Block {
            block_hash: make_hash((i + 1) as u8),
            parent_hash: if i == 0 {
                GENESIS_PARENT
            } else {
                make_hash(i as u8)
            },
            height: i as u64,
            events: vec![Event {
                cursor: TestCursor(0),
                data: (i + 1) as u64, // block at height i contributes value i+1
            }],
        })
        .collect()
}

/// Expected state after processing blocks 0..n: sum(1..=n) = n*(n+1)/2
fn expected_state(n: usize) -> u64 {
    (n * (n + 1) / 2) as u64
}

/// Create an engine with InMemoryStorage sharing the given storage.
fn make_engine(
    storage: InMemoryStorage,
    snapshot_every_n: usize,
) -> Engine<SumMachine, InMemoryStorage, SumCodec> {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let config = EngineConfig::new("test-source", snapshot_every_n);
    Engine::new(core, Arc::new(storage), SumCodec, config)
}

// ============================================================
// Tests
// ============================================================

/// Warm recovery with snapshot: process 10 blocks (snapshot at block 5 and 10),
/// simulate crash (new engine, initial state 0), recover with gap fetch to block 14.
/// Final state must equal sum(1..=15) = 120.
#[tokio::test]
async fn test_crash_recovery_with_snapshot() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(15);

    // --- Phase 1: Process blocks 0-9 (10 blocks, snapshot_every_n=5)
    let mut engine = make_engine(storage.clone(), 5);
    for block in chain.iter().take(10) {
        engine.process_block(block).expect("process_block");
    }
    assert_eq!(engine.state(), expected_state(10), "pre-crash state incorrect");

    // Verify snapshots exist (at blocks 5 and 10, i.e., hashes [5] and [10]).
    let snap_at_5 = storage
        .find_nearest_snapshot("test-source", &make_hash(5))
        .expect("find_nearest_snapshot");
    assert!(snap_at_5.is_some(), "snapshot should exist at block 5");
    let snap_at_10 = storage
        .find_nearest_snapshot("test-source", &make_hash(10))
        .expect("find_nearest_snapshot");
    assert!(snap_at_10.is_some(), "snapshot should exist at block 10");

    // --- Phase 2: Crash. New engine with same storage, initial state = 0.
    let mut recovered_engine = make_engine(storage.clone(), 5);
    assert_eq!(recovered_engine.state(), 0u64, "new engine should start at 0");

    // Source has blocks 0-14 (including 5 gap blocks 10-14).
    let source = MockBlockSource::new(chain.clone());

    // Recover to live_tip_height = 14.
    recover(&mut recovered_engine, &source, 14)
        .await
        .expect("recovery should succeed");

    // Final state: sum(1..=15) = 120
    assert_eq!(
        recovered_engine.state(),
        expected_state(15),
        "recovered state should equal sum(1..=15) = 120"
    );

    // Canonical tip should be block 14 (hash [15]).
    let tip = storage
        .get_canonical_tip("test-source")
        .expect("get_canonical_tip");
    assert_eq!(
        tip,
        Some(make_hash(15)),
        "canonical tip should point to block 14 (hash [15])"
    );
}

/// Warm recovery without snapshot: process 5 blocks, no snapshots.
/// New engine recovers by replaying all 5 blocks from genesis.
/// Final state = sum(1..=5) = 15.
#[tokio::test]
async fn test_crash_recovery_without_snapshot() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(5);

    // Process 5 blocks, snapshot_every_n = 1000 (effectively never).
    let mut engine = make_engine(storage.clone(), 1000);
    for block in &chain {
        engine.process_block(block).expect("process_block");
    }
    assert_eq!(engine.state(), expected_state(5));

    // No snapshots should exist.
    let snap = storage
        .find_nearest_snapshot("test-source", &make_hash(5))
        .expect("find_nearest_snapshot");
    assert!(snap.is_none(), "no snapshot should exist with snapshot_every_n=1000");

    // Crash: new engine with initial state 0.
    let mut recovered_engine = make_engine(storage.clone(), 1000);

    // Source has same 5 blocks; live_tip_height = 4 (no gap).
    let source = MockBlockSource::new(chain.clone());

    recover(&mut recovered_engine, &source, 4)
        .await
        .expect("recovery should succeed");

    // All 5 blocks replayed from genesis — state = sum(1..=5) = 15.
    assert_eq!(
        recovered_engine.state(),
        expected_state(5),
        "state should be recovered by replaying all stored blocks"
    );
}

/// Cold start: empty DB. Engine has initial state 0.
/// Recover with live_tip_height = 4 — fetches all 5 blocks from source.
/// Final state = sum(1..=5) = 15.
#[tokio::test]
async fn test_cold_start_recovery() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(5);

    let mut engine = make_engine(storage.clone(), 1000);
    assert_eq!(engine.state(), 0u64);

    // No blocks in storage.
    let tip = storage
        .get_canonical_tip("test-source")
        .expect("get_canonical_tip");
    assert!(tip.is_none(), "storage should be empty");

    let source = MockBlockSource::new(chain.clone());

    recover(&mut engine, &source, 4)
        .await
        .expect("cold start recovery should succeed");

    // All 5 blocks fetched and processed.
    assert_eq!(
        engine.state(),
        expected_state(5),
        "state should equal sum(1..=5) = 15"
    );

    // All 5 blocks should be in storage.
    for i in 0..5usize {
        let height = storage
            .get_block_height(&make_hash((i + 1) as u8))
            .expect("get_block_height");
        assert_eq!(
            height,
            Some(i as u64),
            "block {} should be in storage",
            i
        );
    }
}

/// Gap fetch: process 5 blocks, crash, live tip is at block 9.
/// Recovery replays stored blocks 0-4, then fetches gap blocks 5-9.
/// Final state = sum(1..=10) = 55.
#[tokio::test]
async fn test_gap_fetch() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(10); // blocks 0-9

    // Process blocks 0-4 (5 blocks).
    let mut engine = make_engine(storage.clone(), 1000);
    for block in chain.iter().take(5) {
        engine.process_block(block).expect("process_block");
    }
    assert_eq!(engine.state(), expected_state(5));

    // Crash: new engine.
    let mut recovered_engine = make_engine(storage.clone(), 1000);

    // Source has all 10 blocks; live_tip_height = 9.
    let source = MockBlockSource::new(chain.clone());

    recover(&mut recovered_engine, &source, 9)
        .await
        .expect("recovery should succeed");

    // State = sum(1..=10) = 55
    assert_eq!(
        recovered_engine.state(),
        expected_state(10),
        "state should equal sum(1..=10) = 55 after gap fetch"
    );

    // Blocks 5-9 should now be in storage.
    for i in 5..10usize {
        let height = storage
            .get_block_height(&make_hash((i + 1) as u8))
            .expect("get_block_height");
        assert_eq!(
            height,
            Some(i as u64),
            "gap block {} should be in storage after recovery",
            i
        );
    }
}

/// Chunked gap fetch: source returns at most 3 blocks per fetch_blocks call.
/// Verifies that the gap-fetch loop correctly handles pagination.
/// Process blocks 0-2, recover to block 9 with page_size=3.
/// Final state = sum(1..=10) = 55.
#[tokio::test]
async fn test_chunked_gap_fetch() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(10); // blocks 0-9

    // Process blocks 0-2 (3 blocks).
    let mut engine = make_engine(storage.clone(), 1000);
    for block in chain.iter().take(3) {
        engine.process_block(block).expect("process_block");
    }
    assert_eq!(engine.state(), expected_state(3));

    // Crash: new engine.
    let mut recovered_engine = make_engine(storage.clone(), 1000);

    // Source returns at most 3 blocks per call (3 pages needed for 7 gap blocks).
    let source = MockBlockSource::new(chain.clone()).with_page_size(3);

    recover(&mut recovered_engine, &source, 9)
        .await
        .expect("chunked recovery should succeed");

    // State = sum(1..=10) = 55
    assert_eq!(
        recovered_engine.state(),
        expected_state(10),
        "state should equal sum(1..=10) = 55 after chunked gap fetch"
    );

    // All 10 blocks should be in storage.
    for i in 0..10usize {
        let height = storage
            .get_block_height(&make_hash((i + 1) as u8))
            .expect("get_block_height");
        assert_eq!(
            height,
            Some(i as u64),
            "block {} should be in storage after chunked recovery",
            i
        );
    }
}

/// Gap stitch: no duplication when source includes already-stored blocks.
///
/// Process blocks 0-4 into engine. Source has blocks 0-9 (blocks 0-4 already stored).
/// Recover with live_tip_height = 9.
/// State must be sum(1..=10) = 55, not doubled for blocks 0-4.
#[tokio::test]
async fn test_gap_stitch_no_duplication() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(10);

    // Process blocks 0-4.
    let mut engine = make_engine(storage.clone(), 1000);
    for block in chain.iter().take(5) {
        engine.process_block(block).expect("process_block");
    }
    assert_eq!(engine.state(), expected_state(5));

    // Crash: new engine.
    let mut recovered_engine = make_engine(storage.clone(), 1000);

    // Source returns ALL 10 blocks, including blocks 0-4 that are already in storage.
    // engine.process_block handles deduplication (returns false for already-stored blocks).
    // The recovery should NOT double-apply events for blocks 0-4.
    let source = MockBlockSource::new(chain.clone());

    // Note: gap fetch starts at tip_height + 1 = 5 (not 0), so blocks 0-4 are NOT
    // re-fetched from source. But warm recovery (no snapshot) replays them from storage.
    // This test verifies the stitch point: we replay 0-4 from storage, fetch 5-9 from source.
    recover(&mut recovered_engine, &source, 9)
        .await
        .expect("recovery should succeed");

    assert_eq!(
        recovered_engine.state(),
        expected_state(10),
        "state should be sum(1..=10) = 55, not doubled"
    );
}

/// Recovery state matches sequential processing.
///
/// Sequential: process blocks 0-9, record final state.
/// Crash: process blocks 0-4, crash, recover to block 9.
/// Both paths must produce identical final state.
#[tokio::test]
async fn test_recovery_matches_sequential_processing() {
    let chain = make_chain(10);

    // --- Sequential processing ---
    let storage1 = InMemoryStorage::new();
    let mut sequential_engine = make_engine(storage1.clone(), 1000);
    for block in &chain {
        sequential_engine
            .process_block(block)
            .expect("sequential process_block");
    }
    let sequential_state = sequential_engine.state();

    // --- Crash + recovery ---
    let storage2 = InMemoryStorage::new();
    let mut pre_crash_engine = make_engine(storage2.clone(), 1000);
    for block in chain.iter().take(5) {
        pre_crash_engine
            .process_block(block)
            .expect("pre-crash process_block");
    }

    // Crash: new engine.
    let mut recovered_engine = make_engine(storage2.clone(), 1000);
    let source = MockBlockSource::new(chain.clone());

    recover(&mut recovered_engine, &source, 9)
        .await
        .expect("recovery should succeed");

    let recovered_state = recovered_engine.state();

    assert_eq!(
        sequential_state, recovered_state,
        "recovered state must match sequential processing (deterministic recovery)"
    );
}

/// Snapshot triggers during gap fetch.
///
/// Engine with snapshot_every_n = 3 and empty storage.
/// Cold start recovery with 9 blocks.
/// Snapshots should be created approximately every 3 blocks during gap fetch.
#[tokio::test]
async fn test_snapshot_triggers_during_gap_fetch() {
    let storage = InMemoryStorage::new();
    let chain = make_chain(9); // blocks 0-8

    let mut engine = make_engine(storage.clone(), 3);

    let source = MockBlockSource::new(chain.clone());

    // Cold start: fetch all 9 blocks from source. Snapshot every 3 blocks.
    recover(&mut engine, &source, 8)
        .await
        .expect("cold start recovery should succeed");

    // State = sum(1..=9) = 45
    assert_eq!(engine.state(), expected_state(9), "state should be sum(1..=9) = 45");

    // At least one snapshot should exist (triggered during gap fetch via process_block).
    // With 9 blocks and snapshot_every_n=3, snapshots are at blocks 3, 6, 9 (indices 2, 5, 8).
    // Check that find_nearest_snapshot from the tip finds at least one snapshot.
    let snap = storage
        .find_nearest_snapshot("test-source", &make_hash(9))
        .expect("find_nearest_snapshot");
    assert!(snap.is_some(), "at least one snapshot should exist after processing 9 blocks");
}
