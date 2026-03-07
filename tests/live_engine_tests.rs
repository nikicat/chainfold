/// Integration tests for Engine::run() — the live event loop.
///
/// Tests cover:
/// - Live stream consumption: engine processes all blocks from a live stream
/// - Cancellation: run() returns Ok(()) when CancellationToken is cancelled
/// - Reconnect on stream end: engine re-subscribes after stream terminates
/// - Max retries exhausted: subscribe always fails, Err(ReconnectExhausted) returned
/// - Gap fill on reconnect: engine fills missed blocks between stream sessions
/// - Status callbacks: Disconnected, Reconnecting, Reconnected, GapFilling fire correctly
/// - Reorg during live stream: process_or_reorg handles reorg transparently
///
/// # Termination patterns used in tests
///
/// Since Engine::run() is a long-lived loop, tests use two termination patterns:
///
/// **Pattern A — Natural exhaustion (max_retries = 0):** The stream is finite. When
/// it ends, run() tries to reconnect. MockStreamSource has no more streams, so
/// subscribe() fails. With max_retries = 0, run() returns Err(ReconnectExhausted)
/// immediately. State is inspected after run() returns.
///
/// **Pattern B — Cancellation via watcher:** A tokio::spawn'd task watches the
/// engine's state via watch::Receiver and cancels the CancellationToken once the
/// expected state is reached. This works because subscribe().await (the async mock)
/// provides a yield point for the watcher to run.
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Mutex,
};

use chainfold::coordinator::EngineStatus;
use chainfold::storage::InMemoryStorage;
use chainfold::{Block, Codec, Engine, EngineConfig, Event, ReorgEvent, ReducerCore, StateMachine};
use tokio_util::sync::CancellationToken;

// ============================================================
// Test infrastructure: SumMachine + SumCodec (same as other tests)
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

// ============================================================
// Block helpers
// ============================================================

/// Create a block hash where byte[0] = n and the rest are 0.
fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// Genesis sentinel: all-zeros (no parent).
const GENESIS_PARENT: [u8; 32] = [0u8; 32];

/// Build a linear chain of n blocks.
///
/// Block at index i:
/// - block_hash: [i+1, 0, 0, ...] (1-indexed to avoid collision with GENESIS_PARENT)
/// - parent_hash: [i, 0, 0, ...] (genesis block's parent = all-zeros sentinel)
/// - height: i as u64
/// - event value: (i+1) as u64 — sum(1..=n) = n*(n+1)/2
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
                data: (i + 1) as u64,
            }],
        })
        .collect()
}

/// Create an engine with the shared InMemoryStorage.
///
/// `max_retries` controls how many reconnect retries are allowed before
/// Engine::run() returns Err(ReconnectExhausted). Set to 0 for "fail fast"
/// tests. Delays are always 0ms for fast test execution.
fn make_engine(
    storage: InMemoryStorage,
    max_retries: u32,
) -> Engine<SumMachine, InMemoryStorage, SumCodec> {
    let sm = Arc::new(SumMachine);
    let (core, _rx) = ReducerCore::new(sm, 0u64);
    let mut config = EngineConfig::new("test-source", 1000);
    config.max_retries = max_retries;
    config.initial_delay_ms = 0;
    config.max_delay_ms = 0;
    config.batch_size = 64;
    Engine::new(core, Arc::new(storage), SumCodec, config)
}

// ============================================================
// MockStreamSource
// ============================================================

/// Error type for MockStreamSource.
#[derive(Debug, thiserror::Error)]
#[error("mock subscribe error: no more streams")]
struct MockError;

/// Mock EventSource that serves pre-configured streams and a block catalog.
///
/// - `subscribe(from_height)`: pops the first entry from `streams`. Each entry
///   is a Vec<Block> converted to an infallible stream. Returns Err when empty.
/// - `fetch_blocks(from, to)`: returns blocks from `all_blocks` in [from, to).
/// - `subscribe_count`: tracks number of subscribe() calls (for assertions).
struct MockStreamSource {
    /// Pre-configured streams, consumed in order on each subscribe() call.
    streams: Mutex<Vec<Vec<Block<TestCursor, u64>>>>,
    /// All blocks available for fetch_blocks() gap fill.
    all_blocks: Vec<Block<TestCursor, u64>>,
    /// Number of subscribe() calls made so far.
    subscribe_count: AtomicU32,
}

impl MockStreamSource {
    /// Create a source with a single stream and no blocks for gap fill.
    fn single_stream(blocks: Vec<Block<TestCursor, u64>>) -> Self {
        Self {
            streams: Mutex::new(vec![blocks]),
            all_blocks: Vec::new(),
            subscribe_count: AtomicU32::new(0),
        }
    }

    /// Create a source with multiple streams and a full block catalog for gap fill.
    fn multi_stream(
        streams: Vec<Vec<Block<TestCursor, u64>>>,
        all_blocks: Vec<Block<TestCursor, u64>>,
    ) -> Self {
        Self {
            streams: Mutex::new(streams),
            all_blocks,
            subscribe_count: AtomicU32::new(0),
        }
    }

    /// Create a source where all subscribe calls fail immediately.
    fn always_fail() -> Self {
        Self {
            streams: Mutex::new(Vec::new()),
            all_blocks: Vec::new(),
            subscribe_count: AtomicU32::new(0),
        }
    }

    fn subscribe_call_count(&self) -> u32 {
        self.subscribe_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl chainfold::EventSource for MockStreamSource {
    type Cursor = TestCursor;
    type Event = u64;
    type Error = MockError;

    async fn fetch_state(&self, _at: &TestCursor) -> Result<Vec<u8>, MockError> {
        Ok(0u64.to_le_bytes().to_vec())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<TestCursor, u64>>, MockError> {
        let filtered: Vec<Block<TestCursor, u64>> = self
            .all_blocks
            .iter()
            .filter(|b| b.height >= from_height && b.height < to_height)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<TestCursor, u64>, MockError>,
                > + Unpin
                + Send,
        >,
        MockError,
    > {
        self.subscribe_count.fetch_add(1, Ordering::SeqCst);

        let mut streams = self.streams.lock().expect("lock poisoned");
        if streams.is_empty() {
            return Err(MockError);
        }

        let blocks = streams.remove(0);
        let stream = futures_util::stream::iter(blocks.into_iter().map(Ok));
        Ok(Box::new(stream))
    }
}

// ============================================================
// Tests
// ============================================================

/// Live stream consumption (Pattern A — natural exhaustion):
/// Engine processes all 5 blocks from a single finite stream.
/// After stream ends, subscribe fails (max_retries=0), run returns ReconnectExhausted.
/// State must be sum(1..=5) = 15.
#[tokio::test]
async fn test_live_stream_consumption() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let blocks = make_chain(5);
    let source = MockStreamSource::single_stream(blocks);

    let token = CancellationToken::new();

    let result = engine.run(&source, token, |_| {}, |_| {}).await;

    // With max_retries = 0, run() returns Err(ReconnectExhausted) after stream ends.
    // The important thing is that ALL blocks were processed before the error.
    assert!(
        matches!(result, Err(chainfold::EngineError::ReconnectExhausted(0, _))),
        "run() should return ReconnectExhausted(0) after stream ends with max_retries=0, got: {:?}",
        result
    );
    assert_eq!(engine.state(), 15, "state should be sum(1..=5) = 15");
}

/// Cancellation (Pattern B — pre-cancelled token):
/// Token is cancelled before run() is called. Engine exits cleanly on the first
/// check inside the loop — before even subscribing.
#[tokio::test]
async fn test_cancellation_before_subscribe() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let blocks = make_chain(5);
    let source = MockStreamSource::single_stream(blocks);

    let token = CancellationToken::new();
    // Cancel before calling run().
    token.cancel();

    let result = engine.run(&source, token, |_| {}, |_| {}).await;
    assert!(result.is_ok(), "run() should return Ok(()) when token is pre-cancelled");
    assert_eq!(engine.state(), 0, "no blocks should be processed with pre-cancelled token");
}

/// Reconnect on stream end (Pattern A — natural exhaustion):
/// Stream 1 delivers blocks 0-2, ends. Engine reconnects to stream 2.
/// Stream 2 delivers blocks 3-4, ends. Third reconnect fails (max_retries=0).
/// Final state = sum(1..=5) = 15.
///
/// subscribe is called 3 times total (stream1, stream2, failing reconnect).
#[tokio::test]
async fn test_reconnect_on_stream_end() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let chain = make_chain(5);
    // Stream 1: blocks 0-2 (heights 0-2)
    let stream1 = chain[..3].to_vec();
    // Stream 2: blocks 3-4 (heights 3-4)
    let stream2 = chain[3..].to_vec();

    // Multi-stream with no all_blocks (no gap fill needed since streams are contiguous).
    let source = MockStreamSource::multi_stream(vec![stream1, stream2], Vec::new());

    let token = CancellationToken::new();
    let result = engine.run(&source, token, |_| {}, |_| {}).await;

    // After stream 1 + stream 2 are consumed, third subscribe fails → ReconnectExhausted(0).
    assert!(
        matches!(result, Err(chainfold::EngineError::ReconnectExhausted(0, _))),
        "run() should return ReconnectExhausted after all streams consumed: {:?}",
        result
    );
    assert_eq!(engine.state(), 15, "state should be sum(1..=5) = 15 across two streams");

    // Subscribe called at least twice (stream1 + stream2; third attempt also counted).
    assert!(
        source.subscribe_call_count() >= 2,
        "subscribe should have been called at least twice, got: {}",
        source.subscribe_call_count()
    );
}

/// Max retries exhausted: subscribe always fails, engine returns ReconnectExhausted
/// after max_retries attempts.
///
/// With max_retries = 2:
/// - attempt=0: subscribe fails → Reconnecting(1) → backoff(0ms) → attempt=1
/// - attempt=1: subscribe fails → Reconnecting(2) → backoff(0ms) → attempt=2
/// - attempt=2: subscribe fails → 2 >= max_retries(2) → ReconnectExhausted(2)
#[tokio::test]
async fn test_max_retries_exhausted() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 2);

    let source = MockStreamSource::always_fail();

    let statuses: Arc<Mutex<Vec<EngineStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let statuses_clone = statuses.clone();

    let token = CancellationToken::new();

    let result = engine
        .run(
            &source,
            token,
            |_| {},
            |s| {
                statuses_clone.lock().unwrap().push(s);
            },
        )
        .await;

    // Should return ReconnectExhausted(2) — exhausted at attempt 2.
    match result {
        Err(chainfold::EngineError::ReconnectExhausted(count, _)) => {
            assert_eq!(count, 2, "should exhaust at attempt count = max_retries = 2");
        }
        other => panic!("expected ReconnectExhausted, got: {:?}", other),
    }

    // Status callbacks: Reconnecting(1) and Reconnecting(2) should have fired.
    let s = statuses.lock().unwrap();
    assert_eq!(s.len(), 2, "exactly 2 Reconnecting statuses expected");
    assert!(
        matches!(s[0], EngineStatus::Reconnecting(1)),
        "first status should be Reconnecting(1), got: {:?}",
        s[0]
    );
    assert!(
        matches!(s[1], EngineStatus::Reconnecting(2)),
        "second status should be Reconnecting(2), got: {:?}",
        s[1]
    );
}

/// Gap fill on reconnect (Pattern A — natural exhaustion):
/// Stream 1 delivers heights 0-4. Stream 2 starts at height 7 (gap at 5-6).
/// Engine detects gap, fills heights 5-6 via fetch_blocks, then processes 7-9.
/// After stream 2 ends, third reconnect fails (max_retries=0) → ReconnectExhausted.
/// Final state = sum(1..=10) = 55.
#[tokio::test]
async fn test_gap_fill_on_reconnect() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let full_chain = make_chain(10); // heights 0-9, values 1-10

    // Stream 1: heights 0-4 (blocks 0..5)
    let stream1 = full_chain[..5].to_vec();
    // Stream 2: heights 7-9 (gap: heights 5-6 are missing from the stream)
    let stream2 = full_chain[7..].to_vec();

    let source = MockStreamSource::multi_stream(
        vec![stream1, stream2],
        full_chain.clone(), // all_blocks: used for gap fill of heights 5-6
    );

    let statuses: Arc<Mutex<Vec<EngineStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let statuses_clone = statuses.clone();

    let token = CancellationToken::new();
    let result = engine
        .run(
            &source,
            token,
            |_| {},
            |s| {
                statuses_clone.lock().unwrap().push(s);
            },
        )
        .await;

    // After stream1 + gap fill + stream2, third subscribe fails → ReconnectExhausted(0).
    assert!(
        matches!(result, Err(chainfold::EngineError::ReconnectExhausted(0, _))),
        "expected ReconnectExhausted(0) after all streams consumed: {:?}",
        result
    );
    assert_eq!(engine.state(), 55, "state should be sum(1..=10) = 55 after gap fill");

    // GapFilling status must have fired.
    let s = statuses.lock().unwrap();
    assert!(
        s.iter().any(|e| matches!(e, EngineStatus::GapFilling)),
        "GapFilling status should have fired, statuses: {:?}",
        s.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>()
    );
}

/// Status callbacks — Disconnected fires when stream ends (Pattern A):
/// Stream 1 ends → Disconnected fires.
/// Stream 2 delivers remaining blocks and ends → Disconnected fires again.
/// Third subscribe fails → ReconnectExhausted.
#[tokio::test]
async fn test_disconnected_status_fires() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let chain = make_chain(5);
    let stream1 = chain[..3].to_vec();
    let stream2 = chain[3..].to_vec();

    let source = MockStreamSource::multi_stream(vec![stream1, stream2], Vec::new());

    let statuses: Arc<Mutex<Vec<EngineStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let statuses_clone = statuses.clone();

    let token = CancellationToken::new();
    let _ = engine
        .run(
            &source,
            token,
            |_| {},
            |s| {
                statuses_clone.lock().unwrap().push(s);
            },
        )
        .await; // We don't care about the Err result here

    // Disconnected must have fired after each stream ended.
    let s = statuses.lock().unwrap();
    let disconnected_count = s.iter().filter(|e| matches!(e, EngineStatus::Disconnected)).count();
    assert!(
        disconnected_count >= 1,
        "Disconnected should fire when stream ends, count: {}",
        disconnected_count
    );
    assert_eq!(engine.state(), 15, "state should be sum(1..=5) = 15");
}

/// Reconnected fires after a successful retry following a subscribe failure.
///
/// Pattern: source fails on first subscribe, succeeds on second (FailOnceThenSucceed).
/// After first failure: Reconnecting(1) fires, sleep(0ms).
/// After second success: Reconnected fires (because attempt > 0 at subscribe success).
/// Stream delivers 3 blocks, ends. Third subscribe fails → ReconnectExhausted.
#[tokio::test]
async fn test_reconnected_status_after_retry() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 1); // 1 retry allowed

    let chain = make_chain(3);

    let source = FailOnceThenSucceedSource {
        fail_count: AtomicU32::new(0),
        blocks: chain.clone(),
    };

    let statuses: Arc<Mutex<Vec<EngineStatus>>> = Arc::new(Mutex::new(Vec::new()));
    let statuses_clone = statuses.clone();

    let token = CancellationToken::new();
    let _ = engine
        .run(
            &source,
            token,
            |_| {},
            |s| {
                statuses_clone.lock().unwrap().push(s);
            },
        )
        .await; // We don't assert the result; the stream ends and reconnects until exhaustion.

    // After first subscribe fails: Reconnecting(1) must have fired.
    let s = statuses.lock().unwrap();
    assert!(
        s.iter().any(|e| matches!(e, EngineStatus::Reconnecting(1))),
        "Reconnecting(1) should fire after first subscribe failure, statuses: {:?}",
        s.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>()
    );
    // After second subscribe succeeds: Reconnected must have fired.
    assert!(
        s.iter().any(|e| matches!(e, EngineStatus::Reconnected)),
        "Reconnected should fire after successful retry, statuses: {:?}",
        s.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>()
    );

    // All 3 blocks should have been processed.
    assert_eq!(engine.state(), 6, "state should be sum(1..=3) = 6");
}

/// Reorg during live stream (Pattern A — natural exhaustion):
/// Stream delivers A(h=0, val=10), B(h=1, val=20), C(h=2, val=30),
/// then D(h=2, parent=B — reorg) with val=100.
/// Engine detects reorg, on_reorg fires, state = A+B+D = 130.
#[tokio::test]
async fn test_reorg_during_live_stream() {
    let storage = InMemoryStorage::new();
    let mut engine = make_engine(storage.clone(), 0);

    let block_a = Block {
        block_hash: make_hash(1),
        parent_hash: GENESIS_PARENT,
        height: 0,
        events: vec![Event {
            cursor: TestCursor(0),
            data: 10u64,
        }],
    };
    let block_b = Block {
        block_hash: make_hash(2),
        parent_hash: make_hash(1),
        height: 1,
        events: vec![Event {
            cursor: TestCursor(1),
            data: 20u64,
        }],
    };
    let block_c = Block {
        block_hash: make_hash(3),
        parent_hash: make_hash(2),
        height: 2,
        events: vec![Event {
            cursor: TestCursor(2),
            data: 30u64,
        }],
    };
    // D: parent=B (not C) — triggers a reorg rolling back C.
    let block_d = Block {
        block_hash: make_hash(4),
        parent_hash: make_hash(2),
        height: 2,
        events: vec![Event {
            cursor: TestCursor(3),
            data: 100u64,
        }],
    };

    let stream = vec![block_a, block_b, block_c, block_d];
    let source = MockStreamSource::single_stream(stream);

    let reorg_events: Arc<Mutex<Vec<ReorgEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let reorg_clone = reorg_events.clone();

    let token = CancellationToken::new();
    let result = engine
        .run(
            &source,
            token,
            |e| {
                reorg_clone.lock().unwrap().push(e);
            },
            |_| {},
        )
        .await;

    // With max_retries = 0, run() returns Err(ReconnectExhausted) after stream ends.
    assert!(
        matches!(result, Err(chainfold::EngineError::ReconnectExhausted(0, _))),
        "expected ReconnectExhausted(0) after stream ends: {:?}",
        result
    );

    // State = A.val + B.val + D.val = 10 + 20 + 100 = 130 (C was rolled back).
    assert_eq!(engine.state(), 130, "state should be A+B+D = 10+20+100 = 130");

    let reorgs = reorg_events.lock().unwrap();
    assert_eq!(reorgs.len(), 1, "on_reorg should fire exactly once");
    assert_eq!(reorgs[0].common_ancestor, make_hash(2), "common ancestor should be B");
    assert_eq!(reorgs[0].old_tip, make_hash(3), "old_tip should be C");
    assert_eq!(reorgs[0].new_tip, make_hash(4), "new_tip should be D");
    assert_eq!(reorgs[0].depth, 1, "reorg depth should be 1");
}

// ============================================================
// FailOnceThenSucceedSource
// ============================================================

/// EventSource that fails the first subscribe() call, then succeeds on the second
/// (returning the configured block stream), then fails on all subsequent calls.
struct FailOnceThenSucceedSource {
    fail_count: AtomicU32,
    blocks: Vec<Block<TestCursor, u64>>,
}

#[async_trait::async_trait]
impl chainfold::EventSource for FailOnceThenSucceedSource {
    type Cursor = TestCursor;
    type Event = u64;
    type Error = MockError;

    async fn fetch_state(&self, _at: &TestCursor) -> Result<Vec<u8>, MockError> {
        Ok(0u64.to_le_bytes().to_vec())
    }

    async fn fetch_blocks(
        &self,
        _from_height: u64,
        _to_height: u64,
    ) -> Result<Vec<Block<TestCursor, u64>>, MockError> {
        Ok(Vec::new())
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<TestCursor, u64>, MockError>,
                > + Unpin
                + Send,
        >,
        MockError,
    > {
        let call_number = self.fail_count.fetch_add(1, Ordering::SeqCst);

        if call_number == 0 {
            // First call: fail
            return Err(MockError);
        }

        if call_number == 1 {
            // Second call: return the block stream
            let blocks = self.blocks.clone();
            let stream = futures_util::stream::iter(blocks.into_iter().map(Ok));
            return Ok(Box::new(stream));
        }

        // All subsequent calls fail.
        Err(MockError)
    }
}
