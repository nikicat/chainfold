//! Integration tests proving all 5 Phase 1 success criteria.
//!
//! Each test maps directly to a ROADMAP.md Phase 1 success criterion.
//!
//! CORE-01: EventSource trait abstraction
//! CORE-02: StateMachine pure reducer
//! CORE-03: Cursor Ord + Serialize round-trip
//! CORE-04: In-memory reducer determinism
//! CORE-05: Generic type parameter propagation (no runtime dispatch)

use chainfold::cursor::{BtcCursor, CexCursor, EvmCursor};
use chainfold::event::Event;
use chainfold::machine::{ReducerCore, StateMachine};
use chainfold::source::EventSource;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// BalanceTracker: a minimal StateMachine over EvmCursor for all tests.
/// State = u128 (balance). Event = i128 (signed transfer).
struct BalanceTracker;

impl StateMachine for BalanceTracker {
    type State = u128;
    type Event = i128;
    type Cursor = EvmCursor;

    fn apply(
        &self,
        state: Self::State,
        event: &Event<Self::Cursor, Self::Event>,
    ) -> Self::State {
        (state as i128).saturating_add(event.data).max(0) as u128
    }
}

fn evm_event(block_number: u64, log_index: u32, amount: i128) -> Event<EvmCursor, i128> {
    Event {
        cursor: EvmCursor {
            block_number,
            log_index,
        },
        data: amount,
        block_hash: None,
    }
}

/// MockEventSource: holds a fixed Vec of events, filters by cursor range.
/// E requires Clone + Send + Sync because EventSource requires Sync on the impl.
struct MockEventSource<C: chainfold::ChainCursor, E: Clone + Send + Sync + 'static> {
    events: Vec<Event<C, E>>,
    state_bytes: Vec<u8>,
}

#[async_trait::async_trait]
impl<C, E> EventSource for MockEventSource<C, E>
where
    C: chainfold::ChainCursor,
    E: Clone + Send + Sync + 'static,
{
    type Cursor = C;
    type Event = E;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &C) -> Result<Vec<u8>, Self::Error> {
        Ok(self.state_bytes.clone())
    }

    async fn fetch_events(
        &self,
        from: &C,
        to: &C,
    ) -> Result<Vec<Event<C, E>>, Self::Error> {
        let filtered = self
            .events
            .iter()
            .filter(|e| &e.cursor >= from && &e.cursor < to)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn subscribe(
        &self,
        _from: &C,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Event<C, E>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        Ok(Box::new(futures_util::stream::empty()))
    }
}

// ---------------------------------------------------------------------------
// CORE-01: EventSource trait abstraction
// ---------------------------------------------------------------------------

/// Generic helper that uses EventSource without knowing the concrete type.
/// This proves the trait is a proper abstraction.
async fn fetch_from_source<S: EventSource>(
    source: &S,
    from: &S::Cursor,
    to: &S::Cursor,
) -> Vec<Event<S::Cursor, S::Event>> {
    source.fetch_events(from, to).await.unwrap()
}

/// CORE-01: A caller can implement the EventSource trait for a mock chain and pass it to
/// library code without knowing about EVM or Bitcoin specifics.
#[tokio::test]
async fn core_01_event_source_trait_abstraction() {
    let source = MockEventSource {
        events: vec![
            Event {
                cursor: EvmCursor {
                    block_number: 10,
                    log_index: 0,
                },
                data: 100_u64,
                block_hash: None,
            },
            Event {
                cursor: EvmCursor {
                    block_number: 20,
                    log_index: 0,
                },
                data: 200_u64,
                block_hash: None,
            },
            Event {
                cursor: EvmCursor {
                    block_number: 30,
                    log_index: 0,
                },
                data: 300_u64,
                block_hash: None,
            },
        ],
        state_bytes: vec![],
    };

    // Call a generic function — no knowledge of EvmCursor inside fetch_from_source.
    let from = EvmCursor {
        block_number: 10,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 25,
        log_index: 0,
    };
    let events = fetch_from_source(&source, &from, &to).await;

    assert_eq!(events.len(), 2, "CORE-01: generic fetch returned wrong number of events");
    assert_eq!(events[0].data, 100);
    assert_eq!(events[1].data, 200);

    // Verify fetch_state also works through the abstraction.
    let source_with_state = MockEventSource::<EvmCursor, u64> {
        events: vec![],
        state_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
    };
    let bytes = source_with_state
        .fetch_state(&EvmCursor {
            block_number: 0,
            log_index: 0,
        })
        .await
        .unwrap();
    assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

// ---------------------------------------------------------------------------
// CORE-02: StateMachine pure reducer
// ---------------------------------------------------------------------------

/// CORE-02: A caller can implement the StateMachine trait with a pure Apply reducer and
/// have the library drive state forward via apply(state, event) without any I/O.
#[test]
fn core_02_state_machine_pure_reducer() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, receiver) = ReducerCore::new(sm, 0_u128);

    // Apply +1000, -250, +75 — all synchronous, no await anywhere.
    core.apply(&evm_event(1, 0, 1000));
    core.apply(&evm_event(2, 0, -250));
    core.apply(&evm_event(3, 0, 75));

    assert_eq!(
        *receiver.borrow(),
        825,
        "CORE-02: expected balance of 825 after +1000 -250 +75"
    );

    // Verify apply is synchronous — no async machinery involved.
    // (If this code compiles and runs without tokio::spawn or .await, it proves it's sync.)
    let sm2 = Arc::new(BalanceTracker);
    let (mut core2, receiver2) = ReducerCore::new(sm2, 0_u128);
    for amount in [1000_i128, -250, 75] {
        core2.apply(&evm_event(1, 0, amount));
    }
    assert_eq!(*receiver2.borrow(), 825, "CORE-02: second core should also reach 825");
}

// ---------------------------------------------------------------------------
// CORE-03: Cursor Ord + Serialize round-trip
// ---------------------------------------------------------------------------

/// CORE-03: Cursors from two different positions can be compared with Ord and serialized
/// round-trip without loss.
#[test]
fn core_03_cursor_ord_and_serde_roundtrip() {
    // EvmCursor: ordering is by (block_number, log_index) lexicographically.
    let evm_early = EvmCursor {
        block_number: 100,
        log_index: 0,
    };
    let evm_late = EvmCursor {
        block_number: 200,
        log_index: 0,
    };
    assert!(evm_early < evm_late, "CORE-03: block 100 should be < block 200");

    // EvmCursor serde round-trip.
    let json = serde_json::to_string(&evm_early).unwrap();
    let restored: EvmCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(evm_early, restored, "CORE-03: EvmCursor round-trip failed");

    // BtcCursor: ordering by (block_height, tx_index).
    let btc_early = BtcCursor {
        block_height: 800_000,
        tx_index: 0,
    };
    let btc_late = BtcCursor {
        block_height: 800_001,
        tx_index: 0,
    };
    assert!(btc_early < btc_late, "CORE-03: btc block 800000 should be < 800001");

    let json = serde_json::to_string(&btc_early).unwrap();
    let restored: BtcCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(btc_early, restored, "CORE-03: BtcCursor round-trip failed");

    // CexCursor: ordering by seq_id ONLY (timestamp_ms is ignored for ordering).
    let cex_first = CexCursor {
        seq_id: 1,
        timestamp_ms: 9999, // high timestamp, but seq_id is lower
    };
    let cex_second = CexCursor {
        seq_id: 2,
        timestamp_ms: 1, // low timestamp, but seq_id is higher
    };
    assert!(
        cex_first < cex_second,
        "CORE-03: CexCursor ordering must be by seq_id only, not timestamp"
    );

    let json = serde_json::to_string(&cex_first).unwrap();
    let restored: CexCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cex_first, restored, "CORE-03: CexCursor round-trip failed");

    // Verify CexCursor with same seq_id but different timestamp_ms are equal in ordering.
    let cex_a = CexCursor {
        seq_id: 5,
        timestamp_ms: 100,
    };
    let cex_b = CexCursor {
        seq_id: 5,
        timestamp_ms: 999,
    };
    assert_eq!(
        cex_a.cmp(&cex_b),
        std::cmp::Ordering::Equal,
        "CORE-03: CexCursors with same seq_id must compare equal regardless of timestamp"
    );
}

// ---------------------------------------------------------------------------
// CORE-04: In-memory reducer determinism
// ---------------------------------------------------------------------------

/// CORE-04: An in-memory reducer accumulates events correctly in the order they are applied,
/// producing a deterministic final state verifiable by a unit test.
#[test]
fn core_04_reducer_accumulation_determinism() {
    // Sequence of 10 events with known values.
    let events: Vec<i128> = vec![100, 200, -50, 300, -100, 50, 400, -200, 150, -75];
    // Hand-calculated: 100+200-50+300-100+50+400-200+150-75 = 775
    let expected: u128 = 775;

    let sm1 = Arc::new(BalanceTracker);
    let (mut core1, receiver1) = ReducerCore::new(sm1, 0_u128);

    for (i, &amount) in events.iter().enumerate() {
        core1.apply_and_accumulate(evm_event(i as u64, 0, amount));
    }

    assert_eq!(
        *receiver1.borrow(),
        expected,
        "CORE-04: first run expected {expected}"
    );

    // Apply the SAME sequence to a fresh core — must produce identical result.
    let sm2 = Arc::new(BalanceTracker);
    let (mut core2, receiver2) = ReducerCore::new(sm2, 0_u128);

    for (i, &amount) in events.iter().enumerate() {
        core2.apply_and_accumulate(evm_event(i as u64, 0, amount));
    }

    assert_eq!(
        *receiver2.borrow(),
        expected,
        "CORE-04: second run expected same {expected} (determinism)"
    );

    // Verify both cores accumulated events in identical order.
    assert_eq!(
        core1.events().len(),
        core2.events().len(),
        "CORE-04: both cores should have same number of accumulated events"
    );
    for i in 0..core1.events().len() {
        assert_eq!(
            core1.events()[i].data,
            core2.events()[i].data,
            "CORE-04: accumulated event {i} should be identical between runs"
        );
    }
}

// ---------------------------------------------------------------------------
// CORE-05: Generic type parameter propagation (no runtime dispatch)
// ---------------------------------------------------------------------------

/// A generic pipeline wrapping ReducerCore — proves the generic propagates without dyn.
struct Pipeline<S: StateMachine> {
    core: ReducerCore<S>,
}

impl<S: StateMachine> Pipeline<S> {
    fn new(sm: Arc<S>, initial: S::State) -> (Self, tokio::sync::watch::Receiver<S::State>) {
        let (core, rx) = ReducerCore::new(sm, initial);
        (Pipeline { core }, rx)
    }

    fn process(&mut self, event: &Event<S::Cursor, S::Event>) {
        self.core.apply(event);
    }
}

/// Generic function operating on any StateMachine — no dyn, no Box.
fn apply_sequence<S: StateMachine>(
    sm: Arc<S>,
    initial: S::State,
    events: &[Event<S::Cursor, S::Event>],
) -> S::State {
    let (mut core, rx) = ReducerCore::new(sm, initial);
    for event in events {
        core.apply(event);
    }
    let state = rx.borrow().clone();
    state
}

/// CORE-05: The library compiles with the generic type parameter C: ChainCursor propagated
/// through the type system without requiring runtime dispatch.
#[test]
fn core_05_generic_type_propagation_no_runtime_dispatch() {
    // Instantiate Pipeline<BalanceTracker> — generic parameter propagates through type system.
    let sm = Arc::new(BalanceTracker);
    let (mut pipeline, receiver) = Pipeline::new(sm, 0_u128);

    pipeline.process(&evm_event(1, 0, 500));
    pipeline.process(&evm_event(2, 0, -100));

    assert_eq!(
        *receiver.borrow(),
        400,
        "CORE-05: pipeline should produce 400 from +500 -100"
    );

    // apply_sequence: generic function with no dyn or Box — pure static dispatch.
    let events = vec![
        evm_event(1, 0, 1000),
        evm_event(2, 0, -200),
        evm_event(3, 0, 50),
    ];
    let final_state = apply_sequence(Arc::new(BalanceTracker), 0_u128, &events);
    assert_eq!(
        final_state, 850,
        "CORE-05: apply_sequence should produce 850 from +1000 -200 +50"
    );
}
