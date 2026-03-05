use chainfold::cursor::EvmCursor;
use chainfold::event::Event;
use chainfold::machine::{ReducerCore, StateMachine};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// BalanceTracker — test implementation of StateMachine
// ---------------------------------------------------------------------------

/// Simple state machine that tracks a balance.
/// State = u128 (balance, always non-negative via saturating arithmetic)
/// Event = i128 (signed transfer: positive = deposit, negative = withdrawal)
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
        // Saturating arithmetic: balance can't go below 0.
        let balance = state as i128;
        balance.saturating_add(event.data).max(0) as u128
    }
}

fn make_event(block_number: u64, log_index: u32, amount: i128) -> Event<EvmCursor, i128> {
    Event {
        cursor: EvmCursor {
            block_number,
            log_index,
        },
        data: amount,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// After applying +100, +50, -30 the balance should be 120.
#[test]
fn apply_three_events_produces_correct_balance() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, receiver) = ReducerCore::new(sm, 0_u128);

    core.apply(&make_event(1, 0, 100));
    core.apply(&make_event(2, 0, 50));
    core.apply(&make_event(3, 0, -30));

    assert_eq!(*receiver.borrow(), 120, "expected balance of 120 after +100 +50 -30");
}

/// With no events applied, receiver should reflect the initial state.
#[test]
fn initial_state_reflected_without_events() {
    let sm = Arc::new(BalanceTracker);
    let (_, receiver) = ReducerCore::new(sm, 0_u128);

    assert_eq!(*receiver.borrow(), 0, "initial balance should be 0");
}

/// Initial state can be non-zero.
#[test]
fn initial_state_nonzero_preserved() {
    let sm = Arc::new(BalanceTracker);
    let (_, receiver) = ReducerCore::new(sm, 500_u128);

    assert_eq!(*receiver.borrow(), 500, "initial balance of 500 should be preserved");
}

/// apply_and_accumulate stores events in the events() slice.
#[test]
fn apply_and_accumulate_stores_events_in_order() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, _receiver) = ReducerCore::new(sm, 0_u128);

    core.apply_and_accumulate(make_event(1, 0, 100));
    core.apply_and_accumulate(make_event(2, 0, 50));
    core.apply_and_accumulate(make_event(3, 0, -30));

    let events = core.events();
    assert_eq!(events.len(), 3, "expected 3 accumulated events");
    assert_eq!(events[0].data, 100);
    assert_eq!(events[1].data, 50);
    assert_eq!(events[2].data, -30);
}

/// events_in_range filters by [from, to).
#[test]
fn events_in_range_filters_correctly() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, _receiver) = ReducerCore::new(sm, 0_u128);

    core.apply_and_accumulate(make_event(10, 0, 10));
    core.apply_and_accumulate(make_event(20, 0, 20));
    core.apply_and_accumulate(make_event(30, 0, 30));
    core.apply_and_accumulate(make_event(40, 0, 40));

    let from = EvmCursor {
        block_number: 20,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 40,
        log_index: 0,
    };

    let range: Vec<_> = core.events_in_range(&from, &to).collect();
    assert_eq!(range.len(), 2, "expected 2 events in [block 20, block 40)");
    assert_eq!(range[0].data, 20);
    assert_eq!(range[1].data, 30);
}

/// events_in_range with equal from and to produces empty iterator.
#[test]
fn events_in_range_empty_when_from_equals_to() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, _receiver) = ReducerCore::new(sm, 0_u128);

    core.apply_and_accumulate(make_event(10, 0, 10));

    let cursor = EvmCursor {
        block_number: 10,
        log_index: 0,
    };
    let range: Vec<_> = core.events_in_range(&cursor, &cursor).collect();
    assert!(range.is_empty(), "half-open range [c, c) should be empty");
}

/// subscribe() returns a new receiver that reflects the latest state.
#[test]
fn subscribe_returns_new_receiver_with_current_state() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, _original_receiver) = ReducerCore::new(sm, 0_u128);

    core.apply(&make_event(1, 0, 200));

    let new_receiver = core.subscribe();
    assert_eq!(*new_receiver.borrow(), 200, "subscribe() receiver should see current state");
}

/// Both receivers (original and from subscribe()) track the same state.
#[test]
fn multiple_receivers_track_same_state() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, receiver1) = ReducerCore::new(sm, 0_u128);
    let receiver2 = core.subscribe();

    core.apply(&make_event(1, 0, 300));

    assert_eq!(*receiver1.borrow(), 300);
    assert_eq!(*receiver2.borrow(), 300, "all receivers should see same state");
}

/// Accumulated events remain in cursor order after multiple apply_and_accumulate calls.
#[test]
fn accumulated_events_are_in_cursor_order() {
    let sm = Arc::new(BalanceTracker);
    let (mut core, _) = ReducerCore::new(sm, 0_u128);

    // Apply in ascending cursor order.
    for i in 0..5_u64 {
        core.apply_and_accumulate(make_event(i, 0, i as i128 * 10));
    }

    let events = core.events();
    assert_eq!(events.len(), 5);
    for i in 0..5_usize {
        assert_eq!(events[i].cursor.block_number, i as u64);
    }
}
