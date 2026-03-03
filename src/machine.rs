use crate::cursor::ChainCursor;
use crate::event::Event;
use std::sync::Arc;
use tokio::sync::watch;

/// Client implements this trait. Apply must be pure: no I/O, no side effects, deterministic.
/// Infallible: if an event is irrelevant, return state unchanged.
///
/// The library drives state forward by calling `apply` for each incoming event.
/// The implementor never calls I/O or mutates shared state — all effects are side-effect-free.
pub trait StateMachine: Send + Sync + 'static {
    /// The accumulated state type. Requires only `Clone + Send` in Phase 1.
    type State: Clone + Send + 'static;

    /// The user-defined event payload type.
    type Event: Send + 'static;

    /// The cursor type for this source.
    type Cursor: ChainCursor;

    /// Pure reducer. Given current state and an event, return next state.
    ///
    /// Must be deterministic: same inputs always produce the same output.
    /// Must be infallible: if an event is irrelevant, return `state` unchanged.
    fn apply(&self, state: Self::State, event: &Event<Self::Cursor, Self::Event>) -> Self::State;
}

/// Core state management for a `StateMachine`.
///
/// Drives state forward by applying events through the state machine's pure reducer.
/// State changes are broadcast to all subscribers via a `tokio::sync::watch` channel.
///
/// # Two application modes
///
/// - `apply(&Event)` — borrows the event, applies it, does NOT store it.
///   Use for streaming live events when history need not be kept in memory.
///
/// - `apply_and_accumulate(Event)` — takes ownership of the event, applies it,
///   and stores it in an in-memory `Vec`. Use for accumulated state tracking (CORE-05).
///   Phase 2 will replace the `Vec` with SQLite-backed storage.
///
/// # State access
///
/// Call `subscribe()` or use the `Receiver` returned by `new()` to watch state changes.
/// The watch channel always holds the latest state — no historical state is kept.
pub struct ReducerCore<SM: StateMachine> {
    sm: Arc<SM>,
    sender: watch::Sender<SM::State>,
    /// Accumulated events in application order (cursor order if events are applied in order).
    /// Phase 1: in-memory Vec. Phase 2 will replace with SQLite-backed storage.
    events: Vec<Event<SM::Cursor, SM::Event>>,
}

impl<SM: StateMachine> ReducerCore<SM> {
    /// Create a new ReducerCore with the given state machine and initial state.
    ///
    /// Returns `(core, receiver)` — the receiver immediately reflects `initial_state`.
    pub fn new(sm: Arc<SM>, initial_state: SM::State) -> (Self, watch::Receiver<SM::State>) {
        let (sender, receiver) = watch::channel(initial_state);
        let core = ReducerCore {
            sm,
            sender,
            events: Vec::new(),
        };
        (core, receiver)
    }

    /// Replace the current state with the given value, broadcasting to all watch receivers.
    ///
    /// Used by the recovery pipeline to restore state from a loaded snapshot.
    /// All existing watch receivers will see the new state immediately.
    pub fn reset_state(&mut self, state: SM::State) {
        let _ = self.sender.send(state);
    }

    /// Apply an event to advance state. Borrows the event — caller retains ownership.
    ///
    /// The new state is sent to all watch receivers immediately.
    pub fn apply(&mut self, event: &Event<SM::Cursor, SM::Event>) {
        let current = self.sender.borrow().clone();
        let next = self.sm.apply(current, event);
        // send() only fails if all receivers are dropped; we ignore that case
        // since the core can outlive individual subscribers.
        let _ = self.sender.send(next);
    }

    /// Apply an event and store it in the accumulated event log.
    /// Takes ownership of the event.
    ///
    /// The new state is sent to all watch receivers immediately.
    /// The event is appended to the internal `Vec` and retrievable via `events()`.
    pub fn apply_and_accumulate(&mut self, event: Event<SM::Cursor, SM::Event>) {
        // Borrow for apply, then move into Vec.
        self.apply(&event);
        self.events.push(event);
    }

    /// Return a new watch receiver subscribed to state changes.
    ///
    /// The returned receiver immediately holds the current state.
    pub fn subscribe(&self) -> watch::Receiver<SM::State> {
        self.sender.subscribe()
    }

    /// Read-only access to accumulated events.
    ///
    /// Returns all events stored via `apply_and_accumulate`, in application order.
    pub fn events(&self) -> &[Event<SM::Cursor, SM::Event>] {
        &self.events
    }

    /// Iterate over accumulated events in the cursor range `[from, to)`.
    ///
    /// Linear scan — O(n). Phase 2 will replace with a SQLite index lookup.
    pub fn events_in_range<'a>(
        &'a self,
        from: &'a SM::Cursor,
        to: &'a SM::Cursor,
    ) -> impl Iterator<Item = &'a Event<SM::Cursor, SM::Event>> {
        self.events
            .iter()
            .filter(move |e| &e.cursor >= from && &e.cursor < to)
    }
}
