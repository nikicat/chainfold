pub mod query;

use std::sync::Arc;
use tokio::sync::watch;

/// Reactive read-only access to state managed by an Engine.
///
/// Stores decouple state queries from the Engine's live loop:
/// - `get()` returns the current state (sync, cheap clone from watch channel)
/// - `subscribe()` returns a `watch::Receiver` for async change notifications
///
/// The Engine updates state internally; stores expose it to callers without
/// requiring a reference to Engine. Multiple stores and subscribers can
/// coexist concurrently with the live loop.
pub trait Store: Send + Sync {
    /// The state type exposed by this store.
    type State: Clone + Send + 'static;

    /// Get the current state. Sync and cheap (clones from watch channel).
    fn get(&self) -> Self::State;

    /// Subscribe to state changes. Returns a `watch::Receiver` that can
    /// be polled with `changed().await` to receive notifications.
    fn subscribe(&self) -> watch::Receiver<Self::State>;
}

/// A store backed by a `watch::Sender`, driven by a ReducerCore inside Engine.
///
/// Holds `Arc<watch::Sender<S>>` — the same sender that ReducerCore uses
/// to broadcast state changes. This allows ReducedStore to:
/// - Read the current value via `sender.borrow().clone()` (get)
/// - Create new subscribers via `sender.subscribe()` (subscribe)
///
/// Multiple ReducedStore instances can coexist (they share the Arc).
/// The store is read-only from the caller's perspective — only Engine
/// (via ReducerCore) writes to the sender.
pub struct ReducedStore<S: Clone + Send + 'static> {
    sender: Arc<watch::Sender<S>>,
}

impl<S: Clone + Send + 'static> ReducedStore<S> {
    /// Create a new ReducedStore wrapping a shared watch::Sender.
    ///
    /// The sender is typically obtained from Engine via `engine.reduced_store()`.
    pub fn new(sender: Arc<watch::Sender<S>>) -> Self {
        Self { sender }
    }
}

impl<S: Clone + Send + Sync + 'static> Store for ReducedStore<S> {
    type State = S;

    fn get(&self) -> S {
        self.sender.borrow().clone()
    }

    fn subscribe(&self) -> watch::Receiver<S> {
        self.sender.subscribe()
    }
}
