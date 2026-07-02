//! [`RangeIndexStore`] — the generic reactive layer over a [`RangeIndex`],
//! written once for every [`RangeSpec`] (= the reference `history.ts`'s
//! `HistStoreImpl` over its `RangeTree`).
//!
//! A leaf source authors a [`RangeSpec`] (what to store/resolve/persist) and
//! feeds this store through two entry points (the reference's
//! `HistoryCallbacks`):
//!
//! * [`on_events`](RangeIndexStore::on_events) — a live page of items:
//!   in-memory tree update, then the tip advances to the page's end.
//! * [`on_bump`](RangeIndexStore::on_bump) — the source's `end` advanced
//!   with no items (e.g. a new head with no matching logs). Monotonic:
//!   a bump at-or-before the current end is a no-op.
//!
//! The `end` tip starts **unset**: until the first bump the store is
//! uninitialized and [`snapshots`](HistStoreBackend::snapshots) /
//! [`next`](HistStoreBackend::next) wait rather than fabricate an empty
//! snapshot at `earliest`.
//!
//! Everything reactive — snapshots off the single tip, [`HistBackend::iter`]
//! with lazy window-load + fetcher-driven gap fill, store-owned position
//! deserialization — lives here, once. The wiring that *drives* the entry
//! points (subscriptions, head clocks, reconnect gap-fill) is
//! source-specific and lives with the source — for EVM logs, in
//! [`EvmChain`](crate::evm::EvmChain).

use std::sync::{Arc, Mutex, OnceLock, Weak};

use futures_util::stream::{empty, once, unfold};
use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use super::RangeIndex;
use super::spec::RangeSpec;
use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Timestamp,
};

/// Wall-clock → position estimation, injected by the source (for EVM it
/// closes over the chain's block-time anchor). Synchronous — estimation
/// must not RPC.
pub type TimePosFn<P> = Arc<dyn Fn(Timestamp) -> Result<P, HistoryError> + Send + Sync>;

/// Generic [`HistStoreBackend`] over a [`RangeIndex<S>`] plus a single
/// reactive `end` tip. One instance per source stream (for EVM logs: per
/// filter).
pub struct RangeIndexStore<S: RangeSpec> {
    index: Arc<RangeIndex<S>>,
    /// Current `end` position + change notification (the reference's
    /// `end` field and `endChanged` event, fused into a watch channel).
    /// `None` until the first [`on_bump`](Self::on_bump).
    end: watch::Sender<Option<S::PubPos>>,
    time_pos_fn: TimePosFn<S::PubPos>,
    /// Background wiring driving `on_events`/`on_bump` (subscription loops,
    /// head-clock listeners). Aborted when the store drops — the wiring
    /// holds only a `Weak` back-reference, so there is no cycle.
    wiring: Mutex<Vec<AbortOnDropHandle<()>>>,
    // Self-reference so we can hand out Arc<dyn HistBackend>.
    self_ref: OnceLock<Weak<Self>>,
}

impl<S: RangeSpec> RangeIndexStore<S> {
    /// Wrap `index` with the reactive layer. The tip starts unset — the
    /// source wiring seeds it with the first [`on_bump`](Self::on_bump) /
    /// [`on_events`](Self::on_events).
    pub fn new(index: Arc<RangeIndex<S>>, time_pos_fn: TimePosFn<S::PubPos>) -> Arc<Self> {
        let (end, _) = watch::channel(None);
        let store = Arc::new(Self {
            index,
            end,
            time_pos_fn,
            wiring: Mutex::new(Vec::new()),
            self_ref: OnceLock::new(),
        });
        let _ = store.self_ref.set(Arc::downgrade(&store));
        store
    }

    /// Wrap this store as a public [`HistStore`].
    pub fn hist_store(self: &Arc<Self>) -> HistStore<S::Event, S::PubPos> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<S::Event, S::PubPos>>)
    }

    /// The underlying [`RangeIndex`]. Wiring + tests.
    pub fn index(&self) -> &Arc<RangeIndex<S>> {
        &self.index
    }

    /// Current `end` position; `None` until the source has seeded the tip.
    pub fn end(&self) -> Option<S::PubPos> {
        self.end.borrow().clone()
    }

    /// Live delivery: install a page into the in-memory tree (persisted
    /// asynchronously when a drain is attached), then advance the tip to
    /// the page's end.
    pub fn on_events(&self, page: &S::Page) {
        self.index.insert_page(page);
        self.on_bump(S::page_tip(page));
    }

    /// The source's `end` advanced (or is being seeded). Monotonic: keeps
    /// the maximum of the current and offered positions, notifying
    /// [`snapshots`](HistStoreBackend::snapshots) only on a real advance.
    pub fn on_bump(&self, pos: S::PubPos) {
        self.end.send_if_modified(|cur| match cur {
            Some(c) if *c >= pos => false,
            _ => {
                *cur = Some(pos);
                true
            }
        });
    }

    /// Tie a background wiring task's lifetime to this store: the handle
    /// aborts the task when the store drops. The task must hold only a
    /// [`Weak`] reference back to the store.
    pub fn attach_wiring(&self, handle: AbortOnDropHandle<()>) {
        self.wiring.lock().unwrap().push(handle);
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref
            .get()
            .expect("self_ref not initialized")
            .upgrade()
            .expect("RangeIndexStore dropped while still in use")
    }

    /// Await a set tip, yielding the current value if already set.
    /// Cancel-safe.
    async fn seeded_end(&self, rx: &mut watch::Receiver<Option<S::PubPos>>) -> S::PubPos {
        loop {
            let cur = rx.borrow_and_update().clone();
            if let Some(tip) = cur {
                return tip;
            }
            // The sender lives on `self`, which the caller borrows — the
            // channel cannot close while we wait.
            rx.changed().await.expect("tip sender lives with the store");
        }
    }
}

// ============================================================
// HistBackend impl
// ============================================================

impl<S: RangeSpec> HistBackend<S::Event, S::PubPos> for RangeIndexStore<S> {
    fn iter(&self, start: S::PubPos, end: S::PubPos) -> ItemStream<S::Event, S::PubPos> {
        if end <= start {
            return Box::pin(empty());
        }

        // Materialize any persisted scanned ranges covering this window
        // into the in-memory tree, so items() can answer from cache
        // instead of falling through to the fetcher.
        if let Err(e) = self.index.ensure_window_loaded(S::window(&start, &end)) {
            return Box::pin(once(async move { Err(e) }));
        }

        self.index
            .clone()
            .items(S::index_pos(&start), S::index_pos(&end))
    }
}

// ============================================================
// HistStoreBackend impl
// ============================================================

#[async_trait::async_trait]
impl<S: RangeSpec> HistStoreBackend<S::Event, S::PubPos> for RangeIndexStore<S> {
    fn snapshots(&self) -> HistStream<S::Event, S::PubPos> {
        let store = self.arc_self();
        let rx = self.end.subscribe();
        Box::pin(unfold((store, rx), |(store, mut rx)| async move {
            let tip = store.seeded_end(&mut rx).await;
            let backend: Arc<dyn HistBackend<S::Event, S::PubPos>> = store.clone();
            let hist = Hist::new(backend, S::PubPos::default(), tip);
            Some((hist, (store, rx)))
        }))
    }

    async fn next(&self) -> Hist<S::Event, S::PubPos> {
        let mut rx = self.end.subscribe();
        let tip = self.seeded_end(&mut rx).await;
        let backend: Arc<dyn HistBackend<S::Event, S::PubPos>> = self.arc_self();
        Hist::new(backend, S::PubPos::default(), tip)
    }

    async fn time_pos(&self, time: Timestamp) -> Result<S::PubPos, HistoryError> {
        (self.time_pos_fn)(time)
    }

    fn load(&self, value: serde_json::Value) -> Option<S::PubPos> {
        self.index.spec().load_pos(value)
    }
}
