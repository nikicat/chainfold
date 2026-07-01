//! Shared in-memory mock source used by combinator unit tests.
//!
//! Mirrors the `MockPos`/`MockSource` shape from `tests/phase3_hist_store.rs`
//! so combinator tests stay source-agnostic (no EVM reference in any
//! generic test — see [[source-agnostic-core]]).

#![cfg(test)]

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures_core::Stream;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Store, Timestamp,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct MockPos(pub u64);

impl Position for MockPos {
    fn marshal(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(u64::from_le_bytes).map(MockPos)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MockEvent(pub String);

pub struct MockSource {
    items: Mutex<Vec<MockEvent>>,
    latest_store: Store<MockPos>,
    latest_tx: watch::Sender<MockPos>,
    best_store: Store<MockPos>,
    safe_store: Store<MockPos>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl MockSource {
    pub fn new(initial: Vec<MockEvent>) -> Arc<Self> {
        let tip = MockPos(initial.len() as u64);
        let (tx, rx) = watch::channel(tip);
        let (_, b_rx) = watch::channel(tip);
        let (_, s_rx) = watch::channel(tip);
        let s = Arc::new(Self {
            items: Mutex::new(initial),
            latest_store: Store::new(rx),
            latest_tx: tx,
            best_store: Store::new(b_rx),
            safe_store: Store::new(s_rx),
            self_ref: std::sync::OnceLock::new(),
        });
        let _ = s.self_ref.set(Arc::downgrade(&s));
        s
    }

    pub fn append(&self, event: MockEvent) {
        let new_tip = {
            let mut items = self.items.lock().unwrap();
            items.push(event);
            MockPos(items.len() as u64)
        };
        let _ = self.latest_tx.send(new_tip);
    }

    fn snapshot(&self) -> Vec<MockEvent> {
        self.items.lock().unwrap().clone()
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref.get().unwrap().upgrade().unwrap()
    }

    pub fn hist_store(self: &Arc<Self>) -> HistStore<MockEvent, MockPos> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<MockEvent, MockPos>>)
    }
}

impl HistBackend<MockEvent, MockPos> for MockSource {
    fn iter(&self, start: MockPos, end: MockPos) -> ItemStream<MockEvent, MockPos> {
        let items = self.snapshot();
        let from = (start.0 as usize).min(items.len());
        let to = (end.0 as usize).min(items.len());
        let yielded: Vec<Positioned<MockEvent, MockPos>> = items
            .into_iter()
            .enumerate()
            .skip(from)
            .take(to.saturating_sub(from))
            .map(|(i, item)| Positioned::new(item, MockPos((i + 1) as u64)))
            .collect();
        Box::pin(iter(yielded.into_iter().map(Ok)))
    }
}

#[async_trait::async_trait]
impl HistStoreBackend<MockEvent, MockPos> for MockSource {
    fn snapshots(&self) -> HistStream<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        Box::pin(poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
        }))
    }

    async fn next(&self) -> Hist<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        Hist::new(backend, MockPos(0), self.latest_store.get())
    }

    async fn time_pos(&self, _time: Timestamp) -> Result<MockPos, HistoryError> {
        Ok(MockPos(0))
    }

    fn latest(&self) -> &Store<MockPos> {
        &self.latest_store
    }
    fn best(&self) -> &Store<MockPos> {
        &self.best_store
    }
    fn safe(&self) -> &Store<MockPos> {
        &self.safe_store
    }
}

pub async fn collect<T, P>(hist: Hist<T, P>) -> Vec<Positioned<T, P>>
where
    T: Send + Sync + 'static,
    P: Position,
{
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}
