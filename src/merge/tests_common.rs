//! Test fixtures shared by the ordered and interleave test suites: one
//! in-memory mock source over a single `u64` position space, with explicit
//! item positions so tests can place items from different sources at the
//! identical position (the self-transfer shape) or leave gaps.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use futures_core::Stream;
use futures_util::TryStreamExt;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Store, Timestamp,
};
use crate::merge::{DiscretePosition, MergedItem, SourceKey};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct MockPos(pub u64);

impl Position for MockPos {
    fn marshal(&self) -> serde_json::Value {
        serde_json::json!(self.0)
    }
}

impl DiscretePosition for MockPos {
    fn pred(&self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MockEvent {
    pub time: u64,
    pub label: String,
}

pub fn ev(time: u64, label: &str) -> MockEvent {
    MockEvent {
        time,
        label: label.into(),
    }
}

pub struct MockSource {
    items: Mutex<Vec<(MockPos, MockEvent)>>,
    tip: watch::Sender<MockPos>,
    self_ref: OnceLock<Weak<Self>>,
}

impl MockSource {
    pub fn new(initial: Vec<(u64, MockEvent)>) -> Arc<Self> {
        let items: Vec<(MockPos, MockEvent)> =
            initial.into_iter().map(|(p, e)| (MockPos(p), e)).collect();
        let tip = items.last().map(|(p, _)| *p).unwrap_or_default();
        let s = Arc::new(Self {
            items: Mutex::new(items),
            tip: watch::channel(tip).0,
            self_ref: OnceLock::new(),
        });
        let _ = s.self_ref.set(Arc::downgrade(&s));
        s
    }

    pub fn append(&self, pos: u64, e: MockEvent) {
        {
            let mut items = self.items.lock().unwrap();
            items.push((MockPos(pos), e));
        }
        self.tip.send_replace(MockPos(pos));
    }

    /// Advance the tip without an item — an empty-but-caught-up source.
    pub fn advance_tip(&self, pos: u64) {
        self.tip.send_replace(MockPos(pos));
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
        let items = self.items.lock().unwrap().clone();
        let yielded: Vec<Positioned<MockEvent, MockPos>> = items
            .into_iter()
            .filter(|(p, _)| *p > start && *p <= end)
            .map(|(p, e)| Positioned::new(e, p))
            .collect();
        Box::pin(iter(yielded.into_iter().map(Ok)))
    }
}

#[async_trait::async_trait]
impl HistStoreBackend<MockEvent, MockPos> for MockSource {
    fn snapshots(&self) -> HistStream<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        let mut sub = Store::new(self.tip.subscribe()).subscribe();
        Box::pin(poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
        }))
    }

    async fn next(&self) -> Hist<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        Hist::new(backend, MockPos(0), *self.tip.borrow())
    }

    async fn time_pos(&self, _time: Timestamp) -> Result<MockPos, HistoryError> {
        Ok(MockPos(0))
    }

    fn load(&self, value: serde_json::Value) -> Option<MockPos> {
        value.as_u64().map(MockPos)
    }
}

/// The usual two-source fixture, keyed "a" and "b".
pub fn two_sources(
    a: &Arc<MockSource>,
    b: &Arc<MockSource>,
) -> BTreeMap<SourceKey, HistStore<MockEvent, MockPos>> {
    let mut sources = BTreeMap::new();
    sources.insert("a".into(), a.hist_store());
    sources.insert("b".into(), b.hist_store());
    sources
}

/// Drain a merged hist into a vec, unwrapping errors.
pub async fn collect<T, P>(hist: Hist<T, P>) -> Vec<Positioned<T, P>>
where
    T: Send + Sync + 'static,
    P: Position,
{
    hist.iter().try_collect().await.unwrap()
}

/// The yielded labels, in order — the assertion currency of these suites.
pub fn labels<P: Position>(items: &[Positioned<MergedItem<MockEvent, MockPos>, P>]) -> Vec<&str> {
    items.iter().map(|p| p.item.value.label.as_str()).collect()
}
