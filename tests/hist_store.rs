//! `Hist` / `HistStore` split (Phase 3 acceptance in `docs/refactor.md`).
//!
//! These tests exercise the new `Hist` / `HistStore` types from
//! `chainfold::arch` directly — they're source-agnostic and use a
//! synthetic `MockPos`/`MockEvent` per the layering rule
//! (see [[source-agnostic-core]] in project memory). Generic primitives
//! must not reference EVM.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chainfold::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Signal, Timestamp, act_and_wait, watch_hist,
};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;

// ============================================================
// MockPos / MockEvent / MockSource
// ============================================================

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
struct MockPos(u64);

impl Position for MockPos {
    fn marshal(&self) -> serde_json::Value {
        serde_json::json!(self.0)
    }
}

impl MockPos {
    fn from_value(v: &serde_json::Value) -> Option<Self> {
        v.as_u64().map(MockPos)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct MockEvent(String);

/// In-memory source: items[i] sits at MockPos(i+1). Tip is `items.len()`.
/// (0,0] is empty; (0, len] iterates everything.
struct MockSource {
    items: Mutex<Vec<MockEvent>>,
    tip: watch::Sender<MockPos>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl MockSource {
    fn new(initial: Vec<MockEvent>) -> Arc<Self> {
        let tip = MockPos(initial.len() as u64);
        let s = Arc::new(Self {
            items: Mutex::new(initial),
            tip: watch::channel(tip).0,
            self_ref: std::sync::OnceLock::new(),
        });
        let _ = s.self_ref.set(Arc::downgrade(&s));
        s
    }

    fn append(&self, event: MockEvent) {
        let new_tip = {
            let mut items = self.items.lock().unwrap();
            items.push(event);
            MockPos(items.len() as u64)
        };
        self.tip.send_replace(new_tip);
    }

    fn snapshot(&self) -> Vec<MockEvent> {
        self.items.lock().unwrap().clone()
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref.get().unwrap().upgrade().unwrap()
    }

    /// The tip as a reactive [`Signal`]; `.subscribe()` streams its changes.
    fn tip_signal(&self) -> Signal<MockPos> {
        Signal::new(self.tip.subscribe())
    }

    fn hist_store(self: &Arc<Self>) -> HistStore<MockEvent, MockPos> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<MockEvent, MockPos>>)
    }
}

impl HistBackend<MockEvent, MockPos> for MockSource {
    fn iter(&self, start: MockPos, end: MockPos) -> ItemStream<MockEvent, MockPos> {
        let items = self.snapshot();
        // Items at positions (start, end] — positions are 1-based indices.
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
        let mut sub = self.tip_signal().subscribe();
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
        MockPos::from_value(&value)
    }
}

async fn collect(hist: Hist<MockEvent, MockPos>) -> Vec<Positioned<MockEvent, MockPos>> {
    futures_util::TryStreamExt::try_collect(hist.iter())
        .await
        .unwrap()
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn hist_iter_walks_window_exclusive_inclusive() {
    let src = MockSource::new(vec![
        MockEvent("a".into()),
        MockEvent("b".into()),
        MockEvent("c".into()),
    ]);
    let hs = src.hist_store();
    let h = hs.next().await;
    let items = collect(h).await;
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].item.0, "a");
    assert_eq!(items[0].pos, MockPos(1));
    assert_eq!(items[2].pos, MockPos(3));
}

#[tokio::test]
async fn hist_split_clamps_to_window() {
    let src = MockSource::new(vec![
        MockEvent("a".into()),
        MockEvent("b".into()),
        MockEvent("c".into()),
    ]);
    let hs = src.hist_store();
    let h = hs.next().await;

    // Split below start ⇒ head empty, tail full.
    let (head, tail) = h.split(MockPos(0));
    assert!(head.is_empty());
    assert_eq!(collect(tail).await.len(), 3);

    // Split past end ⇒ head full, tail empty.
    let (head, tail) = h.split(MockPos(99));
    assert_eq!(collect(head).await.len(), 3);
    assert!(tail.is_empty());

    // Split in the middle.
    let (head, tail) = h.split(MockPos(2));
    let head_items = collect(head).await;
    let tail_items = collect(tail).await;
    assert_eq!(head_items.len(), 2);
    assert_eq!(tail_items.len(), 1);
    assert_eq!(tail_items[0].item.0, "c");
}

#[tokio::test]
async fn hist_split_round_trips_via_marshalled_anchor() {
    let src = MockSource::new(vec![
        MockEvent("a".into()),
        MockEvent("b".into()),
        MockEvent("c".into()),
        MockEvent("d".into()),
    ]);
    let hs = src.hist_store();
    let h = hs.next().await;

    let (_, tail) = h.split(MockPos(2));
    let first_after = collect(tail).await.first().unwrap().pos;
    let value = first_after.marshal();
    let restored = MockPos::from_value(&value).expect("from_value");
    assert_eq!(restored, MockPos(3));

    // Resuming from the anchor of "c" yields only items AFTER "c" — that's
    // the (start, end] convention. Resume at "c"'s pos → next iter starts
    // at "d".
    let (_, resumed_tail) = h.split(restored);
    let items = collect(resumed_tail).await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].item.0, "d");
}

#[tokio::test]
async fn hist_iter_stops_at_end() {
    let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
    let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = src.clone();
    let h = Hist::new(backend, MockPos(0), MockPos(1));
    let items = collect(h).await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].item.0, "a");
}

#[tokio::test]
async fn hist_store_gen_yields_current_then_on_tip_advance() {
    let src = MockSource::new(vec![MockEvent("a".into())]);
    let hs = src.hist_store();
    let mut stream = hs.snapshots();

    // First yield: current snapshot.
    let h1 = stream.next().await.expect("first snapshot");
    assert_eq!(*h1.end(), MockPos(1));

    // Append + verify a second snapshot arrives.
    src.append(MockEvent("b".into()));
    let h2 = stream.next().await.expect("second snapshot");
    assert_eq!(*h2.end(), MockPos(2));
}

#[tokio::test]
async fn watch_hist_yields_only_post_start() {
    let src = MockSource::new(vec![
        MockEvent("a".into()),
        MockEvent("b".into()),
        MockEvent("c".into()),
    ]);
    let hs = src.hist_store();

    let mut stream = watch_hist(&hs, MockPos(2));
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.item.0, "c");
    assert_eq!(first.pos, MockPos(3));

    // Append a new item; watch_hist should pick it up.
    src.append(MockEvent("d".into()));
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.item.0, "d");
    assert_eq!(second.pos, MockPos(4));
}

#[tokio::test]
async fn act_and_wait_snapshot_before_op_not_after() {
    // Race: an event is appended IMMEDIATELY when `op` runs — if
    // act_and_wait captured its anchor after op completed, it would miss
    // the event. Correct behavior: anchor is captured before op.
    let src = MockSource::new(vec![MockEvent("pre".into())]);
    let hs = src.hist_store();
    let src_for_op = src.clone();

    let (item, _) = act_and_wait(
        &hs,
        || async move {
            src_for_op.append(MockEvent("op-fired".into()));
            42
        },
        |item: &MockEvent, _result: &i32| item.0 == "op-fired",
    )
    .await
    .expect("act_and_wait found the post-op event");
    assert_eq!(item.item.0, "op-fired");
    assert_eq!(item.pos, MockPos(2));
}

#[tokio::test]
async fn position_marshal_round_trip() {
    let p = MockPos(123_456_789);
    let value = p.marshal();
    let restored = MockPos::from_value(&value).unwrap();
    assert_eq!(restored, p);
}

#[tokio::test]
async fn forward_walk_multiple_snapshots_no_duplicates() {
    let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
    let hs = src.hist_store();

    // Snapshot 1: walk a, b.
    let h1 = hs.next().await;
    let items1 = collect(h1).await;
    let anchor = items1.last().unwrap().pos;

    // Append more, snapshot 2, split at anchor, iterate tail.
    src.append(MockEvent("c".into()));
    src.append(MockEvent("d".into()));
    let h2 = hs.next().await;
    let (_, tail) = h2.split(anchor);
    let items2 = collect(tail).await;

    assert_eq!(
        items1.iter().map(|p| p.item.0.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(
        items2.iter().map(|p| p.item.0.as_str()).collect::<Vec<_>>(),
        vec!["c", "d"]
    );
}
