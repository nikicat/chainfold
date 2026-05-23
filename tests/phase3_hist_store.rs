//! Phase 3 acceptance: Hist / HistStore split.
//!
//! These tests exercise the new `Hist` / `HistStore` types from
//! `chainfold::arch` directly — they're source-agnostic and use a
//! synthetic `MockPos`/`MockEvent` per the layering rule
//! (see [[source-agnostic-core]] in project memory). Generic primitives
//! must not reference EVM.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chainfold::arch::{
    act_and_wait, watch_hist, AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend,
    HistStream, HistoryError, ItemStream, Position, Positioned, Store, Timestamp,
};
use futures_core::Stream;
use futures_util::StreamExt;

// ============================================================
// MockPos / MockEvent / MockSource
// ============================================================

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
struct MockPos(u64);

impl Position for MockPos {
    fn marshal(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(u64::from_le_bytes).map(MockPos)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct MockEvent(String);

/// In-memory source: items[i] sits at MockPos(i+1). Tip is items.len().
/// (0,0] is empty; (0, len] iterates everything.
struct MockSource {
    items: Mutex<Vec<MockEvent>>,
    latest_store: Store<MockPos>,
    latest_tx: tokio::sync::watch::Sender<MockPos>,
    best_store: Store<MockPos>,
    safe_store: Store<MockPos>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl MockSource {
    fn new(initial: Vec<MockEvent>) -> Arc<Self> {
        let tip = MockPos(initial.len() as u64);
        let (tx, rx) = tokio::sync::watch::channel(tip);
        let (_b_tx, b_rx) = tokio::sync::watch::channel(tip);
        let (_s_tx, s_rx) = tokio::sync::watch::channel(tip);
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

    fn append(&self, event: MockEvent) {
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
        Box::pin(futures_util::stream::iter(yielded.into_iter().map(Ok)))
    }
}

#[async_trait::async_trait]
impl HistStoreBackend<MockEvent, MockPos> for MockSource {
    fn snapshots(&self) -> HistStream<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        Box::pin(futures_util::stream::poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
        }))
    }

    async fn next(&self) -> Hist<MockEvent, MockPos> {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        Hist::new(backend, MockPos(0), self.latest_store.get())
    }

    fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<MockEvent, MockPos>) + Send + Sync>,
    ) -> AsyncDisposable {
        let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel2 = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel2.cancelled() => break,
                    tip = sub.next() => {
                        let Some(tip) = tip else { break };
                        cb(Hist::new(backend.clone(), MockPos(0), tip));
                    }
                }
            }
        });
        AsyncDisposable::new(cancel, handle)
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
    let bytes = first_after.marshal();
    let restored = MockPos::unmarshal(&bytes).expect("unmarshal");
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
    let bytes = p.marshal();
    let restored = MockPos::unmarshal(&bytes).unwrap();
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
