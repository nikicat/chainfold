//! Phase 4 acceptance: map / filter / since combinators.
//!
//! Source-agnostic — uses a synthetic `MockPos`/`MockSource` per the
//! layering rule. Combinators must not reference any concrete source.
//! Exercises composition (map ∘ filter, since(map(src))), anchor
//! round-tripping via marshaled positions, and live-tip propagation
//! through proxy stores.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chainfold::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Store, Timestamp, watch_hist,
};
use chainfold::combinators::{
    FilteredItem, MappedItem, filter_hist_store, map_hist_store, since_hist_store,
};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;

// ============================================================
// MockPos / MockEvent / MockSource (mirror of phase3_hist_store.rs)
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
struct MockEvent(u64);

struct MockSource {
    items: Mutex<Vec<MockEvent>>,
    latest_store: Store<MockPos>,
    latest_tx: watch::Sender<MockPos>,
    best_store: Store<MockPos>,
    safe_store: Store<MockPos>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl MockSource {
    fn new(initial: Vec<MockEvent>) -> Arc<Self> {
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

    fn append(&self, e: MockEvent) {
        let new_tip = {
            let mut items = self.items.lock().unwrap();
            items.push(e);
            MockPos(items.len() as u64)
        };
        let _ = self.latest_tx.send(new_tip);
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
        let items = self.items.lock().unwrap().clone();
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

async fn collect<T, P>(hist: Hist<T, P>) -> Vec<Positioned<T, P>>
where
    T: Send + Sync + 'static,
    P: Position,
{
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}

fn evens(items: Vec<u64>) -> Vec<MockEvent> {
    items.into_iter().map(MockEvent).collect()
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn map_composes_with_filter() {
    // map(filter(src, p), f) on items [1..=5] with p = even, f = x*10
    // expected items: kept=[2, 4], mapped=[20, 40]
    let src = MockSource::new(evens(vec![1, 2, 3, 4, 5]));
    let filtered = filter_hist_store(src.hist_store(), |e: &MockEvent| e.0.is_multiple_of(2));
    let mapped: HistStore<MappedItem<FilteredItem<MockEvent>, u64>, MockPos> =
        map_hist_store(filtered, |f: &FilteredItem<MockEvent>| f.value.0 * 10);

    let items = collect(mapped.next().await).await;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].item.value, 20);
    assert_eq!(items[0].item.source.value.0, 2);
    assert_eq!(
        items[0]
            .item
            .source
            .skipped
            .iter()
            .map(|i| i.0)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(items[1].item.value, 40);
    assert_eq!(items[1].item.source.value.0, 4);
    assert_eq!(items[1].pos, MockPos(4));
}

#[tokio::test]
async fn filter_pos_after_round_trips_via_marshal() {
    // Save the position of the last yielded filtered item, marshal it,
    // unmarshal, split the upstream — resume yields the same continuation.
    let src = MockSource::new(evens(vec![1, 2, 3, 4, 5, 6]));
    let filtered = filter_hist_store(src.hist_store(), |e: &MockEvent| e.0.is_multiple_of(2));
    let h1 = filtered.next().await;
    let items1 = collect(h1.clone()).await;
    // Consume first two: kept [2, 4], anchor = pos of "4"
    let anchor = items1[1].pos;
    let bytes = anchor.marshal();
    let restored = MockPos::unmarshal(&bytes).unwrap();
    assert_eq!(restored, MockPos(4));

    // Split the filtered hist at the restored anchor → tail yields "6".
    let (_, tail) = h1.split(restored);
    let items2 = collect(tail).await;
    assert_eq!(items2.len(), 1);
    assert_eq!(items2[0].item.value.0, 6);
}

#[tokio::test]
async fn since_skips_mapper_on_pre_cut_items_full_pipeline() {
    let src = MockSource::new(evens(vec![1, 2, 3, 4, 5, 6]));
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let mapped = map_hist_store(src.hist_store(), move |e: &MockEvent| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
        e.0 * 2
    });
    let cut = MockPos(3);
    let since = since_hist_store(mapped, cut);
    let items = collect(since.next().await).await;
    let kept: Vec<u64> = items.iter().map(|p| p.item.value).collect();
    assert_eq!(kept, vec![8, 10, 12]); // x*2 for x in [4,5,6]
    // Mapper only saw the post-cut items.
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn filter_advances_with_tip() {
    // After source appends new items past the filter, watch_hist on the
    // filtered store delivers them in order.
    let src = MockSource::new(evens(vec![1, 2, 3]));
    let filtered = filter_hist_store(src.hist_store(), |e: &MockEvent| e.0.is_multiple_of(2));
    let mut stream = watch_hist(&filtered, MockPos(0));

    // First yield is "2".
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.item.value.0, 2);
    assert_eq!(first.pos, MockPos(2));

    // Append more — watch_hist should re-snapshot and yield 4, then 6.
    src.append(MockEvent(4));
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.item.value.0, 4);

    src.append(MockEvent(5));
    src.append(MockEvent(6));
    let third = stream.next().await.unwrap().unwrap();
    assert_eq!(third.item.value.0, 6);
    assert_eq!(third.pos, MockPos(6));
}

#[tokio::test]
async fn since_then_filter_composes() {
    // filter(since(src, cut), pred) — filter inside since's pre-split.
    let src = MockSource::new(evens(vec![1, 2, 3, 4, 5, 6, 7]));
    let since = since_hist_store(src.hist_store(), MockPos(3));
    let filtered = filter_hist_store(since, |e: &MockEvent| e.0.is_multiple_of(2));
    let items = collect(filtered.next().await).await;
    let kept: Vec<u64> = items.iter().map(|p| p.item.value.0).collect();
    // Items strictly after pos=3 are [4,5,6,7]; evens are [4, 6].
    assert_eq!(kept, vec![4, 6]);
}

#[tokio::test]
async fn map_then_since_composes() {
    // since(map(src, f), cut) — primary use case: cheap to skip pre-cut
    // through the mapper.
    let src = MockSource::new(evens(vec![1, 2, 3, 4]));
    let mapped = map_hist_store(src.hist_store(), |e: &MockEvent| e.0 + 100);
    let since = since_hist_store(mapped, MockPos(2));
    let items = collect(since.next().await).await;
    let kept: Vec<u64> = items.iter().map(|p| p.item.value).collect();
    // Post-cut items at positions 3, 4 → mapped to 103, 104.
    assert_eq!(kept, vec![103, 104]);
}
