//! Phase 5 acceptance: heterogeneous-source merge + keyed `MergedPos`.
//!
//! Source-agnostic — uses two mock source types with *different* `Position`
//! types (`MockPosU64` and `MockPosNamed`) to exercise the type-erasure
//! path. Merge primitives must not reference any concrete source.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chainfold::arch::{
    watch_hist, AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend, HistStream,
    HistoryError, ItemStream, Position, Positioned, Store, Timestamp,
};
use chainfold::combinators::{filter_hist_store, since_hist_store};
use chainfold::merge::{merge_hist_stores, ErasedHistStore, MergedItem, MergedPos};
use futures_core::Stream;
use futures_util::StreamExt;

// ============================================================
// Source A — integer position, "trade" events
// ============================================================

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
struct MockPosU64(u64);

impl Position for MockPosU64 {
    fn marshal(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(u64::from_le_bytes).map(MockPosU64)
    }
}

// ============================================================
// Source B — variable-length-bytes position, "burn" events.
// Demonstrates a *different* Position type sharing the same item type.
// ============================================================

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Default)]
struct MockPosNamed(String);

impl Position for MockPosNamed {
    fn marshal(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        std::str::from_utf8(bytes).ok().map(|s| MockPosNamed(s.to_owned()))
    }
}

// ============================================================
// Common event type
// ============================================================

#[derive(Debug, Clone, Eq, PartialEq)]
struct CommonEvent {
    time: u64,
    label: String,
}

fn ev(time: u64, label: &str) -> CommonEvent {
    CommonEvent { time, label: label.into() }
}

fn cmp_by_time(a: &MergedItem<CommonEvent>, b: &MergedItem<CommonEvent>) -> Ordering {
    a.item.time.cmp(&b.item.time)
}

// ============================================================
// Generic in-memory mock source over (P, Item).
//
// Source A uses (MockPosU64, CommonEvent) — items at pos = 1-based index.
// Source B uses (MockPosNamed, CommonEvent) — items at named positions
// like "n01", "n02" (alphabetically ordered).
// ============================================================

struct Source<P: Position + 'static> {
    items: Mutex<Vec<(P, CommonEvent)>>,
    latest_store: Store<P>,
    latest_tx: tokio::sync::watch::Sender<P>,
    best_store: Store<P>,
    safe_store: Store<P>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl<P: Position + 'static> Source<P> {
    fn new(initial: Vec<(P, CommonEvent)>) -> Arc<Self> {
        let tip = initial.last().map(|(p, _)| p.clone()).unwrap_or_default();
        let (tx, rx) = tokio::sync::watch::channel(tip.clone());
        let (_, b_rx) = tokio::sync::watch::channel(tip.clone());
        let (_, s_rx) = tokio::sync::watch::channel(tip);
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

    fn append(&self, pos: P, e: CommonEvent) {
        {
            let mut items = self.items.lock().unwrap();
            items.push((pos.clone(), e));
        }
        let _ = self.latest_tx.send(pos);
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref.get().unwrap().upgrade().unwrap()
    }

    fn hist_store(self: &Arc<Self>) -> HistStore<CommonEvent, P> {
        HistStore::new(
            Arc::clone(self) as Arc<dyn HistStoreBackend<CommonEvent, P>>,
        )
    }
}

impl<P: Position + 'static> HistBackend<CommonEvent, P> for Source<P> {
    fn iter(&self, start: P, end: P) -> ItemStream<CommonEvent, P> {
        let items = self.items.lock().unwrap().clone();
        let yielded: Vec<Positioned<CommonEvent, P>> = items
            .into_iter()
            .filter(|(p, _)| p > &start && p <= &end)
            .map(|(p, e)| Positioned::new(e, p))
            .collect();
        Box::pin(futures_util::stream::iter(yielded.into_iter().map(Ok)))
    }
}

#[async_trait::async_trait]
impl<P: Position + 'static> HistStoreBackend<CommonEvent, P> for Source<P> {
    fn snapshots(&self) -> HistStream<CommonEvent, P> {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        Box::pin(futures_util::stream::poll_fn(move |cx| {
            Pin::new(&mut sub).poll_next(cx).map(|tip_opt| {
                tip_opt.map(|tip| Hist::new(backend.clone(), P::default(), tip))
            })
        }))
    }

    async fn next(&self) -> Hist<CommonEvent, P> {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        Hist::new(backend, P::default(), self.latest_store.get())
    }

    fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<CommonEvent, P>) + Send + Sync>,
    ) -> AsyncDisposable {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel2 = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel2.cancelled() => break,
                    tip = sub.next() => {
                        let Some(tip) = tip else { break };
                        cb(Hist::new(backend.clone(), P::default(), tip));
                    }
                }
            }
        });
        AsyncDisposable::new(cancel, handle)
    }

    async fn time_pos(&self, _time: Timestamp) -> Result<P, HistoryError> {
        Ok(P::default())
    }

    fn latest(&self) -> &Store<P> {
        &self.latest_store
    }
    fn best(&self) -> &Store<P> {
        &self.best_store
    }
    fn safe(&self) -> &Store<P> {
        &self.safe_store
    }
}

async fn collect_merged(
    hist: Hist<MergedItem<CommonEvent>, MergedPos>,
) -> Vec<Positioned<MergedItem<CommonEvent>, MergedPos>> {
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn merged_pos_marshal_round_trip_across_heterogeneous_anchors() {
    // Build a MergedPos with one u64-positioned source and one string-positioned source.
    let a = Source::<MockPosU64>::new(vec![(MockPosU64(1), ev(10, "a1"))]);
    let b = Source::<MockPosNamed>::new(vec![(MockPosNamed("n01".into()), ev(15, "b1"))]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let items = collect_merged(merged.next().await).await;
    let anchor = items.last().unwrap().pos.clone();
    let bytes = anchor.marshal();
    let restored = MergedPos::unmarshal(&bytes).expect("unmarshal");
    assert_eq!(restored, anchor);

    // The restored anchor decodes per-source bytes correctly.
    let alpha_bytes = restored.get("alpha").expect("alpha key present");
    let named_bytes = restored.get("named").expect("named key present");
    assert_eq!(MockPosU64::unmarshal(alpha_bytes), Some(MockPosU64(1)));
    assert_eq!(
        MockPosNamed::unmarshal(named_bytes),
        Some(MockPosNamed("n01".into())),
    );
}

#[tokio::test]
async fn merge_interleaves_heterogeneous_sources_by_time() {
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (MockPosNamed("n01".into()), ev(20, "b1")),
        (MockPosNamed("n02".into()), ev(30, "b2")),
    ]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let items = collect_merged(merged.next().await).await;
    let labels: Vec<&str> = items.iter().map(|p| p.item.item.label.as_str()).collect();
    assert_eq!(labels, vec!["a1", "b1", "b2", "a2"]);

    // source_key alternates correctly.
    let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
    assert_eq!(keys, vec!["alpha", "named", "named", "alpha"]);
}

#[tokio::test]
async fn merge_resumes_from_marshalled_anchor() {
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
        (MockPosU64(3), ev(60, "a3")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (MockPosNamed("n01".into()), ev(20, "b1")),
        (MockPosNamed("n02".into()), ev(50, "b2")),
    ]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let hist = merged.next().await;
    let items_all = collect_merged(hist.clone()).await;
    assert_eq!(items_all.len(), 5);

    // Save anchor after "b1" (index 1).
    let anchor = items_all[1].pos.clone();
    let bytes = anchor.marshal();
    let restored = MergedPos::unmarshal(&bytes).expect("unmarshal");

    let (_, tail) = hist.split(restored);
    let items_tail = collect_merged(tail).await;
    let labels: Vec<&str> = items_tail.iter().map(|p| p.item.item.label.as_str()).collect();
    assert_eq!(labels, vec!["a2", "b2", "a3"]);
}

#[tokio::test]
async fn merged_pos_handles_added_source() {
    // Phase 1: one source, walk halfway, save anchor.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let mut sources1 = BTreeMap::new();
    sources1.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    let merged1 = merge_hist_stores(sources1, cmp_by_time);
    let items = collect_merged(merged1.next().await).await;
    let anchor = items[0].pos.clone();
    assert!(anchor.get("alpha").is_some());
    assert!(anchor.get("named").is_none());
    let saved_bytes = anchor.marshal();

    // Phase 2: add a second source. Use the same saved anchor.
    let a2 = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (MockPosNamed("n01".into()), ev(20, "b1")),
        (MockPosNamed("n02".into()), ev(50, "b2")),
    ]);
    let mut sources2 = BTreeMap::new();
    sources2.insert("alpha".into(), ErasedHistStore::new(a2.hist_store()));
    sources2.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged2 = merge_hist_stores(sources2, cmp_by_time);

    let restored = MergedPos::unmarshal(&saved_bytes).expect("unmarshal");
    let hist = merged2.next().await;
    let (_, tail) = hist.split(restored);
    let items_tail = collect_merged(tail).await;
    let labels: Vec<&str> = items_tail
        .iter()
        .map(|p| p.item.item.label.as_str())
        .collect();
    // Items after anchor: alpha past pos 1 → a2 (time 40); named all → b1 (20), b2 (50).
    // Time-merged: b1 (20), a2 (40), b2 (50).
    assert_eq!(labels, vec!["b1", "a2", "b2"]);
}

#[tokio::test]
async fn merge_composes_with_filter() {
    // Filter the merged stream by even-timestamp items.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(35, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (MockPosNamed("n01".into()), ev(20, "b1")),
        (MockPosNamed("n02".into()), ev(45, "b2")),
    ]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);
    let filtered = filter_hist_store(merged, |mi: &MergedItem<CommonEvent>| {
        mi.item.time.is_multiple_of(2)
    });
    let items = collect_merged_filtered(filtered.next().await).await;
    let labels: Vec<&str> = items
        .iter()
        .map(|p| p.item.item.item.label.as_str())
        .collect();
    // Even-time items: a1 (10), b1 (20). a2 (35) and b2 (45) are odd.
    assert_eq!(labels, vec!["a1", "b1"]);
}

async fn collect_merged_filtered(
    hist: Hist<chainfold::combinators::FilteredItem<MergedItem<CommonEvent>>, MergedPos>,
) -> Vec<Positioned<chainfold::combinators::FilteredItem<MergedItem<CommonEvent>>, MergedPos>> {
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}

#[tokio::test]
async fn merge_composes_with_since() {
    // since(merge(a, b), cut) — split below a saved merged anchor.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (MockPosNamed("n01".into()), ev(20, "b1")),
        (MockPosNamed("n02".into()), ev(50, "b2")),
    ]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    // Build a cut "between a1 and b1": positions = { alpha: 1, named: "" }.
    let mut cut_positions = BTreeMap::new();
    cut_positions.insert("alpha".into(), MockPosU64(1).marshal());
    // named has not advanced — leave at default (= empty string).
    cut_positions.insert("named".into(), MockPosNamed::default().marshal());
    let cut = MergedPos::new(cut_positions);

    let since = since_hist_store(merged, cut);
    let items = collect_merged(since.next().await).await;
    let labels: Vec<&str> = items.iter().map(|p| p.item.item.label.as_str()).collect();
    // Skipped: a1 (alpha=1 cut). Yielded: b1, a2, b2.
    assert_eq!(labels, vec!["b1", "a2", "b2"]);
}

#[tokio::test]
async fn merge_advances_with_tip_via_watch_hist() {
    let a = Source::<MockPosU64>::new(vec![(MockPosU64(1), ev(10, "a1"))]);
    let b = Source::<MockPosNamed>::new(vec![]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.clone().hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.clone().hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let mut stream = watch_hist(&merged, MergedPos::default());
    // First item: a1.
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("first item within timeout")
        .unwrap()
        .unwrap();
    assert_eq!(first.item.item.label, "a1");

    // Append to b; the merged store should re-snapshot and yield it.
    b.append(MockPosNamed("n01".into()), ev(20, "b1"));
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .expect("second item within timeout")
        .unwrap()
        .unwrap();
    assert_eq!(second.item.item.label, "b1");
}

#[tokio::test]
async fn merge_time_pos_aggregates_per_source() {
    let a = Source::<MockPosU64>::new(vec![(MockPosU64(1), ev(10, "a1"))]);
    let b = Source::<MockPosNamed>::new(vec![(MockPosNamed("n01".into()), ev(20, "b1"))]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let tp = merged.time_pos(42).await.expect("time_pos");
    // Both source stubs return P::default() — verify marshalled bytes
    // match each source's default.
    assert_eq!(tp.get("alpha"), Some(&MockPosU64::default().marshal()[..]));
    assert_eq!(tp.get("named"), Some(&MockPosNamed::default().marshal()[..]));
}
