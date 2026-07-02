//! Phase 5 acceptance: heterogeneous-source merge + keyed `MergedPos`.
//!
//! Source-agnostic — uses two mock source types with *different* `Position`
//! types (`MockPosU64` and `MockPosNamed`) to exercise the type-erasure
//! path. Merge primitives must not reference any concrete source.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chainfold::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Store, Timestamp, watch_hist,
};
use chainfold::combinators::{FilteredItem, filter_hist_store, since_hist_store};
use chainfold::merge::{ErasedHistStore, MergedItem, MergedPos, merge_hist_stores};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;
use tokio::time::timeout;

// ============================================================
// Source A — integer position, "trade" events
// ============================================================

/// Reconstruct a position from a [`Position::marshal`] value. Deserialization
/// no longer lives on the `Position` trait; the generic `Source<P>` below needs
/// this as a bound so its `HistStoreBackend::load` can rebuild the concrete `P`.
trait FromValue: Sized {
    fn from_value(v: &serde_json::Value) -> Option<Self>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
struct MockPosU64(u64);

impl Position for MockPosU64 {
    fn marshal(&self) -> serde_json::Value {
        serde_json::json!(self.0)
    }
}

impl FromValue for MockPosU64 {
    fn from_value(v: &serde_json::Value) -> Option<Self> {
        v.as_u64().map(MockPosU64)
    }
}

// ============================================================
// Source B — variable-length-bytes position, "burn" events.
// Demonstrates a *different* Position type sharing the same item type.
// ============================================================

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Default)]
struct MockPosNamed(String);

impl Position for MockPosNamed {
    fn marshal(&self) -> serde_json::Value {
        serde_json::json!(self.0)
    }
}

impl FromValue for MockPosNamed {
    fn from_value(v: &serde_json::Value) -> Option<Self> {
        v.as_str().map(|s| MockPosNamed(s.to_owned()))
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
    CommonEvent {
        time,
        label: label.into(),
    }
}

fn cmp_by_time(a: &MergedItem<CommonEvent>, b: &MergedItem<CommonEvent>) -> Ordering {
    a.value.time.cmp(&b.value.time)
}

// ============================================================
// Generic in-memory mock source over (P, Item).
//
// Source A uses (MockPosU64, CommonEvent) — items at pos = 1-based index.
// Source B uses (MockPosNamed, CommonEvent) — items at named positions
// like "n01", "n02" (alphabetically ordered).
// ============================================================

struct Source<P: Position + FromValue + 'static> {
    items: Mutex<Vec<(P, CommonEvent)>>,
    tip: Store<P>,
    tip_tx: watch::Sender<P>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl<P: Position + FromValue + 'static> Source<P> {
    fn new(initial: Vec<(P, CommonEvent)>) -> Arc<Self> {
        let tip = initial.last().map(|(p, _)| p.clone()).unwrap_or_default();
        let (tx, rx) = watch::channel(tip);
        let s = Arc::new(Self {
            items: Mutex::new(initial),
            tip: Store::new(rx),
            tip_tx: tx,
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
        let _ = self.tip_tx.send(pos);
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref.get().unwrap().upgrade().unwrap()
    }

    fn hist_store(self: &Arc<Self>) -> HistStore<CommonEvent, P> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<CommonEvent, P>>)
    }
}

impl<P: Position + FromValue + 'static> HistBackend<CommonEvent, P> for Source<P> {
    fn iter(&self, start: P, end: P) -> ItemStream<CommonEvent, P> {
        let items = self.items.lock().unwrap().clone();
        let yielded: Vec<Positioned<CommonEvent, P>> = items
            .into_iter()
            .filter(|(p, _)| p > &start && p <= &end)
            .map(|(p, e)| Positioned::new(e, p))
            .collect();
        Box::pin(iter(yielded.into_iter().map(Ok)))
    }
}

#[async_trait::async_trait]
impl<P: Position + FromValue + 'static> HistStoreBackend<CommonEvent, P> for Source<P> {
    fn snapshots(&self) -> HistStream<CommonEvent, P> {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        let mut sub = self.tip.subscribe();
        Box::pin(poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), P::default(), tip)))
        }))
    }

    async fn next(&self) -> Hist<CommonEvent, P> {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        Hist::new(backend, P::default(), self.tip.get())
    }

    async fn time_pos(&self, _time: Timestamp) -> Result<P, HistoryError> {
        Ok(P::default())
    }

    fn load(&self, value: serde_json::Value) -> Option<P> {
        P::from_value(&value)
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
    let value = anchor.marshal();
    let restored = merged.load(value).expect("load");
    assert_eq!(restored, anchor);

    // The restored anchor decodes each per-source position back to its
    // concrete type correctly.
    assert_eq!(restored.get_as::<MockPosU64>("alpha"), Some(MockPosU64(1)));
    assert_eq!(
        restored.get_as::<MockPosNamed>("named"),
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
    let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
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
    let value = anchor.marshal();
    let restored = merged.load(value).expect("load");

    let (_, tail) = hist.split(restored);
    let items_tail = collect_merged(tail).await;
    let labels: Vec<&str> = items_tail
        .iter()
        .map(|p| p.item.value.label.as_str())
        .collect();
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
    let saved_value = anchor.marshal();

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

    // Reload the saved (alpha-only) anchor against the two-source merge —
    // the missing "named" key defaults to that source's earliest.
    let restored = merged2.load(saved_value).expect("load");
    let hist = merged2.next().await;
    let (_, tail) = hist.split(restored);
    let items_tail = collect_merged(tail).await;
    let labels: Vec<&str> = items_tail
        .iter()
        .map(|p| p.item.value.label.as_str())
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
        mi.value.time.is_multiple_of(2)
    });
    let items = collect_merged_filtered(filtered.next().await).await;
    let labels: Vec<&str> = items
        .iter()
        .map(|p| p.item.value.value.label.as_str())
        .collect();
    // Even-time items: a1 (10), b1 (20). a2 (35) and b2 (45) are odd.
    assert_eq!(labels, vec!["a1", "b1"]);
}

async fn collect_merged_filtered(
    hist: Hist<FilteredItem<MergedItem<CommonEvent>>, MergedPos>,
) -> Vec<Positioned<FilteredItem<MergedItem<CommonEvent>>, MergedPos>> {
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

    // Build a cut "between a1 and b1": positions = { alpha: 1, named: default }.
    // `MergedPos::new` is crate-private, so rebuild the cut by loading its
    // marshalled JSON form through the merge store (named left at its default,
    // i.e. empty string — it has not advanced).
    let cut_value = serde_json::json!({
        "alpha": MockPosU64(1).marshal(),
        "named": MockPosNamed::default().marshal(),
    });
    let cut = merged.load(cut_value).expect("load cut");

    let since = since_hist_store(merged, cut);
    let items = collect_merged(since.next().await).await;
    let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
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
    let first = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("first item within timeout")
        .unwrap()
        .unwrap();
    assert_eq!(first.item.value.label, "a1");

    // Append to b; the merged store should re-snapshot and yield it.
    b.append(MockPosNamed("n01".into()), ev(20, "b1"));
    let second = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("second item within timeout")
        .unwrap()
        .unwrap();
    assert_eq!(second.item.value.label, "b1");
}

#[tokio::test]
async fn merge_time_pos_aggregates_per_source() {
    let a = Source::<MockPosU64>::new(vec![(MockPosU64(1), ev(10, "a1"))]);
    let b = Source::<MockPosNamed>::new(vec![(MockPosNamed("n01".into()), ev(20, "b1"))]);
    let mut sources = BTreeMap::new();
    sources.insert("alpha".into(), ErasedHistStore::new(a.hist_store()));
    sources.insert("named".into(), ErasedHistStore::new(b.hist_store()));
    let merged = merge_hist_stores(sources, cmp_by_time);

    let tp = merged.time_pos(Timestamp::new(42)).await.expect("time_pos");
    // Both source stubs return P::default() — verify each aggregated
    // per-source position is that source's default.
    assert_eq!(
        tp.get_as::<MockPosU64>("alpha"),
        Some(MockPosU64::default())
    );
    assert_eq!(
        tp.get_as::<MockPosNamed>("named"),
        Some(MockPosNamed::default())
    );
}
