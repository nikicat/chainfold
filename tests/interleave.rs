//! Cross-family interleave over an app-defined position enum — the
//! **multi-chain wallet recipe** (Phase 5 acceptance, reshaped by the
//! ordered/interleave merge split).
//!
//! Source-agnostic — two mock source families with *different* `Position`
//! types (`MockPosU64` and `MockPosNamed`) unify into one closed enum
//! (`DuoPos`) via `map_positions`, then merge with
//! `interleave_hist_stores`. This is the pattern an app uses to track a
//! runtime-configured set of sources across compile-time-bounded families
//! (EVM/Tron/BTC/…): the chain *set* is dynamic (source keys), the chain
//! *families* are the enum's variants. Also pins the tagged-anchor marshal
//! round-trip across a restart (rebuild stores, reload the saved JSON
//! anchor, resume exactly).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chainfold::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Signal, Timestamp, watch_hist,
};
use chainfold::combinators::{
    FilteredItem, PosMapper, filter_hist_store, map_positions, since_hist_store,
};
use chainfold::merge::{MergedItem, MergedPos, SourceKey, interleave_hist_stores};
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::{iter, poll_fn};
use tokio::sync::watch;
use tokio::time::timeout;

// ============================================================
// Source family A — integer positions
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
// Source family B — string positions ("n01", "n02", … alphabetical)
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
// DuoPos — the app's closed position enum over both families
// ============================================================

/// The app-defined unification: one variant per compiled-in source family.
/// Cross-variant `Ord` is arbitrary (derive: variant order) and never
/// consulted by the interleave — positions are only compared within one
/// source key, which always carries one variant.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum DuoPos {
    U64(MockPosU64),
    Named(MockPosNamed),
}

impl Default for DuoPos {
    fn default() -> Self {
        Self::U64(MockPosU64::default())
    }
}

impl Position for DuoPos {
    /// Tagged so `load` can pick the family: `{"u64": ..}` / `{"named": ..}`.
    fn marshal(&self) -> serde_json::Value {
        match self {
            Self::U64(p) => serde_json::json!({ "u64": p.marshal() }),
            Self::Named(p) => serde_json::json!({ "named": p.marshal() }),
        }
    }
}

fn duo_from_value(v: &serde_json::Value) -> Option<DuoPos> {
    let obj = v.as_object()?;
    if let Some(inner) = obj.get("u64") {
        return MockPosU64::from_value(inner).map(DuoPos::U64);
    }
    MockPosNamed::from_value(obj.get("named")?).map(DuoPos::Named)
}

/// Embeds family A into [`DuoPos`].
struct U64Mapper;

impl PosMapper<MockPosU64, DuoPos> for U64Mapper {
    fn wrap(&self, p: MockPosU64) -> DuoPos {
        DuoPos::U64(p)
    }
    fn unwrap(&self, q: DuoPos) -> Option<MockPosU64> {
        match q {
            DuoPos::U64(p) => Some(p),
            DuoPos::Named(_) => None,
        }
    }
    fn load(&self, value: serde_json::Value) -> Option<DuoPos> {
        duo_from_value(&value)
    }
}

/// Embeds family B into [`DuoPos`].
struct NamedMapper;

impl PosMapper<MockPosNamed, DuoPos> for NamedMapper {
    fn wrap(&self, p: MockPosNamed) -> DuoPos {
        DuoPos::Named(p)
    }
    fn unwrap(&self, q: DuoPos) -> Option<MockPosNamed> {
        match q {
            DuoPos::U64(_) => None,
            DuoPos::Named(p) => Some(p),
        }
    }
    fn load(&self, value: serde_json::Value) -> Option<DuoPos> {
        duo_from_value(&value)
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

fn cmp_by_time(
    a: &MergedItem<CommonEvent, DuoPos>,
    b: &MergedItem<CommonEvent, DuoPos>,
) -> Ordering {
    a.value.time.cmp(&b.value.time)
}

// ============================================================
// Generic in-memory mock source over (P, Item).
//
// Family A uses (MockPosU64, CommonEvent); family B uses
// (MockPosNamed, CommonEvent).
// ============================================================

struct Source<P: Position + FromValue + 'static> {
    items: Mutex<Vec<(P, CommonEvent)>>,
    tip: watch::Sender<P>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl<P: Position + FromValue + 'static> Source<P> {
    fn new(initial: Vec<(P, CommonEvent)>) -> Arc<Self> {
        let tip = initial.last().map(|(p, _)| p.clone()).unwrap_or_default();
        let s = Arc::new(Self {
            items: Mutex::new(initial),
            tip: watch::channel(tip).0,
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
        self.tip.send_replace(pos);
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref.get().unwrap().upgrade().unwrap()
    }

    /// The tip as a reactive [`Signal`]; `.subscribe()` streams its changes.
    fn tip_signal(&self) -> Signal<P> {
        Signal::new(self.tip.subscribe())
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
        let mut sub = self.tip_signal().subscribe();
        Box::pin(poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), P::default(), tip)))
        }))
    }

    async fn next(&self) -> Hist<CommonEvent, P> {
        let backend: Arc<dyn HistBackend<CommonEvent, P>> = self.arc_self();
        Hist::new(backend, P::default(), self.tip.borrow().clone())
    }

    async fn time_pos(&self, _time: Timestamp) -> Result<P, HistoryError> {
        Ok(P::default())
    }

    fn load(&self, value: serde_json::Value) -> Option<P> {
        P::from_value(&value)
    }
}

// ============================================================
// Wallet-recipe wiring: lift both families into DuoPos, interleave.
// ============================================================

type WalletStore = HistStore<MergedItem<CommonEvent, DuoPos>, MergedPos<DuoPos>>;

fn wallet_store(alpha: &Arc<Source<MockPosU64>>, named: &Arc<Source<MockPosNamed>>) -> WalletStore {
    let mut sources: BTreeMap<SourceKey, HistStore<CommonEvent, DuoPos>> = BTreeMap::new();
    sources.insert("alpha".into(), map_positions(alpha.hist_store(), U64Mapper));
    sources.insert(
        "named".into(),
        map_positions(named.hist_store(), NamedMapper),
    );
    interleave_hist_stores(sources, cmp_by_time)
}

fn named_pos(s: &str) -> MockPosNamed {
    MockPosNamed(s.to_owned())
}

async fn collect_merged(
    hist: Hist<MergedItem<CommonEvent, DuoPos>, MergedPos<DuoPos>>,
) -> Vec<Positioned<MergedItem<CommonEvent, DuoPos>, MergedPos<DuoPos>>> {
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn merged_pos_marshal_round_trip_across_heterogeneous_anchors() {
    // Build a MergedPos with one u64-family source and one string-family
    // source, both lifted into DuoPos.
    let a = Source::<MockPosU64>::new(vec![(MockPosU64(1), ev(10, "a1"))]);
    let b = Source::<MockPosNamed>::new(vec![(named_pos("n01"), ev(15, "b1"))]);
    let merged = wallet_store(&a, &b);

    let items = collect_merged(merged.next().await).await;
    let anchor = items.last().unwrap().pos.clone();
    let value = anchor.marshal();
    let restored = merged.load(value).expect("load");
    assert_eq!(restored, anchor);

    // The restored anchor decodes each per-source position back to its
    // family's variant correctly.
    assert_eq!(
        restored.get(&"alpha".into()),
        Some(&DuoPos::U64(MockPosU64(1)))
    );
    assert_eq!(
        restored.get(&"named".into()),
        Some(&DuoPos::Named(named_pos("n01"))),
    );
}

#[tokio::test]
async fn merge_interleaves_heterogeneous_sources_by_time() {
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(30, "b2")),
    ]);
    let merged = wallet_store(&a, &b);

    let items = collect_merged(merged.next().await).await;
    let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
    assert_eq!(labels, vec!["a1", "b1", "b2", "a2"]);

    // source_key alternates correctly, and each item carries its typed
    // family position.
    let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
    assert_eq!(keys, vec!["alpha", "named", "named", "alpha"]);
    assert_eq!(items[0].item.source_pos, DuoPos::U64(MockPosU64(1)));
    assert_eq!(items[1].item.source_pos, DuoPos::Named(named_pos("n01")));
}

#[tokio::test]
async fn merge_resumes_from_marshalled_anchor() {
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
        (MockPosU64(3), ev(60, "a3")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(50, "b2")),
    ]);
    let merged = wallet_store(&a, &b);

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
async fn wallet_recipe_resumes_across_restart() {
    // "Session 1": walk halfway, persist the anchor as a JSON string.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![(named_pos("n01"), ev(20, "b1"))]);
    let merged = wallet_store(&a, &b);
    let items = collect_merged(merged.next().await).await;
    let saved = serde_json::to_string(&items[1].pos.marshal()).expect("serialize anchor");
    drop(merged);

    // "Session 2": rebuild everything from scratch — new sources (with an
    // event that arrived while we were down), new mappers, new interleave —
    // and resume from the persisted JSON.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(50, "b2")),
    ]);
    let merged = wallet_store(&a, &b);
    let value: serde_json::Value = serde_json::from_str(&saved).expect("parse anchor");
    let restored = merged.load(value).expect("load");

    let hist = merged.next().await;
    let (_, tail) = hist.split(restored);
    let labels: Vec<String> = collect_merged(tail)
        .await
        .iter()
        .map(|p| p.item.value.label.clone())
        .collect();
    // Consumed through b1 before the "crash"; a2 and the new b2 remain.
    assert_eq!(labels, vec!["a2", "b2"]);
}

#[tokio::test]
async fn merged_pos_handles_added_source() {
    // Phase 1: one source, walk halfway, save anchor.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let mut sources1: BTreeMap<SourceKey, HistStore<CommonEvent, DuoPos>> = BTreeMap::new();
    sources1.insert("alpha".into(), map_positions(a.hist_store(), U64Mapper));
    let merged1 = interleave_hist_stores(sources1, cmp_by_time);
    let items = collect_merged(merged1.next().await).await;
    let anchor = items[0].pos.clone();
    assert!(anchor.get(&"alpha".into()).is_some());
    assert!(anchor.get(&"named".into()).is_none());
    let saved_value = anchor.marshal();

    // Phase 2: add a second source. Use the same saved anchor.
    let a2 = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(50, "b2")),
    ]);
    let merged2 = wallet_store(&a2, &b);

    // Reload the saved (alpha-only) anchor against the two-source merge —
    // the missing "named" key defaults to `DuoPos::default()`, which the
    // named source's clamp treats as its own beginning.
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
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(45, "b2")),
    ]);
    let merged = wallet_store(&a, &b);
    let filtered = filter_hist_store(merged, |mi: &MergedItem<CommonEvent, DuoPos>| {
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
    hist: Hist<FilteredItem<MergedItem<CommonEvent, DuoPos>>, MergedPos<DuoPos>>,
) -> Vec<Positioned<FilteredItem<MergedItem<CommonEvent, DuoPos>>, MergedPos<DuoPos>>> {
    use futures_util::TryStreamExt;
    hist.iter().try_collect().await.unwrap()
}

#[tokio::test]
async fn merge_composes_with_since() {
    // since(interleave(a, b), cut) — split below a saved merged anchor.
    let a = Source::<MockPosU64>::new(vec![
        (MockPosU64(1), ev(10, "a1")),
        (MockPosU64(2), ev(40, "a2")),
    ]);
    let b = Source::<MockPosNamed>::new(vec![
        (named_pos("n01"), ev(20, "b1")),
        (named_pos("n02"), ev(50, "b2")),
    ]);
    let merged = wallet_store(&a, &b);

    // A cut "between a1 and b1": alpha consumed through 1, named untouched —
    // built directly with the public typed constructor.
    let cut = MergedPos::from([
        ("alpha", DuoPos::U64(MockPosU64(1))),
        ("named", DuoPos::Named(MockPosNamed::default())),
    ]);

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
    let merged = wallet_store(&a, &b);

    let mut stream = watch_hist(&merged, MergedPos::default());
    // First item: a1.
    let first = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("first item within timeout")
        .unwrap()
        .unwrap();
    assert_eq!(first.item.value.label, "a1");

    // Append to b; the merged store should re-snapshot and yield it.
    b.append(named_pos("n01"), ev(20, "b1"));
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
    let b = Source::<MockPosNamed>::new(vec![(named_pos("n01"), ev(20, "b1"))]);
    let merged = wallet_store(&a, &b);

    let tp = merged.time_pos(Timestamp::new(42)).await.expect("time_pos");
    // Both source stubs return P::default(), wrapped into each family's
    // variant — verify the aggregation keeps the families apart.
    assert_eq!(
        tp.get(&"alpha".into()),
        Some(&DuoPos::U64(MockPosU64::default()))
    );
    assert_eq!(
        tp.get(&"named".into()),
        Some(&DuoPos::Named(MockPosNamed::default()))
    );
}
