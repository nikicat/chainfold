//! Phase 6 acceptance: `Matcher` abstraction.
//!
//! Source-agnostic — uses two independent `Source<SP=SrcPos>` and
//! `Source<DP=DstPos>` mock backends so the cross-source nature is
//! exercised even though the position types happen to match shape. Tests
//! cover pure-compute purity, anchor-driven output transitions, and
//! `AnchoredMatcher` persistence across simulated restarts via
//! `SqliteAnchorStore` on a tempfile.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chainfold::arch::{
    AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError,
    ItemStream, Position, Positioned, Store, Timestamp,
};
use chainfold::matcher::{
    match_fn, AnchorStore, AnchoredMatcher, BaseMatcher, InMemoryAnchorStore, Match, MatchFn,
    MatchInput, MatchOutput, Matcher, MatcherAnchors, SqliteAnchorStore,
};
use futures_core::Stream;
use futures_util::stream::{iter, poll_fn};
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

// ============================================================
// MockPos — same shape for both src and dst in these tests
// ============================================================

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
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
struct Ev {
    bridge_id: u64,
    label: String,
}

fn ev(bridge_id: u64, label: &str) -> Ev {
    Ev {
        bridge_id,
        label: label.into(),
    }
}

// ============================================================
// MockSource (mirror of the lib-internal one but parametric per-test)
// ============================================================

struct MockSource {
    items: Mutex<Vec<Ev>>,
    latest_store: Store<MockPos>,
    latest_tx: watch::Sender<MockPos>,
    best_store: Store<MockPos>,
    safe_store: Store<MockPos>,
    self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl MockSource {
    fn new(initial: Vec<Ev>) -> Arc<Self> {
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

    fn append(&self, e: Ev) {
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

    fn hist_store(self: &Arc<Self>) -> HistStore<Ev, MockPos> {
        HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<Ev, MockPos>>)
    }
}

impl HistBackend<Ev, MockPos> for MockSource {
    fn iter(&self, start: MockPos, end: MockPos) -> ItemStream<Ev, MockPos> {
        let items = self.items.lock().unwrap().clone();
        let from = (start.0 as usize).min(items.len());
        let to = (end.0 as usize).min(items.len());
        let yielded: Vec<Positioned<Ev, MockPos>> = items
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
impl HistStoreBackend<Ev, MockPos> for MockSource {
    fn snapshots(&self) -> HistStream<Ev, MockPos> {
        let backend: Arc<dyn HistBackend<Ev, MockPos>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        Box::pin(poll_fn(move |cx| {
            Pin::new(&mut sub)
                .poll_next(cx)
                .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
        }))
    }

    async fn next(&self) -> Hist<Ev, MockPos> {
        let backend: Arc<dyn HistBackend<Ev, MockPos>> = self.arc_self();
        Hist::new(backend, MockPos(0), self.latest_store.get())
    }

    fn subscribe(&self, cb: Box<dyn Fn(Hist<Ev, MockPos>) + Send + Sync>) -> AsyncDisposable {
        let backend: Arc<dyn HistBackend<Ev, MockPos>> = self.arc_self();
        let mut sub = self.latest_store.subscribe();
        let cancel = CancellationToken::new();
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

// ============================================================
// Bridge-id match fn
// ============================================================

fn bridge_match_fn(
) -> Arc<dyn MatchFn<Ev, Ev, MatchOutput<Ev, Ev>, MockPos, MockPos>> {
    match_fn(|input: MatchInput<Ev, Ev, MockPos, MockPos>| {
        let mut dst_by_id: std::collections::HashMap<u64, Vec<Ev>> =
            std::collections::HashMap::new();
        for d in &input.dst_items {
            dst_by_id
                .entry(d.item.bridge_id)
                .or_default()
                .push(d.item.clone());
        }
        let mut matched = Vec::new();
        let mut pending = Vec::new();
        for s in &input.src_items {
            if let Some(bucket) = dst_by_id.get_mut(&s.item.bridge_id) {
                if !bucket.is_empty() {
                    let d = bucket.remove(0);
                    matched.push(Match {
                        src: s.item.clone(),
                        dst: d,
                    });
                    continue;
                }
            }
            pending.push(s.item.clone());
        }
        let orphans: Vec<Ev> = dst_by_id.into_values().flatten().collect();
        Ok(MatchOutput {
            matched,
            pending,
            orphans,
        })
    })
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn end_to_end_pair_burn_mint() {
    let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2"), ev(3, "burn3")]);
    let dst = MockSource::new(vec![ev(1, "mint1"), ev(2, "mint2")]);
    let base = BaseMatcher::new(src.hist_store(), dst.hist_store(), bridge_match_fn());
    let state = base
        .compute_state(MatcherAnchors::new(MockPos(0), MockPos(0)))
        .await
        .unwrap();
    assert_eq!(state.output.matched.len(), 2);
    assert_eq!(state.output.pending.len(), 1);
    assert_eq!(state.output.pending[0].label, "burn3");
    assert_eq!(state.output.orphans.len(), 0);
}

#[tokio::test]
async fn anchored_matcher_persists_across_restart() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_str().unwrap().to_owned();

    // Phase A — create matcher, advance anchors, persist via SQLite.
    {
        let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
        let dst = MockSource::new(vec![ev(1, "mint1"), ev(2, "mint2")]);
        let store = Arc::new(SqliteAnchorStore::open(&path).expect("open sqlite"));
        let m: Matcher<Ev, Ev, MockPos, MockPos> = AnchoredMatcher::new(
            src.hist_store(),
            dst.hist_store(),
            bridge_match_fn(),
            store as Arc<dyn AnchorStore>,
            "matcher-test",
            0,
        );
        let s0 = m.current_state().await.unwrap();
        assert_eq!(s0.output.matched.len(), 2);

        // Advance both anchors past the first matched pair.
        m.set_anchors(MatcherAnchors::new(MockPos(1), MockPos(1)))
            .await
            .unwrap();
    }

    // Phase B — reopen with a fresh matcher pointing at the same sqlite
    // file and the same anchor key. Anchors should persist.
    {
        let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
        let dst = MockSource::new(vec![ev(1, "mint1"), ev(2, "mint2")]);
        let store = Arc::new(SqliteAnchorStore::open(&path).expect("reopen sqlite"));
        let m: Matcher<Ev, Ev, MockPos, MockPos> = AnchoredMatcher::new(
            src.hist_store(),
            dst.hist_store(),
            bridge_match_fn(),
            store as Arc<dyn AnchorStore>,
            "matcher-test",
            0,
        );
        // Should load the persisted (1, 1) anchors — only burn2/mint2 left.
        let anchors = m.get_anchors().await.unwrap();
        assert_eq!(anchors, MatcherAnchors::new(MockPos(1), MockPos(1)));

        let s1 = m.current_state().await.unwrap();
        assert_eq!(s1.output.matched.len(), 1);
        assert_eq!(s1.output.matched[0].src.label, "burn2");
        assert_eq!(s1.output.matched[0].dst.label, "mint2");
        assert_eq!(s1.output.pending.len(), 0);
        assert_eq!(s1.output.orphans.len(), 0);
    }
}

#[tokio::test]
async fn iter_advances_on_either_side_append() {
    let src = MockSource::new(vec![]);
    let dst = MockSource::new(vec![]);
    let store = Arc::new(InMemoryAnchorStore::new());
    let m: Arc<Matcher<Ev, Ev, MockPos, MockPos>> = Arc::new(AnchoredMatcher::new(
        src.hist_store(),
        dst.hist_store(),
        bridge_match_fn(),
        store as Arc<dyn AnchorStore>,
        "matcher-test",
        0,
    ));
    let mut stream = m.iter();

    // Initial state is empty.
    let s0 = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("initial within timeout")
        .expect("not closed")
        .unwrap();
    assert!(s0.output.matched.is_empty());
    assert!(s0.output.pending.is_empty());

    // Append a src burn — should show up in pending.
    src.append(ev(1, "burn1"));
    let s1 = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("src tick")
        .expect("not closed")
        .unwrap();
    assert_eq!(s1.output.pending.len(), 1);

    // Append the dst mint — should now show up as matched.
    dst.append(ev(1, "mint1"));
    let s2 = timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("dst tick")
        .expect("not closed")
        .unwrap();
    assert_eq!(s2.output.matched.len(), 1);
    assert_eq!(s2.output.matched[0].src.label, "burn1");
    assert_eq!(s2.output.matched[0].dst.label, "mint1");
}

#[tokio::test]
async fn pure_recompute_does_not_mutate_store() {
    let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
    let dst = MockSource::new(vec![ev(1, "mint1")]);
    let store = Arc::new(InMemoryAnchorStore::new());
    let m: Matcher<Ev, Ev, MockPos, MockPos> = AnchoredMatcher::new(
        src.hist_store(),
        dst.hist_store(),
        bridge_match_fn(),
        store.clone() as Arc<dyn AnchorStore>,
        "matcher-test",
        0,
    );

    // First load. The store starts empty, so get_anchors falls back to
    // time_anchors(start) = (default, default). load_anchors is a private
    // method, so we test via the public surface: persistence isn't touched
    // because the time fallback doesn't write back.
    let _s1 = m.current_state().await.unwrap();
    assert_eq!(store.get("matcher-test").await.unwrap(), None);

    // Calling current_state again still doesn't write the cache.
    let _s2 = m.current_state().await.unwrap();
    assert_eq!(store.get("matcher-test").await.unwrap(), None);

    // Explicit set_anchors persists.
    m.set_anchors(MatcherAnchors::new(MockPos(1), MockPos(1)))
        .await
        .unwrap();
    assert!(store.get("matcher-test").await.unwrap().is_some());
}

#[tokio::test]
async fn get_history_returns_full_window_regardless_of_persisted_anchors() {
    let src = MockSource::new(vec![ev(1, "burn1")]);
    let dst = MockSource::new(vec![ev(1, "mint1")]);
    let store = Arc::new(InMemoryAnchorStore::new());
    let key = "matcher-test";
    store
        .set(key, MatcherAnchors::new(MockPos(99), MockPos(99)).marshal())
        .await
        .unwrap();
    let m: Matcher<Ev, Ev, MockPos, MockPos> = AnchoredMatcher::new(
        src.hist_store(),
        dst.hist_store(),
        bridge_match_fn(),
        store as Arc<dyn AnchorStore>,
        key,
        0,
    );

    // current_state respects persisted anchors → empty.
    let cs = m.current_state().await.unwrap();
    assert!(cs.output.matched.is_empty());

    // get_history uses time-anchors → sees everything.
    let hs = m.base().get_history(0).await.unwrap();
    assert_eq!(hs.output.matched.len(), 1);
}

#[tokio::test]
async fn missing_anchor_key_falls_back_to_time_anchors() {
    let src = MockSource::new(vec![ev(1, "burn1")]);
    let dst = MockSource::new(vec![ev(1, "mint1")]);
    let store = Arc::new(InMemoryAnchorStore::new());
    let m: Matcher<Ev, Ev, MockPos, MockPos> = AnchoredMatcher::new(
        src.hist_store(),
        dst.hist_store(),
        bridge_match_fn(),
        store as Arc<dyn AnchorStore>,
        "fresh-key",
        0,
    );
    // No persisted anchors → time_anchors(0) → default = (0, 0) → sees
    // burn1 ↔ mint1 matched.
    let s = m.current_state().await.unwrap();
    assert_eq!(s.output.matched.len(), 1);
}
