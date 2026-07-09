//! The vector-clock interleave: sources with disjoint position spaces,
//! best-effort item order via a user `cmp`, per-source clock anchors
//! ([`MergedPos`]).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all, unfold};

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Subscription, Timestamp,
};
use crate::share::Share;

use super::kway::{MergeStream, take_first_error};
use super::types::{MergedCmp, MergedItem, SourceKey, Sources, TaggedEnd};

// ============================================================
// MergedPos<P> — the interleave's vector clock
// ============================================================

/// Keyed position into an [`interleave_hist_stores`] product:
/// [`SourceKey`] → that source's typed position. Missing keys are treated
/// as that source's [`Default`].
///
/// Construct via [`FromIterator`] / `From<[(K, P); N]>` (anything
/// `Into<SourceKey>` works as a key); read back via [`get`](Self::get),
/// which speaks the [`SourceKey`] domain type. Serialization only happens
/// at persistence ([`Position::marshal`] → a JSON object of per-key
/// values); deserialization lives on the interleaved store's `load`.
#[derive(Clone, Debug, Default)]
pub struct MergedPos<P: Position> {
    positions: BTreeMap<SourceKey, P>,
}

impl<P: Position> MergedPos<P> {
    /// The position recorded for `key`, or `None` if absent.
    pub fn get(&self, key: &SourceKey) -> Option<&P> {
        self.positions.get(key)
    }
}

impl<P: Position, K: Into<SourceKey>> FromIterator<(K, P)> for MergedPos<P> {
    fn from_iter<I: IntoIterator<Item = (K, P)>>(iter: I) -> Self {
        Self {
            positions: iter.into_iter().map(|(k, p)| (k.into(), p)).collect(),
        }
    }
}

impl<P: Position, K: Into<SourceKey>, const N: usize> From<[(K, P); N]> for MergedPos<P> {
    fn from(entries: [(K, P); N]) -> Self {
        entries.into_iter().collect()
    }
}

impl<P: Position> PartialEq for MergedPos<P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl<P: Position> Eq for MergedPos<P> {}
impl<P: Position> PartialOrd for MergedPos<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<P: Position> Ord for MergedPos<P> {
    /// Lexicographic over the union of source keys (both are sorted
    /// `BTreeMap`s), each component compared with `P`'s own [`Ord`]. Off the
    /// hot path: the interleave overrides `clamp_pos` with component-wise
    /// semantics, so this only backs the `Position: Ord`/`Eq` bounds and
    /// `Hist`'s `start <= end` / `is_empty` checks. A present component
    /// sorts after an absent one (a missing key ≡ that source's earliest).
    fn cmp(&self, other: &Self) -> Ordering {
        let keys: std::collections::BTreeSet<&SourceKey> = self
            .positions
            .keys()
            .chain(other.positions.keys())
            .collect();
        for k in keys {
            let ord = match (self.positions.get(k), other.positions.get(k)) {
                (Some(a), Some(b)) => a.cmp(b),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

impl<P: Position> Position for MergedPos<P> {
    /// A JSON object mapping each source key to that source's serialized
    /// position — pure composition, no framing.
    fn marshal(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.positions
                .iter()
                .map(|(k, p)| (k.0.clone(), p.marshal()))
                .collect(),
        )
    }
}

// ============================================================
// interleave_hist_stores
// ============================================================

/// Interleave `sources` with mutually incomparable position spaces into a
/// single [`HistStore`] over [`MergedItem<T, P>`].
///
/// `cmp` orders items across sources — return [`Ordering::Less`] when the
/// first argument should be yielded before the second. Ties resolve in
/// sorted-key order (stable). Each yielded item carries a vector-clock
/// [`MergedPos`] that survives a marshal/`load` round-trip and is a valid
/// [`Hist::split`] anchor for resume.
pub fn interleave_hist_stores<T, P, F>(
    sources: BTreeMap<SourceKey, HistStore<T, P>>,
    cmp: F,
) -> HistStore<MergedItem<T, P>, MergedPos<P>>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
    F: Fn(&MergedItem<T, P>, &MergedItem<T, P>) -> Ordering + Send + Sync + 'static,
{
    HistStore::new(Arc::new(InterleaveHistStoreBackend {
        sources: Arc::new(sources),
        cmp: Arc::new(cmp),
    }))
}

struct InterleaveHistStoreBackend<T, P: Position> {
    sources: Sources<T, P>,
    cmp: MergedCmp<T, P>,
}

/// The running vector clock: one position per source.
type Clock<P> = BTreeMap<SourceKey, P>;

/// Build an interleaved [`Hist`] spanning `(earliest, end]` where `earliest`
/// is each source's [`Default`] and `end` is the supplied vector clock.
fn build_interleaved_hist<T, P>(
    sources: &Sources<T, P>,
    cmp: &MergedCmp<T, P>,
    end: MergedPos<P>,
) -> Hist<MergedItem<T, P>, MergedPos<P>>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let earliest: Clock<P> = sources.keys().map(|k| (k.clone(), P::default())).collect();
    let backend: Arc<dyn HistBackend<MergedItem<T, P>, MergedPos<P>>> =
        Arc::new(InterleaveHistBackend {
            sources: sources.share(),
            cmp: cmp.share(),
        });
    Hist::new(
        backend,
        MergedPos {
            positions: earliest,
        },
        end,
    )
}

/// State for the interleaved `snapshots()` stream: seed one snapshot from
/// each child, then re-emit whenever any child advances.
enum InterleaveSnapState<T, P: Position> {
    /// Await the first end from each child to build the initial vector clock.
    Seed {
        sources: Sources<T, P>,
        cmp: MergedCmp<T, P>,
        tagged: Vec<Subscription<TaggedEnd<P>>>,
    },
    /// Steady state: `select_all` over the tagged child end-streams.
    Run {
        sources: Sources<T, P>,
        cmp: MergedCmp<T, P>,
        select: SelectAll<Subscription<TaggedEnd<P>>>,
        clock: Clock<P>,
    },
}

#[allow(clippy::type_complexity)]
async fn interleave_snap_step<T, P>(
    state: InterleaveSnapState<T, P>,
) -> Option<(
    Hist<MergedItem<T, P>, MergedPos<P>>,
    InterleaveSnapState<T, P>,
)>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    match state {
        InterleaveSnapState::Seed {
            sources,
            cmp,
            mut tagged,
        } => {
            // Seed the full vector clock BEFORE the first emit. Each child
            // stream's first item is its current tip; `watch`/snapshot streams
            // coalesce, so an advance between subscribe and this poll arrives
            // as the seed rather than being lost. Missing this seeding step is
            // the footgun pinned by `snapshots_advance_when_any_source_appends`.
            let mut clock: Clock<P> = BTreeMap::new();
            for s in &mut tagged {
                if let Some((k, p)) = s.next().await {
                    clock.insert(k, p);
                }
            }
            // Any child whose stream ended before yielding contributes its
            // earliest position, so every source key is present.
            for k in sources.keys() {
                clock.entry(k.clone()).or_default();
            }
            let hist = build_interleaved_hist(
                &sources,
                &cmp,
                MergedPos {
                    positions: clock.clone(),
                },
            );
            let select = select_all(tagged);
            Some((
                hist,
                InterleaveSnapState::Run {
                    sources,
                    cmp,
                    select,
                    clock,
                },
            ))
        }
        InterleaveSnapState::Run {
            sources,
            cmp,
            mut select,
            mut clock,
        } => {
            let (key, p) = select.next().await?;
            clock.insert(key, p);
            let hist = build_interleaved_hist(
                &sources,
                &cmp,
                MergedPos {
                    positions: clock.clone(),
                },
            );
            Some((
                hist,
                InterleaveSnapState::Run {
                    sources,
                    cmp,
                    select,
                    clock,
                },
            ))
        }
    }
}

#[async_trait::async_trait]
impl<T, P> HistStoreBackend<MergedItem<T, P>, MergedPos<P>> for InterleaveHistStoreBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn snapshots(&self) -> HistStream<MergedItem<T, P>, MergedPos<P>> {
        let sources = self.sources.share();
        let cmp = self.cmp.share();
        // Tag each child's end-stream with its source key so the k-way select
        // knows which clock entry to bump.
        let tagged: Vec<Subscription<TaggedEnd<P>>> = sources
            .iter()
            .map(|(k, s)| {
                let k = k.clone();
                Box::pin(s.snapshots().map(move |h| (k.clone(), h.end().clone())))
                    as Subscription<TaggedEnd<P>>
            })
            .collect();
        Box::pin(unfold(
            InterleaveSnapState::Seed {
                sources,
                cmp,
                tagged,
            },
            interleave_snap_step,
        ))
    }

    async fn next(&self) -> Hist<MergedItem<T, P>, MergedPos<P>> {
        // Current tip = each source's current snapshot end.
        let mut clock: Clock<P> = BTreeMap::new();
        for (k, s) in self.sources.iter() {
            clock.insert(k.clone(), s.next().await.end().clone());
        }
        build_interleaved_hist(&self.sources, &self.cmp, MergedPos { positions: clock })
    }

    async fn time_pos(&self, time: Timestamp) -> Result<MergedPos<P>, HistoryError> {
        let mut positions: Clock<P> = BTreeMap::new();
        for (k, s) in self.sources.iter() {
            positions.insert(k.clone(), s.time_pos(time).await?);
        }
        Ok(MergedPos { positions })
    }

    fn load(&self, value: serde_json::Value) -> Option<MergedPos<P>> {
        // The persisted form is a JSON object keyed by source; each source
        // reconstructs its own typed position via its store `load` (the
        // deserialize-on-the-store asymmetry). A missing key → that source's
        // earliest, so a saved anchor from a smaller source set still resumes.
        let obj = value.as_object()?;
        let mut positions: Clock<P> = BTreeMap::new();
        for (key, store) in self.sources.iter() {
            let p = match obj.get(key.as_str()) {
                Some(v) => store.load(v.clone())?,
                None => P::default(),
            };
            positions.insert(key.clone(), p);
        }
        Some(MergedPos { positions })
    }
}

// ============================================================
// Interleave hist backend (iter + component-wise clamp)
// ============================================================

struct InterleaveHistBackend<T, P: Position> {
    sources: Sources<T, P>,
    cmp: MergedCmp<T, P>,
}

impl<T, P> HistBackend<MergedItem<T, P>, MergedPos<P>> for InterleaveHistBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn iter(
        &self,
        start: MergedPos<P>,
        end: MergedPos<P>,
    ) -> ItemStream<MergedItem<T, P>, MergedPos<P>> {
        Box::pin(unfold(
            InterleaveIterState::Init {
                sources: self.sources.share(),
                cmp: self.cmp.share(),
                start,
                end,
            },
            interleave_iter_step,
        ))
    }

    /// Component-wise clamping. For each source key, clamp its position to
    /// `[start[k], end[k]]` using `P`'s own `Ord`. Missing keys substitute
    /// [`Default`] (so a saved [`MergedPos`] from a smaller source set still
    /// resumes).
    fn clamp_pos(&self, p: MergedPos<P>, start: &MergedPos<P>, end: &MergedPos<P>) -> MergedPos<P> {
        let mut clamped: Clock<P> = BTreeMap::new();
        for key in self.sources.keys() {
            let start_p = start.positions.get(key).cloned().unwrap_or_default();
            let end_p = end.positions.get(key).cloned().unwrap_or_default();
            let p_k = p.positions.get(key).cloned().unwrap_or_default();
            let chosen = if p_k < start_p {
                start_p
            } else if p_k > end_p {
                end_p
            } else {
                p_k
            };
            clamped.insert(key.clone(), chosen);
        }
        MergedPos { positions: clamped }
    }
}

struct InterleaveState<T, P: Position> {
    streams: Vec<MergeStream<T, P>>,
    clock: Clock<P>,
    cmp: MergedCmp<T, P>,
}

enum InterleaveIterState<T, P: Position> {
    Init {
        sources: Sources<T, P>,
        cmp: MergedCmp<T, P>,
        start: MergedPos<P>,
        end: MergedPos<P>,
    },
    Run(InterleaveState<T, P>),
}

#[allow(clippy::type_complexity)]
async fn interleave_iter_step<T, P>(
    state: InterleaveIterState<T, P>,
) -> Option<(
    Result<Positioned<MergedItem<T, P>, MergedPos<P>>, HistoryError>,
    InterleaveIterState<T, P>,
)>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let mut state = match state {
        InterleaveIterState::Init {
            sources,
            cmp,
            start,
            end,
        } => {
            let mut clock: Clock<P> = BTreeMap::new();
            let mut streams = Vec::with_capacity(sources.len());
            for (key, store) in sources.iter() {
                let start_p = start.positions.get(key).cloned().unwrap_or_default();
                let end_p = end.positions.get(key).cloned().unwrap_or_default();
                clock.insert(key.clone(), start_p.clone());
                // Only build a per-source iter if the window is non-empty.
                if start_p < end_p {
                    let hist = store.next().await;
                    let (_, tail) = hist.split(start_p);
                    let (window, _) = tail.split(end_p.clone());
                    streams.push(MergeStream {
                        key: key.clone(),
                        stream: window.iter(),
                        head: None,
                        exhausted: false,
                        end_pos: end_p,
                    });
                }
            }
            InterleaveIterState::Run(InterleaveState {
                streams,
                clock,
                cmp,
            })
        }
        run @ InterleaveIterState::Run(_) => run,
    };
    let InterleaveIterState::Run(st) = &mut state else {
        unreachable!("Init transitions to Run above")
    };
    fill_heads_interleave(&mut st.streams, &mut st.clock).await;
    if let Some(err) = take_first_error(&mut st.streams) {
        return Some((Err(err), state));
    }
    let idx = pick_winner_interleave(st)?;
    let p = st.streams[idx]
        .head
        .take()
        .expect("winner head set")
        .expect("error case handled");
    let key = st.streams[idx].key.clone();
    st.clock.insert(key.clone(), p.pos.clone());
    let merged_pos = MergedPos {
        positions: st.clock.clone(),
    };
    let item = MergedItem {
        value: p.item,
        source_key: key,
        source_pos: p.pos,
    };
    Some((Ok(Positioned::new(item, merged_pos)), state))
}

/// Pull a head from any stream that doesn't have one yet. Streams that
/// finish within their window advance the vector clock to their `end_pos`
/// so resumes know we consumed through.
async fn fill_heads_interleave<T, P>(streams: &mut [MergeStream<T, P>], clock: &mut Clock<P>)
where
    T: Send + Sync + 'static,
    P: Position,
{
    for s in streams {
        if s.head.is_some() || s.exhausted {
            continue;
        }
        if let Some(item) = s.stream.next().await {
            s.head = Some(item);
        } else {
            s.exhausted = true;
            clock.insert(s.key.clone(), s.end_pos.clone());
        }
    }
}

/// Index of the stream whose head is smallest by the user `cmp`. Skips
/// streams without an `Ok` head; `None` when nothing is yieldable.
fn pick_winner_interleave<T, P>(st: &InterleaveState<T, P>) -> Option<usize>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let mut winner: Option<usize> = None;
    for i in 0..st.streams.len() {
        if !matches!(&st.streams[i].head, Some(Ok(_))) {
            continue;
        }
        winner = Some(match winner {
            None => i,
            Some(j) => {
                let a = head_as_merged_item(&st.streams[i]);
                let b = head_as_merged_item(&st.streams[j]);
                if (st.cmp)(&a, &b) == Ordering::Less {
                    i
                } else {
                    j
                }
            }
        });
    }
    winner
}

/// Materialize a stream's `Ok` head as a [`MergedItem`] for the user `cmp`.
fn head_as_merged_item<T, P>(s: &MergeStream<T, P>) -> MergedItem<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let p = s
        .head
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .expect("head checked Ok");
    MergedItem {
        value: p.item.clone(),
        source_key: s.key.clone(),
        source_pos: p.pos.clone(),
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::tests_common::{
        MockEvent, MockPos, MockSource, collect, ev, labels, two_sources,
    };
    use std::time::Duration;
    use tokio::time::timeout;

    fn cmp_by_time(
        a: &MergedItem<MockEvent, MockPos>,
        b: &MergedItem<MockEvent, MockPos>,
    ) -> Ordering {
        a.value.time.cmp(&b.value.time)
    }

    #[tokio::test]
    async fn merged_pos_public_construction() {
        let pos = MergedPos::from([("a", MockPos(3)), ("b", MockPos(1))]);
        assert_eq!(pos.get(&"a".into()), Some(&MockPos(3)));
        assert_eq!(pos.get(&"b".into()), Some(&MockPos(1)));
        assert_eq!(pos.get(&"c".into()), None);
        let same: MergedPos<MockPos> = [("a", MockPos(3)), ("b", MockPos(1))].into_iter().collect();
        assert_eq!(pos, same);
    }

    #[tokio::test]
    async fn merged_pos_marshal_round_trip() {
        use crate::arch::Position;

        // Round-trips through the store's `load` (deserialize-on-the-store).
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(30, "a2"))]);
        let b = MockSource::new(vec![(1, ev(20, "b1"))]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);
        let items = collect(merged.next().await).await;
        let pos = items[0].pos.clone();

        let value = pos.marshal();
        let restored = merged.load(value).expect("load");
        assert_eq!(restored, pos);
    }

    #[tokio::test]
    async fn interleave_orders_by_provided_cmp() {
        // a@1 (time=10), a@2 (time=30), b@1 (time=20)
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(30, "a2"))]);
        let b = MockSource::new(vec![(1, ev(20, "b1"))]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);

        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "b1", "a2"]);
        let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "a"]);
    }

    #[tokio::test]
    async fn vector_clock_advances_per_source_only() {
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(30, "a2"))]);
        let b = MockSource::new(vec![(1, ev(20, "b1"))]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);
        let items = collect(merged.next().await).await;

        // After yielding a1 (a@1), vector_clock = {a: 1, b: 0}.
        assert_eq!(items[0].pos.get(&"a".into()), Some(&MockPos(1)));
        assert_eq!(items[0].pos.get(&"b".into()), Some(&MockPos(0)));

        // After yielding b1 (b@1), vector_clock = {a: 1, b: 1}.
        assert_eq!(items[1].pos.get(&"a".into()), Some(&MockPos(1)));
        assert_eq!(items[1].pos.get(&"b".into()), Some(&MockPos(1)));

        // After yielding a2 (a@2), vector_clock = {a: 2, b: 1}.
        assert_eq!(items[2].pos.get(&"a".into()), Some(&MockPos(2)));
        assert_eq!(items[2].pos.get(&"b".into()), Some(&MockPos(1)));
    }

    #[tokio::test]
    async fn split_resumes_after_anchor_per_source() {
        let a = MockSource::new(vec![
            (1, ev(10, "a1")),
            (2, ev(30, "a2")),
            (3, ev(50, "a3")),
        ]);
        let b = MockSource::new(vec![(1, ev(20, "b1")), (2, ev(40, "b2"))]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);

        let hist = merged.next().await;
        let items_all = collect(hist.clone()).await;
        // Expected: a1(10), b1(20), a2(30), b2(40), a3(50).
        assert_eq!(items_all.len(), 5);

        // Anchor after b1 (index 1).
        let anchor = items_all[1].pos.clone();
        let (_, tail) = hist.split(anchor);
        let items_tail = collect(tail).await;
        assert_eq!(labels(&items_tail), vec!["a2", "b2", "a3"]);
    }

    #[tokio::test]
    async fn merged_pos_handles_added_source() {
        use crate::arch::Position;

        // Merge with one source, save anchor, then merge with two sources
        // using the same saved anchor (missing the new source's key).
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(30, "a2"))]);
        let mut sources1 = BTreeMap::new();
        sources1.insert(SourceKey::from("a"), a.hist_store());
        let merged1 = interleave_hist_stores(sources1, cmp_by_time);
        let items = collect(merged1.next().await).await;
        let anchor_after_a1 = items[0].pos.clone();
        assert!(anchor_after_a1.get(&"a".into()).is_some());
        assert!(anchor_after_a1.get(&"b".into()).is_none());

        // Serialize the saved (a-only) anchor to simulate persistence.
        let value = anchor_after_a1.marshal();

        // Build a new merge that adds source "b".
        let a2 = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(30, "a2"))]);
        let b = MockSource::new(vec![(1, ev(20, "b1")), (2, ev(40, "b2"))]);
        let merged2 = interleave_hist_stores(two_sources(&a2, &b), cmp_by_time);

        // Reload against the two-source merge — the missing "b" key
        // defaults to b's earliest.
        let restored = merged2.load(value).expect("load");
        let hist = merged2.next().await;
        let (_, tail) = hist.split(restored);
        let items_tail = collect(tail).await;
        // Items after anchor_after_a1: from "a" only items past pos=1 (so
        // a2); from "b" everything (it wasn't in the old anchor → default
        // = pos 0). By time: b1(20), a2(30), b2(40).
        assert_eq!(labels(&items_tail), vec!["b1", "a2", "b2"]);
    }

    #[tokio::test]
    async fn empty_source_does_not_block_others() {
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(20, "a2"))]);
        let b = MockSource::new(vec![]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn stable_order_on_cmp_tie() {
        // Tie at time=10 — sorted-key order wins: "a" before "z".
        let a = MockSource::new(vec![(1, ev(10, "a1"))]);
        let z = MockSource::new(vec![(1, ev(10, "z1"))]);
        let mut sources = BTreeMap::new();
        sources.insert(SourceKey::from("z"), z.hist_store());
        sources.insert(SourceKey::from("a"), a.hist_store());
        let merged = interleave_hist_stores(sources, cmp_by_time);
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "z1"]);
    }

    #[tokio::test]
    async fn snapshots_advance_when_any_source_appends() {
        let a = MockSource::new(vec![(1, ev(10, "a1"))]);
        let b = MockSource::new(vec![]);
        let merged = interleave_hist_stores(two_sources(&a, &b), cmp_by_time);
        let mut snaps = merged.snapshots();

        let h1 = snaps.next().await.expect("first snapshot");
        assert_eq!(labels(&collect(h1).await), vec!["a1"]);

        // Append to b — merged store should emit a new snapshot.
        b.append(1, ev(20, "b1"));
        // The aggregation is async; give it a chance to propagate.
        let h2 = timeout(Duration::from_secs(2), snaps.next())
            .await
            .expect("snapshot within timeout")
            .expect("snapshot stream not closed");
        assert_eq!(labels(&collect(h2).await), vec!["a1", "b1"]);
    }
}
