//! The ordered merge: sources sharing one position space, stream order =
//! `P`'s total order, single-position resume anchors ([`OrderedPos`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all, unfold};

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Subscription, Timestamp,
};

use super::kway::{MergeStream, take_first_error};
use super::types::{MergedItem, SourceKey, Sources, TaggedEnd};

// ============================================================
// DiscretePosition
// ============================================================

/// A [`Position`] whose total order is *discrete*: every non-[`Default`]
/// position has an immediate predecessor.
///
/// Required by [`merge_hist_stores`] only. The core iteration protocol is
/// strictly exclusive-start (`(start, end]`), so resuming an ordered merge at
/// an [`At`](SourceBound::At)-tagged anchor — where later-keyed sources still
/// owe their item *at* `start.pos` — needs a fetch window opening strictly
/// below `start.pos`. `pred` names that opening; the merge drops the overlap
/// item-exactly, so a **conservative (lower) `pred` is still correct, just
/// over-fetches**. Returning `self` unchanged for a non-default position is
/// the one wrong answer (the boundary item would be silently skipped).
///
/// The vector-clock [`interleave_hist_stores`](super::interleave_hist_stores)
/// does not need this — its [`MergedPos`](super::MergedPos) anchors carry
/// each source's own last position, which is already strictly below anything
/// that source still owes.
pub trait DiscretePosition: Position {
    /// The greatest position strictly below `self`; saturates at
    /// [`Default`] (`P::default().pred() == P::default()`).
    fn pred(&self) -> Self;
}

// ============================================================
// OrderedPos — the ordered merge's anchor
// ============================================================

/// Position in an ordered-merge stream: an inner position plus a
/// [`SourceBound`] locating the cut *within* the set of items sitting
/// exactly at `pos`. Total order: `(pos, bound)` lexicographic.
///
/// * Yielded items carry `bound: At(source_key)` — exact resume among
///   equal-position duplicates.
/// * [`From<P>`](Self::from) builds `bound: After` — "everything at or
///   before `pos`, all sources" — the one-conversion anchor for callers
///   holding a plain inner position (e.g. a `--from-block` bound).
/// * [`Default`] is `(P::default(), Before)`: strictly before every real
///   item, even one sitting exactly at `P::default()`.
///
/// The tri-state (rather than a bare `(P, SourceKey)` pair) exists because
/// tips/boundary cuts must sit strictly *after* every real item at that
/// `pos`, and `Default` strictly *before* them — `After`/`Before` express
/// both without magic key sentinels.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrderedPos<P: Position> {
    pub pos: P,
    pub bound: SourceBound,
}

/// Where an [`OrderedPos`] cut sits among the items at exactly its `pos`:
/// before all of them, after the item of one tagged source, or after all of
/// them. Derived `Ord`: `Before < At(key) < After`, `At`s by key order —
/// matching the merge's cross-source tie order.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub enum SourceBound {
    #[default]
    Before,
    At(SourceKey),
    After,
}

impl<P: Position> From<P> for OrderedPos<P> {
    /// Anchor covering everything at or before `pos`, from all sources.
    fn from(pos: P) -> Self {
        Self {
            pos,
            bound: SourceBound::After,
        }
    }
}

impl<P: Position> Position for OrderedPos<P> {
    /// `{"pos": .., "bound": "before"|"after"}` or `{"pos": .., "at": "<key>"}`.
    /// Deserialization lives on the merged store's `load` (any source's
    /// `load` parses the inner `pos` — all sources share `P`).
    fn marshal(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert("pos".to_owned(), self.pos.marshal());
        match &self.bound {
            SourceBound::Before => obj.insert("bound".to_owned(), "before".into()),
            SourceBound::After => obj.insert("bound".to_owned(), "after".into()),
            SourceBound::At(key) => obj.insert("at".to_owned(), key.as_str().into()),
        };
        serde_json::Value::Object(obj)
    }
}

// ============================================================
// merge_hist_stores
// ============================================================

/// Merge `sources` sharing one position space into a single [`HistStore`]
/// over [`MergedItem<T, P>`], ordered by `P`'s own total order (ties across
/// sources resolve in [`SourceKey`] order, stable and deterministic).
///
/// The merged snapshot end is the minimum of the sources' ends — see the
/// [module docs](crate::merge). Resume anchors are [`OrderedPos`]; a plain
/// `P` converts via `OrderedPos::from`.
pub fn merge_hist_stores<T, P>(
    sources: BTreeMap<SourceKey, HistStore<T, P>>,
) -> HistStore<MergedItem<T, P>, OrderedPos<P>>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    HistStore::new(Arc::new(OrderedHistStoreBackend {
        sources: Arc::new(sources),
    }))
}

struct OrderedHistStoreBackend<T, P: Position> {
    sources: Sources<T, P>,
}

/// Build an ordered merged [`Hist`] spanning `(Default, (end, After)]`.
fn build_ordered_hist<T, P>(
    sources: &Sources<T, P>,
    end: P,
) -> Hist<MergedItem<T, P>, OrderedPos<P>>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    let backend: Arc<dyn HistBackend<MergedItem<T, P>, OrderedPos<P>>> =
        Arc::new(OrderedHistBackend {
            sources: sources.clone(),
        });
    Hist::new(backend, OrderedPos::default(), OrderedPos::from(end))
}

/// The min-over-sources frontier: the greatest position *all* sources have
/// reported coverage through.
fn min_end<P: Position>(ends: &BTreeMap<SourceKey, P>) -> P {
    ends.values().min().cloned().unwrap_or_default()
}

/// State for the ordered `snapshots()` stream: seed one end per child, emit,
/// then re-emit **only when the min frontier advances**.
enum OrderedSnapState<T, P: Position> {
    /// Await the first end from each child to seed the full frontier.
    Seed {
        sources: Sources<T, P>,
        tagged: Vec<Subscription<TaggedEnd<P>>>,
    },
    /// Steady state: `select_all` over the tagged child end-streams.
    Run {
        sources: Sources<T, P>,
        select: SelectAll<Subscription<TaggedEnd<P>>>,
        ends: BTreeMap<SourceKey, P>,
        emitted: P,
    },
}

async fn ordered_snap_step<T, P>(
    state: OrderedSnapState<T, P>,
) -> Option<(
    Hist<MergedItem<T, P>, OrderedPos<P>>,
    OrderedSnapState<T, P>,
)>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    match state {
        OrderedSnapState::Seed {
            sources,
            mut tagged,
        } => {
            // Seed every source's end BEFORE the first emit. Each child
            // stream's first item is its current tip; snapshot streams
            // coalesce, so an advance between subscribe and this poll
            // arrives as the seed rather than being lost.
            let mut ends: BTreeMap<SourceKey, P> = BTreeMap::new();
            for s in &mut tagged {
                if let Some((k, p)) = s.next().await {
                    ends.insert(k, p);
                }
            }
            // A child whose stream ended before yielding contributes its
            // earliest position, holding the frontier at the bottom.
            for k in sources.keys() {
                ends.entry(k.clone()).or_default();
            }
            let frontier = min_end(&ends);
            let hist = build_ordered_hist(&sources, frontier.clone());
            Some((
                hist,
                OrderedSnapState::Run {
                    sources,
                    select: select_all(tagged),
                    ends,
                    emitted: frontier,
                },
            ))
        }
        OrderedSnapState::Run {
            sources,
            mut select,
            mut ends,
            emitted,
        } => {
            // Advances of a *leading* source don't move the min — swallow
            // them and keep waiting for the frontier itself to move.
            loop {
                let (k, p) = select.next().await?;
                ends.insert(k, p);
                let frontier = min_end(&ends);
                if frontier > emitted {
                    let hist = build_ordered_hist(&sources, frontier.clone());
                    return Some((
                        hist,
                        OrderedSnapState::Run {
                            sources,
                            select,
                            ends,
                            emitted: frontier,
                        },
                    ));
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl<T, P> HistStoreBackend<MergedItem<T, P>, OrderedPos<P>> for OrderedHistStoreBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    fn snapshots(&self) -> HistStream<MergedItem<T, P>, OrderedPos<P>> {
        let sources = self.sources.clone();
        let tagged: Vec<Subscription<TaggedEnd<P>>> = sources
            .iter()
            .map(|(k, s)| {
                let k = k.clone();
                Box::pin(s.snapshots().map(move |h| (k.clone(), h.end().clone())))
                    as Subscription<TaggedEnd<P>>
            })
            .collect();
        Box::pin(unfold(
            OrderedSnapState::Seed { sources, tagged },
            ordered_snap_step,
        ))
    }

    async fn next(&self) -> Hist<MergedItem<T, P>, OrderedPos<P>> {
        let mut ends: BTreeMap<SourceKey, P> = BTreeMap::new();
        for (k, s) in self.sources.iter() {
            ends.insert(k.clone(), s.next().await.end().clone());
        }
        build_ordered_hist(&self.sources, min_end(&ends))
    }

    async fn time_pos(&self, time: Timestamp) -> Result<OrderedPos<P>, HistoryError> {
        // Sources share the position space, so their estimates should agree;
        // take the minimum and cut Before it, so an item at the estimated
        // position stays in the resumable tail (estimates are approximate —
        // err toward re-including, never toward missing).
        let mut min: Option<P> = None;
        for s in self.sources.values() {
            let p = s.time_pos(time).await?;
            min = Some(match min {
                Some(m) if m <= p => m,
                _ => p,
            });
        }
        Ok(OrderedPos {
            pos: min.unwrap_or_default(),
            bound: SourceBound::Before,
        })
    }

    fn load(&self, value: serde_json::Value) -> Option<OrderedPos<P>> {
        let obj = value.as_object()?;
        // All sources share `P`, so any store's `load` parses the inner pos.
        let pos = self
            .sources
            .values()
            .next()?
            .load(obj.get("pos")?.clone())?;
        let bound = if let Some(at) = obj.get("at") {
            SourceBound::At(SourceKey::new(at.as_str()?))
        } else {
            match obj.get("bound")?.as_str()? {
                "before" => SourceBound::Before,
                "after" => SourceBound::After,
                _ => return None,
            }
        };
        Some(OrderedPos { pos, bound })
    }
}

// ============================================================
// Ordered hist backend (iter; default Ord clamp is correct)
// ============================================================

struct OrderedHistBackend<T, P: Position> {
    sources: Sources<T, P>,
}

impl<T, P> HistBackend<MergedItem<T, P>, OrderedPos<P>> for OrderedHistBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    fn iter(
        &self,
        start: OrderedPos<P>,
        end: OrderedPos<P>,
    ) -> ItemStream<MergedItem<T, P>, OrderedPos<P>> {
        Box::pin(unfold(
            OrderedIterState::Init {
                sources: self.sources.clone(),
                start,
                end,
            },
            ordered_iter_step,
        ))
    }

    // No `clamp_pos` override: `OrderedPos`'s `(pos, bound)` order is the
    // real containment order (one shared position space), so the default
    // Ord-based clamp is exact.
}

enum OrderedIterState<T, P: Position> {
    Init {
        sources: Sources<T, P>,
        start: OrderedPos<P>,
        end: OrderedPos<P>,
    },
    Run {
        streams: Vec<MergeStream<T, P>>,
        start: OrderedPos<P>,
        end: OrderedPos<P>,
    },
}

#[allow(clippy::type_complexity)]
async fn ordered_iter_step<T, P>(
    state: OrderedIterState<T, P>,
) -> Option<(
    Result<Positioned<MergedItem<T, P>, OrderedPos<P>>, HistoryError>,
    OrderedIterState<T, P>,
)>
where
    T: Clone + Send + Sync + 'static,
    P: DiscretePosition,
{
    let mut state = match state {
        OrderedIterState::Init {
            sources,
            start,
            end,
        } => {
            // Unless the start bound already excludes everything at
            // `start.pos` (`After`), later-keyed sources may still owe their
            // item exactly *at* it — open the fetch window one discrete step
            // below and let the per-item bound filter drop the overlap.
            let fetch_start = match &start.bound {
                SourceBound::After => start.pos.clone(),
                SourceBound::Before | SourceBound::At(_) => start.pos.pred(),
            };
            let mut streams = Vec::with_capacity(sources.len());
            for (key, store) in sources.iter() {
                let hist = store.next().await;
                let (_, tail) = hist.split(fetch_start.clone());
                let (window, _) = tail.split(end.pos.clone());
                if window.is_empty() {
                    continue;
                }
                streams.push(MergeStream {
                    key: key.clone(),
                    stream: window.iter(),
                    head: None,
                    exhausted: false,
                    end_pos: end.pos.clone(),
                });
            }
            OrderedIterState::Run {
                streams,
                start,
                end,
            }
        }
        run @ OrderedIterState::Run { .. } => run,
    };
    let OrderedIterState::Run {
        streams,
        start,
        end,
    } = &mut state
    else {
        unreachable!("Init transitions to Run above")
    };
    fill_heads_ordered(streams, start, end).await;
    if let Some(err) = take_first_error(streams) {
        return Some((Err(err), state));
    }
    let idx = pick_winner_ordered(streams)?;
    let p = streams[idx]
        .head
        .take()
        .expect("winner head set")
        .expect("error case handled");
    let key = streams[idx].key.clone();
    let merged_pos = OrderedPos {
        pos: p.pos.clone(),
        bound: SourceBound::At(key.clone()),
    };
    let item = MergedItem {
        value: p.item,
        source_key: key,
        source_pos: p.pos,
    };
    Some((Ok(Positioned::new(item, merged_pos)), state))
}

/// Pull a head from any ordered stream that doesn't have one yet, dropping
/// items outside the `(start, end]` bound window (the pre-window overlap
/// from the `pred()` widening, and anything past the end bound — streams
/// ascend, so the first past-end item exhausts its stream).
async fn fill_heads_ordered<T, P>(
    streams: &mut [MergeStream<T, P>],
    start: &OrderedPos<P>,
    end: &OrderedPos<P>,
) where
    T: Send + Sync + 'static,
    P: Position,
{
    for s in streams {
        if s.head.is_some() || s.exhausted {
            continue;
        }
        while let Some(next) = s.stream.next().await {
            match next {
                Ok(p) => {
                    let opos = OrderedPos {
                        pos: p.pos.clone(),
                        bound: SourceBound::At(s.key.clone()),
                    };
                    if opos <= *start {
                        continue;
                    }
                    if opos > *end {
                        break;
                    }
                    s.head = Some(Ok(p));
                    break;
                }
                Err(e) => {
                    s.head = Some(Err(e));
                    break;
                }
            }
        }
        if s.head.is_none() {
            s.exhausted = true;
        }
    }
}

/// Index of the ordered stream whose head is smallest by `(pos, key)`.
/// Skips streams without an `Ok` head; `None` when nothing is yieldable.
fn pick_winner_ordered<T, P: Position>(streams: &[MergeStream<T, P>]) -> Option<usize> {
    let mut winner: Option<usize> = None;
    for (i, s) in streams.iter().enumerate() {
        let Some(Ok(p)) = &s.head else { continue };
        winner = Some(match winner {
            None => i,
            Some(j) => {
                let pj = streams[j]
                    .head
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .expect("winner head Ok");
                if (&p.pos, &s.key) < (&pj.pos, &streams[j].key) {
                    i
                } else {
                    j
                }
            }
        });
    }
    winner
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::tests_common::{MockPos, MockSource, collect, ev, labels, two_sources};
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn merges_by_position_across_sources() {
        let a = MockSource::new(vec![(1, ev(10, "a1")), (3, ev(30, "a2"))]);
        let b = MockSource::new(vec![(2, ev(20, "b1")), (3, ev(35, "b2"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "b1", "a2", "b2"]);
        // Item anchors carry the source tag.
        assert_eq!(
            items[0].pos,
            OrderedPos {
                pos: MockPos(1),
                bound: SourceBound::At("a".into())
            }
        );
        let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "a", "b"]);
    }

    #[tokio::test]
    async fn cross_source_tie_resolves_in_key_order() {
        let a = MockSource::new(vec![(5, ev(10, "a1"))]);
        let z = MockSource::new(vec![(5, ev(10, "z1"))]);
        let mut sources = std::collections::BTreeMap::new();
        sources.insert("z".into(), z.hist_store());
        sources.insert("a".into(), a.hist_store());
        let merged = merge_hist_stores(sources);
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "z1"]);
    }

    #[tokio::test]
    async fn duplicate_position_exact_resume() {
        // The self-transfer shape: both sources carry an item at the
        // identical position. An anchor between the two copies resumes
        // exactly at the second copy.
        let a = MockSource::new(vec![(5, ev(10, "a1")), (7, ev(30, "a2"))]);
        let b = MockSource::new(vec![(5, ev(10, "b1")), (7, ev(31, "b2"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let hist = merged.next().await;
        let all = collect(hist.clone()).await;
        assert_eq!(labels(&all), vec!["a1", "b1", "a2", "b2"]);

        // Anchor after the first copy at pos 5 (source "a").
        let anchor = all[0].pos.clone();
        assert_eq!(anchor.bound, SourceBound::At("a".into()));
        let (head, tail) = hist.split(anchor);
        assert_eq!(labels(&collect(head).await), vec!["a1"]);
        assert_eq!(labels(&collect(tail).await), vec!["b1", "a2", "b2"]);
    }

    #[tokio::test]
    async fn from_position_anchor_covers_whole_position() {
        // `OrderedPos::from(p)` = everything at or before p, all sources:
        // both copies at pos 5 land in the head; the tail starts past 5.
        let a = MockSource::new(vec![(5, ev(10, "a1")), (7, ev(30, "a2"))]);
        let b = MockSource::new(vec![(5, ev(10, "b1")), (7, ev(31, "b2"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let hist = merged.next().await;
        let (head, tail) = hist.split(OrderedPos::from(MockPos(5)));
        assert_eq!(labels(&collect(head).await), vec!["a1", "b1"]);
        assert_eq!(labels(&collect(tail).await), vec!["a2", "b2"]);
    }

    #[tokio::test]
    async fn merged_end_is_min_over_source_ends() {
        // a's tip is ahead (pos 3), b's lags (pos 1): the merged window
        // must not claim coverage past 1 — a2 stays outside until b
        // catches up.
        let a = MockSource::new(vec![(1, ev(10, "a1")), (3, ev(30, "a2"))]);
        let b = MockSource::new(vec![(1, ev(11, "b1"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "b1"]);
    }

    #[tokio::test]
    async fn snapshots_gate_on_min_frontier() {
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(20, "a2"))]);
        let b = MockSource::new(vec![(1, ev(11, "b1"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let mut snaps = merged.snapshots();

        let h1 = snaps.next().await.expect("first snapshot");
        assert_eq!(h1.end(), &OrderedPos::from(MockPos(1)));

        // Advancing the *leading* source doesn't move the min frontier —
        // no snapshot.
        a.append(3, ev(30, "a3"));
        assert!(
            timeout(Duration::from_millis(200), snaps.next())
                .await
                .is_err(),
            "leading-source advance must not emit a snapshot"
        );

        // Advancing the lagging source does.
        b.append(2, ev(21, "b2"));
        let h2 = timeout(Duration::from_secs(2), snaps.next())
            .await
            .expect("snapshot within timeout")
            .expect("snapshot stream not closed");
        assert_eq!(h2.end(), &OrderedPos::from(MockPos(2)));
        assert_eq!(labels(&collect(h2).await), vec!["a1", "b1", "a2", "b2"]);
    }

    #[tokio::test]
    async fn empty_caught_up_source_does_not_block() {
        // An empty source whose tip has advanced (it reported "nothing
        // here through pos 2") doesn't hold the frontier down.
        let a = MockSource::new(vec![(1, ev(10, "a1")), (2, ev(20, "a2"))]);
        let b = MockSource::new(vec![]);
        b.advance_tip(2);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn marshal_round_trips_all_bound_forms() {
        use crate::arch::Position;

        let a = MockSource::new(vec![(1, ev(10, "a1"))]);
        let b = MockSource::new(vec![(1, ev(11, "b1"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;

        let at = items[0].pos.clone();
        let after = OrderedPos::from(MockPos(1));
        let before = OrderedPos::<MockPos>::default();
        for pos in [at, after, before] {
            let restored = merged.load(pos.marshal()).expect("load");
            assert_eq!(restored, pos);
        }
    }
}
