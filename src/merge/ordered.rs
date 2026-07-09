//! The ordered merge: sources sharing one position space, stream order =
//! `P`'s total order, plain-`P` resume anchors, same-position coalescing.

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
use super::types::{MergedItem, SourceKey, Sources, TaggedEnd};

// ============================================================
// merge_hist_stores
// ============================================================

/// Merge `sources` sharing one position space into a single [`HistStore`]
/// over [`MergedItem<T, P>`], ordered by `P`'s own total order. The merged
/// store's positions are plain `P` — an anchor saved from the merged stream
/// is an ordinary position, interchangeable with the inner stores'.
///
/// **Coalescing:** within one position space a position *identifies* an
/// item, so sources carrying an item at the identical position (overlapping
/// filters — e.g. a self-transfer matching both `OwnedErc20` filters) are
/// reporting the same item. The merged stream yields each position **once**,
/// from the lowest [`SourceKey`]; the other copies are dropped.
///
/// The merged snapshot end is the minimum of the sources' ends — see the
/// [module docs](crate::merge).
pub fn merge_hist_stores<T, P>(
    sources: BTreeMap<SourceKey, HistStore<T, P>>,
) -> HistStore<MergedItem<T, P>, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    HistStore::new(Arc::new(OrderedHistStoreBackend {
        sources: Arc::new(sources),
    }))
}

struct OrderedHistStoreBackend<T, P: Position> {
    sources: Sources<T, P>,
}

/// Build an ordered merged [`Hist`] spanning `(P::default(), end]`.
fn build_ordered_hist<T, P>(sources: &Sources<T, P>, end: P) -> Hist<MergedItem<T, P>, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let backend: Arc<dyn HistBackend<MergedItem<T, P>, P>> = Arc::new(OrderedHistBackend {
        sources: sources.share(),
    });
    Hist::new(backend, P::default(), end)
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
) -> Option<(Hist<MergedItem<T, P>, P>, OrderedSnapState<T, P>)>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
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
impl<T, P> HistStoreBackend<MergedItem<T, P>, P> for OrderedHistStoreBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn snapshots(&self) -> HistStream<MergedItem<T, P>, P> {
        let sources = self.sources.share();
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

    async fn next(&self) -> Hist<MergedItem<T, P>, P> {
        let mut ends: BTreeMap<SourceKey, P> = BTreeMap::new();
        for (k, s) in self.sources.iter() {
            ends.insert(k.clone(), s.next().await.end().clone());
        }
        build_ordered_hist(&self.sources, min_end(&ends))
    }

    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        // Sources share the position space, so their estimates should agree;
        // take the minimum — estimates are approximate, err toward
        // re-including, never toward missing.
        let mut min: Option<P> = None;
        for s in self.sources.values() {
            let p = s.time_pos(time).await?;
            min = Some(match min {
                Some(m) if m <= p => m,
                _ => p,
            });
        }
        Ok(min.unwrap_or_default())
    }

    fn load(&self, value: serde_json::Value) -> Option<P> {
        // All sources share `P`, so any store's `load` parses the position.
        self.sources.values().next()?.load(value)
    }
}

// ============================================================
// Ordered hist backend (iter; default Ord clamp is exact)
// ============================================================

struct OrderedHistBackend<T, P: Position> {
    sources: Sources<T, P>,
}

impl<T, P> HistBackend<MergedItem<T, P>, P> for OrderedHistBackend<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn iter(&self, start: P, end: P) -> ItemStream<MergedItem<T, P>, P> {
        Box::pin(unfold(
            OrderedIterState::Init {
                sources: self.sources.share(),
                start,
                end,
            },
            ordered_iter_step,
        ))
    }

    // No `clamp_pos` override: positions are the space's own `P`, whose
    // `Ord` is the containment order.
}

enum OrderedIterState<T, P: Position> {
    Init {
        sources: Sources<T, P>,
        start: P,
        end: P,
    },
    Run {
        streams: Vec<MergeStream<T, P>>,
        /// The last yielded position — heads at-or-below it are coalesced
        /// duplicates of an already-yielded item and get dropped.
        last: Option<P>,
    },
}

#[allow(clippy::type_complexity)]
async fn ordered_iter_step<T, P>(
    state: OrderedIterState<T, P>,
) -> Option<(
    Result<Positioned<MergedItem<T, P>, P>, HistoryError>,
    OrderedIterState<T, P>,
)>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    let mut state = match state {
        OrderedIterState::Init {
            sources,
            start,
            end,
        } => {
            let mut streams = Vec::with_capacity(sources.len());
            for (key, store) in sources.iter() {
                let hist = store.next().await;
                let (_, tail) = hist.split(start.clone());
                let (window, _) = tail.split(end.clone());
                if window.is_empty() {
                    continue;
                }
                streams.push(MergeStream {
                    key: key.clone(),
                    stream: window.iter(),
                    head: None,
                    exhausted: false,
                    end_pos: end.clone(),
                });
            }
            OrderedIterState::Run {
                streams,
                last: None,
            }
        }
        run @ OrderedIterState::Run { .. } => run,
    };
    let OrderedIterState::Run { streams, last } = &mut state else {
        unreachable!("Init transitions to Run above")
    };
    fill_heads_ordered(streams, last.as_ref()).await;
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
    *last = Some(p.pos.clone());
    let item = MergedItem {
        value: p.item,
        source_key: key,
        source_pos: p.pos.clone(),
    };
    Some((Ok(Positioned::new(item, p.pos)), state))
}

/// Pull a head for any ordered stream that doesn't have one, dropping items
/// at-or-below the last yielded position — those are the same item reported
/// by another source (one position space ⇒ one item per position), already
/// yielded from a lower key. Also clears an already-buffered head that the
/// last yield revealed to be such a duplicate.
async fn fill_heads_ordered<T, P>(streams: &mut [MergeStream<T, P>], last: Option<&P>)
where
    T: Send + Sync + 'static,
    P: Position,
{
    let is_dup = |pos: &P| last.is_some_and(|l| pos <= l);
    for s in streams {
        if let Some(Ok(head)) = &s.head
            && is_dup(&head.pos)
        {
            s.head = None;
        }
        if s.head.is_some() || s.exhausted {
            continue;
        }
        while let Some(next) = s.stream.next().await {
            match next {
                Ok(p) if is_dup(&p.pos) => {}
                other => {
                    s.head = Some(other);
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
        let b = MockSource::new(vec![(2, ev(20, "b1")), (4, ev(35, "b2"))]);
        // b leads (tip 4), a lags (tip 3) — the min frontier is 3.
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1", "b1", "a2"]);
        // Anchors are the items' own plain positions.
        assert_eq!(items[0].pos, MockPos(1));
        let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "a"]);
    }

    #[tokio::test]
    async fn same_position_coalesces_to_lowest_key() {
        // One position space ⇒ one item per position: the copy from "z" is
        // the same item as the copy from "a" (the self-transfer shape) and
        // is dropped.
        let a = MockSource::new(vec![(5, ev(10, "a1"))]);
        let z = MockSource::new(vec![(5, ev(10, "z1"))]);
        let mut sources = std::collections::BTreeMap::new();
        sources.insert("z".into(), z.hist_store());
        sources.insert("a".into(), a.hist_store());
        let merged = merge_hist_stores(sources);
        let items = collect(merged.next().await).await;
        assert_eq!(labels(&items), vec!["a1"]);
        assert_eq!(items[0].item.source_key.as_str(), "a");
    }

    #[tokio::test]
    async fn coalesced_stream_resumes_from_plain_position() {
        // Duplicates at 5 and 7; the merged stream has exactly one item per
        // position, and a saved anchor is a plain position that resumes it.
        let a = MockSource::new(vec![(5, ev(10, "a1")), (7, ev(30, "a2"))]);
        let b = MockSource::new(vec![(5, ev(10, "b1")), (7, ev(31, "b2"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let hist = merged.next().await;
        let all = collect(hist.clone()).await;
        assert_eq!(labels(&all), vec!["a1", "a2"]);

        let anchor = all[0].pos;
        assert_eq!(anchor, MockPos(5));
        let (head, tail) = hist.split(anchor);
        assert_eq!(labels(&collect(head).await), vec!["a1"]);
        assert_eq!(labels(&collect(tail).await), vec!["a2"]);
    }

    #[tokio::test]
    async fn merged_end_is_min_over_source_ends() {
        // a's tip is ahead (pos 3), b's lags (pos 2): the merged window
        // must not claim coverage past 2 — a2 stays outside until b
        // catches up.
        let a = MockSource::new(vec![(1, ev(10, "a1")), (3, ev(30, "a2"))]);
        let b = MockSource::new(vec![(2, ev(11, "b1"))]);
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
        assert_eq!(h1.end(), &MockPos(1));

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
        assert_eq!(h2.end(), &MockPos(2));
        // Position 1 coalesces (a1 wins over b1); position 2 has only a2.
        assert_eq!(labels(&collect(h2).await), vec!["a1", "a2"]);
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
    async fn anchors_marshal_as_plain_positions() {
        use crate::arch::Position;

        let a = MockSource::new(vec![(1, ev(10, "a1"))]);
        let b = MockSource::new(vec![(1, ev(11, "b1"))]);
        let merged = merge_hist_stores(two_sources(&a, &b));
        let items = collect(merged.next().await).await;

        // The merged anchor IS an inner-store position — same wire form,
        // loadable by the merged store (or any inner store).
        let pos = items[0].pos;
        assert_eq!(pos.marshal(), serde_json::json!(1));
        assert_eq!(merged.load(pos.marshal()), Some(pos));
    }
}
