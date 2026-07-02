//! `filter_hist_store` — keep only items matching a predicate.
//!
//! Each yielded [`FilteredItem`] carries the run of preceding filtered-out
//! items on `.skipped`, so callers that need to reconstruct the upstream
//! prefix between yields can do so without re-iterating.
//!
//! Positions pass through unchanged. Anchors taken from the filtered
//! stream are still valid against the upstream, since `Hist::split(p)`
//! cuts on position only.

use std::sync::Arc;

use futures_util::StreamExt;

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Timestamp,
};

type Predicate<Item> = Arc<dyn Fn(&Item) -> bool + Send + Sync>;

/// Item emitted by a filtered history. Carries the kept item and the run
/// of preceding upstream items that the predicate rejected since the last
/// yield (or since the start of the window for the first yield).
#[derive(Clone, Debug)]
pub struct FilteredItem<Item> {
    pub value: Item,
    pub skipped: Vec<Item>,
}

/// Keep only items in `upstream` for which `pred` returns `true`. The
/// filter is sync — callers needing async predicates should `map_hist_store`
/// the async work into a `bool` field first.
pub fn filter_hist_store<Item, P, F>(
    upstream: HistStore<Item, P>,
    pred: F,
) -> HistStore<FilteredItem<Item>, P>
where
    Item: Clone + Send + Sync + 'static,
    P: Position,
    F: Fn(&Item) -> bool + Send + Sync + 'static,
{
    let pred: Predicate<Item> = Arc::new(pred);
    HistStore::new(Arc::new(FilteredHistStoreBackend { upstream, pred }))
}

// ============================================================
// FilteredHistStoreBackend
// ============================================================

struct FilteredHistStoreBackend<Item, P: Position> {
    upstream: HistStore<Item, P>,
    pred: Predicate<Item>,
}

#[async_trait::async_trait]
impl<Item, P> HistStoreBackend<FilteredItem<Item>, P> for FilteredHistStoreBackend<Item, P>
where
    Item: Clone + Send + Sync + 'static,
    P: Position,
{
    fn snapshots(&self) -> HistStream<FilteredItem<Item>, P> {
        let pred = self.pred.clone();
        Box::pin(
            self.upstream
                .snapshots()
                .map(move |hist| wrap_hist(&hist, pred.clone())),
        )
    }

    async fn next(&self) -> Hist<FilteredItem<Item>, P> {
        wrap_hist(&self.upstream.next().await, self.pred.clone())
    }

    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        self.upstream.time_pos(time).await
    }

    fn load(&self, value: serde_json::Value) -> Option<P> {
        self.upstream.load(value)
    }
}

// ============================================================
// FilteredHistBackend
// ============================================================

struct FilteredHistBackend<Item, P: Position> {
    upstream: Arc<dyn HistBackend<Item, P>>,
    pred: Predicate<Item>,
}

impl<Item, P> HistBackend<FilteredItem<Item>, P> for FilteredHistBackend<Item, P>
where
    Item: Clone + Send + Sync + 'static,
    P: Position,
{
    fn iter(&self, start: P, end: P) -> ItemStream<FilteredItem<Item>, P> {
        let pred = self.pred.clone();
        let mut skipped: Vec<Item> = Vec::new();
        Box::pin(self.upstream.iter(start, end).filter_map(move |result| {
            let pred = pred.clone();
            // Move `skipped` into the per-iteration closure via a re-borrow:
            // we can't directly capture by `&mut` in a `FnMut`-returning
            // combinator, so accumulate into a local that's drained on
            // every yield.
            let action = match result {
                Err(e) => Some(Err(e)),
                Ok(p) if pred(&p.item) => {
                    let drained = std::mem::take(&mut skipped);
                    Some(Ok(Positioned::new(
                        FilteredItem {
                            value: p.item,
                            skipped: drained,
                        },
                        p.pos,
                    )))
                }
                Ok(p) => {
                    skipped.push(p.item);
                    None
                }
            };
            async move { action }
        }))
    }
}

fn wrap_hist<Item, P>(
    upstream: &Hist<Item, P>,
    pred: Predicate<Item>,
) -> Hist<FilteredItem<Item>, P>
where
    Item: Clone + Send + Sync + 'static,
    P: Position,
{
    let backend = Arc::new(FilteredHistBackend {
        upstream: upstream.backend().clone(),
        pred,
    });
    Hist::new(backend, upstream.start().clone(), upstream.end().clone())
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combinators::tests_common::{MockEvent, MockPos, MockSource, collect};

    fn even(e: &MockEvent) -> bool {
        e.0.parse::<u64>().is_ok_and(|n| n.is_multiple_of(2))
    }

    fn mk(i: u64) -> MockEvent {
        MockEvent(i.to_string())
    }

    #[tokio::test]
    async fn filter_yields_only_matching_items() {
        let src = MockSource::new(vec![mk(1), mk(2), mk(3), mk(4), mk(5)]);
        let filtered = filter_hist_store(src.hist_store(), even);
        let items = collect(filtered.next().await).await;
        let kept: Vec<&str> = items.iter().map(|p| p.item.value.0.as_str()).collect();
        assert_eq!(kept, vec!["2", "4"]);
    }

    #[tokio::test]
    async fn filter_accumulates_skipped_between_yields() {
        let src = MockSource::new(vec![mk(1), mk(2), mk(3), mk(4), mk(5), mk(6)]);
        let filtered = filter_hist_store(src.hist_store(), even);
        let items = collect(filtered.next().await).await;
        assert_eq!(items.len(), 3);
        // First kept item is "2" — only "1" was skipped before it.
        assert_eq!(items[0].item.value.0, "2");
        assert_eq!(
            items[0]
                .item
                .skipped
                .iter()
                .map(|i| i.0.as_str())
                .collect::<Vec<_>>(),
            vec!["1"]
        );
        // Second kept is "4" — "3" skipped in between.
        assert_eq!(items[1].item.value.0, "4");
        assert_eq!(
            items[1]
                .item
                .skipped
                .iter()
                .map(|i| i.0.as_str())
                .collect::<Vec<_>>(),
            vec!["3"]
        );
        // Third kept is "6" — "5" skipped.
        assert_eq!(items[2].item.value.0, "6");
        assert_eq!(
            items[2]
                .item
                .skipped
                .iter()
                .map(|i| i.0.as_str())
                .collect::<Vec<_>>(),
            vec!["5"]
        );
    }

    #[tokio::test]
    async fn filter_positions_match_upstream() {
        let src = MockSource::new(vec![mk(1), mk(2), mk(3), mk(4)]);
        let filtered = filter_hist_store(src.hist_store(), even);
        let items = collect(filtered.next().await).await;
        assert_eq!(items[0].pos, MockPos(2));
        assert_eq!(items[1].pos, MockPos(4));
    }

    #[tokio::test]
    async fn filter_split_resumes_after_anchor() {
        let src = MockSource::new(vec![mk(1), mk(2), mk(3), mk(4), mk(5), mk(6)]);
        let filtered = filter_hist_store(src.hist_store(), even);
        let hist = filtered.next().await;
        let (_, tail) = hist.split(MockPos(2));
        let items = collect(tail).await;
        // Items kept after pos=2: "4" and "6"
        let kept: Vec<&str> = items.iter().map(|p| p.item.value.0.as_str()).collect();
        assert_eq!(kept, vec!["4", "6"]);
    }

    #[tokio::test]
    async fn filter_no_matches_empty_stream() {
        let src = MockSource::new(vec![mk(1), mk(3), mk(5)]);
        let filtered = filter_hist_store(src.hist_store(), even);
        let items = collect(filtered.next().await).await;
        assert!(items.is_empty());
    }
}
