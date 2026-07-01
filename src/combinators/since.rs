//! `since_hist_store` — pre-split every emitted `Hist` at a fixed cut.
//!
//! This sits *above* a [`HistStore`] and rewrites each yielded
//! [`Hist`](crate::arch::Hist) to start at `cut`. Crucially, the cut is
//! applied via [`Hist::split`](crate::arch::Hist::split) — at the snapshot
//! level — so any combinator wrapped between `since_hist_store` and the
//! source only iterates post-cut items.
//!
//! Compare with `filter_hist_store(hs, |i| i.pos > cut)`: that variant
//! iterates the entire upstream window and rejects items in the iterator
//! body, so an expensive `map_hist_store` between the filter and the
//! source still runs for every pre-cut item. `since_hist_store` skips
//! them entirely.
//!
//! ```text
//! since(map(src, expensive_f), cut)
//!     |
//!     +--> emits Hist with start=cut
//!     +--> .iter() calls map_backend.iter(cut, end)
//!                  └─> calls src.iter(cut, end) — only post-cut items
//!                  └─> expensive_f applied per item
//! ```

use std::sync::Arc;

use futures_util::StreamExt;

use crate::arch::{
    Hist, HistStore, HistStoreBackend, HistStream, HistoryError, Position, Store, Timestamp,
};

/// Pre-split every snapshot emitted by `upstream` at `cut`. Items strictly
/// after `cut` are visible; items at-or-before `cut` are not.
///
/// Use this when an expensive combinator sits between `since` and the
/// source — it saves the wrapped combinator from processing items the
/// caller will never see.
pub fn since_hist_store<Item, P>(upstream: HistStore<Item, P>, cut: P) -> HistStore<Item, P>
where
    Item: Send + Sync + 'static,
    P: Position,
{
    HistStore::new(Arc::new(SinceHistStoreBackend { upstream, cut }))
}

// ============================================================
// SinceHistStoreBackend
// ============================================================

struct SinceHistStoreBackend<Item, P: Position> {
    upstream: HistStore<Item, P>,
    cut: P,
}

#[async_trait::async_trait]
impl<Item, P> HistStoreBackend<Item, P> for SinceHistStoreBackend<Item, P>
where
    Item: Send + Sync + 'static,
    P: Position,
{
    fn snapshots(&self) -> HistStream<Item, P> {
        let cut = self.cut.clone();
        Box::pin(
            self.upstream
                .snapshots()
                .map(move |hist| hist.split(cut.clone()).1),
        )
    }

    async fn next(&self) -> Hist<Item, P> {
        self.upstream.next().await.split(self.cut.clone()).1
    }

    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        self.upstream.time_pos(time).await
    }

    fn latest(&self) -> &Store<P> {
        self.upstream.latest()
    }
    fn best(&self) -> &Store<P> {
        self.upstream.best()
    }
    fn safe(&self) -> &Store<P> {
        self.upstream.safe()
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combinators::mapped::map_hist_store;
    use crate::combinators::tests_common::{MockEvent, MockPos, MockSource, collect};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn since_pre_splits_emitted_hist() {
        let src = MockSource::new(vec![
            MockEvent("a".into()),
            MockEvent("b".into()),
            MockEvent("c".into()),
            MockEvent("d".into()),
        ]);
        let cut = MockPos(2);
        let since = since_hist_store(src.hist_store(), cut);
        let hist = since.next().await;
        assert_eq!(*hist.start(), MockPos(2));
        assert_eq!(*hist.end(), MockPos(4));
        let items = collect(hist).await;
        let kept: Vec<&str> = items.iter().map(|p| p.item.0.as_str()).collect();
        assert_eq!(kept, vec!["c", "d"]);
    }

    #[tokio::test]
    async fn since_skips_mapper_on_pre_cut_items() {
        // Wrap an expensive mapper between since and the source; assert
        // the mapper counter only increments for post-cut items.
        let src = MockSource::new(vec![
            MockEvent("a".into()),
            MockEvent("b".into()),
            MockEvent("c".into()),
            MockEvent("d".into()),
        ]);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let mapped = map_hist_store(src.hist_store(), move |e: &MockEvent| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            e.0.to_uppercase()
        });
        let cut = MockPos(2);
        let since = since_hist_store(mapped, cut);

        let items = collect(since.next().await).await;
        let kept: Vec<&str> = items.iter().map(|p| p.item.value.as_str()).collect();
        assert_eq!(kept, vec!["C", "D"]);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn since_cut_below_start_no_op() {
        let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
        let since = since_hist_store(src.hist_store(), MockPos(0));
        let items = collect(since.next().await).await;
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn since_cut_above_end_empty() {
        let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
        let since = since_hist_store(src.hist_store(), MockPos(99));
        let hist = since.next().await;
        assert!(hist.is_empty());
    }
}
