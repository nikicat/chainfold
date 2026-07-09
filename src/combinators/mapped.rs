//! `map_hist_store` — transform each item without changing positions.
//!
//! Each yield carries both the mapped value and the original upstream item
//! on [`MappedItem`]. Positions pass through unchanged: `map` does not
//! affect window boundaries or anchors, so a `MappedHist` shares the
//! `(start, end]` of its upstream.

use std::sync::Arc;

use futures_util::StreamExt;

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Timestamp,
};
use crate::share::Share;

/// Item emitted by a mapped history. Carries both the transformed value and
/// the original upstream item, so callers that need access to either can
/// reach it without re-fetching.
#[derive(Clone, Debug)]
pub struct MappedItem<Src, Dst> {
    pub value: Dst,
    pub source: Src,
}

/// Type-erased mapping function. Sync only — async mapping would need a
/// separate combinator that pays the boxing cost everywhere.
pub type MapFn<Src, Dst> = Arc<dyn Fn(&Src) -> Dst + Send + Sync>;

/// Transform each item in `upstream` through `f`. Positions are preserved
/// — `map` does not change window boundaries or anchors, so anchors saved
/// upstream remain valid on the mapped store.
pub fn map_hist_store<Src, Dst, P, F>(
    upstream: HistStore<Src, P>,
    f: F,
) -> HistStore<MappedItem<Src, Dst>, P>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    P: Position,
    F: Fn(&Src) -> Dst + Send + Sync + 'static,
{
    let f: MapFn<Src, Dst> = Arc::new(f);
    HistStore::new(Arc::new(MappedHistStoreBackend { upstream, f }))
}

// ============================================================
// MappedHistStoreBackend
// ============================================================

struct MappedHistStoreBackend<Src, Dst, P: Position> {
    upstream: HistStore<Src, P>,
    f: MapFn<Src, Dst>,
}

#[async_trait::async_trait]
impl<Src, Dst, P> HistStoreBackend<MappedItem<Src, Dst>, P> for MappedHistStoreBackend<Src, Dst, P>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    P: Position,
{
    fn snapshots(&self) -> HistStream<MappedItem<Src, Dst>, P> {
        let f = self.f.share();
        Box::pin(
            self.upstream
                .snapshots()
                .map(move |hist| wrap_hist(&hist, f.share())),
        )
    }

    async fn next(&self) -> Hist<MappedItem<Src, Dst>, P> {
        wrap_hist(&self.upstream.next().await, self.f.share())
    }

    async fn time_pos(&self, time: Timestamp) -> Result<P, HistoryError> {
        self.upstream.time_pos(time).await
    }

    fn load(&self, value: serde_json::Value) -> Option<P> {
        self.upstream.load(value)
    }
}

// ============================================================
// MappedHistBackend
// ============================================================

struct MappedHistBackend<Src, Dst, P: Position> {
    upstream: Arc<dyn HistBackend<Src, P>>,
    f: MapFn<Src, Dst>,
}

impl<Src, Dst, P> HistBackend<MappedItem<Src, Dst>, P> for MappedHistBackend<Src, Dst, P>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    P: Position,
{
    fn iter(&self, start: P, end: P) -> ItemStream<MappedItem<Src, Dst>, P> {
        let f = self.f.share();
        Box::pin(self.upstream.iter(start, end).map(move |result| {
            result.map(|p| {
                let mapped = f(&p.item);
                Positioned::new(
                    MappedItem {
                        value: mapped,
                        source: p.item,
                    },
                    p.pos,
                )
            })
        }))
    }
}

fn wrap_hist<Src, Dst, P>(
    upstream: &Hist<Src, P>,
    f: MapFn<Src, Dst>,
) -> Hist<MappedItem<Src, Dst>, P>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    P: Position,
{
    let backend = Arc::new(MappedHistBackend {
        upstream: upstream.backend().share(),
        f,
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

    #[tokio::test]
    async fn map_yields_mapped_item_with_source_and_position() {
        let src = MockSource::new(vec![
            MockEvent("a".into()),
            MockEvent("bb".into()),
            MockEvent("ccc".into()),
        ]);
        let mapped = map_hist_store(src.hist_store(), |e: &MockEvent| e.0.len());
        let hist = mapped.next().await;
        let items = collect(hist).await;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].item.value, 1);
        assert_eq!(items[0].item.source.0, "a");
        assert_eq!(items[0].pos, MockPos(1));
        assert_eq!(items[2].item.value, 3);
        assert_eq!(items[2].pos, MockPos(3));
    }

    #[tokio::test]
    async fn map_split_preserves_window() {
        let src = MockSource::new(vec![
            MockEvent("a".into()),
            MockEvent("b".into()),
            MockEvent("c".into()),
        ]);
        let mapped = map_hist_store(src.hist_store(), |e: &MockEvent| e.0.to_uppercase());
        let hist = mapped.next().await;
        let (_, tail) = hist.split(MockPos(1));
        let items = collect(tail).await;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].item.value, "B");
        assert_eq!(items[1].item.value, "C");
    }

    #[tokio::test]
    async fn map_proxies_tip_from_upstream() {
        let src = MockSource::new(vec![MockEvent("a".into())]);
        let mapped = map_hist_store(src.hist_store(), |e: &MockEvent| e.0.clone());
        // The mapped store's tip (the current snapshot's `end`) proxies from
        // upstream — finality accessors are gone, so read it via `next().end()`.
        assert_eq!(*mapped.next().await.end(), MockPos(1));
        src.append(MockEvent("b".into()));
        // `next()` re-reads the upstream tip, which `append` bumped synchronously.
        assert_eq!(*mapped.next().await.end(), MockPos(2));
    }
}
