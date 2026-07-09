//! `map_positions` — lift a store into a wider position type without
//! changing items.
//!
//! The companion of [`map_hist_store`](super::map_hist_store) (which maps
//! items and preserves positions; this maps positions and preserves items).
//! Its job is the app-enum pattern feeding
//! [`interleave_hist_stores`](crate::merge::interleave_hist_stores): sources
//! from different families (EVM, Tron, …) have different [`Position`] types,
//! so an app defines a closed enum over the compiled-in families, lifts each
//! source with `map_positions`, and interleaves the now
//! position-homogeneous stores. Chain *sets* stay runtime-dynamic (source
//! keys); chain *families* are compile-time-bounded, so the enum costs no
//! real flexibility.
//!
//! The [`PosMapper`] owns the `P ↔ Q` embedding *and* `Q`'s deserialization:
//! the upstream store can only parse its own `P` marshal, not the enum's
//! tagged form, so `load` must ride the mapper — honoring the
//! "deserialization lives on the store" split.

use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::iter;

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Timestamp,
};
use crate::share::Share;

/// An embedding of position type `P` into a wider position type `Q`
/// (typically one variant of an app-defined enum).
///
/// Requirements: `wrap` is injective, order-preserving over the positions a
/// single source produces, and `unwrap(wrap(p)) == Some(p)`. `unwrap`
/// returns `None` for a `Q` outside the embedding (a foreign enum variant).
pub trait PosMapper<P: Position, Q: Position>: Send + Sync + 'static {
    /// Lift an upstream position into `Q`.
    fn wrap(&self, p: P) -> Q;
    /// Recover the upstream position; `None` if `q` is a foreign variant.
    fn unwrap(&self, q: Q) -> Option<P>;
    /// Deserialize a [`Position::marshal`]led `Q` — the enum's own decode,
    /// which the upstream store cannot perform.
    fn load(&self, value: serde_json::Value) -> Option<Q>;
}

/// Lift `upstream` into position type `Q` through `mapper`. Items are
/// untouched; every position (window bounds, yielded anchors, time anchors)
/// is wrapped, and `load` delegates to the mapper.
pub fn map_positions<T, P, Q>(
    upstream: HistStore<T, P>,
    mapper: impl PosMapper<P, Q>,
) -> HistStore<T, Q>
where
    T: Send + Sync + 'static,
    P: Position,
    Q: Position,
{
    HistStore::new(Arc::new(PosMappedHistStoreBackend {
        upstream,
        mapper: Arc::new(mapper),
    }))
}

// ============================================================
// PosMappedHistStoreBackend
// ============================================================

struct PosMappedHistStoreBackend<T, P: Position, Q: Position> {
    upstream: HistStore<T, P>,
    mapper: Arc<dyn PosMapper<P, Q>>,
}

#[async_trait::async_trait]
impl<T, P, Q> HistStoreBackend<T, Q> for PosMappedHistStoreBackend<T, P, Q>
where
    T: Send + Sync + 'static,
    P: Position,
    Q: Position,
{
    fn snapshots(&self) -> HistStream<T, Q> {
        let mapper = self.mapper.share();
        Box::pin(
            self.upstream
                .snapshots()
                .map(move |hist| wrap_hist(&hist, mapper.share())),
        )
    }

    async fn next(&self) -> Hist<T, Q> {
        wrap_hist(&self.upstream.next().await, self.mapper.share())
    }

    async fn time_pos(&self, time: Timestamp) -> Result<Q, HistoryError> {
        Ok(self.mapper.wrap(self.upstream.time_pos(time).await?))
    }

    fn load(&self, value: serde_json::Value) -> Option<Q> {
        self.mapper.load(value)
    }
}

// ============================================================
// PosMappedHistBackend
// ============================================================

struct PosMappedHistBackend<T, P: Position, Q: Position> {
    upstream: Arc<dyn HistBackend<T, P>>,
    mapper: Arc<dyn PosMapper<P, Q>>,
}

impl<T, P, Q> HistBackend<T, Q> for PosMappedHistBackend<T, P, Q>
where
    T: Send + Sync + 'static,
    P: Position,
    Q: Position,
{
    fn iter(&self, start: Q, end: Q) -> ItemStream<T, Q> {
        let (Some(start_p), Some(end_p)) = (self.mapper.unwrap(start), self.mapper.unwrap(end))
        else {
            // Bounds always come from wrapped hists or from `clamp_pos`
            // below, so a foreign variant here is a caller-constructed
            // window — surface it rather than yielding a wrong window.
            return Box::pin(iter([Err(HistoryError::internal(
                "map_positions: window bound is a foreign position variant",
            ))]));
        };
        let mapper = self.mapper.share();
        Box::pin(
            self.upstream
                .iter(start_p, end_p)
                .map(move |result| result.map(|p| Positioned::new(p.item, mapper.wrap(p.pos)))),
        )
    }

    fn clamp_pos(&self, p: Q, start: &Q, end: &Q) -> Q {
        if let (Some(p_in), Some(start_in), Some(end_in)) = (
            self.mapper.unwrap(p.clone()),
            self.mapper.unwrap(start.clone()),
            self.mapper.unwrap(end.clone()),
        ) {
            return self
                .mapper
                .wrap(self.upstream.clamp_pos(p_in, &start_in, &end_in));
        }
        // A foreign-variant position sorts as a block against this window's
        // variant, so plain `Ord` clamping lands it on the correct boundary
        // (a foreign `p` is out-of-window by construction).
        if &p < start {
            start.clone()
        } else if &p > end {
            end.clone()
        } else {
            p
        }
    }
}

fn wrap_hist<T, P, Q>(upstream: &Hist<T, P>, mapper: Arc<dyn PosMapper<P, Q>>) -> Hist<T, Q>
where
    T: Send + Sync + 'static,
    P: Position,
    Q: Position,
{
    let start = mapper.wrap(upstream.start().clone());
    let end = mapper.wrap(upstream.end().clone());
    let backend = Arc::new(PosMappedHistBackend {
        upstream: upstream.backend().share(),
        mapper,
    });
    Hist::new(backend, start, end)
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::combinators::tests_common::{MockEvent, MockPos, MockSource, collect};

    /// Two-family app enum: this suite's sources live in the `Num` variant;
    /// `Other` plays the foreign family.
    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    enum TestPos {
        Num(MockPos),
        Other(u64),
    }

    impl Default for TestPos {
        fn default() -> Self {
            Self::Num(MockPos::default())
        }
    }

    impl Position for TestPos {
        fn marshal(&self) -> serde_json::Value {
            match self {
                Self::Num(p) => serde_json::json!({ "num": p.marshal() }),
                Self::Other(n) => serde_json::json!({ "other": n }),
            }
        }
    }

    struct NumMapper;

    impl PosMapper<MockPos, TestPos> for NumMapper {
        fn wrap(&self, p: MockPos) -> TestPos {
            TestPos::Num(p)
        }
        fn unwrap(&self, q: TestPos) -> Option<MockPos> {
            match q {
                TestPos::Num(p) => Some(p),
                TestPos::Other(_) => None,
            }
        }
        fn load(&self, value: serde_json::Value) -> Option<TestPos> {
            let obj = value.as_object()?;
            if let Some(v) = obj.get("num") {
                return Some(TestPos::Num(MockPos(v.as_u64()?)));
            }
            Some(TestPos::Other(obj.get("other")?.as_u64()?))
        }
    }

    #[tokio::test]
    async fn positions_wrap_and_items_pass_through() {
        let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
        let mapped = map_positions(src.hist_store(), NumMapper);
        let hist = mapped.next().await;
        assert_eq!(hist.end(), &TestPos::Num(MockPos(2)));
        let items = collect(hist).await;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].item.0, "a");
        assert_eq!(items[0].pos, TestPos::Num(MockPos(1)));
    }

    #[tokio::test]
    async fn split_at_wrapped_position_windows_upstream() {
        let src = MockSource::new(vec![
            MockEvent("a".into()),
            MockEvent("b".into()),
            MockEvent("c".into()),
        ]);
        let mapped = map_positions(src.hist_store(), NumMapper);
        let hist = mapped.next().await;
        let (head, tail) = hist.split(TestPos::Num(MockPos(1)));
        assert_eq!(collect(head).await.len(), 1);
        let tail_items = collect(tail).await;
        assert_eq!(tail_items.len(), 2);
        assert_eq!(tail_items[0].item.0, "b");
    }

    #[tokio::test]
    async fn foreign_variant_clamps_to_boundary() {
        let src = MockSource::new(vec![MockEvent("a".into()), MockEvent("b".into())]);
        let mapped = map_positions(src.hist_store(), NumMapper);
        let hist = mapped.next().await;
        // `Other` sorts after every `Num`, so it clamps to the window end:
        // the head is the whole window, the tail empty.
        let (head, tail) = hist.split(TestPos::Other(0));
        assert_eq!(collect(head).await.len(), 2);
        assert!(tail.is_empty());
    }

    #[tokio::test]
    async fn load_uses_the_enum_decode() {
        let src = MockSource::new(vec![MockEvent("a".into())]);
        let mapped = map_positions(src.hist_store(), NumMapper);
        let pos = TestPos::Num(MockPos(1));
        assert_eq!(mapped.load(pos.marshal()), Some(pos));
        let foreign = TestPos::Other(7);
        assert_eq!(mapped.load(foreign.marshal()), Some(foreign));
    }

    #[tokio::test]
    async fn time_pos_wraps() {
        let src = MockSource::new(vec![MockEvent("a".into())]);
        let mapped = map_positions(src.hist_store(), NumMapper);
        let t = mapped.time_pos(Timestamp::new(0)).await.unwrap();
        assert_eq!(t, TestPos::Num(MockPos(0)));
    }
}
