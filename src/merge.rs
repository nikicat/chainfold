//! Heterogeneous-source merge — combine multiple [`HistStore`]s yielding a
//! common item type `T` into a single [`HistStore<MergedItem<T>, MergedPos>`].
//!
//! Each source can have its own [`Position`] type; positions live as real
//! objects inside [`MergedPos`] (type-erased via [`ErasedAnchor`]) and are
//! serialized only when an anchor is persisted. Items are ordered across
//! sources by a user-supplied [`MergedCmp`].
//!
//! ## Resume semantics
//!
//! Each yielded [`MergedItem`] carries a [`MergedPos`] that is a *vector
//! clock* of per-source positions at the moment of yield: the source that
//! just emitted advances to the new item's position; other sources stay at
//! whatever they last yielded (or each source's [`Default`] position if
//! they haven't yielded anything in this window yet). Saving the last
//! yielded `MergedPos` and resuming via `hist.split(pos).1` produces the
//! exact continuation — items from sources that *had* advanced past `pos`
//! at the yield time are skipped; items from other sources are not.
//!
//! ## Position trait gotcha
//!
//! [`Position`]'s required [`Ord`] on [`MergedPos`] is lexicographic over the
//! sorted union of source keys, comparing each per-source position with its
//! own concrete `Ord` (via [`ErasedAnchor::cmp_erased`]; a key present on one
//! side only orders after absent). This is a total order suitable for
//! [`Position`] but *does not* match component-wise containment (clock A can
//! sort before clock B while being ahead of B on some other source).
//! [`MergedHistBackend`] therefore overrides [`HistBackend::clamp_pos`] to
//! clamp each per-source position separately, which is the correct semantic
//! for [`Hist::split`].
//!
//! ## Missing keys
//!
//! A [`MergedPos`] saved from an older version of the merge that omits a
//! source key (or had a different set of sources) is still usable: the
//! missing source contributes its [`Default`] position, equivalent to
//! "start from the beginning for that source." This is the
//! `merged_pos_handles_added_source` semantic in `tests/phase5_merge.rs`.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all, unfold};

use crate::arch::{
    Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError, ItemStream, Position,
    Positioned, Subscription, Timestamp,
};

// ============================================================
// SourceKey
// ============================================================

/// Identifies one input source within a [`merge_hist_stores`] product.
///
/// A newtype over the source-name `String` so a source key can't be confused
/// with the other strings flowing through the merge. It is the key type of
/// [`MergedPos::positions`] and the `source_key` of [`MergedItem`].
///
/// `#[serde(transparent)]` so it serializes as a bare string — a [`MergedPos`]
/// marshalled before this newtype existed is byte-identical and still
/// round-trips, keeping saved resume anchors valid. `Borrow<str>` lets the
/// keyed [`BTreeMap`] be probed with a plain `&str`.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct SourceKey(pub String);

impl SourceKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SourceKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SourceKey {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl std::borrow::Borrow<str> for SourceKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ============================================================
// MergedPos
// ============================================================

// TODO(merge-erasure): revisit whether the `dyn ErasedAnchor` / `dyn
// ErasedSource` type erasure can be avoided altogether.
//
// Erasure exists only because `merge_hist_stores` combines sources with
// *different* `Position`/item types into one `HistStore<MergedItem<T>,
// MergedPos>`. That forces: (a) `Arc<dyn ErasedAnchor>` in `MergedPos`, whose
// only real cost is the `cmp_erased` runtime-downcast (`Ord`/`Eq` aren't
// object-safe); and (b) `Arc<dyn ErasedSource<T>>` per source.
//
// Directions to evaluate (each trades ergonomics/scope against the erasure):
//   1. Typed tuple/HList merge for the common *homogeneous-P* case (all
//      sources share one `P` — e.g. OwnedErc20's two EVM filters). A monomorphic
//      `merge<P>(...)` would need no erasure at all; keep the heterogeneous
//      path as a separate, rarely-used API. Likely the highest-value option.
//   2. A `const N`/generics-over-tuple design so the source set is statically
//      known — removes the `BTreeMap<_, dyn>` but loses runtime source add/remove.
//   3. Keep erasure but shrink its surface: this file already collapsed to a
//      single `cmp_erased` seam; `as_any` downcasts are the remainder.
// The reference (`history.ts`) sidesteps this with TS structural typing, which
// has no Rust equivalent — so some erasure or monomorphization choice is
// unavoidable here. Decide before merge grows more consumers.

// ============================================================
// ErasedAnchor — a real position object, type-erased
// ============================================================

/// A per-source [`Position`] object with its concrete type erased, so a
/// heterogeneous [`MergedPos`] can hold real objects (not eagerly-marshalled
/// bytes). Serialization happens only at persistence ([`marshal`](Self::marshal));
/// deserialization is source-mediated via [`HistStore::load`], never on this
/// value — mirroring the reference `history.ts`'s object-anchor model.
pub trait ErasedAnchor: std::fmt::Debug + Send + Sync {
    /// Serialize the wrapped position to a structured value for persistence.
    fn marshal(&self) -> serde_json::Value;
    /// The wrapped position, for downcasting back to its concrete `P`.
    fn as_any(&self) -> &dyn std::any::Any;
    /// Compare against another anchor using the wrapped `P`'s own [`Ord`].
    ///
    /// The single "compare erased positions" seam. `Ord`/`Eq` aren't
    /// object-safe (`Self` in argument position), so there's no way to
    /// compare two `dyn ErasedAnchor` without recovering the concrete `P` at
    /// runtime. Anchors of a different concrete type compare
    /// [`Equal`](Ordering::Equal); that never happens within one source key.
    fn cmp_erased(&self, other: &dyn ErasedAnchor) -> Ordering;
}

#[derive(Debug)]
struct TypedAnchor<P: Position>(P);

impl<P: Position> ErasedAnchor for TypedAnchor<P> {
    fn marshal(&self) -> serde_json::Value {
        self.0.marshal()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        &self.0
    }
    fn cmp_erased(&self, other: &dyn ErasedAnchor) -> Ordering {
        other
            .as_any()
            .downcast_ref::<P>()
            .map_or(Ordering::Equal, |o| self.0.cmp(o))
    }
}

/// Box a concrete position into an [`ErasedAnchor`].
fn anchor<P: Position>(p: P) -> Arc<dyn ErasedAnchor> {
    Arc::new(TypedAnchor(p))
}

/// Keyed position into a [`merge_hist_stores`] product. Maps source key → the
/// source's live position **object** (via [`ErasedAnchor`]).
///
/// Positions are kept as real objects; bytes appear only when
/// [`marshal`](Position::marshal)ing for persistence, or transiently as the
/// input to [`HistStore::load`] on resume. Missing keys are treated as that
/// source's [`Default`].
#[derive(Clone, Debug, Default)]
pub struct MergedPos {
    positions: BTreeMap<SourceKey, Arc<dyn ErasedAnchor>>,
}

impl MergedPos {
    pub(crate) fn new(positions: BTreeMap<SourceKey, Arc<dyn ErasedAnchor>>) -> Self {
        Self { positions }
    }

    /// The erased position object for `key`, or `None` if absent.
    pub fn get(&self, key: &str) -> Option<&Arc<dyn ErasedAnchor>> {
        self.positions.get(key)
    }

    /// The position for `key` downcast to a concrete `P`, or `None` if absent
    /// or of a different type.
    pub fn get_as<P: Position>(&self, key: &str) -> Option<P> {
        self.positions
            .get(key)?
            .as_any()
            .downcast_ref::<P>()
            .cloned()
    }
}

impl PartialEq for MergedPos {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for MergedPos {}
impl PartialOrd for MergedPos {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MergedPos {
    /// Lexicographic over the union of source keys (both are sorted
    /// `BTreeMap`s), each component compared via its own [`Ord`]
    /// ([`ErasedAnchor::cmp_erased`]) — no serialization. Off the hot path:
    /// `merge` overrides `clamp_pos` with the same component-wise semantics,
    /// so this only backs the `Position: Ord`/`Eq` bounds and `Hist`'s
    /// `start <= end` / `is_empty` checks. A present component sorts after an
    /// absent one (a missing key ≡ that source's earliest).
    fn cmp(&self, other: &Self) -> Ordering {
        let keys: std::collections::BTreeSet<&SourceKey> = self
            .positions
            .keys()
            .chain(other.positions.keys())
            .collect();
        for k in keys {
            let ord = match (self.positions.get(k), other.positions.get(k)) {
                (Some(a), Some(b)) => a.cmp_erased(b.as_ref()),
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

impl Position for MergedPos {
    /// A JSON object mapping each source key to that source's serialized
    /// position — pure composition, no framing.
    fn marshal(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.positions
                .iter()
                .map(|(k, a)| (k.0.clone(), a.marshal()))
                .collect(),
        )
    }
}

// ============================================================
// MergedItem<T>
// ============================================================

/// Item emitted by a merged history. `source_key` identifies which input
/// source produced it; `source_pos` is that source's live position object.
#[derive(Clone, Debug)]
pub struct MergedItem<T> {
    pub value: T,
    pub source_key: SourceKey,
    pub source_pos: Arc<dyn ErasedAnchor>,
}

/// Ordering function passed to [`merge_hist_stores`]. Items where this
/// returns [`Ordering::Less`] are yielded first.
pub type MergedCmp<T> = Arc<dyn Fn(&MergedItem<T>, &MergedItem<T>) -> Ordering + Send + Sync>;

// ============================================================
// ErasedHistStore<T> + ErasedSource<T>
// ============================================================

/// Type-erased [`HistStore`] wrapping a concrete [`HistStore<T, P>`] for
/// some [`Position`] type known only to this value. Constructed via
/// [`ErasedHistStore::new`]; consumed by [`merge_hist_stores`].
pub struct ErasedHistStore<T> {
    inner: Arc<dyn ErasedSource<T>>,
}

impl<T: Send + Sync + Clone + 'static> ErasedHistStore<T> {
    /// Erase the per-source [`Position`] type. The resulting handle can be
    /// mixed with other erased sources having different `P` in
    /// [`merge_hist_stores`].
    pub fn new<P: Position>(store: HistStore<T, P>) -> Self {
        Self {
            inner: Arc::new(ErasedSourceImpl::<T, P> {
                store,
                _p: std::marker::PhantomData,
            }),
        }
    }
}

/// One item (or error) from an erased source's iterator: the value plus the
/// source position it occurred at, as an anchor object.
type ErasedItem<T> = Result<(T, Arc<dyn ErasedAnchor>), HistoryError>;

#[async_trait::async_trait]
trait ErasedSource<T: Send + Sync + 'static>: Send + Sync {
    /// The source's [`Position::default`] as an erased anchor object.
    fn earliest(&self) -> Arc<dyn ErasedAnchor>;
    /// Iterate items in `(start, end]` for this source. Anchors are this
    /// source's position objects.
    fn iter(
        &self,
        start: Arc<dyn ErasedAnchor>,
        end: Arc<dyn ErasedAnchor>,
    ) -> Subscription<ErasedItem<T>>;
    /// The source's current snapshot `end` (its tip) as an erased anchor.
    async fn current_end(&self) -> Arc<dyn ErasedAnchor>;
    /// Stream of `end` anchors, one per source tip advance (first item is the
    /// current tip). Drives the merged `snapshots()`.
    fn end_stream(&self) -> Subscription<Arc<dyn ErasedAnchor>>;
    /// Translate a wall-clock time into an erased position anchor.
    async fn time_pos(&self, time: Timestamp) -> Result<Arc<dyn ErasedAnchor>, HistoryError>;
    /// Deserialize a marshalled position via the source's store `load`.
    fn load(&self, value: serde_json::Value) -> Option<Arc<dyn ErasedAnchor>>;
}

struct ErasedSourceImpl<T, P: Position> {
    store: HistStore<T, P>,
    _p: std::marker::PhantomData<P>,
}

/// Downcast an erased anchor to `P`, falling back to `P::default` (a
/// well-typed anchor of the wrong `P` is a bug; missing/other → earliest).
fn downcast_or_default<P: Position>(a: &dyn ErasedAnchor) -> P {
    a.as_any().downcast_ref::<P>().cloned().unwrap_or_default()
}

#[async_trait::async_trait]
impl<T, P> ErasedSource<T> for ErasedSourceImpl<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn earliest(&self) -> Arc<dyn ErasedAnchor> {
        anchor(P::default())
    }

    fn iter(
        &self,
        start: Arc<dyn ErasedAnchor>,
        end: Arc<dyn ErasedAnchor>,
    ) -> Subscription<ErasedItem<T>> {
        let store = self.store.clone();
        Box::pin(unfold(
            ErasedIterState::<T, P>::Init { start, end, store },
            erased_iter_step::<T, P>,
        ))
    }

    async fn current_end(&self) -> Arc<dyn ErasedAnchor> {
        anchor(self.store.next().await.end().clone())
    }

    fn end_stream(&self) -> Subscription<Arc<dyn ErasedAnchor>> {
        Box::pin(self.store.snapshots().map(|h| anchor(h.end().clone())))
    }

    async fn time_pos(&self, time: Timestamp) -> Result<Arc<dyn ErasedAnchor>, HistoryError> {
        Ok(anchor(self.store.time_pos(time).await?))
    }

    fn load(&self, value: serde_json::Value) -> Option<Arc<dyn ErasedAnchor>> {
        self.store.load(value).map(anchor)
    }
}

enum ErasedIterState<T, P: Position> {
    Init {
        start: Arc<dyn ErasedAnchor>,
        end: Arc<dyn ErasedAnchor>,
        store: HistStore<T, P>,
    },
    Streaming(ItemStream<T, P>),
    Done,
}

async fn erased_iter_step<T, P>(
    state: ErasedIterState<T, P>,
) -> Option<(
    Result<(T, Arc<dyn ErasedAnchor>), HistoryError>,
    ErasedIterState<T, P>,
)>
where
    T: Send + Sync + 'static,
    P: Position,
{
    match state {
        ErasedIterState::Init { start, end, store } => {
            let start_p = downcast_or_default::<P>(start.as_ref());
            let end_p = downcast_or_default::<P>(end.as_ref());
            let hist = store.next().await;
            // Take the window via two splits — uses per-source Ord, which
            // is the canonical clamping for each source's own P.
            let (_, tail) = hist.split(start_p);
            let (window, _) = tail.split(end_p);
            let mut stream = window.iter();
            match stream.next().await {
                Some(Ok(p)) => Some((
                    Ok((p.item, anchor(p.pos))),
                    ErasedIterState::Streaming(stream),
                )),
                Some(Err(e)) => Some((Err(e), ErasedIterState::Done)),
                None => None,
            }
        }
        ErasedIterState::Streaming(mut stream) => match stream.next().await {
            Some(Ok(p)) => Some((
                Ok((p.item, anchor(p.pos))),
                ErasedIterState::Streaming(stream),
            )),
            Some(Err(e)) => Some((Err(e), ErasedIterState::Done)),
            None => None,
        },
        ErasedIterState::Done => None,
    }
}

// ============================================================
// MergedHistStoreBackend
// ============================================================

/// Merge `sources` into a single [`HistStore`] over [`MergedItem<T>`].
///
/// `cmp` orders items across sources — return [`Ordering::Less`] when the
/// first argument should be yielded before the second. Ties resolve in
/// sorted-key order (stable). Each yielded item carries a vector-clock
/// [`MergedPos`] that survives a marshal/`load` round-trip and is a valid
/// [`Hist::split`] anchor for resume.
pub fn merge_hist_stores<T, F>(
    sources: BTreeMap<SourceKey, ErasedHistStore<T>>,
    cmp: F,
) -> HistStore<MergedItem<T>, MergedPos>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(&MergedItem<T>, &MergedItem<T>) -> Ordering + Send + Sync + 'static,
{
    HistStore::new(Arc::new(MergedHistStoreBackend::new(
        sources,
        Arc::new(cmp),
    )))
}

struct MergedHistStoreBackend<T> {
    sources: BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
    cmp: MergedCmp<T>,
}

impl<T: Clone + Send + Sync + 'static> MergedHistStoreBackend<T> {
    fn new(stores: BTreeMap<SourceKey, ErasedHistStore<T>>, cmp: MergedCmp<T>) -> Self {
        let sources: BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>> =
            stores.into_iter().map(|(k, s)| (k, s.inner)).collect();
        Self { sources, cmp }
    }
}

/// Build a merged [`Hist`] spanning `(earliest, end]` where `earliest` is
/// each source's [`Position::default`] and `end` is the supplied vector clock.
fn build_merged_hist<T: Clone + Send + Sync + 'static>(
    sources: &BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
    cmp: &MergedCmp<T>,
    end: MergedPos,
) -> Hist<MergedItem<T>, MergedPos> {
    let earliest: Clock = sources
        .iter()
        .map(|(k, s)| (k.clone(), s.earliest()))
        .collect();
    let backend: Arc<dyn HistBackend<MergedItem<T>, MergedPos>> = Arc::new(MergedHistBackend {
        sources: sources.clone(),
        cmp: cmp.clone(),
    });
    Hist::new(backend, MergedPos::new(earliest), end)
}

/// One source's tip advance: which source, and its new `end` anchor object.
type TaggedEnd = (SourceKey, Arc<dyn ErasedAnchor>);
/// The running vector clock: one live position object per source.
type Clock = BTreeMap<SourceKey, Arc<dyn ErasedAnchor>>;

/// State for the merged `snapshots()` stream: seed one snapshot from each
/// child, then re-emit whenever any child advances.
enum MergeSnapState<T> {
    /// Await the first `end` from each child to build the initial vector clock.
    Seed {
        sources: BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
        cmp: MergedCmp<T>,
        tagged: Vec<Subscription<TaggedEnd>>,
    },
    /// Steady state: `select_all` over the tagged child end-streams.
    Run {
        sources: BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
        cmp: MergedCmp<T>,
        select: SelectAll<Subscription<TaggedEnd>>,
        clock: Clock,
    },
}

#[allow(clippy::type_complexity)]
async fn merge_snap_step<T: Clone + Send + Sync + 'static>(
    state: MergeSnapState<T>,
) -> Option<(Hist<MergedItem<T>, MergedPos>, MergeSnapState<T>)> {
    match state {
        MergeSnapState::Seed {
            sources,
            cmp,
            mut tagged,
        } => {
            // Seed the full vector clock BEFORE the first emit. Each child
            // stream's first item is its current tip; `watch`/snapshot streams
            // coalesce, so an advance between subscribe and this poll arrives
            // as the seed rather than being lost. Missing this seeding step is
            // the footgun pinned by `snapshots_advance_when_any_source_appends`.
            let mut clock: Clock = BTreeMap::new();
            for s in &mut tagged {
                if let Some((k, a)) = s.next().await {
                    clock.insert(k, a);
                }
            }
            // Any child whose stream ended before yielding contributes its
            // earliest position, so every source key is present.
            for (k, src) in &sources {
                clock.entry(k.clone()).or_insert_with(|| src.earliest());
            }
            let hist = build_merged_hist(&sources, &cmp, MergedPos::new(clock.clone()));
            let select = select_all(tagged);
            Some((
                hist,
                MergeSnapState::Run {
                    sources,
                    cmp,
                    select,
                    clock,
                },
            ))
        }
        MergeSnapState::Run {
            sources,
            cmp,
            mut select,
            mut clock,
        } => {
            let (key, a) = select.next().await?;
            clock.insert(key, a);
            let hist = build_merged_hist(&sources, &cmp, MergedPos::new(clock.clone()));
            Some((
                hist,
                MergeSnapState::Run {
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
impl<T> HistStoreBackend<MergedItem<T>, MergedPos> for MergedHistStoreBackend<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn snapshots(&self) -> HistStream<MergedItem<T>, MergedPos> {
        let sources = self.sources.clone();
        let cmp = self.cmp.clone();
        // Tag each child's end-stream with its source key so the k-way select
        // knows which clock entry to bump.
        let tagged: Vec<Subscription<TaggedEnd>> = sources
            .iter()
            .map(|(k, s)| {
                let k = k.clone();
                Box::pin(s.end_stream().map(move |a| (k.clone(), a))) as Subscription<TaggedEnd>
            })
            .collect();
        Box::pin(unfold(
            MergeSnapState::Seed {
                sources,
                cmp,
                tagged,
            },
            merge_snap_step,
        ))
    }

    async fn next(&self) -> Hist<MergedItem<T>, MergedPos> {
        // Current tip = each source's current snapshot end.
        let mut clock: Clock = BTreeMap::new();
        for (k, s) in &self.sources {
            clock.insert(k.clone(), s.current_end().await);
        }
        build_merged_hist(&self.sources, &self.cmp, MergedPos::new(clock))
    }

    async fn time_pos(&self, time: Timestamp) -> Result<MergedPos, HistoryError> {
        let mut positions: Clock = BTreeMap::new();
        for (k, s) in &self.sources {
            positions.insert(k.clone(), s.time_pos(time).await?);
        }
        Ok(MergedPos::new(positions))
    }

    fn load(&self, value: serde_json::Value) -> Option<MergedPos> {
        // The persisted form is a JSON object keyed by source; each source
        // reconstructs its own typed anchor via its store `load` (the
        // deserialize-on-the-store asymmetry). A missing key → that source's
        // earliest, so a saved anchor from a smaller source set still resumes.
        let obj = value.as_object()?;
        let mut positions: Clock = BTreeMap::new();
        for (key, src) in &self.sources {
            let a = match obj.get(key.as_str()) {
                Some(v) => src.load(v.clone())?,
                None => src.earliest(),
            };
            positions.insert(key.clone(), a);
        }
        Some(MergedPos::new(positions))
    }
}

// ============================================================
// MergedHistBackend (iter + clamp_pos)
// ============================================================

struct MergedHistBackend<T> {
    sources: BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
    cmp: MergedCmp<T>,
}

impl<T: Clone + Send + Sync + 'static> HistBackend<MergedItem<T>, MergedPos>
    for MergedHistBackend<T>
{
    fn iter(&self, start: MergedPos, end: MergedPos) -> ItemStream<MergedItem<T>, MergedPos> {
        Box::pin(unfold(
            MergeState::init(&self.sources, self.cmp.clone(), &start, &end),
            merge_step,
        ))
    }

    /// Component-wise clamping. For each source key, clamp its position object
    /// to `[start[k], end[k]]` using that source's own `Ord` (via
    /// `cmp_anchors`). Missing keys substitute the source's [`Default`] (so a
    /// saved [`MergedPos`] from a smaller source set still resumes).
    fn clamp_pos(&self, p: MergedPos, start: &MergedPos, end: &MergedPos) -> MergedPos {
        let mut clamped: Clock = BTreeMap::new();
        for (key, source) in &self.sources {
            let earliest = source.earliest();
            let start_a = start
                .positions
                .get(key)
                .cloned()
                .unwrap_or_else(|| earliest.clone());
            let end_a = end
                .positions
                .get(key)
                .cloned()
                .unwrap_or_else(|| earliest.clone());
            let p_a = p.positions.get(key).cloned().unwrap_or(earliest);
            let chosen = if p_a.cmp_erased(start_a.as_ref()) == Ordering::Less {
                start_a
            } else if p_a.cmp_erased(end_a.as_ref()) == Ordering::Greater {
                end_a
            } else {
                p_a
            };
            clamped.insert(key.clone(), chosen);
        }
        MergedPos::new(clamped)
    }
}

// ============================================================
// K-way merge
// ============================================================

struct MergeStream<T> {
    key: SourceKey,
    stream: Subscription<ErasedItem<T>>,
    head: Option<ErasedItem<T>>,
    exhausted: bool,
    end_pos: Arc<dyn ErasedAnchor>,
}

struct MergeState<T> {
    streams: Vec<MergeStream<T>>,
    vector_clock: Clock,
    cmp: MergedCmp<T>,
}

impl<T: Clone + Send + Sync + 'static> MergeState<T> {
    fn init(
        sources: &BTreeMap<SourceKey, Arc<dyn ErasedSource<T>>>,
        cmp: MergedCmp<T>,
        start: &MergedPos,
        end: &MergedPos,
    ) -> Self {
        let mut vector_clock: Clock = BTreeMap::new();
        let mut streams = Vec::with_capacity(sources.len());
        for (key, source) in sources {
            let earliest = source.earliest();
            let start_a = start
                .positions
                .get(key)
                .cloned()
                .unwrap_or_else(|| earliest.clone());
            let end_a = end.positions.get(key).cloned().unwrap_or(earliest);
            vector_clock.insert(key.clone(), start_a.clone());
            // Only build a per-source iter if the window is non-empty.
            if start_a.cmp_erased(end_a.as_ref()) == Ordering::Less {
                let stream = source.iter(start_a, end_a.clone());
                streams.push(MergeStream {
                    key: key.clone(),
                    stream,
                    head: None,
                    exhausted: false,
                    end_pos: end_a,
                });
            }
        }
        Self {
            streams,
            vector_clock,
            cmp,
        }
    }
}

#[allow(clippy::type_complexity)]
async fn merge_step<T>(
    mut state: MergeState<T>,
) -> Option<(
    Result<Positioned<MergedItem<T>, MergedPos>, HistoryError>,
    MergeState<T>,
)>
where
    T: Clone + Send + Sync + 'static,
{
    fill_heads(&mut state).await;
    if let Some(err) = take_first_error(&mut state) {
        return Some((Err(err), state));
    }
    let idx = pick_winner(&state)?;
    let (item, source_pos) = state.streams[idx]
        .head
        .take()
        .expect("winner head set")
        .expect("error case handled");
    let source_key = state.streams[idx].key.clone();
    state
        .vector_clock
        .insert(source_key.clone(), source_pos.clone());
    let merged_pos = MergedPos::new(state.vector_clock.clone());
    let merged_item = MergedItem {
        value: item,
        source_key,
        source_pos,
    };
    Some((Ok(Positioned::new(merged_item, merged_pos)), state))
}

/// Pull a head from any stream that doesn't have one yet. Streams that
/// finish within `(start, end]` advance the vector clock to their
/// `end_pos` so resumes know we consumed through.
async fn fill_heads<T>(state: &mut MergeState<T>)
where
    T: Send + Sync + 'static,
{
    for s in &mut state.streams {
        if s.head.is_some() || s.exhausted {
            continue;
        }
        if let Some(item) = s.stream.next().await {
            s.head = Some(item);
        } else {
            s.exhausted = true;
            state.vector_clock.insert(s.key.clone(), s.end_pos.clone());
        }
    }
}

/// Take the first head that is an error and mark that stream exhausted
/// so the caller doesn't keep polling it.
fn take_first_error<T>(state: &mut MergeState<T>) -> Option<HistoryError> {
    let i = state
        .streams
        .iter()
        .position(|s| matches!(&s.head, Some(Err(_))))?;
    let err = state.streams[i]
        .head
        .take()
        .expect("position checked head is Some")
        .err()
        .expect("position checked head is Err");
    state.streams[i].exhausted = true;
    Some(err)
}

/// Index of the stream whose head is smallest by `state.cmp`. Skips
/// streams without an `Ok` head. Returns `None` when no stream is
/// currently yieldable.
fn pick_winner<T>(state: &MergeState<T>) -> Option<usize>
where
    T: Clone + Send + Sync + 'static,
{
    let mut winner: Option<usize> = None;
    for i in 0..state.streams.len() {
        if !matches!(&state.streams[i].head, Some(Ok(_))) {
            continue;
        }
        winner = Some(match winner {
            None => i,
            Some(j) => {
                let (ai, ap) = state.streams[i]
                    .head
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .expect("head Ok above");
                let (bi, bp) = state.streams[j]
                    .head
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .expect("head Ok above");
                let mi_a = MergedItem {
                    value: ai.clone(),
                    source_key: state.streams[i].key.clone(),
                    source_pos: ap.clone(),
                };
                let mi_b = MergedItem {
                    value: bi.clone(),
                    source_key: state.streams[j].key.clone(),
                    source_pos: bp.clone(),
                };
                if (state.cmp)(&mi_a, &mi_b) == Ordering::Less {
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
    use crate::arch::Store;
    use futures_core::Stream;
    use futures_util::stream::{iter, poll_fn};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::time::timeout;

    // -- Mock source A: integer positions, simple "events" carrying a time. --

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Default)]
    struct MockPos(u64);

    impl Position for MockPos {
        fn marshal(&self) -> serde_json::Value {
            serde_json::json!(self.0)
        }
    }

    impl MockPos {
        fn from_value(v: &serde_json::Value) -> Option<Self> {
            v.as_u64().map(MockPos)
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct MockEvent {
        time: u64,
        label: String,
    }

    struct MockSource {
        #[allow(dead_code)]
        name: String,
        items: Mutex<Vec<MockEvent>>,
        tip: watch::Sender<MockPos>,
        self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
    }

    impl MockSource {
        fn new(name: &str, initial: Vec<MockEvent>) -> Arc<Self> {
            let tip = MockPos(initial.len() as u64);
            let s = Arc::new(Self {
                name: name.into(),
                items: Mutex::new(initial),
                tip: watch::channel(tip).0,
                self_ref: std::sync::OnceLock::new(),
            });
            let _ = s.self_ref.set(Arc::downgrade(&s));
            s
        }

        fn append(&self, e: MockEvent) {
            let new_tip = {
                let mut items = self.items.lock().unwrap();
                items.push(e);
                MockPos(items.len() as u64)
            };
            self.tip.send_replace(new_tip);
        }

        fn arc_self(&self) -> Arc<Self> {
            self.self_ref.get().unwrap().upgrade().unwrap()
        }

        fn hist_store(self: &Arc<Self>) -> HistStore<MockEvent, MockPos> {
            HistStore::new(Arc::clone(self) as Arc<dyn HistStoreBackend<MockEvent, MockPos>>)
        }
    }

    impl HistBackend<MockEvent, MockPos> for MockSource {
        fn iter(&self, start: MockPos, end: MockPos) -> ItemStream<MockEvent, MockPos> {
            let items = self.items.lock().unwrap().clone();
            let from = (start.0 as usize).min(items.len());
            let to = (end.0 as usize).min(items.len());
            let yielded: Vec<Positioned<MockEvent, MockPos>> = items
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
    impl HistStoreBackend<MockEvent, MockPos> for MockSource {
        fn snapshots(&self) -> HistStream<MockEvent, MockPos> {
            let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
            let mut sub = Store::new(self.tip.subscribe()).subscribe();
            Box::pin(poll_fn(move |cx| {
                Pin::new(&mut sub)
                    .poll_next(cx)
                    .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
            }))
        }

        async fn next(&self) -> Hist<MockEvent, MockPos> {
            let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
            Hist::new(backend, MockPos(0), *self.tip.borrow())
        }

        async fn time_pos(&self, _time: Timestamp) -> Result<MockPos, HistoryError> {
            Ok(MockPos(0))
        }

        fn load(&self, value: serde_json::Value) -> Option<MockPos> {
            MockPos::from_value(&value)
        }
    }

    fn ev(time: u64, label: &str) -> MockEvent {
        MockEvent {
            time,
            label: label.into(),
        }
    }

    fn cmp_by_time(a: &MergedItem<MockEvent>, b: &MergedItem<MockEvent>) -> Ordering {
        a.value.time.cmp(&b.value.time)
    }

    async fn collect_merged(
        hist: Hist<MergedItem<MockEvent>, MergedPos>,
    ) -> Vec<Positioned<MergedItem<MockEvent>, MergedPos>> {
        use futures_util::TryStreamExt;
        hist.iter().try_collect().await.unwrap()
    }

    #[tokio::test]
    async fn merged_pos_marshal_round_trip() {
        // Round-trips through the store's `load` (deserialize-on-the-store).
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let b = MockSource::new("b", vec![ev(20, "b1")]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);
        let items = collect_merged(merged.next().await).await;
        let pos = items[0].pos.clone();

        let value = pos.marshal();
        let restored = merged.load(value).expect("load");
        assert_eq!(restored, pos);
    }

    #[tokio::test]
    async fn merge_orders_by_provided_cmp() {
        // a@1 (time=10), a@2 (time=30), b@1 (time=20)
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let b = MockSource::new("b", vec![ev(20, "b1")]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);

        let items = collect_merged(merged.next().await).await;
        let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
        assert_eq!(labels, vec!["a1", "b1", "a2"]);
        // Check source_keys.
        let keys: Vec<&str> = items.iter().map(|p| p.item.source_key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "a"]);
    }

    #[tokio::test]
    async fn vector_clock_advances_per_source_only() {
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let b = MockSource::new("b", vec![ev(20, "b1")]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);
        let items = collect_merged(merged.next().await).await;

        // After yielding a1 (a@1), vector_clock = {a: 1, b: 0}.
        let p0 = &items[0].pos;
        assert_eq!(p0.get_as::<MockPos>("a"), Some(MockPos(1)));
        assert_eq!(p0.get_as::<MockPos>("b"), Some(MockPos(0)));

        // After yielding b1 (b@1), vector_clock = {a: 1, b: 1}.
        let p1 = &items[1].pos;
        assert_eq!(p1.get_as::<MockPos>("a"), Some(MockPos(1)));
        assert_eq!(p1.get_as::<MockPos>("b"), Some(MockPos(1)));

        // After yielding a2 (a@2), vector_clock = {a: 2, b: 1}.
        let p2 = &items[2].pos;
        assert_eq!(p2.get_as::<MockPos>("a"), Some(MockPos(2)));
        assert_eq!(p2.get_as::<MockPos>("b"), Some(MockPos(1)));
    }

    #[tokio::test]
    async fn split_resumes_after_anchor_per_source() {
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2"), ev(50, "a3")]);
        let b = MockSource::new("b", vec![ev(20, "b1"), ev(40, "b2")]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);

        let hist = merged.next().await;
        let items_all = collect_merged(hist.clone()).await;
        // Expected: a1(10), b1(20), a2(30), b2(40), a3(50).
        assert_eq!(items_all.len(), 5);

        // Anchor after b1 (index 1).
        let anchor = items_all[1].pos.clone();
        let (_, tail) = hist.split(anchor);
        let items_tail = collect_merged(tail).await;
        let labels: Vec<&str> = items_tail
            .iter()
            .map(|p| p.item.value.label.as_str())
            .collect();
        assert_eq!(labels, vec!["a2", "b2", "a3"]);
    }

    #[tokio::test]
    async fn merged_pos_handles_added_source() {
        // Merge with one source, save anchor, then merge with two sources
        // using the same saved anchor (missing the new source's key).
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let mut sources1 = BTreeMap::new();
        sources1.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        let merged1 = merge_hist_stores(sources1, cmp_by_time);
        let items = collect_merged(merged1.next().await).await;
        let anchor_after_a1 = items[0].pos.clone();
        assert!(anchor_after_a1.get("a").is_some());
        assert!(anchor_after_a1.get("b").is_none());

        // Serialize the saved (a-only) anchor to simulate persistence.
        let value = anchor_after_a1.marshal();

        // Build a new merge that adds source "b".
        let a2 = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let b = MockSource::new("b", vec![ev(20, "b1"), ev(40, "b2")]);
        let mut sources2 = BTreeMap::new();
        sources2.insert("a".into(), ErasedHistStore::new(a2.hist_store()));
        sources2.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged2 = merge_hist_stores(sources2, cmp_by_time);

        // Reload against the two-source merge — the missing "b" key defaults
        // to b's earliest.
        let restored = merged2.load(value).expect("load");
        let hist = merged2.next().await;
        let (_, tail) = hist.split(restored);
        let items_tail = collect_merged(tail).await;
        // Items after anchor_after_a1: from "a" only items past pos=1 (so a2);
        // from "b" everything (it wasn't in the old anchor → default = pos 0).
        let labels: Vec<&str> = items_tail
            .iter()
            .map(|p| p.item.value.label.as_str())
            .collect();
        // By time: b1(20), a2(30), b2(40).
        assert_eq!(labels, vec!["b1", "a2", "b2"]);
    }

    #[tokio::test]
    async fn empty_source_does_not_block_others() {
        let a = MockSource::new("a", vec![ev(10, "a1"), ev(20, "a2")]);
        let b = MockSource::new("b", vec![]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);
        let items = collect_merged(merged.next().await).await;
        let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
        assert_eq!(labels, vec!["a1", "a2"]);
    }

    #[tokio::test]
    async fn stable_order_on_cmp_tie() {
        // Tie at time=10 — sorted-key order wins: "a" before "z".
        let a = MockSource::new("a", vec![ev(10, "a1")]);
        let z = MockSource::new("z", vec![ev(10, "z1")]);
        let mut sources = BTreeMap::new();
        sources.insert("z".into(), ErasedHistStore::new(z.hist_store()));
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);
        let items = collect_merged(merged.next().await).await;
        let labels: Vec<&str> = items.iter().map(|p| p.item.value.label.as_str()).collect();
        assert_eq!(labels, vec!["a1", "z1"]);
    }

    #[tokio::test]
    async fn snapshots_advance_when_any_source_appends() {
        let a = MockSource::new("a", vec![ev(10, "a1")]);
        let b = MockSource::new("b", vec![]);
        let mut sources = BTreeMap::new();
        sources.insert("a".into(), ErasedHistStore::new(a.hist_store()));
        sources.insert("b".into(), ErasedHistStore::new(b.clone().hist_store()));
        let merged = merge_hist_stores(sources, cmp_by_time);
        let mut snaps = merged.snapshots();

        let h1 = snaps.next().await.expect("first snapshot");
        let labels1: Vec<String> = collect_merged(h1)
            .await
            .iter()
            .map(|p| p.item.value.label.clone())
            .collect();
        assert_eq!(labels1, vec!["a1"]);

        // Append to b — merged store should emit a new snapshot.
        b.append(ev(20, "b1"));
        // The aggregation task is async; give it a chance to propagate.
        let h2 = timeout(Duration::from_secs(2), snaps.next())
            .await
            .expect("snapshot within timeout")
            .expect("snapshot stream not closed");
        let labels2: Vec<String> = collect_merged(h2)
            .await
            .iter()
            .map(|p| p.item.value.label.clone())
            .collect();
        assert_eq!(labels2, vec!["a1", "b1"]);
    }
}
