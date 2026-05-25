//! Heterogeneous-source merge — combine multiple [`HistStore`]s yielding a
//! common item type `T` into a single [`HistStore<MergedItem<T>, MergedPos>`].
//!
//! Each source can have its own [`Position`] type; positions live as
//! marshalled bytes inside [`MergedPos`]. Items are ordered across sources
//! by a user-supplied [`MergedCmp`].
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
//! [`Position`]'s required [`Ord`] is derived on [`MergedPos`] from
//! [`BTreeMap<String, Vec<u8>>`], which is byte-lex over sorted keys. This
//! is a total order suitable for [`Position`] but *does not* match
//! component-wise containment (the marshalled bytes of e.g. an
//! `EvmPosition` are postcard leb128 and do not preserve numeric order
//! across byte-length boundaries). [`MergedHistBackend`] overrides
//! [`HistBackend::clamp_pos`] to clamp each per-source position separately
//! via that source's own [`Ord`], which is the correct semantic for
//! [`Hist::split`].
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
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::stream::{poll_fn, unfold};
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::arch::{
    AsyncDisposable, Hist, HistBackend, HistStore, HistStoreBackend, HistStream, HistoryError,
    ItemStream, Position, Positioned, Store, Subscription, Timestamp,
};

// ============================================================
// MergedPos
// ============================================================

/// Keyed position into a [`merge_hist_stores`] product. Maps source key →
/// marshalled per-source position.
///
/// Stored bytes are the per-source [`Position::marshal`] output. Missing
/// keys are treated as that source's [`Default`].
#[derive(
    Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct MergedPos {
    pub positions: BTreeMap<String, Vec<u8>>,
}

impl MergedPos {
    pub fn new(positions: BTreeMap<String, Vec<u8>>) -> Self {
        Self { positions }
    }

    /// Marshalled bytes for `key`, or `None` if absent.
    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.positions.get(key).map(std::vec::Vec::as_slice)
    }
}

impl Position for MergedPos {
    fn marshal(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("MergedPos serialization cannot fail")
    }

    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

// ============================================================
// MergedItem<T>
// ============================================================

/// Item emitted by a merged history. `source_key` identifies which input
/// source produced it; `source_pos` is the marshalled per-source position.
#[derive(Clone, Debug)]
pub struct MergedItem<T> {
    pub value: T,
    pub source_key: String,
    pub source_pos: Vec<u8>,
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

/// Which of a [`HistStore`]'s three tip stores to read or subscribe to.
#[derive(Copy, Clone, Debug)]
enum TipKind {
    Latest,
    Best,
    Safe,
}

#[async_trait::async_trait]
trait ErasedSource<T: Send + Sync + 'static>: Send + Sync {
    /// Marshalled bytes of the source's [`Position::default`].
    fn earliest_pos(&self) -> Vec<u8>;
    /// Compare two marshalled positions via the source's own [`Ord`].
    fn cmp_bytes(&self, a: &[u8], b: &[u8]) -> Ordering;
    /// Iterate items in `(start, end]` for this source. Bytes are this
    /// source's marshalled positions.
    fn iter(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Subscription<Result<(T, Vec<u8>), HistoryError>>;
    /// Current marshalled tip for `kind`.
    fn tip_pos(&self, kind: TipKind) -> Vec<u8>;
    /// Subscribe to changes on the `kind` tip.
    fn subscribe_tip(&self, kind: TipKind) -> Subscription<Vec<u8>>;
    /// Translate a wall-clock time into a marshalled position.
    async fn time_pos(&self, time: Timestamp) -> Result<Vec<u8>, HistoryError>;
}

struct ErasedSourceImpl<T, P: Position> {
    store: HistStore<T, P>,
    _p: std::marker::PhantomData<P>,
}

impl<T, P> ErasedSourceImpl<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn tip_store(&self, kind: TipKind) -> &Store<P> {
        match kind {
            TipKind::Latest => self.store.latest(),
            TipKind::Best => self.store.best(),
            TipKind::Safe => self.store.safe(),
        }
    }
}

#[async_trait::async_trait]
impl<T, P> ErasedSource<T> for ErasedSourceImpl<T, P>
where
    T: Clone + Send + Sync + 'static,
    P: Position,
{
    fn earliest_pos(&self) -> Vec<u8> {
        P::default().marshal()
    }

    fn cmp_bytes(&self, a: &[u8], b: &[u8]) -> Ordering {
        match (P::unmarshal(a), P::unmarshal(b)) {
            (Some(pa), Some(pb)) => pa.cmp(&pb),
            // Fall back to bytewise compare on garbage input — keeps
            // clamp_pos total without panicking.
            _ => a.cmp(b),
        }
    }

    fn iter(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Subscription<Result<(T, Vec<u8>), HistoryError>> {
        let store = self.store.clone();
        Box::pin(unfold(
            ErasedIterState::<T, P>::Init { start, end, store },
            erased_iter_step::<T, P>,
        ))
    }

    fn tip_pos(&self, kind: TipKind) -> Vec<u8> {
        self.tip_store(kind).get().marshal()
    }

    fn subscribe_tip(&self, kind: TipKind) -> Subscription<Vec<u8>> {
        Box::pin(self.tip_store(kind).subscribe().map(|p| p.marshal()))
    }

    async fn time_pos(&self, time: Timestamp) -> Result<Vec<u8>, HistoryError> {
        let p = self.store.time_pos(time).await?;
        Ok(p.marshal())
    }
}

enum ErasedIterState<T, P: Position> {
    Init {
        start: Vec<u8>,
        end: Vec<u8>,
        store: HistStore<T, P>,
    },
    Streaming(ItemStream<T, P>),
    Done,
}

async fn erased_iter_step<T, P>(
    state: ErasedIterState<T, P>,
) -> Option<(Result<(T, Vec<u8>), HistoryError>, ErasedIterState<T, P>)>
where
    T: Send + Sync + 'static,
    P: Position,
{
    match state {
        ErasedIterState::Init { start, end, store } => {
            let Some(start_p) = P::unmarshal(&start) else {
                return Some((
                    Err(HistoryError::internal(
                        "merge: failed to unmarshal start position",
                    )),
                    ErasedIterState::Done,
                ));
            };
            let Some(end_p) = P::unmarshal(&end) else {
                return Some((
                    Err(HistoryError::internal(
                        "merge: failed to unmarshal end position",
                    )),
                    ErasedIterState::Done,
                ));
            };
            let hist = store.next().await;
            // Take the window via two splits — uses per-source Ord, which
            // is the canonical clamping for each source's own P.
            let (_, tail) = hist.split(start_p);
            let (window, _) = tail.split(end_p);
            let mut stream = window.iter();
            match stream.next().await {
                Some(Ok(p)) => Some((
                    Ok((p.item, p.pos.marshal())),
                    ErasedIterState::Streaming(stream),
                )),
                Some(Err(e)) => Some((Err(e), ErasedIterState::Done)),
                None => None,
            }
        }
        ErasedIterState::Streaming(mut stream) => match stream.next().await {
            Some(Ok(p)) => Some((
                Ok((p.item, p.pos.marshal())),
                ErasedIterState::Streaming(stream),
            )),
            Some(Err(e)) => Some((Err(e), ErasedIterState::Done)),
            None => None,
        },
        ErasedIterState::Done => None,
    }
}

// ============================================================
// AbortAll — JoinHandle cleanup on drop
// ============================================================

struct AbortAll(Vec<JoinHandle<()>>);

impl Drop for AbortAll {
    fn drop(&mut self) {
        for h in self.0.drain(..) {
            h.abort();
        }
    }
}

// ============================================================
// Tip aggregation
// ============================================================

/// Build a [`Store<MergedPos>`] that aggregates per-source tip bytes
/// (for the given [`TipKind`]) via background tasks. Tasks abort when
/// the returned [`AbortAll`] drops.
fn build_tip_store<T>(
    sources: &BTreeMap<String, Arc<dyn ErasedSource<T>>>,
    kind: TipKind,
) -> (Store<MergedPos>, AbortAll)
where
    T: Send + Sync + 'static,
{
    let initial_positions: BTreeMap<String, Vec<u8>> = sources
        .iter()
        .map(|(k, s)| (k.clone(), s.tip_pos(kind)))
        .collect();
    let (tx, rx) = watch::channel(MergedPos::new(initial_positions.clone()));

    let state = Arc::new(std::sync::Mutex::new(initial_positions));
    let tx = Arc::new(tx);
    let mut handles = Vec::with_capacity(sources.len());
    for (key, source) in sources {
        let key = key.clone();
        let mut sub = source.subscribe_tip(kind);
        let state = state.clone();
        let tx = tx.clone();
        // Don't skip the first emission. Watch channels coalesce, so if a
        // source updates between `merge_hist_stores` returning and the
        // task starting, the update would arrive as the "first" emission
        // and get lost. The redundant initial send is harmless because
        // the consumer-side watch channel coalesces too.
        handles.push(tokio::spawn(async move {
            while let Some(bytes) = sub.next().await {
                let new_pos = {
                    let mut s = state.lock().unwrap();
                    s.insert(key.clone(), bytes);
                    MergedPos::new(s.clone())
                };
                let _ = tx.send(new_pos);
            }
        }));
    }

    (Store::new(rx), AbortAll(handles))
}

// ============================================================
// MergedHistStoreBackend
// ============================================================

/// Merge `sources` into a single [`HistStore`] over [`MergedItem<T>`].
///
/// `cmp` orders items across sources — return [`Ordering::Less`] when the
/// first argument should be yielded before the second. Ties resolve in
/// sorted-key order (stable). Each yielded item carries a vector-clock
/// [`MergedPos`] that survives marshal/unmarshal and is a valid
/// [`Hist::split`] anchor for resume.
pub fn merge_hist_stores<T, F>(
    sources: BTreeMap<String, ErasedHistStore<T>>,
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
    sources: BTreeMap<String, Arc<dyn ErasedSource<T>>>,
    cmp: MergedCmp<T>,
    latest_store: Store<MergedPos>,
    best_store: Store<MergedPos>,
    safe_store: Store<MergedPos>,
    _tasks: Vec<AbortAll>,
}

impl<T: Clone + Send + Sync + 'static> MergedHistStoreBackend<T> {
    fn new(stores: BTreeMap<String, ErasedHistStore<T>>, cmp: MergedCmp<T>) -> Self {
        let sources: BTreeMap<String, Arc<dyn ErasedSource<T>>> =
            stores.into_iter().map(|(k, s)| (k, s.inner)).collect();

        let (latest_store, latest_task) = build_tip_store(&sources, TipKind::Latest);
        let (best_store, best_task) = build_tip_store(&sources, TipKind::Best);
        let (safe_store, safe_task) = build_tip_store(&sources, TipKind::Safe);

        Self {
            sources,
            cmp,
            latest_store,
            best_store,
            safe_store,
            _tasks: vec![latest_task, best_task, safe_task],
        }
    }

    fn make_hist(&self, end: MergedPos) -> Hist<MergedItem<T>, MergedPos> {
        let earliest: BTreeMap<String, Vec<u8>> = self
            .sources
            .iter()
            .map(|(k, s)| (k.clone(), s.earliest_pos()))
            .collect();
        let backend: Arc<dyn HistBackend<MergedItem<T>, MergedPos>> = Arc::new(MergedHistBackend {
            sources: self.sources.clone(),
            cmp: self.cmp.clone(),
        });
        Hist::new(backend, MergedPos::new(earliest), end)
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
        let mut tip_sub = self.latest_store.subscribe();
        Box::pin(poll_fn(move |cx| {
            use std::task::Poll;
            match Pin::new(&mut tip_sub).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(tip)) => {
                    let earliest: BTreeMap<String, Vec<u8>> = sources
                        .iter()
                        .map(|(k, s)| (k.clone(), s.earliest_pos()))
                        .collect();
                    let backend: Arc<dyn HistBackend<MergedItem<T>, MergedPos>> =
                        Arc::new(MergedHistBackend {
                            sources: sources.clone(),
                            cmp: cmp.clone(),
                        });
                    Poll::Ready(Some(Hist::new(backend, MergedPos::new(earliest), tip)))
                }
            }
        }))
    }

    async fn next(&self) -> Hist<MergedItem<T>, MergedPos> {
        let tip = self.latest_store.get();
        self.make_hist(tip)
    }

    fn subscribe(
        &self,
        cb: Box<dyn Fn(Hist<MergedItem<T>, MergedPos>) + Send + Sync>,
    ) -> AsyncDisposable {
        let mut stream = self.snapshots();
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel2.cancelled() => break,
                    next = stream.next() => match next {
                        Some(h) => cb(h),
                        None => break,
                    }
                }
            }
        });
        AsyncDisposable::new(cancel, handle)
    }

    async fn time_pos(&self, time: Timestamp) -> Result<MergedPos, HistoryError> {
        let mut positions = BTreeMap::new();
        for (k, s) in &self.sources {
            let bytes = s.time_pos(time).await?;
            positions.insert(k.clone(), bytes);
        }
        Ok(MergedPos::new(positions))
    }

    fn latest(&self) -> &Store<MergedPos> {
        &self.latest_store
    }
    fn best(&self) -> &Store<MergedPos> {
        &self.best_store
    }
    fn safe(&self) -> &Store<MergedPos> {
        &self.safe_store
    }
}

// ============================================================
// MergedHistBackend (iter + clamp_pos)
// ============================================================

struct MergedHistBackend<T> {
    sources: BTreeMap<String, Arc<dyn ErasedSource<T>>>,
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

    /// Component-wise clamping. For each source key, deserialize the
    /// per-source position via that source's `cmp_bytes` and clamp to
    /// `[start[k], end[k]]`. Missing keys substitute the source's
    /// [`Default`] (so saved [`MergedPos`] from older snapshots survive).
    fn clamp_pos(&self, p: MergedPos, start: &MergedPos, end: &MergedPos) -> MergedPos {
        let mut clamped: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (key, source) in &self.sources {
            let earliest = source.earliest_pos();
            let start_bytes = start
                .positions
                .get(key)
                .cloned()
                .unwrap_or_else(|| earliest.clone());
            let end_bytes = end.positions.get(key).cloned().unwrap_or(earliest.clone());
            let p_bytes = p.positions.get(key).cloned().unwrap_or(earliest);
            let chosen = if source.cmp_bytes(&p_bytes, &start_bytes) == Ordering::Less {
                start_bytes
            } else if source.cmp_bytes(&p_bytes, &end_bytes) == Ordering::Greater {
                end_bytes
            } else {
                p_bytes
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
    key: String,
    stream: Subscription<Result<(T, Vec<u8>), HistoryError>>,
    head: Option<Result<(T, Vec<u8>), HistoryError>>,
    exhausted: bool,
    end_pos: Vec<u8>,
}

struct MergeState<T> {
    streams: Vec<MergeStream<T>>,
    vector_clock: BTreeMap<String, Vec<u8>>,
    cmp: MergedCmp<T>,
}

impl<T: Clone + Send + Sync + 'static> MergeState<T> {
    fn init(
        sources: &BTreeMap<String, Arc<dyn ErasedSource<T>>>,
        cmp: MergedCmp<T>,
        start: &MergedPos,
        end: &MergedPos,
    ) -> Self {
        let mut vector_clock: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut streams = Vec::with_capacity(sources.len());
        for (key, source) in sources {
            let earliest = source.earliest_pos();
            let start_bytes = start
                .positions
                .get(key)
                .cloned()
                .unwrap_or_else(|| earliest.clone());
            let end_bytes = end.positions.get(key).cloned().unwrap_or(earliest);
            vector_clock.insert(key.clone(), start_bytes.clone());
            // Only build a per-source iter if the window is non-empty.
            if source.cmp_bytes(&start_bytes, &end_bytes) == Ordering::Less {
                let stream = source.iter(start_bytes, end_bytes.clone());
                streams.push(MergeStream {
                    key: key.clone(),
                    stream,
                    head: None,
                    exhausted: false,
                    end_pos: end_bytes,
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
    let (item, source_pos) = state
        .streams[idx]
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
    use futures_util::stream::iter;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::time::timeout;

    // -- Mock source A: integer positions, simple "events" carrying a time. --

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
    struct MockEvent {
        time: u64,
        label: String,
    }

    struct MockSource {
        #[allow(dead_code)]
        name: String,
        items: Mutex<Vec<MockEvent>>,
        latest_store: Store<MockPos>,
        latest_tx: watch::Sender<MockPos>,
        best_store: Store<MockPos>,
        safe_store: Store<MockPos>,
        self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
    }

    impl MockSource {
        fn new(name: &str, initial: Vec<MockEvent>) -> Arc<Self> {
            let tip = MockPos(initial.len() as u64);
            let (tx, rx) = watch::channel(tip);
            let (_, b_rx) = watch::channel(tip);
            let (_, s_rx) = watch::channel(tip);
            let s = Arc::new(Self {
                name: name.into(),
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

        fn append(&self, e: MockEvent) {
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
            let mut sub = self.latest_store.subscribe();
            Box::pin(poll_fn(move |cx| {
                Pin::new(&mut sub)
                    .poll_next(cx)
                    .map(|tip_opt| tip_opt.map(|tip| Hist::new(backend.clone(), MockPos(0), tip)))
            }))
        }

        async fn next(&self) -> Hist<MockEvent, MockPos> {
            let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
            Hist::new(backend, MockPos(0), self.latest_store.get())
        }

        fn subscribe(
            &self,
            cb: Box<dyn Fn(Hist<MockEvent, MockPos>) + Send + Sync>,
        ) -> AsyncDisposable {
            let backend: Arc<dyn HistBackend<MockEvent, MockPos>> = self.arc_self();
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
        let mut m = BTreeMap::new();
        m.insert("eth".into(), vec![1, 2, 3]);
        m.insert("op".into(), vec![4, 5, 6, 7]);
        let pos = MergedPos::new(m.clone());
        let bytes = pos.marshal();
        let restored = MergedPos::unmarshal(&bytes).expect("unmarshal");
        assert_eq!(restored.positions, m);
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
        assert_eq!(MockPos::unmarshal(p0.get("a").unwrap()), Some(MockPos(1)));
        assert_eq!(MockPos::unmarshal(p0.get("b").unwrap()), Some(MockPos(0)));

        // After yielding b1 (b@1), vector_clock = {a: 1, b: 1}.
        let p1 = &items[1].pos;
        assert_eq!(MockPos::unmarshal(p1.get("a").unwrap()), Some(MockPos(1)));
        assert_eq!(MockPos::unmarshal(p1.get("b").unwrap()), Some(MockPos(1)));

        // After yielding a2 (a@2), vector_clock = {a: 2, b: 1}.
        let p2 = &items[2].pos;
        assert_eq!(MockPos::unmarshal(p2.get("a").unwrap()), Some(MockPos(2)));
        assert_eq!(MockPos::unmarshal(p2.get("b").unwrap()), Some(MockPos(1)));
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

        // Marshal/unmarshal to simulate persistence.
        let bytes = anchor_after_a1.marshal();
        let restored = MergedPos::unmarshal(&bytes).expect("unmarshal");

        // Build a new merge that adds source "b".
        let a2 = MockSource::new("a", vec![ev(10, "a1"), ev(30, "a2")]);
        let b = MockSource::new("b", vec![ev(20, "b1"), ev(40, "b2")]);
        let mut sources2 = BTreeMap::new();
        sources2.insert("a".into(), ErasedHistStore::new(a2.hist_store()));
        sources2.insert("b".into(), ErasedHistStore::new(b.hist_store()));
        let merged2 = merge_hist_stores(sources2, cmp_by_time);

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
