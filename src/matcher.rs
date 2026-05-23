//! `Matcher` — pure correlation between a *src* stream and a *dst* stream.
//!
//! A matcher has two inputs (each a [`HistStore`]) and a user-supplied
//! [`MatchFn`] that, given the post-anchor tails of both streams, produces
//! an [`Output`] (commonly [`MatchOutput`] with `matched`/`pending`/`orphans`).
//! Anchors are consumer-owned and explicit: [`BaseMatcher::compute_state`]
//! mutates nothing, returns a fresh [`BaseMatcherState`]; the caller decides
//! when to advance.
//!
//! ## Two layers
//!
//! * [`BaseMatcher`] is the pure compute kernel. It owns nothing mutable and
//!   no background loop other than the [`changes`](BaseMatcher::changes)
//!   aggregator. Tests and ad-hoc callers drive it via
//!   [`compute_state`](BaseMatcher::compute_state).
//! * [`AnchoredMatcher`] wraps a [`BaseMatcher`] with persistence over an
//!   [`AnchorStore`] plus an in-memory anchor cache. Production callers
//!   typically use this — `current_state` / `set_anchors` / `iter` provide
//!   the lazy-load + advance + persist convenience.
//!
//! [`Matcher`] is a type alias for the common case
//! `AnchoredMatcher<Src, Dst, MatchOutput<Src, Dst>, SP, DP>`.
//!
//! ## Matched pairs are transient
//!
//! Matched pairs appear in [`MatchOutput::matched`] for one or more
//! compute calls and then disappear once anchors advance past them. This is
//! intentional: the matcher is a *windowed correlator*, not a permanent
//! event log. Consumers that need a historical record persist matched
//! pairs themselves before advancing anchors.
//!
//! ## Asymmetric anchor advance
//!
//! Advance *only* the src anchor past a matched pair (without advancing
//! dst) and on the next [`compute_state`](BaseMatcher::compute_state) call
//! the pair surfaces as an **orphan** — the dst item is still inside the
//! dst tail but the src item is gone. This is by design: a matcher anchor
//! pair `{src, dst}` does *not* represent "everything processed up to this
//! pair," it represents "items strictly after these positions are still
//! eligible for matching." If consumers want monotonic guarantees they
//! advance anchors as a synchronized pair.

use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};

use crate::arch::{
    Hist, HistStore, HistoryError, Position, Positioned, Store, Subscription, Timestamp,
};

// ============================================================
// MatcherAnchors<SP, DP>
// ============================================================

/// Pair of source/destination anchors. Marshaled as
/// `len(src) || src || len(dst) || dst` (4-byte little-endian lengths).
pub struct MatcherAnchors<SP: Position, DP: Position> {
    pub src: SP,
    pub dst: DP,
}

impl<SP: Position, DP: Position> Clone for MatcherAnchors<SP, DP> {
    fn clone(&self) -> Self {
        Self {
            src: self.src.clone(),
            dst: self.dst.clone(),
        }
    }
}

impl<SP: Position, DP: Position> std::fmt::Debug for MatcherAnchors<SP, DP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatcherAnchors")
            .field("src", &self.src)
            .field("dst", &self.dst)
            .finish()
    }
}

impl<SP: Position, DP: Position> PartialEq for MatcherAnchors<SP, DP> {
    fn eq(&self, other: &Self) -> bool {
        self.src == other.src && self.dst == other.dst
    }
}

impl<SP: Position, DP: Position> Eq for MatcherAnchors<SP, DP> {}

impl<SP: Position, DP: Position> MatcherAnchors<SP, DP> {
    pub fn new(src: SP, dst: DP) -> Self {
        Self { src, dst }
    }

    pub fn marshal(&self) -> Vec<u8> {
        let s = self.src.marshal();
        let d = self.dst.marshal();
        let mut out = Vec::with_capacity(8 + s.len() + d.len());
        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
        out.extend_from_slice(&s);
        out.extend_from_slice(&(d.len() as u32).to_le_bytes());
        out.extend_from_slice(&d);
        out
    }

    pub fn unmarshal(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let s_len = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
        if bytes.len() < 4 + s_len + 4 {
            return None;
        }
        let src = SP::unmarshal(&bytes[4..4 + s_len])?;
        let d_off = 4 + s_len;
        let d_len = u32::from_le_bytes(bytes[d_off..d_off + 4].try_into().ok()?) as usize;
        if bytes.len() < d_off + 4 + d_len {
            return None;
        }
        let dst = DP::unmarshal(&bytes[d_off + 4..d_off + 4 + d_len])?;
        Some(Self::new(src, dst))
    }
}

// ============================================================
// Match / MatchOutput
// ============================================================

/// One matched pair.
#[derive(Clone, Debug)]
pub struct Match<Src, Dst> {
    pub src: Src,
    pub dst: Dst,
}

/// Default [`MatchFn`] output shape: matched pairs + unpaired src/dst.
#[derive(Clone, Debug)]
pub struct MatchOutput<Src, Dst> {
    pub matched: Vec<Match<Src, Dst>>,
    pub pending: Vec<Src>,
    pub orphans: Vec<Dst>,
}

impl<Src, Dst> Default for MatchOutput<Src, Dst> {
    fn default() -> Self {
        Self {
            matched: Vec::new(),
            pending: Vec::new(),
            orphans: Vec::new(),
        }
    }
}

// ============================================================
// MatchInput / MatchFn
// ============================================================

/// Input to a [`MatchFn`]. Carries both the snapshot tails (`src_hist`,
/// `dst_hist`) and the already-collected item vectors so callers can pick
/// whichever is more convenient.
pub struct MatchInput<Src, Dst, SP: Position, DP: Position>
where
    Src: Send + Sync + 'static,
    Dst: Send + Sync + 'static,
{
    pub src_hist: Hist<Src, SP>,
    pub dst_hist: Hist<Dst, DP>,
    pub src_items: Vec<Positioned<Src, SP>>,
    pub dst_items: Vec<Positioned<Dst, DP>>,
    pub anchors: MatcherAnchors<SP, DP>,
}

/// User-supplied matching function. Implement directly for async or
/// stateful match logic; for sync, use [`match_fn`].
#[async_trait::async_trait]
pub trait MatchFn<Src, Dst, Output, SP, DP>: Send + Sync
where
    Src: Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    Output: Send + 'static,
    SP: Position,
    DP: Position,
{
    async fn matchup(&self, input: MatchInput<Src, Dst, SP, DP>) -> Result<Output, HistoryError>;
}

/// Wrap a sync closure as a [`MatchFn`]. Equivalent to implementing the
/// trait directly with `async fn matchup` returning a non-awaiting body.
pub fn match_fn<F, Src, Dst, Output, SP, DP>(f: F) -> Arc<dyn MatchFn<Src, Dst, Output, SP, DP>>
where
    F: Fn(MatchInput<Src, Dst, SP, DP>) -> Result<Output, HistoryError> + Send + Sync + 'static,
    Src: Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    Output: Send + 'static,
    SP: Position,
    DP: Position,
{
    struct SyncMatchFn<F>(F);

    #[async_trait::async_trait]
    impl<F, Src, Dst, Output, SP, DP> MatchFn<Src, Dst, Output, SP, DP> for SyncMatchFn<F>
    where
        F: Fn(MatchInput<Src, Dst, SP, DP>) -> Result<Output, HistoryError> + Send + Sync,
        Src: Send + Sync + 'static,
        Dst: Send + Sync + 'static,
        Output: Send + 'static,
        SP: Position,
        DP: Position,
    {
        async fn matchup(
            &self,
            input: MatchInput<Src, Dst, SP, DP>,
        ) -> Result<Output, HistoryError> {
            (self.0)(input)
        }
    }

    Arc::new(SyncMatchFn(f))
}

// ============================================================
// BaseMatcherState
// ============================================================

/// What [`BaseMatcher::compute_state`] returns: the [`MatchFn`] output
/// plus the inputs that produced it (items + anchors), so callers can
/// derive next-anchors without re-fetching.
#[derive(Clone, Debug)]
pub struct BaseMatcherState<Src, Dst, Output, SP, DP>
where
    Src: Send + Sync + 'static,
    Dst: Send + Sync + 'static,
    Output: 'static,
    SP: Position,
    DP: Position,
{
    pub output: Output,
    pub src_items: Vec<Positioned<Src, SP>>,
    pub dst_items: Vec<Positioned<Dst, DP>>,
    pub anchors: MatcherAnchors<SP, DP>,
}

// ============================================================
// AbortOnDrop
// ============================================================

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// ============================================================
// BaseMatcher
// ============================================================

/// Pure compute kernel. Construct once, call
/// [`compute_state`](Self::compute_state) per snapshot. No mutable state
/// other than a background task that drives [`changes`](Self::changes).
pub struct BaseMatcher<Src, Dst, Output, SP, DP>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Clone + Send + Sync + 'static,
    Output: Send + 'static,
    SP: Position,
    DP: Position,
{
    src_store: HistStore<Src, SP>,
    dst_store: HistStore<Dst, DP>,
    match_fn: Arc<dyn MatchFn<Src, Dst, Output, SP, DP>>,
    changes_store: Store<()>,
    _changes_task: AbortOnDrop,
}

impl<Src, Dst, Output, SP, DP> BaseMatcher<Src, Dst, Output, SP, DP>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Clone + Send + Sync + 'static,
    Output: Send + 'static,
    SP: Position,
    DP: Position,
{
    pub fn new(
        src_store: HistStore<Src, SP>,
        dst_store: HistStore<Dst, DP>,
        match_fn: Arc<dyn MatchFn<Src, Dst, Output, SP, DP>>,
    ) -> Self {
        let (changes_store, task) = build_changes_store::<SP, DP>(
            src_store.latest().subscribe(),
            dst_store.latest().subscribe(),
        );
        Self {
            src_store,
            dst_store,
            match_fn,
            changes_store,
            _changes_task: task,
        }
    }

    /// Compute the matcher state at `anchors`. Pure — call twice with the
    /// same `anchors` and get the same output (modulo new items at the tip).
    pub async fn compute_state(
        &self,
        anchors: MatcherAnchors<SP, DP>,
    ) -> Result<BaseMatcherState<Src, Dst, Output, SP, DP>, HistoryError> {
        let src = self.src_store.next().await;
        let dst = self.dst_store.next().await;
        let (_, src_hist) = src.split(anchors.src.clone());
        let (_, dst_hist) = dst.split(anchors.dst.clone());
        let src_items: Vec<Positioned<Src, SP>> = src_hist.iter().try_collect().await?;
        let dst_items: Vec<Positioned<Dst, DP>> = dst_hist.iter().try_collect().await?;
        let input = MatchInput {
            src_hist: src_hist.clone(),
            dst_hist: dst_hist.clone(),
            src_items: src_items.clone(),
            dst_items: dst_items.clone(),
            anchors: anchors.clone(),
        };
        let output = self.match_fn.matchup(input).await?;
        Ok(BaseMatcherState {
            output,
            src_items,
            dst_items,
            anchors,
        })
    }

    /// Wake-up signal: emits `()` whenever either src or dst tip advances.
    /// First emission is the current value (per [`Store::subscribe`]).
    pub fn changes(&self) -> Store<()> {
        self.changes_store.clone()
    }

    /// Build matcher anchors at wall-clock time `since` by delegating to
    /// each side's [`HistStore::time_pos`].
    pub async fn time_anchors(
        &self,
        since: Timestamp,
    ) -> Result<MatcherAnchors<SP, DP>, HistoryError> {
        let src = self.src_store.time_pos(since).await?;
        let dst = self.dst_store.time_pos(since).await?;
        Ok(MatcherAnchors::new(src, dst))
    }

    /// Compute state with anchors at `since`. Bypasses any persisted
    /// anchors — useful for API / CLI history backfills.
    pub async fn get_history(
        &self,
        since: Timestamp,
    ) -> Result<BaseMatcherState<Src, Dst, Output, SP, DP>, HistoryError> {
        let anchors = self.time_anchors(since).await?;
        self.compute_state(anchors).await
    }
}

fn build_changes_store<SP, DP>(
    mut src_sub: Subscription<SP>,
    mut dst_sub: Subscription<DP>,
) -> (Store<()>, AbortOnDrop)
where
    SP: Position,
    DP: Position,
{
    let (tx, rx) = tokio::sync::watch::channel(());
    let handle = tokio::spawn(async move {
        // Always propagate every emission. Tokio `watch` channels coalesce
        // missed updates into the next observable value, so skipping the
        // first emission to dedupe with the initial `()` of the changes
        // channel would silently lose any update that arrived between
        // subscribe-time and task-poll. The extra startup `()` ticks are
        // harmless — consumers recompute against unchanged state.
        loop {
            tokio::select! {
                pos = src_sub.next() => {
                    if pos.is_none() { break; }
                    let _ = tx.send(());
                }
                pos = dst_sub.next() => {
                    if pos.is_none() { break; }
                    let _ = tx.send(());
                }
            }
        }
    });
    (Store::new(rx), AbortOnDrop(handle))
}

// ============================================================
// AnchorStore
// ============================================================

/// Tiny KV interface for persisting matcher anchors. Source-agnostic —
/// implementations live alongside this trait so consumers can pick a
/// backing they like without dragging in `LogStore` / EVM.
#[async_trait::async_trait]
pub trait AnchorStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, HistoryError>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), HistoryError>;
}

/// Hash-map-backed [`AnchorStore`] for tests and ephemeral runs.
#[derive(Default)]
pub struct InMemoryAnchorStore {
    inner: tokio::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl InMemoryAnchorStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl AnchorStore for InMemoryAnchorStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, HistoryError> {
        Ok(self.inner.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), HistoryError> {
        self.inner.lock().await.insert(key.to_owned(), value);
        Ok(())
    }
}

/// SQLite-backed [`AnchorStore`]. Opens (or creates) a 2-column kv table
/// at the given path. Owns its own DB file — independent of `LogStore`.
pub struct SqliteAnchorStore {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
}

impl SqliteAnchorStore {
    pub fn open(path: &str) -> Result<Self, HistoryError> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| HistoryError::internal(format!("sqlite open: {e}")))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS matcher_anchors (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            "#,
        )
        .map_err(|e| HistoryError::internal(format!("sqlite create: {e}")))?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }
}

#[async_trait::async_trait]
impl AnchorStore for SqliteAnchorStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, HistoryError> {
        let key = key.to_owned();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>, HistoryError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare_cached("SELECT value FROM matcher_anchors WHERE key = ?1")
                .map_err(|e| HistoryError::internal(format!("sqlite prep: {e}")))?;
            use rusqlite::OptionalExtension;
            let row = stmt
                .query_row([&key], |r| r.get::<_, Vec<u8>>(0))
                .optional()
                .map_err(|e| HistoryError::internal(format!("sqlite query: {e}")))?;
            Ok(row)
        })
        .await
        .map_err(|e| HistoryError::internal(format!("blocking join: {e}")))?
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), HistoryError> {
        let key = key.to_owned();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(), HistoryError> {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT INTO matcher_anchors (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![&key, &value],
            )
            .map_err(|e| HistoryError::internal(format!("sqlite insert: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| HistoryError::internal(format!("blocking join: {e}")))?
    }
}

// ============================================================
// AnchoredMatcher
// ============================================================

/// [`BaseMatcher`] + lazy-loaded persisted anchors + advance-and-persist
/// convenience.
pub struct AnchoredMatcher<Src, Dst, Output, SP, DP>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Clone + Send + Sync + 'static,
    Output: Send + 'static,
    SP: Position,
    DP: Position,
{
    base: BaseMatcher<Src, Dst, Output, SP, DP>,
    anchor_store: Arc<dyn AnchorStore>,
    anchor_key: String,
    start: Timestamp,
    cached: tokio::sync::Mutex<Option<MatcherAnchors<SP, DP>>>,
}

impl<Src, Dst, Output, SP, DP> AnchoredMatcher<Src, Dst, Output, SP, DP>
where
    Src: Clone + Send + Sync + 'static,
    Dst: Clone + Send + Sync + 'static,
    Output: Clone + Send + 'static,
    SP: Position,
    DP: Position,
{
    pub fn new(
        src_store: HistStore<Src, SP>,
        dst_store: HistStore<Dst, DP>,
        match_fn: Arc<dyn MatchFn<Src, Dst, Output, SP, DP>>,
        anchor_store: Arc<dyn AnchorStore>,
        anchor_key: impl Into<String>,
        start: Timestamp,
    ) -> Self {
        Self {
            base: BaseMatcher::new(src_store, dst_store, match_fn),
            anchor_store,
            anchor_key: anchor_key.into(),
            start,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// Direct access to the underlying [`BaseMatcher`] — useful for tests
    /// or for callers that want pure compute without touching persistence.
    pub fn base(&self) -> &BaseMatcher<Src, Dst, Output, SP, DP> {
        &self.base
    }

    /// Anchor cache key (caller-supplied at construction).
    pub fn anchor_key(&self) -> &str {
        &self.anchor_key
    }

    /// Lazily load persisted anchors; falls back to `time_anchors(start)`
    /// on cache miss. Subsequent calls return the in-memory copy mutated
    /// by [`set_anchors`](Self::set_anchors).
    pub async fn get_anchors(&self) -> Result<MatcherAnchors<SP, DP>, HistoryError> {
        let mut cached = self.cached.lock().await;
        if let Some(a) = cached.as_ref() {
            return Ok(a.clone());
        }
        let loaded = self.load_anchors().await?;
        *cached = Some(loaded.clone());
        Ok(loaded)
    }

    async fn load_anchors(&self) -> Result<MatcherAnchors<SP, DP>, HistoryError> {
        match self.anchor_store.get(&self.anchor_key).await? {
            Some(bytes) => MatcherAnchors::<SP, DP>::unmarshal(&bytes)
                .ok_or_else(|| HistoryError::internal("failed to unmarshal anchors")),
            None => self.base.time_anchors(self.start).await,
        }
    }

    /// Persist `next` and update the in-memory cache.
    pub async fn set_anchors(&self, next: MatcherAnchors<SP, DP>) -> Result<(), HistoryError> {
        let bytes = next.marshal();
        self.anchor_store.set(&self.anchor_key, bytes).await?;
        *self.cached.lock().await = Some(next);
        Ok(())
    }

    /// Compute state at the cached anchors.
    pub async fn current_state(
        &self,
    ) -> Result<BaseMatcherState<Src, Dst, Output, SP, DP>, HistoryError> {
        let anchors = self.get_anchors().await?;
        self.base.compute_state(anchors).await
    }

    /// Stream of states, one per `changes()` tick. Each yield re-uses the
    /// cached anchors; consumers advance via [`set_anchors`](Self::set_anchors).
    pub fn iter(self: &Arc<Self>) -> MatcherStateStream<Src, Dst, Output, SP, DP> {
        let matcher = self.clone();
        let sub = self.base.changes().subscribe();
        Box::pin(futures_util::stream::unfold(
            (matcher, sub),
            |(matcher, mut sub)| async move {
                // Wait for a change tick (first tick is the current value,
                // delivered immediately).
                sub.next().await?;
                let state = matcher.current_state().await;
                Some((state, (matcher, sub)))
            },
        ))
    }
}

// ============================================================
// Matcher — default-output alias
// ============================================================

/// Default-output [`AnchoredMatcher`], with output set to
/// [`MatchOutput<Src, Dst>`].
pub type Matcher<Src, Dst, SP, DP> = AnchoredMatcher<Src, Dst, MatchOutput<Src, Dst>, SP, DP>;

/// Stream type returned by [`AnchoredMatcher::iter`]. Extracted as a type
/// alias to keep the method signature readable.
pub type MatcherStateStream<Src, Dst, Output, SP, DP> = Pin<
    Box<dyn Stream<Item = Result<BaseMatcherState<Src, Dst, Output, SP, DP>, HistoryError>> + Send>,
>;

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::{
        AsyncDisposable, HistBackend, HistStore, HistStoreBackend, HistStream, ItemStream,
    };
    use futures_core::Stream;
    use std::pin::Pin as StdPin;
    use std::sync::Mutex as StdMutex;

    // -- Mock source (same-shape across src/dst, parameterised by P) --

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

    /// Each item carries an integer "bridge_id" — that's what the bridge
    /// MatchFn correlates on.
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

    struct MockSource {
        items: StdMutex<Vec<Ev>>,
        latest_store: Store<MockPos>,
        latest_tx: tokio::sync::watch::Sender<MockPos>,
        best_store: Store<MockPos>,
        safe_store: Store<MockPos>,
        self_ref: std::sync::OnceLock<std::sync::Weak<Self>>,
    }

    impl MockSource {
        fn new(initial: Vec<Ev>) -> Arc<Self> {
            let tip = MockPos(initial.len() as u64);
            let (tx, rx) = tokio::sync::watch::channel(tip);
            let (_, b_rx) = tokio::sync::watch::channel(tip);
            let (_, s_rx) = tokio::sync::watch::channel(tip);
            let s = Arc::new(Self {
                items: StdMutex::new(initial),
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
            Box::pin(futures_util::stream::iter(yielded.into_iter().map(Ok)))
        }
    }

    #[async_trait::async_trait]
    impl HistStoreBackend<Ev, MockPos> for MockSource {
        fn snapshots(&self) -> HistStream<Ev, MockPos> {
            let backend: Arc<dyn HistBackend<Ev, MockPos>> = self.arc_self();
            let mut sub = self.latest_store.subscribe();
            Box::pin(futures_util::stream::poll_fn(move |cx| {
                StdPin::new(&mut sub)
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
            let cancel = tokio_util::sync::CancellationToken::new();
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

    /// Simple bridge_id-based matcher: items with the same bridge_id pair.
    /// Pending = src items without dst match. Orphans = dst items without
    /// src match.
    fn bridge_match_fn() -> Arc<dyn MatchFn<Ev, Ev, MatchOutput<Ev, Ev>, MockPos, MockPos>> {
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

    #[tokio::test]
    async fn compute_state_is_pure() {
        let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
        let dst = MockSource::new(vec![ev(1, "mint1")]);
        let base = BaseMatcher::new(src.hist_store(), dst.hist_store(), bridge_match_fn());
        let anchors = MatcherAnchors::new(MockPos(0), MockPos(0));

        let s1 = base.compute_state(anchors.clone()).await.unwrap();
        let s2 = base.compute_state(anchors).await.unwrap();
        assert_eq!(s1.output.matched.len(), s2.output.matched.len());
        assert_eq!(s1.output.pending.len(), s2.output.pending.len());
        assert_eq!(s1.output.orphans.len(), s2.output.orphans.len());

        // Specifically: matched [burn1↔mint1], pending [burn2], no orphans.
        assert_eq!(s1.output.matched.len(), 1);
        assert_eq!(s1.output.matched[0].src.label, "burn1");
        assert_eq!(s1.output.matched[0].dst.label, "mint1");
        assert_eq!(s1.output.pending.len(), 1);
        assert_eq!(s1.output.pending[0].label, "burn2");
        assert_eq!(s1.output.orphans.len(), 0);
    }

    #[tokio::test]
    async fn advance_anchors_removes_matched_from_output() {
        let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
        let dst = MockSource::new(vec![ev(1, "mint1"), ev(2, "mint2")]);
        let base = BaseMatcher::new(src.hist_store(), dst.hist_store(), bridge_match_fn());

        // First compute: both pairs matched.
        let s1 = base
            .compute_state(MatcherAnchors::new(MockPos(0), MockPos(0)))
            .await
            .unwrap();
        assert_eq!(s1.output.matched.len(), 2);

        // Advance both anchors past the first pair (src @ 1, dst @ 1).
        let s2 = base
            .compute_state(MatcherAnchors::new(MockPos(1), MockPos(1)))
            .await
            .unwrap();
        // burn1↔mint1 should be GONE entirely — no longer matched, no
        // longer pending, no longer orphaned.
        assert_eq!(s2.output.matched.len(), 1);
        assert_eq!(s2.output.matched[0].src.label, "burn2");
        assert_eq!(s2.output.pending.len(), 0);
        assert_eq!(s2.output.orphans.len(), 0);
    }

    #[tokio::test]
    async fn asymmetric_anchor_advance_demotes_match_to_orphan() {
        let src = MockSource::new(vec![ev(1, "burn1"), ev(2, "burn2")]);
        let dst = MockSource::new(vec![ev(1, "mint1"), ev(2, "mint2")]);
        let base = BaseMatcher::new(src.hist_store(), dst.hist_store(), bridge_match_fn());

        // Advance ONLY src past burn1 (keep dst at 0).
        let s = base
            .compute_state(MatcherAnchors::new(MockPos(1), MockPos(0)))
            .await
            .unwrap();
        // burn1 is gone but mint1 still in dst tail → mint1 surfaces as orphan.
        assert_eq!(s.output.matched.len(), 1);
        assert_eq!(s.output.matched[0].src.label, "burn2");
        assert_eq!(s.output.pending.len(), 0);
        assert_eq!(s.output.orphans.len(), 1);
        assert_eq!(s.output.orphans[0].label, "mint1");
    }

    #[tokio::test]
    async fn get_history_uses_time_anchors_not_persisted() {
        // Persist an anchor pair manually that would skip everything;
        // verify get_history bypasses it.
        let src = MockSource::new(vec![ev(1, "burn1")]);
        let dst = MockSource::new(vec![ev(1, "mint1")]);
        let store = Arc::new(InMemoryAnchorStore::new());
        let key = "test-key";
        // Persist anchors that point past all items.
        store
            .set(key, MatcherAnchors::new(MockPos(99), MockPos(99)).marshal())
            .await
            .unwrap();

        let m = AnchoredMatcher::new(
            src.hist_store(),
            dst.hist_store(),
            bridge_match_fn(),
            store.clone() as Arc<dyn AnchorStore>,
            key,
            0,
        );

        // current_state respects the persisted anchors → empty output.
        let cs = m.current_state().await.unwrap();
        assert_eq!(cs.output.matched.len(), 0);
        assert_eq!(cs.output.pending.len(), 0);

        // get_history(0) ignores persistence → sees all items.
        let hs = m.base().get_history(0).await.unwrap();
        assert_eq!(hs.output.matched.len(), 1);
    }

    #[tokio::test]
    async fn in_memory_anchor_store_round_trip() {
        let store = InMemoryAnchorStore::new();
        assert_eq!(store.get("k").await.unwrap(), None);
        store.set("k", vec![1, 2, 3]).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(vec![1, 2, 3]));
        store.set("k", vec![4, 5]).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(vec![4, 5]));
    }

    #[tokio::test]
    async fn matcher_anchors_marshal_round_trip() {
        let a = MatcherAnchors::new(MockPos(42), MockPos(7));
        let bytes = a.marshal();
        let restored: MatcherAnchors<MockPos, MockPos> =
            MatcherAnchors::unmarshal(&bytes).expect("unmarshal");
        assert_eq!(restored.src, MockPos(42));
        assert_eq!(restored.dst, MockPos(7));
    }

    #[tokio::test]
    async fn anchored_matcher_set_get_cached() {
        let src = MockSource::new(vec![ev(1, "burn1")]);
        let dst = MockSource::new(vec![ev(1, "mint1")]);
        let store = Arc::new(InMemoryAnchorStore::new());
        let m = AnchoredMatcher::new(
            src.hist_store(),
            dst.hist_store(),
            bridge_match_fn(),
            store.clone() as Arc<dyn AnchorStore>,
            "k",
            0,
        );

        // First load: from store (empty) → time_anchors(0) = (default, default).
        let a1 = m.get_anchors().await.unwrap();
        assert_eq!(
            a1,
            MatcherAnchors::new(MockPos::default(), MockPos::default())
        );

        // Set a new pair, verify get returns the updated value.
        let next = MatcherAnchors::new(MockPos(7), MockPos(3));
        m.set_anchors(next.clone()).await.unwrap();
        let a2 = m.get_anchors().await.unwrap();
        assert_eq!(a2, next);

        // Persistence written.
        let bytes = store.get("k").await.unwrap().expect("persisted");
        let parsed: MatcherAnchors<MockPos, MockPos> = MatcherAnchors::unmarshal(&bytes).unwrap();
        assert_eq!(parsed, next);
    }

    #[tokio::test]
    async fn changes_fires_on_either_source_advance() {
        let src = MockSource::new(vec![]);
        let dst = MockSource::new(vec![]);
        let base = BaseMatcher::new(src.hist_store(), dst.hist_store(), bridge_match_fn());
        let mut sub = base.changes().subscribe();

        // First yield is the current value (immediate).
        sub.next().await.expect("initial");

        // Advance src — changes should fire.
        src.append(ev(1, "burn1"));
        tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
            .await
            .expect("src change within timeout")
            .expect("subscription not closed");

        // Advance dst — changes should fire too.
        dst.append(ev(1, "mint1"));
        tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
            .await
            .expect("dst change within timeout")
            .expect("subscription not closed");
    }

    #[tokio::test]
    async fn iter_yields_state_on_each_change() {
        let src = MockSource::new(vec![]);
        let dst = MockSource::new(vec![]);
        let store = Arc::new(InMemoryAnchorStore::new());
        let m: Arc<Matcher<Ev, Ev, MockPos, MockPos>> = Arc::new(AnchoredMatcher::new(
            src.hist_store(),
            dst.hist_store(),
            bridge_match_fn(),
            store as Arc<dyn AnchorStore>,
            "k",
            0,
        ));

        let mut stream = m.iter();
        // Initial state (changes() emits immediately) → empty.
        let s0 = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("initial state within timeout")
            .expect("stream not closed")
            .unwrap();
        assert_eq!(s0.output.matched.len(), 0);

        // Append a src item — iter should yield a new state.
        src.append(ev(1, "burn1"));
        let s1 = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("post-append state within timeout")
            .expect("stream not closed")
            .unwrap();
        assert_eq!(s1.output.matched.len(), 0);
        assert_eq!(s1.output.pending.len(), 1);
    }
}
