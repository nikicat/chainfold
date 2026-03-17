//! ReducedHistory — wraps an inner history, applying a [`Reducer`] to
//! accumulate state alongside each event.
//!
//! The output is `History<ReducedEvent<E, S>, P>` where each cursor carries
//! both the original event and the state after applying that event.

use std::cmp::Ordering;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use futures_util::StreamExt;

use crate::arch::{
    Cursor, History, HistoryBackend, HistoryError, Position, SearchEvent, SearchStream, Store,
    Timestamp,
};
use crate::provider::Reducer;

// ============================================================
// ReducedEvent
// ============================================================

/// An event paired with the reducer state after applying it.
#[derive(Debug, Clone)]
pub struct ReducedEvent<E, S> {
    pub event: E,
    /// State AFTER applying this event.
    pub state: S,
}

// ============================================================
// ReducedHistory
// ============================================================

/// Combinator: wraps an inner `History<E, P>` and applies a `Reducer` to
/// produce `History<ReducedEvent<E, S>, P>`.
///
/// State resolution uses an in-memory cache of `(position, state)` pairs.
/// Cache misses fall back to `reducer.fetch_state()` (e.g. RPC call).
pub struct ReducedHistory<R: Reducer<P>, P: Position> {
    inner: History<R::Event, P>,
    reducer: R,
    pos_cmp: fn(&P, &P) -> Ordering,
    /// Sorted vec of (position, state_after_event). Floor lookup via pos_cmp.
    state_cache: Mutex<Vec<(P, R::State)>>,
    /// Cost per eth_getLogs RPC call (provider config).
    scan_cost: u64,
    /// Cost per eth_call / state fetch RPC (provider config).
    fetch_cost: u64,
    self_ref: OnceLock<Weak<Self>>,
}

impl<R: Reducer<P>, P: Position> ReducedHistory<R, P> {
    /// Create a new ReducedHistory wrapping `inner` with the given reducer.
    ///
    /// `scan_cost` / `fetch_cost` control the cost-based query planner in
    /// `resolve_state`. Typical: free RPC `(1, 1)`, premium `(5, 1)`.
    pub fn new(
        inner: History<R::Event, P>,
        reducer: R,
        pos_cmp: fn(&P, &P) -> Ordering,
        scan_cost: u64,
        fetch_cost: u64,
    ) -> Arc<Self> {
        let rh = Arc::new(Self {
            inner,
            reducer,
            pos_cmp,
            state_cache: Mutex::new(Vec::new()),
            scan_cost,
            fetch_cost,
            self_ref: OnceLock::new(),
        });
        let _ = rh.self_ref.set(Arc::downgrade(&rh));
        rh
    }

    /// Create a `History` handle from this backend.
    pub fn history(self: &Arc<Self>) -> History<ReducedEvent<R::Event, R::State>, P> {
        History::new(self.clone() as Arc<dyn HistoryBackend<ReducedEvent<R::Event, R::State>, P>>)
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref
            .get()
            .expect("self_ref not initialized")
            .upgrade()
            .expect("ReducedHistory dropped while still in use")
    }

    // --------------------------------------------------------
    // State cache operations
    // --------------------------------------------------------

    /// Insert a (position, state) pair into the sorted cache.
    fn cache_state(&self, pos: P, state: R::State) {
        let mut cache = self.state_cache.lock().unwrap();
        let idx = cache
            .binary_search_by(|(p, _)| (self.pos_cmp)(p, &pos))
            .unwrap_or_else(|i| i);
        // Avoid duplicates at same position
        if idx < cache.len() && (self.pos_cmp)(&cache[idx].0, &pos) == Ordering::Equal {
            cache[idx].1 = state;
        } else {
            cache.insert(idx, (pos, state));
        }
    }

    /// Floor lookup: find the closest cached state at or before `pos`.
    fn floor_state(&self, pos: &P) -> Option<(P, R::State)> {
        let cache = self.state_cache.lock().unwrap();
        // Find rightmost entry where entry.pos <= pos
        let idx = cache
            .binary_search_by(|(p, _)| (self.pos_cmp)(p, pos))
            .unwrap_or_else(|i| i);
        // binary_search returns the insertion point; we want the entry before it
        // (or at it if exact match)
        if idx < cache.len() && (self.pos_cmp)(&cache[idx].0, pos) == Ordering::Equal {
            Some(cache[idx].clone())
        } else if idx > 0 {
            Some(cache[idx - 1].clone())
        } else {
            None
        }
    }

    // --------------------------------------------------------
    // State resolution
    // --------------------------------------------------------

    /// Replay forward from `floor_state` at `floor_pos` to `target`,
    /// caching intermediate states along the way.
    async fn replay_forward(
        &self,
        floor_state: R::State,
        floor_pos: &P,
        target: &P,
    ) -> Result<R::State, HistoryError> {
        let mut state = floor_state;
        let mut cursor = self.inner.after(floor_pos, Some(target)).await?;
        while let Some(c) = cursor {
            state = self.reducer.apply(state, c.value());
            let event_pos = c.after().clone();
            self.cache_state(event_pos.clone(), state.clone());
            cursor = self.inner.after(c.after(), Some(target)).await?;
        }
        Ok(state)
    }

    /// Resolve state at `pos` using cost-based query planning.
    ///
    /// Checks three sources for the closest floor state (in-memory cache,
    /// reducer's external cache, earliest position), then compares the cost
    /// of replaying from the floor vs. fetching state one chunk back.
    async fn resolve_state(&self, pos: &P) -> Result<R::State, HistoryError> {
        // 1. Find closest known state from all sources
        let mut best_floor: Option<(P, R::State)> = None;

        // Check in-memory cache
        if let Some((p, s)) = self.floor_state(pos) {
            if (self.pos_cmp)(&p, pos) == Ordering::Equal {
                return Ok(s); // exact hit
            }
            best_floor = Some((p, s));
        }

        // Check Reducer's external cache (e.g. SQLite)
        if let Some((p, s)) = self.reducer.find_cached_state_floor(pos) {
            let dominated = best_floor
                .as_ref()
                .is_some_and(|(bp, _)| (self.pos_cmp)(bp, &p) != Ordering::Less);
            if !dominated {
                best_floor = Some((p, s));
            }
        }

        // 2. If we have a floor, compare costs
        if let Some((floor_pos, floor_state)) = best_floor {
            let scan_count = self.inner.estimate_scan_count(&floor_pos, pos);
            let restore_cost = self.scan_cost * scan_count;
            let fetch_and_scan = self.fetch_cost + self.scan_cost;

            if restore_cost <= fetch_and_scan {
                return self.replay_forward(floor_state, &floor_pos, pos).await;
            }
        }

        // 3. Fallback: fetch state one chunk back, scan that range
        let checkpoint = self.inner.scan_chunk_before(pos);
        let cp_state = self.reducer.fetch_state(&checkpoint).await?;
        self.cache_state(checkpoint.clone(), cp_state.clone());
        self.replay_forward(cp_state, &checkpoint, pos).await
    }

    /// Resolve state at the position of a found event. The event itself
    /// has already been located by the inner history; we need the state
    /// *before* the event, then apply to get state *after*.
    async fn resolve_and_apply(
        &self,
        event: &R::Event,
        event_before: &P,
    ) -> Result<R::State, HistoryError> {
        // Get state just before this event
        let state_before = self.resolve_state(event_before).await?;
        // Apply to get state after
        let state_after = self.reducer.apply(state_before, event);
        Ok(state_after)
    }
}

#[async_trait::async_trait]
impl<R: Reducer<P>, P: Position> HistoryBackend<ReducedEvent<R::Event, R::State>, P>
    for ReducedHistory<R, P>
{
    fn source_id(&self) -> u64 {
        self.inner.source_id()
    }

    fn earliest(&self) -> P {
        self.inner.earliest()
    }

    fn never(&self) -> P {
        self.inner.never()
    }

    fn latest(&self) -> &Store<P> {
        self.inner.latest()
    }

    fn best(&self) -> &Store<P> {
        self.inner.best()
    }

    fn safe(&self) -> &Store<P> {
        self.inner.safe()
    }

    fn at(&self, time: Timestamp) -> P {
        self.inner.at(time)
    }

    async fn is_equiv(&self, a: &P, b: &P) -> Result<bool, HistoryError> {
        self.inner.is_equiv(a, b).await
    }

    async fn lca(&self, a: &P, b: &P) -> Result<P, HistoryError> {
        self.inner.lca(a, b).await
    }

    fn search_after(&self, position: &P, cutoff: Option<&P>) -> SearchStream<ReducedEvent<R::Event, R::State>, P> {
        let pos = position.clone();
        let cut = cutoff.cloned();
        let this = self.arc_self();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            // Resolve initial state at the search start position
            let mut state = match this.resolve_state(&pos).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            // Stream inner search events
            let mut stream = this.inner.search_after(&pos, cut.as_ref());
            while let Some(event) = stream.next().await {
                let msg = match event {
                    Ok(SearchEvent::Scanned(p)) => Ok(SearchEvent::Scanned(p)),
                    Ok(SearchEvent::Found(cursor)) => {
                        state = this.reducer.apply(state, cursor.value());
                        let reduced = ReducedEvent {
                            event: cursor.value().clone(),
                            state: state.clone(),
                        };
                        let backend = this.clone()
                            as Arc<dyn HistoryBackend<ReducedEvent<R::Event, R::State>, P>>;
                        let new_cursor = Cursor::new(
                            reduced,
                            cursor.before().clone(),
                            cursor.after().clone(),
                            backend,
                        );
                        this.cache_state(cursor.after().clone(), state.clone());
                        Ok(SearchEvent::Found(new_cursor))
                    }
                    Err(e) => Err(e),
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
            }
        });

        Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    fn search_before(&self, position: &P, cutoff: Option<&P>) -> SearchStream<ReducedEvent<R::Event, R::State>, P> {
        let pos = position.clone();
        let cut = cutoff.cloned();
        let this = self.arc_self();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let mut stream = this.inner.search_before(&pos, cut.as_ref());
            while let Some(event) = stream.next().await {
                let msg = match event {
                    Ok(SearchEvent::Scanned(p)) => Ok(SearchEvent::Scanned(p)),
                    Ok(SearchEvent::Found(cursor)) => {
                        // Resolve state at the found event's position
                        match this.resolve_and_apply(cursor.value(), cursor.before()).await {
                            Ok(state_after) => {
                                this.cache_state(cursor.after().clone(), state_after.clone());
                                let reduced = ReducedEvent {
                                    event: cursor.value().clone(),
                                    state: state_after,
                                };
                                let backend = this.clone()
                                    as Arc<dyn HistoryBackend<ReducedEvent<R::Event, R::State>, P>>;
                                let new_cursor = Cursor::new(
                                    reduced,
                                    cursor.before().clone(),
                                    cursor.after().clone(),
                                    backend,
                                );
                                Ok(SearchEvent::Found(new_cursor))
                            }
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(e),
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
            }
        });

        Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    async fn after(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<ReducedEvent<R::Event, R::State>, P>>, HistoryError> {
        // Find event in inner history
        let inner_cursor = match self.inner.after(position, cutoff).await? {
            Some(c) => c,
            None => return Ok(None),
        };

        // Resolve state: get state at position (before event), then apply
        let state_before = self.resolve_state(position).await?;
        let state_after = self.reducer.apply(state_before, inner_cursor.value());
        self.cache_state(inner_cursor.after().clone(), state_after.clone());

        let reduced = ReducedEvent {
            event: inner_cursor.value().clone(),
            state: state_after,
        };
        let backend =
            self.arc_self() as Arc<dyn HistoryBackend<ReducedEvent<R::Event, R::State>, P>>;
        Ok(Some(Cursor::new(
            reduced,
            inner_cursor.before().clone(),
            inner_cursor.after().clone(),
            backend,
        )))
    }

    async fn before(
        &self,
        position: &P,
        cutoff: Option<&P>,
    ) -> Result<Option<Cursor<ReducedEvent<R::Event, R::State>, P>>, HistoryError> {
        let inner_cursor = match self.inner.before(position, cutoff).await? {
            Some(c) => c,
            None => return Ok(None),
        };

        let state_after =
            self.resolve_and_apply(inner_cursor.value(), inner_cursor.before()).await?;
        self.cache_state(inner_cursor.after().clone(), state_after.clone());

        let reduced = ReducedEvent {
            event: inner_cursor.value().clone(),
            state: state_after,
        };
        let backend =
            self.arc_self() as Arc<dyn HistoryBackend<ReducedEvent<R::Event, R::State>, P>>;
        Ok(Some(Cursor::new(
            reduced,
            inner_cursor.before().clone(),
            inner_cursor.after().clone(),
            backend,
        )))
    }

    async fn is_after(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError> {
        Ok((self.pos_cmp)(cursor_pos, p) == Ordering::Greater)
    }

    async fn is_before(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError> {
        Ok((self.pos_cmp)(cursor_pos, p) == Ordering::Less)
    }

    fn estimate_scan_count(&self, from: &P, to: &P) -> u64 {
        self.inner.estimate_scan_count(from, to)
    }

    fn scan_chunk_before(&self, target: &P) -> P {
        self.inner.scan_chunk_before(target)
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::evm::EvmPosition;
    use crate::arch::Side;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn evm_cmp(a: &EvmPosition, b: &EvmPosition) -> Ordering {
        (a.block_number, a.log_index).cmp(&(b.block_number, b.log_index))
    }

    const TEST_CHAIN: u64 = 1;

    // --------------------------------------------------------
    // FakeHistory — minimal in-memory HistoryBackend for tests
    // --------------------------------------------------------

    struct FakeHistory {
        chain_id: u64,
        events: Vec<(u64, u32, u64)>, // (block_number, log_index, value)
        latest_store: Store<EvmPosition>,
        best_store: Store<EvmPosition>,
        safe_store: Store<EvmPosition>,
        self_ref: OnceLock<Weak<Self>>,
    }

    impl FakeHistory {
        fn new(events: Vec<(u64, u32, u64)>) -> Arc<Self> {
            let tip = EvmPosition::never(TEST_CHAIN);
            let (_, lrx) = tokio::sync::watch::channel(tip.clone());
            let (_, brx) = tokio::sync::watch::channel(tip.clone());
            let (_, srx) = tokio::sync::watch::channel(tip);
            let h = Arc::new(Self {
                chain_id: TEST_CHAIN,
                events,
                latest_store: Store::new(lrx),
                best_store: Store::new(brx),
                safe_store: Store::new(srx),
                self_ref: OnceLock::new(),
            });
            let _ = h.self_ref.set(Arc::downgrade(&h));
            h
        }

        fn into_history(self: Arc<Self>) -> History<u64, EvmPosition> {
            History::new(self as Arc<dyn HistoryBackend<u64, EvmPosition>>)
        }

        fn arc_self(&self) -> Arc<Self> {
            self.self_ref.get().unwrap().upgrade().unwrap()
        }

        fn make_cursor(&self, block: u64, idx: u32, value: u64) -> Cursor<u64, EvmPosition> {
            let before = EvmPosition {
                chain_id: self.chain_id,
                block_number: block,
                log_index: idx,
                block_hash: [0; 32],
                timestamp: block,
                side: Side::Before,
            };
            let after = EvmPosition {
                chain_id: self.chain_id,
                block_number: block,
                log_index: idx,
                block_hash: [0; 32],
                timestamp: block,
                side: Side::After,
            };
            Cursor::new(
                value,
                before,
                after,
                self.arc_self() as Arc<dyn HistoryBackend<u64, EvmPosition>>,
            )
        }
    }

    #[async_trait::async_trait]
    impl HistoryBackend<u64, EvmPosition> for FakeHistory {
        fn source_id(&self) -> u64 {
            self.chain_id
        }
        fn earliest(&self) -> EvmPosition {
            EvmPosition::earliest(self.chain_id)
        }
        fn never(&self) -> EvmPosition {
            EvmPosition::never(self.chain_id)
        }
        fn latest(&self) -> &Store<EvmPosition> {
            &self.latest_store
        }
        fn best(&self) -> &Store<EvmPosition> {
            &self.best_store
        }
        fn safe(&self) -> &Store<EvmPosition> {
            &self.safe_store
        }
        fn at(&self, time: Timestamp) -> EvmPosition {
            EvmPosition {
                chain_id: self.chain_id,
                block_number: time,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: time,
                side: Side::Before,
            }
        }
        async fn is_equiv(
            &self,
            a: &EvmPosition,
            b: &EvmPosition,
        ) -> Result<bool, HistoryError> {
            Ok(a.block_number == b.block_number
                && a.log_index == b.log_index
                && a.side == b.side)
        }
        async fn lca(
            &self,
            a: &EvmPosition,
            b: &EvmPosition,
        ) -> Result<EvmPosition, HistoryError> {
            if (a.block_number, a.log_index) <= (b.block_number, b.log_index) {
                Ok(a.clone())
            } else {
                Ok(b.clone())
            }
        }

        fn search_after(
            &self,
            position: &EvmPosition,
            cutoff: Option<&EvmPosition>,
        ) -> SearchStream<u64, EvmPosition> {
            let pos = position.clone();
            let cut = cutoff.cloned();
            let this = self.arc_self();
            Box::pin(futures_util::stream::unfold(
                Some((this, pos, cut)),
                |state| async move {
                    let (this, pos, cut) = state?;
                    match this.after(&pos, cut.as_ref()).await {
                        Ok(Some(cursor)) => Some((Ok(SearchEvent::Found(cursor)), None)),
                        Ok(None) => None,
                        Err(e) => Some((Err(e), None)),
                    }
                },
            ))
        }

        fn search_before(
            &self,
            position: &EvmPosition,
            cutoff: Option<&EvmPosition>,
        ) -> SearchStream<u64, EvmPosition> {
            let pos = position.clone();
            let cut = cutoff.cloned();
            let this = self.arc_self();
            Box::pin(futures_util::stream::unfold(
                Some((this, pos, cut)),
                |state| async move {
                    let (this, pos, cut) = state?;
                    match this.before(&pos, cut.as_ref()).await {
                        Ok(Some(cursor)) => Some((Ok(SearchEvent::Found(cursor)), None)),
                        Ok(None) => None,
                        Err(e) => Some((Err(e), None)),
                    }
                },
            ))
        }

        async fn after(
            &self,
            position: &EvmPosition,
            cutoff: Option<&EvmPosition>,
        ) -> Result<Option<Cursor<u64, EvmPosition>>, HistoryError> {
            let pos = (position.block_number, position.log_index);
            for &(b, i, v) in &self.events {
                let matches = match position.side {
                    Side::Before => (b, i) >= pos,
                    Side::After => (b, i) > pos,
                };
                if matches {
                    if let Some(c) = cutoff {
                        if (b, i) > (c.block_number, c.log_index) {
                            return Ok(None);
                        }
                    }
                    return Ok(Some(self.make_cursor(b, i, v)));
                }
            }
            Ok(None)
        }

        async fn before(
            &self,
            position: &EvmPosition,
            cutoff: Option<&EvmPosition>,
        ) -> Result<Option<Cursor<u64, EvmPosition>>, HistoryError> {
            let pos = (position.block_number, position.log_index);
            for &(b, i, v) in self.events.iter().rev() {
                let matches = match position.side {
                    Side::After => (b, i) <= pos,
                    Side::Before => (b, i) < pos,
                };
                if matches {
                    if let Some(c) = cutoff {
                        if (b, i) < (c.block_number, c.log_index) {
                            return Ok(None);
                        }
                    }
                    return Ok(Some(self.make_cursor(b, i, v)));
                }
            }
            Ok(None)
        }

        async fn is_after(
            &self,
            cursor_pos: &EvmPosition,
            p: &EvmPosition,
        ) -> Result<bool, HistoryError> {
            Ok((cursor_pos.block_number, cursor_pos.log_index)
                > (p.block_number, p.log_index))
        }

        async fn is_before(
            &self,
            cursor_pos: &EvmPosition,
            p: &EvmPosition,
        ) -> Result<bool, HistoryError> {
            Ok((cursor_pos.block_number, cursor_pos.log_index)
                < (p.block_number, p.log_index))
        }
    }

    // --------------------------------------------------------
    // SumReducer — mock reducer for tests
    // --------------------------------------------------------

    struct SumReducer {
        /// Fixed values returned by fetch_state, keyed by block_number.
        fetch_values: HashMap<u64, u64>,
        fetch_count: AtomicUsize,
    }

    impl SumReducer {
        fn new(fetch_values: HashMap<u64, u64>) -> Self {
            Self {
                fetch_values,
                fetch_count: AtomicUsize::new(0),
            }
        }

        fn fetch_count(&self) -> usize {
            self.fetch_count.load(AtomicOrdering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Reducer<EvmPosition> for SumReducer {
        type Event = u64;
        type State = u64;

        fn apply(&self, state: u64, event: &u64) -> u64 {
            state + event
        }

        async fn fetch_state(&self, pos: &EvmPosition) -> Result<u64, HistoryError> {
            self.fetch_count.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(*self.fetch_values.get(&pos.block_number).unwrap_or(&0))
        }
    }

    fn cutoff(block: u64) -> EvmPosition {
        EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: block,
            log_index: u32::MAX,
            block_hash: [0; 32],
            timestamp: 0,
            side: Side::After,
        }
    }

    // --------------------------------------------------------
    // Tests
    // --------------------------------------------------------

    #[tokio::test]
    async fn forward_search_with_cached_state() {
        // Pre-cache state at block 100, then search_after from 100
        let h = FakeHistory::new(vec![(100, 0, 10), (200, 0, 20)]);
        let history = h.into_history();

        let reducer = SumReducer::new(HashMap::new());
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        // Pre-cache state at block 100 (state = 50)
        let pos_100 = EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: 100,
            log_index: 0,
            block_hash: [0; 32],
            timestamp: 100,
            side: Side::After,
        };
        rh.cache_state(pos_100, 50);

        // Search after block 100 — should find event at 200 with state = 50 + 20 = 70
        let cut = cutoff(300);
        let c = rh.after(&EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: 100,
            log_index: 0,
            block_hash: [0; 32],
            timestamp: 100,
            side: Side::After,
        }, Some(&cut)).await.unwrap().unwrap();

        assert_eq!(c.value().event, 20);
        assert_eq!(c.value().state, 70); // 50 + 20
        assert_eq!(rh.reducer.fetch_count(), 0); // no RPC needed
    }

    #[tokio::test]
    async fn forward_search_with_fetch_fallback() {
        // No cached state — should call fetch_state
        let h = FakeHistory::new(vec![(100, 0, 10), (200, 0, 20)]);
        let history = h.into_history();

        let mut fetch_vals = HashMap::new();
        fetch_vals.insert(0, 100); // fetch at earliest returns 100
        let reducer = SumReducer::new(fetch_vals);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        let cut = cutoff(300);
        let start = EvmPosition::earliest(TEST_CHAIN);
        let c = rh.after(&start, Some(&cut)).await.unwrap().unwrap();

        assert_eq!(c.value().event, 10);
        assert_eq!(c.value().state, 110); // 100 (fetched) + 10
        assert_eq!(rh.reducer.fetch_count(), 1);
    }

    #[tokio::test]
    async fn sequential_forward_iteration() {
        let h = FakeHistory::new(vec![(100, 0, 10), (200, 0, 20), (300, 0, 30)]);
        let history = h.into_history();

        let mut fetch_vals = HashMap::new();
        fetch_vals.insert(0, 0);
        let reducer = SumReducer::new(fetch_vals);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        let cut = cutoff(400);
        let start = EvmPosition::earliest(TEST_CHAIN);

        let c0 = rh.after(&start, Some(&cut)).await.unwrap().unwrap();
        assert_eq!(c0.value().event, 10);
        assert_eq!(c0.value().state, 10); // 0 + 10

        let c1 = rh.after(c0.after(), Some(&cut)).await.unwrap().unwrap();
        assert_eq!(c1.value().event, 20);
        assert_eq!(c1.value().state, 30); // 10 + 20

        let c2 = rh.after(c1.after(), Some(&cut)).await.unwrap().unwrap();
        assert_eq!(c2.value().event, 30);
        assert_eq!(c2.value().state, 60); // 30 + 30

        assert!(rh.after(c2.after(), Some(&cut)).await.unwrap().is_none());

        // fetch_state only called once (for earliest)
        assert_eq!(rh.reducer.fetch_count(), 1);
    }

    #[tokio::test]
    async fn backward_search() {
        let h = FakeHistory::new(vec![(100, 0, 10), (200, 0, 20), (300, 0, 30)]);
        let history = h.into_history();

        let mut fetch_vals = HashMap::new();
        fetch_vals.insert(0, 0); // earliest
        fetch_vals.insert(200, 30); // at block 200
        let reducer = SumReducer::new(fetch_vals);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        let never = EvmPosition::never(TEST_CHAIN);
        let c = rh.before(&never, None).await.unwrap().unwrap();
        assert_eq!(c.value().event, 30);
        // fetch_state at block 300 (Before side) — resolve_state for the event's before pos
        // This will call fetch_state since no cache exists
    }

    #[tokio::test]
    async fn cache_floor_lookup() {
        let h = FakeHistory::new(vec![(100, 0, 10), (200, 0, 20), (300, 0, 30)]);
        let history = h.into_history();

        let reducer = SumReducer::new(HashMap::new());
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        // Cache states at blocks 100 and 200
        let pos_100 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: [0; 32], timestamp: 100, side: Side::After,
        };
        let pos_200 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: 0,
            block_hash: [0; 32], timestamp: 200, side: Side::After,
        };
        rh.cache_state(pos_100, 10);
        rh.cache_state(pos_200.clone(), 30);

        // Floor at block 250 should return state at 200
        let pos_250 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 250, log_index: 0,
            block_hash: [0; 32], timestamp: 250, side: Side::Before,
        };
        let (floor_pos, floor_state) = rh.floor_state(&pos_250).unwrap();
        assert_eq!(floor_pos.block_number, 200);
        assert_eq!(floor_state, 30);

        // Floor at block 200 exact match
        let (floor_pos, floor_state) = rh.floor_state(&pos_200).unwrap();
        assert_eq!(floor_pos.block_number, 200);
        assert_eq!(floor_state, 30);

        // Floor at block 50 — nothing before it
        let pos_50 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 50, log_index: 0,
            block_hash: [0; 32], timestamp: 50, side: Side::Before,
        };
        assert!(rh.floor_state(&pos_50).is_none());
    }

    #[tokio::test]
    async fn merge_compatibility() {
        // Two ReducedHistories merged
        let h1 = FakeHistory::new(vec![(100, 0, 10), (300, 0, 30)]);
        let h2 = FakeHistory::new(vec![(200, 0, 20)]);

        let mut fv = HashMap::new();
        fv.insert(0, 0);
        let r1 = SumReducer::new(fv.clone());
        let r2 = SumReducer::new(fv);

        let rh1 = ReducedHistory::new(h1.into_history(), r1, evm_cmp, 1, 1);
        let rh2 = ReducedHistory::new(h2.into_history(), r2, evm_cmp, 1, 1);

        let merged = History::merge(vec![rh1.history(), rh2.history()], evm_cmp);

        let cut = cutoff(400);
        let mut results = vec![];
        let mut cursor = merged.after(&merged.earliest(), Some(&cut)).await.unwrap();
        while let Some(c) = cursor {
            results.push((c.value().event, c.value().state));
            cursor = merged.after(c.after(), Some(&cut)).await.unwrap();
        }

        // Events interleaved: (10, state), (20, state), (30, state)
        // Note: each ReducedHistory tracks its own state independently
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 10); // event=10, state=0+10=10 (from rh1)
        assert_eq!(results[0].1, 10);
        assert_eq!(results[1].0, 20); // event=20, state=0+20=20 (from rh2)
        assert_eq!(results[1].1, 20);
        assert_eq!(results[2].0, 30); // event=30, state=10+30=40 (from rh1)
        assert_eq!(results[2].1, 40);
    }

    #[tokio::test]
    async fn delegate_methods() {
        let h = FakeHistory::new(vec![]);
        let history = h.into_history();

        let reducer = SumReducer::new(HashMap::new());
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);
        let rh_history = rh.history();

        assert_eq!(rh_history.source_id(), TEST_CHAIN);
        assert_eq!(rh_history.earliest().block_number, 0);
        assert_eq!(rh_history.never().block_number, u64::MAX);
        assert_eq!(rh_history.at(12345).block_number, 12345);
    }

    // --------------------------------------------------------
    // Cost-aware FakeHistory — configurable estimate_scan_count
    // --------------------------------------------------------

    struct CostFakeHistory {
        inner: Arc<FakeHistory>,
        scan_count: u64,
        chunk_size: u64,
    }

    impl CostFakeHistory {
        fn new(events: Vec<(u64, u32, u64)>, scan_count: u64, chunk_size: u64) -> Arc<Self> {
            let inner = FakeHistory::new(events);
            let h = Arc::new(Self {
                inner,
                scan_count,
                chunk_size,
            });
            h
        }

        fn into_history(self: Arc<Self>) -> History<u64, EvmPosition> {
            History::new(self as Arc<dyn HistoryBackend<u64, EvmPosition>>)
        }
    }

    #[async_trait::async_trait]
    impl HistoryBackend<u64, EvmPosition> for CostFakeHistory {
        fn source_id(&self) -> u64 { self.inner.source_id() }
        fn earliest(&self) -> EvmPosition { self.inner.earliest() }
        fn never(&self) -> EvmPosition { self.inner.never() }
        fn latest(&self) -> &Store<EvmPosition> { self.inner.latest() }
        fn best(&self) -> &Store<EvmPosition> { self.inner.best() }
        fn safe(&self) -> &Store<EvmPosition> { self.inner.safe() }
        fn at(&self, time: Timestamp) -> EvmPosition { self.inner.at(time) }

        async fn is_equiv(&self, a: &EvmPosition, b: &EvmPosition) -> Result<bool, HistoryError> {
            self.inner.is_equiv(a, b).await
        }
        async fn lca(&self, a: &EvmPosition, b: &EvmPosition) -> Result<EvmPosition, HistoryError> {
            self.inner.lca(a, b).await
        }
        fn search_after(&self, position: &EvmPosition, cutoff: Option<&EvmPosition>) -> SearchStream<u64, EvmPosition> {
            self.inner.search_after(position, cutoff)
        }
        fn search_before(&self, position: &EvmPosition, cutoff: Option<&EvmPosition>) -> SearchStream<u64, EvmPosition> {
            self.inner.search_before(position, cutoff)
        }
        async fn after(&self, position: &EvmPosition, cutoff: Option<&EvmPosition>) -> Result<Option<Cursor<u64, EvmPosition>>, HistoryError> {
            self.inner.after(position, cutoff).await
        }
        async fn before(&self, position: &EvmPosition, cutoff: Option<&EvmPosition>) -> Result<Option<Cursor<u64, EvmPosition>>, HistoryError> {
            self.inner.before(position, cutoff).await
        }
        async fn is_after(&self, cursor_pos: &EvmPosition, p: &EvmPosition) -> Result<bool, HistoryError> {
            self.inner.is_after(cursor_pos, p).await
        }
        async fn is_before(&self, cursor_pos: &EvmPosition, p: &EvmPosition) -> Result<bool, HistoryError> {
            self.inner.is_before(cursor_pos, p).await
        }

        fn estimate_scan_count(&self, _from: &EvmPosition, _to: &EvmPosition) -> u64 {
            self.scan_count
        }

        fn scan_chunk_before(&self, target: &EvmPosition) -> EvmPosition {
            EvmPosition {
                block_number: target.block_number.saturating_sub(self.chunk_size),
                log_index: 0,
                side: Side::Before,
                ..target.clone()
            }
        }
    }

    // --------------------------------------------------------
    // SumReducer with external cache
    // --------------------------------------------------------

    struct CostSumReducer {
        inner: SumReducer,
        external_cache: HashMap<u64, u64>, // block_number -> state
    }

    impl CostSumReducer {
        fn new(
            fetch_values: HashMap<u64, u64>,
            external_cache: HashMap<u64, u64>,
        ) -> Self {
            Self {
                inner: SumReducer::new(fetch_values),
                external_cache,
            }
        }

        fn fetch_count(&self) -> usize {
            self.inner.fetch_count()
        }
    }

    #[async_trait::async_trait]
    impl Reducer<EvmPosition> for CostSumReducer {
        type Event = u64;
        type State = u64;

        fn apply(&self, state: u64, event: &u64) -> u64 {
            self.inner.apply(state, event)
        }

        async fn fetch_state(&self, pos: &EvmPosition) -> Result<u64, HistoryError> {
            self.inner.fetch_state(pos).await
        }

        fn find_cached_state_floor(&self, pos: &EvmPosition) -> Option<(EvmPosition, u64)> {
            // Find closest block_number <= pos.block_number
            let mut best: Option<(u64, u64)> = None;
            for (&block, &state) in &self.external_cache {
                if block <= pos.block_number {
                    if best.is_none() || block > best.unwrap().0 {
                        best = Some((block, state));
                    }
                }
            }
            best.map(|(block, state)| {
                let p = EvmPosition {
                    chain_id: TEST_CHAIN,
                    block_number: block,
                    log_index: 0,
                    block_hash: [0; 32],
                    timestamp: block,
                    side: Side::After,
                };
                (p, state)
            })
        }
    }

    // --------------------------------------------------------
    // Cost-based resolve_state tests
    // --------------------------------------------------------

    #[tokio::test]
    async fn cost_prefers_replay_when_cheap() {
        // scan_count=0 from CostFakeHistory, floor exists in-memory.
        // Should replay (free) without calling fetch_state.
        let h = CostFakeHistory::new(vec![(200, 0, 20)], 0, 500);
        let history = h.into_history();

        let mut fv = HashMap::new();
        fv.insert(0, 0);
        let reducer = SumReducer::new(fv);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        // Pre-cache state at block 100
        let pos_100 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: [0; 32], timestamp: 100, side: Side::After,
        };
        rh.cache_state(pos_100, 50);

        // Search for event at 200 — floor at 100, scan_count=0, replay is free
        let c = rh.after(
            &EvmPosition {
                chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
                block_hash: [0; 32], timestamp: 100, side: Side::After,
            },
            Some(&cutoff(300)),
        ).await.unwrap().unwrap();

        assert_eq!(c.value().event, 20);
        assert_eq!(c.value().state, 70); // 50 + 20
        assert_eq!(rh.reducer.fetch_count(), 0); // no RPC needed
    }

    #[tokio::test]
    async fn cost_prefers_fetch_when_replay_expensive() {
        // scan_count=10, scan_cost=1, fetch_cost=1 → restore_cost=10 > fetch+scan=2.
        // Floor at block 1, target at 1_000_000.
        // Should use fetch fallback, not replay from floor.
        let h = CostFakeHistory::new(
            vec![(999_990, 0, 42)],
            10, // estimate_scan_count always returns 10
            500,
        );
        let history = h.into_history();

        let mut fv = HashMap::new();
        fv.insert(0, 0);
        fv.insert(999_490, 100); // fetch at checkpoint (999_990 - 500 = 999_490)
        let reducer = SumReducer::new(fv);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        // Pre-cache floor at block 1 (very far away)
        let pos_1 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 1, log_index: 0,
            block_hash: [0; 32], timestamp: 1, side: Side::After,
        };
        rh.cache_state(pos_1, 5);

        // Target: find event near block 1M
        let start = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 999_000, log_index: 0,
            block_hash: [0; 32], timestamp: 999_000, side: Side::Before,
        };
        let c = rh.after(&start, Some(&cutoff(1_000_000))).await.unwrap().unwrap();

        assert_eq!(c.value().event, 42);
        // fetch_state was called (checkpoint at 999_490)
        assert!(rh.reducer.fetch_count() >= 1);
    }

    #[tokio::test]
    async fn fallback_uses_scan_chunk_before() {
        // No floor at all, no external cache. Should fetch at checkpoint = target - chunk.
        // resolve_state(start=block 900) → checkpoint = 900-500 = 400 → fetch_state(400)
        let h = CostFakeHistory::new(vec![(1000, 0, 77)], 5, 500);
        let history = h.into_history();

        let mut fv = HashMap::new();
        fv.insert(400, 10); // fetch at checkpoint (900 - 500 = 400)
        let reducer = SumReducer::new(fv);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        let start = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 900, log_index: 0,
            block_hash: [0; 32], timestamp: 900, side: Side::Before,
        };
        let c = rh.after(&start, Some(&cutoff(1100))).await.unwrap().unwrap();

        assert_eq!(c.value().event, 77);
        assert_eq!(c.value().state, 87); // 10 (fetched at 400) + 77
        assert_eq!(rh.reducer.fetch_count(), 1);
    }

    #[tokio::test]
    async fn external_cache_floor_used() {
        // Reducer returns a floor from find_cached_state_floor (simulating SQLite).
        // scan_count=0, so replay from external floor is free.
        let h = CostFakeHistory::new(vec![(200, 0, 20)], 0, 500);
        let history = h.into_history();

        let mut ext_cache = HashMap::new();
        ext_cache.insert(150, 99); // external state at block 150

        let reducer = CostSumReducer::new(HashMap::new(), ext_cache);
        let rh = ReducedHistory::new(history, reducer, evm_cmp, 1, 1);

        // Query from block 160 (after the external floor at 150).
        // resolve_state(160) finds external floor at 150 with state=99.
        // scan_count=0, restore_cost=0 <= fetch_and_scan=2, so replay.
        // No events between 150 and 160, state stays 99.
        // Then apply event 20 → state = 99 + 20 = 119.
        let start = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 160, log_index: 0,
            block_hash: [0; 32], timestamp: 160, side: Side::After,
        };
        let c = rh.after(&start, Some(&cutoff(300))).await.unwrap().unwrap();

        assert_eq!(c.value().event, 20);
        assert_eq!(c.value().state, 119); // 99 (external floor) + 20
        assert_eq!(rh.reducer.fetch_count(), 0); // no RPC fetch needed
    }
}
