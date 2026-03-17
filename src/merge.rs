//! Merged history — interleaves N same-type histories into one.
//!
//! Uses per-source search streams with progressive cancellation:
//! when one source finds a result, sources that have scanned past it
//! are aborted immediately rather than waiting for all to complete.

use std::cmp::Ordering;
use std::sync::{Arc, OnceLock, Weak};

use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::arch::{
    Cursor, History, HistoryBackend, HistoryError, Position, SearchEvent, SearchStream, Store,
    Timestamp,
};

/// Whether we're scanning forward or backward.
#[derive(Clone, Copy)]
enum SearchDir {
    Forward,
    Backward,
}

/// Tagged event from a per-source reader task.
enum SourceEvent<T: Send + Sync + 'static, P: Position> {
    Scanned { source: usize, pos: P },
    Found { source: usize, cursor: Cursor<T, P> },
    Done { source: usize },
    Error(HistoryError),
}

/// Interleaves N histories sharing the same event and position type.
///
/// Constructed via [`History::merge`].
pub(crate) struct MergedHistory<T: Send + Sync + 'static, P: Position> {
    sources: Vec<History<T, P>>,
    pos_cmp: fn(&P, &P) -> Ordering,
    source_id: u64,

    // Reactively merged across all sources (max by pos_cmp).
    latest_store: Store<P>,
    best_store: Store<P>,
    safe_store: Store<P>,

    self_ref: OnceLock<Weak<Self>>,
    _tip_handles: Vec<JoinHandle<()>>,
}

impl<T: Send + Sync + 'static, P: Position> Drop for MergedHistory<T, P> {
    fn drop(&mut self) {
        for h in &self._tip_handles {
            h.abort();
        }
    }
}

/// Spawn a background task that tracks the max across all source stores.
fn spawn_tip_merge<P: Position>(
    stores: Vec<Store<P>>,
    pos_cmp: fn(&P, &P) -> Ordering,
) -> (Store<P>, JoinHandle<()>) {
    let initial = stores
        .iter()
        .map(|s| s.get())
        .max_by(|a, b| pos_cmp(a, b))
        .unwrap();

    let (tx, rx) = tokio::sync::watch::channel(initial);
    let merged_store = Store::new(rx);

    let handle = tokio::spawn(async move {
        let subs: Vec<_> = stores.iter().map(|s| s.subscribe()).collect();
        let mut merged = futures_util::stream::select_all(subs);

        while merged.next().await.is_some() {
            let max = stores
                .iter()
                .map(|s| s.get())
                .max_by(|a, b| pos_cmp(a, b))
                .unwrap();
            let _ = tx.send(max);
        }
    });

    (merged_store, handle)
}

impl<T: Send + Sync + 'static, P: Position> MergedHistory<T, P> {
    /// Create a MergedHistory and wrap it in a [`History`].
    pub(crate) fn into_history(
        sources: Vec<History<T, P>>,
        pos_cmp: fn(&P, &P) -> Ordering,
    ) -> History<T, P> {
        assert!(!sources.is_empty(), "merge requires at least one source");

        let source_id = sources[0].source_id();
        assert!(
            sources.iter().all(|s| s.source_id() == source_id),
            "all merged sources must be from the same source (expected {}, got mixed)",
            source_id,
        );

        let (latest_store, h1) = spawn_tip_merge(
            sources.iter().map(|s| s.latest().clone()).collect(),
            pos_cmp,
        );
        let (best_store, h2) = spawn_tip_merge(
            sources.iter().map(|s| s.best().clone()).collect(),
            pos_cmp,
        );
        let (safe_store, h3) = spawn_tip_merge(
            sources.iter().map(|s| s.safe().clone()).collect(),
            pos_cmp,
        );

        let merged = Arc::new(Self {
            sources,
            pos_cmp,
            source_id,
            latest_store,
            best_store,
            safe_store,
            self_ref: OnceLock::new(),
            _tip_handles: vec![h1, h2, h3],
        });

        let _ = merged.self_ref.set(Arc::downgrade(&merged));

        History::new(merged as Arc<dyn HistoryBackend<T, P>>)
    }

    fn arc_self(&self) -> Arc<Self> {
        self.self_ref
            .get()
            .expect("self_ref not initialized")
            .upgrade()
            .expect("MergedHistory dropped while still in use")
    }

    /// True if `new` is a better (closer to search origin) result than `current`.
    fn is_better(&self, dir: SearchDir, new: &P, current: &P) -> bool {
        match dir {
            SearchDir::Forward => (self.pos_cmp)(new, current) == Ordering::Less,
            SearchDir::Backward => (self.pos_cmp)(new, current) == Ordering::Greater,
        }
    }

    /// True if a source's scanned position has moved past the best found result,
    /// meaning it can't find anything closer.
    fn has_scanned_past(&self, dir: SearchDir, scanned: &P, best: &P) -> bool {
        match dir {
            SearchDir::Forward => (self.pos_cmp)(scanned, best) != Ordering::Less,
            SearchDir::Backward => (self.pos_cmp)(scanned, best) != Ordering::Greater,
        }
    }

    /// Spawn per-source reader tasks and a coordinator, return the output stream.
    fn run_search(
        &self,
        dir: SearchDir,
        position: &P,
        cutoff: Option<&P>,
    ) -> SearchStream<T, P> {
        let pos = position.clone();
        let cut = cutoff.cloned();
        let this = self.arc_self();
        let (out_tx, out_rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            this.coordinate(dir, &pos, cut.as_ref(), &out_tx).await;
        });

        Box::pin(futures_util::stream::unfold(out_rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    /// Coordinator: manages per-source tasks, tracks best result, cancels
    /// sources that have scanned past the best.
    async fn coordinate(
        &self,
        dir: SearchDir,
        position: &P,
        cutoff: Option<&P>,
        out_tx: &tokio::sync::mpsc::Sender<Result<SearchEvent<T, P>, HistoryError>>,
    ) {
        let n = self.sources.len();
        let (tag_tx, mut tag_rx) = tokio::sync::mpsc::channel::<SourceEvent<T, P>>(n);

        // Spawn a reader task per source
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(n);
        for (i, source) in self.sources.iter().enumerate() {
            let mut stream = match dir {
                SearchDir::Forward => source.search_after(position, cutoff),
                SearchDir::Backward => source.search_before(position, cutoff),
            };
            let tx = tag_tx.clone();
            handles.push(tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    let msg = match event {
                        Ok(SearchEvent::Scanned(pos)) => {
                            SourceEvent::Scanned { source: i, pos }
                        }
                        Ok(SearchEvent::Found(cursor)) => {
                            let _ = tx.send(SourceEvent::Found { source: i, cursor }).await;
                            return;
                        }
                        Err(e) => {
                            let _ = tx.send(SourceEvent::Error(e)).await;
                            return;
                        }
                    };
                    if tx.send(msg).await.is_err() {
                        return; // Coordinator dropped
                    }
                }
                // Source stream ended without Found
                let _ = tx.send(SourceEvent::Done { source: i }).await;
            }));
        }
        // Drop our copy so tag_rx closes when all source tasks finish
        drop(tag_tx);

        let mut best: Option<Cursor<T, P>> = None;
        let mut scanned_to: Vec<Option<P>> = (0..n).map(|_| None).collect();
        let mut done = vec![false; n];

        while let Some(event) = tag_rx.recv().await {
            match event {
                SourceEvent::Found { source: idx, cursor } => {
                    done[idx] = true;
                    let dominated = best
                        .as_ref()
                        .is_some_and(|b| !self.is_better(dir, cursor.before(), b.before()));
                    if !dominated {
                        best = Some(cursor);
                    }
                    // Cancel sources that have scanned past the (new) best
                    if let Some(ref b) = best {
                        for (i, s) in scanned_to.iter().enumerate() {
                            if done[i] {
                                continue;
                            }
                            if let Some(ref pos) = s {
                                if self.has_scanned_past(dir, pos, b.before()) {
                                    handles[i].abort();
                                    done[i] = true;
                                }
                            }
                        }
                    }
                }
                SourceEvent::Scanned { source: idx, pos } => {
                    scanned_to[idx] = Some(pos);
                    if let Some(ref b) = best {
                        let scanned = scanned_to[idx].as_ref().unwrap();
                        if self.has_scanned_past(dir, scanned, b.before()) {
                            handles[idx].abort();
                            done[idx] = true;
                        }
                    }
                }
                SourceEvent::Done { source: idx } => {
                    done[idx] = true;
                }
                SourceEvent::Error(e) => {
                    for h in &handles {
                        h.abort();
                    }
                    let _ = out_tx.send(Err(e)).await;
                    return;
                }
            }

            // All sources resolved?
            if done.iter().all(|&d| d) {
                break;
            }

            // Emit progress when no result yet and all sources have reported
            if best.is_none() {
                let all_reported = scanned_to.iter().all(|s| s.is_some());
                if all_reported {
                    let positions: Vec<&P> =
                        scanned_to.iter().map(|s| s.as_ref().unwrap()).collect();
                    let frontier = match dir {
                        SearchDir::Forward => {
                            positions.into_iter().min_by(|a, b| (self.pos_cmp)(a, b))
                        }
                        SearchDir::Backward => {
                            positions.into_iter().max_by(|a, b| (self.pos_cmp)(a, b))
                        }
                    };
                    if let Some(pos) = frontier {
                        if out_tx
                            .send(Ok(SearchEvent::Scanned(pos.clone())))
                            .await
                            .is_err()
                        {
                            for h in &handles {
                                h.abort();
                            }
                            return; // Output cancelled
                        }
                    }
                }
            }
        }

        // Emit final result
        if let Some(cursor) = best {
            let backend = self.arc_self() as Arc<dyn HistoryBackend<T, P>>;
            let _ = out_tx
                .send(Ok(SearchEvent::Found(cursor.with_backend(backend))))
                .await;
        }
    }
}

#[async_trait::async_trait]
impl<T: Send + Sync + 'static, P: Position> HistoryBackend<T, P> for MergedHistory<T, P> {
    fn source_id(&self) -> u64 {
        self.source_id
    }

    fn earliest(&self) -> P {
        self.sources[0].earliest()
    }

    fn never(&self) -> P {
        self.sources[0].never()
    }

    fn latest(&self) -> &Store<P> {
        &self.latest_store
    }

    fn best(&self) -> &Store<P> {
        &self.best_store
    }

    fn safe(&self) -> &Store<P> {
        &self.safe_store
    }

    fn at(&self, time: Timestamp) -> P {
        // All same-chain sources produce identical results for at().
        self.sources[0].at(time)
    }

    async fn is_equiv(&self, a: &P, b: &P) -> Result<bool, HistoryError> {
        Ok((self.pos_cmp)(a, b) == Ordering::Equal && a.side() == b.side())
    }

    async fn lca(&self, a: &P, b: &P) -> Result<P, HistoryError> {
        if (self.pos_cmp)(a, b) != Ordering::Greater {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    }

    fn search_after(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P> {
        self.run_search(SearchDir::Forward, position, cutoff)
    }

    fn search_before(&self, position: &P, cutoff: Option<&P>) -> SearchStream<T, P> {
        self.run_search(SearchDir::Backward, position, cutoff)
    }

    async fn is_after(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError> {
        Ok((self.pos_cmp)(cursor_pos, p) == Ordering::Greater)
    }

    async fn is_before(&self, cursor_pos: &P, p: &P) -> Result<bool, HistoryError> {
        Ok((self.pos_cmp)(cursor_pos, p) == Ordering::Less)
    }

    fn estimate_scan_count(&self, from: &P, to: &P) -> u64 {
        self.sources.iter().map(|s| s.estimate_scan_count(from, to)).sum()
    }

    fn scan_chunk_before(&self, target: &P) -> P {
        self.sources[0].scan_chunk_before(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::evm::EvmPosition;
    use crate::arch::Side;

    fn evm_cmp(a: &EvmPosition, b: &EvmPosition) -> Ordering {
        (a.block_number, a.log_index).cmp(&(b.block_number, b.log_index))
    }

    const TEST_CHAIN: u64 = 1;

    /// Minimal in-memory history backend for testing merge logic.
    struct StaticHistory {
        chain_id: u64,
        events: Vec<(u64, u32)>, // (block_number, log_index)
        latest_store: Store<EvmPosition>,
        best_store: Store<EvmPosition>,
        safe_store: Store<EvmPosition>,
        latest_tx: tokio::sync::watch::Sender<EvmPosition>,
        _best_tx: tokio::sync::watch::Sender<EvmPosition>,
        _safe_tx: tokio::sync::watch::Sender<EvmPosition>,
        self_ref: OnceLock<Weak<Self>>,
    }

    impl StaticHistory {
        fn new(events: Vec<(u64, u32)>) -> Arc<Self> {
            Self::with_chain(TEST_CHAIN, events)
        }

        fn with_chain(chain_id: u64, events: Vec<(u64, u32)>) -> Arc<Self> {
            Self::with_tips(chain_id, events, EvmPosition::never(chain_id), EvmPosition::never(chain_id), EvmPosition::never(chain_id))
        }

        fn with_tips(
            chain_id: u64,
            events: Vec<(u64, u32)>,
            latest: EvmPosition,
            best: EvmPosition,
            safe: EvmPosition,
        ) -> Arc<Self> {
            let (ltx, lrx) = tokio::sync::watch::channel(latest);
            let (btx, brx) = tokio::sync::watch::channel(best);
            let (stx, srx) = tokio::sync::watch::channel(safe);
            let h = Arc::new(Self {
                chain_id,
                events,
                latest_store: Store::new(lrx),
                best_store: Store::new(brx),
                safe_store: Store::new(srx),
                latest_tx: ltx,
                _best_tx: btx,
                _safe_tx: stx,
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

        fn make_cursor(&self, block: u64, idx: u32) -> Cursor<u64, EvmPosition> {
            let value = block * 1000 + idx as u64;
            let before = EvmPosition {
                chain_id: self.chain_id, block_number: block, log_index: idx,
                block_hash: [0; 32], timestamp: block, side: Side::Before,
            };
            let after = EvmPosition {
                chain_id: self.chain_id, block_number: block, log_index: idx,
                block_hash: [0; 32], timestamp: block, side: Side::After,
            };
            Cursor::new(value, before, after, self.arc_self() as Arc<dyn HistoryBackend<u64, EvmPosition>>)
        }
    }

    #[async_trait::async_trait]
    impl HistoryBackend<u64, EvmPosition> for StaticHistory {
        fn source_id(&self) -> u64 { self.chain_id }

        fn earliest(&self) -> EvmPosition { EvmPosition::earliest(self.chain_id) }

        fn never(&self) -> EvmPosition { EvmPosition::never(self.chain_id) }

        fn latest(&self) -> &Store<EvmPosition> { &self.latest_store }
        fn best(&self) -> &Store<EvmPosition> { &self.best_store }
        fn safe(&self) -> &Store<EvmPosition> { &self.safe_store }

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

        async fn is_equiv(&self, a: &EvmPosition, b: &EvmPosition) -> Result<bool, HistoryError> {
            Ok(a.block_number == b.block_number && a.log_index == b.log_index && a.side == b.side)
        }

        async fn lca(&self, a: &EvmPosition, b: &EvmPosition) -> Result<EvmPosition, HistoryError> {
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
            for &(b, i) in &self.events {
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
                    return Ok(Some(self.make_cursor(b, i)));
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
            for &(b, i) in self.events.iter().rev() {
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
                    return Ok(Some(self.make_cursor(b, i)));
                }
            }
            Ok(None)
        }

        async fn is_after(&self, cursor_pos: &EvmPosition, p: &EvmPosition) -> Result<bool, HistoryError> {
            Ok((cursor_pos.block_number, cursor_pos.log_index) > (p.block_number, p.log_index))
        }

        async fn is_before(&self, cursor_pos: &EvmPosition, p: &EvmPosition) -> Result<bool, HistoryError> {
            Ok((cursor_pos.block_number, cursor_pos.log_index) < (p.block_number, p.log_index))
        }
    }

    #[tokio::test]
    async fn merge_interleaves_two_sources() {
        let a = StaticHistory::new(vec![(100, 0), (100, 2), (200, 0)]);
        let b = StaticHistory::new(vec![(100, 1), (150, 0)]);

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 300, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        let mut results = vec![];
        let mut cursor = merged.after(&merged.earliest(), Some(&cutoff)).await.unwrap();
        while let Some(c) = cursor {
            results.push(*c.value());
            cursor = merged.after(c.after(), Some(&cutoff)).await.unwrap();
        }

        assert_eq!(results, vec![100_000, 100_001, 100_002, 150_000, 200_000]);
    }

    #[tokio::test]
    async fn merge_reverse_order() {
        let a = StaticHistory::new(vec![(100, 0), (200, 0)]);
        let b = StaticHistory::new(vec![(150, 0)]);

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        let mut results = vec![];
        let mut cursor = merged.before(&merged.never(), None).await.unwrap();
        while let Some(c) = cursor {
            results.push(*c.value());
            cursor = merged.before(c.before(), None).await.unwrap();
        }

        assert_eq!(results, vec![200_000, 150_000, 100_000]);
    }

    #[tokio::test]
    async fn merge_deduplicates_shared_position() {
        // Same event at (100, 0) in both sources — should appear once.
        let a = StaticHistory::new(vec![(100, 0), (200, 0)]);
        let b = StaticHistory::new(vec![(100, 0), (300, 0)]);

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 400, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        let mut results = vec![];
        let mut cursor = merged.after(&merged.earliest(), Some(&cutoff)).await.unwrap();
        while let Some(c) = cursor {
            results.push(*c.value());
            cursor = merged.after(c.after(), Some(&cutoff)).await.unwrap();
        }

        assert_eq!(results, vec![100_000, 200_000, 300_000]);
    }

    #[tokio::test]
    async fn merge_empty_sources() {
        let a = StaticHistory::new(vec![]);
        let b = StaticHistory::new(vec![(100, 0)]);

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        let c = merged.after(&merged.earliest(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(*c.value(), 100_000);
        assert!(merged.after(c.after(), Some(&cutoff)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn merge_single_source_passthrough() {
        let a = StaticHistory::new(vec![(10, 0), (20, 0)]);

        let merged = History::merge(vec![a.into_history()], evm_cmp);

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        let c = merged.after(&merged.earliest(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(*c.value(), 10_000);
    }

    #[tokio::test]
    async fn merge_cursor_next_walks_merged() {
        // Verify that cursor.next() navigates via the merged backend, not the source.
        let a = StaticHistory::new(vec![(100, 0), (300, 0)]);
        let b = StaticHistory::new(vec![(200, 0)]);

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        let cutoff = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 400, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };

        let c0 = merged.after(&merged.earliest(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(*c0.value(), 100_000);

        // next() goes through the merged backend, should find (200, 0) from source B
        let c1 = merged.after(c0.after(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(*c1.value(), 200_000);

        let c2 = merged.after(c1.after(), Some(&cutoff)).await.unwrap().unwrap();
        assert_eq!(*c2.value(), 300_000);

        assert!(merged.after(c2.after(), Some(&cutoff)).await.unwrap().is_none());
    }

    fn pos(block: u64) -> EvmPosition {
        EvmPosition {
            chain_id: TEST_CHAIN,
            block_number: block,
            log_index: 0,
            block_hash: [0; 32],
            timestamp: block,
            side: Side::After,
        }
    }

    #[tokio::test]
    async fn merge_tip_reflects_max_across_sources() {
        let a = StaticHistory::with_tips(TEST_CHAIN, vec![], pos(100), pos(50), pos(10));
        let b = StaticHistory::with_tips(TEST_CHAIN, vec![], pos(200), pos(30), pos(20));

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        assert_eq!(merged.latest().get().block_number, 200);
        assert_eq!(merged.best().get().block_number, 50);
        assert_eq!(merged.safe().get().block_number, 20);
    }

    #[tokio::test]
    async fn merge_tip_updates_reactively() {
        let a = StaticHistory::with_tips(TEST_CHAIN, vec![], pos(100), pos(100), pos(100));
        let b = StaticHistory::with_tips(TEST_CHAIN, vec![], pos(100), pos(100), pos(100));
        let a_ref = a.clone();

        let merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );

        assert_eq!(merged.latest().get().block_number, 100);

        // Update source A's latest to 300
        a_ref.latest_tx.send(pos(300)).unwrap();
        // Let the merge task process the update
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(merged.latest().get().block_number, 300);
    }

    #[tokio::test]
    async fn merge_is_equiv_self_contained() {
        let a = StaticHistory::new(vec![(100, 0)]);
        let merged = History::merge(vec![a.into_history()], evm_cmp);

        let p1 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 5,
            block_hash: [0; 32], timestamp: 100, side: Side::Before,
        };
        let p2 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 5,
            block_hash: [0; 32], timestamp: 100, side: Side::Before,
        };
        let p3 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 5,
            block_hash: [0; 32], timestamp: 100, side: Side::After,
        };
        let p4 = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: 0,
            block_hash: [0; 32], timestamp: 200, side: Side::Before,
        };

        // Same coordinate, same side → equivalent
        assert!(merged.is_equiv(&p1, &p2).await.unwrap());
        // Same coordinate, different side → not equivalent
        assert!(!merged.is_equiv(&p1, &p3).await.unwrap());
        // Different coordinate → not equivalent
        assert!(!merged.is_equiv(&p1, &p4).await.unwrap());
    }

    #[tokio::test]
    async fn merge_lca_returns_earlier() {
        let a = StaticHistory::new(vec![]);
        let merged = History::merge(vec![a.into_history()], evm_cmp);

        let early = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 100, log_index: 0,
            block_hash: [0; 32], timestamp: 100, side: Side::Before,
        };
        let late = EvmPosition {
            chain_id: TEST_CHAIN, block_number: 200, log_index: 0,
            block_hash: [0; 32], timestamp: 200, side: Side::Before,
        };

        let lca = merged.lca(&early, &late).await.unwrap();
        assert_eq!(lca.block_number, 100);

        let lca = merged.lca(&late, &early).await.unwrap();
        assert_eq!(lca.block_number, 100);

        // Same position → returns that position
        let lca = merged.lca(&early, &early).await.unwrap();
        assert_eq!(lca.block_number, 100);
    }

    #[tokio::test]
    #[should_panic(expected = "all merged sources must be from the same source")]
    async fn merge_rejects_different_source_ids() {
        let a = StaticHistory::with_chain(1, vec![(100, 0)]);
        let b = StaticHistory::with_chain(2, vec![(200, 0)]);

        // Should panic because chain_ids differ
        let _merged = History::merge(
            vec![a.into_history(), b.into_history()],
            evm_cmp,
        );
    }

    #[test]
    fn position_carries_source_id() {
        use crate::arch::Position;

        let pos = EvmPosition {
            chain_id: 42, block_number: 100, log_index: 0,
            block_hash: [0; 32], timestamp: 100, side: Side::Before,
        };
        assert_eq!(pos.source_id(), 42);

        let earliest = EvmPosition::earliest(137);
        assert_eq!(earliest.source_id(), 137);

        let never = EvmPosition::never(10);
        assert_eq!(never.source_id(), 10);
    }
}
