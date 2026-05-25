//! In-memory range tree backed by [`LogStore`].
//!
//! A `BTreeMap` keyed by the *end* position of each [`Range`], with
//! singleflight + late-item handling. Items themselves live in
//! [`LogStore`]; the tree only holds [`LogRef`]s, so a long-running
//! history's resident memory is bounded by the number of distinct
//! positions (eviction is a future addition).
//!
//! Lazy loading: rather than slurping every `scanned_range` from
//! [`LogStore`] on first access, the tree materializes per-window on
//! demand via [`LogIndex::ensure_window_loaded`]. This caps startup cost
//! at the cost of one bookkeeping `Vec<(u64, u64)>` of windows already
//! pulled in.
//!
//! The module is split across submodules:
//! - [`block_pos`] — the position/pointer data types ([`BlockPos`],
//!   [`LogRef`], [`LogPage`]).
//! - [`range_tree`] — the [`Range`] entry type, page insertion (four
//!   late-item cases — see [`LogIndex::insert_late_item`]), and the lazy
//!   load of scanned ranges.
//! - [`nav`] — cached-only navigation (`first_at_or_after`, `first_after`,
//!   `last_at_or_before`, `last_before`) plus [`LogRef`] resolution.
//! - [`gap_fill`] — the fetcher type, `ensure_cached`, the `items()` async
//!   stream, and the fetcher-driven singleflight.
//!
//! This `mod.rs` owns the [`LogIndex`] struct itself, its constructors and
//! accessors, and the async drain to [`LogStore`].

use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::log_store::LogStore;
use std::sync::Arc;

mod block_pos;
mod gap_fill;
mod nav;
mod range_tree;

pub use block_pos::{BlockPos, LogPage, LogRef};
pub use gap_fill::Fetcher;

use range_tree::Range;

// ============================================================
// LogIndex
// ============================================================

/// Per-filter in-memory range tree on top of [`LogStore`].
///
/// One instance per `(LogStore, filter_hash)` pair. Cheap to construct
/// — actual `scanned_ranges` rows are pulled into the tree on demand by
/// [`Self::ensure_window_loaded`].
pub struct LogIndex {
    store: Arc<LogStore>,
    filter_hash: [u8; 32],
    fetcher: Fetcher,
    /// Aligned scanned-range chunk size (blocks). Determines the
    /// granularity at which gaps get fetched and the alignment of
    /// `scanned_ranges` rows recorded in [`LogStore`].
    chunk_size: u64,
    tree: Mutex<BTreeMap<BlockPos, Range>>,
    /// Block-level windows already materialized into the tree from
    /// `LogStore::get_scanned_ranges`. Tracked so a second visit to the
    /// same window does not re-slurp.
    loaded_windows: Mutex<Vec<(u64, u64)>>,
    /// Optional async drain. When attached, every page passed to
    /// [`Self::insert_page`] is cloned onto the channel and persisted to
    /// [`LogStore`] by a background task. None means the index is purely
    /// in-memory (tests, or the legacy fetcher-driven path which writes
    /// inline in [`Self::finalize_gap_fill`]).
    drain_tx: Mutex<Option<UnboundedSender<LogPage>>>,
}

impl LogIndex {
    /// Construct a fresh, empty index over `store` for `filter_hash`.
    /// Aligned chunk size defaults to `1000` blocks via [`Self::with_chunk_size`].
    pub fn new(store: Arc<LogStore>, filter_hash: [u8; 32], fetcher: Fetcher) -> Self {
        Self::with_chunk_size(store, filter_hash, fetcher, 1000)
    }

    /// Like [`Self::new`] but with an explicit aligned chunk size.
    pub fn with_chunk_size(
        store: Arc<LogStore>,
        filter_hash: [u8; 32],
        fetcher: Fetcher,
        chunk_size: u64,
    ) -> Self {
        assert!(chunk_size > 0, "chunk_size must be positive");
        Self {
            store,
            filter_hash,
            fetcher,
            chunk_size,
            tree: Mutex::new(BTreeMap::new()),
            loaded_windows: Mutex::new(Vec::new()),
            drain_tx: Mutex::new(None),
        }
    }

    /// Spawn a background task that persists every page passed to
    /// [`Self::insert_page`] to [`LogStore`]. Returns `false` (and skips
    /// spawning) if a drain is already attached.
    ///
    /// Items are written via [`LogStore::insert_logs`]; the scanned range
    /// is recorded via [`LogStore::record_scanned_range`]. Failures are
    /// logged but never propagated — the in-memory tree remains the source
    /// of truth on the live-event path.
    pub fn attach_drain(&self) -> bool {
        let mut drain = self.drain_tx.lock().unwrap();
        if drain.is_some() {
            return false;
        }
        let (tx, mut rx) = unbounded_channel::<LogPage>();
        let store = self.store.clone();
        let filter_hash = self.filter_hash;
        tokio::spawn(async move {
            while let Some(page) = rx.recv().await {
                if !page.items.is_empty()
                    && let Err(e) = store.insert_logs(&filter_hash, &page.items)
                {
                    tracing::warn!(error = %e, "drain insert_logs");
                }
                if let Err(e) = store.record_scanned_range(
                    &filter_hash,
                    page.from_block,
                    page.to_block,
                    &page.end_block_hash,
                ) {
                    tracing::warn!(error = %e, "drain record_scanned_range");
                }
            }
        });
        *drain = Some(tx);
        true
    }

    /// Whether a drain task is currently attached. Test helper.
    #[cfg(test)]
    pub(crate) fn has_drain(&self) -> bool {
        self.drain_tx.lock().unwrap().is_some()
    }

    /// Filter hash this index serves.
    pub fn filter_hash(&self) -> &[u8; 32] {
        &self.filter_hash
    }

    /// Aligned chunk size in blocks.
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Number of ranges currently in the tree. Test helper.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tree.lock().unwrap().len()
    }

    /// Whether the block window `[from, to]` has been materialized.
    /// Test helper.
    #[cfg(test)]
    pub(crate) fn window_loaded(&self, from: u64, to: u64) -> bool {
        self.loaded_windows
            .lock()
            .unwrap()
            .iter()
            .any(|&(a, b)| a <= from && b >= to)
    }
}

// ============================================================
// Shared test helpers + cross-cutting tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::gap_fill::never_fetch;
    use super::range_tree::align_window;
    use super::*;
    use crate::evm::log_store::LogStore;
    use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
    use alloy::rpc::types::Log;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::yield_now;
    use tokio::time::sleep;

    pub(super) const FH: [u8; 32] = [0xAA; 32];
    pub(super) const HASH_A: [u8; 32] = [0xBB; 32];
    pub(super) const HASH_B: [u8; 32] = [0xBC; 32];

    pub(super) fn mk_log(block: u64, idx: u64, block_hash: [u8; 32]) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01]);
        let log_data = LogData::new_unchecked(vec![topic0], data);
        Log {
            inner: LogInner {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(B256::from(block_hash)),
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: Some(B256::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(idx),
            removed: false,
        }
    }

    pub(super) fn mk_index() -> LogIndex {
        LogIndex::new(
            Arc::new(LogStore::open_in_memory().unwrap()),
            FH,
            never_fetch(),
        )
    }

    // -- BlockPos ordering --

    #[test]
    fn block_pos_bottom_below_everything() {
        assert!(BlockPos::Bottom < BlockPos::at(0, 0));
        assert!(BlockPos::Bottom < BlockPos::end_of_block(0));
        assert!(BlockPos::before_block(0) == BlockPos::Bottom);
        assert!(
            BlockPos::before_block(1)
                == BlockPos::At {
                    block: 0,
                    log_index: u32::MAX,
                }
        );
    }

    // -- insert_page basic shapes --

    #[test]
    fn insert_range_in_gap_creates_three_entries() {
        let idx = mk_index();
        // First page: blocks [100, 200] with two logs at (150, 0) and (180, 0).
        let page = LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 0, HASH_B)],
            end_block_hash: HASH_B,
        };
        idx.insert_page(&page);
        // Expect: filled@150, filled@180, empty tail @ end_of_block(200) = 3 entries.
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn insert_range_at_existing_end_extends_not_duplicates() {
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before = idx.len();
        // Second page covering [200, 300] with one log at (250, 0).
        idx.insert_page(&LogPage {
            from_block: 201,
            to_block: 300,
            items: vec![mk_log(250, 0, HASH_B)],
            end_block_hash: HASH_B,
        });
        // Expect new filled@250 + new empty-tail past 250 = 2 more entries.
        assert_eq!(idx.len(), before + 2);
    }

    // -- late_item insertion (4 cases) --

    #[test]
    fn late_item_inside_existing_range_splits_it() {
        let idx = mk_index();
        // Set up: page with single log at (200, 0) covering [100, 300].
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 300,
            items: vec![mk_log(200, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        // Now a late item arrives at (250, 0) — inside the tail empty
        // range (200, 300]. Should split into filled@250 + empty(250, 300].
        let mut tree = idx.tree.lock().unwrap();
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 250,
                log_index: 0,
                block_hash: HASH_B,
            },
            BlockPos::at(200, 0),
        );
        // before_len had: filled@(200, 0), empty(end_of_block(300)). After split:
        // filled@(200, 0), filled@(250, 0), empty(end_of_block(300)) — net +1.
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(250, 0)));
        assert!(tree.contains_key(&BlockPos::end_of_block(300)));
    }

    #[test]
    fn late_item_at_empty_range_end_upgrades_to_filled() {
        let idx = mk_index();
        // Page with no items so tree gets a single empty range ending at
        // end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![],
            end_block_hash: HASH_A,
        });
        assert_eq!(idx.len(), 1);
        let target_pos = BlockPos::end_of_block(200);
        {
            let tree = idx.tree.lock().unwrap();
            assert!(tree.get(&target_pos).unwrap().is_empty());
        }
        let mut tree = idx.tree.lock().unwrap();
        // Late item lands exactly at the empty range's end position.
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 200,
                log_index: u32::MAX,
                block_hash: HASH_B,
            },
            BlockPos::at(150, 0),
        );
        // Count unchanged; the empty was upgraded in-place.
        assert_eq!(tree.len(), 1);
        let r = tree.get(&target_pos).unwrap();
        assert!(!r.is_empty());
    }

    #[test]
    fn late_item_duplicate_is_ignored() {
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        // Same (block, log_index) as existing filled range end.
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 150,
                log_index: 0,
                block_hash: HASH_B, // different hash — still ignored
            },
            BlockPos::at(100, 0),
        );
        assert_eq!(tree.len(), before_len);
    }

    #[test]
    fn late_item_in_gap_inserts_filled_range() {
        let idx = mk_index();
        // First page [100, 200] with one log — gives filled@(150, 0)
        // and empty-tail at end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });
        // Insert a late item at (90, 0) — in a true gap before everything.
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        LogIndex::insert_late_item(
            &mut tree,
            LogRef {
                block_number: 90,
                log_index: 0,
                block_hash: HASH_B,
            },
            BlockPos::before_block(90),
        );
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(90, 0)));
    }

    // -- lazy per-window load --

    #[test]
    fn is_loaded_lazy_first_access_only() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        // Pre-populate the store as if a previous process had scanned [100, 200]
        // with one log at (150, 0).
        store.insert_logs(&FH, &[mk_log(150, 0, HASH_A)]).unwrap();
        store.record_scanned_range(&FH, 100, 200, &HASH_A).unwrap();

        let idx = LogIndex::with_chunk_size(store, FH, never_fetch(), 1000);
        assert_eq!(idx.len(), 0, "tree starts empty");

        idx.ensure_window_loaded(100, 200).unwrap();
        assert!(idx.window_loaded(100, 200));
        assert!(
            idx.len() >= 2,
            "expected at least filled@150 + empty-tail entries"
        );

        let count_after_first_load = idx.len();
        idx.ensure_window_loaded(100, 200).unwrap();
        assert_eq!(
            idx.len(),
            count_after_first_load,
            "second ensure_window_loaded must be a no-op"
        );
    }

    #[test]
    fn align_window_rounds_outward() {
        assert_eq!(align_window(100, 200, 1000), (0, 999));
        assert_eq!(align_window(1500, 2500, 1000), (1000, 2999));
        assert_eq!(align_window(0, 0, 1000), (0, 999));
    }

    // -- prune tombstone forward-compat --

    #[test]
    fn prune_keeps_empty_range_tombstone() {
        // Forward-compat smoke: an empty range survives a "drop the LogRef"
        // operation (which any future pruning would apply). Today we model
        // this with insert_page over a no-log scan.
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![],
            end_block_hash: HASH_A,
        });
        // The empty range IS the tombstone — gap detection downstream
        // depends on its presence.
        assert_eq!(idx.len(), 1);
        let tree = idx.tree.lock().unwrap();
        let r = tree.get(&BlockPos::end_of_block(200)).unwrap();
        assert!(r.is_empty(), "empty range survives as a tombstone");
        assert_eq!(r.start, BlockPos::before_block(100));
        assert_eq!(r.end, BlockPos::end_of_block(200));
    }

    // -- navigation (cached-only) --

    #[tokio::test]
    async fn first_at_or_after_finds_next_cached_log() {
        let idx = Arc::new(mk_index());
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        });
        // We need the logs to exist in LogStore for resolve() to find them.
        idx.store
            .insert_logs(&FH, &[mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)])
            .unwrap();

        let log = idx.first_at_or_after(140, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));

        let log = idx.first_at_or_after(150, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));

        let log = idx.first_at_or_after(150, 1).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        // After the last cached item: empty-tail range is contiguous up to
        // block 200, but there are no items past 180; we return None.
        let none = idx.first_at_or_after(201, 0).unwrap();
        assert!(none.is_none(), "past cached region returns None");
    }

    #[tokio::test]
    async fn last_at_or_before_finds_prev_cached_log() {
        let idx = Arc::new(mk_index());
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        });
        idx.store
            .insert_logs(&FH, &[mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)])
            .unwrap();

        let log = idx.last_at_or_before(200, 0).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        let log = idx.last_at_or_before(180, 2).unwrap().unwrap();
        assert_eq!(log.block_number, Some(180));

        let log = idx.last_before(180, 2).unwrap().unwrap();
        assert_eq!(log.block_number, Some(150));
    }

    // -- items() with fetcher --

    #[tokio::test]
    async fn items_walks_filled_ranges_yields_in_order() {
        use futures_util::StreamExt;
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let logs = vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)];
        store.insert_logs(&FH, &logs).unwrap();
        let idx = Arc::new(LogIndex::new(store, FH, never_fetch()));
        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: logs,
            end_block_hash: HASH_B,
        });
        let mut stream = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let a = stream.next().await.unwrap().unwrap();
        assert_eq!(a.block_number, Some(150));
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(b.block_number, Some(180));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn items_into_uncached_gap_calls_store_and_caches() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log(150, 0, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(LogIndex::new(store.clone(), FH, fetcher));

        // First walk fetches.
        let mut stream = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.block_number, Some(150));
        drop(stream);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second walk over the same range should NOT refetch — the gap has
        // been backfilled both in the in-memory tree and in LogStore.
        let mut stream2 = idx
            .clone()
            .items(BlockPos::before_block(100), BlockPos::end_of_block(200));
        let first2 = stream2.next().await.unwrap().unwrap();
        assert_eq!(first2.block_number, Some(150));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no refetch on warm tree");

        // And the persisted scanned_ranges row is there.
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert!(!ranges.is_empty(), "scanned range was persisted");
    }

    // -- singleflight --

    #[tokio::test]
    async fn concurrent_searches_in_same_gap_fire_one_fetch() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    // Give peers time to enter the wait state.
                    sleep(Duration::from_millis(50)).await;
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log(150, 0, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(LogIndex::new(store, FH, fetcher));

        // Spawn 5 concurrent walkers over the same uncached gap.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let idx = idx.clone();
            handles.push(tokio::spawn(async move {
                let mut s = idx.items(BlockPos::before_block(100), BlockPos::end_of_block(200));
                s.next().await.map(|r| r.map(|log| log.block_number))
            }));
        }
        for h in handles {
            let result = h.await.unwrap();
            let block = result.unwrap().unwrap();
            assert_eq!(block, Some(150));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly one fetch served 5 concurrent walkers"
        );
    }

    // -- attach_drain: async sqlite persistence --

    #[tokio::test]
    async fn insert_page_drains_to_store_asynchronously() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let idx = LogIndex::new(store.clone(), FH, never_fetch());
        assert!(idx.attach_drain(), "first attach succeeds");
        assert!(!idx.attach_drain(), "second attach is a no-op");

        let page = LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A), mk_log(180, 2, HASH_B)],
            end_block_hash: HASH_B,
        };

        // The synchronous tree update must complete before the drain runs.
        // Right after insert_page returns, the tree has the entries but the
        // store has not been written yet (drain task hasn't been polled).
        idx.insert_page(&page);
        assert_eq!(idx.len(), 3, "tree updated synchronously");

        // Yield so the drain task can run.
        for _ in 0..10 {
            yield_now().await;
            sleep(Duration::from_millis(5)).await;
            let logs = store.get_logs_in_range(&FH, 100, 200).unwrap();
            if logs.len() == 2 {
                break;
            }
        }

        let logs = store.get_logs_in_range(&FH, 100, 200).unwrap();
        assert_eq!(logs.len(), 2, "drain wrote both logs to the store");
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert_eq!(ranges, vec![(100, 200)], "drain recorded the scanned range");
    }

    #[tokio::test]
    async fn insert_page_without_drain_does_not_touch_store() {
        let store = Arc::new(LogStore::open_in_memory().unwrap());
        let idx = LogIndex::new(store.clone(), FH, never_fetch());
        assert!(!idx.has_drain());

        idx.insert_page(&LogPage {
            from_block: 100,
            to_block: 200,
            items: vec![mk_log(150, 0, HASH_A)],
            end_block_hash: HASH_A,
        });

        // Give any background work a chance to run.
        sleep(Duration::from_millis(20)).await;
        assert_eq!(
            store.get_logs_in_range(&FH, 100, 200).unwrap().len(),
            0,
            "no drain attached — store remains empty"
        );
        assert!(
            store.get_scanned_ranges(&FH).unwrap().is_empty(),
            "no drain attached — scanned ranges remain empty"
        );
    }
}
