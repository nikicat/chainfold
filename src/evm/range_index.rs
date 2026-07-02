//! Generic in-memory range tree over EVM range-based sources (logs, blocks).
//!
//! A `BTreeMap` keyed by the *end* position of each [`Range`], with
//! singleflight + late-item handling, generic over a [`RangeSpec`]. The tree
//! holds lightweight [`RangeSpec::Item`]s (a cache pointer for logs); the full
//! value is reconstructed on demand via [`RangeSpec::resolve`], so a
//! long-running history's resident memory is bounded by the number of distinct
//! positions (eviction is a future addition).
//!
//! Lazy loading: rather than slurping every scanned range from the durable
//! store on first access, the tree materializes per-window on demand via
//! [`RangeIndex::ensure_window_loaded`].
//!
//! Split across submodules:
//! - [`spec`] — the source-injected [`RangeSpec`]/[`IndexPos`] seam + [`Fetcher`].
//! - [`block_pos`] — the concrete EVM-log position/pointer types
//!   ([`BlockPos`], [`LogRef`], [`LogPage`]).
//! - [`range_tree`] — the [`Range`] entry type, page insertion (four late-item
//!   cases), the lazy load of scanned ranges, and the gap placeholder.
//! - [`gap_fill`] — [`RangeIndex::ensure_cached`], the [`RangeIndex::items`]
//!   async stream, and the fetcher-driven singleflight.
//!
//! This file owns the [`RangeIndex`] struct itself, its constructors and
//! accessors, [`RangeIndex::resolve`], and the async drain to the durable store.

use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::arch::{HistoryError, Positioned};
use crate::evm::types::{BlockRange, ChunkSize};

mod block_pos;
mod gap_fill;
mod log_spec;
mod range_tree;
mod spec;
mod store;

pub use block_pos::{BlockPos, LogPage, LogRef};
pub use log_spec::LogSpec;
pub use spec::{Fetcher, IndexPos, LoadedSpan, RangeSpec};
pub use store::{RangeIndexStore, TimePosFn};

use range_tree::Range;

// ============================================================
// RangeIndex
// ============================================================

/// Per-source in-memory range tree over a [`RangeSpec`] `S`.
///
/// One instance per `(durable store, filter)` pair. Cheap to construct — the
/// scanned ranges are pulled into the tree on demand by
/// [`Self::ensure_window_loaded`].
pub struct RangeIndex<S: RangeSpec> {
    spec: S,
    fetcher: Fetcher<S::Page>,
    /// Aligned scanned-range chunk size. Determines the granularity at
    /// which gaps get fetched and the alignment of scanned-range rows.
    chunk_size: ChunkSize,
    tree: Mutex<BTreeMap<S::Pos, Range<S>>>,
    /// Block windows already materialized into the tree. Tracked so a second
    /// visit to the same window does not re-slurp.
    loaded_windows: Mutex<Vec<BlockRange>>,
    /// Optional async drain. When attached, every page passed to
    /// [`Self::insert_page`] is cloned onto the channel and persisted by a
    /// background task. `None` means purely in-memory (the gap-fill path
    /// persists inline via [`RangeSpec::persist`]).
    drain_tx: Mutex<Option<UnboundedSender<S::Page>>>,
}

impl<S: RangeSpec> RangeIndex<S> {
    /// Construct an empty index over `spec`, fetching absent ranges via
    /// `fetcher`. Aligned chunk size defaults to `1000` blocks.
    pub fn new(spec: S, fetcher: Fetcher<S::Page>) -> Self {
        Self::with_chunk_size(spec, fetcher, ChunkSize::new(1000))
    }

    /// Like [`Self::new`] but with an explicit aligned chunk size.
    pub fn with_chunk_size(spec: S, fetcher: Fetcher<S::Page>, chunk_size: ChunkSize) -> Self {
        Self {
            spec,
            fetcher,
            chunk_size,
            tree: Mutex::new(BTreeMap::new()),
            loaded_windows: Mutex::new(Vec::new()),
            drain_tx: Mutex::new(None),
        }
    }

    /// The spec this index delegates storage/resolution to.
    pub(super) fn spec(&self) -> &S {
        &self.spec
    }

    /// Resolve a stored [`Item`](RangeSpec::Item) to the public positioned
    /// value — delegates to [`RangeSpec::resolve`] (synchronous cache read).
    pub(super) fn resolve(
        &self,
        item: &S::Item,
    ) -> Result<Positioned<S::Event, S::PubPos>, HistoryError> {
        self.spec.resolve(item)
    }

    /// Spawn a background task that persists every page passed to
    /// [`Self::insert_page`] via [`RangeSpec::persist`]. Returns `false` (and
    /// skips spawning) if a drain is already attached. Persistence failures
    /// are logged but never propagated — the in-memory tree is the source of
    /// truth on the live path.
    pub fn attach_drain(&self) -> bool {
        let mut drain = self.drain_tx.lock().unwrap();
        if drain.is_some() {
            return false;
        }
        let (tx, mut rx) = unbounded_channel::<S::Page>();
        let spec = self.spec.clone();
        tokio::spawn(async move {
            while let Some(page) = rx.recv().await {
                if let Err(e) = spec.persist(&page) {
                    tracing::warn!(error = %e, "range_index drain persist");
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

    /// Number of ranges currently in the tree. Test helper.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tree.lock().unwrap().len()
    }

    /// Whether the block window `[range.from, range.to]` has been
    /// materialized. Test helper.
    #[cfg(test)]
    pub(crate) fn window_loaded(&self, range: BlockRange) -> bool {
        self.loaded_windows
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.from <= range.from && w.to >= range.to)
    }
}

// ============================================================
// Shared test helpers + cross-cutting tests
// ============================================================

// Ported from the pre-R3 `log_index` suite, over `RangeIndex<LogSpec>`. The
// old cached-only navigation tests (`first_at_or_after` / `last_at_or_before`)
// were dropped with `nav.rs` itself (dead since Phase 3).
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData};
    use alloy::rpc::types::Log;
    use futures_util::StreamExt;
    use tokio::task::yield_now;
    use tokio::time::sleep;

    use super::gap_fill::never_fetch;
    use super::range_tree::align_window;
    use super::*;
    use crate::arch::Timestamp;
    use crate::evm::cache::LogCache;
    use crate::evm::types::{BlockHash, BlockNumber, EvmPosition, FilterHash, LogIdx, TxHash};
    use crate::shared::Shared;

    const FH: FilterHash = FilterHash::new([0xAA; 32]);
    const HASH_A: BlockHash = BlockHash::repeat_byte(0xBB);
    const HASH_B: BlockHash = BlockHash::repeat_byte(0xBC);
    /// Base for fixture block timestamps (`TS_BASE + block`).
    const TS_BASE: u64 = 1_700_000_000;

    /// Shorthand [`BlockNumber`] constructor — these tests are wall-to-wall
    /// block coordinates.
    fn bn(n: u64) -> BlockNumber {
        BlockNumber::from(n)
    }

    fn mk_log(block: BlockNumber, idx: LogIdx, block_hash: BlockHash) -> Log {
        let topic0 = B256::from([0xDD; 32]);
        let data = Bytes::from(vec![0x01]);
        let log_data = LogData::new_unchecked(vec![topic0], data);
        Log {
            inner: LogInner {
                address: Address::from([0x42u8; 20]),
                data: log_data,
            },
            block_hash: Some(block_hash),
            block_number: Some(block.get()),
            block_timestamp: None,
            transaction_hash: Some(TxHash::from([0xEE; 32])),
            transaction_index: Some(0),
            log_index: Some(u64::from(idx.get())),
            removed: false,
        }
    }

    /// Like [`mk_log`] but carrying its own `block_timestamp`, so
    /// [`LogSpec::resolve`] never needs a cached block header (the
    /// headers-present invariant, satisfied per-log).
    fn mk_log_ts(block: BlockNumber, idx: LogIdx, block_hash: B256) -> Log {
        let mut log = mk_log(block, idx, block_hash);
        log.block_timestamp = Some(TS_BASE + block.get());
        log
    }

    fn mk_cache() -> Shared<LogCache> {
        Shared::new(LogCache::open_in_memory().unwrap())
    }

    fn mk_index_over(store: Shared<LogCache>) -> RangeIndex<LogSpec> {
        RangeIndex::new(LogSpec::new(store, FH), never_fetch::<LogSpec>())
    }

    fn mk_index() -> RangeIndex<LogSpec> {
        mk_index_over(mk_cache())
    }

    // -- BlockPos ordering --

    #[test]
    fn block_pos_bottom_below_everything() {
        assert!(BlockPos::Bottom < BlockPos::at(bn(0), LogIdx::ZERO));
        assert!(BlockPos::Bottom < BlockPos::end_of_block(bn(0)));
        assert!(BlockPos::before_block(bn(0)) == BlockPos::Bottom);
        assert!(
            BlockPos::before_block(bn(1))
                == BlockPos::At {
                    block: bn(0),
                    log_index: LogIdx::MAX,
                }
        );
    }

    // -- insert_page basic shapes --

    #[test]
    fn insert_range_in_gap_creates_three_entries() {
        let idx = mk_index();
        // First page: blocks [100, 200] with two logs at (150, 0) and (180, 0).
        let page = LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![
                mk_log(bn(150), LogIdx::ZERO, HASH_A),
                mk_log(bn(180), LogIdx::ZERO, HASH_B),
            ],
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
            from_block: bn(100),
            to_block: bn(200),
            items: vec![mk_log(bn(150), LogIdx::ZERO, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before = idx.len();
        // Second page covering [201, 300] with one log at (250, 0).
        idx.insert_page(&LogPage {
            from_block: bn(201),
            to_block: bn(300),
            items: vec![mk_log(bn(250), LogIdx::ZERO, HASH_B)],
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
            from_block: bn(100),
            to_block: bn(300),
            items: vec![mk_log(bn(200), LogIdx::ZERO, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        // Now a late item arrives at (250, 0) — inside the tail empty
        // range (200, 300]. Should split into filled@250 + empty(250, 300].
        let mut tree = idx.tree.lock().unwrap();
        RangeIndex::<LogSpec>::insert_late_item(
            &mut tree,
            LogRef {
                block_number: bn(250),
                log_index: LogIdx::ZERO,
                block_hash: HASH_B,
            },
            BlockPos::at(bn(200), LogIdx::ZERO),
        );
        // before_len had: filled@(200, 0), empty(end_of_block(300)). After split:
        // filled@(200, 0), filled@(250, 0), empty(end_of_block(300)) — net +1.
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(bn(250), LogIdx::ZERO)));
        assert!(tree.contains_key(&BlockPos::end_of_block(bn(300))));
    }

    #[test]
    fn late_item_at_empty_range_end_upgrades_to_filled() {
        let idx = mk_index();
        // Page with no items so tree gets a single empty range ending at
        // end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![],
            end_block_hash: HASH_A,
        });
        assert_eq!(idx.len(), 1);
        let target_pos = BlockPos::end_of_block(bn(200));
        {
            let tree = idx.tree.lock().unwrap();
            assert!(tree.get(&target_pos).unwrap().is_empty());
        }
        let mut tree = idx.tree.lock().unwrap();
        // Late item lands exactly at the empty range's end position.
        RangeIndex::<LogSpec>::insert_late_item(
            &mut tree,
            LogRef {
                block_number: bn(200),
                log_index: LogIdx::MAX,
                block_hash: HASH_B,
            },
            BlockPos::at(bn(150), LogIdx::ZERO),
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
            from_block: bn(100),
            to_block: bn(200),
            items: vec![mk_log(bn(150), LogIdx::ZERO, HASH_A)],
            end_block_hash: HASH_A,
        });
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        // Same (block, log_index) as existing filled range end.
        RangeIndex::<LogSpec>::insert_late_item(
            &mut tree,
            LogRef {
                block_number: bn(150),
                log_index: LogIdx::ZERO,
                block_hash: HASH_B, // different hash — still ignored
            },
            BlockPos::at(bn(100), LogIdx::ZERO),
        );
        assert_eq!(tree.len(), before_len);
    }

    #[test]
    fn late_item_in_gap_inserts_filled_range() {
        let idx = mk_index();
        // First page [100, 200] with one log — gives filled@(150, 0)
        // and empty-tail at end_of_block(200).
        idx.insert_page(&LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![mk_log(bn(150), LogIdx::ZERO, HASH_A)],
            end_block_hash: HASH_A,
        });
        // Insert a late item at (90, 0) — in a true gap before everything.
        let before_len = idx.len();
        let mut tree = idx.tree.lock().unwrap();
        RangeIndex::<LogSpec>::insert_late_item(
            &mut tree,
            LogRef {
                block_number: bn(90),
                log_index: LogIdx::ZERO,
                block_hash: HASH_B,
            },
            BlockPos::before_block(bn(90)),
        );
        assert_eq!(tree.len(), before_len + 1);
        assert!(tree.contains_key(&BlockPos::at(bn(90), LogIdx::ZERO)));
    }

    // -- lazy per-window load --

    #[test]
    fn is_loaded_lazy_first_access_only() {
        let store = mk_cache();
        // Pre-populate the store as if a previous process had scanned [100, 200]
        // with one log at (150, 0).
        store
            .insert_logs(&FH, &[mk_log(bn(150), LogIdx::ZERO, HASH_A)])
            .unwrap();
        store.record_scanned_range(&FH, 100, 200, &HASH_A).unwrap();

        let idx = RangeIndex::with_chunk_size(
            LogSpec::new(store, FH),
            never_fetch::<LogSpec>(),
            ChunkSize::new(1000),
        );
        assert_eq!(idx.len(), 0, "tree starts empty");

        idx.ensure_window_loaded(BlockRange::from_u64(100, 200))
            .unwrap();
        assert!(idx.window_loaded(BlockRange::from_u64(100, 200)));
        assert!(
            idx.len() >= 2,
            "expected at least filled@150 + empty-tail entries"
        );

        let count_after_first_load = idx.len();
        idx.ensure_window_loaded(BlockRange::from_u64(100, 200))
            .unwrap();
        assert_eq!(
            idx.len(),
            count_after_first_load,
            "second ensure_window_loaded must be a no-op"
        );
    }

    #[test]
    fn align_window_rounds_outward() {
        let chunk = ChunkSize::new(1000);
        assert_eq!(
            align_window(BlockRange::from_u64(100, 200), chunk),
            BlockRange::from_u64(0, 999)
        );
        assert_eq!(
            align_window(BlockRange::from_u64(1500, 2500), chunk),
            BlockRange::from_u64(1000, 2999)
        );
        assert_eq!(
            align_window(BlockRange::from_u64(0, 0), chunk),
            BlockRange::from_u64(0, 999)
        );
    }

    // -- prune tombstone forward-compat --

    #[test]
    fn prune_keeps_empty_range_tombstone() {
        // Forward-compat smoke: an empty range survives a "drop the LogRef"
        // operation (which any future pruning would apply). Today we model
        // this with insert_page over a no-log scan.
        let idx = mk_index();
        idx.insert_page(&LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![],
            end_block_hash: HASH_A,
        });
        // The empty range IS the tombstone — gap detection downstream
        // depends on its presence.
        assert_eq!(idx.len(), 1);
        let tree = idx.tree.lock().unwrap();
        let r = tree.get(&BlockPos::end_of_block(bn(200))).unwrap();
        assert!(r.is_empty(), "empty range survives as a tombstone");
        assert_eq!(r.start, BlockPos::before_block(bn(100)));
        assert_eq!(r.end, BlockPos::end_of_block(bn(200)));
    }

    // -- items() with fetcher --

    #[tokio::test]
    async fn items_walks_filled_ranges_yields_in_order() {
        let store = mk_cache();
        let logs = vec![
            mk_log(bn(150), LogIdx::ZERO, HASH_A),
            mk_log(bn(180), LogIdx::new(2), HASH_B),
        ];
        store.insert_logs(&FH, &logs).unwrap();
        // Fixture logs carry no block_timestamp, so resolution exercises
        // the headers-present path: both block headers must be cached.
        store
            .insert_block_header(150, &HASH_A, &BlockHash::ZERO, TS_BASE + 150)
            .unwrap();
        store
            .insert_block_header(180, &HASH_B, &HASH_A, TS_BASE + 180)
            .unwrap();
        let idx = Arc::new(mk_index_over(store));
        idx.insert_page(&LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: logs,
            end_block_hash: HASH_B,
        });
        let mut stream = idx.clone().items(
            BlockPos::before_block(bn(100)),
            BlockPos::end_of_block(bn(200)),
        );
        let a = stream.next().await.unwrap().unwrap();
        assert_eq!(a.item.block_number, bn(150));
        assert_eq!(a.item.timestamp, Timestamp::new(TS_BASE + 150));
        assert_eq!(a.pos, EvmPosition::new(150, 0));
        let b = stream.next().await.unwrap().unwrap();
        assert_eq!(b.item.block_number, bn(180));
        assert_eq!(b.item.timestamp, Timestamp::new(TS_BASE + 180));
        assert_eq!(b.pos, EvmPosition::new(180, 2));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn items_into_uncached_gap_calls_store_and_caches() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = mk_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher<LogPage> = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log_ts(bn(150), LogIdx::ZERO, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(RangeIndex::new(LogSpec::new(store.share(), FH), fetcher));

        // First walk fetches.
        let mut stream = idx.clone().items(
            BlockPos::before_block(bn(100)),
            BlockPos::end_of_block(bn(200)),
        );
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.item.block_number, bn(150));
        drop(stream);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second walk over the same range should NOT refetch — the gap has
        // been backfilled both in the in-memory tree and in the store.
        let mut stream2 = idx.clone().items(
            BlockPos::before_block(bn(100)),
            BlockPos::end_of_block(bn(200)),
        );
        let first2 = stream2.next().await.unwrap().unwrap();
        assert_eq!(first2.item.block_number, bn(150));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no refetch on warm tree");

        // And the persisted scanned_ranges row is there.
        let ranges = store.get_scanned_ranges(&FH).unwrap();
        assert!(!ranges.is_empty(), "scanned range was persisted");
    }

    // -- singleflight --

    #[tokio::test]
    async fn concurrent_searches_in_same_gap_fire_one_fetch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = mk_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher<LogPage> = {
            let calls = calls.clone();
            Arc::new(move |from, to| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    // Give peers time to enter the wait state.
                    sleep(Duration::from_millis(50)).await;
                    Ok(LogPage {
                        from_block: from,
                        to_block: to,
                        items: vec![mk_log_ts(bn(150), LogIdx::ZERO, HASH_A)],
                        end_block_hash: HASH_A,
                    })
                })
            })
        };
        let idx = Arc::new(RangeIndex::new(LogSpec::new(store, FH), fetcher));

        // Spawn 5 concurrent walkers over the same uncached gap.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let idx = idx.clone();
            handles.push(tokio::spawn(async move {
                let mut s = idx.items(
                    BlockPos::before_block(bn(100)),
                    BlockPos::end_of_block(bn(200)),
                );
                s.next().await.map(|r| r.map(|p| p.item.block_number))
            }));
        }
        for h in handles {
            let result = h.await.unwrap();
            let block = result.unwrap().unwrap();
            assert_eq!(block, bn(150));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly one fetch served 5 concurrent walkers"
        );
    }

    // -- attach_drain: async persistence --

    #[tokio::test]
    async fn insert_page_drains_to_store_asynchronously() {
        let store = mk_cache();
        let idx = mk_index_over(store.share());
        assert!(idx.attach_drain(), "first attach succeeds");
        assert!(!idx.attach_drain(), "second attach is a no-op");

        let page = LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![
                mk_log(bn(150), LogIdx::ZERO, HASH_A),
                mk_log(bn(180), LogIdx::new(2), HASH_B),
            ],
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
        let store = mk_cache();
        let idx = mk_index_over(store.share());
        assert!(!idx.has_drain());

        idx.insert_page(&LogPage {
            from_block: bn(100),
            to_block: bn(200),
            items: vec![mk_log(bn(150), LogIdx::ZERO, HASH_A)],
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
