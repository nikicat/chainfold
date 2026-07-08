//! Cross-cutting tests over `RangeIndex<LogSpec>`: `BlockPos` ordering, page
//! insertion + the four late-item cases, lazy per-window load, the `items()`
//! walk, singleflight, async drain, and the R6 shared-index concurrency
//! coverage (parallel walkers, disjoint/overlapping gaps, reader vs. writer).
//!
//! Ported from the pre-R3 `log_index` suite; the old cached-only navigation
//! tests (`first_at_or_after` / `last_at_or_before`) were dropped with
//! `nav.rs` (dead since Phase 3).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData, U256};
use alloy::rpc::types::Log;
use futures_util::StreamExt;
use tokio::task::yield_now;
use tokio::time::sleep;

use super::gap_fill::never_fetch;
use super::range_tree::align_window;
use super::*;
use crate::arch::Timestamp;
use crate::evm::cache::{LogCache, ScanPartition};
use crate::evm::types::{BlockHash, BlockNumber, EvmEvent, EvmPosition, FilterHash, LogIdx};
use crate::shared::Shared;

const FH: FilterHash = FilterHash::new([0xAA; 32]);
const HASH_A: BlockHash = BlockHash::repeat_byte(0xBB);
const HASH_B: BlockHash = BlockHash::repeat_byte(0xBC);
/// Base for fixture block timestamps (`TS_BASE + block`).
const TS_BASE: Timestamp = Timestamp::new(1_700_000_000);

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
        block_hash: Some(block_hash.b256()),
        block_number: Some(block.get()),
        block_timestamp: None,
        transaction_hash: Some(B256::from([0xEE; 32])),
        transaction_index: Some(0),
        log_index: Some(u64::from(idx.get())),
        removed: false,
    }
}

/// Like [`mk_log`] but carrying its own `block_timestamp`, so
/// [`LogSpec::resolve`] never needs a cached block header (the
/// headers-present invariant, satisfied per-log).
fn mk_log_ts(block: BlockNumber, idx: LogIdx, block_hash: BlockHash) -> Log {
    let mut log = mk_log(block, idx, block_hash);
    log.block_timestamp = Some((TS_BASE + block.get()).get());
    log
}

/// A block hash unique to `block`. `LogSpec::resolve` keys the cache lookup on
/// `(block_hash, log_index)`, so the concurrency tests — which hold several
/// distinct blocks at once — must give each block its own hash or they'd all
/// resolve to the same cached log.
fn hash_for(block: BlockNumber) -> BlockHash {
    BlockHash::new(B256::from(U256::from(block.get())))
}

/// A timestamped log at `(block, log_index)` with a block-unique hash.
fn mk_log_uh(block: BlockNumber, log_index: LogIdx) -> Log {
    mk_log_ts(block, log_index, hash_for(block))
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
    store
        .record_scanned_range(
            &ScanPartition::Filter(FH),
            BlockRange::from_u64(100, 200),
            &HASH_A,
        )
        .unwrap();

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
        .insert_block_header(bn(150), &HASH_A, &BlockHash::ZERO, TS_BASE + 150)
        .unwrap();
    store
        .insert_block_header(bn(180), &HASH_B, &HASH_A, TS_BASE + 180)
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
    assert_eq!(a.item.timestamp, TS_BASE + 150);
    assert_eq!(a.pos, EvmPosition::new(150, 0));
    let b = stream.next().await.unwrap().unwrap();
    assert_eq!(b.item.block_number, bn(180));
    assert_eq!(b.item.timestamp, TS_BASE + 180);
    assert_eq!(b.pos, EvmPosition::new(180, 2));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn items_into_uncached_gap_calls_store_and_caches() {
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
    let ranges = store
        .get_scanned_ranges(&ScanPartition::Filter(FH))
        .unwrap();
    assert!(!ranges.is_empty(), "scanned range was persisted");
}

// -- singleflight --

#[tokio::test]
async fn concurrent_searches_in_same_gap_fire_one_fetch() {
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
        let logs = store
            .get_logs_in_range(&FH, BlockRange::from_u64(100, 200))
            .unwrap();
        if logs.len() == 2 {
            break;
        }
    }

    let logs = store
        .get_logs_in_range(&FH, BlockRange::from_u64(100, 200))
        .unwrap();
    assert_eq!(logs.len(), 2, "drain wrote both logs to the store");
    let ranges = store
        .get_scanned_ranges(&ScanPartition::Filter(FH))
        .unwrap();
    assert_eq!(
        ranges,
        vec![BlockRange::from_u64(100, 200)],
        "drain recorded the scanned range"
    );
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
        store
            .get_logs_in_range(&FH, BlockRange::from_u64(100, 200))
            .unwrap()
            .len(),
        0,
        "no drain attached — store remains empty"
    );
    assert!(
        store
            .get_scanned_ranges(&ScanPartition::Filter(FH))
            .unwrap()
            .is_empty(),
        "no drain attached — scanned ranges remain empty"
    );
}

// ============================================================
// R6: shared-index concurrency coverage
// ============================================================
//
// These exercise the tree's `Mutex`-serialized walk under scenarios the
// original single `concurrent_searches_in_same_gap_fire_one_fetch` didn't:
// true parallelism (multi-thread runtime), disjoint/overlapping gaps, and a
// writer (`insert_page`) mutating the tree while a reader is mid-walk. All
// rely on the invariant that `walk_step` holds no lock across an await, so
// concurrent readers/writers only ever interleave at await points — never
// corrupt the `BTreeMap`.

/// A fetcher for the concurrency tests: counts calls, records each requested
/// [`BlockRange`], sleeps `delay` (so peers reach the singleflight wait state
/// before the first fetch resolves), and returns a block-unique log for every
/// fixture block that falls in the range. Returned logs carry their own
/// `block_timestamp`, so resolution never needs a cached header — and
/// `finalize_gap_fill` persists them, so `resolve` finds them afterward.
fn recording_fetcher(
    calls: Arc<AtomicUsize>,
    fetched: Arc<Mutex<Vec<BlockRange>>>,
    blocks: Vec<BlockNumber>,
    delay: Duration,
) -> Fetcher<LogPage> {
    Arc::new(move |from: BlockNumber, to: BlockNumber| {
        calls.fetch_add(1, Ordering::SeqCst);
        fetched.lock().unwrap().push(BlockRange::new(from, to));
        let blocks = blocks.clone();
        Box::pin(async move {
            if !delay.is_zero() {
                sleep(delay).await;
            }
            let items = blocks
                .iter()
                .filter(|b| **b >= from && **b <= to)
                .map(|b| mk_log_uh(*b, LogIdx::ZERO))
                .collect();
            Ok(LogPage {
                from_block: from,
                to_block: to,
                items,
                end_block_hash: hash_for(to),
            })
        })
    })
}

/// Drain an `items()` stream to the block numbers it yields.
async fn collect_blocks<St>(mut stream: St) -> Vec<BlockNumber>
where
    St: futures_core::Stream<Item = Result<Positioned<EvmEvent, EvmPosition>, HistoryError>>
        + Unpin,
{
    let mut out = Vec::new();
    while let Some(r) = stream.next().await {
        out.push(r.unwrap().item.block_number);
    }
    out
}

/// Assert the recorded fetch ranges tile `[lo, hi]` exactly — no block is
/// fetched twice (the singleflight guarantee) and none is missed.
fn assert_ranges_tile(fetched: &[BlockRange], lo: BlockNumber, hi: BlockNumber) {
    let mut rs = fetched.to_vec();
    rs.sort_by_key(|r| r.from);
    let mut expect = lo;
    for r in &rs {
        assert_eq!(
            r.from, expect,
            "fetched ranges overlap or leave a gap: {rs:?}"
        );
        assert!(r.to >= r.from, "malformed fetched range {r:?}");
        expect = r.to + 1;
    }
    assert_eq!(
        expect,
        hi + 1,
        "fetched ranges must tile [{lo}, {hi}] exactly: {rs:?}"
    );
}

/// Parallel variant of `concurrent_searches_in_same_gap_fire_one_fetch`: the
/// walkers run on a real multi-thread runtime (truly concurrent, not just
/// cooperatively interleaved), yet singleflight still collapses them onto a
/// single fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_searches_same_gap_one_fetch_multi_thread() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = recording_fetcher(
        calls.clone(),
        fetched,
        vec![bn(150)],
        Duration::from_millis(50),
    );
    let idx = Arc::new(RangeIndex::new(LogSpec::new(mk_cache(), FH), fetcher));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let idx = idx.clone();
        handles.push(tokio::spawn(async move {
            let mut s = idx.items(
                BlockPos::before_block(bn(100)),
                BlockPos::end_of_block(bn(200)),
            );
            s.next().await.unwrap().unwrap().item.block_number
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), bn(150));
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one fetch served all parallel walkers"
    );
}

/// Two walkers over disjoint gaps must not coalesce: each fires its own fetch
/// for its own block range. Verifies gap computation + placeholder
/// installation stay per-gap under true parallelism.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disjoint_gaps_fetch_independently() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = recording_fetcher(
        calls.clone(),
        fetched.clone(),
        vec![bn(150), bn(350)],
        Duration::from_millis(30),
    );
    let idx = Arc::new(RangeIndex::new(LogSpec::new(mk_cache(), FH), fetcher));

    let a = {
        let idx = idx.clone();
        tokio::spawn(async move {
            let mut s = idx.items(
                BlockPos::before_block(bn(100)),
                BlockPos::end_of_block(bn(200)),
            );
            s.next().await.unwrap().unwrap().item.block_number
        })
    };
    let b = {
        let idx = idx.clone();
        tokio::spawn(async move {
            let mut s = idx.items(
                BlockPos::before_block(bn(300)),
                BlockPos::end_of_block(bn(400)),
            );
            s.next().await.unwrap().unwrap().item.block_number
        })
    };
    assert_eq!(a.await.unwrap(), bn(150));
    assert_eq!(b.await.unwrap(), bn(350));

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one fetch per disjoint gap"
    );
    let mut ranges = fetched.lock().unwrap().clone();
    ranges.sort_by_key(|r| r.from);
    assert_eq!(
        ranges,
        vec![
            BlockRange::from_u64(100, 200),
            BlockRange::from_u64(300, 400)
        ]
    );
}

/// Two walkers over overlapping windows share the overlap: however the race
/// resolves, the recorded fetches tile the union with no overlap — the shared
/// region is fetched exactly once — and each walker sees precisely the items
/// in its own window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_gaps_never_double_fetch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = recording_fetcher(
        calls.clone(),
        fetched.clone(),
        vec![bn(150), bn(250), bn(350)],
        Duration::from_millis(40),
    );
    let idx = Arc::new(RangeIndex::new(LogSpec::new(mk_cache(), FH), fetcher));

    // Windows overlap on blocks (200, 300].
    let a = {
        let idx = idx.clone();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                BlockPos::before_block(bn(100)),
                BlockPos::end_of_block(bn(300)),
            ))
            .await
        })
    };
    let b = {
        let idx = idx.clone();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                BlockPos::before_block(bn(200)),
                BlockPos::end_of_block(bn(400)),
            ))
            .await
        })
    };
    assert_eq!(a.await.unwrap(), vec![bn(150), bn(250)]);
    assert_eq!(b.await.unwrap(), vec![bn(250), bn(350)]);

    // Singleflight invariant across overlapping (not identical) gaps.
    assert_ranges_tile(&fetched.lock().unwrap(), bn(100), bn(400));
}

/// Reader vs. writer, deterministic. The walker is parked mid-walk inside the
/// first (front) gap fetch — holding a placeholder over only that gap, not the
/// rest of the window. While it is parked, a writer `insert_page`s a block
/// strictly ahead of the cursor, in a region with no in-flight placeholder. On
/// resume the walker must pick that block up exactly once, in order — proving
/// a concurrent live-path insert is neither dropped nor duplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_page_ahead_of_walk_cursor_is_yielded_once() {
    use tokio::sync::Notify;

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let fetcher: Fetcher<LogPage> = {
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        Arc::new(move |from: BlockNumber, to: BlockNumber| {
            // Only the first fetch (the front gap) parks; later tail-gap
            // fetches proceed immediately and return nothing.
            let n = calls.fetch_add(1, Ordering::SeqCst);
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                if n == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                let items = if from <= bn(150) && to >= bn(150) {
                    vec![mk_log_uh(bn(150), LogIdx::ZERO)]
                } else {
                    vec![]
                };
                Ok(LogPage {
                    from_block: from,
                    to_block: to,
                    items,
                    end_block_hash: hash_for(to),
                })
            })
        })
    };
    // The island (200) and the mid-walk insert (300) reach the tree via
    // `insert_page` only — no drain, no fetch — so their logs never hit the
    // cache that `resolve` reads. Seed them up front.
    let store = mk_cache();
    store
        .insert_logs(
            &FH,
            &[
                mk_log_uh(bn(200), LogIdx::ZERO),
                mk_log_uh(bn(300), LogIdx::ZERO),
            ],
        )
        .unwrap();
    let idx = Arc::new(RangeIndex::new(LogSpec::new(store, FH), fetcher));

    // A cached island at block 200 splits the walk into
    // gap[100,199] | cached@200 | gap. The walker parks in the first gap.
    idx.insert_page(&LogPage {
        from_block: bn(200),
        to_block: bn(200),
        items: vec![mk_log_uh(bn(200), LogIdx::ZERO)],
        end_block_hash: hash_for(bn(200)),
    });

    let walker = {
        let idx = idx.clone();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                BlockPos::before_block(bn(100)),
                BlockPos::end_of_block(bn(400)),
            ))
            .await
        })
    };

    // Wait until the walker is parked inside the front-gap fetch; its
    // placeholder covers only (before(100), before(200)].
    entered.notified().await;
    // Insert block 300 — strictly past the island, ahead of the cursor, with
    // no in-flight placeholder over it. Happens-before the release, so the
    // walker cannot have advanced past 200 yet.
    idx.insert_page(&LogPage {
        from_block: bn(300),
        to_block: bn(300),
        items: vec![mk_log_uh(bn(300), LogIdx::ZERO)],
        end_block_hash: hash_for(bn(300)),
    });
    release.notify_one();

    let blocks = walker.await.unwrap();
    assert_eq!(
        blocks,
        vec![bn(150), bn(200), bn(300)],
        "front-gap fetch (150), pre-seeded island (200), and the block \
         inserted mid-walk (300) — each yielded once, in order"
    );
}

/// Reader vs. writer, stressed. A walker streams a fully-cached window one
/// item at a time (yielding between each) while a writer concurrently splits
/// ranges inside the window with late items. Whatever the interleaving:
/// positions are strictly ascending (no duplicate / re-yield), every
/// pre-seeded item survives (no drop), nothing outside the known set appears,
/// and it never panics. `never_fetch` guarantees the walk stays gap-free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walk_races_late_inserts_no_dup_no_drop() {
    use std::collections::BTreeSet;

    let store = mk_cache();
    let idx = Arc::new(mk_index_over(store.share())); // never_fetch — any gap fails loudly.

    // A gap-free window [1, 300]: seeded logs every 10 blocks, plus late items
    // strictly inside the window that split existing ranges when they arrive.
    let seeded_blocks: Vec<BlockNumber> = (1..=30).map(|k| bn(k * 10)).collect();
    let late_blocks: Vec<BlockNumber> = vec![bn(55), bn(155), bn(255)];

    // `resolve` reads logs from the cache; `insert_page` only touches the tree,
    // so pre-seed every log (seeded + late) into the cache up front.
    let all_logs: Vec<Log> = seeded_blocks
        .iter()
        .chain(&late_blocks)
        .map(|b| mk_log_uh(*b, LogIdx::ZERO))
        .collect();
    store.insert_logs(&FH, &all_logs).unwrap();

    idx.insert_page(&LogPage {
        from_block: bn(1),
        to_block: bn(300),
        items: seeded_blocks
            .iter()
            .map(|b| mk_log_uh(*b, LogIdx::ZERO))
            .collect(),
        end_block_hash: hash_for(bn(300)),
    });

    // Writer: each late item may or may not be observed depending on where the
    // cursor is — but must never corrupt the walk.
    let writer = {
        let idx = idx.clone();
        let late_blocks = late_blocks.clone();
        tokio::spawn(async move {
            for b in late_blocks {
                idx.insert_page(&LogPage {
                    from_block: b,
                    to_block: b,
                    items: vec![mk_log_uh(b, LogIdx::ZERO)],
                    end_block_hash: hash_for(b),
                });
                yield_now().await;
            }
        })
    };

    // Reader: walk with a yield between items to widen the interleave.
    let mut stream = idx.clone().items(
        BlockPos::before_block(bn(1)),
        BlockPos::end_of_block(bn(300)),
    );
    let mut got: Vec<BlockNumber> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.unwrap().item.block_number);
        yield_now().await;
    }
    writer.await.unwrap();

    // Strictly ascending ⇒ no duplicate and no re-yield.
    for w in got.windows(2) {
        assert!(w[0] < w[1], "positions must be strictly ascending: {got:?}");
    }
    let got_set: BTreeSet<BlockNumber> = got.iter().copied().collect();
    let seeded_set: BTreeSet<BlockNumber> = seeded_blocks.iter().copied().collect();
    let late_set: BTreeSet<BlockNumber> = late_blocks.iter().copied().collect();
    // No drop: every seeded in-window item is present.
    for s in &seeded_set {
        assert!(got_set.contains(s), "seeded block {s} was dropped: {got:?}");
    }
    // No phantom items: only seeded or the known late inserts appear.
    for g in &got_set {
        assert!(
            seeded_set.contains(g) || late_set.contains(g),
            "unexpected block {g} yielded: {got:?}"
        );
    }
}
