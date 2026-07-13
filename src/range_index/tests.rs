//! Cross-cutting tests over `RangeIndex<MockSpec>`: page insertion + the four
//! late-item cases, lazy per-window load, the `items()` walk, singleflight,
//! async drain, and the R6 shared-index concurrency coverage (parallel
//! walkers, disjoint/overlapping gaps, reader vs. writer).
//!
//! Runs featureless over the synthetic [`MockSpec`] (see [`testkit`]) —
//! source-specific resolution semantics live with the concrete specs
//! (`evm::log_spec` / `evm::block_spec` tests).
//!
//! Ported from the pre-R3 `log_index` suite; the old cached-only navigation
//! tests (`first_at_or_after` / `last_at_or_before`) were dropped with
//! `nav.rs` (dead since Phase 3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::unfold;
use tokio::sync::Semaphore;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout};

use super::gap_fill::never_fetch;
use super::testkit::{MockItem, MockPage, MockPos, MockPubPos, MockSpec, MockStore};
use super::*;
use crate::chain::BlockNumber;
use crate::share::Share;

/// Shorthand [`BlockNumber`] constructor — these tests are wall-to-wall
/// block coordinates.
fn bn(n: u64) -> BlockNumber {
    BlockNumber::from(n)
}

/// An item at `(block, 0)` — most fixtures need one item per block.
fn item(block: u64) -> MockItem {
    MockItem::new(bn(block), 0)
}

/// A page over `[from, to]` carrying `items`.
fn page(from: u64, to: u64, items: Vec<MockItem>) -> MockPage {
    MockPage {
        range: BlockRange::from_u64(from, to),
        items,
    }
}

fn mk_store() -> Arc<MockStore> {
    Arc::new(MockStore::default())
}

fn mk_index_over(store: Arc<MockStore>) -> RangeIndex<MockSpec> {
    RangeIndex::new(MockSpec::new(store), never_fetch::<MockSpec>())
}

fn mk_index() -> RangeIndex<MockSpec> {
    mk_index_over(mk_store())
}

// -- insert_page basic shapes --

#[test]
fn insert_range_in_gap_creates_three_entries() {
    let idx = mk_index();
    // First page: blocks [100, 200] with two items at (150, 0) and (180, 0).
    idx.insert_page(&page(100, 200, vec![item(150), item(180)]));
    // Expect: filled@150, filled@180, empty tail @ end_of_block(200) = 3 entries.
    assert_eq!(idx.len(), 3);
}

#[test]
fn insert_range_at_existing_end_extends_not_duplicates() {
    let idx = mk_index();
    idx.insert_page(&page(100, 200, vec![item(150)]));
    let before = idx.len();
    // Second page covering [201, 300] with one item at (250, 0).
    idx.insert_page(&page(201, 300, vec![item(250)]));
    // Expect new filled@250 + new empty-tail past 250 = 2 more entries.
    assert_eq!(idx.len(), before + 2);
}

// -- late_item insertion (4 cases) --

#[test]
fn late_item_inside_existing_range_splits_it() {
    let idx = mk_index();
    // Set up: page with single item at (200, 0) covering [100, 300].
    idx.insert_page(&page(100, 300, vec![item(200)]));
    let before_len = idx.len();
    // Now a late item arrives at (250, 0) — inside the tail empty
    // range (200, 300]. Should split into filled@250 + empty(250, 300].
    let mut tree = idx.tree.lock().unwrap();
    RangeIndex::<MockSpec>::insert_late_item(&mut tree, item(250), MockPos::at(bn(200), 0));
    // before_len had: filled@(200, 0), empty(end_of_block(300)). After split:
    // filled@(200, 0), filled@(250, 0), empty(end_of_block(300)) — net +1.
    assert_eq!(tree.len(), before_len + 1);
    assert!(tree.contains_key(&MockPos::at(bn(250), 0)));
    assert!(tree.contains_key(&MockPos::end_of_block(bn(300))));
}

#[test]
fn late_item_at_empty_range_end_upgrades_to_filled() {
    let idx = mk_index();
    // Page with no items so tree gets a single empty range ending at
    // end_of_block(200).
    idx.insert_page(&page(100, 200, vec![]));
    assert_eq!(idx.len(), 1);
    let target_pos = MockPos::end_of_block(bn(200));
    {
        let tree = idx.tree.lock().unwrap();
        assert!(tree.get(&target_pos).unwrap().is_empty());
    }
    let mut tree = idx.tree.lock().unwrap();
    // Late item lands exactly at the empty range's end position.
    RangeIndex::<MockSpec>::insert_late_item(
        &mut tree,
        MockItem::new(bn(200), u32::MAX),
        MockPos::at(bn(150), 0),
    );
    // Count unchanged; the empty was upgraded in-place.
    assert_eq!(tree.len(), 1);
    let r = tree.get(&target_pos).unwrap();
    assert!(!r.is_empty());
}

#[test]
fn late_item_duplicate_is_ignored() {
    let idx = mk_index();
    idx.insert_page(&page(100, 200, vec![item(150)]));
    let before_len = idx.len();
    let mut tree = idx.tree.lock().unwrap();
    // Same (block, idx) as the existing filled range end.
    RangeIndex::<MockSpec>::insert_late_item(&mut tree, item(150), MockPos::at(bn(100), 0));
    assert_eq!(tree.len(), before_len);
}

#[test]
fn late_item_in_gap_inserts_filled_range() {
    let idx = mk_index();
    // First page [100, 200] with one item — gives filled@(150, 0)
    // and empty-tail at end_of_block(200).
    idx.insert_page(&page(100, 200, vec![item(150)]));
    // Insert a late item at (90, 0) — in a true gap before everything.
    let before_len = idx.len();
    let mut tree = idx.tree.lock().unwrap();
    RangeIndex::<MockSpec>::insert_late_item(&mut tree, item(90), MockPos::before_block(bn(90)));
    assert_eq!(tree.len(), before_len + 1);
    assert!(tree.contains_key(&MockPos::at(bn(90), 0)));
}

// -- lazy per-window load --

#[test]
fn is_loaded_lazy_first_access_only() {
    let store = mk_store();
    // Pre-populate the store as if a previous process had scanned [100, 200]
    // with one item at (150, 0).
    store.insert_items(&[item(150)]);
    store.record_scanned(BlockRange::from_u64(100, 200));

    let idx = RangeIndex::with_chunk_size(
        MockSpec::new(store),
        never_fetch::<MockSpec>(),
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

// -- prune tombstone forward-compat --

#[test]
fn prune_keeps_empty_range_tombstone() {
    // Forward-compat smoke: an empty range survives a "drop the item"
    // operation (which any future pruning would apply). Today we model
    // this with insert_page over a no-item scan.
    let idx = mk_index();
    idx.insert_page(&page(100, 200, vec![]));
    // The empty range IS the tombstone — gap detection downstream
    // depends on its presence.
    assert_eq!(idx.len(), 1);
    let tree = idx.tree.lock().unwrap();
    let r = tree.get(&MockPos::end_of_block(bn(200))).unwrap();
    assert!(r.is_empty(), "empty range survives as a tombstone");
    assert_eq!(r.start, MockPos::before_block(bn(100)));
    assert_eq!(r.end, MockPos::end_of_block(bn(200)));
}

// -- items() with fetcher --

#[tokio::test]
async fn items_walks_filled_ranges_yields_in_order() {
    let idx = Arc::new(mk_index());
    idx.insert_page(&page(100, 200, vec![item(150), MockItem::new(bn(180), 2)]));
    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(200)),
    );
    let a = stream.next().await.unwrap().unwrap();
    assert_eq!(a.item, item(150));
    assert_eq!(a.pos, MockPubPos::new(bn(150), 0));
    let b = stream.next().await.unwrap().unwrap();
    assert_eq!(b.item, MockItem::new(bn(180), 2));
    assert_eq!(b.pos, MockPubPos::new(bn(180), 2));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn items_into_uncached_gap_calls_store_and_caches() {
    let store = mk_store();
    let calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let calls = calls.share();
        Arc::new(SinglePageFetcher(move |range: BlockRange| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(MockPage {
                    range,
                    items: vec![item(150)],
                })
            }
        }))
    };
    let idx = Arc::new(RangeIndex::new(MockSpec::new(store.share()), fetcher));

    // First walk fetches.
    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(200)),
    );
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.item.block, bn(150));
    drop(stream);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Second walk over the same range should NOT refetch — the gap has
    // been backfilled both in the in-memory tree and in the store.
    let mut stream2 = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(200)),
    );
    let first2 = stream2.next().await.unwrap().unwrap();
    assert_eq!(first2.item.block, bn(150));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no refetch on warm tree");

    // And the persisted coverage row is there.
    assert!(
        !store.scanned_ranges().is_empty(),
        "scanned range was persisted"
    );
}

// -- singleflight --

#[tokio::test]
async fn concurrent_searches_in_same_gap_fire_one_fetch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let calls = calls.share();
        Arc::new(SinglePageFetcher(move |range: BlockRange| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                // Give peers time to enter the wait state.
                sleep(Duration::from_millis(50)).await;
                Ok(MockPage {
                    range,
                    items: vec![item(150)],
                })
            }
        }))
    };
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    // Spawn 5 concurrent walkers over the same uncached gap.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let idx = idx.share();
        handles.push(tokio::spawn(async move {
            let mut s = idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(200)),
            );
            s.next().await.map(|r| r.map(|p| p.item.block))
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
    let store = mk_store();
    let idx = mk_index_over(store.share());
    assert!(idx.attach_drain(), "first attach succeeds");
    assert!(!idx.attach_drain(), "second attach is a no-op");

    let pg = page(100, 200, vec![item(150), MockItem::new(bn(180), 2)]);

    // The synchronous tree update must complete before the drain runs.
    // Right after insert_page returns, the tree has the entries but the
    // store has not been written yet (drain task hasn't been polled).
    idx.insert_page(&pg);
    assert_eq!(idx.len(), 3, "tree updated synchronously");

    // Yield so the drain task can run.
    for _ in 0..10 {
        yield_now().await;
        sleep(Duration::from_millis(5)).await;
        if store.items_in(BlockRange::from_u64(100, 200)).len() == 2 {
            break;
        }
    }

    assert_eq!(
        store.items_in(BlockRange::from_u64(100, 200)).len(),
        2,
        "drain wrote both items to the store"
    );
    assert_eq!(
        store.scanned_ranges(),
        vec![BlockRange::from_u64(100, 200)],
        "drain recorded the scanned range"
    );
}

#[tokio::test]
async fn insert_page_without_drain_does_not_touch_store() {
    let store = mk_store();
    let idx = mk_index_over(store.share());
    assert!(!idx.has_drain());

    idx.insert_page(&page(100, 200, vec![item(150)]));

    // Give any background work a chance to run.
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        store.items_in(BlockRange::from_u64(100, 200)).len(),
        0,
        "no drain attached — store remains empty"
    );
    assert!(
        store.scanned_ranges().is_empty(),
        "no drain attached — scanned ranges remain empty"
    );
}

// -- chunked gap fill --

/// A cold walk over a multi-chunk gap fetches one aligned chunk at a time —
/// never a request spanning a chunk boundary. Guards the RPC range cap that
/// providers enforce (and the peers-share-cache-entries alignment property).
#[tokio::test]
async fn gap_fill_fetches_in_aligned_chunks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = recording_fetcher(
        calls.share(),
        fetched.share(),
        vec![bn(985)],
        Duration::ZERO,
    );
    let idx = Arc::new(RangeIndex::with_chunk_size(
        MockSpec::new(mk_store()),
        fetcher,
        ChunkSize::new(10),
    ));

    // 21 blocks [980, 1000] at chunk 10 → three fetches, tail clamped.
    let blocks = collect_blocks(idx.share().items(
        MockPos::before_block(bn(980)),
        MockPos::end_of_block(bn(1000)),
    ))
    .await;

    assert_eq!(blocks, vec![bn(985)]);
    assert_eq!(
        *fetched.lock().unwrap(),
        vec![
            BlockRange::from_u64(980, 989),
            BlockRange::from_u64(990, 999),
            BlockRange::from_u64(1000, 1000),
        ]
    );
}

/// A gap starting mid-chunk fetches a partial leading chunk from the cursor
/// — never rounded down over already-covered blocks — then lands on aligned
/// boundaries for the rest.
#[tokio::test]
async fn gap_fill_mid_chunk_start_is_not_rounded_down() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = recording_fetcher(calls.share(), fetched.share(), vec![], Duration::ZERO);
    let idx = Arc::new(RangeIndex::with_chunk_size(
        MockSpec::new(mk_store()),
        fetcher,
        ChunkSize::new(10),
    ));

    let blocks = collect_blocks(idx.share().items(
        MockPos::before_block(bn(985)),
        MockPos::end_of_block(bn(1005)),
    ))
    .await;

    assert!(blocks.is_empty());
    assert_eq!(
        *fetched.lock().unwrap(),
        vec![
            BlockRange::from_u64(985, 989),
            BlockRange::from_u64(990, 999),
            BlockRange::from_u64(1000, 1005),
        ]
    );
}

// ============================================================
// Streaming page contract — per-page finalize
// ============================================================

/// A scripted streaming fetcher: each `fetch` call pops the next script (a
/// list of page results emitted as one stream), and every emission first
/// acquires one gate permit — so tests control exactly when each page
/// lands. Panics on a fetch call with no script left, making warm re-walks
/// fetch-free by construction.
struct ScriptedFetcher {
    scripts: Mutex<VecDeque<Vec<Result<MockPage, HistoryError>>>>,
    gate: Arc<Semaphore>,
    fetched: Mutex<Vec<BlockRange>>,
}

impl ScriptedFetcher {
    /// Starts fully gated — nothing emits until [`Self::release_pages`].
    fn new(scripts: Vec<Vec<Result<MockPage, HistoryError>>>) -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(scripts.into()),
            gate: Arc::new(Semaphore::new(0)),
            fetched: Mutex::new(Vec::new()),
        })
    }

    /// Let `n` more pages (or stream errors) emit.
    fn release_pages(&self, n: usize) {
        self.gate.add_permits(n);
    }

    /// Every `fetch` call's requested range, in call order.
    fn fetch_calls(&self) -> Vec<BlockRange> {
        self.fetched.lock().unwrap().clone()
    }
}

impl PageFetcher<MockPage> for ScriptedFetcher {
    fn fetch(&self, range: BlockRange) -> PageStream<MockPage> {
        self.fetched.lock().unwrap().push(range);
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected fetch call: no script left");
        let gate = self.gate.share();
        Box::pin(unfold(
            (script.into_iter(), gate),
            |(mut pages, gate)| async move {
                let next = pages.next()?;
                gate.acquire().await.expect("gate closed").forget();
                Some((next, (pages, gate)))
            },
        ))
    }
}

/// Each delivered page finalizes on arrival: its items yield and its
/// coverage persists while later pages of the same gap are still in
/// flight — time-to-first-item is one page, not the whole gap.
#[tokio::test]
async fn pages_finalize_incrementally() {
    let store = mk_store();
    let fetcher = ScriptedFetcher::new(vec![vec![
        Ok(page(100, 170, vec![item(150)])),
        Ok(page(171, 240, vec![item(200)])),
        Ok(page(241, 300, vec![item(280)])),
    ]]);
    let idx = Arc::new(RangeIndex::new(
        MockSpec::new(store.share()),
        fetcher.share(),
    ));

    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );

    // Page 1 lands; its item yields while pages 2-3 are still gated.
    fetcher.release_pages(1);
    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first item must yield after page 1, before later pages")
        .unwrap()
        .unwrap();
    assert_eq!(first.item.block, bn(150));
    assert_eq!(
        store.scanned_ranges(),
        vec![BlockRange::from_u64(100, 170)],
        "page 1 persisted before pages 2-3 landed"
    );

    fetcher.release_pages(2);
    assert_eq!(collect_blocks(stream).await, vec![bn(200), bn(280)]);
    assert_eq!(
        store.scanned_ranges(),
        vec![
            BlockRange::from_u64(100, 170),
            BlockRange::from_u64(171, 240),
            BlockRange::from_u64(241, 300),
        ],
        "one coverage row per page"
    );
    assert_eq!(fetcher.fetch_calls(), vec![BlockRange::from_u64(100, 300)]);

    // Warm re-walk: fully cached — a fetch would panic (no script left).
    let again = collect_blocks(idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    ))
    .await;
    assert_eq!(again, vec![bn(150), bn(200), bn(280)]);
}

/// An `Err` item ends the stream: the delivered prefix stands as coverage,
/// the suffix stays a gap — and the retry fetches only that suffix.
#[tokio::test]
async fn stream_error_keeps_prefix_suffix_stays_gap() {
    let store = mk_store();
    let fetcher = ScriptedFetcher::new(vec![
        vec![
            Ok(page(100, 200, vec![item(150)])),
            Err(HistoryError::internal("mid-gap failure")),
        ],
        vec![Ok(page(201, 300, vec![item(250)]))],
    ]);
    fetcher.release_pages(10); // no gating in this test
    let idx = Arc::new(RangeIndex::new(
        MockSpec::new(store.share()),
        fetcher.share(),
    ));

    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(
        first.item.block,
        bn(150),
        "prefix item yields before the error"
    );
    assert!(
        stream.next().await.unwrap().is_err(),
        "the stream error surfaces to the walker"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(
        store.scanned_ranges(),
        vec![BlockRange::from_u64(100, 200)],
        "the delivered prefix is persisted coverage"
    );

    // The retry walks the cached prefix and fetches ONLY the suffix.
    let again = collect_blocks(idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    ))
    .await;
    assert_eq!(again, vec![bn(150), bn(250)]);
    assert_eq!(
        fetcher.fetch_calls(),
        vec![
            BlockRange::from_u64(100, 300),
            BlockRange::from_u64(201, 300),
        ],
        "the retry covers only the suffix past the delivered prefix"
    );
}

/// A stream that ends cleanly *before* covering its gap is a contract
/// violation surfaced as a loud error — silently keeping the suffix a gap
/// would make the very next walk re-fetch it, looping forever against a
/// deterministic short-streaming fetcher. The delivered prefix stands.
#[tokio::test]
async fn stream_ending_short_of_gap_errors_loudly() {
    let store = mk_store();
    let fetcher = ScriptedFetcher::new(vec![
        vec![Ok(page(100, 200, vec![item(150)]))], // ends short of 300
        vec![Ok(page(201, 300, vec![item(250)]))], // the retry's suffix fetch
    ]);
    fetcher.release_pages(10);
    let idx = Arc::new(RangeIndex::new(
        MockSpec::new(store.share()),
        fetcher.share(),
    ));

    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    assert_eq!(stream.next().await.unwrap().unwrap().item.block, bn(150));
    let err = stream.next().await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("ended before covering"),
        "the short stream must surface as a contract-violation error: {err}"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(
        store.scanned_ranges(),
        vec![BlockRange::from_u64(100, 200)],
        "the delivered prefix stands as coverage"
    );

    // The retry walks the cached prefix and fetches only the suffix.
    let again = collect_blocks(idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    ))
    .await;
    assert_eq!(again, vec![bn(150), bn(250)]);
    assert_eq!(
        fetcher.fetch_calls(),
        vec![
            BlockRange::from_u64(100, 300),
            BlockRange::from_u64(201, 300),
        ]
    );
}

/// A persist failure on an *interior* page aborts the remainder exactly
/// like a fetch error: the shrunk placeholder is released, the suffix
/// stays a gap, and the retry re-fetches only the suffix. (The failed
/// page recorded no durable coverage; in-process its items still serve
/// from the tree.)
#[tokio::test]
async fn persist_error_on_interior_page_aborts_the_suffix() {
    let store = mk_store();
    let fetcher = ScriptedFetcher::new(vec![
        vec![
            Ok(page(100, 200, vec![item(150)])), // persist fails on this one
            Ok(page(201, 300, vec![item(250)])), // never reached
        ],
        vec![Ok(page(201, 300, vec![item(250)]))], // the retry's suffix fetch
    ]);
    fetcher.release_pages(10);
    let idx = Arc::new(RangeIndex::new(
        MockSpec::new(store.share()),
        fetcher.share(),
    ));

    store.fail_next_persist();
    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    assert!(
        stream.next().await.unwrap().is_err(),
        "the persist failure surfaces to the walker"
    );
    assert!(stream.next().await.is_none());
    assert!(
        store.scanned_ranges().is_empty(),
        "the failed page recorded no durable coverage"
    );

    // Retry: the prefix serves from the in-memory tree; only the suffix is
    // fetched, and its coverage row is the only one recorded.
    let again = timeout(
        Duration::from_secs(5),
        collect_blocks(idx.share().items(
            MockPos::before_block(bn(100)),
            MockPos::end_of_block(bn(300)),
        )),
    )
    .await
    .expect("the aborted fill must not strand its shrunk placeholder");
    assert_eq!(again, vec![bn(150), bn(250)]);
    assert_eq!(
        fetcher.fetch_calls(),
        vec![
            BlockRange::from_u64(100, 300),
            BlockRange::from_u64(201, 300),
        ]
    );
    assert_eq!(store.scanned_ranges(), vec![BlockRange::from_u64(201, 300)]);
}

/// A peer parked on an in-flight fill advances as soon as a page lands —
/// it does not wait for the whole gap.
#[tokio::test]
async fn waiter_advances_on_partial_delivery() {
    let fetcher = ScriptedFetcher::new(vec![vec![
        Ok(page(100, 200, vec![item(150)])),
        Ok(page(201, 300, vec![item(250)])),
    ]]);
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher.share()));

    // Owner: installs the placeholder, parks awaiting page 1's permit.
    let owner = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(300)),
            ))
            .await
        })
    };
    while fetcher.fetch_calls().is_empty() {
        yield_now().await;
    }

    // Waiter: coalesces onto the owner's fill; parked while nothing landed.
    let mut waiter = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    assert!(
        timeout(Duration::from_millis(100), waiter.next())
            .await
            .is_err(),
        "no page landed yet — the waiter must still be parked"
    );

    // Page 1 lands; page 2 stays gated — the waiter advances regardless.
    fetcher.release_pages(1);
    let first = timeout(Duration::from_secs(5), waiter.next())
        .await
        .expect("waiter must advance once page 1 lands, before the gap completes")
        .unwrap()
        .unwrap();
    assert_eq!(first.item.block, bn(150));

    fetcher.release_pages(1);
    assert_eq!(collect_blocks(waiter).await, vec![bn(250)]);
    assert_eq!(owner.await.unwrap(), vec![bn(150), bn(250)]);
    assert_eq!(
        fetcher.fetch_calls().len(),
        1,
        "one fetch stream served both walkers"
    );
}

/// Dropping a walker mid-fill releases the placeholder remainder — a later
/// walker must be able to fill the suffix instead of parking forever on an
/// orphaned notify.
#[tokio::test]
async fn dropping_mid_fill_walker_frees_placeholder() {
    let fetcher = ScriptedFetcher::new(vec![
        vec![
            Ok(page(100, 200, vec![item(150)])),
            Ok(page(201, 300, vec![item(250)])), // never released to the owner
        ],
        vec![Ok(page(201, 300, vec![item(250)]))], // the retry's suffix fetch
    ]);
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher.share()));

    let mut owner = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    fetcher.release_pages(1);
    let first = owner.next().await.unwrap().unwrap();
    assert_eq!(first.item.block, bn(150));
    // Page 2 never landed: the fill still holds the placeholder over the
    // suffix. Dropping the walker must release it.
    drop(owner);

    fetcher.release_pages(10);
    let blocks = timeout(
        Duration::from_secs(5),
        collect_blocks(idx.share().items(
            MockPos::before_block(bn(100)),
            MockPos::end_of_block(bn(300)),
        )),
    )
    .await
    .expect("dropped walker must not strand its placeholder");
    assert_eq!(blocks, vec![bn(150), bn(250)]);
    assert_eq!(
        fetcher.fetch_calls(),
        vec![
            BlockRange::from_u64(100, 300),
            BlockRange::from_u64(201, 300),
        ]
    );
}

/// Per-page finalize integrates live inserts into the *delivered prefix*
/// mid-fill: once page 1 lands, its region is real ranges rather than
/// placeholder, so a late live item there splits in immediately — under
/// the old whole-gap placeholder the same insert was silently skipped
/// ("range mid-fetch") until the entire gap finished.
#[tokio::test]
async fn late_insert_into_finalized_prefix_lands_mid_fill() {
    let fetcher = ScriptedFetcher::new(vec![vec![
        Ok(page(100, 200, vec![item(150)])),
        Ok(page(201, 300, vec![item(250)])),
    ]]);
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher.share()));

    let mut stream = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    fetcher.release_pages(1);
    assert_eq!(stream.next().await.unwrap().unwrap().item.block, bn(150));

    // Page 1's region is finalized while page 2 is still in flight — a
    // live insert into that prefix must land, not be dropped.
    idx.insert_page(&page(160, 160, vec![item(160)]));
    fetcher.release_pages(1);
    assert_eq!(collect_blocks(stream).await, vec![bn(160), bn(250)]);
}

/// A persist failure surfaces to the fill owner but still wakes parked
/// waiters — a waiter must never hang on a placeholder that no longer
/// exists. (Regression: the pre-streaming code propagated the persist
/// error *before* `notify_waiters`, stranding every registered waiter.)
#[tokio::test]
async fn persist_error_wakes_waiters() {
    let store = mk_store();
    let fetcher = ScriptedFetcher::new(vec![vec![Ok(page(100, 300, vec![item(150)]))]]);
    let idx = Arc::new(RangeIndex::new(
        MockSpec::new(store.share()),
        fetcher.share(),
    ));

    // Owner installs the placeholder and parks awaiting the page permit.
    let mut owner = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    assert!(
        timeout(Duration::from_millis(100), owner.next())
            .await
            .is_err()
    );

    // Waiter parks on the in-flight fill.
    let mut waiter = idx.share().items(
        MockPos::before_block(bn(100)),
        MockPos::end_of_block(bn(300)),
    );
    assert!(
        timeout(Duration::from_millis(100), waiter.next())
            .await
            .is_err()
    );

    // The page lands but persisting it fails: the owner surfaces the error;
    // the waiter is woken and completes over the in-memory ranges.
    store.fail_next_persist();
    fetcher.release_pages(1);
    assert!(
        owner.next().await.unwrap().is_err(),
        "the persist failure surfaces to the fill owner"
    );
    let woken = timeout(Duration::from_secs(5), waiter.next())
        .await
        .expect("a persist failure must still wake parked waiters")
        .unwrap()
        .unwrap();
    assert_eq!(woken.item.block, bn(150));
}

/// Cancelling an `ensure_cached` future mid-fill releases the placeholder,
/// exactly like dropping an `items()` walker.
#[tokio::test]
async fn cancelling_ensure_cached_frees_placeholder() {
    let fetcher = ScriptedFetcher::new(vec![
        vec![Ok(page(100, 300, vec![item(150)]))], // owner's — never released
        vec![Ok(page(100, 300, vec![item(150)]))], // retry after cancellation
    ]);
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher.share()));

    let owner = {
        let idx = idx.share();
        tokio::spawn(async move { idx.ensure_cached(BlockRange::from_u64(100, 300)).await })
    };
    while fetcher.fetch_calls().is_empty() {
        yield_now().await;
    }
    owner.abort(); // cancel mid-fill, parked before page 1
    let _ = owner.await;

    fetcher.release_pages(10);
    timeout(
        Duration::from_secs(5),
        idx.share().ensure_cached(BlockRange::from_u64(100, 300)),
    )
    .await
    .expect("cancelled ensure_cached must not strand its placeholder")
    .unwrap();
    assert_eq!(
        fetcher.fetch_calls().len(),
        2,
        "the retry refetches the gap"
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
/// before the first fetch resolves), and returns an item for every fixture
/// block that falls in the range.
fn recording_fetcher(
    calls: Arc<AtomicUsize>,
    fetched: Arc<Mutex<Vec<BlockRange>>>,
    blocks: Vec<BlockNumber>,
    delay: Duration,
) -> Arc<dyn PageFetcher<MockPage>> {
    Arc::new(SinglePageFetcher(move |range: BlockRange| {
        calls.fetch_add(1, Ordering::SeqCst);
        fetched.lock().unwrap().push(range);
        let blocks = blocks.clone();
        async move {
            if !delay.is_zero() {
                sleep(delay).await;
            }
            let items = blocks
                .iter()
                .filter(|b| **b >= range.from && **b <= range.to)
                .map(|b| MockItem::new(*b, 0))
                .collect();
            Ok(MockPage { range, items })
        }
    }))
}

/// The streaming sibling of [`recording_fetcher`]: splits each requested
/// gap into `pages` contiguous sub-ranges and paces one page per `delay`,
/// so peers race the fill *between* pages — the interleavings the
/// single-page fixtures never produce.
struct PacedPagedFetcher {
    calls: Arc<AtomicUsize>,
    fetched: Arc<Mutex<Vec<BlockRange>>>,
    blocks: Vec<BlockNumber>,
    pages: u64,
    delay: Duration,
}

impl PageFetcher<MockPage> for PacedPagedFetcher {
    fn fetch(&self, range: BlockRange) -> PageStream<MockPage> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.fetched.lock().unwrap().push(range);
        let per = range.count().get().div_ceil(self.pages);
        let mut subs = Vec::new();
        let mut from = range.from;
        while from <= range.to {
            let to = range.to.min(from + (per - 1));
            subs.push(BlockRange::new(from, to));
            from = to + 1;
        }
        let blocks = self.blocks.clone();
        let delay = self.delay;
        Box::pin(unfold(
            (subs.into_iter(), blocks),
            move |(mut subs, blocks)| async move {
                let sub = subs.next()?;
                sleep(delay).await;
                let items = blocks
                    .iter()
                    .filter(|b| **b >= sub.from && **b <= sub.to)
                    .map(|b| MockItem::new(*b, 0))
                    .collect();
                Some((Ok(MockPage { range: sub, items }), (subs, blocks)))
            },
        ))
    }
}

/// Drain an `items()` stream to the block numbers it yields.
async fn collect_blocks<St>(mut stream: St) -> Vec<BlockNumber>
where
    St: futures_core::Stream<Item = Result<Positioned<MockItem, MockPubPos>, HistoryError>> + Unpin,
{
    let mut out = Vec::new();
    while let Some(r) = stream.next().await {
        out.push(r.unwrap().item.block);
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
        calls.share(),
        fetched,
        vec![bn(150)],
        Duration::from_millis(50),
    );
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let idx = idx.share();
        handles.push(tokio::spawn(async move {
            let mut s = idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(200)),
            );
            s.next().await.unwrap().unwrap().item.block
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
        calls.share(),
        fetched.share(),
        vec![bn(150), bn(350)],
        Duration::from_millis(30),
    );
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    let a = {
        let idx = idx.share();
        tokio::spawn(async move {
            let mut s = idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(200)),
            );
            s.next().await.unwrap().unwrap().item.block
        })
    };
    let b = {
        let idx = idx.share();
        tokio::spawn(async move {
            let mut s = idx.items(
                MockPos::before_block(bn(300)),
                MockPos::end_of_block(bn(400)),
            );
            s.next().await.unwrap().unwrap().item.block
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
        calls.share(),
        fetched.share(),
        vec![bn(150), bn(250), bn(350)],
        Duration::from_millis(40),
    );
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    // Windows overlap on blocks (200, 300].
    let a = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(300)),
            ))
            .await
        })
    };
    let b = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(200)),
                MockPos::end_of_block(bn(400)),
            ))
            .await
        })
    };
    assert_eq!(a.await.unwrap(), vec![bn(150), bn(250)]);
    assert_eq!(b.await.unwrap(), vec![bn(250), bn(350)]);

    // Singleflight invariant across overlapping (not identical) gaps.
    assert_ranges_tile(&fetched.lock().unwrap(), bn(100), bn(400));
}

/// Streaming analog of `concurrent_searches_same_gap_one_fetch_multi_thread`:
/// truly parallel walkers race one *paged* fill — pages land while peers are
/// parked, the placeholder shrinks between them — yet all coalesce onto a
/// single fetch stream and every walker sees every item exactly once, in
/// order (no dup, no drop, no TOCTOU refetch of the shrunk remainder).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paged_fill_singleflight_multi_thread() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = Arc::new(PacedPagedFetcher {
        calls: calls.share(),
        fetched: fetched.share(),
        blocks: vec![bn(120), bn(150), bn(180)],
        pages: 3,
        delay: Duration::from_millis(20),
    });
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let idx = idx.share();
        handles.push(tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(200)),
            ))
            .await
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), vec![bn(120), bn(150), bn(180)]);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one paged fetch stream served all parallel walkers"
    );
    assert_eq!(
        *fetched.lock().unwrap(),
        vec![BlockRange::from_u64(100, 200)]
    );
}

/// Overlapping windows over *paged* fills: however the race resolves, the
/// gap-level fetches tile the union exactly — the shrink-from-left
/// placeholder bookkeeping never lets the overlap double-fetch — and each
/// walker sees precisely its own window's items.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_paged_fills_never_double_fetch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let fetcher = Arc::new(PacedPagedFetcher {
        calls: calls.share(),
        fetched: fetched.share(),
        blocks: vec![bn(150), bn(250), bn(350)],
        pages: 3,
        delay: Duration::from_millis(15),
    });
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    // Windows overlap on blocks (200, 300].
    let a = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(300)),
            ))
            .await
        })
    };
    let b = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(200)),
                MockPos::end_of_block(bn(400)),
            ))
            .await
        })
    };
    assert_eq!(a.await.unwrap(), vec![bn(150), bn(250)]);
    assert_eq!(b.await.unwrap(), vec![bn(250), bn(350)]);
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
    let fetcher = {
        let entered = entered.share();
        let release = release.share();
        let calls = calls.share();
        Arc::new(SinglePageFetcher(move |range: BlockRange| {
            // Only the first fetch (the front gap) parks; later tail-gap
            // fetches proceed immediately and return nothing.
            let n = calls.fetch_add(1, Ordering::SeqCst);
            let entered = entered.share();
            let release = release.share();
            async move {
                if n == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                let items = if range.from <= bn(150) && range.to >= bn(150) {
                    vec![item(150)]
                } else {
                    vec![]
                };
                Ok(MockPage { range, items })
            }
        }))
    };
    let idx = Arc::new(RangeIndex::new(MockSpec::new(mk_store()), fetcher));

    // A cached island at block 200 splits the walk into
    // gap[100,199] | cached@200 | gap. The walker parks in the first gap.
    idx.insert_page(&page(200, 200, vec![item(200)]));

    let walker = {
        let idx = idx.share();
        tokio::spawn(async move {
            collect_blocks(idx.items(
                MockPos::before_block(bn(100)),
                MockPos::end_of_block(bn(400)),
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
    idx.insert_page(&page(300, 300, vec![item(300)]));
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

    let idx = Arc::new(mk_index()); // never_fetch — any gap fails loudly.

    // A gap-free window [1, 300]: seeded items every 10 blocks, plus late items
    // strictly inside the window that split existing ranges when they arrive.
    let seeded_blocks: Vec<BlockNumber> = (1..=30).map(|k| bn(k * 10)).collect();
    let late_blocks: Vec<BlockNumber> = vec![bn(55), bn(155), bn(255)];

    idx.insert_page(&page(
        1,
        300,
        seeded_blocks.iter().map(|b| MockItem::new(*b, 0)).collect(),
    ));

    // Writer: each late item may or may not be observed depending on where the
    // cursor is — but must never corrupt the walk.
    let writer = {
        let idx = idx.share();
        let late_blocks = late_blocks.clone();
        tokio::spawn(async move {
            for b in late_blocks {
                idx.insert_page(&MockPage {
                    range: BlockRange::new(b, b),
                    items: vec![MockItem::new(b, 0)],
                });
                yield_now().await;
            }
        })
    };

    // Reader: walk with a yield between items to widen the interleave.
    let mut stream = idx
        .share()
        .items(MockPos::before_block(bn(1)), MockPos::end_of_block(bn(300)));
    let mut got: Vec<BlockNumber> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.unwrap().item.block);
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
