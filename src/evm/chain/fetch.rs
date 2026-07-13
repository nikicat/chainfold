//! [`LogFetcher`] — the adaptive slicing executor behind every log
//! store's gap fill (`docs/ARCHITECTURE.md` §9A).
//!
//! One `fetch(range)` call plans the range into slices at the endpoint's
//! current working size, runs them bounded-parallel under the shared
//! [`EndpointBudget`], and releases the resulting [`LogPage`]s **in
//! ascending order** through a bounded reorder buffer — the streaming
//! contract the range index consumes (`docs/ARCHITECTURE.md` §9A).
//!
//! **No spawned tasks:** the stream is a `stream::unfold` over a
//! [`SliceDriver`] whose slices are futures in a `FuturesUnordered` —
//! concurrent while the consumer polls, all cancelled the moment the
//! stream drops, which is exactly the core's cancellation contract.
//!
//! Failed slices are handled by error class (the adaptor's taxonomy):
//!
//! * `RangeTooLarge` — shrink the shared working size (once per
//!   generation) and requeue the range; it re-slices at the new size on
//!   launch.
//! * `RateLimited` — shrink concurrency and pause, requeue as-is; the
//!   size is fine. Retried indefinitely — a throttle is expected to
//!   clear, and the budget's pause paces the attempts.
//! * `Transient` — retry the same slice up to [`TRANSIENT_RETRIES`]
//!   times, then bisect it *slice-locally* (never the shared working
//!   size) — the generic adaptor's discovery driver. A single block
//!   still failing is fatal.
//! * `Fatal` / store errors — end the stream. The **lowest-from** failure
//!   wins so released coverage is maximal; completed later slices drop
//!   with the driver (`docs/ARCHITECTURE.md` §19, decision #14).
//!
//! Header fill runs *inside* the slice, before release: the
//! headers-present invariant requires headers cached before a page is
//! consumable, and header RPC errors classify like slice errors (a 429 on
//! a header burst must shrink concurrency too).

use std::collections::{BTreeMap, BTreeSet};

use alloy::network::Ethereum;
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, FuturesUnordered};

use crate::arch::{HistoryError, error_chain};
use crate::chain::{BlockNumber, BlockRange, ChunkSize};
use crate::evm::LogPage;
use crate::evm::budget::Generation;
use crate::evm::chain_provider::{ChainProvider, RawFetchError};
use crate::evm::provider::ErrorClass;
use crate::evm::types::BlockHash;
use crate::range_index::{PageFetcher, PageStream};

/// Same-range retries of a `Transient` slice before it bisects.
const TRANSIENT_RETRIES: u32 = 2;
/// In-flight header fetches per slice's post-`get_logs` header fill.
const HEADER_CONCURRENCY: usize = 8;

/// The [`PageFetcher`] for one log filter: adaptive slicing over
/// `eth_getLogs` + in-slice header fill. Construction is free; all state
/// lives per `fetch` call (the endpoint-shared state rides the provider).
pub(super) struct LogFetcher<P> {
    provider: ChainProvider<P>,
    /// Address + topics; each slice adds its block bounds.
    base_filter: Filter,
    /// The working-size ceiling — one placeholder's span
    /// (`docs/ARCHITECTURE.md` §19, decision #20).
    ceiling: ChunkSize,
}

impl<P> LogFetcher<P> {
    pub(super) fn new(provider: ChainProvider<P>, base_filter: Filter, ceiling: ChunkSize) -> Self {
        Self {
            provider,
            base_filter,
            ceiling,
        }
    }
}

impl<P> PageFetcher<LogPage> for LogFetcher<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    fn fetch(&self, range: BlockRange) -> PageStream<LogPage> {
        debug_assert!(range.from <= range.to, "inverted fetch range {range}");
        tracing::debug!(
            %range,
            working = %self.provider.budget().working_size(self.ceiling),
            "adaptive fetch started"
        );
        let driver = SliceDriver {
            provider: self.provider.clone(),
            base_filter: self.base_filter.clone(),
            ceiling: self.ceiling,
            range,
            next_from: Some(range.from),
            inflight: FuturesUnordered::new(),
            retry: BTreeMap::new(),
            parked: BTreeMap::new(),
            release_cursor: range.from,
            fatal: None,
            done: false,
        };
        Box::pin(stream::unfold(driver, |mut driver| async move {
            let item = driver.next_page().await?;
            Some((item, driver))
        }))
    }
}

/// One slice's work order. `from`-keyed in the retry queue; relaunched
/// verbatim unless [`resize`](Self::resize) is set.
struct SliceWork {
    range: BlockRange,
    /// `Transient` retries already spent on this exact range.
    attempts: u32,
    /// Re-carve at the current working size on launch — set when the
    /// range was rejected as too large, so the requeue picks up the
    /// shrunk (or adopted) size for free.
    resize: bool,
}

/// What one slice future resolves to; the driver classifies errors and
/// mutates the AIMD state — completion handling is the *only* place that
/// happens.
struct SliceOutcome {
    work: SliceWork,
    /// The working-size generation the slice was carved under.
    stamp: Generation,
    result: Result<LogPage, RawFetchError>,
}

/// The per-`fetch` execution state driven by `stream::unfold`; see the
/// module docs for the lifecycle.
struct SliceDriver<P> {
    provider: ChainProvider<P>,
    base_filter: Filter,
    ceiling: ChunkSize,
    range: BlockRange,
    /// Lazy plan cursor: the first block no slice has been carved for.
    /// `None` once the plan is fully carved. Lazy so a working-size halve
    /// re-plans the remainder for free.
    next_from: Option<BlockNumber>,
    inflight: FuturesUnordered<BoxFuture<'static, SliceOutcome>>,
    /// Failed slices awaiting relaunch, by `range.from` — relaunched
    /// lowest-first, before any new carving, so the release frontier is
    /// always the next thing in flight.
    retry: BTreeMap<BlockNumber, SliceWork>,
    /// Completed pages waiting for their turn — the bounded reorder
    /// buffer (bounded by the launch window).
    parked: BTreeMap<BlockNumber, LogPage>,
    /// The `from` of the next page to release.
    release_cursor: BlockNumber,
    /// The lowest-from terminal failure; surfaced once everything below
    /// it has released.
    fatal: Option<(BlockNumber, HistoryError)>,
    done: bool,
}

impl<P> SliceDriver<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    /// Drive until one page is releasable in order (`Some(Ok)`), the
    /// terminal failure surfaces (`Some(Err)`, ending the stream), or the
    /// range is fully released (`None`).
    async fn next_page(&mut self) -> Option<Result<LogPage, HistoryError>> {
        if self.done {
            return None;
        }
        loop {
            if let Some(page) = self.pop_ready() {
                return Some(Ok(page));
            }
            if self.fatal.is_none() {
                self.fill_launch_window();
            }
            if let Some(outcome) = self.inflight.next().await {
                self.handle(outcome);
            } else {
                // Nothing in flight and nothing launchable: the fetch is
                // finished — cleanly, or at the recorded failure (queued
                // retries and parked pages past it are abandoned; the
                // released prefix stands, the suffix stays a gap).
                self.done = true;
                if let Some((from, error)) = self.fatal.take() {
                    tracing::debug!(
                        %from,
                        released_to = %self.release_cursor,
                        error = %error,
                        "adaptive fetch failed; released prefix stands"
                    );
                    return Some(Err(error));
                }
                debug_assert!(
                    self.parked.is_empty(),
                    "clean finish must have released every page"
                );
                return None;
            }
        }
    }

    /// The parked page at the release frontier, if it has landed.
    fn pop_ready(&mut self) -> Option<LogPage> {
        let page = self.parked.remove(&self.release_cursor)?;
        self.release_cursor = page.range.to.saturating_add(1);
        Some(page)
    }

    /// Launch retries (lowest-first), then carve new slices, until the
    /// window is full or the work runs out. The window is the budget's
    /// live concurrency target: it bounds in-flight slices *and* the
    /// reorder buffer together (a completed slice moves from one to the
    /// other), which is what keeps parked memory bounded.
    ///
    /// One exception: a queued retry at the *release frontier* always
    /// launches, window notwithstanding. Nothing can release until that
    /// slice lands — and if the window shrank mid-flight (a rate limit),
    /// parked pages alone can fill it, which would strand the frontier in
    /// the queue and end the stream early. The overshoot is bounded by
    /// one, and the budget's semaphore still bounds the wire.
    fn fill_launch_window(&mut self) {
        let window = self.provider.budget().concurrency();
        loop {
            let frontier_queued = self
                .retry
                .first_key_value()
                .is_some_and(|(from, _)| *from == self.release_cursor);
            if !frontier_queued && self.inflight.len() + self.parked.len() >= window {
                return;
            }
            let Some(work) = self.take_retry().or_else(|| self.carve()) else {
                return;
            };
            let stamp = self.provider.budget().generation();
            tracing::debug!(range = %work.range, attempts = work.attempts, "slice launched");
            self.inflight.push(Box::pin(run_slice(
                self.provider.clone(),
                self.base_filter.clone(),
                work,
                stamp,
            )));
        }
    }

    /// The lowest queued retry. A `resize` entry is re-carved at the
    /// current working size: the head slice launches, the remainder goes
    /// back in the queue.
    fn take_retry(&mut self) -> Option<SliceWork> {
        let (_, mut work) = self.retry.pop_first()?;
        if work.resize {
            let size = self.provider.budget().working_size(self.ceiling).get();
            if work.range.count().get() > size {
                let head_to = work.range.from.saturating_add(size - 1);
                let rest = BlockRange::new(head_to.saturating_add(1), work.range.to);
                self.retry.insert(
                    rest.from,
                    SliceWork {
                        range: rest,
                        attempts: 0,
                        resize: true,
                    },
                );
                work.range = BlockRange::new(work.range.from, head_to);
            }
            work.resize = false;
            work.attempts = 0;
        }
        Some(work)
    }

    /// Carve the next slice off the unplanned remainder at the current
    /// working size.
    fn carve(&mut self) -> Option<SliceWork> {
        let from = self.next_from?;
        let size = self.provider.budget().working_size(self.ceiling).get();
        let to = from.saturating_add(size - 1).min(self.range.to);
        self.next_from = (to < self.range.to).then(|| to.saturating_add(1));
        Some(SliceWork {
            range: BlockRange::new(from, to),
            attempts: 0,
            resize: false,
        })
    }

    /// Classify a completed slice and mutate the AIMD state accordingly —
    /// the only place that happens.
    fn handle(&mut self, outcome: SliceOutcome) {
        let SliceOutcome {
            work,
            stamp,
            result,
        } = outcome;
        let error = match result {
            Ok(page) => {
                debug_assert_eq!(page.range, work.range, "slice must answer its own range");
                self.provider.budget().on_clean_slice(self.ceiling);
                self.parked.insert(page.range.from, page);
                return;
            }
            Err(RawFetchError::Store(error)) => {
                self.record_fatal(work.range.from, error);
                return;
            }
            Err(RawFetchError::Rpc(error)) => error,
        };

        let class = self.provider.adaptor().classify(&error);
        tracing::debug!(
            range = %work.range,
            class = ?class,
            error = %error_chain(&error),
            "slice failed"
        );
        match class {
            ErrorClass::RangeTooLarge { suggested } => {
                self.provider.budget().on_range_too_large(
                    stamp,
                    suggested.map(BlockRange::count),
                    self.ceiling,
                );
                self.retry.insert(
                    work.range.from,
                    SliceWork {
                        range: work.range,
                        attempts: 0,
                        resize: true,
                    },
                );
            }
            ErrorClass::RateLimited { retry_after } => {
                self.provider.budget().on_rate_limited(retry_after);
                self.retry.insert(work.range.from, work);
            }
            ErrorClass::Transient if work.attempts < TRANSIENT_RETRIES => {
                self.retry.insert(
                    work.range.from,
                    SliceWork {
                        range: work.range,
                        attempts: work.attempts + 1,
                        resize: false,
                    },
                );
            }
            ErrorClass::Transient => match bisect(work.range) {
                Some((head, tail)) => {
                    tracing::debug!(
                        range = %work.range,
                        "transient retries exhausted: bisecting the slice"
                    );
                    for half in [head, tail] {
                        self.retry.insert(
                            half.from,
                            SliceWork {
                                range: half,
                                attempts: 0,
                                resize: false,
                            },
                        );
                    }
                }
                None => self.record_fatal(
                    work.range.from,
                    HistoryError::internal(format!(
                        "slice {} failed after {TRANSIENT_RETRIES} retries: {}",
                        work.range,
                        error_chain(&error)
                    )),
                ),
            },
            ErrorClass::Fatal => self.record_fatal(
                work.range.from,
                HistoryError::internal(format!(
                    "slice {} failed: {}",
                    work.range,
                    error_chain(&error)
                )),
            ),
        }
    }

    /// Keep the lowest-from terminal failure — earlier in-flight slices
    /// still release first, so their coverage persists.
    fn record_fatal(&mut self, from: BlockNumber, error: HistoryError) {
        match &self.fatal {
            Some((existing, _)) if *existing <= from => {}
            _ => self.fatal = Some((from, error)),
        }
    }
}

/// Split an inclusive range in half; `None` for a single block.
fn bisect(range: BlockRange) -> Option<(BlockRange, BlockRange)> {
    if range.from == range.to {
        return None;
    }
    let mid = BlockNumber::new(range.from.get() + (range.to.get() - range.from.get()) / 2);
    Some((
        BlockRange::new(range.from, mid),
        BlockRange::new(mid.saturating_add(1), range.to),
    ))
}

/// One slice, end to end: budget slot → `eth_getLogs` → in-slice header
/// fill. The budget slot covers only the wire round-trip; header fetches
/// take their own.
async fn run_slice<P>(
    provider: ChainProvider<P>,
    base_filter: Filter,
    work: SliceWork,
    stamp: Generation,
) -> SliceOutcome
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let range = work.range;
    let filter = base_filter
        .from_block(range.from.get())
        .to_block(range.to.get());
    let logs = {
        let _permit = provider.budget().acquire().await;
        tracing::debug!(%range, blocks = %range.count(), "eth_getLogs slice");
        provider.get_logs_raw(&filter).await
    };
    let result = match logs {
        Err(error) => Err(RawFetchError::Rpc(error)),
        Ok(items) => {
            tracing::debug!(%range, num_logs = items.len(), "eth_getLogs slice returned");
            fill_headers(&provider, range, &items)
                .await
                .map(|end_block_hash| LogPage {
                    range,
                    items,
                    end_block_hash,
                })
        }
    };
    SliceOutcome {
        work,
        stamp,
        result,
    }
}

/// Ensure every header the page needs is cached before it releases: the
/// slice-end header (its hash is the page's coverage claim) plus each
/// log-bearing block whose log lacks its own timestamp — [`LogSpec`]'s
/// synchronous resolve reads timestamps from cached headers. Bounded-
/// parallel, each fetch under its own budget slot; errors are *not*
/// swallowed — they classify like slice errors.
///
/// [`LogSpec`]: crate::evm::LogSpec
async fn fill_headers<P>(
    provider: &ChainProvider<P>,
    range: BlockRange,
    logs: &[Log],
) -> Result<BlockHash, RawFetchError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut blocks: BTreeSet<BlockNumber> = logs
        .iter()
        .filter(|log| log.block_timestamp.is_none())
        .filter_map(|log| log.block_number.map(BlockNumber::new))
        .collect();
    blocks.insert(range.to);

    let mut headers = stream::iter(blocks)
        .map(|block| async move {
            let _permit = provider.budget().acquire().await;
            provider
                .ensure_block_header_raw(block)
                .await
                .map(|header| (block, header))
        })
        .buffer_unordered(HEADER_CONCURRENCY);

    let mut end_hash = None;
    while let Some(result) = headers.next().await {
        let (block, header) = result?;
        if block == range.to {
            end_hash = Some(header.hash);
        }
    }
    Ok(end_hash.expect("the slice end is always in the header set"))
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use alloy::primitives::Address;
    use futures_util::FutureExt;
    use serde_json::json;
    use tokio::sync::Semaphore;
    use tokio::time::{sleep, timeout};

    use super::super::testkit::{
        ADDR, STUB_FATAL, STUB_RANGE_TOO_LARGE, STUB_RATE_LIMITED, StubAdaptor, StubRpc,
        dense_header_cache, insert_header, logs_range, make_log, stub_provider, ts,
    };
    use super::*;
    use crate::chain::BlockCount;
    use crate::evm::cache::LogCache;
    use crate::evm::provider::Concurrency;
    use crate::evm::types::LogIdx;
    use crate::share::Share;

    static C2: StubAdaptor = StubAdaptor {
        max_range: None,
        concurrency: Concurrency::new(2),
    };
    static C4: StubAdaptor = StubAdaptor {
        max_range: None,
        concurrency: Concurrency::new(4),
    };

    fn base_filter() -> Filter {
        Filter::new().address(ADDR)
    }

    fn stub_error(code: i64, message: &str) -> serde_json::Value {
        json!({ "code": code, "message": message })
    }

    /// Collect the whole stream, expecting every page to be `Ok`.
    async fn collect_pages(stream: PageStream<LogPage>) -> Vec<LogPage> {
        stream
            .map(|page| page.expect("no page may fail"))
            .collect()
            .await
    }

    fn ranges(pages: &[LogPage]) -> Vec<BlockRange> {
        pages.iter().map(|p| p.range).collect()
    }

    /// Decision 4 end to end: four ceiling-width slices carved under one
    /// generation all fail `RangeTooLarge` → exactly one halve (400 →
    /// 200), and the requeued ranges re-slice at 200. Halve-per-failure
    /// would collapse to 25 and show narrower retries.
    #[tokio::test]
    async fn halves_once_per_generation() {
        let stub = StubRpc::new(|call| {
            let reply = if logs_range(call).count().get() > 200 {
                Err(stub_error(STUB_RANGE_TOO_LARGE, "range too wide"))
            } else {
                Ok(json!([]))
            };
            std::future::ready(reply).boxed()
        });
        let provider = stub_provider(
            stub.clone(),
            dense_header_cache(BlockNumber::new(1_599)),
            &C4,
        );
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(400));

        let pages = collect_pages(fetcher.fetch(BlockRange::from_u64(0, 1_599))).await;
        let expected: Vec<BlockRange> = (0..8)
            .map(|i| BlockRange::from_u64(i * 200, i * 200 + 199))
            .collect();
        assert_eq!(ranges(&pages), expected);

        let calls = stub.get_logs_ranges();
        let wide = calls.iter().filter(|r| r.count().get() > 200).count();
        let narrow: Vec<u64> = calls
            .iter()
            .filter(|r| r.count().get() <= 200)
            .map(|r| r.count().get())
            .collect();
        assert_eq!(wide, 4, "all four ceiling-width slices fired: {calls:?}");
        assert_eq!(
            narrow,
            vec![200; 8],
            "one halve → retries at exactly 200: {calls:?}"
        );
    }

    /// Decision 3: a rate limit shrinks *concurrency* (and pauses), never
    /// the slice size — every retried request keeps its width.
    #[tokio::test(start_paused = true)]
    async fn rate_limit_shrinks_concurrency_not_size() {
        let throttle_once = Arc::new(AtomicBool::new(true));
        let stub = StubRpc::new(move |_call| {
            let reply = if throttle_once.swap(false, Ordering::AcqRel) {
                Err(stub_error(STUB_RATE_LIMITED, "slow down"))
            } else {
                Ok(json!([]))
            };
            std::future::ready(reply).boxed()
        });
        let provider = stub_provider(stub.clone(), dense_header_cache(BlockNumber::new(399)), &C4);
        let budget = provider.budget().share();
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let pages = collect_pages(fetcher.fetch(BlockRange::from_u64(0, 399))).await;
        assert_eq!(pages.len(), 4);

        let calls = stub.get_logs_ranges();
        assert_eq!(calls.len(), 5, "the throttled slice retried once");
        assert!(
            calls.iter().all(|r| r.count().get() == 100),
            "the size never moved: {calls:?}"
        );
        assert_eq!(
            budget.working_size(ChunkSize::new(100)),
            BlockCount::new(100)
        );
        assert_eq!(budget.concurrency(), 3, "one shrink, no recovery yet");
    }

    /// Decision 1: slices run concurrently but release in ascending
    /// order — nothing yields while the lowest slice is still in flight,
    /// even with its successors long finished.
    #[tokio::test]
    async fn releases_pages_in_order() {
        let gate = Arc::new(Semaphore::new(0));
        let stub = StubRpc::new({
            let gate = gate.share();
            move |call| {
                let held = logs_range(call).from == BlockNumber::ZERO;
                let gate = gate.share();
                async move {
                    if held {
                        let _ = gate.acquire().await;
                    }
                    Ok(json!([]))
                }
                .boxed()
            }
        });
        let provider = stub_provider(stub.clone(), dense_header_cache(BlockNumber::new(299)), &C4);
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let mut pages = fetcher.fetch(BlockRange::from_u64(0, 299));
        assert!(
            timeout(Duration::from_millis(200), pages.next())
                .await
                .is_err(),
            "nothing may yield while the lowest slice is gated"
        );
        assert_eq!(
            stub.inflight(),
            1,
            "the later slices completed meanwhile; only the gated one is left"
        );

        gate.add_permits(1);
        let released: Vec<BlockRange> = pages.map(|page| page.unwrap().range).collect().await;
        assert_eq!(
            released,
            vec![
                BlockRange::from_u64(0, 99),
                BlockRange::from_u64(100, 199),
                BlockRange::from_u64(200, 299),
            ]
        );
    }

    /// The budget is endpoint-scoped: two filters' fetchers over one
    /// cloned provider never exceed the shared concurrency on the wire.
    #[tokio::test(start_paused = true)]
    async fn budget_bounds_inflight_across_filters() {
        let stub = StubRpc::new(|_call| {
            async {
                // Long enough that requests overlap whenever slots allow.
                sleep(Duration::from_millis(10)).await;
                Ok(json!([]))
            }
            .boxed()
        });
        let provider = stub_provider(stub.clone(), dense_header_cache(BlockNumber::new(499)), &C2);
        let a = LogFetcher::new(provider.clone(), base_filter(), ChunkSize::new(100));
        let b = LogFetcher::new(
            provider,
            Filter::new().address(Address::repeat_byte(0x43)),
            ChunkSize::new(100),
        );

        let range = BlockRange::from_u64(0, 499);
        let (pages_a, pages_b) =
            tokio::join!(collect_pages(a.fetch(range)), collect_pages(b.fetch(range)));
        assert_eq!(pages_a.len() + pages_b.len(), 10);
        assert_eq!(
            stub.high_water(),
            2,
            "the shared budget must bound (and allow) exactly 2 in flight"
        );
    }

    /// A fatal slice ends the stream — after the slices below it have
    /// released (maximal prefix), and without the ones above it (they
    /// drop with the driver; the suffix stays a gap).
    #[tokio::test]
    async fn fatal_slice_ends_stream_after_released_prefix() {
        let stub = StubRpc::new(|call| {
            let reply = if logs_range(call).from == BlockNumber::new(200) {
                Err(stub_error(STUB_FATAL, "access denied"))
            } else {
                Ok(json!([]))
            };
            std::future::ready(reply).boxed()
        });
        let provider = stub_provider(stub, dense_header_cache(BlockNumber::new(399)), &C4);
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let mut pages = fetcher.fetch(BlockRange::from_u64(0, 399));
        assert_eq!(
            pages.next().await.unwrap().unwrap().range,
            BlockRange::from_u64(0, 99)
        );
        assert_eq!(
            pages.next().await.unwrap().unwrap().range,
            BlockRange::from_u64(100, 199)
        );
        let error = pages.next().await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("200..=299"),
            "the error names the failed slice: {error}"
        );
        assert!(pages.next().await.is_none(), "the stream ends on Err");
    }

    /// Two fatal slices: the *lowest-from* failure is the one surfaced,
    /// whichever order the failures arrive in — that's what makes the
    /// released prefix maximal.
    #[tokio::test(start_paused = true)]
    async fn lowest_from_fatal_wins_regardless_of_arrival_order() {
        use std::collections::HashMap;

        // Four gated slices over [0, 399]; the ones at 100 and 300 are
        // fatal. `release_order` opens the two fatal gates in the given
        // order (with a drive of the stream between them), then the two
        // clean ones.
        async fn run(release_order: [u64; 2]) {
            let gates: Arc<HashMap<u64, Arc<Semaphore>>> = Arc::new(
                [0, 100, 200, 300]
                    .into_iter()
                    .map(|from| (from, Arc::new(Semaphore::new(0))))
                    .collect(),
            );
            let stub = StubRpc::new({
                let gates = gates.share();
                move |call| {
                    let from = logs_range(call).from.get();
                    let gate = gates[&from].share();
                    async move {
                        let _ = gate.acquire().await;
                        if matches!(from, 100 | 300) {
                            Err(stub_error(STUB_FATAL, "access denied"))
                        } else {
                            Ok(json!([]))
                        }
                    }
                    .boxed()
                }
            });
            let provider = stub_provider(stub, dense_header_cache(BlockNumber::new(399)), &C4);
            let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));
            let mut pages = fetcher.fetch(BlockRange::from_u64(0, 399));

            for from in release_order {
                gates[&from].add_permits(1);
                // Drive the stream so this failure is recorded before the
                // next gate opens; nothing is releasable yet (slice 0 is
                // still gated), so this poll must time out.
                assert!(
                    timeout(Duration::from_millis(10), pages.next())
                        .await
                        .is_err(),
                    "nothing may yield before the first slice lands"
                );
            }
            gates[&0].add_permits(1);
            gates[&200].add_permits(1);

            let page = pages.next().await.unwrap().unwrap();
            assert_eq!(page.range, BlockRange::from_u64(0, 99));
            let error = pages.next().await.unwrap().unwrap_err();
            assert!(
                error.to_string().contains("100..=199"),
                "the lowest failure must win (released {release_order:?}): {error}"
            );
            assert!(pages.next().await.is_none());
        }

        run([100, 300]).await; // lowest first: the later failure is ignored
        run([300, 100]).await; // lowest last: it displaces the earlier one
    }

    /// A store-side failure inside a slice (here: the node claims the
    /// slice-end block doesn't exist) is terminal — no classification, no
    /// retry.
    #[tokio::test]
    async fn missing_block_is_a_terminal_store_error() {
        let stub = StubRpc::new(|call| {
            let reply = match call.method.as_str() {
                "eth_getLogs" => Ok(json!([])),
                "eth_getBlockByNumber" => Ok(serde_json::Value::Null),
                other => panic!("unexpected method {other}"),
            };
            std::future::ready(reply).boxed()
        });
        // Empty cache: the slice-end header must be fetched — and the
        // node answers null.
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        let provider = stub_provider(stub, cache, &C4);
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let mut pages = fetcher.fetch(BlockRange::from_u64(0, 9));
        let error = pages.next().await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("block 9 not found"),
            "the store error surfaces untouched: {error}"
        );
        assert!(pages.next().await.is_none());
    }

    /// The generic-adaptor discovery driver: a `Transient` slice retries
    /// as-is, then bisects *slice-locally* — the shared working size
    /// never moves.
    #[tokio::test]
    async fn transient_retries_then_bisects_slice_locally() {
        let stub = StubRpc::new(|call| {
            let reply = if logs_range(call).count().get() > 50 {
                Err(stub_error(-9_999, "mystery failure"))
            } else {
                Ok(json!([]))
            };
            std::future::ready(reply).boxed()
        });
        let provider = stub_provider(stub.clone(), dense_header_cache(BlockNumber::new(99)), &C4);
        let budget = provider.budget().share();
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let pages = collect_pages(fetcher.fetch(BlockRange::from_u64(0, 99))).await;
        assert_eq!(
            ranges(&pages),
            vec![BlockRange::from_u64(0, 49), BlockRange::from_u64(50, 99)]
        );

        let calls = stub.get_logs_ranges();
        let full = BlockRange::from_u64(0, 99);
        assert_eq!(
            calls.iter().filter(|r| **r == full).count(),
            1 + TRANSIENT_RETRIES as usize,
            "the whole slice was tried and retried before bisecting: {calls:?}"
        );
        assert_eq!(calls.len(), 3 + 2);
        assert_eq!(
            budget.working_size(ChunkSize::new(100)),
            BlockCount::new(100),
            "bisection is slice-local, never the shared size"
        );
    }

    /// The bisection floor: a single block that keeps failing
    /// transiently is a terminal error, not an infinite loop.
    #[tokio::test]
    async fn single_block_transient_failure_is_fatal() {
        let stub = StubRpc::new(|_call| {
            std::future::ready(Err(stub_error(-9_999, "mystery failure"))).boxed()
        });
        let provider = stub_provider(stub.clone(), dense_header_cache(BlockNumber::new(5)), &C4);
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let mut pages = fetcher.fetch(BlockRange::from_u64(5, 5));
        let error = pages.next().await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("5..=5"),
            "the error names the failed block: {error}"
        );
        assert!(pages.next().await.is_none());
        assert_eq!(stub.get_logs_ranges().len(), 1 + TRANSIENT_RETRIES as usize);
    }

    /// Header RPC failures inside a slice classify like slice errors
    /// instead of being swallowed (the old fetcher's `let _ =` would have
    /// delivered the page and left the timestamp fill to fail later).
    #[tokio::test]
    async fn header_fill_errors_classify_like_slice_errors() {
        let log = serde_json::to_value(make_log(
            BlockNumber::new(3),
            LogIdx::ZERO,
            BlockHash::repeat_byte(0x9A),
        ))
        .unwrap();
        let stub = StubRpc::new(move |call| {
            let reply = match call.method.as_str() {
                "eth_getLogs" => Ok(json!([log])),
                "eth_getBlockByNumber" => Err(stub_error(STUB_FATAL, "access denied")),
                other => panic!("unexpected method {other}"),
            };
            std::future::ready(reply).boxed()
        });
        // The slice-end header is cached; block 3 (the log's block, no
        // block_timestamp) is not — its fetch must fail the slice.
        let cache = Arc::new(LogCache::open_in_memory().unwrap());
        insert_header(
            &cache,
            BlockNumber::new(9),
            BlockHash::repeat_byte(0xA9),
            ts(108),
        );
        let provider = stub_provider(stub, cache, &C4);
        let fetcher = LogFetcher::new(provider, base_filter(), ChunkSize::new(100));

        let mut pages = fetcher.fetch(BlockRange::from_u64(0, 9));
        let error = pages.next().await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("access denied"),
            "the header failure surfaces: {error}"
        );
        assert!(pages.next().await.is_none());
    }
}
