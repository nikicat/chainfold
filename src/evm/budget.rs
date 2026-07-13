//! [`EndpointBudget`] — the per-endpoint adaptive fetch state every filter
//! store on a chain shares (`docs/FETCH.md` §3). It lives on
//! [`ChainProvider`](crate::evm::chain_provider::ChainProvider) beside the
//! header singleflight, for the same reason that does: every gap-fill hits
//! the same endpoint, and without shared state N stores × M slices
//! thundering-herd it.
//!
//! The budget is a thin facade over two independent machines — AIMD's two
//! knobs, never conflated (FETCH.md §5, decision 3):
//!
//! * [`SliceSize`] — blocks per `eth_getLogs` slice. Shrinks on
//!   `RangeTooLarge` (adopt the provider's suggested width, else halve) —
//!   at most **once per generation** (decision 4): slices are stamped with
//!   the [`Generation`] they were carved under, and only the first failure
//!   of a generation shrinks, so twenty concurrent failures are one
//!   congestion event. Regrows additively after [`REGROW_AFTER`] clean
//!   slices, toward the [`ChunkSize`] ceiling. Pure atomics; knows nothing
//!   about time or tokens.
//! * [`RequestSlots`] — in-flight requests against the endpoint. Shrinks
//!   by one on `RateLimited` (floor 1) with a request pause; recovers by
//!   one per rate-limit-free [`QUIET_PERIOD`]. Shrinking uses a
//!   deficit-forget scheme: the shrink never blocks on outstanding
//!   requests — the next permit releases are swallowed instead of
//!   returned. Owns all the clock state (pause + recovery pacing).
//!
//! The two never read each other; a clean slice is simply one event fanned
//! out to both ([`EndpointBudget::on_clean_slice`]).
//!
//! Every decision emits a `tracing::debug!` at the decision site
//! (diagnostics ride tracing, not return types). The constants are tuning
//! knobs for the unseeded (generic-adaptor) path; a provider adaptor's
//! [`FetchSeeds`] supersedes the starting points.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep_until};

use crate::chain::{BlockCount, ChunkSize};
use crate::evm::provider::{Concurrency, FetchSeeds};

/// Clean slices at the current working size before one additive regrow
/// step.
const REGROW_AFTER: u64 = 8;
/// The additive regrow step is `ceiling / REGROW_DIVISOR` (at least one
/// block), so recovery from a halve takes a bounded number of streaks
/// regardless of the ceiling.
const REGROW_DIVISOR: u64 = 8;
/// Request pause after a rate limit that states no `retry_after`.
const DEFAULT_PAUSE: Duration = Duration::from_secs(1);
/// Rate-limit-free time before concurrency recovers one step; also the
/// pacing between consecutive recovery steps.
const QUIET_PERIOD: Duration = Duration::from_secs(10);

/// Per-endpoint adaptive state: the slice-width dial + the request-slot
/// pool. See the module docs; the methods here just route to the right
/// machine.
pub(crate) struct EndpointBudget {
    size: SliceSize,
    slots: RequestSlots,
}

impl EndpointBudget {
    /// State seeded from a provider adaptor's known limits ([`FetchSeeds`])
    /// so the AIMD starts converged instead of probing.
    pub(crate) fn new(seeds: FetchSeeds) -> Self {
        Self {
            size: SliceSize::new(seeds.max_range),
            slots: RequestSlots::new(seeds.concurrency),
        }
    }

    /// The current slice width, clamped to the caller's ceiling (the
    /// range-index [`ChunkSize`] — alignment and placeholder granularity;
    /// the working size never exceeds one placeholder's span).
    pub(crate) fn working_size(&self, ceiling: ChunkSize) -> BlockCount {
        self.size.current(ceiling)
    }

    /// The stamp to carve slices under — see [`Generation`].
    pub(crate) fn generation(&self) -> Generation {
        self.size.generation()
    }

    /// The current concurrency target. The executor uses it as its launch
    /// window, so in-flight slices track shrinks and recoveries.
    pub(crate) fn concurrency(&self) -> usize {
        self.slots.target()
    }

    /// Wait out any rate-limit pause, then take an in-flight slot. Every
    /// wire request the executor issues — slices and header fills alike —
    /// holds one of these across its round-trip.
    pub(crate) async fn acquire(&self) -> BudgetPermit<'_> {
        self.slots.acquire().await
    }

    /// A slice was rejected as too large — size-dial news only.
    pub(crate) fn on_range_too_large(
        &self,
        stamp: Generation,
        suggested: Option<BlockCount>,
        ceiling: ChunkSize,
    ) {
        self.size.on_too_large(stamp, suggested, ceiling);
    }

    /// The endpoint asked to slow down — slot-pool news only. The working
    /// size is untouched: shrinking it here would *raise* total request
    /// count against a throttling endpoint (decision 3).
    pub(crate) fn on_rate_limited(&self, retry_after: Option<Duration>) {
        self.slots.on_rate_limited(retry_after);
    }

    /// A slice completed cleanly at the current size — good news for both
    /// machines: the size dial counts its streak, the slot pool checks
    /// whether the quiet period has earned a slot back.
    pub(crate) fn on_clean_slice(&self, ceiling: ChunkSize) {
        self.size.on_clean(ceiling);
        self.slots.maybe_recover();
    }
}

// ============================================================
// SliceSize — the width dial
// ============================================================

/// A working-size generation stamp. The executor reads one when it carves
/// a slice; on a `RangeTooLarge` failure the stamp decides whether this
/// failure is news (first of its generation → shrink) or an echo of a
/// congestion event already handled (FETCH.md §5, decision 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Generation(u64);

/// The slice-width dial: multiplicative decrease (halve, or adopt the
/// provider's hint) once per generation, additive regrow after a clean
/// streak. Pure atomics — no time, no tokens.
struct SliceSize {
    /// Blocks per slice; `u64::MAX` = unlearned (reads clamp to the
    /// caller's [`ChunkSize`] ceiling, so unlearned means "start at the
    /// ceiling and probe down").
    working: AtomicU64,
    /// The generation counter behind [`Generation`].
    generation: AtomicU64,
    /// Clean slices since the last size change, for the additive regrow.
    clean_streak: AtomicU64,
}

impl SliceSize {
    fn new(seed: Option<BlockCount>) -> Self {
        Self {
            working: AtomicU64::new(seed.map_or(u64::MAX, BlockCount::get)),
            generation: AtomicU64::new(0),
            clean_streak: AtomicU64::new(0),
        }
    }

    fn current(&self, ceiling: ChunkSize) -> BlockCount {
        BlockCount::new(self.working.load(Ordering::Acquire).min(ceiling.get()))
    }

    fn generation(&self) -> Generation {
        Generation(self.generation.load(Ordering::Acquire))
    }

    /// Adopt the provider's suggested width when the payload carries one,
    /// else halve — once per generation: only the first failure carrying
    /// the current stamp shrinks; peers and stragglers are the same
    /// congestion event.
    fn on_too_large(&self, stamp: Generation, suggested: Option<BlockCount>, ceiling: ChunkSize) {
        if self
            .generation
            .compare_exchange(stamp.0, stamp.0 + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let effective = self.current(ceiling).get();
        // A failure never *raises* the size: a suggested width is trusted
        // downward only.
        let new = suggested.map_or_else(|| (effective / 2).max(1), |s| s.get().clamp(1, effective));
        self.working.store(new, Ordering::Release);
        self.clean_streak.store(0, Ordering::Relaxed);
        tracing::debug!(
            generation = stamp.0 + 1,
            working = new,
            suggested = suggested.map(BlockCount::get),
            "range too large: working size shrunk"
        );
    }

    /// Count a clean slice; after [`REGROW_AFTER`] in a row, step the
    /// width back up toward the ceiling.
    fn on_clean(&self, ceiling: ChunkSize) {
        let streak = self.clean_streak.fetch_add(1, Ordering::AcqRel) + 1;
        if streak < REGROW_AFTER {
            return;
        }
        self.clean_streak.store(0, Ordering::Relaxed);
        let effective = self.current(ceiling).get();
        if effective < ceiling.get() {
            let step = (ceiling.get() / REGROW_DIVISOR).max(1);
            let new = effective.saturating_add(step).min(ceiling.get());
            self.working.store(new, Ordering::Release);
            tracing::debug!(working = new, "clean streak: working size regrown");
        }
    }
}

// ============================================================
// RequestSlots — the token pool
// ============================================================

/// The in-flight-request pool: a semaphore of slots plus everything that
/// governs it — the deficit-forget shrink, the rate-limit pause, and the
/// paced quiet-period recovery. All the clock state lives here; it exists
/// only to pace this pool.
struct RequestSlots {
    /// The slots themselves. Grown via `add_permits`; shrunk via the
    /// deficit (permits are `forget`-acquired and re-added on release, so
    /// a release can be swallowed instead).
    inflight: Semaphore,
    /// Permits to swallow on release instead of returning — the pending
    /// shrink.
    deficit: AtomicUsize,
    /// Current slot target, in `[1, seed]`.
    target: AtomicUsize,
    /// The seeded slot count — the recovery ceiling.
    seed: usize,
    /// The pause and the quiet-period tracking. Rarely contended —
    /// touched on rate limits and recovery checks only.
    timing: Mutex<Timing>,
}

struct Timing {
    /// Requests wait until this instant before acquiring a slot.
    paused_until: Instant,
    /// When the endpoint last rate-limited us — the quiet-period anchor.
    last_rate_limit: Option<Instant>,
    /// The last recovery step, for pacing.
    last_grow: Instant,
}

impl RequestSlots {
    fn new(seed: Concurrency) -> Self {
        let seed = seed.get();
        Self {
            inflight: Semaphore::new(seed),
            deficit: AtomicUsize::new(0),
            target: AtomicUsize::new(seed),
            seed,
            timing: Mutex::new(Timing {
                paused_until: Instant::now(),
                last_rate_limit: None,
                last_grow: Instant::now(),
            }),
        }
    }

    fn target(&self) -> usize {
        self.target.load(Ordering::Acquire)
    }

    async fn acquire(&self) -> BudgetPermit<'_> {
        loop {
            let until = self.timing.lock().unwrap().paused_until;
            if until <= Instant::now() {
                break;
            }
            sleep_until(until).await;
        }
        let permit = self
            .inflight
            .acquire()
            .await
            .expect("budget semaphore is never closed");
        // Manual return-on-release (see BudgetPermit::drop) so a pending
        // shrink can swallow the slot instead.
        permit.forget();
        BudgetPermit { slots: self }
    }

    /// Drop the target by one (floor 1) and pause requests until
    /// `retry_after` (default [`DEFAULT_PAUSE`]; an existing longer pause
    /// stands).
    fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let mut target = self.target.load(Ordering::Acquire);
        while target > 1 {
            match self.target.compare_exchange_weak(
                target,
                target - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.deficit.fetch_add(1, Ordering::AcqRel);
                    target -= 1;
                    break;
                }
                Err(current) => target = current,
            }
        }
        let pause = retry_after.unwrap_or(DEFAULT_PAUSE);
        let now = Instant::now();
        let mut timing = self.timing.lock().unwrap();
        timing.paused_until = timing.paused_until.max(now + pause);
        timing.last_rate_limit = Some(now);
        tracing::debug!(
            concurrency = target,
            pause = ?pause,
            "rate limited: concurrency shrunk, requests paused"
        );
    }

    /// One `+1` step, when shrunk below the seed and the endpoint has
    /// been rate-limit-free for a [`QUIET_PERIOD`] — paced, so recovery
    /// is one step per period, not a burst.
    fn maybe_recover(&self) {
        if self.target.load(Ordering::Acquire) >= self.seed {
            return;
        }
        let now = Instant::now();
        {
            let mut timing = self.timing.lock().unwrap();
            let quiet = timing
                .last_rate_limit
                .is_none_or(|at| now.duration_since(at) >= QUIET_PERIOD);
            if !quiet || now.duration_since(timing.last_grow) < QUIET_PERIOD {
                return;
            }
            timing.last_grow = now;
        }
        let target = self.target.fetch_add(1, Ordering::AcqRel) + 1;
        // Cancel a pending shrink if one is queued; otherwise mint a slot.
        if !self.consume_deficit() {
            self.inflight.add_permits(1);
        }
        tracing::debug!(concurrency = target, "quiet period: concurrency recovered");
    }

    /// Absorb one unit of pending shrink. `false` when none is pending.
    fn consume_deficit(&self) -> bool {
        let mut deficit = self.deficit.load(Ordering::Acquire);
        while deficit > 0 {
            match self.deficit.compare_exchange_weak(
                deficit,
                deficit - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => deficit = current,
            }
        }
        false
    }
}

/// An in-flight request slot from [`EndpointBudget::acquire`]. Dropping
/// releases the slot — or swallows it when a shrink is pending, which is
/// how a shrink converges without blocking on outstanding requests.
pub(crate) struct BudgetPermit<'a> {
    slots: &'a RequestSlots,
}

impl Drop for BudgetPermit<'_> {
    fn drop(&mut self) {
        if !self.slots.consume_deficit() {
            self.slots.inflight.add_permits(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::advance;

    use super::*;

    fn seeds(concurrency: usize, max_range: Option<u64>) -> FetchSeeds {
        FetchSeeds {
            max_range: max_range.map(BlockCount::new),
            max_logs: None,
            concurrency: Concurrency::new(concurrency),
        }
    }

    const CEILING: ChunkSize = ChunkSize::new(1_000);

    /// Decision 4: concurrent failures from one generation are one
    /// congestion event — stale stamps (peers, stragglers) never compound
    /// the halve.
    #[test]
    fn halves_once_per_generation() {
        let budget = EndpointBudget::new(seeds(4, None));
        let stamp = budget.generation();

        budget.on_range_too_large(stamp, None, CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(500));

        // Three peers carved under the same generation fail too — echoes.
        for _ in 0..3 {
            budget.on_range_too_large(stamp, None, CEILING);
        }
        assert_eq!(budget.working_size(CEILING), BlockCount::new(500));

        // A failure at the *new* size is news again.
        budget.on_range_too_large(budget.generation(), None, CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(250));
    }

    /// A provider-suggested width is adopted directly — but only
    /// downward, and never above the current size.
    #[test]
    fn adopts_suggested_size_downward_only() {
        let budget = EndpointBudget::new(seeds(4, None));
        budget.on_range_too_large(budget.generation(), Some(BlockCount::new(300)), CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(300));

        // A suggestion wider than the current size is clamped: a failure
        // never raises the size.
        budget.on_range_too_large(budget.generation(), Some(BlockCount::new(5_000)), CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(300));
    }

    /// Unlearned budgets start at whatever ceiling the caller supplies;
    /// seeded ones clamp to it.
    #[test]
    fn unlearned_sentinel_clamps_to_the_ceiling() {
        let unlearned = EndpointBudget::new(seeds(4, None));
        assert_eq!(unlearned.working_size(CEILING), BlockCount::new(1_000));
        assert_eq!(
            unlearned.working_size(ChunkSize::new(50)),
            BlockCount::new(50)
        );

        let seeded = EndpointBudget::new(seeds(4, Some(2_000)));
        assert_eq!(seeded.working_size(CEILING), BlockCount::new(1_000));
        assert_eq!(
            seeded.working_size(ChunkSize::new(4_000)),
            BlockCount::new(2_000)
        );
    }

    /// Additive regrow: a streak of clean slices steps the size back up by
    /// `ceiling / 8`, clamped at the ceiling; a failure resets the streak.
    #[test]
    fn regrows_after_a_clean_streak() {
        let budget = EndpointBudget::new(seeds(4, None));
        budget.on_range_too_large(budget.generation(), None, CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(500));

        for _ in 0..7 {
            budget.on_clean_slice(CEILING);
        }
        assert_eq!(budget.working_size(CEILING), BlockCount::new(500));
        budget.on_clean_slice(CEILING);
        assert_eq!(budget.working_size(CEILING), BlockCount::new(625));

        // The failure resets the streak: 7 pre-failure cleans don't count.
        budget.on_range_too_large(budget.generation(), None, CEILING);
        for _ in 0..7 {
            budget.on_clean_slice(CEILING);
        }
        assert_eq!(budget.working_size(CEILING), BlockCount::new(312));

        // Regrow clamps at the ceiling.
        for _ in 0..1_000 {
            budget.on_clean_slice(CEILING);
        }
        assert_eq!(budget.working_size(CEILING), BlockCount::new(1_000));
    }

    /// Concurrency floors at 1 and recovers one step per quiet period —
    /// paced, never a burst.
    #[tokio::test(start_paused = true)]
    async fn concurrency_floors_and_recovers_paced() {
        let budget = EndpointBudget::new(seeds(2, None));
        budget.on_rate_limited(None);
        assert_eq!(budget.concurrency(), 1);
        budget.on_rate_limited(None);
        assert_eq!(budget.concurrency(), 1, "floor is 1");

        // Still inside the quiet period: no recovery.
        budget.on_clean_slice(CEILING);
        assert_eq!(budget.concurrency(), 1);

        advance(QUIET_PERIOD).await;
        budget.on_clean_slice(CEILING);
        assert_eq!(budget.concurrency(), 2);

        // At the seed: no growth past it.
        advance(QUIET_PERIOD).await;
        budget.on_clean_slice(CEILING);
        assert_eq!(budget.concurrency(), 2);
    }

    /// Recovery's other branch: when no shrink is pending (the deficit
    /// was already absorbed by a release), recovering a slot must mint a
    /// fresh permit — cancelling is not an option.
    #[tokio::test(start_paused = true)]
    async fn recovery_mints_a_slot_when_no_shrink_is_pending() {
        let budget = EndpointBudget::new(seeds(2, None));
        let permit = budget.acquire().await;
        budget.on_rate_limited(None); // target 1, one pending shrink
        drop(permit); // absorbs the shrink
        assert_eq!(budget.slots.inflight.available_permits(), 1);

        advance(QUIET_PERIOD).await;
        budget.on_clean_slice(CEILING);
        assert_eq!(budget.concurrency(), 2);
        assert_eq!(
            budget.slots.inflight.available_permits(),
            2,
            "no deficit left to cancel — the slot must be minted"
        );
    }

    /// The deficit-forget scheme: a shrink is absorbed by the next permit
    /// release instead of blocking on it.
    #[tokio::test]
    async fn shrink_swallows_the_next_release() {
        let budget = EndpointBudget::new(seeds(2, None));
        let first = budget.acquire().await;
        let second = budget.acquire().await;

        budget.on_rate_limited(None);
        drop(first); // swallowed by the pending shrink
        assert_eq!(budget.slots.inflight.available_permits(), 0);
        drop(second); // returned normally
        assert_eq!(budget.slots.inflight.available_permits(), 1);
    }

    /// The rate-limit pause gates `acquire`, takes the longest outstanding
    /// pause, and expires.
    #[tokio::test(start_paused = true)]
    async fn rate_limit_pause_gates_acquire() {
        use tokio::time::timeout;

        let budget = EndpointBudget::new(seeds(2, None));
        budget.on_rate_limited(Some(Duration::from_secs(10)));
        // A later, shorter pause never shortens the standing one.
        budget.on_rate_limited(Some(Duration::from_secs(1)));

        assert!(
            timeout(Duration::from_secs(9), budget.acquire())
                .await
                .is_err(),
            "acquire must wait out the pause"
        );
        let _permit = timeout(Duration::from_secs(2), budget.acquire())
            .await
            .expect("pause expired");
    }
}
