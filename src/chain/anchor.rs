//! Wall-clock → block-height estimation, shared by every chain family.
//!
//! An estimate needs one `(block, timestamp)` anchor plus the chain's
//! nominal block time; from there it is pure arithmetic. The anchor
//! resolves lazily — the first use demands the head (starting tracking if
//! needed), awaits the observation reactively, then asks the family for
//! that block's timestamp through [`BlockTimeSource`] — at most once per
//! chain.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell;

use crate::arch::{HistoryError, Timestamp};
use crate::chain::BlockNumber;
use crate::chain::head::HeadSeeder;
use crate::range_index::TipSeed;

/// Anchor for wall-clock → block estimation: a recent `(block, timestamp)`
/// observation. Anchored at a recent head rather than genesis because some
/// chains use timestamp 0 there.
#[derive(Clone, Copy)]
pub struct TimeAnchor {
    pub block: BlockNumber,
    pub time: Timestamp,
}

/// The one family-specific piece of anchoring: "the timestamp of block
/// `n`". Implementations must be cache-first and cheap on repeat (for EVM:
/// `ensure_block_header`, singleflight, at most one header fetch per
/// chain).
#[async_trait::async_trait]
pub trait BlockTimeSource: Send + Sync {
    async fn block_time_at(&self, n: BlockNumber) -> Result<Timestamp, HistoryError>;
}

/// Lazy shared resolver for the chain's [`TimeAnchor`]: demands the head
/// (starting tracking if needed), awaits the observation reactively, then
/// reads the block time through the family's [`BlockTimeSource`] — at most
/// one resolution per chain.
#[derive(Clone)]
pub struct LazyAnchor {
    seeder: Arc<HeadSeeder>,
    source: Arc<dyn BlockTimeSource>,
    cell: Arc<OnceCell<TimeAnchor>>,
}

impl LazyAnchor {
    /// Bundle the head machinery with the family's block-time reader. The
    /// `cell` is chain-owned and shared across clones, so every estimator
    /// on the chain resolves the anchor at most once between them.
    pub fn new(
        seeder: Arc<HeadSeeder>,
        source: Arc<dyn BlockTimeSource>,
        cell: Arc<OnceCell<TimeAnchor>>,
    ) -> Self {
        Self {
            seeder,
            source,
            cell,
        }
    }

    pub async fn get(&self) -> Result<TimeAnchor, HistoryError> {
        self.cell
            .get_or_try_init(|| async {
                self.seeder.request();
                let block = self.seeder.head().observed().await;
                let time = self.source.block_time_at(block).await?;
                Ok(TimeAnchor { block, time })
            })
            .await
            .copied()
    }
}

/// Extrapolate the block mined around wall-clock `time` from an anchor and
/// the chain's nominal block time. Pure arithmetic; clamps at genesis.
///
/// A **rough initial guess only** — real cadence drifts (Bitcoin's
/// especially), so consumers refine it against real block timestamps via
/// [`TimeLocator::locate`].
pub fn estimate_block_from_time(
    anchor_block: BlockNumber,
    anchor_time: Timestamp,
    block_time: Duration,
    time: Timestamp,
) -> BlockNumber {
    let block_time = block_time.as_secs().max(1);
    if time >= anchor_time {
        anchor_block + (time.get() - anchor_time.get()) / block_time
    } else {
        anchor_block.saturating_sub((anchor_time.get() - time.get()) / block_time)
    }
}

// ============================================================
// TimeLocator — refined wall-clock → height search
// ============================================================

/// Interpolation probes before the search falls back to pure bisection.
/// Real chains have near-linear timestamps, so interpolation converges in a
/// handful of probes; the cap keeps adversarial distributions O(log n).
const MAX_INTERPOLATION_PROBES: u32 = 16;

/// Wall-clock → block-height search: a nominal-cadence guess
/// ([`estimate_block_from_time`]) used **only to seed** an
/// interpolation/bisection search over real block timestamps (read through
/// the family's cache-first [`BlockTimeSource`], so warm searches are
/// offline and every probe warms the cache for the next one).
///
/// [`locate`](Self::locate) returns the first block whose timestamp is
/// at/after the target — exact on monotonic timestamps; off by at most the
/// local wobble on chains (Bitcoin) whose consensus only enforces
/// median-time monotonicity.
#[derive(Clone)]
pub struct TimeLocator {
    anchor: LazyAnchor,
    source: Arc<dyn BlockTimeSource>,
    block_time: Duration,
}

impl TimeLocator {
    /// Bundle the head machinery with the family's block-time reader; the
    /// chain-owned `cell` shares one anchor resolution across every
    /// estimator on the chain.
    pub fn new(
        seeder: Arc<HeadSeeder>,
        source: Arc<dyn BlockTimeSource>,
        cell: Arc<OnceCell<TimeAnchor>>,
        block_time: Duration,
    ) -> Self {
        Self {
            anchor: LazyAnchor::new(seeder, source.clone(), cell),
            source,
            block_time,
        }
    }

    /// The first block whose timestamp is at/after `time`, clamped to the
    /// anchor head (a time at/after the anchor observation returns the
    /// anchor block — new blocks arrive through the head clock, not through
    /// this search). The first call resolves the anchor (head demand + one
    /// block-time read); each search costs a handful of cache-first
    /// timestamp probes.
    pub async fn locate(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        let anchor = self.anchor.get().await?;
        if time >= anchor.time {
            return Ok(anchor.block);
        }

        // Bracket invariant: t(hi) >= time; t(lo) < time where probed (the
        // genesis floor starts unprobed — see the epilogue).
        let mut lo = BlockNumber::ZERO;
        let mut t_lo: Option<Timestamp> = None;
        let mut hi = anchor.block;
        let mut t_hi = anchor.time;

        // Seed with the nominal-cadence guess; afterwards interpolate the
        // shrinking bracket, falling back to bisection past the cap.
        let mut guess = Some(estimate_block_from_time(
            anchor.block,
            anchor.time,
            self.block_time,
            time,
        ));
        let mut probes = 0u32;
        while hi.get() - lo.get() > 1 {
            let mid = guess
                .take()
                .filter(|_| probes < MAX_INTERPOLATION_PROBES)
                .map_or_else(
                    || BlockNumber::new(lo.get() + (hi.get() - lo.get()) / 2),
                    |g| BlockNumber::new(g.get().clamp(lo.get() + 1, hi.get() - 1)),
                );
            let t = self.source.block_time_at(mid).await?;
            if t >= time {
                hi = mid;
                t_hi = t;
            } else {
                lo = mid;
                t_lo = Some(t);
            }
            guess = interpolate(lo, t_lo, hi, t_hi, time);
            probes += 1;
        }

        // The loop never probes below `lo + 1`, so the genesis floor stays
        // unprobed: a pre-chain target must land on block 0, not 1.
        if hi.get() == 1
            && t_lo.is_none()
            && self.source.block_time_at(BlockNumber::ZERO).await? >= time
        {
            return Ok(BlockNumber::ZERO);
        }
        Ok(hi)
    }
}

/// Linear interpolation of `target` between the bracket endpoints' real
/// timestamps. `None` until the low end has been probed (no left anchor to
/// interpolate from) or when the bracket's timestamps collapse.
fn interpolate(
    lo: BlockNumber,
    t_lo: Option<Timestamp>,
    hi: BlockNumber,
    t_hi: Timestamp,
    target: Timestamp,
) -> Option<BlockNumber> {
    let t_lo = t_lo?;
    let span_secs = t_hi.get().checked_sub(t_lo.get())?;
    if span_secs == 0 {
        return None;
    }
    let dt = u128::from(target.get().saturating_sub(t_lo.get()));
    let blocks = u128::from(hi.get() - lo.get());
    let offset = (dt * blocks) / u128::from(span_secs);
    Some(BlockNumber::new(lo.get() + offset as u64))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::chain::head::{HeadCell, HeadClock};

    /// Session that publishes one fixed head and then parks forever.
    struct FixedHead(BlockNumber);

    #[async_trait::async_trait]
    impl HeadClock for FixedHead {
        async fn session(&self, head: &HeadCell) -> HistoryError {
            head.publish(self.0);
            std::future::pending().await
        }
    }

    /// Timestamps from a height → seconds function; counts probes.
    struct FnSource<F: Fn(u64) -> u64 + Send + Sync> {
        f: F,
        probes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl<F: Fn(u64) -> u64 + Send + Sync> BlockTimeSource for FnSource<F> {
        async fn block_time_at(&self, n: BlockNumber) -> Result<Timestamp, HistoryError> {
            self.probes.fetch_add(1, Ordering::Relaxed);
            Ok(Timestamp::new((self.f)(n.get())))
        }
    }

    fn locator<F: Fn(u64) -> u64 + Send + Sync + 'static>(
        head: u64,
        block_time: Duration,
        f: F,
    ) -> (TimeLocator, Arc<FnSource<F>>) {
        let source = Arc::new(FnSource {
            f,
            probes: AtomicUsize::new(0),
        });
        let seeder = Arc::new(HeadSeeder::new(
            Arc::new(FixedHead(BlockNumber::new(head))) as Arc<dyn HeadClock>
        ));
        let loc = TimeLocator::new(
            seeder,
            source.clone() as Arc<dyn BlockTimeSource>,
            Arc::new(OnceCell::new()),
            block_time,
        );
        (loc, source)
    }

    async fn locate(loc: &TimeLocator, secs: u64) -> u64 {
        loc.locate(Timestamp::new(secs)).await.unwrap().get()
    }

    #[tokio::test]
    async fn linear_timestamps_locate_exactly() {
        // t(n) = 1000 + 600n — the nominal guess is already exact; the
        // search only confirms the boundary.
        let (loc, _) = locator(1_000, Duration::from_mins(10), |n| 1_000 + 600 * n);
        assert_eq!(locate(&loc, 1_000 + 600 * 500).await, 500, "exact hit");
        // A time strictly inside block 500's slot → the next block is the
        // first at/after it.
        assert_eq!(locate(&loc, 1_000 + 600 * 500 + 1).await, 501);
    }

    #[tokio::test]
    async fn drifting_cadence_is_refined_not_trusted() {
        // Real cadence is 400 s but the nominal config says 600 s — the
        // initial guess lands far off and the probes must correct it.
        let (loc, _) = locator(10_000, Duration::from_mins(10), |n| 1_000 + 400 * n);
        assert_eq!(locate(&loc, 1_000 + 400 * 2_000).await, 2_000);
    }

    #[tokio::test]
    async fn bursty_timestamps_find_the_boundary() {
        // A flat era then a long stall at block 700: everything after the
        // stall is hours later. t is monotonic but far from linear.
        let f = |n: u64| if n < 700 { 1_000 + n } else { 1_000_000 + n };
        let (loc, _) = locator(1_000, Duration::from_mins(10), f);
        assert_eq!(
            locate(&loc, 5_000).await,
            700,
            "first block after the stall"
        );
        assert_eq!(locate(&loc, 1_000 + 699).await, 699, "last pre-stall block");
    }

    #[tokio::test]
    async fn after_anchor_time_returns_anchor_block() {
        let (loc, source) = locator(1_000, Duration::from_mins(10), |n| 1_000 + 600 * n);
        assert_eq!(locate(&loc, 1_000 + 600 * 1_000).await, 1_000);
        assert_eq!(locate(&loc, u64::MAX).await, 1_000);
        // Anchor resolution is the only probe — no search past the tip.
        assert_eq!(source.probes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn pre_genesis_time_lands_on_genesis() {
        let (loc, _) = locator(1_000, Duration::from_mins(10), |n| 1_000 + 600 * n);
        assert_eq!(locate(&loc, 0).await, 0);
        assert_eq!(locate(&loc, 1_000).await, 0, "genesis's own timestamp");
        assert_eq!(locate(&loc, 1_001).await, 1, "just past genesis");
    }

    #[tokio::test]
    async fn probe_count_stays_logarithmic() {
        // Jittery-but-monotonic timestamps over a 1M-block bracket: the
        // search must stay a handful of probes, not scan.
        let f = |n: u64| 600 * n + (n % 7) * 80;
        let (loc, source) = locator(1_000_000, Duration::from_mins(10), f);
        let target = f(123_456);
        assert_eq!(locate(&loc, target).await, 123_456);
        let probes = source.probes.load(Ordering::Relaxed);
        assert!(
            probes <= 2 + 2 * 20,
            "expected O(log) probes over 1M blocks, got {probes}"
        );
    }
}
