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
