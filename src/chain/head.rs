//! Shared head-tracking scaffolding: the chain-wide head channel
//! ([`HeadCell`]), the demand-started clock owner ([`HeadSeeder`]), and the
//! per-family session seam ([`HeadClock`]).
//!
//! Every chain family runs at most **one** head-tracking task per chain,
//! regardless of how many stores exist. Tracking is demand-started: chain
//! construction performs no RPC and spawns nothing; the first demand for a
//! tip (a consumer awaiting a snapshot, or a time-position estimate
//! resolving its anchor) starts the clock exactly once, and it then runs
//! for the life of the chain and its stores (no stop-on-idle yet).
//!
//! The family supplies only the [`HeadClock`] *session* — "watch the chain
//! and publish heads until something breaks" (an `eth_subscribe(newHeads)`
//! stream, an `eth_blockNumber` / `getblockcount` poll loop, …). The
//! reconnect-with-backoff loop around sessions lives here, once.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::sleep;
use tokio_util::task::AbortOnDropHandle;

use crate::arch::HistoryError;
use crate::chain::BlockNumber;
use crate::range_index::TipSeed;
use crate::share::Share;

/// Reconnect backoff bounds shared by the head-clock runner and any
/// family-side reconnecting wiring (e.g. the EVM ws log session).
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Chain-wide finality tier (see `docs/ARCHITECTURE.md`, "three finality
/// levels"). Until Phase 7 wires `best`/`safe` producers into the head
/// clock, all tiers resolve to `Latest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Finality {
    /// Chain head as announced (may reorg).
    Latest,
    /// Mined + confidence heuristic. **Currently mirrors `Latest`.**
    Best,
    /// Finalized. **Currently mirrors `Latest`.**
    Safe,
}

/// One observation of the chain head, as published on the internal head
/// channel by the clock. A family's clock may attach extra guarantees to
/// its publications (the EVM ws clock persists the block header *before*
/// the event goes out — its headers-present invariant); the shared channel
/// carries only the number.
#[derive(Clone, Copy, Debug)]
pub struct HeadEvent {
    pub number: BlockNumber,
}

/// The chain-wide head channel: `None` until the first observation;
/// publishes are monotonic. The chain-level analog of the store's
/// unset-until-seeded tip.
#[derive(Clone)]
pub struct HeadCell {
    chan: watch::Sender<Option<HeadEvent>>,
}

impl Default for HeadCell {
    fn default() -> Self {
        Self::new()
    }
}

impl HeadCell {
    pub fn new() -> Self {
        Self {
            chan: watch::channel(None).0,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<HeadEvent>> {
        self.chan.subscribe()
    }

    /// Monotonic publish: a no-op unless `number` advances past the
    /// current head.
    pub fn publish(&self, number: BlockNumber) {
        self.chan.send_if_modified(|cur| match cur {
            Some(c) if c.number >= number => false,
            _ => {
                *cur = Some(HeadEvent { number });
                true
            }
        });
    }

    /// Await the first head observation — purely reactive, no fetching.
    pub async fn observed(&self) -> BlockNumber {
        let mut rx = self.subscribe();
        loop {
            if let Some(h) = *rx.borrow_and_update() {
                return h.number;
            }
            // `self` holds the sender — the channel cannot close.
            rx.changed().await.expect("head sender lives with the cell");
        }
    }
}

/// One head-tracking session: watch the chain and publish observations into
/// `head` until something breaks, then return the error that ended the
/// session. The shared runner ([`HeadSeeder`]) reconnects sessions with
/// exponential backoff — a session must not loop-and-backoff itself.
#[async_trait::async_trait]
pub trait HeadClock: Send + Sync + 'static {
    async fn session(&self, head: &HeadCell) -> HistoryError;
}

/// Chain-side [`TipSeed`]: the first demand for a tip starts head tracking
/// (the family's [`HeadClock`] sessions under the shared backoff runner),
/// exactly once; observed heads flow through the [`HeadCell`] to every
/// store's wiring. Tracking runs until the chain and all its stores drop
/// (no stop-on-idle yet).
pub struct HeadSeeder {
    clock: Arc<dyn HeadClock>,
    head: HeadCell,
    task: OnceLock<AbortOnDropHandle<()>>,
}

impl HeadSeeder {
    /// Wrap a family clock. Spawns nothing — the clock starts on the first
    /// [`request`](TipSeed::request).
    pub fn new(clock: Arc<dyn HeadClock>) -> Self {
        Self {
            clock,
            head: HeadCell::new(),
            task: OnceLock::new(),
        }
    }

    /// The chain-wide head channel.
    pub fn head(&self) -> &HeadCell {
        &self.head
    }
}

impl TipSeed for HeadSeeder {
    fn request(&self) {
        self.task.get_or_init(|| {
            AbortOnDropHandle::new(tokio::spawn(run_clock(
                self.clock.share(),
                self.head.clone(),
            )))
        });
    }
}

/// Run [`HeadClock::session`]s forever, reconnecting with exponential
/// backoff. Aborted (via the seeder's `AbortOnDropHandle`) when the chain
/// and all its stores drop.
async fn run_clock(clock: Arc<dyn HeadClock>, head: HeadCell) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let err = clock.session(&head).await;
        tracing::warn!(
            error = %err,
            backoff_ms = backoff.as_millis(),
            "head clock error, will reconnect"
        );
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
