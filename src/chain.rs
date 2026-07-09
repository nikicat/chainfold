//! Shared scaffolding for height-organized chain families.
//!
//! Every blockchain source chainfold speaks to (EVM, Bitcoin, …) organizes
//! its history as a linear sequence of blocks addressed by a `u64` height.
//! This module holds the domain types that are *about that shape* and
//! therefore identical across families: block heights and ranges
//! ([`BlockNumber`], [`BlockRange`]), block counts ([`BlockCount`],
//! [`ChunkSize`], [`TxCount`]).
//!
//! Beyond the height types, this module holds the chain-family scaffolding
//! that is identical across families:
//! - `head` — the chain-wide head channel ([`HeadCell`]), the demand-started
//!   clock owner ([`HeadSeeder`]), the per-family [`HeadClock`] session
//!   seam, and the chain-wide [`Finality`] tiers.
//! - `anchor` — lazy wall-clock → height estimation ([`LazyAnchor`]) over a
//!   per-family [`BlockTimeSource`].
//! - `wiring` — the per-store task ([`follow_head`]) driving `on_bump` off
//!   the head channel.
//!
//! What does **not** live here: anything whose shape or display differs per
//! source — block/tx hashes (Bitcoin's byte-reversed hex vs. EVM's `0x`
//! hex), positions richer than a height, gas/fee scalars. Each source
//! defines those in its own module (`crate::evm::types`, …), per the layering
//! rule in [`crate::arch`].

mod anchor;
mod head;
mod height;
mod wiring;

pub use anchor::{BlockTimeSource, LazyAnchor, TimeAnchor, estimate_block_from_time};
pub use head::{
    Finality, HeadCell, HeadClock, HeadEvent, HeadSeeder, INITIAL_BACKOFF, MAX_BACKOFF,
};
pub use height::{BlockCount, BlockNumber, BlockRange, ChunkSize, TxCount};
pub use wiring::follow_head;
