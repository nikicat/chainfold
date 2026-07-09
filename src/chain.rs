//! Shared scaffolding for height-organized chain families.
//!
//! Every blockchain source chainfold speaks to (EVM, Bitcoin, …) organizes
//! its history as a linear sequence of blocks addressed by a `u64` height.
//! This module holds the domain types that are *about that shape* and
//! therefore identical across families: block heights and ranges
//! ([`BlockNumber`], [`BlockRange`]), block counts ([`BlockCount`],
//! [`ChunkSize`], [`TxCount`]).
//!
//! What does **not** live here: anything whose shape or display differs per
//! source — block/tx hashes (Bitcoin's byte-reversed hex vs. EVM's `0x`
//! hex), positions richer than a height, gas/fee scalars. Each source
//! defines those in its own module (`crate::evm::types`, …), per the layering
//! rule in [`crate::arch`].

mod height;

pub use height::{BlockCount, BlockNumber, BlockRange, ChunkSize, TxCount};
