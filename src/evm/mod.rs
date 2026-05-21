//! EVM-specific implementation for chainfold.
//!
//! Phase 1 layout (see `refactor.md`):
//! - [`LogStore`] — durable SQLite layer (schema + slim bulk API).
//! - `LogIndex` — in-memory range tree on top of [`LogStore`] (Phase 1b).
//! - [`LogCache`] — legacy facade over [`LogStore`] preserving the
//!   deprecated `first_log_*` / `last_log_*` navigation methods until
//!   `EvmLogHistory` is rewired in Phase 1c.
//! - [`EvmLogHistory`] — navigates EVM event streams as a
//!   [`History`](crate::arch::History).
//!
//! Requires the `evm` feature flag.

mod aligned_ranges;
mod log_store;
mod log_index;
mod log_cache_legacy;
mod history;
mod retry;
pub mod erc20;

pub use aligned_ranges::{aligned_block_ranges, AlignedRange};

pub use log_store::{
    compute_filter_hash, compute_gaps, versioned_path, BlockHeader, CacheError,
    CachedErc20Metadata, LogStore, CACHE_VERSION,
};
#[allow(deprecated)]
pub use log_cache_legacy::LogCache;
pub use history::EvmLogHistory;
pub use retry::rpc_retry_layer;
pub use erc20::{
    decode_transfer, evm_pos_cmp, fetch_erc20_metadata, Erc20BalanceReducer, Erc20Metadata,
    TransferData, TRANSFER_TOPIC,
};
