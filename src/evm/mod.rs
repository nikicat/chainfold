//! EVM-specific implementation for chainfold.
//!
//! Provides [`LogCache`] (legacy; being replaced in Phase 1 by `LogStore` +
//! `LogIndex`) for caching `eth_getLogs` results with scanned-range
//! tracking, and [`EvmLogHistory`] for navigating EVM event streams
//! as a [`History`](crate::arch::History).
//!
//! Requires the `evm` feature flag.

mod log_cache_legacy;
mod history;
mod retry;
pub mod erc20;

pub use log_cache_legacy::{compute_filter_hash, compute_gaps, versioned_path, CachedErc20Metadata, LogCache, CACHE_VERSION};
pub use history::EvmLogHistory;
pub use retry::rpc_retry_layer;
pub use erc20::{
    decode_transfer, evm_pos_cmp, fetch_erc20_metadata, Erc20BalanceReducer, Erc20Metadata,
    TransferData, TRANSFER_TOPIC,
};
