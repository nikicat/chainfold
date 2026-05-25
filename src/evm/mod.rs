//! EVM-specific implementation for chainfold.
//!
//! Concrete `HistStoreBackend` for EVM log streams, layered over:
//! - [`LogStore`] — durable `SQLite` persistence (logs, scanned ranges,
//!   block headers, erc20 metadata/balance tables).
//! - [`LogIndex`] — in-memory range tree on top of [`LogStore`].
//! - [`EvmLogHistory`] — `HistStoreBackend<Positioned<EvmEvent,
//!   EvmPosition>, EvmPosition>` plus the push-subscription path.
//!
//! Requires the `evm` feature flag.

mod aligned_ranges;
pub mod erc20;
mod history;
mod log_index;
mod log_store;
mod owned_erc20;
mod retry;
mod subscriber;
mod types;

pub use aligned_ranges::{AlignedRange, aligned_block_ranges};
pub use types::{
    Amount, Asset, Erc20, Erc20Balance, Erc20Burn, Erc20Mint, Erc20Transfer, EvmBlock, EvmEvent,
    EvmPosition, EvmTransaction,
};

pub use erc20::{
    Erc20Metadata, TRANSFER_TOPIC, TransferData, apply_transfer, decode_transfer,
    fetch_erc20_metadata,
};
pub use history::EvmLogHistory;
pub use log_index::{BlockPos, LogIndex, LogPage, LogRef};
pub use log_store::{
    BlockHeader, CACHE_VERSION, CacheError, CachedErc20Metadata, EventSignatureRow, LogStore,
    compute_filter_hash, compute_gaps, versioned_path,
};
pub use owned_erc20::{IN_KEY, OUT_KEY, OwnedErc20};
pub use retry::rpc_retry_layer;
pub use subscriber::{AsyncCallback, EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks};
