//! EVM-specific implementation for chainfold.
//!
//! Concrete `HistStoreBackend` for EVM log streams, layered over:
//! - [`LogStore`] — durable SQLite persistence (logs, scanned ranges,
//!   block headers, erc20 metadata/balance tables).
//! - [`LogIndex`] — in-memory range tree on top of [`LogStore`].
//! - [`EvmLogHistory`] — `HistStoreBackend<Positioned<EvmEvent,
//!   EvmPosition>, EvmPosition>` plus the push-subscription path.
//!
//! Requires the `evm` feature flag.

mod aligned_ranges;
mod log_store;
mod log_index;
mod history;
mod retry;
mod subscriber;
pub mod erc20;

pub use aligned_ranges::{aligned_block_ranges, AlignedRange};

pub use log_store::{
    compute_filter_hash, compute_gaps, versioned_path, BlockHeader, CacheError,
    CachedErc20Metadata, LogStore, CACHE_VERSION,
};
pub use log_index::{BlockPos, LogIndex, LogPage, LogRef};
pub use history::EvmLogHistory;
pub use retry::rpc_retry_layer;
pub use subscriber::{
    AsyncCallback, EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks,
};
pub use erc20::{
    apply_transfer, decode_transfer, fetch_erc20_metadata, Erc20Metadata, TransferData,
    TRANSFER_TOPIC,
};
