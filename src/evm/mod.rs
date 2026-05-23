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

pub use aligned_ranges::{aligned_block_ranges, AlignedRange};

pub use erc20::{
    apply_transfer, decode_transfer, fetch_erc20_metadata, Erc20Metadata, TransferData,
    TRANSFER_TOPIC,
};
pub use history::EvmLogHistory;
pub use log_index::{BlockPos, LogIndex, LogPage, LogRef};
pub use log_store::{
    compute_filter_hash, compute_gaps, versioned_path, BlockHeader, CacheError,
    CachedErc20Metadata, LogStore, CACHE_VERSION,
};
pub use owned_erc20::{OwnedErc20, IN_KEY, OUT_KEY};
pub use retry::rpc_retry_layer;
pub use subscriber::{AsyncCallback, EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks};
