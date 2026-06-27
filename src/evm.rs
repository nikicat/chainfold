//! EVM-specific implementation for chainfold.
//!
//! Concrete `HistStoreBackend` for EVM log streams, layered over:
//! - [`CacheDb`] — durable `SQLite` persistence, vending per-concern views:
//!   [`LogCache`] (logs, scanned ranges, block headers), [`Erc20Cache`]
//!   (metadata/balances), and [`EventSignatureCache`] (resolver cache).
//! - [`LogIndex`] — in-memory range tree on top of [`LogCache`].
//! - [`EvmLogHistory`] — `HistStoreBackend<Positioned<EvmEvent,
//!   EvmPosition>, EvmPosition>` plus the push-subscription path.
//!
//! Requires the `evm` feature flag.

mod aligned_ranges;
mod cache;
mod chain_provider;
pub mod erc20;
mod history;
mod log_index;
pub mod observe;
mod owned_erc20;
mod retry;
mod subscriber;
mod types;

pub use aligned_ranges::{AlignedRange, aligned_block_ranges};
pub use types::{
    Amount, Asset, Erc20, Erc20Balance, Erc20Burn, Erc20Mint, Erc20Transfer, EvmBlock, EvmEvent,
    EvmPosition, EvmTransaction,
};

pub use cache::{
    BlockHeader, CACHE_VERSION, CacheDb, CacheError, CachedErc20Metadata, Erc20Cache,
    EventSignatureCache, EventSignatureRow, LogCache, compute_filter_hash, compute_gaps,
    versioned_path,
};
pub use chain_provider::ChainProvider;
pub use erc20::{
    Erc20Metadata, TRANSFER_TOPIC, TransferData, apply_transfer, decode_transfer,
    fetch_erc20_metadata,
};
pub use history::EvmLogHistory;
pub use log_index::{BlockPos, LogIndex, LogPage, LogRef};
pub use observe::{
    CACHE_TARGET, CacheLayer, CacheOutcome, RPC_TARGET, RpcMethod, SQLITE_TARGET, SqliteOp,
    cache_event, rpc_span, sqlite_span,
};
pub use owned_erc20::{IN_KEY, OUT_KEY, OwnedErc20};
pub use retry::rpc_retry_layer;
pub use subscriber::{AsyncCallback, EvmLogSubscriber, SubscribeTransport, SubscriberCallbacks};
