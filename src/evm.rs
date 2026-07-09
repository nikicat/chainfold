//! EVM-specific implementation for chainfold.
//!
//! Concrete client for EVM log streams, layered over:
//! - [`CacheDb`] — durable `SQLite` persistence, vending per-concern views:
//!   [`LogCache`] (logs, scanned ranges, block headers), [`Erc20Cache`]
//!   (metadata/balances), and [`EventSignatureCache`] (resolver cache).
//! - [`RangeIndex`] — the generic in-memory range tree (singleflight
//!   gap-fill, late-item handling) parameterized by a [`RangeSpec`];
//!   [`LogSpec`] is the spec for EVM logs.
//! - [`RangeIndexStore`] — the generic reactive layer
//!   (`HistStoreBackend` + `HistBackend`) over a [`RangeIndex`].
//! - [`EvmChain`] — the per-chain entity: one head clock, per-filter
//!   [`EvmChain::logs`] stores, chain-wide [`Finality`].
//!
//! Requires the `evm` feature flag.

mod block_pos;
mod block_spec;
mod cache;
mod chain;
mod chain_provider;
pub mod erc20;
mod log_spec;
mod owned_erc20;
mod transport;
mod types;

pub use types::{
    Amount, Asset, BlockHash, ChainId, Erc20, Erc20Balance, Erc20Burn, Erc20Mint, Erc20Transfer,
    EvmBlock, EvmEvent, EvmPosition, Gas, GasPrice, LogIdx, Topic, TxHash,
};

pub use block_pos::{BlockPos, LogPage, LogRef};
pub use block_spec::{BlockPage, BlockSpec};
pub use cache::{
    BlockHeader, CachedErc20Metadata, Erc20Cache, EventSignatureCache, EventSignatureRow, LogCache,
    compute_filter_hash,
};
pub use chain::{
    DEFAULT_BLOCK_TIME, DEFAULT_CHUNK_SIZE, EvmChain, EvmChainConfig, LogFilter, SubscribeTransport,
};
pub use chain_provider::ChainProvider;
pub use erc20::{
    Erc20Metadata, TRANSFER_TOPIC, TransferData, apply_transfer, decode_transfer,
    fetch_erc20_metadata,
};
pub use log_spec::LogSpec;
pub use owned_erc20::{IN_KEY, OUT_KEY, OwnedErc20, TransferDirection};
pub use transport::{RpcTimingLayer, rpc_retry_layer};
