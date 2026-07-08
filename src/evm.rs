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

mod aligned_ranges;
mod cache;
mod chain;
mod chain_provider;
pub mod erc20;
pub mod observe;
mod owned_erc20;
mod range_index;
mod transport;
mod types;

pub use aligned_ranges::{AlignedRange, Alignment, aligned_block_ranges};
pub use types::{
    Amount, Asset, BlockCount, BlockHash, BlockNumber, BlockRange, ChainId, ChunkSize, Erc20,
    Erc20Balance, Erc20Burn, Erc20Mint, Erc20Transfer, EvmBlock, EvmEvent, EvmPosition, FilterHash,
    Gas, GasPrice, LogIdx, Topic, TxCount, TxHash,
};

pub use cache::{
    BlockHeader, CACHE_VERSION, CacheDb, CacheError, CacheSection, CacheTable, CachedErc20Metadata,
    Erc20Cache, EventSignatureCache, EventSignatureRow, LogCache, ScanPartition, TableCount,
    TableStat, compute_filter_hash, compute_gaps, versioned_path,
};
pub use chain::{
    DEFAULT_BLOCK_TIME, DEFAULT_CHUNK_SIZE, EvmChain, EvmChainConfig, Finality, LogFilter,
    SubscribeTransport,
};
pub use chain_provider::ChainProvider;
pub use erc20::{
    Erc20Metadata, TRANSFER_TOPIC, TransferData, apply_transfer, decode_transfer,
    fetch_erc20_metadata,
};
pub use observe::{
    CACHE_TARGET, CONNECT_TARGET, CacheLayer, CacheOutcome, ConnectTimingLayer, RPC_TARGET,
    SETUP_TARGET, SQLITE_TARGET, cache_event, connect_span,
};
pub use owned_erc20::{IN_KEY, OUT_KEY, OwnedErc20, TransferDirection};
pub use range_index::{
    BlockPage, BlockPos, BlockSpec, Fetcher, IndexPos, LoadedSpan, LogPage, LogRef, LogSpec,
    RangeIndex, RangeIndexStore, RangeSpec, TimePos, TipSeed,
};
pub use transport::{RpcTimingLayer, rpc_retry_layer};
