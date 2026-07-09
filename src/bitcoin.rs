//! Bitcoin-specific implementation for chainfold: the block history.
//!
//! Concrete client for the Bitcoin block stream, layered over the same
//! shared engine as the EVM source:
//! - [`CacheDb`](crate::cache::CacheDb) — durable `SQLite` persistence;
//!   this family's view is [`BtcBlockCache`] (`cached_btc_blocks` +
//!   the shared coverage bookkeeping).
//! - [`RangeIndex`](crate::range_index::RangeIndex) — the generic in-memory
//!   range tree, parameterized here by [`BtcBlockSpec`] over the shared
//!   dense [`HeightPos`](crate::range_index::HeightPos) key.
//! - [`BitcoinChain`] — the per-chain entity: one demand-started head
//!   clock (a `getblockcount` poll), [`blocks`](BitcoinChain::blocks) /
//!   [`blocks_hist`](BitcoinChain::blocks_hist).
//!
//! Wire access is bitcoind JSON-RPC: [`BtcRpcClient`] over a
//! reqwest-backed transport, batching `getblockhash` → `getblockheader`
//! (one verbose header carries everything [`BtcBlock`] needs, including
//! `nTx`). Requires the `bitcoin` feature flag.

mod cache;
mod chain;
mod rpc;
mod spec;
mod transport;
mod types;

pub use cache::BtcBlockCache;
pub use chain::{
    BitcoinChain, BitcoinChainConfig, DEFAULT_BTC_BLOCK_TIME, DEFAULT_BTC_CHUNK_SIZE,
    DEFAULT_BTC_POLL_INTERVAL,
};
pub use rpc::BtcRpcClient;
pub use spec::{BtcBlockPage, BtcBlockSpec};
pub use transport::{BtcAuth, ReqwestTransport, TracingTransport};
pub use types::{BtcBlock, BtcBlockHash, CompactTarget, ParseBtcBlockHashError};
