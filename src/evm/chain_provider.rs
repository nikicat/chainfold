//! [`ChainProvider`] — an alloy [`Provider`] bundled with the chain-scoped
//! resources every RPC consumer here needs: the chain id it serves and the
//! durable [`LogCache`].
//!
//! A chain id is an attribute of the endpoint, not a value to rediscover at
//! every construction; the cache is likewise per-chain. Bundling both lets
//! [`EvmChain`](crate::evm::EvmChain) internals pass **one** handle around
//! instead of `(provider, cache)` pairs, and gives the wrapper a home for
//! cross-cutting RPC concerns:
//!
//! * **Observability** — the inherent [`get_block_number`], [`get_logs`]
//!   and [`ensure_block_header`] methods carry their `rpc_span` and error
//!   mapping internally, so call sites don't repeat the ceremony. The
//!   inherent methods deliberately shadow the [`Provider`] trait methods of
//!   the same name.
//! * **Header singleflight** — [`ensure_block_header`] is cache-first and
//!   coalesces concurrent misses for the same block onto one
//!   `eth_getBlockByNumber` (per-block gate + double-checked cache read).
//!   Gap-fill fetchers of many filter stores race toward the same head
//!   block; without this each would pay its own RPC.
//!
//! The wrapper is itself a [`Provider`] — it delegates everything through
//! `root()` — so code generic over `Provider` accepts it unchanged.
//!
//! Cloning is cheap by contract: alloy providers are `Arc`-backed, and the
//! bundled resources are [`Shared`].
//!
//! [`get_block_number`]: ChainProvider::get_block_number
//! [`get_logs`]: ChainProvider::get_logs
//! [`ensure_block_header`]: ChainProvider::ensure_block_header

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use tracing::Instrument;

use crate::arch::HistoryError;
use crate::evm::cache::{BlockHeader, LogCache};
use crate::evm::observe::{CacheLayer, CacheOutcome, RpcMethod, cache_event, rpc_span};
use crate::evm::types::{BlockNumber, ChainId};
use crate::shared::Shared;

/// A provider paired with the chain id it serves and the chain's durable
/// cache.
#[derive(Clone)]
pub struct ChainProvider<P> {
    inner: P,
    chain_id: ChainId,
    headers: Shared<HeaderSingleflight>,
}

/// Per-chain header-ensure state: the cache handle plus one gate per
/// in-flight block fetch. Shared across every clone of the provider.
struct HeaderSingleflight {
    store: Shared<LogCache>,
    inflight: Mutex<HashMap<BlockNumber, Arc<AsyncMutex<()>>>>,
}

impl HeaderSingleflight {
    fn cached(&self, block: BlockNumber) -> Result<Option<BlockHeader>, HistoryError> {
        self.store
            .get_block_header(block.get())
            .map_err(|e| HistoryError::internal(e.to_string()))
    }

    fn gate(&self, block: BlockNumber) -> Arc<AsyncMutex<()>> {
        self.inflight
            .lock()
            .unwrap()
            .entry(block)
            .or_default()
            .clone()
    }

    fn ungate(&self, block: BlockNumber) {
        self.inflight.lock().unwrap().remove(&block);
    }
}

impl<P> ChainProvider<P> {
    /// Wrap a provider whose chain id is already known (e.g. resolved from
    /// CLI args or a config). No RPC.
    pub fn new(inner: P, chain_id: ChainId, cache: Shared<LogCache>) -> Self {
        Self {
            inner,
            chain_id,
            headers: Shared::new(HeaderSingleflight {
                store: cache,
                inflight: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The chain id this provider serves. Free — no `eth_chainId`.
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    /// The wrapped provider.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// The chain's durable cache.
    pub fn log_cache(&self) -> &Shared<LogCache> {
        &self.headers.store
    }
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> ChainProvider<P> {
    /// Discover the chain id from the provider (one `eth_chainId` call) and
    /// wrap it. Use when the caller has a bare provider and doesn't already
    /// know its chain.
    pub async fn discover(inner: P, cache: Shared<LogCache>) -> Result<Self, HistoryError> {
        let chain_id = inner
            .get_chain_id()
            .instrument(rpc_span(RpcMethod::ChainId))
            .await
            .map_err(|e| HistoryError::internal(format!("get_chain_id failed: {e}")))?;
        Ok(Self::new(inner, ChainId::new(chain_id), cache))
    }

    /// `eth_blockNumber`, instrumented. Shadows the [`Provider`] method.
    pub async fn get_block_number(&self) -> Result<BlockNumber, HistoryError> {
        Provider::get_block_number(&self.inner)
            .instrument(rpc_span(RpcMethod::BlockNumber))
            .await
            .map(BlockNumber::new)
            .map_err(|e| HistoryError::internal(format!("get_block_number failed: {e}")))
    }

    /// `eth_getLogs`, instrumented. Shadows the [`Provider`] method.
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, HistoryError> {
        Provider::get_logs(&self.inner, filter)
            .instrument(rpc_span(RpcMethod::GetLogs))
            .await
            .map_err(|e| HistoryError::internal(format!("eth_getLogs failed: {e}")))
    }

    /// Cache-first block-header lookup; falls back to one
    /// `eth_getBlockByNumber` and persists. Concurrent misses for the same
    /// block coalesce onto a single fetch (reported as a
    /// [`Singleflight`](CacheLayer::Singleflight) hit).
    pub async fn ensure_block_header(
        &self,
        block: BlockNumber,
    ) -> Result<BlockHeader, HistoryError> {
        if let Some(header) = self.headers.cached(block)? {
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
            return Ok(header);
        }
        cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);

        let gate = self.headers.gate(block);
        let _guard = gate.lock().await;
        // Double-check: a peer holding the gate may have fetched and
        // persisted while we waited.
        if let Some(header) = self.headers.cached(block)? {
            cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
            return Ok(header);
        }

        let result = self.fetch_and_persist_header(block).await;
        // Always drop the gate entry — on error the next caller retries
        // with a fresh gate; on success everyone hits the cache.
        self.headers.ungate(block);
        result
    }

    async fn fetch_and_persist_header(
        &self,
        block: BlockNumber,
    ) -> Result<BlockHeader, HistoryError> {
        let fetched = self
            .inner
            .get_block_by_number(BlockNumberOrTag::Number(block.get()))
            .into_future()
            .instrument(rpc_span(RpcMethod::GetBlockByNumber))
            .await
            .map_err(|e| HistoryError::internal(format!("get_block failed: {e}")))?
            .ok_or_else(|| HistoryError::internal(format!("block {block} not found")))?;

        let hash = fetched.header.hash;
        let parent_hash = fetched.header.parent_hash;
        let timestamp = fetched.header.timestamp;

        self.headers
            .store
            .insert_block_header(block.get(), &hash, &parent_hash, timestamp)
            .map_err(|e| HistoryError::internal(e.to_string()))?;

        Ok(BlockHeader {
            height: block.get(),
            hash,
            parent_hash,
            timestamp,
        })
    }
}

// `root()` is the Provider trait's only required method; every other method
// (get_logs, get_block_number, the real get_chain_id, …) has a default that
// routes through it. So delegating root() makes ChainProvider a fully
// functional Provider that simply also carries its chain-scoped resources.
impl<P: Provider<Ethereum>> Provider<Ethereum> for ChainProvider<P> {
    fn root(&self) -> &RootProvider<Ethereum> {
        self.inner.root()
    }
}
