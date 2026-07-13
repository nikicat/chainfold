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
//! * **Error mapping** — the inherent [`get_block_number`], [`get_logs`]
//!   and [`ensure_block_header`] methods map transport errors into
//!   [`HistoryError`] internally, so call sites don't repeat the ceremony.
//!   (RPC *timing* needs no ceremony anywhere: the transport stack's
//!   [`RpcTimingLayer`](crate::evm::transport::RpcTimingLayer) times every
//!   request.) The inherent methods deliberately shadow the [`Provider`]
//!   trait methods of the same name. The adaptive executor sits *below*
//!   that funnel: [`get_logs_raw`] and [`ensure_block_header_raw`] keep
//!   the structured [`TransportError`] intact, because its classifier
//!   ([`ProviderAdaptor::classify`]) needs the payload, not a string.
//! * **Header singleflight** — [`ensure_block_header`] is cache-first and
//!   coalesces concurrent misses for the same block onto one
//!   `eth_getBlockByNumber` (per-block gate + double-checked cache read).
//!   Gap-fill fetchers of many filter stores race toward the same head
//!   block; without this each would pay its own RPC.
//! * **Adaptive fetch state** — the per-endpoint [`EndpointBudget`] and
//!   the [`ProviderAdaptor`] handle live here for the same reason the
//!   header singleflight does: every filter's gap-fill hits the same
//!   endpoint, and the AIMD state must be shared or N stores
//!   thundering-herd it (`docs/FETCH.md` §3).
//!
//! The wrapper is itself a [`Provider`] — it delegates everything through
//! `root()` — so code generic over `Provider` accepts it unchanged.
//!
//! Cloning is cheap by contract: alloy providers are `Arc`-backed, and the
//! bundled resources are `Arc`s.
//!
//! [`get_block_number`]: ChainProvider::get_block_number
//! [`get_logs`]: ChainProvider::get_logs
//! [`get_logs_raw`]: ChainProvider::get_logs_raw
//! [`ensure_block_header`]: ChainProvider::ensure_block_header
//! [`ensure_block_header_raw`]: ChainProvider::ensure_block_header_raw

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportError;

use crate::arch::{HistoryError, Timestamp, error_chain};
use crate::chain::BlockNumber;
use crate::evm::budget::EndpointBudget;
use crate::evm::cache::{BlockHeader, LogCache};
use crate::evm::provider::{ProviderAdaptor, generic_adaptor};
use crate::evm::types::{BlockHash, ChainId};
use crate::observe::{CacheLayer, CacheOutcome, cache_event};
use crate::share::Share;

/// A provider paired with the chain id it serves and the chain's durable
/// cache.
#[derive(Clone)]
pub struct ChainProvider<P> {
    inner: P,
    chain_id: ChainId,
    headers: Arc<HeaderSingleflight>,
    /// Per-endpoint adaptive fetch state, shared by every clone (every
    /// filter store on the chain drives the same endpoint).
    budget: Arc<EndpointBudget>,
    /// The endpoint's provider intelligence for the executor's error
    /// classification. Stateless unit structs behind `&'static` — no
    /// `Arc` ceremony.
    adaptor: &'static dyn ProviderAdaptor,
}

/// A fetch failure below the [`HistoryError`] funnel, split by what the
/// adaptive executor can do with it: an [`Rpc`](Self::Rpc) error goes to
/// the adaptor's classifier (resize / back off / retry / fail); a
/// [`Store`](Self::Store) error is local and terminal for the slice.
#[derive(Debug)]
pub(crate) enum RawFetchError {
    Rpc(TransportError),
    Store(HistoryError),
}

/// Per-chain header-ensure state: the cache handle plus one gate per
/// in-flight block fetch. Shared across every clone of the provider.
struct HeaderSingleflight {
    store: Arc<LogCache>,
    inflight: Mutex<HashMap<BlockNumber, Arc<AsyncMutex<()>>>>,
}

impl HeaderSingleflight {
    fn cached(&self, block: BlockNumber) -> Result<Option<BlockHeader>, HistoryError> {
        self.store
            .get_block_header(block)
            .map_err(|e| HistoryError::internal(e.to_string()))
    }

    fn gate(&self, block: BlockNumber) -> Arc<AsyncMutex<()>> {
        self.inflight
            .lock()
            .unwrap()
            .entry(block)
            .or_default()
            .share()
    }

    fn ungate(&self, block: BlockNumber) {
        self.inflight.lock().unwrap().remove(&block);
    }
}

impl<P> ChainProvider<P> {
    /// Wrap a provider whose chain id is already known (e.g. resolved from
    /// CLI args or a config). No RPC. Starts on the generic adaptor —
    /// [`with_adaptor`](Self::with_adaptor) installs provider intelligence.
    pub fn new(inner: P, chain_id: ChainId, cache: Arc<LogCache>) -> Self {
        let adaptor = generic_adaptor();
        Self {
            inner,
            chain_id,
            headers: Arc::new(HeaderSingleflight {
                store: cache,
                inflight: Mutex::new(HashMap::new()),
            }),
            budget: Arc::new(EndpointBudget::new(adaptor.seeds(chain_id))),
            adaptor,
        }
    }

    /// Install a provider adaptor (the endpoint resolver's hand-off) and
    /// re-seed the endpoint budget from its known limits, so the AIMD
    /// starts converged instead of probing. Call before cloning — the
    /// budget is shared via `Arc`, and re-seeding after clones exist would
    /// split them onto separate budgets.
    pub fn with_adaptor(mut self, adaptor: &'static dyn ProviderAdaptor) -> Self {
        self.adaptor = adaptor;
        self.budget = Arc::new(EndpointBudget::new(adaptor.seeds(self.chain_id)));
        self
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
    pub fn log_cache(&self) -> &Arc<LogCache> {
        &self.headers.store
    }

    /// The endpoint's shared adaptive fetch state.
    pub(crate) fn budget(&self) -> &Arc<EndpointBudget> {
        &self.budget
    }

    /// The endpoint's provider adaptor (classification + seeds).
    pub(crate) fn adaptor(&self) -> &'static dyn ProviderAdaptor {
        self.adaptor
    }
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> ChainProvider<P> {
    /// Discover the chain id from the provider (one `eth_chainId` call) and
    /// wrap it. Use when the caller has a bare provider and doesn't already
    /// know its chain.
    pub async fn discover(inner: P, cache: Arc<LogCache>) -> Result<Self, HistoryError> {
        let chain_id = inner.get_chain_id().await.map_err(|e| {
            HistoryError::internal(format!("get_chain_id failed: {}", error_chain(&e)))
        })?;
        Ok(Self::new(inner, ChainId::new(chain_id), cache))
    }

    /// `eth_blockNumber`, error-mapped. Shadows the [`Provider`] method.
    pub async fn get_block_number(&self) -> Result<BlockNumber, HistoryError> {
        Provider::get_block_number(&self.inner)
            .await
            .map(BlockNumber::new)
            .map_err(|e| {
                HistoryError::internal(format!("get_block_number failed: {}", error_chain(&e)))
            })
    }

    /// `eth_getLogs` with the structured [`TransportError`] intact — the
    /// adaptive executor classifies it ([`ProviderAdaptor::classify`]).
    /// Everything above the executor uses the [`get_logs`](Self::get_logs)
    /// funnel instead.
    pub async fn get_logs_raw(&self, filter: &Filter) -> Result<Vec<Log>, TransportError> {
        Provider::get_logs(&self.inner, filter).await
    }

    /// `eth_getLogs`, error-mapped. Shadows the [`Provider`] method.
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, HistoryError> {
        self.get_logs_raw(filter)
            .await
            .map_err(|e| HistoryError::internal(format!("eth_getLogs failed: {}", error_chain(&e))))
    }

    /// Cache-first block-header lookup; falls back to one
    /// `eth_getBlockByNumber` and persists. Concurrent misses for the same
    /// block coalesce onto a single fetch (reported as a
    /// [`Singleflight`](CacheLayer::Singleflight) hit).
    pub async fn ensure_block_header(
        &self,
        block: BlockNumber,
    ) -> Result<BlockHeader, HistoryError> {
        self.ensure_block_header_raw(block)
            .await
            .map_err(|e| match e {
                RawFetchError::Rpc(e) => {
                    HistoryError::internal(format!("get_block failed: {}", error_chain(&e)))
                }
                RawFetchError::Store(e) => e,
            })
    }

    /// [`ensure_block_header`](Self::ensure_block_header) below the
    /// [`HistoryError`] funnel: RPC failures stay structured for the
    /// executor's classifier (a 429 on a header burst must shrink
    /// concurrency like any other), store failures stay [`HistoryError`].
    /// Same cache-first singleflight either way.
    pub(crate) async fn ensure_block_header_raw(
        &self,
        block: BlockNumber,
    ) -> Result<BlockHeader, RawFetchError> {
        if let Some(header) = self.headers.cached(block).map_err(RawFetchError::Store)? {
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
            return Ok(header);
        }
        cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);

        let gate = self.headers.gate(block);
        let _guard = gate.lock().await;
        // Double-check: a peer holding the gate may have fetched and
        // persisted while we waited.
        if let Some(header) = self.headers.cached(block).map_err(RawFetchError::Store)? {
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
    ) -> Result<BlockHeader, RawFetchError> {
        let fetched = self
            .inner
            .get_block_by_number(BlockNumberOrTag::Number(block.get()))
            .await
            .map_err(RawFetchError::Rpc)?
            .ok_or_else(|| {
                RawFetchError::Store(HistoryError::internal(format!("block {block} not found")))
            })?;

        let hash = BlockHash::new(fetched.header.hash);
        let parent_hash = BlockHash::new(fetched.header.parent_hash);
        let timestamp = Timestamp::new(fetched.header.timestamp);

        self.headers
            .store
            .insert_block_header(block, &hash, &parent_hash, timestamp)
            .map_err(|e| RawFetchError::Store(HistoryError::internal(e.to_string())))?;

        Ok(BlockHeader {
            height: block,
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

#[cfg(test)]
mod tests {
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::client::ClientBuilder;
    use alloy::transports::mock::{Asserter, MockTransport};
    use serde_json::Value;

    use super::*;

    /// The public `ensure_block_header` funnels raw fetch errors into
    /// [`HistoryError`] with the pre-executor message texts: RPC failures
    /// gain the `get_block failed` prefix, store-side errors (here: a
    /// block the node claims not to have) pass through untouched.
    #[tokio::test]
    async fn ensure_block_header_funnels_raw_errors() {
        let asserter = Asserter::new();
        let client = ClientBuilder::default().transport(MockTransport::new(asserter.clone()), true);
        let provider = ChainProvider::new(
            ProviderBuilder::default().connect_client(client),
            ChainId::new(1),
            Arc::new(LogCache::open_in_memory().unwrap()),
        );

        // Rpc arm: nothing queued — the mock transport errors on the wire.
        let error = provider
            .ensure_block_header(BlockNumber::new(7))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("get_block failed"), "{error}");

        // Store arm: a null answer (block not found) — no transport
        // prefix, the store text verbatim.
        asserter.push_success(&Value::Null);
        let error = provider
            .ensure_block_header(BlockNumber::new(7))
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(text.contains("block 7 not found"), "{text}");
        assert!(!text.contains("get_block failed"), "{text}");
    }
}
