//! [`ChainProvider`] — an alloy [`Provider`] bundled with the chain id it
//! serves.
//!
//! A chain id is an attribute of the endpoint, not a value to rediscover at
//! every history construction. Wrapping the provider lets `EvmLogHistory` and
//! `OwnedErc20` read [`ChainProvider::chain_id`] for free (no `eth_chainId`
//! round-trip) while still using the inner provider for real RPC. The wrapper
//! is itself a [`Provider`] — it delegates everything through `root()` — so
//! any code generic over `Provider` accepts it unchanged.

use alloy::network::Ethereum;
use alloy::providers::{Provider, RootProvider};
use tracing::Instrument;

use crate::arch::HistoryError;
use crate::evm::observe::{RpcMethod, rpc_span};

/// A provider paired with the chain id it serves.
#[derive(Clone)]
pub struct ChainProvider<P> {
    inner: P,
    chain_id: u64,
}

impl<P> ChainProvider<P> {
    /// Wrap a provider whose chain id is already known (e.g. resolved from CLI
    /// args or a config). No RPC.
    pub fn new(inner: P, chain_id: u64) -> Self {
        Self { inner, chain_id }
    }

    /// The chain id this provider serves. Free — no `eth_chainId`.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// The wrapped provider.
    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> ChainProvider<P> {
    /// Discover the chain id from the provider (one `eth_chainId` call) and
    /// wrap it. Use when the caller has a bare provider and doesn't already
    /// know its chain.
    pub async fn discover(inner: P) -> Result<Self, HistoryError> {
        let chain_id = inner
            .get_chain_id()
            .instrument(rpc_span(RpcMethod::ChainId))
            .await
            .map_err(|e| HistoryError::internal(format!("get_chain_id failed: {e}")))?;
        Ok(Self { inner, chain_id })
    }
}

// `root()` is the Provider trait's only required method; every other method
// (get_logs, get_block_number, the real get_chain_id, …) has a default that
// routes through it. So delegating root() makes ChainProvider a fully
// functional Provider that simply also carries its chain id.
impl<P: Provider<Ethereum>> Provider<Ethereum> for ChainProvider<P> {
    fn root(&self) -> &RootProvider<Ethereum> {
        self.inner.root()
    }
}
