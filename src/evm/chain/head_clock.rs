//! [`EvmHeadClock`] — the EVM [`HeadClock`] sessions, one per
//! [`EvmChain`](super::EvmChain), publishing head observations into the
//! chain's head channel. The demand-start and reconnect-with-backoff
//! machinery lives in the shared [`HeadSeeder`](crate::chain::HeadSeeder).
//!
//! Ws heads have their block header persisted *before* publication — the
//! headers-present invariant for push-delivered logs (the `newHeads`
//! payload carries the header for free). Poll heads publish the bare
//! number: in poll mode every log arrives through the fetcher, which
//! ensures its own headers.

use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::time::sleep;

use super::config::SubscribeTransport;
use crate::arch::{HistoryError, Timestamp};
use crate::chain::{BlockNumber, HeadCell, HeadClock};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::BlockHash;

/// The EVM head-tracking session: `newHeads` over ws, or an
/// `eth_blockNumber` poll.
pub(super) struct EvmHeadClock<P> {
    provider: ChainProvider<P>,
    transport: SubscribeTransport,
}

impl<P> EvmHeadClock<P> {
    pub(super) fn new(provider: ChainProvider<P>, transport: SubscribeTransport) -> Self {
        Self {
            provider,
            transport,
        }
    }
}

#[async_trait::async_trait]
impl<P> HeadClock for EvmHeadClock<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    async fn session(&self, head: &HeadCell) -> HistoryError {
        match &self.transport {
            SubscribeTransport::Ws => ws_heads(&self.provider, head).await,
            SubscribeTransport::Poll { interval } => {
                poll_heads(&self.provider, *interval, head).await
            }
        }
    }
}

/// Poll `eth_blockNumber` and publish (the publish is monotonic, so
/// non-advances are no-ops). Polls immediately on entry — the clock is
/// demand-started, so a consumer is already waiting on the first
/// observation. Only returns on error (the shared runner reconnects with
/// backoff).
async fn poll_heads<P>(
    provider: &ChainProvider<P>,
    interval: Duration,
    head: &HeadCell,
) -> HistoryError
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    loop {
        match provider.get_block_number().await {
            Ok(n) => head.publish(n),
            Err(e) => return e,
        }
        sleep(interval).await;
    }
}

/// `eth_subscribe(newHeads)`: persist each header (free — the payload
/// carries it) and publish. Only returns on error.
async fn ws_heads<P>(provider: &ChainProvider<P>, head: &HeadCell) -> HistoryError
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    use futures_util::StreamExt;

    let sub = match provider.subscribe_blocks().await {
        Ok(s) => s,
        Err(e) => return HistoryError::internal(format!("subscribe_blocks: {e}")),
    };
    let mut heads = sub.into_stream();
    loop {
        let Some(header) = heads.next().await else {
            return HistoryError::internal("heads subscription closed");
        };
        let number = BlockNumber::new(header.number);
        if let Err(e) = provider.log_cache().insert_block_header(
            number,
            &BlockHash::new(header.hash),
            &BlockHash::new(header.parent_hash),
            Timestamp::new(header.timestamp),
        ) {
            return HistoryError::internal(format!("insert_block_header: {e}"));
        }
        head.publish(number);
    }
}
