//! The chain-wide head clock: one demand-started task per
//! [`EvmChain`](super::EvmChain), publishing head observations into the
//! chain's head channel (see [`HeadCell`]).
//!
//! Ws heads have their block header persisted *before* publication — the
//! headers-present invariant for push-delivered logs (the `newHeads`
//! payload carries the header for free). Poll heads publish the bare
//! number: in poll mode every log arrives through the fetcher, which
//! ensures its own headers. Errors reconnect with exponential backoff; the
//! task is aborted when the chain and all its stores drop.

use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::time::sleep;

use super::config::SubscribeTransport;
use super::{HeadCell, INITIAL_BACKOFF, MAX_BACKOFF};
use crate::arch::{HistoryError, Timestamp};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::{BlockHash, BlockNumber};

pub(super) async fn run<P>(
    provider: ChainProvider<P>,
    transport: SubscribeTransport,
    head: HeadCell,
) where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let err = match &transport {
            SubscribeTransport::Ws => ws_heads(&provider, &head).await,
            SubscribeTransport::Poll { interval } => poll_heads(&provider, *interval, &head).await,
        };
        tracing::warn!(
            error = %err,
            backoff_ms = backoff.as_millis(),
            "head clock error, will reconnect"
        );
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Poll `eth_blockNumber` and publish (the publish is monotonic, so
/// non-advances are no-ops). Polls immediately on entry — the clock is
/// demand-started, so a consumer is already waiting on the first
/// observation. Only returns on error (the caller reconnects with
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
