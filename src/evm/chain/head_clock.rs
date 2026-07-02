//! The chain-wide head clock: one task per [`EvmChain`](super::EvmChain)
//! in follow mode, publishing [`HeadEvent`]s on the chain's watch channel.
//!
//! Every published head has its block header persisted first (the
//! headers-present invariant). Errors reconnect with exponential backoff;
//! the task is aborted by the chain's `AbortOnDropHandle`.

use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use tokio::sync::watch;
use tokio::time::sleep;

use super::config::{HeadEvent, SubscribeTransport};
use super::{INITIAL_BACKOFF, MAX_BACKOFF};
use crate::arch::HistoryError;
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::BlockNumber;

pub(super) async fn run<P>(
    provider: ChainProvider<P>,
    transport: SubscribeTransport,
    head: watch::Sender<HeadEvent>,
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

/// Poll `eth_blockNumber`; on advance, persist the header and publish.
/// Only returns on error (the caller reconnects with backoff).
async fn poll_heads<P>(
    provider: &ChainProvider<P>,
    interval: Duration,
    head: &watch::Sender<HeadEvent>,
) -> HistoryError
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    loop {
        sleep(interval).await;
        let n = match provider.get_block_number().await {
            Ok(n) => n,
            Err(e) => return e,
        };
        if n <= head.borrow().number {
            continue;
        }
        let header = match provider.ensure_block_header(n).await {
            Ok(h) => h,
            Err(e) => return e,
        };
        head.send_replace(HeadEvent {
            number: n,
            hash: header.hash,
        });
    }
}

/// `eth_subscribe(newHeads)`: persist each header (free — the payload
/// carries it) and publish. Only returns on error.
async fn ws_heads<P>(provider: &ChainProvider<P>, head: &watch::Sender<HeadEvent>) -> HistoryError
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
            number.get(),
            &header.hash,
            &header.parent_hash,
            header.timestamp,
        ) {
            return HistoryError::internal(format!("insert_block_header: {e}"));
        }
        if number > head.borrow().number {
            head.send_replace(HeadEvent {
                number,
                hash: header.hash,
            });
        }
    }
}
