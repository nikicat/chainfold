//! Filter plumbing and the gap-fill fetcher. Everything RPC-shaped
//! (instrumentation, error mapping, header singleflight) lives on
//! [`ChainProvider`] itself.

use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;

use crate::evm::chain_provider::ChainProvider;
use crate::evm::range_index::{Fetcher, LogPage};
use crate::evm::types::{BlockNumber, Topic};

/// Apply `topics[0..3]` to an alloy [`Filter`] in the conventional slots
/// (`event_signature` / `topic1` / `topic2` / `topic3`). Skips slots that
/// are `None` (matches anything in that position).
pub(super) fn apply_topics(mut filter: Filter, topics: &[Option<Topic>; 4]) -> Filter {
    if let Some(t) = topics[0] {
        filter = filter.event_signature(t.b256());
    }
    if let Some(t) = topics[1] {
        filter = filter.topic1(t.b256());
    }
    if let Some(t) = topics[2] {
        filter = filter.topic2(t.b256());
    }
    if let Some(t) = topics[3] {
        filter = filter.topic3(t.b256());
    }
    filter
}

/// The gap-fill fetcher for one log filter: chunked `eth_getLogs` +
/// header pre-caching so `LogSpec::resolve`'s synchronous timestamp fill
/// always hits the cache.
pub(super) fn build_fetcher<P>(
    provider: ChainProvider<P>,
    address: Address,
    topics: [Option<Topic>; 4],
) -> Fetcher<LogPage>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    Arc::new(move |from: BlockNumber, to: BlockNumber| {
        let provider = provider.clone();
        Box::pin(async move {
            let filter = Filter::new()
                .address(address)
                .from_block(from.get())
                .to_block(to.get());
            let filter = apply_topics(filter, &topics);
            tracing::debug!(
                from = %from,
                to = %to,
                blocks = (to - from.get()).get() + 1,
                "eth_getLogs"
            );
            let logs = provider.get_logs(&filter).await?;
            tracing::debug!(from = %from, to = %to, num_logs = logs.len(), "eth_getLogs returned");

            let end_header = provider.ensure_block_header(to).await?;
            for log in &logs {
                // Logs that carry their own timestamp need no header — the
                // query path reads it straight off the persisted log.
                if log.block_timestamp.is_some() {
                    continue;
                }
                if let Some(bn) = log.block_number {
                    let _ = provider.ensure_block_header(BlockNumber::new(bn)).await;
                }
            }

            Ok(LogPage {
                from_block: from,
                to_block: to,
                items: logs,
                end_block_hash: end_header.hash,
            })
        })
    })
}
