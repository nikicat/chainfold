//! Filter plumbing and the gap-fill fetchers (logs + blocks). Everything
//! RPC-shaped (instrumentation, error mapping, header singleflight) lives
//! on [`ChainProvider`] itself.

use std::sync::Arc;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::{Block, Filter};
use tracing::Instrument;

use crate::arch::{HistoryError, Timestamp};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::observe::{RpcMethod, rpc_span};
use crate::evm::range_index::{BlockPage, Fetcher, LogPage};
use crate::evm::types::{
    BlockCount, BlockHash, BlockNumber, EvmBlock, Gas, GasPrice, Topic, TxCount,
};

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

/// JSON-RPC batch size for the block fetcher: one `eth_getBlockByNumber`
/// per height, packed [`BLOCKS_PER_BATCH`] to an HTTP request (the same
/// batching the ERC20 metadata path uses). Small enough for batch-capped
/// public endpoints.
const BLOCKS_PER_BATCH: BlockCount = BlockCount::new(100);

/// Hard cap on one gap-fill's span. Logs pay one `eth_getLogs` regardless
/// of span; blocks pay one call *per height*, so an unbounded default-start
/// walk (`(Bottom, tip]` on mainnet ≈ 23M calls) must fail loudly instead
/// of grinding. CLI paths always bound the walk via `split()`.
const MAX_BLOCK_FETCH: BlockCount = BlockCount::new(10_000);

/// The gap-fill fetcher for the blocks history: batched
/// `eth_getBlockByNumber(n, false)` over the inclusive range. `hydrate =
/// false` returns transaction hashes only — enough for the count without
/// pulling bodies.
pub(super) fn build_block_fetcher<P>(provider: ChainProvider<P>) -> Fetcher<BlockPage>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    Arc::new(move |from: BlockNumber, to: BlockNumber| {
        let provider = provider.clone();
        Box::pin(async move {
            let span = to.get().saturating_sub(from.get()) + 1;
            if span > MAX_BLOCK_FETCH.get() {
                return Err(HistoryError::internal(format!(
                    "blocks gap-fill span {span} exceeds {MAX_BLOCK_FETCH} \
                     ({from}..={to}) — bound the walk with split() before iterating"
                )));
            }
            tracing::debug!(from = %from, to = %to, blocks = span, "eth_getBlockByNumber batch");

            let mut items: Vec<EvmBlock> = Vec::with_capacity(span as usize);
            let mut n = from;
            while n <= to {
                let batch_to = to.min(n + (BLOCKS_PER_BATCH.get() - 1));
                items.extend(fetch_block_batch(&provider, n, batch_to).await?);
                n = batch_to + 1;
            }

            let end_block_hash = items.last().expect("span >= 1").hash;
            Ok(BlockPage {
                from_block: from,
                to_block: to,
                items,
                end_block_hash,
            })
        })
    })
}

/// One JSON-RPC batch of `eth_getBlockByNumber(n, false)` for every height
/// in `[from, to]`, decoded to dense ascending [`EvmBlock`]s.
async fn fetch_block_batch<P>(
    provider: &ChainProvider<P>,
    from: BlockNumber,
    to: BlockNumber,
) -> Result<Vec<EvmBlock>, HistoryError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut batch = alloy::rpc::client::BatchRequest::new(provider.client());
    let mut waiters = Vec::with_capacity((to.get() - from.get()) as usize + 1);
    let mut n = from;
    while n <= to {
        let waiter = batch
            .add_call::<_, Option<Block>>(
                "eth_getBlockByNumber",
                &(BlockNumberOrTag::Number(n.get()), false),
            )
            .map_err(|e| HistoryError::internal(format!("batch build: {e}")))?;
        waiters.push((n, waiter));
        n = n + 1;
    }

    batch
        .send()
        .instrument(rpc_span(RpcMethod::GetBlockByNumber))
        .await
        .map_err(|e| HistoryError::internal(format!("batch send: {e}")))?;

    let mut blocks = Vec::with_capacity(waiters.len());
    for (expected, waiter) in waiters {
        let block = waiter
            .await
            .map_err(|e| HistoryError::internal(format!("get_block {expected}: {e}")))?
            .ok_or_else(|| {
                HistoryError::internal(format!("block {expected} not found (beyond head?)"))
            })?;
        let header = &block.header;
        if header.number != expected.get() {
            return Err(HistoryError::internal(format!(
                "block number mismatch: asked {expected}, got {}",
                header.number
            )));
        }
        blocks.push(EvmBlock {
            number: expected,
            hash: BlockHash::new(header.hash),
            parent_hash: BlockHash::new(header.parent_hash),
            timestamp: Timestamp::new(header.timestamp),
            tx_count: TxCount::from_len(block.transactions.len()),
            gas_used: Gas::new(header.gas_used),
            gas_limit: Gas::new(header.gas_limit),
            base_fee: header.base_fee_per_gas.map(GasPrice::new),
            miner: header.beneficiary,
        });
    }
    Ok(blocks)
}
