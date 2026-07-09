//! The blocks source: the chain's dense block history over
//! [`RangeIndexStore<BlockSpec>`] — the batched `eth_getBlockByNumber`
//! gap-fill fetcher, the reactive store ([`EvmChain::blocks`]), and the
//! bounded hist ([`EvmChain::blocks_hist`]).
//!
//! Blocks have no push delivery path (`newHeads` payloads lack the
//! transaction count), so the reactive store always rides the shared
//! head-follow wiring — eager gap-fill on each head advance — and ws and
//! poll transports behave identically here.

use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::Provider;
use alloy::rpc::types::Block;
use tokio_util::task::AbortOnDropHandle;

use super::{EvmChain, Finality, LazyAnchor, estimate_block_from_time, wiring};
use crate::arch::{Hist, HistStore, HistoryError, Timestamp};
use crate::chain::{BlockCount, BlockNumber, BlockRange, TxCount};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::types::{BlockHash, EvmBlock, Gas, GasPrice};
use crate::evm::{BlockPage, BlockSpec};
use crate::range_index::{Fetcher, RangeIndex, RangeIndexStore, TimePos, TipSeed};

// ============================================================
// EvmChain — blocks source
// ============================================================

impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> EvmChain<P> {
    /// Open the chain's block history at `tier`. Cheap, synchronous and
    /// side-effect-free — no RPC; cached blocks materialize lazily, gaps
    /// fill on demand (batched `eth_getBlockByNumber`). The store's tip is
    /// driven by this chain's head clock; the first consumer awaiting a
    /// snapshot demand-starts it. All background wiring dies with the
    /// returned store.
    pub fn blocks(&self, tier: Finality) -> HistStore<EvmBlock, BlockNumber> {
        let _ = tier; // Latest-only until Phase 7.

        let store = self.block_store(Some(self.seeder.clone() as Arc<dyn TipSeed>));

        let wiring = tokio::spawn(wiring::follow_head::<BlockSpec>(
            self.seeder.head.subscribe(),
            Arc::downgrade(&store),
        ));
        store.attach_wiring(AbortOnDropHandle::new(wiring));

        tracing::debug!(chain_id = %self.chain_id(), "block store opened");

        store.hist_store()
    }

    /// Open a finite block history over the explicit inclusive `range`.
    /// Head-free: nothing is fetched at construction, the clock is never
    /// demanded, and a fully-cached walk performs no RPC at all; gaps fill
    /// on demand through the batched block fetcher. A range extending
    /// beyond the real chain head fails loudly at iteration (`block …
    /// not found`) rather than clamping.
    pub fn blocks_hist(&self, range: BlockRange) -> Hist<EvmBlock, BlockNumber> {
        let store = self.block_store(None);

        tracing::debug!(
            chain_id = %self.chain_id(),
            from = %range.from,
            to = %range.to,
            "bounded block hist opened"
        );

        Hist::new(store, block_cut_before(range.from), range.to)
    }

    /// The store both entry points share: spec + fetcher + reactive layer.
    fn block_store(&self, tip_seed: Option<Arc<dyn TipSeed>>) -> Arc<RangeIndexStore<BlockSpec>> {
        let spec = BlockSpec::new(self.cache().share());
        let fetcher = build_block_fetcher(self.provider().clone());
        let index = Arc::new(RangeIndex::with_chunk_size(spec, fetcher, self.chunk_size));
        RangeIndexStore::new(
            index,
            Arc::new(BlockTimePos {
                anchor: self.lazy_anchor(),
                block_time: self.block_time,
            }),
            tip_seed,
        )
    }
}

// ============================================================
// Positions
// ============================================================

/// The block-space cut "just before `block`": iteration after this cut
/// starts at `block` itself. ZERO doubles as the lower-bound sentinel,
/// keeping genesis included (the documented position-0 wart).
fn block_cut_before(block: BlockNumber) -> BlockNumber {
    block.checked_sub(1).unwrap_or(BlockNumber::ZERO)
}

/// [`TimePos`] for the dense blocks space: the cut "just before" the
/// estimated block.
struct BlockTimePos<P> {
    anchor: LazyAnchor<P>,
    block_time: Duration,
}

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> TimePos<BlockNumber>
    for BlockTimePos<P>
{
    async fn time_pos(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        let anchor = self.anchor.get().await?;
        let block = estimate_block_from_time(anchor.block, anchor.time, self.block_time, time);
        Ok(block_cut_before(block))
    }
}

// ============================================================
// Fetcher
// ============================================================

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
fn build_block_fetcher<P>(provider: ChainProvider<P>) -> Fetcher<BlockPage>
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

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use tracing::subscriber::set_default;
    use tracing_subscriber::prelude::*;

    use super::super::testkit::{
        GENESIS, HEAD, RpcCounter, make_chain, offline_chain, test_block, ts, warm_blocks_cache,
    };
    use super::*;
    use crate::arch::Positioned;

    #[tokio::test]
    async fn blocks_tip_seeded_from_demanded_head() {
        let chain = make_chain(warm_blocks_cache());
        // The first consumer await demand-starts the clock, whose first
        // poll observes the head and seeds the store's tip.
        let hist = chain.blocks(Finality::Latest).next().await;
        assert_eq!(*hist.end(), HEAD);
    }

    #[tokio::test]
    async fn blocks_iter_walks_dense_window_including_genesis() {
        let chain = make_chain(warm_blocks_cache());
        let hist = chain.blocks(Finality::Latest).next().await;

        let items: Vec<Positioned<EvmBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        assert_eq!(items.len(), HEAD.get() as usize + 1, "dense incl. genesis");
        assert_eq!(items[0].pos, BlockNumber::ZERO);
        assert_eq!(items.last().unwrap().pos, HEAD);
        // Items carry the rich fields, positioned at their own number.
        let b100 = &items[100];
        assert_eq!(b100.item.number, BlockNumber::new(100));
        assert_eq!(b100.item.tx_count, TxCount::new(100));
        assert_eq!(b100.item.timestamp, ts(1200));
    }

    #[tokio::test]
    async fn blocks_time_pos_cuts_just_before_estimated_block() {
        let chain = make_chain(warm_blocks_cache());
        let store = chain.blocks(Finality::Latest);
        // 12 s cadence anchored at (HEAD, ts(2400)): ts(1200) → block 100,
        // cut just before it = 99 (iteration after the anchor starts at 100).
        // Resolving the anchor demands the head (the queued response).
        assert_eq!(
            store.time_pos(ts(1200)).await.unwrap(),
            BlockNumber::new(99)
        );
        // Before-anchor underflow clamps to the ZERO sentinel.
        assert_eq!(
            store.time_pos(GENESIS).await.unwrap(),
            BlockNumber::ZERO,
            "estimate at/below genesis clamps to the lower-bound sentinel"
        );
    }

    /// A fully-cached bounded walk is completely offline: the mock
    /// provider has NO responses queued, so any RPC — head demand
    /// included — fails the test loudly.
    #[tokio::test]
    async fn blocks_hist_fully_cached_walk_is_offline() {
        let chain = offline_chain(warm_blocks_cache());
        let hist = chain.blocks_hist(BlockRange::from_u64(100, 110));

        let items: Vec<Positioned<EvmBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        assert_eq!(items.len(), 11);
        assert_eq!(items[0].pos, BlockNumber::new(100));
        assert_eq!(items[0].item.hash, test_block(100).hash);
        assert_eq!(items.last().unwrap().pos, BlockNumber::new(110));
    }

    /// Same guard as `warm_walk_issues_zero_get_logs`, for the blocks
    /// source: a fully-cached walk must not issue `eth_getBlockByNumber`.
    #[tokio::test]
    async fn blocks_warm_walk_issues_zero_get_block_by_number() {
        let chain = make_chain(warm_blocks_cache());
        let hist = chain.blocks(Finality::Latest).next().await;

        let counter = RpcCounter::new("eth_getBlockByNumber");
        let guard = set_default(tracing_subscriber::registry().with(counter.clone()));
        let items: Vec<Positioned<EvmBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        drop(guard);

        assert_eq!(items.len(), HEAD.get() as usize + 1);
        assert_eq!(
            counter.count(),
            0,
            "a fully-cached walk must not issue eth_getBlockByNumber"
        );
    }
}
