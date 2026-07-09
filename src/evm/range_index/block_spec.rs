//! [`BlockSpec`] — the [`RangeSpec`] for EVM blocks: the second instance
//! after [`LogSpec`](super::LogSpec), backing
//! [`EvmChain::blocks`](crate::evm::EvmChain::blocks).
//!
//! Unlike logs, blocks are **dense**: every height in a scanned range is an
//! item, pinned at `end_of_block(n)`, so a page produces one tree entry per
//! block and no empty tails. The tree stores the whole [`EvmBlock`]
//! (all-`Copy`) — [`resolve`](RangeSpec::resolve) is a pure copy, with no
//! per-row `SQLite` read (no `LogRef`-style indirection).
//!
//! Coverage lives in `scanned_ranges` under [`ScanPartition::Blocks`];
//! rows live in `cached_blocks`, written only by this spec's
//! [`persist`](RangeSpec::persist).

use super::block_pos::BlockPos;
use super::spec::{IndexPos, LoadedSpan, RangeSpec};
use crate::arch::{HistoryError, Positioned};
use crate::chain::{BlockNumber, BlockRange};
use crate::evm::cache::{LogCache, ScanPartition};
use crate::evm::types::{BlockHash, EvmBlock};
use crate::shared::Shared;

/// [`RangeSpec`] for the chain's block history over a [`LogCache`].
#[derive(Clone)]
pub struct BlockSpec {
    store: Shared<LogCache>,
}

impl BlockSpec {
    pub fn new(store: Shared<LogCache>) -> Self {
        Self { store }
    }
}

/// A page of consecutive blocks covering `[from_block, to_block]` from one
/// gap fill: `items` is dense ascending — exactly one [`EvmBlock`] per
/// height in the range.
#[derive(Clone)]
pub struct BlockPage {
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
    pub items: Vec<EvmBlock>,
    /// The last item's hash — recorded as the coverage range's end hash so
    /// a reorg at the range end invalidates the coverage claim.
    pub end_block_hash: BlockHash,
}

impl RangeSpec for BlockSpec {
    type Pos = BlockPos;
    type Item = EvmBlock;
    type Event = EvmBlock;
    type PubPos = BlockNumber;
    type Page = BlockPage;

    fn item_pos(item: &EvmBlock) -> BlockPos {
        BlockPos::end_of_block(item.number)
    }

    fn page_items(page: &BlockPage) -> Vec<EvmBlock> {
        page.items.clone()
    }

    fn page_bounds(page: &BlockPage) -> BlockRange {
        BlockRange::new(page.from_block, page.to_block)
    }

    fn page_tip(page: &BlockPage) -> BlockNumber {
        page.to_block
    }

    fn resolve(&self, item: &EvmBlock) -> Result<Positioned<EvmBlock, BlockNumber>, HistoryError> {
        // The tree stores the whole block — a pure copy, never a store read.
        Ok(Positioned::new(*item, item.number))
    }

    fn persist(&self, page: &BlockPage) -> Result<(), HistoryError> {
        if !page.items.is_empty() {
            self.store
                .insert_blocks(&page.items)
                .map_err(|e| HistoryError::internal(format!("insert_blocks: {e}")))?;
        }
        self.store
            .record_scanned_range(
                &ScanPartition::Blocks,
                BlockRange::new(page.from_block, page.to_block),
                &page.end_block_hash,
            )
            .map_err(|e| HistoryError::internal(format!("record_scanned_range: {e}")))
    }

    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<EvmBlock>>, HistoryError> {
        let scanned = self
            .store
            .get_scanned_ranges(&ScanPartition::Blocks)
            .map_err(|e| HistoryError::internal(format!("get_scanned_ranges: {e}")))?;

        let mut spans = Vec::new();
        for scanned_range in scanned {
            if scanned_range.to < window.from || scanned_range.from > window.to {
                continue;
            }
            let items = self
                .store
                .get_blocks_in_range(scanned_range)
                .map_err(|e| HistoryError::internal(format!("get_blocks_in_range: {e}")))?;
            spans.push(LoadedSpan {
                range: scanned_range,
                items,
            });
        }
        Ok(spans)
    }

    fn load_pos(&self, value: serde_json::Value) -> Option<BlockNumber> {
        BlockNumber::from_value(value)
    }

    fn index_pos(p: &BlockNumber) -> BlockPos {
        // `ZERO` is the lower-bound sentinel (the `Position` impl's
        // documented wart, mirroring logs' `earliest()`): mapping it to
        // `Bottom` keeps genesis reachable from a default-start `Hist`.
        if *p == BlockNumber::ZERO {
            BlockPos::Bottom
        } else {
            BlockPos::end_of_block(*p)
        }
    }

    fn head_pos(n: BlockNumber) -> BlockNumber {
        n
    }

    fn window(from: &BlockNumber, to: &BlockNumber) -> BlockRange {
        BlockRange::new(*from, *to)
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::super::gap_fill::never_fetch;
    use super::super::{Fetcher, RangeIndex};
    use super::*;
    use crate::arch::{Position, Timestamp};
    use crate::chain::{ChunkSize, TxCount};
    use crate::evm::types::{Gas, GasPrice};
    use alloy::primitives::{Address, B256, U256};

    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    /// A deterministic per-height hash so parent links are checkable.
    /// Offset by 1 so block 0 does not hash to `ZERO`; `u128` arithmetic so
    /// genesis's `parent_hash` (height `u64::MAX` after wrapping) is fine.
    fn hash_for(n: u64) -> BlockHash {
        BlockHash::new(B256::from(U256::from(u128::from(n) + 1)))
    }

    fn mk_block(n: u64) -> EvmBlock {
        EvmBlock {
            number: bn(n),
            hash: hash_for(n),
            parent_hash: hash_for(n.wrapping_sub(1)),
            timestamp: Timestamp::new(1_700_000_000 + n * 12),
            tx_count: TxCount::new(100 + n as u32),
            gas_used: Gas::new(15_000_000),
            gas_limit: Gas::new(30_000_000),
            base_fee: Some(GasPrice::new(1_000_000_000)),
            miner: Address::repeat_byte(0x99),
        }
    }

    /// Dense page over `[from, to]`, one block per height.
    fn mk_page(from: u64, to: u64) -> BlockPage {
        let items: Vec<EvmBlock> = (from..=to).map(mk_block).collect();
        BlockPage {
            from_block: bn(from),
            to_block: bn(to),
            end_block_hash: items.last().expect("non-empty page").hash,
            items,
        }
    }

    fn mk_cache() -> Shared<LogCache> {
        Shared::new(LogCache::open_in_memory().unwrap())
    }

    fn mk_index_over(store: Shared<LogCache>) -> RangeIndex<BlockSpec> {
        RangeIndex::new(BlockSpec::new(store), never_fetch::<BlockSpec>())
    }

    /// Collect the numbers yielded by a `(end_of_block(from), end_of_block(to)]`
    /// walk — i.e. blocks `from+1 ..= to`.
    async fn walk(idx: &std::sync::Arc<RangeIndex<BlockSpec>>, from: u64, to: u64) -> Vec<u64> {
        idx.clone()
            .items(
                BlockPos::end_of_block(bn(from)),
                BlockPos::end_of_block(bn(to)),
            )
            .map(|r| r.expect("resolve").pos.get())
            .collect()
            .await
    }

    // -- dense page shape --

    #[test]
    fn dense_page_one_entry_per_block_no_empty_tail() {
        // Contrast with logs: page [100, 104] with 5 items fills the whole
        // span, so the last item sits exactly at the page end — 5 entries,
        // no trailing empty range.
        let idx = mk_index_over(mk_cache());
        idx.insert_page(&mk_page(100, 104));
        assert_eq!(idx.len(), 5);
    }

    // -- warm walk over persisted coverage --

    #[tokio::test]
    async fn warm_walk_serves_persisted_blocks_without_fetch() {
        let store = mk_cache();
        let spec = BlockSpec::new(store.share());
        spec.persist(&mk_page(100, 104)).unwrap();

        // Fresh index over the same durable store; never_fetch proves the
        // walk is served entirely from the lazy-loaded window.
        let idx = std::sync::Arc::new(mk_index_over(store));
        idx.ensure_window_loaded(BlockRange::new(bn(100), bn(104)))
            .unwrap();
        assert_eq!(walk(&idx, 99, 104).await, vec![100, 101, 102, 103, 104]);
    }

    // -- stub-fetcher gap fill --

    #[tokio::test]
    async fn gap_fill_fetches_once_and_persists() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = mk_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher<BlockPage> = {
            let calls = calls.clone();
            Arc::new(move |from: BlockNumber, to: BlockNumber| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(mk_page(from.get(), to.get())) })
            })
        };
        let idx = Arc::new(RangeIndex::with_chunk_size(
            BlockSpec::new(store.share()),
            fetcher,
            ChunkSize::new(1000),
        ));

        let got = idx
            .clone()
            .items(
                <BlockSpec as RangeSpec>::index_pos(&bn(99)),
                BlockPos::end_of_block(bn(104)),
            )
            .map(|r| r.expect("resolve").pos.get())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(got, vec![100, 101, 102, 103, 104]);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one fetch per gap");

        // A second index over the same store answers warm — the gap fill
        // persisted both rows and coverage.
        let idx2 = std::sync::Arc::new(mk_index_over(store));
        idx2.ensure_window_loaded(BlockRange::new(bn(100), bn(104)))
            .unwrap();
        assert_eq!(walk(&idx2, 99, 104).await, vec![100, 101, 102, 103, 104]);
    }

    // -- position mapping --

    #[test]
    fn index_pos_maps_zero_to_bottom_keeping_genesis_reachable() {
        assert_eq!(
            <BlockSpec as RangeSpec>::index_pos(&BlockNumber::ZERO),
            BlockPos::Bottom
        );
        // Genesis's own item position is strictly above Bottom, so a
        // default-start Hist (start = ZERO → Bottom) includes block 0.
        assert!(BlockSpec::item_pos(&mk_block(0)) > BlockPos::Bottom);
        assert_eq!(
            <BlockSpec as RangeSpec>::index_pos(&bn(7)),
            BlockSpec::item_pos(&mk_block(7))
        );
    }

    #[test]
    fn marshal_load_pos_round_trip() {
        let spec = BlockSpec::new(mk_cache());
        for n in [0u64, 1, 22_000_000] {
            assert_eq!(spec.load_pos(bn(n).marshal()), Some(bn(n)));
        }
        assert_eq!(spec.load_pos(serde_json::json!("not a number")), None);
    }

    #[test]
    fn page_tip_agrees_with_head_pos() {
        let page = mk_page(100, 104);
        assert_eq!(
            BlockSpec::page_tip(&page),
            <BlockSpec as RangeSpec>::head_pos(bn(104))
        );
    }

    #[tokio::test]
    async fn walk_from_bottom_including_genesis() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Pre-seed the store with blocks 1..3 (as the poll wiring would),
        // then walk from Bottom — which must also pull genesis (block 0).
        let store = mk_cache();
        BlockSpec::new(store.share())
            .persist(&mk_page(1, 3))
            .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher<BlockPage> = {
            let calls = calls.clone();
            Arc::new(move |from: BlockNumber, to: BlockNumber| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(mk_page(from.get(), to.get())) })
            })
        };
        let idx = Arc::new(RangeIndex::new(BlockSpec::new(store), fetcher));

        let got = idx
            .clone()
            .items(BlockPos::bottom(), BlockPos::end_of_block(bn(3)))
            .map(|r| r.expect("resolve").pos.get())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(got, vec![0, 1, 2, 3], "from Bottom yields genesis first");
    }
}
