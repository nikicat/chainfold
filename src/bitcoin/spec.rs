//! [`BtcBlockSpec`] — the [`RangeSpec`] for Bitcoin blocks: the third
//! instance after the EVM `LogSpec`/`BlockSpec`, backing
//! [`BitcoinChain::blocks`](super::BitcoinChain::blocks).
//!
//! Blocks are **dense**: every height in a scanned range is an item,
//! pinned at `end_of_block(n)` in the shared
//! [`HeightPos`](crate::range_index::HeightPos) key space, so a page
//! produces one tree entry per block and no empty tails. The tree stores
//! the whole [`BtcBlock`] (all-`Copy`) — [`resolve`](RangeSpec::resolve)
//! is a pure copy, with no per-row `SQLite` read.
//!
//! Coverage lives in `scanned_ranges` under [`ScanPartition::BtcBlocks`];
//! rows live in `cached_btc_blocks`, written only by this spec's
//! [`persist`](RangeSpec::persist).
use std::sync::Arc;

use super::cache::BtcBlockCache;
use super::types::{BtcBlock, BtcBlockHash};
use crate::arch::{HistoryError, Positioned};
use crate::cache::ScanPartition;
use crate::chain::{BlockNumber, BlockRange};
use crate::range_index::{HeightPos, IndexPos, LoadedSpan, RangeSpec};

/// [`RangeSpec`] for the chain's block history over a [`BtcBlockCache`].
#[derive(Clone)]
pub struct BtcBlockSpec {
    store: Arc<BtcBlockCache>,
}

impl BtcBlockSpec {
    pub fn new(store: Arc<BtcBlockCache>) -> Self {
        Self { store }
    }
}

/// A page of consecutive blocks covering `[from_block, to_block]` from one
/// gap fill: `items` is dense ascending — exactly one [`BtcBlock`] per
/// height in the range.
#[derive(Clone)]
pub struct BtcBlockPage {
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
    pub items: Vec<BtcBlock>,
    /// The last item's hash — recorded as the coverage range's end hash so
    /// a reorg at the range end invalidates the coverage claim.
    pub end_block_hash: BtcBlockHash,
}

impl RangeSpec for BtcBlockSpec {
    type Pos = HeightPos;
    type Item = BtcBlock;
    type Event = BtcBlock;
    type PubPos = BlockNumber;
    type Page = BtcBlockPage;

    fn item_pos(item: &BtcBlock) -> HeightPos {
        HeightPos::end_of_block(item.height)
    }

    fn page_items(page: &BtcBlockPage) -> Vec<BtcBlock> {
        page.items.clone()
    }

    fn page_bounds(page: &BtcBlockPage) -> BlockRange {
        BlockRange::new(page.from_block, page.to_block)
    }

    fn page_tip(page: &BtcBlockPage) -> BlockNumber {
        page.to_block
    }

    fn resolve(&self, item: &BtcBlock) -> Result<Positioned<BtcBlock, BlockNumber>, HistoryError> {
        // The tree stores the whole block — a pure copy, never a store read.
        Ok(Positioned::new(*item, item.height))
    }

    fn persist(&self, page: &BtcBlockPage) -> Result<(), HistoryError> {
        if !page.items.is_empty() {
            self.store
                .insert_blocks(&page.items)
                .map_err(|e| HistoryError::internal(format!("insert_blocks: {e}")))?;
        }
        self.store
            .coverage()
            .record_scanned_range(
                &ScanPartition::BtcBlocks,
                BlockRange::new(page.from_block, page.to_block),
                page.end_block_hash.into(),
            )
            .map_err(|e| HistoryError::internal(format!("record_scanned_range: {e}")))
    }

    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<BtcBlock>>, HistoryError> {
        let scanned = self
            .store
            .coverage()
            .get_scanned_ranges(&ScanPartition::BtcBlocks)
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

    fn index_pos(p: &BlockNumber) -> HeightPos {
        // `ZERO` is the lower-bound sentinel (the `Position` impl's
        // documented wart, mirroring the EVM sources' `earliest()`):
        // mapping it to `Bottom` keeps genesis reachable from a
        // default-start `Hist`.
        if *p == BlockNumber::ZERO {
            HeightPos::Bottom
        } else {
            HeightPos::end_of_block(*p)
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

    use super::*;
    use crate::arch::{Position, Timestamp};
    use crate::bitcoin::types::CompactTarget;
    use crate::chain::{ChunkSize, TxCount};
    use crate::range_index::{Fetcher, RangeIndex, never_fetch};
    use crate::share::Share;

    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    /// A deterministic per-height hash so parent links are checkable.
    /// Offset by 1 so block 0 does not hash to `ZERO`.
    fn hash_for(n: u64) -> BtcBlockHash {
        let mut bytes = [0u8; 32];
        bytes[24..].copy_from_slice(&(n + 1).to_be_bytes());
        BtcBlockHash::new(bytes)
    }

    fn mk_block(n: u64) -> BtcBlock {
        BtcBlock {
            height: bn(n),
            hash: hash_for(n),
            prev_hash: if n == 0 {
                BtcBlockHash::ZERO
            } else {
                hash_for(n - 1)
            },
            time: Timestamp::new(1_700_000_000 + n * 600),
            tx_count: TxCount::new(1_000 + n as u32),
            bits: CompactTarget::new(0x1703_2f14),
        }
    }

    /// Dense page over `[from, to]`, one block per height.
    fn mk_page(from: u64, to: u64) -> BtcBlockPage {
        let items: Vec<BtcBlock> = (from..=to).map(mk_block).collect();
        BtcBlockPage {
            from_block: bn(from),
            to_block: bn(to),
            end_block_hash: items.last().expect("non-empty page").hash,
            items,
        }
    }

    fn mk_cache() -> Arc<BtcBlockCache> {
        Arc::new(BtcBlockCache::open_in_memory().unwrap())
    }

    fn mk_index_over(store: Arc<BtcBlockCache>) -> RangeIndex<BtcBlockSpec> {
        RangeIndex::new(BtcBlockSpec::new(store), never_fetch::<BtcBlockSpec>())
    }

    /// Collect the heights yielded by a `(end_of_block(from), end_of_block(to)]`
    /// walk — i.e. blocks `from+1 ..= to`.
    async fn walk(idx: &std::sync::Arc<RangeIndex<BtcBlockSpec>>, from: u64, to: u64) -> Vec<u64> {
        idx.share()
            .items(
                HeightPos::end_of_block(bn(from)),
                HeightPos::end_of_block(bn(to)),
            )
            .map(|r| r.expect("resolve").pos.get())
            .collect()
            .await
    }

    // -- dense page shape --

    #[test]
    fn dense_page_one_entry_per_block_no_empty_tail() {
        let idx = mk_index_over(mk_cache());
        idx.insert_page(&mk_page(100, 104));
        assert_eq!(idx.len(), 5);
    }

    // -- warm walk over persisted coverage --

    #[tokio::test]
    async fn warm_walk_serves_persisted_blocks_without_fetch() {
        let store = mk_cache();
        let spec = BtcBlockSpec::new(store.share());
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
        let fetcher: Fetcher<BtcBlockPage> = {
            let calls = calls.share();
            Arc::new(move |from: BlockNumber, to: BlockNumber| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(mk_page(from.get(), to.get())) })
            })
        };
        let idx = Arc::new(RangeIndex::with_chunk_size(
            BtcBlockSpec::new(store.share()),
            fetcher,
            ChunkSize::new(1000),
        ));

        let got = idx
            .share()
            .items(
                <BtcBlockSpec as RangeSpec>::index_pos(&bn(99)),
                HeightPos::end_of_block(bn(104)),
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
            <BtcBlockSpec as RangeSpec>::index_pos(&BlockNumber::ZERO),
            HeightPos::Bottom
        );
        // Genesis's own item position is strictly above Bottom, so a
        // default-start Hist (start = ZERO → Bottom) includes block 0.
        assert!(BtcBlockSpec::item_pos(&mk_block(0)) > HeightPos::Bottom);
        assert_eq!(
            <BtcBlockSpec as RangeSpec>::index_pos(&bn(7)),
            BtcBlockSpec::item_pos(&mk_block(7))
        );
    }

    #[test]
    fn marshal_load_pos_round_trip() {
        let spec = BtcBlockSpec::new(mk_cache());
        for n in [0u64, 1, 840_000] {
            assert_eq!(spec.load_pos(bn(n).marshal()), Some(bn(n)));
        }
        assert_eq!(spec.load_pos(serde_json::json!("not a number")), None);
    }

    #[test]
    fn page_tip_agrees_with_head_pos() {
        let page = mk_page(100, 104);
        assert_eq!(
            BtcBlockSpec::page_tip(&page),
            <BtcBlockSpec as RangeSpec>::head_pos(bn(104))
        );
    }

    #[tokio::test]
    async fn walk_from_bottom_including_genesis() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Pre-seed the store with blocks 1..3 (as the poll wiring would),
        // then walk from Bottom — which must also pull genesis (block 0).
        let store = mk_cache();
        BtcBlockSpec::new(store.share())
            .persist(&mk_page(1, 3))
            .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let fetcher: Fetcher<BtcBlockPage> = {
            let calls = calls.share();
            Arc::new(move |from: BlockNumber, to: BlockNumber| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(mk_page(from.get(), to.get())) })
            })
        };
        let idx = Arc::new(RangeIndex::new(BtcBlockSpec::new(store), fetcher));

        let got = idx
            .share()
            .items(HeightPos::bottom(), HeightPos::end_of_block(bn(3)))
            .map(|r| r.expect("resolve").pos.get())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(got, vec![0, 1, 2, 3], "from Bottom yields genesis first");
    }
}
