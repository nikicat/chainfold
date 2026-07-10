//! [`BitcoinChain`] — the per-chain client entity. Owns the RPC client,
//! the durable [`BtcBlockCache`], **one** demand-started head clock (a
//! `getblockcount` poll), and vends the block history:
//! [`blocks`](BitcoinChain::blocks) / [`blocks_hist`](BitcoinChain::blocks_hist).
//!
//! The concrete sibling of the EVM `EvmChain` over the same shared engine
//! (see `crate::chain` for the head/anchor/wiring scaffolding): tracking is
//! demand-started, bounded hists never demand the head, and a fully-cached
//! walk is completely offline.
//!
//! Head tracking is poll-only for now — bitcoind has no JSON-RPC push;
//! ZMQ's `hashblock` notifications are the natural future upgrade (they
//! slot in as a second [`HeadClock`] session, exactly like the EVM ws
//! path).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell;
use tokio::time::sleep;
use tokio_util::task::AbortOnDropHandle;

use super::cache::BtcBlockCache;
use super::rpc::BtcRpcClient;
use super::spec::{BtcBlockPage, BtcBlockSpec};
use super::types::BtcBlock;
use crate::arch::{Hist, HistStore, HistoryError, Timestamp};
use crate::chain::{
    BlockCount, BlockNumber, BlockRange, BlockTimeSource, ChunkSize, Finality, HeadCell, HeadClock,
    HeadSeeder, TimeAnchor, TimeLocator, follow_head,
};
use crate::observe::{CacheLayer, CacheOutcome, cache_event};
use crate::range_index::{Fetcher, RangeIndex, RangeIndexStore, TimePos, TipSeed};
use crate::share::Share;

/// Default gap-fill / alignment chunk size.
pub const DEFAULT_BTC_CHUNK_SIZE: ChunkSize = ChunkSize::new(1000);

/// Bitcoin's nominal block cadence, for
/// [`HistStore::time_pos`](crate::arch::HistStore::time_pos) estimation.
pub const DEFAULT_BTC_BLOCK_TIME: Duration = Duration::from_mins(10);

/// Default `getblockcount` poll cadence — a small fraction of the ten-minute
/// block time; snappy enough for a follow without hammering the node.
pub const DEFAULT_BTC_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Construction options for [`BitcoinChain`].
#[derive(Clone, Debug)]
pub struct BitcoinChainConfig {
    /// Gap-fill chunk size (block-fetch alignment).
    pub chunk_size: ChunkSize,
    /// Assumed time per block, for `time_pos` estimation. Bitcoin's
    /// variance around the 10-minute target is high, but the estimate only
    /// anchors a walk — same contract as the EVM side.
    pub block_time: Duration,
    /// Head-poll cadence. Tracking starts lazily — exactly once, on the
    /// first demand for a tip — and then runs for the life of the chain
    /// and its stores. Construction itself performs no RPC and spawns
    /// nothing.
    pub poll_interval: Duration,
}

impl Default for BitcoinChainConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_BTC_CHUNK_SIZE,
            block_time: DEFAULT_BTC_BLOCK_TIME,
            poll_interval: DEFAULT_BTC_POLL_INTERVAL,
        }
    }
}

// ============================================================
// BitcoinChain
// ============================================================

pub struct BitcoinChain {
    rpc: BtcRpcClient,
    cache: Arc<BtcBlockCache>,
    chunk_size: ChunkSize,
    block_time: Duration,

    /// Shared head machinery: the head channel plus the demand-started
    /// clock (a [`BtcHeadClock`] poll session under the shared backoff
    /// runner). Every store holds it as its
    /// [`TipSeed`](crate::range_index::TipSeed).
    seeder: Arc<HeadSeeder>,

    /// Wall-clock → height estimation anchor, resolved lazily on the first
    /// `time_pos` / [`Self::estimate_block`] call.
    anchor: Arc<OnceCell<TimeAnchor>>,
}

impl BitcoinChain {
    /// Create the chain entity. Construction does nothing observable: no
    /// RPC, no tasks. Head tracking (the `getblockcount` poll) starts on
    /// the first demand for a tip.
    pub fn new(rpc: BtcRpcClient, cache: Arc<BtcBlockCache>, config: &BitcoinChainConfig) -> Self {
        tracing::debug!(
            poll_interval = ?config.poll_interval,
            chunk_size = %config.chunk_size,
            "BitcoinChain created"
        );

        let clock = BtcHeadClock {
            rpc: rpc.clone(),
            interval: config.poll_interval,
        };
        Self {
            seeder: Arc::new(HeadSeeder::new(Arc::new(clock) as Arc<dyn HeadClock>)),
            rpc,
            cache,
            chunk_size: config.chunk_size,
            block_time: config.block_time,
            anchor: Arc::new(OnceCell::new()),
        }
    }

    /// The shared durable cache.
    pub fn cache(&self) -> &Arc<BtcBlockCache> {
        &self.cache
    }

    /// Open the chain's block history at `tier`. Cheap, synchronous and
    /// side-effect-free — no RPC; cached blocks materialize lazily, gaps
    /// fill on demand (batched `getblockhash` → `getblockheader`). The
    /// store's tip is driven by this chain's head clock; the first
    /// consumer awaiting a snapshot demand-starts it. All background
    /// wiring dies with the returned store.
    pub fn blocks(&self, tier: Finality) -> HistStore<BtcBlock, BlockNumber> {
        let _ = tier; // Latest-only until Phase 7 (same as the EVM side).

        let store = self.block_store(Some(self.seeder.share() as Arc<dyn TipSeed>));

        let wiring = tokio::spawn(follow_head::<BtcBlockSpec>(
            self.seeder.head().subscribe(),
            Arc::downgrade(&store),
        ));
        store.attach_wiring(AbortOnDropHandle::new(wiring));

        tracing::debug!("bitcoin block store opened");

        store.hist_store()
    }

    /// Open a finite block history over the explicit inclusive `range`.
    /// Head-free: nothing is fetched at construction, the clock is never
    /// demanded, and a fully-cached walk performs no RPC at all; gaps fill
    /// on demand through the batched block fetcher. A range extending
    /// beyond the real chain head fails loudly at iteration rather than
    /// clamping.
    pub fn blocks_hist(&self, range: BlockRange) -> Hist<BtcBlock, BlockNumber> {
        let store = self.block_store(None);

        tracing::debug!(from = %range.from, to = %range.to, "bounded bitcoin block hist opened");

        Hist::new(store, block_cut_before(range.from), range.to)
    }

    /// The first block minted at/after wall-clock `time` (clamped to the
    /// head observed when the anchor resolved; exact up to Bitcoin's local
    /// timestamp wobble — consensus only enforces median-time
    /// monotonicity). The nominal ten-minute cadence seeds only an initial
    /// guess, refined against real block timestamps by the shared
    /// [`TimeLocator`] — cache-first, so warm searches are offline. The
    /// first call demands the head (starting tracking if needed).
    pub async fn estimate_block(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        self.locator().locate(time).await
    }

    /// The store both entry points share: spec + fetcher + reactive layer.
    fn block_store(
        &self,
        tip_seed: Option<Arc<dyn TipSeed>>,
    ) -> Arc<RangeIndexStore<BtcBlockSpec>> {
        let spec = BtcBlockSpec::new(self.cache.share());
        let fetcher = build_btc_block_fetcher(self.rpc.clone());
        let index = Arc::new(RangeIndex::with_chunk_size(spec, fetcher, self.chunk_size));
        RangeIndexStore::new(
            index,
            Arc::new(BtcBlockTimePos {
                locator: self.locator(),
            }),
            tip_seed,
        )
    }

    fn locator(&self) -> TimeLocator {
        TimeLocator::new(
            self.seeder.share(),
            Arc::new(BtcBlockTimeSource {
                rpc: self.rpc.clone(),
                cache: self.cache.share(),
            }),
            self.anchor.share(),
            self.block_time,
        )
    }
}

// ============================================================
// Head clock
// ============================================================

/// The Bitcoin head-tracking session: a `getblockcount` poll. Polls
/// immediately on entry — the clock is demand-started, so a consumer is
/// already waiting on the first observation. Only returns on error (the
/// shared runner reconnects with backoff).
struct BtcHeadClock {
    rpc: BtcRpcClient,
    interval: Duration,
}

#[async_trait::async_trait]
impl HeadClock for BtcHeadClock {
    async fn session(&self, head: &HeadCell) -> HistoryError {
        loop {
            match self.rpc.get_block_count().await {
                Ok(n) => head.publish(n),
                Err(e) => return e,
            }
            sleep(self.interval).await;
        }
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

/// [`TimePos`] for the dense blocks space: the cut "just before" the first
/// block minted at/after the target time (located against real block
/// timestamps, cache-first — see [`TimeLocator`]), so iteration after the
/// cut starts with that block.
struct BtcBlockTimePos {
    locator: TimeLocator,
}

#[async_trait::async_trait]
impl TimePos<BlockNumber> for BtcBlockTimePos {
    async fn time_pos(&self, time: Timestamp) -> Result<BlockNumber, HistoryError> {
        Ok(block_cut_before(self.locator.locate(time).await?))
    }
}

/// The Bitcoin [`BlockTimeSource`]: block `n`'s timestamp, cache-first over
/// `cached_btc_blocks`; on a miss, one `getblockhash` + `getblockheader`
/// round-trip whose full row is written back (no coverage claim — a bare
/// row is invisible to walks until a scan covers it, exactly like the EVM
/// header cache).
struct BtcBlockTimeSource {
    rpc: BtcRpcClient,
    cache: Arc<BtcBlockCache>,
}

#[async_trait::async_trait]
impl BlockTimeSource for BtcBlockTimeSource {
    async fn block_time_at(&self, n: BlockNumber) -> Result<Timestamp, HistoryError> {
        if let Some(block) = self
            .cache
            .get_block_at(n)
            .map_err(|e| HistoryError::internal(format!("get_block_at: {e}")))?
        {
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
            return Ok(block.time);
        }
        cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);

        let hashes = self.rpc.get_block_hashes(BlockRange::new(n, n)).await?;
        let blocks = self.rpc.get_block_headers(&hashes).await?;
        let block = blocks
            .first()
            .copied()
            .ok_or_else(|| HistoryError::internal(format!("block {n} not found")))?;
        self.cache
            .insert_blocks(std::slice::from_ref(&block))
            .map_err(|e| HistoryError::internal(format!("insert_blocks: {e}")))?;
        Ok(block.time)
    }
}

// ============================================================
// Fetcher
// ============================================================

/// JSON-RPC batch size for the block fetcher: one `getblockhash` + one
/// `getblockheader` per height, packed [`BLOCKS_PER_BATCH`] to a wire
/// batch. bitcoind processes a batch sequentially on one RPC thread and
/// buffers the whole response, so batches stay small.
const BLOCKS_PER_BATCH: BlockCount = BlockCount::new(100);

/// Hard cap on one gap-fill's span. Blocks pay two calls *per height*, so
/// an unbounded default-start walk (`(Bottom, tip]` on mainnet ≈ 1.7M
/// calls) must fail loudly instead of grinding. CLI paths always bound the
/// walk via `split()`.
const MAX_BLOCK_FETCH: BlockCount = BlockCount::new(10_000);

/// The gap-fill fetcher for the blocks history: batched
/// `getblockhash(height)` resolving to batched `getblockheader(hash)` over
/// the inclusive range — one verbose header carries every [`BtcBlock`]
/// field including `nTx`.
fn build_btc_block_fetcher(rpc: BtcRpcClient) -> Fetcher<BtcBlockPage> {
    Arc::new(move |from: BlockNumber, to: BlockNumber| {
        let rpc = rpc.clone();
        Box::pin(async move {
            let span = to.get().saturating_sub(from.get()) + 1;
            if span > MAX_BLOCK_FETCH.get() {
                return Err(HistoryError::internal(format!(
                    "blocks gap-fill span {span} exceeds {MAX_BLOCK_FETCH} \
                     ({from}..={to}) — bound the walk with split() before iterating"
                )));
            }
            tracing::debug!(from = %from, to = %to, blocks = span, "filling block gap");

            let mut items: Vec<BtcBlock> = Vec::with_capacity(span as usize);
            let mut n = from;
            while n <= to {
                let batch_to = to.min(n + (BLOCKS_PER_BATCH.get() - 1));
                let hashes = rpc.get_block_hashes(BlockRange::new(n, batch_to)).await?;
                let blocks = rpc.get_block_headers(&hashes).await?;
                for (offset, block) in blocks.iter().enumerate() {
                    let expected = n + offset as u64;
                    if block.height != expected {
                        return Err(HistoryError::internal(format!(
                            "block height mismatch: asked {expected}, got {}",
                            block.height
                        )));
                    }
                }
                items.extend(blocks);
                n = batch_to + 1;
            }

            let end_block_hash = items.last().expect("span >= 1").hash;
            Ok(BtcBlockPage {
                from_block: from,
                to_block: to,
                items,
                end_block_hash,
            })
        })
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::super::rpc::tests::ScriptedTransport;
    use super::super::types::{BtcBlockHash, CompactTarget};
    use super::*;
    use crate::arch::Positioned;
    use crate::cache::ScanPartition;
    use crate::chain::TxCount;

    const HEAD: BlockNumber = BlockNumber::new(200);
    /// Head timestamp for the warm fixture: `ts(HEAD * 600)`.
    fn ts(offset: u64) -> Timestamp {
        Timestamp::new(1_700_000_000 + offset)
    }

    fn hash_for(n: u64) -> BtcBlockHash {
        let mut bytes = [0u8; 32];
        bytes[24..].copy_from_slice(&(n + 1).to_be_bytes());
        BtcBlockHash::new(bytes)
    }

    fn test_block(n: u64) -> BtcBlock {
        BtcBlock {
            height: BlockNumber::new(n),
            hash: hash_for(n),
            prev_hash: if n == 0 {
                BtcBlockHash::ZERO
            } else {
                hash_for(n - 1)
            },
            time: ts(n * 600),
            tx_count: TxCount::new(n as u32),
            bits: CompactTarget::new(0x207f_ffff),
        }
    }

    /// A cache pre-populated with a dense `[0, HEAD]` block history plus
    /// its coverage claim — the walk-warm fixture.
    fn warm_cache() -> Arc<BtcBlockCache> {
        let cache = Arc::new(BtcBlockCache::open_in_memory().unwrap());
        let blocks: Vec<BtcBlock> = (0..=HEAD.get()).map(test_block).collect();
        cache.insert_blocks(&blocks).unwrap();
        cache
            .coverage()
            .record_scanned_range(
                &ScanPartition::BtcBlocks,
                BlockRange::new(BlockNumber::ZERO, HEAD),
                blocks.last().unwrap().hash.into(),
            )
            .unwrap();
        cache
    }

    /// A chain whose transport panics on ANY wire call — construction and
    /// fully-cached paths must be completely offline.
    fn offline_chain(cache: Arc<BtcBlockCache>) -> BitcoinChain {
        BitcoinChain::new(
            BtcRpcClient::new(ScriptedTransport::default()),
            cache,
            &BitcoinChainConfig::default(),
        )
    }

    /// A chain whose script holds exactly one `getblockcount` reply — the
    /// clock's first poll consumes it; any further RPC fails loudly.
    fn chain_with_head(cache: Arc<BtcBlockCache>) -> BitcoinChain {
        let transport = ScriptedTransport::default();
        transport.push_ok(serde_json::json!(HEAD.get()));
        BitcoinChain::new(
            BtcRpcClient::new(transport),
            cache,
            &BitcoinChainConfig {
                // Long interval: the first poll seeds the tip, the second
                // never fires within the test.
                poll_interval: Duration::from_hours(1),
                ..BitcoinChainConfig::default()
            },
        )
    }

    /// Construction (and opening stores) is completely offline: the
    /// scripted transport has no replies queued, so any RPC panics.
    #[tokio::test]
    async fn construction_and_store_open_are_offline() {
        let chain = offline_chain(warm_cache());
        let _blocks = chain.blocks(Finality::Latest);
        let _hist = chain.blocks_hist(BlockRange::from_u64(100, 110));
    }

    #[tokio::test]
    async fn blocks_tip_seeded_from_demanded_head() {
        let chain = chain_with_head(warm_cache());
        // The first consumer await demand-starts the clock, whose first
        // poll observes the head and seeds the store's tip.
        let hist = chain.blocks(Finality::Latest).next().await;
        assert_eq!(*hist.end(), HEAD);
    }

    /// A fully-cached bounded walk is completely offline: the scripted
    /// transport has NO replies queued, so any RPC — head demand included
    /// — fails the test loudly. (The zero-RPC guard for free: every wire
    /// call panics.)
    #[tokio::test]
    async fn blocks_hist_fully_cached_walk_is_offline() {
        let chain = offline_chain(warm_cache());
        let hist = chain.blocks_hist(BlockRange::from_u64(100, 110));

        let items: Vec<Positioned<BtcBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        assert_eq!(items.len(), 11);
        assert_eq!(items[0].pos, BlockNumber::new(100));
        assert_eq!(items[0].item.hash, test_block(100).hash);
        assert_eq!(items.last().unwrap().pos, BlockNumber::new(110));
    }

    #[tokio::test]
    async fn blocks_iter_walks_dense_window_including_genesis() {
        let chain = chain_with_head(warm_cache());
        let hist = chain.blocks(Finality::Latest).next().await;

        let items: Vec<Positioned<BtcBlock, BlockNumber>> =
            hist.iter().try_collect().await.unwrap();
        assert_eq!(items.len(), HEAD.get() as usize + 1, "dense incl. genesis");
        assert_eq!(items[0].pos, BlockNumber::ZERO);
        assert_eq!(items.last().unwrap().pos, HEAD);
        let b100 = &items[100];
        assert_eq!(b100.item.height, BlockNumber::new(100));
        assert_eq!(b100.item.tx_count, TxCount::new(100));
        assert_eq!(b100.item.time, ts(60_000));
    }

    #[tokio::test]
    async fn blocks_time_pos_cuts_just_before_estimated_block() {
        let chain = chain_with_head(warm_cache());
        let store = chain.blocks(Finality::Latest);
        // 600 s cadence anchored at (HEAD, ts(HEAD*600)); the anchor block
        // is served from the warm cache (no header RPC). ts(60000) is
        // block 100's time → cut just before it = 99.
        assert_eq!(
            store.time_pos(ts(60_000)).await.unwrap(),
            BlockNumber::new(99)
        );
        // Before-anchor underflow clamps to the ZERO sentinel.
        assert_eq!(
            store.time_pos(Timestamp::new(0)).await.unwrap(),
            BlockNumber::ZERO,
            "estimate at/below genesis clamps to the lower-bound sentinel"
        );
    }

    /// The wall-clock estimator resolves its anchor once (head demand +
    /// cache-first block read) and is pure arithmetic afterwards.
    #[tokio::test]
    async fn estimate_block_resolves_anchor_on_demand() {
        let chain = chain_with_head(warm_cache());
        assert_eq!(
            chain.estimate_block(ts(60_000)).await.unwrap(),
            BlockNumber::new(100)
        );
        // Second call: no head demand, no block read — the script is
        // empty, so an RPC here would panic.
        assert_eq!(
            chain.estimate_block(Timestamp::new(0)).await.unwrap(),
            BlockNumber::ZERO
        );
    }
}
