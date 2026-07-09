//! Shared unit-test fixtures for the chain modules: a mock-provider chain
//! over a warm in-memory cache, plus an RPC-span counter for pinning
//! "zero RPC" guarantees.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, Bytes, Log as LogInner, LogData, U64, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::rpc::types::Log;
use alloy::transports::mock::{Asserter, MockTransport};
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use super::{EvmChain, EvmChainConfig};
use crate::arch::Timestamp;
use crate::cache::{FilterHash, ScanPartition};
use crate::chain::{BlockNumber, BlockRange, ChunkSize, TxCount};
use crate::evm::cache::{LogCache, compute_filter_hash};
use crate::evm::chain_provider::ChainProvider;
use crate::evm::transport::RpcTimingLayer;
use crate::evm::types::{BlockHash, ChainId, EvmBlock, Gas, GasPrice, LogIdx, Topic};
use crate::observe::RPC_TARGET;
use crate::shared::Shared;

pub(super) const ADDR: Address = Address::repeat_byte(0x42);
pub(super) const TOPICS: [Option<Topic>; 4] = [None; 4];
pub(super) const GENESIS: Timestamp = Timestamp::new(1_600_000_000);
pub(super) const TEST_CHAIN: ChainId = ChainId::new(1);
/// The mock chain head — the one `eth_blockNumber` answer.
pub(super) const HEAD: BlockNumber = BlockNumber::new(200);

pub(super) const BH_0: BlockHash = BlockHash::ZERO;
pub(super) const BH_100: BlockHash = BlockHash::repeat_byte(0xA0);
pub(super) const BH_101: BlockHash = BlockHash::repeat_byte(0xA1);
pub(super) const BH_200: BlockHash = BlockHash::repeat_byte(0xA2);

/// `GENESIS` shifted by `secs` — headers are stamped on a 12 s cadence.
pub(super) fn ts(secs: u64) -> Timestamp {
    GENESIS + secs
}

pub(super) fn make_log(block: BlockNumber, log_index: LogIdx, block_hash: BlockHash) -> Log {
    let topic0 = B256::from([0xDD; 32]);
    let data = Bytes::from(vec![0x01, 0x02]);
    let log_data = LogData::new_unchecked(vec![topic0], data);

    Log {
        inner: LogInner {
            address: ADDR,
            data: log_data,
        },
        block_hash: Some(block_hash.b256()),
        block_number: Some(block.get()),
        block_timestamp: None,
        transaction_hash: Some(B256::from([0xCC; 32])),
        transaction_index: Some(0),
        log_index: Some(log_index.get() as u64),
        removed: false,
    }
}

/// Insert a header row.
pub(super) fn insert_header(
    cache: &LogCache,
    block: BlockNumber,
    hash: BlockHash,
    time: Timestamp,
) {
    // Parent hash is irrelevant to these tests — resolve only reads
    // (hash, timestamp).
    cache
        .insert_block_header(block, &hash, &BlockHash::ZERO, time)
        .unwrap();
}

/// Warm log cache: three logs in blocks 100/101, headers for the anchor
/// blocks, full `[0, HEAD]` scan coverage.
pub(super) fn warm_cache() -> (Shared<LogCache>, FilterHash) {
    let cache = Shared::new(LogCache::open_in_memory().unwrap());
    let fh = compute_filter_hash(&ADDR, &TOPICS);

    // Dense headers on the 12 s cadence: the time locator's search probes
    // arbitrary heights (cache-first), and this fixture pins offline-ness.
    for n in 0..=HEAD.get() {
        let hash = match n {
            0 => BH_0,
            100 => BH_100,
            101 => BH_101,
            200 => BH_200,
            _ => BlockHash::new(B256::from(U256::from(n + 1_000))),
        };
        insert_header(&cache, BlockNumber::new(n), hash, ts(n * 12));
    }

    let logs = vec![
        make_log(BlockNumber::new(100), LogIdx::ZERO, BH_100),
        make_log(BlockNumber::new(100), LogIdx::new(1), BH_100),
        make_log(BlockNumber::new(101), LogIdx::ZERO, BH_101),
    ];
    cache.insert_logs(&fh, &logs).unwrap();
    cache
        .coverage()
        .record_scanned_range(
            &ScanPartition::Filter(fh),
            BlockRange::new(BlockNumber::ZERO, HEAD),
            BH_200.into(),
        )
        .unwrap();
    (cache, fh)
}

pub(super) fn test_block(n: u64) -> EvmBlock {
    EvmBlock {
        number: BlockNumber::new(n),
        // Unique per height (offset so genesis isn't ZERO).
        hash: BlockHash::new(B256::from(U256::from(n + 1))),
        parent_hash: BlockHash::new(B256::from(U256::from(n))),
        timestamp: ts(n * 12),
        tx_count: TxCount::new(n as u32),
        gas_used: Gas::new(15_000_000),
        gas_limit: Gas::new(30_000_000),
        base_fee: Some(GasPrice::new(1_000_000_000)),
        miner: Address::repeat_byte(0x99),
    }
}

/// Warm cache for the blocks source: anchor headers + dense `[0, HEAD]`
/// block coverage under the blocks partition.
pub(super) fn warm_blocks_cache() -> Shared<LogCache> {
    let cache = Shared::new(LogCache::open_in_memory().unwrap());
    insert_header(&cache, BlockNumber::ZERO, BH_0, GENESIS);
    insert_header(&cache, HEAD, BH_200, ts(2400));

    // The dense cached_blocks rows below double as the locator's probe
    // source (EvmBlockTimeSource reads them before any header fetch).
    let blocks: Vec<EvmBlock> = (0..=HEAD.get()).map(test_block).collect();
    let end_hash = blocks.last().unwrap().hash;
    cache.insert_blocks(&blocks).unwrap();
    cache
        .coverage()
        .record_scanned_range(
            &ScanPartition::Blocks,
            BlockRange::new(BlockNumber::ZERO, HEAD),
            end_hash.into(),
        )
        .unwrap();
    cache
}

/// Chain over a mock provider with `responses` pre-queued `eth_blockNumber`
/// answers of [`HEAD`]. Construction itself is offline; the queue is
/// consumed by whatever first demands the head (the clock's first poll).
pub(super) fn chain_with_queued_heads(
    cache: Shared<LogCache>,
    responses: usize,
) -> EvmChain<impl Provider<Ethereum> + Clone + 'static> {
    let asserter = Asserter::new();
    for _ in 0..responses {
        asserter.push_success(&U64::from(HEAD.get()));
    }
    // Same client stack as production wiring: the rpc-timing layer wraps the
    // (mock) transport. Without it every rpc span disappears here and the
    // RpcCounter zero-RPC guards would pass vacuously.
    let client = ClientBuilder::default()
        .layer(RpcTimingLayer)
        .transport(MockTransport::new(asserter), true);
    let provider = ProviderBuilder::default().connect_client(client);
    let provider = ChainProvider::new(provider, TEST_CHAIN, cache);
    EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(100),
            ..Default::default()
        },
    )
}

/// The standard test chain: one queued head answer for the single
/// expected demand.
pub(super) fn make_chain(
    cache: Shared<LogCache>,
) -> EvmChain<impl Provider<Ethereum> + Clone + 'static> {
    chain_with_queued_heads(cache, 1)
}

/// A chain whose mock provider has NO responses queued: any RPC — head
/// demand included — fails loudly. Pins fully-offline paths.
pub(super) fn offline_chain(
    cache: Shared<LogCache>,
) -> EvmChain<impl Provider<Ethereum> + Clone + 'static> {
    chain_with_queued_heads(cache, 0)
}

/// Tracing layer counting the rpc spans [`RpcTimingLayer`] emits for one
/// method (matched against the span's `method` field) — pins "zero RPC"
/// guarantees loudly. Install with `tracing::subscriber::set_default`
/// around the code under test.
#[derive(Clone)]
pub(super) struct RpcCounter {
    method: &'static str,
    count: Arc<AtomicUsize>,
}

impl RpcCounter {
    pub(super) fn new(method: &'static str) -> Self {
        Self {
            method,
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl<S> Layer<S> for RpcCounter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if attrs.metadata().target() != RPC_TARGET {
            return;
        }
        #[derive(Default)]
        struct MethodField(Option<String>);
        impl Visit for MethodField {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "method" {
                    self.0 = Some(value.to_owned());
                }
            }
            fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
        }
        let mut found = MethodField::default();
        attrs.record(&mut found);
        if found.0.as_deref() == Some(self.method) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }
}
