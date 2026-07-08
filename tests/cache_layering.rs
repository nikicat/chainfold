//! Cache-layering integration tests: durable `LogCache` + in-memory range
//! index (Phase 1 acceptance in `docs/refactor.md`).
//!
//! These hit a real RPC (mainnet via the anvil fork harness) so the cold
//! cache populates via actual `eth_getLogs` calls. After that we drop the
//! chain (and its in-memory index), reopen, and verify the warm walk
//! answers from the durable cache without refetching.
//!
//! Marked `#[ignore]` because they spawn anvil + hit external RPC. Run
//! with `cargo test --test cache_layering -- --ignored`.

mod common;

use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::http::reqwest::Url;
use chainfold::Shared;
use chainfold::arch::HistStore;
use chainfold::evm::{
    BlockNumber, ChainProvider, ChunkSize, EvmChain, EvmChainConfig, EvmEvent, EvmPosition,
    Finality, LogCache, LogFilter, TRANSFER_TOPIC,
};
use futures_util::StreamExt;
use tempfile::TempDir;

use common::{
    anvil::{fork_mainnet, skip_no_anvil},
    fixtures::{FORK_BLOCK, FORK_SWEEP_BLOCKS, USDC},
};

async fn build_store(
    db_path: &std::path::Path,
    endpoint: Url,
    chunk_size: u64,
) -> (
    EvmChain<impl Provider<Ethereum> + Clone + 'static + use<>>,
    HistStore<EvmEvent, EvmPosition>,
) {
    let cache = Shared::new(LogCache::open(db_path).expect("open cache"));
    let provider = ChainProvider::discover(ProviderBuilder::new().connect_http(endpoint), cache)
        .await
        .expect("discover chain id");
    let chain = EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(chunk_size),
            block_time: Duration::from_secs(12),
            ..Default::default()
        },
    );
    let store = chain.logs(
        &LogFilter::new(USDC, [Some(TRANSFER_TOPIC), None, None, None]),
        Finality::Latest,
    );
    (chain, store)
}

async fn walk_forward(
    store: &HistStore<EvmEvent, EvmPosition>,
    from_block: BlockNumber,
    to_block: BlockNumber,
) -> usize {
    let start = match from_block.checked_sub(1) {
        None => EvmPosition::earliest(),
        Some(prev) => EvmPosition::new(prev, u32::MAX),
    };
    let end = EvmPosition::new(to_block, u32::MAX);
    let hist = store.next().await;
    let (_, after_start) = hist.split(start);
    let (window, _) = after_start.split(end);
    let mut stream = window.iter();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        let _ = item.expect("iter item");
        count += 1;
    }
    count
}

#[tokio::test]
#[ignore = "fork-mainnet integration: spawns anvil + hits external RPC"]
async fn warm_store_then_drop_index_then_reopen() {
    let Some(anvil) = fork_mainnet(FORK_BLOCK) else {
        skip_no_anvil("warm_store_then_drop_index_then_reopen");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("phase1.v3.db");
    let endpoint = anvil.endpoint_url();

    let from = FORK_BLOCK - 20;
    let to = FORK_BLOCK;
    let _ = FORK_SWEEP_BLOCKS;
    let chunk_size = 10;

    let (chain_cold, store_cold) = build_store(&db_path, endpoint.clone(), chunk_size).await;
    let cold_start = Instant::now();
    let cold_count = walk_forward(&store_cold, from, to).await;
    let cold_elapsed = cold_start.elapsed();
    assert!(
        cold_count > 0,
        "expected USDC transfers in the sweep window"
    );
    drop(store_cold);
    drop(chain_cold);

    let (_chain_warm, store_warm) = build_store(&db_path, endpoint, chunk_size).await;
    let warm_start = Instant::now();
    let warm_count = walk_forward(&store_warm, from, to).await;
    let warm_elapsed = warm_start.elapsed();

    assert_eq!(
        cold_count, warm_count,
        "warm walk should yield identical event count"
    );

    assert!(
        cold_elapsed.as_micros() > warm_elapsed.as_micros() * 5,
        "warm walk should be much faster (cold={cold_elapsed:?}, warm={warm_elapsed:?})"
    );

    eprintln!(
        "cache_layering: count={cold_count}, cold={cold_elapsed:?}, warm={warm_elapsed:?}, speedup={:.1}x",
        cold_elapsed.as_secs_f64() / warm_elapsed.as_secs_f64()
    );
}
