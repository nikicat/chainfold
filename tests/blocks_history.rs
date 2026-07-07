//! Integration tests for the blocks history (`BlockSpec` / `EvmChain::blocks`).
//!
//! - Cold/warm walk over a mainnet fork: the cold walk populates
//!   `cached_blocks` via real `eth_getBlockByNumber` batches; after dropping
//!   the chain (and its in-memory index) and reopening, the warm walk answers
//!   from the durable cache — same blocks, much faster.
//! - Poll-follow over a plain local anvil: mining new blocks flows through
//!   the store's tip and `watch_hist` delivers them.
//!
//! Marked `#[ignore]` because they spawn anvil (+ external RPC for the fork).
//! Run with `cargo test --test blocks_history -- --ignored`.

mod common;

use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::http::reqwest::Url;
use chainfold::Shared;
use chainfold::arch::{HistStore, watch_hist};
use chainfold::evm::{
    BlockNumber, ChainProvider, ChunkSize, EvmBlock, EvmChain, EvmChainConfig, Finality, LogCache,
    SubscribeTransport,
};
use futures_util::StreamExt;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use common::{
    anvil::{fork_mainnet, local, skip_no_anvil},
    fixtures::FORK_BLOCK,
};

async fn build_chain(
    db_path: &std::path::Path,
    endpoint: Url,
    follow: Option<SubscribeTransport>,
) -> EvmChain<impl Provider<Ethereum> + Clone + 'static + use<>> {
    let cache = Shared::new(LogCache::open(db_path.to_str().unwrap()).expect("open cache"));
    let provider = ChainProvider::discover(ProviderBuilder::new().connect_http(endpoint), cache)
        .await
        .expect("discover chain id");
    EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(50),
            block_time: Duration::from_secs(12),
            follow,
        },
    )
    .await
    .expect("EvmChain::new")
}

/// Walk `(from, to]` of the blocks store into a Vec.
async fn walk_blocks(
    store: &HistStore<EvmBlock, BlockNumber>,
    from: BlockNumber,
    to: BlockNumber,
) -> Vec<EvmBlock> {
    let hist = store.next().await;
    let (_, after_start) = hist.split(from);
    let (window, _) = after_start.split(to);
    let mut stream = window.iter();
    let mut blocks = Vec::new();
    while let Some(item) = stream.next().await {
        blocks.push(item.expect("iter item").item);
    }
    blocks
}

#[tokio::test]
#[ignore = "fork-mainnet integration: spawns anvil + hits external RPC"]
async fn cold_then_warm_walk_over_fork_window() {
    let Some(anvil) = fork_mainnet(FORK_BLOCK) else {
        skip_no_anvil("cold_then_warm_walk_over_fork_window");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("blocks.v3.db");
    let endpoint = anvil.endpoint_url();

    // (from, to] = the 20 blocks ending at FORK_BLOCK.
    let from = FORK_BLOCK - 20;
    let to = FORK_BLOCK;

    // Cold: populate cached_blocks via real eth_getBlockByNumber batches.
    let chain_cold = build_chain(&db_path, endpoint.clone(), None).await;
    let store_cold = chain_cold.blocks(Finality::Latest);
    let cold_start = Instant::now();
    let cold = walk_blocks(&store_cold, from, to).await;
    let cold_elapsed = cold_start.elapsed();

    // Dense + ascending + one per height.
    assert_eq!(
        cold.len(),
        20,
        "dense window: one block per height in (from, to]"
    );
    for (i, b) in cold.iter().enumerate() {
        assert_eq!(
            b.number,
            from + (i as u64 + 1),
            "blocks are dense and ascending"
        );
    }
    // Timestamps strictly increase; at least one block carries transactions.
    for pair in cold.windows(2) {
        assert!(pair[1].timestamp > pair[0].timestamp, "timestamps ascend");
    }
    assert!(
        cold.iter().any(|b| b.tx_count.get() > 0),
        "some block in the window has transactions"
    );
    // Parent links chain up.
    for pair in cold.windows(2) {
        assert_eq!(pair[1].parent_hash, pair[0].hash, "parent links chain up");
    }
    drop(store_cold);
    drop(chain_cold);

    // Warm: fresh chain + store over the same DB; served from cached_blocks.
    let chain_warm = build_chain(&db_path, endpoint, None).await;
    let store_warm = chain_warm.blocks(Finality::Latest);
    let warm_start = Instant::now();
    let warm = walk_blocks(&store_warm, from, to).await;
    let warm_elapsed = warm_start.elapsed();

    assert_eq!(cold, warm, "warm walk yields identical blocks");
    assert!(
        cold_elapsed.as_micros() > warm_elapsed.as_micros() * 5,
        "warm walk should be much faster (cold={cold_elapsed:?}, warm={warm_elapsed:?})"
    );
    eprintln!(
        "blocks_history: n={}, cold={cold_elapsed:?}, warm={warm_elapsed:?}, speedup={:.1}x",
        cold.len(),
        cold_elapsed.as_secs_f64() / warm_elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
    );
}

/// Mine `n` blocks on a plain local anvil. `anvil_mine` is synchronous.
async fn mine_blocks<P: Provider>(provider: &P, n: u64) {
    let _: serde_json::Value = provider
        .raw_request("anvil_mine".into(), [U256::from(n)])
        .await
        .expect("anvil_mine");
}

#[tokio::test]
#[ignore = "requires anvil on PATH"]
async fn poll_follow_delivers_mined_blocks() {
    let Some(anvil) = local() else {
        skip_no_anvil("poll_follow_delivers_mined_blocks");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("blocks_follow.v3.db");
    let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

    // Mine one block first so the construction head is a real block (not
    // genesis): following strictly-after a genesis (ZERO) anchor would also
    // surface block 0, since ZERO is the "before all blocks" sentinel.
    mine_blocks(&provider, 1).await;

    let chain = build_chain(
        &db_path,
        anvil.endpoint_url(),
        Some(SubscribeTransport::Poll {
            interval: Duration::from_millis(50),
        }),
    )
    .await;
    let store = chain.blocks(Finality::Latest);

    // Anchor at the construction tip; follow blocks strictly after it.
    let start = *store.next().await.end();
    assert!(
        start > BlockNumber::ZERO,
        "anchor is a real (non-genesis) block"
    );

    // Mine while following; each head advance gap-fills through the fetcher.
    mine_blocks(&provider, 3).await;

    let mut stream = watch_hist(&store, start);
    let mut got: Vec<BlockNumber> = Vec::new();
    let collected = timeout(Duration::from_secs(5), async {
        while got.len() < 3 {
            let item = stream.next().await.expect("stream stays live");
            got.push(item.expect("block item").pos);
        }
    })
    .await;
    assert!(collected.is_ok(), "delivered 3 mined blocks (got {got:?})");

    // Delivered in order, strictly after the anchor, dense.
    assert_eq!(got[0], start + 1);
    for pair in got.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "blocks delivered dense + ascending");
    }

    drop(stream);
    drop(store);
    drop(chain);
    sleep(Duration::from_millis(10)).await; // let wiring abort settle
}
