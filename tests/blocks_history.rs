//! Integration tests for the blocks history (`BlockSpec` / `EvmChain::blocks`).
//!
//! Both run against a **plain local anvil** — no fork, no upstream RPC — so
//! they are fully offline and don't depend on `MAINNET_RPC_URL` or a public
//! endpoint's rate limits. Anvil auto-mines a block per transaction, so a mix
//! of `eth_sendTransaction` (blocks with txs) and `anvil_mine` (empty blocks)
//! gives a realistic dense chain to walk.
//!
//! - `cold_then_warm_walk`: the cold walk populates `cached_blocks` via real
//!   `eth_getBlockByNumber` batches against anvil; after dropping the chain
//!   (and its in-memory index) and reopening the DB, the warm walk answers
//!   from the durable cache — identical blocks, no RPC.
//! - `poll_follow_delivers_mined_blocks`: mining new blocks flows through the
//!   store's tip and `watch_hist` delivers them.
//!
//! Marked `#[ignore]` because they spawn anvil. Run with
//! `cargo test --test blocks_history -- --ignored`.

mod common;

use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, U256};
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

use common::anvil::{local, skip_no_anvil};

async fn build_chain(
    db_path: &std::path::Path,
    endpoint: Url,
    transport: SubscribeTransport,
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
            transport,
        },
    )
}

/// Mine `n` empty blocks on a local anvil. `anvil_mine` is synchronous.
async fn mine_blocks<P: Provider>(provider: &P, n: u64) {
    let _: serde_json::Value = provider
        .raw_request("anvil_mine".into(), [U256::from(n)])
        .await
        .expect("anvil_mine");
}

/// Send one self-transfer from anvil's first (unlocked, pre-funded) dev
/// account. Anvil auto-mines it into a fresh block carrying one transaction.
async fn send_tx<P: Provider>(provider: &P, sender: Address) {
    let _: B256 = provider
        .raw_request(
            "eth_sendTransaction".into(),
            [serde_json::json!({ "from": sender, "to": sender, "value": "0x1" })],
        )
        .await
        .expect("eth_sendTransaction");
}

/// Grow the local chain to a dense mix of tx-bearing and empty blocks, and
/// return the resulting head.
async fn seed_blocks<P: Provider>(provider: &P) -> BlockNumber {
    let accounts: Vec<Address> = provider
        .raw_request("eth_accounts".into(), ())
        .await
        .expect("eth_accounts");
    let sender = accounts[0];
    for _ in 0..7 {
        send_tx(provider, sender).await; // a block with 1 tx
        mine_blocks(provider, 1).await; // an empty block
    }
    BlockNumber::new(provider.get_block_number().await.expect("block number"))
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
#[ignore = "requires anvil on PATH"]
async fn cold_then_warm_walk() {
    let Some(anvil) = local() else {
        skip_no_anvil("cold_then_warm_walk");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("blocks.v3.db");
    let endpoint = anvil.endpoint_url();
    let provider = ProviderBuilder::new().connect_http(endpoint.clone());

    let tip = seed_blocks(&provider).await;
    assert!(tip.get() >= 14, "seeded a dense chain (tip {tip})");

    // Cold: walk genesis..=tip, populating cached_blocks via real
    // eth_getBlockByNumber batches against anvil. Starting from ZERO also
    // exercises the from-genesis (Bottom) walk over a real fetcher.
    let chain_cold = build_chain(
        &db_path,
        endpoint.clone(),
        SubscribeTransport::poll_default(),
    )
    .await;
    let store_cold = chain_cold.blocks(Finality::Latest);
    let cold = walk_blocks(&store_cold, BlockNumber::ZERO, tip).await;

    // Dense, ascending, covering genesis..=tip.
    assert_eq!(cold.len(), tip.get() as usize + 1, "one block per height");
    for (i, b) in cold.iter().enumerate() {
        assert_eq!(b.number, BlockNumber::new(i as u64), "dense + ascending");
    }
    // Parent links chain up; timestamps never go backwards.
    for pair in cold.windows(2) {
        assert_eq!(pair[1].parent_hash, pair[0].hash, "parent links chain up");
        assert!(
            pair[1].timestamp >= pair[0].timestamp,
            "timestamps monotonic"
        );
    }
    // The tx-bearing blocks are visible — the fetcher read the tx array.
    assert!(
        cold.iter().any(|b| b.tx_count.get() > 0),
        "some seeded block carries transactions"
    );
    // Gas limit is always populated; a tx block used gas.
    assert!(
        cold.iter().all(|b| b.gas_limit.get() > 0),
        "gas limit populated for every block"
    );
    drop(store_cold);
    drop(chain_cold);

    // Warm: fresh chain + store over the same DB, served entirely from
    // cached_blocks (the zero-RPC guarantee is unit-pinned in chain.rs).
    let chain_warm = build_chain(&db_path, endpoint, SubscribeTransport::poll_default()).await;
    let store_warm = chain_warm.blocks(Finality::Latest);
    let warm = walk_blocks(&store_warm, BlockNumber::ZERO, tip).await;

    assert_eq!(cold, warm, "warm walk yields identical blocks");
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

    // Mine one block first so the first observed head is a real block (not
    // genesis): following strictly-after a genesis (ZERO) anchor would also
    // surface block 0, since ZERO is the "before all blocks" sentinel.
    mine_blocks(&provider, 1).await;

    let chain = build_chain(
        &db_path,
        anvil.endpoint_url(),
        SubscribeTransport::Poll {
            interval: Duration::from_millis(50),
        },
    )
    .await;
    let store = chain.blocks(Finality::Latest);

    // Anchor at the demanded tip (the first snapshot starts the clock);
    // follow blocks strictly after it.
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
