//! Integration tests for the Bitcoin block history (`BtcBlockSpec` /
//! `BitcoinChain::blocks`) — the Bitcoin siblings of `blocks_history.rs`.
//!
//! Both run against a **throwaway regtest bitcoind** (fresh datadir, cookie
//! auth) via `corepc-node`, so they are fully offline: blocks are mined
//! locally with `generatetoaddress`.
//!
//! - `cold_then_warm_walk`: the cold walk populates `cached_btc_blocks` via
//!   real batched `getblockhash`/`getblockheader` calls; the node is then
//!   **shut down** and a fresh chain over the same DB serves the same walk
//!   entirely from the durable cache — any RPC would fail loudly.
//! - `poll_follow_delivers_mined_blocks`: mining new blocks flows through
//!   the store's tip and `watch_hist` delivers them.
//!
//! Marked `#[ignore]` because they spawn bitcoind. Run with
//! `cargo test --features bitcoin --test bitcoin_blocks_history -- --ignored`.

mod common;

use std::path::Path;
use std::time::Duration;

use chainfold::Shared;
use chainfold::arch::{HistStore, watch_hist};
use chainfold::bitcoin::{
    BitcoinChain, BitcoinChainConfig, BtcAuth, BtcBlock, BtcBlockCache, BtcBlockHash, BtcRpcClient,
    ReqwestTransport, TracingTransport,
};
use chainfold::chain::{BlockNumber, BlockRange, ChunkSize, Finality};
use corepc_node::Node;
use futures_util::StreamExt;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use common::bitcoind::{local, skip_no_bitcoind};

fn build_chain(db_path: &Path, node: &Node, poll_interval: Duration) -> BitcoinChain {
    let cache = Shared::new(BtcBlockCache::open(db_path).expect("open cache"));
    let url: reqwest::Url = node.rpc_url().parse().expect("node url");
    let auth = BtcAuth::Cookie(node.params.cookie_file.clone());
    let transport = TracingTransport(
        ReqwestTransport::new(reqwest::Client::new(), url, &auth).expect("read cookie"),
    );
    BitcoinChain::new(
        BtcRpcClient::new(transport),
        cache,
        &BitcoinChainConfig {
            chunk_size: ChunkSize::new(50),
            poll_interval,
            ..BitcoinChainConfig::default()
        },
    )
}

/// Mine `n` regtest blocks to a fresh wallet address and return the tip.
fn mine_blocks(node: &Node, n: usize) -> BlockNumber {
    let addr = node.client.new_address().expect("new address");
    node.client
        .generate_to_address(n, &addr)
        .expect("generatetoaddress");
    let count = node.client.get_block_count().expect("getblockcount");
    BlockNumber::new(count.0)
}

/// Walk `(from, to]` of the blocks store into a Vec.
async fn walk_blocks(
    store: &HistStore<BtcBlock, BlockNumber>,
    from: BlockNumber,
    to: BlockNumber,
) -> Vec<BtcBlock> {
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
#[ignore = "requires bitcoind"]
async fn cold_then_warm_walk() {
    let Some(node) = local() else {
        skip_no_bitcoind("cold_then_warm_walk");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("btc_blocks.v3.db");

    let tip = mine_blocks(&node, 15);
    assert!(tip.get() >= 15, "seeded a regtest chain (tip {tip})");

    // Cold: walk genesis..=tip, populating cached_btc_blocks via real
    // batched getblockhash/getblockheader calls. Starting from ZERO also
    // exercises the from-genesis (Bottom) walk over a real fetcher.
    let chain_cold = build_chain(&db_path, &node, Duration::from_millis(50));
    let store_cold = chain_cold.blocks(Finality::Latest);
    let cold = walk_blocks(&store_cold, BlockNumber::ZERO, tip).await;

    // Dense, ascending, covering genesis..=tip.
    assert_eq!(cold.len(), tip.get() as usize + 1, "one block per height");
    for (i, b) in cold.iter().enumerate() {
        assert_eq!(b.height, BlockNumber::new(i as u64), "dense + ascending");
    }
    // Parent links chain up from a ZERO-prev genesis; every block carries
    // at least its coinbase transaction and real header facts.
    assert_eq!(cold[0].prev_hash, BtcBlockHash::ZERO, "genesis has no prev");
    for pair in cold.windows(2) {
        assert_eq!(pair[1].prev_hash, pair[0].hash, "parent links chain up");
    }
    assert!(
        cold.iter().all(|b| b.tx_count.get() >= 1),
        "every block carries its coinbase tx"
    );
    assert!(
        cold.iter().all(|b| b.time.get() > 0),
        "timestamps populated"
    );
    drop(store_cold);
    drop(chain_cold);

    // Shut the node down: the warm walk must be served entirely from the
    // durable cache — any RPC now fails loudly.
    drop(node);

    let chain_warm = build_chain_offline(&db_path);
    let warm_hist = chain_warm.blocks_hist(BlockRange::new(BlockNumber::ZERO, tip));
    let mut stream = warm_hist.iter();
    let mut warm = Vec::new();
    while let Some(item) = stream.next().await {
        warm.push(item.expect("warm iter item").item);
    }
    assert_eq!(cold, warm, "warm walk yields identical blocks, offline");
}

/// A chain whose endpoint points at nothing (the node is gone) — used to
/// prove the warm path never leaves the cache.
fn build_chain_offline(db_path: &Path) -> BitcoinChain {
    let cache = Shared::new(BtcBlockCache::open(db_path).expect("open cache"));
    let url: reqwest::Url = "http://127.0.0.1:1".parse().expect("url");
    let transport = TracingTransport(
        ReqwestTransport::new(reqwest::Client::new(), url, &BtcAuth::None).expect("no cookie io"),
    );
    BitcoinChain::new(
        BtcRpcClient::new(transport),
        cache,
        &BitcoinChainConfig::default(),
    )
}

#[tokio::test]
#[ignore = "requires bitcoind"]
async fn poll_follow_delivers_mined_blocks() {
    let Some(node) = local() else {
        skip_no_bitcoind("poll_follow_delivers_mined_blocks");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("btc_blocks_follow.v3.db");

    // Mine one block first so the first observed head is a real block (not
    // genesis): following strictly-after a genesis (ZERO) anchor would also
    // surface block 0, since ZERO is the "before all blocks" sentinel.
    mine_blocks(&node, 1);

    let chain = build_chain(&db_path, &node, Duration::from_millis(50));
    let store = chain.blocks(Finality::Latest);

    // Anchor at the demanded tip (the first snapshot starts the clock);
    // follow blocks strictly after it.
    let start = *store.next().await.end();
    assert!(
        start > BlockNumber::ZERO,
        "anchor is a real (non-genesis) block"
    );

    // Mine while following; each head advance gap-fills through the fetcher.
    mine_blocks(&node, 3);

    let mut stream = watch_hist(&store, start);
    let mut got: Vec<BlockNumber> = Vec::new();
    let collected = timeout(Duration::from_secs(10), async {
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
