//! Phase 2 acceptance: live-follow path against local anvil.
//!
//! All tests are `#[ignore]` because they require the `anvil` binary on
//! `PATH`. Run with `cargo test --test phase2_push -- --ignored`.
//!
//! Coverage:
//! - `live_event_latency_ws` — WS follow mode delivers a mined block to
//!   the log store's tip within a tight latency budget.
//! - `poll_drain_populates_scanned_ranges` — Poll follow mode observes new
//!   blocks, gap-fills through the store's fetcher, and the resulting
//!   `scanned_ranges` cover the post-construction window.
//! - `resume_seeds_from_current_tip` — a fresh chain + store built after
//!   blocks were mined while disconnected seeds its tip from the current
//!   head, so consumers wake up at the right place.

mod common;

use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use futures_util::StreamExt;
use tokio::time::{sleep, timeout};

use chainfold::Shared;
use chainfold::evm::{
    ChainProvider, EvmChain, EvmChainConfig, Finality, LogCache, LogFilter, SubscribeTransport,
    Topic, compute_filter_hash,
};

use common::anvil::{local, skip_no_anvil};

// Filter that matches no contract — live-follow mechanics are exercised
// purely by new-head deliveries (empty pages). Independent of any
// contract emitting events.
const SENTINEL_TOPIC: Topic = Topic::new(B256::repeat_byte(0xCC));
const SENTINEL_ADDR: Address = Address::repeat_byte(0xDE);

fn sentinel_filter() -> LogFilter {
    LogFilter::new(SENTINEL_ADDR, [Some(SENTINEL_TOPIC), None, None, None])
}

fn poll_config(interval: Duration) -> EvmChainConfig {
    EvmChainConfig {
        follow: Some(SubscribeTransport::Poll { interval }),
        ..Default::default()
    }
}

fn temp_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("phase2.sqlite");
    let path_str = path.to_str().unwrap().to_string();
    (dir, path_str)
}

/// Mine `n` blocks on the local anvil. `anvil_mine` is synchronous.
async fn mine_blocks<P: Provider>(provider: &P, n: u64) {
    let _: serde_json::Value = provider
        .raw_request("anvil_mine".into(), [U256::from(n)])
        .await
        .expect("anvil_mine");
}

#[tokio::test]
#[ignore = "requires anvil on PATH"]
async fn live_event_latency_ws() {
    let Some(anvil) = local() else {
        skip_no_anvil("live_event_latency_ws");
        return;
    };

    let ws_url = anvil.ws_endpoint_url();
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.as_str()))
        .await
        .expect("ws connect");
    let (_tmp, db_path) = temp_db();
    let cache = Shared::new(LogCache::open(&db_path).expect("LogCache::open"));
    let provider = ChainProvider::discover(provider, cache)
        .await
        .expect("discover chain id");

    let chain = EvmChain::new(
        provider.clone(),
        EvmChainConfig {
            follow: Some(SubscribeTransport::Ws),
            ..Default::default()
        },
    )
    .await
    .expect("EvmChain::new");
    let store = chain.logs(&sentinel_filter(), Finality::Latest);

    let mut snapshots = store.snapshots();
    // Drain the initial snapshot (tip seeded from the construction head).
    let initial = snapshots.next().await.expect("initial snapshot");
    let initial_end = *initial.end();

    // Give the WS subscriptions a beat to install.
    sleep(Duration::from_millis(150)).await;

    let start = Instant::now();
    mine_blocks(&provider, 1).await;

    let observed = timeout(Duration::from_secs(2), async {
        loop {
            let hist = snapshots.next().await.expect("snapshots stream stays live");
            if *hist.end() > initial_end {
                break *hist.end();
            }
        }
    })
    .await
    .expect("timeout waiting for tip advance");
    let latency = start.elapsed();

    assert!(
        observed.block.get() > 0,
        "tip advanced past block 0 (got {})",
        observed.block
    );
    println!(
        "WS push latency: {} ms (tip now {})",
        latency.as_millis(),
        observed.block
    );
    // WS over localhost is typically <50 ms. Generous 500ms bound to
    // absorb CI noise; tighten later once the soak metrics are in place.
    assert!(
        latency < Duration::from_millis(500),
        "WS push latency {}ms exceeded 500ms budget",
        latency.as_millis()
    );

    // Live wiring dies with the store; the head clock with the chain.
    drop(store);
    drop(chain);
}

#[tokio::test]
#[ignore = "requires anvil on PATH"]
async fn poll_drain_populates_scanned_ranges() {
    let Some(anvil) = local() else {
        skip_no_anvil("poll_drain_populates_scanned_ranges");
        return;
    };

    let (_tmp, db_path) = temp_db();
    let cache = Shared::new(LogCache::open(&db_path).expect("LogCache::open"));
    let provider = ChainProvider::discover(
        ProviderBuilder::new().connect_http(anvil.endpoint_url()),
        cache,
    )
    .await
    .expect("discover chain id");

    let chain = EvmChain::new(provider.clone(), poll_config(Duration::from_millis(50)))
        .await
        .expect("EvmChain::new");
    let store = chain.logs(&sentinel_filter(), Finality::Latest);

    let tip_before = store.next().await.end().block;

    // Mine blocks while following. Each head advance is gap-filled through
    // the store's fetcher on the next poll cycle, which persists coverage.
    mine_blocks(&provider, 5).await;

    // Wait for poll cycle + gap-fill.
    sleep(Duration::from_millis(400)).await;

    let final_tip = store.next().await.end().block;
    assert!(
        final_tip >= tip_before + 5,
        "tip advanced (before={tip_before} final={final_tip})",
    );

    let filter = sentinel_filter();
    let ranges = chain
        .cache()
        .get_scanned_ranges(&compute_filter_hash(&filter.address, &filter.topics))
        .expect("get_scanned_ranges");
    assert!(
        !ranges.is_empty(),
        "scanned_ranges populated by the follow path (got 0)",
    );
    let max_end = ranges.iter().map(|(_, end)| *end).max().unwrap_or(0);
    assert!(
        max_end >= final_tip.get(),
        "scanned coverage at or past final tip (max_end={max_end} final={final_tip})",
    );
}

#[tokio::test]
#[ignore = "requires anvil on PATH"]
async fn resume_seeds_from_current_tip() {
    let Some(anvil) = local() else {
        skip_no_anvil("resume_seeds_from_current_tip");
        return;
    };

    let (_tmp, db_path) = temp_db();
    let cache = Shared::new(LogCache::open(&db_path).expect("LogCache::open"));
    let provider = ChainProvider::discover(
        ProviderBuilder::new().connect_http(anvil.endpoint_url()),
        cache,
    )
    .await
    .expect("discover chain id");

    // First chain + store; drop both after a brief moment ("disconnect":
    // the wiring dies with the store, the head clock with the chain).
    let chain1 = EvmChain::new(provider.clone(), poll_config(Duration::from_millis(50)))
        .await
        .expect("EvmChain::new chain1");
    let store1 = chain1.logs(&sentinel_filter(), Finality::Latest);
    mine_blocks(&provider, 2).await;
    sleep(Duration::from_millis(200)).await;
    let tip_at_disconnect = store1.next().await.end().block;
    drop(store1);
    drop(chain1);

    // Mine while nothing is following.
    mine_blocks(&provider, 4).await;

    // Fresh chain + store. Construction fetches the current head and the
    // new store seeds its tip from it (NOT from tip_at_disconnect), so
    // consumers wake up at the right place.
    let chain2 = EvmChain::new(provider.clone(), poll_config(Duration::from_millis(50)))
        .await
        .expect("EvmChain::new chain2");
    let store2 = chain2.logs(&sentinel_filter(), Finality::Latest);

    let seeded_tip = store2.next().await.end().block;
    assert!(
        seeded_tip >= tip_at_disconnect + 4,
        "fresh chain seeds the current tip past the disconnect (seeded={seeded_tip} disconnect={tip_at_disconnect})",
    );

    // Newly mined block after resume should still flow through.
    mine_blocks(&provider, 1).await;
    sleep(Duration::from_millis(200)).await;
    let after_one_more = store2.next().await.end().block;
    assert!(
        after_one_more > seeded_tip,
        "post-resume mines still observed (after={after_one_more} seeded={seeded_tip})",
    );
}
