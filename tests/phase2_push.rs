//! Phase 2 acceptance: push-subscription path against local anvil.
//!
//! Both tests are `#[ignore]` because they require the `anvil` binary on
//! `PATH`. Run with `cargo test --test phase2_push -- --ignored`.
//!
//! Coverage:
//! - `live_event_latency_ws` — WS push transport delivers a mined block
//!   to the tip store within a tight latency budget.
//! - `poll_drain_populates_scanned_ranges` — Poll transport observes new
//!   blocks, fires `on_page`, drains to `LogCache`, and the resulting
//!   `scanned_ranges` cover the post-subscribe window.
//! - `resume_seeds_from_current_tip` — a fresh `subscribe()` after blocks
//!   were mined while disconnected seeds `latest_store` to the current
//!   tip on its first cycle, so consumers wake up at the right place.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use futures_util::StreamExt;
use tokio::time::{sleep, timeout};

use chainfold::evm::{ChainProvider, EvmLogHistory, LogCache, SubscribeTransport};

use common::anvil::{local, skip_no_anvil};

// Filter that matches no contract — push-path mechanics are exercised
// purely by new-head deliveries (empty pages). Independent of any
// contract emitting events.
const SENTINEL_TOPIC: B256 = B256::repeat_byte(0xCC);
const SENTINEL_ADDR: Address = Address::repeat_byte(0xDE);

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
    let provider = ChainProvider::discover(provider)
        .await
        .expect("discover chain id");

    let (_tmp, db_path) = temp_db();
    let store = Arc::new(LogCache::open(&db_path).expect("LogCache::open"));

    let history = EvmLogHistory::new(
        provider.clone(),
        store.clone(),
        SENTINEL_ADDR,
        [Some(SENTINEL_TOPIC), None, None, None],
    )
    .await
    .expect("EvmLogHistory::new");

    let subscriber = history
        .subscribe(SubscribeTransport::Ws)
        .await
        .expect("subscribe Ws");

    let mut latest_rx = history.latest_store().subscribe();
    // Drain the initial yield (current tip seeded by subscribe()).
    let _initial = latest_rx.next().await;

    // Give the WS subscriptions a beat to install.
    sleep(Duration::from_millis(150)).await;

    let start = Instant::now();
    mine_blocks(&provider, 1).await;

    let observed = timeout(Duration::from_secs(2), latest_rx.next())
        .await
        .expect("timeout waiting for tip advance")
        .expect("latest_rx closed");
    let latency = start.elapsed();

    assert!(
        observed.block > 0,
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

    drop(subscriber);
}

#[tokio::test]
#[ignore = "requires anvil on PATH"]
async fn poll_drain_populates_scanned_ranges() {
    let Some(anvil) = local() else {
        skip_no_anvil("poll_drain_populates_scanned_ranges");
        return;
    };

    let provider =
        ChainProvider::discover(ProviderBuilder::new().connect_http(anvil.endpoint_url()))
            .await
            .expect("discover chain id");
    let (_tmp, db_path) = temp_db();
    let store = Arc::new(LogCache::open(&db_path).expect("LogCache::open"));

    let history = EvmLogHistory::new(
        provider.clone(),
        store.clone(),
        SENTINEL_ADDR,
        [Some(SENTINEL_TOPIC), None, None, None],
    )
    .await
    .expect("EvmLogHistory::new");

    let _sub = history
        .subscribe(SubscribeTransport::Poll {
            interval: Duration::from_millis(50),
        })
        .await
        .expect("subscribe Poll");

    let tip_before = history.latest_store().get().block;

    // Mine blocks while subscribed. Each batch yields a page on the next
    // poll cycle, which the drain persists.
    mine_blocks(&provider, 5).await;

    // Wait for poll cycle + drain flush.
    sleep(Duration::from_millis(400)).await;

    let final_tip = history.latest_store().get().block;
    assert!(
        final_tip >= tip_before + 5,
        "tip advanced (before={tip_before} final={final_tip})",
    );

    let ranges = store
        .get_scanned_ranges(history.filter_hash())
        .expect("get_scanned_ranges");
    assert!(
        !ranges.is_empty(),
        "scanned_ranges populated by the push path (got 0)",
    );
    let max_end = ranges.iter().map(|(_, end)| *end).max().unwrap_or(0);
    assert!(
        max_end >= final_tip,
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

    let provider =
        ChainProvider::discover(ProviderBuilder::new().connect_http(anvil.endpoint_url()))
            .await
            .expect("discover chain id");
    let (_tmp, db_path) = temp_db();
    let store = Arc::new(LogCache::open(&db_path).expect("LogCache::open"));

    let history = EvmLogHistory::new(
        provider.clone(),
        store.clone(),
        SENTINEL_ADDR,
        [Some(SENTINEL_TOPIC), None, None, None],
    )
    .await
    .expect("EvmLogHistory::new");

    // First subscription, drop after a brief moment.
    let sub1 = history
        .subscribe(SubscribeTransport::Poll {
            interval: Duration::from_millis(50),
        })
        .await
        .expect("subscribe sub1");
    mine_blocks(&provider, 2).await;
    sleep(Duration::from_millis(200)).await;
    let tip_at_disconnect = history.latest_store().get().block;
    drop(sub1);

    // Mine while no subscriber is active.
    mine_blocks(&provider, 4).await;

    // Fresh subscription. Its initial seed should jump latest_store to
    // the current tip (NOT remain at tip_at_disconnect).
    let _sub2 = history
        .subscribe(SubscribeTransport::Poll {
            interval: Duration::from_millis(50),
        })
        .await
        .expect("subscribe sub2");

    // subscribe() already called update_tip before returning, so the seed
    // has already happened. Read the store directly.
    let seeded_tip = history.latest_store().get().block;
    assert!(
        seeded_tip >= tip_at_disconnect + 4,
        "fresh subscribe() seeds the current tip past the disconnect (seeded={seeded_tip} disconnect={tip_at_disconnect})",
    );

    // Newly mined block after resume should still flow through.
    mine_blocks(&provider, 1).await;
    sleep(Duration::from_millis(200)).await;
    let after_one_more = history.latest_store().get().block;
    assert!(
        after_one_more > seeded_tip,
        "post-resume mines still observed (after={after_one_more} seeded={seeded_tip})",
    );
}
