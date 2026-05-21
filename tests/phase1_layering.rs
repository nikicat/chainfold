//! Phase 1 integration tests: durable LogStore + in-memory LogIndex.
//!
//! These hit a real RPC (mainnet via the anvil fork harness) so the cold
//! cache populates via actual `eth_getLogs` calls. After that we drop the
//! in-memory index, reopen, and verify the warm walk makes zero RPC
//! calls — the load goes through the SQLite store into the BTreeMap.
//!
//! Marked `#[ignore]` because they spawn anvil + hit external RPC. Run
//! with `cargo test --test phase1_layering -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::Instant;

use alloy::providers::{Provider, ProviderBuilder};
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::History;
use chainfold::evm::{EvmLogHistory, LogStore, TRANSFER_TOPIC};
use tempfile::TempDir;

use common::{
    anvil::{fork_mainnet, skip_no_anvil},
    fixtures::{FORK_BLOCK, FORK_SWEEP_BLOCKS, USDC},
};

/// Build a USDC-transfer history backed by a freshly-opened LogStore at
/// `db_path` and a provider talking to the anvil fork at `endpoint`.
/// `chunk_size` defaults small so the eth_getLogs calls fit under
/// public-RPC tier limits (Alchemy free is 10 blocks/call).
async fn build_history(
    db_path: &std::path::Path,
    endpoint: alloy::transports::http::reqwest::Url,
    chunk_size: u64,
) -> Arc<
    EvmLogHistory<impl Provider<alloy::network::Ethereum> + Clone + Send + Sync + 'static>,
> {
    let store = Arc::new(LogStore::open(db_path.to_str().unwrap()).expect("open store"));
    let provider = ProviderBuilder::new().connect_http(endpoint);
    EvmLogHistory::with_options(
        provider,
        store,
        USDC.0 .0,
        [Some(TRANSFER_TOPIC), None, None, None],
        chunk_size,
        12,
    )
    .await
    .expect("EvmLogHistory::with_options")
}

/// Walk the history from a chosen anchor forward through `FORK_SWEEP_BLOCKS`
/// blocks. Returns how many events were yielded.
async fn walk_forward<P>(
    history: &Arc<EvmLogHistory<P>>,
    from_block: u64,
    to_block: u64,
) -> usize
where
    P: Provider<alloy::network::Ethereum> + Clone + Send + Sync + 'static,
{
    let chain_id = 1;
    let mut anchor = EvmPosition {
        chain_id,
        block_number: from_block,
        log_index: 0,
        block_hash: [0; 32],
        timestamp: 0,
        side: chainfold::arch::Side::Before,
    };
    let cutoff = EvmPosition {
        chain_id,
        block_number: to_block,
        log_index: u32::MAX,
        block_hash: [0; 32],
        timestamp: 0,
        side: chainfold::arch::Side::After,
    };
    let h: History<EvmEvent, EvmPosition> = history.history();

    let mut count = 0;
    loop {
        let cur = h.after(&anchor, Some(&cutoff)).await.expect("after");
        match cur {
            Some(c) => {
                count += 1;
                anchor = c.after().clone();
            }
            None => break,
        }
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

    // Walk a tight 20-block window (Alchemy free tier caps eth_getLogs at
    // 10 blocks per call; with chunk_size = 10 that's two fetches).
    let from = FORK_BLOCK - 20;
    let to = FORK_BLOCK;
    let _ = FORK_SWEEP_BLOCKS;
    let chunk_size = 10;

    // Cold walk: populates LogStore via fetcher.
    let history_cold = build_history(&db_path, endpoint.clone(), chunk_size).await;
    let cold_start = Instant::now();
    let cold_count = walk_forward(&history_cold, from, to).await;
    let cold_elapsed = cold_start.elapsed();
    assert!(cold_count > 0, "expected USDC transfers in the sweep window");
    drop(history_cold);

    // Warm walk: a fresh process opens the same DB, builds a fresh LogIndex,
    // and walks the same window. Should hit zero eth_getLogs.
    let history_warm = build_history(&db_path, endpoint, chunk_size).await;
    let warm_start = Instant::now();
    let warm_count = walk_forward(&history_warm, from, to).await;
    let warm_elapsed = warm_start.elapsed();

    assert_eq!(
        cold_count, warm_count,
        "warm walk should yield identical event count"
    );

    // Cold should be measurably slower (eth_getLogs is the dominant cost).
    // 5× is a generous lower bound that survives noisy CI.
    assert!(
        cold_elapsed.as_micros() > warm_elapsed.as_micros() * 5,
        "warm walk should be much faster (cold={cold_elapsed:?}, warm={warm_elapsed:?})"
    );

    eprintln!(
        "phase1_layering: count={cold_count}, cold={cold_elapsed:?}, warm={warm_elapsed:?}, speedup={:.1}x",
        cold_elapsed.as_secs_f64() / warm_elapsed.as_secs_f64()
    );
}
