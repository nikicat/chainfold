//! Phase 1 integration tests: durable `LogCache` + in-memory `LogIndex`.
//!
//! These hit a real RPC (mainnet via the anvil fork harness) so the cold
//! cache populates via actual `eth_getLogs` calls. After that we drop the
//! in-memory index, reopen, and verify the warm walk makes zero RPC calls.
//!
//! Marked `#[ignore]` because they spawn anvil + hit external RPC. Run
//! with `cargo test --test phase1_layering -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::Instant;

use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::transports::http::reqwest::Url;
use chainfold::arch::{Hist, HistBackend};
use chainfold::evm::{EvmEvent, EvmLogHistory, EvmPosition, LogCache, TRANSFER_TOPIC};
use futures_util::StreamExt;
use tempfile::TempDir;

use common::{
    anvil::{fork_mainnet, skip_no_anvil},
    fixtures::{FORK_BLOCK, FORK_SWEEP_BLOCKS, USDC},
};

async fn build_history(
    db_path: &std::path::Path,
    endpoint: Url,
    chunk_size: u64,
) -> Arc<EvmLogHistory<impl Provider<Ethereum> + Clone + 'static + use<>>> {
    let store = Arc::new(LogCache::open(db_path.to_str().unwrap()).expect("open store"));
    let provider = ProviderBuilder::new().connect_http(endpoint);
    EvmLogHistory::with_options(
        provider,
        store,
        USDC.0.0,
        [Some(TRANSFER_TOPIC), None, None, None],
        chunk_size,
        12,
    )
    .await
    .expect("EvmLogHistory::with_options")
}

async fn walk_forward<P>(history: &Arc<EvmLogHistory<P>>, from_block: u64, to_block: u64) -> usize
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let start = if from_block == 0 {
        EvmPosition::earliest()
    } else {
        EvmPosition::new(from_block - 1, u32::MAX)
    };
    let end = EvmPosition::new(to_block, u32::MAX);
    let backend: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = history.clone();
    let hist = Hist::new(backend, start, end);
    let mut stream = hist.iter();
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

    let history_cold = build_history(&db_path, endpoint.clone(), chunk_size).await;
    let cold_start = Instant::now();
    let cold_count = walk_forward(&history_cold, from, to).await;
    let cold_elapsed = cold_start.elapsed();
    assert!(
        cold_count > 0,
        "expected USDC transfers in the sweep window"
    );
    drop(history_cold);

    let history_warm = build_history(&db_path, endpoint, chunk_size).await;
    let warm_start = Instant::now();
    let warm_count = walk_forward(&history_warm, from, to).await;
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
        "phase1_layering: count={cold_count}, cold={cold_elapsed:?}, warm={warm_elapsed:?}, speedup={:.1}x",
        cold_elapsed.as_secs_f64() / warm_elapsed.as_secs_f64()
    );
}
