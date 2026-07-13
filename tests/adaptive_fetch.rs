//! Adaptive-fetch integration scenarios (`docs/FETCH.md` acceptance): the
//! production client stack — `HttpTransport` under the narrowed retry
//! layer under `RpcTimingLayer` — driven against the capped mock JSON-RPC
//! server in `common::rpc_server`, plus a hermetic CLI subprocess run and
//! the `#[ignore]` real-provider smokes.

mod common;

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use chainfold::arch::Positioned;
use chainfold::chain::{BlockRange, ChunkSize};
use chainfold::evm::{
    ChainId, ChainProvider, EvmChain, EvmChainConfig, EvmEvent, EvmPosition, HttpTransport,
    LogCache, LogFilter, ProviderAdaptor, ProviderKind, RpcTimingLayer, Topic, generic_adaptor,
    rpc_retry_layer,
};
use chainfold::signatures::{TRANSFER_SIGNATURE, topic_from_signature};
use futures_util::{StreamExt, TryStreamExt};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

use common::rpc_server::{ErrorStyle, LOG_ADDRESS, MockChainData, MockRpc, ProviderCaps};

/// The mock's chain id (`MockChainData::default`), typed for the lib side.
const TEST_CHAIN: ChainId = ChainId::new(31_337);

/// The one filter every scenario scans: the mock stamps [`LOG_ADDRESS`]
/// and [`TRANSFER_SIGNATURE`]'s selector on every log it generates, so
/// the topic0 here derives from that same signature.
fn filter() -> LogFilter {
    let topic0 = Topic::new(B256::from(topic_from_signature(TRANSFER_SIGNATURE)));
    LogFilter::new(
        LOG_ADDRESS.parse().expect("static address parses"),
        [Some(topic0), None, None, None],
    )
}

/// The production wire stack over the mock endpoint — the same layering
/// the CLI builds (`HttpTransport` for URL redaction, the narrowed retry
/// policy, the rpc-timing layer) — with the given adaptor seeding the
/// shared endpoint budget, over a fresh in-memory cache.
fn chain_over(
    mock: &MockRpc,
    chunk_size: ChunkSize,
    adaptor: &'static dyn ProviderAdaptor,
) -> EvmChain<impl Provider<Ethereum> + Clone + 'static> {
    let url = mock.url().parse().expect("mock url parses");
    let http = HttpTransport::new(reqwest::Client::new(), url);
    let is_local = http.guess_local();
    let client = ClientBuilder::default()
        .layer(RpcTimingLayer)
        .layer(rpc_retry_layer())
        .transport(http, is_local);
    let cache = Arc::new(LogCache::open_in_memory().expect("in-memory cache"));
    let provider = ChainProvider::new(
        ProviderBuilder::default().connect_client(client),
        TEST_CHAIN,
        cache,
    )
    .with_adaptor(adaptor);
    EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size,
            ..Default::default()
        },
    )
}

/// Walk a bounded window through the adaptive executor, collecting every
/// event; any stream error fails the test.
async fn collect_window<P>(
    chain: &EvmChain<P>,
    range: BlockRange,
) -> Vec<Positioned<EvmEvent, EvmPosition>>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    chain
        .logs_hist(&filter(), range)
        .iter()
        .try_collect()
        .await
        .expect("windowed walk succeeds")
}

/// Strictly ascending positions — the no-duplicate/no-reorder guarantee.
fn assert_ascending(items: &[Positioned<EvmEvent, EvmPosition>]) {
    for pair in items.windows(2) {
        assert!(
            pair[0].pos < pair[1].pos,
            "positions must strictly ascend: {:?} then {:?}",
            pair[0].pos,
            pair[1].pos
        );
    }
}

/// Infura-style width cap: the first probe at the ceiling is rejected with
/// a suggested window, the executor adopts it, and the backfill completes
/// in near-minimal calls without ever exceeding the cap again.
#[tokio::test]
async fn backfill_completes_under_range_cap() {
    let data = MockChainData {
        logs_at: (0..500).step_by(10).map(|b| (b, 1)).collect(),
        ..Default::default()
    };
    let expected = data.logs_in(0, 499);
    let mock = MockRpc::spawn(
        data,
        ProviderCaps {
            max_range: Some(100),
            max_logs: None,
            style: ErrorStyle::Infura,
        },
    )
    .await;

    // Infura seeds no width cap, so the working size starts at the chunk
    // ceiling (500) and the first slice covers the whole window.
    let chain = chain_over(&mock, ChunkSize::new(500), ProviderKind::Infura.adaptor());
    let items = collect_window(&chain, BlockRange::from_u64(0, 499)).await;

    assert_eq!(items.len(), expected, "every log collected exactly once");
    assert_ascending(&items);

    let ranges = mock.get_logs_ranges();
    let widths: Vec<u64> = ranges.iter().map(|(from, to)| to - from + 1).collect();
    assert!(
        widths[0] > 100,
        "the first probe runs at the ceiling: {widths:?}"
    );
    assert!(
        widths[1..].iter().all(|w| *w <= 100),
        "post-adaptation slices respect the cap: {widths:?}"
    );
    let minimum = 500usize.div_ceil(100);
    assert!(
        ranges.len() <= 2 * minimum,
        "{} eth_getLogs calls for a {minimum}-call minimum: {ranges:?}",
        ranges.len()
    );
}

/// Alchemy-style content cap: a dense cluster trips the logs-in-response
/// rejection, the suggested range narrows the working size, and the dense
/// region is re-sliced — with exact delivery (no duplicates, no losses).
#[tokio::test]
async fn backfill_completes_under_log_count_cap() {
    let mut logs_at: BTreeMap<u64, u32> = (0..300).step_by(20).map(|b| (b, 1)).collect();
    for block in 200..220 {
        logs_at.insert(block, 5);
    }
    let data = MockChainData {
        logs_at,
        ..Default::default()
    };
    let expected = data.logs_in(0, 299);
    let mock = MockRpc::spawn(
        data,
        ProviderCaps {
            max_range: None,
            max_logs: Some(25),
            style: ErrorStyle::Alchemy,
        },
    )
    .await;

    let chain = chain_over(&mock, ChunkSize::new(100), ProviderKind::Alchemy.adaptor());
    let items = collect_window(&chain, BlockRange::from_u64(0, 299)).await;

    assert_eq!(items.len(), expected, "every log collected exactly once");
    assert_ascending(&items);

    let ranges = mock.get_logs_ranges();
    assert!(
        ranges
            .iter()
            .any(|(from, to)| *from >= 200 && to - from + 1 < 100),
        "the dense region was re-sliced below the carve width: {ranges:?}"
    );
}

/// A mid-gap fatal failure surfaces after the delivered prefix; the
/// persisted prefix stands, and the retry walk fetches only the suffix —
/// in disjoint slices, never touching covered ground.
#[tokio::test]
async fn midgap_failure_retry_fetches_only_suffix() {
    let data = MockChainData {
        logs_at: (0..300).step_by(10).map(|b| (b, 1)).collect(),
        ..Default::default()
    };
    let expected = data.logs_in(0, 299);
    let mock = MockRpc::spawn(data, ProviderCaps::unrestricted(ErrorStyle::Alchemy)).await;
    // The slice holding block 150 gets Alchemy's fatal "not enabled"
    // rejection — no resize or retry helps, the stream must end.
    mock.fail_once_at(150);

    let chain = chain_over(&mock, ChunkSize::new(100), ProviderKind::Alchemy.adaptor());

    let hist = chain.logs_hist(&filter(), BlockRange::from_u64(0, 299));
    let mut stream = hist.iter();
    let mut prefix = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(item)) => prefix.push(item),
            Some(Err(e)) => break e,
            None => panic!("walk 1 must surface the planted fatal error"),
        }
    };
    drop(stream);
    assert!(
        error.to_string().contains("not enabled"),
        "the planted rejection surfaces intact: {error}"
    );
    assert_eq!(
        prefix.len(),
        10,
        "the prefix below the failed slice is delivered before the error"
    );
    assert!(
        prefix.iter().all(|p| p.item.block_number.get() < 100),
        "nothing at or above the failed slice leaks out"
    );

    let calls_before_retry = mock.get_logs_ranges().len();

    // The retry: the same window again. Blocks 0–99 answer from the
    // persisted prefix; only [100, 299] goes back to the wire.
    let items = collect_window(&chain, BlockRange::from_u64(0, 299)).await;
    assert_eq!(items.len(), expected, "the retry completes the window");
    assert_ascending(&items);

    let retry_ranges = &mock.get_logs_ranges()[calls_before_retry..];
    assert!(!retry_ranges.is_empty(), "the retry reaches the wire");
    assert!(
        retry_ranges.iter().all(|(from, _)| *from >= 100),
        "the retry never refetches the persisted prefix: {retry_ranges:?}"
    );
    let mut sorted = retry_ranges.to_vec();
    sorted.sort_unstable();
    for pair in sorted.windows(2) {
        assert!(
            pair[0].1 < pair[1].0,
            "no range is fetched twice: {sorted:?}"
        );
    }
}

/// Streaming acceptance: the first event lands while the *last* slice is
/// still being served — pages release in order, ahead of completion.
#[tokio::test]
async fn first_yield_lands_before_last_slice() {
    let data = MockChainData {
        logs_at: (0..400).step_by(10).map(|b| (b, 1)).collect(),
        ..Default::default()
    };
    let expected = data.logs_in(0, 399);
    let mock = MockRpc::spawn(data, ProviderCaps::unrestricted(ErrorStyle::GenericHttp500)).await;
    mock.hold_logs_from(300);

    let chain = chain_over(&mock, ChunkSize::new(100), generic_adaptor());
    let hist = chain.logs_hist(&filter(), BlockRange::from_u64(0, 399));
    let mut stream = hist.iter();

    // The gate over [300, 399] stays closed until release() below — so a
    // first item proves in-order release doesn't wait for the tail. The
    // timeout converts a would-be deadlock into a test failure.
    let first = timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("the first item must not wait for the held last slice")
        .expect("the stream yields")
        .expect("the first item is ok");
    assert_eq!(first.item.block_number.get(), 0);

    mock.release();
    let mut count = 1;
    while let Some(item) = stream.next().await {
        item.expect("remaining items are ok");
        count += 1;
    }
    assert_eq!(count, expected, "completion after release");
}

/// A hermetic subprocess environment: empty config/cache homes and a temp
/// db — no user config, no chainlist, no provider keys.
fn hermetic_cmd(scratch: &TempDir) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_chainfold"));
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("XDG_CONFIG_HOME", scratch.path().join("config"))
        .env("XDG_CACHE_HOME", scratch.path().join("cache"))
        .arg("--db")
        .arg(scratch.path().join("cache.db"));
    cmd
}

/// The full binary against the capped mock: a hermetic run that adapts
/// (bisection under a width cap the generic adaptor can't see), prints
/// the rows, exits 0 — and never lets the key planted in the RPC URL
/// reach either output stream, even at `-vv`.
#[tokio::test]
async fn cli_events_against_capped_mock() {
    let data = MockChainData {
        logs_at: [(110, 2), (150, 1), (175, 1)].into_iter().collect(),
        ..Default::default()
    };
    let mock = MockRpc::spawn(
        data,
        ProviderCaps {
            max_range: Some(50),
            max_logs: None,
            style: ErrorStyle::GenericHttp500,
        },
    )
    .await;

    const PLANTED_KEY: &str = "plantedsecret123";
    let rpc_url = mock.url_with_path(&format!("/v2/{PLANTED_KEY}"));
    let scratch = TempDir::new().expect("temp dir");

    let output = hermetic_cmd(&scratch)
        .args([
            "-vv",
            "evm",
            "--chain-id",
            "31337",
            "--rpc-url",
            &rpc_url,
            "--from-block",
            "100",
            "--num-blocks",
            "100",
            "events",
            "--address",
            LOG_ADDRESS,
            "--event-sig",
            TRANSFER_SIGNATURE,
        ])
        .output()
        .await
        .expect("chainfold runs");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "exit 0 expected; stderr:\n{stderr}"
    );
    assert!(
        stdout.contains("4 events shown."),
        "row count matches the mock's density map:\n{stdout}"
    );
    assert!(
        !stderr.contains(PLANTED_KEY),
        "the key must never reach stderr:\n{stderr}"
    );
    assert!(
        !stdout.contains(PLANTED_KEY),
        "the key must never reach stdout:\n{stdout}"
    );
    // The 100-block window overflows the 50-block cap; the generic
    // adaptor sees opaque 500s, so completion proves bisection landed
    // under the cap.
    let ranges = mock.get_logs_ranges();
    assert!(
        ranges.iter().any(|(from, to)| to - from < 50),
        "bisected slices under the cap: {ranges:?}"
    );
}

/// `ALCHEMY_KEY` from the env or the gitignored `.env` — the smokes skip
/// (loudly) without it.
fn alchemy_key() -> Option<String> {
    let _ = dotenvy::dotenv();
    std::env::var("ALCHEMY_KEY").ok().filter(|k| !k.is_empty())
}

/// USDC on mainnet — dense Transfer traffic at any historic block.
const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";

/// Live acceptance: with only `ALCHEMY_KEY` in the environment (no
/// `--rpc-url`), the endpoint resolves from the configured key, a real
/// historic backfill completes, and the key never reaches the output.
#[tokio::test]
#[ignore = "network + ALCHEMY_KEY: hits the real Alchemy endpoint"]
async fn configured_key_reaches_real_provider() {
    let Some(key) = alchemy_key() else {
        eprintln!("skipping: no ALCHEMY_KEY in env/.env");
        return;
    };
    let scratch = TempDir::new().expect("temp dir");

    let output = hermetic_cmd(&scratch)
        .env("ALCHEMY_KEY", &key)
        .args([
            "-v",
            "evm",
            "--chain-id",
            "1",
            "--from-block",
            "18000000",
            "--num-blocks",
            "3",
            "events",
            "--address",
            USDC,
            "--event-sig",
            TRANSFER_SIGNATURE,
        ])
        .output()
        .await
        .expect("chainfold runs");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "exit 0 expected; stderr:\n{stderr}"
    );
    assert!(
        stdout.contains("events shown."),
        "a historic USDC window always has transfers:\n{stdout}"
    );
    assert!(!stdout.contains(&key), "key leaked into stdout");
    assert!(!stderr.contains(&key), "key leaked into stderr");
}

/// Live ws acceptance: `--follow` opens the dedicated subscriptions-only
/// ws connection — the redacted "connected ws endpoint" line appears and
/// the key stays out of stderr. The child streams forever, so it is
/// killed once the line lands.
#[tokio::test]
#[ignore = "network + ALCHEMY_KEY: opens a real Alchemy ws subscription"]
async fn configured_key_follow_connects_ws() {
    let Some(key) = alchemy_key() else {
        eprintln!("skipping: no ALCHEMY_KEY in env/.env");
        return;
    };
    let scratch = TempDir::new().expect("temp dir");

    let mut child = hermetic_cmd(&scratch)
        .env("ALCHEMY_KEY", &key)
        .args([
            "-v",
            "evm",
            "--chain-id",
            "1",
            "--follow",
            "--num-blocks",
            "2",
            "blocks",
            "list",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("chainfold spawns");

    let stderr = child.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    let mut seen = Vec::new();
    let connected = timeout(Duration::from_secs(30), async {
        while let Ok(Some(line)) = lines.next_line().await {
            let hit = line.contains("connected ws endpoint for subscriptions");
            seen.push(line);
            if hit {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    child.kill().await.ok();

    assert!(
        connected,
        "expected the ws-connected line; stderr so far:\n{}",
        seen.join("\n")
    );
    assert!(
        seen.iter().all(|line| !line.contains(&key)),
        "key leaked into stderr"
    );
}
