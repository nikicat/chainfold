//! Cross-chain event explorer over `interleave_hist_stores`.
//!
//! Pairs a `--chain` key with `--rpc-url` and `--address` (repeated). Builds
//! an [`EvmChain`] per chain, opens one log store each, and interleaves them
//! by `EvmEvent.timestamp`. The chains' position spaces are disjoint (block
//! 100 on one chain says nothing about another), so this is the vector-clock
//! variant: anchors are keyed [`MergedPos`] clocks, and the chronological
//! order is best-effort. Prints events with a `[chain:block:idx]` prefix —
//! verifies end-to-end that the interleave primitive works against real RPC.
//!
//! Manual smoke test (USDC on Ethereum + Arbitrum):
//!
//!   cargo run --features evm --example `bridge_explorer` -- \
//!     --chain eth --rpc-url <https://eth.llamarpc.com> \
//!       --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --chain arb --rpc-url <https://arb1.arbitrum.io/rpc> \
//!       --address 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 \
//!     --num-blocks 50

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::arch::HistStore;
use chainfold::cache::{CacheDb, versioned_path};
use chainfold::chain::{ChunkSize, Finality};
use chainfold::evm::{
    ChainProvider, EvmChain, EvmChainConfig, EvmEvent, EvmPosition, HttpTransport, LogFilter,
    TRANSFER_TOPIC, Topic, rpc_retry_layer,
};
use chainfold::merge::{MergedItem, MergedPos, SourceKey, interleave_hist_stores};
use clap::Parser;
use futures_util::StreamExt;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "bridge_explorer",
    about = "Merge EVM log streams across chains"
)]
struct Args {
    /// Symbolic chain key, e.g. "eth", "arb". Repeat once per --rpc-url/--address.
    #[arg(long)]
    chain: Vec<String>,

    /// RPC URL per chain, in the same order as --chain.
    #[arg(long)]
    rpc_url: Vec<String>,

    /// Contract address per chain, in the same order as --chain.
    #[arg(long)]
    address: Vec<String>,

    /// Topic0 filter. Default = ERC20 Transfer.
    #[arg(long)]
    topic0: Option<String>,

    /// Window size (blocks back from current tip per chain).
    #[arg(long, default_value = "50")]
    num_blocks: u64,

    /// Database path; each chain appends its key as a subdirectory.
    #[arg(long, default_value = "./bridge_explorer.db")]
    db: String,

    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    /// Limit total merged events printed.
    #[arg(long, default_value = "100")]
    limit: usize,
}

fn cmp_by_time(
    a: &MergedItem<EvmEvent, EvmPosition>,
    b: &MergedItem<EvmEvent, EvmPosition>,
) -> Ordering {
    a.value.timestamp.cmp(&b.value.timestamp)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    if args.chain.is_empty()
        || args.chain.len() != args.rpc_url.len()
        || args.chain.len() != args.address.len()
    {
        return Err(format!(
            "expected matched --chain / --rpc-url / --address counts; got {}/{}/{}",
            args.chain.len(),
            args.rpc_url.len(),
            args.address.len(),
        )
        .into());
    }

    let topic0: Topic = match &args.topic0 {
        Some(s) => s.parse()?,
        None => TRANSFER_TOPIC,
    };

    // Wrap the raw --chain strings into typed source keys at the CLI seam.
    let chain_keys: Vec<SourceKey> = args.chain.iter().map(SourceKey::new).collect();

    let mut sources: BTreeMap<SourceKey, HistStore<EvmEvent, EvmPosition>> = BTreeMap::new();

    for (i, key) in chain_keys.iter().enumerate() {
        println!("Setting up '{}' from {}...", key, args.rpc_url[i]);
        let url: Url = args.rpc_url[i].parse()?;
        let transport = HttpTransport::new(reqwest::Client::new(), url);
        let is_local = transport.guess_local();
        let client = ClientBuilder::default()
            .layer(rpc_retry_layer())
            .transport(transport, is_local);
        let address: Address = args.address[i].parse()?;

        let db_dir = format!("{}/{}", args.db.trim_end_matches('/'), key);
        std::fs::create_dir_all(&db_dir).ok();
        let db_path = versioned_path(&Path::new(&db_dir).join("log_store.db"));
        let cache_db = CacheDb::open(&db_path)?;

        let provider = ChainProvider::discover(
            ProviderBuilder::new().connect_client(client),
            Arc::new(cache_db.logs()),
        )
        .await?;

        let chain = EvmChain::new(
            provider,
            EvmChainConfig {
                chunk_size: ChunkSize::new(args.chunk_size),
                ..Default::default()
            },
        );

        let topics: [Option<Topic>; 4] = [Some(topic0), None, None, None];
        let store = chain.logs(&LogFilter::new(address, topics), Finality::Latest);
        let chain_id = chain.chain_id();
        // The first snapshot demands the head (one eth_blockNumber) — the
        // tip is the snapshot's end, not a separate "head" API.
        let hist = store.next().await;
        let tip = hist.end().block;
        println!("  chain_id={chain_id} tip_block={tip}");

        // Pre-cache the window so merge doesn't fight cold fetches per chain.
        let from_block = tip.saturating_sub(args.num_blocks);
        let from_pos = EvmPosition::new(from_block.saturating_sub(1).get(), u32::MAX);
        let (_, _tail) = hist.split(from_pos);

        sources.insert(key.clone(), store);
    }

    let merged: HistStore<MergedItem<EvmEvent, EvmPosition>, MergedPos<EvmPosition>> =
        interleave_hist_stores(sources, cmp_by_time);

    // Build a starting anchor clock: each source's current tip minus
    // num_blocks, read back per key from the merged tip.
    let tip = merged.next().await.end().clone();
    let mut starts: Vec<(SourceKey, EvmPosition)> = Vec::new();
    for key in &chain_keys {
        let pos = *tip
            .get(key)
            .ok_or_else(|| format!("source {key} must contribute to tip"))?;
        let from = EvmPosition::new(pos.block.saturating_sub(args.num_blocks).get(), u32::MAX);
        starts.push((key.clone(), from));
    }
    let start = MergedPos::from_iter(starts);

    let hist = merged.next().await;
    let (_, tail) = hist.split(start);
    let mut stream = tail.iter();
    let mut count = 0;
    println!();
    println!("Merging events in chronological order:");
    while let Some(item) = stream.next().await {
        let item = item?;
        let m = &item.item;
        let topic0_hex = m.value.topics.first().map_or_else(
            || "(none)".into(),
            |t| format!("0x{}", hex::encode(&t.as_slice()[..4])),
        );
        let ts = chrono::DateTime::from_timestamp(m.value.timestamp.get() as i64, 0).map_or_else(
            || "----------T--:--:--Z".into(),
            |dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        );
        println!(
            "[{:>4}] {:<10} [{}:{:>9}:{:>3}]  topic0={}  tx=0x{}",
            count,
            ts,
            m.source_key,
            m.value.block_number,
            m.value.log_index,
            topic0_hex,
            hex::encode(&m.value.tx_hash.as_slice()[..8]),
        );
        count += 1;
        if count >= args.limit {
            println!("... limit reached ({})", args.limit);
            break;
        }
    }
    if count == 0 {
        println!("No events in window.");
    } else {
        println!("\n{count} merged events printed.");
    }

    Ok(())
}
