//! Cross-chain event explorer over `merge_hist_stores`.
//!
//! Pairs a `--chain` key with `--rpc-url` and `--address` (repeated). Builds
//! an [`EvmLogHistory`] per chain, erases per-chain positions into
//! [`MergedPos`], and merges by `EvmEvent.timestamp`. Prints events in
//! interleaved chronological order with a `[chain:block:idx]` prefix —
//! verifies end-to-end that the Phase 5 merge primitive works against real
//! RPC.
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
use std::sync::Arc;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::arch::{HistStore, Position};
use chainfold::evm::{
    ChainProvider, EvmEvent, EvmLogHistory, EvmPosition, LogCache, TRANSFER_TOPIC, Topic,
    rpc_retry_layer, versioned_path,
};
use chainfold::merge::{ErasedHistStore, MergedItem, MergedPos, merge_hist_stores};
use clap::Parser;
use futures_util::StreamExt;

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

fn cmp_by_time(a: &MergedItem<EvmEvent>, b: &MergedItem<EvmEvent>) -> Ordering {
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

    let mut sources: BTreeMap<String, ErasedHistStore<EvmEvent>> = BTreeMap::new();

    for (i, key) in args.chain.iter().enumerate() {
        println!("Setting up '{}' from {}...", key, args.rpc_url[i]);
        let url: Url = args.rpc_url[i].parse()?;
        let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
        let provider =
            ChainProvider::discover(ProviderBuilder::new().connect_client(client)).await?;
        let address: Address = args.address[i].parse()?;

        let db_dir = format!("{}/{}", args.db.trim_end_matches('/'), key);
        std::fs::create_dir_all(&db_dir).ok();
        let db_path = versioned_path(&format!("{db_dir}/log_store.db"));
        let store = Arc::new(LogCache::open(&db_path)?);

        let topics: [Option<Topic>; 4] = [Some(topic0), None, None, None];
        let history = EvmLogHistory::with_options(
            provider.clone(),
            store,
            address,
            topics,
            args.chunk_size,
            12,
        )
        .await?;
        let chain_id = provider.get_chain_id().await?;
        let tip = history.latest_store().get();
        println!("  chain_id={chain_id} tip_block={}", tip.block);

        // Pre-cache the window so merge doesn't fight cold fetches per chain.
        let from_block = tip.block.saturating_sub(args.num_blocks);
        let from_pos = EvmPosition::new(from_block.saturating_sub(1).get(), u32::MAX);
        let hs = history.hist_store();
        let hist = hs.next().await;
        let (_, _tail) = hist.split(from_pos);

        let erased = ErasedHistStore::new(history.hist_store());
        sources.insert(key.clone(), erased);
    }

    let merged: HistStore<MergedItem<EvmEvent>, MergedPos> =
        merge_hist_stores(sources, cmp_by_time);

    // Build a starting anchor: each source's current_tip - num_blocks. Combine
    // into a MergedPos by collecting each source's window-start position.
    let mut start_positions = BTreeMap::new();
    for key in &args.chain {
        let tip = merged.latest().get();
        let key_bytes = tip
            .get(key)
            .expect("source must contribute to tip")
            .to_vec();
        let pos = EvmPosition::unmarshal(&key_bytes)
            .ok_or_else(|| format!("could not unmarshal position for {key}"))?;
        let from = EvmPosition::new(pos.block.saturating_sub(args.num_blocks).get(), u32::MAX);
        start_positions.insert(key.clone(), from.marshal());
    }
    let start = MergedPos::new(start_positions);

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
        let ts = chrono::DateTime::from_timestamp(m.value.timestamp as i64, 0).map_or_else(
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
            &hex::encode(&m.value.tx_hash[..8]),
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
