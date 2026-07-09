//! CLI to manually test [`EvmChain`] log-store navigation over the
//! `Hist` / `HistStore` API.
//!
//! Usage:
//!   cargo run --features evm --example `history_explorer` -- \
//!     --rpc-url <https://eth.llamarpc.com> \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --topic0 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef \
//!     --from-block 22000000 --num-blocks 10
//!
//!   # or with dates:
//!   cargo run --features evm --example `history_explorer` -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --from 2025-01-15 --to 2025-01-16

use std::path::Path;

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::Shared;
use chainfold::arch::{HistStore, HistoryError, Positioned, Timestamp, watch_hist};
use chainfold::chain::ChunkSize;
use chainfold::evm::{
    CacheDb, ChainProvider, EvmChain, EvmChainConfig, EvmEvent, EvmPosition, Finality, LogFilter,
    Topic, rpc_retry_layer, versioned_path,
};
use chrono::NaiveDate;
use clap::Parser;
use futures_util::StreamExt;

#[derive(Parser)]
#[command(
    name = "history_explorer",
    about = "Explore EVM log history via chainfold"
)]
struct Args {
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    address: String,

    #[arg(long)]
    topic0: Option<String>,

    #[arg(long)]
    from_block: Option<u64>,

    #[arg(long, default_value = "100")]
    num_blocks: u64,

    #[arg(long, value_name = "DATE")]
    from: Option<String>,

    #[arg(long, value_name = "DATE")]
    to: Option<String>,

    #[arg(long, default_value = "20")]
    limit: usize,

    #[arg(long, default_value = "./history_explorer.db")]
    db: String,

    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    #[arg(long)]
    follow: bool,

    /// Run a cold → warm → hot timing sweep over the resolved range and
    /// print wall-clock for each. Implies forward navigation; ignores
    /// --follow.
    #[arg(long)]
    measure: bool,
}

fn parse_datetime(s: &str) -> Result<Timestamp, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|dt| Timestamp::new(dt.and_utc().timestamp() as u64))
        .map_err(|e| format!("invalid date: {e}"))
}

fn print_event(item: &Positioned<EvmEvent, EvmPosition>, index: usize) {
    let e = &item.item;
    let topic0_hex = e
        .topics
        .first()
        .map_or_else(|| "(none)".into(), |t| format!("{t}"));

    println!(
        "[{:>4}] block={} log_idx={} tx=0x{}..{} topic0={}..{}",
        index,
        e.block_number,
        e.log_index,
        &hex::encode(&e.tx_hash.as_slice()[..4]),
        &hex::encode(&e.tx_hash.as_slice()[30..]),
        &topic0_hex[..topic0_hex.len().min(10)],
        &topic0_hex[topic0_hex.len().saturating_sub(6)..],
    );
}

async fn resolve_range(
    args: &Args,
    store: &HistStore<EvmEvent, EvmPosition>,
) -> Result<(EvmPosition, EvmPosition), Box<dyn std::error::Error>> {
    match (&args.from, &args.to, args.from_block) {
        (Some(from_str), Some(to_str), None) => {
            let from_ts = parse_datetime(from_str)?;
            let to_ts = parse_datetime(to_str)?;
            let start = store.time_pos(from_ts).await?;
            let end = store.time_pos(to_ts).await?;
            println!("  Date range: {from_str} → {to_str}");
            println!("  Estimated blocks: {} → {}", start.block + 1, end.block + 1);
            // time_pos returns the cut "just before block B"; for `end` we
            // want the cut "after the last log of block B", i.e. end_of_block.
            let end_inclusive = EvmPosition::new(end.block.saturating_add(1).get(), u32::MAX);
            Ok((start, end_inclusive))
        }
        (None, None, Some(from_block)) => {
            let to_block = from_block + args.num_blocks - 1;
            let start = if from_block == 0 {
                EvmPosition::earliest()
            } else {
                EvmPosition::new(from_block - 1, u32::MAX)
            };
            let end = EvmPosition::new(to_block, u32::MAX);
            Ok((start, end))
        }
        (None, None, None) => {
            let from_block = 22_000_000u64;
            let to_block = from_block + args.num_blocks - 1;
            Ok((
                EvmPosition::new(from_block - 1, u32::MAX),
                EvmPosition::new(to_block, u32::MAX),
            ))
        }
        _ => Err(
            "provide either --from + --to (dates) or --from-block (with optional --num-blocks), not both".into(),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let address: Address = args.address.parse()?;
    let topic0 = args
        .topic0
        .as_deref()
        .map(str::parse::<Topic>)
        .transpose()?;
    let topics: [Option<Topic>; 4] = [topic0, None, None, None];

    let url: Url = args.rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    let db_path = versioned_path(Path::new(&args.db));
    let cache_db = CacheDb::open(&db_path)?;
    let provider = ChainProvider::discover(
        ProviderBuilder::new().connect_client(client),
        Shared::new(cache_db.logs()),
    )
    .await?;

    println!("Building EvmChain...");
    println!("  Address:  {address:#x}");
    if let Some(t) = &args.topic0 {
        println!("  Topic0:   {t}");
    }

    // Head tracking (a 2s poll) starts lazily on the first demand for a
    // tip; --follow only changes how this example consumes the stores.
    let config = EvmChainConfig {
        chunk_size: ChunkSize::new(args.chunk_size),
        ..Default::default()
    };
    let chain = EvmChain::new(provider, config.clone());
    let filter = LogFilter::new(address, topics);
    let hist_store = chain.logs(&filter, Finality::Latest);

    let chain_id = chain.chain_id();
    let (start, end) = resolve_range(&args, &hist_store).await?;

    println!("  Chain:    {chain_id}");
    println!("  Range:    block {} .. {}", start.block, end.block);
    println!("  DB:       {}", db_path.display());
    println!();

    if args.measure {
        use std::time::Instant;

        async fn walk_window(
            store: &HistStore<EvmEvent, EvmPosition>,
            start: EvmPosition,
            end: EvmPosition,
        ) -> Result<usize, HistoryError> {
            let hist = store.next().await;
            let (_, tail) = hist.split(start);
            let (window, _) = tail.split(end);
            let mut s = window.iter();
            let mut n = 0;
            while let Some(item) = s.next().await {
                let _ = item?;
                n += 1;
            }
            Ok(n)
        }

        // Cold: against the current store (may fetch RPC if DB is empty).
        let t0 = Instant::now();
        let cold_n = walk_window(&hist_store, start, end).await?;
        let cold_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Warm: drop + reopen against the same DB. Same provider client —
        // should never reach RPC since every gap is already in scanned_ranges.
        drop(hist_store);
        drop(chain);
        let cache_db2 = CacheDb::open(&db_path)?;
        let url2: Url = args.rpc_url.parse()?;
        let client2 = ClientBuilder::default().layer(rpc_retry_layer()).http(url2);
        let provider2 = ChainProvider::discover(
            ProviderBuilder::new().connect_client(client2),
            Shared::new(cache_db2.logs()),
        )
        .await?;
        let chain2 = EvmChain::new(provider2, config);
        let hist_store2 = chain2.logs(&filter, Finality::Latest);
        let t1 = Instant::now();
        let warm_n = walk_window(&hist_store2, start, end).await?;
        let warm_ms = t1.elapsed().as_secs_f64() * 1000.0;

        // Hot: same store, second pass — should be pure BTreeMap walk +
        // get_log_by_ref lookups.
        let t2 = Instant::now();
        let hot_n = walk_window(&hist_store2, start, end).await?;
        let hot_ms = t2.elapsed().as_secs_f64() * 1000.0;

        println!(
            "--measure summary over {} blocks:",
            end.block.get() - start.block.get() + 1
        );
        println!("  cold  {cold_n:>6} events  {cold_ms:>10.2} ms  (RPC + SQLite + BTree)");
        println!("  warm  {warm_n:>6} events  {warm_ms:>10.2} ms  (SQLite -> fresh BTree)");
        println!("  hot   {hot_n:>6} events  {hot_ms:>10.2} ms  (warm BTree only)");
        println!(
            "  speedup: cold/warm={:.1}x  warm/hot={:.1}x  cold/hot={:.1}x",
            cold_ms / warm_ms,
            warm_ms / hot_ms,
            cold_ms / hot_ms
        );
        return Ok(());
    }

    if args.follow {
        // Live wiring runs from construction (`follow` in the config); the
        // chain's head clock drives the store's tip while both stay alive.
        println!("Following new events (Ctrl+C to stop)...\n");
        let mut stream = watch_hist(&hist_store, start);
        let mut count = 0;
        while let Some(item) = stream.next().await {
            let item = item?;
            print_event(&item, count);
            count += 1;
        }
        println!("\nHistory closed ({count} events).");
    } else {
        println!("Navigating forward from block {}:\n", start.block);
        let hist = hist_store.next().await;
        let (_, tail) = hist.split(start);
        let (window, _) = tail.split(end);
        let mut s = window.iter();
        let mut count = 0;
        while let Some(item) = s.next().await {
            let item = item?;
            if count >= args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            print_event(&item, count);
            count += 1;
        }
        if count == 0 {
            println!("No events found.");
        } else {
            println!("\n{count} events shown.");
        }
    }

    Ok(())
}
