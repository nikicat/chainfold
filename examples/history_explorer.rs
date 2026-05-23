//! CLI to manually test EvmLogHistory navigation over the new
//! `Hist` / `HistStore` API.
//!
//! Usage:
//!   cargo run --features evm --example history_explorer -- \
//!     --rpc-url https://eth.llamarpc.com \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --topic0 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef \
//!     --from-block 22000000 --num-blocks 10
//!
//!   # or with dates:
//!   cargo run --features evm --example history_explorer -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --from 2025-01-15 --to 2025-01-16

use std::sync::Arc;

use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Hist, HistStore, Positioned};
use chainfold::evm::{EvmLogHistory, LogStore};
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

fn parse_hex_address(s: &str) -> Result<[u8; 20], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid address hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "address must be 20 bytes".into())
}

fn parse_datetime(s: &str) -> Result<u64, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|dt| dt.and_utc().timestamp() as u64)
        .map_err(|e| format!("invalid date: {e}"))
}

fn parse_hex_32(s: &str) -> Result<[u8; 32], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "value must be 32 bytes".into())
}

fn print_event(item: &Positioned<EvmEvent, EvmPosition>, index: usize) {
    let e = &item.item;
    let topic0_hex = e
        .topics
        .first()
        .map(|t| format!("0x{}", hex::encode(t)))
        .unwrap_or_else(|| "(none)".into());

    println!(
        "[{:>4}] block={} log_idx={} tx=0x{}..{} topic0={}..{}",
        index,
        e.block_number,
        e.log_index,
        &hex::encode(&e.tx_hash[..4]),
        &hex::encode(&e.tx_hash[30..]),
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
            println!("  Date range: {} → {}", from_str, to_str);
            println!("  Estimated blocks: {} → {}", start.block + 1, end.block + 1);
            // time_pos returns the cut "just before block B"; for `end` we
            // want the cut "after the last log of block B", i.e. end_of_block.
            let end_inclusive = EvmPosition::new(end.block.saturating_add(1), u32::MAX);
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

    let address = parse_hex_address(&args.address)?;
    let topic0 = args.topic0.as_deref().map(parse_hex_32).transpose()?;
    let topics: [Option<[u8; 32]>; 4] = [topic0, None, None, None];

    let url: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
    let client = alloy::rpc::client::ClientBuilder::default()
        .layer(chainfold::evm::rpc_retry_layer())
        .http(url);
    let provider = alloy::providers::ProviderBuilder::new().connect_client(client);
    let db_path = chainfold::evm::versioned_path(&args.db);
    let store = Arc::new(LogStore::open(&db_path)?);

    println!("Building EvmLogHistory...");
    println!("  Address:  0x{}", hex::encode(address));
    if let Some(t) = &args.topic0 {
        println!("  Topic0:   {}", t);
    }

    let backend =
        EvmLogHistory::with_options(provider, store, address, topics, args.chunk_size, 12).await?;
    let hist_store = backend.hist_store();

    let chain_id = backend.chain_id();
    let (start, end) = resolve_range(&args, &hist_store).await?;

    println!("  Chain:    {}", chain_id);
    println!("  Range:    block {} .. {}", start.block, end.block);
    println!("  DB:       {}", db_path);
    println!();

    if args.measure {
        use std::time::Instant;

        async fn walk_window(
            backend: &Arc<
                EvmLogHistory<
                    impl alloy::providers::Provider<alloy::network::Ethereum>
                        + Clone
                        + Send
                        + Sync
                        + 'static,
                >,
            >,
            start: EvmPosition,
            end: EvmPosition,
        ) -> Result<usize, chainfold::arch::HistoryError> {
            use chainfold::arch::HistBackend;
            let backend: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = backend.clone();
            let hist = Hist::new(backend, start, end);
            let mut s = hist.iter();
            let mut n = 0;
            while let Some(item) = s.next().await {
                let _ = item?;
                n += 1;
            }
            Ok(n)
        }

        // Cold: against the current backend (may fetch RPC if DB is empty).
        let t0 = Instant::now();
        let cold_n = walk_window(&backend, start, end).await?;
        let cold_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Warm: drop + reopen against the same DB. Same provider client —
        // should never reach RPC since every gap is already in scanned_ranges.
        drop(backend);
        let store2 = Arc::new(LogStore::open(&db_path)?);
        let url2: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
        let client2 = alloy::rpc::client::ClientBuilder::default()
            .layer(chainfold::evm::rpc_retry_layer())
            .http(url2);
        let provider2 = alloy::providers::ProviderBuilder::new().connect_client(client2);
        let backend2 =
            EvmLogHistory::with_options(provider2, store2, address, topics, args.chunk_size, 12)
                .await?;
        let t1 = Instant::now();
        let warm_n = walk_window(&backend2, start, end).await?;
        let warm_ms = t1.elapsed().as_secs_f64() * 1000.0;

        // Hot: same backend2, second pass — should be pure BTreeMap walk +
        // get_log_by_ref lookups.
        let t2 = Instant::now();
        let hot_n = walk_window(&backend2, start, end).await?;
        let hot_ms = t2.elapsed().as_secs_f64() * 1000.0;

        println!(
            "--measure summary over {} blocks:",
            end.block - start.block + 1
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
        println!("Following new events (Ctrl+C to stop)...\n");
        let _subscriber = backend
            .subscribe(chainfold::evm::SubscribeTransport::Poll {
                interval: std::time::Duration::from_secs(2),
            })
            .await?;

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
        use chainfold::arch::HistBackend;
        let backend_dyn: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = backend.clone();
        let hist = Hist::new(backend_dyn, start, end);
        let mut s = hist.iter();
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
