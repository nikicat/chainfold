//! CLI to manually test EvmLogHistory navigation.
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
use chainfold::arch::{Cursor, History, Side};
use chainfold::evm::{EvmLogHistory, LogStore};
use chrono::NaiveDate;
use clap::Parser;

#[derive(Parser)]
#[command(name = "history_explorer", about = "Explore EVM log history via chainfold")]
struct Args {
    /// Ethereum RPC endpoint
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    /// Contract address (0x-prefixed hex)
    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    address: String,

    /// Topic0 filter (event signature, 0x-prefixed hex). Omit for all events.
    #[arg(long)]
    topic0: Option<String>,

    /// Starting block (use --from/--to for date-based ranges instead)
    #[arg(long)]
    from_block: Option<u64>,

    /// Number of blocks to scan (used with --from-block)
    #[arg(long, default_value = "100")]
    num_blocks: u64,

    /// Start date, UTC (e.g. 2025-01-15 or 2025-01-15T12:00:00)
    #[arg(long, value_name = "DATE")]
    from: Option<String>,

    /// End date, UTC (e.g. 2025-01-16 or 2025-01-16T12:00:00)
    #[arg(long, value_name = "DATE")]
    to: Option<String>,

    /// Max logs to print
    #[arg(long, default_value = "20")]
    limit: usize,

    /// SQLite cache path
    #[arg(long, default_value = "./history_explorer.db")]
    db: String,

    /// Block chunk size for eth_getLogs
    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    /// Navigate backwards instead of forwards
    #[arg(long)]
    reverse: bool,

    /// Follow (block at tip for new events)
    #[arg(long)]
    follow: bool,

    /// Run a cold → warm → hot timing sweep over the resolved range and
    /// print wall-clock for each. Cold = whatever the DB currently looks
    /// like (may fetch from RPC). Warm = drop & reopen — pure SQLite into
    /// a fresh BTreeMap. Hot = walk the already-warm BTreeMap a second
    /// time. Implies forward navigation; ignores --reverse / --follow.
    #[arg(long)]
    measure: bool,
}

fn parse_hex_address(s: &str) -> Result<[u8; 20], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid address hex: {e}"))?;
    bytes.try_into().map_err(|_| "address must be 20 bytes".into())
}

fn parse_datetime(s: &str) -> Result<u64, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|dt| dt.and_utc().timestamp() as u64)
        .map_err(|e| format!("invalid date: {e}"))
}

fn parse_hex_32(s: &str) -> Result<[u8; 32], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid hex: {e}"))?;
    bytes.try_into().map_err(|_| "value must be 32 bytes".into())
}

fn print_event(cursor: &Cursor<EvmEvent, EvmPosition>, index: usize) {
    let e = cursor.value();
    let topic0_hex = e.topics.first()
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

fn resolve_range(
    args: &Args,
    history: &History<EvmEvent, EvmPosition>,
    chain_id: u64,
) -> Result<(EvmPosition, EvmPosition), String> {
    match (&args.from, &args.to, args.from_block) {
        (Some(from_str), Some(to_str), None) => {
            let from_ts = parse_datetime(from_str)?;
            let to_ts = parse_datetime(to_str)?;
            let start = history.at(from_ts);
            let end = history.at(to_ts);
            println!("  Date range: {} → {}", from_str, to_str);
            println!("  Estimated blocks: {} → {}", start.block_number, end.block_number);
            let cutoff = EvmPosition {
                chain_id, block_number: end.block_number, log_index: u32::MAX,
                block_hash: [0; 32], timestamp: to_ts, side: Side::After,
            };
            let start = EvmPosition {
                chain_id, block_number: start.block_number, log_index: 0,
                block_hash: [0; 32], timestamp: from_ts, side: Side::Before,
            };
            Ok((start, cutoff))
        }
        (None, None, Some(from_block)) => {
            let to_block = from_block + args.num_blocks - 1;
            let cutoff = EvmPosition {
                chain_id, block_number: to_block, log_index: u32::MAX,
                block_hash: [0; 32], timestamp: 0, side: Side::After,
            };
            let start = EvmPosition {
                chain_id, block_number: from_block, log_index: 0,
                block_hash: [0; 32], timestamp: 0, side: Side::Before,
            };
            Ok((start, cutoff))
        }
        (None, None, None) => {
            // Default: block 22000000 + num_blocks
            let from_block = 22000000u64;
            let to_block = from_block + args.num_blocks - 1;
            let cutoff = EvmPosition {
                chain_id, block_number: to_block, log_index: u32::MAX,
                block_hash: [0; 32], timestamp: 0, side: Side::After,
            };
            let start = EvmPosition {
                chain_id, block_number: from_block, log_index: 0,
                block_hash: [0; 32], timestamp: 0, side: Side::Before,
            };
            Ok((start, cutoff))
        }
        _ => Err("provide either --from + --to (dates) or --from-block (with optional --num-blocks), not both".into()),
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

    let backend = EvmLogHistory::with_options(
        provider, store, address, topics, args.chunk_size, 12,
    ).await?;
    let history = backend.history();

    let chain_id = history.source_id();

    // Resolve range: either --from/--to (dates) or --from-block/--num-blocks
    let (start, cutoff) = resolve_range(&args, &history, chain_id)?;

    println!("  Chain:    {}", chain_id);
    println!("  Range:    block {} .. {}",
        start.block_number, cutoff.block_number);
    println!("  DB:       {}", db_path);
    println!();

    if args.measure {
        use std::time::Instant;
        async fn walk_forward(
            history: &chainfold::arch::History<EvmEvent, EvmPosition>,
            start: &EvmPosition,
            cutoff: &EvmPosition,
        ) -> Result<usize, chainfold::arch::HistoryError> {
            let mut anchor = start.clone();
            let mut count = 0;
            while let Some(cursor) = history.after(&anchor, Some(cutoff)).await? {
                anchor = cursor.after().clone();
                count += 1;
            }
            Ok(count)
        }

        // Cold walk: against the current backend (may fetch RPC if DB is empty).
        let t0 = Instant::now();
        let cold_n = walk_forward(&history, &start, &cutoff).await?;
        let cold_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Warm walk: drop the existing index/history, reopen against the
        // same DB. Same provider client — should never reach RPC because
        // every gap is already in scanned_ranges.
        drop(history);
        drop(backend);
        let store2 = Arc::new(LogStore::open(&db_path)?);
        let url2: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
        let client2 = alloy::rpc::client::ClientBuilder::default()
            .layer(chainfold::evm::rpc_retry_layer())
            .http(url2);
        let provider2 = alloy::providers::ProviderBuilder::new().connect_client(client2);
        let backend2 = EvmLogHistory::with_options(
            provider2, store2, address, topics, args.chunk_size, 12,
        ).await?;
        let history2 = backend2.history();
        let t1 = Instant::now();
        let warm_n = walk_forward(&history2, &start, &cutoff).await?;
        let warm_ms = t1.elapsed().as_secs_f64() * 1000.0;

        // Hot walk: same history2, second pass over the same range.
        // Should be pure BTreeMap traversal + LogStore::get_log_by_ref lookups.
        let t2 = Instant::now();
        let hot_n = walk_forward(&history2, &start, &cutoff).await?;
        let hot_ms = t2.elapsed().as_secs_f64() * 1000.0;

        println!("--measure summary over {} blocks:",
            cutoff.block_number - start.block_number + 1);
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
        let _handle = backend.start_tip_tracking(std::time::Duration::from_secs(2));
        backend.poll_tip().await?;

        let mut count = 0;
        let mut maybe_cursor = history.after(&start, None).await?;
        while let Some(cursor) = maybe_cursor {
            print_event(&cursor, count);
            count += 1;
            maybe_cursor = history.after(cursor.after(), None).await?;
        }
        println!("\nHistory closed ({count} events).");
    } else if args.reverse {
        println!("Navigating backwards from block {}:\n", cutoff.block_number);
        let mut maybe_cursor = history.before(&cutoff, Some(&start)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            print_event(&cursor, count);
            count += 1;
            maybe_cursor = history.before(cursor.before(), Some(&start)).await?;
        }
        if count == 0 { println!("No events found."); }
        else { println!("\n{count} events shown."); }
    } else {
        println!("Navigating forward from block {}:\n", start.block_number);
        let mut maybe_cursor = history.after(&start, Some(&cutoff)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            print_event(&cursor, count);
            count += 1;
            maybe_cursor = history.after(cursor.after(), Some(&cutoff)).await?;
        }
        if count == 0 { println!("No events found."); }
        else { println!("\n{count} events shown."); }
    }

    Ok(())
}
