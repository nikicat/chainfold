//! Dedicated ERC20 transfer explorer.
//!
//! Auto-fetches token metadata (name, symbol, decimals) from the contract.
//! Shows decoded transfers for a specific owner address.
//!
//! Usage:
//!   cargo run --features evm --example erc20_explorer -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --owner 0xYourAddress \
//!     --from 2025-01-15 --to 2025-01-16
//!
//!   # or with block numbers:
//!   cargo run --features evm --example erc20_explorer -- \
//!     --owner 0xYourAddress --from-block 22000000 --num-blocks 100

use std::sync::Arc;

use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{Cursor, History, Side};
use chainfold::evm::{
    decode_transfer, evm_pos_cmp, fetch_erc20_metadata, EvmLogHistory, LogCache, TRANSFER_TOPIC,
};
use chrono::NaiveDate;
use clap::Parser;

#[derive(Parser)]
#[command(name = "erc20_explorer", about = "Explore ERC20 transfers for an owner")]
struct Args {
    /// Ethereum RPC endpoint
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    /// Token contract address (0x-prefixed hex)
    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    address: String,

    /// Owner address to track transfers for (0x-prefixed hex, required)
    #[arg(long)]
    owner: String,

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

    /// Max transfers to print
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

fn parse_hex_address(s: &str) -> Result<[u8; 20], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid address hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "address must be 20 bytes".into())
}

fn format_amount(raw: U256, decimals: u8) -> String {
    let divisor = U256::from(10u64).pow(U256::from(decimals as u64));
    let whole = raw / divisor;
    let frac = raw % divisor;

    if decimals == 0 {
        return whole.to_string();
    }

    let frac_str = format!("{:0>width$}", frac, width = decimals as usize);
    let trimmed = frac_str.trim_end_matches('0');
    if trimmed.is_empty() {
        whole.to_string()
    } else {
        format!("{}.{}", whole, trimmed)
    }
}

fn abbrev_addr(addr: &[u8; 20]) -> String {
    format!(
        "0x{}..{}",
        hex::encode(&addr[..3]),
        hex::encode(&addr[18..])
    )
}

fn print_transfer(
    cursor: &Cursor<EvmEvent, EvmPosition>,
    index: usize,
    owner: &[u8; 20],
    decimals: u8,
    symbol: &str,
) {
    let e = cursor.value();
    if let Some(t) = decode_transfer(e) {
        let direction = if &t.to == owner { " IN" } else { "OUT" };
        let amount = format_amount(t.amount, decimals);

        println!(
            "[{:>4}] block={:<10} {:>3}  {} → {}  {} {}",
            index,
            e.block_number,
            direction,
            abbrev_addr(&t.from),
            abbrev_addr(&t.to),
            amount,
            symbol,
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let address = parse_hex_address(&args.address)?;
    let owner = parse_hex_address(&args.owner)?;

    let url: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
    let client = alloy::rpc::client::ClientBuilder::default()
        .layer(chainfold::evm::rpc_retry_layer())
        .http(url);
    let provider = alloy::providers::ProviderBuilder::new().connect_client(client);
    let db_path = chainfold::evm::versioned_path(&args.db);
    let cache = Arc::new(LogCache::open(&db_path)?);

    // Fetch token metadata (cached in SQLite, Multicall3 on first run)
    let chain_id = provider.get_chain_id().await?;
    println!("Fetching token metadata...");
    let meta = fetch_erc20_metadata(&provider, address, chain_id, &cache).await?;
    println!(
        "Token: {} ({}), {} decimals",
        meta.symbol, meta.name, meta.decimals
    );
    println!();

    // Build merged history: outgoing + incoming transfers for owner
    let mut padded_owner = [0u8; 32];
    padded_owner[12..32].copy_from_slice(&owner);

    // Outgoing: Transfer(from=owner, *, *)
    let topics_out: [Option<[u8; 32]>; 4] =
        [Some(TRANSFER_TOPIC), Some(padded_owner), None, None];
    let h_out = EvmLogHistory::with_options(
        provider.clone(),
        cache.clone(),
        address,
        topics_out,
        args.chunk_size,
        12,
    )
    .await?;

    // Incoming: Transfer(*, to=owner, *)
    let topics_in: [Option<[u8; 32]>; 4] =
        [Some(TRANSFER_TOPIC), None, Some(padded_owner), None];
    let h_in = EvmLogHistory::with_options(
        provider.clone(),
        cache.clone(),
        address,
        topics_in,
        args.chunk_size,
        12,
    )
    .await?;

    let history = History::merge(
        vec![h_out.history(), h_in.history()],
        evm_pos_cmp,
    );
    // Keep backends alive — they own the tip tracking state.
    let _backends = vec![h_out, h_in];

    let chain_id = history.source_id();

    println!("  Address:  0x{}", hex::encode(address));
    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Chain:    {}", chain_id);

    // Resolve range: either --from/--to (dates) or --from-block/--num-blocks
    let (start, cutoff) = resolve_range(&args, &history, chain_id)?;

    println!("  Range:    block {} .. {}", start.block_number, cutoff.block_number);
    println!("  DB:       {}", db_path);
    println!();

    if args.follow {
        println!("Following new events (Ctrl+C to stop)...\n");
        for backend in &_backends {
            let _handle = backend.start_tip_tracking(std::time::Duration::from_secs(2));
            backend.poll_tip().await?;
        }

        let mut count = 0;
        let mut maybe_cursor = history.after(&start, None).await?;
        while let Some(cursor) = maybe_cursor {
            print_transfer(&cursor, count, &owner, meta.decimals, &meta.symbol);
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
            print_transfer(&cursor, count, &owner, meta.decimals, &meta.symbol);
            count += 1;
            maybe_cursor = history.before(cursor.before(), Some(&start)).await?;
        }
        if count == 0 {
            println!("No transfers found.");
        } else {
            println!("\n{count} transfers shown.");
        }
    } else {
        println!("Navigating forward from block {}:\n", start.block_number);
        let mut maybe_cursor = history.after(&start, Some(&cutoff)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            print_transfer(&cursor, count, &owner, meta.decimals, &meta.symbol);
            count += 1;
            maybe_cursor = history.after(cursor.after(), Some(&cutoff)).await?;
        }
        if count == 0 {
            println!("No transfers found.");
        } else {
            println!("\n{count} transfers shown.");
        }
    }

    Ok(())
}
