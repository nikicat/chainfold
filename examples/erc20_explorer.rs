//! Dedicated ERC20 transfer explorer.
//!
//! Auto-fetches token metadata (name, symbol, decimals) from the contract.
//! Shows decoded transfers for a specific owner address. Supports multiple tokens.
//!
//! Usage:
//!   cargo run --features evm --example erc20_explorer -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --owner 0xYourAddress \
//!     --from 2025-01-15 --to 2025-01-16
//!
//!   # Multiple tokens (USDC + WETH):
//!   cargo run --features evm --example erc20_explorer -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --address 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 \
//!     --owner 0xYourAddress --from-block 22000000 --num-blocks 100

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{Cursor, History, Side};
use chainfold::evm::{
    decode_transfer, evm_pos_cmp, fetch_erc20_metadata, Erc20Metadata, EvmLogHistory, LogCache,
    TRANSFER_TOPIC,
};
use chrono::NaiveDate;
use clap::Parser;

#[derive(Parser)]
#[command(name = "erc20_explorer", about = "Explore ERC20 transfers for an owner")]
struct Args {
    /// Ethereum RPC endpoint
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    /// Token contract address(es) (0x-prefixed hex). Repeat for multiple tokens.
    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    address: Vec<String>,

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

fn explorer_base_url(chain_id: u64) -> &'static str {
    match chain_id {
        1 => "https://etherscan.io",
        10 => "https://optimistic.etherscan.io",
        56 => "https://bscscan.com",
        100 => "https://gnosisscan.io",
        137 => "https://polygonscan.com",
        8453 => "https://basescan.org",
        42161 => "https://arbiscan.io",
        43114 => "https://snowscan.xyz",
        534352 => "https://scrollscan.com",
        _ => "https://etherscan.io",
    }
}

fn osc8_link(url: &str, text: &str) -> String {
    format!("\x1b]8;;{url}\x07{text}\x1b]8;;\x07")
}

fn abbrev_addr(addr: &[u8; 20]) -> String {
    format!(
        "0x{}…{}",
        hex::encode(&addr[..3]),
        hex::encode(&addr[18..])
    )
}

fn abbrev_tx_hash(hash: &[u8; 32]) -> String {
    format!(
        "0x{}…{}",
        hex::encode(&hash[..4]),
        hex::encode(&hash[30..])
    )
}

fn linked_addr(addr: &[u8; 20], base_url: &str) -> String {
    let url = format!("{}/address/0x{}", base_url, hex::encode(addr));
    osc8_link(&url, &abbrev_addr(addr))
}

fn linked_tx_hash(hash: &[u8; 32], base_url: &str) -> String {
    let url = format!("{}/tx/0x{}", base_url, hex::encode(hash));
    osc8_link(&url, &abbrev_tx_hash(hash))
}

const AMOUNT_WHOLE_WIDTH: usize = 6;

fn format_amount(raw: U256, decimals: u8, whole_width: usize, frac_width: usize) -> String {
    let divisor = U256::from(10u64).pow(U256::from(decimals as u64));
    let whole = raw / divisor;
    let frac = raw % divisor;

    let whole_str = format!("{:>width$}", whole, width = whole_width);

    if frac_width == 0 {
        return whole_str;
    }

    let frac_str = format!("{:0>width$}", frac, width = decimals as usize);
    let trimmed = frac_str.trim_end_matches('0');
    if trimmed.is_empty() {
        // No fractional part — pad with spaces where digits would be
        format!("{}{}", whole_str, " ".repeat(frac_width + 1))
    } else {
        // Show fractional, pad trimmed portion to frac_width
        let trailing_pad = frac_width - trimmed.len();
        format!("{}.{}{}", whole_str, trimmed, " ".repeat(trailing_pad))
    }
}

fn print_transfer(
    cursor: &Cursor<EvmEvent, EvmPosition>,
    index: usize,
    owner: &[u8; 20],
    metadata: &HashMap<[u8; 20], Erc20Metadata>,
    max_sym_width: usize,
    max_decimals: usize,
    base_url: &str,
) {
    let e = cursor.value();
    if let Some(t) = decode_transfer(e) {
        let meta = match metadata.get(&e.contract_address) {
            Some(m) => m,
            None => return,
        };
        let direction = if &t.to == owner { " IN" } else { "OUT" };
        let amount = format_amount(t.amount, meta.decimals, AMOUNT_WHOLE_WIDTH, max_decimals);
        let sym = format!("{:<width$}", meta.symbol, width = max_sym_width);

        println!(
            "[{:>4}] block={:<10} {}  {:>3}  {} → {}  {} {}",
            index,
            e.block_number,
            linked_tx_hash(&e.tx_hash, base_url),
            direction,
            linked_addr(&t.from, base_url),
            linked_addr(&t.to, base_url),
            amount,
            sym,
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let addresses: Vec<[u8; 20]> = args
        .address
        .iter()
        .map(|a| parse_hex_address(a))
        .collect::<Result<_, _>>()?;
    let owner = parse_hex_address(&args.owner)?;

    let url: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
    let client = alloy::rpc::client::ClientBuilder::default()
        .layer(chainfold::evm::rpc_retry_layer())
        .http(url);
    let provider = alloy::providers::ProviderBuilder::new().connect_client(client);
    let db_path = chainfold::evm::versioned_path(&args.db);
    let cache = Arc::new(LogCache::open(&db_path)?);

    // Fetch token metadata for all addresses (cached in SQLite, Multicall3 on first run)
    let chain_id = provider.get_chain_id().await?;
    println!("Fetching token metadata...");
    let meta_futures = addresses
        .iter()
        .map(|addr| fetch_erc20_metadata(&provider, *addr, chain_id, &cache));
    let meta_results = futures_util::future::join_all(meta_futures).await;

    let mut metadata: HashMap<[u8; 20], Erc20Metadata> = HashMap::new();
    for (addr, result) in addresses.iter().zip(meta_results) {
        let meta = result?;
        println!(
            "  {} ({}) — {} decimals",
            meta.symbol, meta.name, meta.decimals
        );
        metadata.insert(*addr, meta);
    }
    let max_sym_width = metadata.values().map(|m| m.symbol.len()).max().unwrap_or(0);
    let max_decimals = metadata.values().map(|m| m.decimals as usize).max().unwrap_or(0);
    println!();

    // Build merged history: outgoing + incoming transfers for owner, per token
    let mut padded_owner = [0u8; 32];
    padded_owner[12..32].copy_from_slice(&owner);

    let mut all_histories = Vec::new();
    let mut all_backends = Vec::new();

    for &address in &addresses {
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

        all_histories.push(h_out.history());
        all_histories.push(h_in.history());
        all_backends.push(h_out);
        all_backends.push(h_in);
    }

    let history = History::merge(all_histories, evm_pos_cmp);

    let chain_id = history.source_id();
    let base_url = explorer_base_url(chain_id);

    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Tokens:   {}", addresses.len());
    println!("  Chain:    {}", chain_id);

    // Resolve range: either --from/--to (dates) or --from-block/--num-blocks
    let (start, cutoff) = resolve_range(&args, &history, chain_id)?;

    println!("  Range:    block {} .. {}", start.block_number, cutoff.block_number);
    println!("  DB:       {}", db_path);
    println!();

    if args.follow {
        println!("Following new events (Ctrl+C to stop)...\n");
        for backend in &all_backends {
            let _handle = backend.start_tip_tracking(std::time::Duration::from_secs(2));
            backend.poll_tip().await?;
        }

        let mut count = 0;
        let mut maybe_cursor = history.after(&start, None).await?;
        while let Some(cursor) = maybe_cursor {
            print_transfer(&cursor, count, &owner, &metadata, max_sym_width, max_decimals, base_url);
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
            print_transfer(&cursor, count, &owner, &metadata, max_sym_width, max_decimals, base_url);
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
            print_transfer(&cursor, count, &owner, &metadata, max_sym_width, max_decimals, base_url);
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
