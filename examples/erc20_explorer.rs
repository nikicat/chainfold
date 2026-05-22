//! ERC20 transfer explorer with running balance, over the new
//! `Hist` / `HistStore` API.
//!
//! Phase 3 trade-off: prior version used `merge` + `ReducedHistory` to
//! interleave IN/OUT histories and fold balance reactively. Both pieces
//! were deleted in Phase 3. This rewrite scans the full Transfer history
//! for the token contract, filters for the owner client-side, and folds
//! [`apply_transfer`] inline. Phase 5 brings the per-topic IN/OUT split
//! back via `merge_hist_stores`; Phase 6 will rebuild a proper `Reducer`.
//!
//! Multi-token support: each `--address` is processed independently and
//! its transfers print in its own section. No cross-token merge yet.
//!
//! Usage:
//!   cargo run --features evm --example erc20_explorer -- \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --owner 0xYourAddress \
//!     --from 2025-01-15 --to 2025-01-16

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Hist, HistBackend, Positioned};
use chainfold::evm::{
    apply_transfer, decode_transfer, fetch_erc20_metadata, Erc20Metadata, EvmLogHistory,
    LogStore, TRANSFER_TOPIC,
};
use chrono::NaiveDate;
use clap::Parser;
use futures_util::StreamExt;

#[derive(Parser)]
#[command(name = "erc20_explorer", about = "Explore ERC20 transfers for an owner")]
struct Args {
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    /// Token contract address(es) (0x-prefixed hex). Repeat for multiple tokens.
    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    address: Vec<String>,

    /// Owner address to track transfers for (0x-prefixed hex, required)
    #[arg(long)]
    owner: String,

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
}

fn parse_datetime(s: &str) -> Result<u64, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap()))
        .map(|dt| dt.and_utc().timestamp() as u64)
        .map_err(|e| format!("invalid date: {e}"))
}

fn parse_hex_address(s: &str) -> Result<[u8; 20], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid address hex: {e}"))?;
    bytes.try_into().map_err(|_| "address must be 20 bytes".into())
}

async fn resolve_range<P: Provider<alloy::network::Ethereum> + Clone + Send + Sync + 'static>(
    args: &Args,
    backend: &Arc<EvmLogHistory<P>>,
) -> Result<(EvmPosition, EvmPosition), Box<dyn std::error::Error>> {
    let hs = backend.hist_store();
    match (&args.from, &args.to, args.from_block) {
        (Some(from_str), Some(to_str), None) => {
            let from_ts = parse_datetime(from_str)?;
            let to_ts = parse_datetime(to_str)?;
            let start = hs.time_pos(from_ts).await?;
            let end = hs.time_pos(to_ts).await?;
            println!("  Date range: {} → {}", from_str, to_str);
            println!("  Estimated blocks: {} → {}", start.block + 1, end.block + 1);
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
            Ok((start, EvmPosition::new(to_block, u32::MAX)))
        }
        (None, None, None) => {
            let from_block = 22_000_000u64;
            let to_block = from_block + args.num_blocks - 1;
            Ok((
                EvmPosition::new(from_block - 1, u32::MAX),
                EvmPosition::new(to_block, u32::MAX),
            ))
        }
        _ => Err("provide either --from + --to or --from-block, not both".into()),
    }
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
    format!("0x{}…{}", hex::encode(&addr[..3]), hex::encode(&addr[18..]))
}

fn abbrev_tx_hash(hash: &[u8; 32]) -> String {
    format!("0x{}…{}", hex::encode(&hash[..4]), hex::encode(&hash[30..]))
}

fn linked_addr(addr: &[u8; 20], base_url: &str) -> String {
    let url = format!("{}/address/0x{}", base_url, hex::encode(addr));
    osc8_link(&url, &abbrev_addr(addr))
}

fn linked_tx_hash(hash: &[u8; 32], base_url: &str) -> String {
    let url = format!("{}/tx/0x{}", base_url, hex::encode(hash));
    osc8_link(&url, &abbrev_tx_hash(hash))
}

fn format_timestamp(ts: u64) -> String {
    chrono::DateTime::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "----------T--:--:--Z".into())
}

const AMOUNT_WHOLE_WIDTH: usize = 6;
const BALANCE_WHOLE_WIDTH: usize = 10;

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
        format!("{}{}", whole_str, " ".repeat(frac_width + 1))
    } else {
        let trailing_pad = frac_width - trimmed.len();
        format!("{}.{}{}", whole_str, trimmed, " ".repeat(trailing_pad))
    }
}

#[allow(clippy::too_many_arguments)]
fn print_transfer(
    item: &Positioned<EvmEvent, EvmPosition>,
    balance: U256,
    index: usize,
    owner: &[u8; 20],
    meta: &Erc20Metadata,
    max_sym_width: usize,
    max_decimals: usize,
    base_url: &str,
) {
    let e = &item.item;
    let Some(t) = decode_transfer(e) else {
        return;
    };
    let direction = if &t.to == owner { " IN" } else { "OUT" };
    let counterparty = if &t.to == owner { &t.from } else { &t.to };
    let amount = format_amount(t.amount, meta.decimals, AMOUNT_WHOLE_WIDTH, max_decimals);
    let balance_str = format_amount(balance, meta.decimals, BALANCE_WHOLE_WIDTH, max_decimals);
    let sym = format!("{:<width$}", meta.symbol, width = max_sym_width);

    println!(
        "[{:>4}] {:<10} {}  {}  {:>3}  {}  {} {} = {}",
        index,
        e.block_number,
        format_timestamp(e.timestamp),
        linked_tx_hash(&e.tx_hash, base_url),
        direction,
        linked_addr(counterparty, base_url),
        amount,
        sym,
        balance_str,
    );
}

fn involves_owner(event: &EvmEvent, owner: &[u8; 20]) -> bool {
    decode_transfer(event)
        .map(|t| t.from == *owner || t.to == *owner)
        .unwrap_or(false)
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
    let cache = Arc::new(LogStore::open(&db_path)?);

    let chain_id = provider.get_chain_id().await?;
    let base_url = explorer_base_url(chain_id);
    println!("Fetching token metadata...");
    let meta_futures = addresses
        .iter()
        .map(|addr| fetch_erc20_metadata(&provider, *addr, chain_id, &cache));
    let meta_results = futures_util::future::join_all(meta_futures).await;

    let mut metadata: HashMap<[u8; 20], Erc20Metadata> = HashMap::new();
    for (addr, result) in addresses.iter().zip(meta_results) {
        let meta = result?;
        println!("  {} ({}) — {} decimals", meta.symbol, meta.name, meta.decimals);
        metadata.insert(*addr, meta);
    }
    let max_sym_width = metadata.values().map(|m| m.symbol.len()).max().unwrap_or(0);
    let max_decimals = metadata.values().map(|m| m.decimals as usize).max().unwrap_or(0);
    println!();

    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Tokens:   {}", addresses.len());
    println!("  Chain:    {}", chain_id);
    println!("  DB:       {}", db_path);
    println!();

    for address in &addresses {
        let topics: [Option<[u8; 32]>; 4] = [Some(TRANSFER_TOPIC), None, None, None];
        let backend = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            *address,
            topics,
            args.chunk_size,
            12,
        )
        .await?;
        let meta = metadata.get(address).expect("metadata fetched above");

        let (start, end) = resolve_range(&args, &backend).await?;
        println!(
            "── {} transfers, block {}..{} ──",
            meta.symbol, start.block, end.block
        );

        let mut balance = U256::ZERO;

        if args.follow {
            let _subscriber = backend
                .subscribe(chainfold::evm::SubscribeTransport::Poll {
                    interval: std::time::Duration::from_secs(2),
                })
                .await?;
            let hs = backend.hist_store();
            let mut stream = watch_hist(&hs, start);
            let mut count = 0;
            while let Some(item) = stream.next().await {
                let item = item?;
                balance = apply_transfer(owner, balance, &item.item);
                if involves_owner(&item.item, &owner) {
                    print_transfer(
                        &item,
                        balance,
                        count,
                        &owner,
                        meta,
                        max_sym_width,
                        max_decimals,
                        base_url,
                    );
                    count += 1;
                }
            }
        } else {
            let backend_dyn: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = backend.clone();
            let hist = Hist::new(backend_dyn, start, end);
            let mut stream = hist.iter();
            let mut count = 0;
            while let Some(item) = stream.next().await {
                let item = item?;
                balance = apply_transfer(owner, balance, &item.item);
                if involves_owner(&item.item, &owner) {
                    if count >= args.limit {
                        println!("... (limit reached, use --limit to show more)");
                        break;
                    }
                    print_transfer(
                        &item,
                        balance,
                        count,
                        &owner,
                        meta,
                        max_sym_width,
                        max_decimals,
                        base_url,
                    );
                    count += 1;
                }
            }
            if count == 0 {
                println!("No transfers involving owner.");
            } else {
                println!("{count} transfers shown.\n");
            }
        }
    }

    Ok(())
}
