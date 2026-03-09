//! ERC20 balance tracker — proof of concept using the full chainfold stack.
//!
//! Demonstrates: cold start, live tracking, restart recovery, balance history.
//!
//! Usage:
//!   # Top balances for the last day (auto-detects block time):
//!   cargo run --features evm --example erc20_tracker -- --rpc-url $RPC
//!
//!   # Last 20 balance changes for an account over 7 days:
//!   cargo run --features evm --example erc20_tracker -- --rpc-url $RPC --days 7 \
//!     --account 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640 --last 20
//!
//!   # Explicit block range:
//!   cargo run --features evm --example erc20_tracker -- --rpc-url $RPC --from-block 24612100 --num-blocks 50
//!
//! On restart, it recovers from the SQLite cache and resumes from where it left off.

use std::sync::Arc;

use chainfold::coordinator::recovery::recover;
use chainfold::modules::erc20::{
    Address, Balances, Erc20Codec, Erc20Filter, Erc20Machine, Erc20Source, Provider,
    ProviderBuilder,
};
use chainfold::storage::SqliteStorage;
use chainfold::store::query::EventRangeIterator;
use chainfold::store::Store;
use chainfold::Storage;
use chainfold::{Engine, EngineConfig, ReducerCore};
use clap::Parser;

// ============================================================
// CLI
// ============================================================

#[derive(Parser)]
#[command(name = "erc20_tracker", about = "ERC20 balance tracker using chainfold")]
struct Args {
    /// Ethereum RPC endpoint
    #[arg(long, default_value = "https://eth.llamarpc.com")]
    rpc_url: String,

    /// ERC20 token contract address (default: USDC on mainnet)
    #[arg(long, default_value = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")]
    token: String,

    /// Starting block height (overrides --days)
    #[arg(long)]
    from_block: Option<u64>,

    /// Number of blocks to process (used with --from-block)
    #[arg(long)]
    num_blocks: Option<u64>,

    /// Look back N days from chain tip (default: 1). Auto-detects block time.
    #[arg(long)]
    days: Option<f64>,

    /// Max block range per eth_getLogs RPC call (varies by provider)
    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    /// Show balance history for this account address (e.g. 0xabc...)
    #[arg(long)]
    account: Option<String>,

    /// Only show the last N balance changes (requires --account)
    #[arg(long)]
    last: Option<usize>,

    /// SQLite cache file path
    #[arg(long, default_value = "./erc20_cache.db")]
    db: String,
}

/// Estimate blocks-per-day by sampling two block timestamps from the chain.
async fn estimate_blocks_per_day(
    provider: &impl Provider,
) -> Result<f64, Box<dyn std::error::Error>> {
    let latest = provider
        .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
        .await?
        .ok_or("cannot fetch latest block")?;
    let latest_number = latest.header.number;
    let latest_ts = latest.header.timestamp;

    // Sample a block 1000 blocks back (or genesis if chain is short).
    let sample_dist = latest_number.min(1000);
    let earlier = provider
        .get_block_by_number((latest_number - sample_dist).into())
        .await?
        .ok_or("cannot fetch sample block")?;
    let earlier_ts = earlier.header.timestamp;

    let elapsed_secs = latest_ts.saturating_sub(earlier_ts) as f64;
    if elapsed_secs == 0.0 {
        return Ok(7200.0); // fallback
    }
    let block_time = elapsed_secs / sample_dist as f64;
    Ok(86400.0 / block_time)
}

// ============================================================
// Main
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let token: Address = args.token.parse()?;
    let token_bytes: [u8; 20] = token.0 .0;
    let url: alloy::transports::http::reqwest::Url = args.rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);

    // Resolve block range: --from-block/--num-blocks, --days, or default (1 day).
    let (from_block, target) = match (args.from_block, args.num_blocks, args.days) {
        (Some(from), num, _) => {
            // Explicit from-block: use num_blocks or fetch to chain tip.
            let latest = provider.get_block_number().await?;
            let target = match num {
                Some(n) => from + n - 1,
                None => latest,
            };
            (from, target)
        }
        (None, _, days_opt) => {
            // Days-based: estimate block time from chain, look back N days.
            let days = days_opt.unwrap_or(1.0);
            let bpd = estimate_blocks_per_day(&provider).await?;
            let lookback = (days * bpd) as u64;
            let latest = provider.get_block_number().await?;
            let from = latest.saturating_sub(lookback);
            println!(
                "Estimated {:.0} blocks/day ({:.1}s block time)",
                bpd,
                86400.0 / bpd
            );
            (from, latest)
        }
    };

    println!("ERC20 Tracker");
    println!("  RPC:        {}", args.rpc_url);
    println!("  Token:      {}", args.token);
    println!(
        "  Range:      {} .. {} ({} blocks)",
        from_block,
        target,
        target - from_block + 1
    );
    println!("  DB:         {}", args.db);
    println!();

    // Storage
    let storage = SqliteStorage::open(&args.db)?;
    let storage = Arc::new(storage);

    // Engine
    let sm = Arc::new(Erc20Machine);
    let (core, _rx) = ReducerCore::new(sm, Balances::new());
    let config = EngineConfig::new("erc20-mainnet", 10);
    let mut engine = Engine::new(core, storage.clone(), Erc20Codec, config);

    // Source — uses library Erc20Source with filter pushdown
    let source = Erc20Source::new(
        provider,
        token_bytes,
        Erc20Filter::AllTransfers,
        args.chunk_size,
        from_block,
    );

    // Check if this is a warm or cold start.
    let cached_tip = engine.storage().get_canonical_tip("erc20-mainnet")?;
    match cached_tip {
        Some(tip_hash) => {
            let tip_height = engine.storage().get_block_height(&tip_hash)?.unwrap_or(0);
            println!(
                "Warm start: recovering from cached block {} (height {})",
                hex::encode(tip_hash),
                tip_height
            );
            recover(&mut engine, &source, target)
                .await
                .map_err(|e| format!("{}", e))?;
            println!("Recovered. Now at height {}.", target);
        }
        None => {
            println!("Cold start: fetching blocks from height {}...", from_block);
            recover(&mut engine, &source, target)
                .await
                .map_err(|e| format!("{}", e))?;
            println!("Processed blocks {} to {}.", from_block, target);
        }
    }

    match &args.account {
        Some(account_str) => {
            // Balance history mode: show per-block balance for a specific account.
            let account: [u8; 20] =
                hex::decode(account_str.strip_prefix("0x").unwrap_or(account_str))?
                    .try_into()
                    .map_err(|_| "account address must be 20 bytes")?;

            let chain_tip = engine
                .storage()
                .get_canonical_tip("erc20-mainnet")?
                .ok_or("no canonical tip")?;
            let tip_height = engine.storage().get_block_height(&chain_tip)?.unwrap_or(0);

            // Collect per-block balance changes.
            // Use i128: when starting from an arbitrary block, we don't know the
            // initial balance so outflows before inflows produce negative values.
            let mut changes: Vec<(u64, i128)> = Vec::new();
            let mut balance: i128 = 0;
            let mut prev_balance: i128 = 0;

            let iter = EventRangeIterator::<_, Erc20Codec>::new(
                storage.clone(),
                Erc20Codec,
                "erc20-mainnet".into(),
                from_block,
                tip_height,
                256,
            );

            let mut current_block = 0u64;
            for (cursor, transfer) in iter {
                if cursor.block_number != current_block
                    && balance != prev_balance
                    && current_block != 0
                {
                    changes.push((current_block, balance));
                    prev_balance = balance;
                }
                current_block = cursor.block_number;

                if transfer.to == account {
                    balance += transfer.amount as i128;
                }
                if transfer.from == account {
                    balance -= transfer.amount as i128;
                }
            }
            if balance != prev_balance && current_block != 0 {
                changes.push((current_block, balance));
            }

            // Apply --last filter.
            let display: &[(u64, i128)] = match args.last {
                Some(n) if n < changes.len() => &changes[changes.len() - n..],
                _ => &changes,
            };

            println!();
            println!("Balance history for 0x{}", hex::encode(account));
            if args.last.is_some() {
                println!(
                    "(showing last {} of {} changes)",
                    display.len(),
                    changes.len()
                );
            }
            println!("{:<12} {:>24}", "Block", "Balance (delta)");
            println!("{}", "-".repeat(38));
            for (block, bal) in display {
                println!("{:<12} {:>24}", block, bal);
            }

            println!();
            println!("Net balance change: {}", balance);
        }
        None => {
            // Default mode: show top balances.
            let store = engine.reduced_store();
            let balances = store.get();
            println!();
            println!("Balances ({} addresses):", balances.len());
            let mut sorted: Vec<_> = balances.iter().collect();
            sorted.sort_by_key(|(_, v)| std::cmp::Reverse(**v));
            for (a, bal) in sorted.iter().take(20) {
                println!("  0x{}: {}", hex::encode(a), bal);
            }
            if sorted.len() > 20 {
                println!("  ... and {} more", sorted.len() - 20);
            }
            println!();
            println!("Done. Run again to see warm recovery from cache.");
        }
    }

    Ok(())
}
