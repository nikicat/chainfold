//! CLI to manually test EvmLogHistory navigation.
//!
//! Usage:
//!   cargo run --features evm --example history_explorer -- \
//!     --rpc-url https://eth.llamarpc.com \
//!     --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --topic0 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef \
//!     --from-block 22000000 --num-blocks 10

use std::sync::Arc;

use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{Cursor, Side};
use chainfold::evm::{EvmLogHistory, LogCache};
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

    /// Starting block
    #[arg(long, default_value = "22000000")]
    from_block: u64,

    /// Number of blocks to scan
    #[arg(long, default_value = "100")]
    num_blocks: u64,

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
}

fn parse_hex_address(s: &str) -> Result<[u8; 20], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid address hex: {e}"))?;
    bytes.try_into().map_err(|_| "address must be 20 bytes".into())
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
    let cache = Arc::new(LogCache::open(&db_path)?);

    println!("Building EvmLogHistory...");
    println!("  Address:  0x{}", hex::encode(address));
    if let Some(t) = &args.topic0 {
        println!("  Topic0:   {}", t);
    }

    let backend = EvmLogHistory::with_options(
        provider, cache, address, topics, args.chunk_size, 12,
    ).await?;
    let history = backend.history();

    let chain_id = history.source_id();

    println!("  Chain:    {}", chain_id);
    println!("  Range:    {} .. {} ({} blocks)",
        args.from_block, args.from_block + args.num_blocks - 1, args.num_blocks);
    println!("  DB:       {}", db_path);
    println!();

    let to_block = args.from_block + args.num_blocks - 1;
    let cutoff = EvmPosition {
        chain_id, block_number: to_block, log_index: u32::MAX,
        block_hash: [0; 32], timestamp: 0, side: Side::After,
    };
    let start = EvmPosition {
        chain_id, block_number: args.from_block, log_index: 0,
        block_hash: [0; 32], timestamp: 0, side: Side::Before,
    };

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
        println!("Navigating backwards from block {}:\n", to_block);
        let end = EvmPosition {
            chain_id, block_number: to_block, log_index: u32::MAX,
            block_hash: [0; 32], timestamp: 0, side: Side::After,
        };
        let mut maybe_cursor = history.before(&end, Some(&start)).await?;
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
        println!("Navigating forward from block {}:\n", args.from_block);
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
