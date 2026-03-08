//! ERC20 balance tracker — proof of concept using the full chainfold stack.
//!
//! Demonstrates: cold start, live tracking, restart recovery, balance history.
//!
//! Usage:
//!   # Top balances for the last day (auto-detects block time):
//!   cargo run --example erc20_tracker -- --rpc-url $RPC
//!
//!   # Last 20 balance changes for an account over 7 days:
//!   cargo run --example erc20_tracker -- --rpc-url $RPC --days 7 \
//!     --account 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640 --last 20
//!
//!   # Explicit block range:
//!   cargo run --example erc20_tracker -- --rpc-url $RPC --from-block 24612100 --num-blocks 50
//!
//! On restart, it recovers from the SQLite cache and resumes from where it left off.

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::{Address, FixedBytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Filter;
use chainfold::coordinator::recovery::recover;
use chainfold::cursor::EvmCursor;
use chainfold::storage::SqliteStorage;
use chainfold::store::Store;
use chainfold::Storage;
use chainfold::{
    Block, Codec, Engine, EngineConfig, Event, EventSource, ReducerCore, StateMachine,
};
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
async fn estimate_blocks_per_day(provider: &impl Provider) -> Result<f64, Box<dyn std::error::Error>> {
    let latest = provider.get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
        .await?
        .ok_or("cannot fetch latest block")?;
    let latest_number = latest.header.number;
    let latest_ts = latest.header.timestamp;

    // Sample a block 1000 blocks back (or genesis if chain is short).
    let sample_dist = latest_number.min(1000);
    let earlier = provider.get_block_by_number((latest_number - sample_dist).into())
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
// ERC20 domain types
// ============================================================

#[derive(Debug, Clone)]
struct Transfer {
    from: [u8; 20],
    to: [u8; 20],
    amount: u128,
}

type Balances = HashMap<[u8; 20], u128>;

struct Erc20Machine;

impl StateMachine for Erc20Machine {
    type State = Balances;
    type Event = Transfer;
    type Cursor = EvmCursor;

    fn apply(&self, mut state: Balances, event: &Event<EvmCursor, Transfer>) -> Balances {
        let t = &event.data;
        if t.from != [0u8; 20] {
            let bal = state.entry(t.from).or_insert(0);
            *bal = bal.saturating_sub(t.amount);
        }
        *state.entry(t.to).or_insert(0) += t.amount;
        state
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TransferSerde {
    from: Vec<u8>,
    to: Vec<u8>,
    amount: u128,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BalancesSerde(Vec<(Vec<u8>, u128)>);

struct Erc20Codec;

impl Codec for Erc20Codec {
    type Cursor = EvmCursor;
    type Event = Transfer;
    type State = Balances;

    fn encode_event(&self, event: &Transfer) -> Vec<u8> {
        serde_json::to_vec(&TransferSerde {
            from: event.from.to_vec(),
            to: event.to.to_vec(),
            amount: event.amount,
        })
        .unwrap()
    }

    fn decode_event(&self, payload: &[u8]) -> Option<Transfer> {
        let s: TransferSerde = serde_json::from_slice(payload).ok()?;
        Some(Transfer {
            from: s.from.try_into().ok()?,
            to: s.to.try_into().ok()?,
            amount: s.amount,
        })
    }

    fn encode_state(&self, state: &Balances) -> Vec<u8> {
        let entries: Vec<(Vec<u8>, u128)> =
            state.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
        serde_json::to_vec(&BalancesSerde(entries)).unwrap()
    }

    fn decode_state(&self, bytes: &[u8]) -> Option<Balances> {
        let s: BalancesSerde = serde_json::from_slice(bytes).ok()?;
        let mut map = HashMap::new();
        for (k, v) in s.0 {
            let addr: [u8; 20] = k.try_into().ok()?;
            map.insert(addr, v);
        }
        Some(map)
    }
}

// ============================================================
// Alloy-based EventSource
// ============================================================

/// Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_TOPIC: FixedBytes<32> = FixedBytes::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37,
    0x8d, 0xaa, 0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d,
    0xf5, 0x23, 0xb3, 0xef,
]);

/// Error type for the Alloy-based source.
#[derive(Debug)]
struct SourceErr(String);

impl std::fmt::Display for SourceErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SourceErr {}

impl From<String> for SourceErr {
    fn from(s: String) -> Self {
        SourceErr(s)
    }
}

struct AlloyErc20Source {
    rpc_url: String,
    token: Address,
    chunk_size: u64,
    /// Earliest block this source cares about — skips anything below.
    from_block: u64,
}

#[async_trait::async_trait]
impl EventSource for AlloyErc20Source {
    type Cursor = EvmCursor;
    type Event = Transfer;
    type Error = SourceErr;

    async fn fetch_state(&self, _at: &EvmCursor) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<EvmCursor, Transfer>>, Self::Error> {
        // Clamp to our configured floor — ignore blocks before from_block.
        let from_height = from_height.max(self.from_block);
        if from_height >= to_height {
            return Ok(Vec::new());
        }

        let url: alloy::transports::http::reqwest::Url = self.rpc_url.parse().map_err(|e| SourceErr(format!("{}", e)))?;
        let provider = ProviderBuilder::new().connect_http(url);

        // Chunk into batches to stay within RPC provider limits.
        let chunk = self.chunk_size;
        let mut logs = Vec::new();
        let mut cursor = from_height;
        let end = to_height.saturating_sub(1); // half-open -> inclusive
        while cursor <= end {
            let chunk_end = end.min(cursor + chunk - 1);
            let filter = Filter::new()
                .address(self.token)
                .event_signature(TRANSFER_TOPIC)
                .from_block(cursor)
                .to_block(chunk_end);
            let chunk_logs = provider.get_logs(&filter).await.map_err(|e| SourceErr(e.to_string()))?;
            tracing::debug!(from = cursor, to = chunk_end, events = chunk_logs.len(), "eth_getLogs");
            logs.extend(chunk_logs);
            cursor = chunk_end + 1;
        }

        // Group logs by block number.
        let mut blocks_map: std::collections::BTreeMap<u64, Vec<Event<EvmCursor, Transfer>>> =
            std::collections::BTreeMap::new();

        for log in &logs {
            let block_number = log.block_number.unwrap_or(0);
            let log_index = log.log_index.unwrap_or(0) as u32;

            let topics = log.topics();
            if topics.len() < 3 {
                continue;
            }
            let from_bytes: [u8; 20] = topics[1].as_slice()[12..32]
                .try_into()
                .map_err(|_| SourceErr("bad from topic".into()))?;
            let to_bytes: [u8; 20] = topics[2].as_slice()[12..32]
                .try_into()
                .map_err(|_| SourceErr("bad to topic".into()))?;

            let amount = if log.data().data.len() >= 32 {
                U256::from_be_slice(&log.data().data[..32]).to::<u128>()
            } else {
                0u128
            };

            blocks_map.entry(block_number).or_default().push(Event {
                cursor: EvmCursor {
                    block_number,
                    log_index,
                },
                data: Transfer {
                    from: from_bytes,
                    to: to_bytes,
                    amount,
                },
            });
        }

        // Convert to Block vec with real block hashes from RPC.
        let mut result = Vec::new();
        for (height, events) in blocks_map {
            let block = provider
                .get_block_by_number(height.into())
                .await
                .map_err(|e| SourceErr(e.to_string()))?;
            let (block_hash, parent_hash) = match block {
                Some(b) => {
                    let bh: [u8; 32] = b.header.hash.0.into();
                    let ph: [u8; 32] = b.header.parent_hash.0.into();
                    (bh, ph)
                }
                None => {
                    let mut bh = [0u8; 32];
                    bh[0..8].copy_from_slice(&height.to_le_bytes());
                    let mut ph = [0u8; 32];
                    ph[0..8].copy_from_slice(&(height.saturating_sub(1)).to_le_bytes());
                    (bh, ph)
                }
            };

            result.push(Block {
                block_hash,
                parent_hash,
                height,
                events,
            });
        }

        Ok(result)
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<EvmCursor, Transfer>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        // PoC: no live subscription, just return empty stream.
        Ok(Box::new(futures_util::stream::empty()))
    }
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
            println!("Estimated {:.0} blocks/day ({:.1}s block time)", bpd, 86400.0 / bpd);
            (from, latest)
        }
    };

    println!("ERC20 Tracker");
    println!("  RPC:        {}", args.rpc_url);
    println!("  Token:      {}", args.token);
    println!("  Range:      {} .. {} ({} blocks)", from_block, target, target - from_block + 1);
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

    // Source
    let source = AlloyErc20Source {
        rpc_url: args.rpc_url.clone(),
        token,
        chunk_size: args.chunk_size,
        from_block,
    };

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
            let account: [u8; 20] = hex::decode(account_str.strip_prefix("0x").unwrap_or(account_str))?
                .try_into()
                .map_err(|_| "account address must be 20 bytes")?;

            let chain_tip = engine.storage().get_canonical_tip("erc20-mainnet")?
                .ok_or("no canonical tip")?;
            let tip_height = engine.storage().get_block_height(&chain_tip)?.unwrap_or(0);

            // Collect per-block balance changes.
            // Use i128: when starting from an arbitrary block, we don't know the
            // initial balance so outflows before inflows produce negative values.
            let mut changes: Vec<(u64, i128)> = Vec::new();
            let mut balance: i128 = 0;
            let mut prev_balance: i128 = 0;

            let iter = chainfold::store::query::EventRangeIterator::new(
                storage.clone(),
                Erc20Codec,
                "erc20-mainnet".into(),
                from_block,
                tip_height,
                256,
            );

            let mut current_block = 0u64;
            for (cursor, transfer) in iter {
                if cursor.block_number != current_block && balance != prev_balance && current_block != 0 {
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
                println!("(showing last {} of {} changes)", display.len(), changes.len());
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
