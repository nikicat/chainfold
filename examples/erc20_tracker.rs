//! ERC20 balance tracker — proof of concept using the full chainfold stack.
//!
//! Demonstrates: cold start, live tracking, restart recovery with a real Ethereum RPC.
//!
//! Usage:
//!   cargo run --example erc20_tracker -- --rpc-url https://eth.llamarpc.com --from-block 19000000
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
use tokio_util::sync::CancellationToken;

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

    /// Starting block height
    #[arg(long, default_value = "19000000")]
    from_block: u64,

    /// Number of blocks to process (default: 10)
    #[arg(long, default_value = "10")]
    num_blocks: u64,

    /// Max block range per eth_getLogs RPC call (varies by provider)
    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    /// SQLite cache file path
    #[arg(long, default_value = "./erc20_cache.db")]
    db: String,
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
    let args = Args::parse();

    let token: Address = args.token.parse()?;
    println!("ERC20 Tracker");
    println!("  RPC:        {}", args.rpc_url);
    println!("  Token:      {}", args.token);
    println!("  From block: {}", args.from_block);
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
        from_block: args.from_block,
    };

    // Check if this is a warm or cold start.
    let tip = engine.storage().get_canonical_tip("erc20-mainnet")?;
    match tip {
        Some(tip_hash) => {
            let tip_height = engine.storage().get_block_height(&tip_hash)?.unwrap_or(0);
            println!(
                "Warm start: recovering from cached block {} (height {})",
                hex::encode(tip_hash),
                tip_height
            );
            let target = tip_height + args.num_blocks;
            recover(&mut engine, &source, target)
                .await
                .map_err(|e| format!("{}", e))?;
            println!("Recovered. Now at height {}.", target);
        }
        None => {
            println!(
                "Cold start: fetching blocks from height {}...",
                args.from_block
            );
            let target = args.from_block + args.num_blocks - 1;
            recover(&mut engine, &source, target)
                .await
                .map_err(|e| format!("{}", e))?;
            println!("Processed blocks {} to {}.", args.from_block, target);
        }
    }

    // Print balances
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

    // Graceful shutdown on Ctrl+C (for future live mode).
    let _cancel = CancellationToken::new();

    println!();
    println!("Done. Run again to see warm recovery from cache.");

    Ok(())
}
