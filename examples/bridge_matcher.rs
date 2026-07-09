//! Cross-chain `Matcher` demo over [`EvmChain`] log stores.
//!
//! Builds two [`EvmChain`]s (one per chain), opens one log store each and
//! wires them as the src/dst stores of an `AnchoredMatcher`, pairing events
//! by a caller-chosen "match key" — extracted from a topic slot or a fixed
//! slice of event data. Prints a live `matched | pending | orphans` table
//! that refreshes on each snapshot tick.
//!
//! **Bridge semantics are bridge-specific.** This example uses a *generic*
//! key extractor so you can demonstrate the matcher against any pair of
//! event streams whose pairing is "two events share value X." Real bridges
//! (OP, Arbitrum, `LayerZero`, etc.) need a custom `MatchFn` that decodes
//! their actual sequence/messageId field — extend `make_match_fn` or
//! implement `MatchFn` directly.
//!
//! Manual smoke (USDC Transfers on Ethereum vs Arbitrum, paired by
//! sender topic):
//!
//!   cargo run --features evm --example `bridge_matcher` -- \
//!     --src-rpc <https://eth.llamarpc.com> \
//!       --src-address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
//!     --dst-rpc <https://arb1.arbitrum.io/rpc> \
//!       --dst-address 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 \
//!     --match-on topic1 --num-blocks 50

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::Shared;
use chainfold::arch::Timestamp;
use chainfold::cache::{CacheDb, versioned_path};
use chainfold::chain::ChunkSize;
use chainfold::evm::{
    ChainProvider, EvmChain, EvmChainConfig, EvmEvent, EvmPosition, Finality, LogFilter,
    SubscribeTransport, TRANSFER_TOPIC, Topic, rpc_retry_layer,
};
use chainfold::matcher::{
    AnchorStore, AnchoredMatcher, Match, MatchFn, MatchInput, MatchOutput, Matcher,
    SqliteAnchorStore, match_fn,
};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;

#[derive(Parser)]
#[command(
    name = "bridge_matcher",
    about = "Pair EVM log events across two chains"
)]
struct Args {
    /// RPC URL for the source chain.
    #[arg(long)]
    src_rpc: String,
    /// Source contract address.
    #[arg(long)]
    src_address: String,
    /// Source topic0 (default = ERC20 Transfer).
    #[arg(long)]
    src_topic0: Option<String>,

    /// RPC URL for the destination chain.
    #[arg(long)]
    dst_rpc: String,
    /// Destination contract address.
    #[arg(long)]
    dst_address: String,
    /// Destination topic0 (default = ERC20 Transfer).
    #[arg(long)]
    dst_topic0: Option<String>,

    /// What to extract as the match key.
    #[arg(long, value_enum, default_value_t = MatchOn::Topic1)]
    match_on: MatchOn,

    /// Lookback in blocks per chain.
    #[arg(long, default_value = "100")]
    num_blocks: u64,

    /// DB root; each chain appends `/src` or `/dst`.
    #[arg(long, default_value = "./bridge_matcher.db")]
    db: String,

    /// Anchor cache file (separate from `LogCache` DBs).
    #[arg(long, default_value = "./bridge_matcher.db/anchors.sqlite")]
    anchor_db: String,

    /// Cache key for anchors — bumping this resets state.
    #[arg(long, default_value = "bridge-matcher-v1")]
    anchor_key: String,

    #[arg(long, default_value = "1000")]
    chunk_size: u64,

    /// Run in live mode: chains follow their heads and the matcher
    /// re-prints on every snapshot tick. Default is a single snapshot.
    #[arg(long)]
    follow: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MatchOn {
    /// First indexed topic (e.g. `from` for Transfer).
    Topic1,
    /// Second indexed topic (e.g. `to` for Transfer).
    Topic2,
    /// Third indexed topic.
    Topic3,
    /// Full data field (often `amount` for Transfer).
    Data,
}

fn extract_key(e: &EvmEvent, on: MatchOn) -> Option<Vec<u8>> {
    match on {
        MatchOn::Topic1 => e.topics.get(1).map(|t| t.as_slice().to_vec()),
        MatchOn::Topic2 => e.topics.get(2).map(|t| t.as_slice().to_vec()),
        MatchOn::Topic3 => e.topics.get(3).map(|t| t.as_slice().to_vec()),
        MatchOn::Data => Some(e.data.clone()),
    }
}

fn make_match_fn(
    on: MatchOn,
) -> Arc<dyn MatchFn<EvmEvent, EvmEvent, MatchOutput<EvmEvent, EvmEvent>, EvmPosition, EvmPosition>>
{
    match_fn(
        move |input: MatchInput<EvmEvent, EvmEvent, EvmPosition, EvmPosition>| {
            let mut dst_by_key: HashMap<Vec<u8>, Vec<EvmEvent>> = HashMap::new();
            for d in &input.dst_items {
                if let Some(k) = extract_key(&d.item, on) {
                    dst_by_key.entry(k).or_default().push(d.item.clone());
                }
            }
            let mut matched = Vec::new();
            let mut pending = Vec::new();
            for s in &input.src_items {
                let Some(key) = extract_key(&s.item, on) else {
                    pending.push(s.item.clone());
                    continue;
                };
                if let Some(bucket) = dst_by_key.get_mut(&key)
                    && !bucket.is_empty()
                {
                    let d = bucket.remove(0);
                    matched.push(Match {
                        src: s.item.clone(),
                        dst: d,
                    });
                    continue;
                }
                pending.push(s.item.clone());
            }
            let orphans: Vec<EvmEvent> = dst_by_key.into_values().flatten().collect();
            Ok(MatchOutput {
                matched,
                pending,
                orphans,
            })
        },
    )
}

async fn open_chain(
    rpc_url: &str,
    db_subdir: &str,
    chunk_size: u64,
    transport: SubscribeTransport,
) -> Result<EvmChain<impl Provider<Ethereum> + Clone + 'static + use<>>, Box<dyn std::error::Error>>
{
    let url: Url = rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    std::fs::create_dir_all(db_subdir).ok();
    let db_path = versioned_path(&Path::new(db_subdir).join("log_store.db"));
    let cache_db = CacheDb::open(&db_path)?;
    let provider = ChainProvider::discover(
        ProviderBuilder::new().connect_client(client),
        Shared::new(cache_db.logs()),
    )
    .await?;
    Ok(EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(chunk_size),
            transport,
            ..Default::default()
        },
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let src_addr: Address = args.src_address.parse()?;
    let dst_addr: Address = args.dst_address.parse()?;
    let src_topic0 = args
        .src_topic0
        .as_ref()
        .map(|s| s.parse::<Topic>())
        .transpose()?
        .unwrap_or(TRANSFER_TOPIC);
    let dst_topic0 = args
        .dst_topic0
        .as_ref()
        .map(|s| s.parse::<Topic>())
        .transpose()?
        .unwrap_or(TRANSFER_TOPIC);

    // Follow mode is fixed at chain construction; the chains stay alive for
    // the whole run so their head clocks keep driving the stores.
    let transport = SubscribeTransport::poll_default();

    println!("Opening src history ({})...", args.src_rpc);
    let src_chain = open_chain(
        &args.src_rpc,
        &format!("{}/src", args.db.trim_end_matches('/')),
        args.chunk_size,
        transport.clone(),
    )
    .await?;
    let src_store = src_chain.logs(
        &LogFilter::new(src_addr, [Some(src_topic0), None, None, None]),
        Finality::Latest,
    );
    let src_chain_id = src_chain.chain_id();
    // The first snapshot demands the head — the tip is the snapshot's end.
    let src_hist = src_store.next().await;
    let src_tip = src_hist.end().block;
    println!("  src chain_id={src_chain_id} tip_block={src_tip}");

    println!("Opening dst history ({})...", args.dst_rpc);
    let dst_chain = open_chain(
        &args.dst_rpc,
        &format!("{}/dst", args.db.trim_end_matches('/')),
        args.chunk_size,
        transport,
    )
    .await?;
    let dst_store = dst_chain.logs(
        &LogFilter::new(dst_addr, [Some(dst_topic0), None, None, None]),
        Finality::Latest,
    );
    let dst_chain_id = dst_chain.chain_id();
    let dst_hist = dst_store.next().await;
    let dst_tip = dst_hist.end().block;
    println!("  dst chain_id={dst_chain_id} tip_block={dst_tip}");

    // Pre-warm caches with a single window split so the matcher's first
    // compute_state doesn't fight a fully-cold range.
    {
        let src_from = src_tip.saturating_sub(args.num_blocks);
        let dst_from = dst_tip.saturating_sub(args.num_blocks);
        let _ = src_hist.split(EvmPosition::new(src_from.saturating_sub(1).get(), u32::MAX));
        let _ = dst_hist.split(EvmPosition::new(dst_from.saturating_sub(1).get(), u32::MAX));
    }

    // Set up anchor store + matcher.
    if let Some(parent) = std::path::Path::new(&args.anchor_db).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let anchor_store: Arc<dyn AnchorStore> = Arc::new(SqliteAnchorStore::open(&args.anchor_db)?);
    let m: Arc<Matcher<EvmEvent, EvmEvent, EvmPosition, EvmPosition>> =
        Arc::new(AnchoredMatcher::new(
            src_store,
            dst_store,
            make_match_fn(args.match_on),
            anchor_store,
            args.anchor_key.clone(),
            // start = "now - num_blocks * ~12s" as an initial fallback;
            // EVM time_pos rounds to block boundaries.
            Timestamp::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs()
                    .saturating_sub(args.num_blocks * 12),
            ),
        ));

    println!();
    println!("Matching on: {:?}", args.match_on);
    println!("Anchor key:  {}", args.anchor_key);
    println!();

    if args.follow {
        // Live mode: print on every snapshot tick.
        let mut stream = m.iter();
        let mut tick = 0u64;
        while let Some(result) = stream.next().await {
            let state = result?;
            println!("--- tick {tick} ---");
            print_state(&state.output);
            tick += 1;
        }
    } else {
        // One-shot: compute over the time window starting num_blocks back.
        let since = Timestamp::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                .saturating_sub(args.num_blocks * 12),
        );
        let state = m.base().get_history(since).await?;
        print_state(&state.output);
    }

    Ok(())
}

fn print_state(out: &MatchOutput<EvmEvent, EvmEvent>) {
    println!(
        "matched: {:>4} | pending: {:>4} | orphans: {:>4}",
        out.matched.len(),
        out.pending.len(),
        out.orphans.len(),
    );
    if !out.matched.is_empty() {
        println!("\nrecent matches (up to 10):");
        for (i, m) in out.matched.iter().take(10).enumerate() {
            let src_tx = hex::encode(&m.src.tx_hash.as_slice()[..8]);
            let dst_tx = hex::encode(&m.dst.tx_hash.as_slice()[..8]);
            println!(
                "  [{i:>3}] src=blk{:<9} tx=0x{}  ↔  dst=blk{:<9} tx=0x{}",
                m.src.block_number, src_tx, m.dst.block_number, dst_tx,
            );
        }
    }
}
