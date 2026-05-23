use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Hist, HistBackend, HistStore, Positioned};
use chainfold::evm::{EvmLogHistory, LogStore, SubscribeTransport};
use chainfold::signatures::{resolve_event_topic, topic_from_signature};
use futures_util::StreamExt;

use crate::cli::{EventsArgs, EvmArgs};
use crate::config::Config;
use crate::fmt::format_timestamp;
use crate::signature_cache::LogStoreSignatureCache;
use crate::util::{parse_datetime, parse_hex_address};

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    provider: &P,
    cache: &Arc<LogStore>,
    db_path: &str,
    config: &Config,
    evm_args: &EvmArgs,
    events_args: &EventsArgs,
    chain_id: u64,
    _reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = parse_hex_address(&events_args.address)?;
    let (signature, topic0) = resolve_topic0(cache, config, chain_id, address, events_args).await?;

    let topics: [Option<[u8; 32]>; 4] = [Some(topic0), None, None, None];
    let backend = EvmLogHistory::with_options(
        provider.clone(),
        cache.clone(),
        address,
        topics,
        evm_args.chunk_size,
        12,
    )
    .await?;
    let hist_store = backend.hist_store();
    let (start, end) = resolve_range(evm_args, &hist_store).await?;

    println!("  Address:  0x{}", hex::encode(address));
    if let Some(sig) = &signature {
        println!("  Event:    {sig}");
    }
    println!("  Topic0:   0x{}", hex::encode(topic0));
    println!("  Chain:    {chain_id}");
    println!("  Range:    block {} .. {}", start.block, end.block);
    println!("  DB:       {db_path}");
    println!();

    if follow {
        println!("Following new events (Ctrl+C to stop)...\n");
        let _subscriber = backend
            .subscribe(SubscribeTransport::Poll {
                interval: Duration::from_secs(2),
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
        let backend_dyn: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = backend.clone();
        let hist = Hist::new(backend_dyn, start, end);
        let mut stream = hist.iter();
        let mut count = 0;
        while let Some(item) = stream.next().await {
            let item = item?;
            if count >= evm_args.limit {
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

/// Resolve `--topic0` / `--event-sig` / `--event` to `(signature?, topic0)`.
/// `signature` is `None` only when the user supplied `--topic0` directly.
async fn resolve_topic0(
    cache: &Arc<LogStore>,
    config: &Config,
    chain_id: u64,
    address: [u8; 20],
    args: &EventsArgs,
) -> Result<(Option<String>, [u8; 32]), Box<dyn std::error::Error>> {
    if let Some(hex) = &args.topic0 {
        return Ok((None, parse_topic_hex(hex)?));
    }
    if let Some(sig) = &args.event_sig {
        return Ok((Some(sig.clone()), topic_from_signature(sig)));
    }
    if let Some(name) = &args.event {
        let cache = LogStoreSignatureCache::new(cache.clone());
        let (sig, topic) =
            resolve_event_topic(&cache, chain_id, address, name, config.etherscan_api_key())
                .await?;
        return Ok((Some(sig), topic));
    }
    Err("provide one of --topic0, --event-sig, or --event".into())
}

fn parse_topic_hex(s: &str) -> Result<[u8; 32], String> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| format!("invalid topic0 hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "topic0 must be 32 bytes".into())
}

async fn resolve_range(
    args: &EvmArgs,
    hs: &HistStore<EvmEvent, EvmPosition>,
) -> Result<(EvmPosition, EvmPosition), Box<dyn std::error::Error>> {
    match (&args.from, &args.to, args.from_block) {
        (Some(from_str), Some(to_str), None) => {
            let from_ts = parse_datetime(from_str)?;
            let to_ts = parse_datetime(to_str)?;
            let start = hs.time_pos(from_ts).await?;
            let end = hs.time_pos(to_ts).await?;
            println!("  Date range: {from_str} → {to_str}");
            println!(
                "  Estimated blocks: {} → {}",
                start.block + 1,
                end.block + 1
            );
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

fn print_event(item: &Positioned<EvmEvent, EvmPosition>, index: usize) {
    let e = &item.item;
    let topics: Vec<String> = e
        .topics
        .iter()
        .skip(1) // topic0 is fixed by the filter
        .map(|t| format!("0x{}…{}", hex::encode(&t[..4]), hex::encode(&t[28..])))
        .collect();
    let topics_str = if topics.is_empty() {
        String::new()
    } else {
        format!("  topics=[{}]", topics.join(", "))
    };
    let data_preview = if e.data.is_empty() {
        String::new()
    } else {
        let n = e.data.len().min(16);
        let suffix = if e.data.len() > n { "…" } else { "" };
        format!("  data=0x{}{}", hex::encode(&e.data[..n]), suffix)
    };
    println!(
        "[{:>4}] {:<10} {}  0x{}…{}{}{}",
        index,
        e.block_number,
        format_timestamp(e.timestamp),
        hex::encode(&e.tx_hash[..4]),
        hex::encode(&e.tx_hash[30..]),
        topics_str,
        data_preview,
    );
}
