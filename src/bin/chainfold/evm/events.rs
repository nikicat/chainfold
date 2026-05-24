use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::Event;
use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Hist, HistBackend, HistStore, Positioned};
use chainfold::evm::{EvmLogHistory, LogStore, SubscribeTransport};
use chainfold::signatures::{resolve_event, topic_from_signature};
use futures_util::StreamExt;

use crate::cli::{EventsArgs, EvmArgs};
use crate::config::Config;
use crate::fmt::format_timestamp;
use crate::signature_cache::LogStoreSignatureCache;
use crate::util::{parse_datetime, parse_hex_address};

struct PrintOpts {
    full: bool,
    decoder: Option<Event>,
}

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
    let resolved = resolve_lookup(cache, config, chain_id, address, events_args).await?;

    let decoder = if events_args.decode {
        Some(build_decoder(&resolved)?)
    } else {
        None
    };
    let opts = PrintOpts {
        full: events_args.full,
        decoder,
    };

    let topics: [Option<[u8; 32]>; 4] = [Some(resolved.topic0), None, None, None];
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
    if let Some(sig) = &resolved.signature {
        println!("  Event:    {sig}");
    }
    println!("  Topic0:   0x{}", hex::encode(resolved.topic0));
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
            print_event(&item, count, &opts);
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
            print_event(&item, count, &opts);
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

/// Result of `--topic0` / `--event-sig` / `--event` resolution.
struct Lookup {
    /// Canonical signature, when known (always present unless the user
    /// supplied `--topic0` directly).
    signature: Option<String>,
    topic0: [u8; 32],
    /// ABI item JSON for the matched event. Only populated when `--event`
    /// was used and Sourcify/Etherscan returned an ABI.
    abi_json: Option<String>,
}

async fn resolve_lookup(
    cache: &Arc<LogStore>,
    config: &Config,
    chain_id: u64,
    address: [u8; 20],
    args: &EventsArgs,
) -> Result<Lookup, Box<dyn std::error::Error>> {
    if let Some(hex) = &args.topic0 {
        return Ok(Lookup {
            signature: None,
            topic0: parse_topic_hex(hex)?,
            abi_json: None,
        });
    }
    if let Some(sig) = &args.event_sig {
        return Ok(Lookup {
            signature: Some(sig.clone()),
            topic0: topic_from_signature(sig),
            abi_json: None,
        });
    }
    if let Some(name) = &args.event {
        let sig_cache = LogStoreSignatureCache::new(cache.clone());
        let resolved =
            resolve_event(&sig_cache, chain_id, address, name, config.etherscan_api_key()).await?;
        return Ok(Lookup {
            signature: Some(resolved.signature),
            topic0: resolved.topic0,
            abi_json: resolved.abi_json,
        });
    }
    Err("provide one of --topic0, --event-sig, or --event".into())
}

/// Build an `alloy_json_abi::Event` from a resolved lookup, for `--decode`.
fn build_decoder(lookup: &Lookup) -> Result<Event, Box<dyn std::error::Error>> {
    let json = lookup.abi_json.as_deref().ok_or(
        "--decode requires --event (the ABI item is fetched and cached by name; \
         --topic0 and --event-sig do not carry indexed/non-indexed info)",
    )?;
    let event: Event =
        serde_json::from_str(json).map_err(|e| format!("ABI event item JSON: {e}"))?;
    Ok(event)
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

fn print_event(item: &Positioned<EvmEvent, EvmPosition>, index: usize, opts: &PrintOpts) {
    let e = &item.item;
    let tx_str = if opts.full {
        format!("0x{}", hex::encode(e.tx_hash))
    } else {
        format!(
            "0x{}…{}",
            hex::encode(&e.tx_hash[..4]),
            hex::encode(&e.tx_hash[30..])
        )
    };
    let topics_str = format_topics(&e.topics, opts.full);
    let data_str = format_data(&e.data, opts.full);
    println!(
        "[{:>4}] {:<10} {}  {}{}{}",
        index,
        e.block_number,
        format_timestamp(e.timestamp),
        tx_str,
        topics_str,
        data_str,
    );
    if let Some(event) = &opts.decoder {
        print_decoded(event, e);
    }
}

fn format_topics(topics: &[[u8; 32]], full: bool) -> String {
    // topic0 is the event selector — fixed by the filter, redundant.
    let rest = topics.iter().skip(1);
    let parts: Vec<String> = if full {
        rest.map(|t| format!("0x{}", hex::encode(t))).collect()
    } else {
        rest.map(|t| format!("0x{}…{}", hex::encode(&t[..4]), hex::encode(&t[28..])))
            .collect()
    };
    if parts.is_empty() {
        String::new()
    } else {
        format!("  topics=[{}]", parts.join(", "))
    }
}

fn format_data(data: &[u8], full: bool) -> String {
    if data.is_empty() {
        return String::new();
    }
    if full {
        return format!("  data=0x{}", hex::encode(data));
    }
    let n = data.len().min(16);
    let suffix = if data.len() > n { "…" } else { "" };
    format!("  data=0x{}{}", hex::encode(&data[..n]), suffix)
}

fn print_decoded(event: &Event, log: &EvmEvent) {
    let topics: Vec<B256> = log.topics.iter().map(|t| B256::from(*t)).collect();
    let decoded = match event.decode_log_parts(topics.iter().copied(), &log.data) {
        Ok(d) => d,
        Err(err) => {
            println!("       decode error: {err}");
            return;
        }
    };
    let DecodedEvent { indexed, body, .. } = decoded;
    let mut idx_iter = indexed.into_iter();
    let mut body_iter = body.into_iter();
    for param in &event.inputs {
        let value = if param.indexed {
            idx_iter.next()
        } else {
            body_iter.next()
        };
        let Some(value) = value else { continue };
        let name = if param.name.is_empty() {
            "_".to_string()
        } else {
            param.name.clone()
        };
        let ty = param.selector_type();
        let kind = if param.indexed {
            format!("{ty} indexed")
        } else {
            ty.into_owned()
        };
        println!("       {name} ({kind}) = {}", format_value(&value));
    }
}

fn format_value(v: &DynSolValue) -> String {
    match v {
        DynSolValue::Bool(b) => b.to_string(),
        DynSolValue::Int(i, _) => i.to_string(),
        DynSolValue::Uint(u, _) => u.to_string(),
        DynSolValue::Address(a) => format!("0x{}", hex::encode(a.as_slice())),
        DynSolValue::Function(f) => format!("0x{}", hex::encode(f.as_slice())),
        DynSolValue::FixedBytes(b, n) => format!("0x{}", hex::encode(&b.as_slice()[..*n])),
        DynSolValue::Bytes(b) => format!("0x{}", hex::encode(b)),
        DynSolValue::String(s) => format!("\"{s}\""),
        DynSolValue::Array(items)
        | DynSolValue::FixedArray(items)
        | DynSolValue::Tuple(items) => {
            let parts: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", parts.join(", "))
        }
    }
}
