use std::path::Path;

use alloy::dyn_abi::{DecodedEvent, DynSolValue, EventExt};
use alloy::json_abi::Event;
use alloy::network::Ethereum;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use chainfold::arch::{Positioned, watch_hist};
use chainfold::cache::CacheDb;
use chainfold::chain::Finality;
use chainfold::evm::{ChainId, EvmChain, EvmEvent, EvmPosition, LogFilter, Topic};
use chainfold::signatures::{resolve_event, topic_from_signature};
use futures_util::StreamExt;

use crate::cli::{Direction, EventsArgs, EvmArgs, FollowMode};
use crate::config::Config;
use crate::fmt::format_timestamp;
use crate::signature_cache::SqliteSignatureCache;
use crate::util::{log_cut, parse_hex_address, resolve_log_range};

/// How much of a hex value to render — driven by the `--full` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HexDetail {
    /// Every byte.
    Full,
    /// Abbreviated `0xdead…beef` head/tail.
    Abbreviated,
}

impl HexDetail {
    /// From the `--full` flag: `true` → [`Self::Full`].
    fn from_full(full: bool) -> Self {
        if full { Self::Full } else { Self::Abbreviated }
    }

    fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

struct PrintOpts {
    detail: HexDetail,
    decoder: Option<Event>,
}

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    cache: &CacheDb,
    db_path: &Path,
    config: &Config,
    evm_args: &EvmArgs,
    events_args: &EventsArgs,
    _direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = parse_hex_address(&events_args.address)?;
    let chain_id = chain.chain_id();
    let resolved = resolve_lookup(cache, config, chain_id, address, events_args).await?;

    let decoder = if events_args.decode {
        Some(build_decoder(&resolved)?)
    } else {
        None
    };
    let opts = PrintOpts {
        detail: HexDetail::from_full(events_args.full),
        decoder,
    };

    let topics: [Option<Topic>; 4] = [Some(Topic::from(resolved.topic0)), None, None, None];
    let filter = LogFilter::new(address, topics);
    let range = resolve_log_range(chain, &evm_args.window).await?;

    println!("  Address:  {address:#x}");
    if let Some(sig) = &resolved.signature {
        println!("  Event:    {sig}");
    }
    println!("  Topic0:   {:#x}", resolved.topic0);
    println!("  Chain:    {chain_id}");
    println!("  Range:    block {range}");
    println!("  DB:       {}", db_path.display());
    println!();

    if follow.is_follow() {
        // Awaiting the stream demand-starts the head clock; consume items
        // past the window's start.
        println!("Following new events (Ctrl+C to stop)...\n");
        let store = chain.logs(&filter, Finality::Latest);
        let mut stream = watch_hist(&store, log_cut(range.from));
        let mut count = 0;
        while let Some(item) = stream.next().await {
            let item = item?;
            print_event(&item, count, &opts);
            count += 1;
        }
        println!("\nHistory closed ({count} events).");
    } else {
        // A bounded hist over exactly the resolved window — head-free,
        // fully offline when the cache is warm.
        let hist = chain.logs_hist(&filter, range);
        let mut stream = hist.iter();
        let mut count = 0;
        while let Some(item) = stream.next().await {
            let item = item?;
            if count >= evm_args.window.limit {
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
    topic0: B256,
    /// ABI item JSON for the matched event. Only populated when `--event`
    /// was used and Sourcify/Etherscan returned an ABI.
    abi_json: Option<String>,
}

async fn resolve_lookup(
    cache: &CacheDb,
    config: &Config,
    chain_id: ChainId,
    address: Address,
    args: &EventsArgs,
) -> Result<Lookup, Box<dyn std::error::Error>> {
    if let Some(hex) = &args.topic0 {
        return Ok(Lookup {
            signature: None,
            topic0: hex
                .parse()
                .map_err(|e| format!("invalid topic0 hex: {e}"))?,
            abi_json: None,
        });
    }
    if let Some(sig) = &args.event_sig {
        return Ok(Lookup {
            signature: Some(sig.clone()),
            topic0: B256::from(topic_from_signature(sig)),
            abi_json: None,
        });
    }
    if let Some(name) = &args.event {
        let sig_cache = SqliteSignatureCache::new(cache.signatures());
        // `signatures` is source-agnostic and speaks `[u8; 20]`/`[u8; 32]`;
        // convert at this boundary.
        let resolved = resolve_event(
            &sig_cache,
            chain_id.get(),
            address.0.0,
            name,
            config.etherscan_api_key(),
        )
        .await?;
        return Ok(Lookup {
            signature: Some(resolved.signature),
            topic0: B256::from(resolved.topic0),
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

fn print_event(item: &Positioned<EvmEvent, EvmPosition>, index: usize, opts: &PrintOpts) {
    let e = &item.item;
    let tx_str = if opts.detail.is_full() {
        format!("{:#x}", e.tx_hash)
    } else {
        let b = e.tx_hash.as_slice();
        format!("0x{}…{}", hex::encode(&b[..4]), hex::encode(&b[30..]))
    };
    // When decoding, raw topics + data are redundant with the per-param
    // breakdown below.
    let (topics_str, data_str) = if opts.decoder.is_some() {
        (String::new(), String::new())
    } else {
        (
            format_topics(&e.topics, opts.detail),
            format_data(&e.data, opts.detail),
        )
    };
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
        print_decoded(event, e, opts.detail);
    }
}

fn format_topics(topics: &[Topic], detail: HexDetail) -> String {
    // topic0 is the event selector — fixed by the filter, redundant.
    let rest = topics.iter().skip(1);
    let parts: Vec<String> = if detail.is_full() {
        rest.map(|t| format!("{t}")).collect()
    } else {
        rest.map(|t| {
            let b = t.as_slice();
            format!("0x{}…{}", hex::encode(&b[..4]), hex::encode(&b[28..]))
        })
        .collect()
    };
    if parts.is_empty() {
        String::new()
    } else {
        format!("  topics=[{}]", parts.join(", "))
    }
}

fn format_data(data: &[u8], detail: HexDetail) -> String {
    if data.is_empty() {
        return String::new();
    }
    if detail.is_full() {
        return format!("  data=0x{}", hex::encode(data));
    }
    let n = data.len().min(16);
    let suffix = if data.len() > n { "…" } else { "" };
    format!("  data=0x{}{}", hex::encode(&data[..n]), suffix)
}

fn print_decoded(event: &Event, log: &EvmEvent, detail: HexDetail) {
    let decoded = match event.decode_log_parts(log.topics.iter().map(|t| t.b256()), &log.data) {
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
        println!("       {name} ({kind}) = {}", format_value(&value, detail));
    }
}

/// Max bytes shown for a `Bytes` value when not in `--full` mode.
const PREVIEW_BYTES: usize = 16;
/// Max chars shown for a `String` value when not in `--full` mode.
const PREVIEW_STRING_CHARS: usize = 60;

fn format_value(v: &DynSolValue, detail: HexDetail) -> String {
    match v {
        DynSolValue::Bool(b) => b.to_string(),
        DynSolValue::Int(i, _) => i.to_string(),
        DynSolValue::Uint(u, _) => u.to_string(),
        DynSolValue::Address(a) => format!("0x{}", hex::encode(a.as_slice())),
        DynSolValue::Function(f) => format!("0x{}", hex::encode(f.as_slice())),
        DynSolValue::FixedBytes(b, n) => format!("0x{}", hex::encode(&b.as_slice()[..*n])),
        DynSolValue::Bytes(b) => format_bytes(b, detail),
        DynSolValue::String(s) => format_string(s, detail),
        DynSolValue::Array(items) | DynSolValue::FixedArray(items) | DynSolValue::Tuple(items) => {
            let parts: Vec<String> = items.iter().map(|i| format_value(i, detail)).collect();
            format!("[{}]", parts.join(", "))
        }
    }
}

fn format_bytes(b: &[u8], detail: HexDetail) -> String {
    if detail.is_full() || b.len() <= PREVIEW_BYTES {
        return format!("0x{}", hex::encode(b));
    }
    format!(
        "0x{}… ({} bytes)",
        hex::encode(&b[..PREVIEW_BYTES]),
        b.len()
    )
}

fn format_string(s: &str, detail: HexDetail) -> String {
    if detail.is_full() || s.chars().count() <= PREVIEW_STRING_CHARS {
        return format!("\"{s}\"");
    }
    let truncated: String = s.chars().take(PREVIEW_STRING_CHARS).collect();
    format!("\"{truncated}…\" ({} chars)", s.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};

    fn t32(b: u8) -> Topic {
        Topic::new(B256::repeat_byte(b))
    }

    #[test]
    fn topics_truncate_skips_topic0_and_shortens() {
        let topics = [t32(0xaa), t32(0xbb)];
        let s = format_topics(&topics, HexDetail::Abbreviated);
        // topic0 (0xaa…) must be filtered out; topic1 shown truncated
        // (first 4 + last 4 bytes only, not the full 32).
        assert!(!s.contains("aaaa"));
        assert!(s.contains("0xbbbbbbbb…bbbbbbbb"));
        assert!(!s.contains(&"bb".repeat(32)));
    }

    #[test]
    fn topics_full_shows_each_topic_in_full() {
        let topics = [t32(0xaa), t32(0xbb), t32(0xcc)];
        let s = format_topics(&topics, HexDetail::Full);
        assert!(s.contains(&format!("0x{}", hex::encode(t32(0xbb).as_slice()))));
        assert!(s.contains(&format!("0x{}", hex::encode(t32(0xcc).as_slice()))));
        // topic0 still filtered.
        assert!(!s.contains(&format!("0x{}", hex::encode(t32(0xaa).as_slice()))));
    }

    #[test]
    fn topics_empty_when_only_topic0() {
        assert_eq!(format_topics(&[t32(0xaa)], HexDetail::Abbreviated), "");
        assert_eq!(format_topics(&[], HexDetail::Abbreviated), "");
    }

    #[test]
    fn data_truncates_to_16_bytes_with_ellipsis() {
        let data = vec![0xab; 32];
        let s = format_data(&data, HexDetail::Abbreviated);
        // 16 bytes = 32 hex chars, plus ellipsis.
        assert_eq!(s, format!("  data=0x{}…", "ab".repeat(16)));
    }

    #[test]
    fn data_full_shows_everything() {
        let data = vec![0xab; 32];
        let s = format_data(&data, HexDetail::Full);
        assert_eq!(s, format!("  data=0x{}", "ab".repeat(32)));
    }

    #[test]
    fn data_empty_is_blank() {
        assert_eq!(format_data(&[], HexDetail::Abbreviated), "");
        assert_eq!(format_data(&[], HexDetail::Full), "");
    }

    #[test]
    fn format_bytes_below_preview_threshold_is_not_truncated() {
        let b = vec![0u8; PREVIEW_BYTES];
        assert_eq!(
            format_bytes(&b, HexDetail::Abbreviated),
            format!("0x{}", "00".repeat(PREVIEW_BYTES))
        );
    }

    #[test]
    fn format_bytes_above_threshold_truncates_and_reports_length() {
        let b = vec![0xcd; 100];
        let s = format_bytes(&b, HexDetail::Abbreviated);
        assert_eq!(s, format!("0x{}… (100 bytes)", "cd".repeat(PREVIEW_BYTES)));
    }

    #[test]
    fn format_bytes_full_overrides_truncation() {
        let b = vec![0xcd; 100];
        let s = format_bytes(&b, HexDetail::Full);
        assert_eq!(s, format!("0x{}", "cd".repeat(100)));
    }

    #[test]
    fn format_string_truncates_with_char_count() {
        let s = "x".repeat(120);
        let out = format_string(&s, HexDetail::Abbreviated);
        assert_eq!(
            out,
            format!("\"{}…\" (120 chars)", "x".repeat(PREVIEW_STRING_CHARS))
        );
    }

    #[test]
    fn format_string_short_is_quoted_in_full() {
        assert_eq!(format_string("hello", HexDetail::Abbreviated), "\"hello\"");
        assert_eq!(format_string("hello", HexDetail::Full), "\"hello\"");
    }

    #[test]
    fn format_value_recursive_tuple_truncation() {
        // tuple(uint256, bytes) — bytes leaf should truncate when not full.
        let value = DynSolValue::Tuple(vec![
            DynSolValue::Uint(U256::from(42u64), 256),
            DynSolValue::Bytes(vec![0xee; 50]),
        ]);
        let s = format_value(&value, HexDetail::Abbreviated);
        assert_eq!(
            s,
            format!("[42, 0x{}… (50 bytes)]", "ee".repeat(PREVIEW_BYTES))
        );
    }

    #[test]
    fn format_value_address_and_fixed_bytes() {
        let addr = Address::from([0x12u8; 20]);
        assert_eq!(
            format_value(&DynSolValue::Address(addr), HexDetail::Abbreviated),
            "0x1212121212121212121212121212121212121212"
        );

        let v = DynSolValue::FixedBytes(B256::from([0x34u8; 32]), 4);
        // bytes4 should show only first 4 bytes.
        assert_eq!(format_value(&v, HexDetail::Abbreviated), "0x34343434");
    }

    #[test]
    fn format_value_bool_int_uint() {
        assert_eq!(
            format_value(&DynSolValue::Bool(true), HexDetail::Abbreviated),
            "true"
        );
        assert_eq!(
            format_value(
                &DynSolValue::Uint(U256::from(1_000_000u64), 256),
                HexDetail::Abbreviated
            ),
            "1000000"
        );
    }
}
