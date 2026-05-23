use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Position, Positioned};
use chainfold::evm::{
    apply_transfer, fetch_erc20_metadata, Erc20Metadata, EvmLogHistory, LogStore, OwnedErc20,
    SubscribeTransport, IN_KEY, OUT_KEY,
};
use chainfold::merge::{MergedItem, MergedPos};
use futures_util::future::join_all;
use futures_util::StreamExt;

use crate::cli::{Erc20Args, Erc20Command, EvmArgs};
use crate::fmt::{self, FmtContext};
use crate::util::{parse_datetime, parse_hex_address};

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    provider: &P,
    cache: &Arc<LogStore>,
    db_path: &str,
    evm_args: &EvmArgs,
    erc20_args: &Erc20Args,
    chain_id: u64,
    _reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let addresses: Vec<[u8; 20]> = erc20_args
        .address
        .iter()
        .map(|a| parse_hex_address(a))
        .collect::<Result<_, _>>()?;
    let owner = parse_hex_address(&erc20_args.owner)?;

    println!("Fetching token metadata...");
    let meta_futures = addresses
        .iter()
        .map(|addr| fetch_erc20_metadata(provider, *addr, chain_id, cache));
    let meta_results = join_all(meta_futures).await;

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
    let max_decimals = metadata
        .values()
        .map(|m| m.decimals as usize)
        .max()
        .unwrap_or(0);
    println!();

    let base_url = fmt::explorer_base_url(chain_id);
    let ctx = FmtContext {
        owner: &owner,
        metadata: &metadata,
        max_sym_width,
        max_decimals,
        base_url,
    };

    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Tokens:   {}", addresses.len());
    println!("  Chain:    {chain_id}");
    println!("  DB:       {db_path}");
    println!();

    let show_balance = matches!(erc20_args.command, Erc20Command::Balances);

    for address in &addresses {
        let meta_symbol = metadata
            .get(address)
            .map(|m| m.symbol.clone())
            .unwrap_or_default();
        let owned = OwnedErc20::with_options(
            provider.clone(),
            cache.clone(),
            owner,
            *address,
            evm_args.chunk_size,
            12,
        )
        .await?;
        owned.poll_tips().await?;

        let (start, end) = resolve_range(evm_args, owned.out_history()).await?;
        println!("── {} ({}..{}) ──", meta_symbol, start.block, end.block);

        let merged_hs = owned.transfers();
        let mut balance = U256::ZERO;
        let mut last_pos: Option<EvmPosition> = None;

        if follow {
            let _sub_out = owned
                .out_history()
                .subscribe(SubscribeTransport::Poll {
                    interval: Duration::from_secs(2),
                })
                .await?;
            let _sub_in = owned
                .in_history()
                .subscribe(SubscribeTransport::Poll {
                    interval: Duration::from_secs(2),
                })
                .await?;
            let mut stream = watch_hist(&merged_hs, merged_pos_for(start));
            let mut count = 0;
            while let Some(item) = stream.next().await {
                let item = item?;
                let mi: &MergedItem<EvmEvent> = &item.item;
                let event_pos = mi.item.position();
                if Some(event_pos) == last_pos {
                    continue;
                }
                last_pos = Some(event_pos);
                if show_balance {
                    balance = apply_transfer(owner, balance, &mi.item);
                }
                let positioned = Positioned::new(mi.item.clone(), event_pos);
                if show_balance {
                    fmt::print_balance(&positioned, balance, count, &ctx);
                } else {
                    fmt::print_transfer(&positioned, count, &ctx);
                }
                count += 1;
            }
        } else {
            let hist = merged_hs.next().await;
            let (_, after_start) = hist.split(merged_pos_for(start));
            let (window, _) = after_start.split(merged_pos_for(end));
            let mut stream = window.iter();
            let mut count = 0;
            while let Some(item) = stream.next().await {
                let item = item?;
                let mi: &MergedItem<EvmEvent> = &item.item;
                let event_pos = mi.item.position();
                if Some(event_pos) == last_pos {
                    continue;
                }
                last_pos = Some(event_pos);
                if show_balance {
                    balance = apply_transfer(owner, balance, &mi.item);
                }
                if count >= evm_args.limit {
                    println!("... (limit reached, use --limit to show more)");
                    break;
                }
                let positioned = Positioned::new(mi.item.clone(), event_pos);
                if show_balance {
                    fmt::print_balance(&positioned, balance, count, &ctx);
                } else {
                    fmt::print_transfer(&positioned, count, &ctx);
                }
                count += 1;
            }
            if count == 0 {
                println!("No transfers involving owner.");
            } else {
                println!("\n{count} transfers shown.\n");
            }
        }
    }

    Ok(())
}

/// Build the two-key `MergedPos` representing `p` for both the `in` and
/// `out` sub-histories. Used to bracket the merged stream when the caller
/// has only an [`EvmPosition`] (e.g. from `resolve_range`).
fn merged_pos_for(p: EvmPosition) -> MergedPos {
    let bytes = p.marshal();
    let mut m = BTreeMap::new();
    m.insert(IN_KEY.into(), bytes.clone());
    m.insert(OUT_KEY.into(), bytes);
    MergedPos::new(m)
}

async fn resolve_range<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    args: &EvmArgs,
    backend: &Arc<EvmLogHistory<P>>,
) -> Result<(EvmPosition, EvmPosition), Box<dyn std::error::Error>> {
    let hs = backend.hist_store();
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
