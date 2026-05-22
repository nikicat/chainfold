use std::collections::HashMap;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Hist, HistBackend};
use chainfold::evm::{
    apply_transfer, fetch_erc20_metadata, Erc20Metadata, EvmLogHistory, LogStore, TRANSFER_TOPIC,
};
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
    println!("  Chain:    {}", chain_id);
    println!("  DB:       {}", db_path);
    println!();

    let show_balance = matches!(erc20_args.command, Erc20Command::Balances);

    for address in &addresses {
        let meta_symbol = metadata.get(address).map(|m| m.symbol.clone()).unwrap_or_default();
        let topics: [Option<[u8; 32]>; 4] = [Some(TRANSFER_TOPIC), None, None, None];
        let backend = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            *address,
            topics,
            evm_args.chunk_size,
            12,
        )
        .await?;

        let (start, end) = resolve_range(evm_args, &backend).await?;
        println!("── {} ({}..{}) ──", meta_symbol, start.block, end.block);

        let mut balance = U256::ZERO;

        if follow {
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
                let involved = involves_owner(&item.item, &owner);
                if show_balance {
                    balance = apply_transfer(owner, balance, &item.item);
                }
                if involved {
                    if show_balance {
                        fmt::print_balance(&item, balance, count, &ctx);
                    } else {
                        fmt::print_transfer(&item, count, &ctx);
                    }
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
                let involved = involves_owner(&item.item, &owner);
                if show_balance {
                    balance = apply_transfer(owner, balance, &item.item);
                }
                if involved {
                    if count >= evm_args.limit {
                        println!("... (limit reached, use --limit to show more)");
                        break;
                    }
                    if show_balance {
                        fmt::print_balance(&item, balance, count, &ctx);
                    } else {
                        fmt::print_transfer(&item, count, &ctx);
                    }
                    count += 1;
                }
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

fn involves_owner(event: &EvmEvent, owner: &[u8; 20]) -> bool {
    chainfold::evm::decode_transfer(event)
        .map(|t| t.from == *owner || t.to == *owner)
        .unwrap_or(false)
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
