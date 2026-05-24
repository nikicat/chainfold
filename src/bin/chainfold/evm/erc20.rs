use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{watch_hist, Position, Positioned};
use chainfold::evm::{
    apply_transfer, fetch_erc20_metadata, Erc20Metadata, LogStore, OwnedErc20, SubscribeTransport,
    IN_KEY, OUT_KEY,
};
use chainfold::merge::{MergedItem, MergedPos};
use futures_util::future::join_all;
use futures_util::StreamExt;

use crate::cli::{Erc20Args, Erc20Command, EvmArgs};
use crate::fmt::{self, FmtContext};
use crate::util::{parse_hex_address, resolve_range};

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

        let out_hs = owned.out_history().hist_store();
        let (start, end) = resolve_range(evm_args, &out_hs).await?;
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
                if render_transfer(
                    &item?,
                    &mut balance,
                    &mut last_pos,
                    count,
                    &ctx,
                    show_balance,
                    owner,
                ) {
                    count += 1;
                }
            }
        } else {
            let hist = merged_hs.next().await;
            let (_, after_start) = hist.split(merged_pos_for(start));
            let (window, _) = after_start.split(merged_pos_for(end));
            let mut stream = window.iter();
            let mut count = 0;
            while let Some(item) = stream.next().await {
                if count >= evm_args.limit {
                    println!("... (limit reached, use --limit to show more)");
                    break;
                }
                if render_transfer(
                    &item?,
                    &mut balance,
                    &mut last_pos,
                    count,
                    &ctx,
                    show_balance,
                    owner,
                ) {
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

/// Render one merged transfer: dedupe on event position (the in/out
/// pre-filter yields the same log twice when owner == from == to is
/// theoretically possible, and reorg-edge cases can re-emit), fold the
/// running balance when enabled, and print. Returns `true` if printed —
/// the caller increments its count on `true`.
fn render_transfer(
    item: &Positioned<MergedItem<EvmEvent>, MergedPos>,
    balance: &mut U256,
    last_pos: &mut Option<EvmPosition>,
    count: usize,
    ctx: &FmtContext<'_>,
    show_balance: bool,
    owner: [u8; 20],
) -> bool {
    let mi = &item.item;
    let event_pos = mi.item.position();
    if Some(event_pos) == *last_pos {
        return false;
    }
    *last_pos = Some(event_pos);
    if show_balance {
        *balance = apply_transfer(owner, *balance, &mi.item);
    }
    let positioned = Positioned::new(mi.item.clone(), event_pos);
    if show_balance {
        fmt::print_balance(&positioned, *balance, count, ctx);
    } else {
        fmt::print_transfer(&positioned, count, ctx);
    }
    true
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

