use std::collections::HashMap;

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use chainfold::arch::{Positioned, watch_hist};
use chainfold::evm::{
    CacheDb, Erc20Metadata, EvmChain, EvmEvent, EvmPosition, OwnedErc20, apply_transfer,
    fetch_erc20_metadata,
};
use chainfold::merge::{MergedItem, OrderedPos};
use futures_util::StreamExt;
use futures_util::future::join_all;

use crate::cli::{Direction, Erc20Args, Erc20Command, EvmArgs, FollowMode};
use crate::fmt::{self, FmtContext};
use crate::util::{parse_hex_address, resolve_range};

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    cache: &CacheDb,
    db_path: &str,
    evm_args: &EvmArgs,
    erc20_args: &Erc20Args,
    _direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let addresses: Vec<Address> = erc20_args
        .address
        .iter()
        .map(|a| parse_hex_address(a))
        .collect::<Result<_, _>>()?;
    let owner = parse_hex_address(&erc20_args.owner)?;
    let chain_id = chain.chain_id();

    // ERC20-metadata view over the shared cache connection (the log cache
    // rides inside the chain's provider).
    let erc20_cache = cache.erc20();

    println!("Fetching token metadata...");
    let meta_futures = addresses
        .iter()
        .map(|addr| fetch_erc20_metadata(chain.provider(), *addr, chain_id, &erc20_cache));
    let meta_results = join_all(meta_futures).await;

    let mut metadata: HashMap<Address, Erc20Metadata> = HashMap::new();
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

    let base_url = fmt::explorer_base_url(chain_id.get());
    let ctx = FmtContext {
        owner: &owner,
        metadata: &metadata,
        max_sym_width,
        max_decimals,
        base_url,
    };

    println!("  Owner:    0x{}", hex::encode(owner.as_slice()));
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
        let owned = OwnedErc20::new(chain, owner, *address);

        let (start, end) = resolve_range(evm_args, owned.out_transfers()).await?;
        println!("── {} ({}..{}) ──", meta_symbol, start.block, end.block);

        let merged_hs = owned.transfers();
        let mut balance = U256::ZERO;
        let mut last_pos: Option<EvmPosition> = None;

        if follow.is_follow() {
            // The chain was built with a poll transport, so both inner
            // stores' tips are already driven live by the head clock.
            let mut stream = watch_hist(&merged_hs, OrderedPos::from(start));
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
            let (_, after_start) = hist.split(OrderedPos::from(start));
            let (window, _) = after_start.split(OrderedPos::from(end));
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
    item: &Positioned<MergedItem<EvmEvent, EvmPosition>, OrderedPos<EvmPosition>>,
    balance: &mut U256,
    last_pos: &mut Option<EvmPosition>,
    count: usize,
    ctx: &FmtContext<'_>,
    show_balance: bool,
    owner: Address,
) -> bool {
    let mi = &item.item;
    let event_pos = mi.value.position();
    if Some(event_pos) == *last_pos {
        return false;
    }
    *last_pos = Some(event_pos);
    if show_balance {
        *balance = apply_transfer(owner, *balance, &mi.value);
    }
    let positioned = Positioned::new(mi.value.clone(), event_pos);
    if show_balance {
        fmt::print_balance(&positioned, *balance, count, ctx);
    } else {
        fmt::print_transfer(&positioned, count, ctx);
    }
    true
}
