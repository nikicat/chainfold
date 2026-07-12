use std::collections::HashMap;

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use chainfold::arch::{Positioned, watch_hist};
use chainfold::cache::CacheDb;
use chainfold::evm::tokenlist::{default_token_list_url, resolve_token};
use chainfold::evm::{
    Erc20Metadata, EvmChain, EvmEvent, EvmPosition, OwnedErc20, apply_transfer,
    fetch_erc20_metadata,
};
use chainfold::merge::MergedItem;
use futures_util::StreamExt;
use futures_util::future::join_all;

use crate::cli::{Direction, Erc20Args, Erc20Command, EvmArgs, FollowMode, TokenRef};
use crate::fmt::{self, FmtContext};
use crate::table::{Cell, Out, TableBuf};
use crate::util::{log_cut, log_end, parse_hex_address, resolve_log_range};

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    cache: &CacheDb,
    evm_args: &EvmArgs,
    erc20_args: &Erc20Args,
    _direction: Direction,
    follow: FollowMode,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = parse_hex_address(&erc20_args.owner)?;
    let chain_id = chain.chain_id();

    // Symbols go through the token-list resolver (cached; refreshes over
    // HTTP when stale); addresses pass through untouched.
    let token_list = cache.token_list();
    let list_url = default_token_list_url();
    let mut addresses: Vec<Address> = Vec::with_capacity(erc20_args.token.len());
    for token in &erc20_args.token {
        addresses.push(match token {
            TokenRef::Address(address) => *address,
            TokenRef::Symbol(symbol) => {
                resolve_token(&token_list, chain_id, symbol, &list_url)
                    .await?
                    .address
            }
        });
    }

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
        base_url: &base_url,
    };

    println!("  Owner:    0x{}", hex::encode(owner.as_slice()));
    println!("  Tokens:   {}", addresses.len());
    println!();

    let show_balance = matches!(erc20_args.command, Erc20Command::Balances);

    // One window for every token (the merge is per token, the window is
    // not).
    let range = resolve_log_range(chain, &evm_args.window).await?;

    for address in &addresses {
        let meta_symbol = metadata
            .get(address)
            .map(|m| m.symbol.clone())
            .unwrap_or_default();
        let owned = OwnedErc20::new(chain, owner, *address);

        let merged_hs = owned.transfers();
        let mut balance = U256::ZERO;

        if follow.is_follow() {
            let mut writer = out.rows(fmt::transfer_spec(&ctx, show_balance));
            writer.title(format!(
                "{meta_symbol} transfers {range} · chain {chain_id}"
            ));
            writer.print_header();
            // Awaiting the merged stream demand-starts the head clock,
            // which drives both inner stores' tips.
            let mut stream = watch_hist(&merged_hs, log_cut(range.from));
            let mut count = 0;
            while let Some(item) = stream.next().await {
                if let Some(row) =
                    render_transfer(&item?, &mut balance, count, &ctx, show_balance, owner)
                {
                    writer.print_row(&row);
                    count += 1;
                }
            }
        } else {
            // The merge works on reactive stores, so the snapshot demands
            // the head (once per process, singleflight) and the window is
            // cut out of it.
            let hist = merged_hs.next().await;
            let (_, after_start) = hist.split(log_cut(range.from));
            let (window, _) = after_start.split(log_end(range.to));
            let mut stream = window.iter();
            let mut table = TableBuf::new(fmt::transfer_spec(&ctx, show_balance));
            table.title(format!(
                "{meta_symbol} transfers {range} · chain {chain_id}"
            ));
            let mut count = 0;
            let mut limited = false;
            while let Some(item) = stream.next().await {
                if count >= evm_args.window.limit {
                    limited = true;
                    break;
                }
                if let Some(row) =
                    render_transfer(&item?, &mut balance, count, &ctx, show_balance, owner)
                {
                    table.push(row);
                    count += 1;
                }
            }
            if count == 0 {
                println!("No transfers involving owner.");
            } else {
                out.table(&table);
                if limited {
                    println!("... (limit reached, use --limit to show more)");
                }
                println!("\n{count} transfers shown.\n");
            }
        }
    }

    Ok(())
}

/// One merged transfer as table cells: fold the running balance when
/// enabled, and build the row. The merge yields each log once
/// (same-position copies coalesce), so no dedupe is needed here.
fn render_transfer(
    item: &Positioned<MergedItem<EvmEvent, EvmPosition>, EvmPosition>,
    balance: &mut U256,
    count: usize,
    ctx: &FmtContext<'_>,
    show_balance: bool,
    owner: Address,
) -> Option<Vec<Cell>> {
    let mi = &item.item;
    if show_balance {
        *balance = apply_transfer(owner, *balance, &mi.value);
    }
    let positioned = Positioned::new(mi.value.clone(), item.pos);
    fmt::transfer_row(&positioned, show_balance.then_some(*balance), count, ctx)
}
