use std::collections::HashMap;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{History, Side};
use chainfold::evm::{
    evm_pos_cmp, fetch_erc20_metadata, Erc20BalanceReducer, Erc20Metadata, EvmLogHistory,
    LogStore, TRANSFER_TOPIC,
};
use chainfold::reduce::{ReducedEvent, ReducedHistory};

use crate::cli::{Erc20Args, Erc20Command, EvmArgs};
use crate::fmt::{self, FmtContext};
use crate::util::{parse_datetime, parse_hex_address};

type BalanceEvent = ReducedEvent<EvmEvent, U256>;

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    provider: &P,
    cache: &Arc<LogStore>,
    db_path: &str,
    evm_args: &EvmArgs,
    erc20_args: &Erc20Args,
    chain_id: u64,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let addresses: Vec<[u8; 20]> = erc20_args
        .address
        .iter()
        .map(|a| parse_hex_address(a))
        .collect::<Result<_, _>>()?;
    let owner = parse_hex_address(&erc20_args.owner)?;

    // Fetch token metadata
    println!("Fetching token metadata...");
    let meta_futures = addresses
        .iter()
        .map(|addr| fetch_erc20_metadata(provider, *addr, chain_id, cache));
    let meta_results = futures_util::future::join_all(meta_futures).await;

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

    match &erc20_args.command {
        Erc20Command::Transfers => {
            run_transfers(
                provider, cache, db_path, evm_args, &addresses, &owner, chain_id, &metadata,
                max_sym_width, max_decimals, reverse, follow,
            )
            .await
        }
        Erc20Command::Balances => {
            run_balances(
                provider, cache, db_path, evm_args, &addresses, &owner, chain_id, &metadata,
                max_sym_width, max_decimals, reverse, follow,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_transfers<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    provider: &P,
    cache: &Arc<LogStore>,
    db_path: &str,
    evm_args: &EvmArgs,
    addresses: &[[u8; 20]],
    owner: &[u8; 20],
    chain_id: u64,
    metadata: &HashMap<[u8; 20], Erc20Metadata>,
    max_sym_width: usize,
    max_decimals: usize,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut padded_owner = [0u8; 32];
    padded_owner[12..32].copy_from_slice(owner);

    let mut all_histories: Vec<History<EvmEvent, EvmPosition>> = Vec::new();
    let mut _log_backends: Vec<Arc<EvmLogHistory<_>>> = Vec::new();

    for &address in addresses {
        let topics_out: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), Some(padded_owner), None, None];
        let h_out = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            address,
            topics_out,
            evm_args.chunk_size,
            12,
        )
        .await?;

        let topics_in: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), None, Some(padded_owner), None];
        let h_in = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            address,
            topics_in,
            evm_args.chunk_size,
            12,
        )
        .await?;

        let per_token = History::merge(vec![h_out.history(), h_in.history()], evm_pos_cmp);
        all_histories.push(per_token);
        _log_backends.push(h_out);
        _log_backends.push(h_in);
    }

    let history = History::merge(all_histories, evm_pos_cmp);
    let base_url = fmt::explorer_base_url(chain_id);

    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Tokens:   {}", addresses.len());
    println!("  Chain:    {}", chain_id);

    let (start, cutoff) = resolve_range_raw(evm_args, &history, chain_id)?;

    println!("  Range:    block {} .. {}", start.block_number, cutoff.block_number);
    println!("  DB:       {}", db_path);
    println!();

    let ctx = FmtContext {
        owner,
        metadata,
        max_sym_width,
        max_decimals,
        base_url,
    };

    if follow {
        println!("Following new events (Ctrl+C to stop)...\n");
        for backend in &_log_backends {
            let _handle = backend.start_tip_tracking(std::time::Duration::from_secs(2));
            backend.poll_tip().await?;
        }

        let mut count = 0;
        let mut maybe_cursor = history.after(&start, None).await?;
        while let Some(cursor) = maybe_cursor {
            fmt::print_transfer(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.after(cursor.after(), None).await?;
        }
        println!("\nHistory closed ({count} events).");
    } else if reverse {
        println!("Navigating backwards from block {}:\n", cutoff.block_number);
        let mut maybe_cursor = history.before(&cutoff, Some(&start)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= evm_args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            fmt::print_transfer(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.before(cursor.before(), Some(&start)).await?;
        }
        print_count(count);
    } else {
        println!("Navigating forward from block {}:\n", start.block_number);
        let mut maybe_cursor = history.after(&start, Some(&cutoff)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= evm_args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            fmt::print_transfer(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.after(cursor.after(), Some(&cutoff)).await?;
        }
        print_count(count);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_balances<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    provider: &P,
    cache: &Arc<LogStore>,
    db_path: &str,
    evm_args: &EvmArgs,
    addresses: &[[u8; 20]],
    owner: &[u8; 20],
    chain_id: u64,
    metadata: &HashMap<[u8; 20], Erc20Metadata>,
    max_sym_width: usize,
    max_decimals: usize,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut padded_owner = [0u8; 32];
    padded_owner[12..32].copy_from_slice(owner);

    let mut all_histories: Vec<History<BalanceEvent, EvmPosition>> = Vec::new();
    let mut _log_backends: Vec<Arc<EvmLogHistory<_>>> = Vec::new();
    let mut _reduced_backends: Vec<Arc<ReducedHistory<Erc20BalanceReducer<_>, EvmPosition>>> =
        Vec::new();

    for &address in addresses {
        let topics_out: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), Some(padded_owner), None, None];
        let h_out = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            address,
            topics_out,
            evm_args.chunk_size,
            12,
        )
        .await?;

        let topics_in: [Option<[u8; 32]>; 4] =
            [Some(TRANSFER_TOPIC), None, Some(padded_owner), None];
        let h_in = EvmLogHistory::with_options(
            provider.clone(),
            cache.clone(),
            address,
            topics_in,
            evm_args.chunk_size,
            12,
        )
        .await?;

        let per_token = History::merge(vec![h_out.history(), h_in.history()], evm_pos_cmp);

        let reducer = Erc20BalanceReducer::new(
            *owner,
            address,
            chain_id,
            provider.clone(),
            cache.clone(),
        );
        let reduced = ReducedHistory::new(per_token, reducer, evm_pos_cmp, 1, 1);

        all_histories.push(reduced.history());
        _reduced_backends.push(reduced);
        _log_backends.push(h_out);
        _log_backends.push(h_in);
    }

    let history = History::merge(all_histories, evm_pos_cmp);
    let base_url = fmt::explorer_base_url(chain_id);

    println!("  Owner:    0x{}", hex::encode(owner));
    println!("  Tokens:   {}", addresses.len());
    println!("  Chain:    {}", chain_id);

    let (start, cutoff) = resolve_range_balance(evm_args, &history, chain_id)?;

    println!("  Range:    block {} .. {}", start.block_number, cutoff.block_number);
    println!("  DB:       {}", db_path);
    println!();

    let ctx = FmtContext {
        owner,
        metadata,
        max_sym_width,
        max_decimals,
        base_url,
    };

    if follow {
        println!("Following new events (Ctrl+C to stop)...\n");
        for backend in &_log_backends {
            let _handle = backend.start_tip_tracking(std::time::Duration::from_secs(2));
            backend.poll_tip().await?;
        }

        let mut count = 0;
        let mut maybe_cursor = history.after(&start, None).await?;
        while let Some(cursor) = maybe_cursor {
            fmt::print_balance(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.after(cursor.after(), None).await?;
        }
        println!("\nHistory closed ({count} events).");
    } else if reverse {
        println!("Navigating backwards from block {}:\n", cutoff.block_number);
        let mut maybe_cursor = history.before(&cutoff, Some(&start)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= evm_args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            fmt::print_balance(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.before(cursor.before(), Some(&start)).await?;
        }
        print_count(count);
    } else {
        println!("Navigating forward from block {}:\n", start.block_number);
        let mut maybe_cursor = history.after(&start, Some(&cutoff)).await?;
        let mut count = 0;
        while let Some(cursor) = maybe_cursor {
            if count >= evm_args.limit {
                println!("... (limit reached, use --limit to show more)");
                break;
            }
            fmt::print_balance(&cursor, count, &ctx);
            count += 1;
            maybe_cursor = history.after(cursor.after(), Some(&cutoff)).await?;
        }
        print_count(count);
    }

    Ok(())
}

fn resolve_range_raw(
    args: &EvmArgs,
    history: &History<EvmEvent, EvmPosition>,
    chain_id: u64,
) -> Result<(EvmPosition, EvmPosition), String> {
    resolve_range_impl(args, chain_id, |ts| history.at(ts))
}

fn resolve_range_balance(
    args: &EvmArgs,
    history: &History<BalanceEvent, EvmPosition>,
    chain_id: u64,
) -> Result<(EvmPosition, EvmPosition), String> {
    resolve_range_impl(args, chain_id, |ts| history.at(ts))
}

fn resolve_range_impl(
    args: &EvmArgs,
    chain_id: u64,
    at: impl Fn(u64) -> EvmPosition,
) -> Result<(EvmPosition, EvmPosition), String> {
    match (&args.from, &args.to, args.from_block) {
        (Some(from_str), Some(to_str), None) => {
            let from_ts = parse_datetime(from_str)?;
            let to_ts = parse_datetime(to_str)?;
            let start_pos = at(from_ts);
            let end_pos = at(to_ts);
            println!("  Date range: {} → {}", from_str, to_str);
            println!(
                "  Estimated blocks: {} → {}",
                start_pos.block_number, end_pos.block_number
            );
            let cutoff = EvmPosition {
                chain_id,
                block_number: end_pos.block_number,
                log_index: u32::MAX,
                block_hash: [0; 32],
                timestamp: to_ts,
                side: Side::After,
            };
            let start = EvmPosition {
                chain_id,
                block_number: start_pos.block_number,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: from_ts,
                side: Side::Before,
            };
            Ok((start, cutoff))
        }
        (None, None, Some(from_block)) => {
            let to_block = from_block + args.num_blocks - 1;
            let cutoff = EvmPosition {
                chain_id,
                block_number: to_block,
                log_index: u32::MAX,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::After,
            };
            let start = EvmPosition {
                chain_id,
                block_number: from_block,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::Before,
            };
            Ok((start, cutoff))
        }
        (None, None, None) => {
            let from_block = 22000000u64;
            let to_block = from_block + args.num_blocks - 1;
            let cutoff = EvmPosition {
                chain_id,
                block_number: to_block,
                log_index: u32::MAX,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::After,
            };
            let start = EvmPosition {
                chain_id,
                block_number: from_block,
                log_index: 0,
                block_hash: [0; 32],
                timestamp: 0,
                side: Side::Before,
            };
            Ok((start, cutoff))
        }
        _ => Err(
            "provide either --from + --to (dates) or --from-block (with optional --num-blocks), not both"
                .into(),
        ),
    }
}

fn print_count(count: usize) {
    if count == 0 {
        println!("No transfers found.");
    } else {
        println!("\n{count} transfers shown.");
    }
}
