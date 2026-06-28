use alloy::primitives::Address;
use chainfold::arch::HistStore;
use chainfold::evm::{EvmEvent, EvmPosition};
use chrono::NaiveDate;

use crate::cli::EvmArgs;

pub fn parse_hex_address(s: &str) -> Result<Address, String> {
    s.parse().map_err(|e| format!("invalid address: {e}"))
}

pub fn parse_datetime(s: &str) -> Result<u64, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|dt| dt.and_utc().timestamp() as u64)
        .map_err(|e| format!("invalid date: {e}"))
}

/// Default `--from-block` when neither date range nor explicit block is given.
const DEFAULT_FROM_BLOCK: u64 = 22_000_000;

/// Resolve `(from, to)` `EvmPosition` from the user's `--from` / `--to` /
/// `--from-block` / `--num-blocks` choice. Used by both `events` and
/// `erc20` subcommands.
///
/// Prints date-range info to stdout on the date-range branch — preserves
/// the CLI output the per-command callers used to emit.
pub async fn resolve_range(
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
            let end_inclusive = EvmPosition::new(end.block.saturating_add(1).get(), u32::MAX);
            Ok((start, end_inclusive))
        }
        (None, None, Some(from_block)) => Ok(block_range(from_block, args.num_blocks)),
        (None, None, None) => Ok(block_range(DEFAULT_FROM_BLOCK, args.num_blocks)),
        _ => Err("provide either --from + --to or --from-block, not both".into()),
    }
}

fn block_range(from_block: u64, num_blocks: u64) -> (EvmPosition, EvmPosition) {
    let to_block = from_block + num_blocks - 1;
    let start = if from_block == 0 {
        EvmPosition::earliest()
    } else {
        EvmPosition::new(from_block - 1, u32::MAX)
    };
    (start, EvmPosition::new(to_block, u32::MAX))
}
