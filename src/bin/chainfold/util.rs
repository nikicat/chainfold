use alloy::primitives::Address;
use chainfold::arch::{HistStore, Timestamp};
use chainfold::evm::{BlockCount, BlockNumber, EvmBlock, EvmEvent, EvmPosition, LogIdx};
use chrono::NaiveDate;

use crate::cli::EvmArgs;

pub fn parse_hex_address(s: &str) -> Result<Address, String> {
    s.parse().map_err(|e| format!("invalid address: {e}"))
}

pub fn parse_datetime(s: &str) -> Result<Timestamp, String> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|dt| Timestamp::new(dt.and_utc().timestamp() as u64))
        .map_err(|e| format!("invalid date: {e}"))
}

/// Default `--from-block` when neither date range nor explicit block is given.
const DEFAULT_FROM_BLOCK: u64 = 22_000_000;

/// The user's window selection — the shared interpretation of `--from` /
/// `--to` vs `--from-block` / `--num-blocks`. Domain types: clap's raw
/// integers wrap here, once.
pub enum WindowSel {
    /// `--from` + `--to` (UTC dates).
    Dates { from: Timestamp, to: Timestamp },
    /// `--from-block` (+ `--num-blocks`).
    Blocks {
        from_block: BlockNumber,
        num_blocks: BlockCount,
    },
    /// Neither given — each command picks its own default window of
    /// `num_blocks`.
    Unspecified { num_blocks: BlockCount },
}

/// Decode the window flags. Errors on conflicting or zero-sized selections.
pub fn window_sel(args: &EvmArgs) -> Result<WindowSel, Box<dyn std::error::Error>> {
    if args.num_blocks == 0 {
        return Err("--num-blocks must be positive".into());
    }
    let num_blocks = BlockCount::new(args.num_blocks);
    match (&args.from, &args.to, args.from_block) {
        (Some(from), Some(to), None) => Ok(WindowSel::Dates {
            from: parse_datetime(from)?,
            to: parse_datetime(to)?,
        }),
        (None, None, Some(from_block)) => Ok(WindowSel::Blocks {
            from_block: BlockNumber::new(from_block),
            num_blocks,
        }),
        (None, None, None) => Ok(WindowSel::Unspecified { num_blocks }),
        _ => Err("provide either --from + --to or --from-block, not both".into()),
    }
}

/// Resolve `(from, to)` `EvmPosition` from the user's window selection.
/// Used by both `events` and `erc20` subcommands.
///
/// Prints date-range info to stdout on the date-range branch — preserves
/// the CLI output the per-command callers used to emit.
pub async fn resolve_range(
    args: &EvmArgs,
    hs: &HistStore<EvmEvent, EvmPosition>,
) -> Result<(EvmPosition, EvmPosition), Box<dyn std::error::Error>> {
    match window_sel(args)? {
        WindowSel::Dates { from, to } => {
            let start = hs.time_pos(from).await?;
            let end = hs.time_pos(to).await?;
            print_date_range(args, start.block, end.block);
            let end_inclusive = EvmPosition::new(end.block.saturating_add(1), LogIdx::MAX);
            Ok((start, end_inclusive))
        }
        WindowSel::Blocks {
            from_block,
            num_blocks,
        } => Ok(log_window(from_block, num_blocks)),
        WindowSel::Unspecified { num_blocks } => {
            Ok(log_window(BlockNumber::new(DEFAULT_FROM_BLOCK), num_blocks))
        }
    }
}

/// Resolve `(start, end)` cuts for the blocks history — `(start, end]`
/// covers the selected blocks. Unlike [`resolve_range`], the unspecified
/// default is the last `--num-blocks` ending at the store's tip: a fixed
/// mainnet block default is meaningless for a block lister and wrong on
/// every other chain.
pub async fn resolve_blocks_range(
    args: &EvmArgs,
    hs: &HistStore<EvmBlock, BlockNumber>,
) -> Result<(BlockNumber, BlockNumber), Box<dyn std::error::Error>> {
    match window_sel(args)? {
        WindowSel::Dates { from, to } => {
            let start = hs.time_pos(from).await?;
            let end = hs.time_pos(to).await?;
            print_date_range(args, start, end);
            Ok((start, end.saturating_add(1)))
        }
        WindowSel::Blocks {
            from_block,
            num_blocks,
        } => Ok(blocks_window(from_block, num_blocks)),
        WindowSel::Unspecified { num_blocks } => {
            // No RPC: the tip is seeded from the chain head at store
            // construction.
            let tip = *hs.next().await.end();
            let from_block = tip.saturating_sub(num_blocks.get() - 1);
            Ok(blocks_window(from_block, num_blocks))
        }
    }
}

/// The `Dates` branch's stdout note. `start`/`end` are the *cut* positions;
/// the first block after each cut is the estimate shown.
fn print_date_range(args: &EvmArgs, start: BlockNumber, end: BlockNumber) {
    // The Dates arm implies both flags are present.
    let (from_str, to_str) = (
        args.from.as_deref().expect("Dates arm has --from"),
        args.to.as_deref().expect("Dates arm has --to"),
    );
    println!("  Date range: {from_str} → {to_str}");
    println!("  Estimated blocks: {} → {}", start + 1, end + 1);
}

fn log_window(from_block: BlockNumber, num_blocks: BlockCount) -> (EvmPosition, EvmPosition) {
    let to_block = from_block + num_blocks - 1;
    let start = match from_block.checked_sub(1) {
        None => EvmPosition::earliest(),
        Some(prev) => EvmPosition::new(prev, LogIdx::MAX),
    };
    (start, EvmPosition::new(to_block, LogIdx::MAX))
}

fn blocks_window(from_block: BlockNumber, num_blocks: BlockCount) -> (BlockNumber, BlockNumber) {
    // ZERO is the lower-bound sentinel, so a window starting at genesis (or
    // at block 1 — the documented position-0 wart) begins at the very start.
    let start = from_block.checked_sub(1).unwrap_or(BlockNumber::ZERO);
    (start, from_block + num_blocks - 1)
}
