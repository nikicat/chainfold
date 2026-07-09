use std::path::{Path, PathBuf};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use chainfold::arch::Timestamp;
use chainfold::cache::versioned_path;
use chainfold::evm::{EvmChain, EvmPosition, LogIdx};
use chrono::NaiveDate;

use crate::cli::WindowArgs;
use chainfold::chain::{BlockCount, BlockNumber, BlockRange};

/// Setup-phase span (target `chainfold::setup`): the name is the phase's row
/// label in the `--stats` summary. A macro, not a fn — `tracing` span names
/// are static callsite metadata, so each needs its own literal.
macro_rules! setup_span {
    ($name:literal) => {
        tracing::span!(target: "chainfold::setup", tracing::Level::DEBUG, $name)
    };
}
pub(crate) use setup_span;

/// Default cache DB location: `$XDG_CACHE_HOME/chainfold/chainfold.db`.
/// Falls back to `./chainfold.db` if no XDG cache dir is resolvable.
fn default_db_path() -> PathBuf {
    dirs::cache_dir().map_or_else(
        || PathBuf::from("./chainfold.db"),
        |d| d.join("chainfold").join("chainfold.db"),
    )
}

/// The base (unversioned) cache path for a `--db` flag: the flag value, or
/// the XDG default.
pub fn db_base_path(db_flag: Option<&Path>) -> PathBuf {
    db_flag.map_or_else(default_db_path, Path::to_path_buf)
}

/// The resolved, versioned cache path for a `--db` flag — the file that
/// actually gets opened (`chainfold.db` → `chainfold.v3.db`). Every command
/// must resolve through here (or [`db_base_path`] + `versioned_path`) so
/// `evm` and `cache` always operate on the same file. Does NOT create the
/// file or its parent directory.
pub fn resolved_db_path(db_flag: Option<&Path>) -> PathBuf {
    versioned_path(&db_base_path(db_flag))
}

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
pub fn window_sel(args: &WindowArgs) -> Result<WindowSel, Box<dyn std::error::Error>> {
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

/// The inclusive window of `num_blocks` blocks starting at `from_block`.
pub fn inclusive_range(from_block: BlockNumber, num_blocks: BlockCount) -> BlockRange {
    BlockRange::new(from_block, from_block + num_blocks - 1)
}

/// Estimate the inclusive `[from, to]` block window for a date range via
/// the chain's block-time anchor, printing the CLI estimate note.
pub async fn estimate_date_range<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    args: &WindowArgs,
    from: Timestamp,
    to: Timestamp,
) -> Result<BlockRange, Box<dyn std::error::Error>> {
    let range = BlockRange::new(
        chain.estimate_block(from).await?,
        chain.estimate_block(to).await?,
    );
    print_date_estimate(args, range);
    Ok(range)
}

/// The CLI note printed with a dates-window estimate.
pub fn print_date_estimate(args: &WindowArgs, range: BlockRange) {
    // The Dates arm implies both flags are present.
    let (from_str, to_str) = (
        args.from.as_deref().expect("Dates arm has --from"),
        args.to.as_deref().expect("Dates arm has --to"),
    );
    println!("  Date range: {from_str} → {to_str}");
    println!("  Estimated blocks: {range}");
}

/// Resolve the inclusive block window for the log commands (`events`,
/// `erc20`). The unspecified default starts at a fixed block — no head
/// demand; a fully-cached run stays offline.
pub async fn resolve_log_range<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    args: &WindowArgs,
) -> Result<BlockRange, Box<dyn std::error::Error>> {
    match window_sel(args)? {
        WindowSel::Dates { from, to } => estimate_date_range(chain, args, from, to).await,
        WindowSel::Blocks {
            from_block,
            num_blocks,
        } => Ok(inclusive_range(from_block, num_blocks)),
        WindowSel::Unspecified { num_blocks } => Ok(inclusive_range(
            BlockNumber::new(DEFAULT_FROM_BLOCK),
            num_blocks,
        )),
    }
}

/// The block-space split cut just before `from` — `(cut, …]` starts
/// iteration at `from`. ZERO doubles as the lower-bound sentinel (the
/// documented position-0 wart).
pub fn block_cut(from: BlockNumber) -> BlockNumber {
    from.checked_sub(1).unwrap_or(BlockNumber::ZERO)
}

/// The log-space split cut just before `from`.
pub fn log_cut(from: BlockNumber) -> EvmPosition {
    match from.checked_sub(1) {
        None => EvmPosition::earliest(),
        Some(prev) => EvmPosition::new(prev, LogIdx::MAX),
    }
}

/// The log-space cut at the end of block `to` — `(…, cut]` includes every
/// log up to and including the last one in `to`.
pub fn log_end(to: BlockNumber) -> EvmPosition {
    EvmPosition::new(to, LogIdx::MAX)
}
