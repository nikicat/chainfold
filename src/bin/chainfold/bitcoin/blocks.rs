//! `bitcoin blocks` — the Bitcoin block-history command group. The Bitcoin
//! sibling of `evm blocks`: the default output is the block *history* over
//! a window (cached, resumable, followable).

use std::path::Path;

use chainfold::arch::{Hist, Positioned, Timestamp, watch_hist};
use chainfold::bitcoin::{BitcoinChain, BtcBlock, BtcBlockHash};
use chainfold::chain::{BlockNumber, BlockRange, Finality};
use futures_util::{StreamExt, TryStreamExt};

use crate::cli::{
    BitcoinArgs, BtcBlocksArgs, BtcBlocksCommand, BtcBlocksListArgs, Direction, FollowMode,
    WindowArgs,
};
use crate::fmt::{format_timestamp, osc8_link};
use crate::util::{WindowSel, block_cut, inclusive_range, print_date_estimate, window_sel};

/// Which columns to render — driven by the `--full` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnSet {
    /// HEIGHT / TIME / TXS / DIFFICULTY / HASH.
    Default,
    /// The default set plus PARENT; full hashes.
    Full,
}

impl ColumnSet {
    fn from_full(full: bool) -> Self {
        if full { Self::Full } else { Self::Default }
    }

    fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

pub async fn run(
    chain: &BitcoinChain,
    db_path: &Path,
    btc_args: &BitcoinArgs,
    args: &BtcBlocksArgs,
    direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    match &args.command {
        BtcBlocksCommand::List(list_args) => {
            list(chain, db_path, btc_args, list_args, direction, follow).await
        }
    }
}

async fn list(
    chain: &BitcoinChain,
    db_path: &Path,
    btc_args: &BitcoinArgs,
    args: &BtcBlocksListArgs,
    direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let columns = ColumnSet::from_full(args.full);

    if follow.is_follow() {
        if direction == Direction::Backward {
            eprintln!("note: --reverse is ignored with --follow (append-only stream)");
        }
        let store = chain.blocks(Finality::Latest);
        // Only the stream's start matters when following.
        let from = match window_sel(&btc_args.window)? {
            WindowSel::Blocks { from_block, .. } => from_block,
            WindowSel::Dates { from, to } => {
                estimate_date_range(chain, &btc_args.window, from, to)
                    .await?
                    .from
            }
            WindowSel::Unspecified { num_blocks } => {
                // Last N ending at the tip: awaiting the first snapshot
                // demand-starts the head clock.
                let tip = *store.next().await.end();
                tip.saturating_sub(num_blocks.get() - 1)
            }
        };
        print_info(db_path, from, None);
        print_header(columns);
        println!("Following new blocks (Ctrl+C to stop)...\n");
        let mut stream = watch_hist(&store, block_cut(from));
        let mut count: usize = 0;
        while let Some(item) = stream.next().await {
            print_block(&item?.item, columns);
            count += 1;
        }
        println!("\nHistory closed ({count} blocks).");
        return Ok(());
    }

    // One-shot: a finite window. Explicit and date windows are bounded
    // hists — head-free, fully offline when the cache is warm. Only the
    // unspecified "last N blocks" inherently needs the head: it slices the
    // tail off a snapshot of the reactive store.
    let hist: Hist<BtcBlock, BlockNumber> = match window_sel(&btc_args.window)? {
        WindowSel::Blocks {
            from_block,
            num_blocks,
        } => chain.blocks_hist(inclusive_range(from_block, num_blocks)),
        WindowSel::Dates { from, to } => {
            chain.blocks_hist(estimate_date_range(chain, &btc_args.window, from, to).await?)
        }
        WindowSel::Unspecified { num_blocks } => {
            let hist = chain.blocks(Finality::Latest).next().await;
            let tip = *hist.end();
            let from = tip.saturating_sub(num_blocks.get() - 1);
            let (_, tail) = hist.split(block_cut(from));
            tail
        }
    };

    let from = *hist.start() + 1;
    let to = *hist.end();
    print_info(db_path, from, Some(to));
    print_header(columns);

    // The window is bounded (≤ --num-blocks), so collecting it is fine —
    // and lets --reverse print newest-first.
    let items: Vec<Positioned<BtcBlock, BlockNumber>> = hist.iter().try_collect().await?;

    let total = items.len();
    let shown = total.min(btc_args.window.limit);
    match direction {
        // Newest-first, keeping the newest `--limit` blocks.
        Direction::Backward => {
            for item in items.iter().rev().take(shown) {
                print_block(&item.item, columns);
            }
        }
        Direction::Forward => {
            for item in items.iter().take(shown) {
                print_block(&item.item, columns);
            }
        }
    }

    if total == 0 {
        println!("No blocks found.");
    } else if shown < total {
        println!("... (limit reached, use --limit to show more)");
        println!("\n{shown} of {total} blocks shown.");
    } else {
        println!("\n{total} blocks shown.");
    }

    Ok(())
}

/// The dates-window estimate for the Bitcoin chain (the `EvmChain` twin
/// lives in `util::estimate_date_range` — no shared trait by design).
async fn estimate_date_range(
    chain: &BitcoinChain,
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

fn print_info(db_path: &Path, from: BlockNumber, to: Option<BlockNumber>) {
    println!("  Chain:    bitcoin");
    match to {
        Some(to) => println!("  Range:    block {}", BlockRange::new(from, to)),
        None => println!("  Range:    block {from}..  (following)"),
    }
    println!("  DB:       {}", db_path.display());
    println!();
}

fn print_header(columns: ColumnSet) {
    let base = format!(
        "{:>9}  {:<20}  {:>6}  {:>14}  {:<16}",
        "HEIGHT", "TIME", "TXS", "DIFFICULTY", "HASH"
    );
    if columns.is_full() {
        println!("{base}  PARENT");
    } else {
        println!("{base}");
    }
}

/// mempool.space link wrapping the (possibly abbreviated) hash.
fn linked_hash(hash: &BtcBlockHash, full: bool) -> String {
    let text = if full {
        hash.to_string()
    } else {
        // Bitcoin hashes lead with zeroes — the suffix is the telling part.
        let hex = hash.to_string();
        format!("..{}", &hex[hex.len() - 14..])
    };
    osc8_link(&format!("https://mempool.space/block/{hash}"), &text)
}

fn print_block(b: &BtcBlock, columns: ColumnSet) {
    let base = format!(
        "{:>9}  {:<20}  {:>6}  {:>14.4e}  {:<16}",
        b.height,
        format_timestamp(b.time),
        b.tx_count,
        b.bits.difficulty(),
        linked_hash(&b.hash, columns.is_full()),
    );
    if columns.is_full() {
        println!("{base}  {}", b.prev_hash);
    } else {
        println!("{base}");
    }
}
