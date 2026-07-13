//! `bitcoin blocks` — the Bitcoin block-history command group. The Bitcoin
//! sibling of `evm blocks`: the default output is the block *history* over
//! a window (cached, resumable, followable).

use chainfold::arch::{Hist, Positioned, Timestamp, watch_hist};
use chainfold::bitcoin::{BitcoinChain, BtcBlock, BtcBlockHash};
use chainfold::chain::{BlockNumber, BlockRange, Finality};
use futures_util::{StreamExt, TryStreamExt};

use crate::cli::{
    BitcoinArgs, BtcBlocksArgs, BtcBlocksCommand, BtcBlocksListArgs, Direction, FollowMode,
    WindowArgs,
};
use crate::fmt::{HexDetail, explorer_url};
use crate::table::{Cell, Column, Out, TableBuf, TableSpec};
use crate::theme::{Content, TermTheme};
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

    /// `--full` also means full hashes — the value detail follows the
    /// column set.
    fn hex_detail(self) -> HexDetail {
        HexDetail::from_full(self.is_full())
    }
}

pub async fn run(
    chain: &BitcoinChain,
    btc_args: &BitcoinArgs,
    args: &BtcBlocksArgs,
    direction: Direction,
    follow: FollowMode,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    match &args.command {
        BtcBlocksCommand::List(list_args) => {
            list(chain, btc_args, list_args, direction, follow, out).await
        }
    }
}

async fn list(
    chain: &BitcoinChain,
    btc_args: &BitcoinArgs,
    args: &BtcBlocksListArgs,
    direction: Direction,
    follow: FollowMode,
    out: Out,
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
                estimate_date_range(chain, &btc_args.window, from, to, out)
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
        let mut writer = out.rows(spec(columns));
        writer.title(title(from, None));
        writer.print_header();
        println!("Following new blocks (Ctrl+C to stop)...\n");
        let mut stream = watch_hist(&store, block_cut(from));
        let mut count: usize = 0;
        while let Some(item) = stream.next().await {
            writer.print_row(&row(&item?.item, columns));
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
            chain.blocks_hist(estimate_date_range(chain, &btc_args.window, from, to, out).await?)
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

    // The window is bounded (≤ --num-blocks), so collecting it is fine —
    // and lets --reverse print newest-first.
    let items: Vec<Positioned<BtcBlock, BlockNumber>> = hist.iter().try_collect().await?;

    let total = items.len();
    let shown = total.min(btc_args.window.limit);
    let mut table = TableBuf::new(spec(columns));
    table.title(title(from, Some(to)));
    match direction {
        // Newest-first, keeping the newest `--limit` blocks.
        Direction::Backward => {
            for item in items.iter().rev().take(shown) {
                table.push(row(&item.item, columns));
            }
        }
        Direction::Forward => {
            for item in items.iter().take(shown) {
                table.push(row(&item.item, columns));
            }
        }
    }

    if total == 0 {
        println!("No blocks found.");
        return Ok(());
    }
    out.table(&table);
    if shown < total {
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
    out: Out,
) -> Result<BlockRange, Box<dyn std::error::Error>> {
    let range = BlockRange::new(
        chain.estimate_block(from).await?,
        chain.estimate_block(to).await?,
    );
    print_date_estimate(args, range, out);
    Ok(range)
}

/// Table title: entity, window, and chain — `blocks 100..=199 · bitcoin`;
/// an open end (`--follow`) renders as `blocks 100.. · bitcoin`. Composed
/// [`Content`], so the numbers dress per theme at render time.
fn title(from: BlockNumber, to: Option<BlockNumber>) -> Content {
    Content::from(move |theme: &dyn TermTheme| match to {
        Some(to) => format!(
            "blocks {} · bitcoin",
            theme.block_range(BlockRange::new(from, to))
        ),
        None => format!("blocks {}.. · bitcoin", theme.block_number(from)),
    })
}

/// The block table's columns — the one source of truth for both the
/// buffered one-shot table and the `--follow` row stream.
fn spec(columns: ColumnSet) -> TableSpec {
    let mut cols = vec![
        Column::right("height").min(9),
        Column::left("time").min(20),
        Column::right("txs").min(6),
        Column::right("difficulty").min(14),
        Column::left("hash").min(16),
    ];
    if columns.is_full() {
        cols.push(Column::left("parent"));
    }
    TableSpec::new(cols)
}

/// mempool.space-linked cell on the (possibly abbreviated) hash.
fn hash_cell(hash: &BtcBlockHash, detail: HexDetail) -> Cell {
    let text = if detail.is_full() {
        hash.to_string()
    } else {
        // Bitcoin hashes lead with zeroes — the suffix is the telling part.
        let hex = hash.to_string();
        format!("..{}", &hex[hex.len() - 14..])
    };
    Cell::new(text).link(explorer_url(&format!("https://mempool.space/block/{hash}")))
}

fn row(b: &BtcBlock, columns: ColumnSet) -> Vec<Cell> {
    let mut row = vec![
        Cell::new(b.height),
        Cell::new(b.time),
        Cell::new(b.tx_count.to_string()),
        Cell::new(format!("{:.4e}", b.bits.difficulty())),
        hash_cell(&b.hash, columns.hex_detail()),
    ];
    if columns.is_full() {
        row.push(Cell::new(b.prev_hash.to_string()));
    }
    row
}
