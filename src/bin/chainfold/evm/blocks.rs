//! `evm blocks` — the block-history command group. The reference instance
//! of the histories-first CLI: the default output is the block *history*
//! over a window (cached, resumable, followable); a single block is just a
//! slice of it.

use alloy::network::Ethereum;
use alloy::providers::Provider;
use chainfold::arch::{Hist, Positioned, watch_hist};
use chainfold::evm::{ChainId, EvmBlock, EvmChain};
use futures_util::{StreamExt, TryStreamExt};

use url::Url;

use crate::cli::{BlocksArgs, BlocksCommand, BlocksListArgs, Direction, EvmArgs, FollowMode};
use crate::fmt::{
    HexDetail, addr_cell, block_hash_cell, explorer_base_url, format_base_fee, format_gas,
    format_timestamp,
};
use crate::table::{Cell, Column, Out, TableBuf, TableSpec};
use crate::util::{WindowSel, block_cut, estimate_date_range, inclusive_range, window_sel};
use chainfold::chain::{BlockNumber, BlockRange, Finality};

/// Which columns to render — driven by the `--full` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnSet {
    /// BLOCK / TIME / TXS / GAS USED / BASE FEE / HASH.
    Default,
    /// The default set plus MINER and PARENT; full hashes.
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

pub async fn run<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    evm_args: &EvmArgs,
    args: &BlocksArgs,
    direction: Direction,
    follow: FollowMode,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    match &args.command {
        BlocksCommand::List(list_args) => {
            list(chain, evm_args, list_args, direction, follow, out).await
        }
    }
}

async fn list<P: Provider<Ethereum> + Clone + Send + Sync + 'static>(
    chain: &EvmChain<P>,
    evm_args: &EvmArgs,
    args: &BlocksListArgs,
    direction: Direction,
    follow: FollowMode,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    let columns = ColumnSet::from_full(args.full);
    let base_url = explorer_base_url(chain.chain_id().get());

    if follow.is_follow() {
        if direction == Direction::Backward {
            eprintln!("note: --reverse is ignored with --follow (append-only stream)");
        }
        let store = chain.blocks(Finality::Latest);
        // Only the stream's start matters when following.
        let from = match window_sel(&evm_args.window)? {
            WindowSel::Blocks { from_block, .. } => from_block,
            WindowSel::Dates { from, to } => {
                estimate_date_range(chain, &evm_args.window, from, to)
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
        writer.title(title(chain.chain_id(), from, None));
        writer.print_header();
        println!("Following new blocks (Ctrl+C to stop)...\n");
        let mut stream = watch_hist(&store, block_cut(from));
        let mut count: usize = 0;
        while let Some(item) = stream.next().await {
            writer.print_row(&row(&item?.item, columns, &base_url));
            count += 1;
        }
        println!("\nHistory closed ({count} blocks).");
        return Ok(());
    }

    // One-shot: a finite window. Explicit and date windows are bounded
    // hists — head-free, fully offline when the cache is warm. Only the
    // unspecified "last N blocks" inherently needs the head: it slices the
    // tail off a snapshot of the reactive store.
    let hist: Hist<EvmBlock, BlockNumber> = match window_sel(&evm_args.window)? {
        WindowSel::Blocks {
            from_block,
            num_blocks,
        } => chain.blocks_hist(inclusive_range(from_block, num_blocks)),
        WindowSel::Dates { from, to } => {
            chain.blocks_hist(estimate_date_range(chain, &evm_args.window, from, to).await?)
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
    let items: Vec<Positioned<EvmBlock, BlockNumber>> = hist.iter().try_collect().await?;

    let total = items.len();
    let shown = total.min(evm_args.window.limit);
    let mut table = TableBuf::new(spec(columns));
    table.title(title(chain.chain_id(), from, Some(to)));
    match direction {
        // Newest-first, keeping the newest `--limit` blocks.
        Direction::Backward => {
            for item in items.iter().rev().take(shown) {
                table.push(row(&item.item, columns, &base_url));
            }
        }
        Direction::Forward => {
            for item in items.iter().take(shown) {
                table.push(row(&item.item, columns, &base_url));
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

/// Table title: entity, window, and chain — `blocks 100..=199 · chain 1`;
/// an open end (`--follow`) renders as `blocks 100.. · chain 1`.
fn title(chain_id: ChainId, from: BlockNumber, to: Option<BlockNumber>) -> String {
    match to {
        Some(to) => format!("blocks {} · chain {chain_id}", BlockRange::new(from, to)),
        None => format!("blocks {from}.. · chain {chain_id}"),
    }
}

/// The block table's columns — the one source of truth for both the
/// buffered one-shot table and the `--follow` row stream.
fn spec(columns: ColumnSet) -> TableSpec {
    let mut cols = vec![
        Column::right("block").min(10),
        Column::left("time").min(20),
        Column::right("txs").min(5),
        Column::right("gas used").min(13),
        Column::right("base fee").min(10),
        Column::left("hash").min(13),
    ];
    if columns.is_full() {
        cols.push(Column::left("miner").min(13));
        cols.push(Column::left("parent"));
    }
    TableSpec::new(cols)
}

fn row(b: &EvmBlock, columns: ColumnSet, base_url: &Url) -> Vec<Cell> {
    let mut row = vec![
        Cell::new(b.number.to_string()),
        Cell::new(format_timestamp(b.timestamp)),
        Cell::new(b.tx_count.to_string()),
        Cell::new(format_gas(b.gas_used, b.gas_limit)),
        Cell::new(format_base_fee(b.base_fee)),
        block_hash_cell(b.number, &b.hash, base_url, columns.hex_detail()),
    ];
    if columns.is_full() {
        row.push(addr_cell(&b.miner, base_url));
        row.push(Cell::new(format!("{:#x}", b.parent_hash)));
    }
    row
}
