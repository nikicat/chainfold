use std::collections::HashMap;

use alloy::primitives::{Address, U256};
use chainfold::arch::{Positioned, Timestamp};
use chainfold::chain::BlockNumber;
use chainfold::evm::{
    BlockHash, Erc20Metadata, EvmEvent, EvmPosition, Gas, GasPrice, TxHash, decode_transfer,
};
use url::Url;

use crate::table::{Cell, CellStyle, Column, TableSpec};

/// How much of a hex (or otherwise long) value to render — the user's
/// `--full` flag, funneled once. Orthogonal to [`RenderMode`]
/// (`crate::table::RenderMode`): this picks the *text* of a value, the
/// render mode picks how it is dressed (links, colors, borders).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HexDetail {
    /// Every byte.
    Full,
    /// Abbreviated `0xdead…beef` head/tail.
    Abbreviated,
}

impl HexDetail {
    /// From the `--full` flag: `true` → [`Self::Full`].
    pub fn from_full(full: bool) -> Self {
        if full { Self::Full } else { Self::Abbreviated }
    }

    pub fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// The block-explorer base for a chain id, parsed once per command and
/// threaded down as a typed [`Url`].
pub fn explorer_base_url(chain_id: u64) -> Url {
    let base = match chain_id {
        10 => "https://optimistic.etherscan.io",
        56 => "https://bscscan.com",
        100 => "https://gnosisscan.io",
        137 => "https://polygonscan.com",
        8453 => "https://basescan.org",
        42161 => "https://arbiscan.io",
        43114 => "https://snowscan.xyz",
        534_352 => "https://scrollscan.com",
        _ => "https://etherscan.io",
    };
    Url::parse(base).expect("static explorer bases are valid URLs")
}

/// Join a path onto an explorer base. http(s) URLs always join cleanly and
/// our path segments are hex-formatted, so a failure is a programmer error.
fn explorer_join(base: &Url, path: &str) -> Url {
    base.join(path)
        .expect("explorer paths are well-formed by construction")
}

pub fn abbrev_addr(addr: &Address) -> String {
    let b = addr.as_slice();
    format!("0x{}…{}", hex::encode(&b[..3]), hex::encode(&b[18..]))
}

/// `0xdead…beef` head/tail preview of a 32-byte hash.
fn abbrev_hash32(b: &[u8]) -> String {
    format!("0x{}…{}", hex::encode(&b[..4]), hex::encode(&b[30..]))
}

fn abbrev_tx_hash(hash: &TxHash) -> String {
    abbrev_hash32(hash.as_slice())
}

/// Parse a URL we constructed ourselves from static parts — a parse failure
/// is a programmer error.
pub fn explorer_url(url: &str) -> Url {
    Url::parse(url).expect("explorer URLs are well-formed by construction")
}

/// Abbreviated address cell linking to the explorer's address page.
pub fn addr_cell(addr: &Address, base_url: &Url) -> Cell {
    Cell::new(abbrev_addr(addr)).link(explorer_join(base_url, &format!("address/{addr:#x}")))
}

/// Tx hash cell linking to the explorer's tx page.
pub fn tx_cell(hash: &TxHash, base_url: &Url, detail: HexDetail) -> Cell {
    let text = if detail.is_full() {
        format!("{hash:#x}")
    } else {
        abbrev_tx_hash(hash)
    };
    Cell::new(text).link(explorer_join(base_url, &format!("tx/{hash:#x}")))
}

/// Block hash cell linking to the explorer's block page.
pub fn block_hash_cell(
    number: BlockNumber,
    hash: &BlockHash,
    base_url: &Url,
    detail: HexDetail,
) -> Cell {
    let text = if detail.is_full() {
        format!("{hash:#x}")
    } else {
        abbrev_hash32(hash.as_slice())
    };
    Cell::new(text).link(explorer_join(base_url, &format!("block/{number}")))
}

/// `14.9M ( 50%)` — gas used with its share of the block limit.
pub fn format_gas(used: Gas, limit: Gas) -> String {
    let pct = if limit.get() == 0 {
        0
    } else {
        used.get() * 100 / limit.get()
    };
    format!("{:>6} ({pct:>3}%)", format_millions(used.get()))
}

/// `14.9M` — one decimal place, integer math (display only, no float cast).
fn format_millions(units: u64) -> String {
    format!("{}.{}M", units / 1_000_000, (units % 1_000_000) / 100_000)
}

/// Base fee in gwei, 2 decimal places; `-` on pre-EIP-1559 blocks.
pub fn format_base_fee(fee: Option<GasPrice>) -> String {
    fee.map_or_else(
        || "-".to_string(),
        |f| {
            let wei = f.get();
            format!(
                "{}.{:02} gwei",
                wei / 1_000_000_000,
                (wei % 1_000_000_000) / 10_000_000
            )
        },
    )
}

pub fn format_timestamp(ts: Timestamp) -> String {
    chrono::DateTime::from_timestamp(ts.get() as i64, 0).map_or_else(
        || "----------T--:--:--Z".into(),
        |dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    )
}

const AMOUNT_WHOLE_WIDTH: usize = 6;
const BALANCE_WHOLE_WIDTH: usize = 10;

pub fn format_amount(raw: U256, decimals: u8, whole_width: usize, frac_width: usize) -> String {
    let divisor = U256::from(10u64).pow(U256::from(decimals as u64));
    let whole = raw / divisor;
    let frac = raw % divisor;

    let whole_str = format!("{whole:>whole_width$}");

    if frac_width == 0 {
        return whole_str;
    }

    let frac_str = format!("{:0>width$}", frac, width = decimals as usize);
    let trimmed = frac_str.trim_end_matches('0');
    if trimmed.is_empty() {
        format!("{}{}", whole_str, " ".repeat(frac_width + 1))
    } else {
        let trailing_pad = frac_width - trimmed.len();
        format!("{}.{}{}", whole_str, trimmed, " ".repeat(trailing_pad))
    }
}

/// Human-readable token amount with no column padding — for prose lines
/// ("Opening balance …"), as opposed to the fixed-width table cells.
pub fn format_amount_trim(raw: U256, decimals: u8) -> String {
    format_amount(raw, decimals, 1, decimals as usize)
        .trim()
        .to_string()
}

pub struct FmtContext<'a> {
    pub owner: &'a Address,
    pub metadata: &'a HashMap<Address, Erc20Metadata>,
    pub max_sym_width: usize,
    pub max_decimals: usize,
    pub base_url: &'a Url,
}

/// The transfer table's columns, sized from the context's max symbol width
/// and decimal count (`balance` appends the running-balance column).
pub fn transfer_spec(ctx: &FmtContext, show_balance: bool) -> TableSpec {
    // format_amount emits `whole_width` chars plus `.` + fraction when any
    // token has decimals — the columns fit that pre-padded width exactly.
    let frac = if ctx.max_decimals == 0 {
        0
    } else {
        ctx.max_decimals + 1
    };
    let mut cols = vec![
        Column::right("#").min(4),
        Column::left("block").min(10),
        Column::left("time").min(20),
        Column::left("tx").min(15),
        Column::right("dir").min(3),
        Column::left("counterparty").min(13),
        Column::right("amount").min(AMOUNT_WHOLE_WIDTH + frac),
        Column::left("token").min(ctx.max_sym_width),
    ];
    if show_balance {
        cols.push(Column::right("balance").min(BALANCE_WHOLE_WIDTH + frac));
    }
    TableSpec::new(cols)
}

/// Cells for one transfer row; `balance` appends the running-balance cell.
/// `None` when the event doesn't decode as a Transfer or its token metadata
/// is missing (skipped, as before).
pub fn transfer_row(
    item: &Positioned<EvmEvent, EvmPosition>,
    balance: Option<U256>,
    index: usize,
    ctx: &FmtContext,
) -> Option<Vec<Cell>> {
    let e = &item.item;
    let t = decode_transfer(e)?;
    let meta = ctx.metadata.get(&e.contract_address)?;
    let incoming = &t.to == ctx.owner;
    let counterparty = if incoming { &t.from } else { &t.to };
    let direction = if incoming {
        Cell::new("IN").style(CellStyle::Good)
    } else {
        Cell::new("OUT").style(CellStyle::Bad)
    };
    let mut row = vec![
        Cell::new(index.to_string()),
        Cell::new(e.block_number.to_string()),
        Cell::new(format_timestamp(e.timestamp)),
        tx_cell(&e.tx_hash, ctx.base_url, HexDetail::Abbreviated),
        direction,
        addr_cell(counterparty, ctx.base_url),
        Cell::new(format_amount(
            t.amount,
            meta.decimals,
            AMOUNT_WHOLE_WIDTH,
            ctx.max_decimals,
        )),
        Cell::new(meta.symbol.as_str()),
    ];
    if let Some(balance) = balance {
        row.push(Cell::new(format_amount(
            balance,
            meta.decimals,
            BALANCE_WHOLE_WIDTH,
            ctx.max_decimals,
        )));
    }
    Some(row)
}
