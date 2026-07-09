use std::collections::HashMap;

use alloy::primitives::{Address, U256};
use chainfold::arch::{Positioned, Timestamp};
use chainfold::chain::BlockNumber;
use chainfold::evm::{
    BlockHash, Erc20Metadata, EvmEvent, EvmPosition, Gas, GasPrice, TxHash, decode_transfer,
};

pub fn explorer_base_url(chain_id: u64) -> &'static str {
    match chain_id {
        10 => "https://optimistic.etherscan.io",
        56 => "https://bscscan.com",
        100 => "https://gnosisscan.io",
        137 => "https://polygonscan.com",
        8453 => "https://basescan.org",
        42161 => "https://arbiscan.io",
        43114 => "https://snowscan.xyz",
        534_352 => "https://scrollscan.com",
        _ => "https://etherscan.io",
    }
}

pub fn osc8_link(url: &str, text: &str) -> String {
    format!("\x1b]8;;{url}\x07{text}\x1b]8;;\x07")
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

pub fn linked_addr(addr: &Address, base_url: &str) -> String {
    let url = format!("{base_url}/address/{addr:#x}");
    osc8_link(&url, &abbrev_addr(addr))
}

fn linked_tx_hash(hash: &TxHash, base_url: &str) -> String {
    let url = format!("{base_url}/tx/{hash:#x}");
    osc8_link(&url, &abbrev_tx_hash(hash))
}

/// Block hash as an explorer link to the block page, abbreviated unless
/// `full`.
pub fn linked_block_hash(
    number: BlockNumber,
    hash: &BlockHash,
    base_url: &str,
    full: bool,
) -> String {
    let url = format!("{base_url}/block/{number}");
    let text = if full {
        format!("{hash:#x}")
    } else {
        abbrev_hash32(hash.as_slice())
    };
    osc8_link(&url, &text)
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

pub struct FmtContext<'a> {
    pub owner: &'a Address,
    pub metadata: &'a HashMap<Address, Erc20Metadata>,
    pub max_sym_width: usize,
    pub max_decimals: usize,
    pub base_url: &'static str,
}

pub fn print_transfer(item: &Positioned<EvmEvent, EvmPosition>, index: usize, ctx: &FmtContext) {
    let e = &item.item;
    if let Some(t) = decode_transfer(e) {
        let Some(meta) = ctx.metadata.get(&e.contract_address) else {
            return;
        };
        let direction = if &t.to == ctx.owner { " IN" } else { "OUT" };
        let counterparty = if &t.to == ctx.owner { &t.from } else { &t.to };
        let amount = format_amount(
            t.amount,
            meta.decimals,
            AMOUNT_WHOLE_WIDTH,
            ctx.max_decimals,
        );
        let sym = format!("{:<width$}", meta.symbol, width = ctx.max_sym_width);

        println!(
            "[{:>4}] {:<10} {}  {}  {:>3}  {}  {} {}",
            index,
            e.block_number,
            format_timestamp(e.timestamp),
            linked_tx_hash(&e.tx_hash, ctx.base_url),
            direction,
            linked_addr(counterparty, ctx.base_url),
            amount,
            sym,
        );
    }
}

pub fn print_balance(
    item: &Positioned<EvmEvent, EvmPosition>,
    balance: U256,
    index: usize,
    ctx: &FmtContext,
) {
    let e = &item.item;
    if let Some(t) = decode_transfer(e) {
        let Some(meta) = ctx.metadata.get(&e.contract_address) else {
            return;
        };
        let direction = if &t.to == ctx.owner { " IN" } else { "OUT" };
        let counterparty = if &t.to == ctx.owner { &t.from } else { &t.to };
        let amount = format_amount(
            t.amount,
            meta.decimals,
            AMOUNT_WHOLE_WIDTH,
            ctx.max_decimals,
        );
        let balance_str = format_amount(
            balance,
            meta.decimals,
            BALANCE_WHOLE_WIDTH,
            ctx.max_decimals,
        );
        let sym = format!("{:<width$}", meta.symbol, width = ctx.max_sym_width);

        println!(
            "[{:>4}] {:<10} {}  {}  {:>3}  {}  {} {} = {}",
            index,
            e.block_number,
            format_timestamp(e.timestamp),
            linked_tx_hash(&e.tx_hash, ctx.base_url),
            direction,
            linked_addr(counterparty, ctx.base_url),
            amount,
            sym,
            balance_str,
        );
    }
}
