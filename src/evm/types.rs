//! Concrete EVM types: position, raw event, block/tx, ERC20 metadata + events.
//!
//! Source-agnostic core lives in [`crate::arch`]; this module is the
//! EVM-specific counterpart referenced by the rest of `crate::evm`.

use std::sync::Arc;

use crate::arch::{Position, Timestamp};

// ============================================================
// EvmPosition — totally ordered (block, log_index)
// ============================================================

/// Position in an EVM log stream. Totally ordered: `(block, log_index)`
/// lexicographically.
///
/// Items in a [`Hist<_, EvmPosition>`](crate::arch::Hist) are at positions in
/// `(start, end]`. The lower-bound sentinel for "before all logs on
/// this chain" is `EvmPosition { block: 0, log_index: 0 }` — no real
/// EVM chain has a log at genesis-block / index-0 (genesis carries no
/// transactions on any production chain).
///
/// Layer note: this struct carries *only* the coordinates needed to
/// totally-order events on one chain. Chain identity lives on the
/// [`EvmLogHistory`](crate::evm::EvmLogHistory) producing the stream;
/// block hashes live in [`LogCache`](crate::evm::LogCache); timestamps
/// live on [`EvmEvent`].
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct EvmPosition {
    pub block: u64,
    pub log_index: u32,
}

impl EvmPosition {
    pub const fn new(block: u64, log_index: u32) -> Self {
        Self { block, log_index }
    }

    /// Lower-bound sentinel: strictly less than every real log position.
    pub const fn earliest() -> Self {
        Self {
            block: 0,
            log_index: 0,
        }
    }
}

impl Position for EvmPosition {
    fn marshal(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("EvmPosition serialization cannot fail")
    }

    fn unmarshal(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

// ============================================================
// Raw EVM event
// ============================================================

/// A single log event. Carries its own (block, `log_index`, `block_hash`,
/// timestamp) so consumers don't need to round-trip the store for them.
#[derive(Debug, Clone)]
pub struct EvmEvent {
    pub contract_address: [u8; 20],
    pub block_number: u64,
    pub timestamp: Timestamp,
    pub block_hash: [u8; 32],
    pub log_index: u32,
    pub tx_hash: [u8; 32],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

impl EvmEvent {
    pub fn position(&self) -> EvmPosition {
        EvmPosition {
            block: self.block_number,
            log_index: self.log_index,
        }
    }
}

// ============================================================
// Block / Transaction
// ============================================================

#[derive(Debug, Clone)]
pub struct EvmBlock {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone)]
pub struct EvmTransaction {
    pub hash: [u8; 32],
    pub from: [u8; 20],
    pub to: Option<[u8; 20]>,
    pub value: u128,
}

// ============================================================
// Asset / Amount
// ============================================================

pub trait Asset: Send + Sync {
    fn symbol(&self) -> &str;
    fn name(&self) -> &str;
}

/// Asset-aware fixed-point amount.
#[derive(Clone)]
pub struct Amount {
    pub raw: u128,
    pub decimals: u8,
    pub asset: Arc<dyn Asset>,
}

impl std::fmt::Debug for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Amount({} {})", self, self.asset.symbol())
    }
}

impl std::fmt::Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.decimals == 0 {
            return write!(f, "{}", self.raw);
        }
        let divisor = 10u128.pow(self.decimals as u32);
        let whole = self.raw / divisor;
        let frac = self.raw % divisor;
        write!(
            f,
            "{}.{:0>width$}",
            whole,
            frac,
            width = self.decimals as usize
        )
    }
}

// ============================================================
// ERC20 events
// ============================================================

#[derive(Debug, Clone)]
pub struct Erc20Transfer {
    pub event: EvmEvent,
    pub token: Arc<Erc20>,
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct Erc20Mint {
    pub event: EvmEvent,
    pub token: Arc<Erc20>,
    pub to: [u8; 20],
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct Erc20Burn {
    pub event: EvmEvent,
    pub token: Arc<Erc20>,
    pub from: [u8; 20],
    pub amount: Amount,
}

// ============================================================
// Erc20 / OwnedErc20 (metadata only — backends live elsewhere in src/evm)
// ============================================================

#[derive(Debug)]
pub struct Erc20 {
    pub address: [u8; 20],
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
}

impl Asset for Erc20 {
    fn symbol(&self) -> &str {
        &self.symbol
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Reduced state: a running account balance.
#[derive(Clone)]
pub struct Erc20Balance {
    pub token: Arc<Erc20>,
    pub owner: [u8; 20],
    pub balance: Amount,
}
