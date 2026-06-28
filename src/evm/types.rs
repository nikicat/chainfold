//! Concrete EVM types: position, raw event, block/tx, ERC20 metadata + events.
//!
//! Source-agnostic core lives in [`crate::arch`]; this module is the
//! EVM-specific counterpart referenced by the rest of `crate::evm`.

use std::sync::Arc;

use alloy::primitives::{Address, B256, BlockHash, TxHash};

use crate::arch::{Position, Timestamp};

// ============================================================
// ChainId
// ============================================================

/// An EVM chain id — the value `eth_chainId` returns. A newtype over `u64`
/// so it can't be silently swapped with a block number, log index, or any
/// other bare integer, and so APIs that key on chain say so in their types.
///
/// Convert at the edges: [`ChainId::new`] from a resolved/fetched `u64`,
/// [`ChainId::get`] back to `u64` for `SQLite` columns and RPC-URL formatting.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ChainId(u64);

impl ChainId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ChainId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

// ============================================================
// Topic
// ============================================================

/// An EVM log topic (`topic0`..`topic3`). A real newtype over [`B256`] —
/// unlike block/transaction hashes, which use alloy's [`BlockHash`]/[`TxHash`]
/// *aliases* (documentation only) — because a topic is a distinct concept that
/// flows through chainfold's own filter API, and confusing one with a hash
/// would be a genuine bug. alloy itself models all three as a bare `B256`.
///
/// Unwrap to the inner `B256` with [`Topic::b256`] at the alloy `Filter`/`Log`
/// boundary; everything chainfold-facing speaks `Topic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Topic(B256);

impl Topic {
    pub const fn new(value: B256) -> Self {
        Self(value)
    }

    /// The inner 32-byte value, for the alloy boundary.
    pub const fn b256(self) -> B256 {
        self.0
    }

    /// Raw bytes, for hashing / `SQLite` binds.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Interpret this topic as an indexed `address` event parameter — the low
    /// 20 bytes (alloy's [`Address::from_word`]). Inverse of `Topic::from(addr)`.
    pub fn address(self) -> Address {
        Address::from_word(self.0)
    }
}

impl From<B256> for Topic {
    fn from(value: B256) -> Self {
        Self(value)
    }
}

impl From<Address> for Topic {
    /// An indexed `address` event parameter is the address left-padded with 12
    /// zero bytes (alloy's [`Address::into_word`]).
    fn from(address: Address) -> Self {
        Self(address.into_word())
    }
}

impl From<Topic> for B256 {
    fn from(topic: Topic) -> Self {
        topic.0
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl std::str::FromStr for Topic {
    type Err = <B256 as std::str::FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<B256>().map(Self)
    }
}

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
    pub contract_address: Address,
    pub block_number: u64,
    pub timestamp: Timestamp,
    pub block_hash: BlockHash,
    pub log_index: u32,
    pub tx_hash: TxHash,
    pub topics: Vec<Topic>,
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
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone)]
pub struct EvmTransaction {
    pub hash: TxHash,
    pub from: Address,
    pub to: Option<Address>,
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
    pub from: Address,
    pub to: Address,
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct Erc20Mint {
    pub event: EvmEvent,
    pub token: Arc<Erc20>,
    pub to: Address,
    pub amount: Amount,
}

#[derive(Debug, Clone)]
pub struct Erc20Burn {
    pub event: EvmEvent,
    pub token: Arc<Erc20>,
    pub from: Address,
    pub amount: Amount,
}

// ============================================================
// Erc20 / OwnedErc20 (metadata only — backends live elsewhere in src/evm)
// ============================================================

#[derive(Debug)]
pub struct Erc20 {
    pub address: Address,
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
    pub owner: Address,
    pub balance: Amount,
}
