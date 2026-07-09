//! Concrete EVM types: position, raw event, block/tx, ERC20 metadata + events.
//!
//! Source-agnostic core lives in [`crate::arch`]; this module is the
//! EVM-specific counterpart referenced by the rest of `crate::evm`.

use std::sync::Arc;

use alloy::primitives::{Address, B256};

use crate::arch::{Position, Timestamp};
use crate::chain::{BlockNumber, TxCount};

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
        // Forward to the inner u64's Display so formatter flags (width, fill,
        // alignment) are honored. `write!(f, "{}", self.0)` would silently drop
        // a `{:>12}` width — e.g. the right-aligned chainId column in
        // `chains list`.
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<u64> for ChainId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

// ============================================================
// BlockHash / TxHash
// ============================================================

/// An EVM block hash. A real newtype over [`B256`] — like [`Topic`], and no
/// longer an alloy alias — so a block hash can't be confused with a
/// transaction hash, a topic, or any other 32-byte value, and so it binds and
/// reads directly at the `SQLite` boundary. Kept EVM-specific on purpose:
/// other sources' block identifiers differ in shape and display (Bitcoin's
/// byte-reversed hex, TON's base64), so each source defines its own.
///
/// Unwrap to the inner `B256` with [`BlockHash::b256`] at the alloy boundary;
/// everything chainfold-facing speaks `BlockHash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockHash(B256);

impl BlockHash {
    /// The all-zero hash — the "unknown/absent" placeholder at decode
    /// boundaries.
    pub const ZERO: Self = Self(B256::ZERO);

    pub const fn new(value: B256) -> Self {
        Self(value)
    }

    /// The inner 32-byte value, for the alloy boundary.
    pub const fn b256(self) -> B256 {
        self.0
    }

    /// Raw bytes, for hashing / hex formatting.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Every byte set to `byte` — test fixtures.
    pub const fn repeat_byte(byte: u8) -> Self {
        Self(B256::repeat_byte(byte))
    }
}

impl From<B256> for BlockHash {
    fn from(value: B256) -> Self {
        Self(value)
    }
}

impl From<BlockHash> for B256 {
    fn from(hash: BlockHash) -> Self {
        hash.0
    }
}

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl std::fmt::LowerHex for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward so `{:#x}` (explorer URLs, log lines) keeps working.
        std::fmt::LowerHex::fmt(&self.0, f)
    }
}

/// An EVM transaction hash. See [`BlockHash`] for why this is a real newtype
/// and why it stays EVM-specific (Solana identifies transactions by 64-byte
/// signatures, TON by `(logical_time, hash)` — a common tx-id type would
/// misrepresent them).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxHash(B256);

impl TxHash {
    /// The all-zero hash — the "unknown/absent" placeholder at decode
    /// boundaries.
    pub const ZERO: Self = Self(B256::ZERO);

    pub const fn new(value: B256) -> Self {
        Self(value)
    }

    /// The inner 32-byte value, for the alloy boundary.
    pub const fn b256(self) -> B256 {
        self.0
    }

    /// Raw bytes, for hashing / hex formatting.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Every byte set to `byte` — test fixtures.
    pub const fn repeat_byte(byte: u8) -> Self {
        Self(B256::repeat_byte(byte))
    }
}

impl From<B256> for TxHash {
    fn from(value: B256) -> Self {
        Self(value)
    }
}

impl From<TxHash> for B256 {
    fn from(hash: TxHash) -> Self {
        hash.0
    }
}

impl std::fmt::Display for TxHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

impl std::fmt::LowerHex for TxHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward so `{:#x}` (explorer URLs, log lines) keeps working.
        std::fmt::LowerHex::fmt(&self.0, f)
    }
}

// ============================================================
// Topic
// ============================================================

/// An EVM log topic (`topic0`..`topic3`). A real newtype over [`B256`], like
/// [`BlockHash`]/[`TxHash`] — a topic is a distinct concept that flows
/// through chainfold's own filter API, and confusing one with a hash would be
/// a genuine bug. alloy itself models all three as a bare `B256`.
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
// LogIdx
// ============================================================

/// A log's index within its block (the EVM `log_index`). A newtype over `u32`
/// so it can't be silently swapped with a block number or any other bare
/// integer as it flows through positions, events, and the range tree.
///
/// It is part of [`EvmPosition`], and therefore of its marshalled form —
/// hence `#[serde(transparent)]`, so saved resume-anchors stay identical
/// to the pre-newtype `u32` encoding. [`LogIdx::MAX`] is the end-of-block
/// sentinel (`BlockPos::end_of_block`, tip positions): strictly greater than
/// every real log index in a block.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct LogIdx(u32);

impl LogIdx {
    /// The first log index in a block.
    pub const ZERO: Self = Self(0);
    /// End-of-block sentinel: strictly greater than every real log index.
    pub const MAX: Self = Self(u32::MAX);

    pub const fn new(idx: u32) -> Self {
        Self(idx)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for LogIdx {
    fn from(idx: u32) -> Self {
        Self(idx)
    }
}

impl std::fmt::Display for LogIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags survive — see the
        // note on `ChainId`'s Display.
        std::fmt::Display::fmt(&self.0, f)
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
    pub block: BlockNumber,
    pub log_index: LogIdx,
}

impl EvmPosition {
    /// Coordinate constructor. `impl Into` on both parameters so call sites
    /// pass domain values (`BlockNumber`, `LogIdx`) directly *and* raw
    /// `u64`/`u32` literals keep working in tests — no `.get()` unwrapping
    /// on either side.
    pub fn new(block: impl Into<BlockNumber>, log_index: impl Into<LogIdx>) -> Self {
        Self {
            block: block.into(),
            log_index: log_index.into(),
        }
    }

    /// Lower-bound sentinel: strictly less than every real log position.
    pub const fn earliest() -> Self {
        Self {
            block: BlockNumber::ZERO,
            log_index: LogIdx::ZERO,
        }
    }
}

impl Position for EvmPosition {
    fn marshal(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("EvmPosition serialization cannot fail")
    }
}

impl EvmPosition {
    /// Reconstruct from a [`Position::marshal`] value. Called by the owning
    /// store's `load` (deserialization lives on the store, not the position).
    pub fn from_value(value: serde_json::Value) -> Option<Self> {
        serde_json::from_value(value).ok()
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
    pub block_number: BlockNumber,
    pub timestamp: Timestamp,
    pub block_hash: BlockHash,
    pub log_index: LogIdx,
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
// Block scalars — Gas / GasPrice
// ============================================================

/// A quantity of gas units (a block's `gas_used` / `gas_limit`). A newtype
/// over `u64` so gas can't be silently swapped with a block number, a
/// timestamp, or a [`GasPrice`] (units vs. price — confusing the two is a
/// genuine bug).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gas(u64);

impl Gas {
    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Gas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `ChainId`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// A gas price in wei-per-gas (a block's EIP-1559 `base_fee_per_gas`; later
/// also effective/priority prices). A newtype over `u64` — alloy's header
/// carries `base_fee_per_gas: Option<u64>` — kept distinct from [`Gas`]
/// (price vs. units).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GasPrice(u64);

impl GasPrice {
    pub const fn new(wei: u64) -> Self {
        Self(wei)
    }

    /// The raw wei-per-gas value, for formatting and the `SQLite` boundary.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for GasPrice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `ChainId`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}

// ============================================================
// EVM block
// ============================================================

/// One EVM block: header facts + transaction count. The item type of the
/// blocks history (`HistStore<EvmBlock, BlockNumber>`); its position is its
/// own [`number`](Self::number).
///
/// All-`Copy` (~136 bytes) so it can be stored directly in the range tree as
/// the `RangeSpec::Item` — resolving a block is then a pure copy, with no
/// per-row `SQLite` read (unlike logs' `LogRef` indirection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvmBlock {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: Timestamp,
    pub tx_count: TxCount,
    pub gas_used: Gas,
    pub gas_limit: Gas,
    /// EIP-1559 base fee; `None` on pre-London blocks (and chains without it).
    pub base_fee: Option<GasPrice>,
    /// The block's beneficiary (`miner` field in the RPC block).
    pub miner: Address,
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
