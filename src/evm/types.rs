//! Concrete EVM types: position, raw event, block/tx, ERC20 metadata + events.
//!
//! Source-agnostic core lives in [`crate::arch`]; this module is the
//! EVM-specific counterpart referenced by the rest of `crate::evm`.

use std::ops::{Add, Sub};
use std::sync::Arc;

use alloy::primitives::{Address, B256};

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
// FilterHash
// ============================================================

/// A chainfold cache **partition key**: the BLAKE3 hash of a log filter's
/// `(address, topic0..3)`. A newtype over `[u8; 32]`, *not* a [`B256`] or a
/// `BlockHash`/`TxHash` alias — a filter hash never appears on-chain, so
/// confusing it with an EVM hash would be a category error.
///
/// Scopes every cached log, scanned range, and in-memory tree to one filter.
/// Compute one with
/// [`compute_filter_hash`](crate::evm::cache::compute_filter_hash); it is
/// threaded through [`LogCache`](crate::evm::LogCache) and
/// [`LogIndex`](crate::evm::LogIndex). It binds to `SQLite` as a 32-byte BLOB
/// via a `ToSql` impl that lives in the cache layer, keeping this type
/// persistence-free (the same split [`ChainId`] uses).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterHash([u8; 32]);

impl FilterHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 bytes — for hashing and the cache layer's `SQLite` binds.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Byte slice view, for `SQLite` binds and `hex` formatting.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for FilterHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A short prefix is enough to correlate logs; the full 32 bytes are
        // rarely useful in diagnostics.
        write!(f, "FilterHash(0x{})", hex::encode(&self.0[..8]))
    }
}

// ============================================================
// BlockNumber
// ============================================================

/// A block height. A newtype over `u64` so it can't be silently swapped with a
/// [`ChainId`], a [`LogIdx`], a timestamp, or any other bare integer as it
/// flows through positions, events, headers, the range tree, and the cache.
///
/// It is part of [`EvmPosition`], so `#[serde(transparent)]` keeps the
/// marshalled form identical to the pre-newtype `u64` and saved resume-anchors
/// loadable.
///
/// Arithmetic is *affine*: a block ± a `u64` offset is another block ([`Add`] /
/// [`Sub`] / [`BlockNumber::saturating_add`] / [`BlockNumber::saturating_sub`]
/// / [`BlockNumber::checked_sub`]); the *distance* between two blocks is a
/// `u64`, computed via [`BlockNumber::get`]. Convert to `u64` at the alloy RPC
/// and `SQLite` edges with [`BlockNumber::get`].
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
pub struct BlockNumber(u64);

impl BlockNumber {
    /// Genesis / lower-bound sentinel.
    pub const ZERO: Self = Self(0);

    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// `self + rhs`, saturating at `u64::MAX`.
    pub const fn saturating_add(self, rhs: u64) -> Self {
        Self(self.0.saturating_add(rhs))
    }

    /// `self - rhs`, saturating at `0`.
    pub const fn saturating_sub(self, rhs: u64) -> Self {
        Self(self.0.saturating_sub(rhs))
    }

    /// `self - rhs`, or `None` if it would underflow below genesis.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }
}

impl From<u64> for BlockNumber {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl Add<u64> for BlockNumber {
    type Output = Self;
    fn add(self, rhs: u64) -> Self {
        Self(self.0 + rhs)
    }
}

impl Sub<u64> for BlockNumber {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self {
        Self(self.0 - rhs)
    }
}

impl std::fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `ChainId`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// An inclusive block range `[from, to]`.
///
/// Names the two ends of what was an unnamed `(u64, u64)` in the scanned-range
/// and gap-fill bookkeeping. The `SQLite` layer and the alloy RPC boundary still
/// speak raw `u64`; convert at the seam with [`from_u64`](Self::from_u64) /
/// [`as_u64`](Self::as_u64).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub from: BlockNumber,
    pub to: BlockNumber,
}

impl BlockRange {
    pub const fn new(from: BlockNumber, to: BlockNumber) -> Self {
        Self { from, to }
    }

    /// From raw block heights (`SQLite` rows, RPC bounds, aligned windows).
    pub const fn from_u64(from: u64, to: u64) -> Self {
        Self {
            from: BlockNumber::new(from),
            to: BlockNumber::new(to),
        }
    }

    /// Raw `(from, to)` heights for the `SQLite` / RPC / arithmetic boundary.
    pub const fn as_u64(self) -> (u64, u64) {
        (self.from.get(), self.to.get())
    }
}

// ============================================================
// ChunkSize
// ============================================================

/// A gap-fetch / scanned-range alignment granularity — a positive count of
/// blocks.
///
/// A newtype so a chunk size can't be swapped with a block height
/// ([`BlockNumber`]) or any other bare integer, and so the "must be
/// positive" invariant is structural (`NonZeroU64` — the alignment math
/// divides by it). A *count* is a distance in the affine [`BlockNumber`]
/// algebra, hence not a `BlockNumber`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkSize(std::num::NonZeroU64);

impl ChunkSize {
    /// Wrap a block count.
    ///
    /// # Panics
    /// Panics if `n == 0` — a zero alignment granularity is meaningless.
    /// `const`-evaluable, so literal misuse fails at compile time.
    pub const fn new(n: u64) -> Self {
        match std::num::NonZeroU64::new(n) {
            Some(v) => Self(v),
            None => panic!("chunk size must be positive"),
        }
    }

    /// The inner block count, for the alignment arithmetic boundary.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for ChunkSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `ChainId`'s Display.
        std::fmt::Display::fmt(&self.0, f)
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
    pub const fn new(block: u64, log_index: u32) -> Self {
        Self {
            block: BlockNumber::new(block),
            log_index: LogIdx::new(log_index),
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
