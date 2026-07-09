//! Height-space numeric newtypes shared by every chain family.
//!
//! Moved out of `evm::types` when the Bitcoin source landed: a block height
//! is the same concept on every linear chain, so the range index, the cache
//! coverage bookkeeping, and each family's chain client all speak these
//! types.

use std::ops::{Add, Sub};

use crate::arch::Position;

// ============================================================
// BlockNumber
// ============================================================

/// A block height. A newtype over `u64` so it can't be silently swapped with
/// a chain id, a log index, a timestamp, or any other bare integer as it
/// flows through positions, events, headers, the range tree, and the cache.
///
/// It is part of the EVM `EvmPosition`, so `#[serde(transparent)]` keeps the
/// marshalled form identical to the pre-newtype `u64` and saved
/// resume-anchors loadable.
///
/// Arithmetic is *affine*: a block ± a `u64` offset is another block ([`Add`] /
/// [`Sub`] / [`BlockNumber::saturating_add`] / [`BlockNumber::saturating_sub`]
/// / [`BlockNumber::checked_sub`]); the *distance* between two blocks is a
/// `u64`, computed via [`BlockNumber::get`]. Convert to `u64` at the RPC
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

/// Block + a typed count of blocks = block — the affine algebra extended to
/// [`BlockCount`], so window arithmetic reads
/// `from_block + num_blocks - 1` with no `.get()` unwrapping.
impl Add<BlockCount> for BlockNumber {
    type Output = Self;
    fn add(self, rhs: BlockCount) -> Self {
        Self(self.0 + rhs.get())
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
        // survive — a plain `write!` would silently drop a `{:>12}` width.
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// [`BlockNumber`] is the public position of the block histories
/// (`HistStore<_, BlockNumber>`): block `N` sits *at* position `N`.
///
/// Wart, mirroring the EVM position's `(0, 0)` earliest-sentinel convention:
/// [`BlockNumber::ZERO`] doubles as the lower-bound sentinel ("before all
/// blocks") and as genesis's own position, so `split(ZERO)` excludes genesis
/// and a resume anchor of `ZERO` replays it. Harmless in practice — no real
/// query starts a window at genesis — so documented rather than engineered
/// around.
impl Position for BlockNumber {
    fn marshal(&self) -> serde_json::Value {
        // `#[serde(transparent)]` — a bare JSON number.
        serde_json::to_value(self).expect("BlockNumber serialization cannot fail")
    }
}

impl BlockNumber {
    /// Reconstruct from a [`Position::marshal`] value. Called by the owning
    /// store's `load` (deserialization lives on the store, not the position).
    pub fn from_value(value: serde_json::Value) -> Option<Self> {
        serde_json::from_value(value).ok()
    }
}

/// An inclusive block range `[from, to]`.
///
/// Names the two ends of what was an unnamed `(u64, u64)` in the scanned-range
/// and gap-fill bookkeeping. The `SQLite` layer and the RPC boundary still
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

impl std::fmt::Display for BlockRange {
    /// Inclusive `from..=to`, matching Rust range syntax.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..={}", self.from, self.to)
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
        // survive — see the note on `BlockNumber`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}

// ============================================================
// BlockCount
// ============================================================

/// A positive count of blocks — a window size, a batch size.
///
/// Like [`ChunkSize`], a count is a *distance* in the affine [`BlockNumber`]
/// algebra, hence not a `BlockNumber`; the two counts stay distinct types
/// because they play different roles (a fetch-alignment granularity vs. a
/// how-many-blocks quantity) and neither should be swappable with the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockCount(std::num::NonZeroU64);

impl BlockCount {
    /// Wrap a block count.
    ///
    /// # Panics
    /// Panics if `n == 0` — a zero-block window/batch is meaningless.
    /// `const`-evaluable, so literal misuse fails at compile time.
    pub const fn new(n: u64) -> Self {
        match std::num::NonZeroU64::new(n) {
            Some(v) => Self(v),
            None => panic!("block count must be positive"),
        }
    }

    /// The inner count, for the window/batch arithmetic boundary.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for BlockCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `BlockNumber`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}

// ============================================================
// TxCount
// ============================================================

/// A count of transactions in a block. A newtype over `u32` so it can't be
/// silently swapped with a log index or any other bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxCount(u32);

impl TxCount {
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    /// From a transactions-list length (the RPC boundary). Saturates at
    /// `u32::MAX` — no real block approaches 4B transactions, but a
    /// misbehaving RPC shouldn't panic us.
    pub fn from_len(len: usize) -> Self {
        Self(u32::try_from(len).unwrap_or(u32::MAX))
    }
}

impl std::fmt::Display for TxCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Forward to the inner Display so formatter flags (width, alignment)
        // survive — see the note on `BlockNumber`'s Display.
        std::fmt::Display::fmt(&self.0, f)
    }
}
