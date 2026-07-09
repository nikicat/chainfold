//! Concrete Bitcoin types: block hash, compact target, and the block item.
//!
//! Source-agnostic core lives in [`crate::arch`] and the shared height
//! types in [`crate::chain`]; this module is the Bitcoin-specific
//! counterpart referenced by the rest of `crate::bitcoin`.

use crate::arch::Timestamp;
use crate::cache::CoverageHash;
use crate::chain::{BlockNumber, TxCount};

// ============================================================
// BtcBlockHash
// ============================================================

/// A Bitcoin block hash. A real newtype over `[u8; 32]` — per the same
/// convention as the EVM `BlockHash`: block identifiers differ in shape and
/// display across sources, so each source defines its own.
///
/// The bytes are stored in **RPC/display order** — exactly the hex string
/// `getblockhash`/`getblockheader` return (which is the byte-reversed
/// internal/consensus order, the convention Bitcoin tooling displays).
/// Parsing and display are hex round-trips of the stored bytes; no reversal
/// logic exists anywhere, so none can go wrong. We never touch
/// consensus-order serialization (raw block headers) on this path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BtcBlockHash([u8; 32]);

impl BtcBlockHash {
    /// The all-zero hash — the "unknown/absent" placeholder at decode
    /// boundaries (and genesis's `prev_hash`).
    pub const ZERO: Self = Self([0u8; 32]);

    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 bytes (RPC/display order), for the `SQLite` boundary.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Byte slice view, for `SQLite` binds and hex formatting.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Every byte set to `byte` — test fixtures.
    pub const fn repeat_byte(byte: u8) -> Self {
        Self([byte; 32])
    }
}

/// A Bitcoin block hash enters the shared coverage tables as an opaque
/// [`CoverageHash`] — the conversion point where the Bitcoin hash type
/// stops and the family-agnostic persistence identity begins.
impl From<BtcBlockHash> for CoverageHash {
    fn from(hash: BtcBlockHash) -> Self {
        Self::new(hash.0)
    }
}

impl std::fmt::Display for BtcBlockHash {
    /// The 64-char lowercase hex string as the RPC returns it — no `0x`
    /// prefix (Bitcoin tooling convention).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&hex::encode(self.0))
    }
}

/// Error parsing a [`BtcBlockHash`] from hex.
#[derive(Debug, thiserror::Error)]
#[error("invalid bitcoin block hash: {0}")]
pub struct ParseBtcBlockHashError(String);

impl std::str::FromStr for BtcBlockHash {
    type Err = ParseBtcBlockHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes)
            .map_err(|e| ParseBtcBlockHashError(format!("{s:?}: {e}")))?;
        Ok(Self(bytes))
    }
}

// ============================================================
// CompactTarget
// ============================================================

/// A block's difficulty target in Bitcoin's compact "bits" encoding — the
/// exact consensus `u32` from the header. Stored instead of the RPC's
/// floating-point `difficulty` so [`BtcBlock`] stays all-`Copy` + `Eq` and
/// the persisted value is exact; the conventional difficulty number is
/// derived on demand via [`difficulty`](Self::difficulty), display-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactTarget(u32);

impl CompactTarget {
    pub const fn new(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    /// Parse the RPC's hex `bits` string (e.g. `"1d00ffff"`).
    pub fn from_hex(s: &str) -> Result<Self, std::num::ParseIntError> {
        u32::from_str_radix(s, 16).map(Self)
    }

    /// The conventional difficulty: the ratio of the difficulty-1 target
    /// (bits `0x1d00ffff`) to this target. Display-only — comparisons and
    /// storage use the exact compact bits.
    pub fn difficulty(self) -> f64 {
        let exponent = (self.0 >> 24) as i32;
        let mantissa = f64::from(self.0 & 0x00ff_ffff);
        if mantissa == 0.0 {
            return 0.0;
        }
        // target = mantissa * 256^(exponent-3); difficulty-1 target has
        // mantissa 0xffff, exponent 0x1d.
        (65535.0 / mantissa) * 256f64.powi(29 - exponent)
    }
}

impl std::fmt::Display for CompactTarget {
    /// The 8-char hex form the RPC uses (`"1d00ffff"`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&format!("{:08x}", self.0))
    }
}

// ============================================================
// Bitcoin block
// ============================================================

/// One Bitcoin block: header facts + transaction count — exactly the field
/// set one batched `getblockheader` (verbose) delivers. The item type of
/// the blocks history (`HistStore<BtcBlock, BlockNumber>`); its position is
/// its own [`height`](Self::height).
///
/// All-`Copy` (~88 bytes) so it can be stored directly in the range tree as
/// the `RangeSpec::Item` — resolving a block is then a pure copy, with no
/// per-row `SQLite` read (same design as the EVM `EvmBlock`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BtcBlock {
    pub height: BlockNumber,
    pub hash: BtcBlockHash,
    /// [`BtcBlockHash::ZERO`] at genesis (the RPC omits the field there).
    pub prev_hash: BtcBlockHash,
    /// The header's `time` field (median-time-past rules apply — not
    /// strictly monotonic block-to-block, monotonic over any 11-block
    /// window).
    pub time: Timestamp,
    pub tx_count: TxCount,
    /// The difficulty target in compact "bits" encoding.
    pub bits: CompactTarget,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_hex_round_trip_preserves_display_order() {
        // Genesis, as every explorer and the RPC display it.
        let s = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        let h: BtcBlockHash = s.parse().unwrap();
        assert_eq!(h.to_string(), s);
        // Leading zero bytes land at the front of the stored array — the
        // stored order IS the display order.
        assert_eq!(h.as_bytes()[0], 0x00);
        assert_eq!(h.as_bytes()[31], 0x6f);
    }

    #[test]
    fn hash_rejects_bad_hex() {
        assert!("zz".parse::<BtcBlockHash>().is_err());
        // Wrong length (31 bytes).
        assert!(
            "00000000000000000000000000000000000000000000000000000000000000"
                .parse::<BtcBlockHash>()
                .is_err()
        );
    }

    #[test]
    fn difficulty_one_at_limit_bits() {
        let bits = CompactTarget::from_hex("1d00ffff").unwrap();
        assert!((bits.difficulty() - 1.0).abs() < 1e-12);
        assert_eq!(bits.to_string(), "1d00ffff");
    }

    #[test]
    fn difficulty_regtest_bits_is_tiny() {
        // Regtest's 0x207fffff target is far easier than difficulty 1.
        let bits = CompactTarget::from_hex("207fffff").unwrap();
        let d = bits.difficulty();
        assert!(
            d > 0.0 && d < 1e-9,
            "regtest difficulty ≈ 4.66e-10, got {d}"
        );
    }
}
