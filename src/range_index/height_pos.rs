//! [`HeightPos`] — the shared dense tree key for whole-block histories.
//!
//! A blocks source has exactly one item per height, pinned at the end of
//! its block, so the tree key needs only "before everything" and "end of
//! block N". Every blocks spec (EVM, Bitcoin, …) uses this key; the log
//! tree keeps its own richer `BlockPos` (which also orders log indexes
//! *within* a block).

use crate::chain::{BlockNumber, BlockRange};
use crate::range_index::IndexPos;

/// Position within a dense per-height item stream.
///
/// `Bottom` is the universal lower bound used as a range start covering the
/// earliest blocks (avoids the underflow problem when a scanned range
/// starts at block 0). `End(n)` sits at the end of block `n` — the position
/// of block `n` itself in a dense blocks history.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum HeightPos {
    /// Strictly below every real height.
    Bottom,
    /// End of block `n` — where block `n`'s single item sits.
    End(BlockNumber),
}

impl IndexPos for HeightPos {
    fn bottom() -> Self {
        HeightPos::Bottom
    }

    fn before_block(block: BlockNumber) -> Self {
        match block.checked_sub(1) {
            Some(prev) => HeightPos::End(prev),
            None => HeightPos::Bottom,
        }
    }

    fn end_of_block(block: BlockNumber) -> Self {
        HeightPos::End(block)
    }

    fn fetch_bounds(start: Self, end: Self) -> BlockRange {
        // A range `(start, end]` in end-of-block space covers the blocks
        // strictly after `start`'s block through `end`'s block inclusive.
        let from = match start {
            HeightPos::Bottom => BlockNumber::ZERO,
            HeightPos::End(block) => block.saturating_add(1),
        };
        let to = match end {
            HeightPos::Bottom => BlockNumber::ZERO,
            HeightPos::End(block) => block,
        };
        BlockRange::new(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn bn(n: u64) -> BlockNumber {
        BlockNumber::new(n)
    }

    #[test]
    fn ordering_bottom_below_all_heights_ascending() {
        assert!(HeightPos::Bottom < HeightPos::End(bn(0)));
        assert!(HeightPos::End(bn(0)) < HeightPos::End(bn(1)));
        assert!(HeightPos::End(bn(1)) < HeightPos::End(bn(u64::MAX)));
    }

    #[test]
    fn before_block_is_end_of_previous_and_bottom_at_genesis() {
        assert_eq!(HeightPos::before_block(bn(0)), HeightPos::Bottom);
        assert_eq!(HeightPos::before_block(bn(7)), HeightPos::End(bn(6)));
    }

    #[test]
    fn fetch_bounds_round_trip() {
        // (end_of_block(99), end_of_block(104)] covers blocks 100..=104.
        assert_eq!(
            HeightPos::fetch_bounds(
                HeightPos::end_of_block(bn(99)),
                HeightPos::end_of_block(bn(104)),
            ),
            BlockRange::new(bn(100), bn(104))
        );
        // From Bottom the fetch starts at genesis.
        assert_eq!(
            HeightPos::fetch_bounds(HeightPos::Bottom, HeightPos::end_of_block(bn(3))),
            BlockRange::new(bn(0), bn(3))
        );
    }
}
