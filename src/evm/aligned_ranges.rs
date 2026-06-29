//! Iterator that walks `[start, end]` forward in `batch_size`-aligned
//! block ranges.
//!
//! Two processes scanning the same filter at the same batch size record
//! matching `scanned_ranges` rows when they fetch aligned chunks, so
//! their caches can share work. The tail chunk may be partial
//! ([`Alignment::Trailing`] lets callers skip cache-record for those).

/// Whether an [`AlignedRange`] fills a whole `batch_size` chunk or is a
/// clamped tail.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Alignment {
    /// `to == from + batch_size - 1` — a full, boundary-aligned chunk, safe
    /// to record as scanned.
    Aligned,
    /// A partial tail clamped to `end`; a later request may extend past it,
    /// so callers should not record it as a scanned range.
    Trailing,
}

impl Alignment {
    pub fn is_aligned(self) -> bool {
        matches!(self, Self::Aligned)
    }
}

/// One block range from [`aligned_block_ranges`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AlignedRange {
    /// Inclusive lower bound.
    pub from: u64,
    /// Inclusive upper bound (clamped to `end`).
    pub to: u64,
    /// Whether this range is a full aligned chunk or a clamped tail. The
    /// first chunk is aligned iff `start % batch_size == 0`; the last is
    /// aligned iff it fits a full batch.
    pub alignment: Alignment,
}

/// Yield block ranges walking forward from `start` to `end`, aligned to
/// `batch_size` boundaries.
///
/// The first range may start *before* `start` (rounded down to the
/// previous batch boundary). The last range may be a partial tail
/// ([`Alignment::Trailing`]).
pub fn aligned_block_ranges(
    start: u64,
    end: u64,
    batch_size: u64,
) -> impl Iterator<Item = AlignedRange> {
    assert!(batch_size > 0, "batch_size must be positive");
    let mut from = (start / batch_size) * batch_size;
    std::iter::from_fn(move || {
        if from > end {
            return None;
        }
        let full_to = from.saturating_add(batch_size - 1);
        let (to, alignment) = if full_to <= end {
            (full_to, Alignment::Aligned)
        } else {
            (end, Alignment::Trailing)
        };
        let item = AlignedRange {
            from,
            to,
            alignment,
        };
        from = from.saturating_add(batch_size);
        Some(item)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(start: u64, end: u64, batch: u64) -> Vec<AlignedRange> {
        aligned_block_ranges(start, end, batch).collect()
    }

    #[test]
    fn first_range_rounds_down_to_batch_boundary() {
        let ranges = collect(1500, 4500, 1000);
        assert_eq!(ranges[0].from, 1000);
        assert_eq!(ranges[0].to, 1999);
        assert!(ranges[0].alignment.is_aligned());
    }

    #[test]
    fn last_range_clamped_to_end_marks_unaligned() {
        let ranges = collect(0, 2500, 1000);
        let last = ranges.last().unwrap();
        assert_eq!(last.from, 2000);
        assert_eq!(last.to, 2500);
        assert!(!last.alignment.is_aligned());
    }

    #[test]
    fn middle_ranges_are_aligned() {
        let ranges = collect(0, 4500, 1000);
        // Every range except the tail (4000..4500) is aligned.
        for r in ranges.iter().take(ranges.len() - 1) {
            assert!(r.alignment.is_aligned(), "range {r:?} should be aligned");
            assert_eq!(r.to - r.from + 1, 1000);
        }
    }

    #[test]
    fn single_block_range() {
        let ranges = collect(100, 100, 1000);
        assert_eq!(
            ranges,
            vec![AlignedRange {
                from: 0,
                to: 100,
                alignment: Alignment::Trailing,
            }]
        );
    }

    #[test]
    fn exact_batch_boundary() {
        let ranges = collect(0, 999, 1000);
        assert_eq!(
            ranges,
            vec![AlignedRange {
                from: 0,
                to: 999,
                alignment: Alignment::Aligned,
            }]
        );
    }

    #[test]
    fn lookback_from_recent_start_bounded_by_batch() {
        // Seed scan is bounded by lookback, not genesis: with start near
        // the current tip, we should NOT scan from 0.
        let current = 18_000_000;
        let start = current - 10_000;
        let ranges: Vec<_> = collect(start, current, 1000);
        assert!(ranges.first().unwrap().from >= start - 1000);
    }
}
