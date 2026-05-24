//! Iterator that walks `[start, end]` forward in `batch_size`-aligned
//! block ranges.
//!
//! Two processes scanning the same filter at the same batch size record
//! matching `scanned_ranges` rows when they fetch aligned chunks, so
//! their caches can share work. The tail chunk may be partial (the
//! `is_aligned: false` signal lets callers skip cache-record for those).

/// One block range from [`aligned_block_ranges`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AlignedRange {
    /// Inclusive lower bound.
    pub from: u64,
    /// Inclusive upper bound (clamped to `end`).
    pub to: u64,
    /// `true` when `to == from + batch_size - 1`. The first chunk is
    /// aligned iff `start % batch_size == 0`; the last is aligned iff
    /// it fits a full batch.
    pub is_aligned: bool,
}

/// Yield block ranges walking forward from `start` to `end`, aligned to
/// `batch_size` boundaries.
///
/// The first range may start *before* `start` (rounded down to the
/// previous batch boundary). The last range may be a partial tail
/// (`is_aligned == false`).
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
        let (to, is_aligned) = if full_to <= end {
            (full_to, true)
        } else {
            (end, false)
        };
        let item = AlignedRange {
            from,
            to,
            is_aligned,
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
        assert!(ranges[0].is_aligned);
    }

    #[test]
    fn last_range_clamped_to_end_marks_unaligned() {
        let ranges = collect(0, 2500, 1000);
        let last = ranges.last().unwrap();
        assert_eq!(last.from, 2000);
        assert_eq!(last.to, 2500);
        assert!(!last.is_aligned);
    }

    #[test]
    fn middle_ranges_are_aligned() {
        let ranges = collect(0, 4500, 1000);
        // Every range except the tail (4000..4500) is aligned.
        for r in ranges.iter().take(ranges.len() - 1) {
            assert!(r.is_aligned, "range {r:?} should be aligned");
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
                is_aligned: false,
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
                is_aligned: true,
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
