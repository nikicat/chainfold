//! Cached-only navigation: the four `first_*` / `last_*` entry points, the
//! forward/backward tree walks they share, and [`LogRef`] resolution back to
//! a full [`Log`]. None of these methods fetch — they read the tree as-is.

use std::ops::Bound;

use alloy::rpc::types::Log;

use super::LogIndex;
use super::block_pos::{BlockPos, LogRef};
use crate::arch::HistoryError;

impl LogIndex {
    // --------------------------------------------------------
    // Cached-only navigation (no fetching)
    // --------------------------------------------------------

    /// First cached log at or after `(block, log_index)`. Pure tree
    /// lookup: never triggers a fetch and never blocks on a peer's fetch.
    /// Empty (tombstone) ranges are skipped. Returns `None` once the
    /// search exits a contiguous cached region (i.e. hits a gap or the
    /// end of the tree).
    pub fn first_at_or_after(
        &self,
        block: u64,
        log_index: u32,
    ) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate_forward(BlockPos::at(block, log_index), true)
    }

    /// First cached log strictly after `(block, log_index)`.
    pub fn first_after(&self, block: u64, log_index: u32) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate_forward(BlockPos::at(block, log_index), false)
    }

    /// Last cached log at or before `(block, log_index)`.
    pub fn last_at_or_before(
        &self,
        block: u64,
        log_index: u32,
    ) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate_backward(BlockPos::at(block, log_index), true)
    }

    /// Last cached log strictly before `(block, log_index)`.
    pub fn last_before(&self, block: u64, log_index: u32) -> Result<Option<Log>, HistoryError> {
        self.cached_navigate_backward(BlockPos::at(block, log_index), false)
    }

    /// Forward variant of cached navigation. `inclusive` controls whether
    /// the pivot itself is a candidate.
    ///
    /// Tombstone ranges are skipped but participate in contiguity tracking
    /// — if the first range we inspect doesn't sit at-or-before `pivot`,
    /// the pivot is in an uncached gap and we return `None`. The same
    /// guard is reapplied before a filled range is yielded.
    fn cached_navigate_forward(
        &self,
        pivot: BlockPos,
        inclusive: bool,
    ) -> Result<Option<Log>, HistoryError> {
        let lower = if inclusive {
            Bound::Included(pivot)
        } else {
            Bound::Excluded(pivot)
        };
        let log_ref_opt = {
            let tree = self.tree.lock().unwrap();
            let mut last_end: Option<BlockPos> = None;
            let mut found = None;
            for (_, r) in tree.range((lower, Bound::Unbounded)) {
                if r.item.is_none() {
                    if let Some(prev_end) = last_end {
                        if r.start > prev_end {
                            break;
                        }
                    } else if r.start > pivot {
                        break;
                    }
                    last_end = Some(r.end);
                    continue;
                }
                let satisfies = if inclusive {
                    r.end >= pivot
                } else {
                    r.end > pivot
                };
                if !satisfies {
                    last_end = Some(r.end);
                    continue;
                }
                if let Some(prev_end) = last_end {
                    if r.start > prev_end {
                        break;
                    }
                } else if r.start > pivot {
                    break;
                }
                found = Some(r.item.expect("filled"));
                break;
            }
            found
        };
        self.resolve_opt(log_ref_opt)
    }

    /// Backward variant. Asymmetric vs. forward: the first filled range
    /// with `r.end <= pivot` is always yielded — we don't require it to
    /// abut the pivot. (The tombstone "first-range reaches pivot" check
    /// only fires when the closest cached region is empty and ends before
    /// pivot, i.e. pivot is in a gap that isn't even tombstoned.)
    fn cached_navigate_backward(
        &self,
        pivot: BlockPos,
        inclusive: bool,
    ) -> Result<Option<Log>, HistoryError> {
        let upper = if inclusive {
            Bound::Included(pivot)
        } else {
            Bound::Excluded(pivot)
        };
        let log_ref_opt = {
            let tree = self.tree.lock().unwrap();
            let mut last_start: Option<BlockPos> = None;
            let mut found = None;
            for (_, r) in tree.range((Bound::Unbounded, upper)).rev() {
                if r.item.is_none() {
                    if let Some(prev_start) = last_start {
                        if r.end < prev_start {
                            break;
                        }
                    } else if r.end < pivot {
                        break;
                    }
                    last_start = Some(r.start);
                    continue;
                }
                let satisfies = if inclusive {
                    r.end <= pivot
                } else {
                    r.end < pivot
                };
                if !satisfies {
                    last_start = Some(r.start);
                    continue;
                }
                if let Some(prev_start) = last_start
                    && r.end < prev_start
                {
                    break;
                }
                found = Some(r.item.expect("filled"));
                break;
            }
            found
        };
        self.resolve_opt(log_ref_opt)
    }

    fn resolve_opt(&self, lr: Option<LogRef>) -> Result<Option<Log>, HistoryError> {
        match lr {
            Some(lr) => self.resolve(&lr).map(Some),
            None => Ok(None),
        }
    }

    /// Resolve a [`LogRef`] to a full [`Log`] via `LogCache`. Returns an
    /// internal error if the cached block has since been marked reorged —
    /// consumers detect that condition by comparing `EvmEvent::block_hash`
    /// against what the store currently reports (see module docs on
    /// consumer-owned reorg recovery).
    pub(super) fn resolve(&self, lr: &LogRef) -> Result<Log, HistoryError> {
        match self
            .store
            .get_log_by_ref(&self.filter_hash, &lr.block_hash, lr.log_index)
            .map_err(|e| HistoryError::internal(format!("get_log_by_ref: {e}")))?
        {
            Some(log) => Ok(log),
            None => Err(HistoryError::internal(format!(
                "log_ref points to reorged block {:?} (log_index {})",
                lr.block_hash, lr.log_index
            ))),
        }
    }
}
