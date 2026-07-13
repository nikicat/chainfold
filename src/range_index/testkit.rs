//! Synthetic [`RangeSpec`] fixture for the `range_index` test suites.
//!
//! [`MockSpec`] mirrors the log-shaped *sparse* case — several items per
//! block, ordered by an intra-block index — with zero source dependencies:
//! items resolve by pure copy and persistence goes to an in-memory
//! [`MockStore`]. This keeps the generic tree/store tests featureless, per
//! the source-agnostic-core testing rule (`MockPos`, not `EvmPosition`).
//! Source-specific resolution semantics (e.g. `LogSpec`'s cache indirection
//! and headers-present invariant) are covered by the specs' own test
//! modules in their source trees.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::spec::{IndexPos, LoadedSpan, RangeSpec};
use crate::arch::{HistoryError, Position, Positioned};
use crate::chain::{BlockNumber, BlockRange};

/// Sparse synthetic tree key: `(block, idx)` with a universal lower bound —
/// the same shape as the EVM log tree's `BlockPos`, minus the EVM.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(super) enum MockPos {
    Bottom,
    At { block: BlockNumber, idx: u32 },
}

impl MockPos {
    pub(super) fn at(block: BlockNumber, idx: u32) -> Self {
        MockPos::At { block, idx }
    }
}

impl IndexPos for MockPos {
    fn bottom() -> Self {
        MockPos::Bottom
    }

    fn before_block(block: BlockNumber) -> Self {
        match block.checked_sub(1) {
            Some(prev) => MockPos::At {
                block: prev,
                idx: u32::MAX,
            },
            None => MockPos::Bottom,
        }
    }

    fn end_of_block(block: BlockNumber) -> Self {
        MockPos::At {
            block,
            idx: u32::MAX,
        }
    }

    fn fetch_bounds(start: Self, end: Self) -> BlockRange {
        let from = match start {
            MockPos::Bottom => BlockNumber::ZERO,
            MockPos::At { block, idx } => {
                if idx == u32::MAX {
                    block.saturating_add(1)
                } else {
                    block
                }
            }
        };
        let to = match end {
            MockPos::Bottom => BlockNumber::ZERO,
            MockPos::At { block, .. } => block,
        };
        BlockRange::new(from, to)
    }
}

/// The tree item *and* the resolved event — all-`Copy`, so
/// [`MockSpec::resolve`] is a pure copy like a dense blocks spec's.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub(super) struct MockItem {
    pub block: BlockNumber,
    pub idx: u32,
}

impl MockItem {
    pub(super) fn new(block: BlockNumber, idx: u32) -> Self {
        Self { block, idx }
    }
}

/// Public position of the mock stream — `(block, idx)` lexicographic, the
/// synthetic analog of `EvmPosition`.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub(super) struct MockPubPos {
    pub block: BlockNumber,
    pub idx: u32,
}

impl MockPubPos {
    pub(super) fn new(block: BlockNumber, idx: u32) -> Self {
        Self { block, idx }
    }
}

impl Position for MockPubPos {
    fn marshal(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("MockPubPos serialization cannot fail")
    }
}

/// A page of mock items covering the inclusive `range`.
#[derive(Clone)]
pub(super) struct MockPage {
    pub range: BlockRange,
    pub items: Vec<MockItem>,
}

/// In-memory stand-in for the durable layer: items + scanned-range coverage,
/// mirroring the narrow store surface a spec's `persist`/`load_window` use.
#[derive(Default)]
pub(super) struct MockStore {
    items: Mutex<BTreeSet<MockItem>>,
    scanned: Mutex<Vec<BlockRange>>,
    fail_next_persist: AtomicBool,
}

impl MockStore {
    /// Make the next `persist` call fail once (self-clearing) — for the
    /// persist-error paths of the gap fill.
    pub(super) fn fail_next_persist(&self) {
        self.fail_next_persist.store(true, Ordering::SeqCst);
    }

    /// Pre-populate items as if a previous process had persisted them.
    pub(super) fn insert_items(&self, items: &[MockItem]) {
        self.items.lock().unwrap().extend(items.iter().copied());
    }

    /// Pre-record coverage as if a previous process had scanned `range`.
    pub(super) fn record_scanned(&self, range: BlockRange) {
        self.scanned.lock().unwrap().push(range);
    }

    /// Recorded coverage, in insertion order.
    pub(super) fn scanned_ranges(&self) -> Vec<BlockRange> {
        self.scanned.lock().unwrap().clone()
    }

    /// Persisted items with blocks in `range` (inclusive), ascending.
    pub(super) fn items_in(&self, range: BlockRange) -> Vec<MockItem> {
        self.items
            .lock()
            .unwrap()
            .iter()
            .filter(|i| i.block >= range.from && i.block <= range.to)
            .copied()
            .collect()
    }
}

/// Synthetic sparse [`RangeSpec`] over a [`MockStore`].
#[derive(Clone)]
pub(super) struct MockSpec {
    store: Arc<MockStore>,
}

impl MockSpec {
    pub(super) fn new(store: Arc<MockStore>) -> Self {
        Self { store }
    }
}

impl RangeSpec for MockSpec {
    type Pos = MockPos;
    type Item = MockItem;
    type Event = MockItem;
    type PubPos = MockPubPos;
    type Page = MockPage;

    fn item_pos(item: &MockItem) -> MockPos {
        MockPos::at(item.block, item.idx)
    }

    fn page_items(page: &MockPage) -> Vec<MockItem> {
        page.items.clone()
    }

    fn page_bounds(page: &MockPage) -> BlockRange {
        page.range
    }

    fn resolve(&self, item: &MockItem) -> Result<Positioned<MockItem, MockPubPos>, HistoryError> {
        Ok(Positioned::new(
            *item,
            MockPubPos::new(item.block, item.idx),
        ))
    }

    fn persist(&self, page: &MockPage) -> Result<(), HistoryError> {
        if self.store.fail_next_persist.swap(false, Ordering::SeqCst) {
            return Err(HistoryError::internal("scripted persist failure"));
        }
        self.store.insert_items(&page.items);
        self.store.record_scanned(page.range);
        Ok(())
    }

    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<MockItem>>, HistoryError> {
        let mut spans = Vec::new();
        for range in self.store.scanned_ranges() {
            if range.to < window.from || range.from > window.to {
                continue;
            }
            spans.push(LoadedSpan {
                range,
                items: self.store.items_in(range),
            });
        }
        Ok(spans)
    }

    fn load_pos(&self, value: serde_json::Value) -> Option<MockPubPos> {
        serde_json::from_value(value).ok()
    }

    fn index_pos(p: &MockPubPos) -> MockPos {
        if *p == MockPubPos::default() {
            MockPos::Bottom
        } else {
            MockPos::at(p.block, p.idx)
        }
    }

    fn head_pos(n: BlockNumber) -> MockPubPos {
        MockPubPos::new(n, u32::MAX)
    }

    fn window(from: &MockPubPos, to: &MockPubPos) -> BlockRange {
        BlockRange::new(from.block, to.block)
    }
}
