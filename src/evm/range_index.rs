//! Generic in-memory range tree over EVM range-based sources (logs, blocks).
//!
//! A `BTreeMap` keyed by the *end* position of each [`Range`], with
//! singleflight + late-item handling, generic over a [`RangeSpec`]. The tree
//! holds lightweight [`RangeSpec::Item`]s (a cache pointer for logs); the full
//! value is reconstructed on demand via [`RangeSpec::resolve`], so a
//! long-running history's resident memory is bounded by the number of distinct
//! positions (eviction is a future addition).
//!
//! Lazy loading: rather than slurping every scanned range from the durable
//! store on first access, the tree materializes per-window on demand via
//! [`RangeIndex::ensure_window_loaded`].
//!
//! Split across submodules:
//! - [`spec`] — the source-injected [`RangeSpec`]/[`IndexPos`] seam + [`Fetcher`].
//! - [`block_pos`] — the concrete EVM-log position/pointer types
//!   ([`BlockPos`], [`LogRef`], [`LogPage`]).
//! - [`range_tree`] — the [`Range`] entry type, page insertion (four late-item
//!   cases), the lazy load of scanned ranges, and the gap placeholder.
//! - [`gap_fill`] — [`RangeIndex::ensure_cached`], the [`RangeIndex::items`]
//!   async stream, and the fetcher-driven singleflight.
//!
//! This file owns the [`RangeIndex`] struct itself, its constructors and
//! accessors, [`RangeIndex::resolve`], and the async drain to the durable store.

use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::arch::{HistoryError, Positioned};
use crate::evm::types::{BlockRange, ChunkSize};

mod block_pos;
mod gap_fill;
mod log_spec;
mod range_tree;
mod spec;
mod store;

pub use block_pos::{BlockPos, LogPage, LogRef};
pub use log_spec::LogSpec;
pub use spec::{Fetcher, IndexPos, LoadedSpan, RangeSpec};
pub use store::{RangeIndexStore, TimePosFn};

use range_tree::Range;

// ============================================================
// RangeIndex
// ============================================================

/// Per-source in-memory range tree over a [`RangeSpec`] `S`.
///
/// One instance per `(durable store, filter)` pair. Cheap to construct — the
/// scanned ranges are pulled into the tree on demand by
/// [`Self::ensure_window_loaded`].
pub struct RangeIndex<S: RangeSpec> {
    spec: S,
    fetcher: Fetcher<S::Page>,
    /// Aligned scanned-range chunk size. Determines the granularity at
    /// which gaps get fetched and the alignment of scanned-range rows.
    chunk_size: ChunkSize,
    tree: Mutex<BTreeMap<S::Pos, Range<S>>>,
    /// Block windows already materialized into the tree. Tracked so a second
    /// visit to the same window does not re-slurp.
    loaded_windows: Mutex<Vec<BlockRange>>,
    /// Optional async drain. When attached, every page passed to
    /// [`Self::insert_page`] is cloned onto the channel and persisted by a
    /// background task. `None` means purely in-memory (the gap-fill path
    /// persists inline via [`RangeSpec::persist`]).
    drain_tx: Mutex<Option<UnboundedSender<S::Page>>>,
}

impl<S: RangeSpec> RangeIndex<S> {
    /// Construct an empty index over `spec`, fetching absent ranges via
    /// `fetcher`. Aligned chunk size defaults to `1000` blocks.
    pub fn new(spec: S, fetcher: Fetcher<S::Page>) -> Self {
        Self::with_chunk_size(spec, fetcher, ChunkSize::new(1000))
    }

    /// Like [`Self::new`] but with an explicit aligned chunk size.
    pub fn with_chunk_size(spec: S, fetcher: Fetcher<S::Page>, chunk_size: ChunkSize) -> Self {
        Self {
            spec,
            fetcher,
            chunk_size,
            tree: Mutex::new(BTreeMap::new()),
            loaded_windows: Mutex::new(Vec::new()),
            drain_tx: Mutex::new(None),
        }
    }

    /// The spec this index delegates storage/resolution to.
    pub(super) fn spec(&self) -> &S {
        &self.spec
    }

    /// Resolve a stored [`Item`](RangeSpec::Item) to the public positioned
    /// value — delegates to [`RangeSpec::resolve`] (synchronous cache read).
    pub(super) fn resolve(
        &self,
        item: &S::Item,
    ) -> Result<Positioned<S::Event, S::PubPos>, HistoryError> {
        self.spec.resolve(item)
    }

    /// Spawn a background task that persists every page passed to
    /// [`Self::insert_page`] via [`RangeSpec::persist`]. Returns `false` (and
    /// skips spawning) if a drain is already attached. Persistence failures
    /// are logged but never propagated — the in-memory tree is the source of
    /// truth on the live path.
    pub fn attach_drain(&self) -> bool {
        let mut drain = self.drain_tx.lock().unwrap();
        if drain.is_some() {
            return false;
        }
        let (tx, mut rx) = unbounded_channel::<S::Page>();
        let spec = self.spec.clone();
        tokio::spawn(async move {
            while let Some(page) = rx.recv().await {
                if let Err(e) = spec.persist(&page) {
                    tracing::warn!(error = %e, "range_index drain persist");
                }
            }
        });
        *drain = Some(tx);
        true
    }

    /// Whether a drain task is currently attached. Test helper.
    #[cfg(test)]
    pub(crate) fn has_drain(&self) -> bool {
        self.drain_tx.lock().unwrap().is_some()
    }

    /// Number of ranges currently in the tree. Test helper.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tree.lock().unwrap().len()
    }

    /// Whether the block window `[range.from, range.to]` has been
    /// materialized. Test helper.
    #[cfg(test)]
    pub(crate) fn window_loaded(&self, range: BlockRange) -> bool {
        self.loaded_windows
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.from <= range.from && w.to >= range.to)
    }
}

#[cfg(test)]
mod tests;
