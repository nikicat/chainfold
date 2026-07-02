//! Source-agnostic combinators over [`HistStore`](crate::arch::HistStore).
//!
//! * [`map_hist_store`] — transform each item, preserving positions.
//! * [`map_positions`] — lift positions into a wider type (the app-enum
//!   pattern feeding the cross-family interleave), preserving items.
//! * [`filter_hist_store`] — keep items matching a predicate. Each yield
//!   carries the run of preceding filtered-out items on `.skipped`.
//! * [`since_hist_store`] — pre-split each emitted `Hist` at a cut so
//!   expensive mappers sitting between `since` and the source only fire for
//!   post-cut items.
//!
//! These are pure proxies — they own no I/O. The wrapped [`HistStore`]
//! continues to provide tip stores, time anchors, and snapshot delivery.

mod filtered;
mod map_pos;
mod mapped;
mod since;

#[cfg(test)]
mod tests_common;

pub use filtered::{FilteredItem, filter_hist_store};
pub use map_pos::{PosMapper, map_positions};
pub use mapped::{MapFn, MappedItem, map_hist_store};
pub use since::since_hist_store;
