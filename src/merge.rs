//! Merging multiple [`HistStore`](crate::arch::HistStore)s into one stream —
//! **two variants for two semantically different operations**, both
//! monomorphic over one shared item/position type (no `dyn` erasure
//! anywhere):
//!
//! * [`merge_hist_stores`] — **ordered merge**, for sources that share one
//!   *position space* (N filters on one chain: `OwnedErc20` in/out, a DEX
//!   pool's Mint/Burn/Swap/Sync, generic contract-state tracking). The
//!   space's total order *is* the stream order — no user `cmp` — and a
//!   single per-source position is a valid resume anchor for all sources at
//!   once. Anchor type: [`OrderedPos`] (position + [`SourceBound`] tag).
//! * [`interleave_hist_stores`] — **vector-clock interleave**, for sources
//!   with *disjoint* position spaces (cross-chain / cross-family: a wallet
//!   tracking a runtime-configured set of EVM/Tron/BTC/Solana addresses).
//!   Positions are incomparable across sources *even when the type matches*
//!   (block 100 on ETH vs Polygon), so item ordering is inherently
//!   best-effort via a user `cmp` (typically time, with unresolvable ties)
//!   and the honest anchor is a per-source vector clock: [`MergedPos`].
//!
//! Sources with *different* position types unify **before** the interleave
//! via an app-defined closed enum lifted with
//! [`map_positions`](crate::combinators::map_positions) — chain *sets* are
//! runtime-dynamic (via [`SourceKey`]s), but chain *families* are bounded at
//! compile time by which source impls are compiled in, so the enum costs no
//! real flexibility. (This replaced the old `ErasedAnchor`/`ErasedHistStore`
//! type-erasure layer, which had zero production users — every real merge is
//! position-homogeneous per family. Resolution of the old
//! `TODO(merge-erasure)`: monomorphize, split by position-space semantics.)
//!
//! ## Resume semantics — ordered merge
//!
//! Every yielded item carries `OrderedPos { pos: item_pos, bound: At(key) }`.
//! Because all sources live in one position space, that single tagged
//! position orders against *every* source's items: resume via
//! `hist.split(saved).1` is exact even between two items at the identical
//! inner position (e.g. the two copies of a self-transfer, which is the same
//! log surfacing through both the in- and the out-filter).
//!
//! The merged snapshot `end` is the **minimum over the sources' ends** — the
//! frontier where *all* sources are complete. A snapshot must not claim
//! coverage a lagging source hasn't reported; `snapshots()` re-emits only
//! when that minimum advances. (Under the chain design, one head clock
//! drives all of a chain's filter stores, so ends advance together in
//! practice.)
//!
//! ## Resume semantics — interleave
//!
//! Each yielded item carries a [`MergedPos`] that is a *vector clock* of
//! per-source positions at the moment of yield: the source that just emitted
//! advances to the new item's position; other sources stay at whatever they
//! last yielded (or each source's [`Default`] position if they haven't
//! yielded anything in this window yet). Saving the last yielded `MergedPos`
//! and resuming via `hist.split(pos).1` produces the exact continuation —
//! items from sources that *had* advanced past `pos` at the yield time are
//! skipped; items from other sources are not.
//!
//! ## Missing keys (interleave)
//!
//! A [`MergedPos`] saved from an older version of the interleave that omits
//! a source key (or had a different set of sources) is still usable: the
//! missing source contributes its [`Default`] position, equivalent to "start
//! from the beginning for that source." This is the
//! `merged_pos_handles_added_source` semantic pinned in the interleave tests.
//!
//! ## Position trait gotcha (interleave)
//!
//! [`Position`](crate::arch::Position)'s required [`Ord`] on [`MergedPos`] is
//! lexicographic over the sorted union of source keys, comparing each
//! per-source position with `P`'s own `Ord` (a key present on one side only
//! orders after absent). This is a total order suitable for the trait but
//! *does not* match component-wise containment (clock A can sort before
//! clock B while being ahead of B on some other source). The interleave's
//! hist backend therefore overrides
//! [`HistBackend::clamp_pos`](crate::arch::HistBackend::clamp_pos) to clamp
//! each per-source position separately, which is the correct semantic for
//! [`Hist::split`](crate::arch::Hist::split). Positions are never compared
//! across keys. Item ordering is **best-effort / presentational** (stable
//! tie-break by source key); no strict-order promise.

mod interleave;
mod kway;
mod ordered;
mod types;

#[cfg(test)]
mod tests_common;

pub use interleave::{MergedPos, interleave_hist_stores};
pub use ordered::{DiscretePosition, OrderedPos, SourceBound, merge_hist_stores};
pub use types::{MergedCmp, MergedItem, SourceKey};
