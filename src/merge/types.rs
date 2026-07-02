//! Shared merge vocabulary: [`SourceKey`], [`MergedItem`], [`MergedCmp`],
//! and crate-internal aliases used by both merge variants.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::arch::{HistStore, Position};

// ============================================================
// SourceKey
// ============================================================

/// Identifies one input source within a merge product.
///
/// A newtype over the source-name `String` so a source key can't be confused
/// with the other strings flowing through the merge. It is the key type of
/// the source maps handed to
/// [`merge_hist_stores`](crate::merge::merge_hist_stores) /
/// [`interleave_hist_stores`](crate::merge::interleave_hist_stores), of
/// [`MergedPos`](crate::merge::MergedPos)'s clock, and the `source_key` of
/// [`MergedItem`].
///
/// `#[serde(transparent)]` so it serializes as a bare string — a
/// [`MergedPos`](crate::merge::MergedPos) marshalled before this newtype
/// existed is byte-identical and still round-trips, keeping saved resume
/// anchors valid.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct SourceKey(pub String);

impl SourceKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SourceKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SourceKey {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl std::fmt::Display for SourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ============================================================
// MergedItem<T, P>
// ============================================================

/// Item emitted by a merged history (either variant). `source_key`
/// identifies which input source produced it; `source_pos` is that source's
/// typed position for the item.
#[derive(Clone, Debug)]
pub struct MergedItem<T, P: Position> {
    pub value: T,
    pub source_key: SourceKey,
    pub source_pos: P,
}

/// Ordering function passed to
/// [`interleave_hist_stores`](crate::merge::interleave_hist_stores). Items
/// where this returns [`Ordering::Less`] are yielded first.
pub type MergedCmp<T, P> =
    Arc<dyn Fn(&MergedItem<T, P>, &MergedItem<T, P>) -> Ordering + Send + Sync>;

// ============================================================
// Crate-internal aliases
// ============================================================

/// The keyed source set both variants merge over.
pub(crate) type Sources<T, P> = Arc<BTreeMap<SourceKey, HistStore<T, P>>>;

/// One source's tip advance: which source, and its new end position.
pub(crate) type TaggedEnd<P> = (SourceKey, P);
