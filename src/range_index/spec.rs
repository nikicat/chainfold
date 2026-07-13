//! The source-injected seam for [`RangeIndex`](super::RangeIndex).
//!
//! The range tree, singleflight gap-fill, streaming `items()`, late-item
//! insertion, lazy window-load and async drain are all generic over a
//! [`RangeSpec`]. A concrete source (EVM logs, EVM blocks, …) supplies:
//!
//! - a position type ([`IndexPos`]) that keys the tree and maps a
//!   position-gap back to an inclusive [`BlockRange`] to fetch,
//! - a lightweight [`RangeSpec::Item`] stored in the tree and the
//!   [`RangeSpec::Full`] value yielded to the consumer (logs store a
//!   `LogRef` and resolve to a full `Log`; blocks store the whole header),
//! - a [`RangeSpec::Page`] (one fetch / one live delivery),
//! - and the three I/O methods that touch the durable store:
//!   [`resolve`](RangeSpec::resolve), [`persist`](RangeSpec::persist), and
//!   [`load_window`](RangeSpec::load_window).
//!
//! Everything else — the concurrency-critical walk and the singleflight
//! placeholder — lives once, in [`RangeIndex`](super::RangeIndex).
//!
//! Block numbers cross this seam as the [`BlockNumber`] / [`BlockRange`]
//! domain types, never bare `u64`; an impl drops to `u64` only at its own
//! durable-store calls.

use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;

use futures_core::Stream;
use futures_util::stream;

use crate::arch::{HistoryError, Position, Positioned};
use crate::chain::{BlockNumber, BlockRange};

/// The tree-key position type.
///
/// Ordered cut points on one source's stream. `bottom()` is the universal
/// lower bound (`Range.start` for ranges covering the earliest blocks,
/// avoiding the block-0 underflow). `before_block` / `end_of_block` bracket
/// a block-level scanned range; `fetch_bounds` turns a position-gap
/// `(start, end]` back into the inclusive [`BlockRange`] an RPC fetch needs.
pub trait IndexPos: Copy + Ord + Eq + Hash + Debug + Send + Sync + 'static {
    /// Strictly below every real position.
    fn bottom() -> Self;

    /// The position *just before* the first item in `block` — the exclusive
    /// lower bound of a block-level scanned range starting at `block`.
    fn before_block(block: BlockNumber) -> Self;

    /// The position *at the end* of `block` — strictly greater than every
    /// item in it; the inclusive upper bound of a scanned range ending at
    /// `block`.
    fn end_of_block(block: BlockNumber) -> Self;

    /// Inclusive block range to fetch for the position-gap `(start, end]`.
    fn fetch_bounds(start: Self, end: Self) -> BlockRange;
}

/// One contiguous cached span materialized from the durable store by
/// [`RangeSpec::load_window`]: the block [`range`](Self::range) that was
/// scanned, plus the items found within it (ascending by position).
pub struct LoadedSpan<I> {
    pub range: BlockRange,
    pub items: Vec<I>,
}

/// A streaming gap filler. Wraps the source's RPC (`eth_getLogs`,
/// `eth_getBlockByNumber`, …); yields the [`Page`](RangeSpec::Page)s covering
/// the requested inclusive block range. Injected so tests can pass a
/// pure-Rust stub and so the provider-capturing fetch stays separate from
/// the persistence/interpretation in [`RangeSpec`].
///
/// Contract (`docs/FETCH.md` §2 owns the full text):
///
/// 1. Pages arrive **in ascending order, contiguous from `range.from`** —
///    each page's [`page_bounds`](RangeSpec::page_bounds) starts one block
///    after the previous page ended; the first starts at `range.from`. No
///    page extends past `range.to`.
/// 2. A stream that ends after a page reaching `range.to` is a success.
///    Ending cleanly *before* that is a contract violation the gap fill
///    surfaces as an error (the delivered prefix stands either way).
/// 3. An `Err` item ends the stream: the delivered prefix is a valid
///    coverage claim; the suffix past the last delivered page stays a gap.
pub trait PageFetcher<Page>: Send + Sync + 'static {
    fn fetch(&self, range: BlockRange) -> PageStream<Page>;
}

/// The page stream a [`PageFetcher`] returns.
pub type PageStream<Page> = Pin<Box<dyn Stream<Item = Result<Page, HistoryError>> + Send>>;

/// Adapts a one-page async closure to the streaming contract: the whole
/// range answered in a single page. The shim for single-shot sources
/// (EVM blocks, Bitcoin blocks) and test stubs.
pub struct SinglePageFetcher<F>(pub F);

impl<Page, F, Fut> PageFetcher<Page> for SinglePageFetcher<F>
where
    Page: Send + Sync + 'static,
    F: Fn(BlockRange) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Page, HistoryError>> + Send + 'static,
{
    fn fetch(&self, range: BlockRange) -> PageStream<Page> {
        Box::pin(stream::once((self.0)(range)))
    }
}

/// The source-specific behavior the generic [`RangeIndex`](super::RangeIndex)
/// delegates to. One impl per source (EVM logs, EVM blocks, …).
///
/// `Clone` is required so the async drain task can own a copy; impls hold
/// only cheap handles (an `Arc<Cache>` + a filter key), so cloning is cheap.
pub trait RangeSpec: Clone + Send + Sync + 'static {
    /// Tree-key position type.
    type Pos: IndexPos;
    /// Lightweight value stored in the tree (a cache pointer for logs; the
    /// whole header for blocks). `Copy` so the tree walk can read it under
    /// the lock without cloning.
    type Item: Copy + Send + Sync + 'static;
    /// The public item type yielded to consumers (EVM logs: `EvmEvent`).
    type Event: Send + Sync + 'static;
    /// The public, serializable position type consumers navigate by (EVM
    /// logs: `EvmPosition`). Distinct from the tree-key [`Pos`](Self::Pos),
    /// which is an internal ordering key.
    type PubPos: Position;
    /// One fetch result / one live delivery.
    type Page: Clone + Send + Sync + 'static;

    /// The tree position an item sits at.
    fn item_pos(item: &Self::Item) -> Self::Pos;

    /// The items a page carries, ascending by position. Un-cacheable entries
    /// (e.g. pending logs without a block hash) are dropped here so they
    /// never enter the tree.
    fn page_items(page: &Self::Page) -> Vec<Self::Item>;

    /// The inclusive block range a page covers (its scanned-range bounds).
    /// A delivered page advances the store's `end` to
    /// [`head_pos`](Self::head_pos)`(bounds.to)` — pages end at block
    /// boundaries, so no separate page-tip mapping exists.
    fn page_bounds(page: &Self::Page) -> BlockRange;

    /// Resolve a stored [`Item`](Self::Item) to the public positioned value
    /// yielded to consumers, reading the durable store synchronously (never
    /// RPC — see the headers-present invariant). Errors if the referent is
    /// gone (e.g. the cached block was reorged away).
    fn resolve(
        &self,
        item: &Self::Item,
    ) -> Result<Positioned<Self::Event, Self::PubPos>, HistoryError>;

    /// Persist a fetched/delivered page to the durable store (items +
    /// scanned-range record). Used by both the inline gap-fill finalize and
    /// the async drain.
    fn persist(&self, page: &Self::Page) -> Result<(), HistoryError>;

    /// Materialize the cached spans overlapping the aligned block `window`
    /// from the durable store, for lazy load into the tree.
    fn load_window(&self, window: BlockRange) -> Result<Vec<LoadedSpan<Self::Item>>, HistoryError>;

    /// Map a public position to its tree-key position.
    fn index_pos(p: &Self::PubPos) -> Self::Pos;

    /// The public tip position for a head at block `n`. The head clock
    /// speaks [`BlockNumber`]; each source maps it into its own position
    /// space (EVM logs: the end-of-block cut `(n, LogIdx::MAX)`; EVM
    /// blocks: `n` itself). Drives the generic head wiring's `on_bump`.
    fn head_pos(n: BlockNumber) -> Self::PubPos;

    /// Deserialize a public position from a
    /// [`Position::marshal`](crate::arch::Position::marshal) value. The
    /// store-side seam behind [`HistStore::load`](crate::arch::HistStore::load)
    /// — deserialization lives here, never on the position itself. Returns
    /// `None` on invalid input.
    fn load_pos(&self, value: serde_json::Value) -> Option<Self::PubPos>;

    /// The inclusive block window a public `(from, to]` range spans, for
    /// [`ensure_window_loaded`](super::RangeIndex::ensure_window_loaded).
    fn window(from: &Self::PubPos, to: &Self::PubPos) -> BlockRange;
}
