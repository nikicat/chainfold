pub mod btc;
pub mod cex;
pub mod evm;

pub use btc::BtcCursor;
pub use cex::CexCursor;
pub use evm::EvmCursor;

/// Supertrait collecting all bounds that a cursor type must satisfy.
///
/// Cursors must be:
/// - `Ord`: total ordering drives event sequencing
/// - `Clone`: cursors are passed around and stored
/// - `Send + Sync + 'static`: cursors cross thread boundaries in the async engine
/// - `Serialize + DeserializeOwned`: cursors are persisted in the SQLite cache (Phase 2+)
///
/// The blanket impl below means any type satisfying all these bounds is automatically
/// a ChainCursor. Concrete cursor types (EvmCursor, BtcCursor, CexCursor) satisfy all
/// bounds via derive macros and/or manual impls.
pub trait ChainCursor:
    Ord + Clone + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned
{
}

/// Blanket impl: any type satisfying the bounds is automatically a ChainCursor.
/// Concrete cursor types do not need to write `impl ChainCursor for MyType {}`.
impl<T> ChainCursor for T where
    T: Ord + Clone + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned
{
}
