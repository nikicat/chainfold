// Placeholder for TDD RED phase — cursor types not yet implemented.
pub mod btc;
pub mod cex;
pub mod evm;

pub use btc::BtcCursor;
pub use cex::CexCursor;
pub use evm::EvmCursor;

pub trait ChainCursor:
    Ord + Clone + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned
{
}
