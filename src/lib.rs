pub mod block;
pub mod coordinator;
pub mod cursor;
pub mod error;
pub mod event;
pub mod machine;
pub mod source;
pub mod storage;
pub mod store;

// Re-export key public types at the crate root for ergonomic use.
pub use block::Block;
pub use coordinator::{Codec, Engine, EngineConfig, EngineError, ReorgEvent, EngineStatus};
pub use cursor::ChainCursor;
pub use error::SourceError;
pub use event::Event;
pub use machine::{ReducerCore, StateMachine};
pub use source::EventSource;
pub use storage::Storage;
pub use store::{Store, ReducedStore};

#[cfg(feature = "evm")]
pub mod modules;
