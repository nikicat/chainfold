pub mod cursor;
pub mod error;
pub mod event;
pub mod machine;
pub mod source;
pub mod storage;

// Re-export key public types at the crate root for ergonomic use.
pub use cursor::ChainCursor;
pub use error::SourceError;
pub use event::Event;
pub use machine::{ReducerCore, StateMachine};
pub use source::EventSource;
pub use storage::{Anchor, Snapshot, Storage, StoredEvent};
