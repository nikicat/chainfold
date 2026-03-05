pub mod recovery;

use crate::block::Block;
use crate::cursor::ChainCursor;
use crate::event::Event;
use crate::machine::{ReducerCore, StateMachine};
use crate::storage::Storage;
use tokio::sync::watch;

/// Bridges typed events/state to opaque byte representation for storage.
///
/// Client provides encode/decode for event payloads and state only.
/// Cursor serialization is library-internal (postcard).
/// No cursor_to_tuple/tuple_to_cursor — cursors stored as postcard bytes.
///
/// Note: encode_event/decode_event operate on the event PAYLOAD (Self::Event),
/// not the full Event<C, E> wrapper. The cursor is serialized separately by
/// the library. This is the key simplification from the old Codec.
pub trait Codec: Send + Sync + 'static {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type State: Clone + Send + 'static;

    /// Serialize event payload to bytes.
    fn encode_event(&self, event: &Self::Event) -> Vec<u8>;

    /// Deserialize event payload from bytes. Returns None on corrupt/incompatible data.
    fn decode_event(&self, payload: &[u8]) -> Option<Self::Event>;

    /// Serialize state to opaque bytes for snapshot storage.
    fn encode_state(&self, state: &Self::State) -> Vec<u8>;

    /// Deserialize state from opaque bytes. Returns None on corrupt/incompatible.
    fn decode_state(&self, bytes: &[u8]) -> Option<Self::State>;
}

/// Unified error type for Engine::run().
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync>),

    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),

    #[error("codec error: failed to decode state")]
    DecodeState,

    #[error("reconnection exhausted after {0} attempts: {1}")]
    ReconnectExhausted(u32, Box<dyn std::error::Error + Send + Sync>),

    #[error("reorg detected but common ancestor not found in storage")]
    ReorgWithoutAncestor,
}

/// Notification of a chain reorganization.
#[derive(Debug, Clone)]
pub struct ReorgEvent {
    /// Hash of the fork point block (last common block between old and new chains).
    pub common_ancestor: [u8; 32],
    /// Hash of the last block on the discarded branch.
    pub old_tip: [u8; 32],
    /// Hash of the first block on the new canonical branch.
    pub new_tip: [u8; 32],
    /// Number of blocks rolled back from old_tip to common_ancestor.
    pub depth: u32,
}

/// Operational status signals for Engine::run().
#[derive(Debug, Clone)]
pub enum EngineStatus {
    /// WebSocket/stream connection was lost.
    Disconnected,
    /// Reconnection attempt in progress (attempt number, 1-based).
    Reconnecting(u32),
    /// Successfully reconnected to the event source.
    Reconnected,
    /// Filling gap between stored tip and live stream start.
    GapFilling,
}

/// Configuration for the Engine coordinator.
pub struct EngineConfig {
    /// Logical source identifier — used as the key in storage (canonical_tip, snapshots).
    pub source_id: String,
    /// Trigger a snapshot after this many distinct (non-duplicate) new block inserts.
    pub snapshot_every_n: usize,
    /// Maximum reconnection attempts before returning Err. 0 = no retries.
    pub max_retries: u32,
    /// Starting backoff delay in milliseconds.
    pub initial_delay_ms: u64,
    /// Maximum backoff delay in milliseconds (exponential backoff cap).
    pub max_delay_ms: u64,
    /// Micro-batch capacity for ready_chunks(). Default: 64.
    pub batch_size: usize,
}

impl EngineConfig {
    /// Create a config with sensible defaults for reconnection settings.
    pub fn new(source_id: impl Into<String>, snapshot_every_n: usize) -> Self {
        Self {
            source_id: source_id.into(),
            snapshot_every_n,
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 30_000,
            batch_size: 64,
        }
    }
}

/// Block-oriented coordinator. Owns ReducerCore + Storage + Codec.
///
/// Write-first pipeline:
/// 1. Encode block events to storage bytes via Codec
/// 2. Write block + events + update canonical_tip atomically via Storage
/// 3. Apply events to ReducerCore (state advances)
/// 4. Trigger snapshot if block insert count crosses threshold
///
/// Duplicate blocks (block_hash already in storage) are skipped entirely:
/// events are NOT re-applied to ReducerCore, and the snapshot counter is NOT
/// incremented.
pub struct Engine<SM, S, C>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    core: ReducerCore<SM>,
    storage: S,
    codec: C,
    config: EngineConfig,
    /// Count of new (non-duplicate) block inserts since the last snapshot.
    blocks_since_snapshot: usize,
}

impl<SM, S, C> Engine<SM, S, C>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    /// Create a new Engine with the given components and configuration.
    pub fn new(core: ReducerCore<SM>, storage: S, codec: C, config: EngineConfig) -> Self {
        Self {
            core,
            storage,
            codec,
            config,
            blocks_since_snapshot: 0,
        }
    }

    /// Process a block through the write-first pipeline.
    ///
    /// Returns `true` if the block was new (inserted into storage and applied
    /// to state machine). Returns `false` if the block was a duplicate (already
    /// in storage) — in that case, state is unchanged and the snapshot counter
    /// is not incremented.
    pub fn process_block(
        &mut self,
        block: &Block<SM::Cursor, SM::Event>,
    ) -> Result<bool, S::Error> {
        // Step 1: Encode events to (cursor_bytes, payload_bytes) pairs.
        // Cursors are serialized with postcard; payloads via the client Codec.
        let encoded_events: Vec<(Vec<u8>, Vec<u8>)> = block
            .events
            .iter()
            .map(|e| {
                let cursor_bytes = postcard::to_allocvec(&e.cursor)
                    .expect("cursor serialization is infallible for well-formed types");
                let payload_bytes = self.codec.encode_event(&e.data);
                (cursor_bytes, payload_bytes)
            })
            .collect();

        let event_refs: Vec<(&[u8], &[u8])> = encoded_events
            .iter()
            .map(|(c, p)| (c.as_slice(), p.as_slice()))
            .collect();

        // Step 2: Write to storage (write-first invariant).
        // This atomically writes block metadata, events, and updates canonical_tip.
        let is_new = self.storage.write_block(
            &self.config.source_id,
            &block.block_hash,
            &block.parent_hash,
            block.height,
            &event_refs,
        )?;

        if !is_new {
            // Duplicate block: skip state advance and snapshot counter.
            return Ok(false);
        }

        // Step 3: Apply events to ReducerCore (AFTER storage write).
        for event in &block.events {
            self.core.apply(event);
        }

        // Step 4: Snapshot triggering — only on new block inserts.
        self.blocks_since_snapshot += 1;
        if self.blocks_since_snapshot >= self.config.snapshot_every_n {
            let state = self.state();
            let payload = self.codec.encode_state(&state);
            self.storage.write_snapshot(
                &self.config.source_id,
                &block.block_hash,
                &payload,
            )?;
            self.blocks_since_snapshot = 0;
        }

        Ok(true)
    }

    /// Get the current state (clones the state from the watch channel).
    pub fn state(&self) -> SM::State {
        self.core.subscribe().borrow().clone()
    }

    /// Subscribe to state changes via a watch channel receiver.
    pub fn subscribe(&self) -> watch::Receiver<SM::State> {
        self.core.subscribe()
    }

    /// Get a reference to the underlying storage (for test inspection and recovery).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get the configured source_id.
    pub fn source_id(&self) -> &str {
        &self.config.source_id
    }

    /// Get a reference to the codec.
    pub fn codec(&self) -> &C {
        &self.codec
    }

    /// Replace the current state with the given value.
    ///
    /// Used by the recovery pipeline to restore state from a loaded snapshot.
    pub fn replace_state(&mut self, state: SM::State) {
        self.core.reset_state(state);
    }

    /// Replay events from a block WITHOUT writing to storage.
    ///
    /// Used during recovery to replay stored events through ReducerCore
    /// after restoring from a snapshot.
    pub fn replay_events(&mut self, events: &[Event<SM::Cursor, SM::Event>]) {
        for event in events {
            self.core.apply(event);
        }
    }
}
