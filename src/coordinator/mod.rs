pub mod recovery;

use crate::block::Block;
use crate::cursor::ChainCursor;
use crate::event::Event;
use crate::machine::{ReducerCore, StateMachine};
use crate::source::EventSource;
use crate::storage::Storage;
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

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
    /// Initial state captured at construction time. Used during reorg recovery
    /// when no snapshot exists on the canonical chain — allows re-deriving state
    /// from genesis without needing to re-fetch from the source.
    initial_state: SM::State,
}

impl<SM, S, C> Engine<SM, S, C>
where
    SM: StateMachine,
    S: Storage,
    C: Codec<Cursor = SM::Cursor, Event = SM::Event, State = SM::State>,
{
    /// Create a new Engine with the given components and configuration.
    pub fn new(core: ReducerCore<SM>, storage: S, codec: C, config: EngineConfig) -> Self {
        // Capture the initial state from the watch channel at construction time.
        // This is used during reorg recovery when no snapshot exists on the canonical
        // chain — the engine needs to replay from genesis using the same starting state.
        let initial_state = core.subscribe().borrow().clone();
        Self {
            core,
            storage,
            codec,
            config,
            blocks_since_snapshot: 0,
            initial_state,
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

    // ================================================================
    // Reorg detection and state re-derivation
    // ================================================================

    /// Process a block, detecting and handling reorgs transparently.
    ///
    /// If the incoming block's parent_hash matches the current canonical tip,
    /// this is a normal extension of the chain — delegates to process_block.
    ///
    /// If the parent_hash does NOT match the canonical tip, a reorg has occurred.
    /// The Engine:
    /// 1. Finds the common ancestor between old and new chains
    /// 2. Notifies the client via on_reorg callback (BEFORE state changes)
    /// 3. Re-derives state from fork point (snapshot + replay to common_ancestor)
    /// 4. Updates canonical_tip to common_ancestor
    /// 5. Processes the incoming block (which extends from the new branch)
    ///
    /// Returns Ok(true) if the block was new, Ok(false) if duplicate.
    pub fn process_or_reorg(
        &mut self,
        block: &Block<SM::Cursor, SM::Event>,
        on_reorg: &mut impl FnMut(ReorgEvent),
    ) -> Result<bool, EngineError> {
        let canonical_tip = self
            .storage
            .get_canonical_tip(&self.config.source_id)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;

        if let Some(tip_hash) = canonical_tip {
            if block.parent_hash != tip_hash {
                // Reorg detected: incoming block's parent is NOT the current tip.
                self.handle_reorg(&tip_hash, block, on_reorg)?;
            }
        }
        // else: no canonical tip — first block ever, just process normally.

        self.process_block(block)
            .map_err(|e| EngineError::Storage(Box::new(e)))
    }

    /// Walk parent pointers from both the old tip and the new block's parent chain
    /// to find the common ancestor (fork point).
    ///
    /// Algorithm:
    /// 1. Walk backward from old_tip, collecting all hashes into a HashSet
    /// 2. Walk backward from new_parent until finding a hash in the old set
    ///
    /// This is O(depth_old + depth_new) — typically O(1-5) for real chain reorgs.
    fn find_common_ancestor(
        &self,
        old_tip: &[u8; 32],
        new_parent: &[u8; 32],
    ) -> Result<[u8; 32], EngineError> {
        let zero_hash = [0u8; 32];

        // Build set of all hashes on the old canonical chain.
        let mut old_chain = std::collections::HashSet::new();
        let mut cursor = *old_tip;
        loop {
            old_chain.insert(cursor);
            let parent = self
                .storage
                .get_parent_hash(&cursor)
                .map_err(|e| EngineError::Storage(Box::new(e)))?;
            match parent {
                None => break,                       // block not found — stop walking
                Some(p) if p == zero_hash => break,  // genesis sentinel — stop
                Some(p) => cursor = p,
            }
        }

        // Walk new chain backward until finding a hash in old_chain.
        let mut new_cursor = *new_parent;
        loop {
            if old_chain.contains(&new_cursor) {
                return Ok(new_cursor);
            }
            let parent = self
                .storage
                .get_parent_hash(&new_cursor)
                .map_err(|e| EngineError::Storage(Box::new(e)))?;
            match parent {
                None => return Err(EngineError::ReorgWithoutAncestor),
                Some(p) if p == zero_hash => {
                    // Reached genesis of new chain. Check if genesis itself is in old chain.
                    if old_chain.contains(&new_cursor) {
                        return Ok(new_cursor);
                    }
                    return Err(EngineError::ReorgWithoutAncestor);
                }
                Some(p) => new_cursor = p,
            }
        }
    }

    /// Handle a detected reorg: notify client, re-derive state from fork point.
    ///
    /// Steps:
    /// 1. Find common ancestor between old_tip and new block's parent chain
    /// 2. Compute reorg depth (old_tip_height - ancestor_height)
    /// 3. Fire on_reorg callback with ReorgEvent (BEFORE state changes)
    /// 4. Re-derive state: find nearest snapshot to ancestor, load it, replay to ancestor
    /// 5. Update canonical_tip to point to common_ancestor
    ///
    /// After this returns, the caller processes the incoming block via process_block,
    /// which extends the chain from common_ancestor and updates canonical_tip.
    fn handle_reorg(
        &mut self,
        old_tip: &[u8; 32],
        incoming_block: &Block<SM::Cursor, SM::Event>,
        on_reorg: &mut impl FnMut(ReorgEvent),
    ) -> Result<(), EngineError> {
        // Step 1: Find common ancestor.
        let common_ancestor =
            self.find_common_ancestor(old_tip, &incoming_block.parent_hash)?;

        // Step 2: Compute reorg depth.
        let old_height = self
            .storage
            .get_block_height(old_tip)
            .map_err(|e| EngineError::Storage(Box::new(e)))?
            .unwrap_or(0);
        let ancestor_height = self
            .storage
            .get_block_height(&common_ancestor)
            .map_err(|e| EngineError::Storage(Box::new(e)))?
            .unwrap_or(0);
        let depth = old_height.saturating_sub(ancestor_height) as u32;

        // Step 3: Notify client BEFORE state re-derivation.
        on_reorg(ReorgEvent {
            common_ancestor,
            old_tip: *old_tip,
            new_tip: incoming_block.block_hash,
            depth,
        });

        // Step 4: Re-derive state from the fork point.
        // Find the nearest snapshot on the canonical chain up to common_ancestor.
        let snapshot = self
            .storage
            .find_nearest_snapshot(&self.config.source_id, &common_ancestor)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;

        let zero_hash = [0u8; 32];
        match snapshot {
            Some((snap_hash, snap_payload)) => {
                let state = self
                    .codec
                    .decode_state(&snap_payload)
                    .ok_or(EngineError::DecodeState)?;
                self.replace_state(state);
                // Replay from snapshot to common_ancestor (if they differ).
                if snap_hash != common_ancestor {
                    self.replay_chain_for_reorg(&snap_hash, &common_ancestor)?;
                }
            }
            None => {
                // No snapshot on the chain up to common_ancestor.
                // Reset to initial state and replay all blocks from genesis to ancestor.
                self.replace_state(self.initial_state.clone());
                let chain = self
                    .storage
                    .collect_canonical_chain(&zero_hash, &common_ancestor)
                    .map_err(|e| EngineError::Storage(Box::new(e)))?;
                for block_hash in chain {
                    self.replay_block_events_for_reorg(&block_hash)?;
                }
            }
        }

        // Step 5: Update canonical_tip to common_ancestor.
        // process_block (called after handle_reorg) will then extend from here.
        self.storage
            .set_canonical_tip(&self.config.source_id, &common_ancestor)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;

        // Reset snapshot counter — we're starting fresh from the fork point.
        self.blocks_since_snapshot = 0;

        Ok(())
    }

    /// Replay stored events from from_hash (exclusive) to to_hash (inclusive).
    ///
    /// Reuses the DAG walk pattern from recovery. Adapts storage errors to EngineError.
    fn replay_chain_for_reorg(
        &mut self,
        from_hash: &[u8; 32],
        to_hash: &[u8; 32],
    ) -> Result<(), EngineError> {
        let chain = self
            .storage
            .collect_canonical_chain(from_hash, to_hash)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;
        for block_hash in chain {
            self.replay_block_events_for_reorg(&block_hash)?;
        }
        Ok(())
    }

    /// Decode and replay events for a single block through the engine WITHOUT writing to storage.
    ///
    /// Used during reorg re-derivation: loads raw (cursor_bytes, payload_bytes) from
    /// storage, decodes each event via Codec, and calls replay_events.
    fn replay_block_events_for_reorg(
        &mut self,
        block_hash: &[u8; 32],
    ) -> Result<(), EngineError> {
        let stored = self
            .storage
            .load_block_events(block_hash)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;
        let events: Vec<Event<SM::Cursor, SM::Event>> = stored
            .iter()
            .filter_map(|(cursor_bytes, payload_bytes)| {
                let cursor: SM::Cursor = postcard::from_bytes(cursor_bytes).ok()?;
                let data: SM::Event = self.codec.decode_event(payload_bytes)?;
                Some(Event { cursor, data })
            })
            .collect();
        self.replay_events(&events);
        Ok(())
    }

    // ================================================================
    // Live event loop
    // ================================================================

    /// Run the live event loop.
    ///
    /// Subscribes to the EventSource, processes blocks in micro-batches,
    /// handles disconnection with exponential backoff, and fills gaps after
    /// reconnection. Reorgs are detected and handled transparently via
    /// process_or_reorg.
    ///
    /// Returns Ok(()) on graceful cancellation (CancellationToken triggered).
    /// Returns Err on unrecoverable errors (storage failure, max retries exhausted).
    ///
    /// `source` is borrowed (not moved) so that reconnections can call `subscribe()`
    /// again and gap fills can call `fetch_blocks` on the same source.
    ///
    /// Caller owns task lifecycle:
    /// ```ignore
    /// let token = CancellationToken::new();
    /// let result = engine.run(&source, token.clone(), on_reorg, on_status).await;
    /// ```
    pub async fn run<ES>(
        &mut self,
        source: &ES,
        cancel: CancellationToken,
        mut on_reorg: impl FnMut(ReorgEvent),
        mut on_status: impl FnMut(EngineStatus),
    ) -> Result<(), EngineError>
    where
        ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
    {
        let mut attempt: u32 = 0;

        loop {
            // Attempt to subscribe.
            let stream = match source.subscribe(self.subscribe_from_height()?).await {
                Ok(s) => {
                    if attempt > 0 {
                        on_status(EngineStatus::Reconnected);
                    }
                    attempt = 0;
                    s
                }
                Err(e) => {
                    if attempt >= self.config.max_retries {
                        return Err(EngineError::ReconnectExhausted(
                            attempt,
                            Box::new(e),
                        ));
                    }
                    on_status(EngineStatus::Reconnecting(attempt + 1));
                    let delay = backoff_delay(&self.config, attempt);
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(delay) => {}
                    }
                    attempt += 1;
                    continue;
                }
            };

            // Micro-batch the stream with ready_chunks.
            let mut batched = stream.ready_chunks(self.config.batch_size);

            // Inner loop: drain batches until stream ends or cancellation.
            let stream_result: Result<bool, EngineError> = loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        break Ok(true); // true = cancelled
                    }
                    batch = batched.next() => {
                        match batch {
                            None => {
                                // Stream ended cleanly.
                                break Ok(false); // false = stream ended (not cancelled)
                            }
                            Some(results) => {
                                for result in results {
                                    let block = result
                                        .map_err(|e| EngineError::Source(Box::new(e)))?;

                                    // Gap fill: if block arrived at a height ahead of our
                                    // stored tip, fetch the missing blocks from source first.
                                    let tip_h = self.current_tip_height()?;
                                    if block.height > tip_h + 1 {
                                        on_status(EngineStatus::GapFilling);
                                        crate::coordinator::recovery::fetch_gap_blocks(
                                            self,
                                            source,
                                            tip_h + 1,
                                            block.height - 1,
                                        )
                                        .await
                                        .map_err(EngineError::from_recovery)?;
                                    }

                                    self.process_or_reorg(&block, &mut on_reorg)?;
                                }
                            }
                        }
                    }
                }
            };

            match stream_result {
                Ok(true) => return Ok(()), // Graceful cancellation.
                Ok(false) => {
                    // Stream ended — reconnect. attempt is already 0 from the last
                    // successful subscribe, so no reset needed.
                    on_status(EngineStatus::Disconnected);
                    // Continue the outer loop to re-subscribe.
                }
                Err(e) => return Err(e), // Unrecoverable error.
            }
        }
    }

    /// Get the height to subscribe from: stored canonical tip height + 1, or 0.
    fn subscribe_from_height(&self) -> Result<u64, EngineError> {
        Ok(self.current_tip_height()? + 1)
    }

    /// Get the current canonical tip height, or 0 if no tip exists.
    fn current_tip_height(&self) -> Result<u64, EngineError> {
        let tip_hash = self
            .storage
            .get_canonical_tip(&self.config.source_id)
            .map_err(|e| EngineError::Storage(Box::new(e)))?;
        match tip_hash {
            None => Ok(0),
            Some(hash) => {
                let height = self
                    .storage
                    .get_block_height(&hash)
                    .map_err(|e| EngineError::Storage(Box::new(e)))?
                    .unwrap_or(0);
                Ok(height)
            }
        }
    }
}

impl EngineError {
    /// Convert a RecoveryError to an EngineError.
    fn from_recovery(e: crate::coordinator::recovery::RecoveryError) -> Self {
        match e {
            crate::coordinator::recovery::RecoveryError::Storage(e) => EngineError::Storage(e),
            crate::coordinator::recovery::RecoveryError::Source(e) => EngineError::Source(e),
            crate::coordinator::recovery::RecoveryError::DecodeState => EngineError::DecodeState,
        }
    }
}

/// Compute exponential backoff delay for reconnection attempts.
///
/// Delay = base * 2^attempt, capped at max. Uses saturating arithmetic to
/// avoid overflow on large attempt counts.
fn backoff_delay(config: &EngineConfig, attempt: u32) -> std::time::Duration {
    let base = config.initial_delay_ms;
    let max = config.max_delay_ms;
    // Saturating left-shift: cap shift at 31 to stay within u64 range.
    let shift = attempt.min(31);
    let ms = base.saturating_mul(1u64 << shift).min(max);
    std::time::Duration::from_millis(ms)
}
