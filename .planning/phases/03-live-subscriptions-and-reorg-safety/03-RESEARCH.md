# Phase 3: Live Subscriptions and Reorg Safety - Research

**Researched:** 2026-03-05
**Domain:** Async event loop, stream consumption, reorg detection, reconnection with backoff
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Run loop ownership**
- `Engine::run()` is a long-lived async fn the caller spawns as a tokio task. Library owns the event loop internally; caller owns task lifecycle.
- Shutdown via `tokio_util::CancellationToken` passed to `Engine::run()`. Engine finishes current batch, flushes, returns `Ok(())`.
- `Engine::run()` returns `Result<(), EngineError>`. `Ok` on graceful cancellation, `Err` on unrecoverable failures (storage errors, reconnection exhausted).
- Recovery and run are separate calls: caller calls `engine.recover(&source).await?` first, then `engine.run(source, token).await?`. Matches Phase 2's existing `recover()` API. Fresh starts skip `recover()`.

**Reorg detection model**
- Detection via block hash comparison: when a new block arrives whose parent_hash does not match the stored canonical tip hash, a reorg has occurred.
- Notification carries full context: `ReorgEvent { common_ancestor, old_tip, new_tip, depth }`. Matches ROADMAP.md success criteria exactly.
- Client receives reorg notification via synchronous callback `on_reorg: FnMut(ReorgEvent)` passed to `Engine::run()`. Runs inline on the event processing path, same pattern as OnChange.
- Library handles mechanical cleanup after reorg: purge invalidated blocks from storage (all blocks past common_ancestor on old branch are **not** in new canonical chain — update canonical_tip only; the old blocks stay as DAG nodes but canonical_tip now points to the new branch tip), re-derive state from the fork point by replaying the new canonical chain. Client only decides whether to continue.

**Reconnection ownership**
- Library retries `EventSource::subscribe()` on stream end/error with exponential backoff. Client's `subscribe()` impl creates a new connection each time. Library controls retry logic; client controls connection creation.
- Backoff strategy configurable in `EngineConfig`: `max_retries`, `initial_delay_ms`, `max_delay_ms`. Caller tunes for their use case.
- After reconnection, reuse Phase 2's `fetch_gap()` code path to fill the gap between last known canonical tip height and new subscription start. Same tested pipeline, no redundant RPC calls.
- Status callback `on_status: FnMut(EngineStatus)` for operational signals: `Disconnected`, `Reconnecting(attempt)`, `Reconnected`, `GapFilling`. Optional — useful for logging/monitoring.

**Hot path optimization**
- Keep async-trait for Phase 3. 10–50ns heap allocation per call is negligible vs network latency (ms-order). Migration to `trait_variant` deferred until benchmarks show need.
- Keep snapshot writes synchronous. WAL mode + periodic snapshots (~1ms write) is fast enough. Background writes add concurrency complexity for minimal gain.
- Live events micro-batched: drain all currently-available events from the stream (non-blocking poll), process as a single batch in one SQLite transaction. Falls back to single-event processing when stream is slow. Better throughput during bursts (new block with many events).
- `block_hash` stays `Option<Hash>` on... wait — in Phase 2, `block_hash` moved OFF `Event` and onto `Block`. Reorg detection activates when the incoming `Block`'s `parent_hash` does not match the stored canonical tip. CEX sources (no blocks/reorgs) can use sentinel parent_hash or a special flag; naturally skip reorg checks via a CEX adapter.

### Claude's Discretion
- Exact `EngineError` variants and error hierarchy
- Internal task structure within `Engine::run()` (single loop vs multiple tasks)
- `ReorgEvent` struct fields beyond the decided four (`common_ancestor`, `old_tip`, `new_tip`, `depth`)
- `EngineStatus` enum variants beyond the decided four
- Micro-batch drain strategy (`poll_next` vs `try_next` vs `FuturesUnordered`)
- How to find `common_ancestor` during reorg detection (walk back stored block hashes)

### Deferred Ideas (OUT OF SCOPE)
None — discussion stayed within phase scope.
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| INGEST-01 | Library consumes live events from an EventSource subscription and applies them to state in real-time | `StreamExt::next()` in a cancel-safe `select!` loop; `process_block()` on each yielded block; micro-batch via `ready_chunks()` |
| INGEST-05 | Hot path is in-process: events flow from source through Apply to callbacks without IPC or serialization on the critical path | Already achieved by Phase 2 architecture; `Engine::run()` calls `process_block()` which calls `core.apply()` inline; no IPC |
| SAFETY-01 | Library maintains a block DAG and tracks which block is the canonical tip as reported by EventSource | `write_block()` updates `canonical_tip` atomically; reorg handler updates canonical_tip to new branch tip |
| SAFETY-02 | When canonical tip changes to a different branch (reorg), library transparently re-derives state from fork point on new canonical chain | Walk parent pointers to find common ancestor; replay new canonical chain blocks via existing `replay_chain()` helper |
| SAFETY-03 | Library handles WebSocket disconnection with exponential backoff, re-subscribe from last anchor, and gap refetch | Manual backoff loop using `tokio::time::sleep()`; `fetch_gap_blocks()` reused from recovery.rs after reconnect |
</phase_requirements>

---

## Summary

Phase 3 activates the Engine — turning the passive block-processor from Phase 2 into a live-running event loop. The architecture is straightforward: `Engine::run()` subscribes to an `EventSource`, drains blocks from the stream in micro-batches, detects reorgs by comparing incoming `parent_hash` against the stored canonical tip, handles reconnection with configurable exponential backoff, and exposes callbacks for reorg events and operational status changes.

The codebase is in excellent shape for this phase. Phase 2 already delivers `process_block()` (write-first pipeline), `recover()` (gap-fill + replay), `collect_canonical_chain()` (backward DAG walk), and `replay_chain()` (state replay from a branch point). Phase 3 stitches these together into a live loop. No new storage primitives are needed beyond two new Storage trait methods: `set_canonical_tip()` (explicit tip update for reorg branch switch) and `delete_blocks_after()` (optional — the DAG keeps old blocks but canonical_tip simply points to the new branch; old blocks remain as DAG nodes). The Engine also needs two new fields: reorg callback and status callback.

The primary technical risks are: (1) getting the reorg detection sequence right — specifically distinguishing a genuine reorg from a block arriving out of order on a fork; (2) ensuring `tokio::select!` cancel safety with `StreamExt::next()` (confirmed safe per official docs); and (3) correctly wiring the micro-batch drain so single-event fallback preserves the write-first invariant under all timing conditions.

**Primary recommendation:** Single-loop architecture — one `tokio::select!` loop polls `stream.next()` vs `token.cancelled()`. Micro-batch via `ready_chunks(N)` adaptor from `futures_util`. Manual exponential backoff with `tokio::time::sleep()` — no external retry crate needed for this use case.

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `tokio-util` | 0.7.18 | `CancellationToken` for graceful shutdown | Official tokio ecosystem; already in transitive dep graph via tokio |
| `futures-util` | 0.3.32 | `StreamExt::next()`, `TryStreamExt::try_next()`, `ready_chunks()` | Already in dev-dependencies; provides stream combinators |
| `tokio` | 1.49.0 | `tokio::select!`, `tokio::time::sleep()` | Already in Cargo.toml |

Already present in Cargo.toml (no new deps needed):
- `tokio = { version = "1", features = ["sync"] }` — needs `features = ["time"]` added for `sleep()`
- `futures-core = "0.3"` — already present
- `futures-util` — in dev-dependencies; move to main dependencies

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `tokio-retry` | 0.3.0 | Retry strategies with exponential backoff | Available, but hand-rolling is simpler here — see below |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Manual backoff with `tokio::time::sleep()` | `tokio-retry` (0.3.0) or `backon` | tokio-retry is clean but adds a dep; our reconnect loop is <20 lines — hand-roll it |
| `ready_chunks(N)` from futures-util | Manual poll loop with `poll_next_unpin()` | `ready_chunks` is cleaner and already handles the "return on Pending if non-empty" logic correctly |
| Single `tokio::select!` loop | Separate task per concern | Multi-task adds channel complexity; single loop is simpler, the bottleneck is I/O not CPU |

**Installation changes needed:**

```toml
# Cargo.toml changes:
tokio = { version = "1", features = ["sync", "time"] }   # add "time" feature
futures-util = "0.3"                                       # move from dev-deps to deps
tokio-util = { version = "0.7", features = ["sync"] }     # new dep, CancellationToken
```

---

## Architecture Patterns

### Recommended Module Structure

```
src/
├── coordinator/
│   ├── mod.rs          # Engine struct + process_block() + run() (new)
│   ├── recovery.rs     # recover() + fetch_gap_blocks() — reused as-is
│   └── reorg.rs        # (new) reorg detection + state re-derivation helpers
├── error.rs            # EngineError (new unified error type)
└── source.rs           # EventSource trait — unchanged
```

Alternatively, keep `run()` and reorg logic directly in `coordinator/mod.rs` since they're tightly coupled to `Engine`. Only extract to `reorg.rs` if the reorg logic exceeds ~100 lines.

### Pattern 1: The Core Event Loop

The canonical `Engine::run()` loop structure:

```rust
// Source: official tokio docs + tokio::select! cancel safety docs
pub async fn run<ES>(
    &mut self,
    source: ES,
    cancel: CancellationToken,
    mut on_reorg: impl FnMut(ReorgEvent),
    mut on_status: impl FnMut(EngineStatus),
) -> Result<(), EngineError>
where
    ES: EventSource<Cursor = SM::Cursor, Event = SM::Event>,
{
    let mut attempt = 0u32;
    loop {
        // Subscribe to live blocks.
        let mut stream = match source.subscribe(self.current_tip_height()).await {
            Ok(s) => { attempt = 0; on_status(EngineStatus::Reconnected); s }
            Err(e) => {
                if attempt >= self.config.max_retries {
                    return Err(EngineError::ReconnectExhausted(e.into()));
                }
                on_status(EngineStatus::Reconnecting(attempt));
                let delay = backoff_delay(&self.config, attempt);
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(delay) => {}
                }
                attempt += 1;
                continue;
            }
        };

        // Micro-batch loop: drain ready blocks, process as batch.
        let result = self.run_stream(&mut stream, &cancel, &mut on_reorg, &mut on_status).await;

        match result {
            Ok(RunResult::Cancelled) => return Ok(()),
            Ok(RunResult::StreamEnded) => {
                on_status(EngineStatus::Disconnected);
                // Loop back to reconnect.
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Pattern 2: Stream Consumption with Micro-Batching

The `ready_chunks()` adaptor from `futures_util::StreamExt` is the right tool for micro-batching. It returns a stream of `Vec<Item>` where each vec contains all items that were immediately ready when polled — if nothing is ready it waits for at least one, then drains all immediately-available items up to `capacity`.

```rust
// Source: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html
use futures_util::StreamExt;

// Wrap the stream with ready_chunks to get micro-batches.
// Each batch: all blocks immediately available on the stream (up to 64).
// Falls back to single-block batches when stream is slow (1 block at a time = correct).
let mut batched = stream.ready_chunks(64);

loop {
    tokio::select! {
        biased;  // check cancellation first
        _ = cancel.cancelled() => return Ok(RunResult::Cancelled),
        batch = batched.next() => {
            match batch {
                None => return Ok(RunResult::StreamEnded),   // stream ended
                Some(results) => {
                    // Process all blocks in the batch as ONE SQLite transaction.
                    for result in results {
                        let block = result.map_err(|e| EngineError::Source(e.into()))?;
                        self.process_or_reorg(block, &mut on_reorg)?;
                    }
                }
            }
        }
    }
}
```

**Why `biased;`?** Ensures cancellation is always checked before pulling the next batch. Without it, `select!` is random — under high throughput the cancel branch could be starved.

**Why NOT `token.run_until_cancelled()`?** That method doesn't work well in loops — it drops the inner future on cancellation, which loses the batched stream adapter's internal buffer.

### Pattern 3: Reorg Detection and State Re-derivation

Detection: when an incoming block's `parent_hash` does not match the hash of the current canonical tip.

```rust
fn process_or_reorg(
    &mut self,
    block: Block<SM::Cursor, SM::Event>,
    on_reorg: &mut impl FnMut(ReorgEvent),
) -> Result<(), EngineError> {
    let canonical_tip = self.storage().get_canonical_tip(&self.config.source_id)
        .map_err(|e| EngineError::Storage(e.into()))?;

    if let Some(tip_hash) = canonical_tip {
        if block.parent_hash != tip_hash {
            // Reorg detected. Find common ancestor and re-derive state.
            let common_ancestor = self.find_common_ancestor(&block.parent_hash, &tip_hash)?;
            self.handle_reorg(common_ancestor, &block, on_reorg)?;
        }
    }

    // Normal path: process the block.
    self.process_block(&block).map_err(|e| EngineError::Storage(e.into()))?;
    Ok(())
}
```

**Finding the common ancestor:** Walk the incoming block's parent chain backward through stored blocks until hitting a hash that is already stored. That stored hash is the common ancestor. The walk uses the same parent-pointer structure from Phase 2.

```rust
fn find_common_ancestor(
    &self,
    new_parent: &[u8; 32],
    old_tip: &[u8; 32],
) -> Result<[u8; 32], EngineError> {
    // Build the set of all hashes on the old canonical chain
    // by walking backward from old_tip. Then walk new_parent chain
    // forward until we find a hash in that set.
    //
    // Simple O(depth) approach — reorg depth is typically 1-5 blocks.
    let mut old_chain_hashes = std::collections::HashSet::new();
    let mut cursor = *old_tip;
    loop {
        old_chain_hashes.insert(cursor);
        match self.storage().get_parent_hash(&cursor)? {
            None | Some(ZERO_HASH) => break,
            Some(parent) => cursor = parent,
        }
    }

    // Walk the new chain backward until we find a hash in old_chain_hashes.
    let mut new_cursor = *new_parent;
    loop {
        if old_chain_hashes.contains(&new_cursor) {
            return Ok(new_cursor);
        }
        match self.storage().get_parent_hash(&new_cursor)? {
            None => return Err(EngineError::ReorgWithoutAncestor),
            Some(ZERO_HASH) => return Ok(ZERO_HASH),
            Some(parent) => new_cursor = parent,
        }
    }
}
```

This requires a new Storage trait method: `get_parent_hash(block_hash) -> Result<Option<[u8;32]>>`.

### Pattern 4: Reorg Cleanup and State Re-derivation

After finding the common ancestor:

```rust
fn handle_reorg(
    &mut self,
    common_ancestor: [u8; 32],
    incoming_block: &Block<SM::Cursor, SM::Event>,
    on_reorg: &mut impl FnMut(ReorgEvent),
) -> Result<(), EngineError> {
    let old_tip = self.storage().get_canonical_tip(&self.config.source_id)
        .map_err(|e| EngineError::Storage(e.into()))?
        .unwrap_or([0u8; 32]);

    // Compute reorg depth.
    let old_tip_height = self.storage().get_block_height(&old_tip)
        .map_err(|e| EngineError::Storage(e.into()))?
        .unwrap_or(0);
    let ancestor_height = self.storage().get_block_height(&common_ancestor)
        .map_err(|e| EngineError::Storage(e.into()))?
        .unwrap_or(0);
    let depth = old_tip_height.saturating_sub(ancestor_height) as u32;

    // Step 1: Notify client BEFORE state changes.
    on_reorg(ReorgEvent {
        common_ancestor,
        old_tip,
        new_tip: incoming_block.block_hash,
        depth,
    });

    // Step 2: Reset state to snapshot nearest to common_ancestor.
    // Reuse Phase 2's find_nearest_snapshot() + replay_chain() logic.
    let snapshot = self.storage()
        .find_nearest_snapshot(&self.config.source_id, &common_ancestor)
        .map_err(|e| EngineError::Storage(e.into()))?;

    match snapshot {
        Some((snap_hash, snap_payload)) => {
            let state = self.codec().decode_state(&snap_payload)
                .ok_or(EngineError::DecodeState)?;
            self.replace_state(state);
            // Replay snap_hash -> common_ancestor (if snap is before ancestor).
            if snap_hash != common_ancestor {
                replay_chain(self, &snap_hash, &common_ancestor)
                    .map_err(|e| EngineError::Storage(e.into()))?;
            }
        }
        None => {
            // No snapshot: replay from genesis to common_ancestor.
            replay_full_chain(self, &common_ancestor)
                .map_err(|e| EngineError::Storage(e.into()))?;
        }
    }

    // Step 3: Update canonical_tip to point to common_ancestor.
    // The incoming block will be written by the caller (process_block).
    // Old blocks on the discarded branch remain as DAG nodes — they're just unreachable
    // from the new canonical_tip. No deletion needed.
    self.storage_mut().set_canonical_tip(&self.config.source_id, &common_ancestor)
        .map_err(|e| EngineError::Storage(e.into()))?;

    Ok(())
}
```

### Pattern 5: Exponential Backoff (hand-rolled)

Hand-rolled is appropriate here — the logic is trivial and tokio-retry would be overkill:

```rust
fn backoff_delay(config: &EngineConfig, attempt: u32) -> std::time::Duration {
    let base = config.initial_delay_ms as u64;
    let max = config.max_delay_ms as u64;
    // Saturating exponentiation: 2^attempt, capped at max.
    let ms = base.saturating_mul(1u64 << attempt.min(31)).min(max);
    std::time::Duration::from_millis(ms)
}
```

### Pattern 6: Gap Fill After Reconnection

Reuse `fetch_gap_blocks()` from `recovery.rs` directly — it already handles chunked pagination and the write-first invariant:

```rust
// After successful reconnect, fill gap from last canonical tip height to
// the first block from the new stream subscription start.
let tip_height = self.current_tip_height();
let stream_start_height = stream_first_block.height;
if tip_height + 1 < stream_start_height {
    on_status(EngineStatus::GapFilling);
    fetch_gap_blocks(self, &source, tip_height + 1, stream_start_height - 1).await
        .map_err(|e| EngineError::from_recovery(e))?;
}
```

### Anti-Patterns to Avoid

- **Spawning internal tasks:** Don't spawn a sub-task for stream consumption. Keep it in the calling task to avoid channel overhead and lifetime complexity.
- **Skipping `biased;` in select!:** Under high block throughput, random select! order can starve the cancel branch. Always use `biased;` when cancellation must be responsive.
- **Deleting old branch blocks from storage:** The DAG model retains all blocks. Changing canonical_tip is enough. Deletion would break historical queries (Phase 4) and add complexity.
- **Calling on_reorg after state changes:** Notify before re-deriving state. Client inspects old state before it changes.
- **Using tokio::time::sleep without select!:** During reconnect delay, the engine must still respond to cancellation. Always select! sleep vs cancelled().

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Stream draining / micro-batching | Manual poll loop with `poll_next_unpin()` | `ready_chunks(N)` from `futures_util::StreamExt` | `ready_chunks` correctly handles the "drain ready, wait for one if empty" semantics; manual version is tricky to get right |
| Cancellation token | Custom `Arc<AtomicBool>` flag | `tokio_util::CancellationToken` | Thread-safe, composable, official ecosystem solution; already the decided approach |

**Key insight:** The reorg detection and common-ancestor walk ARE hand-rolled because they depend on our specific block DAG storage — no library knows our storage schema.

---

## Common Pitfalls

### Pitfall 1: Reorg Detection Race — Block Arriving Out of Order vs Genuine Reorg

**What goes wrong:** A block arrives whose `parent_hash` doesn't match the canonical tip, but this is because it arrived out of order (a future block's sibling), not because of a reorg.

**Why it happens:** EVM subscriptions can occasionally deliver blocks slightly out of order or deliver uncle blocks.

**How to avoid:** The detection condition (incoming `parent_hash != stored canonical tip hash`) is correct for detecting that a block is NOT a child of the current tip. However, we must also handle the case where the incoming block's `parent_hash` IS known but is a sibling (not an ancestor) of the tip. The `find_common_ancestor()` walk handles this correctly — it will find the actual fork point regardless.

**Warning signs:** `ReorgWithoutAncestor` error in production with depth=1 blocks on test networks.

### Pitfall 2: State Re-derivation Cost on Deep Reorgs

**What goes wrong:** A deep reorg (10+ blocks, rare but possible) triggers full replay from nearest snapshot, which could be many blocks back.

**Why it happens:** Snapshot density determines replay cost. With `snapshot_every_n = 100`, a reorg past the last snapshot triggers 100-block replay.

**How to avoid:** This is acceptable for Phase 3 — the decision to keep snapshots synchronous was deliberate. Document the cost model for users. Phase 4 / v2 can add adaptive snapshots (OPT-01). For Phase 3, typical reorgs are 1–3 blocks deep and resolution is instant.

**Warning signs:** `on_reorg` callback taking >100ms in production (indicates deep reorg + no nearby snapshot).

### Pitfall 3: `ready_chunks()` Capacity Too Small or Too Large

**What goes wrong:** If capacity=1, micro-batching is disabled (never more than 1 block per transaction). If capacity is very large (e.g., 10000), memory spikes during sync.

**Why it happens:** `ready_chunks(N)` buffers up to N items before yielding; the batch size is bounded by N.

**How to avoid:** 64 is a reasonable default — covers normal block burst scenarios without excessive memory. Expose as `batch_size` in `EngineConfig`.

**Warning signs:** SQLite WAL growing unboundedly (too-large batches), or slow throughput during sync (too-small batches).

### Pitfall 4: Cancel Safety — Don't Use `run_until_cancelled()` in a Loop

**What goes wrong:** `token.run_until_cancelled(batch_stream.next())` in a loop drops the stream adaptor's internal buffer on cancellation, losing buffered blocks.

**Why it happens:** `run_until_cancelled` drops the future on cancel — the `ready_chunks` adaptor accumulates items internally, and this accumulation is lost.

**How to avoid:** Use `tokio::select! { biased; _ = token.cancelled() => ..., batch = batched.next() => ... }`. `StreamExt::next()` is documented as cancel-safe.

**Warning signs:** Dropped blocks / gaps in state after graceful restart.

### Pitfall 5: Canonical Tip Not Updated Before Gap Fill

**What goes wrong:** After reorg, `set_canonical_tip()` is not called before `fetch_gap_blocks()`. Gap fill starts from wrong height.

**Why it happens:** The gap fill reads `get_canonical_tip()` to know where to start. If tip still points to old branch, gap fill fetches wrong range.

**How to avoid:** Call `set_canonical_tip(common_ancestor)` before any gap-fill operations. The incoming block's `process_block()` call will then update tip to the new block correctly.

### Pitfall 6: Storage Trait Requires New Methods

**What goes wrong:** Forgetting that `get_parent_hash()` and `set_canonical_tip()` are not in the current Storage trait. Both InMemoryStorage and SqliteStorage need implementations.

**Why it happens:** Phase 2 Storage trait has `write_block` (which atomically updates canonical_tip), but no explicit `set_canonical_tip()` and no `get_parent_hash()`.

**How to avoid:** Plan tasks to add these Storage trait methods first, before implementing reorg logic. See Storage additions section below.

---

## Code Examples

### CancellationToken Basic Pattern

```rust
// Source: https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html
use tokio_util::sync::CancellationToken;

let token = CancellationToken::new();
let cloned_token = token.clone();

let task = tokio::spawn(async move {
    engine.run(source, cloned_token, on_reorg, on_status).await
});

// Trigger graceful shutdown:
token.cancel();
task.await??;
```

### Select Loop with Cancel Safety

```rust
// Source: https://docs.rs/tokio/latest/tokio/macro.select.html (cancel safety docs)
// StreamExt::next() is documented as cancel-safe.
loop {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            // Flush anything pending, then return.
            return Ok(());
        }
        batch = batched.next() => {
            match batch {
                None => break,   // stream ended cleanly
                Some(items) => { /* process batch */ }
            }
        }
    }
}
```

### Backoff During Reconnect (Cancel-Aware)

```rust
// Reconnect with cancel-aware sleep between attempts.
let delay = backoff_delay(&self.config, attempt);
tokio::select! {
    biased;
    _ = cancel.cancelled() => return Ok(()),
    _ = tokio::time::sleep(delay) => {}
}
```

### Gap Fill After Reconnection

```rust
// Reuse recovery.rs fetch_gap_blocks — no changes needed.
use crate::coordinator::recovery::fetch_gap_blocks;

let last_height = self.current_tip_height();
fetch_gap_blocks(&mut self, &source, last_height + 1, reconnect_height - 1).await
    .map_err(EngineError::from_recovery)?;
```

---

## Storage Trait Additions Required

Two new methods needed on the `Storage` trait (both InMemoryStorage and SqliteStorage):

### 1. `get_parent_hash()`

```rust
/// Get the parent hash of a block, or None if block doesn't exist.
/// Returns Some([0u8; 32]) for genesis (no parent sentinel).
fn get_parent_hash(&self, block_hash: &[u8; 32]) -> Result<Option<[u8; 32]>, Self::Error>;
```

InMemoryStorage: single map lookup on `inner.blocks[block_hash].parent_hash`.
SqliteStorage: `SELECT parent_hash FROM blocks WHERE hash = ?`.

### 2. `set_canonical_tip()`

```rust
/// Explicitly set the canonical tip for a source to the given block hash.
/// Used during reorg handling to update the tip to the common ancestor
/// before replaying the new canonical branch.
fn set_canonical_tip(&self, source_id: &str, block_hash: &[u8; 32]) -> Result<(), Self::Error>;
```

InMemoryStorage: `inner.canonical_tips.insert(source_id, *block_hash)`.
SqliteStorage: `INSERT OR REPLACE INTO canonical_tip (source_id, block_hash) VALUES (?, ?)`.

Note: `write_block()` already updates canonical_tip atomically as part of block insertion — this new method is the explicit setter for reorg path only.

---

## New Public Types

### `EngineError`

```rust
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
```

### `ReorgEvent`

```rust
#[derive(Debug, Clone)]
pub struct ReorgEvent {
    pub common_ancestor: [u8; 32],  // hash of the fork point block
    pub old_tip: [u8; 32],          // last hash on the discarded branch
    pub new_tip: [u8; 32],          // first block on the new canonical branch
    pub depth: u32,                 // number of blocks rolled back
}
```

### `EngineStatus`

```rust
#[derive(Debug, Clone)]
pub enum EngineStatus {
    Disconnected,
    Reconnecting(u32),   // attempt number (1-based)
    Reconnected,
    GapFilling,
}
```

### `EngineConfig` additions

```rust
pub struct EngineConfig {
    pub source_id: String,
    pub snapshot_every_n: usize,
    // New in Phase 3:
    pub max_retries: u32,        // 0 = no retries (fail immediately)
    pub initial_delay_ms: u64,   // starting backoff delay
    pub max_delay_ms: u64,       // cap on backoff delay
    pub batch_size: usize,       // micro-batch capacity for ready_chunks()
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Reorg = client responsibility | Reorg = transparent branch switch, library re-derives state | Phase 3 design (2026-03-05) | Client never sees inconsistent state |
| Blocking event loops | `tokio::select!` with cancel-safe `StreamExt::next()` | Standard since tokio 1.x | Correct graceful shutdown without dropped blocks |
| External retry crates for backoff | Hand-rolled with `tokio::time::sleep` | Always viable for simple cases | Zero additional deps, full control |
| `ready_chunks()` in futures-util 0.3+ | Same — already available | Available since futures 0.3 | Stream micro-batching without hand-rolling |

**Deprecated/outdated:**
- `tokio::time::sleep` without cancel-aware `select!`: pattern is safe but ignores cancellation during sleep — must always pair with `select!`.
- `bincode` for serialization: already tombstoned per project decision (use `postcard`). Confirmed in project state.

---

## Open Questions

1. **Should `Engine::run()` accept callbacks by value (consuming) or as `&mut impl FnMut`?**
   - What we know: FnMut passed as `mut on_reorg: impl FnMut(ReorgEvent)` works for owned closures; `&mut impl FnMut` allows caller to inspect callback state after run() returns.
   - What's unclear: Phase 4 may want to observe events from outside the callback. Watch channels (already used for state) would be cleaner long-term.
   - Recommendation: Use `impl FnMut` for Phase 3 (matches `on_status` pattern decided in CONTEXT.md). Phase 4 can wrap in watch if needed.

2. **Gap fill strategy after reconnect: what height does the new subscription start at?**
   - What we know: `EventSource::subscribe(from_height)` is called with the last tip height. The new stream MAY or MAY NOT include blocks at exactly `from_height` depending on implementation.
   - What's unclear: Off-by-one at the subscription start height. If the subscription delivers `from_height` and we also gap-fill `from_height`, we get a duplicate.
   - Recommendation: Start subscription at `tip_height + 1` (next block). Use existing `process_block()` deduplication as the safety net — duplicate blocks from overlapping ranges are silently ignored. This is tested behavior from Phase 2.

3. **`storage_mut()` on Engine: does the Engine need interior mutability for storage?**
   - What we know: Current Storage trait methods take `&self` (the trait uses interior mutability via `Mutex<Connection>`). `Engine::storage()` returns `&S`.
   - What's unclear: `set_canonical_tip` would be a `&self` method on Storage (consistent with existing design). Engine can call it via `self.storage().set_canonical_tip(...)`.
   - Recommendation: Follow existing pattern — all Storage methods take `&self`. No `storage_mut()` needed.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in (`#[tokio::test]` via tokio 1.49.0 with `rt` + `macros` features) |
| Config file | None — Cargo.toml `[dev-dependencies]` |
| Quick run command | `cargo test --test coordinator_tests` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INGEST-01 | Live blocks from stream applied to state | integration | `cargo test --test live_subscription_tests` | No — Wave 0 |
| INGEST-05 | No IPC on hot path (structural — verify by inspection) | unit (structural) | `cargo test --test coordinator_tests` | Partially (coordinator_tests.rs exists) |
| SAFETY-01 | Canonical tip updated correctly on each live block | integration | `cargo test --test live_subscription_tests` | No — Wave 0 |
| SAFETY-02 | Reorg: state re-derived from fork point transparently | integration | `cargo test --test reorg_tests` | No — Wave 0 |
| SAFETY-03 | Reconnect with backoff; gap fill after reconnect | integration | `cargo test --test reconnect_tests` | No — Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before verify

### Wave 0 Gaps
- [ ] `tests/live_subscription_tests.rs` — covers INGEST-01, SAFETY-01 (mock stream, verify state updates)
- [ ] `tests/reorg_tests.rs` — covers SAFETY-02 (inject block with wrong parent_hash, verify state re-derivation)
- [ ] `tests/reconnect_tests.rs` — covers SAFETY-03 (mock stream that ends, verify reconnect + gap fill)

The existing test pattern from `recovery_tests.rs` is the model: mock `EventSource`, mock `InMemoryStorage`, assert final state. Reorg tests will need a `MockStream` that can inject blocks with arbitrary parent hashes.

---

## Sources

### Primary (HIGH confidence)
- `https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html` — CancellationToken API, version 0.7.18, `run_until_cancelled()`, cancel/cancelled methods
- `https://docs.rs/tokio/latest/tokio/macro.select.html` — tokio::select! cancel safety docs; confirms StreamExt::next() is cancel-safe
- `https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html` — `ready_chunks()`, `poll_next_unpin()` signatures
- `https://tokio.rs/tokio/topics/shutdown` — canonical graceful shutdown pattern with CancellationToken + select!
- Codebase read: `src/coordinator/mod.rs`, `src/coordinator/recovery.rs`, `src/storage/mod.rs`, `src/storage/memory.rs`, `src/storage/sqlite.rs`, `tests/recovery_tests.rs` — verified existing APIs and test patterns

### Secondary (MEDIUM confidence)
- `https://bilinearlabs.io/blog/indexing-blockchain-events/` — blockchain indexer reorg handling: parent_hash mismatch detection, hybrid polling/subscription model
- QuickNode reorg handling docs — confirms "walk backward from new block parent until finding stored hash = common ancestor" is the standard pattern
- WebSearch: tokio cancel safety patterns — cybernetist.com 2024 — confirms biased select! for ordered branch priority

### Tertiary (LOW confidence)
- WebSearch: blockchain reorg depth statistics — typical reorg depth 1-5 blocks on mainnet (unverified, cited from multiple sources)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libraries already in Cargo.toml or verified via crates.io; tokio-util version confirmed at docs.rs
- Architecture patterns: HIGH — patterns derived directly from official tokio docs and existing codebase analysis
- Pitfalls: HIGH — pitfalls 1, 4, 5, 6 derived from code analysis; pitfalls 2, 3 from domain knowledge (verified against existing snapshot mechanism)
- Storage additions: HIGH — derived from code analysis of existing Storage trait; additions are minimal extensions of existing design

**Research date:** 2026-03-05
**Valid until:** 2026-04-05 (stable ecosystem — tokio/futures APIs are stable)
