# Phase 3: Live Subscriptions and Reorg Safety - Context

**Gathered:** 2026-03-05
**Status:** Ready for planning

<domain>
## Phase Boundary

The library tracks the chain tip in real time — events flow from a live EventSource subscription through the write-first pipeline to state updates and callbacks, the library reconnects transparently after disconnection with gap recovery, and clients are notified of chain reorganizations so they can decide how to respond. No query API, no historical reconstruction optimization, no ERC20 PoC — those are Phase 4.

</domain>

<decisions>
## Implementation Decisions

### Run loop ownership
- Engine::run() is a long-lived async fn the caller spawns as a tokio task. Library owns the event loop internally; caller owns task lifecycle.
- Shutdown via tokio_util::CancellationToken passed to Engine::run(). Engine finishes current batch, flushes, returns Ok(()).
- Engine::run() returns Result<(), EngineError>. Ok on graceful cancellation, Err on unrecoverable failures (storage errors, reconnection exhausted).
- Recovery and run are separate calls: caller calls engine.recover(&source).await? first, then engine.run(source, token).await?. Matches Phase 2's existing recover() API. Fresh starts skip recover().

### Reorg detection model
- Detection via block hash comparison: when a new event arrives at a cursor already stored, compare block_hash values. Divergence means reorg.
- Notification carries full context: ReorgEvent { common_ancestor, old_tip, new_tip, depth }. Matches ROADMAP.md success criteria exactly.
- Client receives reorg notification via synchronous callback (on_reorg: FnMut(ReorgEvent)) passed to Engine::run(). Runs inline on the event processing path, same pattern as OnChange.
- Library handles mechanical cleanup after reorg: purge invalidated events from storage (cursors > common_ancestor), roll back coverage, replay new canonical events. Client only decides whether to continue.

### Reconnection ownership
- Library retries EventSource::subscribe() on stream end/error with exponential backoff. Client's subscribe() impl creates a new connection each time. Library controls retry logic; client controls connection creation.
- Backoff strategy configurable in EngineConfig: max_retries, initial_delay_ms, max_delay_ms. Caller tunes for their use case.
- After reconnection, reuse Phase 2's fetch_gap() code path to fill the gap between last anchor and new subscription start. Same tested pipeline, no redundant RPC calls.
- Status callback (on_status: FnMut(EngineStatus)) for operational signals: Disconnected, Reconnecting(attempt), Reconnected, GapFilling. Optional — useful for logging/monitoring.

### Hot path optimization
- Keep async-trait for Phase 3. 10-50ns heap allocation per call is negligible vs network latency (ms-order). Migration to trait_variant deferred until benchmarks show need.
- Keep snapshot writes synchronous. WAL mode + periodic snapshots (~1ms write) is fast enough. Background writes add concurrency complexity (crash safety) for minimal gain.
- Live events micro-batched: drain all currently-available events from the stream (non-blocking poll), process as a single batch in one SQLite transaction. Falls back to single-event processing when stream is slow. Better throughput during bursts (new block with many events).
- block_hash stays Option<Hash> on Event. Reorg detection activates only when block_hash is Some. CEX sources (no blocks) naturally skip reorg checks. No API change needed.

### Claude's Discretion
- Exact EngineError variants and error hierarchy
- Internal task structure within Engine::run() (single loop vs multiple tasks)
- ReorgEvent struct fields beyond the decided four (common_ancestor, old_tip, new_tip, depth)
- EngineStatus enum variants beyond the decided four
- Micro-batch drain strategy (poll_next vs try_next vs FuturesUnordered)
- How to find common_ancestor during reorg detection (walk back stored block hashes)

</decisions>

<specifics>
## Specific Ideas

- Phase 2's fetch_gap() in recovery.rs is the proven pattern for gap recovery — reuse it for post-reconnection gap filling rather than building a separate mechanism
- The write-first invariant (store then apply) from Phase 2 must be preserved in the live event loop — events from the subscription go through process_events() which already enforces this
- Engine currently has no run() method — it's a passive struct. Phase 3 adds the active event loop while keeping the existing API (process_events, replay_stored_events, subscribe, state) intact

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `Engine<SM, S, C>` (coordinator/mod.rs): already owns ReducerCore + Storage + Codec, has process_events() and subscribe()
- `fetch_gap()` (coordinator/recovery.rs): proven gap recovery with chunked fetching and coverage tracking
- `recover()` (coordinator/recovery.rs): full cold-start pipeline — can be called before run()
- `EventSource::subscribe()` (source.rs): returns Box<dyn Stream> — the stream Engine::run() will consume
- `process_events()` (coordinator/mod.rs): write-first pipeline already handles dedup, anchors, coverage, snapshots
- `Event<C, E>` (event.rs): carries block_hash: Option<Hash> — used for reorg detection

### Established Patterns
- Write-first invariant: store events in SQLite before applying to ReducerCore (coordinator/mod.rs)
- tokio::sync::watch for state distribution — Engine::subscribe() already exposes this
- Opaque bytes: client-owned serialization via Codec trait, library never interprets payloads
- Sync Storage trait with spawn_blocking bridge for async callers
- INSERT OR IGNORE deduplication at storage boundary

### Integration Points
- Engine::run() is the new entry point — consumes EventSource stream, calls existing process_events()
- EngineConfig gets new fields: backoff configuration, max_retries
- Engine gets new method: run(source, cancel_token, on_reorg, on_status)
- Storage trait may need delete_events_after(cursor) for reorg purge
- Coverage table needs rollback support for reorg (remove coverage past common_ancestor)

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 03-live-subscriptions-and-reorg-safety*
*Context gathered: 2026-03-05*
