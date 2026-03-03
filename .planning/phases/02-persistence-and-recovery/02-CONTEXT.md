# Phase 2: Persistence and Recovery - Context

**Gathered:** 2026-03-03
**Status:** Ready for planning

<domain>
## Phase Boundary

SQLite-backed persistence for events, snapshots, anchors, and coverage ranges. The library survives process restarts: it persists state to SQLite, tracks which cursor ranges are cached, and executes the full cold-start recovery pipeline (load snapshot → replay cached events → catch up via FetchEvents → ready for live). No live subscriptions, no reorg handling, no query API — those are Phase 3+.

</domain>

<decisions>
## Implementation Decisions

### SQLite schema design
- Cursors stored as canonical (major, minor) INTEGER pair columns — enables native SQLite range queries and B-tree indexing. Each cursor type maps to its natural pair (block_number+log_index, height+tx_index, timestamp+seq_id).
- Single shared database per app process with source_id discriminator column on every table. Aligns with PROJECT.md "one SQLite file per app" constraint.
- Event payloads stored as opaque BLOB — client provides encode/decode (to_bytes/from_bytes trait). Library never interprets event contents. Consistent with snapshot pattern.
- Coverage tracked as per-source interval list: (source_id, range_start_major, range_start_minor, range_end_major, range_end_minor). Adjacent intervals merged on insert. Supports non-contiguous cached regions from backfill + live subscription.
- Deduplication enforced by UNIQUE constraint on (source_id, cursor_major, cursor_minor). INSERT OR IGNORE — duplicate inserts silently dropped at the database boundary.

### Snapshot lifecycle
- Snapshots triggered every N events (configurable). Predictable worst-case replay cost: at most N events to replay on recovery.
- Snapshot format is client-owned opaque bytes — library stores/loads Vec<u8>, client picks their own serialization format. Library is format-agnostic.
- All snapshots retained — never deleted. Supports Phase 4 At(cursor) historical queries. Pruning can be added later.
- Snapshot writes are synchronous — block event processing until write completes. Simple, guarantees durability. Hot-path optimization deferred to Phase 3.

### Recovery pipeline
- On corrupt/undeserializable snapshot: fall back to next-oldest snapshot. Keep trying until one deserializes. If none work, fail with error — client handles (clear DB, provide migration, etc).
- Gap fetching (catch-up) uses client-controlled pagination — EventSource::fetch_events called in a loop with advancing cursors. Page size is implicit in the EventSource implementation, which knows its RPC provider limits.
- Anchor updates in the same SQLite transaction as the event batch writes they track. Either both succeed or neither does. Guarantees consistency on crash.

### Storage API surface
- Storage defined as a trait with SQLite implementation. Enables in-memory mocks for testing the recovery pipeline without filesystem I/O.
- External coordinator pattern: a new Engine/Pipeline type owns both ReducerCore and Storage. Coordinates event flow: receive → store → apply. ReducerCore stays pure (no storage awareness), unchanged from Phase 1.
- Event writes batched — accumulate events in memory, flush to SQLite in a single transaction. Anchor update happens at flush time. Higher throughput, clear consistency boundary.
- Storage trait methods are sync (matching rusqlite's native API). Coordinator handles spawn_blocking when calling from async context.

### Claude's Discretion
- Exact SQLite table definitions and index choices
- Batch size defaults and configuration mechanism
- Internal coordinator architecture (channels, task structure)
- Coverage interval merge algorithm
- Snapshot table structure (cursor reference, timestamp, size)
- Error types for storage operations

</decisions>

<specifics>
## Specific Ideas

- Phase 1 decision: `events_in_range()` is O(n) linear scan on in-memory Vec — Phase 2 replaces with SQLite B-tree index lookup
- Phase 1 decision: `apply_and_accumulate()` owns events for the Vec — coordinator pattern means this method may change or the coordinator handles storage separately
- STATE.md flags: bincode is dead, use postcard for any library-internal serialization (cursor serialization for BLOB columns if needed). But event and snapshot serialization is now client-owned, so this mainly affects internal library storage (coverage ranges, anchors).
- Snapshot serialization is client-owned opaque bytes — same pattern as events. Library stores both as BLOB without interpreting contents.

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ChainCursor` trait (cursor/mod.rs): already requires `Serialize + DeserializeOwned` — ready for persistence
- `EvmCursor`, `BtcCursor`, `CexCursor`: concrete cursor types with natural (major, minor) pairs
- `Event<C, E>` envelope (event.rs): cursor + data + block_hash — needs serialization trait on E for persistence
- `ReducerCore` (machine.rs): current in-memory state management — coordinator wraps this, doesn't replace it
- `EventSource` trait (source.rs): `fetch_events(from, to)` and `fetch_state(at)` — used directly in recovery pipeline

### Established Patterns
- Opaque bytes for client data: `fetch_state` returns `Vec<u8>`, library stores without interpreting
- `tokio::sync::watch` for state distribution — coordinator publishes through ReducerCore
- `async-trait` for EventSource — coordinator calls fetch_events in the recovery loop
- Infallible `apply()` — coordinator doesn't need error handling on the state transition path

### Integration Points
- `ReducerCore::new()` stays unchanged — coordinator creates it with initial state from snapshot
- `ReducerCore::apply()` and `apply_and_accumulate()` called by coordinator after storing events
- `EventSource::fetch_events()` called by coordinator during gap catch-up
- `EventSource::fetch_state()` called during initial cold start when no snapshot exists
- New `Storage` trait introduced — SQLite impl is the production backend, in-memory impl for tests

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-persistence-and-recovery*
*Context gathered: 2026-03-03*
