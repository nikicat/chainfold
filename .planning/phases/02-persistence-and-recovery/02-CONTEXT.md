# Phase 2: Block DAG Storage and Recovery - Context

**Gathered:** 2026-03-05
**Status:** Ready for planning

<domain>
## Phase Boundary

SQLite-backed block DAG persistence. Blocks with parent hashes form a DAG (like git). Events belong to blocks. State snapshots keyed by block hash. The library tracks which block is the canonical tip per source, recovers from crash by replaying the canonical chain from the nearest snapshot, and fetches gap blocks from the source. No live subscriptions, no reorg handling, no query API — those are Phase 3+.

**Architecture change (2026-03-05):** This phase was redesigned from a flat linear event store to a block DAG model. Old Phase 2 code (Storage trait, SqliteStorage, Engine, recovery pipeline) is replaced.

</domain>

<decisions>
## Implementation Decisions

### Block type design
- New `Block<C, E>` wrapper type — first-class concept holding block_hash, parent_hash, height, and `Vec<Event<C, E>>`
- `block_hash` removed from `Event<C, E>` — it lives exclusively on `Block`. Events always exist within a Block context.
- `EventSource` gains `fetch_blocks(from, to) → Vec<Block>` replacing `fetch_events`. `subscribe()` returns `Stream<Block>`. Block-oriented API throughout.
- `StateMachine::apply(state, event)` unchanged — coordinator iterates `block.events` and calls apply for each. Client doesn't need to know about blocks. Pure reducer stays simple.

### SQLite schema design
- Normalized schema: `blocks(hash, parent_hash, height, source_id)` + `events(block_hash, cursor, payload)` with foreign key to blocks. Clean relational model.
- Single shared database per app process with source_id discriminator. Aligns with PROJECT.md "one SQLite file per app" constraint.
- Explicit `canonical_tip(source_id, block_hash)` table — updated when new blocks extend the chain or when reorg changes the tip. Simple to query.
- Event payloads stored as opaque BLOB — client provides encode/decode. Library never interprets event contents.
- No coverage table — block existence in the blocks table IS coverage. Gap detection = walk canonical chain and find missing blocks.
- No anchors table — canonical_tip is the only bookmark needed. Application-level bookmarks are not library concerns.
- Block-level deduplication — if block_hash already exists in blocks table, skip all its events. Blocks always arrive atomically with all their events.

### Snapshot lifecycle
- Snapshots keyed by block_hash: `snapshots(source_id, block_hash, payload)`. State at a block is deterministic regardless of which branch it's on.
- Snapshots triggered every N blocks (configurable). Predictable worst-case replay cost.
- Snapshot format is client-owned opaque bytes — library stores/loads Vec<u8>, client picks serialization format.
- All snapshots retained — never deleted. Supports Phase 4 At(cursor) historical queries.
- Snapshot writes are synchronous — block processing until write completes. Simple, guarantees durability.

### Recovery pipeline
- On crash recovery: read canonical tip → walk parent pointers back until a block with a snapshot is found → load snapshot → replay canonical chain forward to tip → fetch any gap blocks from source. Simple, deterministic.
- On corrupt/undeserializable snapshot: fall back to next-oldest snapshot along the canonical chain. If none work, fail with error.
- Gap fetching uses `EventSource::fetch_blocks()` — block-oriented, not event-oriented.

### Codec simplification
- Library handles block metadata serialization (hash, parent_hash, height) with fixed schema — not client-controlled.
- Client provides encode/decode only for event payloads and state snapshots. Codec becomes simpler.
- Cursor serialization for storage uses postcard (library-internal). Event and snapshot serialization is client-owned.

### Storage API surface
- Storage defined as a trait with SQLite implementation. Enables in-memory mocks for testing.
- External coordinator pattern: Engine owns ReducerCore + Storage. Coordinates: receive block → store block+events → apply events to state. ReducerCore stays pure.
- Block writes are transactional — block row + all event rows in a single SQLite transaction.
- Storage trait methods are sync (matching rusqlite's native API). Coordinator handles spawn_blocking for async context.

### Claude's Discretion
- Exact SQLite table DDL and index choices
- Block<C, E> struct layout details
- Internal coordinator architecture (channels, task structure)
- Batch size defaults and configuration mechanism
- Error types for storage operations
- How fetch_blocks pagination works internally

</decisions>

<specifics>
## Specific Ideas

- Old Phase 2 code (src/storage/, src/coordinator/) must be removed/replaced — it's built on the flat linear model
- Phase 1 ReducerCore stays unchanged — coordinator wraps it, doesn't replace it
- postcard for library-internal serialization (cursors in storage columns). bincode is dead.
- Block DAG degenerates to linear chain for non-block sources (CEX) — but that's Phase 3+ concern, not Phase 2

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ChainCursor` trait (cursor/mod.rs): `Ord + Clone + Serialize + DeserializeOwned` — ready for persistence
- `EvmCursor`, `BtcCursor`, `CexCursor`: concrete cursor types
- `Event<C, E>` envelope (event.rs): cursor + data — block_hash to be removed, moves to Block
- `ReducerCore` (machine.rs): in-memory state management — coordinator wraps this
- `EventSource` trait (source.rs): needs `fetch_blocks` added, `fetch_events` replaced or deprecated
- `tokio::sync::watch` for state distribution — coordinator publishes through ReducerCore

### Established Patterns
- Opaque bytes for client data: `fetch_state` returns `Vec<u8>`, library stores without interpreting
- `async-trait` for EventSource — coordinator calls fetch_blocks in recovery loop
- Infallible `apply()` — coordinator iterates block events, no error handling on state transitions
- `Codec` trait pattern from old Phase 2 — concept preserved but simplified for DAG model

### Integration Points
- `ReducerCore::new()` unchanged — coordinator creates it with initial state from snapshot
- `ReducerCore::apply()` called by coordinator for each event in a block
- `EventSource::fetch_blocks()` (new) called during gap catch-up
- `EventSource::fetch_state()` called during initial cold start when no snapshot exists
- New `Storage` trait replacing old one — block-aware API instead of flat event API
- Old storage/coordinator modules to be deleted and rewritten

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-persistence-and-recovery*
*Context gathered: 2026-03-05*
