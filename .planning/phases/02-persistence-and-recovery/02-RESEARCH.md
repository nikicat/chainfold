# Phase 2: Block DAG Storage and Recovery - Research

**Researched:** 2026-03-05
**Domain:** SQLite block DAG persistence, block-oriented event sourcing, canonical chain recovery
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Block type design
- New `Block<C, E>` wrapper type — first-class concept holding block_hash, parent_hash, height, and `Vec<Event<C, E>>`
- `block_hash` removed from `Event<C, E>` — it lives exclusively on `Block`. Events always exist within a Block context.
- `EventSource` gains `fetch_blocks(from, to) → Vec<Block>` replacing `fetch_events`. `subscribe()` returns `Stream<Block>`. Block-oriented API throughout.
- `StateMachine::apply(state, event)` unchanged — coordinator iterates `block.events` and calls apply for each. Client doesn't need to know about blocks. Pure reducer stays simple.

#### SQLite schema design
- Normalized schema: `blocks(hash, parent_hash, height, source_id)` + `events(block_hash, cursor, payload)` with foreign key to blocks. Clean relational model.
- Single shared database per app process with source_id discriminator. Aligns with PROJECT.md "one SQLite file per app" constraint.
- Explicit `canonical_tip(source_id, block_hash)` table — updated when new blocks extend the chain or when reorg changes the tip. Simple to query.
- Event payloads stored as opaque BLOB — client provides encode/decode. Library never interprets event contents.
- No coverage table — block existence in the blocks table IS coverage. Gap detection = walk canonical chain and find missing blocks.
- No anchors table — canonical_tip is the only bookmark needed. Application-level bookmarks are not library concerns.
- Block-level deduplication — if block_hash already exists in blocks table, skip all its events. Blocks always arrive atomically with all their events.

#### Snapshot lifecycle
- Snapshots keyed by block_hash: `snapshots(source_id, block_hash, payload)`. State at a block is deterministic regardless of which branch it's on.
- Snapshots triggered every N blocks (configurable). Predictable worst-case replay cost.
- Snapshot format is client-owned opaque bytes — library stores/loads Vec<u8>, client picks serialization format.
- All snapshots retained — never deleted. Supports Phase 4 At(cursor) historical queries.
- Snapshot writes are synchronous — block processing until write completes. Simple, guarantees durability.

#### Recovery pipeline
- On crash recovery: read canonical tip → walk parent pointers back until a block with a snapshot is found → load snapshot → replay canonical chain forward to tip → fetch any gap blocks from source. Simple, deterministic.
- On corrupt/undeserializable snapshot: fall back to next-oldest snapshot along the canonical chain. If none work, fail with error.
- Gap fetching uses `EventSource::fetch_blocks()` — block-oriented, not event-oriented.

#### Codec simplification
- Library handles block metadata serialization (hash, parent_hash, height) with fixed schema — not client-controlled.
- Client provides encode/decode only for event payloads and state snapshots. Codec becomes simpler.
- Cursor serialization for storage uses postcard (library-internal). Event and snapshot serialization is client-owned.

#### Storage API surface
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

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| INGEST-02 | Library fetches historical blocks via EventSource in chunked range queries respecting RPC provider limits | `fetch_blocks(from, to)` called in a loop with advancing block height/hash cursor; chunk size implicit in EventSource impl |
| INGEST-03 | Library stitches gap between cached blocks and live subscription start without missing or duplicating blocks | canonical_tip is the bookmark; gap = blocks between canonical_tip.height and live subscription start; fetch_blocks fills it atomically |
| INGEST-04 | Library deduplicates by block_hash — overlapping backfill/subscription windows don't corrupt state | Block existence check before insert: if block_hash row exists, skip entire block + all its events. INSERT OR IGNORE on blocks table. |
| PERSIST-01 | Library persists blocks (with parent hash), events (per block), and state snapshots (per block hash) in SQLite with WAL mode | Three-table schema: blocks + events + snapshots + canonical_tip. WAL mode via pragma. rusqlite bundled. |
| PERSIST-02 | Library tracks block DAG — which blocks exist, their parent relationships, and which branch is canonical | blocks table has parent_hash column enabling parent pointer traversal. canonical_tip(source_id, block_hash) tracks the canonical branch head. |
| PERSIST-03 | Library recovers from crash: find canonical tip, load nearest snapshot on canonical chain, replay forward, catch up via fetch_blocks | Recovery: read canonical_tip → walk parent pointers to find snapshot → load snapshot → replay forward along canonical chain → fetch_blocks for gap |
| PERSIST-04 | Library creates periodic state snapshots (client-owned serialization) at block boundaries | Snapshot triggered every N blocks (counted on distinct block inserts). snapshots(source_id, block_hash, payload). |
| PERSIST-05 | Library maintains named anchor positions (persistent cursors) updated atomically with block processing | canonical_tip table IS the anchor — updated in same transaction as block write. No separate anchors table needed. |
</phase_requirements>

---

## Summary

Phase 2 redesigns the persistence layer from a flat linear event store to a block DAG model. The fundamental shift is that **blocks are the unit of persistence**, not individual events. Each block has a `block_hash` (32-byte identifier), `parent_hash` (pointer to parent block, enabling graph traversal), `height`, and owns a list of events. The SQLite schema is normalized: a `blocks` table holds block metadata, an `events` table holds event payloads with a foreign key to `blocks`. This is structurally similar to how git stores commits and trees.

The canonical chain is tracked via a `canonical_tip(source_id, block_hash)` table — a single row per source pointing to the tip of the canonical branch. Recovery is deterministic: read the canonical tip, walk parent pointers backward until a snapshotted block is found, load that snapshot, then replay events from each block on the canonical chain forward to the tip, then fetch any gap blocks from the source. Block-level deduplication (if `block_hash` exists in the blocks table, skip the entire block) is far simpler than the old cursor-range coverage table approach.

The existing codebase (Phase 1) contributes: `ReducerCore` (unchanged), `StateMachine` trait (unchanged), `ChainCursor` trait (unchanged), cursor types (`EvmCursor`, `BtcCursor`, `CexCursor`, unchanged). The `EventSource` trait gains `fetch_blocks()` replacing `fetch_events()`. `Event<C,E>` loses its `block_hash` field (moves to `Block<C,E>`). The old Phase 2 code (`src/storage/`, `src/coordinator/`) is fully replaced.

**Primary recommendation:** Keep rusqlite + rusqlite_migration (already in Cargo.toml). Replace the flat schema with the normalized block DAG schema. The coordinator becomes block-oriented: receive `Block<C,E>` → write block+events in one transaction → iterate block.events calling ReducerCore::apply for each. Recovery walks the parent pointer chain.

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| rusqlite | 0.34 (bundled, already in Cargo.toml) | SQLite bindings | Already present; sync API matches Storage trait pattern; bundled feature eliminates system SQLite dep |
| rusqlite_migration | 2 (already in Cargo.toml) | Schema migrations | Already present; user_version pragma; const migrations; will add new migration version for DAG schema |
| postcard | 1 (already in Cargo.toml) | Cursor serialization (library-internal) | Already present; STATE.md decision: postcard replaces bincode. Used for serializing ChainCursor values to TEXT/BLOB in the blocks table |
| thiserror | 2 (already in Cargo.toml) | Error types | Already present |

### No New Dependencies Required

The entire Phase 2 implementation uses only libraries already in `Cargo.toml`. No new crates needed.

The old research mentioned `tokio-rusqlite` as the async bridge. The existing implementation uses `Mutex<Connection>` directly (Storage trait is sync, coordinator uses it synchronously). This pattern is already proven in the existing code and tests — continue with it. `tokio-rusqlite` is unnecessary complexity.

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tempfile | 3 (already in dev-dependencies) | Temp DB files in tests | Already used in storage_tests.rs for SqliteStorage tests |
| tokio | 1 (already in dev-dependencies) | Async test runtime | Already used in recovery_tests.rs |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Mutex<Connection> (current pattern) | tokio-rusqlite | tokio-rusqlite adds cleaner async ergonomics but the existing sync Storage trait + Mutex pattern works, is already tested, and avoids a new dependency |
| block_hash as BLOB(32) | TEXT (hex) | BLOB is more compact and faster for equality checks; TEXT is human-readable in sqlite3 CLI. BLOB wins for a library cache. |
| postcard for cursor serialization | serde_json | postcard is already present and chosen in STATE.md; JSON is larger and slower |

---

## Architecture Patterns

### Recommended Module Structure

```
src/
├── block.rs              # NEW: Block<C, E> type definition
├── storage/
│   ├── mod.rs            # REPLACE: Storage trait for DAG model (Block-aware API)
│   ├── sqlite.rs         # REPLACE: SqliteStorage implementing DAG schema
│   ├── memory.rs         # REPLACE: InMemoryStorage for testing (same DAG semantics)
│   └── schema.rs         # REPLACE: New DDL migration (v2 schema: blocks, events, snapshots, canonical_tip)
├── coordinator/
│   ├── mod.rs            # REPLACE: Engine — block-oriented coordinator
│   └── recovery.rs       # REPLACE: Block DAG recovery pipeline
├── event.rs              # MODIFY: Remove block_hash field from Event<C,E>
├── source.rs             # MODIFY: Add fetch_blocks(), change subscribe() return type to Stream<Block>
├── cursor/               # UNCHANGED
├── machine.rs            # UNCHANGED (ReducerCore stays pure)
├── error.rs              # UNCHANGED
└── lib.rs                # UPDATE: re-exports for new types
```

### Pattern 1: Block<C, E> Type

**What:** First-class wrapper type that is the unit of persistence and processing.

**When to use:** Everywhere blocks are passed — storage writes, EventSource returns, coordinator input.

```rust
// Source: CONTEXT.md locked decisions + standard block chain data model
use crate::cursor::ChainCursor;
use crate::event::Event;

#[derive(Debug, Clone)]
pub struct Block<C: ChainCursor, E> {
    /// SHA-256 (or chain-native) hash of this block. 32 bytes.
    pub block_hash: [u8; 32],
    /// Hash of the parent block. All-zeros for genesis.
    pub parent_hash: [u8; 32],
    /// Block height (0-indexed from genesis).
    pub height: u64,
    /// Events in this block, in cursor order.
    pub events: Vec<Event<C, E>>,
}
```

### Pattern 2: Modified Event<C, E> (Remove block_hash)

**What:** `Event` no longer holds `block_hash` — that context lives on `Block`.

```rust
// MODIFY src/event.rs: remove block_hash field
#[derive(Debug, Clone)]
pub struct Event<C: ChainCursor, E> {
    pub cursor: C,
    pub data: E,
    // block_hash removed — lives on Block<C,E> instead
}
```

### Pattern 3: Updated EventSource Trait

**What:** `fetch_blocks` replaces `fetch_events`. `subscribe` returns `Stream<Block>`.

```rust
// Source: CONTEXT.md locked decisions
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch initial state snapshot at a specific cursor (cold start, no blocks in DB).
    async fn fetch_state(&self, at: &Self::Cursor) -> Result<Vec<u8>, Self::Error>;

    /// Fetch blocks in the height range [from_height, to_height).
    /// Returns blocks in ascending height order. Implementations respect RPC page limits.
    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<Self::Cursor, Self::Event>>, Self::Error>;

    /// Subscribe to live blocks from the given block height onwards.
    async fn subscribe(
        &self,
        from_height: u64,
    ) -> Result<
        Box<dyn futures_core::Stream<
            Item = Result<Block<Self::Cursor, Self::Event>, Self::Error>
        > + Unpin + Send>,
        Self::Error,
    >;
}
```

Note: Phase 2 only implements `fetch_blocks` for recovery. `subscribe()` is wired up in Phase 3. The trait must compile with `subscribe()` present but Phase 2 tests only exercise `fetch_blocks` and `fetch_state`.

### Pattern 4: Normalized Block DAG Schema

**What:** Three tables for block data + one for canonical tip tracking. Migration adds a new version to the existing `rusqlite_migration` chain.

**Key design points:**
- `blocks.hash` is BLOB(32) primary key — binary hash, not text
- `events.block_hash` foreign key enforces referential integrity
- `canonical_tip` is an upsert table — single row per source_id
- `snapshots.block_hash` keys snapshots to specific blocks (not cursors)

```sql
-- Migration v2 (replaces v1 tables entirely — or run as separate migration step)
-- OPTION A: Drop-and-replace (acceptable since this is a redesign, old data is invalid)
DROP TABLE IF EXISTS coverage;
DROP TABLE IF EXISTS anchors;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS snapshots;

CREATE TABLE blocks (
    hash        BLOB    NOT NULL PRIMARY KEY,  -- 32-byte block hash
    parent_hash BLOB    NOT NULL,              -- 32-byte parent hash (all-zeros for genesis)
    height      INTEGER NOT NULL,
    source_id   TEXT    NOT NULL
);

CREATE INDEX idx_blocks_source_height
    ON blocks(source_id, height);

CREATE INDEX idx_blocks_source_parent
    ON blocks(source_id, parent_hash);

CREATE TABLE events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    block_hash  BLOB    NOT NULL REFERENCES blocks(hash),
    cursor      BLOB    NOT NULL,  -- postcard-serialized ChainCursor
    payload     BLOB    NOT NULL   -- client-owned opaque bytes
);

CREATE INDEX idx_events_block
    ON events(block_hash);

CREATE TABLE snapshots (
    source_id   TEXT    NOT NULL,
    block_hash  BLOB    NOT NULL REFERENCES blocks(hash),
    payload     BLOB    NOT NULL,  -- client-owned opaque state bytes
    PRIMARY KEY (source_id, block_hash)
);

CREATE TABLE canonical_tip (
    source_id   TEXT    NOT NULL PRIMARY KEY,
    block_hash  BLOB    NOT NULL REFERENCES blocks(hash)
);
```

### Pattern 5: Block Write — Single Transaction

**What:** Block row + all event rows written atomically. canonical_tip updated in the same transaction.

**Critical invariant:** If block_hash already exists in blocks table, the entire block (including all events) is skipped — deduplication at block granularity.

```rust
// Source: CONTEXT.md locked decisions + rusqlite transaction pattern
fn write_block<C: ChainCursor, E>(
    conn: &rusqlite::Connection,
    source_id: &str,
    block: &Block<C, E>,
    encode_cursor: impl Fn(&C) -> Vec<u8>,   // postcard::to_allocvec
    encode_payload: impl Fn(&E) -> Vec<u8>,   // client-provided
) -> rusqlite::Result<bool> {  // returns true if block was new, false if duplicate
    // Check for duplicate block first (block-level dedup)
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = ?1)",
        rusqlite::params![&block.block_hash[..]],
        |row| row.get(0),
    )?;
    if exists {
        return Ok(false);  // skip entire block
    }

    let tx = conn.unchecked_transaction()?;

    // Insert block metadata
    tx.execute(
        "INSERT INTO blocks (hash, parent_hash, height, source_id)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            &block.block_hash[..],
            &block.parent_hash[..],
            block.height as i64,
            source_id,
        ],
    )?;

    // Insert events
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO events (block_hash, cursor, payload) VALUES (?1, ?2, ?3)"
        )?;
        for event in &block.events {
            let cursor_bytes = encode_cursor(&event.cursor);
            let payload_bytes = encode_payload(&event.data);
            stmt.execute(rusqlite::params![
                &block.block_hash[..],
                cursor_bytes,
                payload_bytes,
            ])?;
        }
    }

    // Update canonical_tip
    tx.execute(
        "INSERT INTO canonical_tip (source_id, block_hash) VALUES (?1, ?2)
         ON CONFLICT(source_id) DO UPDATE SET block_hash = excluded.block_hash",
        rusqlite::params![source_id, &block.block_hash[..]],
    )?;

    tx.commit()?;
    Ok(true)
}
```

### Pattern 6: Parent Pointer Walk for Recovery

**What:** Starting from canonical tip, walk parent_hash pointers backward through the blocks table to find the nearest block with a snapshot.

**When to use:** Recovery step 1 — finding the snapshot to load.

```rust
// Source: CONTEXT.md recovery pipeline design
fn find_nearest_snapshot(
    conn: &rusqlite::Connection,
    source_id: &str,
    tip_hash: [u8; 32],
) -> rusqlite::Result<Option<([u8; 32], Vec<u8>)>> {
    // Walk from tip toward genesis, check for snapshot at each block
    let mut current_hash = tip_hash;
    loop {
        // Check for snapshot at current block
        let snap: Option<Vec<u8>> = {
            let mut stmt = conn.prepare_cached(
                "SELECT payload FROM snapshots WHERE source_id = ?1 AND block_hash = ?2"
            )?;
            let mut rows = stmt.query_map(
                rusqlite::params![source_id, &current_hash[..]],
                |row| row.get(0),
            )?;
            rows.next().transpose()?
        };

        if let Some(payload) = snap {
            return Ok(Some((current_hash, payload)));
        }

        // Walk to parent
        let parent: Option<[u8; 32]> = {
            let mut stmt = conn.prepare_cached(
                "SELECT parent_hash FROM blocks WHERE hash = ?1"
            )?;
            let mut rows = stmt.query_map(
                rusqlite::params![&current_hash[..]],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    Ok(arr)
                },
            )?;
            rows.next().transpose()?
        };

        match parent {
            None => return Ok(None),  // genesis reached, no snapshot
            Some(p) if p == [0u8; 32] => return Ok(None),  // parent is all-zeros (genesis sentinel)
            Some(p) => current_hash = p,
        }
    }
}
```

### Pattern 7: Canonical Chain Forward Replay

**What:** After finding the snapshot block, collect all blocks on the canonical chain between snapshot and tip, in forward order, then replay their events.

**The challenge:** We have the tip and walk backward. To replay forward, collect the path during the backward walk, then reverse it.

```rust
// Source: CONTEXT.md recovery pipeline design
fn collect_canonical_chain(
    conn: &rusqlite::Connection,
    from_hash: [u8; 32],   // snapshot block (exclusive — state already loaded)
    to_hash: [u8; 32],     // canonical tip (inclusive)
) -> rusqlite::Result<Vec<[u8; 32]>> {
    // Walk backward from tip to from_hash, collect hashes, reverse
    let mut chain: Vec<[u8; 32]> = Vec::new();
    let mut current = to_hash;

    while current != from_hash {
        chain.push(current);

        let parent = conn.query_row(
            "SELECT parent_hash FROM blocks WHERE hash = ?1",
            rusqlite::params![&current[..]],
            |row| {
                let bytes: Vec<u8> = row.get(0)?;
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(arr)
            },
        )?;
        current = parent;
    }

    chain.reverse();  // now in forward order: first block to replay, last is tip
    Ok(chain)
}
```

### Pattern 8: Recovery Pipeline

**What:** Full recovery sequence, orchestrating patterns 6 and 7.

```
RECOVERY SEQUENCE:
1. Read canonical_tip → get tip_hash (None = no data, proceed to cold start)
2. Walk parent pointers from tip_hash → find (snapshot_block_hash, snapshot_payload)
   - If no snapshot: call EventSource::fetch_state() for cold start state
3. Decode snapshot payload via client Codec → load state into ReducerCore
4. Collect canonical chain from snapshot_block_hash to tip_hash (Pattern 7)
5. For each block_hash in canonical chain:
   a. Load events from storage (SELECT * FROM events WHERE block_hash = ?)
   b. Decode each event via client Codec
   c. Call ReducerCore::apply(&event) — NO storage write (replay only)
6. Determine gap: tip_hash.height → live_tip_height
7. If gap > 0: call EventSource::fetch_blocks(tip_height + 1, live_tip_height)
   - For each returned block: write_block() (stores + updates canonical_tip) + apply events
8. Hand off to Phase 3 live subscription
```

### Pattern 9: Simplified Codec Trait

**What:** Client provides encode/decode for event payloads and state only. Cursor serialization is library-internal (postcard).

```rust
// Source: CONTEXT.md codec simplification decision
pub trait Codec: Send + Sync + 'static {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type State: Clone + Send + 'static;

    /// Serialize event payload to bytes. Library wraps with block/cursor metadata.
    fn encode_event(&self, event: &Event<Self::Cursor, Self::Event>) -> Vec<u8>;

    /// Deserialize event payload. Returns None on corrupt/incompatible data.
    fn decode_event(
        &self,
        cursor: Self::Cursor,   // decoded by library via postcard
        payload: &[u8],
    ) -> Option<Event<Self::Cursor, Self::Event>>;

    /// Serialize state to opaque bytes for snapshot storage.
    fn encode_state(&self, state: &Self::State) -> Vec<u8>;

    /// Deserialize state from snapshot bytes. Returns None on corrupt/incompatible.
    fn decode_state(&self, bytes: &[u8]) -> Option<Self::State>;
}
```

Note: `cursor_to_tuple`/`tuple_to_cursor` from the old Codec are gone — no longer needed since cursors are stored via postcard, not as (major, minor) integer pairs in the block DAG schema.

### Anti-Patterns to Avoid

- **Deduplication at event granularity (old model):** The new model deduplicates at block granularity. If `block_hash` exists in the blocks table, skip the ENTIRE block. Never try to partially insert a block's events.

- **Walking the full chain from genesis on every recovery:** Walk backward from tip until you find a snapshot, then stop. Do not traverse the entire chain.

- **Using the old Storage trait methods (`write_events`, `cached_tip`, `add_coverage`, `write_anchor`):** These are all removed. The new API is block-oriented. Do not try to adapt old code — it must be replaced.

- **Storing block_hash as TEXT (hex):** Store as BLOB(32). Equality checks on BLOB are faster than TEXT comparison. The CLI can display as hex with `hex(hash)` if needed.

- **Committing block rows and event rows in separate transactions:** Block + all its events must be atomic. On crash between block insert and event inserts, the block would exist with no events — state would be wrong on recovery.

- **Applying events before writing the block to storage:** Write-first invariant from the old model still applies. Store the block atomically, then apply its events to ReducerCore.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Schema migrations | Manual `CREATE TABLE IF NOT EXISTS` + version tracking | rusqlite_migration (already in Cargo.toml) | Already used; user_version pragma is atomic; add a new M::up() for the DAG schema |
| Cursor serialization to bytes | Custom binary format | `postcard::to_allocvec` (already in Cargo.toml) | Already chosen in STATE.md; stable wire format; already present |
| Block hash → bytes conversion | New type | `[u8; 32]` directly | Block hashes ARE 32-byte arrays; no wrapper needed |
| Forward chain reconstruction | Graph algorithm | Backward walk + Vec::reverse() | Simple, no extra data structures needed since chain is linear between snapshot and tip |

**Key insight:** The block DAG model is simpler than the old coverage/anchor model. Block existence IS coverage. canonical_tip IS the anchor. The parent pointer chain IS the recovery path. The schema reflects these simplifications — four lightweight tables instead of the old events+snapshots+anchors+coverage.

---

## Common Pitfalls

### Pitfall 1: Block-level vs Event-level Deduplication

**What goes wrong:** Code checks for duplicate events by (block_hash, cursor) instead of checking for duplicate blocks by block_hash alone. This is slower and misses the architectural point — a block always arrives with all its events.

**Why it happens:** Carrying over the old cursor-level dedup mindset.

**How to avoid:** Single check: `SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = ?1)`. If true, skip the entire block, return `inserted = false`. Never partially insert a block.

**Warning signs:** Events table growing with duplicate entries; tests showing partial block inserts.

### Pitfall 2: Forward Replay from Wrong Start Point

**What goes wrong:** Recovery replays from the snapshot's parent block rather than the snapshot block itself. Events in the snapshot block get applied twice (once in the snapshot, once replayed). State becomes wrong.

**Why it happens:** Off-by-one confusion about whether the snapshot block's events are included in the snapshot payload.

**How to avoid:** The snapshot is keyed by `block_hash`. The snapshot payload contains state AFTER applying all events in that block. Therefore, forward replay starts from the NEXT block after the snapshot block (i.e., walk from snapshot_block.parent toward tip, but collect blocks AFTER the snapshot block). Concretely: `collect_canonical_chain(from_hash = snapshot_block_hash, to_hash = tip_hash)` — the from_hash block is excluded from replay.

**Warning signs:** State after recovery is higher than expected; crash-recovery round-trip test fails with off-by-N-events error.

### Pitfall 3: Parent Pointer Walk Infinite Loop

**What goes wrong:** The parent pointer walk doesn't terminate when reaching genesis (all-zeros parent hash). If genesis has no snapshot, the walk loops forever.

**Why it happens:** Genesis block's parent_hash is `[0u8; 32]` sentinel. If the walk doesn't recognize this sentinel, it tries to query a block with hash = all-zeros, finds nothing, and panics or loops.

**How to avoid:** In `find_nearest_snapshot`, explicitly check: `if parent_hash == [0u8; 32] { break }`. Also check if the block doesn't exist in the DB at all (should not happen if data is consistent, but defensively handle it).

**Warning signs:** Recovery hanging indefinitely; query returning None for parent_hash unexpectedly.

### Pitfall 4: Transaction Scope for canonical_tip Update

**What goes wrong:** `canonical_tip` is updated in a separate transaction after the block+events transaction commits. Process crashes between the two commits. canonical_tip still points to the old tip. On recovery, the new block's events are replayed (block is in DB) but canonical_tip doesn't include it — recovery stops one block short.

**Why it happens:** Splitting what should be one transaction into two for "simplicity."

**How to avoid:** ALWAYS update canonical_tip in the SAME transaction as the block and event rows. See Pattern 5 — it is a single `tx.commit()`.

**Warning signs:** After crash-recovery test, state is one block behind expected; canonical_tip doesn't match the highest block in the blocks table.

### Pitfall 5: Gap Fetch Pagination with Block Heights

**What goes wrong:** Gap fetch calls `fetch_blocks(from_height, to_height)` but the implementation advances `from_height` by the returned block count rather than by the actual height of the last returned block + 1. If the source returns blocks non-contiguously (e.g., skips heights), the cursor math is wrong.

**Why it happens:** Assuming returned block count == height range size.

**How to avoid:** Advance from_height by `last_returned_block.height + 1`, not by `returned_blocks.len()`. Check the actual height of the last block in each chunk.

**Warning signs:** Duplicate blocks fetched across chunk boundaries; gap never closes.

### Pitfall 6: Snapshot Trigger Counting on Duplicate Blocks

**What goes wrong:** `blocks_since_snapshot` counter increments even when a block is a duplicate (already in DB). Snapshots trigger more frequently than expected.

**Why it happens:** Incrementing the counter before checking for duplicate, or incrementing regardless of `write_block` return value.

**How to avoid:** Only increment `blocks_since_snapshot` when `write_block()` returns `true` (new block inserted). If it returns `false` (duplicate), do not count it.

**Warning signs:** Snapshots created far more frequently than the configured N-block interval.

### Pitfall 7: Migration Strategy for Old Schema

**What goes wrong:** The existing migration v1 (events/snapshots/anchors/coverage tables) conflicts with the new v2 schema. Using `CREATE TABLE IF NOT EXISTS` would silently leave old tables and use new ones simultaneously.

**Why it happens:** Not properly versioning the schema change.

**How to avoid:** Add migration v2 to the `rusqlite_migration` chain that DROPS the old tables and CREATES the new ones. Existing test databases will migrate on open. Since this is a development-phase redesign (no production deployments), dropping old data is acceptable. The migration must be:
```rust
M::up("
    DROP TABLE IF EXISTS coverage;
    DROP TABLE IF EXISTS anchors;
    -- Note: cannot drop events/snapshots yet if FK constraints exist
    -- Drop in correct order respecting existing FKs
    ...
    CREATE TABLE blocks (...);
    CREATE TABLE events (...);
    ...
")
```

**Warning signs:** Tests passing against old schema; new schema queries failing against stale test DBs.

---

## Code Examples

Verified patterns from the codebase and official sources:

### rusqlite Transaction + prepare_cached Pattern (already used in codebase)

```rust
// Source: src/storage/sqlite.rs (existing codebase — proven working)
let tx = conn.unchecked_transaction()?;
{
    let mut stmt = tx.prepare_cached(
        "INSERT OR IGNORE INTO events (source_id, cursor_major, cursor_minor, payload)
         VALUES (?1, ?2, ?3, ?4)",
    )?;
    for ev in events {
        stmt.execute(rusqlite::params![
            ev.source_id, ev.cursor_major, ev.cursor_minor, ev.payload
        ])?;
    }
}
tx.commit()?;
```

The DAG version follows the same pattern with block_hash instead of cursor columns.

### BLOB Primary Key Query Pattern

```rust
// Source: rusqlite docs — BLOB params via slice reference
let block_hash: [u8; 32] = /* ... */;
let exists: bool = conn.query_row(
    "SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = ?1)",
    rusqlite::params![&block_hash[..]],  // &[u8] implements ToSql as BLOB
    |row| row.get(0),
)?;
```

### postcard Cursor Serialization (library-internal)

```rust
// Source: src/storage (postcard already in Cargo.toml, decision in STATE.md)
use postcard::{to_allocvec, from_bytes};
use serde::{Serialize, de::DeserializeOwned};

fn serialize_cursor<C: Serialize>(cursor: &C) -> Vec<u8> {
    postcard::to_allocvec(cursor).expect("cursor serialization is infallible for well-formed types")
}

fn deserialize_cursor<C: DeserializeOwned>(bytes: &[u8]) -> Option<C> {
    postcard::from_bytes(bytes).ok()
}
```

ChainCursor already requires `Serialize + DeserializeOwned` bounds (see `src/cursor/mod.rs`), so this works without any new trait bounds.

### InMemoryStorage Clone Pattern (already proven)

```rust
// Source: src/storage/memory.rs — existing pattern, reuse for new DAG InMemoryStorage
#[derive(Clone)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<Inner>>,
}
// Clone gives shared state — test harness and engine share the same storage
```

### rusqlite_migration New Migration Version

```rust
// Source: src/storage/schema.rs (existing) — extend the migration chain
use rusqlite_migration::{Migrations, M};

pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up("/* v1 — old linear schema */"),
        M::up("
            /* v2 — block DAG schema */
            DROP TABLE IF EXISTS coverage;
            DROP TABLE IF EXISTS anchors;
            DROP TABLE IF EXISTS events;
            DROP TABLE IF EXISTS snapshots;

            CREATE TABLE blocks ( ... );
            CREATE TABLE events ( ... );
            CREATE TABLE snapshots ( ... );
            CREATE TABLE canonical_tip ( ... );
        "),
    ])
}
```

---

## What Must Be Deleted Before Implementing

The CONTEXT.md explicitly calls out old code that must be removed. These files must be deleted/rewritten — do not try to incrementally patch them:

| File | Status | Reason |
|------|--------|--------|
| `src/storage/mod.rs` | REWRITE | Storage trait changes completely (block-oriented API, no coverage/anchor methods) |
| `src/storage/sqlite.rs` | REWRITE | New schema, new write_block logic, parent pointer walk |
| `src/storage/memory.rs` | REWRITE | New InMemoryStorage for block DAG semantics |
| `src/storage/schema.rs` | MODIFY | Add migration v2 that drops old tables, creates new ones |
| `src/coordinator/mod.rs` | REWRITE | Engine becomes block-oriented (receives Block, not Vec<Event>) |
| `src/coordinator/recovery.rs` | REWRITE | New recovery pipeline using parent pointer walk |
| `src/event.rs` | MODIFY | Remove `block_hash` field |
| `src/source.rs` | MODIFY | Replace `fetch_events` with `fetch_blocks`; change `subscribe` return type |
| `src/lib.rs` | MODIFY | Update re-exports for new types |

New file to create:
| File | Status |
|------|--------|
| `src/block.rs` | NEW: `Block<C, E>` type |

Tests to delete (old model, will fail against new API):
- `tests/coordinator_tests.rs` — tests old Engine API
- `tests/recovery_tests.rs` — tests old recovery pipeline
- `tests/storage_tests.rs` — tests old Storage trait methods
- `tests/event_source_tests.rs` — tests old fetch_events API

Tests to keep (still valid):
- `tests/cursor_tests.rs` — cursors unchanged
- `tests/reducer_tests.rs` — ReducerCore unchanged (Phase 1)

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Flat events table with (cursor_major, cursor_minor) | Normalized blocks + events with BLOB block_hash | 2026-03-05 (DAG architecture decision) | Parent pointer traversal replaces coverage intervals; block existence IS coverage |
| Coverage table (interval list) | No coverage table — block existence in blocks table IS coverage | 2026-03-05 | Eliminates the complex interval merge algorithm entirely |
| Anchors table (named cursors) | canonical_tip table (single bookmark per source) | 2026-03-05 | Simpler: one row per source, updated atomically with block writes |
| Cursor-level deduplication (UNIQUE on cursor_major, cursor_minor) | Block-level deduplication (check block_hash existence before insert) | 2026-03-05 | Coarser granularity — whole block skip instead of per-event dedup |
| Cursor → (major, minor) INTEGER pair in Codec | Cursor → postcard bytes in blocks.events.cursor column | 2026-03-05 | Codec no longer needs cursor_to_tuple/tuple_to_cursor; postcard handles generic ChainCursor |
| tokio-rusqlite for async bridge | Mutex<Connection> (already implemented, working) | Existing codebase | Simpler, no new dependency; Storage trait is sync |
| bincode for serialization | postcard (tombstone Dec 2025) | STATE.md decision | Already in Cargo.toml as postcard |

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Rust built-in (`#[test]` + `#[tokio::test]`) |
| Config file | none (standard cargo test) |
| Quick run command | `cargo test` |
| Full suite command | `cargo test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INGEST-02 | fetch_blocks called in loop for gap > page_size | integration | `cargo test test_chunked_gap_fetch` | ❌ Wave 0 |
| INGEST-03 | gap between canonical_tip and live tip fetched without duplication | integration | `cargo test test_gap_fetch_no_duplication` | ❌ Wave 0 |
| INGEST-04 | duplicate block_hash skips entire block | unit | `cargo test test_block_level_deduplication` | ❌ Wave 0 |
| PERSIST-01 | blocks, events, snapshots written to SQLite with WAL | integration | `cargo test test_sqlite_block_write` | ❌ Wave 0 |
| PERSIST-02 | blocks table tracks parent_hash; canonical_tip updated | unit | `cargo test test_canonical_tip_update` | ❌ Wave 0 |
| PERSIST-03 | crash recovery: tip → walk parents → snapshot → replay → gap fetch | integration | `cargo test test_crash_recovery_full_cycle` | ❌ Wave 0 |
| PERSIST-04 | snapshot created every N blocks on distinct block inserts | unit | `cargo test test_snapshot_every_n_blocks` | ❌ Wave 0 |
| PERSIST-05 | canonical_tip updated atomically with block write | unit | `cargo test test_canonical_tip_atomic_with_block` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

These test files are NET NEW (old test files for the linear model will be deleted):

- [ ] `tests/block_tests.rs` — covers Block<C,E> type construction; INGEST-04 (block-level dedup)
- [ ] `tests/storage_tests.rs` — REWRITE: covers new Storage trait (write_block, canonical_tip, snapshots by block_hash); PERSIST-01, PERSIST-02, PERSIST-05
- [ ] `tests/recovery_tests.rs` — REWRITE: covers full crash recovery pipeline; PERSIST-03, PERSIST-04, INGEST-02, INGEST-03
- [ ] `tests/coordinator_tests.rs` — REWRITE: covers Engine block processing pipeline

Existing files to keep (no changes needed):
- `tests/cursor_tests.rs` — all 13 tests still pass, cursors unchanged
- `tests/reducer_tests.rs` — all 9 tests still pass, ReducerCore unchanged

---

## Open Questions

1. **How does `fetch_blocks` pagination work — by height range or by block hash?**
   - What we know: The signature `fetch_blocks(from_height: u64, to_height: u64)` uses height ranges. This matches EVM's block range RPC calls (eth_getLogs, eth_getBlockByNumber).
   - What's unclear: For EVM, the source fetches logs for a block range and constructs Block<EvmCursor, E> objects. This is a Phase 3 concern (EVM adapter), but the trait shape must accommodate it in Phase 2.
   - Recommendation: Use height range in the signature. Phase 2 only needs MockEventSource implementations in tests. Leave EVM adapter for Phase 3.

2. **Genesis block representation**
   - What we know: Genesis has no parent. Convention: `parent_hash = [0u8; 32]` sentinel.
   - What's unclear: Should genesis be stored in the blocks table, or is it an implied starting point? If `fetch_state` is called at genesis, the state is loaded without any blocks in the DB.
   - Recommendation: Genesis does NOT need to be in the blocks table. Cold start (no blocks in DB): call `fetch_state()`, initialize ReducerCore. First real block from `fetch_blocks` has some height > 0, its parent_hash is the genesis hash which isn't in DB — that's OK, we don't need to validate parent existence.

3. **What happens when canonical_tip has no snapshot on the canonical chain at all?**
   - What we know: Walk backward from tip looking for snapshot. If genesis reached with no snapshot, fall back to `fetch_state()` and then replay ALL blocks in the canonical chain from genesis to tip.
   - What's unclear: How far back can this walk go? Potentially thousands of blocks if snapshots are absent.
   - Recommendation: This is the correct fallback. With N=1000 snapshot interval, worst case is replaying 1000 blocks. Document that the first run (no snapshots) replays everything — expected behavior.

4. **Event cursor column: what type is the cursor in the events table?**
   - What we know: CONTEXT.md says "Cursor serialization for storage uses postcard (library-internal)." So `events.cursor` is a BLOB containing postcard-serialized `C`.
   - What's unclear: Does Phase 2 actually need to query events by cursor? Recovery replays events within a block (fetching by block_hash), not by cursor range. Cursor in events table is informational only.
   - Recommendation: Store cursor as BLOB (postcard bytes) in the events table. No index needed on events.cursor for Phase 2 — only `events.block_hash` is queried. Phase 4 (At(cursor) queries) will add the cursor index if needed.

---

## Sources

### Primary (HIGH confidence)

- Existing codebase (`src/storage/`, `src/coordinator/`, `src/cursor/mod.rs`) — verified by reading source; patterns from sqlite.rs, memory.rs, schema.rs are proven working (all tests pass)
- rusqlite 0.34 docs — BLOB parameter handling (`&[u8]` implements `ToSql`), `Connection::query_row`, `prepare_cached`, transactions
- rusqlite_migration 2.x — `Migrations::new(vec![M::up(...)])` pattern (currently used in schema.rs)
- postcard 1.x — `to_allocvec`, `from_bytes`; ChainCursor already has Serialize + DeserializeOwned bounds
- CONTEXT.md (02-CONTEXT.md) — locked decisions; authoritative for all architectural choices
- STATE.md — accumulated project decisions; bincode dead, postcard chosen, block DAG decision

### Secondary (MEDIUM confidence)

- SQLite BLOB primary key behavior — BLOB equality comparison is byte-for-byte; correct for block hashes
- Parent pointer traversal pattern — standard linked-list walk; applies directly to block parent_hash chain

### Tertiary (LOW confidence)

- None — all claims verified against codebase or official decisions

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new dependencies; all libraries already in Cargo.toml, already proven working
- Architecture: HIGH — derived directly from locked decisions in CONTEXT.md; backed by existing working code patterns in the codebase
- Pitfalls: HIGH — derived from understanding the new model deeply vs the old model; most pitfalls are architectural (wrong granularity, wrong transaction scope), not library-specific

**Research date:** 2026-03-05
**Valid until:** 2026-06-05 (libraries stable; architecture locked by CONTEXT.md decisions)
