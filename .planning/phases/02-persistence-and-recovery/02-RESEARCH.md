# Phase 2: Persistence and Recovery - Research

**Researched:** 2026-03-03
**Domain:** SQLite persistence, async Rust, event sourcing recovery pipeline
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### SQLite schema design
- Cursors stored as canonical (major, minor) INTEGER pair columns — enables native SQLite range queries and B-tree indexing. Each cursor type maps to its natural pair (block_number+log_index, height+tx_index, timestamp+seq_id).
- Single shared database per app process with source_id discriminator column on every table. Aligns with PROJECT.md "one SQLite file per app" constraint.
- Event payloads stored as opaque BLOB — client provides encode/decode (to_bytes/from_bytes trait). Library never interprets event contents. Consistent with snapshot pattern.
- Coverage tracked as per-source interval list: (source_id, range_start_major, range_start_minor, range_end_major, range_end_minor). Adjacent intervals merged on insert. Supports non-contiguous cached regions from backfill + live subscription.
- Deduplication enforced by UNIQUE constraint on (source_id, cursor_major, cursor_minor). INSERT OR IGNORE — duplicate inserts silently dropped at the database boundary.

#### Snapshot lifecycle
- Snapshots triggered every N events (configurable). Predictable worst-case replay cost: at most N events to replay on recovery.
- Snapshot format is client-owned opaque bytes — library stores/loads Vec<u8>, client picks their own serialization format. Library is format-agnostic.
- All snapshots retained — never deleted. Supports Phase 4 At(cursor) historical queries. Pruning can be added later.
- Snapshot writes are synchronous — block event processing until write completes. Simple, guarantees durability. Hot-path optimization deferred to Phase 3.

#### Recovery pipeline
- On corrupt/undeserializable snapshot: fall back to next-oldest snapshot. Keep trying until one deserializes. If none work, fail with error — client handles (clear DB, provide migration, etc).
- Gap fetching (catch-up) uses client-controlled pagination — EventSource::fetch_events called in a loop with advancing cursors. Page size is implicit in the EventSource implementation, which knows its RPC provider limits.
- Anchor updates in the same SQLite transaction as the event batch writes they track. Either both succeed or neither does. Guarantees consistency on crash.

#### Storage API surface
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

### Deferred Ideas (OUT OF SCOPE)
None — discussion stayed within phase scope
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| INGEST-02 | Library fetches historical events via EventSource in chunked range queries respecting RPC provider limits | Recovery pipeline uses fetch_events() in a loop with page-advancing cursors; coordinator calls EventSource::fetch_events per chunk |
| INGEST-03 | Library stitches the gap between historical backfill endpoint and live subscription start without missing or duplicating events | Coverage table tracks which ranges are cached; gap between cached tip and live tip is fetched before subscribing |
| INGEST-04 | Library deduplicates events by (source, cursor) so overlapping backfill and subscription windows don't corrupt state | UNIQUE constraint on (source_id, cursor_major, cursor_minor) + INSERT OR IGNORE enforces this at the storage boundary |
| PERSIST-01 | Library persists events, snapshots, anchors, and coverage ranges in SQLite with WAL mode | rusqlite 0.38.0 with WAL pragma; four-table schema; tokio-rusqlite 0.7.0 for async bridge |
| PERSIST-02 | Library tracks which cursor ranges are fully cached per source (coverage table) | coverage table with (source_id, range_start_major, range_start_minor, range_end_major, range_end_minor); interval merge on insert |
| PERSIST-03 | Library recovers from crash: load last snapshot, replay cached events, catch up via FetchEvents, switch to live | RecoveryPipeline: load snapshot → replay events from SQLite → call fetch_events for gap → hand off to coordinator |
| PERSIST-04 | Library creates periodic state snapshots (client-owned serialization) to bound replay cost | Coordinator counts events since last snapshot; triggers snapshot write every N events; stores opaque Vec<u8> |
| PERSIST-05 | Library maintains named anchor positions (persistent cursors) updated atomically with event processing | anchors table; anchor write in same transaction as event batch flush |
</phase_requirements>

---

## Summary

Phase 2 introduces SQLite-backed persistence via **rusqlite 0.38.0** with WAL mode. The standard approach in the Rust ecosystem for async use of rusqlite is **tokio-rusqlite 0.7.0**, which spawns a dedicated background thread per connection and exposes an async `call()` API that sends closures over a channel. This directly satisfies the locked decision that storage trait methods are sync (rusqlite's native API) while the coordinator calls them via `spawn_blocking`-equivalent.

The four-table SQLite schema (events, snapshots, anchors, coverage) is conventional for event-sourced systems. The critical subtleties are: (1) the coverage interval merge algorithm, which must handle adjacent and overlapping intervals atomically within a single transaction, and (2) the recovery pipeline sequencing, which must load the latest deserializable snapshot and replay only the events whose cursors are strictly after the snapshot's cursor and within cached coverage. Schema migrations are handled by **rusqlite_migration 2.4.0**, which uses SQLite's built-in `user_version` field rather than a separate migrations table.

Since snapshot and event payload serialization is client-owned opaque bytes, the library has no serialization dependency for data content. For any internal library serialization (if needed for coverage ranges or other metadata stored as BLOB), **postcard 1.1.3** is the correct choice per the STATE.md tombstone decision (bincode is dead; postcard is the replacement). In practice, the schema as designed stores coverage ranges as INTEGER columns, so postcard is likely not needed for Phase 2 — this is called out explicitly below.

**Primary recommendation:** Use rusqlite 0.38.0 + tokio-rusqlite 0.7.0 + rusqlite_migration 2.4.0. Keep the Storage trait sync to match rusqlite's API; bridge to async in the coordinator via tokio-rusqlite's `call()`.

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| rusqlite | 0.38.0 | SQLite bindings | The standard Rust SQLite wrapper; ergonomic, well-maintained, native rusqlite API is sync which matches the locked decision |
| tokio-rusqlite | 0.7.0 | Async bridge for rusqlite | Background thread per connection, `call()` method sends closures via mpsc; the accepted community pattern for using rusqlite from async Rust |
| rusqlite_migration | 2.4.0 | Schema migrations | Uses `user_version` pragma (not a table); const-constructable `Migrations::from_slice`; actively maintained (2.4.0 released 2026-01-19) |
| postcard | 1.1.3 | Internal serialization | Selected in STATE.md after bincode tombstone (Dec 2025); stable wire format since v1.0.0; needed only IF any internal library data is stored as BLOB |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| thiserror | 2 (already in Cargo.toml) | Error types for StorageError | Already present; add StorageError variant via thiserror derive |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| tokio-rusqlite | raw `tokio::task::spawn_blocking` | spawn_blocking works but loses the dedicated-thread model; tokio-rusqlite gives a stable `Connection: Clone + Send + Sync` handle that can be shared cheaply. Either works, but tokio-rusqlite is cleaner for the coordinator |
| tokio-rusqlite | deadpool-sqlite or async-sqlite | Both exist (also async wrappers); tokio-rusqlite is the most widely used; at 0.7.0 it pins to rusqlite ^0.37.0 — check compatibility with 0.38.0 before adopting (see Open Questions) |
| rusqlite_migration | manual user_version tracking | rusqlite_migration is 50 lines of user code saved; const migrations are zero-cost abstraction; use it |
| postcard | serde_json for BLOBs | serde_json produces larger output and is slower; only advantage is human-readability; not relevant for library-internal BLOBs |

**Installation:**
```bash
# Add to Cargo.toml [dependencies]:
rusqlite = { version = "0.38", features = ["bundled"] }
tokio-rusqlite = "0.7"
rusqlite_migration = { version = "2.4", features = [] }
postcard = { version = "1.1", features = ["alloc"] }
```

Note on `bundled` feature: the `bundled` feature compiles SQLite from source and statically links it, eliminating the system SQLite dependency. This is standard for library crates that want deterministic behavior and avoids version mismatches. The tradeoff is compile-time cost (~15 seconds). Appropriate here since chainfold targets operational services where build reproducibility matters.

---

## Architecture Patterns

### Recommended Project Structure

```
src/
├── storage/
│   ├── mod.rs          # Storage trait definition + re-exports
│   ├── sqlite.rs       # SqliteStorage: rusqlite-backed implementation
│   ├── memory.rs       # InMemoryStorage: test double (no filesystem)
│   └── schema.rs       # SQL DDL, MIGRATIONS constant, table names
├── coordinator/
│   ├── mod.rs          # Engine struct: owns ReducerCore + Storage + tokio-rusqlite Connection
│   └── recovery.rs     # Recovery pipeline: snapshot load → replay → gap fetch
├── cursor/             # (Phase 1 — unchanged)
├── machine.rs          # (Phase 1 — ReducerCore unchanged)
├── source.rs           # (Phase 1 — EventSource unchanged)
├── event.rs            # (Phase 1 — Event<C,E> — may add Serialize bound on E for storage)
└── lib.rs
```

### Pattern 1: Storage Trait (Sync API matching rusqlite)

**What:** The Storage trait methods are synchronous. The coordinator bridges to async via tokio-rusqlite's `call()`. This is the locked decision.

**When to use:** Always — it's the locked architecture.

```rust
// Source: CONTEXT.md locked decision + rusqlite native sync API
pub trait Storage: Send + Sync + 'static {
    type Cursor: ChainCursor;
    type Error: std::error::Error + Send + Sync + 'static;

    // Events
    fn write_events_batch(&self, events: &[StoredEvent<Self::Cursor>]) -> Result<(), Self::Error>;
    fn events_in_range(
        &self,
        source_id: &str,
        from: &Self::Cursor,
        to: &Self::Cursor,
    ) -> Result<Vec<StoredEvent<Self::Cursor>>, Self::Error>;

    // Snapshots
    fn write_snapshot(&self, snap: &Snapshot<Self::Cursor>) -> Result<(), Self::Error>;
    fn latest_snapshot(&self, source_id: &str) -> Result<Option<Snapshot<Self::Cursor>>, Self::Error>;
    fn snapshots_before(
        &self,
        source_id: &str,
        at: &Self::Cursor,
    ) -> Result<Vec<Snapshot<Self::Cursor>>, Self::Error>;

    // Anchors
    fn write_anchor(&self, anchor: &Anchor<Self::Cursor>) -> Result<(), Self::Error>;
    fn read_anchor(&self, source_id: &str, name: &str) -> Result<Option<Anchor<Self::Cursor>>, Self::Error>;

    // Coverage
    fn add_coverage(&self, source_id: &str, from: &Self::Cursor, to: &Self::Cursor) -> Result<(), Self::Error>;
    fn is_fully_cached(&self, source_id: &str, from: &Self::Cursor, to: &Self::Cursor) -> Result<bool, Self::Error>;
    fn cached_tip(&self, source_id: &str) -> Result<Option<Self::Cursor>, Self::Error>;
}
```

### Pattern 2: SQLite Connection Setup (WAL Mode)

**What:** Connection initialization with WAL and recommended pragmas. Must be called once after opening.

**When to use:** Every connection open.

```rust
// Source: https://cj.rs/blog/sqlite-pragma-cheatsheet-for-performance-and-consistency/
// Source: rusqlite 0.38.0 docs (pragma_update)
fn configure_connection(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;"
    )
}
```

WAL is persistent — once set on a database file, it survives connection close/reopen. `synchronous = NORMAL` is the right balance for WAL: safe from OS crashes, lower fsync overhead than FULL.

### Pattern 3: tokio-rusqlite Async Bridge

**What:** Using tokio-rusqlite's `call()` to run sync rusqlite closures from async coordinator code.

**When to use:** Every time the coordinator needs to call Storage from an async context.

```rust
// Source: https://docs.rs/tokio-rusqlite/latest/tokio_rusqlite/struct.Connection.html
use tokio_rusqlite::Connection;

// Connection is Clone + Send + Sync — share cheaply via Arc or clone
let conn: Connection = Connection::open("chainfold.db").await?;

// From async coordinator — call() runs the closure on the dedicated SQLite thread
let events = conn.call(|rusqlite_conn| {
    // Everything in here is sync rusqlite
    let mut stmt = rusqlite_conn.prepare(
        "SELECT cursor_major, cursor_minor, payload FROM events
         WHERE source_id = ?1
           AND cursor_major >= ?2 AND cursor_minor >= ?3
           AND cursor_major <= ?4"
    )?;
    // ... collect results
    Ok(events)
}).await?;
```

Key: `call()` accepts `FnOnce(&rusqlite::Connection) -> Result<T, E>` where E: Into<Box<dyn Error + Send + Sync>>. Returns `impl Future<Output = Result<T, tokio_rusqlite::Error>>`.

### Pattern 4: Schema Migrations with rusqlite_migration

**What:** Declare all DDL as a const slice of migrations; apply on connection open. `user_version` tracks current schema version.

**When to use:** Phase 2 introduces the initial schema — this is migration v1.

```rust
// Source: https://docs.rs/rusqlite_migration/latest/rusqlite_migration/
use rusqlite_migration::{Migrations, M};

const MIGRATIONS_SLICE: &[M<'_>] = &[
    M::up("
        CREATE TABLE events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id      TEXT    NOT NULL,
            cursor_major   INTEGER NOT NULL,
            cursor_minor   INTEGER NOT NULL,
            payload        BLOB    NOT NULL,
            UNIQUE(source_id, cursor_major, cursor_minor)
        );
        CREATE INDEX idx_events_source_cursor
            ON events(source_id, cursor_major, cursor_minor);

        CREATE TABLE snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id      TEXT    NOT NULL,
            cursor_major   INTEGER NOT NULL,
            cursor_minor   INTEGER NOT NULL,
            payload        BLOB    NOT NULL,
            created_at     INTEGER NOT NULL  -- unix epoch seconds
        );
        CREATE INDEX idx_snapshots_source_cursor
            ON snapshots(source_id, cursor_major DESC, cursor_minor DESC);

        CREATE TABLE anchors (
            source_id      TEXT    NOT NULL,
            name           TEXT    NOT NULL,
            cursor_major   INTEGER NOT NULL,
            cursor_minor   INTEGER NOT NULL,
            PRIMARY KEY(source_id, name)
        );

        CREATE TABLE coverage (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id      TEXT    NOT NULL,
            range_start_major  INTEGER NOT NULL,
            range_start_minor  INTEGER NOT NULL,
            range_end_major    INTEGER NOT NULL,
            range_end_minor    INTEGER NOT NULL
        );
        CREATE INDEX idx_coverage_source
            ON coverage(source_id, range_start_major, range_start_minor);
    "),
];
const MIGRATIONS: Migrations<'_> = Migrations::from_slice(MIGRATIONS_SLICE);

// On connection open:
MIGRATIONS.to_latest(&mut conn)?;
```

### Pattern 5: Deduplication via INSERT OR IGNORE

**What:** UNIQUE constraint on (source_id, cursor_major, cursor_minor) + INSERT OR IGNORE. Duplicate events from overlapping backfill + live windows silently dropped.

**When to use:** All event batch writes.

```rust
// Source: https://sqlite.org/lang_conflict.html
// Source: CONTEXT.md locked decision
tx.execute(
    "INSERT OR IGNORE INTO events
        (source_id, cursor_major, cursor_minor, payload)
     VALUES (?1, ?2, ?3, ?4)",
    rusqlite::params![source_id, major, minor, payload_bytes],
)?;
```

### Pattern 6: Batch Writes in Single Transaction

**What:** Accumulate events in memory, flush as a single SQLite transaction. Anchor update in the same transaction.

**When to use:** The coordinator's flush path.

```rust
// Source: rusqlite Transaction docs + CONTEXT.md locked decision
fn write_batch_with_anchor(
    conn: &rusqlite::Connection,
    source_id: &str,
    events: &[StoredEvent],
    anchor_name: &str,
    anchor_cursor_major: i64,
    anchor_cursor_minor: i64,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO events
                (source_id, cursor_major, cursor_minor, payload)
             VALUES (?1, ?2, ?3, ?4)"
        )?;
        for event in events {
            stmt.execute(rusqlite::params![
                source_id, event.major, event.minor, &event.payload
            ])?;
        }
    }

    tx.execute(
        "INSERT INTO anchors (source_id, name, cursor_major, cursor_minor)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(source_id, name) DO UPDATE SET
             cursor_major = excluded.cursor_major,
             cursor_minor = excluded.cursor_minor",
        rusqlite::params![source_id, anchor_name, anchor_cursor_major, anchor_cursor_minor],
    )?;

    tx.commit()
}
```

`prepare_cached()` reuses compiled statement objects across calls — important for batch inserts.

### Pattern 7: Coverage Interval Merge

**What:** When adding a new [from, to) range to coverage, merge with adjacent or overlapping existing intervals for the same source_id. All within a single transaction.

**When to use:** After every successful event batch write or gap fetch.

The algorithm (pure Rust, executed inside the `call()` closure):

```rust
// Source: interval merge algorithm (standard CS, verified against CONTEXT.md requirements)
fn add_coverage_and_merge(
    conn: &rusqlite::Connection,
    source_id: &str,
    new_start: (i64, i64),  // (major, minor)
    new_end: (i64, i64),
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;

    // 1. Find all existing intervals that overlap or are adjacent to the new range.
    //    An interval [a,b) is adjacent-or-overlapping with [c,d) if a <= d AND b >= c.
    let mut stmt = tx.prepare(
        "SELECT id, range_start_major, range_start_minor, range_end_major, range_end_minor
         FROM coverage
         WHERE source_id = ?1
           AND NOT (range_end_major < ?2 OR (range_end_major = ?2 AND range_end_minor < ?3))
           AND NOT (range_start_major > ?4 OR (range_start_major = ?4 AND range_start_minor > ?5))"
    )?;

    let overlapping: Vec<(i64, (i64,i64), (i64,i64))> = stmt.query_map(
        rusqlite::params![
            source_id,
            new_start.0, new_start.1,
            new_end.0, new_end.1
        ],
        |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?), (row.get(3)?, row.get(4)?))),
    )?.filter_map(|r| r.ok()).collect();

    // 2. Delete all overlapping intervals.
    for (id, _, _) in &overlapping {
        tx.execute("DELETE FROM coverage WHERE id = ?1", rusqlite::params![id])?;
    }

    // 3. Compute merged interval: min of starts, max of ends.
    let merged_start = overlapping.iter()
        .map(|(_, s, _)| *s)
        .chain(std::iter::once(new_start))
        .min()
        .unwrap_or(new_start);
    let merged_end = overlapping.iter()
        .map(|(_, _, e)| *e)
        .chain(std::iter::once(new_end))
        .max()
        .unwrap_or(new_end);

    // 4. Insert the merged interval.
    tx.execute(
        "INSERT INTO coverage
            (source_id, range_start_major, range_start_minor, range_end_major, range_end_minor)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![source_id, merged_start.0, merged_start.1, merged_end.0, merged_end.1],
    )?;

    tx.commit()
}
```

The (major, minor) comparison ordering in SQL must match the Rust Ord semantics: major first, then minor. The WHERE clause handles both components.

### Pattern 8: Recovery Pipeline Sequencing

**What:** On cold start, execute the deterministic recovery sequence.

```
1. Query: latest snapshot for source_id (ordered by cursor DESC)
2. Loop: try to deserialize snapshot.payload via client-provided fn
   - If deserialize fails: query next-oldest snapshot, retry
   - If no snapshots or all fail: call EventSource::fetch_state(at_genesis)
3. snapshot_cursor = cursor of the deserialized snapshot (or genesis)
4. replay_events = storage.events_in_range(source_id, snapshot_cursor, cached_tip)
5. For each replay_event: coordinator.apply(event) [NO storage write — already stored]
6. gap_start = cached_tip (or snapshot_cursor if no cached events)
7. loop: fetch_events(gap_start, live_tip) in chunks
   - write each chunk to storage (INSERT OR IGNORE)
   - update coverage
   - apply events
   - advance gap_start to last received cursor
   - stop when gap_start >= live_tip
8. Hand off to Phase 3 live subscription
```

The critical invariant: replaying already-stored events does NOT re-write them to storage. The coordinator calls `ReducerCore::apply()` (not `apply_and_accumulate()`-style) for the replay phase.

### Anti-Patterns to Avoid

- **Writing events to storage and in-memory in the same atomic step in ReducerCore:** ReducerCore stays pure — it has no storage awareness. The coordinator sequences: write to storage first, then apply to ReducerCore. If apply panics (it's infallible, so this shouldn't happen), the event is in storage but not in memory — safe, recovery replays it.

- **Using `tokio::sync::Mutex<rusqlite::Connection>` directly:** rusqlite Connection is `!Sync`. Even wrapping in `tokio::sync::Mutex` is problematic because async mutexes hold the guard across await points. Use tokio-rusqlite's dedicated-thread model instead.

- **Opening a new rusqlite connection per operation:** Expensive. Open once; pass the tokio-rusqlite `Connection` handle (which is `Clone`) to wherever it's needed.

- **Storing cursor data as a serialized BLOB instead of INTEGER pair columns:** The locked decision explicitly uses (major, minor) INTEGER columns for range queries and B-tree indexing. Do not serialize cursors to BLOB — it prevents efficient `WHERE cursor_major >= X` queries.

- **Applying events before writing them to storage:** Invert this. Write first, apply second. On crash after write but before apply: recovery will replay the event. On crash after apply but before write: event is lost — catastrophic for correctness.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Schema versioning | Manual `CREATE TABLE IF NOT EXISTS` + version table | rusqlite_migration | `user_version` pragma is atomic; handles upgrade paths correctly; const-constructable |
| Async SQLite bridge | Raw `spawn_blocking` + `Arc<Mutex<Connection>>` | tokio-rusqlite | Handles the `!Send` `!Sync` issue correctly via dedicated thread; `Connection` is `Clone + Send + Sync` |
| Coverage interval overlap query | Custom Rust data structure | Pure SQL within rusqlite transaction | Interval overlap in SQL is 2 conditions; transactional merge is simpler than a concurrent in-memory structure |

**Key insight:** The async-SQLite impedance mismatch (rusqlite is sync, tokio is async) has a well-established solution in tokio-rusqlite. Don't fight it with mutex gymnastics.

---

## Common Pitfalls

### Pitfall 1: tokio-rusqlite version pinning rusqlite

**What goes wrong:** tokio-rusqlite 0.7.0 pins `rusqlite ^0.37.0`. If chainfold takes rusqlite 0.38.0 directly, Cargo may resolve two different rusqlite versions, causing type incompatibility at the `call()` boundary.

**Why it happens:** tokio-rusqlite uses `rusqlite::Connection` directly in its closure signature. If Cargo selects different patch versions, the types don't unify.

**How to avoid:** Check that tokio-rusqlite 0.7.0 is compatible with rusqlite 0.38.0 before committing to both pinned versions. If incompatible, either use rusqlite 0.37.x or wait for a tokio-rusqlite release that targets 0.38. The `^0.37.0` semver range should accept 0.38 (minor version bump) — verify this during implementation.

**Warning signs:** `mismatched types` compiler errors at the `call()` closure boundary mentioning rusqlite types.

### Pitfall 2: WAL mode on network filesystems

**What goes wrong:** WAL mode is incompatible with NFS and some other network filesystems. The WAL file uses shared-memory mapping which doesn't work over the network.

**Why it happens:** WAL uses `-wal` and `-shm` sidecar files that require POSIX shared memory semantics.

**How to avoid:** This is a local-file cache (PROJECT.md: "cache, not transport"). Document that the SQLite file must be on local disk. Not a practical concern for the project's use case, but worth noting in docs.

**Warning signs:** SQLite errors on open if someone tries to put the DB on NFS.

### Pitfall 3: Coverage query with (major, minor) composite ordering

**What goes wrong:** Writing the interval overlap query with only `major` comparison gives wrong results when ranges span the same `major` with different `minor` values.

**Why it happens:** Cursors are (major, minor) pairs with lexicographic order. A range from (100, 5) to (100, 20) must be handled correctly — it's within a single block.

**How to avoid:** The overlap condition for [(s_major, s_minor), (e_major, e_minor)] vs [(a_major, a_minor), (b_major, b_minor)] requires full tuple comparison in SQL. Use the pattern shown in Pattern 7: four separate column comparisons, not just `range_end_major >= new_start_major`.

**Warning signs:** Coverage queries returning `false` for ranges correctly within cached coverage due to same-block cursor ranges.

### Pitfall 4: Snapshot write timing and event count

**What goes wrong:** Snapshot count is incremented even when INSERT OR IGNORE silently drops a duplicate event (which doesn't advance state). Snapshot triggers sooner than expected; worst-case replay cost is artificially bounded lower than N distinct events.

**Why it happens:** The coordinator increments its event counter on every `write_events_batch` call, not on successful distinct inserts.

**How to avoid:** Use `conn.changes()` after batch insert to get the count of rows actually inserted (not ignored). Increment the snapshot counter by `changes()`, not by batch size.

**Warning signs:** Snapshots created more frequently than expected; deduplication tests showing snapshot files created on duplicate-only batches.

### Pitfall 5: Recovery replay re-writing stored events

**What goes wrong:** During recovery replay (step 4 in the pipeline), the coordinator calls a write-then-apply path instead of a replay-only path. Events get re-inserted (INSERT OR IGNORE handles this but wastes work), more importantly the coverage table may get spurious entries, and the event counter resets the snapshot trigger.

**Why it happens:** The coordinator has a single "process event" method that always writes + applies.

**How to avoid:** Separate `apply_stored_event(event)` (apply only, no storage write) from `process_new_event(event)` (write then apply). The recovery pipeline calls the former for cached replay, the latter for gap fetch.

**Warning signs:** Events in the events table have duplicate-looking timestamps in created_at after a crash/restart cycle.

### Pitfall 6: Atomic anchor update

**What goes wrong:** Anchor and event batch are written in separate transactions. Process crashes between the two writes. On recovery, anchor is ahead of stored events — gap calculation is wrong (anchor says "I processed up to X" but events only go to X-1).

**Why it happens:** Two separate `commit()` calls instead of one.

**How to avoid:** Anchor write MUST be in the same transaction as the event batch. This is a locked decision and must not be violated. Verify this in tests by simulating a crash (drop the connection mid-sequence) and confirming anchor and events are always consistent.

**Warning signs:** After crash-restart tests, state reconstruction produces different results than expected, with anchors appearing to be ahead of actual stored events.

---

## Code Examples

Verified patterns from official sources:

### SQLite Connection Open + Configure + Migrate (rusqlite)

```rust
// Source: rusqlite 0.38.0 docs + https://cj.rs/blog/sqlite-pragma-cheatsheet-for-performance-and-consistency/
// Source: rusqlite_migration 2.4.0 docs
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};

const MIGRATIONS: Migrations<'_> = Migrations::from_slice(&[
    M::up(/* see Pattern 4 DDL */),
]);

fn open_storage(path: &str) -> rusqlite::Result<Connection> {
    let mut conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;"
    )?;
    MIGRATIONS.to_latest(&mut conn).expect("migrations failed");
    Ok(conn)
}
```

### Async Call via tokio-rusqlite

```rust
// Source: https://docs.rs/tokio-rusqlite/latest/tokio_rusqlite/struct.Connection.html
use tokio_rusqlite::Connection;

let conn = Connection::open("chainfold.db").await?;

// From async context — coordinator calls this
let result = conn.call(|db| {
    // db is &rusqlite::Connection — fully sync here
    let count: i64 = db.query_row(
        "SELECT COUNT(*) FROM events WHERE source_id = ?1",
        [source_id],
        |row| row.get(0),
    )?;
    Ok(count)
}).await?;
```

### ToSql for Vec<u8> (BLOB payload)

```rust
// Source: rusqlite 0.38.0 ToSql impls — Vec<u8> implements ToSql natively
// No wrapping needed. Pass directly as parameter.
stmt.execute(rusqlite::params![
    source_id,          // TEXT — &str implements ToSql
    cursor_major,       // INTEGER — i64 implements ToSql
    cursor_minor,       // INTEGER — i64 implements ToSql
    payload_bytes,      // BLOB — Vec<u8> implements ToSql directly
])?;
```

### Batch Write with Transaction

```rust
// Source: rusqlite Transaction docs — Transaction implements Deref<Target=Connection>
let tx = conn.transaction()?;
let mut stmt = tx.prepare_cached(
    "INSERT OR IGNORE INTO events (source_id, cursor_major, cursor_minor, payload)
     VALUES (?1, ?2, ?3, ?4)"
)?;
for event in &batch {
    stmt.execute(rusqlite::params![
        &event.source_id, event.major, event.minor, &event.payload
    ])?;
}
drop(stmt); // release borrow before commit
tx.commit()?;
let rows_inserted = conn.changes(); // how many were NOT ignored
```

### Postcard Serialization (only if internal BLOBs needed)

```rust
// Source: https://docs.rs/postcard/1.1.3/postcard/
// Stable wire format since postcard v1.0.0
use postcard::{to_allocvec, from_bytes};

// Serialize
let bytes: Vec<u8> = postcard::to_allocvec(&value)?;

// Deserialize
let value: MyType = postcard::from_bytes(&bytes)?;
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| bincode v2 for serialization | postcard 1.1.3 | Dec 2025 (bincode tombstone) | STATE.md tombstone; postcard is the replacement for no_std-friendly compact binary serde |
| Manual `user_version` tracking | rusqlite_migration with `from_slice` | 2025 (added const constructor in 2.x) | Can now declare migrations as a `const` — zero runtime allocation |
| `tokio::task::spawn_blocking` directly | tokio-rusqlite 0.7.0 | Ecosystem convergence ~2023-2024 | Dedicated thread model; `Connection` is `Clone + Send + Sync` vs spawn_blocking requiring `move` closures with Arc |

**Deprecated/outdated:**
- bincode: tombstone release December 2025. Do not use. Replacement is postcard (for compact binary) or serde_json (for human-readable).
- `pragma_update_and_check` for WAL: works, but `execute_batch` is simpler when setting multiple pragmas at once.

---

## Open Questions

1. **tokio-rusqlite 0.7.0 + rusqlite 0.38.0 compatibility**
   - What we know: tokio-rusqlite 0.7.0 pins `rusqlite ^0.37.0`; semver `^0.37.0` allows 0.37.x and 0.38.x (semver minor is not breaking in Rust crates)
   - What's unclear: Whether rusqlite 0.37→0.38 had any breaking changes that affect tokio-rusqlite's usage of internal Connection types
   - Recommendation: During Wave 0, run `cargo add rusqlite@0.38 tokio-rusqlite@0.7` and verify it compiles. If tokio-rusqlite forces a downgrade to 0.37, use rusqlite 0.37 and note that 0.38 features (if any are needed) are not available.

2. **Coverage query: tuple comparison correctness at block boundaries**
   - What we know: The (major, minor) overlap query requires four independent column comparisons to correctly handle same-major ranges
   - What's unclear: Whether the SQL WHERE clause in Pattern 7 correctly handles the edge case where new_start.major == existing_end.major with different minors
   - Recommendation: Write a unit test specifically for same-major coverage merging before considering Pattern 7 verified.

3. **postcard need in Phase 2**
   - What we know: Event and snapshot payloads are client-owned bytes — no internal serialization. Coverage stored as INTEGER columns. Anchors stored as INTEGER columns.
   - What's unclear: Whether any Phase 2 internal data structures end up needing BLOB storage
   - Recommendation: Do NOT add postcard as a dependency for Phase 2 unless an actual need arises. The schema as designed stores everything either as TEXT or INTEGER pairs. Mark postcard as a Phase 3+ candidate.

4. **`conn.changes()` after batch INSERT OR IGNORE**
   - What we know: rusqlite exposes `Connection::changes()` returning the count of rows affected by the last executed DML statement
   - What's unclear: Whether `changes()` returns correctly after a batch of INSERT OR IGNORE statements (does it return total changes across all statements, or only the last?)
   - Recommendation: Test this explicitly. If `changes()` only reflects the last statement, count ignores manually or use a counter variable during the batch loop.

---

## Sources

### Primary (HIGH confidence)

- rusqlite 0.38.0 — https://docs.rs/rusqlite/0.38.0/rusqlite/ — Connection API, Transaction, ToSql impls, pragma_update, WAL setup
- tokio-rusqlite 0.7.0 — https://docs.rs/tokio-rusqlite/latest/tokio_rusqlite/ — Connection::call() pattern, async bridge architecture
- postcard 1.1.3 — https://docs.rs/postcard/1.1.3/postcard/ — to_allocvec, from_bytes, stable wire format
- rusqlite_migration 2.4.0 — https://docs.rs/rusqlite_migration/latest/rusqlite_migration/ — Migrations::from_slice, M::up, to_latest
- SQLite official docs — https://sqlite.org/lang_conflict.html — INSERT OR IGNORE / ON CONFLICT behavior
- SQLite WAL docs — https://sqlite.org/wal.html — WAL persistence, synchronous=NORMAL behavior

### Secondary (MEDIUM confidence)

- SQLite pragma cheatsheet — https://cj.rs/blog/sqlite-pragma-cheatsheet-for-performance-and-consistency/ — recommended pragma set for WAL mode with rusqlite
- crates.io API — rusqlite 0.38.0 features verified (bundled, cache, etc.)
- STATE.md — bincode tombstone decision (Dec 2025), postcard 1.1.3 as replacement

### Tertiary (LOW confidence)

- WebSearch: tokio spawn_blocking pattern — confirmed by multiple sources that tokio-rusqlite is the accepted community solution; underlying mechanism verified against tokio-rusqlite docs

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — versions verified directly from crates.io API; APIs verified from official docs.rs
- Architecture: HIGH — patterns derived from locked decisions in CONTEXT.md plus official library docs; no novel inventions
- Pitfalls: MEDIUM-HIGH — most derived from understanding the libraries deeply; pitfall 1 (version pinning) is a real operational concern flagged as open question; pitfall 2 (WAL+NFS) is documented SQLite behavior; others are design-level reasoning from the architecture

**Research date:** 2026-03-03
**Valid until:** 2026-06-03 (rusqlite and tokio-rusqlite are stable; postcard wire format stable since v1.0; rusqlite_migration actively maintained but API is stable)
