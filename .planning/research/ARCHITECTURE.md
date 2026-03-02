# Architecture Patterns

**Domain:** Blockchain event sourcing / state reconstruction library (Rust)
**Researched:** 2026-03-02
**Confidence:** HIGH (core patterns well-established; Rust specifics from alloy/ethers ecosystem familiarity; no web search available, relying on training knowledge)

---

## Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Code                               │
│  impl FetchState, impl FetchEvents, impl Apply (pure reducer)    │
└──────────────┬──────────────────────────────┬───────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────┐         ┌────────────────────────┐
│   Chainfold Engine   │         │   Public API Surface   │
│   (orchestrates all  │◄───────►│   Current()            │
│    subsystems)       │         │   At(cursor)           │
└──────┬───────────────┘         │   OnChange(callback)   │
       │                         └────────────────────────┘
       │
       ├──► EventSource Layer (trait abstraction)
       │         EVM adapter / Bitcoin adapter / CEX adapter
       │
       ├──► Cursor / Ordering Layer
       │         BlockCursor, LogCursor, Comparator
       │
       ├──► Subscription Manager
       │         WebSocket mux, gap detection, reorg detection
       │
       ├──► Cache Layer (SQLite, WAL mode)
       │         Events table, Snapshots table, Coverage ranges
       │
       ├──► Snapshot Manager
       │         Snapshot policy, serialization (serde/bincode)
       │
       ├──► Recovery Pipeline
       │         Cold-start sequencer, catch-up flow
       │
       └──► State Machine / Reducer Core
                 Generic S: State, applies events, holds current
```

---

## Component Boundaries

| Component | Responsibility | Accepts | Produces | Communicates With |
|-----------|---------------|---------|----------|-------------------|
| **EventSource trait** | Abstract over EVM/Bitcoin/CEX; vend ordered events | Source-specific config (from client) | `Event<C>` stream tagged with `Cursor<C>` | Subscription Manager, Recovery Pipeline |
| **Cursor / Ordering** | Total ordering of events across heterogeneous chains | Raw chain positions | Comparable `Cursor` values | All components (universal event ID) |
| **State Machine / Reducer** | Apply events to state; hold current in-memory state | `Event<C>` + current `State<S>` | New `State<S>`, `ChangeNotification` | Engine, Cache Layer, Subscription Manager |
| **Cache Layer (SQLite)** | Persist events, snapshots, anchors, coverage ranges | Events, snapshots | Events (for replay), Coverage metadata | Recovery Pipeline, Snapshot Manager, Engine |
| **Subscription Manager** | Live event routing; detect gaps and reorgs | WebSocket/polling feed from EventSource | Ordered events to Engine; gap signals | EventSource, Engine, Recovery Pipeline |
| **Snapshot Manager** | Decide when to snapshot; serialize/deserialize state | State, cursor | Serialized snapshot to Cache | Cache Layer, Recovery Pipeline |
| **Recovery Pipeline** | Cold-start and catch-up sequencing | Snapshot + cached events + live subscription | In-order events to State Machine | Cache Layer, EventSource, Subscription Manager, State Machine |
| **Engine** | Orchestrates all subsystems; owns the lifecycle | Client-provided FetchState/FetchEvents/Apply | Public API (Current, At, OnChange) | All subsystems |

---

## Component Design Details

### 1. EventSource Trait

**Design goal:** Single trait that EVM (block+logIndex), Bitcoin (height+txIndex), and CEX (timestamp+seqId) all implement. The client is not aware of which source is beneath.

```rust
/// C = Cursor type specific to this source
pub trait EventSource {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch historical events in [from, to) cursor range.
    /// Called during catch-up and historical queries.
    async fn fetch_range(
        &self,
        from: &Self::Cursor,
        to: &Self::Cursor,
    ) -> Result<Vec<(Self::Cursor, Self::Event)>, Self::Error>;

    /// Subscribe to live events from `from` cursor onward.
    /// Returns a stream; implementor owns the WebSocket/poll loop.
    async fn subscribe(
        &self,
        from: &Self::Cursor,
    ) -> Result<impl Stream<Item = Result<(Self::Cursor, Self::Event), Self::Error>>, Self::Error>;

    /// Fetch the canonical "current tip" cursor.
    async fn tip(&self) -> Result<Self::Cursor, Self::Error>;
}
```

**Key decisions:**
- App owns connections (PROJECT.md constraint) — `EventSource` is given to the library fully constructed, not a URL/config
- The `fetch_range` and `subscribe` split maps cleanly to the two phases: historical catch-up and live tracking
- No `Unapply` (PROJECT.md: forward-only)
- `ChainCursor` (see below) provides the ordering guarantee

**Confidence:** HIGH — this is the standard approach in Rust async event streaming (alloy providers follow this pattern)

---

### 2. Cursor / Ordering Abstraction

**Problem:** EVM uses (block_number, log_index), Bitcoin uses (height, tx_index, output_index), CEX uses (timestamp_ms, sequence_id). These need a unified ordering guarantee without losing source-specific precision.

```rust
/// All cursors must support total ordering and persistence.
pub trait ChainCursor:
    Ord + Clone + Send + Sync + 'static
    + serde::Serialize + serde::de::DeserializeOwned
{
    /// Is this cursor strictly after `other`?
    fn is_after(&self, other: &Self) -> bool;

    /// Is there a potential gap between `prev` and `self`?
    /// Used by SubscriptionManager for gap detection.
    fn has_gap_after(&self, prev: &Self) -> bool;

    /// Does this cursor represent the same "block" as `other`?
    /// Used for reorg detection (same height, different content).
    fn same_block(&self, other: &Self) -> bool;
}

/// Concrete EVM cursor
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvmCursor {
    pub block_number: u64,
    pub log_index: u32,
}

/// Concrete CEX cursor
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct CexCursor {
    pub timestamp_ms: i64,
    pub seq_id: u64,
}
```

**Key decision:** Cursors are source-specific types, not a unified enum. The generic `C: ChainCursor` propagates through the type system. A unified `AnyCursor` enum would be needed only if mixing sources in one Engine — out of scope for v1, but the door is left open.

**Confidence:** HIGH — this is standard Rust "new-type + trait" approach; well-precedented in alloy's block/filter types

---

### 3. State Machine / Reducer Core

**Design goal:** Client provides three pure functions. Library wraps them in a generic state machine that holds current state and emits change notifications.

```rust
/// Client implements this trio.
pub trait StateMachine: Send + Sync + 'static {
    type State: Clone + Send + 'static;
    type Event: Send + 'static;
    type Cursor: ChainCursor;

    /// Fetch the initial anchor state (called once on first run).
    async fn fetch_state(&self, at: &Self::Cursor) -> anyhow::Result<Self::State>;

    /// Pure reducer: given current state and an event, produce next state.
    /// Must be deterministic and free of side effects.
    fn apply(&self, state: Self::State, event: &Self::Event, cursor: &Self::Cursor) -> Self::State;
}

/// Internal engine holds this
struct ReducerCore<SM: StateMachine> {
    state: SM::State,
    cursor: SM::Cursor,
    sm: Arc<SM>,
    subscribers: Vec<Box<dyn Fn(&SM::State, &SM::Cursor) + Send + Sync>>,
}
```

**Pattern for two state shapes (from PROJECT.md):**

| Shape | How it fits | Example |
|-------|------------|---------|
| **Reduced** (compact fold) | `State = Balance(u128)` — tiny, heap-allocated, kept entirely in `ReducerCore.state` | ERC20 balance |
| **Accumulated** (events ARE state) | `State = ()` — side effect is writing to a separate Accumulator store; `apply` is a no-op on the in-memory value | Transfer history log |

The library does not need to know which shape the client uses — generics handle it.

**Confidence:** HIGH — standard Rust event sourcing pattern; matches what CQRS/ES frameworks like `esrs` and `eventually` use

---

### 4. Cache Layer (SQLite / WAL Mode)

**Schema:**

```sql
-- Ordered events for replay
CREATE TABLE events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    cursor_key BLOB    NOT NULL UNIQUE,  -- serialized ChainCursor (serde_json or bincode)
    payload    BLOB    NOT NULL,         -- serialized Event
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

-- Snapshots: state at a known cursor
CREATE TABLE snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    cursor_key BLOB    NOT NULL UNIQUE,
    state_blob BLOB    NOT NULL,         -- serialized State
    taken_at   INTEGER NOT NULL DEFAULT (unixepoch())
);

-- Coverage: which cursor ranges are fully cached
-- Avoids redundant fetches on partial restarts
CREATE TABLE coverage (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    from_key BLOB NOT NULL,
    to_key   BLOB NOT NULL,
    CHECK (from_key < to_key)
);

-- Anchors: externally meaningful checkpoints (bridge daemons)
CREATE TABLE anchors (
    name       TEXT    NOT NULL PRIMARY KEY,
    cursor_key BLOB    NOT NULL,
    meta       TEXT,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;  -- WAL mode makes NORMAL safe
PRAGMA cache_size = -65536;   -- 64MB page cache
```

**Coverage tracking** is the key design insight: on restart, the library checks coverage ranges before fetching. If `[A, B]` is already covered, it reads from cache; if not, it fetches from the source. This turns partial restarts into cheap local reads.

**WAL mode** is mandatory (not optional) because:
- Readers don't block writers — the live event writer and the "At(cursor)" reader coexist without contention
- `PRAGMA synchronous = NORMAL` is safe in WAL mode (crash recovery via WAL file)
- Write-ahead means the reader sees a consistent snapshot even during rapid event writes

**Confidence:** HIGH — WAL mode SQLite for event logs is well-validated (used by Litestream, Turso internals, many Rust indexers)

---

### 5. Subscription Manager

**Responsibility:** Owns the live event stream from `EventSource::subscribe`. Performs:
1. Gap detection between sequential cursors
2. Reorg detection (same block height, different block hash or content)
3. Fan-out to the State Machine

```rust
struct SubscriptionManager<ES: EventSource> {
    source: Arc<ES>,
    last_seen: Option<ES::Cursor>,
    gap_tx: mpsc::Sender<GapRange<ES::Cursor>>,
    event_tx: mpsc::Sender<LiveEvent<ES::Cursor, ES::Event>>,
}
```

**Gap detection logic:**

```
For each incoming event at cursor C:
  if last_seen is Some(prev):
    if C.has_gap_after(prev):
      emit GapDetected { from: prev, to: C }
      pause forwarding until gap is filled
    elif C.same_block(&prev) && content_differs:
      emit ReorgDetected { at: C }
      notify client (client decides handling per PROJECT.md)
  forward event to Engine
  last_seen = Some(C)
```

**Hot path requirement (PROJECT.md):** The WebSocket → Apply path must not allocate or serialize. The `LiveEvent` is passed by move through the `mpsc` channel directly into `ReducerCore::apply`. No JSON encoding, no IPC.

**Confidence:** HIGH — mpsc channel + gap detection is standard; the no-alloc constraint is achievable since alloy's log types are stack-allocatable for common cases

---

### 6. Snapshot Manager

**When to snapshot:**

| Policy | When to use | Trigger |
|--------|------------|---------|
| **Event-count** (default) | General reduced state | Every N events applied (e.g., N=1000) |
| **Time-based** | Slowly-changing accumulated state | Every T seconds if state changed |
| **Manual** | Client-controlled anchors | `engine.snapshot_now()` |
| **On-shutdown** | Always | `engine.close()` hook |

**Serialization strategy:**

- Primary: `bincode` — zero-copy, compact, fast. No schema evolution story.
- Fallback: `serde_json` — human-readable, schema-evolvable, 2-4x larger/slower.
- Recommendation: `bincode` for snapshots in v1 (schema evolution is a v2 concern; snapshot invalidation on format change is acceptable — the library can re-bootstrap from source).

```rust
pub trait Snapshottable: serde::Serialize + serde::de::DeserializeOwned {}
impl<T: serde::Serialize + serde::de::DeserializeOwned> Snapshottable for T {}
```

The `State` associated type must implement `Snapshottable`. This is enforced at compile time via trait bounds on the Engine generic parameters.

**Confidence:** HIGH — bincode vs serde_json tradeoff is well-documented; policy patterns from event sourcing literature

---

### 7. Recovery Pipeline

**Cold-start sequence (ordered steps):**

```
Step 1: LOAD — Check SQLite for latest snapshot
  If snapshot exists → load State, cursor = snapshot.cursor
  If no snapshot → call FetchState(tip_at_time_of_fetch) → initial_state, cursor = fetch_cursor

Step 2: REPLAY — Read cached events from cache in [cursor, ...]
  For each cached event in cursor order:
    apply(state, event, cursor) → state

Step 3: FETCH — Fetch uncached events from source in [last_cached, live_tip)
  For each fetched event:
    write to cache (with coverage update)
    apply(state, event, cursor) → state

Step 4: GAP CLOSE — The gap between FetchState anchor and subscription start
  This is the critical window: subscription starts at live_tip, but FetchState
  was called at some earlier cursor (due to RPC latency).
  Solution: overlap deliberately — subscribe from (fetch_cursor - N_blocks),
  deduplicate by cursor, apply only events > last_applied_cursor.

Step 5: SWITCH TO LIVE — Subscription manager hands events directly to reducer
  Coverage is now continuous; cache writes happen inline on the hot path.

Step 6: STEADY STATE — events flow WebSocket → SubscriptionManager → ReducerCore
  Snapshots taken periodically by SnapshotManager.
```

**Gap between FetchState and subscription (the tricky part):**

```
Timeline:
  t=0: subscribe(cursor=C_sub) called
  t=1: FetchState(at=C_fetch) called — C_fetch > C_sub due to time passing
  t=2: Historical events [C_sub, C_fetch) fetched via fetch_range
  t=3: Live events from C_sub already buffered in channel
  Merge: replay [C_sub, C_fetch) then drain channel, skipping duplicates
```

The SubscriptionManager buffers live events during catch-up (bounded channel, backpressure). The Engine merges the two streams by cursor comparison.

**Confidence:** HIGH — this gap-close pattern is the same used by alloy's `eth_subscribe` + historical fetch reconciliation

---

### 8. The Hot Path: WebSocket → Apply → Callback

**Constraint:** No IPC, no serialization, no allocation on the critical path (PROJECT.md).

**Data flow:**

```
Network (WebSocket frame)
  │  [alloy parses into Log/Block — unavoidable but zero-copy for bytes]
  ▼
EventSource impl (EVM adapter)
  │  decodes into ES::Event (client-defined type, stack or Arc)
  ▼
tokio::mpsc::channel (bounded, in-process)
  │  moves ownership — zero copy
  ▼
SubscriptionManager
  │  gap/reorg check (comparison only, no alloc)
  ▼
ReducerCore::apply(state, &event, &cursor)
  │  pure function call — no alloc if State fits in existing allocation
  ▼
OnChange callbacks (synchronous, in same task)
  │  passed &State, &Cursor — borrow, no clone unless callback demands it
  ▼
async fn write_event_to_cache(event, cursor)  [spawned task, off hot path]
```

**Key design choices for zero-alloc hot path:**
- `apply` takes `State` by value (move semantics); if `State` is a numeric value (e.g., `u128`), there is zero heap allocation
- Callbacks receive `&State` (shared reference) — client clones if needed
- Cache write is spawned as a separate tokio task, decoupled from the callback delivery
- Event type `ES::Event` is defined by the client — they control whether it's heap-heavy or not. Library documentation should guide them to keep events small.

**Confidence:** HIGH — this is the standard Tokio pattern for low-latency event pipelines; alloy's subscription types follow the same approach

---

## Data Flow

### Startup Flow (write path)

```
Engine::new(source, sm, db_path)
  → SnapshotManager::load_latest(db)
  → if snapshot: ReducerCore::restore(snapshot)
  → CacheLayer::replay_events(from_cursor) → ReducerCore::apply(*)
  → EventSource::subscribe(from_cursor)  [overlapping window]
  → EventSource::fetch_range(gap_range)  [fill gap]
  → ReducerCore::apply(*) for gap events
  → SubscriptionManager::start_forwarding()
  → STEADY STATE
```

### Steady-State Flow (hot path)

```
WebSocket frame
  → EventSource impl → ES::Event
  → mpsc::Sender<LiveEvent>
  → SubscriptionManager (gap check, reorg check)
  → ReducerCore::apply(state, event, cursor) → new state
  → OnChange(callbacks) notified with &new_state
  → [spawned] CacheLayer::insert_event(event, cursor)
  → [periodic] SnapshotManager::maybe_snapshot(state, cursor)
```

### Query Flow (At(cursor))

```
Engine::at(cursor)
  → CacheLayer::coverage_check(cursor)
  → if covered: CacheLayer::replay_from_snapshot_to(cursor)
  → if not covered: EventSource::fetch_range(...) → cache → replay
  → return reconstructed State
```

---

## Suggested Build Order

Dependencies drive the order. Components lower in the list depend on those above.

| Order | Component | Rationale |
|-------|-----------|-----------|
| 1 | **Cursor / ChainCursor trait + concrete types** | Everything else depends on ordering. Build first; defines the type-system foundation. |
| 2 | **StateMachine trait + ReducerCore (in-memory only)** | Can be tested with trivial sources (Vec of events). No I/O. |
| 3 | **EventSource trait + EVM adapter** | First concrete source. Drives trait design validation against a real chain. |
| 4 | **Cache Layer (SQLite schema + read/write ops)** | Can be unit-tested independently with synthetic events. |
| 5 | **Snapshot Manager** | Depends on Cache Layer (writes) and ReducerCore (state shape). |
| 6 | **Recovery Pipeline** | Integrates Cache + ReducerCore + EventSource. First full vertical slice. |
| 7 | **Subscription Manager** | Depends on EventSource (live stream) + Engine (event routing). Gap detection logic. |
| 8 | **Engine (orchestrator)** | Wires all subsystems. Public API surface (`Current`, `At`, `OnChange`). |
| 9 | **ERC20 PoC** | Validates full system end-to-end. Drives ergonomics fixes. |
| 10 | **Bitcoin / CEX adapters** | Once trait is proven correct by EVM. Adds cursor diversity. |

**Why this order:**
- Cursor first because every component that stores or routes events needs the ordering type.
- ReducerCore before Cache because the reducer is the correctness-critical piece — validate it with pure in-memory tests before adding I/O.
- Cache before Recovery because Recovery is the main consumer of Cache reads.
- Subscription Manager after Recovery because gap detection needs to understand the recovery protocol.
- Engine last because it's the integration layer — it shouldn't be built until the pieces it integrates are stable.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Unified `AnyCursor` Enum

**What:** `enum AnyCursor { Evm(EvmCursor), Bitcoin(BitcoinCursor), Cex(CexCursor) }`
**Why bad:** Forces every cursor comparison through a match arm. Breaks generic bounds (can't implement `Ord` cleanly across heterogeneous chains). Couples all source types into a single type.
**Instead:** Generic `C: ChainCursor` propagates through the type system. Each Engine instance is typed to one cursor family.

### Anti-Pattern 2: Serializing Events on the Hot Path

**What:** JSON-encoding every event before passing to the in-memory reducer.
**Why bad:** Adds 50-500μs per event on a busy stream. Defeats the hot-path latency goal.
**Instead:** Events flow as native Rust types through channels. Serialization happens only on the cache-write path, which is spawned off the hot path.

### Anti-Pattern 3: Blocking SQLite Writes on the Hot Path

**What:** `cache.insert_event(event).await` inline before calling callbacks.
**Why bad:** WAL mode SQLite writes are fast (< 1ms) but not zero-latency. On a busy chain, this creates measurable jitter.
**Instead:** Spawn a write task. Accept that the cache lags the in-memory state by < 1 event. (Recovery handles this: on restart, the last event may need re-fetching.)

### Anti-Pattern 4: Callbacks Taking `State` by Value

**What:** `OnChange(fn(state: S, cursor: C))` — clone every notification.
**Why bad:** If `State` is large (accumulated), every callback clones it.
**Instead:** `OnChange(fn(state: &S, cursor: &C))`. Client clones if they need ownership (most don't).

### Anti-Pattern 5: Snapshot Frequency Too High

**What:** Snapshotting every N=10 events.
**Why bad:** Serialization + SQLite write on every 10 events dominates the cache write budget.
**Instead:** N=1000 events as default; configurable. Most workloads need < 1 snapshot/minute.

### Anti-Pattern 6: Recovery Without Coverage Tracking

**What:** On restart, always re-fetch all events from source.
**Why bad:** Expensive (RPC cost, latency). Defeats the cache.
**Instead:** Coverage ranges in SQLite. Read from cache wherever covered; fetch only the gaps.

---

## Scalability Considerations

| Concern | At 1 contract | At 100 contracts | At 10K contracts |
|---------|--------------|-----------------|-----------------|
| Memory (reduced state) | < 1KB per Engine instance | < 100KB total | Consider separate processes or Arc sharing |
| SQLite writes | < 1 event/block | ~100 events/block; WAL handles it | Consider partitioned DB files or batched inserts |
| Snapshot size | < 1KB | < 100KB | Bincode compressed; snapshot less frequently |
| Subscription fan-out | 1 subscription per Engine | May share one alloy subscription with multiplexer | Multiplexer is recommended; single WebSocket |
| Gap recovery time | Seconds | Minutes | Consider parallel fetch_range calls |

**v1 scope:** 1-10 Engine instances per process. SQLite WAL handles this comfortably. Scale-out is a v2 problem.

---

## Sources

- Confidence: HIGH on all architectural patterns
- Sources: Training knowledge (Rust async patterns, alloy ecosystem, SQLite WAL internals, event sourcing literature including "Designing Event-Driven Systems" by Ben Stopford, Martin Fowler's event sourcing pattern)
- No external docs fetched (WebFetch/WebSearch unavailable in this research session)
- Alloy provider/subscription traits: confirmed present in alloy crate ecosystem as of 2024; specific API signatures may differ slightly from alloy 0.x releases — verify against current docs when implementing
- SQLite WAL mode: well-documented, stable since SQLite 3.7.0
- Confidence reduction: MEDIUM on specific alloy trait names (they evolve between releases); HIGH on everything else
