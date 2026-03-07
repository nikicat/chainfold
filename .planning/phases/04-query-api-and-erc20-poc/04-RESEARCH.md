# Phase 4: Query API and ERC20 PoC - Research

**Researched:** 2026-03-07
**Domain:** Reactive store architecture, query API design, lazy SQLite iteration, ERC20 PoC
**Confidence:** HIGH

## Summary

Phase 4 transforms the Engine-centric architecture into a reactive store model where independent stores expose `get() + subscribe()` and compose via constructor injection. The query API (Current/At/Range/OnChange) emerges naturally from store primitives. The key technical challenges are: (1) designing the Store trait and its interaction with `tokio::sync::watch`, (2) implementing lazy cursor-based iteration over SQLite for Range queries without holding connection locks, (3) building the reconstruction planner for At() with the fixed cost hierarchy, and (4) demonstrating the full stack via an ERC20 balance tracker.

The existing codebase already has the building blocks: `ReducerCore` owns `watch::Sender` with `get/subscribe`, `Storage` trait provides DAG operations, `Codec` handles serialization boundaries, and the Engine orchestrates the live loop. The refactor preserves these while lifting query capabilities out of Engine and into composable stores.

**Primary recommendation:** Implement stores as thin wrappers around `watch::Sender<State>` with internal subscription to upstream stores. Use block-height-range SQL queries with batch pagination for lazy Range() iteration -- do NOT try to hold rusqlite `Rows` across an API boundary.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- The core abstraction is a **Store**, not methods on Engine. A Store exposes `get()` (current state) and `subscribe()` (watch::Receiver for change notifications).
- Stores are independent of Engine's `&mut self` -- they work concurrently while the live event loop runs.
- The current Engine becomes the orchestrator that wires stores together and drives the live loop. Query methods (Current/At/OnChange) are fulfilled by stores, not by Engine.
- `subscribe()` returns `watch::Receiver<State>`. No richer change notification needed.
- For reduced state: `get()` returns a value (e.g., `u128` balance).
- For accumulated state: `get()` returns a **lazy, storage-backed** navigable type. Iteration loads events from SQLite on demand. `get()` itself is cheap (returns a handle, not all data).
- Derived stores take upstream store(s) as **constructor arguments** and internally subscribe to their changes.
- Example: `BalanceStore::new(&event_store)` -- BalanceStore subscribes to TransferEventStore and reduces events into a balance.
- Wiring is explicit at construction time. No central registry or graph manager.
- **Cost hierarchy (fixed):** cached state snapshot > cached events replay > fetch missing events from source > direct fetch_state from source.
- Each layer caches its results: fetched events are always cached in the event store; derived state snapshots configurable per store.
- **Data flow:** `Query <-> StateStore <-> StateReducePlanner <-> EventStore <-> EventFetchPlanner <-> EventSource(s)`. Each layer only talks to its neighbor.
- `subscribe()` returns `watch::Receiver<State>` -- same mechanism already in ReducerCore. No event context in change notifications.
- `Current()` = `store.get()`, `At(cursor)` = historical reconstruction via cheapest path, `Range(from, to)` = lazy iteration over materialized state, `OnChange(callback)` = `store.subscribe()` + caller loop.
- ERC20 PoC: **both integration tests AND example binary**. Tests with mock sources (CI-friendly). Binary with real RPC (configurable).
- Restart recovery in binary: visibly demonstrated (run once, stop, run again).

### Claude's Discretion
- Reconstruction planner design (per-store logic vs shared component)
- How to refactor Engine into the store-based architecture (incremental vs clean break)
- Internal store implementation details (threading, locking, update propagation)
- Exact navigable history type API beyond iter_forward/iter_backward/head
- Store trait generic parameters and associated types
- How Range() integrates with the lazy storage-backed iterator

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| QUERY-01 | `Current() -> S` returning live-tracked state at canonical chain tip | Store::get() wrapping watch::Receiver::borrow().clone(); direct mapping from existing ReducerCore pattern |
| QUERY-02 | `At(cursor) -> S` returning historical state via cheapest reconstruction path | Reconstruction planner with 4-tier cost hierarchy; uses existing Storage::find_nearest_snapshot + collect_canonical_chain + load_block_events |
| QUERY-03 | `Range(from, to) -> Iterator<S>` for materialized state across cursor range | Lazy batch-paginated SQL queries by height range; returns owned iterator that fetches pages on demand |
| QUERY-04 | `OnChange(callback)` for live state change notifications | store.subscribe() returns watch::Receiver; caller loops on changed().await; identical to existing ReducerCore pattern |
| PERSIST-06 | Cheapest reconstruction path selection for historical queries | 4-tier cost hierarchy: cached snapshot > cached event replay > fetch missing events > direct fetch_state |
| POC-01 | ERC20 balance tracker end-to-end demonstration | Integration tests with MockEventSource + example binary with real RPC; demonstrates cold start, restart recovery, reorg handling |
</phase_requirements>

## Standard Stack

### Core (already in Cargo.toml)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1 | Async runtime, watch channels, sync primitives | Already used; watch::Sender/Receiver is the state distribution primitive |
| rusqlite | 0.34 | SQLite storage with WAL mode | Already used; lazy iteration via height-range queries |
| postcard | 1 | Cursor serialization (library-internal) | Already used; fast binary format |
| thiserror | 2 | Error types for store errors | Already used |
| async-trait | 0.1 | Async trait support | Already used for EventSource |

### New Dependencies Needed
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| clap | 4 | CLI argument parsing for ERC20 example binary | Example binary only (dev-dependency or example feature) |
| alloy | 0.12+ | EVM types and RPC for ERC20 example binary | Example binary: real RPC interaction, ABI decoding |
| hex | 0.4 | Hex encoding for block hashes in example output | Example binary display |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| clap for CLI | std::env::args | clap is heavier but example binary benefits from --help and validation |
| alloy for EVM | ethers-rs | ethers-rs is deprecated; alloy is the standard as of 2025 |

**Installation (for example binary):**
```bash
# These go in [dev-dependencies] or behind a feature flag, not main deps
cargo add clap --features derive --dev
cargo add alloy --dev
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  store/
    mod.rs           # Store trait definition
    event_store.rs   # EventStore: stores raw events, lazy iteration
    state_store.rs   # StateStore: reduced state from events (wraps ReducerCore pattern)
    planner.rs       # Reconstruction planner: cheapest path for At()
  coordinator/
    mod.rs           # Engine refactored: wires stores, drives live loop
    recovery.rs      # Existing recovery (mostly unchanged)
  storage/           # Existing: Storage trait, SqliteStorage, InMemoryStorage
  machine.rs         # Existing: StateMachine, ReducerCore
  source.rs          # Existing: EventSource trait

examples/
  erc20_tracker.rs   # Example binary: real RPC ERC20 balance tracker

tests/
  store_tests.rs     # Store trait and composition tests
  query_api_tests.rs # Current/At/Range/OnChange integration tests
  erc20_tests.rs     # ERC20 PoC integration tests with mock source
```

### Pattern 1: Store Trait
**What:** Generic trait for reactive state containers with `get() + subscribe()`.
**When to use:** Every state-bearing component in the system.

```rust
// The core abstraction. Stores hold state and notify subscribers.
pub trait Store: Send + Sync {
    type State: Clone + Send + 'static;

    /// Get current state. Cheap for reduced state (clone from watch).
    /// For accumulated state, returns a handle/view, not all data.
    fn get(&self) -> Self::State;

    /// Subscribe to state changes. Returns watch::Receiver.
    fn subscribe(&self) -> watch::Receiver<Self::State>;
}
```

**Key insight:** The Store trait is intentionally minimal. `get()` returns a clone of the current state from the watch channel. `subscribe()` returns a `watch::Receiver`. This maps directly to what ReducerCore already does. The complexity is in the *implementations*, not the trait.

### Pattern 2: Store Composition via Constructor Injection
**What:** Derived stores subscribe to upstream stores at construction time.
**When to use:** Any store whose state is derived from another store's state.

```rust
// BalanceStore subscribes to an event store and reduces events into a balance.
pub struct BalanceStore<SM: StateMachine> {
    sender: watch::Sender<SM::State>,
    // Internal task handle for the subscription loop
}

impl<SM: StateMachine> BalanceStore<SM> {
    pub fn new(event_store: &impl Store<State = Vec<Event<SM::Cursor, SM::Event>>>,
               sm: Arc<SM>,
               initial_state: SM::State) -> Self {
        let (sender, _) = watch::channel(initial_state);
        // Spawn internal task that watches event_store.subscribe()
        // and applies SM::apply to update balance
        // ...
        Self { sender }
    }
}
```

### Pattern 3: Lazy Storage-Backed Iterator for Range()
**What:** Batch-paginated iterator that loads events from SQLite in pages.
**When to use:** Range(from, to) queries over accumulated state.

```rust
// IMPORTANT: rusqlite Rows borrows the Connection. You CANNOT return a
// Rows iterator across an API boundary. Instead, use batch pagination:
// load N events per page, yield them, load next page when exhausted.
pub struct EventRangeIterator<C: ChainCursor, E> {
    storage: Arc<dyn Storage>,  // or concrete type
    codec: Arc<dyn Codec>,
    from_height: u64,
    to_height: u64,
    current_height: u64,
    page_size: usize,
    buffer: Vec<Event<C, E>>,
    buffer_pos: usize,
}

impl<C: ChainCursor, E> Iterator for EventRangeIterator<C, E> {
    type Item = Event<C, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer_pos >= self.buffer.len() {
            // Load next page from SQLite
            self.load_next_page();
        }
        // Return next buffered item
        // ...
    }
}
```

**Critical constraint:** rusqlite's `Rows` struct borrows the `Connection` (via `Statement`), so you cannot return a lazy `Rows` iterator from a function that takes `&self` on `SqliteStorage`. The `Mutex<Connection>` lock would need to be held for the iterator's lifetime, blocking all other operations. Batch pagination (load N rows, release lock, yield, load next N) is the correct pattern.

### Pattern 4: Reconstruction Planner for At()
**What:** Selects the cheapest path to materialize state at a historical cursor position.
**When to use:** `At(cursor)` queries.

```rust
// Cost hierarchy (from cheapest to most expensive):
// 1. Cached state snapshot at exact cursor -> return directly (0 RPC, 0 replay)
// 2. Nearest snapshot + cached event replay to target (0 RPC, O(n) replay)
// 3. Nearest snapshot + fetch missing events + replay (1 RPC, O(n) replay)
// 4. Direct fetch_state from source (1 RPC, 0 replay)
//
// The planner tries each tier in order and falls through to the next.
```

### Anti-Patterns to Avoid
- **Holding Mutex<Connection> across iterator boundaries:** rusqlite Connection is not Send, and holding the lock blocks all storage operations. Always release the lock between pages.
- **Making Store trait async:** The watch channel pattern is synchronous (`borrow()` is instant). Only `At()` needs async (for RPC fallback). Keep Store::get() sync, At() as a separate async method.
- **Buffering entire Range() result:** The whole point of lazy iteration is to avoid loading millions of events into memory. Use page-based loading.
- **Engine owning query methods:** Queries must work concurrently with the live loop. Engine holds `&mut self` during run(). Stores use `Arc<watch::Sender>` and `Arc<Storage>` for concurrent access.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| State change notification | Custom event bus / callback registry | `tokio::sync::watch` | Already proven in codebase; handles multiple subscribers, always has latest value |
| Cursor serialization | Custom binary format | `postcard` | Already used; compact, fast, no schema needed |
| CLI argument parsing | Manual argv parsing | `clap` with derive | Handles --help, validation, error messages for the example binary |
| EVM RPC interaction | Raw HTTP/WS client | `alloy` provider + contract | Type-safe ABI, proper log decoding, reconnection handling |
| Concurrent state access | Manual Arc<Mutex<State>> | `watch::Sender/Receiver` | Lock-free reads via Receiver::borrow(); atomic writes via send_modify |

**Key insight:** The watch channel is the foundation. Every pattern in this phase builds on `watch::Sender` for state updates and `watch::Receiver` for concurrent reads. Don't introduce new synchronization primitives.

## Common Pitfalls

### Pitfall 1: Store::get() Contention with Engine::run()
**What goes wrong:** Engine::run() takes `&mut self`. If stores try to access Engine for queries, they deadlock or can't compile.
**Why it happens:** Engine currently owns ReducerCore, Storage, and Codec. Stores need access to these too.
**How to avoid:** Stores must hold their own `Arc<Storage>`, `Arc<Codec>`, and `watch::Receiver<State>`. Engine shares these via Arc at construction time. The live loop updates state through the shared `watch::Sender`, and stores read through their own `watch::Receiver`.
**Warning signs:** Lifetime errors when trying to borrow Engine from a Store. `&mut self` conflicts with concurrent read access.

### Pitfall 2: rusqlite Rows Lifetime
**What goes wrong:** Trying to return `impl Iterator` that internally holds a rusqlite `Rows` -- the `Rows` borrows `Statement` which borrows `Connection`.
**Why it happens:** rusqlite uses borrowing for zero-copy row access. The borrow chain is `Connection -> Statement -> Rows -> Row`.
**How to avoid:** Collect each page into a `Vec`, release the connection lock, yield from the Vec. Load next page when Vec is exhausted.
**Warning signs:** Lifetime error: "borrowed value does not live long enough" on Statement or Rows.

### Pitfall 3: watch::Receiver::changed() Semantics
**What goes wrong:** Missing state updates or seeing stale state because `changed()` coalesces multiple updates.
**Why it happens:** `watch` only retains the *last* value. If sender updates 3 times before receiver calls `changed().await`, receiver sees only the final value.
**How to avoid:** This is actually correct for reduced state (balance = latest value). Document this behavior. For accumulated state where every event matters, the event store must persist all events -- the watch notification just signals "something changed, re-query".
**Warning signs:** Tests that expect to see every intermediate state update via watch.

### Pitfall 4: At() for Non-Canonical Heights
**What goes wrong:** Requesting At(cursor) for a block height that exists on a non-canonical branch. The reconstruction planner finds a snapshot but it's on the wrong branch.
**Why it happens:** Storage has blocks from multiple branches (reorg history). Snapshots might exist on orphaned branches.
**How to avoid:** The reconstruction planner must walk the canonical chain from tip backward, not just query by height. Use `collect_canonical_chain` + `find_nearest_snapshot` which already follow parent pointers on the canonical chain.
**Warning signs:** At() returning different values depending on whether a reorg happened.

### Pitfall 5: Store Subscription Ordering During Startup
**What goes wrong:** A derived store subscribes to an upstream store after the upstream has already emitted its initial state. The derived store misses the initial value.
**Why it happens:** If stores are wired up in the wrong order, or if the upstream store processes events before the derived store's subscription is active.
**How to avoid:** `watch::Receiver` always has the current value immediately on creation (via `borrow()`). So even if the derived store subscribes "late", it gets the current state. The only risk is if it needs to see the *sequence* of updates, which reduced stores don't.
**Warning signs:** Derived store showing zero/default state after upstream already has data.

## Code Examples

### Current() via Store::get()
```rust
// Source: existing ReducerCore pattern (machine.rs)
// Direct mapping: ReducerCore.subscribe().borrow().clone() -> Store.get()
impl<S: Clone + Send + 'static> Store for ReducedStore<S> {
    type State = S;

    fn get(&self) -> S {
        self.receiver.borrow().clone()
    }

    fn subscribe(&self) -> watch::Receiver<S> {
        self.sender.subscribe()
    }
}
```

### OnChange() via subscribe() + changed()
```rust
// Source: tokio::sync::watch docs
let mut rx = store.subscribe();
loop {
    // Wait for state to change
    rx.changed().await.expect("sender dropped");
    let new_state = rx.borrow().clone();
    callback(new_state);
}
```

### Batch-Paginated Range Query (SQLite)
```rust
// New Storage method needed for lazy iteration
// Added to Storage trait:
fn load_events_by_height_range(
    &self,
    source_id: &str,
    from_height: u64,
    to_height: u64,
    limit: usize,
    offset: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>;

// SQL: SELECT e.cursor, e.payload FROM events e
//      JOIN blocks b ON e.block_hash = b.hash
//      WHERE b.source_id = ?1 AND b.height >= ?2 AND b.height <= ?3
//      ORDER BY b.height ASC, e.id ASC
//      LIMIT ?4 OFFSET ?5
```

### Engine Refactor: Sharing State via Arc
```rust
// Engine construction shares components with stores
let storage = Arc::new(SqliteStorage::open("cache.db")?);
let (sender, receiver) = watch::channel(initial_state);

// Engine uses Arc<Storage> + watch::Sender for writes
// Stores use Arc<Storage> + watch::Receiver for reads
// Both can operate concurrently
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Engine owns all query methods | Stores expose queries independently | Phase 4 | Concurrent reads during live loop |
| ReducerCore in-memory Vec for events | SQLite-backed event storage via Storage trait | Phase 2 | Lazy iteration possible, survives restart |
| Linear event replay for historical queries | 4-tier reconstruction planner | Phase 4 | Minimal RPC calls for historical state |
| Engine::state() for current value | Store::get() | Phase 4 | Decoupled from Engine lifecycle |

**Preserved from earlier phases:**
- `watch::Sender/Receiver` pattern (Phase 1 ReducerCore) -- becomes Store internals
- Write-first invariant (Phase 2) -- still applied by Engine during live loop
- Block DAG with parent pointers (Phase 2) -- used by reconstruction planner for canonical chain walking
- Reorg handling via re-derivation (Phase 3) -- transparent to stores; they see state updates via watch

## Open Questions

1. **Should Storage trait gain height-range query methods, or should this be a separate StorageExt / QueryableStorage trait?**
   - What we know: Current Storage trait has block-hash-based methods only. Range queries need height-based access.
   - What's unclear: Adding methods to Storage means InMemoryStorage also needs them. Could be a separate trait that SqliteStorage implements.
   - Recommendation: Add to Storage trait directly. InMemoryStorage needs it for tests anyway. Keep one trait.

2. **How does Engine share Storage with stores if Storage is not behind Arc?**
   - What we know: Engine currently owns `storage: S` directly. Stores need concurrent access.
   - What's unclear: Refactoring Engine to use `Arc<S>` changes its generic signature.
   - Recommendation: Engine takes `Arc<S>` at construction. This is a breaking change but necessary. All `&self.storage` calls work the same via Deref.

3. **Should At() be on the Store trait or a separate trait?**
   - What we know: `get()` is sync and cheap. `At(cursor)` might need async (RPC fallback). Different method signatures.
   - What's unclear: Whether all stores support At() or only some.
   - Recommendation: `At()` as a separate method (or separate `HistoricalStore` trait). Not all stores need historical reconstruction. Keep Store trait minimal.

## Sources

### Primary (HIGH confidence)
- Existing codebase: `src/machine.rs`, `src/coordinator/mod.rs`, `src/storage/sqlite.rs` -- verified current patterns
- [tokio::sync::watch docs](https://docs.rs/tokio/latest/tokio/sync/watch/index.html) -- watch channel semantics
- [rusqlite Rows docs](https://docs.rs/rusqlite/0.34.0/rusqlite/struct.Rows.html) -- Rows borrowing constraints

### Secondary (MEDIUM confidence)
- [Tokio channels tutorial](https://tokio.rs/tokio/tutorial/channels) -- general channel patterns
- CONTEXT.md decisions -- user-locked architecture choices

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all core libraries already in use; only clap/alloy are new (for example binary only)
- Architecture: HIGH -- Store pattern directly maps to existing ReducerCore; refactor path is clear
- Pitfalls: HIGH -- rusqlite Rows lifetime is well-documented; watch semantics verified from docs
- Reconstruction planner: MEDIUM -- algorithm is straightforward but At() for non-canonical heights needs care

**Research date:** 2026-03-07
**Valid until:** 2026-04-07 (stable domain; tokio/rusqlite APIs are mature)
