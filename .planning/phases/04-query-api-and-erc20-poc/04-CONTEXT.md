# Phase 4: Query API and ERC20 PoC - Context

**Gathered:** 2026-03-07
**Status:** Ready for planning

<domain>
## Phase Boundary

The library is refactored from an Engine-centric model to a reactive store architecture. Stores expose `get() + subscribe()` and compose via constructor injection. The query API (Current/At/Range/OnChange) emerges from store primitives rather than being bolted onto Engine. The ERC20 balance tracker demonstrates the full stack end-to-end: store composition, cold start reconstruction, restart recovery, and transparent reorg handling.

</domain>

<decisions>
## Implementation Decisions

### Reactive store model
- The core abstraction is a **Store**, not methods on Engine. A Store exposes `get()` (current state) and `subscribe()` (watch::Receiver for change notifications).
- Stores are independent of Engine's `&mut self` — they work concurrently while the live event loop runs.
- The current Engine becomes the orchestrator that wires stores together and drives the live loop. Query methods (Current/At/OnChange) are fulfilled by stores, not by Engine.

### Store trait
- `get() + subscribe()` — minimal interface that works for both reduced state (balance = single number) and accumulated state (transfer history = navigable collection).
- `subscribe()` returns `watch::Receiver<State>`. No richer change notification needed — caller diffs with their previous copy if they need deltas.
- For reduced state: `get()` returns a value (e.g., `u128` balance).
- For accumulated state: `get()` returns a **lazy, storage-backed** navigable type. Iteration loads events from SQLite on demand. `get()` itself is cheap (returns a handle, not all data). The navigable type supports forward/backward iteration and cursor-based positioning (e.g., walk backward from head until hitting a known position to find "what's new").

### Store composition
- Derived stores take upstream store(s) as **constructor arguments** and internally subscribe to their changes.
- Example: `BalanceStore::new(&event_store)` — BalanceStore subscribes to TransferEventStore and reduces events into a balance.
- Wiring is explicit at construction time. No central registry or graph manager.

### State reconstruction (PERSIST-06)
- When a store needs historical state that's not cached, it materializes it using the cheapest available path.
- **Cost hierarchy (fixed):** cached state snapshot > cached events replay > fetch missing events from source > direct fetch_state from source.
- Each layer caches its results: fetched events are always cached in the event store; whether derived state snapshots are cached is **configurable per store** (some stores want aggressive caching for expensive reducers, others follow periodic snapshot policy).
- Implementation of the planner (per-store vs shared component) is Claude's discretion.

### Data flow architecture
- `Query <-> StateStore <-> StateReducePlanner <-> EventStore <-> EventFetchPlanner <-> EventSource(s)`
- Each layer only talks to its neighbor. Queries never touch EventSource directly.
- Downstream: requests cascade from query down to source when cache misses occur.
- Upstream: data flows back from sources through stores to the query caller, getting cached at each layer.

### OnChange semantics
- `subscribe()` returns `watch::Receiver<State>` — same mechanism already implemented in ReducerCore.
- No event context in change notifications. Caller watches state, not causality.
- This fulfills QUERY-04 (OnChange callback) via the store's subscribe().

### Current(), At(), Range() mapping
- `Current()` = `store.get()` — returns live-tracked state at canonical tip.
- `At(cursor)` = historical state reconstruction via the cheapest path (see cost hierarchy above). Triggers the reconstruction planner if not cached.
- `Range(from, to)` = lazy iteration over materialized state across a cursor range. Built on the storage-backed navigable type returned by accumulated stores.
- `OnChange(callback)` = `store.subscribe()` + caller loop on `changed().await`.

### ERC20 PoC
- **Both integration tests and example binary.** Tests prove correctness with mock sources (CI-friendly). Binary demonstrates real-world usage.
- **Integration tests (mock source):** Cold start reconstruction, restart from cache, reorg handling, store composition. Deterministic, no RPC needed.
- **Example binary (real RPC):** Configurable via CLI args (RPC URL + token address). Works with any EVM chain. Prints each Transfer (from, to, amount) plus running balance — demonstrates both accumulated (transfer history) and reduced (balance) state.
- **Restart recovery in the binary:** Run once, stop, run again — binary prints recovery info showing it picked up from cache. Not just tested, but visibly demonstrated.

### Claude's Discretion
- Reconstruction planner design (per-store logic vs shared component)
- How to refactor Engine into the store-based architecture (incremental vs clean break)
- Internal store implementation details (threading, locking, update propagation)
- Exact navigable history type API beyond iter_forward/iter_backward/head
- Store trait generic parameters and associated types
- How Range() integrates with the lazy storage-backed iterator

</decisions>

<specifics>
## Specific Ideas

- Engine already has `state()` (clone from watch) and `subscribe()` (watch::Receiver) — these map directly to Store::get() and Store::subscribe(). The refactor should preserve this existing mechanism.
- ReducerCore already drives state via `send_modify` on a watch channel — this is the update propagation primitive that stores will use internally.
- Storage trait already has `find_nearest_snapshot`, `collect_canonical_chain`, `load_block_events` — these are the building blocks for the reconstruction planner's cached replay path.
- EventSource::fetch_state() and fetch_blocks() are the RPC fallback paths when local cache is insufficient.
- The write-first invariant (store then apply) from Phase 2-3 must be preserved: incoming events go to EventStore first, then derived stores react.

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ReducerCore<SM>` (machine.rs): owns watch::Sender, has get/subscribe pattern — direct precursor to Store internals
- `Engine<SM, S, C>` (coordinator/mod.rs): orchestrates process_block, recovery, run loop — refactors into store wiring + live loop
- `Storage` trait (storage/mod.rs): block DAG operations — becomes the backing for EventStore
- `SqliteStorage` (storage/sqlite.rs): WAL-mode SQLite — backs lazy iteration for accumulated state
- `InMemoryStorage` (storage/memory.rs): useful for testing stores without SQLite
- `EventSource` trait (source.rs): fetch_state, fetch_blocks, subscribe — the leaf data source in the reactive graph
- `Codec` trait (coordinator/mod.rs): encode/decode for events and state — used at store boundaries for serialization

### Established Patterns
- `watch::Receiver` for state distribution (machine.rs) — directly maps to Store::subscribe()
- Write-first invariant: store events before applying (coordinator/mod.rs)
- Opaque bytes: client-owned serialization via Codec trait
- Sync Storage trait with spawn_blocking bridge for async callers
- INSERT OR IGNORE deduplication at storage boundary
- Constructor injection already used: Engine::new(core, storage, codec, config)

### Integration Points
- Store trait is NEW — core abstraction for Phase 4
- Engine refactored: live loop stays, query methods move to stores
- ReducerCore may be absorbed into store internals or kept as the reduction engine stores use
- Storage trait may need new methods for lazy cursor-based iteration (forward/backward scan by block height range)
- Accumulated state needs a new navigable type backed by Storage reads

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 04-query-api-and-erc20-poc*
*Context gathered: 2026-03-07*
