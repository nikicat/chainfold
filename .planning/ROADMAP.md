# Roadmap: Chainfold

## Overview

Chainfold is built in four phases that follow the hard dependency order dictated by the architecture: type foundations first, then block DAG persistence and crash recovery, then live subscriptions and reorg safety, then the public query API and ERC20 proof of concept. Each phase delivers a coherent, independently testable capability. Nothing in a later phase can be implemented correctly without the earlier phases being solid.

**Architecture note (2026-03-05):** Phase 2 is being redesigned from a flat linear event store to a block DAG model. State is tracked per block hash (like git commits). Reorgs are branch switches — the canonical tip moves to a different branch, and the library re-derives state from the fork point transparently. Phase 1 (cursors, traits, ReducerCore) is preserved; Phase 2 storage and coordinator are rebuilt.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundations** - Cursor types, EventSource/StateMachine traits, in-memory reducer — the type system everything else builds on
- [x] **Phase 2: Block DAG Storage and Recovery** - Block DAG schema in SQLite, canonical chain tracking, state snapshots per block, crash recovery along canonical chain, gap fetching (completed 2026-03-05)
- [x] **Phase 3: Live Subscriptions and Reorg Safety** - Live event loop, block-aware event processing, transparent reorg via DAG branch switching, reconnection with backoff (completed 2026-03-05)
- [x] **Phase 4: Query API and ERC20 PoC** - Public Current/At/Range/OnChange API, cheapest reconstruction path selection, end-to-end ERC20 demo with reorg handling (completed 2026-03-07)
- [x] **Phase 5: EVM ERC20 Module with Filter Pushdown** - Extract ERC20 PoC into reusable library module, add account-scoped eth_getLogs filter pushdown, feature-gated alloy dependency (completed 2026-03-09)

## Phase Details

### Phase 1: Foundations
**Goal**: The core type system exists — cursor ordering, source abstraction, and reducer contract — so every subsequent component can be built against a stable API
**Depends on**: Nothing (first phase)
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, CORE-05
**Success Criteria** (what must be TRUE):
  1. A caller can implement the `EventSource` trait for a mock chain and pass it to library code without knowing about EVM or Bitcoin specifics
  2. A caller can implement the `StateMachine` trait with a pure `Apply` reducer and have the library drive state forward via `apply(state, event)` without any I/O
  3. Cursors from two different positions can be compared with `Ord` and serialized round-trip without loss
  4. An in-memory reducer accumulates events correctly in the order they are applied, producing a deterministic final state verifiable by a unit test
  5. The library compiles with the generic type parameter `C: ChainCursor` propagated through the type system without requiring runtime dispatch
**Plans**: 2 plans
  - [x] 01-01-PLAN.md — Project scaffold, cursor types, Event envelope, EventSource trait
  - [x] 01-02-PLAN.md — StateMachine trait, ReducerCore, integration tests

### Phase 2: Block DAG Storage and Recovery
**Goal**: The library persists blocks and events in a DAG structure (like git) — each block has a parent hash, events belong to blocks, state snapshots are stored per block hash. The library tracks which branch is canonical, recovers from crash by replaying the canonical chain from the nearest snapshot, and fetches gaps without redundant RPC calls.
**Depends on**: Phase 1
**Requirements**: INGEST-02, INGEST-03, INGEST-04, PERSIST-01, PERSIST-02, PERSIST-03, PERSIST-04, PERSIST-05
**Success Criteria** (what must be TRUE):
  1. Blocks are stored with parent hash references forming a DAG. Given two blocks on different forks, the library can find their common ancestor by walking parent pointers.
  2. The library tracks which block hash is the canonical tip per source. Canonical chain is the path from tip to genesis following parent pointers.
  3. After a simulated crash and restart, the library finds the canonical tip, loads the nearest state snapshot on the canonical chain, replays forward along the canonical chain to the tip, and arrives at the correct state.
  4. State snapshots are associated with block hashes (not linear cursors). Loading a snapshot restores state as of that specific block, regardless of which branch it's on.
  5. Events are deduplicated by (block_hash, cursor) — inserting the same event twice (e.g., from overlapping backfill) has no effect.
  6. Historical backfill over a large range fetches blocks in chunked pages without loading all events into memory at once.
**Plans**: 4 plans (02-01 through 02-04)
  - [x] 02-01-PLAN.md — Block<C,E>, Storage trait, Codec trait, schema v2 types
  - [x] 02-02-PLAN.md — InMemoryStorage implementing Storage trait, 16 unit tests
  - [x] 02-03-PLAN.md — SqliteStorage implementing Storage trait
  - [x] 02-04-PLAN.md — Recovery pipeline: snapshot + canonical chain replay

### Phase 3: Live Subscriptions and Reorg Safety
**Goal**: The library tracks the chain tip in real time — events flow from a live subscription through block-aware processing into state, the library reconnects transparently after disconnection, and reorgs are handled as DAG branch switches where the library re-derives state from the fork point without client intervention
**Depends on**: Phase 2
**Requirements**: INGEST-01, INGEST-05, SAFETY-01, SAFETY-02, SAFETY-03
**Success Criteria** (what must be TRUE):
  1. A new event arriving on the live subscription reaches the state (and any OnChange callback) without crossing a process boundary or serialization step
  2. After a WebSocket disconnection, the library reconnects with exponential backoff, fetches any blocks missed during the outage, and resumes delivering events without duplicates or gaps
  3. When a chain reorganization occurs (canonical tip changes to a different branch), the library detects it via block hash / parent hash comparison, identifies the fork point, and re-derives state from the fork point on the new canonical chain — all transparently
  4. A client watching Current() sees the state silently update to reflect the new canonical chain. If the client was watching closely, they see a state not derived from the previous one (reorg visible). If they were watching infrequently, the reorg is invisible.
**Plans**: 3 plans (03-01 through 03-03)
  - [x] 03-01-PLAN.md — Storage contracts (get_parent_hash, set_canonical_tip), new engine types (EngineError, ReorgEvent, EngineStatus), updated EngineConfig
  - [x] 03-02-PLAN.md — Reorg detection: process_or_reorg, find_common_ancestor, handle_reorg, initial_state field, 10 integration tests
  - [x] 03-03-PLAN.md — Engine::run() live event loop with biased select!, micro-batching, exponential backoff, inline gap fill, 8 integration tests

### Phase 4: Query API and ERC20 PoC
**Goal**: The library exposes a complete, ergonomic public API via reactive stores and the ERC20 balance tracker demonstrates that the entire stack works correctly end-to-end under restarts and reorgs
**Depends on**: Phase 3
**Requirements**: QUERY-01, QUERY-02, QUERY-03, QUERY-04, PERSIST-06, POC-01
**Success Criteria** (what must be TRUE):
  1. `Current()` returns the live-tracked state at canonical chain tip; a caller observing a series of Transfer events sees the correct balance without writing any subscription management code
  2. `At(cursor)` returns historical state and selects the cheapest reconstruction path automatically: cached-only replay (0 RPC), partial gap fetch (1 RPC), or direct FetchState (1 RPC)
  3. `Range(from, to)` iterates over materialized state across a cursor range using a lazy cursor-based iterator that does not buffer the entire result set in memory
  4. `OnChange(callback)` delivers live state change notifications to the registered callback on each event application
  5. The ERC20 balance tracker starts cold, reconstructs balance from Transfer event history, survives a simulated process restart with correct balance restored from cache, and handles reorgs transparently (balance updates to reflect new canonical chain)
**Plans**: 3 plans
  - [x] 04-01-PLAN.md — Store trait, ReducedStore, Engine Arc<Storage> refactor (Current + OnChange)
  - [x] 04-02-PLAN.md — Historical queries: Storage height-range extension, reconstruction planner, EventRangeIterator (At + Range)
  - [x] 04-03-PLAN.md — ERC20 PoC: integration tests with mock source + example binary with real RPC

### Phase 5: EVM ERC20 Module with Filter Pushdown
**Goal**: The ERC20 PoC types and alloy-based EventSource are extracted into a reusable, feature-gated library module with account-scoped eth_getLogs filter pushdown — so users can track ERC20 balances for a single account without fetching all Transfer events
**Depends on**: Phase 4
**Requirements**: MOD-01, MOD-02, MOD-03, MOD-04, MOD-05
**Success Criteria** (what must be TRUE):
  1. `cargo check` (no features) compiles without pulling alloy — the evm feature is fully optional
  2. `chainfold::modules::erc20` exports Transfer, Erc20Machine, Erc20Codec, Erc20Source, Erc20Filter, and alloy re-exports
  3. `Erc20Source` with `Erc20Filter::Account(addr)` fires two scoped eth_getLogs queries per chunk and deduplicates results
  4. Existing 6 ERC20 integration tests pass using imports from the library module
  5. Example binary is CLI-only — all domain types imported from the library module
**Plans**: 2 plans
  - [x] 05-01-PLAN.md — Feature flag, module scaffold, domain types, Erc20Source with filter pushdown
  - [x] 05-02-PLAN.md — Migrate tests and thin example binary to import from module

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundations | 2/2 | Complete | 2026-03-03 |
| 2. Block DAG Storage and Recovery | 4/4 | Complete   | 2026-03-05 |
| 3. Live Subscriptions and Reorg Safety | 3/3 | Complete | 2026-03-05 |
| 4. Query API and ERC20 PoC | 3/3 | Complete | 2026-03-07 |
| 5. EVM ERC20 Module with Filter Pushdown | 2/2 | Complete | 2026-03-09 |
| 6. EVM Log Cache with Range Tracking | 0/2 | Planning | |

### Phase 6: EVM Log Cache with Range Tracking
**Goal**: eth_getLogs results are cached as a raw event layer keyed by RPC-level parameters (contract address, topic sets, block range). Scanned ranges are tracked even when empty — so a second run over the same range hits zero RPC calls. The block DAG layer consumes from this cache instead of directly from RPC. Events are reusable across reducers: same cached logs, different state machines.
**Depends on**: Phase 5
**Requirements**: CACHE-01, CACHE-02, CACHE-03, CACHE-04, CACHE-05
**Success Criteria** (what must be TRUE):
  1. After scanning a block range for a given (contract, topics[]) filter, a second scan of the same range makes zero RPC calls
  2. Scanning adjacent ranges merges into a single scanned range entry — no fragmentation
  3. Scanning a partially-cached range only fetches the uncovered sub-ranges from RPC
  4. Two different reducers (e.g., ERC20 balance tracker and a transfer counter) can be built from the same cached logs without re-fetching
  5. Filters are stored as (address, topic0, topic1, topic2, topic3) — matching eth_getLogs parameters, not application-level abstractions
**Plans**: 2 plans
  - [ ] 06-01-PLAN.md — Schema v4 migration, LogCache struct with range tracking, gap computation, log/header caching
  - [ ] 06-02-PLAN.md — CachingProvider<P> with finality-aware get_logs, BlockHeaderCache trait, Erc20Source wiring, example update
