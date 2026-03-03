# Roadmap: Chainfold

## Overview

Chainfold is built in four phases that follow the hard dependency order dictated by the architecture: type foundations first, then persistence and crash recovery, then live subscriptions and reorg safety, then the public query API and ERC20 proof of concept. Each phase delivers a coherent, independently testable capability. Nothing in a later phase can be implemented correctly without the earlier phases being solid.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundations** - Cursor types, EventSource/StateMachine traits, in-memory reducer — the type system everything else builds on
- [ ] **Phase 2: Persistence and Recovery** - SQLite schema, coverage tracking, crash recovery pipeline, historical backfill, gap stitching
- [ ] **Phase 3: Live Subscriptions and Reorg Safety** - Live event delivery hot path, WebSocket reconnection, reorg detection and notification
- [ ] **Phase 4: Query API and ERC20 PoC** - Public Current/At/Range/OnChange API, cheapest reconstruction path selection, end-to-end ERC20 demo

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

### Phase 2: Persistence and Recovery
**Goal**: The library survives process restarts — it persists events and snapshots to SQLite, tracks which cursor ranges are cached, and executes the full cold-start recovery pipeline without issuing redundant RPC calls
**Depends on**: Phase 1
**Requirements**: INGEST-02, INGEST-03, INGEST-04, PERSIST-01, PERSIST-02, PERSIST-03, PERSIST-04, PERSIST-05
**Success Criteria** (what must be TRUE):
  1. After a simulated crash and restart, the library loads the last snapshot, replays only the cached events not already in the snapshot, fetches only the gap between cached coverage and live tip, and arrives at the correct state without a full re-fetch
  2. Overlapping events from a historical backfill and a subsequent fetch do not produce duplicate state — deduplication by `(source, cursor)` is enforced at the storage boundary
  3. The coverage table accurately reflects which cursor ranges are fully cached; a query for a range fully within cached coverage issues zero RPC calls
  4. Named anchor positions persist across restarts and are updated atomically with the event writes they track
  5. Historical backfill over a large range completes in chunked pages without loading all events into memory at once
**Plans**: TBD

### Phase 3: Live Subscriptions and Reorg Safety
**Goal**: The library tracks the chain tip in real time — events flow from a live WebSocket subscription through Apply to callbacks on the hot path, the library reconnects transparently after disconnection, and clients are notified of reorgs so they can decide how to respond
**Depends on**: Phase 2
**Requirements**: INGEST-01, INGEST-05, SAFETY-01, SAFETY-02, SAFETY-03
**Success Criteria** (what must be TRUE):
  1. A new event arriving on the live WebSocket subscription reaches the client callback without crossing a process boundary or serialization step
  2. After a WebSocket disconnection, the library reconnects with exponential backoff, fetches any events missed during the outage using the same gap-recovery code path as cold start, and resumes delivering events without duplicates or gaps
  3. When a chain reorganization occurs, the library detects it by comparing stored block hashes against the canonical chain and calls the client-provided reorg callback with the affected cursor range and reorg depth
  4. The client reorg callback receives enough information (old tip, new tip, common ancestor, depth) to decide its own rollback strategy without requiring library internals
**Plans**: TBD

### Phase 4: Query API and ERC20 PoC
**Goal**: The library exposes a complete, ergonomic public API and the ERC20 balance tracker demonstrates that the entire stack works correctly end-to-end under restarts and reorgs
**Depends on**: Phase 3
**Requirements**: QUERY-01, QUERY-02, QUERY-03, QUERY-04, PERSIST-06, POC-01
**Success Criteria** (what must be TRUE):
  1. `Current()` returns the live-tracked state at chain tip; a caller observing a series of Transfer events sees the correct balance without writing any subscription management code
  2. `At(cursor)` returns historical state and selects the cheapest reconstruction path automatically: pure cache replay when the range is fully cached, a partial gap fetch when partially cached, or a direct FetchState call when no cache exists
  3. `Range(from, to)` iterates over materialized state across a cursor range using a lazy cursor-based iterator that does not buffer the entire result set in memory
  4. `OnChange(callback)` delivers live state change notifications to the registered callback on each event application
  5. The ERC20 balance tracker starts cold, reconstructs balance from Transfer event history, survives a simulated process restart with correct balance restored from cache, and triggers the reorg callback when the test harness reorgs the chain
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundations | 2/2 | Complete | 2026-03-03 |
| 2. Persistence and Recovery | 0/TBD | Not started | - |
| 3. Live Subscriptions and Reorg Safety | 0/TBD | Not started | - |
| 4. Query API and ERC20 PoC | 0/TBD | Not started | - |
