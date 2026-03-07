---
phase: 04-query-api-and-erc20-poc
plan: 01
subsystem: api
tags: [watch-channel, store, arc, reactive, tokio]

# Dependency graph
requires:
  - phase: 03-live-subscriptions-and-reorg-safety
    provides: "ReducerCore with watch channel, Engine coordinator"
provides:
  - "Store trait (get/subscribe)"
  - "ReducedStore struct backed by Arc<watch::Sender>"
  - "Engine::reduced_store() for decoupled state access"
  - "Arc<S> storage in Engine for concurrent access"
  - "Blanket impl Storage for Arc<S>"
affects: [04-02, 04-03, erc20-poc]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Arc<watch::Sender> shared between ReducerCore and ReducedStore", "blanket Storage impl for Arc<S>"]

key-files:
  created:
    - "src/store/mod.rs"
    - "tests/store_tests.rs"
  modified:
    - "src/machine.rs"
    - "src/coordinator/mod.rs"
    - "src/storage/mod.rs"
    - "src/lib.rs"
    - "tests/coordinator_tests.rs"
    - "tests/recovery_tests.rs"
    - "tests/reorg_tests.rs"
    - "tests/live_engine_tests.rs"

key-decisions:
  - "ReducerCore::sender wrapped in Arc<watch::Sender> to share with ReducedStore — enables store creation without Engine reference"
  - "Engine::new() takes Arc<S> instead of S — callers wrap storage in Arc at construction"
  - "Blanket impl Storage for Arc<S> added for ergonomic direct usage of Arc<InMemoryStorage> etc."
  - "ReducedStore holds Arc<watch::Sender> not Arc<watch::Receiver> — Sender can create new subscribers, Receiver cannot"

patterns-established:
  - "Store trait: minimal get()/subscribe() interface for reactive state access"
  - "ReducedStore pattern: decouple state reads from Engine lifecycle"
  - "Arc<S> storage: concurrent access pattern for stores sharing storage with Engine"

requirements-completed: [QUERY-01, QUERY-04]

# Metrics
duration: 5min
completed: 2026-03-07
---

# Phase 4 Plan 1: Store Trait and ReducedStore Summary

**Store trait with ReducedStore backed by Arc<watch::Sender>, Engine refactored to Arc<Storage> for concurrent access**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-07T20:28:06Z
- **Completed:** 2026-03-07T20:33:00Z
- **Tasks:** 1 (TDD: RED + GREEN)
- **Files modified:** 10

## Accomplishments
- Store trait defined with get() and subscribe() for reactive state access
- ReducedStore implemented, decoupled from Engine — callers use get()/subscribe() without holding Engine reference
- Engine refactored to Arc<S> storage, enabling concurrent access from stores and external readers
- Blanket `impl Storage for Arc<S>` added for ergonomic usage
- All 113 tests passing (107 existing + 6 new store tests, zero regressions)

## Task Commits

Each task was committed atomically:

1. **Task 1 (RED): Store trait + ReducedStore tests** - `e5e28b5` (test)
2. **Task 1 (GREEN): Store trait, ReducedStore, Engine Arc refactor** - `9cfd410` (feat)

_TDD task: RED (failing tests) then GREEN (implementation)_

## Files Created/Modified
- `src/store/mod.rs` - Store trait and ReducedStore implementation
- `src/machine.rs` - ReducerCore::sender wrapped in Arc, added sender() accessor
- `src/coordinator/mod.rs` - Engine::storage changed to Arc<S>, added reduced_store() method
- `src/storage/mod.rs` - Blanket impl Storage for Arc<S>
- `src/lib.rs` - Added store module and re-exports (Store, ReducedStore)
- `tests/store_tests.rs` - 6 tests for Store/ReducedStore/Engine integration
- `tests/coordinator_tests.rs` - Updated Engine::new() calls with Arc::new()
- `tests/recovery_tests.rs` - Updated Engine::new() calls with Arc::new()
- `tests/reorg_tests.rs` - Updated Engine::new() calls with Arc::new()
- `tests/live_engine_tests.rs` - Updated Engine::new() calls with Arc::new()

## Decisions Made
- ReducerCore::sender wrapped in Arc<watch::Sender> — enables sharing with ReducedStore without moving sender out of ReducerCore
- Engine::new() takes Arc<S> instead of S — breaks existing call sites but is the cleanest approach since auto-deref handles all method calls transparently
- Blanket impl Storage for Arc<S> — delegates all methods to inner S, supporting both `Engine<SM, InMemoryStorage, C>` (with storage: Arc<InMemoryStorage>) and direct Arc<S> usage
- ReducedStore holds Arc<watch::Sender> because only Sender can create new subscribers; Receiver cannot

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Store trait and ReducedStore ready for query API (Plan 02)
- Current() = store.get() and OnChange = store.subscribe() + changed().await proven by tests
- Engine::reduced_store() provides decoupled state access for concurrent usage
- Arc<S> storage enables stores to share storage access with the live loop

## Self-Check: PASSED

- FOUND: src/store/mod.rs
- FOUND: tests/store_tests.rs
- FOUND: e5e28b5 (test commit)
- FOUND: 9cfd410 (feat commit)
- All 113 tests passing

---
*Phase: 04-query-api-and-erc20-poc*
*Completed: 2026-03-07*
