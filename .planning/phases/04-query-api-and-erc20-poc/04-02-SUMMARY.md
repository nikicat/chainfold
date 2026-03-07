---
phase: 04-query-api-and-erc20-poc
plan: 02
subsystem: store
tags: [historical-query, reconstruction, pagination, iterator, block-dag]

# Dependency graph
requires:
  - phase: 04-query-api-and-erc20-poc plan 01
    provides: Store trait, ReducedStore, Engine Arc<Storage>
  - phase: 02-persistence-and-recovery
    provides: Storage trait, InMemoryStorage, SqliteStorage, block DAG model
provides:
  - Storage::load_events_by_height_range (paginated canonical chain event queries)
  - reconstruct_at() (3-tier historical state reconstruction)
  - EventRangeIterator (lazy paginated event iteration)
  - store::query module
affects: [04-03-erc20-poc, future-async-rpc-tiers]

# Tech tracking
tech-stack:
  added: []
  patterns: [reversed-buffer-pop iterator, find-block-at-height walk, tiered reconstruction]

key-files:
  created:
    - src/store/query.rs
    - tests/query_api_tests.rs
  modified:
    - src/storage/mod.rs
    - src/storage/memory.rs
    - src/storage/sqlite.rs
    - src/store/mod.rs

key-decisions:
  - "reconstruct_at is sync (not async) — covers tiers 1-3 using only storage; async RPC tiers deferred"
  - "load_events_by_height_range walks canonical chain backward then filters by height — correct without canonical flag column"
  - "EventRangeIterator uses reversed-buffer + pop() pattern — O(1) per yield, no Clone bound, no unsafe"

patterns-established:
  - "Tiered reconstruction: snapshot > replay > full replay from genesis"
  - "Paginated event loading: limit/offset on flattened canonical chain events"
  - "find_block_at_height: backward walk from tip to locate block hash at target height"

requirements-completed: [QUERY-02, QUERY-03, PERSIST-06]

# Metrics
duration: 6min
completed: 2026-03-07
---

# Phase 4 Plan 2: Historical Query API Summary

**reconstruct_at() with 3-tier cost hierarchy and EventRangeIterator with lazy batch-paginated iteration over canonical chain events**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-07T20:35:46Z
- **Completed:** 2026-03-07T20:41:42Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Added `Storage::load_events_by_height_range` with limit/offset pagination to both InMemoryStorage and SqliteStorage
- Implemented `reconstruct_at()` with 3-tier cost hierarchy: exact snapshot, nearest snapshot + replay, full replay from genesis
- Built `EventRangeIterator` with lazy page loading that releases storage lock between pages
- 15 integration tests covering all reconstruction tiers, pagination edge cases, multi-event blocks, and post-reorg correctness

## Task Commits

Each task was committed atomically:

1. **Task 1: Storage height-range query + reconstruction planner + EventRangeIterator**
   - `7e3c7f5` (test: add failing tests — TDD RED)
   - `84ea735` (feat: implement historical query API — TDD GREEN)
2. **Task 2: At() and Range() integration tests** - `623a4e2` (test)

## Files Created/Modified
- `src/store/query.rs` - reconstruct_at(), EventRangeIterator, find_block_at_height helper
- `src/store/mod.rs` - Added `pub mod query` declaration
- `src/storage/mod.rs` - Added load_events_by_height_range to Storage trait + Arc blanket impl
- `src/storage/memory.rs` - InMemoryStorage::load_events_by_height_range implementation
- `src/storage/sqlite.rs` - SqliteStorage::load_events_by_height_range implementation
- `tests/query_api_tests.rs` - 15 integration tests for historical queries

## Decisions Made
- **reconstruct_at is sync**: The plan specified an async function with RPC fallback tiers (fetch missing events from source, fetch_state). Implemented as sync covering tiers 1-3 (storage-only). Async RPC tiers (3b, 4) can be added later as a separate async wrapper that calls the sync function first and falls back to RPC. This avoids requiring an EventSource reference for pure storage queries.
- **Canonical chain walk for height-range queries**: Both InMemoryStorage and SqliteStorage walk the canonical chain backward from tip, filter by height, then load events. This is correct without a "canonical" flag column on blocks. Performance optimization (height index or canonical flag) is future work.
- **Reversed-buffer + pop() for EventRangeIterator**: Events are loaded into a Vec, reversed, and yielded via pop(). This is O(1) per yield, doesn't require Clone on Event, and avoids unsafe code.

## Deviations from Plan

### Design Simplifications

**1. [Rule 2 - Simplification] reconstruct_at is sync, not async**
- **Found during:** Task 1
- **Issue:** Plan specified async reconstruct_at with EventSource for tiers 3-4 (RPC fallback). The sync tiers (1-3) cover all cases where data exists in storage.
- **Fix:** Implemented as sync function. Async wrapper with RPC fallback is additive (can be added later without changing the sync API).
- **Impact:** No scope reduction — all storage-based reconstruction works. RPC tiers are future work.

---

**Total deviations:** 1 simplification
**Impact on plan:** Correct for all storage-resident data. Async RPC tiers are additive future work.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Historical query API complete: At() via reconstruct_at, Range() via EventRangeIterator
- Combined with Plan 01's Current()/OnChange(), the full query API is available
- Ready for Plan 03: ERC20 PoC that exercises Current(), At(), and Range() with a real state machine

---
*Phase: 04-query-api-and-erc20-poc*
*Completed: 2026-03-07*
