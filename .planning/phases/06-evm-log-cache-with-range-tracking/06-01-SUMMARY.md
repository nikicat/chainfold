---
phase: 06-evm-log-cache-with-range-tracking
plan: 01
subsystem: database
tags: [blake3, sqlite, caching, evm, alloy, range-tracking]

# Dependency graph
requires:
  - phase: 05-evm-erc20-module
    provides: alloy Provider pattern, evm feature flag, module structure
provides:
  - LogCache struct with own SQLite connection and range tracking
  - Schema v4 migration (cached_logs, scanned_ranges, cached_block_headers tables)
  - compute_filter_hash for deterministic BLAKE3 cache keys
  - compute_gaps sweep algorithm for uncovered sub-ranges
  - Block header caching by height
affects: [06-02, evm-cache, caching-provider]

# Tech tracking
tech-stack:
  added: [blake3]
  patterns: [eager-merge-on-write, filter-hash-keying, gap-computation-sweep]

key-files:
  created: [src/modules/evm_cache.rs]
  modified: [Cargo.toml, src/storage/schema.rs, src/storage/sqlite.rs, src/modules/mod.rs]

key-decisions:
  - "LogCache opens own Connection to same DB file -- separate from SqliteStorage"
  - "BLAKE3 for filter key hashing with 0xFF sentinel for null topics"
  - "Eager merge-on-write with saturating_sub for adjacency detection"
  - "configure_connection promoted to pub(crate) for LogCache reuse"
  - "alloy Log reconstruction stores all fields except block_timestamp (not available in cache)"

patterns-established:
  - "Merge-on-write: query merge candidates, compute merged bounds, delete+insert in single TX"
  - "Gap sweep: cursor-based linear scan over sorted non-overlapping ranges"
  - "Separate Connection per subsystem: LogCache and SqliteStorage share file, not mutex"

requirements-completed: [CACHE-01, CACHE-02, CACHE-03]

# Metrics
duration: 4min
completed: 2026-03-10
---

# Phase 6 Plan 01: LogCache Storage Layer Summary

**BLAKE3-keyed log cache with schema v4 migration, eager range merging, gap computation, and alloy Log round-trip storage**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-10T00:16:59Z
- **Completed:** 2026-03-10T00:21:24Z
- **Tasks:** 1
- **Files modified:** 5

## Accomplishments
- Schema v4 migration adds cached_logs, scanned_ranges, cached_block_headers tables
- LogCache with deterministic BLAKE3 filter hashing and 0xFF sentinel for null topics
- Scanned range recording with eager merge-on-write (overlapping, adjacent, contained, multi-merge)
- Gap computation sweep algorithm for uncovered sub-ranges
- Alloy Log round-trip: insert and retrieve with full field fidelity
- Block header caching by height with INSERT OR REPLACE
- 24 unit tests covering all behaviors

## Task Commits

Each task was committed atomically:

1. **Task 1: Schema v4 migration and LogCache struct with core methods** - `10e4823` (feat)

## Files Created/Modified
- `src/modules/evm_cache.rs` - LogCache struct, compute_filter_hash, compute_gaps, range tracking, log/header CRUD, 24 tests
- `src/storage/schema.rs` - v4 migration adding 3 cache tables with indexes
- `src/storage/sqlite.rs` - configure_connection promoted to pub(crate)
- `src/modules/mod.rs` - evm_cache module registration behind evm feature
- `Cargo.toml` - blake3 optional dep, added to evm feature gate

## Decisions Made
- LogCache opens its own Connection -- WAL mode supports concurrent connections to same file
- BLAKE3 chosen for filter key hashing (fast, pure Rust, 32-byte output)
- 0xFF sentinel for null topics prevents collision with real topic values
- configure_connection made pub(crate) rather than duplicating pragma setup
- block_timestamp not stored in cache (not available from original log in all cases)
- Used prepare() instead of prepare_cached() in merge query to avoid borrow scope issues

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed rusqlite borrow scope in record_scanned_range**
- **Found during:** Task 1 (compilation)
- **Issue:** prepare_cached borrow of transaction conflicted with tx.commit() move
- **Fix:** Used prepare() with explicit scope block to drop statement before commit
- **Files modified:** src/modules/evm_cache.rs
- **Verification:** Compiles and all tests pass
- **Committed in:** 10e4823 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor implementation detail. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- LogCache storage layer complete, ready for CachingProvider wrapper (Plan 02)
- All 158 tests passing (134 existing + 24 new)
- No feature regressions

---
*Phase: 06-evm-log-cache-with-range-tracking*
*Completed: 2026-03-10*
