---
phase: 02-persistence-and-recovery
plan: "02"
subsystem: database
tags: [rust, storage, in-memory, block-dag, tdd]

requires:
  - phase: 02-01
    provides: "Storage trait, Block<C,E>, Codec trait, schema v2 — the types InMemoryStorage must implement"

provides:
  - "InMemoryStorage struct implementing the block DAG Storage trait"
  - "16 storage unit tests covering all trait methods"
  - "Test double usable by Plans 03 and 04 without filesystem I/O"

affects:
  - 02-03-sqlite
  - 02-04-recovery
  - 03-coordinator

tech-stack:
  added: []
  patterns:
    - "Arc<Mutex<Inner>> for shared, cloneable in-memory state"
    - "All-zeros hash as genesis sentinel (no-parent marker)"
    - "Walk backward → collect → reverse for forward chain ordering"

key-files:
  created:
    - "src/storage/memory.rs"
    - "tests/storage_tests.rs"
  modified:
    - "src/storage/mod.rs"

key-decisions:
  - "All-zeros [u8; 32] is the genesis sentinel — find_nearest_snapshot and collect_canonical_chain both stop on it"
  - "collect_canonical_chain walks backward then reverses — avoids prepend overhead, O(n) allocation"
  - "Block ownership in Inner.blocks is source-agnostic; canonical_tips and snapshots are keyed by source_id"

patterns-established:
  - "TDD: write failing tests first (storage_tests.rs), then implement to make them pass"
  - "Block-level dedup: contains_key check before insert, return false immediately on hit"

requirements-completed:
  - PERSIST-01
  - PERSIST-02
  - PERSIST-05
  - INGEST-04

duration: 2min
completed: 2026-03-05
---

# Phase 2 Plan 02: InMemoryStorage Summary

**Arc<Mutex<Inner>>-backed in-memory block DAG storage with 16 tests covering dedup, parent-pointer walks, chain collection, and source isolation**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-05T16:05:54Z
- **Completed:** 2026-03-05T16:07:56Z
- **Tasks:** 1
- **Files modified:** 3

## Accomplishments

- Rewrote memory.rs to implement the block DAG Storage trait (replacing old flat-event implementation)
- 16 unit tests in storage_tests.rs covering all 7 Storage trait methods plus INGEST-04 deduplication invariant
- Exported InMemoryStorage from storage::mod.rs; full test suite (48 tests) passes

## Task Commits

1. **Task 1: Implement InMemoryStorage and write storage tests** - `c85dda0` (feat)

**Plan metadata:** (docs commit below)

_Note: TDD — tests written first (RED), then implementation (GREEN). Type annotation fix was minor (vec![] inference)._

## Files Created/Modified

- `src/storage/memory.rs` - Full rewrite: InMemoryStorage implementing block DAG Storage trait
- `src/storage/mod.rs` - Added `pub mod memory` and `pub use memory::InMemoryStorage`
- `tests/storage_tests.rs` - 16 unit tests covering all trait methods, dedup, and source isolation

## Decisions Made

- **All-zeros genesis sentinel**: `[0u8; 32]` marks "no parent" — consistent with how the Storage trait docs describe genesis. find_nearest_snapshot and collect_canonical_chain both halt on it.
- **Block storage is source-agnostic**: `blocks: HashMap<[u8; 32], StoredBlock>` doesn't key by source. canonical_tips and snapshots are source-keyed. This matches the expected SqliteStorage schema where block_hash is a global key.
- **Backward walk + reverse for collect_canonical_chain**: Simpler than maintaining a deque or prepending; total allocations are still O(n).

## Deviations from Plan

**1. [Rule 1 - Bug] Fixed vec![] type inference in test**
- **Found during:** Task 1 (GREEN phase compile)
- **Issue:** `assert_eq!(chain, vec![])` — Rust couldn't infer `[u8; 32]` element type for the empty vec literal
- **Fix:** Changed to `Vec::<[u8; 32]>::new()`
- **Files modified:** tests/storage_tests.rs
- **Verification:** Compiles and test passes
- **Committed in:** c85dda0 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 - type annotation bug)
**Impact on plan:** Trivial fix; no scope change.

## Issues Encountered

None beyond the minor type annotation fix above.

## Next Phase Readiness

- InMemoryStorage is ready to use as the test double in Plan 02-03 (SqliteStorage tests use InMemoryStorage for comparison) and Plan 02-04 (Recovery tests).
- No blockers.

---
*Phase: 02-persistence-and-recovery*
*Completed: 2026-03-05*
