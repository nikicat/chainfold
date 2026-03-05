---
phase: 03-live-subscriptions-and-reorg-safety
plan: 02
subsystem: coordinator
tags: [reorg, dag, state-machine, snapshot, replay, block-hash, canonical-tip]

# Dependency graph
requires:
  - phase: 03-01
    provides: get_parent_hash, set_canonical_tip Storage methods, ReorgEvent, EngineError::ReorgWithoutAncestor types
  - phase: 02-03
    provides: Engine write-first pipeline, process_block, snapshot triggering
  - phase: 02-04
    provides: crash recovery pipeline, collect_canonical_chain, find_nearest_snapshot
provides:
  - process_or_reorg: reorg-aware block processing entry point
  - find_common_ancestor: O(depth) DAG walk to find fork point
  - handle_reorg: callback notification + snapshot-based state re-derivation
  - initial_state field on Engine for no-snapshot reorg recovery
  - fetch_gap_blocks is now pub(crate) for Engine::run() in Plan 03
  - Integration tests: 10 tests covering all reorg scenarios
affects:
  - 03-03-PLAN.md: Engine::run() uses process_or_reorg and fetch_gap_blocks

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Reorg detection via parent_hash != canonical_tip comparison"
    - "Common ancestor search: HashSet of old chain + backward walk of new chain"
    - "State re-derivation: find nearest snapshot + replay forward to fork point"
    - "Callback-before-state: on_reorg fires before any state mutation"
    - "Initial state preservation: stored at Engine::new() for no-snapshot reorg"
    - "DAG immutability: old branch blocks never deleted, only canonical_tip moves"

key-files:
  created:
    - tests/reorg_tests.rs
  modified:
    - src/coordinator/mod.rs
    - src/coordinator/recovery.rs

key-decisions:
  - "initial_state stored at Engine::new() from watch channel borrow — zero API change, captures construction-time state for no-snapshot reorg replay"
  - "process_or_reorg delegates to process_block for first block (no canonical tip) — correct, no special case needed"
  - "find_common_ancestor uses HashSet of old chain then backward walk of new chain — O(depth_old + depth_new), simple and correct for typical reorg depths of 1-5"
  - "handle_reorg fires on_reorg BEFORE state mutation — client sees old state context in callback, then state reflects new branch after return"
  - "Old branch blocks remain in DAG storage — no deletion ever; canonical_tip is the only mutable reference"
  - "fetch_gap_blocks made pub(crate) for Plan 03 Engine::run() reuse"

patterns-established:
  - "Reorg = parent_hash mismatch with canonical_tip; no reorg = normal extension"
  - "State re-derivation: load_snapshot + replay_chain OR reset_initial_state + replay_full_chain"
  - "set_canonical_tip(common_ancestor) before process_block — process_block then extends from fork point"

requirements-completed: [SAFETY-01, SAFETY-02]

# Metrics
duration: 4min
completed: 2026-03-05
---

# Phase 3 Plan 02: Reorg Detection and State Re-Derivation Summary

**Engine detects reorgs via parent_hash mismatch, finds fork point with O(depth) DAG walk, re-derives state via snapshot+replay, fires on_reorg callback before state changes — 10 integration tests cover all reorg scenarios**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-05T00:12:33Z
- **Completed:** 2026-03-05T00:16:17Z
- **Tasks:** 2 (Task 1: implementation, Task 2: tests — executed together as TDD cycle)
- **Files modified:** 3

## Accomplishments

- Implemented `process_or_reorg` as the reorg-aware block processing entry point — detects reorgs by comparing incoming block's `parent_hash` to current canonical tip
- Implemented `find_common_ancestor` using a HashSet of old chain hashes + backward walk of new chain — O(depth_old + depth_new), handles genesis sentinel correctly
- Implemented `handle_reorg` orchestrating: callback notification BEFORE state changes, nearest-snapshot + replay re-derivation, canonical_tip reset to fork point
- Added `initial_state: SM::State` field to Engine, captured at construction via `core.subscribe().borrow().clone()` — enables no-snapshot reorg recovery from genesis
- Made `fetch_gap_blocks` pub(crate) for Plan 03 Engine::run() access
- 10 integration tests: single-block reorg, multi-block reorg (depth=3), no-reorg normal extension, ReorgEvent field correctness, callback ordering, state determinism, snapshot fast-path, no-snapshot full replay, old branch preservation, first-block edge case
- Full test suite: 99 tests passing (added 10 new, 89 existing preserved)

## Task Commits

Each task was committed atomically:

1. **Task 1 (TDD RED): Failing reorg tests** - `e0aa980` (test)
2. **Task 1 (TDD GREEN): Engine reorg implementation** - `799486b` (feat)

_Note: Tasks 1 and 2 were implemented as a single TDD cycle — tests written first (RED), then implementation (GREEN). Both tasks complete._

## Files Created/Modified

- `tests/reorg_tests.rs` - 513-line integration test suite covering all reorg scenarios (10 tests)
- `src/coordinator/mod.rs` - Added `initial_state` field, `process_or_reorg`, `find_common_ancestor`, `handle_reorg`, `replay_chain_for_reorg`, `replay_block_events_for_reorg` methods
- `src/coordinator/recovery.rs` - Made `fetch_gap_blocks` pub(crate)

## Decisions Made

- **initial_state stored at construction**: `core.subscribe().borrow().clone()` captures the initial state from the ReducerCore's watch channel. Zero API change — state is already in the channel at construction time. This enables no-snapshot reorg recovery without storing initial state separately.

- **process_or_reorg for first block**: When no canonical tip exists (first block ever), parent_hash comparison is skipped entirely and we just call `process_block`. Correct — first block cannot be a reorg.

- **find_common_ancestor algorithm**: Build HashSet of old chain, walk new chain backward until intersection. O(depth_old + depth_new). Simple and deterministic. Returns `ReorgWithoutAncestor` only when genesis sentinel is reached without finding a match.

- **Callback-before-state invariant**: `on_reorg` is called with the ReorgEvent (containing old_tip, new_tip, depth) BEFORE any state re-derivation. Client code in the callback sees the pre-reorg state. After `process_or_reorg` returns, state reflects the new canonical branch.

## Deviations from Plan

None — plan executed exactly as written. The `initial_state` field design (noted as a potential decision in the plan's action spec) was implemented exactly as the plan suggested: capture from `core.subscribe().borrow().clone()` at construction, zero API change.

## Issues Encountered

None — first compilation passed, all 10 tests passed on first run.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Engine reorg detection is complete and tested
- `process_or_reorg` is the correct entry point for all block processing in Plan 03's `Engine::run()` live subscription loop
- `fetch_gap_blocks` is pub(crate) and ready for use in `Engine::run()` gap-fill phase
- 99 tests passing across all phases — no regressions

---
*Phase: 03-live-subscriptions-and-reorg-safety*
*Completed: 2026-03-05*
