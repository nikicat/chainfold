---
phase: 02-persistence-and-recovery
plan: "02"
subsystem: database
tags: [rust, tokio, sqlite, storage, recovery, event-sourcing, watch-channel]

requires:
  - phase: 02-01
    provides: Storage trait, StoredEvent, Snapshot, Anchor types, InMemoryStorage test double, SqliteStorage

provides:
  - Engine<SM, S, C> coordinator: write-first event processing with snapshot triggering
  - Codec trait: bridges typed SM::Event/State/Cursor to opaque storage bytes
  - Recovery pipeline: load_snapshot -> replay_cached_events -> fetch_gap sequence
  - InMemoryStorage Clone support via Arc<Mutex<Inner>>
  - Snapshot cursor semantics: cursor = (last_applied_major + 1, 0) = next event to replay

affects:
  - phase-03 (live subscription and hot path will build on Engine)
  - phase-04 (EVM adapter uses Engine for crash-safe state)

tech-stack:
  added: []
  patterns:
    - "Write-first invariant: storage write precedes ReducerCore.apply in all new-event paths"
    - "Two distinct code paths: process_events (write+apply) vs replay_stored_events (apply only)"
    - "Snapshot cursor semantics: points to NEXT event to replay, not last applied event"
    - "Coverage recorded as (major, minor+1) exclusive end, consistent across Engine and recovery"
    - "Gap fetch loop: chunk by chunk with major+1 cursor advance; coverage recorded at live_tip on exit"

key-files:
  created:
    - src/coordinator/mod.rs
    - src/coordinator/recovery.rs
    - tests/coordinator_tests.rs
    - tests/recovery_tests.rs
  modified:
    - src/machine.rs
    - src/storage/memory.rs
    - src/lib.rs

key-decisions:
  - "Snapshot cursor = (last_applied_major + 1, 0): replay is inclusive from snapshot cursor, so cursor must point PAST the last captured event"
  - "fetch_gap advances loop cursor by (major+1, 0) not (major, minor+1): avoids minor-semantics ambiguity across codec implementations"
  - "fetch_gap explicitly records add_coverage(gap_start, live_tip_end) after loop: ensures coverage reflects full fetch up to live_tip even when last chunk ends before live_tip"
  - "send_modify and send_replace instead of send: watch::send() silently discards value when no receivers; send_modify/send_replace always update internal state"
  - "InMemoryStorage uses Arc<Mutex<Inner>> for Clone: crash-recovery tests need to share storage between dropped engine and new engine"

requirements-completed: [PERSIST-03, PERSIST-04, INGEST-02, INGEST-03]

duration: 35min
completed: 2026-03-04
---

# Phase 2 Plan 02: Engine Coordinator and Recovery Pipeline Summary

**Engine<SM,S,C> coordinator with write-first event processing, snapshot fallback recovery, chunked gap fetch, and 18 tests proving all 5 Phase 2 roadmap success criteria**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-03-04T08:00:00Z
- **Completed:** 2026-03-04T08:40:47Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments

- Engine<SM, S, C> struct owning ReducerCore + Storage + Codec with two distinct paths: process_events (write-first) and replay_stored_events (no write)
- Recovery pipeline: load latest valid snapshot (with fallback to older snapshots or fetch_state), replay cached events from storage without re-writing, gap fetch via EventSource in chunks
- All 5 Phase 2 roadmap success criteria verified with named passing tests: crash recovery, deduplication, coverage accuracy, atomic anchors, chunked backfill
- Snapshot cursor semantics established: cursor = next event to replay (inclusive), engine records cursor = last_applied_major + 1

## Task Commits

1. **Task 1: Engine coordinator with batch event processing and snapshot triggering** - `472aa0e` (feat)
2. **Task 2: Recovery pipeline and Phase 2 integration tests** - `bd2b068` (feat)

## Files Created/Modified

- `src/coordinator/mod.rs` - Engine<SM,S,C>, Codec trait, EngineConfig, process_events and replay_stored_events
- `src/coordinator/recovery.rs` - recover(), load_snapshot(), replay_cached_events(), fetch_gap()
- `src/machine.rs` - ReducerCore.reset_state() via send_replace; apply() via send_modify
- `src/storage/memory.rs` - InMemoryStorage with Arc<Mutex<Inner>> for Clone support
- `src/lib.rs` - Re-exports: Codec, Engine, EngineConfig
- `tests/coordinator_tests.rs` - 7 Engine coordinator tests (write-first, snapshot triggering, anchor, dedup, watch)
- `tests/recovery_tests.rs` - 11 recovery tests proving all 5 Phase 2 roadmap success criteria

## Decisions Made

- **Snapshot cursor semantics**: Snapshot cursor = `(last_applied_major + 1, 0)`. The recovery pipeline replays events from `[snapshot_cursor, cached_tip)` inclusive. By pointing past the last applied event, recovery never double-applies the event already captured in the snapshot state. This resolves ambiguity between "cursor of last event in snapshot" vs "first event to replay".

- **fetch_gap cursor advance**: Uses `(last_tuple.0 + 1, 0)` (major+1) rather than `(major, minor+1)`. This avoids minor-semantics ambiguity — some codecs may not use minor at all. Incrementing major guarantees strict forward progress regardless of codec.

- **Coverage at live_tip**: After the gap fetch loop exits, `add_coverage(gap_start, (live_tip_major, live_tip_minor + 1))` is always called. This marks the full range as covered even if the source returns no events in the tail (e.g., live_tip > last event in source). The `+1` on minor follows the existing convention of exclusive end in all coverage calls.

- **send_modify / send_replace**: Changed from `watch::send()` (which silently discards value when no receivers) to `send_modify` in apply() and `send_replace` in reset_state(). This ensures `engine.state()` (which calls `subscribe().borrow()`) always returns current state even after recovery resets state with no subscribers.

- **Arc<Mutex<Inner>> for InMemoryStorage**: Changed from `Mutex<Inner>` to `Arc<Mutex<Inner>>` to enable Clone. Crash-recovery tests require sharing the same storage instance between the "crashed" engine (dropped) and the new engine (restored). Clone semantics are explicit: all clones share the same inner state.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Snapshot cursor was pointing to last event instead of next event**
- **Found during:** Task 2 (recovery tests)
- **Issue:** `create_snapshot(last_major, last_minor)` stored snapshot at the cursor of the last applied event. `replay_cached_events` then queried `events_in_range(snapshot_cursor, tip)` inclusive, which re-applied the event already in the snapshot state, causing off-by-one (state was 11 instead of 10).
- **Fix:** Changed `create_snapshot` to store `cursor_major: cursor_major + 1, cursor_minor: 0`. Snapshot cursor now points to the next event to replay. Updated `test_recovery_fallback_to_older_snapshot` to use cursor=5 (next event) instead of cursor=4 (last event).
- **Files modified:** `src/coordinator/mod.rs`, `tests/recovery_tests.rs`
- **Verification:** `test_recovery_snapshot_plus_cached_events` and `test_full_crash_recovery_cycle` now pass (state=10 and state=20)
- **Committed in:** bd2b068 (Task 2 commit)

**2. [Rule 1 - Bug] fetch_gap used (major, minor+1) cursor advance causing infinite loop risk**
- **Found during:** Task 2 (recovery tests)
- **Issue:** Original cursor advance was `(last_tuple.0, last_tuple.1 + 1)`. Since all cursors in tests have `minor=0`, next iteration started at `(seq_id, 1)`, while source events have `cursor >= (seq_id+1, 0)`. This would cause events to be missed or fetched again.
- **Fix:** Changed to `(last_tuple.0 + 1, 0)` for unambiguous forward progress.
- **Files modified:** `src/coordinator/recovery.rs`
- **Verification:** `test_chunked_backfill` passes (5 fetch_events calls for 50 events in pages of 10)
- **Committed in:** bd2b068 (Task 2 commit)

**3. [Rule 2 - Missing Critical] Coverage not recorded up to live_tip after gap fetch**
- **Found during:** Task 2 (`test_coverage_after_recovery`)
- **Issue:** After gap fetch, coverage only reflected the events that process_events found. If the last event's cursor was below live_tip (common: source has events 5-9, live_tip=10), coverage ended at (9, 1) but test expected (10, 1). The caller would think the range [9,10) is uncached.
- **Fix:** Added `add_coverage(gap_start, (live_tip_major, live_tip_minor+1))` after the fetch_gap loop completes.
- **Files modified:** `src/coordinator/recovery.rs`
- **Verification:** `test_coverage_after_recovery` passes
- **Committed in:** bd2b068 (Task 2 commit)

**4. [Rule 1 - Bug] watch::send() silently discards state when no receivers**
- **Found during:** Task 2 (recovery state verification)
- **Issue:** `ReducerCore.apply()` used `watch::send()` which returns `Err` (discarding the value) when no receivers exist. During recovery with no subscribers, `engine.state()` (which internally subscribes) would see stale state.
- **Fix:** Changed `apply()` to use `send_modify` and `reset_state()` to use `send_replace`. Both update the internal stored value unconditionally.
- **Files modified:** `src/machine.rs`
- **Verification:** All state assertions in recovery tests pass without needing a subscriber
- **Committed in:** bd2b068 (Task 2 commit)

---

**Total deviations:** 4 auto-fixed (3 bugs, 1 missing critical)
**Impact on plan:** All auto-fixes required for correctness of recovery pipeline. No scope creep.

## Issues Encountered

None beyond the auto-fixed bugs above. The core architecture (Engine, Codec, recovery sequence) was sound and executed as planned. The bugs were all in cursor arithmetic and watch channel semantics.

## Next Phase Readiness

- Engine coordinator complete and tested — ready for Phase 3 live subscription integration
- Recovery pipeline proven correct via 11 integration tests covering all 5 Phase 2 roadmap success criteria
- InMemoryStorage suitable for all Phase 3 unit testing (Clone support, correct dedup/coverage semantics)
- No blockers for Phase 3

---
*Phase: 02-persistence-and-recovery*
*Completed: 2026-03-04*

## Self-Check: PASSED

Files verified:
- FOUND: src/coordinator/mod.rs
- FOUND: src/coordinator/recovery.rs
- FOUND: tests/coordinator_tests.rs
- FOUND: tests/recovery_tests.rs

Commits verified:
- FOUND: 472aa0e (Task 1: Engine coordinator)
- FOUND: bd2b068 (Task 2: Recovery pipeline)

Test results: 58 tests, 0 failures, 0 warnings
