---
phase: 02-persistence-and-recovery
plan: 01
subsystem: database
tags: [rust, block-dag, storage, event-sourcing, traits, rusqlite, sqlite, migration]

requires:
  - phase: 01-foundations
    provides: ChainCursor, EventSource trait, StateMachine, ReducerCore, Event<C,E>

provides:
  - Block<C, E> type with block_hash, parent_hash, height, events
  - Event<C, E> simplified (no block_hash field — block context on Block)
  - EventSource trait updated to fetch_blocks/subscribe returning Block
  - Storage trait with block DAG API (write_block, get_canonical_tip, etc.)
  - Codec trait simplified (encode/decode event payloads and state; no cursor_to_tuple)
  - SQLite schema migration v2 (blocks, events, snapshots, canonical_tip tables)

affects:
  - 02-02 (InMemoryStorage implementation builds on new Storage trait)
  - 02-03 (SqliteStorage implementation uses schema v2)
  - 02-04 (Recovery pipeline uses new Storage methods and Codec)

tech-stack:
  added: []
  patterns:
    - "Block-DAG model: blocks are primary units of storage; events are block-scoped"
    - "Cursor stored as postcard bytes internally; Codec encodes only event payloads and state"
    - "Storage trait is sync; callers in async context use spawn_blocking"
    - "Schema migration chain preserved: v1 kept for continuity, v2 replaces all v1 tables"

key-files:
  created:
    - src/block.rs
  modified:
    - src/event.rs
    - src/source.rs
    - src/storage/mod.rs
    - src/storage/schema.rs
    - src/coordinator/mod.rs
    - src/coordinator/recovery.rs
    - src/lib.rs
    - tests/event_source_tests.rs
    - tests/integration_tests.rs
    - tests/reducer_tests.rs

key-decisions:
  - "Block<C,E> wraps Vec<Event<C,E>> — block is the storage unit, event is block-scoped"
  - "Event<C,E> loses block_hash field — block_hash lives exclusively on Block"
  - "EventSource::fetch_blocks(from_height, to_height) replaces fetch_events(from, to)"
  - "Codec encodes event payload only (Self::Event), not full Event<C,E>; cursor serialized internally via postcard"
  - "Schema v2 drops coverage/anchors tables (no longer needed in block DAG model)"
  - "canonical_tip table: one row per source_id tracking the canonical head block_hash"

patterns-established:
  - "Block DAG: each block has hash + parent_hash; canonical chain is tracked via canonical_tip"
  - "Storage::write_block is the single write path; deduplication is at block granularity"
  - "Recovery uses collect_canonical_chain then load_block_events to replay without re-fetching"

requirements-completed: [PERSIST-01, PERSIST-02, PERSIST-05, INGEST-04]

duration: 4min
completed: 2026-03-05
---

# Phase 2 Plan 1: Block DAG Type Foundations Summary

**Block<C,E> type + simplified Event<C,E> + block-oriented EventSource + block DAG Storage/Codec traits + SQLite migration v2 establishing the complete API surface for block DAG persistence**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-05T15:57:57Z
- **Completed:** 2026-03-05T16:02:00Z
- **Tasks:** 2
- **Files modified:** 11 (1 created, 7 modified, 3 deleted)

## Accomplishments

- Created `Block<C, E>` as the primary storage unit with block_hash, parent_hash, height, events
- Removed `block_hash` from `Event<C, E>` — block context is now exclusively on Block
- Replaced `EventSource::fetch_events(cursor, cursor)` with `fetch_blocks(from_height, to_height)` and Block-oriented subscribe
- Defined new block DAG `Storage` trait: write_block, get_canonical_tip, get_block_height, write_snapshot, find_nearest_snapshot, collect_canonical_chain, load_block_events
- Defined simplified `Codec` trait: encode/decode event payloads and state only (no cursor_to_tuple/tuple_to_cursor)
- Added SQLite migration v2 (drops coverage/anchors/old events/snapshots; creates blocks, events, snapshots, canonical_tip)
- All 32 tests pass (13 cursor, 5 event_source, 5 integration, 9 reducer)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create Block type and modify Event and EventSource** - `dbd667e` (feat)
2. **Task 2: Define new Storage trait, Codec trait, and schema migration** - `0db4985` (feat)

## Files Created/Modified

- `/home/nb/src/chainfold/src/block.rs` - New Block<C, E> type
- `/home/nb/src/chainfold/src/event.rs` - Removed block_hash field from Event<C, E>
- `/home/nb/src/chainfold/src/source.rs` - EventSource now fetch_blocks/Block-oriented subscribe
- `/home/nb/src/chainfold/src/storage/mod.rs` - New block DAG Storage trait
- `/home/nb/src/chainfold/src/storage/schema.rs` - v1 preserved + v2 block DAG schema
- `/home/nb/src/chainfold/src/coordinator/mod.rs` - New simplified Codec trait
- `/home/nb/src/chainfold/src/coordinator/recovery.rs` - Stubbed (implemented in 02-04)
- `/home/nb/src/chainfold/src/lib.rs` - Updated re-exports (Block, Codec, Storage)
- `/home/nb/src/chainfold/tests/event_source_tests.rs` - Rewritten with MockBlockSource
- `/home/nb/src/chainfold/tests/integration_tests.rs` - Updated for new Event/EventSource API
- `/home/nb/src/chainfold/tests/reducer_tests.rs` - Removed block_hash from Event construction
- Deleted: coordinator_tests.rs, recovery_tests.rs, storage_tests.rs (old API tests)

## Decisions Made

- Block<C,E> wraps Vec<Event<C,E>> — block is the storage unit, events are block-scoped. Matches EVM model naturally and supports reorgs by branch-switching at the block level.
- Codec operates on event payload (Self::Event), not full Event<C,E>. Cursor bytes are managed internally by the library (postcard). This eliminates cursor_to_tuple/tuple_to_cursor, which were the source of complexity in the old Codec.
- Schema v2 drops the coverage and anchors tables entirely. Block DAG recovery walks parent pointers instead of tracking coverage intervals.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed reducer_tests.rs and integration_tests.rs referencing old Event.block_hash field**
- **Found during:** Task 1 (running cargo test after initial changes)
- **Issue:** reducer_tests.rs::make_event and integration_tests.rs::evm_event still included `block_hash: None` which no longer exists on Event<C,E>
- **Fix:** Removed the `block_hash: None` field from both test helpers; removed unused `make_block_i128` helper
- **Files modified:** tests/reducer_tests.rs, tests/integration_tests.rs
- **Verification:** cargo test passes with all 32 tests green
- **Committed in:** dbd667e (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (Rule 3 - blocking compilation failure in pre-existing test files)
**Impact on plan:** Necessary fix — the plan specified updating reducer_tests.rs was in scope (it references Event<C,E>). No scope creep.

## Issues Encountered

None beyond the auto-fixed deviation above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- All type definitions and trait signatures are stable and compilable
- Plan 02-02 (InMemoryStorage) can implement `Storage` trait immediately
- Plan 02-03 (SqliteStorage + Engine) can implement `Storage` trait and run schema v2 migrations
- Plan 02-04 (Recovery) can build on `collect_canonical_chain`, `load_block_events`, `find_nearest_snapshot`
- No blockers

## Self-Check: PASSED

- All key files confirmed present on disk
- Commits dbd667e and 0db4985 confirmed in git log
- 32 tests pass across cursor_tests (13), event_source_tests (5), integration_tests (5), reducer_tests (9)

---
*Phase: 02-persistence-and-recovery*
*Completed: 2026-03-05*
