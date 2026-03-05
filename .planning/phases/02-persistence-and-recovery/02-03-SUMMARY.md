---
phase: 02-persistence-and-recovery
plan: 03
subsystem: database
tags: [sqlite, rusqlite, block-dag, storage, engine, coordinator, wal, tdd]

# Dependency graph
requires:
  - phase: 02-persistence-and-recovery plan 01
    provides: Block<C,E> struct, Codec trait, Storage trait definition, schema.rs v2 migrations
  - phase: 02-persistence-and-recovery plan 02
    provides: InMemoryStorage (reference implementation to match semantics)
  - phase: 01-foundations
    provides: ReducerCore, StateMachine, Event<C,E>, ChainCursor, postcard serialization decision

provides:
  - SqliteStorage: production block DAG storage backed by SQLite WAL
  - Engine<SM,S,C>: block-oriented coordinator with write-first pipeline and snapshot triggering
  - EngineConfig: configuration for source_id and snapshot_every_n

affects:
  - 02-04 (recovery pipeline uses Engine::replace_state, Engine::replay_events, storage::find_nearest_snapshot)
  - phase-03 (EVM integration instantiates Engine with SqliteStorage)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - write-first invariant: storage written atomically before state advance
    - block-level dedup: EXISTS check before any write, return false if duplicate
    - snapshot counter resets only on new blocks (not duplicates)
    - unchecked_transaction for all multi-step SQLite writes
    - BLOB(32) for block hashes (not TEXT)
    - postcard for cursor bytes, Codec::encode_event for payload bytes

key-files:
  created:
    - src/storage/sqlite.rs
    - tests/coordinator_tests.rs
  modified:
    - src/storage/mod.rs
    - src/coordinator/mod.rs
    - src/lib.rs
    - tests/storage_tests.rs

key-decisions:
  - "unchecked_transaction used for multi-step SQLite writes (block + events + canonical_tip in one atomic TX)"
  - "EXISTS check before transaction: avoids opening TX for duplicate blocks, consistent with InMemoryStorage semantics"
  - "INSERT OR REPLACE for snapshots: (source_id, block_hash) PK means snapshot at same block is idempotent"
  - "Engine blocks_since_snapshot resets to 0 after each snapshot, not after each N-block window"

patterns-established:
  - "SqliteStorage pattern: Mutex<Connection> for Send+Sync, prepare_cached for hot queries"
  - "TDD for storage implementations: write tests first, then implement to pass"

requirements-completed: [PERSIST-01, PERSIST-02, PERSIST-04, PERSIST-05, INGEST-04]

# Metrics
duration: 4min
completed: 2026-03-05
---

# Phase 2 Plan 03: SqliteStorage + Engine Coordinator Summary

**SqliteStorage (WAL, block DAG schema) and Engine coordinator (write-first pipeline, snapshot-every-N) with 33 tests across storage and coordinator suites**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-05T16:10:59Z
- **Completed:** 2026-03-05T16:14:52Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- SqliteStorage implements Storage trait with block DAG schema, WAL mode, and atomic transactions
- Engine coordinator implements write-first pipeline: encode -> write -> apply -> snapshot
- 10 new SqliteStorage tests (storage_tests.rs) + 7 Engine tests (coordinator_tests.rs) all pass
- Full test suite: 65 tests pass (cursor, event_source, reducer, integration, storage, coordinator)

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement SqliteStorage with block DAG schema** - `e0217cf` (feat)
2. **Task 2: Implement Engine coordinator with block-oriented processing** - `c722f09` (feat)

## Files Created/Modified
- `src/storage/sqlite.rs` - Rewritten: SqliteStorage implementing block DAG Storage trait with WAL, unchecked_transaction, EXISTS dedup check
- `src/storage/mod.rs` - Added `pub mod sqlite` + `pub use sqlite::SqliteStorage`
- `src/coordinator/mod.rs` - Added Engine<SM,S,C>, EngineConfig; kept Codec trait; added recovery helpers
- `src/lib.rs` - Added `Engine` + `EngineConfig` to re-exports
- `tests/storage_tests.rs` - Added 10 SqliteStorage-specific tests (all pass)
- `tests/coordinator_tests.rs` - Created with 7 Engine tests covering write-first, dedup, snapshots, subscribe

## Decisions Made
- `unchecked_transaction` for write_block: all three writes (block + events + canonical_tip) must be atomic; unchecked_transaction works correctly because we hold the mutex for the duration
- EXISTS check before opening transaction: avoids TX overhead for duplicate blocks, matches InMemoryStorage behavior (early return on duplicate)
- `INSERT OR REPLACE INTO snapshots`: snapshot at a given (source_id, block_hash) is idempotent (same PK just overwrites), which is correct for recovery
- Engine snapshot counter resets to 0 after each trigger, counting fresh N blocks until next snapshot — rolling window, not global

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Minor: `let mut rx` warning in coordinator_tests.rs (watch::Receiver borrow doesn't require mut) — fixed inline before commit.

## Next Phase Readiness
- Engine::replace_state() and Engine::replay_events() are implemented and ready for the recovery pipeline (Plan 02-04)
- SqliteStorage::find_nearest_snapshot() and collect_canonical_chain() are tested and ready for recovery use
- All storage and coordinator abstractions are in place; Plan 02-04 can implement recovery on top

---
*Phase: 02-persistence-and-recovery*
*Completed: 2026-03-05*
