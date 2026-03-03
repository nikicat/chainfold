---
phase: 02-persistence-and-recovery
plan: 01
subsystem: database
tags: [rust, sqlite, rusqlite, rusqlite_migration, wal-mode, event-sourcing, storage-trait]

# Dependency graph
requires:
  - phase: 01-foundations
    provides: ChainCursor supertrait, Event<C,E> envelope, StoredEvent/Anchor/Snapshot rely on same (i64,i64) cursor repr
provides:
  - Storage trait with sync methods (write_events, events_in_range, write_snapshot, latest_snapshot, snapshots_before, write_anchor, read_anchor, add_coverage, is_fully_cached, cached_tip)
  - StoredEvent, Snapshot, Anchor types with (i64, i64) cursor representation
  - SqliteStorage with WAL mode, UNIQUE dedup via INSERT OR IGNORE, interval merge in transactions
  - InMemoryStorage test double with identical semantics for recovery pipeline testing
  - SQL DDL schema with B-tree indexes for events, snapshots, anchors, coverage tables
affects:
  - 02-02 (coordinator/recovery pipeline depends on Storage trait and both implementations)
  - all subsequent plans in phase 02 (every plan uses StoredEvent, Snapshot, Anchor)

# Tech tracking
tech-stack:
  added:
    - rusqlite 0.34.0 (bundled SQLite, WAL mode, prepare_cached, unchecked_transaction)
    - rusqlite_migration 2.0.0 (schema migrations via M::up, Migrations::new, to_latest)
    - tempfile 3.26.0 (dev-dep for SqliteStorage tests with temp file cleanup)
  patterns:
    - Storage trait is sync; async callers bridge via tokio::task::spawn_blocking
    - Cursors stored as (i64, i64) tuple (major, minor) — coordinator maps ChainCursor types to this
    - INSERT OR IGNORE + changes() for dedup with accurate insert count
    - Interval merge algorithm: find overlapping intervals, delete, insert merged union
    - Mutex<Connection> for SqliteStorage Sync impl (rusqlite::Connection is not Sync)
    - Generic test helper fn run_storage_tests<S: Storage>() runs same tests on both backends

key-files:
  created:
    - src/storage/mod.rs (Storage trait definition + StoredEvent/Snapshot/Anchor types + re-exports)
    - src/storage/schema.rs (migrations() fn returning Migrations with DDL for all 4 tables)
    - src/storage/sqlite.rs (SqliteStorage implementing Storage, StorageError with thiserror)
    - src/storage/memory.rs (InMemoryStorage implementing Storage, MemoryStorageError)
    - tests/storage_tests.rs (5 tests: 3 type construction + 2 parameterized backend runners)
  modified:
    - Cargo.toml (added rusqlite 0.34 bundled, rusqlite_migration 2.0, tempfile 3 dev-dep)
    - src/lib.rs (added pub mod storage; re-exported Storage/StoredEvent/Snapshot/Anchor)

key-decisions:
  - "rusqlite 0.34 + rusqlite_migration 2.0 chosen over plan's suggested 1.3 — version conflict between rusqlite_migration 1.3 (requiring rusqlite ^0.32) and rusqlite 0.34 required upgrading migration crate to 2.0"
  - "Mutex<Connection> instead of raw Connection — rusqlite::Connection is not Sync; wrapping in Mutex satisfies Storage: Send + Sync requirement without any code complexity"
  - "CoverageInterval struct includes source_id field — required to correctly filter intervals per source in InMemoryStorage; without it cached_tip and is_fully_cached would mix intervals across sources"

patterns-established:
  - "Storage trait sync + spawn_blocking bridge: all DB methods sync, async coordinator uses tokio::task::spawn_blocking"
  - "Accurate insert count via changes(): call conn.changes() after each individual INSERT OR IGNORE, never use batch size as count"
  - "Interval merge pattern: SELECT overlapping (start <= to AND end >= from), collect IDs + compute merged bounds, DELETE all, INSERT one"
  - "Generic test helper: fn run_storage_tests<S: Storage>(s: S) parameterizes coverage over both SqliteStorage and InMemoryStorage"

requirements-completed: [PERSIST-01, PERSIST-02, PERSIST-05, INGEST-04]

# Metrics
duration: 6min
completed: 2026-03-03
---

# Phase 2 Plan 01: Storage Trait and SQLite/InMemory Implementations Summary

**SQLite-backed event/snapshot/anchor/coverage storage with WAL mode, UNIQUE dedup, interval merge, and an InMemoryStorage test double — all 40 tests passing**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-03T14:12:57Z
- **Completed:** 2026-03-03T14:18:21Z
- **Tasks:** 2 completed
- **Files modified:** 7

## Accomplishments

- Storage trait with 10 sync methods covering the full persistence API needed by the recovery pipeline
- SqliteStorage with WAL mode, UNIQUE constraint dedup (INSERT OR IGNORE), accurate insert count via changes(), and transactional interval merge — all correctness requirements from the locked decisions
- InMemoryStorage test double with identical semantics — coverage merge, dedup, source-scoped intervals — enables testing the coordinator and recovery pipeline without filesystem I/O
- 5 storage tests (3 type construction + 2 parameterized backend runners covering 11 behaviors)

## Task Commits

Each task was committed atomically:

1. **Task 1: Storage trait, types, schema, and dependencies** - `2c0aed9` (feat)
2. **Task 2: SqliteStorage and InMemoryStorage implementations with tests** - `be40bf9` (feat)

## Files Created/Modified

- `/home/nb/src/chainfold/src/storage/mod.rs` — Storage trait + StoredEvent/Snapshot/Anchor types, module re-exports
- `/home/nb/src/chainfold/src/storage/schema.rs` — migrations() returning SQL DDL for events/snapshots/anchors/coverage tables with indexes
- `/home/nb/src/chainfold/src/storage/sqlite.rs` — SqliteStorage with WAL, prepare_cached, unchecked_transaction, StorageError
- `/home/nb/src/chainfold/src/storage/memory.rs` — InMemoryStorage with Mutex<Inner>, CoverageInterval with source_id
- `/home/nb/src/chainfold/tests/storage_tests.rs` — 5 tests covering type construction and both storage backends
- `/home/nb/src/chainfold/Cargo.toml` — added rusqlite 0.34 bundled, rusqlite_migration 2.0, tempfile 3
- `/home/nb/src/chainfold/src/lib.rs` — added pub mod storage + re-exports

## Decisions Made

- **rusqlite_migration 2.0 instead of 1.3**: The plan specified rusqlite_migration 1.3 but it requires rusqlite ^0.32, conflicting with rusqlite 0.34 (both link to sqlite3 native lib). Upgraded to rusqlite_migration 2.0 which is compatible with rusqlite 0.34. API is identical (M::up, Migrations::new, to_latest).

- **Mutex<Connection> for SqliteStorage Sync**: rusqlite::Connection is Send but not Sync. The Storage trait requires Send + Sync + 'static. Wrapping in Mutex<Connection> is the standard pattern and adds no meaningful overhead (storage is not a hot path).

- **CoverageInterval stores source_id**: Initial implementation omitted source_id from the in-memory CoverageInterval struct, discovered during implementation that is_fully_cached and cached_tip would return incorrect results by mixing intervals across different sources. Fixed before first compilation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Upgraded rusqlite_migration 1.3 to 2.0 due to version conflict**
- **Found during:** Task 1 (cargo check after adding dependencies)
- **Issue:** rusqlite_migration 1.3 requires rusqlite ^0.32.1, but chainfold uses rusqlite 0.34.0. Both crates link libsqlite3-sys with different versions, causing "Only one package may specify the same links value" error.
- **Fix:** Changed rusqlite_migration = "1.3" to rusqlite_migration = "2" in Cargo.toml. API is identical.
- **Files modified:** Cargo.toml, Cargo.lock
- **Verification:** cargo check succeeds, no linker conflicts
- **Committed in:** 2c0aed9 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (Rule 3 - blocking version conflict)
**Impact on plan:** Minimal — same API surface, same behavior, just a newer patch of the migration crate. The plan's note about rusqlite 0.38 / tokio-rusqlite was a red herring; the actual conflict was between 0.34 and migration 1.3.

## Issues Encountered

- **Firejail sandbox**: Same noexec tmpfs constraint as Phase 1. Resolved using the pre-existing /tmp/cargo + /tmp/rustc-wrapper binaries from Phase 1. No impact on code.

## Next Phase Readiness

- Storage trait fully defined — Plan 02-02 (coordinator/recovery pipeline) can depend on Storage, SqliteStorage, and InMemoryStorage immediately
- InMemoryStorage is the recommended backend for all Phase 2 unit tests — avoids filesystem setup in test helpers
- Reminder: snapshot serialization should use `postcard` (not bincode) per STATE.md blocker

---
*Phase: 02-persistence-and-recovery*
*Completed: 2026-03-03*
