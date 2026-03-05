---
phase: 03-live-subscriptions-and-reorg-safety
plan: 01
subsystem: storage
tags: [rust, storage, trait, sqlite, in-memory, reorg, engine-config, tokio-util, futures-util]

# Dependency graph
requires:
  - phase: 02-persistence-and-recovery
    provides: "Storage trait, InMemoryStorage, SqliteStorage, Engine, EngineConfig, block DAG schema"
provides:
  - "Storage trait extended with get_parent_hash() and set_canonical_tip() methods"
  - "EngineError unified error type (Storage, Source, DecodeState, ReconnectExhausted, ReorgWithoutAncestor)"
  - "ReorgEvent struct carrying common_ancestor, old_tip, new_tip, depth"
  - "EngineStatus enum (Disconnected, Reconnecting(u32), Reconnected, GapFilling)"
  - "EngineConfig extended with max_retries, initial_delay_ms, max_delay_ms, batch_size; new() constructor"
  - "tokio-util and futures-util in main dependencies; tokio has time feature"
  - "Schema migration v3: canonical_tip FK removed so set_canonical_tip works before blocks are written"
affects:
  - "03-02-PLAN.md (reorg detection uses get_parent_hash, set_canonical_tip, EngineError)"
  - "03-03-PLAN.md (event loop uses EngineStatus, EngineError, tokio-util CancellationToken)"

# Tech tracking
tech-stack:
  added:
    - "tokio-util 0.7 (CancellationToken for Engine::run() cancellation)"
    - "futures-util 0.3 (promoted from dev-dep to main dep for ready_chunks in event loop)"
    - "tokio time feature (needed for reconnection backoff in 03-03)"
  patterns:
    - "Storage trait extension: new methods added to trait + both backends simultaneously"
    - "TDD red-green: tests written first, confirmed failing, then implementations added"
    - "Schema migration: additive v3 migration drops FK constraint via CREATE+INSERT+DROP+RENAME"
    - "EngineConfig::new() constructor pattern with sensible defaults; struct fields remain pub"

key-files:
  created: []
  modified:
    - "src/storage/mod.rs - Storage trait extended with get_parent_hash, set_canonical_tip"
    - "src/storage/memory.rs - InMemoryStorage implements both new methods"
    - "src/storage/sqlite.rs - SqliteStorage implements both new methods"
    - "src/storage/schema.rs - Migration v3: drop FK on canonical_tip.block_hash"
    - "src/coordinator/mod.rs - EngineError, ReorgEvent, EngineStatus, updated EngineConfig + ::new()"
    - "src/lib.rs - Re-exports EngineError, ReorgEvent, EngineStatus"
    - "Cargo.toml - tokio time, futures-util, tokio-util in main deps"
    - "tests/storage_tests.rs - 10 new tests for get_parent_hash and set_canonical_tip (both backends)"
    - "tests/coordinator_tests.rs - 6 new tests for new types; migrated to EngineConfig::new()"
    - "tests/recovery_tests.rs - Migrated to EngineConfig::new()"

key-decisions:
  - "tokio-util 0.7 has no 'sync' feature — CancellationToken is included by default; feature spec removed"
  - "Schema migration v3 drops FK on canonical_tip.block_hash — set_canonical_tip must work during reorg handling before new blocks are written; FK was preventing this insert case"
  - "EngineConfig keeps pub fields + adds new() constructor — existing code (non-test) uses field access; constructor provides ergonomic defaults"
  - "EngineError lives in coordinator (not error.rs) — it is Engine-scoped, while SourceError is EventSource-scoped"

patterns-established:
  - "Storage trait extension pattern: trait + both impls + tests updated atomically in one commit"
  - "EngineConfig::new() for test construction; struct literal allowed in non-test contexts for explicit field control"

requirements-completed:
  - SAFETY-01
  - INGEST-05

# Metrics
duration: 20min
completed: 2026-03-05
---

# Phase 3 Plan 01: Storage Contracts and Type Foundations Summary

**Storage trait extended with get_parent_hash/set_canonical_tip, plus EngineError/ReorgEvent/EngineStatus types and updated EngineConfig — the reorg-handling contracts both Plan 02 and Plan 03 depend on**

## Performance

- **Duration:** ~20 min
- **Started:** 2026-03-05T20:48:00Z
- **Completed:** 2026-03-05T21:08:00Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments

- Extended Storage trait with `get_parent_hash()` (returns parent of any stored block) and `set_canonical_tip()` (explicit canonical tip update for reorg rollback)
- Added 10 new storage tests covering both InMemoryStorage and SqliteStorage for the new methods
- Defined EngineError (5 variants, Send+Sync), ReorgEvent (4 fields), EngineStatus (4 variants, one carrying u32)
- Extended EngineConfig with 4 reconnection/batching fields; added `EngineConfig::new()` constructor with sensible defaults
- Promoted tokio-util and futures-util to main dependencies; added tokio time feature

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend Storage trait with get_parent_hash and set_canonical_tip** - `a7ad759` (feat)
2. **Task 2: Add new public types, update EngineConfig, update deps** - `edc3ff8` (feat)

**Plan metadata:** (created below)

_Note: Both tasks were TDD (red-green). Task 1 had one auto-fix (schema migration v3 for FK removal)._

## Files Created/Modified

- `src/storage/mod.rs` - Two new trait methods: get_parent_hash, set_canonical_tip
- `src/storage/memory.rs` - InMemoryStorage implements both new methods
- `src/storage/sqlite.rs` - SqliteStorage implements both new methods
- `src/storage/schema.rs` - Migration v3: drop FK on canonical_tip.block_hash
- `src/coordinator/mod.rs` - EngineError, ReorgEvent, EngineStatus types; updated EngineConfig
- `src/lib.rs` - Re-exports EngineError, ReorgEvent, EngineStatus
- `Cargo.toml` - tokio time feature, futures-util, tokio-util added to main deps
- `tests/storage_tests.rs` - 10 new tests (get_parent_hash + set_canonical_tip, both backends)
- `tests/coordinator_tests.rs` - 6 new type tests; migrated EngineConfig::new()
- `tests/recovery_tests.rs` - Migrated to EngineConfig::new()

## Decisions Made

- **tokio-util "sync" feature doesn't exist in 0.7** — CancellationToken is included by default; removed feature spec. Plan mentioned it but the feature name was wrong.
- **Schema migration v3 removes FK on canonical_tip.block_hash** — The plan required set_canonical_tip to work without a prior block in storage (insert case). The FK was preventing this. FK is not needed because canonical_tip is a derived index structure — semantic integrity is enforced by the engine, not the DB.
- **EngineError stays in coordinator/mod.rs** — it's Engine-scoped. SourceError stays in error.rs — it's EventSource-scoped. Clean separation.
- **EngineConfig::new() constructor added, pub fields preserved** — Tests and new code use `::new()`; existing code can still do explicit struct literals. Both modes work.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Schema migration v3: removed FK on canonical_tip.block_hash**
- **Found during:** Task 1 (Storage trait implementation)
- **Issue:** `test_sqlite_set_canonical_tip_creates_new` failed with `FOREIGN KEY constraint failed` because `canonical_tip.block_hash` references `blocks(hash)`. The plan specifies set_canonical_tip must work before any blocks exist.
- **Fix:** Added SQLite migration v3 that recreates `canonical_tip` without the FK constraint via CREATE+INSERT+DROP+RENAME pattern (safe for rusqlite_migration).
- **Files modified:** `src/storage/schema.rs`
- **Verification:** All 36 storage tests pass including `test_sqlite_set_canonical_tip_creates_new`
- **Committed in:** `a7ad759` (Task 1 commit)

**2. [Rule 3 - Blocking] tokio-util feature spec corrected**
- **Found during:** Task 2 (Cargo.toml update)
- **Issue:** Plan specified `features = ["sync"]` for tokio-util 0.7, but that feature does not exist. Cargo refused to resolve the dependency.
- **Fix:** Removed the features array — CancellationToken is available without any feature flag in tokio-util 0.7.
- **Files modified:** `Cargo.toml`
- **Verification:** `cargo test` passes with tokio-util resolved.
- **Committed in:** `edc3ff8` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 3 — blocking issues)
**Impact on plan:** Both fixes were necessary for correctness. No scope creep. The schema migration was semantically correct — canonical_tip FK was an over-constraint.

## Issues Encountered

None beyond the two auto-fixed blocking issues above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Storage trait contracts are complete: Plans 02 and 03 can use get_parent_hash, set_canonical_tip
- EngineError, ReorgEvent, EngineStatus are exported and available at crate root
- tokio-util CancellationToken is available for Engine::run() in Plan 03
- futures-util is in main deps for ready_chunks micro-batching in Plan 03
- All 89 tests pass (36 storage + 13 coordinator + 13 cursor + 5 event_source + 5 integration + 8 recovery + 9 reducer)

---
*Phase: 03-live-subscriptions-and-reorg-safety*
*Completed: 2026-03-05*
