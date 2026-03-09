---
phase: 05-evm-erc20-module-with-filter-pushdown
plan: 02
subsystem: erc20-module
tags: [erc20, evm, alloy, module-migration]

requires:
  - phase: 05-evm-erc20-module-with-filter-pushdown
    provides: "ERC20 module with Transfer, Erc20Machine, Erc20Codec, Erc20Source, Erc20Filter in src/modules/erc20.rs"
provides:
  - "Tests importing domain types from library module (not local definitions)"
  - "Example binary with zero local domain types, CLI-only"
affects: []

tech-stack:
  added: []
  patterns:
    - "required-features on [[test]] for feature-gated integration tests"

key-files:
  created: []
  modified:
    - tests/erc20_tests.rs
    - examples/erc20_tracker.rs
    - Cargo.toml

key-decisions:
  - "erc20_tests requires evm feature via [[test]] required-features — tests use library module types"
  - "Example uses Erc20Source with Erc20Filter::AllTransfers (matches prior AlloyErc20Source behavior)"

patterns-established:
  - "Feature-gated test pattern: [[test]] name + required-features in Cargo.toml"

requirements-completed: [MOD-05]

duration: 3min
completed: 2026-03-09
---

# Phase 5 Plan 2: Migrate Tests and Example to Library Module Summary

**Tests and example binary import ERC20 types from chainfold::modules::erc20, eliminating all duplicate domain type definitions**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-09T09:17:52Z
- **Completed:** 2026-03-09T09:21:15Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Removed 99 lines of duplicate domain types from tests/erc20_tests.rs
- Removed 280 lines of duplicate domain types and AlloyErc20Source from examples/erc20_tracker.rs
- All 134 tests pass (6 ERC20 integration + 128 core)
- Example compiles cleanly with --features evm

## Task Commits

Each task was committed atomically:

1. **Task 1: Migrate tests to import from library module** - `b575f6b` (feat)
2. **Task 2: Thin example binary to CLI-only** - `1cfc16b` (feat)

## Files Created/Modified
- `tests/erc20_tests.rs` - Removed local Transfer/Balances/Erc20Machine/Erc20Codec/TransferSerde/BalancesSerde; imports from chainfold::modules::erc20
- `examples/erc20_tracker.rs` - Removed all domain types + AlloyErc20Source + SourceErr + TRANSFER_TOPIC; imports from library module; uses Erc20Source
- `Cargo.toml` - Added [[test]] section with required-features = ["evm"] for erc20_tests

## Decisions Made
- Added `[[test]] required-features = ["evm"]` to Cargo.toml since erc20_tests now imports from the feature-gated modules::erc20
- Example uses `Erc20Source::new(provider, token_bytes, Erc20Filter::AllTransfers, ...)` matching the prior AlloyErc20Source behavior

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added [[test]] required-features for evm feature gate**
- **Found during:** Task 1 (Migrate tests)
- **Issue:** `chainfold::modules` is gated behind `evm` feature; tests importing from it fail without the feature
- **Fix:** Added `[[test]] name = "erc20_tests" required-features = ["evm"]` to Cargo.toml
- **Files modified:** Cargo.toml
- **Verification:** `cargo test --features evm --test erc20_tests` passes all 6 tests
- **Committed in:** b575f6b (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary for compilation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 5 complete: ERC20 module fully integrated with tests and example
- All domain types live in src/modules/erc20.rs; no duplicates anywhere
- 134 tests passing

---
*Phase: 05-evm-erc20-module-with-filter-pushdown*
*Completed: 2026-03-09*
