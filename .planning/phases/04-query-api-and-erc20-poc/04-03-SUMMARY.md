---
phase: 04-query-api-and-erc20-poc
plan: 03
subsystem: poc
tags: [erc20, alloy, state-machine, integration-test, example, sqlite]

# Dependency graph
requires:
  - phase: 04-query-api-and-erc20-poc plan 01
    provides: "Store trait, ReducedStore, Engine Arc<Storage>"
  - phase: 04-query-api-and-erc20-poc plan 02
    provides: "reconstruct_at, EventRangeIterator, load_events_by_height_range"
provides:
  - "ERC20 StateMachine (Transfer -> Balances) proof of concept"
  - "6 integration tests covering cold start, restart, reorg, store composition, OnChange, At()"
  - "Example binary with alloy provider, clap CLI, SqliteStorage"
affects: [future-evm-adapters, documentation]

# Tech tracking
tech-stack:
  added: [alloy 1.x, clap 4, hex 0.4]
  patterns: ["ERC20 Transfer decoding from EVM log topics", "serde_json Codec for PoC simplicity"]

key-files:
  created:
    - tests/erc20_tests.rs
    - examples/erc20_tracker.rs
  modified:
    - Cargo.toml

key-decisions:
  - "ERC20 StateMachine + Codec defined in test file, not library — PoC, not reusable library code"
  - "serde_json for Codec serialization — simplicity over performance for PoC"
  - "alloy 1.x with stable toolchain — project default is rustc 1.90, alloy 1.7+ needs 1.91; example builds with stable"
  - "SourceErr concrete error type — EventSource::Error must be Sized; Box<dyn Error> doesn't satisfy"
  - "No live subscription in example — fetch-only PoC; live mode deferred"

patterns-established:
  - "ERC20 Transfer decoding: topic[1]=from, topic[2]=to, data[0..32]=amount"
  - "Example binary pattern: CLI args, SqliteStorage, recover(), print balances"

requirements-completed: [POC-01]

# Metrics
duration: 8min
completed: 2026-03-07
---

# Phase 4 Plan 3: ERC20 Balance Tracker PoC Summary

**ERC20 Transfer state machine with 6 integration tests (mock source) and example binary using alloy provider against real Ethereum RPC**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-07T20:44:50Z
- **Completed:** 2026-03-07T20:52:48Z
- **Tasks:** 2 completed (+ 1 soft checkpoint for user verification)
- **Files modified:** 3

## Accomplishments
- ERC20 StateMachine reducing Transfer events into address balances, proven by 6 integration tests
- All 6 correctness scenarios verified with mock source: cold start, restart recovery, reorg handling, store composition, OnChange subscription, historical reconstruct_at
- Example binary compiles and demonstrates full stack with real Ethereum RPC via alloy provider
- 134 total tests passing (128 existing + 6 new ERC20 tests, zero regressions)

## Task Commits

Each task was committed atomically:

1. **Task 1: ERC20 StateMachine + integration tests** - `d59e55d` (feat)
2. **Task 2: ERC20 example binary with real RPC** - `6f8dc6e` (feat)

Task 3 (checkpoint:human-verify, soft gate) — user-optional verification of example binary with real RPC.

## Files Created/Modified
- `tests/erc20_tests.rs` - 6 integration tests: cold start, restart, reorg, store composition, OnChange, historical At()
- `examples/erc20_tracker.rs` - Example binary with alloy provider, clap CLI, SqliteStorage, cold/warm start detection
- `Cargo.toml` - Added dev-dependencies: alloy 1.x, clap 4, hex 0.4

## Decisions Made
- **ERC20 in test file, not library**: The state machine and codec are defined in the test file. This is PoC code demonstrating the chainfold stack, not reusable library code. Users implement their own StateMachine + Codec.
- **serde_json for Codec**: Simple and readable for PoC. Production users would use postcard or bincode for performance.
- **alloy 1.x requires stable toolchain**: Project default is rustc 1.90 but alloy 1.7+ needs 1.91+. Example builds with `rustup run stable cargo build --example erc20_tracker`. This is acceptable for a dev-dependency used only by the example binary.
- **SourceErr concrete type**: EventSource::Error requires Sized. A simple newtype wrapper around String was used instead of Box<dyn Error>.
- **No live subscription**: The example binary uses fetch-only mode (cold start + recovery). Live streaming via alloy's subscribe would require a WebSocket RPC endpoint, which is not universally available with free endpoints.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] alloy 1.x API changes from plan's alloy 0.12 spec**
- **Found during:** Task 2
- **Issue:** Plan specified alloy 0.12 API (on_http, etc.) but alloy is now at 1.x with different API (connect_http, different types)
- **Fix:** Updated to alloy 1.x API: ProviderBuilder::new().connect_http(), FixedBytes for topic, alloy::transports::http::reqwest::Url
- **Files modified:** examples/erc20_tracker.rs, Cargo.toml
- **Committed in:** 6f8dc6e

**2. [Rule 3 - Blocking] EventSource::Error must be Sized**
- **Found during:** Task 2
- **Issue:** Plan suggested Box<dyn Error> as error type, but EventSource::Error requires Sized
- **Fix:** Created concrete SourceErr(String) newtype implementing std::error::Error
- **Files modified:** examples/erc20_tracker.rs
- **Committed in:** 6f8dc6e

---

**Total deviations:** 2 auto-fixed (both blocking issues)
**Impact on plan:** Both fixes necessary for compilation. No scope change.

## Issues Encountered
- alloy sub-crates resolve to 1.7.3 via `^1.6.3` deps even when alloy is pinned to 1.6.3 — resolved by building with stable toolchain (1.94.0) which satisfies the rustc 1.91 requirement

## User Setup Required
To verify the example binary with a real Ethereum RPC:
```
rustup run stable cargo run --example erc20_tracker -- --rpc-url https://eth.llamarpc.com --from-block 19000000
```
Run twice to observe warm recovery from SQLite cache.

## Next Phase Readiness
- Phase 4 complete: all 3 plans executed
- Full chainfold stack proven end-to-end: Store trait, ReducedStore, reconstruct_at, EventRangeIterator, ERC20 PoC
- Ready for production adapters: EVM (alloy-based EventSource), CEX, etc.

---
*Phase: 04-query-api-and-erc20-poc*
*Completed: 2026-03-07*
