---
phase: 03-live-subscriptions-and-reorg-safety
plan: 03
subsystem: coordinator
tags: [tokio, futures, cancellation-token, exponential-backoff, gap-fill, micro-batching, live-stream]

requires:
  - phase: 03-02
    provides: process_or_reorg, find_common_ancestor, handle_reorg — reorg-aware block processing
  - phase: 02-04
    provides: fetch_gap_blocks pub(crate) — gap fill for reconnection

provides:
  - Engine::run() — long-lived async event loop consuming live EventSource streams
  - Exponential backoff reconnection with configurable max_retries
  - Inline gap fill: detects block.height > tip+1, fills via fetch_gap_blocks
  - EngineError::from_recovery() conversion helper
  - backoff_delay() free function: 2^attempt * base, capped at max
  - current_tip_height() helper: queries storage for canonical tip height
  - Cancellation safety: biased select! + is_cancelled() checks at loop entry points

affects: [04-evm-adapter, phase-04]

tech-stack:
  added: [tokio macros feature, tokio rt-multi-thread feature (moved from dev to main deps)]
  patterns:
    - "biased select!: cancellation branch checked before data branch in every select!"
    - "Pattern A test termination: max_retries=0, run() returns Err(ReconnectExhausted) after finite stream"
    - "Pattern B test termination: spawned watcher task cancels token — requires subscribe().await yield point"

key-files:
  created:
    - tests/live_engine_tests.rs
  modified:
    - src/coordinator/mod.rs
    - Cargo.toml

key-decisions:
  - "cancel.is_cancelled() checks at two points: top of outer loop AND inside subscribe Err branch — handles race between stream end and cancel token"
  - "source: &ES (borrow, not move) — allows multiple subscribe() calls across reconnect cycles and fetch_gap_blocks calls"
  - "Inline gap fill in batch processing loop: check block.height > tip_h + 1 before each block, call fetch_gap_blocks for the range"
  - "subscribe_from_height returns tip+1 or 0 for cold start — no +1 overflow because u64 starts at 0"
  - "tokio macros moved to main dependencies (not just dev-deps) — Engine::run() uses tokio::select!"
  - "Test Pattern A (natural exhaustion) preferred over Pattern B (watcher cancellation) — eliminates async timing races in single-threaded test executor"
  - "Mock subscribe() is sync-inside-async (no internal .await) — yields synchronously, making Pattern B unreliable without tokio::task::yield_now"

requirements-completed: [INGEST-01, INGEST-05, SAFETY-03]

duration: 35min
completed: 2026-03-05
---

# Phase 03 Plan 03: Live Event Loop Summary

**Engine::run() — long-lived async loop with biased cancellation, micro-batching via ready_chunks, exponential backoff reconnection, and inline gap fill using fetch_gap_blocks**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-03-05T20:53:00Z
- **Completed:** 2026-03-05T21:28:37Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Engine::run() takes `&ES` (borrow) allowing multiple subscribe() calls across reconnect cycles; processes blocks via process_or_reorg for transparent reorg handling
- Micro-batching via ready_chunks(batch_size) drains all immediately-available blocks before yielding to the select! loop
- Inline gap detection: before each block, if block.height > current_tip_height + 1, fires GapFilling status and calls fetch_gap_blocks; no separate post-reconnect step needed
- 8 integration tests covering: live stream consumption, pre-cancel, reconnect on stream end, max retries exhausted, gap fill, Disconnected/Reconnecting/Reconnected/GapFilling status callbacks, reorg during live stream

## Task Commits

1. **Task 1: Engine::run() event loop** - `b59428b` (feat)
2. **Task 2: Live engine integration tests** - `6dd62ad` (test)

## Files Created/Modified

- `src/coordinator/mod.rs` - Added Engine::run(), current_tip_height(), subscribe_from_height(), EngineError::from_recovery(), backoff_delay(); added futures_util/tokio_util/EventSource imports
- `tests/live_engine_tests.rs` - 685-line integration test file with MockStreamSource, FailOnceThenSucceedSource, 8 tests
- `Cargo.toml` - Added tokio `macros` and `rt-multi-thread` features to main dependencies (required by tokio::select! in lib code)

## Decisions Made

- **cancel.is_cancelled() checks at TWO points** — At the top of the outer loop (before subscribe attempt) AND in the subscribe Err branch (after subscribe returns failure). Required because tokio::select! biased only covers the inner batch loop; the outer reconnect path has no select! for the actual subscribe() call.
- **source: &ES borrow** — Taking source by reference (not move) allows: (1) multiple subscribe() calls across reconnect cycles, (2) passing to fetch_gap_blocks which also needs source. The alternative (Arc<ES>) would require API surface changes.
- **Inline gap fill per-block** — Rather than a post-reconnect gap fill step (which would require peeking at the first stream item), we check height continuity before each block in the batch loop. Simpler and handles gaps discovered mid-stream.
- **Test Pattern A preferred** — Using max_retries=0 to let run() return Err(ReconnectExhausted) naturally is more reliable than spawning a watcher task to cancel. The mock subscribe() resolves synchronously (no internal .await), so the watcher task may not get scheduled at the expected yield point.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Added cancellation checks at reconnect path entry points**
- **Found during:** Task 2 (integration tests)
- **Issue:** Plan's biased select! only covers the inner batch loop. When a stream ends naturally and the engine re-enters the outer loop to reconnect, the cancel token may have been triggered but is not checked until the next select! iteration (which never happens if subscribe fails immediately). Tests were returning Err(ReconnectExhausted) instead of Ok(()).
- **Fix:** Added `if cancel.is_cancelled() { return Ok(()); }` at (1) top of outer loop before subscribe attempt, and (2) inside the subscribe Err branch after subscribe returns failure.
- **Files modified:** src/coordinator/mod.rs
- **Verification:** test_cancellation_before_subscribe passes; cancel is now checked at all exit points
- **Committed in:** b59428b (Task 1 commit, fix applied inline during implementation refinement)

**2. [Rule 3 - Blocking] Added tokio macros + rt-multi-thread to main dependencies**
- **Found during:** Task 1 (cargo check)
- **Issue:** tokio::select! macro not available in lib code — tokio `macros` feature was only in dev-dependencies
- **Fix:** Added `macros` and `rt-multi-thread` to main tokio dependency in Cargo.toml
- **Files modified:** Cargo.toml
- **Verification:** cargo check passes with tokio::select! in coordinator/mod.rs
- **Committed in:** b59428b (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 bug/correctness, 1 blocking dependency)
**Impact on plan:** Both fixes necessary for correctness. The cancel check fix is a correctness improvement to Engine::run() itself (more responsive cancellation). The tokio feature fix was a blocking compilation error.

## Issues Encountered

- **Single-threaded tokio test executor and async timing**: Mock subscribe() is implemented as a simple async fn with no internal .await — it resolves synchronously. This means spawning a watcher task that calls token.cancel() during subscribe().await doesn't work reliably (the mock never actually yields to the scheduler). Resolved by using Pattern A (natural exhaustion via max_retries=0) for 7 of 8 tests; only test_gap_fill_on_reconnect originally used Pattern B but was also converted to Pattern A.

## Next Phase Readiness

- Engine::run() is the final piece of Phase 3 — the live subscription loop with reorg safety is complete
- Full pipeline: recover() → run() → process_or_reorg() handles cold start, crash recovery, live streaming, reorgs, disconnects, and gap fills
- 107 tests passing (all phases)
- Phase 4 (EVM adapter): can implement a real EventSource backed by ethers-rs/alloy that calls subscribe() for WebSocket and fetch_blocks() for RPC gap fill; Engine::run() is the consumer

---
*Phase: 03-live-subscriptions-and-reorg-safety*
*Completed: 2026-03-05*
