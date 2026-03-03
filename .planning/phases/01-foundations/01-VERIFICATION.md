---
phase: 01-foundations
verified: 2026-03-03T02:27:29Z
status: passed
score: 11/11 must-haves verified
re_verification: false
---

# Phase 1: Foundations Verification Report

**Phase Goal:** The core type system exists — cursor ordering, source abstraction, and reducer contract — so every subsequent component can be built against a stable API
**Verified:** 2026-03-03T02:27:29Z
**Status:** PASSED
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths (from ROADMAP.md Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A caller can implement the `EventSource` trait for a mock chain and pass it to library code without knowing about EVM or Bitcoin specifics | VERIFIED | `core_01_event_source_trait_abstraction` test passes; `fetch_from_source<S: EventSource>` generic fn in integration_tests.rs proves trait abstraction |
| 2 | A caller can implement the `StateMachine` trait with a pure `Apply` reducer and have the library drive state forward via `apply(state, event)` without any I/O | VERIFIED | `core_02_state_machine_pure_reducer` test passes; `apply()` is sync with no async, no I/O in signature or impl |
| 3 | Cursors from two different positions can be compared with `Ord` and serialized round-trip without loss | VERIFIED | `core_03_cursor_ord_and_serde_roundtrip` test passes; 4 serde roundtrip tests in cursor_tests.rs pass; CexCursor seq_id-only ordering verified |
| 4 | An in-memory reducer accumulates events correctly in the order they are applied, producing a deterministic final state verifiable by a unit test | VERIFIED | `core_04_reducer_accumulation_determinism` test passes; two independent ReducerCore instances produce identical state from same event sequence |
| 5 | The library compiles with the generic type parameter `C: ChainCursor` propagated through the type system without requiring runtime dispatch | VERIFIED | `core_05_generic_type_propagation_no_runtime_dispatch` test passes; `Pipeline<S: StateMachine>` struct and `apply_sequence<S: StateMachine>` fn use zero `dyn` or `Box`; machine.rs contains no `Box` or `dyn` |

**Score: 5/5 success criteria verified**

---

## Must-Have Truths (from Plan Frontmatter)

### Plan 01-01 Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | EvmCursor, BtcCursor, CexCursor all implement Ord + Clone + Serialize + Deserialize via the ChainCursor supertrait | VERIFIED | `cursor_types_satisfy_chain_cursor_bounds` compile-time test passes; blanket impl in cursor/mod.rs confirms auto-satisfaction |
| 2 | CexCursor ordering is by seq_id only — timestamp_ms does not affect comparison | VERIFIED | Manual `Ord` impl on CexCursor compares `self.seq_id.cmp(&other.seq_id)` only; `cex_cursor_orders_by_seq_id_not_timestamp` passes; `cex_cursor_same_seq_id_is_equal_regardless_of_timestamp` passes |
| 3 | Cursor round-trip through serde_json produces identical values | VERIFIED | All three serde round-trip tests pass; `cex_cursor_serde_preserves_timestamp_ms` confirms timestamp_ms field is preserved even though excluded from ordering |
| 4 | EventSource trait compiles with async methods and Send-bounded futures | VERIFIED | `event_source_futures_are_send` test uses `tokio::spawn` with EventSource future; compiles and passes |
| 5 | A MockEventSource can be constructed and called in an async test without compiler errors | VERIFIED | 5 async tests in event_source_tests.rs pass; MockEventSource fully implements EventSource |
| 6 | Event<C, E> envelope carries cursor, data, and optional block_hash | VERIFIED | `Event` struct has `cursor: C`, `data: E`, `block_hash: Option<[u8; 32]>`; both with-block-hash and without-block-hash construction tests pass |

### Plan 01-02 Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A caller can implement StateMachine with a pure Apply reducer and have the library drive state forward via apply(state, event) | VERIFIED | `BalanceTracker` implements `StateMachine` in reducer_tests.rs and integration_tests.rs; all apply tests pass |
| 2 | ReducerCore accumulates events correctly in order, producing a deterministic final state | VERIFIED | `apply_and_accumulate_stores_events_in_order` and `accumulated_events_are_in_cursor_order` pass; determinism verified by CORE-04 integration test |
| 3 | State changes are observable through tokio::sync::watch Receiver | VERIFIED | `subscribe_returns_new_receiver_with_current_state` and `multiple_receivers_track_same_state` pass; `watch::Sender` field present in `ReducerCore` |
| 4 | In-memory event accumulation stores events in cursor order for later iteration | VERIFIED | `events_in_range_filters_correctly` and `events_in_range_empty_when_from_equals_to` pass; `events_in_range()` impl filters on `cursor >= from && cursor < to` |
| 5 | The library compiles with generic C: ChainCursor propagated through the type system without runtime dispatch | VERIFIED | No `dyn` or `Box` in machine.rs; `Pipeline<S: StateMachine>` and `apply_sequence<S>` in integration_tests.rs compile and pass |

**Score: 11/11 must-have truths verified**

---

## Required Artifacts

| Artifact | Provided | Status | Details |
|----------|----------|--------|---------|
| `src/cursor/mod.rs` | ChainCursor supertrait with blanket impl | VERIFIED | Contains `pub trait ChainCursor` and `impl<T> ChainCursor for T where T: ...`; 31 lines, substantive |
| `src/cursor/evm.rs` | EvmCursor with block_number and log_index | VERIFIED | `pub struct EvmCursor { pub block_number: u64, pub log_index: u32 }`; correct derive set including Ord |
| `src/cursor/btc.rs` | BtcCursor with block_height and tx_index | VERIFIED | `pub struct BtcCursor { pub block_height: u64, pub tx_index: u32 }`; correct derive set |
| `src/cursor/cex.rs` | CexCursor with seq_id-only ordering | VERIFIED | Manual `Ord` impl compares seq_id only; `timestamp_ms` excluded from comparison with explanatory comment |
| `src/event.rs` | Event<C, E> envelope struct | VERIFIED | `pub struct Event<C: ChainCursor, E>` with cursor, data, block_hash fields |
| `src/source.rs` | EventSource async trait | VERIFIED | `#[async_trait] pub trait EventSource: Send + Sync` with fetch_state, fetch_events, subscribe methods |
| `src/error.rs` | SourceError wrapper type | VERIFIED | `pub struct SourceError(#[from] pub Box<dyn std::error::Error + Send + Sync>)` with thiserror |
| `src/machine.rs` | StateMachine trait and ReducerCore struct | VERIFIED | Both `pub trait StateMachine` and `pub struct ReducerCore<SM: StateMachine>` present; 117 lines, fully implemented |
| `tests/reducer_tests.rs` | Unit tests for ReducerCore (min 50 lines) | VERIFIED | 177 lines; 9 tests covering watch notification, accumulation, range filtering, multiple receivers |
| `tests/integration_tests.rs` | Integration tests for all CORE requirements (min 80 lines) | VERIFIED | 401 lines; 5 named tests directly mapping to CORE-01 through CORE-05 |

---

## Key Link Verification

| From | To | Via | Status | Evidence |
|------|----|-----|--------|---------|
| `src/cursor/evm.rs` | `src/cursor/mod.rs` | ChainCursor blanket impl auto-satisfied by derive macros | WIRED | `cursor_types_satisfy_chain_cursor_bounds` compile-test passes; blanket impl confirmed at line 27 of cursor/mod.rs |
| `src/source.rs` | `src/cursor/mod.rs` | `type Cursor: ChainCursor` bound | WIRED | Line 19: `type Cursor: ChainCursor;` in EventSource trait |
| `src/event.rs` | `src/cursor/mod.rs` | `Event<C: ChainCursor, E>` generic bound | WIRED | Line 15: `pub struct Event<C: ChainCursor, E>` |
| `src/machine.rs` | `src/event.rs` | `StateMachine::apply takes &Event<C, E>` | WIRED | Line 25: `fn apply(&self, state: Self::State, event: &Event<Self::Cursor, Self::Event>) -> Self::State` |
| `src/machine.rs` | `tokio::sync::watch` | `ReducerCore` holds `watch::Sender` for state notification | WIRED | Line 48: `sender: watch::Sender<SM::State>`; used in `apply()` via `self.sender.send(next)` |
| `tests/integration_tests.rs` | `src/source.rs` | MockEventSource used to drive ReducerCore through event sequence | WIRED | `MockEventSource` implements `EventSource` and is used in `core_01_event_source_trait_abstraction` |

---

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| CORE-01 | 01-01-PLAN.md | Library exposes a generic `EventSource` trait abstracting over EVM, Bitcoin, and CEX ordering schemes | SATISFIED | `EventSource` trait in src/source.rs; MockEventSource tests prove generic abstraction; `core_01` integration test named and passing |
| CORE-02 | 01-02-PLAN.md | Library exposes a `StateMachine` trait where client provides pure Apply reducer | SATISFIED | `StateMachine` trait in src/machine.rs; infallible `fn apply()` with no Result; `core_02` integration test named and passing |
| CORE-03 | 01-01-PLAN.md | Cursor types implement `Ord + Clone + Serialize + Deserialize` | SATISFIED | All three cursor types satisfy ChainCursor bounds; 4 serde round-trip tests pass; `core_03` integration test named and passing |
| CORE-04 | 01-02-PLAN.md | Library supports reduced state — events fold into compact in-memory value | SATISFIED | `ReducerCore::apply()` folds events via watch channel; `core_04` integration test proves determinism with 10-event sequence |
| CORE-05 | 01-02-PLAN.md | Library supports accumulated state — events stored, accessed via cursors | SATISFIED | `apply_and_accumulate()` stores events in Vec; `events_in_range()` provides cursor-range access; `core_05` integration test proves zero runtime dispatch |

No orphaned requirements — all 5 Phase 1 CORE requirements are claimed by plans and verified.

---

## Anti-Patterns Found

None. All source files scanned for TODO/FIXME/XXX/HACK/PLACEHOLDER/placeholder/unimplemented!/todo!() — zero matches across all 8 source files and 4 test files.

No stub implementations: every function has substantive bodies, every trait has real implementations, every test exercises actual behavior.

---

## Test Results Summary

| Test Suite | Tests | Passed | Failed |
|------------|-------|--------|--------|
| cursor_tests | 13 | 13 | 0 |
| event_source_tests | 8 | 8 | 0 |
| reducer_tests | 9 | 9 | 0 |
| integration_tests | 5 | 5 | 0 |
| **Total** | **35** | **35** | **0** |

Note: `rustdoc` is unavailable in the sandbox (blocked by firejail noexec), so doc-tests could not run. There are no doc-test examples in the source code, so zero doc-test failures exist.

---

## Human Verification Required

None. All phase goals are mechanically verifiable:
- Type system correctness is proven by compilation
- Ordering semantics are proven by passing unit tests
- Serialization correctness is proven by round-trip tests
- Generic propagation is proven by compile-time tests
- Determinism is proven by running identical event sequences twice

No visual, real-time, or external service behavior to verify.

---

## Summary

Phase 1 goal is fully achieved. The core type system exists and is verified:

- **ChainCursor supertrait** with blanket impl covers all cursor types without per-type boilerplate
- **CexCursor seq_id-only ordering** is the critical correctness property — manually implemented, commented, and test-proven with the inverse case (higher seq_id with lower timestamp sorts after)
- **EventSource async trait** is a real abstraction — generic functions can consume any implementation without chain-specific knowledge
- **StateMachine trait** is infallible and synchronous — no Result, no async, no I/O in the apply contract
- **ReducerCore** correctly broadcasts state via tokio::sync::watch and accumulates events for range queries
- **Zero runtime dispatch** throughout — all generics are static, no `dyn` in machine.rs

Every subsequent phase can build against this type system with confidence.

---

_Verified: 2026-03-03T02:27:29Z_
_Verifier: Claude (gsd-verifier)_
