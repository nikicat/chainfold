---
phase: 05-evm-erc20-module-with-filter-pushdown
verified: 2026-03-09T10:30:00Z
status: passed
score: 9/9 must-haves verified
notes:
  - "MOD-01 through MOD-05 referenced in ROADMAP and PLANs but never defined in REQUIREMENTS.md — requirement IDs are orphaned from the traceability matrix"
---

# Phase 5: EVM ERC20 Module with Filter Pushdown Verification Report

**Phase Goal:** The ERC20 PoC types and alloy-based EventSource are extracted into a reusable, feature-gated library module with account-scoped eth_getLogs filter pushdown -- so users can track ERC20 balances for a single account without fetching all Transfer events
**Verified:** 2026-03-09T10:30:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

Truths sourced from ROADMAP.md Success Criteria (5 items) plus PLAN must_haves (4 additional items).

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `cargo check` (no features) compiles without pulling alloy | VERIFIED | `cargo check` succeeds in 0.10s; `src/lib.rs` line 22: `#[cfg(feature = "evm")] pub mod modules` |
| 2 | `chainfold::modules::erc20` exports Transfer, Erc20Machine, Erc20Codec, Erc20Source, Erc20Filter, and alloy re-exports | VERIFIED | `cargo check --features evm` succeeds; erc20.rs: pub struct Transfer (L29), pub struct Erc20Machine (L59), pub struct Erc20Codec (L96), pub struct Erc20Source (L157), pub enum Erc20Filter (L47), pub use Provider/ProviderBuilder/Address (L13-14) |
| 3 | Erc20Source with Erc20Filter::Account(addr) fires two scoped eth_getLogs queries per chunk and deduplicates results | VERIFIED | erc20.rs L250-287: Account match arm builds filter_from (topic1) and filter_to (topic2), `tokio::join!` both, deduplicates via `BTreeMap<(u64, u32), Log>` |
| 4 | Existing 6 ERC20 integration tests pass using imports from library module | VERIFIED | `cargo test --features evm --test erc20_tests`: 6 passed, 0 failed; tests import from `chainfold::modules::erc20` (L15) |
| 5 | Example binary is CLI-only -- all domain types imported from library module | VERIFIED | `cargo check --features evm --example erc20_tracker` succeeds; example imports from `chainfold::modules::erc20` (L21-24); no local Transfer/Erc20Machine/Erc20Codec definitions |
| 6 | Erc20Source implements EventSource with filter pushdown via Erc20Filter | VERIFIED | erc20.rs L293: `impl<P: Provider + Clone> EventSource for Erc20Source<P>` with fetch_blocks, fetch_state, subscribe |
| 7 | Account filter fires two eth_getLogs queries per chunk, deduplicates by (block_number, log_index) | VERIFIED | Same evidence as truth 3 |
| 8 | alloy Provider and ProviderBuilder are re-exported from the module | VERIFIED | erc20.rs L14: `pub use alloy::providers::{Provider, ProviderBuilder};` |
| 9 | MockErc20Source remains in tests/erc20_tests.rs (not moved to library) | VERIFIED | tests/erc20_tests.rs L25: `struct MockErc20Source`; not present in src/modules/ |

**Score:** 9/9 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `Cargo.toml` | evm feature flag with optional alloy + tracing deps | VERIFIED | L7: `evm = ["dep:alloy", "dep:tracing"]`; L21: alloy optional; L22: tracing optional; L34-40: [[test]] and [[example]] with required-features |
| `src/lib.rs` | cfg-gated modules re-export | VERIFIED | L22-23: `#[cfg(feature = "evm")] pub mod modules;` |
| `src/modules/mod.rs` | cfg-gated erc20 submodule | VERIFIED | L1-2: `#[cfg(feature = "evm")] pub mod erc20;` |
| `src/modules/erc20.rs` | All public types, min 200 lines | VERIFIED | 399 lines; exports Transfer, Balances, TRANSFER_TOPIC, Erc20Machine, Erc20Codec, Erc20Filter, Erc20Source, Erc20SourceError, Provider, ProviderBuilder, Address |
| `tests/erc20_tests.rs` | ERC20 integration tests using library module types | VERIFIED | L15: `use chainfold::modules::erc20::{Balances, Erc20Codec, Erc20Machine, Transfer}` |
| `examples/erc20_tracker.rs` | CLI-only example importing from library module | VERIFIED | L21-24: imports from `chainfold::modules::erc20`; no local domain type definitions |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/modules/erc20.rs` | `src/machine.rs` | `impl StateMachine for Erc20Machine` | WIRED | L61: `impl StateMachine for Erc20Machine` |
| `src/modules/erc20.rs` | `src/source.rs` | `impl EventSource for Erc20Source` | WIRED | L293: `impl<P: Provider + Clone> EventSource for Erc20Source<P>` |
| `src/modules/erc20.rs` | `src/coordinator/mod.rs` | `impl Codec for Erc20Codec` | WIRED | L98: `impl Codec for Erc20Codec` |
| `tests/erc20_tests.rs` | `src/modules/erc20.rs` | `use chainfold::modules::erc20::*` | WIRED | L15: imports Transfer, Balances, Erc20Machine, Erc20Codec |
| `examples/erc20_tracker.rs` | `src/modules/erc20.rs` | `use chainfold::modules::erc20::*` | WIRED | L21-24: imports Address, Balances, Erc20Codec, Erc20Filter, Erc20Machine, Erc20Source, Provider, ProviderBuilder |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| MOD-01 | 05-01 | NOT DEFINED IN REQUIREMENTS.md | UNDEFINED | ID referenced in ROADMAP and PLAN but never added to REQUIREMENTS.md |
| MOD-02 | 05-01 | NOT DEFINED IN REQUIREMENTS.md | UNDEFINED | Same |
| MOD-03 | 05-01 | NOT DEFINED IN REQUIREMENTS.md | UNDEFINED | Same |
| MOD-04 | 05-01 | NOT DEFINED IN REQUIREMENTS.md | UNDEFINED | Same |
| MOD-05 | 05-02 | NOT DEFINED IN REQUIREMENTS.md | UNDEFINED | Same |

**Note:** MOD-01 through MOD-05 are referenced in `ROADMAP.md` Phase 5 and in both PLANs, but these requirement IDs were never defined in `REQUIREMENTS.md`. The traceability matrix in REQUIREMENTS.md has no entries for any MOD-* IDs. This is a documentation gap only -- the actual implementation satisfies all 5 ROADMAP Success Criteria. The REQUIREMENTS.md should be updated to define these IDs and add them to the traceability table.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | - | - | No TODOs, FIXMEs, placeholders, or stub implementations found |

### Human Verification Required

### 1. Erc20Source with real RPC endpoint

**Test:** Run `cargo run --features evm --example erc20_tracker -- --rpc-url $RPC --from-block 24600000 --num-blocks 10`
**Expected:** Tracker fetches blocks, displays balances, exits cleanly
**Why human:** Requires a live Ethereum RPC endpoint; cannot verify eth_getLogs integration programmatically

### 2. Account filter pushdown with real RPC

**Test:** Run `cargo run --features evm --example erc20_tracker -- --rpc-url $RPC --days 1 --account 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640`
**Expected:** Shows balance history for the specific account; should be faster than AllTransfers mode for the same range
**Why human:** Requires live RPC; filter pushdown correctness (two queries vs one) only observable with real data

### Gaps Summary

No gaps found. All 5 ROADMAP Success Criteria are met. All artifacts exist, are substantive (399-line module), and are fully wired into the crate. All 6 ERC20 integration tests pass. Full test suite (134 tests across 13 suites) passes. No anti-patterns detected.

The only issue is a documentation gap: MOD-01 through MOD-05 requirement IDs need to be defined in REQUIREMENTS.md and added to the traceability matrix. This does not block goal achievement.

---

_Verified: 2026-03-09T10:30:00Z_
_Verifier: Claude (gsd-verifier)_
