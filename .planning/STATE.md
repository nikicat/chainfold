# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-02)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 1 — Foundations

## Current Position

Phase: 1 of 4 (Foundations)
Plan: 2 of 2 in current phase
Status: Phase complete — all plans finished
Last activity: 2026-03-03 — Plan 02 complete

Progress: [██░░░░░░░░] 20%

## Performance Metrics

**Velocity:**
- Total plans completed: 2
- Average duration: 13min
- Total execution time: 0.43 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundations | 2 | 26min | 13min |

**Recent Trend:**
- Last 5 plans: 11min, 15min
- Trend: slight increase

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Pre-roadmap: Rust + alloy for EVM; rusqlite (WAL) for persistence; bincode v2 for snapshots; tokio for async
- Pre-roadmap: Reorg detection notifies client only — no auto-rollback
- Pre-roadmap: One SQLite file per app process; client owns chain connections via EventSource
- 01-01: async-trait chosen over trait_variant for Phase 1 EventSource — simpler ergonomics; migration to trait_variant deferred to Phase 3 hot path optimization
- 01-01: CexCursor Ord implemented manually (not derived) to make seq_id-only ordering invariant explicit and refactor-proof
- 01-01: Event<C, E> with C first — more constrained type first matches alloy convention and improves inference
- 01-01: bincode is DEAD (tombstone release Dec 2025); Phase 2 snapshots must use postcard instead
- 01-02: apply() borrows event (streaming); apply_and_accumulate() owns event (history) — no Clone bound on E needed in either case
- 01-02: watch::send() error on no-receivers is intentionally ignored — ReducerCore can outlive subscribers
- 01-02: events_in_range() is O(n) linear scan in Phase 1; Phase 2 replaces with SQLite B-tree index

### Pending Todos

None.

### Blockers/Concerns

- Phase 2: bincode confirmed dead — use postcard for snapshot serialization (postcard 1.1.3, actively maintained, serde-compatible)
- Phase 2: Decide snapshot schema evolution strategy before finalizing SQLite schema (postcard for speed vs serde_json for forward compatibility)
- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research before implementation; Phase 4 PoC is EVM only

## Session Continuity

Last session: 2026-03-03T02:20:00Z
Stopped at: Completed 01-foundations/01-02-PLAN.md — StateMachine trait, ReducerCore, 35 tests all passing, all Phase 1 CORE criteria proven
Resume file: .planning/phases/02-storage/02-01-PLAN.md (next phase)
Resume command: /gsd:execute-phase 2
