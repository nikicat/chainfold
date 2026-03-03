# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-02)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 1 — Foundations

## Current Position

Phase: 1 of 4 (Foundations)
Plan: 0 of TBD in current phase
Status: Ready to plan
Last activity: 2026-03-03 — Roadmap created

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**
- Total plans completed: 0
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| - | - | - | - |

**Recent Trend:**
- Last 5 plans: —
- Trend: —

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Pre-roadmap: Rust + alloy for EVM; rusqlite (WAL) for persistence; bincode v2 for snapshots; tokio for async
- Pre-roadmap: Reorg detection notifies client only — no auto-rollback
- Pre-roadmap: One SQLite file per app process; client owns chain connections via EventSource

### Pending Todos

None yet.

### Blockers/Concerns

- Phase 2: Verify bincode v2 has left RC before finalizing snapshot serialization choice
- Phase 2: Decide snapshot schema evolution strategy before finalizing SQLite schema (bincode for speed vs serde_json for forward compatibility)
- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research before implementation; Phase 4 PoC is EVM only

## Session Continuity

Last session: 2026-03-03
Stopped at: Phase 1 context gathered — ready to plan
Resume file: .planning/phases/01-foundations/01-CONTEXT.md
