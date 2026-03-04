# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-02)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 2 — Persistence and Recovery

## Current Position

Phase: 2 of 4 (Persistence and Recovery)
Plan: 2 of 4 in current phase
Status: In progress — Plan 02 complete
Last activity: 2026-03-04 — Plan 02-02 complete (Engine coordinator + recovery pipeline, 18 tests all passing)

Progress: [████░░░░░░] 40%

## Performance Metrics

**Velocity:**
- Total plans completed: 4
- Average duration: 16min
- Total execution time: 1.1 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundations | 2 | 26min | 13min |
| 02-persistence-and-recovery | 2 | 41min | 20min |

**Recent Trend:**
- Last 5 plans: 11min, 15min, 6min, 35min
- Trend: recovery pipeline required debugging snapshot cursor semantics

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
- 01-02: watch::send() error on no-receivers is intentionally ignored — ReducerCore can outlive subscribers [SUPERSEDED by 02-02 decision]
- 01-02: events_in_range() is O(n) linear scan in Phase 1; Phase 2 replaces with SQLite B-tree index
- 02-01: rusqlite_migration upgraded from 1.3 to 2.0 — version conflict with rusqlite 0.34 (both link sqlite3); API identical
- 02-01: Mutex<Connection> for SqliteStorage Sync — rusqlite Connection is not Sync; Mutex is standard pattern with no hot-path overhead
- 02-01: Storage trait is fully sync; async callers bridge via tokio::task::spawn_blocking per locked decision
- [Phase 02-02]: Snapshot cursor = (last_applied_major + 1, 0): replay is inclusive from snapshot cursor, so cursor must point PAST the last captured event to avoid double-application
- [Phase 02-02]: send_modify and send_replace over watch::send(): send() silently discards value when no receivers; send_modify/send_replace always update internal state, required for engine.state() to work without subscriber
- [Phase 02-02]: fetch_gap coverage: explicitly call add_coverage(gap_start, live_tip_end) after loop exits, to mark full range as fetched even when source has no events near live_tip

### Pending Todos

None.

### Blockers/Concerns

- Phase 2: postcard 1.1.3 added for snapshot serialization (bincode dead; resolved in 02-01)
- Phase 2: Snapshot schema evolution strategy deferred — using raw bytes (postcard) with no versioning header for Phase 2; Phase 3 may add a version byte prefix if needed
- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research before implementation; Phase 4 PoC is EVM only

## Session Continuity

Last session: 2026-03-04T08:40:47Z
Stopped at: Completed 02-persistence-and-recovery/02-02-PLAN.md — Engine coordinator, recovery pipeline (snapshot fallback + gap fetch), 18 tests all passing
Resume file: .planning/phases/02-persistence-and-recovery/02-03-PLAN.md (next plan)
Resume command: /gsd:execute-phase 2
