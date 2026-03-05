# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-05)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 2 — Block DAG Storage and Recovery (redesign)

## Current Position

Phase: 2 of 4 (Block DAG Storage and Recovery)
Plan: 2 of 4 (02-02 complete — InMemoryStorage done)
Status: Active — Plan 02-02 complete, Plan 02-03 (SqliteStorage) next
Last activity: 2026-03-05 — 02-02 executed: InMemoryStorage with 16 block DAG storage unit tests

Progress: [████░░░░░░] 37% (Phase 1 complete, Phase 2 plans 1-2 complete)

## Architecture Revision (2026-03-05)

**What changed:** Storage model redesigned from flat linear event store to block DAG.

**Why:** Reorgs should be handled transparently by the library, not pushed to client. State is tracked per block hash (like git commits). Each block has a parent hash. Canonical chain is the consensus-determined main branch. Reorg = canonical tip moves to a different branch — library re-derives state from fork point.

**What's preserved:**
- Phase 1: cursors, EventSource trait, StateMachine trait, ReducerCore (all unchanged)
- Phase 1 tests: all passing, still valid

**What's being replaced:**
- Phase 2 storage schema (flat events → block DAG)
- Phase 2 coordinator (linear pipeline → block-aware processing)
- Phase 2 recovery (linear replay → canonical chain replay)
- Phase 3 context (03-CONTEXT.md needs revision for DAG model)

**Key design principle:** Structural sharing for state (e.g., im::HashMap) is the user's choice via `State: Clone`. Library doesn't mandate persistent data structures — it just calls Clone when forking state at a branch point.

## Performance Metrics

**Velocity:**
- Total plans completed: 4 (2 preserved from Phase 1, 2 from old Phase 2 being replaced)
- Average duration: 16min
- Total execution time: 1.1 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 01-foundations | 2 | 26min | 13min |
| 02-persistence-and-recovery (old) | 2 | 41min | 20min |
| 02-persistence-and-recovery (new) | 2 | 6min | 3min |

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Pre-roadmap: Rust + alloy for EVM; rusqlite (WAL) for persistence; tokio for async
- Pre-roadmap: One SQLite file per app process; client owns chain connections via EventSource
- 01-01: async-trait chosen over trait_variant for Phase 1 EventSource — simpler ergonomics; keep for now
- 01-01: CexCursor Ord implemented manually (not derived) to make seq_id-only ordering invariant explicit and refactor-proof
- 01-01: Event<C, E> with C first — more constrained type first matches alloy convention and improves inference
- 01-01: bincode is DEAD (tombstone release Dec 2025); use postcard for any internal serialization
- 01-02: apply() borrows event (streaming); apply_and_accumulate() owns event (history)
- 01-02: watch::send() → use send_modify/send_replace instead (always updates internal state even with no receivers)
- **2026-03-05: Block DAG architecture** — state tracked per block hash, reorgs are branch switches, library handles transparently
- **2026-03-05: Structural sharing is user-side** — State: Clone; user picks im::HashMap for cheap forks or HashMap if reorgs are rare
- **2026-03-05: No reorg callback** — supersedes old "notify client" decision. Library re-derives state on branch switch. Client sees transparent state update via Current().
- **02-01: Block<C,E> wraps Vec<Event<C,E>>** — block is storage unit, event is block-scoped; block_hash removed from Event
- **02-01: Codec encodes payload only** — Codec::encode_event/decode_event operate on Self::Event (payload), not full Event<C,E>; cursor bytes managed internally via postcard
- **02-01: Schema v2 drops coverage/anchors** — block DAG recovery walks parent pointers; coverage intervals not needed
- **02-02: All-zeros genesis sentinel** — [0u8; 32] marks "no parent"; find_nearest_snapshot and collect_canonical_chain halt on it
- **02-02: Block storage source-agnostic** — blocks HashMap keyed by block_hash only; canonical_tips and snapshots keyed by source_id
- **02-02: Backward walk + reverse** — collect_canonical_chain walks backward then reverses; O(n) allocation, simpler than prepend

### Pending Todos

- Revise 03-CONTEXT.md for DAG model (old context assumed linear storage + reorg callbacks)

### Blockers/Concerns

- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research; Phase 4 PoC is EVM only
- CEX sources have no blocks/reorgs — DAG model degenerates to linear chain for non-block sources

## Session Continuity

Last session: 2026-03-05
Stopped at: Completed 02-02-PLAN.md (InMemoryStorage: block DAG storage with 16 unit tests)
Resume file: .planning/phases/02-persistence-and-recovery/02-03-PLAN.md
Resume command: /gsd:execute-phase 02-persistence-and-recovery
