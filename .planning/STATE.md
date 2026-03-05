# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-05)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 3 — Live Subscription (next phase)

## Current Position

Phase: 3 of 4 (Live Subscriptions and Reorg Safety) — IN PROGRESS
Plan: 2 of 3 (03-02 complete — Reorg detection and state re-derivation)
Status: Phase 3 in progress. Plans 03-01 and 03-02 complete.
Last activity: 2026-03-05 — 03-02 executed: process_or_reorg, find_common_ancestor, handle_reorg implemented; 99 tests passing

Progress: [█████████░] 75% (Phase 1 complete, Phase 2 complete, Phase 3 two-thirds done)

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
| 02-persistence-and-recovery (new) | 4 | 30min | 7min |

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
- **02-03: unchecked_transaction for write_block** — all three writes (block + events + canonical_tip) atomic; mutex held for duration so no deadlock risk
- **02-03: EXISTS check before TX** — avoids opening transaction for duplicate blocks; consistent with InMemoryStorage early return
- **02-03: Engine snapshot counter** — resets to 0 after each trigger; rolls forward counting N new blocks until next snapshot
- **02-04: No fetch_state in recovery** — Engine created with initial state by caller; recovery uses that as genesis state; avoids needing a genesis_cursor concept
- **02-04: Genesis sentinel [0u8;32] for full-chain replay** — collect_canonical_chain([0;32], tip) walks backward from tip, stops when parent==sentinel, genesis block IS included
- **02-04: Gap advance by last_height+1** — fetch_gap_blocks advances by actual last block height+1, not block count; handles non-contiguous blocks correctly
- **03-01: canonical_tip FK removed in schema v3** — set_canonical_tip must work before any blocks exist (reorg rollback case); FK was over-constraining; semantic integrity enforced by engine
- **03-01: tokio-util 0.7 has no "sync" feature** — CancellationToken included by default; plan's feature spec was wrong
- **03-01: EngineConfig::new() + pub fields** — constructor for ergonomic defaults, pub fields for explicit control; both modes valid
- **03-02: initial_state in Engine** — captured at Engine::new() via core.subscribe().borrow().clone(); zero API change; used for no-snapshot reorg recovery from genesis
- **03-02: find_common_ancestor algorithm** — HashSet of old chain hashes + backward walk of new chain; O(depth_old + depth_new); returns ReorgWithoutAncestor at genesis sentinel
- **03-02: on_reorg fires before state mutation** — client callback sees pre-reorg state context; state reflects new branch only after process_or_reorg returns
- **03-02: fetch_gap_blocks pub(crate)** — exposed for Engine::run() in Plan 03 gap-fill phase

### Pending Todos

- Revise 03-CONTEXT.md for DAG model (old context assumed linear storage + reorg callbacks)

### Blockers/Concerns

- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research; Phase 4 PoC is EVM only
- CEX sources have no blocks/reorgs — DAG model degenerates to linear chain for non-block sources

## Session Continuity

Last session: 2026-03-05
Stopped at: Completed 03-02-PLAN.md (Reorg detection: process_or_reorg, find_common_ancestor, handle_reorg, initial_state field, 10 new tests, 99 total passing)
Resume file: .planning/phases/03-live-subscriptions-and-reorg-safety/03-03-PLAN.md
Resume command: /gsd:execute-phase 03-live-subscriptions-and-reorg-safety
