---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: in-progress
last_updated: "2026-03-10T00:21:24Z"
progress:
  total_phases: 6
  completed_phases: 5
  total_plans: 16
  completed_plans: 15
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-05)

**Core value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.
**Current focus:** Phase 6 - EVM log cache with range tracking

## Current Position

Phase: 6 of 6 (EVM Log Cache with Range Tracking)
Plan: 1 of 2 (06-01 complete -- LogCache storage layer with schema v4, range tracking, gap computation)
Status: Plan 01 complete. LogCache stores/retrieves logs and block headers, tracks scanned ranges with eager merge, computes gaps. 158 tests passing.
Last activity: 2026-03-10 -- 06-01 executed: LogCache storage layer with BLAKE3 hashing, range merging, alloy Log round-trip

Progress: [█████████░] 94% (15/16 plans complete)

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
| 04-query-api-and-erc20-poc | 3 | 19min | 6min |
| 05-evm-erc20-module | 2 | 14min | 7min |
| 06-evm-log-cache-with-range-tracking | 1/2 | 4min | 4min |

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
- **03-03: cancel.is_cancelled() at two points** — top of outer loop AND inside subscribe Err branch; handles race between stream end and cancel token
- **03-03: source: &ES borrow** — Engine::run() borrows source to allow multiple subscribe() calls and fetch_gap_blocks across reconnect cycles
- **03-03: Inline gap fill per-block** — check block.height > tip+1 before each block in batch loop; calls fetch_gap_blocks for the range
- **03-03: tokio macros in main deps** — tokio::select! used in lib code; requires macros + rt-multi-thread in [dependencies] not just [dev-dependencies]
- **03-03: Test Pattern A preferred** — max_retries=0 causes run() to return Err(ReconnectExhausted) after finite stream; more reliable than watcher-based cancellation with sync mock subscribe()
- **04-01: ReducerCore::sender Arc-wrapped** — Arc<watch::Sender> shared between ReducerCore and ReducedStore; enables store creation without Engine reference
- **04-01: Engine::new() takes Arc<S>** — storage wrapped in Arc at construction; all method calls work via auto-deref; existing call sites updated
- **04-01: Blanket impl Storage for Arc<S>** — delegates all methods to inner S; supports Arc<InMemoryStorage> and Arc<SqliteStorage> directly
- **04-01: ReducedStore holds Arc<watch::Sender>** — only Sender can create new subscribers (Receiver cannot); store is read-only from caller perspective
- **04-02: reconstruct_at is sync** — covers tiers 1-3 (storage-only); async RPC tiers deferred as additive wrapper
- **04-02: Canonical chain walk for height-range** — walks backward from tip, filters by height; correct without canonical flag column
- **04-02: EventRangeIterator reversed-buffer pop** — O(1) per yield, no Clone bound, no unsafe
- **04-03: ERC20 PoC in test file, not library** — StateMachine + Codec defined in tests/erc20_tests.rs; PoC demonstrates stack, not reusable library code
- **04-03: serde_json for PoC Codec** — simplicity over performance; production users choose their own serialization
- **04-03: alloy 1.x needs stable toolchain** — project default rustc 1.90, alloy 1.7+ needs 1.91; example builds with `rustup run stable`
- **05-01: Erc20SourceError uses String-based Rpc variant** — avoids coupling to specific alloy TransportError type which varies across versions
- **05-01: cfg-gated module pattern** — evm feature flag enables pub mod modules in lib.rs; alloy and tracing are optional deps
- **05-01: Filter pushdown via Erc20Filter enum** — AllTransfers = single query, Account = two parallel queries with BTreeMap dedup
- **05-02: [[test]] required-features for feature-gated tests** — erc20_tests requires evm feature; Cargo.toml [[test]] section enforces this
- **06-01: LogCache opens own Connection** — same DB file, separate Mutex<Connection>; WAL mode supports concurrent connections
- **06-01: BLAKE3 for filter key hashing** — 0xFF sentinel for null topics; faster than SHA-256, 32-byte output
- **06-01: Eager merge-on-write for scanned ranges** — query merge candidates, compute merged bounds, delete+insert in single TX
- **06-01: configure_connection pub(crate)** — LogCache reuses pragma setup from sqlite.rs

### Roadmap Evolution

- Phase 5 added: EVM ERC20 module with filter pushdown
- Phase 6 added: EVM log cache with range tracking — raw eth_getLogs result caching keyed by RPC parameters, scanned range tracking (including empty ranges), range merging, reducer-independent event reuse

### Pending Todos

- Revise 03-CONTEXT.md for DAG model (old context assumed linear storage + reorg callbacks)

### Blockers/Concerns

- Phase 4: CEX adapter (Phase v2 scope) needs per-exchange API research; Phase 4 PoC is EVM only
- CEX sources have no blocks/reorgs — DAG model degenerates to linear chain for non-block sources

## Session Continuity

Last session: 2026-03-10
Stopped at: Completed 06-01-PLAN.md (LogCache storage layer with schema v4, range tracking, gap computation)
Resume file: .planning/phases/06-evm-log-cache-with-range-tracking/06-02-PLAN.md
Resume command: /gsd:execute-phase 06
