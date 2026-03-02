# Project Research Summary

**Project:** Chainfold — Rust blockchain event sourcing / state reconstruction library
**Domain:** Blockchain infrastructure library (EVM + Bitcoin + CEX, event sourcing, state reconstruction)
**Researched:** 2026-03-02
**Confidence:** HIGH (core patterns), MEDIUM (specific version numbers)

## Executive Summary

Chainfold occupies a well-defined but technically demanding niche: an in-process, source-agnostic state reconstruction engine for blockchain and exchange data. The domain has established solutions (The Graph, Substreams, Ponder, Envio) but none deliver Chainfold's key constraint — sub-millisecond hot-path delivery with no IPC overhead. The recommended approach is a layered Rust library built around three client-implemented pure functions (`FetchState`, `FetchEvents`, `Apply`) with the library owning the full lifecycle: historical backfill, gap stitching, crash recovery, reorg detection, and snapshot management. The `alloy` crate for EVM, `rusqlite` with WAL mode for persistence, and `tokio` for async are the settled stack choices with no credible alternatives.

The critical design insight, confirmed across all four research areas, is that the gap between historical catch-up and live subscription is the hardest correctness problem in this domain. Every pitfall that causes silent data loss (missed events, duplicates, corrupt state) originates at this seam: at initial startup, after WebSocket reconnection, and after a reorg. The architecture must treat gap stitching as a first-class primitive, not an afterthought. Coverage tracking — recording exactly which cursor ranges are cached — is the mechanism that makes this correct and makes restarts cheap.

The key risks are well-understood and preventable with upfront design choices: premature finality treatment (tag events with finality level; track safe-tip vs latest-tip from day one), off-by-one errors at coverage range boundaries (encapsulate range semantics in a single type with property tests), and snapshot corruption from reorgs (store block hash alongside every snapshot anchor and re-validate on load). None of these can be bolted on later — they must be in the type system and schema from Phase 1.

---

## Key Findings

### Recommended Stack

The stack is high-confidence and well-settled. `alloy` (~0.11, verify on crates.io) is the unambiguous EVM choice — `ethers-rs` entered maintenance-only mode in late 2023 and alloy is its explicit successor, maintained by the same authors and adopted by reth. `rusqlite` with the `bundled` feature and WAL mode is the right persistence choice over `sqlx` (async SQLite in sqlx is fake-async, adds build complexity) and `diesel` (ORM overhead with no benefit for a controlled internal schema). `tokio` 1.x is the only async runtime. All version numbers must be verified on crates.io before pinning — the library was researched without live network access.

**Core technologies:**
- `alloy` (~0.11): EVM RPC, types, ABI, event decoding — active successor to ethers-rs; reth-compatible types
- `bitcoincore-rpc` + `bitcoin` (~0.19 / ~0.32): Bitcoin RPC client + primitive types — canonical `rust-bitcoin` umbrella crates
- `tokio-tungstenite` + `reqwest` (~0.26 / ~0.12): CEX WebSocket + REST — settled community standard, tokio-native
- `rusqlite` (~0.32) with `bundled`: SQLite persistence — direct WAL/PRAGMA control; `!Send` handled via dedicated background thread
- `bincode` v2 (~2.0, verify stable release): Snapshot serialization — zero-copy, compact; requires `snapshot_version: u32` field for invalidation
- `serde` + `serde_json` (1.x): JSON wire format for REST/WS messages
- `tokio` (1.x): Async runtime — no alternative; `broadcast`/`watch` channels for `OnChange` and `Current()` APIs
- `anvil` (Foundry): EVM integration testing — spin up local node, deploy contracts, verify state reconstruction
- `tempfile` (3.x): Temporary SQLite files in tests — required; never hardcode `/tmp/test.db`
- `proptest` (1.x): Property-based testing for gap recovery and reorg invariants

### Expected Features

The feature landscape divides cleanly into must-haves (correctness requirements without which the library is a toy) and differentiators (the reasons users choose Chainfold over hand-rolling). The anti-features list is unusually important: SQL query interface, library-managed chain connections, automatic reorg rollback, and WASM plugins are all tempting scope creep that must be explicitly rejected.

**Must have (table stakes):**
- Live event subscription with reconnection handling — WebSocket-based with backpressure and exponential backoff
- Historical backfill — chunked `eth_getLogs` / equivalent; binary search pagination for RPC limits
- Gap stitching (backfill endpoint to subscription start) — the hardest correctness problem; requires overlap + deduplication
- Event deduplication — keyed by `(source, cursor)` to handle backfill/subscription overlap
- Coverage tracking — records which cursor ranges are cached; makes restarts cheap; prerequisite for gap detection
- Crash recovery: snapshot load → cached replay → live catch-up — without this the library is a daemon not a library
- Anchor / cursor management — named checkpoints for bridge daemon use case
- Reorg detection + notification to client — EVM without reorg handling is incorrect for production; client decides rollback strategy
- Source-agnostic event model (`EventSource` trait) — accommodates block-indexed (EVM/Bitcoin) and sequence-indexed (CEX) ordering

**Should have (differentiators):**
- In-process hot path: WebSocket → Apply → callback with no IPC, no serialization on critical path
- Cheapest reconstruction path selection: pure cache replay vs. partial fetch vs. full RPC fetch
- Reduced state (compact fold) vs. accumulated state (events as state) duality in one library
- Client-owned connections (inversion of control — library calls through `EventSource`, never manages connections)
- Range queries with lazy/streaming cursors for accumulated state
- Adaptive snapshot density (v2: start with fixed-interval every 1000 events)
- CEX API as first-class source (Hyperliquid, dYdX, Binance via timestamp+seqId ordering)

**Defer (v2+):**
- `At(cursor)` historical point-in-time queries — add after core live tracking is stable
- Range queries with cursors — needed for accumulated state, not for reduced state MVP
- Secondary indexes on accumulated state — needed for bridge diff, land after schema is stable
- Adaptive snapshot density — optimize after fixed-interval is working
- Python/TypeScript bindings (PyO3/napi-rs) — get Rust API right first
- Async SQLite writes off hot path — synchronous is simpler and correct; async is an optimization pass

### Architecture Approach

The architecture is an 8-component layered system where the `Engine` orchestrates all subsystems and the client implements three pure functions. The critical design principle is that the hot path (WebSocket → `Apply` → callbacks) must never allocate, serialize, or cross process boundaries — events flow as native Rust types through bounded `tokio::mpsc` channels, SQLite writes are spawned off the hot path. The generic type parameter `C: ChainCursor` (not an `AnyCursor` enum) propagates through the entire type system to maintain source-specific ordering semantics without runtime dispatch.

**Major components:**
1. **EventSource trait** — abstract over EVM/Bitcoin/CEX; `fetch_range` + `subscribe` + `tip`; client-constructed (never URL-configured)
2. **Cursor / ChainCursor trait** — total ordering per source; `EvmCursor(block_number, log_index)`, `CexCursor(timestamp_ms, seq_id)`; never a unified enum
3. **StateMachine / ReducerCore** — holds in-memory state; `apply(state, event, cursor) -> state` must be pure and synchronous
4. **Cache Layer (SQLite WAL)** — `events`, `snapshots`, `coverage`, `anchors` tables; WAL mode mandatory; dedicated background thread (not `spawn_blocking` per query)
5. **Snapshot Manager** — policy: event-count (default N=1000), time-based, manual, on-shutdown; bincode v2; keep 2-3 snapshots max (prune after each write)
6. **Recovery Pipeline** — 6-step cold-start sequence: load snapshot → replay cache → fetch gap → close subscription gap → switch to live → steady state
7. **Subscription Manager** — owns live WebSocket stream; gap detection via `cursor.has_gap_after(prev)`; reorg detection via `cursor.same_block(prev) && content_differs`
8. **Engine** — orchestrates all subsystems; public API: `Current()`, `At(cursor)`, `OnChange(callback)`

**Schema (SQLite):**
- `events`: `(id, cursor_key BLOB UNIQUE, payload BLOB, created_at)` — `cursor_key` is serialized `ChainCursor`
- `snapshots`: `(id, cursor_key BLOB UNIQUE, state_blob BLOB, taken_at)` — plus `block_hash` for anchor validation
- `coverage`: `(id, from_key BLOB, to_key BLOB)` with CHECK constraint
- `anchors`: `(name TEXT PK, cursor_key BLOB, meta TEXT, updated_at)`
- PRAGMAs: `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-65536`, `busy_timeout=5000`

### Critical Pitfalls

1. **Premature finality / unfinalized blocks treated as final** — Maintain `safe_tip` cursor trailing latest by N blocks (12 for ETH, 6 for BTC). Never mark coverage final until block clears safe-tip. Store `block_hash` alongside every event row.

2. **Off-by-one at coverage range boundaries** — `eth_getLogs` is inclusive on both ends; `eth_subscribe` delivers *next* block after subscription. Encapsulate all range semantics in a single `Coverage` type. Write proptest property tests *before* implementing gap stitching. Commit subscription start cursor atomically with last historical coverage range.

3. **Snapshot taken mid-reorg / anchor not hash-validated** — Store `StateAnchor { block_number, block_hash }` always (never block number alone). After FetchState, re-validate `block_hash` is canonical before trusting snapshot. Retry with backoff if orphaned.

4. **WebSocket reconnect race (duplicate or missed events at seam)** — At reconnect: (1) note `last_confirmed_block`, (2) subscribe, (3) record first event block as `live_start`, (4) backfill `[last_confirmed+1, live_start-1]`, (5) deduplicate and apply. Reuse the same code path as initial gap recovery — not a separate reconnect-specific code path.

5. **SQLite write amplification / WAL growth** — Batch inserts per fetch page (not per event). Set `busy_timeout=5000` on every connection. Run `PRAGMA wal_checkpoint(PASSIVE)` after each write batch. Keep only 2-3 snapshots (prune after write). Use `PRAGMA incremental_vacuum` not full `VACUUM` in hot path.

6. **EVM `logIndex` is block-scoped, not transaction-scoped** — Use `(block_number, log_index)` as the canonical sort key. `log_index` is sufficient to order all events within a block; `tx_index` is redundant. Add a test with two logs in the same block from different transactions.

7. **CEX cursors are not monotonic timestamps** — CEX adapters must use exchange sequence IDs or trade IDs, never timestamps. `CexCursor(timestamp_ms, seq_id)` with deduplication by `seq_id`. Fetch one page before desired start and deduplicate.

---

## Implications for Roadmap

The feature dependency graph and the pitfall phase mapping agree on build order: the type foundations must be in place before I/O, the I/O layer before recovery, and recovery before the subscription manager. The ERC20 PoC is a forced integration test at the end of Phase 2 that validates all prior work before differentiators are added.

### Phase 1: Foundations — Type System and Core Traits

**Rationale:** Every component depends on cursor ordering and the `EventSource`/`StateMachine` trait signatures. Getting these wrong requires API-breaking changes that cascade through all layers. The pitfall research is unambiguous: finality semantics, CEX cursor design, `logIndex` ordering, and the purity requirement on `Apply` must all be encoded in types and traits before any I/O is written.

**Delivers:** Compilable, testable trait definitions with concrete cursor types, a working in-memory reducer with no I/O, and an EVM adapter stub that validates the trait design against a real chain API.

**Features addressed:** Source-agnostic event model, EventSource trait, ChainCursor trait, StateMachine trait

**Pitfalls avoided:** Premature finality (safe-tip vs latest-tip in cursor design), CEX non-monotonic cursors (sequence ID not timestamp), logIndex semantics (block-scoped enforced in EVM adapter), Apply impurity (synchronous, no async in trait signature), AnyCursor enum anti-pattern (source-scoped generics from day one)

**Research flag:** Standard Rust patterns — skip phase research. Validate alloy trait API signatures against current docs when implementing.

---

### Phase 2: Persistence, Recovery, and Correctness

**Rationale:** The SQLite schema and gap recovery logic are the highest-risk components in terms of silent correctness failures. All pitfalls related to coverage ranges, snapshot validation, and WAL behavior land here. Build and thoroughly test this before adding live subscriptions — bad persistence is impossible to detect from the outside.

**Delivers:** SQLite schema with WAL mode, coverage tracking, snapshot write/load/prune, 6-step recovery pipeline (cold-start), ERC20 PoC against `anvil` (full vertical slice from cold start through state reconstruction).

**Features addressed:** Coverage tracking, crash recovery, snapshot creation and loading, anchor management, historical backfill, gap stitching between historical and live

**Stack used:** `rusqlite` (bundled, WAL mode), `bincode` v2 (snapshots), `anvil` (integration tests), `tempfile` (test isolation), `proptest` (range boundary tests)

**Pitfalls avoided:** Off-by-one at coverage range boundaries (property tests before implementation), snapshot mid-reorg (StateAnchor with block_hash, re-validation on load), SQLite WAL pitfalls (busy_timeout, batched writes, checkpoint, snapshot pruning), unbounded event buffer (streaming/paged fetch_range from first implementation), per-event transactions (batch per page)

**Research flag:** Standard patterns, but WAL concurrency behavior needs careful integration testing. The gap-stitching logic is novel enough that proptest coverage is mandatory before declaring done.

---

### Phase 3: Live Subscription and Reorg Detection

**Rationale:** Live subscriptions depend on a working recovery pipeline — reconnect behavior reuses the same code path as initial gap recovery (verified by PITFALLS research). Reorg detection requires block header storage (last 128 blocks for Ethereum) and a `ReorgEvent { old_tip, new_tip, common_ancestor, depth }` API that must be stable before any client code is written against it.

**Delivers:** Subscription Manager with gap/reorg detection, `OnChange(callback)` hot path, WebSocket reconnect with gap backfill, reorg notification to client. End-to-end steady-state event flow at target latency.

**Features addressed:** Live event subscription, reconnection handling, reorg detection and notification, in-process hot path (zero-alloc WebSocket → Apply → callback)

**Stack used:** `tokio-tungstenite` (EVM/CEX WebSocket), `tokio::sync::mpsc` (bounded channels), `alloy` subscriptions

**Pitfalls avoided:** WebSocket reconnect race (reuse recovery code path, record `live_start` before backfill), reorg depth not communicated (ReorgEvent includes common_ancestor), no reorg test harness (MockChain injectable via EventSource generics), serializing events on hot path (native Rust types through channels only)

**Research flag:** Needs careful testing with `MockChain` harness. Reconnect-during-reorg scenario is the hardest edge case — simulate it explicitly with a fake WebSocket that drops mid-chain.

---

### Phase 4: Differentiators and Multi-Chain Validation

**Rationale:** Once the core loop is validated end-to-end with EVM, adding Bitcoin and CEX adapters validates that the `EventSource` trait genuinely accommodates all three cursor semantics. The `At(cursor)` historical query and cheapest reconstruction path selection are based on the coverage table built in Phase 2 — they can only be implemented after coverage tracking is proven correct.

**Delivers:** Bitcoin adapter, CEX adapter (at least one: Hyperliquid or similar), `At(cursor)` historical queries, cheapest reconstruction path selection, accumulated state (transfer history) alongside reduced state (balance), range queries.

**Features addressed:** Bitcoin/CEX as first-class sources, At(cursor) historical queries, cheapest reconstruction path, reduced vs accumulated state duality, range queries with lazy cursors, client-owned connection inversion of control validation across all three source types

**Stack used:** `bitcoincore-rpc` + `bitcoin`, `reqwest` + `tokio-tungstenite` (CEX), `wiremock` (CEX mock tests), `mockall`

**Pitfalls avoided:** CEX non-monotonic cursors (sequence ID cursor in CexCursor), cross-source cursor comparison (each Engine instance scoped to one cursor family), eth_getLogs response size limits (binary search pagination in EVM adapter)

**Research flag:** CEX adapter design may need research per specific exchange (Hyperliquid, dYdX APIs differ). Recommend `/gsd:research-phase` for whichever CEX adapter is targeted first. Bitcoin ZMQ vs polling transport decision may need verification.

---

### Phase 5: Hardening and Developer Experience

**Rationale:** Internal correctness without ergonomic APIs produces a library nobody uses. This phase addresses the gap between "works correctly in tests" and "pleasant to use in production": ergonomic builder APIs for the Engine, comprehensive `tracing` integration at correct granularity levels, snapshot schema versioning for forward compatibility, and public documentation with the ERC20 PoC as the canonical example.

**Delivers:** Ergonomic builder API for Engine configuration, `tracing` integration at correct log levels (ERROR/WARN/INFO/DEBUG/TRACE as specified in PITFALLS), snapshot schema versioning (`snapshot_version: u32` + `#[serde(default)]` on new fields), SQLite async wrapper finalization, crate documentation with examples.

**Features addressed:** Production readiness, snapshot schema evolution handling, logging at correct granularity, async SQLite writes off hot path (as optimization over synchronous baseline from Phase 2)

**Pitfalls avoided:** State deserialization breaking on schema evolution (versioned snapshots), logging at wrong granularity (structured tracing levels), tokio runtime conflicts with blocking SQLite (dedicated background thread or `tokio-rusqlite`)

**Research flag:** Standard Rust library ergonomics patterns — skip phase research.

---

### Phase Ordering Rationale

- **Types first (Phase 1):** Cursor semantics and trait signatures are foundation-level. Every component that stores, routes, or compares events needs the ordering type. Wrong decisions here require API-breaking changes.
- **Persistence before subscriptions (Phase 2 before 3):** Gap recovery reuses the same code path as reconnection (confirmed by pitfall research). Recovery must be correct before live subscriptions are added — otherwise reconnect behavior is built on a broken foundation.
- **ERC20 PoC mid-pipeline (end of Phase 2):** Forces a full vertical integration test before Phase 3 adds subscription complexity. Drives ergonomics fixes before the API is used widely.
- **Differentiators after correctness (Phase 4):** Bitcoin/CEX adapters and `At(cursor)` are only meaningful if the core event sourcing loop is correct. Validated by Phases 1-3 first.
- **DX last (Phase 5):** Developer experience improvements are most effective once the API is stable — premature polish is waste.

### Research Flags

Phases needing deeper research during planning:
- **Phase 4 (CEX adapter):** Specific exchange APIs (Hyperliquid, dYdX) differ significantly in WebSocket message formats, sequence ID semantics, and rate limits. Recommend `/gsd:research-phase` when a specific exchange is targeted. Bitcoin ZMQ transport should be verified against current `bitcoincore-rpc` docs.

Phases with standard patterns (skip research-phase):
- **Phase 1:** Standard Rust trait design patterns — well-understood. Verify current alloy trait signatures against docs when implementing.
- **Phase 2:** SQLite WAL patterns are well-documented and stable. Gap recovery is domain-specific but fully specified by the pitfall research.
- **Phase 3:** Tokio mpsc + WebSocket reconnect patterns are standard. The correctness requirements are fully specified.
- **Phase 5:** Standard Rust library ergonomics — no research needed.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH (choices) / MEDIUM (versions) | Technology selections are well-established. All version numbers must be verified on crates.io before pinning — researched without live network access. Specifically: verify bincode v2 has left RC; check alloy for 1.0 release. |
| Features | HIGH (project-specific) / MEDIUM (ecosystem patterns) | Table stakes and differentiators derived from PROJECT.md (high confidence) and comparable system analysis (The Graph, Ponder, Substreams) at training cutoff. Anti-features list is definitive. |
| Architecture | HIGH | Core patterns (layered event sourcing, WAL SQLite, tokio channels, generic cursors) are well-established in Rust async literature and alloy ecosystem. Specific alloy trait names may differ from current docs — verify at implementation time. |
| Pitfalls | HIGH | Drawn from production post-mortems (The Graph, ethers-rs issue trackers, bridge daemon operational context), EVM Yellow Paper log semantics, SQLite WAL documentation. These are stable, well-documented failure modes. |

**Overall confidence:** HIGH for design decisions; MEDIUM for specific version numbers and one CEX-specific item.

### Gaps to Address

- **Version pinning:** All crate versions require verification on crates.io before Cargo.toml is finalized. Prioritize: bincode v2 stability, alloy 1.0 status, tokio-tungstenite current patch.
- **CEX adapter target:** The research treats CEX as a validated concept but no specific exchange API was analyzed in depth. The first CEX target (Hyperliquid is the implied primary from PROJECT.md context) needs per-exchange API research before Phase 4 implementation.
- **Snapshot schema evolution strategy:** Research recommends `bincode` for v1 with `snapshot_version` for invalidation. This is correct but means snapshot format changes require full re-sync. If forward compatibility without re-sync is a hard requirement (e.g., zero-downtime upgrades in bridge daemon), reconsider `serde_json` for snapshots at cost of 2-4x size/speed. Decide before Phase 2 schema is finalized.
- **MockChain harness scope:** PITFALLS research calls for a `MockChain` test harness that can fork at arbitrary blocks. This is a non-trivial test infrastructure investment. Scope it explicitly as a Phase 3 deliverable, not an afterthought.

---

## Sources

### Primary (HIGH confidence)
- `/home/nb/src/chainfold/blockchain_storage.md` — project brainstorm and design summary; direct input to feature categorization
- `/home/nb/src/chainfold/.planning/PROJECT.md` — requirements and constraints; the authoritative source for project-specific decisions
- SQLite WAL mode documentation (stable since SQLite 3.7.0) — persistence architecture
- Ethereum JSON-RPC specification and Yellow Paper — log ordering semantics (logIndex block-scoped)
- Tokio documentation — async patterns, channel selection, blocking call handling

### Secondary (MEDIUM confidence)
- Training knowledge (cutoff Aug 2025): alloy ecosystem, ethers-rs deprecation/succession, `rusqlite` vs `sqlx` tradeoffs, event sourcing literature (Martin Fowler, Ben Stopford "Designing Event-Driven Systems"), Ponder/Envio/Subsquid feature sets
- Production post-mortems from The Graph, ethers-rs issue trackers (through Aug 2025) — pitfall catalogue
- Bridge daemon failure patterns at Symbiosis — directly informs reorg notification requirements and anchor design

### Tertiary (LOW confidence — verify before implementation)
- RPC provider limits: Infura/Alchemy `eth_getLogs` caps (2,000-10,000 blocks / 10,000 logs per request) — verify against current provider documentation
- Specific crate versions: all version numbers in Cargo.toml sketch are approximate; verify on crates.io
- Bincode v2 API stability: was in RC as of early 2025; confirm stable release

---
*Research completed: 2026-03-02*
*Ready for roadmap: yes*
