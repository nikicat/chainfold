# Feature Landscape

**Domain:** Blockchain event sourcing / state reconstruction library
**Researched:** 2026-03-02
**Confidence note:** Web access unavailable. Findings based on training data (cutoff Aug 2025) + project's own documentation (`blockchain_storage.md`, `PROJECT.md`). Comparable systems analyzed: The Graph, Substreams, Ponder, Envio, Subsquid, ethers-rs event watchers, alloy subscriptions, custom bridge indexers. Confidence: MEDIUM for ecosystem patterns (well-established domain), HIGH for project-specific design choices already captured in PROJECT.md.

---

## Table Stakes

Features that users building operational blockchain services will expect. Missing any of these forces them to hand-roll the thing they came to you to avoid.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Live event subscription** | Every operational service needs real-time data; polling is a non-starter for ms-latency workloads | Med | WebSocket-based; must handle backpressure and reconnection |
| **Historical backfill** | State reconstruction requires events from contract deployment or wallet creation; no library is useful without this | Med | `eth_getLogs` / equivalent per chain; must support chunked range queries to avoid RPC limits |
| **Gap stitching between backfill and subscription** | The window between "last historical block fetched" and "first subscription event delivered" is where every hand-rolled impl leaks events | High | Classic TOCTOU problem; requires overlap + deduplication or atomic cursor handoff |
| **Event deduplication** | Subscriptions and backfill overlap; delivering duplicates breaks any reducer | Med | Keyed by (source, block/seq, sub-index); idempotent cache writes |
| **Crash recovery / restart resilience** | A service that loses its state on every restart is a daemon, not a library | High | Load last snapshot → replay cached events → catch up → go live; WAL mode critical for SQLite |
| **Coverage tracking** | Without knowing which ranges are cached, every restart requires full re-fetch | Med | "coverage" table: (source, from_cursor, to_cursor) ranges; gap detection on startup |
| **Reconnection handling** | WebSocket disconnections are routine; silently dropping events is a critical bug | Med | Exponential backoff, re-subscribe from last known anchor, refetch gap |
| **Forward-only state replay** | State reconstruction is snapshot + ordered apply; the "cheapest path" between known points is a core utility | High | Must select: cached-only, partial-fetch, or full RPC re-fetch based on coverage |
| **Snapshot creation and loading** | Without periodic checkpoints, replay cost grows linearly with event history length | Med | Opaque bytes owned by client; periodic (every N events or M seconds) |
| **Anchor / cursor management** | Operational services must remember where they left off; named anchors are the mechanism | Med | Named cursor positions stored persistently; updated atomically with event processing |
| **Reorg detection** | EVM chains produce reorganizations routinely (1-3 blocks deep daily; 5-10 blocks rare but real); silently applying reorged events produces wrong state | High | Compare canonical block hash at stored block height; detect when hash changes |
| **Reorg notification to client** | The library cannot know the right rollback strategy for all workloads; client must be informed | Med | Callback/event with reorg depth and affected cursor range; client decides |
| **Source-agnostic event model** | EVM, Bitcoin, CEX APIs all produce ordered events with different cursor schemes; abstracting this is table stakes for a multi-chain library | High | `EventSource` trait/interface with pluggable ordering (blockNum+logIndex, blockHeight+txIndex, timestamp+seqId) |

---

## Differentiators

Features that set Chainfold apart from existing solutions. Not expected, but create genuine competitive advantage for operational services.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **In-process hot path (zero IPC)** | The Graph / Substreams add 50-200ms overhead from remote execution; in-process delivery achieves µs-ms order latency for live callbacks | High | WebSocket → Apply(state, event) → callbacks without leaving the process address space; SQLite write is async, off critical path |
| **Cheapest reconstruction path selection** | Library automatically picks: (1) pure cache replay (0 RPC), (2) cached snapshot + partial gap fetch (1 RPC), or (3) direct FetchState (1 RPC); client doesn't reason about this | High | Requires coverage table + snapshot index + cost model; distinctly non-trivial |
| **Reduced vs accumulated state duality** | One library handles both "fold events into a compact value" (balance) and "events ARE the state" (transfer history); most libraries handle only one | High | Reduced: in-memory, snapshotted. Accumulated: disk-backed, cursor-navigated. Shared ingestion infrastructure |
| **Client-owned connections** | Library doesn't connect to chains or exchanges; app provides connections through `EventSource`; enables co-located connections, custom auth, connection pooling the library can't know about | Med | Inversion of control vs. "configure your RPC URL" approach; critical for bridge daemons with custom node access |
| **Cross-chain event history comparison** | Bridge daemons need to find events in chain A absent from chain B; accumulated state + cursor iteration enables this without re-fetching | High | Not a query layer — it's cursor-navigable event history that supports diff operations |
| **Range queries with lightweight cursors** | `Range(from, to) → Iterator<S>` with cursors as (block, log_index) — a lightweight pointer, not a heavy DB cursor | Med | Key insight from blockchain_storage.md: results can span millions of blocks, must be lazy/streaming |
| **Adaptive snapshot density** | Fixed-interval snapshots waste disk when events are sparse and are insufficient when events are dense; adaptive density minimizes reconstruction cost | High | Heuristic: target reconstruction time budget; snapshot when replay cost exceeds threshold |
| **Secondary indexes on accumulated state** | Bridge diff requires "find event by crosschain_id not in other chain"; standard event log doesn't have this | High | Application-defined index columns in the events table; tradeoff against schema generality |
| **CEX API as first-class source** | Hyperliquid, dYdX, Binance use timestamp+sequence ordering; treating them equally with EVM/Bitcoin is unusual and directly serves DeFi infra users | Med | `EventSource` trait must accommodate non-block-indexed ordering schemes cleanly |
| **Async SQLite writes off hot path** | Most indexers make persistence synchronous with event processing; decoupling write from delivery preserves latency | Med | Risk: on crash, last N events may not be persisted; covered by coverage tracking + replay on restart |

---

## Anti-Features

Things to deliberately NOT build. Each represents a tempting scope expansion that would undermine the core value proposition.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **SQL query interface / analytics layer** | Dune Analytics exists; adding SQL means owning a query engine, schema migrations, index tuning — a different product entirely. Operational tracking ≠ analytics. | Expose the SQLite file directly; let users query it with their own tooling if needed |
| **Library-managed chain connections** | "Configure your RPC URL" couples the library to connection lifecycle, auth management, and fallback logic the app already owns. Bridge daemons have bespoke node access the library can't know about. | Client implements `EventSource` using its own connections; library calls through the interface |
| **Backward replay / generic Unapply** | Implementing a correct generic inverse of an arbitrary reducer is impossible without domain knowledge. The client's `Apply` function is a black box. | Forward-only: take snapshot at earlier point, replay forward. For historical point-in-time: `At(cursor)` uses this strategy |
| **Multi-process shared SQLite cache** | Writer coordination across processes requires WAL + advisory locks + retry logic; correctness is hard to guarantee and latency implications are severe | One SQLite file per app process; each app caches independently; schema is stable so multiple apps can co-exist with separate files |
| **WASM plugin system (v1)** | Premature abstraction; adds substantial complexity to the core without a validated use case. The option must be kept open (don't close it), not acted on. | Use Rust generics for state type; avoid serialization assumptions that would block WASM later |
| **Python/TypeScript bindings (v1)** | FFI (PyO3, napi-rs) is non-trivial to maintain; without a working Rust core, bindings are premature | Get the Rust API right first; FFI surface area is determined by the stable Rust API |
| **Reorg auto-rollback** | Different workloads need different strategies: balance trackers want rollback, bridge daemons may want to wait and retry, analytics may want to keep both chains | Detect and notify; client decides the rollback strategy for its state shape |
| **The Graph / Substreams as backend** | Both add minimum 50-200ms latency via remote execution; neither supports in-process usage, live push, or CEX sources; using them as backends defeats the hot-path requirement | Direct RPC via client-owned `EventSource`; Substreams could serve as an optional backfill accelerator in a future phase |
| **Generic subscription management (pub/sub bus)** | The library's concern is state reconstruction, not message routing. Adding a pub/sub layer bloats scope and competes with existing solutions (Redis Streams, NATS). | `OnChange(callback)` is a direct callback, not a broker; caller fans out if needed |
| **Block timestamp enrichment as core feature** | Fetching block timestamps requires an additional RPC call per block; many workloads don't need them; forcing it couples the library to block header fetching | Client can include timestamp in their event type if needed; `EventSource` returns client-typed events |

---

## Feature Dependencies

```
Coverage tracking → Gap stitching (can't detect gaps without knowing what's cached)
Coverage tracking → Crash recovery (determines what needs re-fetch on restart)
Anchor management → Crash recovery (anchor is the restart point)
Snapshot creation → Cheapest reconstruction path (snapshots enable the "0 RPC" path)
Snapshot creation → Crash recovery (snapshot + replay is faster than full replay from genesis)
Historical backfill → Gap stitching (gap stitching bridges backfill endpoint to subscription start)
Live event subscription → Gap stitching (gap stitching bridges backfill endpoint to subscription start)
Event deduplication → Gap stitching (overlap window produces duplicates that must be filtered)
Reorg detection → Reorg notification (detection is prerequisite for notification)
Source-agnostic event model → CEX API support (CEX needs non-block ordering)
Source-agnostic event model → Cross-chain event history comparison (comparison requires uniform cursor type)
Reduced vs accumulated duality → Range queries (accumulated state is what range queries traverse)
Cheapest reconstruction path → At(cursor) historical queries (path selection is the engine behind At())
```

### Dependency-Ordered Build Sequence

1. Source-agnostic event model (cursor types, EventSource trait)
2. Live event subscription + Historical backfill (both required before stitching)
3. Event deduplication + Coverage tracking
4. Gap stitching
5. Anchor management
6. Crash recovery (snapshot load → cached replay → live catch-up)
7. Snapshot creation
8. Cheapest reconstruction path (needs coverage + snapshots)
9. Reorg detection + notification
10. Reduced vs accumulated state duality
11. Range queries with cursors
12. In-process hot path optimization (async SQLite writes)
13. Differentiators: secondary indexes, adaptive snapshot density, CEX sources, cross-chain comparison

---

## MVP Recommendation

Prioritize (minimum viable operational service):

1. **Source-agnostic event model** — defines the interface everything else calls through
2. **Live subscription + Historical backfill + Gap stitching** — the core value; without these three together the library doesn't exist
3. **Event deduplication + Coverage tracking** — correctness requirements, not optional
4. **Crash recovery with snapshots** — without this the library is a toy; every restart is expensive
5. **Anchor management** — bridge daemon use case requires this from day one
6. **Reorg detection + notification** — EVM without reorg handling is incorrect for production

This MVP delivers the ERC20 balance tracker proof-of-concept (PROJECT.md) and the bridge daemon use case (Symbiosis's primary driver).

Defer to post-MVP:

- **Cheapest reconstruction path**: start with "always replay from last snapshot"; optimize path selection in a follow-on phase
- **At(cursor) historical queries**: add after core live tracking is stable
- **Range queries with cursors**: needed for accumulated state use cases; not needed for reduced state MVP
- **Secondary indexes**: needed for bridge diff; can land after schema is stable
- **Adaptive snapshot density**: start with fixed interval (every 1000 events); adaptive is an optimization
- **CEX API sources**: validates the source-agnostic model but not needed for ERC20 PoC
- **In-process async SQLite**: synchronous writes are simpler and correct; async is an optimization pass

---

## Sources

- Project documentation: `/home/nb/src/chainfold/blockchain_storage.md` (brainstorm + design summary)
- Project documentation: `/home/nb/src/chainfold/.planning/PROJECT.md` (requirements + constraints)
- Training data (confidence: MEDIUM): The Graph architecture, Substreams design, Ponder/Envio/Subsquid feature sets, ethers-rs provider watchers, alloy subscription APIs — all evaluated against August 2025 cutoff
- Note: Web access unavailable during research session; findings from comparable ecosystem tools have not been verified against current documentation. Core feature categorizations derive from the project's own design documents (HIGH confidence) and domain knowledge of blockchain indexing patterns (MEDIUM confidence).
