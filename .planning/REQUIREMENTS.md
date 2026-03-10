# Requirements: Chainfold

**Defined:** 2026-03-03
**Revised:** 2026-03-05 (DAG architecture)
**Core Value:** Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Core Abstraction

- [x] **CORE-01**: Library exposes a generic `EventSource` trait abstracting over EVM, Bitcoin, and CEX ordering schemes (blockNum+logIndex, blockHeight+txIndex, timestamp+seqId)
- [x] **CORE-02**: Library exposes a `StateMachine` trait where client provides FetchState(at cursor), FetchEvents(from, to cursor), and Apply(state, event) as a pure reducer
- [x] **CORE-03**: Cursor types implement `Ord + Clone + Serialize + Deserialize` enabling generic ordering across all source types
- [x] **CORE-04**: Library supports reduced state — events fold into a compact in-memory value (e.g., balance, pool reserves)
- [x] **CORE-05**: Library supports accumulated state — events stored on disk, accessed via cursors and anchors (e.g., transfer history)

### Event Ingestion

- [x] **INGEST-01**: Library consumes live events from an EventSource subscription and applies them to state in real-time
- [x] **INGEST-02**: Library fetches historical events via EventSource in chunked range queries respecting RPC provider limits
- [x] **INGEST-03**: Library stitches the gap between cached blocks and live subscription start without missing or duplicating events
- [x] **INGEST-04**: Library deduplicates events by (block_hash, cursor) so overlapping backfill and subscription windows don't corrupt state
- [x] **INGEST-05**: Hot path is in-process: events flow from source through Apply to callbacks without IPC or serialization on the critical path

### Persistence & Recovery

- [x] **PERSIST-01**: Library persists blocks (with parent hash), events (per block), and state snapshots (per block hash) in SQLite with WAL mode
- [x] **PERSIST-02**: Library tracks the block DAG — which blocks exist, their parent relationships, and which branch is canonical
- [x] **PERSIST-03**: Library recovers from crash: find canonical tip, load nearest snapshot on canonical chain, replay forward along canonical chain, catch up via FetchEvents, switch to live
- [x] **PERSIST-04**: Library creates periodic state snapshots (client-owned serialization) at block boundaries to bound replay cost
- [x] **PERSIST-05**: Library maintains named anchor positions (persistent cursors) updated atomically with block processing
- [x] **PERSIST-06**: Library selects cheapest reconstruction path for historical queries: cached-only replay (0 RPC), partial gap fetch (1 RPC), or direct FetchState (1 RPC)

### Chain Safety

- [x] **SAFETY-01**: Library maintains a block DAG and tracks which block is the canonical tip as reported by EventSource
- [x] **SAFETY-02**: When the canonical tip changes to a different branch (reorg), the library transparently re-derives state from the fork point on the new canonical chain — client's Current() always reflects canonical state without special reorg handling
- [x] **SAFETY-03**: Library handles WebSocket disconnection with exponential backoff, re-subscribe from last anchor, and gap refetch

### Query API

- [x] **QUERY-01**: Library exposes `Current() → S` returning live-tracked state at canonical chain tip
- [x] **QUERY-02**: Library exposes `At(cursor) → S` returning historical state via cheapest reconstruction path
- [x] **QUERY-03**: Library exposes `Range(from, to) → Iterator<S>` for materialized state across a cursor range with lightweight cursor-based iteration
- [x] **QUERY-04**: Library exposes `OnChange(callback)` for live state change notifications

### Proof of Concept

- [x] **POC-01**: ERC20 balance tracker demonstrates end-to-end: subscribe to Transfer events, reconstruct balance, survive restarts, handle reorgs transparently

### EVM Module

- [x] **MOD-01**: EVM feature flag is fully optional — `cargo check` (no features) compiles without pulling alloy
- [x] **MOD-02**: `chainfold::modules::erc20` exports Transfer, Erc20Machine, Erc20Codec, Erc20Source, Erc20Filter, and alloy re-exports
- [x] **MOD-03**: `Erc20Source` with `Erc20Filter::Account(addr)` fires two scoped eth_getLogs queries per chunk and deduplicates results
- [x] **MOD-04**: Existing ERC20 integration tests pass using imports from the library module
- [x] **MOD-05**: Example binary is CLI-only — all domain types imported from the library module

### Log Cache

- [x] **CACHE-01**: Library caches raw eth_getLogs results keyed by (contract address, topic0, topic1, topic2, topic3) — matching RPC parameters, not application-level filter abstractions
- [x] **CACHE-02**: Library tracks scanned block ranges per topic set — a range that returned zero logs is recorded as "scanned, empty" so subsequent queries skip RPC
- [x] **CACHE-03**: Adjacent and overlapping scanned ranges merge automatically (e.g., [100,200] + [201,300] → [100,300]) — no fragmentation in the range index
- [x] **CACHE-04**: When a requested range partially overlaps cached ranges, the library computes and fetches only the uncovered sub-ranges from RPC
- [x] **CACHE-05**: Cached logs are reusable across reducers — two different state machines consuming the same (contract, topics) logs share the cache without re-fetching

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Multi-Source

- **MSRC-01**: CEX APIs (Hyperliquid, Binance) work as first-class EventSource with timestamp+seqId cursors
- **MSRC-02**: Bitcoin EventSource adapter using bitcoincore-rpc with blockHeight+txIndex ordering

### Advanced Query

- **ADVQ-01**: Cross-chain event history comparison — find events in source chain absent from destination chain
- **ADVQ-02**: Secondary indexes on accumulated state (e.g., crosschain_id for bridge diff)

### Optimization

- **OPT-01**: Adaptive snapshot density — snapshot frequency based on reconstruction time budget, not fixed interval
- **OPT-02**: Async SQLite writes off hot path — decouple persistence from event delivery for lower latency

### Bindings

- **BIND-01**: Python bindings via PyO3
- **BIND-02**: TypeScript bindings via napi-rs
- **BIND-03**: WASM compilation target for state reconstruction modules

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| SQL query interface / analytics | Dune exists; operational tracking ≠ analytics. Users can query SQLite directly. |
| Library-managed chain connections | Client owns connections via EventSource; library can't know about custom auth, co-located nodes |
| Backward replay / generic Unapply | Impossible without domain knowledge; use snapshot + forward replay instead |
| Multi-process shared SQLite | Writer coordination too complex; each app owns its cache file |
| Library-enforced persistent data structures | Structural sharing (im crate) is user's choice via State: Clone, not library-mandated |
| The Graph / Substreams as backend | 50-200ms latency overhead disqualifies them from hot path |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 1 | Complete (01-01) |
| CORE-02 | Phase 1 | Complete (01-02) |
| CORE-03 | Phase 1 | Complete (01-01) |
| CORE-04 | Phase 1 | Complete (01-02) |
| CORE-05 | Phase 1 | Complete (01-02) |
| INGEST-01 | Phase 3 | Complete |
| INGEST-02 | Phase 2 | Complete (02-04) |
| INGEST-03 | Phase 2 | Complete (02-04) |
| INGEST-04 | Phase 2 | Complete (02-01, 02-02, 02-03) |
| INGEST-05 | Phase 3 | Complete (03-01, 03-03) |
| PERSIST-01 | Phase 2 | Complete (02-01, 02-02, 02-03) |
| PERSIST-02 | Phase 2 | Complete (02-01, 02-02, 02-03) |
| PERSIST-03 | Phase 2 | Complete (02-04) |
| PERSIST-04 | Phase 2 | Complete (02-03) |
| PERSIST-05 | Phase 2 | Complete (02-01, 02-02, 02-03) |
| PERSIST-06 | Phase 4 | Complete |
| SAFETY-01 | Phase 3 | Complete (03-02: process_or_reorg, canonical tip tracking, find_common_ancestor) |
| SAFETY-02 | Phase 3 | Complete (03-02: handle_reorg, snapshot+replay re-derivation, initial_state for no-snapshot) |
| SAFETY-03 | Phase 3 | Complete |
| QUERY-01 | Phase 4 | Complete |
| QUERY-02 | Phase 4 | Complete |
| QUERY-03 | Phase 4 | Complete |
| QUERY-04 | Phase 4 | Complete |
| POC-01 | Phase 4, Plan 3 | Complete |
| MOD-01 | Phase 5 | Complete (05-01) |
| MOD-02 | Phase 5 | Complete (05-01) |
| MOD-03 | Phase 5 | Complete (05-01) |
| MOD-04 | Phase 5 | Complete (05-01) |
| MOD-05 | Phase 5 | Complete (05-02) |
| CACHE-01 | Phase 6 | Complete |
| CACHE-02 | Phase 6 | Complete |
| CACHE-03 | Phase 6 | Complete |
| CACHE-04 | Phase 6 | Complete |
| CACHE-05 | Phase 6 | Complete |

**Coverage:**
- v1 requirements: 34 total
- Mapped to phases: 34
- Unmapped: 0

---
*Requirements defined: 2026-03-03*
*Last updated: 2026-03-09 — tech debt cleanup: stale Phase 2/3 statuses updated to Complete, MOD-01 through MOD-05 added to traceability table*
