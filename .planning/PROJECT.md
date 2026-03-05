# Chainfold

## What This Is

A Rust library that hides the complexity of reconstructing and maintaining blockchain state from heterogeneous data sources. The client defines what state it cares about and how events affect that state. The library handles the entire lifecycle: initial state fetch, historical replay, live tracking, caching, gap recovery, and restart resilience.

## Core Value

Live state reconstruction from ordered events must be correct, gap-free, and survive restarts — without the client managing caching, subscriptions, or history stitching.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Define and track state as a function of: initial snapshot + ordered events applied forward
- [ ] Client provides three primitives: FetchState, FetchEvents, Apply (pure reducer)
- [ ] Library provides Current() — live-tracked state at canonical chain tip
- [ ] Library provides At(cursor) — historical state via cheapest reconstruction path
- [ ] Library provides OnChange(callback) — live notifications
- [ ] Hot path is in-process: events flow directly to Apply without IPC or serialization
- [ ] Block DAG storage in SQLite (WAL mode) — blocks with parent hashes, events per block, state snapshots per block
- [ ] On restart: find canonical tip → load nearest snapshot → replay canonical chain forward → catch up → switch to live
- [ ] Gap recovery between state fetch and subscription handled automatically
- [ ] Reorg handled transparently via block DAG — canonical tip moves to a different branch, library re-derives state from fork point
- [ ] Source-agnostic EventSource trait supporting EVM, Bitcoin, and CEX ordering schemes
- [ ] ERC20 balance tracking as the first proof-of-concept workload

### Out of Scope

- WASM plugin system — future possibility, not v1 requirement
- Python/TypeScript bindings — Rust chosen for clean FFI story, bindings come later
- Multi-process shared SQLite cache — each app owns its cache file
- Backward replay / generic Unapply — forward-only, use snapshot + replay for historical
- SQL query interface / analytics layer — this is event sourcing, not Dune
- The Graph / Substreams integration — wrong abstraction for operational tracking
- Library-enforced persistent data structures — structural sharing (im crate) is user's choice via State type, not a library requirement

## Context

Built at Symbiosis to solve recurring pain in blockchain services:
- Bridge daemons need cross-chain event history comparison with anchor management
- Balance trackers need live state with gap-free subscription stitching
- Both scenarios involve redundant RPC fetches begging for a caching layer

Current approach at Symbiosis: each service hand-rolls subscription management, gap recovery, history fetching, and caching. This library extracts the common pattern.

Two state shapes the library must support:
- **Reduced state** — events fold into a compact value (balance, pool reserves). Held in memory.
- **Accumulated state** — events ARE the state (transfer history, order fills). Stored on disk, accessed via cursors.

Existing solutions (The Graph, Substreams) evaluated and rejected for the hot path due to latency overhead (50-200ms minimum) and lack of in-process usage / live push / anchor support.

## Constraints

- **Language**: Rust — no GC for clean FFI to Python/TS/WASM later
- **Hot path latency**: ms-order — no IPC, no serialization on the critical path
- **Persistence**: SQLite with WAL mode — cache, not transport
- **Connection ownership**: App owns connections to chains/exchanges, library doesn't connect
- **Replay direction**: Forward-only — no generic Unapply

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Rust instead of Go | Clean FFI (no GC), future WASM compatibility, strong blockchain ecosystem (alloy) | — Pending |
| Block DAG model for state | State is tracked per block hash in a DAG (like git). Reorgs are branch switches, not errors. Library re-derives state from fork point transparently. | — Active |
| Structural sharing is user-side | `StateMachine::State: Clone` — user picks `im::HashMap` for cheap fork-safe clones, or `HashMap` if reorgs are rare. Library doesn't enforce. | — Active |
| Reorg handled by library, not client | Library maintains DAG, tracks canonical tip, re-derives state on branch switch. Client sees transparent state updates via Current(). No reorg callback needed. | — Active (supersedes old "notify client" decision) |
| One SQLite file per app process | Avoids writer coordination complexity; each app caches independently | — Pending |
| WASM is future door, not v1 | Keep the option open (avoid closing it) but don't design around it now | — Pending |
| Generics for state type in Rust | Natural fit; WASM boundary serialization is a future-self problem | — Pending |

---
*Last updated: 2026-03-02 after initialization*
