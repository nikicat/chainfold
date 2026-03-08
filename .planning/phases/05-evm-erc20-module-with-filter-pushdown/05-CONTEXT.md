# Phase 5: EVM ERC20 Module with Filter Pushdown - Context

**Gathered:** 2026-03-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract the ERC20 PoC (currently in tests/erc20_tests.rs and examples/erc20_tracker.rs) into a reusable library module at `src/modules/erc20.rs`. The module provides Transfer type, Erc20Machine (StateMachine impl), Erc20Codec, and Erc20Source (EventSource impl wrapping alloy). Filter pushdown enables account-scoped `eth_getLogs` queries so users can track a single account's balance without fetching all transfers. The example binary is thinned to CLI-only, importing from the library module.

</domain>

<decisions>
## Implementation Decisions

### Module scope & API surface
- Full module including Erc20Source (EventSource impl) — not just domain types
- Erc20Source accepts an alloy Provider directly (user constructs provider, passes it in)
- alloy provider types re-exported from the module so users don't need alloy as a direct dep
- Module lives at `src/modules/erc20.rs` (generic modules namespace, leaves room for future non-EVM modules)
- Example binary thinned to CLI-only: imports Transfer, Erc20Machine, Erc20Codec, Erc20Source from chainfold::modules::erc20

### Filter pushdown design
- Erc20Filter enum: `AllTransfers` | `Account([u8; 20])`
- Filter set at construction time, immutable — different filter = new source
- Account filter fires two eth_getLogs queries per chunk (from=addr, to=addr), merges and deduplicates internally by (block_number, log_index). Self-transfers handled via dedup.
- Filter lives in Source only — Erc20Machine stays a pure reducer that applies all events it receives
- One source per token, one account per filter. Multiple tokens or accounts = multiple sources.

### Feature flag & dependency strategy
- Single `evm` feature: `features = { evm = ["alloy", "tracing"] }`
- alloy and tracing become optional dependencies (currently dev-dependencies)
- alloy provider types re-exported from modules::erc20 for convenience
- Existing tests in tests/erc20_tests.rs migrated to import from chainfold::modules::erc20 (MockErc20Source stays in tests)

### State model
- State type is always `HashMap<[u8; 20], u128>` regardless of filter — Account filter just means fewer entries
- Filter enum is sufficient API — no typed constructors or separate builder types

### Claude's Discretion
- Internal chunking strategy for eth_getLogs (reuse existing chunk_size pattern from example)
- How to structure the alloy provider generic parameter (concrete type vs trait object)
- Error type design for Erc20Source
- Whether Erc20Codec uses serde_json (like PoC) or postcard for production
- Block hash fetching strategy (batch vs per-block)
- How to handle the subscribe() method for live streaming with filters

</decisions>

<specifics>
## Specific Ideas

- The existing example already has a working AlloyErc20Source with chunking, block hash fetching, and Transfer parsing — this is the reference implementation to extract and clean up
- eth_getLogs topic layout: [0]=event signature, [1]=from (indexed), [2]=to (indexed). AND-based between positions, so account filter needs two queries
- TRANSFER_TOPIC constant (keccak256 of "Transfer(address,address,uint256)") should be public in the module
- The example's `estimate_blocks_per_day()` and `--days` flag are CLI concerns, not module concerns

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tests/erc20_tests.rs`: Transfer, Erc20Machine, Erc20Codec, MockErc20Source — reference implementations to extract
- `examples/erc20_tracker.rs`: AlloyErc20Source with chunking, block hash fetching, Transfer log parsing — production source to extract
- `src/source.rs`: EventSource trait — Erc20Source must implement this
- `src/machine.rs`: StateMachine trait — Erc20Machine already implements this in tests
- `src/coordinator/mod.rs`: Codec trait — Erc20Codec already implements this in tests

### Established Patterns
- EventSource is generic over Cursor and Event — Erc20Source will be `EventSource<EvmCursor, Transfer>`
- Codec encodes payload only (not cursor) — Erc20Codec follows this
- Write-first invariant: Engine stores events before applying — module doesn't change this
- alloy 1.x requires stable toolchain (rustc 1.91+) — already handled in example

### Integration Points
- `src/modules/` directory is NEW — need to create mod.rs
- `src/lib.rs` needs `#[cfg(feature = "evm")] pub mod modules;`
- Cargo.toml: alloy moves from [dev-dependencies] to [dependencies] with optional = true
- tests/erc20_tests.rs: imports change from local definitions to chainfold::modules::erc20::*
- examples/erc20_tracker.rs: domain code removed, imports from library module

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 05-evm-erc20-module-with-filter-pushdown*
*Context gathered: 2026-03-09*
