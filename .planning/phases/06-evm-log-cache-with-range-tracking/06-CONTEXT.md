# Phase 6: EVM Log Cache with Range Tracking - Context

**Gathered:** 2026-03-10
**Status:** Ready for planning

<domain>
## Phase Boundary

Cache raw eth_getLogs results keyed by RPC-level filter parameters (contract address, topic sets, block range). Track scanned ranges — including empty ones — so a second run over the same range hits zero RPC calls. Merge adjacent/overlapping ranges. Serve cached logs to any reducer without re-fetching. Also cache block headers (get_block_by_number) to eliminate all redundant RPC on warm runs.

</domain>

<decisions>
## Implementation Decisions

### Cache placement
- Standalone `LogCache` struct with `get_or_fetch_logs(filter, range)` and `get_or_fetch_block_header(height)` methods
- Thin `CachingProvider<P>` adapter wraps LogCache and delegates non-cached methods to inner provider
- EVM-specific — lives in `modules::evm` (or adjacent to erc20 module). No premature generalization for non-EVM chains
- Caches both `get_logs` results and `get_block_by_number` responses
- Erc20Source receives a CachingProvider instead of a raw Provider — transparent from source's perspective

### Storage model
- Same SQLite file as block DAG storage — new tables added via schema migration v4
- Filter key: canonical hash (e.g., SHA-256 or BLAKE3) of the tuple (address, topic0, topic1, topic2, topic3) where null topics use a sentinel value. Compact 32-byte key for all lookups
- Cached logs stored as normalized fields with full reconstruction set:
  - block_number, log_index, address, topics (up to 4 BLOBs), data BLOB
  - block_hash, transaction_hash, transaction_index, removed flag
- Block headers cached separately: height → (hash, parent_hash, timestamp, + other header fields)

### Range tracking
- SQL table `scanned_ranges(filter_hash BLOB, start_block INTEGER, end_block INTEGER)`
- Inclusive-inclusive `[A, B]` ranges matching eth_getLogs fromBlock/toBlock semantics
- Eager merge-on-write: when recording a new scanned range, immediately merge with adjacent/overlapping ranges (end1 + 1 >= start2) in the same transaction
- Gap computation done in Rust: query overlapping ranges for filter, load into Vec, sweep algorithm to compute uncovered sub-ranges

### Cache invalidation
- Finality-aware: only record ranges as "scanned" when all blocks are below (chain_tip - finality_depth)
- Chain-specific finality presets: e.g., Ethereum mainnet = 64, Arbitrum = 0, etc. Extensible enum
- Blocks within the finality window: always go to RPC, never cached. Simple and correct
- Append-only — no TTL or size-based eviction. User controls by choosing block ranges or deleting the DB file

### Claude's Discretion
- Exact hash algorithm for filter key (SHA-256 vs BLAKE3 vs other)
- CachingProvider implementation details (which alloy Provider methods to delegate vs intercept)
- How to structure the chain presets enum
- Internal batch sizes for log insertion into SQLite
- Whether block header cache shares the scanned_ranges pattern or uses a simpler key-value table

</decisions>

<specifics>
## Specific Ideas

- eth_getLogs filter parameters map directly to cache key: (address, topic0, topic1, topic2, topic3) — not application-level filter enums like Erc20Filter
- The existing `Erc20Source.fetch_logs_in_range()` builds alloy Filter objects with exactly these parameters — the cache intercepts at the provider.get_logs() level below this
- Empty scan results are important: a range that returned zero logs must still be recorded as scanned to avoid re-querying
- The success criterion "two different reducers share the cache" works because both hit the same (address, topic0) key — the cache doesn't know about ERC20 vs anything else

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/storage/schema.rs`: migration chain (v1→v2→v3) — v4 adds cache tables
- `src/storage/sqlite.rs`: SqliteStorage with Mutex<Connection>, WAL mode, configure_connection — same patterns for cache tables
- `src/modules/erc20.rs`: Erc20Source.fetch_logs_in_range() — the call site that CachingProvider intercepts
- `src/modules/erc20.rs`: parse_transfer_log() shows the alloy Log fields available (topics, data, block_number, log_index)

### Established Patterns
- rusqlite with WAL mode, NORMAL synchronous, busy_timeout = 5000
- Schema migrations via rusqlite_migration crate
- Storage behind Mutex<Connection> for Sync requirement
- All storage methods are synchronous — async callers use spawn_blocking

### Integration Points
- `Erc20Source::new()` currently takes `provider: P` — will take `CachingProvider<P>` instead (or user wraps their provider)
- Schema migration v4 added to `src/storage/schema.rs`
- New cache module likely at `src/modules/evm_cache.rs` or `src/cache/` depending on whether it's EVM-module-scoped or storage-scoped
- Example `erc20_tracker.rs` wraps provider construction with CachingProvider

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 06-evm-log-cache-with-range-tracking*
*Context gathered: 2026-03-10*
