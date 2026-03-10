# Phase 6: EVM Log Cache with Range Tracking - Research

**Researched:** 2026-03-10
**Domain:** SQLite caching layer for eth_getLogs results with range tracking and alloy Provider interception
**Confidence:** HIGH

## Summary

Phase 6 adds a transparent caching layer between `Erc20Source` and the Ethereum RPC. The cache stores raw `eth_getLogs` results keyed by a hash of the RPC-level filter parameters (address, topic0-3), tracks which block ranges have been scanned (even if empty), and merges adjacent/overlapping ranges to prevent fragmentation. Block headers from `get_block_by_number` are also cached.

The implementation reuses the project's established patterns: same SQLite file with a v4 schema migration, `Mutex<Connection>`, WAL mode, and synchronous methods called via `spawn_blocking` from async code. The `CachingProvider<P>` wraps an inner alloy `Provider` and intercepts `get_logs` calls. Block header caching uses a dedicated method since `get_block_by_number` returns an `EthGetBlock` builder type that is tightly coupled to alloy's RPC internals and cannot be cleanly intercepted at the trait level.

**Primary recommendation:** Use BLAKE3 for filter key hashing (faster than SHA-256, pure Rust, 32-byte output). Implement `CachingProvider<P>: Provider` overriding only `get_logs`, with a separate `get_or_fetch_block_header()` method for block header caching. Range merging uses an eager merge-on-write SQL approach within the same transaction as log insertion.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- Standalone `LogCache` struct with `get_or_fetch_logs(filter, range)` and `get_or_fetch_block_header(height)` methods
- Thin `CachingProvider<P>` adapter wraps LogCache and delegates non-cached methods to inner provider
- EVM-specific — lives in `modules::evm` (or adjacent to erc20 module). No premature generalization for non-EVM chains
- Caches both `get_logs` results and `get_block_by_number` responses
- Erc20Source receives a CachingProvider instead of a raw Provider — transparent from source's perspective
- Same SQLite file as block DAG storage — new tables added via schema migration v4
- Filter key: canonical hash of the tuple (address, topic0, topic1, topic2, topic3) where null topics use a sentinel value. Compact 32-byte key for all lookups
- Cached logs stored as normalized fields: block_number, log_index, address, topics (up to 4 BLOBs), data BLOB, block_hash, transaction_hash, transaction_index, removed flag
- Block headers cached separately: height -> (hash, parent_hash, timestamp, + other header fields)
- SQL table `scanned_ranges(filter_hash BLOB, start_block INTEGER, end_block INTEGER)` with inclusive-inclusive [A, B] ranges
- Eager merge-on-write: record new range and merge adjacent/overlapping in same transaction
- Gap computation in Rust: query overlapping ranges, load into Vec, sweep algorithm for uncovered sub-ranges
- Finality-aware: only record as "scanned" when blocks are below (chain_tip - finality_depth)
- Chain-specific finality presets: Ethereum mainnet = 64, Arbitrum = 0, etc.
- Blocks within finality window: always go to RPC, never cached
- Append-only — no TTL or size-based eviction

### Claude's Discretion
- Exact hash algorithm for filter key (SHA-256 vs BLAKE3 vs other)
- CachingProvider implementation details (which alloy Provider methods to delegate vs intercept)
- How to structure the chain presets enum
- Internal batch sizes for log insertion into SQLite
- Whether block header cache shares the scanned_ranges pattern or uses a simpler key-value table

### Deferred Ideas (OUT OF SCOPE)
None — discussion stayed within phase scope

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CACHE-01 | Library caches raw eth_getLogs results keyed by (contract address, topic0-3) matching RPC parameters | Filter key hashing, cached_logs table schema, CachingProvider get_logs override |
| CACHE-02 | Library tracks scanned block ranges per topic set — empty ranges recorded as "scanned, empty" | scanned_ranges table, range recording after every fetch (including zero-result fetches) |
| CACHE-03 | Adjacent and overlapping scanned ranges merge automatically | Eager merge-on-write SQL pattern, merge algorithm in Architecture Patterns |
| CACHE-04 | Partial overlap computes and fetches only uncovered sub-ranges | Gap computation sweep algorithm, CachingProvider orchestration logic |
| CACHE-05 | Cached logs reusable across reducers — same (contract, topics) logs shared | Filter key is RPC-level (address+topics), not reducer-specific; any reducer with same filter shares cache |

</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| rusqlite | 0.34 | SQLite access (same as existing) | Already in project; WAL mode, bundled |
| rusqlite_migration | 2 | Schema migration v4 | Already in project; migration chain v1->v2->v3->v4 |
| blake3 | 1.8 | Filter key hashing | 5-10x faster than SHA-256, pure Rust, 32-byte output, no-std compatible |
| alloy | 1 (1.7.3 in lockfile) | Provider trait, Filter, Log types | Already in project; CachingProvider implements Provider trait |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| async-trait | 0.1 | Provider trait uses async_trait | Already in project; CachingProvider impl needs it |
| tracing | 0.1 | Cache hit/miss logging | Already optional dep behind evm feature flag |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| blake3 | sha2 (SHA-256) | SHA-256 is more widely known but 5-10x slower. For a cache key hash, collision resistance beyond 128 bits is irrelevant — BLAKE3 is strictly better here |
| blake3 | Direct tuple comparison | No hashing — store (address, t0, t1, t2, t3) as separate columns with composite index. Simpler but 5 BLOB columns per scanned_ranges row vs 1 compact 32-byte hash. Hash approach is better for indexed lookups |

**Installation:**
```bash
cargo add blake3@1.8
```

**Note:** `blake3` should be an optional dependency gated behind the `evm` feature flag, same as `alloy`.

## Architecture Patterns

### Recommended Project Structure
```
src/
├── modules/
│   ├── mod.rs              # cfg(feature = "evm") pub mod erc20; pub mod evm_cache;
│   ├── erc20.rs            # existing — Erc20Source modified to use CachingProvider
│   └── evm_cache.rs        # NEW — LogCache, CachingProvider, FinalityConfig
├── storage/
│   ├── schema.rs           # v4 migration added
│   └── sqlite.rs           # unchanged
```

### Pattern 1: CachingProvider<P> as Provider Wrapper

**What:** A struct that wraps an inner `P: Provider` and a `LogCache`, implementing the alloy `Provider` trait. It overrides `get_logs` to check the cache first, and provides an additional `get_or_fetch_block_header()` method.

**Why not intercept get_block_by_number:** The alloy `Provider::get_block_by_number` returns `EthGetBlock<N::BlockResponse>`, a builder type tightly coupled to `RpcCall` and `ClientRef`. Overriding it to return cached data would require constructing an `EthGetBlock` with a fake RPC call, which is brittle and version-dependent. Instead, block header caching is exposed as a separate method.

**Transparency approach:** `CachingProvider<P>` implements `Provider` (delegates `root()` to inner). The `get_logs` override is fully transparent — `Erc20Source.fetch_logs_in_range()` calls `self.provider.get_logs(&filter)` and hits the cache automatically. For block headers, `Erc20Source.fetch_blocks()` needs a minor change: instead of `self.provider.get_block_by_number(height.into()).await`, it calls a method on the provider that checks the block header cache. This requires `Erc20Source` to know it holds a `CachingProvider`, not just any `Provider`.

**Design choice:** Accept the slight transparency break for block headers. The `Erc20Source<P: Provider>` generic becomes `Erc20Source<P: Provider + BlockHeaderCache>` or `Erc20Source` takes a concrete `CachingProvider<P>`. The former is cleaner — define a small `BlockHeaderCache` trait with a default no-op impl for raw providers.

**Example:**
```rust
// Source: alloy-provider 1.7.3 trait.rs — Provider trait pattern
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::{Filter, Log};
use alloy::transports::TransportResult;
use alloy::network::{Ethereum, Network};

pub struct CachingProvider<P: Provider> {
    inner: P,
    cache: LogCache,
    finality: FinalityConfig,
}

#[async_trait::async_trait]
impl<P: Provider + Clone> Provider for CachingProvider<P> {
    fn root(&self) -> &RootProvider {
        self.inner.root()
    }

    // Override get_logs — this is the primary cache intercept point
    async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
        // Extract filter parameters, compute filter_hash, check cache, etc.
        // Falls through to self.inner.get_logs() for uncached sub-ranges
        todo!()
    }
}
```

### Pattern 2: Filter Key Hashing

**What:** Compute a 32-byte BLAKE3 hash from (address, topic0, topic1, topic2, topic3) to use as the cache key for scanned ranges and cached logs.

**Sentinel for null topics:** Use `[0xFF; 32]` (all-ones) as sentinel for "no topic filter". This cannot collide with a real topic hash (which would be a keccak256 output and theoretically could be all-ones, but the probability is negligible — 2^-256).

**Example:**
```rust
fn compute_filter_hash(
    address: &[u8; 20],
    topics: &[Option<[u8; 32]>; 4],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(address);
    let sentinel = [0xFF_u8; 32];
    for topic in topics {
        match topic {
            Some(t) => hasher.update(t),
            None => hasher.update(&sentinel),
        };
    }
    *hasher.finalize().as_bytes()
}
```

### Pattern 3: Eager Range Merge-on-Write

**What:** When recording a new scanned range [A, B], immediately merge with all adjacent and overlapping ranges in the same SQL transaction.

**Algorithm:**
1. Within the INSERT transaction, query all existing ranges for this filter_hash where `start_block <= B + 1 AND end_block >= A - 1` (catches overlapping and adjacent)
2. Compute merged range: `min(A, all_starts)` to `max(B, all_ends)`
3. DELETE the old overlapping/adjacent ranges
4. INSERT the single merged range

**Example SQL within transaction:**
```sql
-- Step 1: Find overlapping/adjacent ranges
SELECT id, start_block, end_block FROM scanned_ranges
WHERE filter_hash = ?1 AND start_block <= ?3 AND end_block >= ?2;
-- ?2 = new_start - 1 (for adjacency), ?3 = new_end + 1

-- Step 2: Delete old ranges (by collected IDs)
DELETE FROM scanned_ranges WHERE id IN (...);

-- Step 3: Insert merged range
INSERT INTO scanned_ranges (filter_hash, start_block, end_block) VALUES (?1, ?2, ?3);
```

### Pattern 4: Gap Computation Sweep

**What:** Given a requested range [from, to] and a set of scanned ranges for a filter, compute the uncovered sub-ranges that need RPC fetching.

**Algorithm:**
1. Query scanned_ranges for this filter_hash, ordered by start_block
2. Load into `Vec<(u64, u64)>`
3. Sweep: walk through sorted ranges, track coverage, emit gaps

**Example:**
```rust
fn compute_gaps(
    requested: (u64, u64),  // (from, to) inclusive
    scanned: &[(u64, u64)], // sorted by start_block, non-overlapping (post-merge)
) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();
    let mut cursor = requested.0;
    for &(start, end) in scanned {
        if start > cursor {
            // Gap before this scanned range
            gaps.push((cursor, start - 1));
        }
        cursor = cursor.max(end + 1);
        if cursor > requested.1 {
            break;
        }
    }
    if cursor <= requested.1 {
        gaps.push((cursor, requested.1));
    }
    gaps
}
```

### Pattern 5: Finality-Aware Caching

**What:** Only cache blocks that are finalized (below chain_tip - finality_depth). Blocks within the finality window are always fetched fresh from RPC.

**Implementation:**
```rust
pub enum FinalityConfig {
    /// Ethereum mainnet: 64 blocks
    EthereumMainnet,
    /// Arbitrum: instant finality
    Arbitrum,
    /// Custom finality depth
    Custom(u64),
}

impl FinalityConfig {
    pub fn depth(&self) -> u64 {
        match self {
            Self::EthereumMainnet => 64,
            Self::Arbitrum => 0,
            Self::Custom(d) => *d,
        }
    }
}
```

When processing a request for range [from, to]:
1. Fetch current chain tip from provider
2. Compute finality boundary: `safe_to = min(to, chain_tip - finality_depth)`
3. For [from, safe_to]: use cache (check scanned ranges, fetch gaps)
4. For [safe_to+1, to]: always fetch from RPC, never record as scanned

### Anti-Patterns to Avoid
- **Caching unfinalized blocks:** Recording ranges that include blocks within the finality window. These blocks may be reorged, making cached logs stale. Always clamp cached ranges to below the finality boundary.
- **Lazy range merge:** Deferring merge to read time creates fragmentation in scanned_ranges table that grows unbounded. Merge eagerly on write.
- **Application-level filter keys:** Keying cache by `Erc20Filter::AllTransfers` or similar enums. The cache must use RPC-level parameters so different application filters that produce the same eth_getLogs query share the cache.
- **Separate SQLite file for cache:** The context decision specifies same SQLite file as block DAG storage. This avoids cross-file coordination and simplifies the schema migration chain.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hash function | Custom hash combining | blake3 crate | Cryptographic quality, fast, well-tested, 32-byte output |
| Filter parameter extraction | Manual Filter field parsing | alloy Filter's address/topic accessors | Filter struct may change across alloy versions; use its API |
| Log serialization for cache | Custom binary format | Store individual fields in SQL columns | Queryable, debuggable, no versioning concerns |
| Schema migration | Manual CREATE TABLE IF NOT EXISTS | rusqlite_migration v4 in existing chain | Consistent with v1->v2->v3 pattern; handles upgrades |

**Key insight:** The cache stores denormalized log fields (not serialized blobs) because logs need to be reconstructed into alloy `Log` objects. Storing as individual columns avoids serialization format versioning and makes the cache inspectable with standard SQLite tools.

## Common Pitfalls

### Pitfall 1: Filter Parameter Extraction from alloy Filter
**What goes wrong:** alloy's `Filter` struct uses complex types for topics (can be single, list, or null). Extracting the exact (address, topic0, topic1, topic2, topic3) tuple for hashing requires careful handling of all variants.
**Why it happens:** `Filter::topic0()` returns `Option<ValueOrArray<B256>>` — there are multiple representations.
**How to avoid:** When CachingProvider intercepts `get_logs`, extract parameters from the Filter using its accessor methods. For the cache key, flatten to (single address, single topic per slot or null sentinel). If a filter has multiple addresses or multiple values per topic slot, skip caching and delegate directly to inner provider — these are rare edge cases.
**Warning signs:** Cache misses on filters that should hit. Test with exact Filter objects from `Erc20Source.fetch_logs_in_range()`.

### Pitfall 2: Off-by-One in Range Merging
**What goes wrong:** Adjacent ranges [100,200] and [201,300] not merging, or ranges [100,200] and [202,300] incorrectly merging (leaving block 201 uncovered).
**Why it happens:** The adjacency check `end1 + 1 >= start2` is easy to get wrong, especially with the SQL query bounds.
**How to avoid:** Write exhaustive unit tests for: overlapping ranges, adjacent ranges, gap-of-1 ranges, contained ranges, identical ranges. The SQL query for merge candidates should use `start_block <= new_end + 1 AND end_block >= new_start - 1`.
**Warning signs:** scanned_ranges table growing without bound; repeated RPC calls for already-scanned blocks.

### Pitfall 3: Reconstructing alloy Log from Cached Fields
**What goes wrong:** The cached log fields must be reconstructed into an `alloy::rpc::types::Log` that is identical to what the RPC returned. Missing fields or wrong types cause downstream parsing failures.
**Why it happens:** `Log` has many fields (block_number, log_index, transaction_hash, etc.) and some are `Option`. The reconstruction must match exactly.
**How to avoid:** Store ALL fields present in the original Log. When storing, extract each field. When loading, reconstruct with exact types. Write a round-trip test: fetch real logs, cache them, load from cache, assert equality.
**Warning signs:** `parse_transfer_log()` returning None for cached logs that worked when fresh from RPC.

### Pitfall 4: Mutex Contention with Shared SQLite Connection
**What goes wrong:** The LogCache and SqliteStorage share the same database file but use separate connections (LogCache has its own `Mutex<Connection>`). This is fine because SQLite WAL supports multiple connections. But if they shared the same `Mutex<Connection>`, cache operations would block storage operations.
**Why it happens:** The CONTEXT says "same SQLite file." This means same file, not same connection.
**How to avoid:** LogCache opens its own `Connection` to the same file path. WAL mode supports concurrent readers and serialized writers. The `busy_timeout = 5000` pragma handles brief write contention.
**Warning signs:** Deadlocks if you accidentally try to share the `Mutex<Connection>` from SqliteStorage.

### Pitfall 5: Finality Boundary Race Condition
**What goes wrong:** Between checking the chain tip and recording scanned ranges, new blocks may arrive, shifting the finality boundary.
**Why it happens:** The chain tip is a moving target.
**How to avoid:** This is acceptable — the worst case is caching a range that's slightly into the finality window. On the next query, the boundary will have moved further, and those blocks are now truly final. The window is conservative (64 blocks for Ethereum = ~13 minutes), so a few seconds of race is negligible.
**Warning signs:** None — this is a theoretical concern, not a practical one.

## Code Examples

### Schema v4 Migration
```rust
// Added to src/storage/schema.rs migration chain
// v4 — log cache tables
M::up(
    "
    CREATE TABLE cached_logs (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        filter_hash       BLOB    NOT NULL,
        block_number      INTEGER NOT NULL,
        log_index         INTEGER NOT NULL,
        address           BLOB    NOT NULL,
        topic0            BLOB,
        topic1            BLOB,
        topic2            BLOB,
        topic3            BLOB,
        data              BLOB    NOT NULL,
        block_hash        BLOB    NOT NULL,
        transaction_hash  BLOB    NOT NULL,
        transaction_index INTEGER NOT NULL,
        removed           INTEGER NOT NULL DEFAULT 0,
        UNIQUE(filter_hash, block_number, log_index)
    );
    CREATE INDEX idx_cached_logs_filter_block
        ON cached_logs(filter_hash, block_number);

    CREATE TABLE scanned_ranges (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        filter_hash  BLOB    NOT NULL,
        start_block  INTEGER NOT NULL,
        end_block    INTEGER NOT NULL
    );
    CREATE INDEX idx_scanned_ranges_filter
        ON scanned_ranges(filter_hash, start_block, end_block);

    CREATE TABLE cached_block_headers (
        height       INTEGER PRIMARY KEY,
        hash         BLOB    NOT NULL,
        parent_hash  BLOB    NOT NULL,
        timestamp    INTEGER NOT NULL
    );
    ",
),
```

### LogCache Core Methods
```rust
pub struct LogCache {
    conn: Mutex<Connection>,
}

impl LogCache {
    /// Open a log cache using the same database file as block DAG storage.
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        // Migrations already run by SqliteStorage::open() on the same file.
        // LogCache just opens a second connection.
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Record a scanned range and merge with adjacent/overlapping ranges.
    pub fn record_scanned_range(
        &self,
        filter_hash: &[u8; 32],
        start: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        // Find merge candidates: adjacent or overlapping
        let mut stmt = tx.prepare_cached(
            "SELECT id, start_block, end_block FROM scanned_ranges
             WHERE filter_hash = ?1 AND start_block <= ?3 AND end_block >= ?2"
        )?;
        // ?2 = start - 1 (adjacency), ?3 = end + 1
        let candidates: Vec<(i64, u64, u64)> = stmt
            .query_map(
                rusqlite::params![&filter_hash[..], start.saturating_sub(1) as i64, (end + 1) as i64],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64)),
            )?
            .collect::<Result<_, _>>()?;

        let mut merged_start = start;
        let mut merged_end = end;
        let mut ids_to_delete = Vec::new();

        for (id, s, e) in &candidates {
            merged_start = merged_start.min(*s);
            merged_end = merged_end.max(*e);
            ids_to_delete.push(*id);
        }

        // Delete old ranges
        for id in &ids_to_delete {
            tx.execute("DELETE FROM scanned_ranges WHERE id = ?1", [id])?;
        }

        // Insert merged range
        tx.execute(
            "INSERT INTO scanned_ranges (filter_hash, start_block, end_block) VALUES (?1, ?2, ?3)",
            rusqlite::params![&filter_hash[..], merged_start as i64, merged_end as i64],
        )?;

        tx.commit()?;
        Ok(())
    }
}
```

### Batch Log Insertion
```rust
/// Insert cached logs in batches within a single transaction.
/// Recommended batch size: 500-1000 rows per prepare_cached cycle.
pub fn insert_logs(
    &self,
    filter_hash: &[u8; 32],
    logs: &[Log],
) -> Result<(), StorageError> {
    let conn = self.conn.lock().unwrap();
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO cached_logs
             (filter_hash, block_number, log_index, address, topic0, topic1, topic2, topic3,
              data, block_hash, transaction_hash, transaction_index, removed)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
        )?;
        for log in logs {
            let topics = log.topics();
            stmt.execute(rusqlite::params![
                &filter_hash[..],
                log.block_number.unwrap_or(0) as i64,
                log.log_index.unwrap_or(0) as i64,
                log.address().as_slice(),
                topics.get(0).map(|t| t.as_slice()),
                topics.get(1).map(|t| t.as_slice()),
                topics.get(2).map(|t| t.as_slice()),
                topics.get(3).map(|t| t.as_slice()),
                log.data().data.as_ref(),
                log.block_hash.map(|h| h.0.to_vec()),
                log.transaction_hash.map(|h| h.0.to_vec()),
                log.transaction_index.unwrap_or(0) as i64,
                log.removed as i64,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Each Erc20Source call hits RPC | CachingProvider intercepts, serves from SQLite | Phase 6 | Zero RPC for warm ranges |
| Block headers fetched per-block in loop | Block header cache eliminates redundant get_block_by_number | Phase 6 | Massive speedup for multi-reducer scenarios |
| Erc20Filter determines cache scope | Cache key is RPC-level (address, topics) — filter-independent | Phase 6 | Different reducers share cache automatically |

## Open Questions

1. **LogCache connection sharing with SqliteStorage**
   - What we know: Same SQLite file, separate connections. WAL mode supports this.
   - What's unclear: Should LogCache open its own connection, or should SqliteStorage expose its connection? If LogCache opens its own, it doesn't run migrations (already run by SqliteStorage).
   - Recommendation: LogCache opens its own connection. Migrations are idempotent (rusqlite_migration checks current version). Both can call `migrations().to_latest()` safely. Simpler than sharing connections.

2. **CachingProvider transparency for block headers**
   - What we know: `Provider::get_block_by_number` returns `EthGetBlock` builder, hard to intercept.
   - What's unclear: How much can we keep Erc20Source unaware of CachingProvider?
   - Recommendation: Define a `BlockHeaderCache` trait with `async fn get_or_fetch_block_header(&self, height: u64) -> Result<Option<Block>>`. Implement it on CachingProvider (with cache) and provide a blanket/default impl for any `P: Provider` (no cache, just delegates). Erc20Source gains bound `P: Provider + BlockHeaderCache`. Minimal source change.

3. **alloy Log reconstruction fidelity**
   - What we know: alloy `Log` is a complex type with many optional fields.
   - What's unclear: Which fields are required for `parse_transfer_log()` to work. Need to verify round-trip.
   - Recommendation: Store the full field set. Write a round-trip test early. The fields used by `parse_transfer_log` are: `topics()`, `block_number`, `log_index`, `data().data`.

## Sources

### Primary (HIGH confidence)
- alloy-provider 1.7.3 source code (`trait.rs`) — Provider trait structure, `root()` method, `get_logs` signature, `get_block_by_number` return type
- Project source code — schema.rs (migration chain), sqlite.rs (connection patterns), erc20.rs (Erc20Source fetch patterns)
- blake3 crate 1.8.3 — crates.io listing, API surface

### Secondary (MEDIUM confidence)
- [alloy Provider trait docs](https://docs.rs/alloy/latest/alloy/providers/trait.Provider.html) — trait structure, DynProvider pattern
- [BLAKE3 GitHub](https://github.com/BLAKE3-team/BLAKE3) — performance claims (5-10x vs SHA-256)

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries already in project except blake3 (well-established crate)
- Architecture: HIGH - patterns directly derived from existing codebase and alloy source code analysis
- Pitfalls: HIGH - identified from direct code reading (alloy EthGetBlock builder, Log field structure)

**Research date:** 2026-03-10
**Valid until:** 2026-04-10 (stable domain — SQLite caching patterns don't change)
