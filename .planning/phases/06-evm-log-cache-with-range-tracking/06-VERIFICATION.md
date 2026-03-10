---
phase: 06-evm-log-cache-with-range-tracking
verified: 2026-03-10T03:15:00Z
status: passed
score: 12/12 must-haves verified
---

# Phase 6: EVM Log Cache with Range Tracking Verification Report

**Phase Goal:** eth_getLogs results are cached as a raw event layer keyed by RPC-level parameters (contract address, topic sets, block range). Scanned ranges are tracked even when empty -- so a second run over the same range hits zero RPC calls. The block DAG layer consumes from this cache instead of directly from RPC. Events are reusable across reducers: same cached logs, different state machines.
**Verified:** 2026-03-10T03:15:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | LogCache stores raw eth_getLogs results keyed by BLAKE3 hash of (address, topic0-3) | VERIFIED | `compute_filter_hash` at line 35 uses blake3 with 0xFF sentinel. `insert_logs`/`get_cached_logs` use filter_hash as key. 4 hash tests pass. |
| 2 | Scanned ranges are recorded even when zero logs returned | VERIFIED | `record_scanned_range` is independent of log count -- records range unconditionally. CachingProvider calls `record_scanned_range` for each gap regardless of whether `gap_logs` is empty (line 572). |
| 3 | Adjacent and overlapping ranges merge into a single range on write | VERIFIED | Merge-on-write in `record_scanned_range` (lines 132-190) with merge candidates query. Tests: `range_overlapping_merge`, `range_adjacent_merge`, `range_contained_absorbed`, `range_multi_merge`, `range_identical_idempotent` -- all pass. |
| 4 | Gap computation returns only uncovered sub-ranges for a requested range | VERIFIED | `compute_gaps` sweep algorithm (lines 69-91). Tests: `gaps_no_scanned_returns_full_range`, `gaps_fully_covered_returns_empty`, `gaps_partial_coverage`, `gaps_multiple_scanned`, `gaps_scanned_beyond_request` -- all pass. |
| 5 | Block headers are cached by height | VERIFIED | `insert_block_header`/`get_block_header` with `cached_block_headers` table (INTEGER PRIMARY KEY on height). Tests: `block_header_round_trip`, `block_header_missing`, `block_header_replace` -- all pass. |
| 6 | Cached logs can be retrieved and reconstructed into alloy Log objects | VERIFIED | `get_cached_logs` reconstructs full `alloy::rpc::types::Log` from DB columns (lines 253-320). Test `logs_round_trip` verifies field-by-field equality. |
| 7 | CachingProvider intercepts get_logs and serves cached results for finalized ranges | VERIFIED | `impl Provider for CachingProvider<P>` overrides `get_logs` (line 482). Computes finality boundary, checks cache, fetches only gaps. |
| 8 | Partially-cached ranges only fetch uncovered sub-ranges from RPC | VERIFIED | `get_logs` calls `compute_gaps` then iterates gaps calling `self.inner.get_logs(&sub_filter)` only for uncovered ranges (lines 543-588). |
| 9 | Blocks within the finality window always go to RPC, never cached | VERIFIED | `safe_to = to_block.min(finality_boundary)` (line 514). Unfinalized portion [safe_to+1, to_block] always delegates to inner provider (lines 613-622). |
| 10 | Two different reducers sharing the same (address, topics) filter share the cache | VERIFIED | Cache key is `compute_filter_hash(address, topics)` -- RPC-level, not application-level. `shared_arc_log_cache` test confirms Arc<LogCache> sharing. CachingProvider holds `Arc<LogCache>`. |
| 11 | Erc20Source works transparently with CachingProvider -- no source-level code change for get_logs | VERIFIED | `fetch_logs_in_range` calls `self.provider.get_logs(&filter)` unchanged (line 249). EventSource impl bound is `P: Provider + Clone + BlockHeaderCache` (line 311). CachingProvider satisfies all bounds. |
| 12 | Block headers are cached transparently during fetch_blocks | VERIFIED | `fetch_blocks` calls `self.provider.get_or_fetch_block_header(height)` (line 380). `BlockHeaderCache` impl for CachingProvider checks LogCache first, caches on miss (lines 658-701). |

**Score:** 12/12 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/modules/evm_cache.rs` | LogCache, CachingProvider, FinalityConfig, BlockHeaderCache, filter hashing, gap computation | VERIFIED | 1114 lines (min 200 required). Contains all expected types and implementations. |
| `src/storage/schema.rs` | v4 migration with cached_logs, scanned_ranges, cached_block_headers tables | VERIFIED | v4 migration at lines 121-158 creates all 3 tables with correct columns, indexes, and constraints. |
| `src/modules/erc20.rs` | Updated Erc20Source using BlockHeaderCache | VERIFIED | Imports `BlockHeaderCache` (line 21). EventSource impl bound includes `BlockHeaderCache` (line 311). `fetch_blocks` uses `get_or_fetch_block_header` (line 380). |
| `examples/erc20_tracker.rs` | Example wraps provider in CachingProvider | VERIFIED | Imports `CachingProvider`, `FinalityConfig`, `LogCache` (line 25). Wraps raw provider at lines 122-123. |
| `Cargo.toml` | blake3 optional dep gated behind evm feature | VERIFIED | `blake3 = { version = "1.8", optional = true }` and `evm = ["dep:alloy", "dep:tracing", "dep:blake3"]`. |
| `src/storage/sqlite.rs` | `configure_connection` promoted to `pub(crate)` | VERIFIED | `pub(crate) fn configure_connection` at line 65. |
| `src/modules/mod.rs` | `evm_cache` module registered behind evm feature | VERIFIED | `#[cfg(feature = "evm")] pub mod evm_cache;` at line 4. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `src/modules/evm_cache.rs` | `src/storage/schema.rs` | Uses tables created by v4 migration | WIRED | Queries reference `cached_logs`, `scanned_ranges`, `cached_block_headers` tables throughout LogCache methods. |
| `src/modules/evm_cache.rs` | `Cargo.toml` | blake3 dependency | WIRED | `use blake3::Hasher` via `blake3::Hasher::new()` at line 36. Cargo.toml has blake3 in evm feature gate. |
| `src/modules/evm_cache.rs` | `src/modules/erc20.rs` | CachingProvider passed as P: Provider to Erc20Source | WIRED | Erc20Source<P> where P: Provider + Clone + BlockHeaderCache. CachingProvider implements all three traits. Example constructs `Erc20Source::new(provider, ...)` where provider is CachingProvider. |
| `src/modules/evm_cache.rs` | `src/modules/evm_cache.rs` (LogCache) | CachingProvider holds Arc<LogCache> | WIRED | `cache: Arc<LogCache>` field in CachingProvider struct (line 414). Used throughout get_logs and BlockHeaderCache impl. |
| `examples/erc20_tracker.rs` | `src/modules/evm_cache.rs` | Wraps provider in CachingProvider | WIRED | `CachingProvider::new(raw_provider, log_cache, FinalityConfig::EthereumMainnet)` at line 123. |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| CACHE-01 | 06-01 | Library caches raw eth_getLogs results keyed by (contract address, topic0-3) | SATISFIED | `compute_filter_hash` produces BLAKE3 key from address+topics. LogCache stores/retrieves by filter_hash. |
| CACHE-02 | 06-01 | Library tracks scanned block ranges per topic set -- empty ranges recorded as "scanned" | SATISFIED | `record_scanned_range` is called unconditionally after fetching each gap, regardless of log count. |
| CACHE-03 | 06-01 | Adjacent and overlapping scanned ranges merge automatically | SATISFIED | Merge-on-write in `record_scanned_range` with 5 passing merge tests. |
| CACHE-04 | 06-02 | Partially-overlapping ranges compute and fetch only uncovered sub-ranges | SATISFIED | CachingProvider.get_logs calls compute_gaps then fetches only gaps from inner provider. |
| CACHE-05 | 06-02 | Cached logs reusable across reducers -- same (contract, topics) share cache | SATISFIED | Cache key is RPC-level (address, topics), not application-level. Arc<LogCache> enables sharing. |

No orphaned requirements found. All 5 CACHE requirements appear in plans and are marked Complete in REQUIREMENTS.md traceability table.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | - | - | - | No anti-patterns found. No TODO/FIXME/placeholder comments. No empty implementations. No stub handlers. |

### Human Verification Required

### 1. End-to-End Cache Hit Verification

**Test:** Run `cargo run --features evm --example erc20_tracker -- --rpc-url $RPC --from-block 24612100 --num-blocks 50` twice.
**Expected:** Second run completes significantly faster with zero eth_getLogs RPC calls for the finalized portion (observable via RUST_LOG=debug tracing output).
**Why human:** Requires a live RPC endpoint and observation of actual network calls.

### 2. Cross-Reducer Cache Sharing

**Test:** Run two Erc20Source instances with different Erc20Filter values (AllTransfers vs Account) but same contract address and topic0 (Transfer). Check that the second instance reuses cached logs from the first.
**Expected:** Second instance makes fewer RPC calls because the cache key (address, topic0=Transfer, topic1=None, ...) overlaps.
**Why human:** Requires constructing a multi-reducer scenario with a live RPC.

### Gaps Summary

No gaps found. All 12 observable truths verified. All 7 artifacts pass existence, substantive, and wiring checks. All 5 key links verified as wired. All 5 requirements satisfied. 168 tests pass (34 new evm_cache tests + 134 existing). Example compiles. No-feature build compiles. No anti-patterns detected.

---

_Verified: 2026-03-10T03:15:00Z_
_Verifier: Claude (gsd-verifier)_
