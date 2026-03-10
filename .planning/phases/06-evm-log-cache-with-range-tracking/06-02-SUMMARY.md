---
phase: 06-evm-log-cache-with-range-tracking
plan: 02
subsystem: caching
tags: [alloy, provider-wrapper, finality, block-headers, evm, caching]

# Dependency graph
requires:
  - phase: 06-evm-log-cache-with-range-tracking
    provides: LogCache struct, schema v4, compute_filter_hash, compute_gaps, range tracking
provides:
  - CachingProvider<P> implementing alloy Provider trait with get_logs interception
  - FinalityConfig enum with chain-specific finality presets
  - BlockHeaderCache trait for cached block header access
  - Transparent caching integration with Erc20Source
affects: [evm-cache, erc20-source, examples]

# Tech tracking
tech-stack:
  added: []
  patterns: [provider-wrapper, finality-aware-caching, trait-based-header-cache]

key-files:
  created: []
  modified: [src/modules/evm_cache.rs, src/modules/erc20.rs, examples/erc20_tracker.rs]

key-decisions:
  - "No blanket BlockHeaderCache impl — only CachingProvider implements it (avoids unstable specialization)"
  - "CachingProvider manually implements Clone (Arc<LogCache> + inner.clone())"
  - "Filter extraction bails to inner provider for multi-address or multi-topic-per-slot filters"
  - "spawn_blocking wraps all synchronous LogCache calls from async CachingProvider methods"

patterns-established:
  - "Provider wrapper: implement Provider trait, delegate root(), override get_logs()"
  - "Finality-aware caching: clamp cacheable range to chain_tip - finality_depth"
  - "Trait-based extension: BlockHeaderCache adds methods without changing Provider trait"

requirements-completed: [CACHE-04, CACHE-05]

# Metrics
duration: 5min
completed: 2026-03-10
---

# Phase 6 Plan 02: CachingProvider Integration Summary

**CachingProvider wrapping alloy Provider with finality-aware get_logs interception, BlockHeaderCache trait, and Erc20Source integration**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-10T00:23:45Z
- **Completed:** 2026-03-10T00:29:11Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- CachingProvider<P> implements alloy Provider, intercepts get_logs for finalized ranges
- FinalityConfig enum with EthereumMainnet (64), Arbitrum (0), Custom(N) presets
- BlockHeaderCache trait: CachingProvider checks LogCache first, falls back to RPC
- Erc20Source fetch_blocks uses BlockHeaderCache transparently
- Example wraps provider in CachingProvider with EthereumMainnet finality
- 10 new tests (168 total passing including all existing)

## Task Commits

Each task was committed atomically:

1. **Task 1: CachingProvider, FinalityConfig, and BlockHeaderCache trait** - `f6e1e5a` (feat)
2. **Task 2: Wire CachingProvider into Erc20Source and update example** - `c31c9be` (feat)

## Files Created/Modified
- `src/modules/evm_cache.rs` - CachingProvider<P>, FinalityConfig, BlockHeaderCache trait, filter extraction helpers, 10 new tests
- `src/modules/erc20.rs` - BlockHeaderCache import, EventSource impl gains BlockHeaderCache bound, fetch_blocks uses get_or_fetch_block_header
- `examples/erc20_tracker.rs` - CachingProvider wraps raw provider with EthereumMainnet finality config

## Decisions Made
- No blanket BlockHeaderCache impl: Rust stable doesn't support specialization, so only CachingProvider implements the trait (not all Provider impls). This means Erc20Source requires CachingProvider, which is the intended usage pattern.
- CachingProvider manually implements Clone: inner provider is cloned, cache is Arc-cloned, finality config is Copy.
- Filter extraction helper functions (extract_single_address, extract_topics) bail to inner provider for complex cases (multi-address, multi-topic-per-slot, tag-based block ranges).
- All synchronous LogCache calls wrapped in tokio::spawn_blocking from async CachingProvider.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added Clone impl for CachingProvider**
- **Found during:** Task 2 (compilation of example)
- **Issue:** Erc20Source requires P: Provider + Clone, but CachingProvider didn't implement Clone
- **Fix:** Added manual Clone impl (inner.clone() + Arc::clone(cache) + Copy finality)
- **Files modified:** src/modules/evm_cache.rs
- **Verification:** Example and all tests compile and pass
- **Committed in:** c31c9be (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minor missing derive. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 6 complete: full log caching pipeline from storage to provider to source
- Second run over same finalized range makes zero RPC calls for logs and block headers
- Two reducers with same RPC-level filter share cached logs automatically
- 168 tests passing, zero regressions

---
*Phase: 06-evm-log-cache-with-range-tracking*
*Completed: 2026-03-10*
