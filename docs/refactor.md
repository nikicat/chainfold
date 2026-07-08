# Chainfold refactoring plan

> **Live agenda: Phase 7 + the deferrals below.** Phases 0–6, the OwnedErc20
> follow-up, the post-Phase-6 structural cleanup, and the reactive-core
> refactor (R1–R5, R7) are all shipped on `main`; their design lives in
> [`ARCHITECTURE.md`](./ARCHITECTURE.md) and the per-phase notes under
> `~/.claude-personal/projects/-home-nb-src-chainfold/memory/`. Open backlog is
> [`TODO.md`](./TODO.md). This file carries the forward plan.
>
> **Test suite (`cargo test --all-features`):** lib + CLI-bin unit + active
> integration (anvil_smoke + mock_provider_smoke + the hist_store /
> combinators / interleave / matcher suites), plus anvil/fork-only ignored
> tests (owned_erc20, cache_layering, live_follow, anvil_smoke) and an ignored
> `evm::transport` doctest. Run the command for the live count — this file
> deliberately doesn't pin a number that goes stale.

---

## Design context (locked-in)

A refactor aligning chainfold with the reference implementation's production-validated
layering. Built on three locked-in structural shifts:

1. **Cache layering** — in-memory hot path (`RangeIndex`,
   `src/evm/range_index/`) above SQLite backfill (`LogCache`, `src/evm/cache/`).
   SQLite never sits on the live-event critical path.
2. **Snapshot + tip-stream split** — `Hist<T,P>` (finite, immutable snapshot)
   and `HistStore<T,P>` (yields a fresh `Hist` as the tip advances, via
   `snapshots()`). `Position` doubles as the anchor — there is **no** separate
   `Anchor` trait, and the types are two-param `<T,P>`.
3. **Two reducer shapes** — `Matcher` (pure correlation over src + dst tails)
   shipped in Phase 6. `Reducer` (monotonic fold over one stream) is still
   deferred. They address different problems; both are intended to stay.

### As-built layout

- **Library core** (source-agnostic): `src/arch.rs` (`Position`, `Hist`,
  `HistStore`, `Signal`, `HistBackend`/`HistStoreBackend`), `src/merge/`,
  `src/combinators/` (`mapped`/`filtered`/`since`/`map_positions`),
  `src/matcher/`.
- **EVM source** (concrete): `src/evm/` — `chain/` (`EvmChain`, head clock,
  per-store wiring), `range_index/` (generic tree + `RangeIndexStore` +
  `LogSpec`), `cache/`, `chain_provider.rs`, `erc20.rs`, `owned_erc20.rs`,
  `types.rs`, `transport.rs`, `aligned_ranges.rs`, `signatures.rs`.
- **Product CLI:** `src/bin/chainfold/` (behind the `cli` feature) — the real
  binary, carrying its own unit tests. Distinct from the per-feature **demo
  binaries** in `examples/` (`history_explorer`, `erc20_explorer`,
  `bridge_explorer`, `bridge_matcher`), which validate each phase end-to-end
  against real RPC. When a section below says "CLI deliverable," it means an
  `examples/` demo unless it names `src/bin/chainfold`.

### Reference reading for agents (reference patterns to translate)

- `../reference/src/history.ts` — `RangeTree` (758–951), `HistImpl`
  (683–724), `HistStoreImpl` (978–1067), `ProxyHist`/`ProxyHistStore`
  (191–298), `MergedPos`/`MergedHist`/`MergedHistStore` (399–591), pruning.
- `../reference/src/erc20.ts` — `Erc20TransferSource` (499–603),
  `fetchTransferLogsCached` (295–311), `alignedBlockRanges` (608–620),
  `watchAddress` (342–361).
- `../reference/src/matcher.ts` — `BaseMatcher` (83–166), `AnchoredMatcher`
  (180–215), `Matcher` (241–270), `MatchOutput` (225–232).
- `../reference/src/cache.ts` — `SqliteStore` (133–184).
- `../reference/src/store.ts` — `StoreImpl`, `MergedStore` (159–187).

### User preferences (apply throughout)

- Discuss architectural shifts **before** implementing.
- No hacks that create future bugs.
- Clean abstractions; avoid leaky generics.
- Concrete EVM impl first, then extract traits.

### Conventions

- Each phase produces something runnable and visible via an `examples/` demo.
- Unit tests live alongside code as `#[cfg(test)] mod tests`; integration tests
  in `tests/`. Integration tests needing `MAINNET_RPC_URL` are `#[ignore]`-gated
  and documented in the phase acceptance.
- Use `tracing`, not `println!` (demos may print to stdout for the user).
- Type signatures below are guidance — adapt to keep the codebase consistent.
  Don't introduce traits speculatively.

---

## Phase 7 — Pruning + best/safe wiring (next eligible)

**Estimated:** ~1 week. Opportunistic. **Prereqs:** Phases 1 + 2 plus the R3
reactive-core refactor — all shipped. R3 **relocated this phase's finality
half**: there are no per-history `best_store`/`safe_store` anymore; finality is
the chain-wide `Finality` tier on `EvmChain`.

### Goal

Bound long-running memory, and wire real `Best`/`Safe` producers into
`EvmChain`'s head clock (both tiers currently mirror `Latest`).

### Current state to build on

- **`src/evm/chain.rs`** — `EvmChain::logs(filter, tier)` / `blocks(tier)`
  accept a `Finality` tier but ignore it (`Latest`-only). The demand-started
  head clock (`chain/head_clock.rs`) publishes one internal
  `watch::Sender<Option<HeadEvent>>` (the `HeadCell`); per-tier producers
  slot in as sibling senders the same wiring pattern subscribes to.
- **`src/evm/cache/log_cache.rs`** — durable `compact_ranges(filter, up_to)`
  already exists and is tested (`compact_merges_finalized_ranges`,
  `compact_skips_reorged_ranges`). What's missing is **in-memory** eviction.
- **`src/evm/range_index/range_tree.rs`** — empty-range tombstones are
  already a first-class concept, but no prune/compact methods exist yet. The
  tree is generic over `RangeSpec`; prune/compact land once, for every source.

### Deliverables

**`src/evm/range_index/range_tree.rs`** — in-memory pruning:
- `prune_older_than(threshold: Duration)` — for `Range`s last-touched before
  `now - threshold`, drop the `item` payload (set to `None`) but **keep the
  `Range` entry**; empty ranges stay as tombstones.
- `compact_in_memory(up_to_block)` — merge adjacent empty ranges. Safety: only
  on ranges entirely below the `safe` tip (a tip reorg must never invalidate a
  merged range — locked-in decision #8).
- On `items()` into a pruned range, repopulate from the durable store
  (cache-aside, via `RangeSpec::load_window`).

**`src/evm/chain.rs` + `chain/head_clock.rs`** — `Best`/`Safe` producers:
- `Best`: derive from the `Latest` head minus a configurable confirmation
  depth (default 12 for mainnet) — no extra RPC; publish on the `Best` tier's
  channel.
- `Safe`: poll `eth_getBlockByNumber("finalized", false)` where supported
  (post-Merge ETH, most L2s); fall back to `latest - 64` otherwise.
- `head(tier)` / `logs(filter, tier)` stop collapsing to `Latest`; a
  `Safe`-tier store's wiring subscribes to the `Safe` channel, so its
  `snapshots()` tick on finalization.
- On `Safe` advance, kick off background `LogCache::compact_ranges` for
  ranges entirely below the new safe tip (durable side), and
  `range_tree::compact_in_memory` (in-memory side).

### Test plan

**Unit (`src/evm/range_index/range_tree.rs` tests):**
- `prune_drops_items_keeps_range`
- `prune_then_iter_repopulates_from_cache`
- `compact_in_memory_merges_adjacent_empty`
- `compact_skips_above_safe_tip`

**Integration (`tests/phase7_soak.rs`, new):**
- `long_running_follow_memory_bounded` — `#[ignore]`-flagged ~30-min manual
  test: subscribe to USDC, prune every 30 s, assert RSS plateaus after warmup.

### CLI deliverable

`examples/erc20_explorer.rs`:
- Add `--prune-after <duration>` and `--measure-memory` (emit RSS once/min).
- Manual: 24 h soak against mainnet USDC; RSS chart plateaus.

### Acceptance

- Unit tests above pass.
- Long-running follow no longer grows RSS without bound.
- `best`/`safe` stores tick on real chains; `safe` advance triggers compaction.

---

## Deferred follow-ups

### Bridge decoder (Phase 6 deferral)

`examples/bridge_matcher.rs` ships a **generic** `--match-on <topic>` key
extractor — it correlates "two events that share value X." Production bridges
(OP, Arbitrum, LayerZero, …) need ABI-aware `MatchFn` impls that decode the
specific burn/mint (or send/receive) event shapes and pair on the real bridge
ID. These are inherently app-specific and live downstream of the library;
chainfold provides the `MatchFn` trait and `AnchoredMatcher` machinery.

### Reducer (Phase 6 spec, not yet built)

`Reducer` = stateful monotonic fold over one stream (running balance,
cumulative volume), positioned by a **vector of N source positions** (one per
input — deterministic, resumable, cross-chain; decision #10). It's the sibling
of `Matcher`. The `Erc20Balance` rebuild gates on it. Spec in
`../reference/src/store.ts` (`storeFromGen`) + ARCHITECTURE.md's Matcher/Reducer notes.

---

## Product surfaces beyond the library (planned)

Chainfold is scoped as three surfaces over one caching engine (see
[`ARCHITECTURE.md`](./ARCHITECTURE.md#3-product-surfaces)). The library is shipped; the other two
are forward tracks, not yet sized into numbered phases:

- **`cast`-like CLI depth, histories-first.** The CLI's distinguishing idea:
  where `cast` returns a point-in-time value, chainfold returns the **history**
  it's a slice of — a balance read is a *history of balances*, a block read a
  slice of *block history*, and `value at tip` is just `hist.at(tip)`. Every CLI
  read maps onto the library's `HistStore`, inheriting caching/resume/follow.
  Grows from today's `erc20`/`events`/`chains` toward `call`/`storage`/`code` +
  account-state and block/tx/receipt accessors — each a cacheable history you can
  slice or follow. See the near-term plan below.
- **`chainfoldd` — transparent caching RPC daemon.** A JSON-RPC endpoint that
  fronts an upstream node and serves from the library's cache. Drop-in for
  existing tooling (`cast`, viem, ethers, wallets): point them at `chainfoldd`
  and inherit at-most-once fetching, singleflight dedup, reorg-aware results, and
  live `eth_subscribe` push with no code changes. EVM first; later blockchains'
  RPC dialects follow as their backends land.

Both ride the same `Hist`/`HistStore`/`LogCache`/`RangeIndex` machinery — they
are front-ends, not forks. Sequencing relative to Phase 7 is open; the daemon
in particular benefits from Phase 7's finality wiring (it must answer
`eth_subscribe` and serve `safe`/`finalized` views correctly).

### Near-term CLI plan

The first concrete steps toward the histories-first CLI:

1. **`blocks` command — cached block history.** ✅ **Shipped.** `BlockSpec`
   (a `RangeSpec` in `src/evm/range_index/block_spec.rs` — the second instance
   after `LogSpec`, not a bespoke `EvmBlockHistory`) backs
   `EvmChain::blocks(tier) -> HistStore<EvmBlock, BlockNumber>` on the same
   generic `RangeIndex`/`RangeIndexStore` machinery. The CLI `chainfold evm …
   blocks list` drives it with the same window flags
   (`--from`/`--to`/`--from-block`/`--num-blocks`) plus `--follow`/`--reverse`.
   The reference instance of histories-first: default output is the block
   *history* over the window; a single block is a slice. Concrete impl — no
   `Block` trait until a second source needs one.
   - **M1** — cached range walk. **Deviation from the plan below:** rather than
     reusing `cached_block_headers` (whose three INSERT-OR-REPLACE writers only
     know the four header fields), blocks get their own `cached_blocks` table
     carrying the **full** field set from the first migration — the rich fields
     the plan slated for M3 come free on the same `eth_getBlockByNumber(n,
     false)` call, so M3's schema work folded into M1. Coverage rides
     `scanned_ranges` under a typed `ScanPartition::Blocks` key.
   - **M2** — `--follow` (live tip). No `newHeads` push path: the payload lacks
     the tx count, so every head advance eager-gap-fills through the store's
     batched fetcher (ws and poll follow behave identically here).
   - **M3** — display polish only: the `--full` column set (MINER/PARENT + full
     hashes), gas/base-fee formatting.
2. **Default chain = `ethereum`** when `--chain`/`--chain-id` omitted — shipped.
   Resolution precedence: `--chain-id` > `--chain` > env `CHAINFOLD_CHAIN` >
   config `[evm] default_chain` > built-in `ethereum`.

## What success looks like (north star)

- Live event delivery <10 ms (chainfold-internal latency; network is what it
  is).
- Cold-start of `erc20_explorer` reads cached events from SQLite in <50 ms
  before the first frame.
- Reorgs detected, scanned ranges invalidated automatically, no duplicate
  events delivered.
- `bridge_explorer` / `bridge_matcher` run against real cross-chain setups with
  no bespoke per-bridge code beyond a `MatchFn`.
- Memory bounded under indefinite follow (Phase 7).
- Usable as a dependency via: `Hist`, `HistStore`, `Position`, `Signal`,
  `Matcher` (+ `Reducer` once built), `merge_hist_stores`,
  `interleave_hist_stores`, `map_hist_store`, `filter_hist_store`,
  `since_hist_store`. (`snapshots()` is the tip-stream method; there is no
  separate `Anchor` trait — `Position` serves that role.)

## How to use this plan

- The live work is **Phase 7** + the deferrals above. Phases 0–6 and the
  reactive-core refactor are shipped; their rationale lives in `ARCHITECTURE.md`
  and the per-phase memory files, not here.
- **Read the reference sources first** — they contain production-tested
  versions of every pattern. Translate, don't reinvent.
- **"Decision to confirm" checkpoints happen before implementation**, per the
  standing preference: discuss architectural changes first.
- **Tests are non-negotiable:** every change ships unit + (where it touches
  EVM) integration. Fork/anvil-only tests are `#[ignore]`-gated.
- **A demo binary is the validation surface.** If you can't drive the change
  end-to-end via an `examples/` binary against real RPC, it's not done.
