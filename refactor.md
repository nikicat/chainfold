# Chainfold refactoring plan

> **Status as of 2026-05-26.** Phases 0–6 + the OwnedErc20 follow-up + the
> post-Phase-6 structural cleanup are all shipped on `main`. This file used to
> carry the full per-phase specs for 0–6; those are now redundant with the
> *shipped* design in [`arch.md`](./arch.md) and the per-phase notes in
> `~/.claude-personal/projects/-home-nb-src-chainfold/memory/`
> (`phase5_merge.md`, `phase6_matcher.md`, …). What remains here is the live
> agenda: **Phase 7** plus the deferred follow-ups.
>
> | Phase | Status | Notes |
> |---|---|---|
> | 0 — Housekeeping + CCPs | ✅ Done (2026-05-21) | Dead pre-arch code deleted; anvil + mock harnesses landed. |
> | 1 — LogCache + LogIndex | ✅ Done (2026-05-21) | Layered cache split; 98.5× warm-vs-cold on fork. |
> | 2 — Push subscription | ✅ Done (2026-05-22) | WS latency ~3 ms on local anvil. |
> | 3 — Hist / HistStore split | ✅ Done (2026-05-22) | Full cleanup, **no facade** — `Cursor`/`History` removed outright. |
> | 4 — map / filter / since | ✅ Done (2026-05-22) | Pure proxies; no I/O. |
> | 5 — Heterogeneous merge | ✅ Done (2026-05-22) | `MergedPos`, `ErasedHistStore`; `clamp_pos` override. |
> | 5b — OwnedErc20 follow-up | ✅ Done (2026-05-23) | Two narrow filters + merge; `erc20_explorer --owner` rewired. |
> | 6 — Matcher | ✅ Done (2026-05-23) | `BaseMatcher` / `AnchoredMatcher` / `AnchorStore` (in-mem + SQLite). |
> | Structural cleanup | ✅ Done (2026-05-26) | Function-level dedup + module-tree splits + `mod.rs`→`foo.rs` migration (see below). |
> | **7 — Pruning + best/safe wiring** | ⏳ **Pending — next eligible chunk** | In-memory eviction + `best`/`safe` stores. Facade removal is moot (none exists). |
> | Bridge decoder | ⏳ Deferred | Generic `--match-on` extractor ships; ABI-aware impls per bridge are app-specific. |
> | Same-`P` merge alias | ⏳ Deferred | Not load-bearing; `MergedItem.source_key` is useful as-is. |
> | Reducer | ⏳ Deferred (Phase 6 spec) | Sibling of Matcher; `Erc20Balance` rebuild gates on it. |
>
> **Test totals (2026-05-26, `cargo test --all-features`):** 106 lib + 49
> CLI-bin unit + 31 active integration (anvil_smoke + mock_provider_smoke +
> 9 phase3 + 6 phase4 + 8 phase5 + 6 phase6) = **186 active**, 7 ignored
> (anvil/fork-only: owned_erc20, phase1_layering, phase2_push, 1 anvil_smoke).
> Most targets are gated behind the non-default `evm` feature; the CLI binary
> behind `cli`. Bare `cargo test` only exercises the 35 source-agnostic lib
> tests + phases 3–6.

## What shipped in the structural cleanup (done)

The structural-debt inventory accumulated over Phases 0–6 has all landed:

- **Function-level dedup:** `resolve_range`, `apply_topics`, `render_transfer`,
  `install_gap_placeholder`, `merge_step` decomposition, `walk_step`
  unification, `cached_navigate` forward/backward split, `tip_pos(TipKind)`
  collapse, `find_event_item` extraction, stale-tag/citation removal.
- **`arch::evm` types** (`EvmPosition`/`EvmEvent`/`Erc20*`/…) moved out of
  `arch.rs` into `src/evm/types.rs` — the source-agnostic core no longer names
  EVM.
- **Module-tree splits:**
  - `matcher.rs` → `src/matcher/` (extracting `SqliteAnchorStore` into
    `anchor_store_sqlite.rs`).
  - `evm/log_index.rs` → `src/evm/log_index/`
    (`block_pos`/`range_tree`/`nav`/`gap_fill`).
  - `evm/log_store.rs` → `src/evm/cache/` — a `CacheDb` opener vending
    `LogCache` (logs + ranges + reorg + headers), `Erc20Cache`, and
    `EventSignatureCache` over one shared `Arc<Mutex<Connection>>` (one SQLite
    file). The misleading single-`LogStore` god-object is gone; each consumer
    holds one narrow view.
- **`mod.rs` → `foo.rs` migration (2026-05-26):** all six `*/mod.rs` files
  converted to edition-2024-idiomatic `foo.rs` + `foo/` siblings. No `mod.rs`
  files remain.

---

## Design context (locked-in)

A phased refactor aligning chainfold with the reference implementation's production-validated
layering. Built on three locked-in structural shifts:

1. **Cache layering** — in-memory hot path (`LogIndex`, `src/evm/log_index/`)
   above SQLite backfill (`LogCache`, `src/evm/cache/`). SQLite never sits on
   the live-event critical path.
2. **Snapshot + tip-stream split** — `Hist<T,P>` (finite, immutable snapshot)
   and `HistStore<T,P>` (yields a fresh `Hist` as the tip advances, via
   `snapshots()`). `Position` doubles as the anchor — there is **no** separate
   `Anchor` trait, and the types are two-param `<T,P>`, not the `<T,P,A>`
   sketched in the original plan.
3. **Two reducer shapes** — `Matcher` (pure correlation over src + dst tails)
   shipped in Phase 6. `Reducer` (monotonic fold over one stream) is still
   deferred. They address different problems; both are intended to stay.

### As-built layout (the original plan predates these paths)

- **Library core** (source-agnostic): `src/arch.rs` (`Position`, `Hist`,
  `HistStore`, `Store`, `HistBackend`/`HistStoreBackend`), `src/merge.rs`,
  `src/combinators/` (`mapped`/`filtered`/`since`), `src/matcher/`.
- **EVM source** (concrete): `src/evm/` — `history.rs`, `cache/`, `log_index/`,
  `erc20.rs`, `owned_erc20.rs`, `subscriber.rs`, `types.rs`, `retry.rs`,
  `aligned_ranges.rs`, `signatures.rs`.
- **Product CLI:** `src/bin/chainfold/` (behind the `cli` feature) — the real
  binary, carrying 49 unit tests. This is distinct from the per-feature **demo
  binaries** in `examples/` (`history_explorer`, `erc20_explorer`,
  `bridge_explorer`, `bridge_matcher`), which exist to validate each phase
  end-to-end against real RPC. When a section below says "CLI deliverable," it
  means an `examples/` demo unless it names `src/bin/chainfold`.

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

**Estimated:** ~1 week. Opportunistic. **Prereqs:** Phases 1 + 2 (uses
`LogIndex` and subscriptions) — both shipped.

### Goal

Bound long-running memory, and wire the `best`/`safe` reactive stores that
have been stubbed since Phase 3.

### Current state to build on

- **`src/evm/history.rs`** — `best_store`/`safe_store` are stub `Store`s built
  from `watch::channel(earliest)` whose senders drop immediately (see
  `EvmLogHistory::new`, ~line 140). `get()` answers `earliest`; `subscribe()`
  yields once then ends. `latest_store` is fully wired.
- **`src/evm/cache/log_cache.rs`** — durable `compact_ranges(filter, up_to)`
  already exists and is tested (`compact_merges_finalized_ranges`,
  `compact_skips_reorged_ranges`). What's missing is **in-memory** eviction.
- **`src/evm/log_index/range_tree.rs`** — empty-range tombstones are already a
  first-class concept (the type doc mentions "preserved across compaction"),
  but no prune/compact methods exist yet.
- **No deprecated facade exists** — Phase 3 removed `Cursor`/`History` outright.
  The original plan's "delete the deprecated facade" task is therefore moot.

### Deliverables

**`src/evm/log_index/range_tree.rs`** — in-memory pruning:
- `prune_older_than(threshold: Duration)` — for `Range`s last-touched before
  `now - threshold`, drop the `item` payload (set to `None`) but **keep the
  `Range` entry**; empty ranges stay as tombstones.
- `compact_in_memory(up_to_block)` — merge adjacent empty ranges. Safety: only
  on ranges entirely below the `safe` tip (a tip reorg must never invalidate a
  merged range — see locked-in decision #8).
- On `items()` into a pruned range, repopulate from `LogCache` (cache-aside).

**`src/evm/history.rs`** — `best`/`safe`:
- `best`: poll `eth_getBlockByNumber("latest", false)` minus a configurable
  confirmation depth (default 12 for mainnet); bump `best_store`.
- `safe`: poll `eth_getBlockByNumber("finalized", false)` where supported
  (post-Merge ETH, most L2s); fall back to `latest - 64` otherwise; bump
  `safe_store`.
- On `safe` advance, kick off background `LogCache::compact_ranges` for ranges
  entirely below the new safe tip (durable side), and
  `range_tree::compact_in_memory` (in-memory side).

### Test plan

**Unit (`src/evm/log_index/range_tree.rs` tests):**
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

### Same-`P` typed merge alias (Phase 5 deferral)

When every source shares one `P`, a typed `merge` convenience alias over the
heterogeneous `merge_hist_stores` would avoid the `MergedItem.source_key`
string indirection. Not load-bearing — `source_key` is useful as-is.

### Reducer (Phase 6 spec, not yet built)

`Reducer` = stateful monotonic fold over one stream (running balance,
cumulative volume), positioned by a **vector of N source positions** (one per
input — deterministic, resumable, cross-chain; decision #10). It's the sibling
of `Matcher`. The `Erc20Balance` rebuild gates on it. Spec in
`../reference/src/store.ts` (`storeFromGen`) + arch.md's Reducer section.

---

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
- Usable as a dependency via: `Hist`, `HistStore`, `Position`, `Store`,
  `Matcher` (+ `Reducer` once built), `merge_hist_stores`, `map_hist_store`,
  `filter_hist_store`, `since_hist_store`. (`snapshots()` is the tip-stream
  method; there is no separate `Anchor` trait — `Position` serves that role.)

## How to use this plan

- The live work is **Phase 7** + the three deferrals above. Phases 0–6 are
  shipped; their design rationale lives in `arch.md` and the per-phase memory
  files, not here.
- **Read the reference sources first** — they contain production-tested
  versions of every pattern. Translate, don't reinvent.
- **"Decision to confirm" checkpoints happen before implementation**, per the
  standing preference: discuss architectural changes first.
- **Tests are non-negotiable:** every change ships unit + (where it touches
  EVM) integration. Fork/anvil-only tests are `#[ignore]`-gated.
- **A demo binary is the validation surface.** If you can't drive the change
  end-to-end via an `examples/` binary against real RPC, it's not done.
