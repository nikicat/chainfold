# Chainfold — Roadmap & backlog

> **This document** is the forward plan: what's next, what's deferred, and the
> open backlog. The *as-built* design lives in
> [`ARCHITECTURE.md`](./ARCHITECTURE.md); conventions in
> [`STYLE.md`](./STYLE.md); dev setup in [`DEVELOPMENT.md`](./DEVELOPMENT.md).
> Shipped work is not ledgered here — `git log` and `ARCHITECTURE.md` cover it.
>
> **Live agenda: Phase 7** (pruning + best/safe wiring) plus the deferrals and
> backlog below. Run `cargo test --all-features` for the live test count —
> this file deliberately pins no number that goes stale.

---

## How to work this plan

- **Discuss architectural shifts before implementing** — "decision to
  confirm" checkpoints happen before code, per the standing preference
  ([`STYLE.md`](./STYLE.md#process)).
- **Tests are non-negotiable:** every change ships unit + (where it touches a
  source) integration tests. Fork/anvil/regtest-only tests are `#[ignore]`-gated.
- **A demo binary is the validation surface.** If you can't drive the change
  end-to-end via an `examples/` binary (or the real CLI) against real RPC,
  it's not done.

---

## Phase 7 — Pruning + best/safe wiring (next eligible)

**Goal:** bound long-running memory, and wire real `Best`/`Safe` producers
into the head clock (both tiers currently mirror `Latest`).

### Current state to build on

- **`src/chain/head.rs`** — the shared head scaffolding: one `HeadCell`
  (`watch::Sender<Option<HeadEvent>>`) per chain, demand-started via
  `HeadSeeder` running the family's `HeadClock` sessions. Per-tier producers
  slot in as sibling senders the same `follow_head` wiring pattern subscribes
  to. `EvmChain::logs(filter, tier)` / `blocks(tier)` and
  `BitcoinChain::blocks(tier)` accept a `Finality` tier but ignore it
  (`Latest`-only).
- **`src/cache/coverage.rs`** — durable `compact_ranges(partition, up_to)`
  already exists and is tested (`compact_merges_finalized_ranges`,
  `compact_skips_reorged_ranges`). What's missing is **in-memory** eviction.
- **`src/range_index/range_tree.rs`** — empty-range tombstones are already a
  first-class concept, but no prune/compact methods exist yet. The tree is
  generic over `RangeSpec`; prune/compact land once, for every source.
- **Trap:** `reorged_blocks` is cross-family — see
  [family-scoped reorg tombstones](#family-scoped-reorg-tombstones--blocks-phase-7-cleanup-wiring)
  below. Partition it before wiring finality-driven cleanup.

### Deliverables

**`src/range_index/range_tree.rs`** — in-memory pruning:

- `prune_older_than(threshold: Duration)` — for `Range`s last-touched before
  `now - threshold`, drop the `item` payload (set to `None`) but **keep the
  `Range` entry**; empty ranges stay as tombstones.
- `compact_in_memory(up_to_block)` — merge adjacent empty ranges. Safety: only
  on ranges entirely below the `safe` tip (a tip reorg must never invalidate a
  merged range — decision #8).
- On `items()` into a pruned range, repopulate from the durable store
  (cache-aside, via `RangeSpec::load_window`).

**`src/chain/head.rs` + `src/evm/chain/head_clock.rs`** — `Best`/`Safe`
producers:

- `Best`: derive from the `Latest` head minus a configurable confirmation
  depth (default 12 for mainnet) — no extra RPC.
- `Safe`: poll `eth_getBlockByNumber("finalized", false)` where supported
  (post-Merge ETH, most L2s); fall back to `latest - 64` otherwise.
- Stores built at `Best`/`Safe` stop mirroring `Latest`: their wiring
  subscribes to the tier's channel, so their `snapshots()` tick on
  confirmation/finalization.
- On `Safe` advance, kick off background `Coverage::compact_ranges` for
  ranges entirely below the new safe tip (durable side), and
  `range_tree::compact_in_memory` (in-memory side).

### Test plan

Unit (`src/range_index/` tests, over `MockSpec`): `prune_drops_items_keeps_range`,
`prune_then_iter_repopulates_from_cache`, `compact_in_memory_merges_adjacent_empty`,
`compact_skips_above_safe_tip`.

Integration (`tests/phase7_soak.rs`, new): `long_running_follow_memory_bounded`
— `#[ignore]`-flagged ~30-min manual test: subscribe to USDC, prune every
30 s, assert RSS plateaus after warmup.

CLI deliverable: `evm erc20` gains `--prune-after <duration>` and
`--measure-memory` (emit RSS once/min); manual 24 h soak against mainnet
USDC with a plateauing RSS chart.

### Acceptance

- Unit tests above pass.
- Long-running follow no longer grows RSS without bound.
- `best`/`safe` stores tick on real chains; `safe` advance triggers compaction.

### Phase 7 candidate — reorg-aware `lca()` / `is_equiv()`

Centralize today's consumer-side reorg recovery (track `block_hash`, walk back
on mismatch — [`ARCHITECTURE.md` §13](./ARCHITECTURE.md#13-reorg--finality-model))
into the API: `HistStore::lca(a, b)` rolls a stale anchor back to the canonical
chain's latest common ancestor.

---

## Deferred features

### Reducer (specced, not built)

`Reducer` = stateful monotonic fold over one stream (running balance,
cumulative volume), positioned by a **vector of N source positions** (one per
input — deterministic, resumable, cross-chain; decision #10). The sibling of
`Matcher` — both ship, they solve different problems (fold vs correlation;
decision #9). The `Erc20Balance` rebuild gates on it. See `ARCHITECTURE.md`
§8 for the matcher shape it pairs with.

### Bridge decoder

`examples/bridge_matcher.rs` ships a **generic** `--match-on <topic>` key
extractor — it correlates "two events that share value X." Production bridges
(OP `CrossDomainMessenger`, Arbitrum `Inbox`, LayerZero ULN, …) need ABI-aware
`MatchFn` impls that decode the specific burn/mint (or send/receive) event
shapes and pair on the real bridge ID. Inherently app-specific: they live
downstream of the library, which provides the `MatchFn` trait and
`AnchoredMatcher` machinery.

---

## Product surfaces beyond the library

Chainfold is three surfaces over one engine
([`ARCHITECTURE.md` §3](./ARCHITECTURE.md#3-product-surfaces)). The library is
shipped; these are forward tracks, not yet sized into phases:

- **`cast`-like CLI depth, histories-first.** Where `cast` returns a
  point-in-time value, chainfold returns the **history** it's a slice of — a
  balance read is a *history of balances*, `value at tip` is `hist.at(tip)`.
  Every CLI read maps onto the library's `HistStore`, inheriting
  caching/resume/follow. Grows from today's `erc20`/`events`/`blocks`/`chains`
  toward `call`/`storage`/`code` + account-state and block/tx/receipt
  accessors — each a cacheable history you can slice or follow.
- **`chainfoldd` — transparent caching RPC daemon.** A JSON-RPC endpoint that
  fronts an upstream node and serves from the library's cache. Drop-in for
  existing tooling (`cast`, viem, ethers, wallets): point them at `chainfoldd`
  and inherit at-most-once fetching, singleflight dedup, reorg-aware results,
  and live `eth_subscribe` push with no code changes. EVM first; later
  blockchains' RPC dialects follow as their backends land. Sequencing leans
  after/with Phase 7 — the daemon must serve `safe`/`finalized` views and
  `eth_subscribe` correctly.
- **More source backends** — Solana / CEX websockets fit the shared-engine
  shape (Bitcoin shipped as the pattern to copy: a `RangeSpec`, a `HeadClock`
  session, a wire client, domain types). Each gets its own `Position` type;
  merge and matcher compose over it for free.

---

## Backlog

### Consolidate `examples/` into the CLI

The examples predate the CLI and duplicate its arg-parsing and rendering
(they had already drifted from `fmt.rs` when the 2026-07 table migration
touched them). The CLI bin consumes the library through the same public API,
and `tests/` covers the `evm`-only feature combination, so nothing technical
holds them in place — only the linear-demo documentation value. Staged plan:

- ~~`erc20_explorer`~~ — deleted (2026-07-12); the README quickstart now
  drives `evm erc20`, and the Phase 7 `--prune-after` deliverable (above)
  targets `evm erc20`.
- `history_explorer` — listing is redundant with `evm events`; move the
  `--measure` cold/warm/hot cache benchmark to a new `evm bench` (or
  similar) first.
- `bridge_explorer` / `bridge_matcher` — no CLI surface exists (cross-chain
  interleave, anchored matching); absorbing them needs repeatable
  `--chain`+`--rpc-url`+`--address` group design (chainfoldd-adjacent
  product work, discuss first). Until then they stay, and the `--match-on`
  deliverable (above) stays theirs. Preserve their API-walkthrough value as
  module docs on the new subcommands (or doc examples on `AnchoredMatcher`).
- Once empty: drop the `tabled` `[dev-dependencies]` entry.

### Family-scoped reorg tombstones — blocks Phase 7 cleanup wiring

`reorged_blocks` is shared by every family in the one cache file, and heights
are only comparable within one family — `cleanup_reorged_before`
(height-scoped, currently test-only) driven by one family's finality would
also drop the other family's tombstones at numerically lower heights.
Partition the table (or the delete) per family before Phase 7 wires
finality-driven cleanup. Flagged on the method in `src/cache/coverage.rs`.

### R6 follow-up — `loom` model of the singleflight handshake

The R6 concurrency suite (`src/range_index/tests.rs`) covers parallelism,
disjoint/overlapping gaps, and reader-vs-writer behaviourally, but doesn't
*prove* the singleflight handshake race-free under all interleavings.
`loom` doesn't wrap the async `tokio::sync::Notify` path directly — extract
the coordination protocol (install-placeholder → fetch → `notify_waiters`,
vs. peer register-wait) into a sync analog (`Mutex` + condvar/atomic flag) and
`loom::model` that; the async code becomes a thin wrapper over the proven
core. Stretch goal; the behavioural tests are the load-bearing coverage.
Separate gap: codecov/patch red on the live WS/poll wiring
(`chain/wiring.rs`, `head_clock.rs`) — needs the ignored anvil suites
de-ignored or new wiring tests.

### Log fork tests — remove the real-RPC dependency

Three `#[ignore]` suites still fork mainnet via `fork_mainnet(FORK_BLOCK)`, so
anvil proxies `eth_getLogs`/`eth_getBlockByNumber` to a real upstream — flaky
and rate-limited without a paid `MAINNET_RPC_URL`:

- `tests/cache_layering.rs::warm_store_then_drop_index_then_reopen`
- `tests/owned_erc20.rs` (both `OwnedErc20` merge cases)
- `tests/anvil_smoke.rs::fork_mainnet_reads_usdc_total_supply`

The blocks suite (`tests/blocks_history.rs`) was already converted to a plain
`local()` anvil. The log suites are harder because they validate decoding of
real mainnet USDC data at a pinned block. To make them offline: deploy a mock
ERC20 on local anvil and emit `Transfer`s (`alloy::sol!` + a wallet-signing
provider), then assert against the emitted events. Trade-off: loses "real
chain data at block N" coverage; the point becomes the cache/merge machinery,
which is what these actually test. Keep one thin fork smoke test (behind an
env gate) if real end-to-end coverage is still wanted.

### Time anchor from the newest cached row — offline dated queries

`--from`/`--to` queries resolve the wall-clock→block anchor by demanding the
head and reading its block time (`LazyAnchor` inside the shared
`TimeLocator`): one tip poll + one block-time read per dated run, even fully
cached. The *search* probes are already cache-first; the anchor itself still
demands the head — but anchoring doesn't need the *current* head, any recent
anchor works. Prefer the newest cached row (`cached_block_headers` /
`cached_blocks` for EVM, `cached_btc_blocks` for Bitcoin) and fall back to
the head demand on an empty cache: warm dated queries become fully offline
like the `--from-block` path.

### Head-clock stop-on-idle — deliberately deferred

Tracking starts on first demand and then runs until the chain and all its
stores drop — there is no stop when consumers go away (user call: "keep it
one-shot for a while"). Revisit if long-lived apps with bursty consumption
show the idle poll to matter.

### Bitcoin follow-ups

- **ZMQ `hashblock` head push** — the poll-only `BtcHeadClock` gets a push
  sibling (a second `HeadClock` session, exactly like the EVM ws path) for
  sub-second head latency where the node exposes ZMQ.
- **Tx-level source** — a `(height, tx_index)` position (the `EvmPosition`
  analog) over `getblock` verbosity 2, when a consumer needs Bitcoin
  transactions rather than block facts.
- **Per-network scoping** — like the EVM history tables, `cached_btc_blocks`
  carries no network column; one DB file per network is the (documented)
  operating assumption. Revisit together with EVM per-chain scoping if a
  multi-network daemon materializes.

### CLI table/column printing — consolidate

Tabular output is hand-rolled with ad-hoc `format!` width specifiers across
`main.rs::print_chain_list`, `stats.rs`, `fmt.rs::format_amount`,
`evm/erc20.rs`, `evm/events.rs`, `evm/blocks.rs`, `bitcoin/blocks.rs`,
`config.rs::show` — each picks its own widths, so columns don't line up
across commands. Decide on one approach (a tiny internal `Table` helper, or a
small dep like `tabled` if the surface grows), route the existing renderers
through it, keep stdout pipeable (data to stdout, notes to stderr) and don't
ANSI-decorate when not a TTY. Low priority; do it before adding the *next*
table command.

---

## North star (what success looks like)

- Live event delivery <10 ms chainfold-internal (network is what it is).
- Cold-start of `evm erc20` reads cached events from SQLite in <50 ms
  before the first frame.
- Reorgs detected, scanned ranges invalidated automatically, no duplicate
  events delivered.
- `bridge_explorer`/`bridge_matcher` run against real cross-chain setups with
  no bespoke per-bridge code beyond a `MatchFn`.
- Memory bounded under indefinite follow (Phase 7).
- Usable as a dependency via: `Hist`, `HistStore`, `Position`, `Signal`,
  `Matcher` (+ `Reducer` once built), `merge_hist_stores`,
  `interleave_hist_stores`, `map_hist_store`, `filter_hist_store`,
  `since_hist_store`.
