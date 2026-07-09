# TODO

Active backlog only. Completed work is a one-line ledger below; its rationale
lives in git history and in [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Done (ledger)

- **Phases 0–6 + OwnedErc20** — cache layering, push subscription, `Hist`/
  `HistStore` split, combinators, merge, matcher. See `ARCHITECTURE.md`.
- **Reactive-core refactor R1–R5, R7** — `EvmChain` + generic
  `RangeIndexStore`; finality hoisted to the chain (`head(tier)`); matcher
  folds the Hist that triggered it; `Store<T>` → `Signal<T>`; task guards →
  `AbortOnDropHandle`.
- **Merge split** — ordered `merge_hist_stores` (one position space) +
  `interleave_hist_stores` (vector-clock `MergedPos<P>`); erasure deleted;
  cross-family unification via app enum + `map_positions`.
- **Type-discipline / blind-type audit** — domain newtypes across the EVM +
  CLI layers (`ChainId`, `Topic`, `BlockNumber`, `LogIdx`, `FilterHash`,
  `ChainHandle`, `Url`, …); the audit backlog is empty except the deliberate
  skips (`decode: bool`, chain-identifier lookup strings).
- **CLI** — `ExitCode` + `Display` error rendering, `chains list`, testnet/
  red-flag advisories, default chain = `ethereum`, end-to-end `ChainId` typing.
- **EVM blocks source** — `BlockSpec` (2nd `RangeSpec` after `LogSpec`) →
  `chain.blocks(tier) -> HistStore<EvmBlock, BlockNumber>`; own `cached_blocks`
  table + typed `ScanPartition::Blocks` coverage; batched `eth_getBlockByNumber`
  fetcher; `evm blocks list` CLI (windowed table + `--follow`/`--reverse`). The
  reference histories-first read. Along the way: `BlockHash`/`TxHash` promoted
  to real newtypes, `FromSql`/`ToSql` + name-based row reads across the cache,
  and a `snapshots()` busy-loop fix (emit-on-advance).
- **R6 — shared-index concurrency tests** — `RangeIndex` walk under true
  parallelism (multi-thread runtime), disjoint/overlapping gaps (singleflight
  tiles the union, no double-fetch), and reader-vs-writer (`insert_page` racing
  a mid-walk `items()`: no dup / no drop / no panic). Test suite split out to
  `src/evm/range_index/tests.rs`. Stretch goal **not** done: a `loom` model of
  the singleflight handshake (needs a sync analog of the async `Notify` path).
- **Shared-engine extraction + Bitcoin block-history source** — the second
  chain family, added deliberately to force the extraction: `observe`,
  `chain` (height types · `HeadCell`/`HeadSeeder`/`HeadClock` session seam ·
  `Finality` · `TimeLocator` · `follow_head`), `cache` (`CacheDb` + the
  cross-family `Coverage` view, `ScanPartition`, opaque `CoverageHash`), and
  `range_index` (+ the dense `HeightPos` key and a synthetic `MockSpec`
  testkit that un-gated the generic tests) are crate-root shared modules.
  `src/bitcoin/`: `BtcBlock`/`BtcBlockHash` (display-order hex) /
  `CompactTarget`, a reqwest-backed `jsonrpc` 0.20 transport with the
  `chainfold::rpc` span decorator, batched `getblockhash`→`getblockheader`
  client, `cached_btc_blocks` + `ScanPartition::BtcBlocks`, `BtcBlockSpec`,
  and the concrete `BitcoinChain` sibling (no `Chain` trait — decision #6).
  `chainfold bitcoin blocks list` CLI (shared `WindowArgs`, `[bitcoin]`
  config + `BtcAuth` funnel); regtest integration suite via `corepc-node`.
  Along the way `estimate_block`/`time_pos` gained real refinement: the
  nominal-cadence anchor is only the initial guess for a timestamp-driven
  interpolation search (`TimeLocator`).
- **Demand-started head tracking + bounded hists** — `EvmChain::new` is sync,
  infallible and RPC-free; the head clock (poll/ws, `EvmChainConfig::transport`
  — the `follow: Option<_>` "one-shot chain" mode is gone) starts exactly once
  on the first tip demand (store `TipSeed` signal or the lazy `time_pos`
  anchor) and runs until chain + stores drop. No bare-head API: `head(tier)` /
  `refresh_head()` deleted (the head is the snapshot's `end()`), `HeadEvent`
  internal + hash-less (ws coverage pages read hashes via
  `ensure_block_header`). New `logs_hist(&filter, range)` /
  `blocks_hist(range)` return head-free bounded `Hist`s — a warm
  `--from-block` CLI query is fully offline (0 RPC; was 2 + a guaranteed
  `block_header` miss). `TimePosFn` closure → `TimePos` trait; new `TipSeed`
  trait; `chain.rs` split into per-source `chain/logs.rs` + `chain/blocks.rs`;
  `BlockRange` got `Display`; CLI resolvers return `BlockRange` and one-shot
  paths use the bounded hists ("last N" = snapshot + `split(tip − N)`).

---

# Open work

## R6 follow-up — `loom` model of the singleflight handshake

The R6 concurrency tests (shipped — see the ledger) cover parallelism,
disjoint/overlapping gaps, and reader-vs-writer behaviourally, but they don't
*prove* the singleflight handshake race-free under all interleavings and weak
memory. That's the `loom` model, and it's the one genuinely open R6 piece.

- **`loom` doesn't wrap the async `tokio::sync::Notify` path directly** — it
  targets sync primitives (`Mutex`/atomics/`loom::thread`). To use it, extract
  the *coordination protocol* (install-placeholder → fetch → `notify_waiters`,
  vs. peer register-wait) into a sync analog (`Mutex` + condvar/atomic flag) and
  `loom::model` that, proving the handshake race-free; the async code is then a
  thin wrapper over the proven core.
- Behavioural coverage already in place (`src/range_index/tests.rs`, now
  featureless over the synthetic `MockSpec`): the
  `walk_step`-holds-no-lock-across-await invariant is exercised by the
  multi-thread same-gap, disjoint, overlapping, and reader-vs-writer tests.
- The **codecov/patch red** on the live WS/poll wiring (`chain/wiring.rs`,
  `head_clock.rs`, `chain/rpc.rs`) is a *separate* gap — R6 exercises the
  in-memory `RangeIndex`, not the subscriber plumbing. Closing it needs the
  ignored anvil suites de-ignored or new wiring tests.

Stretch goal; the behavioural tests are the load-bearing coverage.

## Log fork tests — remove the real-RPC dependency

Three `#[ignore]` integration suites still fork mainnet via
`fork_mainnet(FORK_BLOCK)`, so anvil proxies `eth_getLogs` /
`eth_getBlockByNumber` to a real upstream — flaky and rate-limited without a
paid `MAINNET_RPC_URL` (free-tier Alchemy caps `eth_getLogs` at a 10-block
range; the public fallback throttles):

- `tests/cache_layering.rs::warm_store_then_drop_index_then_reopen` — cold/warm
  USDC `Transfer` walk.
- `tests/owned_erc20.rs` — the two `OwnedErc20` merge cases.
- `tests/anvil_smoke.rs::fork_mainnet_reads_usdc_total_supply` — `totalSupply`
  read at the pinned block.

The blocks suite (`tests/blocks_history.rs`) was already converted to a plain
`local()` anvil (mine synthetic tx-bearing blocks — no upstream). The log suites
are harder because they validate decoding of **real mainnet USDC data** at a
pinned block. To make them offline, deploy a mock ERC20 on local anvil and emit
`Transfer`s (via `alloy::sol!` + a wallet-signing provider), then assert against
the emitted events instead of real USDC. Trade-off: loses "real chain data at
block N" coverage; the point becomes the cache/merge machinery, which is what
these actually test. Keep one thin fork smoke test (behind an env gate) if real
end-to-end coverage is still wanted.

## Time anchor from the newest cached row — offline dated queries

`--from`/`--to` queries resolve the wall-clock→block anchor by demanding the
head and reading its block time (`LazyAnchor` inside the shared
`TimeLocator`): one tip poll + one block-time read per dated run, even fully
cached. The *search* probes are already cache-first (the `TimeLocator`
refinement), but the anchor itself still demands the head. Anchoring doesn't
need the *current* head — any recent anchor works. Prefer the newest cached
row (`cached_block_headers`/`cached_blocks` for EVM, `cached_btc_blocks` for
Bitcoin) and fall back to the head demand on an empty cache: warm dated
queries become fully offline like the `--from-block` path.

## Head-clock stop-on-idle — deliberately deferred

Tracking starts on first demand and then runs until the chain and all its
stores drop — there is no stop when consumers go away (user call: "keep it
one-shot for a while"). Revisit if long-lived apps with bursty consumption
show the idle poll to matter.

## R8+ — (incoming)

_Reserved for additional reactive-core points from the user._

## Family-scoped reorg tombstones — before Phase 7 cleanup wiring

`reorged_blocks` is shared by every family in the one cache file, and
heights are only comparable within one family — `cleanup_reorged_before`
(height-scoped, currently test-only) driven by one family's finality would
also drop the other family's tombstones at numerically lower heights.
Partition the table (or the delete) per family before Phase 7 wires
finality-driven cleanup. Flagged on the method in `cache/coverage.rs`.

## Bitcoin follow-ups

- **ZMQ `hashblock` head push** — the poll-only `BtcHeadClock` gets a push
  sibling (a second `HeadClock` session, exactly like the EVM ws path) for
  sub-second head latency where the node exposes ZMQ.
- **Tx-level source** — a `(height, tx_index)` position (the `EvmPosition`
  analog per `blockchain_storage.md`) over `getblock` verbosity 2, when a
  consumer needs Bitcoin transactions rather than block facts.
- **Per-network scoping** — like the EVM history tables, `cached_btc_blocks`
  carries no network column; one DB file per network is the (documented)
  operating assumption. Revisit together with the EVM per-chain scoping if
  a multi-network daemon materializes.

## CLI table/column printing — consolidate

Tabular and width-aligned output is hand-rolled with ad-hoc `format!`/`println!`
width specifiers, scattered across several modules with no shared helper:

- `main.rs::print_chain_list` — the `chains list` table (`{:>12} {:<14} …`).
- `stats.rs` — the `--stats` session summary (column-aligned RPC/cache rows).
- `fmt.rs::format_amount` — `whole_width`/`frac_width` numeric padding.
- `evm/erc20.rs`, `evm/events.rs` — per-row transfer/event renderers.
- `evm/blocks.rs`, `bitcoin/blocks.rs` — the `blocks list` tables
  (`print_header`/`print_block`).
- `config.rs::show` — key/value listing.

Each picks its own widths and alignment by hand, so columns don't line up
across commands and changing the look means editing N call sites. Worth a pass
to:

- Decide on one approach — a tiny internal `Table`/row helper (compute column
  widths from the rows, then render), or pull in a small dep (`tabled`,
  `comfy-table`) if the surface grows.
- Route the existing renderers through it so alignment/escaping/colour live in
  one place.
- Keep stdout pipeable (data to stdout, notes/warnings to stderr — already the
  convention) and don't ANSI-decorate when not a TTY.

Low priority; purely presentational. Do it before adding the *next* table
command, not as urgent debt.
