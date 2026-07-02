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
- **R6 — shared-index concurrency tests** — `RangeIndex` walk under true
  parallelism (multi-thread runtime), disjoint/overlapping gaps (singleflight
  tiles the union, no double-fetch), and reader-vs-writer (`insert_page` racing
  a mid-walk `items()`: no dup / no drop / no panic). Test suite split out to
  `src/evm/range_index/tests.rs`. Stretch goal **not** done: a `loom` model of
  the singleflight handshake (needs a sync analog of the async `Notify` path).

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
- Behavioural coverage already in place (`src/evm/range_index/tests.rs`): the
  `walk_step`-holds-no-lock-across-await invariant is exercised by the
  multi-thread same-gap, disjoint, overlapping, and reader-vs-writer tests.
- The **codecov/patch red** on the live WS/poll wiring (`chain/wiring.rs`,
  `head_clock.rs`, `chain/rpc.rs`) is a *separate* gap — R6 exercises the
  in-memory `RangeIndex`, not the subscriber plumbing. Closing it needs the
  ignored anvil suites de-ignored or new wiring tests.

Stretch goal; the behavioural tests are the load-bearing coverage.

## R8+ — (incoming)

_Reserved for additional reactive-core points from the user._

## CLI table/column printing — consolidate

Tabular and width-aligned output is hand-rolled with ad-hoc `format!`/`println!`
width specifiers, scattered across several modules with no shared helper:

- `main.rs::print_chain_list` — the `chains list` table (`{:>12} {:<14} …`).
- `stats.rs` — the `--stats` session summary (column-aligned RPC/cache rows).
- `fmt.rs::format_amount` — `whole_width`/`frac_width` numeric padding.
- `evm/erc20.rs`, `evm/events.rs` — per-row transfer/event renderers.
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
