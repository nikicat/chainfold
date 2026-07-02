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

---

# Open work

## R6 — Shared-index concurrency test coverage

One test exercises concurrent consumers over the same BTree today:
`concurrent_searches_in_same_gap_fire_one_fetch` (in `range_index.rs` tests) —
5 `tokio::spawn`ed walkers over one `Arc<RangeIndex<LogSpec>>` and the *same*
cold gap, asserting all 5 see the item **and** exactly 1 fetch (singleflight
coalescing via the per-range `Notify`). Good, but narrow. The gaps that matter,
now that per-finality views and the Hist-driven matcher all walk one backend:

- **Parallel, not just concurrent.** It's `#[tokio::test]` (current-thread), so
  tasks interleave at await points but never run on two cores at once. The BTree
  is behind a `Mutex`, so add a `#[tokio::test(flavor = "multi_thread",
  worker_threads = N)]` variant (plus deterministic scheduling via
  `tokio::task::yield_now` orchestration or `turmoil`) to catch true data races /
  lost wakeups, not just cooperative interleaving. A **`loom` model** is the
  gold standard for the lost-wakeup class but does **not** wrap the async
  `tokio::sync::Notify` path directly — loom targets sync primitives
  (`Mutex`/atomics/`loom::thread`). To use it, extract the *coordination
  protocol* (install-placeholder → fetch → `notify_waiters`, vs. peer
  register-wait) into a sync analog (`Mutex` + condvar/atomic flag) and
  `loom::model` that, proving the handshake race-free under all interleavings
  and weak memory; the async code is then a thin wrapper over the proven core.
- **Disjoint / overlapping ranges.** Only the *identical-gap* case is covered.
  Add walkers over different-but-overlapping and fully-disjoint ranges on one
  BTree — verifies gap computation + placeholder installation don't corrupt the
  tree or over/under-fetch when concurrent gaps touch adjacent ranges.
- **Reader vs. writer.** No test drives a walk *in flight* while the subscriber's
  `insert_page` (live path) mutates the same BTree. This is the real production
  race (WS watcher writing while consumers walk, per §14 ownership) and is
  currently unpinned. Add a test that interleaves `items()` iteration with
  `insert_page`/late-item insertion and asserts no dropped/duplicated items and
  no panic.

Self-contained (pure test additions, no API change). Closing the reader-vs-writer
gap also lifts the codecov/patch red on the live WS/poll path.

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
