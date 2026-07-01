# TODO

Latent follow-ups from the `--chain` + RPC-resolution work (commits `7dca4ab..e36d4c1`). None block shipping; each is a self-contained improvement.

---

# Reactive core redesign (big refactor — planned, collecting points)

> A single interrelated reshape of the source-agnostic reactive core
> (`HistStore` / `Store` / matcher), surfaced by design review on 2026-07-01.
> The four points below are **one** change, not four independent ones: they all
> pull the design toward *`snapshots()` (Hists) as the single reactive
> primitive, selected per finality*. Decide the shape **before Phase 7**, which
> is currently specced to wire `best`/`safe` in the shape this refactor
> replaces. Design-first per standing preference — write it up in
> `ARCHITECTURE.md` (§5 core abstractions, §13 finality) and revise the Phase 7
> plan in `refactor.md` before touching code. **More points incoming from the
> user.**

## R1 — `HistStore::subscribe(cb)` is redundant plumbing

The push/callback variant is derivable from `snapshots()` and has **no external
consumers**.

- `subscribe(cb)` (trait `arch.rs:418`, facade `arch.rs:358`) is just
  `snapshots()` pumped into a callback: the EVM impl `history.rs:453` spawns a
  task draining `latest_store` — the *same* source `snapshots()` uses
  (`history.rs:437`). Semantics identical (both seed the current tip first via
  `Store::subscribe`).
- Only callers are the three combinators forwarding it upstream to satisfy the
  trait (`mapped.rs:81`, `filtered.rs:80`, `since.rs:77`). No example / CLI /
  test / `watch_hist` / merge / matcher uses it. (The examples' `.subscribe(
  SubscribeTransport::…)` is a *different* method — `EvmLogHistory::subscribe`,
  `history.rs:263` — the live WS/Poll wiring.)
- **Cost:** ~8 hand-written impls of the same spawn-and-pump boilerplate (EVM,
  merge, 3 combinators, 3–4 mock backends) for zero callers.
- **Direction:** make it a provided/default method over `snapshots()`, or demote
  to a free fn beside `watch_hist`, or drop it. Prefer the default method.

## R2 — `best`/`safe` tip stores are dead on both ends

`latest` is the only live tip; `best`/`safe` have no producer and no consumer.

- **No producer:** EVM stubs them — `history.rs:148-152` builds each from
  `watch::channel(earliest)` and drops the sender immediately, so `best()/safe()
  .get()` return `earliest` forever and `subscribe()` ends after one item.
- **No consumer:** grep for `.best()` / `.safe()` finds only the `arch.rs`
  delegates, combinator pass-throughs (`mapped.rs:93-97`, filtered, since), and
  merge's `TipKind` match arms. Nothing in examples / CLI / tests / matcher reads
  them.
- **Negative cost:** merge still spins up `build_tip_store(Best)` /
  `build_tip_store(Safe)` aggregator tasks (`merge.rs:455-456`) over per-source
  stores that never tick — live background work doing nothing.
- Folded into R3 (they become views, not accessors), but tracked separately in
  case R3 slips: at minimum, suppress the dead merge aggregators until a producer
  exists.

## R3 — Finality as `HistStore` views, not `Store<P>` accessors

Replace `HistStore { snapshots() [latest-driven] + latest()/best()/safe():
Store<P> }` with `history.at(Finality) -> HistStore` — one HistStore **view** per
finality level, each with its own tip-driven `snapshots()`/`next()`.

- **Constraint:** the views share **one** `LogIndex`/`LogCache` (one WS sub, one
  backfill engine); they differ only in the tip cursor bounding
  `snapshots()`/`next()`. The backend already holds three tip watches
  (`latest_tx` live; `best`/`safe` channels exist, senders dropped). Cheap facade,
  not three ingest pipelines.
- **Removes:** the `latest()/best()/safe()` trait methods + their combinator
  pass-through; merge's `TipKind` enum + the dead aggregator tasks (R2); most of
  the `subscribe(cb)` redundancy (R1) since `snapshots()` becomes *the* primitive.
- **Fixes:** finality-sensitive correlation. The matcher is hardwired to `latest`
  today; with views you hand it the `safe` views and it correlates finalized
  tails (reorg-safe bridge matching) — and can mix (src `safe`, dst `latest`),
  which the current shape can't express.
- **Superset:** a bare `Store<P>` tip is derivable from a HistStore
  (`.tip()` / `next().end()`); a snapshot stream is not derivable from a bare
  position. So today's `best()/safe(): Store<P>` are the weaker form.
- **Supersedes** Phase 7's plan to populate `best`/`safe` as `Store<P>` positions
  — build the view shape instead.

## R4 — Matcher should fold the Hist it was triggered by

The matcher is `f(src_hist, dst_hist)` but its wiring triggers on a `latest()`
position tick and then *re-materializes* the Hists via `next()`, bypassing
`snapshots()` entirely.

- **Path today:** `AnchoredMatcher::iter()` (`matcher.rs:531`) → `changes()`
  [merges `src.latest().subscribe()` + `dst.latest().subscribe()` into a bare
  `()` tick, `matcher.rs:366-396`] → `current_state()` → `compute_state()`
  rebuilds windows with `src_store.next()` + `split(anchor)` + `iter()`
  (`matcher.rs:316-321`). `snapshots()` is never used.
- **Legit reason for the split (keep):** the anchor is caller state supplied at
  compute time — `get_history(since)` (`matcher.rs:357`) computes at an arbitrary
  time-derived anchor — so the trigger must be anchor-agnostic.
- **But** that justifies decoupling *anchor* from trigger, not discarding the
  *Hist*. Current cost: domain inversion; trigger ≠ data race (tick at tip T₁,
  `next()` reads current T₂ ≥ T₁, state emitted at T₂); two parallel subscription
  systems feeding one matcher.
- **Not a perf bug:** `next()` is `Hist::new(backend, earliest, tip)` — a watch
  read + Arc clone, no fetch (`history.rs:447`); `Hist` is a lazy handle; the one
  real fetch (`iter()`) is necessary and cached. This is design clarity, not
  wasted RPC/SQLite.
- **Direction:** drive off `src.snapshots()` + `dst.snapshots()` (of the chosen
  finality, R3); carry `(src_hist, dst_hist)` into
  `compute_state(src_hist, dst_hist, anchors)` and split at the anchor inside;
  drop the `latest()`-tick + `next()`-refetch. Collapses the two subscription
  systems and removes the last `latest()` consumer.

## R5 — `Store<T>`: keep the reactive value, shed the Svelte-ism (post-R3/R4)

`Store<T>` (`arch.rs:155`) is a thin newtype over `tokio::sync::watch::Receiver<T>`
— a reactive **value**, not a stream. Do **not** "replace it with a `Stream`":
its load-bearing half is `get()`, the *synchronous* current-value read a `Stream`
can't provide (used by every backend's `next()` → `Hist::new(_, _, latest.get())`:
`history.rs:449`, `merge.rs:513`, mocks; `refresh_tip` `history.rs:254`;
`bridge_explorer.rs:151`). "Current value + change notifications" is exactly
`watch`, which `Store<T>` already is — so the pattern is idiomatic under the
hood; only the surface is TS-flavoured.

- **Skin-deep TS-isms worth revisiting:** the *name* `Store` (Svelte; also
  collides mentally with `LogStore`/`AnchorStore` — consider `Signal`/`Watched`);
  and `subscribe()` returning a boxed `Subscription<T> = Pin<Box<dyn Stream>>`
  (`arch.rs:70`) — a per-call alloc + dyn dispatch, justified only where it
  crosses `dyn HistStoreBackend`; use `impl Stream`/`WatchStream` off the `dyn`
  paths.
- **Or** drop the wrapper and expose `watch::Receiver<T>` directly — less code,
  but leaks `tokio` into the public API, swaps `get()` for `.borrow()` (a `Ref`
  guard), and pushes `tokio-stream` onto consumers. The wrapper's stable,
  tokio-hidden surface is a reasonable newtype, not debt to purge.
- **Sequencing (important):** R3 + R4 delete most public `Store` callers —
  `changes(): Store<()>` (R4) and `latest()/best()/safe(): &Store<P>` (R3) are
  the bulk of them. What survives is mostly the *internal* watch channel driving
  `snapshots()`, possibly a small `.tip() -> Store<P>` on views. **Decide
  `Store`'s fate after R3/R4**, against whatever public reactive-value remains —
  renaming/unboxing it now buffs a surface about to shrink ~70%.

## R6 — Shared-index concurrency test coverage

One test exercises concurrent consumers over the same BTree today:
`concurrent_searches_in_same_gap_fire_one_fetch` (`log_index.rs:586`) — 5
`tokio::spawn`ed walkers over one `Arc<LogIndex>` and the *same* cold gap,
asserting all 5 see the item **and** exactly 1 fetch (singleflight coalescing
via the per-range `Notify`). Good, but narrow. Gaps that matter because R3/R4
change *who* reads the shared index (per-finality views, Hist-driven matcher all
walking one backend):

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

Self-contained (pure test additions, no API change), but sequence *with* R3/R4:
the reshaped reactive core is what these races run through, so write them against
the new shape rather than the current one.

## R7+ — (incoming)

_Reserved for the additional points the user will add._

---

## CLI error rendering uses Debug, not Display — done (2026-06-29)

`main` now returns `std::process::ExitCode` and renders an `Err` via its
`Display` (`eprintln!("Error: {err}")`), then exits non-zero. No `anyhow` dep —
the project's error types already embed their `source` in the `#[error(...)]`
string, so the top-level Display line is the full message; walking `.source()`
would just repeat it. Verified: `--chain notarealchain` now prints the
`UnknownName` sentence instead of its `Debug`.

## `chainfold chains list` subcommand — done (2026-06-29)

`Command::Chains { ChainsCommand::List }` prints `chainId · shortName ·
chainSlug · name` from the cached chainlist, sorted by chain id. Flags
`--testnet` and `--alchemy-supported` filter via `ChainResolver::list(ListFilter)`
(testnet → `isTestnet:true`; alchemy → an `alchemy_chain_slug` exists). Filtering
is unit-tested; printing is a thin `print_chain_list` in `main.rs` (see the
table-consolidation item below).

## Surface `is_testnet` and `red_flags` to the user — done (2026-06-29)

`evm::run` emits advisories for the resolved chain when the resolver is loaded
(name lookup or public-RPC fallback): a `note:` for testnets and a loud
`warning:` listing `red_flags` (we warn, not refuse). Backed by
`ChainlistEntry::is_testnet()` / `red_flags()` accessors (unit-tested) and
`ChainResolver::entry(chain_id)`. The fields are now read in non-test code, but
the struct keeps `#[allow(dead_code)]` for the still-unread schema-fidelity
fields (`chain`, `network_id`, `info_url`, `native_currency`) — reason text
updated to say so. (Follow-up, same session: `from_entries` now resolves
shortName/chainSlug collisions to the **lowest** chain id, so `--chain kub` →
96 — the red-flagged Bitkub mainnet — deterministically; the resolver/config/evm
path was also typed end-to-end with `ChainId` + a `ChainHandle` key newtype, and
resolved RPC URLs are now `Url` not `String`.)

## Reevaluate CLI table/column printing and consolidate

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

## Remaining domain newtypes — done

The type-discipline sweep is complete; no bare block-number, log-index, or
filter-hash primitives remain in the EVM layer. Shipped:

- **`FilterHash([u8; 32])`** — the BLAKE3 cache partition key; `ToSql` lives in
  the cache layer so the domain type stays SQLite-free.
- **`LogIdx(u32)`** — the per-block log index. Named `LogIdx`, *not* `LogIndex`,
  to avoid colliding with the `LogIndex` in-memory range tree.
- **`BlockNumber(u64)`** — block heights, with an *affine* algebra: a block ± a
  `u64` offset is a block (`Add`/`Sub`/`saturating_*`/`checked_sub`); the
  distance between two blocks is a `u64` via `.get()`. The SQLite/`LogCache`
  layer, `compute_gaps`, `aligned_block_ranges`, and the alloy RPC boundary stay
  `u64`, converting at the seam.

Both `LogIdx` and `BlockNumber` are `#[serde(transparent)]` so the `EvmPosition`
postcard marshal — and saved resume-anchors — stay byte-identical.

## Blind-type audit — further unblinding backlog (2026-06-28)

A whole-codebase sweep for the remaining blind primitives (bare `bool`,
stringly-typed vocabularies, domain-meaning integers, and unnamed tuples),
beyond the EVM newtypes above. Findings are tiered by value-to-friction; each
is a self-contained change.

### Shipped (2026-06-29) — one commit each

The dead-type deletion plus all of Tier 1 and Tier 3, and two of three Tier 2
items, landed:

- **Deleted** dead `EvmBlock`/`EvmTransaction` (zero usages). `Amount`/`Asset`/
  `Erc20.decimals` were confirmed **live** (used by `Erc20Transfer`/`Erc20Balance`
  / `impl Asset for Erc20`) and aren't blind primitives — left as-is.
- **`Timestamp`** is now a newtype in `src/arch.rs` (was `type Timestamp = u64`).
  Minimal API (`new`/`get`/`From<u64>` + Ord/Copy); the one arithmetic site
  (`estimate_block_from_time`) drops to `u64` against the internal anchor; seams
  (chrono, alloy RPC, SQLite header, formatters) convert at the boundary.
- **`PrivacyTracking`** serde enum replaced the stringly `privacy_rank` match;
  `#[serde(other)]` + `#[default]` `Unspecified` keeps absent/unknown behavior.
- **`EventSignatureRow`** is a named struct; `topic0` is now a `Topic`.
- **`Direction`/`FollowMode`** funnel the `--reverse`/`--follow` flags
  (mirroring `HexDetail::from_full`), killing the adjacent-positional-bool
  hazard in `run`/`erc20::run`/`events::run`.
- **`Inclusivity {Inclusive, Exclusive}`** replaced the nav `inclusive: bool`
  (named so, not `Bound`, to avoid the `std::ops::Bound` clash).
- **`Alignment {Aligned, Trailing}`** replaced `AlignedRange.is_aligned`.
- **`retry.rs`** magic `3, 500, 100` are now named consts.
- **`TransferDirection {Out, In}`** is the typed source of truth for the
  `OUT_KEY`/`IN_KEY` merge keys (consts now derive from a `const fn as_str`; the
  wire format is unchanged).
- The magic **`12`** block-time literal is now `DEFAULT_BLOCK_TIME`.

**Shipped (2026-06-29, second batch):**

- **`SourceKey(String)`** (Tier 2) — the key type of `MergedPos.positions` and
  `MergedItem.source_key`. Only the *key* is newtyped; the value bytes stay
  `Vec<u8>` (the deliberately-generic erased positions). `#[serde(transparent)]`
  keeps the `MergedPos` marshal byte-identical; `Borrow<str>` keeps
  `MergedPos::get(&str)` ergonomic.
- **`BlockRange { from, to: BlockNumber }`** (Tier 3) — replaced the unnamed
  `(u64, u64)` in `compute_gaps`, `align_window`, and `loaded_windows`. The
  set/alignment arithmetic stays raw `u64` internally (BlockRange wraps only the
  signature); `get_scanned_ranges` keeps returning raw rows and
  `ensure_window_loaded` converts at the one consumption point — honoring the
  "SQLite seam stays u64" principle. `chunk_size`/`block_time` left as scalar
  config (not range tuples; low value).

### Not done — deliberate

- **`decode: bool`** (`cli.rs` `EventsArgs`) is left a bool: unlike `reverse`/
  `follow` it's a named field read once (`if events_args.decode`), not a
  positional arg, so it never had the swap/ambiguity problem the audit targeted.
- **Chain identifier strings** (Tier 3; `chains.rs` `name`/`short_name`/`chain`/
  `chain_slug`). Skipped as low-value/high-friction: the load-bearing ones
  (`short_name`/`chain_slug`) get `.to_ascii_lowercase()`'d into `String`
  lookup keys one line later (a newtype would unwrap immediately), and
  `name`/`chain` are display-only/near-dead fields. Newtyping here would be
  churn without enforcement value.

### Don't touch (intentionally generic)

`MergedPos`'s `BTreeMap<String, Vec<u8>>` and `Position::marshal() -> Vec<u8>`:
the type-erasure boundary is deliberately bytes/strings so the source-agnostic
core can merge heterogeneous positions. Newtyping there fights the generic core.
