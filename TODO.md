# TODO

Latent follow-ups from the `--chain` + RPC-resolution work (commits `7dca4ab..e36d4c1`). None block shipping; each is a self-contained improvement.

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
