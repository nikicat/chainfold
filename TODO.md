# TODO

Latent follow-ups from the `--chain` + RPC-resolution work (commits `7dca4ab..e36d4c1`). None block shipping; each is a self-contained improvement.

## CLI error rendering uses Debug, not Display

`src/bin/chainfold/main.rs` returns `Result<(), Box<dyn std::error::Error>>`, so unhandled errors print via `Debug`. The nice `#[error(...)]` Display strings on `ChainResolveError`, `ConfigError::NoRpcAvailable`, `SignatureError`, etc. never reach the user.

Example today:
```
Error: NoRpcAvailable { chain_id: 314159265358, tried: [...] }
```

Should be:
```
Error: no RPC URL available for chain 314159265358 (tried: ...).
Pass --rpc-url <url>, set ALCHEMY_KEY in the environment for a supported chain, or ...
```

Fix: switch `main`'s return type to `anyhow::Result<()>` (or write a manual print loop in `main` that walks the error chain via `Display`). Affects every error type uniformly — single-line change in `main`, possibly one new dep.

## `chainfold chains list` subcommand

No way to discover which `--chain <name>` values resolve without consulting `chainlist.org` manually. A `chains list` subcommand could print `(chainId, shortName, chainSlug, name)` from the cached chainlist — optional `--testnet` / `--alchemy-supported` filters.

## Surface `is_testnet` and `red_flags` to the user

`ChainlistEntry` already captures both fields (with `#[allow(dead_code, reason = ...)]` documenting the intent). Ideas:

- Warn on stderr when the resolved chain has `is_testnet == true`. (`note: chain 'sepolia' is a testnet`.)
- Refuse to proceed (or warn loudly) when `red_flags` is non-empty — DefiLlama flags chains with known scam/spam issues.

When either is wired up, the field stops being dead and the corresponding `#[allow]` can be removed.

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

### Not done — deliberate

- **`decode: bool`** (`cli.rs` `EventsArgs`) is left a bool: unlike `reverse`/
  `follow` it's a named field read once (`if events_args.decode`), not a
  positional arg, so it never had the swap/ambiguity problem the audit targeted.

### Remaining backlog

- **`SourceKey(String)`** (Tier 2; `src/merge.rs` `positions: BTreeMap<String,
  Vec<u8>>`, `source_key: String`). Source-agnostic, but pervasive and
  serialized into `MergedPos`. Moderate value, medium friction — and it lives
  right next to the deliberately-generic boundary below.
- **`ChunkSize` / block-range tuples** (Tier 3). `chunk_size`/`block_time`
  fields (`history.rs`) and the `(u64, u64)` block ranges in `compute_gaps`,
  `loaded_windows`, `align_window` → a `BlockRange { from, to: BlockNumber }`.
  Lower value now that `BlockNumber` exists.
- **Chain identifier strings** (Tier 3). `chains.rs` `name`/`short_name`/
  `chain_slug` are distinct identifier kinds all typed `String`; newtypes are
  high-friction (chainlist JSON + `by_name` lookups) and low value.

### Don't touch (intentionally generic)

`MergedPos`'s `BTreeMap<String, Vec<u8>>` and `Position::marshal() -> Vec<u8>`:
the type-erasure boundary is deliberately bytes/strings so the source-agnostic
core can merge heterogeneous positions. Newtyping there fights the generic core.
