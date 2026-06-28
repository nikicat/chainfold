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
is a self-contained change. Nothing here is started.

### Dead types — delete, don't unblind

`EvmBlock` (`src/evm/types.rs:405`) and `EvmTransaction` (`src/evm/types.rs:413`)
have **zero usages** — only defined and re-exported from `src/evm.rs`. Their
`u128`/`u64`/`BlockHash` fields look like unblinding candidates but the types
are scaffolding; delete them (and the re-exports) rather than dressing them up.
`Amount`/`Asset` (`types.rs:424`/`431`) and the second `decimals: u8`
(`types.rs:499`) also look thinly used — confirm they're live before investing.

### Tier 1 — worth doing (verified load-bearing)

- **`Timestamp` → newtype.** `src/arch.rs:42` is `pub type Timestamp = u64` — a
  *weak alias*, not a newtype. It's the "a timestamp can't be swapped with a
  block number" gap left after `BlockNumber`. Flows through `EvmEvent.timestamp`,
  `BlockHeader.timestamp`, `HistStore::time_pos`, `act_and_wait`, and the matcher
  time-anchors. Source-agnostic (stays in core, no EVM ref); no serde/marshal
  boundary. Medium friction, high payoff — the natural next newtype.
- **`PrivacyTracking` enum.** `src/bin/chainfold/chains.rs:51` `privacy_rank()`
  matches bare strings `Some("none")/"limited"/"yes"` off
  `RpcEntry.tracking: Option<String>` (`chains.rs:109`, chainlist JSON).
  Stringly-typed and load-bearing (drives RPC selection). Replace with
  `#[serde(rename_all = "lowercase")] enum { None, Limited, Yes, Unspecified }`.
  Low–med friction.
- **`EventSignatureRow` → named struct.** `src/evm/cache/event_signature_cache.rs:17`
  is `pub type EventSignatureRow = (String, B256, Option<String>)` — the naked
  3-tuple smell. `struct EventSignatureRow { signature: String, topic0: Topic,
  abi_json: Option<String> }`. Low friction.
- **CLI flag bools → `clap::ValueEnum`s.** `src/bin/chainfold/cli.rs`:
  `reverse: bool` (`:14`), `follow: bool` (`:18`), `decode: bool` (`:141`) reach
  call sites as `run(.., true, false)` with no clue. `enum Direction { Forward,
  Backward }`, `FollowMode { Once, Follow }`, `DecodeMode { Skip, Decode }` — same
  class as the shipped `HexDetail`. (`full: bool` at `:135` already funnels into
  `HexDetail`.) Medium friction (clap).

### Tier 2 — reasonable

- **nav `inclusive: bool`.** `src/evm/log_index/nav.rs:28/33/42/47` call
  `cached_navigate_forward(pos, true)` / `(pos, false)`; the bare `true`/`false`
  is "inclusive". A `Bound::{Inclusive, Exclusive}` enum kills the mystery bool.
- **`TransferDirection` enum** for `OUT_KEY`/`IN_KEY` (`src/evm/owned_erc20.rs:40`,
  `:42`). Note it bottoms out at the *generic* merge `BTreeMap<String, …>` key, so
  it carries an `.as_str()` at that boundary.
- **`SourceKey(String)`** (`src/merge.rs:69` `positions: BTreeMap<String, Vec<u8>>`,
  `:102` `source_key: String`). Source-agnostic, but pervasive and serialized into
  `MergedPos`. Moderate value, medium friction.

### Tier 3 — cosmetic / cheap wins

- **Quick win: the magic `12`.** `src/bin/chainfold/evm/events.rs:79` and
  `erc20.rs:93` pass a literal `12` as the last `with_options` arg — it's
  `block_time` seconds (the audit agent mis-guessed it as "concurrency", which is
  itself the symptom). Use the lib's `DEFAULT_BLOCK_TIME` const, or call
  `EvmLogHistory::new` (which defaults it). Tiny, removes real confusion.
- **`AlignedRange.is_aligned: bool`** (`src/evm/aligned_ranges.rs:19`) →
  `enum Alignment { Aligned, Trailing }`.
- **`retry.rs:19`** `new_with_policy(3, 500, 100, …)` — three magic numbers
  (retries, backoff-ms, CU budget) → named consts or a small `RetryConfig`.
- **`ChunkSize` / block-range tuples.** `chunk_size`/`block_time` fields
  (`history.rs`), and the `(u64, u64)` block ranges in `compute_gaps`
  (`cache/log_cache.rs:42`), `loaded_windows` (`log_index.rs:69`), and
  `align_window` (`range_tree.rs:302`) → a `BlockRange { from, to: BlockNumber }`.
  Lower value now that `BlockNumber` exists.
- **Chain identifier strings.** `chains.rs:74–78` (`name`/`short_name`/`chain_slug`)
  are distinct identifier kinds all typed `String`; newtypes are possible but
  high-friction (chainlist JSON + `by_name` lookups) and low value.

### Don't touch (intentionally generic)

`MergedPos`'s `BTreeMap<String, Vec<u8>>` and `Position::marshal() -> Vec<u8>`:
the type-erasure boundary is deliberately bytes/strings so the source-agnostic
core can merge heterogeneous positions. Newtyping there fights the generic core.

### Suggested next batch (value-to-friction order)

Delete `EvmBlock`/`EvmTransaction`; then `Timestamp` newtype, `PrivacyTracking`
enum, `EventSignatureRow` struct, and the `12`→const fix — one commit each. The
CLI flag-enums and `SourceKey` are a good follow-on.
