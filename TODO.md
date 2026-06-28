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
