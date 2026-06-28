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

## Remaining domain newtypes (type-discipline follow-ups)

The 2026-06 type pass replaced the blind primitives in the EVM layer with
semantic types — `ChainId`, `Address`/`B256` (+ alloy's `TxHash`/`BlockHash`
aliases), `Topic`, and bundled `chain_id` into `ChainProvider`. Three blind
integers/keys remain; each is a self-contained newtype sweep, listed in
intended order (low friction → high).

### `FilterHash` — the BLAKE3 cache key

`compute_filter_hash` returns `[u8; 32]`, threaded as `filter_hash: &[u8; 32]`
through every `LogCache` method and `LogIndex`. It's a chainfold cache
partition key (BLAKE3 of address + topics), **not** an EVM hash — so it wants
its own `FilterHash([u8; 32])` (or wrap `blake3::Hash`), not a `B256`/alias.
Low friction: lives entirely in the cache/index layer — no alloy interop, no
arithmetic.

### `LogIndex` — the per-block log index

`EvmPosition.log_index` / `EvmEvent.log_index` are bare `u32`. Wrap as
`LogIndex(u32)`. `EvmPosition` is postcard-serialized (`Position::marshal`), so
the newtype needs `#[serde(transparent)]` to keep saved resume-anchors
byte-identical. Pairs naturally with `FilterHash` (both cache/index layer).

### `BlockNumber` — the high-friction one

`EvmPosition.block`, `EvmEvent.block_number`, `BlockHeader.height`,
`scanned_ranges`, `LogPage.from/to_block`, `BlockPos::At { block }`, the fetcher
`(u64, u64)` bounds, `time_pos` / `gap_block_range`, and the SQLite `i64`
boundaries are all bare `u64`. It's used in arithmetic *everywhere*
(`to - from + 1`, `saturating_sub`, `+ 1`), so the open design call before
starting is: `impl Add/Sub` on `BlockNumber` (keeps arithmetic readable) vs.
`.get()`-unwrap at each site. Also needs `#[serde(transparent)]` for the
`EvmPosition` marshal.
