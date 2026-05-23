# Chain name resolver via chainlist.org

**Status:** approved
**Date:** 2026-05-24
**Scope:** `src/bin/chainfold` (CLI only — library crate untouched)

## Goal

Let users specify the target EVM chain by a human-readable name (e.g.
`--chain arbitrum`) instead of a numeric chain ID. Resolution maps the
name to a chain ID via `https://chainlist.org/rpcs.json`, cached on
disk. Also add BNB Smart Chain (chainId 56) to the Alchemy slug table so
`--chain bnb` works end-to-end against a default Alchemy URL.

## Non-goals

- No fuzzy / substring matching. `--chain bsc` does not resolve; user
  must use `--chain bnb` or `--chain binance` (the `shortName` /
  `chainSlug` chainlist.org publishes).
- No alias overlay table maintained inside chainfold.
- No TTL on the cache. Refresh = manual `rm`.
- No library-level changes (the resolver is binary-local).
- No new dependency on an external chainlist crate (evaluated and
  rejected — see "Source selection" below).

## Source selection

`chainlist.org/rpcs.json` is chosen over `chainid.network/chains.json`
because it ships the `chainSlug` field used for name resolution:

| | chainid.network | chainlist.org |
|---|---|---|
| Has `chainSlug` | 0 entries | 302 entries (covers all popular chains) |
| Size | 1.07 MB | 2.00 MB |
| Entries | 2,631 | 2,751 |
| Maintainer | `ethereum-lists` GitHub org | DefiLlama |
| Position | canonical upstream | downstream, enriches upstream |

Without `chainSlug`, `--chain arbitrum` cannot resolve (Arbitrum's
`shortName` is `arb1`, not "arbitrum"). The whole motivation for the
feature evaporates against chainid.network.

The cost of choosing chainlist.org is its third-party bus factor. If
DefiLlama ever drops the endpoint, the fallback is a one-line URL swap
to chainid.network, accepting that slug-style names stop working. We do
not implement a runtime fallback chain; the failure mode is rare enough
that a code change is acceptable.

## CLI surface

Replace the existing `chain_id: u64` (default 1) on `EvmArgs` with two
mutually exclusive `Option` fields. Exactly one must be set; clap
enforces this — no implicit default.

```rust
/// Chain name. Matched against chainlist.org's shortName or chainSlug fields.
/// Example: --chain ethereum, --chain arbitrum, --chain bnb
#[arg(long, required_unless_present = "chain_id", conflicts_with = "chain_id")]
pub chain: Option<String>,

/// Numeric chain ID. Use for chains not listed in chainlist.org.
/// Example: --chain-id 56
#[arg(long, required_unless_present = "chain", conflicts_with = "chain")]
pub chain_id: Option<u64>,
```

This is a breaking change for any script that previously relied on the
implicit mainnet default. Acceptable per the project's "early-stage,
clean-slate" stance — and clap emits a clear "one of --chain or
--chain-id is required" error.

## Data model

New module: `src/bin/chainfold/chains.rs`.

```rust
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainlistEntry {
    pub chain_id: u64,
    pub name: String,
    pub short_name: String,
    pub chain: String, // ticker, e.g. "ETH", "BSC"
    #[serde(default)]
    pub chain_slug: Option<String>,
    #[serde(default)]
    pub network_id: Option<u64>,
    #[serde(default)]
    pub info_url: Option<String>,
    #[serde(default)]
    pub is_testnet: Option<bool>,
    #[serde(default)]
    pub red_flags: Option<Vec<String>>,
    pub native_currency: NativeCurrency,
    // rpc[], explorers[], features[], parent, tvl, icon, status,
    // faucets[], ens, slip44 — not modeled; serde drops unknown fields
    // silently.
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct NativeCurrency {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}
```

`is_testnet` and `red_flags` are modeled now (cheap, `#[serde(default)]`)
so later UX (e.g. testnet warning) doesn't require a struct migration.
All other unmodeled fields are silently ignored by serde — adding them
later is one line each, breaks nothing.

## Resolver

```rust
pub struct ChainResolver {
    cache_path: PathBuf,
    by_name: HashMap<String, u64>, // lowercase(short_name | chain_slug) → chain_id
}

#[derive(Debug, thiserror::Error)]
pub enum ChainResolveError {
    #[error("unknown chain name '{name}': no shortName or chainSlug matches in chainlist.org. \
             Try --chain-id <number>, or check https://chainlist.org for the canonical name.")]
    UnknownName { name: String },
    #[error("fetching chainlist.org: {0}")]
    Fetch(#[from] reqwest::Error),
    #[error("cache I/O at {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("parsing cache at {path} (delete the file to refresh): {source}")]
    Parse { path: PathBuf, source: serde_json::Error },
}

impl ChainResolver {
    pub async fn load_or_fetch() -> Result<Self, ChainResolveError>;
    pub fn resolve(&self, name: &str) -> Result<u64, ChainResolveError>;
}
```

### `load_or_fetch` flow

1. Compute `cache_path = dirs::cache_dir().unwrap_or(PathBuf::from(".")).join("chainfold").join("chainlist.json")`.
2. If the file exists, read it and deserialize as `Vec<ChainlistEntry>`. On parse error, return `ChainResolveError::Parse` (message includes the path so user knows what to delete).
3. If the file does not exist:
   a. Ensure parent directory exists (`fs::create_dir_all`).
   b. `GET https://chainlist.org/rpcs.json` with a 30 s timeout.
   c. Write atomically via `tempfile::NamedTempFile::new_in(parent)?` → write body → `persist(cache_path)` (atomic rename on the same filesystem).
   d. Deserialize as above.
4. Build `by_name`: for each entry, insert `(short_name.to_lowercase(), chain_id)`, and if `chain_slug.is_some()`, insert `(chain_slug.unwrap().to_lowercase(), chain_id)`. Last-write-wins on collision; collisions between distinct chains are unobserved in the current dataset and aren't worth designing around in v1.

### `resolve`

```rust
pub fn resolve(&self, name: &str) -> Result<u64, ChainResolveError> {
    self.by_name
        .get(&name.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| ChainResolveError::UnknownName { name: name.to_owned() })
}
```

No I/O. Pure HashMap lookup.

## Wiring

In `src/bin/chainfold/evm/mod.rs`, replace the current chain-id
extraction at the top of the EVM subcommand handler (currently around
line 20) with:

```rust
let chain_id = match (args.chain_id, args.chain.as_deref()) {
    (Some(id), _)      => id,
    (None, Some(name)) => ChainResolver::load_or_fetch().await?.resolve(name)?,
    (None, None)       => unreachable!("clap enforces required_unless_present"),
};
let rpc_url = config.rpc_url(args.rpc_url.as_deref(), chain_id)?;
// ... rest unchanged; existing chain-ID round-trip check at :29-33 still applies.
```

The `(Some(id), _)` arm short-circuits before `ChainResolver::load_or_fetch`
is called. Users who pass `--chain-id 56` on a fresh machine with no
cache file never hit chainlist.org — the binary works offline as long
as you can name the chain by number.

The provider-side `provider.get_chain_id()` check (currently
`evm/mod.rs:29-33`) stays in place. It catches:
- chainlist.org returning a stale or wrong ID,
- user-supplied `--rpc-url` pointing at a different chain than
  `--chain-id` claimed,
- our cache being corrupted such that the resolver returns a wrong ID.

Defense in depth — the resolver is for ergonomics, the runtime check is
for correctness.

## Alchemy slug table

One line added to `alchemy_chain_slug` in `src/bin/chainfold/config.rs`
(near line 174):

```rust
56 => Ok("bnb-mainnet"),
```

Verified slug against Alchemy's documented URL pattern.

## Error UX

- `--chain unknownthing` → `unknown chain name 'unknownthing': no shortName or chainSlug matches in chainlist.org. Try --chain-id <number>, or check https://chainlist.org for the canonical name.`
- Corrupt cache → `parsing cache at /home/u/.cache/chainfold/chainlist.json (delete the file to refresh): expected ',' or ']' at line 12345 column 67`
- Network failure on first fetch → `fetching chainlist.org: <reqwest error>`. User can retry, or pass `--chain-id <num>` to bypass entirely.
- Unsupported chain for Alchemy (after successful name resolution) →
  existing `ConfigError::UnsupportedChain(chain_id)` is raised by
  `Config::rpc_url`. Already plumbed; no change.

## Dependencies

New (none of these are already in `Cargo.toml`):
- `dirs` — XDG cache directory lookup. Tiny crate, no transitive deps of note.
- `tempfile` — atomic write via `NamedTempFile::persist`.

Already present:
- `reqwest` — used by `signatures.rs` for Sourcify/Etherscan; reuse.
- `serde`, `serde_json` — used everywhere.
- `thiserror` — used by `signatures.rs`, `config.rs`.
- `tokio` — used by the binary's main.

## Testing

In `src/bin/chainfold/chains.rs`:

- `parses_ethereum_entry` — deserialize a single-entry JSON fixture covering all modeled fields including `chain_slug` and `is_testnet`; assert every field round-trips.
- `parses_entry_without_optional_fields` — deserialize an entry with `chainSlug`, `isTestnet`, `redFlags`, `networkId`, `infoURL` all missing; assert no error and `None` for each.
- `parses_unknown_fields_silently` — feed an entry containing `tvl`, `icon`, `ens`, `status`, etc.; assert the known fields still deserialize correctly.
- `resolver_matches_short_name` — `ChainResolver` built from an in-memory fixture; `resolve("eth")` returns 1.
- `resolver_matches_chain_slug` — same fixture; `resolve("ethereum")` returns 1.
- `resolver_match_is_case_insensitive` — `resolve("Arbitrum")` returns 42161.
- `resolver_unknown_name_errors` — `resolve("bsc")` returns `UnknownName` against a fixture containing the BSC entry (which has shortName=bnb, chainSlug=binance, no "bsc").
- `load_from_disk_roundtrip` — write a JSON fixture into a `tempdir`-rooted cache path, instantiate `ChainResolver` from that path (test-only constructor), assert resolution works.

No live network calls in tests. No mocking of HTTP — the only path that
hits the network (`load_or_fetch` when cache is missing) is exercised
via the test-only constructor that takes a cache path directly.

In `src/bin/chainfold/config.rs`:

- `alchemy_slug_bnb_mainnet` — `alchemy_chain_slug(56)` returns `"bnb-mainnet"`.

## Out of scope (deferred)

- Auto-refresh / TTL.
- `chainfold chains list` subcommand. Useful, not blocking, skip for v1.
- Surfacing `is_testnet` in the CLI ("you are pointing at a testnet — proceed?"). Field is captured for future use; no current consumer.
- `red_flags` warnings. Same posture as `is_testnet`.
- Library-crate exposure of the resolver. The binary owns it; library users who need name resolution can copy the module or we extract it later.
