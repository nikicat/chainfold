# Chain Name Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--chain <name>` to the `chainfold` CLI, resolving names against a cached `chainlist.org/rpcs.json`, and wire BNB Smart Chain into the Alchemy slug table.

**Architecture:** A new binary-local module `src/bin/chainfold/chains.rs` owns typed parsing of `chainlist.org/rpcs.json` and a `ChainResolver` that builds a `HashMap<lowercased_name, chain_id>` from the union of `shortName` + `chainSlug` fields. Cache lives at `~/.cache/chainfold/chainlist.json`, written atomically on first miss, refreshed by manual `rm`. The CLI surface on `EvmArgs` replaces the implicit-default `chain_id: u64` with mutually exclusive `chain: Option<String>` and `chain_id: Option<u64>` (exactly one required, enforced by clap). The existing `provider.get_chain_id()` cross-check stays in place as defense in depth.

**Tech Stack:** Rust (edition 2021), serde / serde_json, reqwest (already a dep), thiserror, dirs (already cli-gated), tempfile (added as cli-gated runtime dep), clap derive.

**Spec:** `docs/superpowers/specs/2026-05-24-chain-name-resolver-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `Cargo.toml` | Modify | Promote `tempfile` to a `cli`-gated runtime dep. |
| `src/bin/chainfold/chains.rs` | Create | All new code: `ChainlistEntry`, `NativeCurrency`, `ChainResolver`, `ChainResolveError`, plus unit tests. |
| `src/bin/chainfold/main.rs` | Modify | Declare `mod chains;`. |
| `src/bin/chainfold/cli.rs` | Modify | Replace `chain_id: u64` (default 1) on `EvmArgs` with `chain: Option<String>` + `chain_id: Option<u64>`, mutually exclusive. |
| `src/bin/chainfold/evm/mod.rs` | Modify | Resolve `args.chain` to a numeric chain ID before building the provider; rename the local `chain_id` to avoid shadowing. |
| `src/bin/chainfold/config.rs` | Modify | Add `56 => Ok("bnb-mainnet")` to `alchemy_chain_slug` and add a unit test. |

Existing project conventions to follow:
- Clippy is in pedantic mode with `absolute_paths = warn`. Use `use` statements; avoid `crate::a::b::c::Thing` chains.
- The `cli` feature gates all binary-local code. Tests for `src/bin/chainfold/*.rs` modules run under `cargo test --features cli`.
- The user installs pre-commit hooks via `prek`. Each task ends with explicit `cargo fmt --all`, `cargo clippy --features cli -- -D warnings`, `cargo test --features cli` runs to mirror the hooks before committing.
- Commits use Conventional Commits style (`feat:`, `chore:`, `test:`, etc.) as seen in `git log --oneline`.

---

## Task 1: Add `tempfile` as a cli-gated runtime dependency

**Files:**
- Modify: `Cargo.toml`

`tempfile` is already in `[dev-dependencies]`. Promoting it to a `cli`-gated runtime dep lets the binary use `NamedTempFile::persist` for atomic cache writes. Cargo deduplicates by version, so leaving the existing dev-dep line untouched is fine — non-cli tests that need tempfile keep working.

- [ ] **Step 1: Add the optional runtime dep**

In the `[dependencies]` section of `Cargo.toml`, immediately after the `dirs = { version = "6", optional = true }` line, insert:

```toml
tempfile = { version = "3", optional = true }
```

- [ ] **Step 2: Add it to the `cli` feature list**

In the `[features]` section, modify the `cli` feature to include `"dep:tempfile"`:

```toml
cli = [
  "evm",
  "dep:clap",
  "dep:tracing-subscriber",
  "dep:chrono",
  "dep:toml",
  "dep:dirs",
  "dep:tempfile",
]
```

- [ ] **Step 3: Verify the build still succeeds**

Run: `cargo build --features cli`
Expected: builds cleanly, no warnings about unused deps.

- [ ] **Step 4: Run formatter on Cargo.toml**

Run: `taplo fmt Cargo.toml`
Expected: file may be reformatted; no errors. (This mirrors the `style: taplo fmt Cargo.toml` commit in `git log`.)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml
git commit -m "chore: promote tempfile to cli-gated runtime dep"
```

---

## Task 2: Create `chains.rs` with `ChainlistEntry` parsing

**Files:**
- Create: `src/bin/chainfold/chains.rs`

We use strict TDD: write a deserialization test first, watch it fail, then implement the types.

- [ ] **Step 1: Create the file with a placeholder + first failing test**

Create `src/bin/chainfold/chains.rs` with exactly this content:

```rust
//! Chain name resolution via cached chainlist.org/rpcs.json.
//!
//! Maps user-supplied names (e.g. `arbitrum`, `bnb`) to chain IDs by
//! matching against `shortName` or `chainSlug` from chainlist.org.
//! Cache lives at `~/.cache/chainfold/chainlist.json`; refresh = manual `rm`.

#[cfg(test)]
mod tests {
    use super::*;

    const ETH_ENTRY: &str = r#"{
        "name": "Ethereum Mainnet",
        "chain": "ETH",
        "chainId": 1,
        "shortName": "eth",
        "chainSlug": "ethereum",
        "networkId": 1,
        "infoURL": "https://ethereum.org",
        "isTestnet": false,
        "redFlags": [],
        "nativeCurrency": { "name": "Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    #[test]
    fn parses_ethereum_entry() {
        let entry: ChainlistEntry = serde_json::from_str(ETH_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 1);
        assert_eq!(entry.name, "Ethereum Mainnet");
        assert_eq!(entry.short_name, "eth");
        assert_eq!(entry.chain_slug.as_deref(), Some("ethereum"));
        assert_eq!(entry.chain, "ETH");
        assert_eq!(entry.network_id, Some(1));
        assert_eq!(entry.info_url.as_deref(), Some("https://ethereum.org"));
        assert_eq!(entry.is_testnet, Some(false));
        assert_eq!(entry.red_flags.as_deref(), Some(&[] as &[String]));
        assert_eq!(entry.native_currency.name, "Ether");
        assert_eq!(entry.native_currency.symbol, "ETH");
        assert_eq!(entry.native_currency.decimals, 18);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

First, register the module so the binary picks it up. In `src/bin/chainfold/main.rs`, add `mod chains;` at the top of the module list (after `mod cli;` and before `mod config;`):

```rust
mod chains;
mod cli;
mod config;
mod evm;
mod fmt;
mod signature_cache;
mod util;
```

Then run: `cargo test --features cli --bin chainfold -- chains::tests::parses_ethereum_entry`
Expected: FAIL with "cannot find type `ChainlistEntry` in this scope".

- [ ] **Step 3: Implement the types**

Replace the contents of `src/bin/chainfold/chains.rs` with:

```rust
//! Chain name resolution via cached chainlist.org/rpcs.json.
//!
//! Maps user-supplied names (e.g. `arbitrum`, `bnb`) to chain IDs by
//! matching against `shortName` or `chainSlug` from chainlist.org.
//! Cache lives at `~/.cache/chainfold/chainlist.json`; refresh = manual `rm`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainlistEntry {
    pub chain_id: u64,
    pub name: String,
    pub short_name: String,
    pub chain: String,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct NativeCurrency {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETH_ENTRY: &str = r#"{
        "name": "Ethereum Mainnet",
        "chain": "ETH",
        "chainId": 1,
        "shortName": "eth",
        "chainSlug": "ethereum",
        "networkId": 1,
        "infoURL": "https://ethereum.org",
        "isTestnet": false,
        "redFlags": [],
        "nativeCurrency": { "name": "Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    #[test]
    fn parses_ethereum_entry() {
        let entry: ChainlistEntry = serde_json::from_str(ETH_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 1);
        assert_eq!(entry.name, "Ethereum Mainnet");
        assert_eq!(entry.short_name, "eth");
        assert_eq!(entry.chain_slug.as_deref(), Some("ethereum"));
        assert_eq!(entry.chain, "ETH");
        assert_eq!(entry.network_id, Some(1));
        assert_eq!(entry.info_url.as_deref(), Some("https://ethereum.org"));
        assert_eq!(entry.is_testnet, Some(false));
        assert_eq!(entry.red_flags.as_deref(), Some(&[] as &[String]));
        assert_eq!(entry.native_currency.name, "Ether");
        assert_eq!(entry.native_currency.symbol, "ETH");
        assert_eq!(entry.native_currency.decimals, 18);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::parses_ethereum_entry`
Expected: PASS.

- [ ] **Step 5: Add the optional-fields-missing test**

Append inside the `tests` module (before the closing `}`):

```rust
    const MINIMAL_ENTRY: &str = r#"{
        "name": "OP Mainnet",
        "chain": "ETH",
        "chainId": 10,
        "shortName": "oeth",
        "nativeCurrency": { "name": "Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    #[test]
    fn parses_entry_without_optional_fields() {
        let entry: ChainlistEntry = serde_json::from_str(MINIMAL_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 10);
        assert!(entry.chain_slug.is_none());
        assert!(entry.network_id.is_none());
        assert!(entry.info_url.is_none());
        assert!(entry.is_testnet.is_none());
        assert!(entry.red_flags.is_none());
    }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::parses_entry_without_optional_fields`
Expected: PASS (no implementation change needed — `#[serde(default)]` handles missing fields).

- [ ] **Step 7: Add the unknown-fields-ignored test**

Append inside the `tests` module:

```rust
    const ENRICHED_ENTRY: &str = r#"{
        "name": "BNB Smart Chain Mainnet",
        "chain": "BSC",
        "chainId": 56,
        "shortName": "bnb",
        "chainSlug": "binance",
        "tvl": 7444698897.0,
        "icon": "bnb",
        "ens": null,
        "status": null,
        "faucets": [],
        "rpc": [{"url": "https://bsc-rpc.example", "tracking": "none"}],
        "explorers": [{"name": "bscscan", "url": "https://bscscan.com"}],
        "nativeCurrency": { "name": "BNB", "symbol": "BNB", "decimals": 18 }
    }"#;

    #[test]
    fn parses_unknown_fields_silently() {
        let entry: ChainlistEntry = serde_json::from_str(ENRICHED_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 56);
        assert_eq!(entry.short_name, "bnb");
        assert_eq!(entry.chain_slug.as_deref(), Some("binance"));
    }
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests`
Expected: 3 tests PASS (`parses_ethereum_entry`, `parses_entry_without_optional_fields`, `parses_unknown_fields_silently`).

- [ ] **Step 9: Pre-commit lint + format pass**

Run in sequence:
1. `cargo fmt --all` — expected: no output, exits 0.
2. `cargo clippy --features cli --all-targets -- -D warnings` — expected: no warnings, exits 0.
3. `cargo test --features cli` — expected: all tests pass.

- [ ] **Step 10: Commit**

```bash
git add Cargo.toml src/bin/chainfold/chains.rs src/bin/chainfold/main.rs
git commit -m "feat(cli): chainlist.org entry types for chain name resolution"
```

(`Cargo.toml` is included because Task 1 left a build artifact-able state — but if it was already committed in Task 1, drop it from `git add`. Check `git status` first.)

---

## Task 3: `ChainResolver` with lookup tests

**Files:**
- Modify: `src/bin/chainfold/chains.rs`

- [ ] **Step 1: Write the failing test for `shortName` lookup**

Append inside the `tests` module:

```rust
    fn fixture_entries() -> Vec<ChainlistEntry> {
        serde_json::from_str(&format!("[{ETH_ENTRY},{MINIMAL_ENTRY},{ENRICHED_ENTRY}]"))
            .expect("fixture parse")
    }

    #[test]
    fn resolver_matches_short_name() {
        let r = ChainResolver::from_entries(&fixture_entries());
        assert_eq!(r.resolve("eth").expect("eth resolves"), 1);
        assert_eq!(r.resolve("bnb").expect("bnb resolves"), 56);
        assert_eq!(r.resolve("oeth").expect("oeth resolves"), 10);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features cli --bin chainfold -- chains::tests::resolver_matches_short_name`
Expected: FAIL — `cannot find type 'ChainResolver' in this scope`.

- [ ] **Step 3: Implement `ChainResolver` + `ChainResolveError` + `resolve` + `from_entries`**

Above the `#[cfg(test)]` block in `chains.rs`, add:

```rust
use std::collections::HashMap;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChainResolveError {
    #[error(
        "unknown chain name '{name}': no shortName or chainSlug matches in chainlist.org. \
         Try --chain-id <number>, or check https://chainlist.org for the canonical name."
    )]
    UnknownName { name: String },
    #[error("fetching chainlist.org: {0}")]
    Fetch(#[from] reqwest::Error),
    #[error("cache I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing cache at {path} (delete the file to refresh): {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

pub struct ChainResolver {
    by_name: HashMap<String, u64>,
}

impl ChainResolver {
    /// Build a resolver from an already-deserialized list of entries.
    /// Visible for tests and for the disk-load constructor.
    pub fn from_entries(entries: &[ChainlistEntry]) -> Self {
        let mut by_name = HashMap::with_capacity(entries.len() * 2);
        for e in entries {
            by_name.insert(e.short_name.to_ascii_lowercase(), e.chain_id);
            if let Some(slug) = &e.chain_slug {
                by_name.insert(slug.to_ascii_lowercase(), e.chain_id);
            }
        }
        Self { by_name }
    }

    pub fn resolve(&self, name: &str) -> Result<u64, ChainResolveError> {
        self.by_name
            .get(&name.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| ChainResolveError::UnknownName {
                name: name.to_owned(),
            })
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::resolver_matches_short_name`
Expected: PASS.

- [ ] **Step 5: Add `chainSlug` test**

Append inside the `tests` module:

```rust
    #[test]
    fn resolver_matches_chain_slug() {
        let r = ChainResolver::from_entries(&fixture_entries());
        assert_eq!(r.resolve("ethereum").expect("slug resolves"), 1);
        assert_eq!(r.resolve("binance").expect("slug resolves"), 56);
    }
```

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::resolver_matches_chain_slug`
Expected: PASS (no impl change needed).

- [ ] **Step 7: Add case-insensitivity test**

Append inside the `tests` module:

```rust
    #[test]
    fn resolver_match_is_case_insensitive() {
        let r = ChainResolver::from_entries(&fixture_entries());
        assert_eq!(r.resolve("Ethereum").expect("mixed case"), 1);
        assert_eq!(r.resolve("BNB").expect("upper"), 56);
        assert_eq!(r.resolve("Binance").expect("slug mixed"), 56);
    }
```

- [ ] **Step 8: Run to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::resolver_match_is_case_insensitive`
Expected: PASS (impl already lowercases).

- [ ] **Step 9: Add unknown-name test (covers `--chain bsc` failing as designed)**

Append inside the `tests` module:

```rust
    #[test]
    fn resolver_unknown_name_errors() {
        let r = ChainResolver::from_entries(&fixture_entries());
        // "bsc" isn't a shortName (it's "bnb") or a chainSlug (it's "binance"),
        // so it must error per the design (no alias overlay).
        let err = r.resolve("bsc").expect_err("bsc should not resolve");
        match err {
            ChainResolveError::UnknownName { name } => assert_eq!(name, "bsc"),
            other => panic!("expected UnknownName, got {other:?}"),
        }
    }
```

- [ ] **Step 10: Run to verify it passes**

Run: `cargo test --features cli --bin chainfold -- chains::tests::resolver_unknown_name_errors`
Expected: PASS.

- [ ] **Step 11: Pre-commit checks**

1. `cargo fmt --all` — expected: clean.
2. `cargo clippy --features cli --all-targets -- -D warnings` — expected: clean.
3. `cargo test --features cli --bin chainfold -- chains::` — expected: 7 tests pass (3 from Task 2 + 4 here).

- [ ] **Step 12: Commit**

```bash
git add src/bin/chainfold/chains.rs
git commit -m "feat(cli): ChainResolver shortName/chainSlug lookup"
```

---

## Task 4: Disk roundtrip via `load_from_path`

**Files:**
- Modify: `src/bin/chainfold/chains.rs`

- [ ] **Step 1: Write the failing roundtrip test**

Append inside the `tests` module:

```rust
    #[test]
    fn load_from_disk_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("chainlist.json");
        let json = format!("[{ETH_ENTRY},{MINIMAL_ENTRY},{ENRICHED_ENTRY}]");
        std::fs::write(&path, json).expect("write fixture");

        let r = ChainResolver::load_from_path(&path).expect("load");
        assert_eq!(r.resolve("ethereum").expect("eth"), 1);
        assert_eq!(r.resolve("binance").expect("bnb"), 56);
        assert_eq!(r.resolve("oeth").expect("op"), 10);
    }

    #[test]
    fn load_from_corrupt_file_reports_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("chainlist.json");
        std::fs::write(&path, "{not json").expect("write fixture");

        let err = ChainResolver::load_from_path(&path).expect_err("should fail");
        match err {
            ChainResolveError::Parse { path: p, .. } => assert_eq!(p, path),
            other => panic!("expected Parse, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features cli --bin chainfold -- chains::tests::load_from_disk_roundtrip`
Expected: FAIL — `no function or associated item named 'load_from_path'`.

- [ ] **Step 3: Implement `load_from_path`**

Inside the `impl ChainResolver` block in `chains.rs` (after `resolve`), add:

```rust
    /// Load the resolver from a JSON file on disk. The path is captured into
    /// `Io`/`Parse` errors so the message tells the user what to delete.
    pub fn load_from_path(path: &std::path::Path) -> Result<Self, ChainResolveError> {
        let bytes = std::fs::read(path).map_err(|source| ChainResolveError::Io {
            path: path.to_owned(),
            source,
        })?;
        let entries: Vec<ChainlistEntry> =
            serde_json::from_slice(&bytes).map_err(|source| ChainResolveError::Parse {
                path: path.to_owned(),
                source,
            })?;
        Ok(Self::from_entries(&entries))
    }
```

- [ ] **Step 4: Run to verify both tests pass**

Run: `cargo test --features cli --bin chainfold -- chains::tests::load_from`
Expected: 2 tests pass (`load_from_disk_roundtrip`, `load_from_corrupt_file_reports_path`).

- [ ] **Step 5: Pre-commit checks**

1. `cargo fmt --all`
2. `cargo clippy --features cli --all-targets -- -D warnings`
3. `cargo test --features cli --bin chainfold -- chains::` — expected: 9 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/bin/chainfold/chains.rs
git commit -m "feat(cli): ChainResolver::load_from_path with path-aware errors"
```

---

## Task 5: `load_or_fetch` — production constructor with cache file

**Files:**
- Modify: `src/bin/chainfold/chains.rs`

This is the production entry point. The network-fetch branch is **not** unit-tested (no live HTTP in tests, no mock server scaffolding in v1); the cache-hit branch is exercised end-to-end by the smoke test in Task 10.

- [ ] **Step 1: Add the URL constant and default cache-path helper**

At the top of `chains.rs`, immediately after the existing `use` statements, add:

```rust
const CHAINLIST_URL: &str = "https://chainlist.org/rpcs.json";
const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn default_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("chainfold")
        .join("chainlist.json")
}
```

- [ ] **Step 2: Add `load_or_fetch`**

Inside the `impl ChainResolver` block, after `load_from_path`, add:

```rust
    /// Load the resolver from the default cache path
    /// (`~/.cache/chainfold/chainlist.json`). If the file does not exist,
    /// fetch from chainlist.org, write atomically, and load.
    ///
    /// Refresh = manual `rm` on the cache file.
    pub async fn load_or_fetch() -> Result<Self, ChainResolveError> {
        let path = default_cache_path();
        if !path.exists() {
            Self::fetch_and_persist(&path).await?;
        }
        Self::load_from_path(&path)
    }

    async fn fetch_and_persist(path: &std::path::Path) -> Result<(), ChainResolveError> {
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        std::fs::create_dir_all(parent).map_err(|source| ChainResolveError::Io {
            path: parent.to_owned(),
            source,
        })?;

        let client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .build()?;
        let bytes = client.get(CHAINLIST_URL).send().await?.error_for_status()?.bytes().await?;

        // Atomic write: temp file in the same directory, then persist.
        let mut tmp = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
            ChainResolveError::Io {
                path: parent.to_owned(),
                source,
            }
        })?;
        std::io::Write::write_all(&mut tmp, &bytes).map_err(|source| ChainResolveError::Io {
            path: tmp.path().to_owned(),
            source,
        })?;
        tmp.persist(path).map_err(|e| ChainResolveError::Io {
            path: path.to_owned(),
            source: e.error,
        })?;
        Ok(())
    }
```

- [ ] **Step 3: Verify the binary still builds**

Run: `cargo build --features cli`
Expected: builds cleanly. No new tests yet — fetch path is exercised in Task 10's smoke test.

- [ ] **Step 4: Pre-commit checks**

1. `cargo fmt --all`
2. `cargo clippy --features cli --all-targets -- -D warnings` — expected: clean. If clippy flags `needless_pass_by_value` on `&std::path::Path` parameters or similar, address by following its suggestion; do not silence with `#[allow]`.
3. `cargo test --features cli --bin chainfold -- chains::` — expected: 9 tests pass (no new tests in this task).

- [ ] **Step 5: Commit**

```bash
git add src/bin/chainfold/chains.rs
git commit -m "feat(cli): ChainResolver::load_or_fetch with atomic cache write"
```

---

## Task 6: Update `EvmArgs` CLI surface

**Files:**
- Modify: `src/bin/chainfold/cli.rs` (lines 41-50, the `EvmArgs` struct)

- [ ] **Step 1: Replace the `chain_id` field with two mutually exclusive Options**

In `src/bin/chainfold/cli.rs`, find the `EvmArgs` struct (starts around line 41). Replace the existing `chain_id` field:

```rust
    /// Chain ID (default: 1 for Ethereum mainnet)
    #[arg(long, default_value = "1")]
    pub chain_id: u64,
```

with:

```rust
    /// Chain name. Matched against chainlist.org's shortName or chainSlug.
    /// Example: --chain ethereum, --chain arbitrum, --chain bnb
    #[arg(long, required_unless_present = "chain_id", conflicts_with = "chain_id")]
    pub chain: Option<String>,

    /// Numeric chain ID. Use for chains not listed in chainlist.org.
    /// Example: --chain-id 56
    #[arg(long, required_unless_present = "chain", conflicts_with = "chain")]
    pub chain_id: Option<u64>,
```

- [ ] **Step 2: Verify clap accepts the new surface (build only)**

Run: `cargo build --features cli`
Expected: BUILD FAILS — `evm/mod.rs:20` and `evm/mod.rs:30-33` still reference the old `args.chain_id: u64`. That's fixed in Task 7.

- [ ] **Step 3: Do NOT commit yet**

Task 7 is the matching wiring change. Build won't pass standalone. Skip the commit step here; the two tasks land as one commit.

---

## Task 7: Wire the resolver into the EVM entry point

**Files:**
- Modify: `src/bin/chainfold/evm/mod.rs`

- [ ] **Step 1: Update imports and the chain-id resolution**

In `src/bin/chainfold/evm/mod.rs`, add `use crate::chains::ChainResolver;` to the `use` block (alphabetical order keeps it after `cli::` block — but our existing imports are grouped; just add it after the `use crate::cli::...` line). Then replace lines 20-35 (the existing `rpc_url`, `url`, `client`, `provider`, `db_path`, `cache`, and chain-id cross-check block).

Original lines 20-35:

```rust
    let rpc_url = config.rpc_url(args.rpc_url.as_deref(), args.chain_id)?;

    let url: Url = rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    let provider = ProviderBuilder::new().connect_client(client);

    let db_path = versioned_path(&args.db);
    let cache = Arc::new(LogStore::open(&db_path)?);

    let chain_id = provider.get_chain_id().await?;
    if chain_id != args.chain_id {
        eprintln!(
            "warning: --chain-id {} but RPC reports chain {}",
            args.chain_id, chain_id
        );
    }
```

Replace with:

```rust
    let chain_id = match (args.chain_id, args.chain.as_deref()) {
        (Some(id), _) => id,
        (None, Some(name)) => ChainResolver::load_or_fetch().await?.resolve(name)?,
        (None, None) => unreachable!("clap enforces required_unless_present"),
    };

    let rpc_url = config.rpc_url(args.rpc_url.as_deref(), chain_id)?;

    let url: Url = rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    let provider = ProviderBuilder::new().connect_client(client);

    let db_path = versioned_path(&args.db);
    let cache = Arc::new(LogStore::open(&db_path)?);

    let rpc_chain_id = provider.get_chain_id().await?;
    if rpc_chain_id != chain_id {
        eprintln!(
            "warning: requested chain {chain_id} but RPC reports chain {rpc_chain_id}"
        );
    }
```

Note the rename of the second `chain_id` binding to `rpc_chain_id` — the resolved chain_id from CLI is now the outer scope's `chain_id`, and shadowing it with the RPC-reported one would lose the warning's information.

- [ ] **Step 2: Verify the binary now builds**

Run: `cargo build --features cli`
Expected: builds cleanly.

- [ ] **Step 3: Pre-commit checks**

1. `cargo fmt --all`
2. `cargo clippy --features cli --all-targets -- -D warnings`
3. `cargo test --features cli` — expected: all tests pass (no behavior tests on the wiring beyond what Task 10's smoke covers, but the resolver tests from Tasks 2–4 still pass).

- [ ] **Step 4: Commit (bundles Task 6 and Task 7)**

```bash
git add src/bin/chainfold/cli.rs src/bin/chainfold/evm/mod.rs
git commit -m "feat(cli): --chain <name> with chainlist.org resolution"
```

---

## Task 8: Add BSC to the Alchemy slug table

**Files:**
- Modify: `src/bin/chainfold/config.rs` (the `alchemy_chain_slug` function near line 174)

- [ ] **Step 1: Write the failing test**

At the bottom of `src/bin/chainfold/config.rs`, add (or extend the existing `mod tests` block if one already exists — check first; if none, add fresh):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alchemy_slug_bnb_mainnet() {
        assert_eq!(alchemy_chain_slug(56).expect("bsc supported"), "bnb-mainnet");
    }
}
```

If a `mod tests` already exists in the file, add the single `#[test] fn alchemy_slug_bnb_mainnet` inside it (do not duplicate `mod tests`). Check with: `grep -n "mod tests" src/bin/chainfold/config.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features cli --bin chainfold -- config::tests::alchemy_slug_bnb_mainnet`
Expected: FAIL — `alchemy_chain_slug(56)` returns `Err(UnsupportedChain(56))`, the assertion fails.

- [ ] **Step 3: Add the slug**

In `alchemy_chain_slug` (near line 174), add the BSC arm. Find:

```rust
fn alchemy_chain_slug(chain_id: u64) -> Result<&'static str, ConfigError> {
    match chain_id {
        1 => Ok("eth-mainnet"),
        11_155_111 => Ok("eth-sepolia"),
        137 => Ok("polygon-mainnet"),
        42161 => Ok("arb-mainnet"),
        10 => Ok("opt-mainnet"),
        8453 => Ok("base-mainnet"),
        43114 => Ok("avax-mainnet"),
        _ => Err(ConfigError::UnsupportedChain(chain_id)),
    }
}
```

Insert `56 => Ok("bnb-mainnet"),` between the `137` and `42161` arms (keeping numeric-ascending order):

```rust
fn alchemy_chain_slug(chain_id: u64) -> Result<&'static str, ConfigError> {
    match chain_id {
        1 => Ok("eth-mainnet"),
        10 => Ok("opt-mainnet"),
        56 => Ok("bnb-mainnet"),
        137 => Ok("polygon-mainnet"),
        8453 => Ok("base-mainnet"),
        11_155_111 => Ok("eth-sepolia"),
        42161 => Ok("arb-mainnet"),
        43114 => Ok("avax-mainnet"),
        _ => Err(ConfigError::UnsupportedChain(chain_id)),
    }
}
```

(While here, sort the existing arms in ascending chain-id order — this is a one-line change in spirit but matches the user's preference for clean code; the diff stays small.)

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --features cli --bin chainfold -- config::tests::alchemy_slug_bnb_mainnet`
Expected: PASS.

- [ ] **Step 5: Pre-commit checks**

1. `cargo fmt --all`
2. `cargo clippy --features cli --all-targets -- -D warnings`
3. `cargo test --features cli` — expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/bin/chainfold/config.rs
git commit -m "feat(cli): support BNB Smart Chain (56) on Alchemy"
```

---

## Task 9: End-to-end smoke verification

**Files:** none — this is a manual verification task that the binary parses and behaves correctly.

- [ ] **Step 1: Verify clap requires one of --chain or --chain-id**

Run: `cargo run --features cli --bin chainfold -- evm events --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 --event Transfer --from-block 22000000 --num-blocks 1`
Expected: clap error similar to `error: the following required arguments were not provided: <--chain <CHAIN>|--chain-id <CHAIN_ID>>` and exit code 2.

- [ ] **Step 2: Verify --chain-id numeric still works (no cache touched)**

Pre-condition: `rm -f ~/.cache/chainfold/chainlist.json` to ensure the cache is absent.

Run: `cargo run --features cli --bin chainfold -- evm --chain-id 1 events --help`
Expected: prints help for the `events` subcommand and exits 0.
Post-check: `ls ~/.cache/chainfold/chainlist.json 2>/dev/null` — file should still NOT exist. (Proves the chain-id path doesn't fetch.)

- [ ] **Step 3: Verify --chain triggers a fetch on first run, persists the cache**

Pre-condition: cache still absent from Step 2.

Run: `cargo run --features cli --bin chainfold -- evm --chain ethereum events --help`
Expected: prints help for `events` and exits 0.
Post-check:
- `ls -l ~/.cache/chainfold/chainlist.json` — file exists, size ~2 MB.
- `head -c 50 ~/.cache/chainfold/chainlist.json` — starts with `[{` (valid JSON array).

- [ ] **Step 4: Verify --chain on warm cache does NOT refetch**

Run: `time cargo run --features cli --bin chainfold -- evm --chain arbitrum events --help`
Expected: prints help, exits 0, real time ≪ Step 3's runtime (no network call).

- [ ] **Step 5: Verify the negative-case error message for bsc**

Run: `cargo run --features cli --bin chainfold -- evm --chain bsc events --help`
Expected: exit code != 0; stderr contains `unknown chain name 'bsc'` and the hint `Try --chain-id <number>`.

- [ ] **Step 6: Verify the rename of chain-id mismatch warning**

Run: `cargo run --features cli --bin chainfold -- evm --chain-id 1 --rpc-url https://bsc-rpc.publicnode.com events --address 0xdAC17F958D2ee523a2206206994597C13D831ec7 --event Transfer --from-block 1 --num-blocks 1`
Expected: stderr contains `warning: requested chain 1 but RPC reports chain 56`. (The actual events query may or may not return data — what matters is the warning fires with the new wording.)

- [ ] **Step 7: Commit (none — verification only)**

No file changes. If everything above passed, the feature is complete.

---

## Self-Review

Skimmed against `docs/superpowers/specs/2026-05-24-chain-name-resolver-design.md`:

| Spec section | Where implemented |
|---|---|
| Goal: `--chain <name>` resolution | Tasks 6, 7 |
| CLI surface (`--chain` + `--chain-id` mutually exclusive, no default) | Task 6 |
| `ChainlistEntry` + `NativeCurrency` data model | Task 2 |
| `is_testnet`, `red_flags` modeled now | Task 2 (in struct definition) |
| `ChainResolver` + `resolve` (shortName + chainSlug, case-insensitive) | Task 3 |
| `load_from_path` test-only entry + error-path-aware errors | Task 4 |
| `load_or_fetch` + atomic write + cache path | Task 5 |
| Source: chainlist.org (chosen over chainid.network) | Task 5 (URL constant) |
| Wiring into `evm/mod.rs`, runtime cross-check preserved | Task 7 |
| BSC `56 => "bnb-mainnet"` Alchemy slug | Task 8 |
| Error UX (UnknownName / Parse / Io / Fetch with helpful messages) | Task 3, 4, 5 |
| `--chain bsc` fails (no alias overlay) | Task 3 step 9 explicitly tests this |
| Dependencies: `dirs` (already cli-gated), `tempfile` (newly cli-gated) | Task 1 |
| Test list from spec | Tasks 2, 3, 4, 8 — 1:1 match with spec's testing section |
| Library crate untouched | No `src/lib.rs` or `src/{arch,merge,matcher,combinators,evm}` files modified |

**Placeholder scan:** No "TBD", "TODO", "fill in", or "similar to" patterns. All code blocks are concrete.

**Type consistency:** `ChainResolver::from_entries(&[ChainlistEntry])` is called identically in Tasks 3, 4, and 5. `ChainResolveError::UnknownName { name }` shape is consistent across the definition site (Task 3) and the assertion in Task 3 step 9. `load_from_path(&std::path::Path)` signature is consistent across definition (Task 4) and call site (Task 5).

**Scope:** Single binary, single new module, ~50 LOC implementation + ~80 LOC tests. Fits comfortably in one PR. No decomposition needed.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-24-chain-name-resolver.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — execute in this session using executing-plans, batch with checkpoints

Which approach?
