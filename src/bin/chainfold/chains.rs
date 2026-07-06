//! Chain name resolution via cached chainlist.org/rpcs.json.
//!
//! Maps user-supplied names (e.g. `arbitrum`, `bnb`) to chain IDs by
//! matching against `shortName` or `chainSlug` from chainlist.org.
//! Cache lives at `~/.cache/chainfold/chainlist.json`; refresh = manual `rm`.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy::transports::http::reqwest::Url;
use chainfold::evm::ChainId;
use serde::Deserialize;
use tempfile::NamedTempFile;
use thiserror::Error;

const CHAINLIST_URL: &str = "https://chainlist.org/rpcs.json";
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Map a chain ID to Alchemy's subdomain slug, e.g. `1 → "eth-mainnet"`.
/// `None` means Alchemy doesn't host that chain. Matches on the raw id at the
/// literal table — the one place `ChainId` unwraps to `u64`.
pub fn alchemy_chain_slug(chain_id: ChainId) -> Option<&'static str> {
    match chain_id.get() {
        1 => Some("eth-mainnet"),
        10 => Some("opt-mainnet"),
        56 => Some("bnb-mainnet"),
        137 => Some("polygon-mainnet"),
        8453 => Some("base-mainnet"),
        11_155_111 => Some("eth-sepolia"),
        42161 => Some("arb-mainnet"),
        43114 => Some("avax-mainnet"),
        _ => None,
    }
}

/// Build the Alchemy RPC URL for `chain_id`. `None` if Alchemy doesn't
/// support the chain, or the formatted URL doesn't parse (e.g. an `api_key`
/// with stray characters) — either way there's no usable Alchemy endpoint.
pub fn alchemy_url(chain_id: ChainId, api_key: &str) -> Option<Url> {
    let slug = alchemy_chain_slug(chain_id)?;
    format!("https://{slug}.g.alchemy.com/v2/{api_key}")
        .parse()
        .ok()
}

fn default_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("chainfold")
        .join("chainlist.json")
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(
    dead_code,
    reason = "ChainlistEntry mirrors the chainlist.org schema; the remaining unread fields \
              (`chain`, `network_id`, `info_url`, `native_currency`) are deserialized for \
              fidelity and exercised only by tests. bin-target dead_code analysis ignores \
              test usage, so #[allow] (not #[expect]) keeps the test target's lint quiet too."
)]
pub struct ChainlistEntry {
    pub chain_id: ChainId,
    pub name: String,
    pub short_name: String,
    pub chain: String,
    #[serde(default)]
    pub chain_slug: Option<String>,
    #[serde(default)]
    pub network_id: Option<u64>,
    #[serde(default, rename = "infoURL")]
    pub info_url: Option<String>,
    #[serde(default)]
    pub is_testnet: Option<bool>,
    #[serde(default)]
    pub red_flags: Option<Vec<String>>,
    #[serde(default)]
    pub rpc: Vec<RpcEntry>,
    pub native_currency: NativeCurrency,
}

impl ChainlistEntry {
    /// Whether chainlist.org flags this chain as a testnet. Absent → `false`.
    pub fn is_testnet(&self) -> bool {
        self.is_testnet == Some(true)
    }

    /// `DefiLlama` red flags (known scam/spam advisories); empty when none.
    pub fn red_flags(&self) -> &[String] {
        self.red_flags.as_deref().unwrap_or(&[])
    }
}

#[derive(Debug, Clone, Deserialize)]
#[allow(
    dead_code,
    reason = "fields deserialized for upstream-schema fidelity and tested; \
              bin-target dead_code analysis ignores test context. \
              #[allow] (not #[expect]) — tests read these, so test-target dead_code stays silent."
)]
pub struct NativeCurrency {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

/// Privacy/tracking policy a chainlist RPC entry advertises. Drives public-RPC
/// selection — a lower [`rank`](PrivacyTracking::rank) is more private and
/// therefore preferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrivacyTracking {
    /// No tracking at all — most preferred.
    None,
    /// Limited tracking.
    Limited,
    /// Tracks users — least preferred.
    Yes,
    /// Field absent or an unrecognized value; ranked between `limited` and `yes`.
    #[default]
    #[serde(other)]
    Unspecified,
}

impl PrivacyTracking {
    /// Selection rank — lower is more private, hence preferred. Absent/unknown
    /// (`Unspecified`) sits above an explicit `yes` but below any privacy
    /// commitment.
    fn rank(self) -> u8 {
        match self {
            PrivacyTracking::None => 0,
            PrivacyTracking::Limited => 1,
            PrivacyTracking::Unspecified => 2,
            PrivacyTracking::Yes => 3,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcEntry {
    pub url: String,
    #[serde(default)]
    pub tracking: PrivacyTracking,
    // chainlist.org sometimes includes `isOpenSource`, `trackingDetails`,
    // etc. Not modeled here; serde drops unknown fields silently.
}

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

/// Filters for [`ChainResolver::list`] (the `chains list` subcommand). The
/// default lists every mainnet: testnets are excluded until asked for, and
/// `alchemy_only` is off.
#[derive(Debug, Clone, Copy, Default)]
pub struct ListFilter {
    /// Also keep chains flagged `isTestnet: true` (excluded by default).
    pub include_testnets: bool,
    /// Keep only chains Alchemy hosts (an [`alchemy_chain_slug`] exists).
    pub alchemy_only: bool,
}

/// A user-typeable handle for a chain — a chainlist `shortName` *or*
/// `chainSlug` (both feed the same lookup space, so `--chain eth` and
/// `--chain ethereum` resolve alike). Construction lowercases, so a handle is
/// always normalized and case-insensitive matching can't be bypassed by a
/// stray mixed-case key. Not `ChainName` — that's the full display `name`
/// field; not `ChainSlug` — slug is only one of the two sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChainHandle(String);

impl ChainHandle {
    /// Normalize a raw `shortName` / `chainSlug` / user `--chain` value into a
    /// lookup handle. Lowercasing here is the single source of normalization.
    pub fn new(raw: &str) -> Self {
        Self(raw.to_ascii_lowercase())
    }

    /// The normalized (lowercased) handle string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A chain selected by id or by name, *before* name→id resolution. Parsed from
/// a free-form string (a `CHAINFOLD_CHAIN` env value or a `[evm] default_chain`
/// config value): an all-digits value is a [`ChainId`] (so chains absent from
/// chainlist still work via env/config), anything else a [`ChainHandle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainSelector {
    Id(ChainId),
    Name(ChainHandle),
}

impl ChainSelector {
    /// Parse a selector string. Empty input is treated as unset (`None`), so a
    /// blank env var or config value falls through to the next source.
    pub fn parse(raw: &str) -> Option<Self> {
        if raw.is_empty() {
            return None;
        }
        Some(match raw.parse::<u64>() {
            Ok(n) => Self::Id(ChainId::new(n)),
            Err(_) => Self::Name(ChainHandle::new(raw)),
        })
    }
}

impl std::fmt::Display for ChainSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => std::fmt::Display::fmt(id, f),
            Self::Name(handle) => f.write_str(handle.as_str()),
        }
    }
}

/// Built-in default chain name, used when nothing else selects one.
pub const DEFAULT_CHAIN: &str = "ethereum";

/// Resolve the target chain with precedence:
/// `--chain-id` > `--chain` > `$CHAINFOLD_CHAIN` > `[evm] default_chain` >
/// built-in [`DEFAULT_CHAIN`]. `--chain` and `--chain-id` are mutually exclusive
/// at the clap layer, so their relative order here is moot.
pub fn select_chain(
    chain_id: Option<ChainId>,
    chain: Option<ChainHandle>,
    env_default: Option<ChainSelector>,
    config_default: Option<ChainSelector>,
) -> ChainSelector {
    if let Some(id) = chain_id {
        return ChainSelector::Id(id);
    }
    if let Some(handle) = chain {
        return ChainSelector::Name(handle);
    }
    env_default
        .or(config_default)
        .unwrap_or_else(|| ChainSelector::Name(ChainHandle::new(DEFAULT_CHAIN)))
}

#[derive(Debug)]
pub struct ChainResolver {
    entries: Vec<ChainlistEntry>,
    by_name: HashMap<ChainHandle, ChainId>,
}

impl ChainResolver {
    /// Build a resolver from an already-deserialized list of entries.
    ///
    /// `shortName` / `chainSlug` are **not** unique across chainlist (e.g.
    /// `"kub"` is the shortName of both chain 96 and chain 259251). On a key
    /// collision we keep the **lowest** chain id as the canonical target: it's
    /// deterministic regardless of chainlist ordering, and the lowest id is the
    /// original/most-canonical chain. Ambiguous names that need the other chain
    /// must use `--chain-id`.
    pub fn from_entries(entries: Vec<ChainlistEntry>) -> Self {
        let mut by_name: HashMap<ChainHandle, ChainId> = HashMap::with_capacity(entries.len() * 2);
        for e in &entries {
            let slug_key = e.chain_slug.as_deref().map(ChainHandle::new);
            let keys = std::iter::once(ChainHandle::new(&e.short_name)).chain(slug_key);
            for key in keys {
                by_name
                    .entry(key)
                    .and_modify(|id| *id = (*id).min(e.chain_id))
                    .or_insert(e.chain_id);
            }
        }
        Self { entries, by_name }
    }

    /// Resolve a [`ChainHandle`] (already case-normalized) to its chain id.
    pub fn resolve(&self, handle: &ChainHandle) -> Result<ChainId, ChainResolveError> {
        self.by_name
            .get(handle)
            .copied()
            .ok_or_else(|| ChainResolveError::UnknownName {
                name: handle.as_str().to_owned(),
            })
    }

    /// The cached chainlist entry for `chain_id`, if present.
    pub fn entry(&self, chain_id: ChainId) -> Option<&ChainlistEntry> {
        self.entries.iter().find(|e| e.chain_id == chain_id)
    }

    /// Entries matching `filter`, sorted by chain id — backs `chains list`.
    pub fn list(&self, filter: ListFilter) -> Vec<&ChainlistEntry> {
        let mut out: Vec<&ChainlistEntry> = self
            .entries
            .iter()
            .filter(|e| filter.include_testnets || !e.is_testnet())
            .filter(|e| !filter.alchemy_only || alchemy_chain_slug(e.chain_id).is_some())
            .collect();
        out.sort_by_key(|e| e.chain_id);
        out
    }

    /// Pick a usable public RPC URL for `chain_id` from the cached chainlist
    /// entries. Returns `None` if the chain isn't in chainlist or has no
    /// usable HTTPS endpoint.
    ///
    /// Selection algorithm:
    /// 1. HTTPS only (drop http://, wss://).
    /// 2. Drop URLs containing `${` (template placeholders needing API keys).
    /// 3. Rank by privacy:
    ///    tracking=none (0) > limited (1) > unspecified/None (2) > yes (3).
    /// 4. Stable within tier — first-listed in chainlist wins.
    ///
    /// Steps 1–2 are string-level checks on the raw chainlist value, run before
    /// parsing; the chosen entry is parsed into a [`Url`] at the end (a winner
    /// that somehow fails to parse yields `None`).
    pub fn public_rpc_for(&self, chain_id: ChainId) -> Option<Url> {
        self.entry(chain_id)?
            .rpc
            .iter()
            .filter(|r| r.url.starts_with("https://") && !r.url.contains("${"))
            .min_by_key(|r| r.tracking.rank())
            .and_then(|r| r.url.parse().ok())
    }

    /// Load the resolver from a JSON file on disk. The path is captured into
    /// `Io`/`Parse` errors so the message tells the user what to delete.
    pub fn load_from_path(path: &Path) -> Result<Self, ChainResolveError> {
        let bytes = fs::read(path).map_err(|source| ChainResolveError::Io {
            path: path.to_owned(),
            source,
        })?;
        let entries: Vec<ChainlistEntry> =
            serde_json::from_slice(&bytes).map_err(|source| ChainResolveError::Parse {
                path: path.to_owned(),
                source,
            })?;
        Ok(Self::from_entries(entries))
    }

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

    async fn fetch_and_persist(path: &Path) -> Result<(), ChainResolveError> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source| ChainResolveError::Io {
            path: parent.to_owned(),
            source,
        })?;

        let client = reqwest::Client::builder().timeout(FETCH_TIMEOUT).build()?;
        let bytes = client
            .get(CHAINLIST_URL)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        // Atomic write: temp file in the same directory, then persist.
        let mut tmp = NamedTempFile::new_in(parent).map_err(|source| ChainResolveError::Io {
            path: parent.to_owned(),
            source,
        })?;
        Write::write_all(&mut tmp, &bytes).map_err(|source| ChainResolveError::Io {
            path: tmp.path().to_owned(),
            source,
        })?;
        tmp.persist(path).map_err(|e| ChainResolveError::Io {
            path: path.to_owned(),
            source: e.error,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- alchemy slug / url --

    #[test]
    fn alchemy_chain_slug_returns_some_for_known_chains() {
        assert_eq!(alchemy_chain_slug(ChainId::new(1)), Some("eth-mainnet"));
        assert_eq!(alchemy_chain_slug(ChainId::new(56)), Some("bnb-mainnet"));
        assert_eq!(alchemy_chain_slug(ChainId::new(42161)), Some("arb-mainnet"));
    }

    #[test]
    fn alchemy_chain_slug_returns_none_for_unknown_chains() {
        assert!(alchemy_chain_slug(ChainId::new(99_999)).is_none());
    }

    #[test]
    fn alchemy_url_known_chain() {
        assert_eq!(
            alchemy_url(ChainId::new(1), "abc")
                .as_ref()
                .map(Url::as_str),
            Some("https://eth-mainnet.g.alchemy.com/v2/abc"),
        );
    }

    #[test]
    fn alchemy_url_unknown_chain_is_none() {
        assert!(alchemy_url(ChainId::new(99_999), "abc").is_none());
    }

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
        assert_eq!(entry.chain_id, ChainId::new(1));
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
        assert_eq!(entry.chain_id, ChainId::new(10));
        assert!(entry.chain_slug.is_none());
        assert!(entry.network_id.is_none());
        assert!(entry.info_url.is_none());
        assert!(entry.is_testnet.is_none());
        assert!(entry.red_flags.is_none());
        // rpc defaults to empty when missing from the JSON.
        assert!(entry.rpc.is_empty());
    }

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
        assert_eq!(entry.chain_id, ChainId::new(56));
        assert_eq!(entry.short_name, "bnb");
        assert_eq!(entry.chain_slug.as_deref(), Some("binance"));
        assert_eq!(entry.rpc.len(), 1);
        assert_eq!(entry.rpc[0].url, "https://bsc-rpc.example");
        assert_eq!(entry.rpc[0].tracking, PrivacyTracking::None);
    }

    fn fixture_entries() -> Vec<ChainlistEntry> {
        serde_json::from_str(&format!("[{ETH_ENTRY},{MINIMAL_ENTRY},{ENRICHED_ENTRY}]"))
            .expect("fixture parse")
    }

    /// Resolve a raw name through a [`ChainHandle`], the way production does.
    fn resolve(resolver: &ChainResolver, name: &str) -> Result<ChainId, ChainResolveError> {
        resolver.resolve(&ChainHandle::new(name))
    }

    fn sel_name(s: &str) -> ChainSelector {
        ChainSelector::Name(ChainHandle::new(s))
    }
    fn sel_id(n: u64) -> ChainSelector {
        ChainSelector::Id(ChainId::new(n))
    }

    #[test]
    fn selector_parses_numeric_as_id() {
        assert_eq!(ChainSelector::parse("56"), Some(sel_id(56)));
    }

    #[test]
    fn selector_parses_name_lowercased() {
        assert_eq!(ChainSelector::parse("Arbitrum"), Some(sel_name("arbitrum")));
    }

    #[test]
    fn selector_empty_is_none() {
        assert_eq!(ChainSelector::parse(""), None);
    }

    #[test]
    fn select_chain_precedence() {
        // explicit --chain-id wins over everything
        assert_eq!(
            select_chain(
                Some(ChainId::new(56)),
                None,
                Some(sel_name("base")),
                Some(sel_name("op")),
            ),
            sel_id(56)
        );
        // explicit --chain beats env and config
        assert_eq!(
            select_chain(
                None,
                Some(ChainHandle::new("arbitrum")),
                Some(sel_name("base")),
                Some(sel_name("op")),
            ),
            sel_name("arbitrum")
        );
        // env beats config
        assert_eq!(
            select_chain(None, None, Some(sel_name("base")), Some(sel_name("op"))),
            sel_name("base")
        );
        // config beats the built-in default
        assert_eq!(
            select_chain(None, None, None, Some(sel_name("op"))),
            sel_name("op")
        );
        // built-in fallback is ethereum
        assert_eq!(select_chain(None, None, None, None), sel_name("ethereum"));
    }

    #[test]
    fn resolver_matches_short_name() {
        let r = ChainResolver::from_entries(fixture_entries());
        assert_eq!(resolve(&r, "eth").expect("eth resolves"), ChainId::new(1));
        assert_eq!(resolve(&r, "bnb").expect("bnb resolves"), ChainId::new(56));
        assert_eq!(
            resolve(&r, "oeth").expect("oeth resolves"),
            ChainId::new(10)
        );
    }

    #[test]
    fn resolver_matches_chain_slug() {
        let r = ChainResolver::from_entries(fixture_entries());
        assert_eq!(
            resolve(&r, "ethereum").expect("slug resolves"),
            ChainId::new(1)
        );
        assert_eq!(
            resolve(&r, "binance").expect("slug resolves"),
            ChainId::new(56)
        );
    }

    #[test]
    fn resolver_match_is_case_insensitive() {
        let r = ChainResolver::from_entries(fixture_entries());
        assert_eq!(
            resolve(&r, "Ethereum").expect("mixed case"),
            ChainId::new(1)
        );
        assert_eq!(resolve(&r, "BNB").expect("upper"), ChainId::new(56));
        assert_eq!(
            resolve(&r, "Binance").expect("slug mixed"),
            ChainId::new(56)
        );
    }

    #[test]
    fn resolve_prefers_lowest_chain_id_on_collision_regardless_of_order() {
        // Two chains share shortName "kub" (chainlist really has this: 96 and
        // 259251). The lowest id must win whichever order they're listed in —
        // last-wins would instead depend on chainlist ordering.
        const KUB_LOW: &str = r#"{
            "name": "Bitkub Chain", "chain": "KUB", "chainId": 96,
            "shortName": "kub", "chainSlug": "bitkub",
            "nativeCurrency": { "name": "KUB", "symbol": "KUB", "decimals": 18 }
        }"#;
        const KUB_HIGH: &str = r#"{
            "name": "KUB Testnet", "chain": "KUB", "chainId": 259251,
            "shortName": "kub",
            "nativeCurrency": { "name": "tKUB", "symbol": "tKUB", "decimals": 18 }
        }"#;
        for listing in [
            format!("[{KUB_HIGH},{KUB_LOW}]"),
            format!("[{KUB_LOW},{KUB_HIGH}]"),
        ] {
            let entries: Vec<ChainlistEntry> =
                serde_json::from_str(&listing).expect("fixture parse");
            let r = ChainResolver::from_entries(entries);
            assert_eq!(
                resolve(&r, "kub").expect("kub resolves"),
                ChainId::new(96),
                "lowest chain id must win the collision ({listing})"
            );
        }
    }

    #[test]
    fn resolver_unknown_name_errors() {
        let r = ChainResolver::from_entries(fixture_entries());
        // "bsc" isn't a shortName (it's "bnb") or a chainSlug (it's "binance"),
        // so it must error per the design (no alias overlay).
        let err = resolve(&r, "bsc").expect_err("bsc should not resolve");
        match err {
            ChainResolveError::UnknownName { name } => assert_eq!(name, "bsc"),
            other => panic!("expected UnknownName, got {other:?}"),
        }
    }

    #[test]
    fn load_from_disk_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("chainlist.json");
        let json = format!("[{ETH_ENTRY},{MINIMAL_ENTRY},{ENRICHED_ENTRY}]");
        std::fs::write(&path, json).expect("write fixture");

        let r = ChainResolver::load_from_path(&path).expect("load");
        assert_eq!(resolve(&r, "ethereum").expect("eth"), ChainId::new(1));
        assert_eq!(resolve(&r, "binance").expect("bnb"), ChainId::new(56));
        assert_eq!(resolve(&r, "oeth").expect("op"), ChainId::new(10));
    }

    const RPC_HEAVY_ENTRY: &str = r#"{
        "name": "Test Chain",
        "chain": "TEST",
        "chainId": 99999,
        "shortName": "test",
        "rpc": [
            {"url": "https://a.example", "tracking": "none"},
            {"url": "https://b.example", "tracking": "limited"},
            {"url": "https://c.example"},
            {"url": "wss://d.example/ws", "tracking": "none"},
            {"url": "http://e.example", "tracking": "yes"}
        ],
        "nativeCurrency": { "name": "T", "symbol": "T", "decimals": 18 }
    }"#;

    #[test]
    fn parses_rpc_array_with_mixed_tracking() {
        let entry: ChainlistEntry = serde_json::from_str(RPC_HEAVY_ENTRY).expect("deserialize");
        assert_eq!(entry.rpc.len(), 5);
        assert_eq!(entry.rpc[0].url, "https://a.example");
        assert_eq!(entry.rpc[0].tracking, PrivacyTracking::None);
        assert_eq!(entry.rpc[2].url, "https://c.example");
        assert_eq!(
            entry.rpc[2].tracking,
            PrivacyTracking::Unspecified,
            "missing tracking deserializes to Unspecified"
        );
        assert_eq!(entry.rpc[3].url, "wss://d.example/ws");
        assert_eq!(entry.rpc[4].url, "http://e.example");
    }

    #[test]
    fn tracking_enum_maps_known_and_unknown_values() {
        let parse = |json: &str| -> PrivacyTracking {
            serde_json::from_str::<RpcEntry>(json)
                .expect("deserialize")
                .tracking
        };
        assert_eq!(
            parse(r#"{"url":"x","tracking":"none"}"#),
            PrivacyTracking::None
        );
        assert_eq!(
            parse(r#"{"url":"x","tracking":"limited"}"#),
            PrivacyTracking::Limited
        );
        assert_eq!(
            parse(r#"{"url":"x","tracking":"yes"}"#),
            PrivacyTracking::Yes
        );
        // Unrecognized string and absent field both fall back to Unspecified.
        assert_eq!(
            parse(r#"{"url":"x","tracking":"weird"}"#),
            PrivacyTracking::Unspecified
        );
        assert_eq!(parse(r#"{"url":"x"}"#), PrivacyTracking::Unspecified);
        // Rank order: none < limited < unspecified < yes.
        assert!(
            PrivacyTracking::None.rank() < PrivacyTracking::Limited.rank()
                && PrivacyTracking::Limited.rank() < PrivacyTracking::Unspecified.rank()
                && PrivacyTracking::Unspecified.rank() < PrivacyTracking::Yes.rank()
        );
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

    const PUBLIC_RPC_ENTRY: &str = r#"{
        "name": "Test Chain",
        "chain": "TEST",
        "chainId": 99999,
        "shortName": "test",
        "rpc": [
            {"url": "https://yes.example", "tracking": "yes"},
            {"url": "https://none.example", "tracking": "none"},
            {"url": "https://limited.example", "tracking": "limited"},
            {"url": "https://unmarked.example"}
        ],
        "nativeCurrency": { "name": "T", "symbol": "T", "decimals": 18 }
    }"#;

    fn entries_with_rpc() -> Vec<ChainlistEntry> {
        let entry: ChainlistEntry = serde_json::from_str(PUBLIC_RPC_ENTRY).expect("parse");
        vec![entry]
    }

    #[test]
    fn public_rpc_for_prefers_tracking_none() {
        let r = ChainResolver::from_entries(entries_with_rpc());
        assert_eq!(
            r.public_rpc_for(ChainId::new(99999))
                .as_ref()
                .map(Url::as_str),
            Some("https://none.example/"),
            "tracking=none must win over limited/yes/unspecified"
        );
    }

    #[test]
    fn public_rpc_for_skips_placeholder_urls() {
        let json = r#"{
            "name": "Placeholder Chain",
            "chain": "P",
            "chainId": 88888,
            "shortName": "p",
            "rpc": [
                {"url": "https://example.com/${API_KEY}", "tracking": "none"},
                {"url": "https://plain.example", "tracking": "limited"}
            ],
            "nativeCurrency": { "name": "P", "symbol": "P", "decimals": 18 }
        }"#;
        let entry: ChainlistEntry = serde_json::from_str(json).expect("parse");
        let r = ChainResolver::from_entries(vec![entry]);
        assert_eq!(
            r.public_rpc_for(ChainId::new(88888))
                .as_ref()
                .map(Url::as_str),
            Some("https://plain.example/"),
            "URL with ${{...}} placeholder must be skipped even if tracking=none"
        );
    }

    #[test]
    fn public_rpc_for_skips_non_https() {
        let json = r#"{
            "name": "Mixed Schemes",
            "chain": "M",
            "chainId": 77777,
            "shortName": "m",
            "rpc": [
                {"url": "http://insecure.example", "tracking": "none"},
                {"url": "wss://ws.example", "tracking": "none"},
                {"url": "https://ok.example", "tracking": "yes"}
            ],
            "nativeCurrency": { "name": "M", "symbol": "M", "decimals": 18 }
        }"#;
        let entry: ChainlistEntry = serde_json::from_str(json).expect("parse");
        let r = ChainResolver::from_entries(vec![entry]);
        assert_eq!(
            r.public_rpc_for(ChainId::new(77777))
                .as_ref()
                .map(Url::as_str),
            Some("https://ok.example/"),
            "http:// and wss:// must be skipped, leaving the https one even if tracking=yes"
        );
    }

    #[test]
    fn public_rpc_for_returns_none_when_no_usable_entry() {
        let json = r#"{
            "name": "Bad RPCs",
            "chain": "B",
            "chainId": 66666,
            "shortName": "b",
            "rpc": [
                {"url": "http://insecure.example"},
                {"url": "wss://ws.example"},
                {"url": "https://templated.example/${KEY}"}
            ],
            "nativeCurrency": { "name": "B", "symbol": "B", "decimals": 18 }
        }"#;
        let entry: ChainlistEntry = serde_json::from_str(json).expect("parse");
        let r = ChainResolver::from_entries(vec![entry]);
        assert!(r.public_rpc_for(ChainId::new(66666)).is_none());
    }

    #[test]
    fn public_rpc_for_unknown_chain_id_returns_none() {
        let r = ChainResolver::from_entries(entries_with_rpc());
        assert!(
            r.public_rpc_for(ChainId::new(12345)).is_none(),
            "no matching chain → None"
        );
    }

    #[test]
    fn public_rpc_for_preserves_order_within_tier() {
        // Two `tracking: "none"` entries; the FIRST listed should win
        // (stable sort within tier).
        let json = r#"{
            "name": "Tie",
            "chain": "T",
            "chainId": 55555,
            "shortName": "t",
            "rpc": [
                {"url": "https://first.example", "tracking": "none"},
                {"url": "https://second.example", "tracking": "none"}
            ],
            "nativeCurrency": { "name": "T", "symbol": "T", "decimals": 18 }
        }"#;
        let entry: ChainlistEntry = serde_json::from_str(json).expect("parse");
        let r = ChainResolver::from_entries(vec![entry]);
        assert_eq!(
            r.public_rpc_for(ChainId::new(55555))
                .as_ref()
                .map(Url::as_str),
            Some("https://first.example/"),
            "stable rank → first-listed wins"
        );
    }

    // -- entry / list / advisories (chains list + testnet/red-flag warnings) --

    const TESTNET_ENTRY: &str = r#"{
        "name": "Sepolia",
        "chain": "ETH",
        "chainId": 11155111,
        "shortName": "sep",
        "chainSlug": "sepolia",
        "isTestnet": true,
        "nativeCurrency": { "name": "Sepolia Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    const RED_FLAG_ENTRY: &str = r#"{
        "name": "Sketchy Chain",
        "chain": "SKETCH",
        "chainId": 424242,
        "shortName": "sketch",
        "redFlags": ["scam", "reusedChainId"],
        "nativeCurrency": { "name": "S", "symbol": "S", "decimals": 18 }
    }"#;

    // 1 (eth, alchemy-hosted, mainnet), 11155111 (sepolia testnet, alchemy-hosted),
    // 99999 (test, not alchemy-hosted, no testnet flag).
    fn list_fixture() -> Vec<ChainlistEntry> {
        serde_json::from_str(&format!("[{ETH_ENTRY},{TESTNET_ENTRY},{RPC_HEAVY_ENTRY}]"))
            .expect("fixture parse")
    }

    fn ids(entries: &[&ChainlistEntry]) -> Vec<ChainId> {
        entries.iter().map(|e| e.chain_id).collect()
    }

    #[test]
    fn list_default_excludes_testnets_sorted_by_chain_id() {
        let r = ChainResolver::from_entries(list_fixture());
        // sepolia (11155111) is a testnet and stays hidden by default.
        assert_eq!(
            ids(&r.list(ListFilter::default())),
            [1, 99999].map(ChainId::new)
        );
    }

    #[test]
    fn list_include_testnets_returns_full_list() {
        let r = ChainResolver::from_entries(list_fixture());
        let filter = ListFilter {
            include_testnets: true,
            alchemy_only: false,
        };
        assert_eq!(
            ids(&r.list(filter)),
            [1, 99999, 11_155_111].map(ChainId::new)
        );
    }

    #[test]
    fn list_alchemy_only_keeps_alchemy_hosted_chains() {
        let r = ChainResolver::from_entries(list_fixture());
        let filter = ListFilter {
            include_testnets: false,
            alchemy_only: true,
        };
        // eth (1) and sepolia (11155111) have Alchemy slugs; 99999 does not.
        // sepolia is additionally dropped as a testnet.
        assert_eq!(ids(&r.list(filter)), [ChainId::new(1)]);

        let filter = ListFilter {
            include_testnets: true,
            alchemy_only: true,
        };
        assert_eq!(ids(&r.list(filter)), [1, 11_155_111].map(ChainId::new));
    }

    #[test]
    fn entry_finds_by_chain_id() {
        let r = ChainResolver::from_entries(list_fixture());
        assert_eq!(
            r.entry(ChainId::new(1)).expect("eth present").short_name,
            "eth"
        );
        assert!(r.entry(ChainId::new(123_456)).is_none());
    }

    #[test]
    fn testnet_and_red_flag_accessors() {
        let testnet: ChainlistEntry = serde_json::from_str(TESTNET_ENTRY).expect("parse");
        assert!(testnet.is_testnet());
        assert!(testnet.red_flags().is_empty());

        let eth: ChainlistEntry = serde_json::from_str(ETH_ENTRY).expect("parse");
        assert!(!eth.is_testnet(), "isTestnet:false → not a testnet");
        assert!(eth.red_flags().is_empty(), "redFlags:[] → empty");

        let minimal: ChainlistEntry = serde_json::from_str(MINIMAL_ENTRY).expect("parse");
        assert!(!minimal.is_testnet(), "absent isTestnet → not a testnet");
        assert!(minimal.red_flags().is_empty(), "absent redFlags → empty");

        let flagged: ChainlistEntry = serde_json::from_str(RED_FLAG_ENTRY).expect("parse");
        assert!(!flagged.is_testnet());
        let flags: Vec<&str> = flagged.red_flags().iter().map(String::as_str).collect();
        assert_eq!(flags, ["scam", "reusedChainId"]);
    }
}
