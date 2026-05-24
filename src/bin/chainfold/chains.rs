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

use serde::Deserialize;
use tempfile::NamedTempFile;
use thiserror::Error;

const CHAINLIST_URL: &str = "https://chainlist.org/rpcs.json";
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

fn default_cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("chainfold")
        .join("chainlist.json")
}

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
    #[serde(default, rename = "infoURL")]
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

#[derive(Debug)]
pub struct ChainResolver {
    by_name: HashMap<String, u64>,
}

impl ChainResolver {
    /// Build a resolver from an already-deserialized list of entries.
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
        Ok(Self::from_entries(&entries))
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

    #[test]
    fn resolver_matches_chain_slug() {
        let r = ChainResolver::from_entries(&fixture_entries());
        assert_eq!(r.resolve("ethereum").expect("slug resolves"), 1);
        assert_eq!(r.resolve("binance").expect("slug resolves"), 56);
    }

    #[test]
    fn resolver_match_is_case_insensitive() {
        let r = ChainResolver::from_entries(&fixture_entries());
        assert_eq!(r.resolve("Ethereum").expect("mixed case"), 1);
        assert_eq!(r.resolve("BNB").expect("upper"), 56);
        assert_eq!(r.resolve("Binance").expect("slug mixed"), 56);
    }

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
}
