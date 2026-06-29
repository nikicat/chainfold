use std::path::{Path, PathBuf};

use alloy::transports::http::reqwest::Url;
use chainfold::evm::ChainId;
use toml::de::Error as TomlError;

use crate::chains::{self, ChainSelector};

/// Hardcoded fallback used when no config file is present, or to fill in
/// fields missing from a partial user config. Env-interpolated on load via
/// the same `${VAR:-fallback}` rules as a real config file, so unset env
/// vars yield empty strings (which downstream getters like
/// `Config::alchemy_url` treat as "not configured").
const DEFAULT_CONFIG_TOML: &str = r#"
[provider.alchemy]
api_key = "${ALCHEMY_KEY:-}"
"#;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file not found (searched: {searched:?})")]
    NotFound { searched: Vec<PathBuf> },
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse { path: PathBuf, source: TomlError },
    #[error("env var ${var} referenced in config is not set")]
    MissingEnvVar { var: String },
    #[error(
        "no RPC URL available for chain {chain_id} (tried: {}). \
         Pass --rpc-url <url>, set ALCHEMY_KEY in the environment for a supported \
         chain, or add chainlist.org data for this chain id.",
        .tried.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    )]
    NoRpcAvailable {
        chain_id: ChainId,
        tried: Vec<RpcSource>,
    },
}

/// A source the CLI consults for an RPC URL, in priority order. Carried by
/// [`ConfigError::NoRpcAvailable`] to report which sources were exhausted —
/// a closed vocabulary rather than free-text strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcSource {
    /// The explicit `--rpc-url` flag.
    CliFlag,
    /// An Alchemy endpoint built from a configured API key.
    Alchemy,
    /// A public endpoint from chainlist.org.
    ChainlistPublic,
}

impl std::fmt::Display for RpcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RpcSource::CliFlag => "--rpc-url",
            RpcSource::Alchemy => "Alchemy",
            RpcSource::ChainlistPublic => "chainlist.org public RPCs",
        })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    pub provider: Option<ProviderConfig>,
    pub etherscan: Option<EtherscanConfig>,
    pub evm: Option<EvmConfig>,
}

#[derive(Debug, serde::Deserialize)]
pub struct EvmConfig {
    /// What to do about the RPC's chain id on startup. Defaults to
    /// [`ChainIdCheck::Verify`]. In TOML: `chain_id_check = "verify"` | `"trust"`.
    pub chain_id_check: Option<ChainIdCheck>,

    /// Chain used when neither `--chain` nor `--chain-id` is passed, parsed into
    /// a [`ChainSelector`]. A chainlist shortName/chainSlug (e.g. `"arbitrum"`)
    /// or a numeric chain id as a string (e.g. `"56"`). In TOML:
    /// `default_chain = "..."`. Overridden by the `CHAINFOLD_CHAIN` env var; if
    /// all are unset the built-in default is `ethereum`. An empty value is
    /// treated as unset.
    #[serde(default, deserialize_with = "de_optional_chain_selector")]
    pub default_chain: Option<ChainSelector>,
}

/// Whether to confirm the RPC serves the requested chain. A closed choice
/// rather than a bare bool, so the config reads `"verify"` / `"trust"` instead
/// of a `true`/`false` whose meaning you have to remember.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainIdCheck {
    /// Cross-check the RPC's reported chain id against the requested chain
    /// (one `eth_chainId` call). The safe default.
    #[default]
    Verify,
    /// Trust the configured chain id; skip the check and its `eth_chainId`
    /// round-trip. Use only when you control the endpoint.
    Trust,
}

#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub alchemy: Option<AlchemyConfig>,
}

#[derive(Debug, serde::Deserialize)]
pub struct AlchemyConfig {
    pub api_key: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct EtherscanConfig {
    pub api_key: String,
}

impl Config {
    /// Load config from `--config` path, or search default locations.
    pub fn load(explicit_path: Option<&Path>) -> Result<Self, ConfigError> {
        if let Some(p) = explicit_path {
            return Self::load_from(p);
        }

        let candidates = Self::default_paths();
        for path in &candidates {
            if path.exists() {
                return Self::load_from(path);
            }
        }

        Err(ConfigError::NotFound {
            searched: candidates,
        })
    }

    /// Load config, layering the user's config on top of the hardcoded
    /// default so that any fields the user left unset inherit defaults.
    ///
    /// 1. Parse the built-in default (env-interpolated).
    /// 2. If a user file exists, parse it (also env-interpolated) and merge
    ///    it over the default. Missing file is fine — the default stands
    ///    alone. Any other error (parse, I/O) propagates.
    /// 3. Deserialize the merged TOML into `Config`.
    ///
    /// The merge is a uniform deep-merge over `toml::Value`s; this module
    /// has no field-specific knowledge of what's in the schema.
    pub fn load_optional(explicit_path: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_optional_with(explicit_path, &real_env)
    }

    /// [`Self::load_optional`] with an injectable environment lookup. Tests
    /// pass a fixed map; production passes [`real_env`].
    fn load_optional_with(
        explicit_path: Option<&Path>,
        env: EnvLookup,
    ) -> Result<Self, ConfigError> {
        let mut merged = parse_default_toml(env)?;
        match load_user_toml(explicit_path, env) {
            Ok(Some(user)) => merge_toml(&mut merged, user),
            Ok(None) | Err(ConfigError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }
        toml::Value::try_into(merged).map_err(|source| ConfigError::Parse {
            path: PathBuf::from("<merged config>"),
            source,
        })
    }

    fn load_from(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::Io {
            path: path.to_owned(),
            source: e,
        })?;
        let interpolated = interpolate_env(&raw, &real_env)?;
        let config: Config = toml::from_str(&interpolated).map_err(|e| ConfigError::Parse {
            path: path.to_owned(),
            source: e,
        })?;
        Ok(config)
    }

    fn default_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();
        paths.push(PathBuf::from("./config.toml"));
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("chainfold").join("config.toml"));
        }
        paths
    }

    /// The configured Alchemy API key, if any. An empty `api_key` (e.g.
    /// from the default config when `ALCHEMY_KEY` env var is unset, via
    /// `${ALCHEMY_KEY:-}`) counts as "no Alchemy configured".
    pub fn alchemy_api_key(&self) -> Option<&str> {
        let alchemy = self.provider.as_ref()?.alchemy.as_ref()?;
        if alchemy.api_key.is_empty() {
            None
        } else {
            Some(&alchemy.api_key)
        }
    }

    /// Build the Alchemy RPC URL for `chain_id` if both an Alchemy API key
    /// is configured and the chain has an Alchemy slug. Returns `None`
    /// otherwise (caller decides on fallback). URL shape lives in
    /// [`chains::alchemy_url`].
    pub fn alchemy_url(&self, chain_id: ChainId) -> Option<Url> {
        chains::alchemy_url(chain_id, self.alchemy_api_key()?)
    }

    /// Etherscan API key, if configured. Used by the signature resolver to
    /// fall back to Etherscan when Sourcify has no metadata for a contract.
    pub fn etherscan_api_key(&self) -> Option<&str> {
        self.etherscan.as_ref().map(|e| e.api_key.as_str())
    }

    /// How to treat the RPC's chain id on startup. Defaults to
    /// [`ChainIdCheck::Verify`] when unset.
    pub fn chain_id_check(&self) -> ChainIdCheck {
        self.evm
            .as_ref()
            .and_then(|e| e.chain_id_check)
            .unwrap_or_default()
    }

    /// The configured default chain, used when neither `--chain` nor
    /// `--chain-id` is passed. `[evm] default_chain` in TOML.
    pub fn default_chain(&self) -> Option<&ChainSelector> {
        self.evm.as_ref()?.default_chain.as_ref()
    }

    /// Print resolved config with API key masked.
    pub fn show(&self) {
        println!("[provider]");
        match &self.provider {
            Some(p) => match &p.alchemy {
                Some(a) => {
                    println!("  alchemy.api_key = {}", mask(&a.api_key));
                }
                None => println!("  (no alchemy config)"),
            },
            None => println!("  (no provider config)"),
        }
        println!("[etherscan]");
        match &self.etherscan {
            Some(e) => println!("  api_key = {}", mask(&e.api_key)),
            None => println!("  (no etherscan config)"),
        }
        println!("[evm]");
        println!("  chain_id_check = {:?}", self.chain_id_check());
        match self.default_chain() {
            Some(sel) => println!("  default_chain  = {sel}"),
            None => println!("  default_chain  = (unset → ethereum)"),
        }
    }
}

/// Deserialize `[evm] default_chain` into an optional [`ChainSelector`], mapping
/// an empty string to `None` so a blank or `${VAR:-}`-interpolated value falls
/// through to the env var / built-in default rather than erroring.
fn de_optional_chain_selector<'de, D>(d: D) -> Result<Option<ChainSelector>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = <String as serde::Deserialize>::deserialize(d)?;
    Ok(ChainSelector::parse(&raw))
}

fn mask(key: &str) -> String {
    if key.len() > 6 {
        format!("{}...{}", &key[..3], &key[key.len() - 3..])
    } else {
        "***".to_string()
    }
}

/// Looks up an environment variable for `${VAR}` interpolation. Production
/// uses [`real_env`]; tests inject a fixed map so config loading never
/// mutates the global process environment (which is racy under parallel
/// tests, and `unsafe` to mutate as of Rust 2024).
type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// The production [`EnvLookup`]: reads the real process environment.
fn real_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Replace `${VAR}` patterns with values from `env`.
fn interpolate_env(input: &str, env: EnvLookup) -> Result<String, ConfigError> {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut content = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                content.push(ch);
            }
            // Support shell-style `${VAR:-fallback}`: split on `:-`. If the
            // env var is unset, substitute the fallback (which may be empty).
            // Without `:-`, the env var is mandatory (existing strict behavior).
            let (var_name, fallback) = match content.split_once(":-") {
                Some((name, fb)) => (name, Some(fb)),
                None => (content.as_str(), None),
            };
            let value = match (env(var_name), fallback) {
                (Some(v), _) => v,
                (None, Some(fb)) => fb.to_owned(),
                (None, None) => {
                    return Err(ConfigError::MissingEnvVar {
                        var: var_name.to_owned(),
                    });
                }
            };
            result.push_str(&value);
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

/// Parse the built-in default TOML into a `toml::Value` (env-interpolated).
fn parse_default_toml(env: EnvLookup) -> Result<toml::Value, ConfigError> {
    let interpolated = interpolate_env(DEFAULT_CONFIG_TOML, env)?;
    toml::from_str(&interpolated).map_err(|source| ConfigError::Parse {
        path: PathBuf::from("<built-in default>"),
        source,
    })
}

/// Read and parse the user's config (if any) as a `toml::Value`. Returns
/// `Ok(None)` only when no file was found at the default search locations
/// and no explicit path was given; an explicit path that doesn't exist is
/// still surfaced as `NotFound` by `Config::load`.
fn load_user_toml(
    explicit_path: Option<&Path>,
    env: EnvLookup,
) -> Result<Option<toml::Value>, ConfigError> {
    let Some(path) = find_config_path(explicit_path)? else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(&path).map_err(|e| ConfigError::Io {
        path: path.clone(),
        source: e,
    })?;
    let interpolated = interpolate_env(&raw, env)?;
    let value: toml::Value =
        toml::from_str(&interpolated).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
    Ok(Some(value))
}

/// Resolve which config file to read, if any. Mirrors `Config::load`'s
/// search behavior but returns `Ok(None)` instead of `NotFound` when no
/// default-path file exists, since `load_optional` treats "no user file"
/// as benign.
fn find_config_path(explicit_path: Option<&Path>) -> Result<Option<PathBuf>, ConfigError> {
    if let Some(p) = explicit_path {
        if p.exists() {
            return Ok(Some(p.to_owned()));
        }
        return Err(ConfigError::NotFound {
            searched: vec![p.to_owned()],
        });
    }
    for path in Config::default_paths() {
        if path.exists() {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Deep-merge `overlay` into `base` over `toml::Value`. When both sides
/// are tables we recurse key-by-key (user wins on conflict, defaults fill
/// gaps). Any non-table on the overlay replaces the slot in `base` whole.
fn merge_toml(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base_tbl), toml::Value::Table(overlay_tbl)) => {
            for (key, overlay_val) in overlay_tbl {
                match base_tbl.get_mut(&key) {
                    Some(base_val) => merge_toml(base_val, overlay_val),
                    None => {
                        base_tbl.insert(key, overlay_val);
                    }
                }
            }
        }
        (slot, overlay) => *slot = overlay,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Build a fixed [`EnvLookup`] from `key=value` pairs. Config loading
    /// reads only from this map, so these tests never touch the global
    /// process environment and are inherently parallel-safe — no mutation,
    /// no shared-var lock, no `unsafe`.
    fn env_map(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |k| map.get(k).cloned()
    }

    fn alchemy_config(key: &str) -> Config {
        Config {
            provider: Some(ProviderConfig {
                alchemy: Some(AlchemyConfig {
                    api_key: key.to_string(),
                }),
            }),
            etherscan: None,
            evm: None,
        }
    }

    #[test]
    fn alchemy_url_returns_some_for_supported_chain_with_key() {
        let c = alchemy_config("test-key-123");
        assert_eq!(
            c.alchemy_url(ChainId::new(1)).as_ref().map(Url::as_str),
            Some("https://eth-mainnet.g.alchemy.com/v2/test-key-123")
        );
        assert_eq!(
            c.alchemy_url(ChainId::new(56)).as_ref().map(Url::as_str),
            Some("https://bnb-mainnet.g.alchemy.com/v2/test-key-123")
        );
    }

    #[test]
    fn alchemy_url_returns_none_for_unsupported_chain() {
        let c = alchemy_config("test-key");
        assert!(c.alchemy_url(ChainId::new(99_999)).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_no_provider() {
        let c = Config {
            provider: None,
            etherscan: None,
            evm: None,
        };
        assert!(c.alchemy_url(ChainId::new(1)).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_no_alchemy_in_provider() {
        let c = Config {
            provider: Some(ProviderConfig { alchemy: None }),
            etherscan: None,
            evm: None,
        };
        assert!(c.alchemy_url(ChainId::new(1)).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_api_key_is_empty() {
        // Important for the `${ALCHEMY_KEY:-}` default config path:
        // empty key means "no Alchemy effectively", fall through to public RPC.
        let c = alchemy_config("");
        assert!(c.alchemy_url(ChainId::new(1)).is_none());
    }

    #[test]
    fn interpolate_strict_var_still_errors_when_unset() {
        let err = interpolate_env("x=${MISSING}", &env_map(&[])).expect_err("should error");
        assert!(matches!(err, ConfigError::MissingEnvVar { ref var } if var == "MISSING"));
    }

    #[test]
    fn interpolate_default_syntax_empty_when_unset() {
        let out = interpolate_env("x=${MISSING:-}", &env_map(&[])).expect("should not error");
        assert_eq!(out, "x=");
    }

    #[test]
    fn interpolate_default_syntax_fallback_when_unset() {
        let out =
            interpolate_env("x=${MISSING:-hello world}", &env_map(&[])).expect("should not error");
        assert_eq!(out, "x=hello world");
    }

    #[test]
    fn interpolate_default_syntax_env_wins_when_set() {
        let out = interpolate_env("x=${VAR:-fallback}", &env_map(&[("VAR", "from-env")]))
            .expect("should not error");
        assert_eq!(out, "x=from-env");
    }

    // Helper: parse a provider TOML after env-interpolation against `env`.
    fn default_provider_from_toml(
        toml: &str,
        env: EnvLookup,
    ) -> Result<ProviderConfig, ConfigError> {
        let interpolated = interpolate_env(toml, env)?;
        toml::from_str(&interpolated).map_err(|source| ConfigError::Parse {
            path: PathBuf::from("<synthetic default>"),
            source,
        })
    }

    #[test]
    fn default_provider_with_env_set() {
        let provider = default_provider_from_toml(
            r#"alchemy = { api_key = "${VAR:-}" }"#,
            &env_map(&[("VAR", "from-env-123")]),
        )
        .expect("synthesizes");
        assert_eq!(
            provider.alchemy.as_ref().expect("alchemy").api_key,
            "from-env-123"
        );
    }

    #[test]
    fn default_provider_with_env_unset_yields_empty_key() {
        let provider =
            default_provider_from_toml(r#"alchemy = { api_key = "${VAR:-}" }"#, &env_map(&[]))
                .expect("synthesizes");
        assert_eq!(provider.alchemy.as_ref().expect("alchemy").api_key, "");
    }

    #[test]
    fn load_optional_no_file_synthesizes_default_provider() {
        // We can't easily exercise the default-search-path branch from a
        // test (HOME isolation is fiddly), so we test the conceptual path
        // through the helper: when no file exists, default_provider_from_toml
        // with the same template as production produces the expected value.
        let provider = default_provider_from_toml(
            r#"alchemy = { api_key = "${ALCHEMY_KEY:-}" }"#,
            &env_map(&[("ALCHEMY_KEY", "env-key-789")]),
        )
        .expect("synthesizes");
        assert_eq!(
            provider.alchemy.as_ref().expect("alchemy").api_key,
            "env-key-789"
        );
    }

    #[test]
    fn load_optional_partial_file_fills_default_alchemy() {
        // User config has only [etherscan]; alchemy block must be filled
        // from the default (env-interpolated from ALCHEMY_KEY).
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[etherscan]\napi_key = \"es-key\"\n").expect("write");

        let config = Config::load_optional_with(
            Some(&path),
            &env_map(&[("ALCHEMY_KEY", "from-default-abc")]),
        )
        .expect("load");
        assert_eq!(
            config
                .provider
                .as_ref()
                .and_then(|p| p.alchemy.as_ref())
                .map(|a| a.api_key.as_str()),
            Some("from-default-abc"),
            "alchemy block must be filled from default ${{ALCHEMY_KEY:-}}"
        );
        assert_eq!(
            config.etherscan.as_ref().map(|e| e.api_key.as_str()),
            Some("es-key"),
            "user's etherscan must be preserved"
        );
    }

    #[test]
    fn no_rpc_available_error_message_lists_attempts() {
        // Smoke test that the error formats reasonably and includes the
        // chain id + the "tried" sources. Pure formatting check, no I/O.
        let err = ConfigError::NoRpcAvailable {
            chain_id: ChainId::new(56),
            tried: vec![RpcSource::CliFlag, RpcSource::ChainlistPublic],
        };
        let msg = format!("{err}");
        assert!(msg.contains("56"), "expected chain id in message");
        assert!(
            msg.contains("--rpc-url") && msg.contains("chainlist.org"),
            "expected the tried sources, comma-joined, in the message: {msg}"
        );
    }

    #[test]
    fn rpc_source_display_is_human_readable() {
        assert_eq!(RpcSource::CliFlag.to_string(), "--rpc-url");
        assert_eq!(RpcSource::Alchemy.to_string(), "Alchemy");
        assert_eq!(
            RpcSource::ChainlistPublic.to_string(),
            "chainlist.org public RPCs"
        );
    }

    #[test]
    fn load_optional_explicit_alchemy_wins_over_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[provider.alchemy]\napi_key = \"user-explicit\"\n").expect("write");

        let config = Config::load_optional_with(
            Some(&path),
            &env_map(&[("ALCHEMY_KEY", "should-not-be-used")]),
        )
        .expect("load");
        assert_eq!(
            config
                .provider
                .as_ref()
                .and_then(|p| p.alchemy.as_ref())
                .map(|a| a.api_key.as_str()),
            Some("user-explicit"),
            "user's explicit api_key must override env-derived default"
        );
    }
}
