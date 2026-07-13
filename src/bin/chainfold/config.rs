use std::path::{Path, PathBuf};

use chainfold::evm::{ApiKey, ChainId, ProviderKind};
use toml::de::Error as TomlError;

use crate::chains::{self, ChainSelector};

/// Hardcoded fallback used when no config file is present, or to fill in
/// fields missing from a partial user config. Env-interpolated on load via
/// the same `${VAR:-fallback}` rules as a real config file, so unset env
/// vars yield empty strings (which downstream getters like
/// `Config::provider_key` treat as "not configured").
///
/// Carries every schema default explicitly, so the resolved config (what
/// `config show` prints) states each field's effective value. The typed
/// accessors keep their code-side fallbacks as backstops;
/// `default_config_matches_code_defaults` pins the two in sync. Runtime
/// overrides that outrank the config file (`CHAINFOLD_CHAIN`,
/// `--chain`/`--chain-id`) deliberately do NOT appear here — this is the
/// config-file layer only.
fn default_config_toml() -> String {
    format!(
        r#"
[provider.alchemy]
api_key = "${{ALCHEMY_KEY:-}}"

[provider.infura]
api_key = "${{INFURA_KEY:-}}"

[evm]
chain_id_check = "verify"
default_chain = "{}"

[bitcoin]
rpc_url = "${{BITCOIND_RPC_URL:-http://127.0.0.1:8332}}"
"#,
        chains::DEFAULT_CHAIN
    )
}

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
         Pass --rpc-url <url>, set ALCHEMY_KEY or INFURA_KEY in the \
         environment for a supported chain, or add chainlist.org data for \
         this chain id.",
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
    /// An endpoint built from this provider's configured API key.
    ConfiguredProvider(ProviderKind),
    /// A public endpoint from chainlist.org.
    ChainlistPublic,
}

impl std::fmt::Display for RpcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcSource::CliFlag => f.write_str("--rpc-url"),
            RpcSource::ConfiguredProvider(kind) => write!(f, "configured {kind} key"),
            RpcSource::ChainlistPublic => f.write_str("chainlist.org public RPCs"),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    pub provider: Option<ProviderConfig>,
    pub etherscan: Option<EtherscanConfig>,
    pub evm: Option<EvmConfig>,
    pub bitcoin: Option<BitcoinConfig>,
}

/// The `[bitcoin]` config section: bitcoind endpoint + auth. Flags outrank
/// every field; user/pass outranks the cookie within a layer (see the auth
/// funnel in `bitcoin::resolve_auth`).
// The shared `rpc_` prefix is deliberate: the keys mirror bitcoind's own
// option vocabulary (rpcuser/rpcpassword/rpccookiefile), and bare
// `url`/`user`/`pass` would read ambiguously in a merged TOML.
#[allow(clippy::struct_field_names)]
#[derive(Debug, serde::Deserialize)]
pub struct BitcoinConfig {
    /// bitcoind JSON-RPC endpoint URL.
    pub rpc_url: Option<String>,
    /// Core cookie file for RPC auth.
    pub rpc_cookie_file: Option<PathBuf>,
    /// `rpcuser` credential (pair with `rpc_pass`).
    pub rpc_user: Option<String>,
    /// `rpcpassword` credential (pair with `rpc_user`).
    pub rpc_pass: Option<String>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
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

/// The `[provider.*]` sections — one entry per [`ProviderKind`], all the
/// same shape. Raw strings at this boundary; [`Config::provider_key`] is
/// the typed funnel (empty means unset).
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub alchemy: Option<ProviderKeyEntry>,
    pub infura: Option<ProviderKeyEntry>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ProviderKeyEntry {
    pub api_key: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct EtherscanConfig {
    pub api_key: String,
}

impl Config {
    /// Load config, layering the user's config on top of the hardcoded
    /// default so that any fields the user left unset inherit defaults.
    ///
    /// 1. Parse the built-in default (env-interpolated).
    /// 2. If a user file exists, parse it (also env-interpolated) and merge
    ///    it over the default. Missing file is fine — the default stands
    ///    alone. Any other error (parse, I/O, explicit `--config` path
    ///    absent) propagates.
    /// 3. Deserialize the merged TOML into `Config`.
    ///
    /// The merge is a uniform deep-merge over `toml::Value`s; this module
    /// has no field-specific knowledge of what's in the schema.
    pub fn load_optional(explicit_path: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_resolved(explicit_path).map(|(config, _)| config)
    }

    /// [`Self::load_optional`], also reporting where the user config came
    /// from: the file merged over the defaults, or `None` when the resolved
    /// config is the built-in default alone. Backs `config show`/`check`.
    pub fn load_resolved(
        explicit_path: Option<&Path>,
    ) -> Result<(Self, Option<PathBuf>), ConfigError> {
        Self::load_resolved_with(explicit_path, &real_env)
    }

    /// [`Self::load_resolved`] with an injectable environment lookup. Tests
    /// pass a fixed map; production passes [`real_env`].
    fn load_resolved_with(
        explicit_path: Option<&Path>,
        env: EnvLookup,
    ) -> Result<(Self, Option<PathBuf>), ConfigError> {
        let mut merged = parse_default_toml(env)?;
        let source = match load_user_toml(explicit_path, env)? {
            Some((path, user)) => {
                merge_toml(&mut merged, user);
                Some(path)
            }
            None => None,
        };
        let config = toml::Value::try_into(merged).map_err(|source| ConfigError::Parse {
            path: PathBuf::from("<merged config>"),
            source,
        })?;
        Ok((config, source))
    }

    fn default_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();
        paths.push(PathBuf::from("./config.toml"));
        if let Some(config_dir) = dirs::config_dir() {
            paths.push(config_dir.join("chainfold").join("config.toml"));
        }
        paths
    }

    /// The configured API key for `kind`, if any — the empty→unset funnel:
    /// an empty `api_key` (the default config's `${ALCHEMY_KEY:-}` /
    /// `${INFURA_KEY:-}` with the env var unset) counts as "not
    /// configured", which [`ApiKey::new`] encodes as `None`.
    pub fn provider_key(&self, kind: ProviderKind) -> Option<ApiKey> {
        let provider = self.provider.as_ref()?;
        let entry = match kind {
            ProviderKind::Alchemy => provider.alchemy.as_ref(),
            ProviderKind::Infura => provider.infura.as_ref(),
        }?;
        ApiKey::new(entry.api_key.as_str())
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

    /// The `[bitcoin]` section, if configured (the built-in default always
    /// carries `rpc_url`).
    pub fn bitcoin(&self) -> Option<&BitcoinConfig> {
        self.bitcoin.as_ref()
    }

    /// Print the effective config as TOML — validated, defaults filled in,
    /// API keys masked. What prints is what the run paths consume, not what
    /// the config file says. `source` is the user file that was merged over
    /// the built-in defaults, if one was found.
    pub fn show(&self, source: Option<&Path>) {
        match source {
            Some(p) => println!("# config file: {}", p.display()),
            None => println!("# no config file found — using built-in defaults"),
        }
        let toml = toml::to_string_pretty(&self.effective())
            .expect("the effective config is a plain table");
        print!("{toml}");
    }

    /// The serializable view behind [`Self::show`]: every field routed
    /// through the same accessor the run paths call, so the dump reflects
    /// effective values. Unset optionals are omitted (TOML has no null);
    /// API keys are masked.
    fn effective(&self) -> EffectiveConfig {
        let masked_key = |kind| {
            self.provider_key(kind).map(|key| EffectiveApiKey {
                api_key: mask(key.expose()),
            })
        };
        let alchemy = masked_key(ProviderKind::Alchemy);
        let infura = masked_key(ProviderKind::Infura);
        EffectiveConfig {
            provider: (alchemy.is_some() || infura.is_some())
                .then_some(EffectiveProvider { alchemy, infura }),
            etherscan: self
                .etherscan_api_key()
                .filter(|key| !key.is_empty())
                .map(|key| EffectiveApiKey { api_key: mask(key) }),
            evm: EffectiveEvm {
                chain_id_check: self.chain_id_check(),
                default_chain: self
                    .default_chain()
                    .map_or_else(|| chains::DEFAULT_CHAIN.to_owned(), ToString::to_string),
            },
            bitcoin: self.bitcoin().map(|b| EffectiveBitcoin {
                rpc_url: b.rpc_url.clone().unwrap_or_default(),
                rpc_cookie_file: b.rpc_cookie_file.clone(),
                rpc_user: b.rpc_user.clone(),
                rpc_pass: b.rpc_pass.as_deref().map(mask),
            }),
        }
    }
}

/// [`Config::effective`]'s output shape — mirrors the config schema so the
/// dump pastes back in as a valid config file.
#[derive(serde::Serialize)]
struct EffectiveConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<EffectiveProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    etherscan: Option<EffectiveApiKey>,
    evm: EffectiveEvm,
    #[serde(skip_serializing_if = "Option::is_none")]
    bitcoin: Option<EffectiveBitcoin>,
}

#[allow(clippy::struct_field_names)] // mirrors BitcoinConfig's keys
#[derive(serde::Serialize)]
struct EffectiveBitcoin {
    rpc_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc_cookie_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc_pass: Option<String>,
}

#[derive(serde::Serialize)]
struct EffectiveProvider {
    #[serde(skip_serializing_if = "Option::is_none")]
    alchemy: Option<EffectiveApiKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    infura: Option<EffectiveApiKey>,
}

#[derive(serde::Serialize)]
struct EffectiveApiKey {
    api_key: String,
}

#[derive(serde::Serialize)]
struct EffectiveEvm {
    chain_id_check: ChainIdCheck,
    default_chain: String,
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
    let interpolated = interpolate_env(&default_config_toml(), env)?;
    toml::from_str(&interpolated).map_err(|source| ConfigError::Parse {
        path: PathBuf::from("<built-in default>"),
        source,
    })
}

/// Read and parse the user's config (if any) as a `toml::Value`, tagged
/// with the path it was read from. Returns `Ok(None)` only when no file was
/// found at the default search locations and no explicit path was given; an
/// explicit path that doesn't exist is a `NotFound` error.
fn load_user_toml(
    explicit_path: Option<&Path>,
    env: EnvLookup,
) -> Result<Option<(PathBuf, toml::Value)>, ConfigError> {
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
    Ok(Some((path, value)))
}

/// Resolve which config file to read, if any. Searches the default
/// locations but returns `Ok(None)` instead of `NotFound` when no
/// default-path file exists, since the load path treats "no user file"
/// as benign. An explicit path is never searched around: absent → error.
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

    /// A `Config` whose `[provider]` table carries the given raw keys.
    fn provider_config(alchemy: Option<&str>, infura: Option<&str>) -> Config {
        let entry = |raw: Option<&str>| {
            raw.map(|api_key| ProviderKeyEntry {
                api_key: api_key.to_owned(),
            })
        };
        Config {
            provider: Some(ProviderConfig {
                alchemy: entry(alchemy),
                infura: entry(infura),
            }),
            etherscan: None,
            evm: None,
            bitcoin: None,
        }
    }

    #[test]
    fn provider_key_returns_the_configured_key_per_kind() {
        let c = provider_config(Some("alch-key"), Some("inf-key"));
        assert_eq!(
            c.provider_key(ProviderKind::Alchemy)
                .as_ref()
                .map(ApiKey::expose),
            Some("alch-key")
        );
        assert_eq!(
            c.provider_key(ProviderKind::Infura)
                .as_ref()
                .map(ApiKey::expose),
            Some("inf-key")
        );
    }

    #[test]
    fn provider_key_empty_means_unset() {
        // The default-config path: `${ALCHEMY_KEY:-}` with the env var
        // unset interpolates to an empty string, which must read as "not
        // configured" and fall through to the next RPC source.
        let c = provider_config(Some(""), Some(""));
        assert!(c.provider_key(ProviderKind::Alchemy).is_none());
        assert!(c.provider_key(ProviderKind::Infura).is_none());
    }

    #[test]
    fn provider_key_absent_sections_mean_unset() {
        let none = Config {
            provider: None,
            etherscan: None,
            evm: None,
            bitcoin: None,
        };
        assert!(none.provider_key(ProviderKind::Alchemy).is_none());

        let partial = provider_config(None, Some("inf-key"));
        assert!(partial.provider_key(ProviderKind::Alchemy).is_none());
        assert!(partial.provider_key(ProviderKind::Infura).is_some());
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

        let (config, source) = Config::load_resolved_with(
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
        assert_eq!(source.as_deref(), Some(path.as_path()), "source reported");
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
        assert_eq!(
            RpcSource::ConfiguredProvider(ProviderKind::Alchemy).to_string(),
            "configured alchemy key"
        );
        assert_eq!(
            RpcSource::ConfiguredProvider(ProviderKind::Infura).to_string(),
            "configured infura key"
        );
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

        let (config, _) = Config::load_resolved_with(
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

    #[test]
    fn load_resolved_explicit_missing_path_errors() {
        // An explicit --config pointing nowhere must error, not silently
        // fall back to defaults (it used to be swallowed).
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nope.toml");
        let err = Config::load_resolved_with(Some(&path), &env_map(&[]))
            .expect_err("missing explicit path must error");
        assert!(matches!(err, ConfigError::NotFound { ref searched } if searched == &[path]));
    }

    #[test]
    fn default_config_matches_code_defaults() {
        // The baked-in [evm] defaults must agree with the code-side
        // backstops (ChainIdCheck::default(), chains::DEFAULT_CHAIN) —
        // `config show` prints the former, the run paths fall back to the
        // latter when a user config force-unsets a field.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").expect("write empty user config");

        let (config, _) =
            Config::load_resolved_with(Some(&path), &env_map(&[])).expect("defaults load");
        assert_eq!(config.chain_id_check(), ChainIdCheck::default());
        assert_eq!(
            config.default_chain().map(ToString::to_string).as_deref(),
            Some(chains::DEFAULT_CHAIN)
        );
        assert!(
            config.provider_key(ProviderKind::Alchemy).is_none(),
            "unset ALCHEMY_KEY yields no alchemy provider"
        );
        assert!(
            config.provider_key(ProviderKind::Infura).is_none(),
            "unset INFURA_KEY yields no infura provider"
        );
    }

    /// The default template reads both provider env vars.
    #[test]
    fn default_config_reads_both_provider_env_vars() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").expect("write empty user config");

        let (config, _) = Config::load_resolved_with(
            Some(&path),
            &env_map(&[("ALCHEMY_KEY", "alch-env"), ("INFURA_KEY", "inf-env")]),
        )
        .expect("load");
        assert_eq!(
            config
                .provider_key(ProviderKind::Alchemy)
                .as_ref()
                .map(ApiKey::expose),
            Some("alch-env")
        );
        assert_eq!(
            config
                .provider_key(ProviderKind::Infura)
                .as_ref()
                .map(ApiKey::expose),
            Some("inf-env")
        );
    }

    #[test]
    fn effective_dump_masks_keys_and_fills_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").expect("write empty user config");

        let (config, _) = Config::load_resolved_with(
            Some(&path),
            &env_map(&[
                ("ALCHEMY_KEY", "secret-key-123"),
                ("INFURA_KEY", "infura-secret-456"),
            ]),
        )
        .expect("load");
        let dump = toml::to_string_pretty(&config.effective()).expect("serialize");
        assert!(
            dump.contains("api_key = \"sec...123\""),
            "alchemy key must print masked: {dump}"
        );
        assert!(
            dump.contains("api_key = \"inf...456\""),
            "infura key must print masked: {dump}"
        );
        assert!(
            !dump.contains("secret-key-123") && !dump.contains("infura-secret-456"),
            "raw keys must not leak: {dump}"
        );
        assert!(!dump.contains("[etherscan]"), "unset section omitted");
        assert!(dump.contains("chain_id_check = \"verify\""), "{dump}");
        assert!(dump.contains("default_chain = \"ethereum\""), "{dump}");
    }
}
