use std::path::{Path, PathBuf};

use toml::de::Error as TomlError;

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
    #[error("unsupported chain id {0} for Alchemy")]
    UnsupportedChain(u64),
}

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    pub provider: Option<ProviderConfig>,
    pub etherscan: Option<EtherscanConfig>,
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

    /// Try loading; return None if no config file found.
    pub fn load_optional(explicit_path: Option<&Path>) -> Result<Option<Self>, ConfigError> {
        match Self::load(explicit_path) {
            Ok(c) => Ok(Some(c)),
            Err(ConfigError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn load_from(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::Io {
            path: path.to_owned(),
            source: e,
        })?;
        let interpolated = interpolate_env(&raw)?;
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

    /// Build the Alchemy RPC URL for `chain_id` if both an Alchemy API key
    /// is configured and the chain has an Alchemy slug. Returns `None`
    /// otherwise (caller decides on fallback).
    ///
    /// An empty `api_key` (e.g. from the default config when
    /// `ALCHEMY_KEY` env var is unset, via `${ALCHEMY_KEY:-}`) counts as
    /// "no Alchemy configured" and yields `None`.
    pub fn alchemy_url(&self, chain_id: u64) -> Option<String> {
        let alchemy = self.provider.as_ref()?.alchemy.as_ref()?;
        if alchemy.api_key.is_empty() {
            return None;
        }
        let slug = alchemy_chain_slug(chain_id)?;
        Some(format!(
            "https://{slug}.g.alchemy.com/v2/{}",
            alchemy.api_key
        ))
    }

    /// Resolve RPC URL: explicit `--rpc-url` > Alchemy config > error.
    pub fn rpc_url(
        &self,
        explicit_url: Option<&str>,
        chain_id: u64,
    ) -> Result<String, ConfigError> {
        if let Some(url) = explicit_url {
            return Ok(url.to_string());
        }
        if let Some(url) = self.alchemy_url(chain_id) {
            return Ok(url);
        }
        Err(ConfigError::UnsupportedChain(chain_id))
    }

    /// Etherscan API key, if configured. Used by the signature resolver to
    /// fall back to Etherscan when Sourcify has no metadata for a contract.
    pub fn etherscan_api_key(&self) -> Option<&str> {
        self.etherscan.as_ref().map(|e| e.api_key.as_str())
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
    }
}

fn mask(key: &str) -> String {
    if key.len() > 6 {
        format!("{}...{}", &key[..3], &key[key.len() - 3..])
    } else {
        "***".to_string()
    }
}

/// Replace `${VAR}` patterns with environment variable values.
fn interpolate_env(input: &str) -> Result<String, ConfigError> {
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
            let value = match (std::env::var(var_name), fallback) {
                (Ok(v), _) => v,
                (Err(_), Some(fb)) => fb.to_owned(),
                (Err(_), None) => {
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

fn alchemy_chain_slug(chain_id: u64) -> Option<&'static str> {
    match chain_id {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alchemy_slug_bnb_mainnet() {
        assert_eq!(alchemy_chain_slug(56), Some("bnb-mainnet"));
    }

    fn alchemy_config(key: &str) -> Config {
        Config {
            provider: Some(ProviderConfig {
                alchemy: Some(AlchemyConfig {
                    api_key: key.to_string(),
                }),
            }),
            etherscan: None,
        }
    }

    #[test]
    fn alchemy_url_returns_some_for_supported_chain_with_key() {
        let c = alchemy_config("test-key-123");
        assert_eq!(
            c.alchemy_url(1).as_deref(),
            Some("https://eth-mainnet.g.alchemy.com/v2/test-key-123")
        );
        assert_eq!(
            c.alchemy_url(56).as_deref(),
            Some("https://bnb-mainnet.g.alchemy.com/v2/test-key-123")
        );
    }

    #[test]
    fn alchemy_url_returns_none_for_unsupported_chain() {
        let c = alchemy_config("test-key");
        assert!(c.alchemy_url(99_999).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_no_provider() {
        let c = Config {
            provider: None,
            etherscan: None,
        };
        assert!(c.alchemy_url(1).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_no_alchemy_in_provider() {
        let c = Config {
            provider: Some(ProviderConfig { alchemy: None }),
            etherscan: None,
        };
        assert!(c.alchemy_url(1).is_none());
    }

    #[test]
    fn alchemy_url_returns_none_when_api_key_is_empty() {
        // Important for the `${ALCHEMY_KEY:-}` default config path:
        // empty key means "no Alchemy effectively", fall through to public RPC.
        let c = alchemy_config("");
        assert!(c.alchemy_url(1).is_none());
    }

    #[test]
    fn alchemy_chain_slug_returns_some_for_known_chains() {
        assert_eq!(alchemy_chain_slug(1), Some("eth-mainnet"));
        assert_eq!(alchemy_chain_slug(56), Some("bnb-mainnet"));
        assert_eq!(alchemy_chain_slug(42161), Some("arb-mainnet"));
    }

    #[test]
    fn alchemy_chain_slug_returns_none_for_unknown_chains() {
        assert!(alchemy_chain_slug(99_999).is_none());
    }

    #[test]
    fn interpolate_strict_var_still_errors_when_unset() {
        // Unique name to avoid env collisions across parallel tests.
        std::env::remove_var("CHAINFOLD_TEST_STRICT_UNSET");
        let err = interpolate_env("x=${CHAINFOLD_TEST_STRICT_UNSET}").expect_err("should error");
        assert!(
            matches!(err, ConfigError::MissingEnvVar { ref var } if var == "CHAINFOLD_TEST_STRICT_UNSET")
        );
    }

    #[test]
    fn interpolate_default_syntax_empty_when_unset() {
        std::env::remove_var("CHAINFOLD_TEST_EMPTY_DEFAULT");
        let out = interpolate_env("x=${CHAINFOLD_TEST_EMPTY_DEFAULT:-}").expect("should not error");
        assert_eq!(out, "x=");
    }

    #[test]
    fn interpolate_default_syntax_fallback_when_unset() {
        std::env::remove_var("CHAINFOLD_TEST_FALLBACK");
        let out =
            interpolate_env("x=${CHAINFOLD_TEST_FALLBACK:-hello world}").expect("should not error");
        assert_eq!(out, "x=hello world");
    }

    #[test]
    fn interpolate_default_syntax_env_wins_when_set() {
        std::env::set_var("CHAINFOLD_TEST_ENV_WINS", "from-env");
        let out =
            interpolate_env("x=${CHAINFOLD_TEST_ENV_WINS:-fallback}").expect("should not error");
        assert_eq!(out, "x=from-env");
        std::env::remove_var("CHAINFOLD_TEST_ENV_WINS");
    }
}
