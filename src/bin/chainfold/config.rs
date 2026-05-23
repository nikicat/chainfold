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

    /// Resolve RPC URL: explicit `--rpc-url` > Alchemy config > error.
    pub fn rpc_url(
        &self,
        explicit_url: Option<&str>,
        chain_id: u64,
    ) -> Result<String, ConfigError> {
        if let Some(url) = explicit_url {
            return Ok(url.to_string());
        }
        if let Some(ref provider) = self.provider {
            if let Some(ref alchemy) = provider.alchemy {
                let slug = alchemy_chain_slug(chain_id)?;
                return Ok(format!(
                    "https://{}.g.alchemy.com/v2/{}",
                    slug, alchemy.api_key
                ));
            }
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
            let mut var_name = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                var_name.push(ch);
            }
            let value = std::env::var(&var_name)
                .map_err(|_| ConfigError::MissingEnvVar { var: var_name })?;
            result.push_str(&value);
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

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
