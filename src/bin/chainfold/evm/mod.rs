pub mod erc20;
pub mod events;

use std::path::PathBuf;

use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::evm::{CacheDb, rpc_retry_layer, versioned_path};

use crate::chains::ChainResolver;
use crate::cli::{EvmArgs, EvmCommand};
use crate::config::{Config, ConfigError};

/// Default cache DB location: `$XDG_CACHE_HOME/chainfold/chainfold.db`.
/// Falls back to `./chainfold.db` if no XDG cache dir is resolvable.
fn default_db_path() -> PathBuf {
    dirs::cache_dir().map_or_else(
        || PathBuf::from("./chainfold.db"),
        |d| d.join("chainfold").join("chainfold.db"),
    )
}

pub async fn run(
    config: &Config,
    args: &EvmArgs,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Eagerly load the chainlist resolver if we'll need it: name resolution
    // OR public-RPC fallback (i.e. no explicit --rpc-url).
    let resolver = if args.chain.is_some() || args.rpc_url.is_none() {
        Some(ChainResolver::load_or_fetch().await?)
    } else {
        None
    };

    let chain_id = match (args.chain_id, args.chain.as_deref()) {
        (Some(id), _) => id,
        (None, Some(name)) => resolver
            .as_ref()
            .expect("resolver loaded when args.chain is set")
            .resolve(name)?,
        (None, None) => unreachable!("clap enforces required_unless_present"),
    };

    // RPC URL resolution order:
    // 1. explicit --rpc-url
    // 2. Alchemy from config (file or default-via-env)
    // 3. public RPC from chainlist.org
    // 4. NoRpcAvailable error
    let rpc_url = if let Some(url) = args.rpc_url.as_deref() {
        url.to_string()
    } else if let Some(url) = config.alchemy_url(chain_id) {
        url
    } else if let Some(url) = resolver.as_ref().and_then(|r| r.public_rpc_for(chain_id)) {
        eprintln!("note: using public RPC {url} from chainlist.org");
        url
    } else {
        return Err(ConfigError::NoRpcAvailable {
            chain_id,
            tried: vec![
                "--rpc-url (not provided)".into(),
                "Alchemy (no api_key or unsupported chain)".into(),
                "chainlist.org public RPCs (none usable)".into(),
            ],
        }
        .into());
    };

    let url: Url = rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    let provider = ProviderBuilder::new().connect_client(client);

    let db_path_buf = args.db.clone().unwrap_or_else(default_db_path);
    if let Some(parent) = db_path_buf.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let db_path = versioned_path(&db_path_buf.to_string_lossy());
    let cache = CacheDb::open(&db_path)?;

    let rpc_chain_id = provider.get_chain_id().await?;
    if rpc_chain_id != chain_id {
        eprintln!("warning: requested chain {chain_id} but RPC reports chain {rpc_chain_id}");
    }

    match &args.command {
        EvmCommand::Erc20(erc20_args) => {
            erc20::run(
                &provider, &cache, &db_path, args, erc20_args, chain_id, reverse, follow,
            )
            .await
        }
        EvmCommand::Events(events_args) => {
            events::run(
                &provider,
                &cache,
                &db_path,
                config,
                args,
                events_args,
                chain_id,
                reverse,
                follow,
            )
            .await
        }
    }
}
