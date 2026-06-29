pub mod erc20;
pub mod events;

use std::path::PathBuf;

use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::evm::{
    CacheDb, ChainId, ChainProvider, RpcMethod, rpc_retry_layer, rpc_span, versioned_path,
};
use tracing::Instrument;

use crate::chains::{ChainHandle, ChainResolver, ChainSelector, select_chain};
use crate::cli::{Direction, EvmArgs, EvmCommand, FollowMode};
use crate::config::{ChainIdCheck, Config, ConfigError, RpcSource};

/// Env var overriding the configured/built-in default chain, consulted when
/// neither `--chain` nor `--chain-id` is passed.
const CHAIN_ENV_VAR: &str = "CHAINFOLD_CHAIN";

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
    direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve which chain to target. Precedence (see `select_chain`):
    // --chain-id > --chain > $CHAINFOLD_CHAIN > [evm] default_chain > ethereum.
    let defaulted = args.chain.is_none() && args.chain_id.is_none();
    let env_default = std::env::var(CHAIN_ENV_VAR)
        .ok()
        .and_then(|s| ChainSelector::parse(&s));
    let selector = select_chain(
        args.chain_id.map(ChainId::new),
        args.chain.as_deref().map(ChainHandle::new),
        env_default,
        config.default_chain().cloned(),
    );

    // Load the chainlist resolver if we'll need it: name→id resolution, OR the
    // public-RPC fallback (i.e. no explicit --rpc-url).
    let needs_name = matches!(selector, ChainSelector::Name(_));
    let resolver = if needs_name || args.rpc_url.is_none() {
        Some(ChainResolver::load_or_fetch().await?)
    } else {
        None
    };

    let chain_id = match &selector {
        ChainSelector::Id(id) => *id,
        ChainSelector::Name(handle) => resolver
            .as_ref()
            .expect("resolver loaded when chain is selected by name")
            .resolve(handle)?,
    };

    if defaulted {
        eprintln!(
            "note: no --chain/--chain-id given; using chain {chain_id} \
             (set {CHAIN_ENV_VAR} or [evm] default_chain to change)"
        );
    }

    // Surface chainlist advisories for the resolved chain, when the chainlist
    // data is already loaded (resolver present — i.e. a name lookup or the
    // public-RPC fallback path). Testnet is an FYI note; DefiLlama red flags
    // (known scam/spam chains) are a loud warning.
    if let Some(entry) = resolver.as_ref().and_then(|r| r.entry(chain_id)) {
        if entry.is_testnet() {
            eprintln!(
                "note: chain '{}' (id {chain_id}) is a testnet",
                entry.short_name
            );
        }
        let flags = entry.red_flags();
        if !flags.is_empty() {
            eprintln!(
                "warning: chain '{}' (id {chain_id}) is flagged by chainlist.org: {}",
                entry.short_name,
                flags.join(", "),
            );
        }
    }

    // RPC URL resolution order:
    // 1. explicit --rpc-url (raw user string — parsed at this seam)
    // 2. Alchemy from config (file or default-via-env)
    // 3. public RPC from chainlist.org
    // 4. NoRpcAvailable error
    let url: Url = if let Some(raw) = args.rpc_url.as_deref() {
        raw.parse()?
    } else if let Some(url) = config.alchemy_url(chain_id) {
        url
    } else if let Some(url) = resolver.as_ref().and_then(|r| r.public_rpc_for(chain_id)) {
        eprintln!("note: using public RPC {url} from chainlist.org");
        url
    } else {
        return Err(ConfigError::NoRpcAvailable {
            chain_id,
            tried: vec![
                RpcSource::CliFlag,
                RpcSource::Alchemy,
                RpcSource::ChainlistPublic,
            ],
        }
        .into());
    };

    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    // The chain id is already resolved (and typed) from args — bundle it into
    // the provider so the EVM layer reads it for free instead of re-fetching
    // eth_chainId.
    let provider = ChainProvider::new(ProviderBuilder::new().connect_client(client), chain_id);

    let db_path_buf = args.db.clone().unwrap_or_else(default_db_path);
    if let Some(parent) = db_path_buf.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let db_path = versioned_path(&db_path_buf.to_string_lossy());
    let cache = CacheDb::open(&db_path)?;

    match config.chain_id_check() {
        ChainIdCheck::Verify => {
            let rpc_chain_id = provider
                .get_chain_id()
                .instrument(rpc_span(RpcMethod::ChainId))
                .await?;
            if rpc_chain_id != chain_id.get() {
                eprintln!(
                    "warning: requested chain {chain_id} but RPC reports chain {rpc_chain_id}"
                );
            }
        }
        ChainIdCheck::Trust => {}
    }

    match &args.command {
        EvmCommand::Erc20(erc20_args) => {
            erc20::run(
                &provider, &cache, &db_path, args, erc20_args, chain_id, direction, follow,
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
                direction,
                follow,
            )
            .await
        }
    }
}
