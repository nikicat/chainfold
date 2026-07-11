pub mod blocks;
pub mod erc20;
pub mod events;

use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::Http;
use alloy::transports::http::reqwest::Url;
use std::sync::Arc;

use chainfold::cache::CacheDb;
use chainfold::evm::{
    ChainId, ChainProvider, EvmChain, EvmChainConfig, RpcTimingLayer, rpc_retry_layer,
};
use chainfold::observe::ConnectTimingLayer;

use crate::chains::{ChainHandle, ChainResolver, ChainSelector, select_chain};
use crate::cli::{Direction, EvmArgs, EvmCommand, FollowMode};
use crate::config::{ChainIdCheck, Config, ConfigError, RpcSource};
use crate::table::Out;
use crate::util::{resolved_db_path, setup_span};
use chainfold::chain::ChunkSize;

/// Env var overriding the configured/built-in default chain, consulted when
/// neither `--chain` nor `--chain-id` is passed.
const CHAIN_ENV_VAR: &str = "CHAINFOLD_CHAIN";

pub async fn run(
    config: &Config,
    args: &EvmArgs,
    direction: Direction,
    follow: FollowMode,
    out: Out,
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
        tracing::info!(
            "no --chain/--chain-id given; using chain {chain_id} \
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

    // Open the durable cache before the provider — the ChainProvider bundles
    // the log cache (header singleflight lives behind it).
    let db_path = resolved_db_path(args.db.as_deref());
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let cache = CacheDb::open(&db_path)?;

    // Hand-build the HTTP client (instead of `ClientBuilder::http`) so the
    // connector is wrapped with the connect-timing layer: DNS+TCP+TLS setup
    // gets its own `connect` span in --stats, and the first RPC on a fresh
    // connection stops looking mysteriously slow.
    let http_client = {
        // `build()` eagerly loads and PEM-parses the system TLS root store —
        // several ms paid even when the whole run is served from cache.
        let _span = setup_span!("client_build").entered();
        reqwest::Client::builder()
            .connector_layer(ConnectTimingLayer)
            .build()?
    };
    let transport = Http::with_client(http_client, url);
    let is_local = transport.guess_local();
    // Rpc timing goes outermost (before retry) so one `rpc` span covers each
    // logical request end-to-end, retries and backoff included.
    let client = ClientBuilder::default()
        .layer(RpcTimingLayer)
        .layer(rpc_retry_layer())
        .transport(transport, is_local);
    // The chain id is already resolved (and typed) from args — bundle it into
    // the provider so the EVM layer reads it for free instead of re-fetching
    // eth_chainId.
    let provider = ChainProvider::new(
        ProviderBuilder::new().connect_client(client),
        chain_id,
        Arc::new(cache.logs()),
    );

    match config.chain_id_check() {
        ChainIdCheck::Verify => {
            let rpc_chain_id = provider.get_chain_id().await?;
            if rpc_chain_id != chain_id.get() {
                eprintln!(
                    "warning: requested chain {chain_id} but RPC reports chain {rpc_chain_id}"
                );
            }
        }
        ChainIdCheck::Trust => {}
    }

    // One chain entity per invocation. Construction is free (no RPC, no
    // tasks); head tracking starts lazily on the first demand for a tip.
    // This binary always builds an HTTP provider, so the transport is the
    // default 2s poll — `--follow` only changes how commands consume the
    // stores, never the chain itself.
    let chain = EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(args.chunk_size),
            ..EvmChainConfig::default()
        },
    );

    match &args.command {
        EvmCommand::Blocks(blocks_args) => {
            blocks::run(&chain, &db_path, args, blocks_args, direction, follow, out).await
        }
        EvmCommand::Erc20(erc20_args) => {
            erc20::run(
                &chain, &cache, &db_path, args, erc20_args, direction, follow, out,
            )
            .await
        }
        EvmCommand::Events(events_args) => {
            events::run(
                &chain,
                &cache,
                &db_path,
                config,
                args,
                events_args,
                direction,
                follow,
                out,
            )
            .await
        }
    }
}
