pub mod blocks;
pub mod erc20;
pub mod events;

use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use std::path::Path;
use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::transports::TransportError;
use chainfold::cache::CacheDb;
use chainfold::evm::{
    ApiKey, ChainId, ChainProvider, EvmChain, EvmChainConfig, HttpTransport, LogCache,
    ProviderAdaptor, ProviderKind, RpcTimingLayer, RpcTransport, SubscribeTransport,
    detect_provider, generic_adaptor, redact_url, rpc_retry_layer,
};
use chainfold::observe::ConnectTimingLayer;

use crate::chains::{ChainResolver, ChainSelector, select_chain};
use crate::cli::{EvmArgs, EvmCommand, FollowMode};
use crate::config::{ChainIdCheck, Config, ConfigError, RpcSource};
use crate::table::Out;
use crate::util::{resolved_db_path, setup_span};
use chainfold::chain::ChunkSize;

/// Env var overriding the configured/built-in default chain, consulted when
/// neither `--chain` nor `--chain-id` is passed.
const CHAIN_ENV_VAR: &str = "CHAINFOLD_CHAIN";

/// An RPC endpoint resolution: the http URL to drive, the ws sibling when
/// the winning source can derive one (keyed providers), the adaptor to
/// install on the [`ChainProvider`], and which source won (diagnostics +
/// the `NoRpcAvailable` report).
struct ResolvedRpc {
    http: Url,
    ws: Option<Url>,
    adaptor: &'static dyn ProviderAdaptor,
    source: RpcSource,
}

/// The outcome of the offline resolution steps ([`resolve_rpc`]).
enum RpcResolution {
    Resolved(ResolvedRpc),
    /// Every offline source was exhausted — the caller runs the lazy
    /// chainlist step and reports `tried` (plus chainlist) on failure.
    NeedPublicFallback {
        tried: Vec<RpcSource>,
    },
}

/// RPC endpoint resolution, offline steps. Precedence:
///
/// 1. `--rpc-url` — always wins. A recognized provider URL behaves like
///    autoconfig: its adaptor is adopted and the ws sibling derived, so a
///    pasted Alchemy URL follows over ws like a configured key would.
/// 2. Configured provider keys (`ALCHEMY_KEY` / `INFURA_KEY` env vars via
///    the config template, or explicit `[provider.*]` sections), in
///    [`ProviderKind::ALL`] order — the first provider hosting the chain
///    wins.
///
/// Step 3 (chainlist public RPCs) stays with the caller: it needs the
/// lazily-loaded chainlist, and a run that resolves here never loads it.
fn resolve_rpc(rpc_url: Option<&Url>, config: &Config, chain_id: ChainId) -> RpcResolution {
    if let Some(url) = rpc_url {
        let (adaptor, ws) = match detect_provider(url) {
            Some((kind, key)) => (
                kind.adaptor(),
                kind.adaptor().endpoint(chain_id, &key, RpcTransport::Ws),
            ),
            None => (generic_adaptor(), None),
        };
        return RpcResolution::Resolved(ResolvedRpc {
            http: url.clone(),
            ws,
            adaptor,
            source: RpcSource::CliFlag,
        });
    }
    // `tried` lists every source consulted, in order — including the flag
    // the user *could* have passed — so the failure report doubles as the
    // "here are your options" list.
    let mut tried = vec![RpcSource::CliFlag];

    for kind in ProviderKind::ALL.iter().copied() {
        if let Some(key) = config.provider_key(kind)
            && let Some(resolved) =
                keyed_endpoint(kind, &key, chain_id, RpcSource::ConfiguredProvider(kind))
        {
            return RpcResolution::Resolved(resolved);
        }
        tried.push(RpcSource::ConfiguredProvider(kind));
    }

    RpcResolution::NeedPublicFallback { tried }
}

/// The http+ws endpoint pair a keyed provider serves for `chain_id`;
/// `None` when the provider doesn't host the chain.
fn keyed_endpoint(
    kind: ProviderKind,
    key: &ApiKey,
    chain_id: ChainId,
    source: RpcSource,
) -> Option<ResolvedRpc> {
    let adaptor = kind.adaptor();
    let http = adaptor.endpoint(chain_id, key, RpcTransport::Http)?;
    Some(ResolvedRpc {
        ws: adaptor.endpoint(chain_id, key, RpcTransport::Ws),
        http,
        adaptor,
        source,
    })
}

/// Which chain to target, with any chainlist advisories printed and the
/// resolver handed back for reuse (the public-RPC fallback wants it).
/// Precedence (see `select_chain`): `--chain-id` > `--chain` >
/// `$CHAINFOLD_CHAIN` > `[evm] default_chain` > ethereum. The chainlist
/// resolver loads lazily: upfront only for name→id resolution — a
/// keyed/`--chain-id` run never touches it.
async fn resolve_chain(
    config: &Config,
    args: &EvmArgs,
) -> Result<(ChainId, Option<ChainResolver>), Box<dyn std::error::Error>> {
    let defaulted = args.chain.is_none() && args.chain_id.is_none();
    let env_default = std::env::var(CHAIN_ENV_VAR)
        .ok()
        .and_then(|s| ChainSelector::parse(&s));
    let selector = select_chain(
        args.chain_id,
        args.chain.clone(),
        env_default,
        config.default_chain().cloned(),
    );

    let needs_name = matches!(selector, ChainSelector::Name(_));
    let resolver = if needs_name {
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
    print_chain_advisories(resolver.as_ref(), chain_id);
    Ok((chain_id, resolver))
}

/// Chainlist advisories for the resolved chain, when the chainlist data
/// is already loaded (a name lookup happened). Testnet is an FYI note;
/// `DefiLlama` red flags (known scam/spam chains) are a loud warning.
fn print_chain_advisories(resolver: Option<&ChainResolver>, chain_id: ChainId) {
    let Some(entry) = resolver.and_then(|r| r.entry(chain_id)) else {
        return;
    };
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

/// [`resolve_rpc`] plus its final step: when every offline source is
/// exhausted, fall back to a chainlist public RPC (loading the chainlist
/// on demand) — or fail with every source tried.
async fn resolve_endpoint(
    args: &EvmArgs,
    config: &Config,
    chain_id: ChainId,
    resolver: &mut Option<ChainResolver>,
) -> Result<ResolvedRpc, Box<dyn std::error::Error>> {
    let resolved = match resolve_rpc(args.rpc_url.as_ref(), config, chain_id) {
        RpcResolution::Resolved(resolved) => resolved,
        RpcResolution::NeedPublicFallback { tried } => {
            public_rpc_fallback(chain_id, tried, resolver).await?
        }
    };
    tracing::debug!(
        source = %resolved.source,
        endpoint = %redact_url(&resolved.http),
        "rpc endpoint resolved"
    );
    Ok(resolved)
}

/// The last-resort RPC source: a public chainlist endpoint on the generic
/// adaptor (no ws sibling to offer), or the `NoRpcAvailable` report
/// carrying every source consulted.
async fn public_rpc_fallback(
    chain_id: ChainId,
    mut tried: Vec<RpcSource>,
    resolver: &mut Option<ChainResolver>,
) -> Result<ResolvedRpc, Box<dyn std::error::Error>> {
    if resolver.is_none() {
        *resolver = Some(ChainResolver::load_or_fetch().await?);
    }
    let public = resolver
        .as_ref()
        .expect("resolver just loaded")
        .public_rpc_for(chain_id);
    let Some(url) = public else {
        tried.push(RpcSource::ChainlistPublic);
        return Err(ConfigError::NoRpcAvailable { chain_id, tried }.into());
    };
    // redact_url structurally on every echo — even keyless public URLs
    // (an unknown shape may still embed one).
    eprintln!(
        "note: using public RPC {} from chainlist.org",
        redact_url(&url)
    );
    Ok(ResolvedRpc {
        http: url,
        ws: None,
        adaptor: generic_adaptor(),
        source: RpcSource::ChainlistPublic,
    })
}

/// Open the durable cache, creating its parent directory on first run.
fn open_cache(db: Option<&Path>) -> Result<CacheDb, Box<dyn std::error::Error>> {
    let db_path = resolved_db_path(db);
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(CacheDb::open(&db_path)?)
}

/// The main wire provider — always HTTP: backfills, header fills and
/// every other query stay off ws (hosted providers load-balance http and
/// recommend it for archival work, while ws pins everything to one
/// connection and can bill higher). The client is hand-built (instead of
/// `ClientBuilder::http`) so the connector is wrapped with the
/// connect-timing layer: DNS+TCP+TLS setup gets its own `connect` span in
/// --stats, and the first RPC on a fresh connection stops looking
/// mysteriously slow. The resolved adaptor rides along: it re-seeds the
/// shared endpoint budget with the provider's known limits and classifies
/// fetch errors for the adaptive executor.
fn build_provider(
    resolved: &ResolvedRpc,
    chain_id: ChainId,
    cache: Arc<LogCache>,
) -> Result<ChainProvider<impl Provider<Ethereum> + Clone + 'static>, reqwest::Error> {
    let http_client = {
        // `build()` eagerly loads and PEM-parses the system TLS root
        // store — several ms paid even when the whole run is served from
        // cache.
        let _span = setup_span!("client_build").entered();
        reqwest::Client::builder()
            .connector_layer(ConnectTimingLayer)
            .build()?
    };
    let http = HttpTransport::new(http_client, resolved.http.clone());
    let is_local = http.guess_local();
    // Rpc timing goes outermost (before retry) so one `rpc` span covers
    // each logical request end-to-end, retries and backoff included.
    let client = ClientBuilder::default()
        .layer(RpcTimingLayer)
        .layer(rpc_retry_layer())
        .transport(http, is_local);
    // The chain id is already resolved (and typed), so the EVM layer reads
    // it for free instead of re-fetching eth_chainId.
    Ok(ChainProvider::new(
        ProviderBuilder::new().connect_client(client),
        chain_id,
        cache,
    )
    .with_adaptor(resolved.adaptor))
}

/// The subscription upgrade: with `--follow` (unless `--no-ws` opts out)
/// and a resolved ws sibling, open a dedicated pubsub connection that
/// carries the `eth_subscribe` traffic — and nothing else (see
/// [`SubscribeTransport::Ws`]). A connect failure falls back to HTTP
/// polling, so `--follow` never regresses to an error.
async fn subscription_transport(
    follow: FollowMode,
    no_ws: bool,
    ws_url: Option<&Url>,
) -> SubscribeTransport {
    let ws_url = match ws_url {
        Some(url) if follow.is_follow() && !no_ws => url,
        _ => return SubscribeTransport::poll_default(),
    };
    match ClientBuilder::default()
        .layer(RpcTimingLayer)
        .layer(rpc_retry_layer())
        .ws(WsConnect::new(ws_url.as_str()))
        .await
    {
        Ok(ws_client) => {
            tracing::info!(
                endpoint = %redact_url(ws_url),
                "connected ws endpoint for subscriptions"
            );
            SubscribeTransport::Ws {
                subscriber: RootProvider::new(ws_client),
            }
        }
        Err(e) => {
            tracing::warn!(
                endpoint = %redact_url(ws_url),
                error = %e,
                "ws connect failed; --follow falls back to HTTP polling"
            );
            SubscribeTransport::poll_default()
        }
    }
}

/// Cross-check the RPC's reported chain id against the requested one
/// (one `eth_chainId` call), unless the config says to trust it.
async fn verify_chain_id<P>(
    config: &Config,
    provider: &ChainProvider<P>,
    chain_id: ChainId,
) -> Result<(), TransportError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
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
    Ok(())
}

pub async fn run(
    config: &Config,
    db: Option<&Path>,
    args: &EvmArgs,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    let (chain_id, mut resolver) = resolve_chain(config, args).await?;
    let resolved = resolve_endpoint(args, config, chain_id, &mut resolver).await?;

    // The durable cache opens before the provider — the ChainProvider
    // bundles the log cache (header singleflight lives behind it).
    let cache = open_cache(db)?;
    let provider = build_provider(&resolved, chain_id, Arc::new(cache.logs()))?;
    let transport =
        subscription_transport(args.follow_mode(), args.no_ws, resolved.ws.as_ref()).await;
    verify_chain_id(config, &provider, chain_id).await?;

    // One chain entity per invocation. Construction is free (no RPC, no
    // tasks); head tracking starts lazily on the first demand for a tip.
    let chain = EvmChain::new(
        provider,
        EvmChainConfig {
            chunk_size: ChunkSize::new(args.chunk_size),
            transport,
            ..EvmChainConfig::default()
        },
    );

    match &args.command {
        EvmCommand::Blocks(blocks_args) => blocks::run(&chain, args, blocks_args, out).await,
        EvmCommand::Erc20(erc20_args) => erc20::run(&chain, &cache, args, erc20_args, out).await,
        EvmCommand::Events(events_args) => {
            events::run(&chain, &cache, config, args, events_args, out).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProviderConfig, ProviderKeyEntry};

    /// A `Config` whose `[provider]` table carries the given raw keys.
    fn config_with(alchemy: Option<&str>, infura: Option<&str>) -> Config {
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

    fn resolved(resolution: RpcResolution) -> ResolvedRpc {
        match resolution {
            RpcResolution::Resolved(r) => r,
            RpcResolution::NeedPublicFallback { tried } => {
                panic!("expected a resolution, fell through (tried {tried:?})")
            }
        }
    }

    const MAINNET: ChainId = ChainId::new(1);

    /// `--rpc-url` always wins, and a recognized provider URL behaves like
    /// autoconfig: adaptor adopted, ws sibling derived.
    #[test]
    fn rpc_url_wins_and_adopts_detected_adaptor() {
        let url: Url = "https://eth-mainnet.g.alchemy.com/v2/k3y".parse().unwrap();
        let r = resolved(resolve_rpc(
            Some(&url),
            &config_with(Some("cfg-key"), None),
            MAINNET,
        ));
        assert_eq!(r.source, RpcSource::CliFlag);
        assert_eq!(r.http, url);
        assert_eq!(
            r.ws.as_ref().map(Url::as_str),
            Some("wss://eth-mainnet.g.alchemy.com/v2/k3y"),
            "a pasted provider URL derives its ws sibling"
        );
    }

    /// An unrecognized `--rpc-url` still wins, on the generic adaptor,
    /// with no ws sibling to offer.
    #[test]
    fn rpc_url_unknown_shape_is_generic_without_ws() {
        let url: Url = "http://localhost:8545/".parse().unwrap();
        let r = resolved(resolve_rpc(
            Some(&url),
            &config_with(Some("cfg-key"), None),
            MAINNET,
        ));
        assert_eq!(r.source, RpcSource::CliFlag);
        assert_eq!(r.http, url);
        assert!(r.ws.is_none());
    }

    /// Configured keys resolve in `ProviderKind::ALL` order and carry
    /// both endpoints; a provider without a key falls through.
    #[test]
    fn configured_keys_resolve_in_all_order() {
        let both = resolved(resolve_rpc(
            None,
            &config_with(Some("alch"), Some("inf")),
            MAINNET,
        ));
        assert_eq!(
            both.source,
            RpcSource::ConfiguredProvider(ProviderKind::Alchemy)
        );
        assert_eq!(
            both.http.as_str(),
            "https://eth-mainnet.g.alchemy.com/v2/alch"
        );
        assert_eq!(
            both.ws.as_ref().map(Url::as_str),
            Some("wss://eth-mainnet.g.alchemy.com/v2/alch")
        );

        let infura_only = resolved(resolve_rpc(None, &config_with(None, Some("inf")), MAINNET));
        assert_eq!(
            infura_only.source,
            RpcSource::ConfiguredProvider(ProviderKind::Infura)
        );
        assert_eq!(
            infura_only.http.as_str(),
            "https://mainnet.infura.io/v3/inf"
        );
    }

    /// The first configured provider *hosting the chain* wins — one with
    /// a key but no hosting is skipped, not an error.
    #[test]
    fn configured_key_without_hosting_is_skipped() {
        // Chain 324 (zksync era) is in the Infura table but not the
        // Alchemy one, so the alchemy key is consulted and skipped.
        let zksync = ChainId::new(324);
        let r = resolved(resolve_rpc(
            None,
            &config_with(Some("alch"), Some("inf")),
            zksync,
        ));
        assert_eq!(
            r.source,
            RpcSource::ConfiguredProvider(ProviderKind::Infura)
        );
        assert!(
            r.http.as_str().contains("infura.io"),
            "the hosting provider wins: {}",
            r.http
        );
    }

    /// The ws upgrade's gates: no `--follow`, `--no-ws`, or no resolved ws
    /// sibling each mean HTTP polling.
    #[tokio::test]
    async fn subscription_transport_gates_before_connecting() {
        let url: Url = "ws://127.0.0.1:1/".parse().unwrap();
        for (follow, no_ws, ws) in [
            (FollowMode::Once, false, Some(&url)),
            (FollowMode::Follow, true, Some(&url)),
            (FollowMode::Follow, false, None),
        ] {
            let transport = subscription_transport(follow, no_ws, ws).await;
            assert!(
                matches!(transport, SubscribeTransport::Poll { .. }),
                "gate ({follow:?}, no_ws={no_ws}, ws={}) must poll",
                ws.is_some()
            );
        }
    }

    /// A ws sibling that refuses the connection degrades to HTTP polling —
    /// `--follow` never regresses to an error.
    #[tokio::test]
    async fn subscription_transport_falls_back_on_connect_failure() {
        use tokio::net::TcpListener;

        // Bind-then-drop reserves a port nothing listens on, so the ws
        // connect fails at the TCP dial.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let url: Url = format!("ws://{addr}/").parse().unwrap();
        let transport = subscription_transport(FollowMode::Follow, false, Some(&url)).await;
        assert!(
            matches!(transport, SubscribeTransport::Poll { .. }),
            "a failed ws connect must fall back to polling"
        );
    }

    /// A chain no source can serve falls through with every consulted
    /// source in `tried` — the `NoRpcAvailable` report.
    #[test]
    fn exhausted_sources_fall_through_with_tried() {
        let unknown_chain = ChainId::new(99_999);
        let resolution = resolve_rpc(None, &config_with(Some("a"), Some("i")), unknown_chain);
        let RpcResolution::NeedPublicFallback { tried } = resolution else {
            panic!("no provider hosts chain 99999");
        };
        assert_eq!(
            tried,
            vec![
                RpcSource::CliFlag,
                RpcSource::ConfiguredProvider(ProviderKind::Alchemy),
                RpcSource::ConfiguredProvider(ProviderKind::Infura),
            ]
        );
    }
}
