//! `bitcoin` — the Bitcoin command subtree. Mirrors `evm`'s wiring, minus
//! the chainlist machinery (there is exactly one Bitcoin network per
//! endpoint): resolve the bitcoind URL + auth, open the shared cache file,
//! build the traced JSON-RPC client, and dispatch.

pub mod blocks;

use std::sync::Arc;

use chainfold::bitcoin::{
    BitcoinChain, BitcoinChainConfig, BtcAuth, BtcRpcClient, ReqwestTransport, TracingTransport,
};
use chainfold::cache::CacheDb;
use chainfold::chain::ChunkSize;
use chainfold::observe::ConnectTimingLayer;

use crate::cli::{BitcoinArgs, BitcoinCommand, Direction, FollowMode};
use crate::config::Config;
use crate::util::{resolved_db_path, setup_span};

/// Fallback endpoint when neither `--rpc-url` nor the config provides one
/// (the built-in default config does — this is the belt-and-suspenders).
const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8332";

pub async fn run(
    config: &Config,
    args: &BitcoinArgs,
    direction: Direction,
    follow: FollowMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // Endpoint: --rpc-url > [bitcoin] rpc_url > the local-node default.
    let url: reqwest::Url = args
        .rpc_url
        .as_deref()
        .or_else(|| config.bitcoin().and_then(|b| b.rpc_url.as_deref()))
        .filter(|u| !u.is_empty())
        .unwrap_or(DEFAULT_RPC_URL)
        .parse()?;

    let auth = resolve_auth(args, config);

    let db_path = resolved_db_path(args.db.as_deref());
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let cache = CacheDb::open(&db_path)?;

    // Hand-build the HTTP client so the connector is wrapped with the
    // connect-timing layer — DNS+TCP+TLS setup gets its own `connect` span
    // in --stats, exactly like the EVM path.
    let http_client = {
        let _span = setup_span!("client_build").entered();
        reqwest::Client::builder()
            .connector_layer(ConnectTimingLayer)
            .build()?
    };
    let transport = TracingTransport(ReqwestTransport::new(http_client, url, &auth)?);
    let rpc = BtcRpcClient::new(transport);

    // One chain entity per invocation. Construction is free (no RPC, no
    // tasks); head tracking starts lazily on the first demand for a tip.
    let chain = BitcoinChain::new(
        rpc,
        Arc::new(cache.btc_blocks()),
        &BitcoinChainConfig {
            chunk_size: ChunkSize::new(args.chunk_size),
            ..BitcoinChainConfig::default()
        },
    );

    match &args.command {
        BitcoinCommand::Blocks(blocks_args) => {
            blocks::run(&chain, &db_path, args, blocks_args, direction, follow).await
        }
    }
}

/// Funnel the auth flags/config into one [`BtcAuth`], once. Precedence:
/// flag user/pass > flag cookie > config user/pass > config cookie >
/// `~/.bitcoin/.cookie` when present > none.
fn resolve_auth(args: &BitcoinArgs, config: &Config) -> BtcAuth {
    if let (Some(user), Some(pass)) = (&args.rpc_user, &args.rpc_pass) {
        return BtcAuth::UserPass {
            user: user.clone(),
            pass: pass.clone(),
        };
    }
    if let Some(cookie) = &args.rpc_cookie_file {
        return BtcAuth::Cookie(cookie.clone());
    }
    if let Some(bitcoin) = config.bitcoin() {
        if let (Some(user), Some(pass)) = (&bitcoin.rpc_user, &bitcoin.rpc_pass) {
            return BtcAuth::UserPass {
                user: user.clone(),
                pass: pass.clone(),
            };
        }
        if let Some(cookie) = &bitcoin.rpc_cookie_file {
            return BtcAuth::Cookie(cookie.clone());
        }
    }
    if let Some(home) = dirs::home_dir() {
        let cookie = home.join(".bitcoin").join(".cookie");
        if cookie.exists() {
            return BtcAuth::Cookie(cookie);
        }
    }
    BtcAuth::None
}
