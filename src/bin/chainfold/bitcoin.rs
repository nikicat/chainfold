//! `bitcoin` — the Bitcoin command subtree. Mirrors `evm`'s wiring, minus
//! the chainlist machinery (there is exactly one Bitcoin network per
//! endpoint): resolve the bitcoind URL + auth, open the shared cache file,
//! build the traced JSON-RPC client, and dispatch.

pub mod blocks;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chainfold::bitcoin::{
    BitcoinChain, BitcoinChainConfig, BtcAuth, BtcRpcClient, ReqwestTransport, TracingTransport,
};
use chainfold::cache::CacheDb;
use chainfold::chain::ChunkSize;
use chainfold::observe::ConnectTimingLayer;
use url::Host;

use crate::cli::{BitcoinArgs, BitcoinCommand};
use crate::config::Config;
use crate::table::Out;
use crate::util::{resolved_db_path, setup_span};

/// Fallback endpoint when neither `--rpc-url` nor the config provides one
/// (the built-in default config does — this is the belt-and-suspenders).
const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8332";

pub async fn run(
    config: &Config,
    db: Option<&Path>,
    args: &BitcoinArgs,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    // Endpoint: --rpc-url > [bitcoin] rpc_url > the local-node default.
    let url: reqwest::Url = args
        .rpc_url
        .as_deref()
        .or_else(|| config.bitcoin().and_then(|b| b.rpc_url.as_deref()))
        .filter(|u| !u.is_empty())
        .unwrap_or(DEFAULT_RPC_URL)
        .parse()?;

    let auth = resolve_auth(args, config, &url);

    let db_path = resolved_db_path(db);
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
        BitcoinCommand::Blocks(blocks_args) => blocks::run(&chain, args, blocks_args, out).await,
    }
}

/// Funnel the auth flags/config into one [`BtcAuth`], once. Precedence:
/// flag user/pass > flag cookie > config user/pass > config cookie >
/// `~/.bitcoin/.cookie` when present and `url` is loopback > none.
fn resolve_auth(args: &BitcoinArgs, config: &Config, url: &reqwest::Url) -> BtcAuth {
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
    implicit_cookie(url, dirs::home_dir().as_deref()).map_or(BtcAuth::None, BtcAuth::Cookie)
}

/// The implicit cookie fallback: `<home>/.bitcoin/.cookie`, honored only
/// when `url` points at the local machine. An *explicit* cookie flag/config
/// works for any endpoint; the implicit one must never reach a remote host —
/// it would leak the local node's secret off-machine, and hosted providers
/// (e.g. Alchemy) reject any request carrying an unexpected `Authorization`
/// header even when the URL itself is properly keyed.
fn implicit_cookie(url: &reqwest::Url, home: Option<&Path>) -> Option<PathBuf> {
    if !url_is_loopback(url) {
        return None;
    }
    let cookie = home?.join(".bitcoin").join(".cookie");
    cookie.exists().then_some(cookie)
}

/// Whether `url`'s host is the local machine: `localhost`, an IPv4 loopback
/// (127.0.0.0/8), or IPv6 `::1`.
fn url_is_loopback(url: &reqwest::Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain == "localhost",
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> reqwest::Url {
        s.parse().unwrap()
    }

    /// A home dir whose `.bitcoin/.cookie` exists — the machine state that
    /// used to leak credentials to remote endpoints.
    fn home_with_cookie() -> tempfile::TempDir {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".bitcoin");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join(".cookie"), "__cookie__:s3cret").unwrap();
        home
    }

    /// The footgun this gate exists for: a remote endpoint must never
    /// inherit the local node's cookie (Alchemy answers "Must be
    /// authenticated!" to any request carrying a stray Authorization
    /// header, even with a valid key in the URL path).
    #[test]
    fn remote_endpoint_never_inherits_home_cookie() {
        let home = home_with_cookie();
        for endpoint in [
            "https://bitcoin-mainnet.g.alchemy.com/v2/some-key",
            "http://192.168.1.10:8332",
        ] {
            assert_eq!(
                implicit_cookie(&url(endpoint), Some(home.path())),
                None,
                "{endpoint}"
            );
        }
    }

    #[test]
    fn loopback_endpoints_pick_up_home_cookie() {
        let home = home_with_cookie();
        let expected = home.path().join(".bitcoin").join(".cookie");
        for endpoint in [
            "http://127.0.0.1:8332",
            "http://127.1.2.3:8332",
            "http://localhost:8332",
            "http://[::1]:8332",
        ] {
            assert_eq!(
                implicit_cookie(&url(endpoint), Some(home.path())),
                Some(expected.clone()),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn missing_cookie_file_resolves_to_no_auth() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(
            implicit_cookie(&url("http://127.0.0.1:8332"), Some(home.path())),
            None
        );
    }
}
