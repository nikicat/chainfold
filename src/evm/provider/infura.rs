//! Infura / `MetaMask` Developer (`*.infura.io`).
//!
//! `eth_getLogs` policy (docs.infura.io): any range as long as ≤10k
//! results and the query completes in time. The tricky part is that
//! Infura reuses code `-32005` for **both** the result-window rejection and
//! request-rate throttling — the payloads disambiguate: a result-window
//! error carries `data.from`/`data.to` (a suggested window), a rate-limit
//! error carries `data.backoff_seconds`.

use std::time::Duration;

use alloy::transports::{RpcError, TransportError};
use reqwest::Url;

use super::{
    ApiKey, Concurrency, ErrorClass, FetchSeeds, LogCount, ProviderAdaptor, RpcTransport,
    classify_http_status, parse_bracketed_range, parse_hex_block,
};
use crate::chain::{BlockCount, BlockRange};
use crate::evm::types::ChainId;

// The vendored chain-id → network-subdomain table (`NETWORK_SUBDOMAINS`).
// The canonical list is the docs' raw-markdown export
// <https://docs.infura.io/get-started/endpoints.md> (`MetaMask`'s own
// `eth-json-rpc-infura` network list is stale; don't use it) — but it
// carries no chain ids, so the generator pairs each subdomain by a keyed
// `eth_chainId` probe (needs `INFURA_KEY`). Refresh via
// `just refresh-provider-networks`; the `#[ignore]` drift test guards
// staleness, and a wrong pairing also fails loudly at the CLI's chain-id
// verify.
include!("data/infura_networks.rs");

/// `None` means Infura doesn't host that chain (as far as the vendored
/// snapshot knows). Raw `u64` ids at the table — the one place `ChainId`
/// unwraps.
fn network_subdomain(chain: ChainId) -> Option<&'static str> {
    NETWORK_SUBDOMAINS
        .iter()
        .find(|(id, _)| *id == chain.get())
        .map(|(_, net)| *net)
}

/// The disambiguating payload of a `-32005` error. Both faces carry
/// `data`; the fields tell them apart. Fixtures:
/// `infura_32005_result_window.json`, `infura_32005_rate_limit.json`.
#[derive(serde::Deserialize, Default)]
struct Infura32005Data {
    backoff_seconds: Option<f64>,
    from: Option<String>,
    to: Option<String>,
}

#[derive(Debug)]
pub(super) struct InfuraAdaptor;

impl ProviderAdaptor for InfuraAdaptor {
    fn classify(&self, error: &TransportError) -> ErrorClass {
        if let Some(class) = classify_http_status(error) {
            return class;
        }
        let RpcError::ErrorResp(payload) = error else {
            return ErrorClass::Transient;
        };
        if payload.code != -32005 {
            return ErrorClass::Transient;
        }

        let data: Infura32005Data = payload
            .data
            .as_ref()
            .and_then(|raw| serde_json::from_str(raw.get()).ok())
            .unwrap_or_default();

        if let Some(backoff) = data.backoff_seconds {
            return ErrorClass::RateLimited {
                retry_after: Some(Duration::from_secs_f64(backoff)),
            };
        }
        // A result-window rejection: prefer the structured hint, fall back
        // to the bracketed range embedded in the message.
        let suggested = match (
            data.from.as_deref().and_then(parse_hex_block),
            data.to.as_deref().and_then(parse_hex_block),
        ) {
            (Some(from), Some(to)) => Some(BlockRange::new(from, to)),
            _ => parse_bracketed_range(&payload.message),
        };
        if suggested.is_some() || payload.message.contains("results") {
            ErrorClass::RangeTooLarge { suggested }
        } else {
            // A -32005 with neither hint nor a recognizable message —
            // most likely a throttle without a stated backoff.
            ErrorClass::RateLimited { retry_after: None }
        }
    }

    fn seeds(&self, _chain: ChainId) -> FetchSeeds {
        FetchSeeds {
            max_range: Some(BlockCount::new(10_000)),
            max_logs: Some(LogCount::new(10_000)),
            concurrency: Concurrency::new(8),
        }
    }

    fn detect(&self, url: &Url) -> Option<ApiKey> {
        let host = url.host_str()?;
        if host != "infura.io" && !host.ends_with(".infura.io") {
            return None;
        }
        let path = url.path();
        let key = path
            .strip_prefix("/ws/v3/")
            .or_else(|| path.strip_prefix("/v3/"))?;
        ApiKey::new(key)
    }

    fn endpoint(&self, chain: ChainId, key: &ApiKey, kind: RpcTransport) -> Option<Url> {
        let net = network_subdomain(chain)?;
        let key = key.expose();
        match kind {
            RpcTransport::Http => format!("https://{net}.infura.io/v3/{key}"),
            RpcTransport::Ws => format!("wss://{net}.infura.io/ws/v3/{key}"),
        }
        .parse()
        .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::super::error_from_fixture;
    use super::*;

    const RESULT_WINDOW: &str = include_str!("fixtures/infura_32005_result_window.json");
    const RATE_LIMIT: &str = include_str!("fixtures/infura_32005_rate_limit.json");

    /// The dual-use `-32005`: the payload, not the code, decides the class.
    #[test]
    fn classify_32005_result_window_vs_rate_limit() {
        let window = InfuraAdaptor.classify(&error_from_fixture(RESULT_WINDOW));
        assert_eq!(
            window,
            ErrorClass::RangeTooLarge {
                suggested: Some(BlockRange::from_u64(0xCB3D, 0x0007_B737)),
            }
        );

        let throttled = InfuraAdaptor.classify(&error_from_fixture(RATE_LIMIT));
        assert_eq!(
            throttled,
            ErrorClass::RateLimited {
                retry_after: Some(Duration::from_secs_f64(30.0)),
            }
        );
    }

    #[test]
    fn classify_32005_without_data_falls_back_to_message() {
        let bare = error_from_fixture(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,
                "message":"query returned more than 10000 results. Try with this block range [0x10, 0x20]."}}"#,
        );
        assert_eq!(
            InfuraAdaptor.classify(&bare),
            ErrorClass::RangeTooLarge {
                suggested: Some(BlockRange::from_u64(0x10, 0x20)),
            }
        );

        let hintless = error_from_fixture(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"project ID request rate exceeded"}}"#,
        );
        assert_eq!(
            InfuraAdaptor.classify(&hintless),
            ErrorClass::RateLimited { retry_after: None }
        );
    }

    /// Live pairing guard, the strongest drift source — the endpoint
    /// itself: every snapshot row's https endpoint must report the chain
    /// id it is filed under. Needs `INFURA_KEY` in the env or the
    /// gitignored `.env`. A failure means the snapshot drifted — or the
    /// key has that network disabled in the Infura dashboard.
    #[tokio::test]
    #[ignore = "network + INFURA_KEY: probes live provider endpoints"]
    async fn network_pairing_matches_live_endpoints() {
        use super::super::{live_chain_id, probe_key};
        let key = probe_key("INFURA_KEY").expect("set INFURA_KEY in .env to run this probe");
        for (id, net) in NETWORK_SUBDOMAINS {
            let url = InfuraAdaptor
                .endpoint(ChainId::new(*id), &key, RpcTransport::Http)
                .expect("snapshot rows always template");
            match live_chain_id(&url).await {
                Ok(got) => assert_eq!(got, *id, "'{net}' answered the wrong chain"),
                Err(reply) => panic!(
                    "chain {id} via '{net}': {reply} — snapshot drift, or \
                     the key lacks this network"
                ),
            }
        }
    }

    /// The docs' raw-markdown endpoints export (the docs ship an
    /// LLM-friendly `.md` view per page — stabler than scraping the HTML).
    const ENDPOINTS_DOCS: &str = "https://docs.infura.io/get-started/endpoints.md";

    /// Drift guard for the subdomain snapshot: every subdomain must appear
    /// in the docs' endpoints export. Complements the live probe above,
    /// which needs a key.
    #[tokio::test]
    #[ignore = "network: fetches the Infura endpoints docs page"]
    async fn network_snapshot_matches_endpoints_docs() {
        let page = super::super::fetch_text(ENDPOINTS_DOCS).await;
        let upstream = infura_hosts(&page);
        assert!(
            upstream.len() > 20,
            "endpoints-page parse found too few subdomains — did the .md \
             export move? parsed: {upstream:?}"
        );
        for (id, net) in NETWORK_SUBDOMAINS {
            assert!(
                upstream.contains(net),
                "subdomain '{net}' (chain {id}) is not on the endpoints \
                 page — the snapshot drifted"
            );
        }
    }

    /// Every `<sub>.infura.io` host mentioned in `page`.
    fn infura_hosts(page: &str) -> std::collections::BTreeSet<&str> {
        const SUFFIX: &str = ".infura.io";
        let mut hosts = std::collections::BTreeSet::new();
        for (at, _) in page.match_indices(SUFFIX) {
            let head = &page[..at];
            let start = head
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
                .map_or(0, |p| p + 1);
            if start < at {
                hosts.insert(&page[start..at]);
            }
        }
        hosts
    }

    /// Regenerate `data/infura_networks.rs`: every subdomain from the
    /// docs' endpoints export, paired by a keyed `eth_chainId` probe
    /// (non-EVM subdomains — solana, starknet, ipfs — fail the probe and
    /// are skipped with a note). Without `INFURA_KEY` nothing can be
    /// paired and the current table is preserved as-is. Run via
    /// `just refresh-provider-networks`; review and commit the diff.
    #[tokio::test]
    #[ignore = "generator: rewrites data/infura_networks.rs from the upstreams"]
    async fn regenerate_networks_table() {
        use std::collections::BTreeMap;

        use super::super::{fetch_text, live_chain_id, probe_key, write_networks_file};

        let mut table: BTreeMap<u64, String> = BTreeMap::new();
        if let Some(key) = probe_key("INFURA_KEY") {
            let page = fetch_text(ENDPOINTS_DOCS).await;
            for sub in infura_hosts(&page) {
                let url: Url = format!("https://{sub}.infura.io/v3/{}", key.expose())
                    .parse()
                    .expect("subdomain templates parse");
                match live_chain_id(&url).await {
                    Ok(id) => {
                        table.insert(id, sub.to_owned());
                    }
                    Err(reply) => eprintln!("skipping subdomain '{sub}': {reply}"),
                }
            }
        } else {
            eprintln!("no INFURA_KEY — subdomains cannot be paired; preserving the current table");
        }
        // Preserve rows this run couldn't re-derive.
        for (id, net) in NETWORK_SUBDOMAINS {
            table.entry(*id).or_insert_with(|| (*net).to_owned());
        }

        write_networks_file(
            "infura_networks.rs",
            "NETWORK_SUBDOMAINS",
            "docs.infura.io/get-started/endpoints.md subdomains, paired by\n\
             // keyed eth_chainId probes (needs INFURA_KEY).",
            &table,
        );
    }

    #[test]
    fn detect_endpoint_round_trip_http_and_ws() {
        let key = ApiKey::new("k3y").unwrap();
        let mainnet = ChainId::new(1);
        for kind in [RpcTransport::Http, RpcTransport::Ws] {
            let url = InfuraAdaptor.endpoint(mainnet, &key, kind).unwrap();
            assert_eq!(InfuraAdaptor.detect(&url), Some(key.clone()));
        }
        assert_eq!(
            InfuraAdaptor
                .endpoint(mainnet, &key, RpcTransport::Ws)
                .unwrap()
                .as_str(),
            "wss://mainnet.infura.io/ws/v3/k3y"
        );
        assert!(
            InfuraAdaptor
                .endpoint(ChainId::new(4_242), &key, RpcTransport::Http)
                .is_none()
        );
        let foreign: Url = "https://mainnet.infura.io.example.org/v3/k3y"
            .parse()
            .unwrap();
        assert_eq!(InfuraAdaptor.detect(&foreign), None);
    }
}
