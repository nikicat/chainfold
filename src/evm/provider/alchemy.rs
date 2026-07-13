//! Alchemy (`*.g.alchemy.com`).
//!
//! `eth_getLogs` policy (docs.alchemy.com): any request up to a 2k-block
//! range, or any range returning ≤10k logs — content-dependent, with the
//! rejection message carrying a suggested `[from, to]` that fits.

use alloy::transports::{RpcError, TransportError};
use reqwest::Url;

use super::{
    ApiKey, Concurrency, ErrorClass, FetchSeeds, LogCount, ProviderAdaptor, RpcTransport,
    classify_http_status, parse_bracketed_range,
};
use crate::chain::BlockCount;
use crate::evm::types::ChainId;

// The vendored chain-id → subdomain-slug table (`CHAIN_SLUGS`). Alchemy
// publishes no queryable networks API; the generator merges the
// auto-generated id→URL registry in
// [aa-sdk](https://github.com/alchemyplatform/aa-sdk)
// (`packages/common/src/transport/chainRegistry.ts` — pairs ids with
// slugs, but only covers account-abstraction chains) with the `Network`
// enum in
// [alchemy-sdk-js](https://github.com/alchemyplatform/alchemy-sdk-js)
// (`src/types/types.ts` — every RPC slug, no ids), pairing enum-only
// slugs by keyed `eth_chainId` probes. Refresh via
// `just refresh-provider-networks`; the `#[ignore]` drift test guards
// staleness, and a wrong pairing also fails loudly at the CLI's chain-id
// verify.
include!("data/alchemy_networks.rs");

/// `None` means Alchemy doesn't host that chain (as far as the vendored
/// snapshot knows). Raw `u64` ids at the table — the one place `ChainId`
/// unwraps.
fn chain_slug(chain: ChainId) -> Option<&'static str> {
    CHAIN_SLUGS
        .iter()
        .find(|(id, _)| *id == chain.get())
        .map(|(_, slug)| *slug)
}

/// Whether Alchemy hosts `chain` — the `chains list --alchemy-supported`
/// filter, kept beside the slug table it mirrors.
pub fn alchemy_supported(chain: ChainId) -> bool {
    chain_slug(chain).is_some()
}

#[derive(Debug)]
pub(super) struct AlchemyAdaptor;

impl ProviderAdaptor for AlchemyAdaptor {
    fn classify(&self, error: &TransportError) -> ErrorClass {
        if let Some(class) = classify_http_status(error) {
            return class;
        }
        let RpcError::ErrorResp(payload) = error else {
            return ErrorClass::Transient;
        };
        match payload.code {
            // Range-cap rejections carry "…this block range should work:
            // [0xFROM, 0xTO]" — as -32600 on the free tier (recorded:
            // alchemy_range_error_free_tier.json, a 10-block cap) and
            // -32602 for the documented 2k-blocks/10k-logs rule
            // (alchemy_range_error.json).
            -32600 | -32602 if payload.message.contains("this block range should work") => {
                ErrorClass::RangeTooLarge {
                    suggested: parse_bracketed_range(&payload.message),
                }
            }
            // "<NETWORK> is not enabled for this app": the dashboard gates
            // the network for this key — no retry or resize will help.
            -32600 if payload.message.contains("is not enabled for this app") => ErrorClass::Fatal,
            // Compute-units throttling arrives as a JSON-RPC error with the
            // HTTP status as its code. Fixture: alchemy_429.json.
            429 => ErrorClass::RateLimited { retry_after: None },
            _ => ErrorClass::Transient,
        }
    }

    fn seeds(&self, _chain: ChainId) -> FetchSeeds {
        FetchSeeds {
            max_range: Some(BlockCount::new(2_000)),
            max_logs: Some(LogCount::new(10_000)),
            concurrency: Concurrency::new(8),
        }
    }

    fn detect(&self, url: &Url) -> Option<ApiKey> {
        let host = url.host_str()?;
        if host != "g.alchemy.com" && !host.ends_with(".g.alchemy.com") {
            return None;
        }
        let key = url.path().strip_prefix("/v2/")?;
        ApiKey::new(key)
    }

    fn endpoint(&self, chain: ChainId, key: &ApiKey, kind: RpcTransport) -> Option<Url> {
        let slug = chain_slug(chain)?;
        let scheme = match kind {
            RpcTransport::Http => "https",
            RpcTransport::Ws => "wss",
        };
        format!("{scheme}://{slug}.g.alchemy.com/v2/{}", key.expose())
            .parse()
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::super::{ProviderKind, detect_provider, error_from_fixture, fetch_text, probe_key};
    use super::*;
    use crate::chain::BlockRange;

    const RANGE_ERROR: &str = include_str!("fixtures/alchemy_range_error.json");
    const RANGE_ERROR_FREE_TIER: &str = include_str!("fixtures/alchemy_range_error_free_tier.json");
    const RATE_LIMIT: &str = include_str!("fixtures/alchemy_429.json");

    /// The auto-generated id→URL registry (account-abstraction chains).
    const CHAIN_REGISTRY_SRC: &str = "https://raw.githubusercontent.com/\
         alchemyplatform/aa-sdk/main/packages/common/src/transport/chainRegistry.ts";
    /// The `Network` enum (every RPC slug, no ids).
    const NETWORK_ENUM_SRC: &str = "https://raw.githubusercontent.com/\
         alchemyplatform/alchemy-sdk-js/master/src/types/types.ts";

    /// Parse registry rows —
    /// `  42161: "https://arb-mainnet.g.alchemy.com/v2", // arbMainnet`.
    fn registry_pairs(source: &str) -> BTreeMap<u64, &str> {
        let pairs: BTreeMap<u64, &str> = source
            .lines()
            .filter_map(|line| {
                let (id, rest) = line.trim().split_once(": \"https://")?;
                Some((id.parse().ok()?, rest.split_once(".g.alchemy.com")?.0))
            })
            .collect();
        assert!(
            pairs.len() > 30,
            "chainRegistry parse found too few entries — did the aa-sdk \
             source format change? parsed: {pairs:?}"
        );
        pairs
    }

    /// Parse `Network`-enum slugs — `ETH_MAINNET = 'eth-mainnet',` — from
    /// that enum's block only (`types.ts` holds several other string
    /// enums: webhook types, trace kinds, …).
    fn enum_slugs(source: &str) -> BTreeSet<&str> {
        let block = source
            .split_once("export enum Network {")
            .map(|(_, rest)| rest.split_once('}').map_or(rest, |(block, _)| block))
            .expect("types.ts must declare `export enum Network`");
        let slugs: BTreeSet<&str> = block
            .lines()
            .filter_map(|line| line.trim().split_once("= '")?.1.strip_suffix("',"))
            .collect();
        assert!(
            slugs.len() > 20,
            "Network-enum parse found too few slugs — did the SDK source \
             format change? parsed: {slugs:?}"
        );
        slugs
    }

    /// The probes' key: `ALCHEMY_KEY`, or detected from the fork tests'
    /// `MAINNET_RPC_URL` via [`detect_provider`].
    fn alchemy_probe_key() -> Option<ApiKey> {
        probe_key("ALCHEMY_KEY").or_else(|| {
            let url: Url = std::env::var("MAINNET_RPC_URL").ok()?.parse().ok()?;
            let (kind, key) = detect_provider(&url)?;
            (kind == ProviderKind::Alchemy).then_some(key)
        })
    }

    #[test]
    fn classify_range_error_extracts_suggested_range() {
        let class = AlchemyAdaptor.classify(&error_from_fixture(RANGE_ERROR));
        assert_eq!(
            class,
            ErrorClass::RangeTooLarge {
                suggested: Some(BlockRange::from_u64(0x0084_9000, 0x0084_92FF)),
            }
        );
    }

    /// The free tier rejects with `-32600` (not the documented `-32602`)
    /// and a different message — caught by recording the real payload; the
    /// distinctive "this block range should work" phrase is what both
    /// shapes share.
    #[test]
    fn classify_free_tier_range_error_extracts_suggested_range() {
        let class = AlchemyAdaptor.classify(&error_from_fixture(RANGE_ERROR_FREE_TIER));
        assert_eq!(
            class,
            ErrorClass::RangeTooLarge {
                suggested: Some(BlockRange::from_u64(0x0100_0000, 0x0100_0009)),
            }
        );
    }

    /// A network the key's dashboard config gates is fatal — no retry or
    /// resize helps. (Inline payload, shape observed live; not a fixture
    /// because the real reply embeds the app's dashboard URL.)
    #[test]
    fn classify_network_not_enabled_is_fatal() {
        let not_enabled = error_from_fixture(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,
                "message":"OPT_MAINNET is not enabled for this app. Visit this page to enable the network: https://dashboard.alchemy.com/apps/…/networks"}}"#,
        );
        assert_eq!(AlchemyAdaptor.classify(&not_enabled), ErrorClass::Fatal);
    }

    #[test]
    fn classify_429_is_rate_limited() {
        let class = AlchemyAdaptor.classify(&error_from_fixture(RATE_LIMIT));
        assert_eq!(class, ErrorClass::RateLimited { retry_after: None });
    }

    #[test]
    fn detect_endpoint_round_trip_http_and_ws() {
        let key = ApiKey::new("k3y").unwrap();
        let mainnet = ChainId::new(1);
        for kind in [RpcTransport::Http, RpcTransport::Ws] {
            let url = AlchemyAdaptor.endpoint(mainnet, &key, kind).unwrap();
            assert_eq!(AlchemyAdaptor.detect(&url), Some(key.clone()));
        }
        assert_eq!(
            AlchemyAdaptor
                .endpoint(mainnet, &key, RpcTransport::Ws)
                .unwrap()
                .as_str(),
            "wss://eth-mainnet.g.alchemy.com/v2/k3y"
        );
        // Unsupported chain → no endpoint.
        assert!(
            AlchemyAdaptor
                .endpoint(ChainId::new(4_242), &key, RpcTransport::Http)
                .is_none()
        );
        // Foreign URLs and lookalike hosts don't detect.
        let foreign: Url = "https://evil-g.alchemy.com.example.org/v2/k3y"
            .parse()
            .unwrap();
        assert_eq!(AlchemyAdaptor.detect(&foreign), None);
        let wrong_path: Url = "https://eth-mainnet.g.alchemy.com/other/k3y"
            .parse()
            .unwrap();
        assert_eq!(AlchemyAdaptor.detect(&wrong_path), None);
    }

    /// Live pairing guard, the strongest drift source — the endpoint
    /// itself: every snapshot row's https endpoint must report the chain
    /// id it is filed under (existence checks like the SDK-enum test below
    /// can't catch a wrong *pairing*). Key: `ALCHEMY_KEY`, or detected
    /// from the fork tests' `MAINNET_RPC_URL` via [`detect_provider`]. A
    /// failure means the snapshot drifted — or the key has that network
    /// disabled in the Alchemy dashboard.
    #[tokio::test]
    #[ignore = "network + Alchemy key: probes live provider endpoints"]
    async fn chain_slug_pairing_matches_live_endpoints() {
        use super::super::live_chain_id;
        let key =
            alchemy_probe_key().expect("set ALCHEMY_KEY (or an Alchemy MAINNET_RPC_URL) in .env");
        for (id, slug) in CHAIN_SLUGS {
            let url = AlchemyAdaptor
                .endpoint(ChainId::new(*id), &key, RpcTransport::Http)
                .expect("snapshot rows always template");
            match live_chain_id(&url).await {
                Ok(got) => assert_eq!(got, *id, "'{slug}' answered the wrong chain"),
                // "…is not enabled for this app" still *confirms* the
                // slug: Alchemy recognized the network and named it —
                // only this key's dashboard config gates it. Full pairing
                // needs a key with the network enabled.
                Err(reply) if reply.contains("is not enabled for this app") => {
                    eprintln!("chain {id} '{slug}': confirmed but not enabled for this key");
                }
                Err(reply) => panic!("chain {id} via '{slug}': {reply} — snapshot drift"),
            }
        }
    }

    /// Drift guard for the slug snapshot, against both Alchemy sources:
    /// the aa-sdk registry pairs chain ids with slugs (`43114: "https://
    /// avax-mainnet…"`-style rows), so snapshot rows it carries are
    /// checked as full id↔slug *pairings*; rows it lacks (it only covers
    /// account-abstraction chains — avax is RPC-only today) fall back to
    /// slug-existence in the alchemy-sdk `Network` enum. Network-gated
    /// like the other `#[ignore]` suites; run on demand to turn silent
    /// drift into an actionable diff.
    #[tokio::test]
    #[ignore = "network: fetches the aa-sdk + alchemy-sdk-js sources"]
    async fn chain_slug_snapshot_matches_alchemy_sources() {
        let registry_src = fetch_text(CHAIN_REGISTRY_SRC).await;
        let registry = registry_pairs(&registry_src);
        let enum_src = fetch_text(NETWORK_ENUM_SRC).await;
        let known_slugs = enum_slugs(&enum_src);

        for (id, slug) in CHAIN_SLUGS {
            match registry.get(id) {
                Some(upstream) => assert_eq!(
                    upstream, slug,
                    "chain {id}: the aa-sdk registry pairs it with a \
                     different slug — the snapshot drifted"
                ),
                None => assert!(
                    known_slugs.contains(slug),
                    "slug '{slug}' (chain {id}) is in neither the aa-sdk \
                     registry nor the alchemy-sdk Network enum — the \
                     snapshot drifted"
                ),
            }
        }
    }

    /// Regenerate `data/alchemy_networks.rs`: every id↔slug pair from the
    /// aa-sdk registry, plus `Network`-enum slugs the registry lacks —
    /// paired by a keyed `eth_chainId` probe when a key is available
    /// (probe rejections are skipped with a note), with rows the sources
    /// couldn't re-derive preserved from the current table. Run via
    /// `just refresh-provider-networks`; review and commit the diff.
    #[tokio::test]
    #[ignore = "generator: rewrites data/alchemy_networks.rs from the upstreams"]
    async fn regenerate_networks_table() {
        use super::super::{live_chain_id, write_networks_file};

        let registry_src = fetch_text(CHAIN_REGISTRY_SRC).await;
        let mut table: BTreeMap<u64, String> = registry_pairs(&registry_src)
            .into_iter()
            .map(|(id, slug)| (id, slug.to_owned()))
            .collect();

        let enum_src = fetch_text(NETWORK_ENUM_SRC).await;
        let key = alchemy_probe_key();
        if key.is_none() {
            eprintln!("no Alchemy key — enum-only slugs stay unpaired; current rows preserved");
        }
        for slug in enum_slugs(&enum_src) {
            if table.values().any(|s| s == slug) {
                continue;
            }
            let Some(key) = &key else { continue };
            let url: Url = format!("https://{slug}.g.alchemy.com/v2/{}", key.expose())
                .parse()
                .expect("slug templates parse");
            match live_chain_id(&url).await {
                Ok(id) => {
                    table.entry(id).or_insert_with(|| slug.to_owned());
                }
                Err(reply) => eprintln!("skipping enum-only slug '{slug}': {reply}"),
            }
        }
        // Preserve rows this run couldn't re-derive (e.g. probed on an
        // earlier run with a differently-enabled key).
        for (id, slug) in CHAIN_SLUGS {
            table.entry(*id).or_insert_with(|| (*slug).to_owned());
        }

        write_networks_file(
            "alchemy_networks.rs",
            "CHAIN_SLUGS",
            "aa-sdk chainRegistry.ts (id↔slug pairs) + the alchemy-sdk\n\
             // Network enum, paired by keyed eth_chainId probes where the registry\n\
             // lacks the chain.",
            &table,
        );
    }
}
