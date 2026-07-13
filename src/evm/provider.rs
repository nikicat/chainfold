//! Provider adaptors — per-provider intelligence at the endpoint boundary
//! (`docs/FETCH.md` §4).
//!
//! Hosted RPC providers differ in their `eth_getLogs` limits, their error
//! payloads, and their URL shapes. Each supported provider implements
//! [`ProviderAdaptor`]: a strict error taxonomy for the adaptive fetch
//! executor ([`classify`](ProviderAdaptor::classify)), known limits so the
//! AIMD starts converged ([`seeds`](ProviderAdaptor::seeds)), and the URL
//! intelligence serving autoconfig ([`detect`](ProviderAdaptor::detect) /
//! [`endpoint`](ProviderAdaptor::endpoint)). The [`generic_adaptor`] is the
//! always-available fallback: it classifies protocol-level signals only and
//! degrades unknown providers to bisection discovery, never to failure.
//!
//! Keyed providers form the closed [`ProviderKind`] vocabulary (the
//! `--api-key <provider>:<KEY>` prefix); the generic adaptor is deliberately
//! not a variant — it has no key and no URL shape. API keys travel as the
//! redacting [`ApiKey`] newtype, which has no `Display` at all: the secret
//! leaves only through [`ApiKey::expose`].

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use alloy::transports::{RpcError, TransportError, TransportErrorKind};
use reqwest::Url;

use crate::chain::{BlockCount, BlockNumber, BlockRange};
use crate::evm::transport::redacted_endpoint;
use crate::evm::types::ChainId;

mod alchemy;
mod generic;
mod infura;
mod key;

pub use alchemy::alchemy_supported;
pub use key::{ApiKey, ParseProviderKeyError, ProviderKey, UnknownProvider};

// ============================================================
// The adaptor contract
// ============================================================

/// Per-provider intelligence, consumed by the slicing executor (classify,
/// seeds) and the endpoint resolver (detect, endpoint).
pub trait ProviderAdaptor: fmt::Debug + Send + Sync {
    /// Strict per-provider error taxonomy for the adaptive executor.
    /// Providers match their exact codes/payloads; unknown errors fall
    /// through to [`ErrorClass::Transient`], whose bounded-retry-then-bisect
    /// handling makes misclassification degrade to discovery, not failure.
    fn classify(&self, error: &TransportError) -> ErrorClass;

    /// Known limits for `chain`, so the executor's AIMD starts converged
    /// instead of probing. Data-driven constants — expected to drift with
    /// provider policy; the escape hatch (`--rpc-url` + the generic
    /// adaptor) is always one flag away.
    fn seeds(&self, chain: ChainId) -> FetchSeeds;

    /// Recognize this provider's URL shape and extract the embedded key.
    /// Serves the `--rpc-url` path: a pasted provider URL behaves like
    /// autoconfig (adaptor adopted, sibling endpoints derivable).
    fn detect(&self, url: &Url) -> Option<ApiKey>;

    /// Endpoint template: chain + key → URL for the given transport.
    /// `None` when the provider doesn't host `chain` (or doesn't offer the
    /// transport).
    fn endpoint(&self, chain: ChainId, key: &ApiKey, kind: RpcTransport) -> Option<Url>;
}

/// What a failed slice means for the adaptive executor — the two knobs are
/// never conflated: range errors shrink the *size*, rate limits shrink the
/// *concurrency* (`docs/FETCH.md` §5, decision 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorClass {
    /// The requested range exceeded a provider cap (width- or
    /// content-based). `suggested` carries the provider's own hint when
    /// the payload includes one.
    RangeTooLarge { suggested: Option<BlockRange> },
    /// The endpoint asked to slow down. The size is fine — shrinking it
    /// here would *raise* total request count against a throttling
    /// endpoint.
    RateLimited { retry_after: Option<Duration> },
    /// Worth retrying as-is (bounded), then bisecting — the generic
    /// adaptor's discovery driver.
    Transient,
    /// No retry will help (bad auth, malformed request); end the stream.
    Fatal,
}

/// Known provider limits seeding the executor's adaptive state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchSeeds {
    /// Widest block range known to always be accepted; `None` = unknown
    /// (start at the ceiling and discover).
    pub max_range: Option<BlockCount>,
    /// The provider's result-count cap, when published.
    pub max_logs: Option<LogCount>,
    /// Sane starting request concurrency for the endpoint.
    pub concurrency: Concurrency,
}

/// Which wire protocol an endpoint template targets. Names a URL shape —
/// distinct from [`SubscribeTransport`](crate::evm::SubscribeTransport),
/// which names a head-tracking strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcTransport {
    Http,
    Ws,
}

/// A count of logs in one RPC response — the unit provider result caps are
/// quoted in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogCount(NonZeroU64);

impl LogCount {
    /// # Panics
    /// Panics if `n == 0` — a zero-log cap is meaningless. `const`, so
    /// literal misuse fails at compile time.
    pub const fn new(n: u64) -> Self {
        match NonZeroU64::new(n) {
            Some(v) => Self(v),
            None => panic!("log count must be positive"),
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for LogCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// How many requests may be in flight against one endpoint at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Concurrency(NonZeroUsize);

impl Concurrency {
    /// # Panics
    /// Panics if `n == 0` — zero in-flight requests can make no progress.
    /// `const`, so literal misuse fails at compile time.
    pub const fn new(n: usize) -> Self {
        match NonZeroUsize::new(n) {
            Some(v) => Self(v),
            None => panic!("concurrency must be positive"),
        }
    }

    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl fmt::Display for Concurrency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

// ============================================================
// The registry
// ============================================================

/// The closed vocabulary of keyed providers — the `--api-key` prefix.
/// The generic adaptor is deliberately not a variant (no key, no URL
/// shape); reach it via [`generic_adaptor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Alchemy,
    Infura,
}

impl ProviderKind {
    /// Every keyed provider, in the order configured keys are tried.
    pub const ALL: &[Self] = &[Self::Alchemy, Self::Infura];

    /// The provider's adaptor. Adaptors are stateless unit structs, so a
    /// `&'static` handle travels without `Arc` ceremony.
    pub fn adaptor(self) -> &'static dyn ProviderAdaptor {
        match self {
            Self::Alchemy => &alchemy::AlchemyAdaptor,
            Self::Infura => &infura::InfuraAdaptor,
        }
    }
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Alchemy => "alchemy",
            Self::Infura => "infura",
        })
    }
}

impl FromStr for ProviderKind {
    type Err = UnknownProvider;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| s.eq_ignore_ascii_case(&kind.to_string()))
            .ok_or_else(|| UnknownProvider(s.to_owned()))
    }
}

/// The always-available fallback adaptor: protocol-level classification
/// only, everything else [`ErrorClass::Transient`] (the executor bisects).
pub fn generic_adaptor() -> &'static dyn ProviderAdaptor {
    &generic::GenericAdaptor
}

/// Registry scan for the `--rpc-url` path: which known provider hosts this
/// URL, and what key rides in it.
pub fn detect_provider(url: &Url) -> Option<(ProviderKind, ApiKey)> {
    ProviderKind::ALL
        .iter()
        .copied()
        .find_map(|kind| kind.adaptor().detect(url).map(|key| (kind, key)))
}

/// Adaptor-aware URL redaction for user-facing echoes: a detected key is
/// spliced out of the rendering; an unrecognized URL degrades to the
/// scheme+host+port rule (path/query dropped wholesale, exactly like the
/// transport's `endpoint` span field) so an unknown provider's key can
/// never leak either.
pub fn redact_url(url: &Url) -> String {
    match detect_provider(url) {
        Some((_, key)) => url.as_str().replace(key.expose(), "…"),
        None => redacted_endpoint(url),
    }
}

// ============================================================
// Shared classification helpers
// ============================================================

/// Protocol-level HTTP statuses every provider means the same way. Runs
/// before any provider-specific payload matching.
fn classify_http_status(error: &TransportError) -> Option<ErrorClass> {
    let RpcError::Transport(TransportErrorKind::HttpError(http)) = error else {
        return None;
    };
    match http.status {
        // RFC 6585: slow down. Not provider intelligence — classifying
        // this as Transient would make the executor bisect on a rate
        // limit, exactly what decision 3 forbids.
        429 => Some(ErrorClass::RateLimited { retry_after: None }),
        // Auth failures: no retry or resize will help.
        401 | 403 => Some(ErrorClass::Fatal),
        _ => None,
    }
}

/// Parse the trailing `[0xFROM, 0xTO]` hint providers append to range-cap
/// error messages (Alchemy: "…this block range should work: [0x849000,
/// 0x8492FF]"; Infura embeds the same shape).
fn parse_bracketed_range(message: &str) -> Option<BlockRange> {
    let open = message.rfind('[')?;
    let close = message[open..].find(']')? + open;
    let (from, to) = message[open + 1..close].split_once(',')?;
    Some(BlockRange::new(
        parse_hex_block(from)?,
        parse_hex_block(to)?,
    ))
}

fn parse_hex_block(s: &str) -> Option<BlockNumber> {
    let hex = s.trim().strip_prefix("0x")?;
    u64::from_str_radix(hex, 16).ok().map(BlockNumber::new)
}

// ============================================================
// Test fixtures — recorded provider error payloads
// ============================================================

/// POST an `eth_chainId` at `url` — the probe behind the adaptors'
/// live-pairing drift tests (each snapshot row's endpoint must report the
/// chain id it is filed under; the endpoint itself is the strongest drift
/// source). `Err` carries the endpoint's verbatim reply for diagnosis.
#[cfg(test)]
async fn live_chain_id(url: &Url) -> Result<u64, String> {
    // `without_url` — reqwest errors embed the full URL, key included.
    let response = reqwest::Client::new()
        .post(url.clone())
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []
        }))
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| e.without_url().to_string())?;
    // Non-JSON replies (e.g. Infura's text/plain 401 "project ID does not
    // have access to this network") surface verbatim with their status.
    let reply: serde_json::Value =
        serde_json::from_str(&body).map_err(|_| format!("HTTP {status}: {}", body.trim()))?;
    let hex = reply
        .get("result")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| reply.to_string())?;
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).map_err(|e| e.to_string())
}

/// A live-probe key from the process env or the gitignored `.env`
/// (`dotenvy`, same pattern as the fork tests' `MAINNET_RPC_URL`).
#[cfg(test)]
fn probe_key(var: &str) -> Option<ApiKey> {
    dotenvy::dotenv().ok();
    std::env::var(var).ok().and_then(ApiKey::new)
}

/// Fetch a drift-test/generator upstream as text.
#[cfg(test)]
async fn fetch_text(url: &str) -> String {
    reqwest::get(url)
        .await
        .unwrap_or_else(|e| panic!("fetch {url}: {e}"))
        .text()
        .await
        .unwrap_or_else(|e| panic!("read {url}: {e}"))
}

/// Rewrite a vendored network-table data file (`data/{file}`) — the
/// generators' shared writer. Rows are id-sorted (the `BTreeMap`) for
/// stable diffs; ids are digit-grouped so generated literals stay
/// readable.
#[cfg(test)]
fn write_networks_file(
    file: &str,
    const_name: &str,
    sources_note: &str,
    rows: &std::collections::BTreeMap<u64, String>,
) {
    use std::fmt::Write as _;

    let mut out = String::new();
    writeln!(
        out,
        "// @generated — DO NOT EDIT; refresh via `just refresh-provider-networks`\n\
         // and commit the diff (the drift tests guard staleness).\n\
         // Sources: {sources_note}\n\
         const {const_name}: &[(u64, &str)] = &["
    )
    .expect("writing to a String cannot fail");
    for (id, name) in rows {
        writeln!(out, "    ({}, \"{name}\"),", grouped_digits(*id))
            .expect("writing to a String cannot fail");
    }
    out.push_str("];\n");

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/evm/provider/data")
        .join(file);
    std::fs::write(&path, out).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    eprintln!("wrote {} rows to {}", rows.len(), path.display());
}

/// `11155111` → `"11_155_111"`, so generated literals stay readable (and
/// clippy-clean). Short ids stay plain.
#[cfg(test)]
fn grouped_digits(id: u64) -> String {
    let digits = id.to_string();
    if digits.len() <= 4 {
        return digits;
    }
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push('_');
        }
        out.push(c);
    }
    out
}

/// Build a [`TransportError`] from a recorded JSON-RPC error response, the
/// way the wire layer would surface it. Fixture files are verbatim
/// recordings — never edit them by hand.
#[cfg(test)]
fn error_from_fixture(raw: &str) -> TransportError {
    use alloy::rpc::json_rpc::ErrorPayload;

    #[derive(serde::Deserialize)]
    struct Envelope {
        error: ErrorPayload,
    }
    let envelope: Envelope = serde_json::from_str(raw).expect("fixture must parse");
    RpcError::ErrorResp(envelope.error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bracketed_range_reads_the_provider_hint() {
        assert_eq!(
            parse_bracketed_range("this block range should work: [0x849000, 0x8492FF]"),
            Some(BlockRange::from_u64(0x0084_9000, 0x0084_92FF))
        );
        assert_eq!(parse_bracketed_range("no hint here"), None);
        assert_eq!(parse_bracketed_range("[0x10, not-hex]"), None);
        assert_eq!(parse_bracketed_range("[decimal, 15]"), None);
    }

    #[test]
    fn provider_kind_round_trips_names() {
        for kind in ProviderKind::ALL {
            assert_eq!(kind.to_string().parse::<ProviderKind>().unwrap(), *kind);
        }
        assert_eq!(
            "ALCHEMY".parse::<ProviderKind>().unwrap(),
            ProviderKind::Alchemy
        );
        assert!("quicknode".parse::<ProviderKind>().is_err());
    }

    #[test]
    fn detect_provider_scans_the_registry() {
        let url: Url = "https://eth-mainnet.g.alchemy.com/v2/sekrit"
            .parse()
            .unwrap();
        let (kind, key) = detect_provider(&url).expect("known shape");
        assert_eq!(kind, ProviderKind::Alchemy);
        assert_eq!(key.expose(), "sekrit");
        let foreign: Url = "https://rpc.example.org/v2/sekrit".parse().unwrap();
        assert!(detect_provider(&foreign).is_none());
    }

    #[test]
    fn redact_url_splices_detected_keys() {
        let url: Url = "wss://eth-mainnet.g.alchemy.com/v2/sekrit123"
            .parse()
            .unwrap();
        let redacted = redact_url(&url);
        assert!(!redacted.contains("sekrit123"), "{redacted}");
        assert!(redacted.contains("eth-mainnet.g.alchemy.com"), "{redacted}");

        // Unknown shape: identity only — the whole path is dropped, so an
        // unrecognized provider's key cannot leak either.
        let foreign: Url = "https://rpc.example.org:8545/secret-key?token=s3"
            .parse()
            .unwrap();
        let redacted = redact_url(&foreign);
        assert_eq!(redacted, "https://rpc.example.org:8545");
    }
}
