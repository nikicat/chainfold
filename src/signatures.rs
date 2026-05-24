//! Event signature resolver: `(contract address, event name) → (canonical
//! signature, topic0)`.
//!
//! Standalone, source-agnostic. Knows nothing about the rest of chainfold:
//! the caller supplies a [`SignatureCache`] implementation and (optionally)
//! an Etherscan API key; the resolver does HTTP I/O against Sourcify and
//! Etherscan, parses the contract ABI, and returns the canonical event
//! signature plus its keccak256 hash (topic0).
//!
//! Lookup order:
//! 1. Cache hit → return immediately.
//! 2. Sourcify (`https://sourcify.dev/server/files/any/<chain>/<address>`),
//!    no API key required, only verified contracts uploaded to Sourcify.
//! 3. Etherscan v2 (`https://api.etherscan.io/v2/api?chainid=...&module=contract
//!    &action=getabi`), needs an API key but covers far more contracts.
//!
//! On success, the result is written back through the cache so subsequent
//! lookups are zero-network.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tiny_keccak::{Hasher, Keccak};

const SOURCIFY_TIMEOUT: Duration = Duration::from_secs(20);
const ETHERSCAN_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, thiserror::Error)]
pub enum SignatureError {
    #[error("event '{0}' not found in contract ABI")]
    EventNotFound(String),
    #[error("contract ABI not available (sourcify: {sourcify}; etherscan: {etherscan})")]
    AbiUnavailable { sourcify: String, etherscan: String },
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cache: {0}")]
    Cache(String),
    #[error("decoding ABI: {0}")]
    Decode(String),
}

/// A resolved event entry: the canonical signature, topic0 hash, and (when
/// available) the JSON of the matched ABI event item. The ABI JSON is what
/// callers need to decode logs — it carries per-input names, types, and the
/// indexed flag.
#[derive(Debug, Clone)]
pub struct ResolvedEvent {
    pub signature: String,
    pub topic0: [u8; 32],
    /// JSON of the matched event item. Missing for entries cached before
    /// the column was added; present for every entry written by
    /// [`resolve_event`].
    pub abi_json: Option<String>,
}

/// Cache surface used by [`resolve_event`].
#[async_trait]
pub trait SignatureCache: Send + Sync {
    async fn get(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
    ) -> Result<Option<ResolvedEvent>, SignatureError>;

    async fn put(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
        entry: &ResolvedEvent,
    ) -> Result<(), SignatureError>;
}

/// Resolve `(chain_id, contract address, event_name) → ResolvedEvent`.
///
/// Consults `cache` first; on miss (or on a cache hit missing `abi_json`),
/// fetches the contract ABI from Sourcify (always tried) and falls back to
/// Etherscan (when `etherscan_api_key` is `Some`). Writes any successful
/// resolution back through `cache`.
pub async fn resolve_event(
    cache: &dyn SignatureCache,
    chain_id: u64,
    address: [u8; 20],
    event_name: &str,
    etherscan_api_key: Option<&str>,
) -> Result<ResolvedEvent, SignatureError> {
    if let Some(hit) = cache.get(chain_id, address, event_name).await? {
        if hit.abi_json.is_some() {
            return Ok(hit);
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let abi_json = fetch_abi_json(&client, chain_id, address, etherscan_api_key).await?;
    let (signature, topic0, item_json) = event_from_abi(&abi_json, event_name)?;
    let entry = ResolvedEvent {
        signature,
        topic0,
        abi_json: Some(item_json),
    };
    cache.put(chain_id, address, event_name, &entry).await?;
    Ok(entry)
}

/// Resolve only the canonical signature + topic0. Convenience wrapper around
/// [`resolve_event`] for callers that don't need decoding.
pub async fn resolve_event_topic(
    cache: &dyn SignatureCache,
    chain_id: u64,
    address: [u8; 20],
    event_name: &str,
    etherscan_api_key: Option<&str>,
) -> Result<(String, [u8; 32]), SignatureError> {
    let r = resolve_event(cache, chain_id, address, event_name, etherscan_api_key).await?;
    Ok((r.signature, r.topic0))
}

/// Compute topic0 from a canonical event signature like
/// `"Transfer(address,address,uint256)"`. No I/O, no cache.
pub fn topic_from_signature(signature: &str) -> [u8; 32] {
    keccak256(signature.as_bytes())
}

// ============================================================
// ABI fetch
// ============================================================

async fn fetch_abi_json(
    client: &reqwest::Client,
    chain_id: u64,
    address: [u8; 20],
    etherscan_key: Option<&str>,
) -> Result<String, SignatureError> {
    let sourcify_err = match fetch_sourcify(client, chain_id, address).await {
        Ok(abi) => return Ok(abi),
        Err(e) => e.to_string(),
    };
    let etherscan_err = match etherscan_key {
        Some(key) => match fetch_etherscan(client, chain_id, address, key).await {
            Ok(abi) => return Ok(abi),
            Err(e) => e.to_string(),
        },
        None => "etherscan API key not configured".to_string(),
    };
    Err(SignatureError::AbiUnavailable {
        sourcify: sourcify_err,
        etherscan: etherscan_err,
    })
}

#[derive(Deserialize)]
struct SourcifyResponse {
    #[serde(default)]
    files: Vec<SourcifyFile>,
}

#[derive(Deserialize)]
struct SourcifyFile {
    name: String,
    content: String,
}

#[derive(Deserialize)]
struct SourcifyMetadata {
    #[serde(default)]
    output: SourcifyOutput,
}

#[derive(Deserialize, Default)]
struct SourcifyOutput {
    #[serde(default)]
    abi: serde_json::Value,
}

async fn fetch_sourcify(
    client: &reqwest::Client,
    chain_id: u64,
    address: [u8; 20],
) -> Result<String, SignatureError> {
    let url = format!(
        "https://sourcify.dev/server/files/any/{}/0x{}",
        chain_id,
        hex::encode(address)
    );
    let resp = client.get(&url).timeout(SOURCIFY_TIMEOUT).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(SignatureError::Decode(format!(
            "sourcify returned {status} for {url}"
        )));
    }
    let body: SourcifyResponse = resp.json().await?;
    let metadata = body
        .files
        .iter()
        .find(|f| f.name == "metadata.json" || f.name.ends_with("/metadata.json"))
        .ok_or_else(|| SignatureError::Decode("sourcify response had no metadata.json".into()))?;
    let parsed: SourcifyMetadata = serde_json::from_str(&metadata.content)
        .map_err(|e| SignatureError::Decode(format!("metadata.json: {e}")))?;
    Ok(parsed.output.abi.to_string())
}

#[derive(Deserialize)]
struct EtherscanResponse {
    status: String,
    message: String,
    result: String,
}

async fn fetch_etherscan(
    client: &reqwest::Client,
    chain_id: u64,
    address: [u8; 20],
    api_key: &str,
) -> Result<String, SignatureError> {
    let url = format!(
        "https://api.etherscan.io/v2/api?chainid={}&module=contract&action=getabi&address=0x{}&apikey={}",
        chain_id,
        hex::encode(address),
        api_key,
    );
    let resp: EtherscanResponse = client
        .get(&url)
        .timeout(ETHERSCAN_TIMEOUT)
        .send()
        .await?
        .json()
        .await?;
    if resp.status != "1" {
        return Err(SignatureError::Decode(format!(
            "etherscan: {} ({})",
            resp.message, resp.result
        )));
    }
    Ok(resp.result)
}

// ============================================================
// ABI → event signature
// ============================================================

#[derive(Deserialize)]
struct AbiItem {
    #[serde(default)]
    name: String,
    #[serde(default)]
    inputs: Vec<AbiParam>,
}

#[derive(Deserialize)]
struct AbiParam {
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    components: Vec<AbiParam>,
}

fn event_from_abi(
    abi_json: &str,
    event_name: &str,
) -> Result<(String, [u8; 32], String), SignatureError> {
    let items: Vec<serde_json::Value> = serde_json::from_str(abi_json)
        .map_err(|e| SignatureError::Decode(format!("ABI JSON: {e}")))?;
    let raw = items
        .into_iter()
        .find(|v| {
            v.get("type").and_then(|t| t.as_str()) == Some("event")
                && v.get("name").and_then(|n| n.as_str()) == Some(event_name)
        })
        .ok_or_else(|| SignatureError::EventNotFound(event_name.into()))?;
    let parsed: AbiItem = serde_json::from_value(raw.clone())
        .map_err(|e| SignatureError::Decode(format!("event item: {e}")))?;
    let signature = canonical_signature(&parsed.name, &parsed.inputs);
    let topic0 = keccak256(signature.as_bytes());
    let item_json = serde_json::to_string(&raw)
        .map_err(|e| SignatureError::Decode(format!("re-serialize event item: {e}")))?;
    Ok((signature, topic0, item_json))
}

#[cfg(test)]
fn event_topic_from_abi(
    abi_json: &str,
    event_name: &str,
) -> Result<(String, [u8; 32]), SignatureError> {
    let (sig, topic, _json) = event_from_abi(abi_json, event_name)?;
    Ok((sig, topic))
}

fn canonical_signature(name: &str, inputs: &[AbiParam]) -> String {
    let types: Vec<String> = inputs.iter().map(canonical_type).collect();
    format!("{}({})", name, types.join(","))
}

fn canonical_type(p: &AbiParam) -> String {
    // Tuples ("tuple", "tuple[]", "tuple[N]") expand their components.
    if let Some(suffix) = p.ty.strip_prefix("tuple") {
        let inner: Vec<String> = p.components.iter().map(canonical_type).collect();
        format!("({}){}", inner.join(","), suffix)
    } else {
        p.ty.clone()
    }
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak::v256();
    h.update(data);
    let mut out = [0u8; 32];
    h.finalize(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_from_known_signatures() {
        let t = topic_from_signature("Transfer(address,address,uint256)");
        assert_eq!(
            hex::encode(t),
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );

        let t = topic_from_signature("Approval(address,address,uint256)");
        assert_eq!(
            hex::encode(t),
            "8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
        );
    }

    #[test]
    fn signature_from_minimal_abi() {
        let abi = r#"[
            {"type":"event","name":"Transfer","inputs":[
                {"type":"address","name":"from","indexed":true},
                {"type":"address","name":"to","indexed":true},
                {"type":"uint256","name":"value","indexed":false}
            ],"anonymous":false}
        ]"#;
        let (sig, topic) = event_topic_from_abi(abi, "Transfer").unwrap();
        assert_eq!(sig, "Transfer(address,address,uint256)");
        assert_eq!(
            hex::encode(topic),
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn tuple_components_are_expanded() {
        let abi = r#"[
            {"type":"event","name":"Bag","inputs":[
                {"type":"tuple","components":[
                    {"type":"address"},
                    {"type":"uint256"}
                ]},
                {"type":"uint8"}
            ]}
        ]"#;
        let (sig, _) = event_topic_from_abi(abi, "Bag").unwrap();
        assert_eq!(sig, "Bag((address,uint256),uint8)");
    }

    #[test]
    fn tuple_array_suffix_preserved() {
        let abi = r#"[
            {"type":"event","name":"Bags","inputs":[
                {"type":"tuple[]","components":[
                    {"type":"address"},
                    {"type":"uint256"}
                ]}
            ]}
        ]"#;
        let (sig, _) = event_topic_from_abi(abi, "Bags").unwrap();
        assert_eq!(sig, "Bags((address,uint256)[])");
    }

    #[test]
    fn missing_event_errors() {
        let abi = r#"[{"type":"event","name":"Other","inputs":[]}]"#;
        let err = event_topic_from_abi(abi, "Transfer").unwrap_err();
        assert!(matches!(err, SignatureError::EventNotFound(_)));
    }

    #[test]
    fn event_from_abi_returns_matched_item_json() {
        // Two events in the ABI — only the matched one should be returned,
        // and its JSON should round-trip parseable.
        let abi = r#"[
            {"type":"function","name":"transfer","inputs":[]},
            {"type":"event","name":"Approval","inputs":[
                {"type":"address","name":"owner","indexed":true},
                {"type":"address","name":"spender","indexed":true},
                {"type":"uint256","name":"value","indexed":false}
            ]},
            {"type":"event","name":"Transfer","inputs":[
                {"type":"address","name":"from","indexed":true},
                {"type":"address","name":"to","indexed":true},
                {"type":"uint256","name":"value","indexed":false}
            ],"anonymous":false}
        ]"#;
        let (sig, _, item_json) = event_from_abi(abi, "Transfer").unwrap();
        assert_eq!(sig, "Transfer(address,address,uint256)");
        let v: serde_json::Value = serde_json::from_str(&item_json).unwrap();
        assert_eq!(v["name"], "Transfer");
        assert_eq!(v["type"], "event");
        // The non-matching event must not bleed through.
        assert_ne!(v["name"], "Approval");
        // Indexed flags must be preserved (decoding depends on this).
        assert_eq!(v["inputs"][0]["indexed"], true);
        assert_eq!(v["inputs"][2]["indexed"], false);
    }
}
