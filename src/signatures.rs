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

/// Cache surface used by [`resolve_event_topic`].
///
/// `get` returns `(canonical_signature, topic0)` when the (chain, address,
/// `event_name`) triple is known; `put` stores a freshly resolved entry.
#[async_trait]
pub trait SignatureCache: Send + Sync {
    async fn get(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
    ) -> Result<Option<(String, [u8; 32])>, SignatureError>;

    async fn put(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
        signature: &str,
        topic0: [u8; 32],
    ) -> Result<(), SignatureError>;
}

/// Resolve `(chain_id, contract address, event_name) → (signature, topic0)`.
///
/// Consults `cache` first; on miss, fetches the contract ABI from Sourcify
/// (always tried) and falls back to Etherscan (when `etherscan_api_key`
/// is `Some`). Writes any successful resolution back through `cache`.
pub async fn resolve_event_topic(
    cache: &dyn SignatureCache,
    chain_id: u64,
    address: [u8; 20],
    event_name: &str,
    etherscan_api_key: Option<&str>,
) -> Result<(String, [u8; 32]), SignatureError> {
    if let Some(hit) = cache.get(chain_id, address, event_name).await? {
        return Ok(hit);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let abi_json = fetch_abi_json(&client, chain_id, address, etherscan_api_key).await?;
    let (sig, topic0) = event_topic_from_abi(&abi_json, event_name)?;
    cache
        .put(chain_id, address, event_name, &sig, topic0)
        .await?;
    Ok((sig, topic0))
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
    #[serde(rename = "type", default)]
    kind: String,
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

fn event_topic_from_abi(
    abi_json: &str,
    event_name: &str,
) -> Result<(String, [u8; 32]), SignatureError> {
    let items: Vec<AbiItem> = serde_json::from_str(abi_json)
        .map_err(|e| SignatureError::Decode(format!("ABI JSON: {e}")))?;
    let event = items
        .iter()
        .find(|i| i.kind == "event" && i.name == event_name)
        .ok_or_else(|| SignatureError::EventNotFound(event_name.into()))?;
    let signature = canonical_signature(&event.name, &event.inputs);
    let topic0 = keccak256(signature.as_bytes());
    Ok((signature, topic0))
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
}
