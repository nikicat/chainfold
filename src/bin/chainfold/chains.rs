//! Chain name resolution via cached chainlist.org/rpcs.json.
//!
//! Maps user-supplied names (e.g. `arbitrum`, `bnb`) to chain IDs by
//! matching against `shortName` or `chainSlug` from chainlist.org.
//! Cache lives at `~/.cache/chainfold/chainlist.json`; refresh = manual `rm`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainlistEntry {
    pub chain_id: u64,
    pub name: String,
    pub short_name: String,
    pub chain: String,
    #[serde(default)]
    pub chain_slug: Option<String>,
    #[serde(default)]
    pub network_id: Option<u64>,
    #[serde(default, rename = "infoURL")]
    pub info_url: Option<String>,
    #[serde(default)]
    pub is_testnet: Option<bool>,
    #[serde(default)]
    pub red_flags: Option<Vec<String>>,
    pub native_currency: NativeCurrency,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NativeCurrency {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETH_ENTRY: &str = r#"{
        "name": "Ethereum Mainnet",
        "chain": "ETH",
        "chainId": 1,
        "shortName": "eth",
        "chainSlug": "ethereum",
        "networkId": 1,
        "infoURL": "https://ethereum.org",
        "isTestnet": false,
        "redFlags": [],
        "nativeCurrency": { "name": "Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    #[test]
    fn parses_ethereum_entry() {
        let entry: ChainlistEntry = serde_json::from_str(ETH_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 1);
        assert_eq!(entry.name, "Ethereum Mainnet");
        assert_eq!(entry.short_name, "eth");
        assert_eq!(entry.chain_slug.as_deref(), Some("ethereum"));
        assert_eq!(entry.chain, "ETH");
        assert_eq!(entry.network_id, Some(1));
        assert_eq!(entry.info_url.as_deref(), Some("https://ethereum.org"));
        assert_eq!(entry.is_testnet, Some(false));
        assert_eq!(entry.red_flags.as_deref(), Some(&[] as &[String]));
        assert_eq!(entry.native_currency.name, "Ether");
        assert_eq!(entry.native_currency.symbol, "ETH");
        assert_eq!(entry.native_currency.decimals, 18);
    }

    const MINIMAL_ENTRY: &str = r#"{
        "name": "OP Mainnet",
        "chain": "ETH",
        "chainId": 10,
        "shortName": "oeth",
        "nativeCurrency": { "name": "Ether", "symbol": "ETH", "decimals": 18 }
    }"#;

    #[test]
    fn parses_entry_without_optional_fields() {
        let entry: ChainlistEntry = serde_json::from_str(MINIMAL_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 10);
        assert!(entry.chain_slug.is_none());
        assert!(entry.network_id.is_none());
        assert!(entry.info_url.is_none());
        assert!(entry.is_testnet.is_none());
        assert!(entry.red_flags.is_none());
    }

    const ENRICHED_ENTRY: &str = r#"{
        "name": "BNB Smart Chain Mainnet",
        "chain": "BSC",
        "chainId": 56,
        "shortName": "bnb",
        "chainSlug": "binance",
        "tvl": 7444698897.0,
        "icon": "bnb",
        "ens": null,
        "status": null,
        "faucets": [],
        "rpc": [{"url": "https://bsc-rpc.example", "tracking": "none"}],
        "explorers": [{"name": "bscscan", "url": "https://bscscan.com"}],
        "nativeCurrency": { "name": "BNB", "symbol": "BNB", "decimals": 18 }
    }"#;

    #[test]
    fn parses_unknown_fields_silently() {
        let entry: ChainlistEntry = serde_json::from_str(ENRICHED_ENTRY).expect("deserialize");
        assert_eq!(entry.chain_id, 56);
        assert_eq!(entry.short_name, "bnb");
        assert_eq!(entry.chain_slug.as_deref(), Some("binance"));
    }
}
