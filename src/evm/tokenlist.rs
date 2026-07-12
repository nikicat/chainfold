//! Token-symbol resolver: `(chain id, symbol) → ERC20 contract address`.
//!
//! Backed by a curated token list in the [Token Lists] JSON format (default:
//! Uniswap Labs' list). Symbols are not unique on-chain — anyone can deploy
//! a "USDC" — so a trusted curated list is the resolution anchor, not any
//! on-chain search.
//!
//! The list is fetched over HTTP and snapshotted into the
//! [`TokenListCache`]; [`resolve_token`] transparently refreshes a missing
//! or stale snapshot (and degrades to a stale one, with a warning, when the
//! fetch fails). Lookups are case-insensitive; among same-chain collisions a
//! unique exact-case match wins, anything else errors with the candidates.
//!
//! [Token Lists]: https://tokenlists.org

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use reqwest::Url;
use serde::Deserialize;

use crate::arch::Timestamp;
use crate::cache::CacheError;
use crate::evm::cache::{TokenListCache, TokenListEntry, TokenListMeta, TokenListVersion};
use crate::evm::types::{ChainId, TokenSymbol};

/// The default list: curated by Uniswap Labs, ~1.5k tokens across the major
/// chains, `chainId`-keyed per entry.
pub const DEFAULT_TOKEN_LIST_URL: &str = "https://tokens.uniswap.org";

/// A cached snapshot older than this is refreshed before resolving. Token
/// addresses essentially never change; a day keeps warm runs zero-network.
const REFRESH_INTERVAL: Duration = Duration::from_hours(24);

const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// [`DEFAULT_TOKEN_LIST_URL`] as a parsed [`Url`].
pub fn default_token_list_url() -> Url {
    DEFAULT_TOKEN_LIST_URL.parse().expect("static URL parses")
}

#[derive(Debug, thiserror::Error)]
pub enum TokenListError {
    #[error(
        "token '{symbol}' not found on chain {chain_id} in the cached token list — pass the \
         0x contract address, or refresh the list with `chainfold tokens refresh`"
    )]
    NotFound {
        symbol: TokenSymbol,
        chain_id: ChainId,
    },
    #[error(
        "token symbol '{symbol}' is ambiguous on chain {chain_id}: {candidates} — pass the \
         0x contract address instead"
    )]
    Ambiguous {
        symbol: TokenSymbol,
        chain_id: ChainId,
        /// The colliding entries, rendered `address (name)`.
        candidates: String,
    },
    #[error("fetching token list from {url}: {source}")]
    Fetch {
        url: Url,
        #[source]
        source: reqwest::Error,
    },
    #[error("token list at {url}: {reason}")]
    Decode { url: Url, reason: String },
    #[error("cache: {0}")]
    Cache(#[from] CacheError),
}

/// What a [`refresh`] stored.
#[derive(Debug, Clone)]
pub struct RefreshOutcome {
    pub meta: TokenListMeta,
    /// Entries stored.
    pub tokens: usize,
}

// ============================================================
// Wire format (the Token Lists JSON schema; unknown fields dropped)
// ============================================================

#[derive(Deserialize)]
struct WireTokenList {
    name: String,
    version: TokenListVersion,
    tokens: Vec<WireToken>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireToken {
    chain_id: u64,
    address: String,
    symbol: String,
    name: String,
    decimals: u8,
}

/// Convert wire entries to cache rows. Entries whose address isn't a
/// 20-byte EVM address (e.g. the Solana mints the Uniswap list carries) are
/// dropped, with a debug trace each.
fn entries_from_wire(tokens: Vec<WireToken>) -> Vec<TokenListEntry> {
    let mut entries = Vec::with_capacity(tokens.len());
    for t in tokens {
        if let Ok(address) = t.address.parse::<Address>() {
            entries.push(TokenListEntry {
                chain_id: ChainId::new(t.chain_id),
                address,
                symbol: TokenSymbol::new(t.symbol),
                name: t.name,
                decimals: t.decimals,
            });
        } else {
            tracing::debug!(
                chain_id = t.chain_id,
                symbol = %t.symbol,
                address = %t.address,
                "skipping non-EVM token-list entry"
            );
        }
    }
    entries
}

// ============================================================
// Refresh
// ============================================================

/// Fetch the list at `url` and replace the cached snapshot with it.
pub async fn refresh(cache: &TokenListCache, url: &Url) -> Result<RefreshOutcome, TokenListError> {
    let fetch_err = |source| TokenListError::Fetch {
        url: url.clone(),
        source,
    };
    let client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()
        .map_err(fetch_err)?;
    let wire: WireTokenList = client
        .get(url.clone())
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(fetch_err)?
        .json()
        .await
        .map_err(fetch_err)?;

    let entries = entries_from_wire(wire.tokens);
    if entries.is_empty() {
        // Caching an all-skipped list would make every later lookup a
        // fresh-looking NotFound — almost certainly a wrong URL; say so now.
        return Err(TokenListError::Decode {
            url: url.clone(),
            reason: "no EVM-addressed tokens in the list".to_owned(),
        });
    }
    let meta = TokenListMeta {
        url: url.clone(),
        name: wire.name,
        version: wire.version,
        fetched_at: now(),
    };
    cache.replace(&entries, &meta)?;
    Ok(RefreshOutcome {
        meta,
        tokens: entries.len(),
    })
}

fn now() -> Timestamp {
    Timestamp::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_secs(),
    )
}

/// Whether the snapshot behind `meta` needs a refresh at time `now`.
fn is_stale(meta: Option<&TokenListMeta>, now: Timestamp) -> bool {
    meta.is_none_or(|m| now.get().saturating_sub(m.fetched_at.get()) >= REFRESH_INTERVAL.as_secs())
}

// ============================================================
// Resolve
// ============================================================

/// Resolve `symbol` on `chain_id` to its token-list entry, refreshing a
/// missing/stale snapshot from `url` first. A refresh failure over an
/// existing (stale) snapshot degrades to that snapshot with a warning; with
/// nothing cached it is fatal.
pub async fn resolve_token(
    cache: &TokenListCache,
    chain_id: ChainId,
    symbol: &TokenSymbol,
    url: &Url,
) -> Result<TokenListEntry, TokenListError> {
    let meta = cache.meta()?;
    if is_stale(meta.as_ref(), now()) {
        match refresh(cache, url).await {
            Ok(outcome) => tracing::debug!(
                tokens = outcome.tokens,
                version = %outcome.meta.version,
                "token list refreshed"
            ),
            Err(e) if meta.is_some() => {
                tracing::warn!("token-list refresh failed; using the stale snapshot: {e}");
            }
            Err(e) => return Err(e),
        }
    }
    pick(cache.find(chain_id, symbol)?, chain_id, symbol)
}

/// Choose among the case-insensitive `hits` for `symbol`: exactly one hit
/// wins; among several, a unique exact-case match wins; anything else is
/// [`TokenListError::Ambiguous`] (empty: [`TokenListError::NotFound`]).
fn pick(
    mut hits: Vec<TokenListEntry>,
    chain_id: ChainId,
    symbol: &TokenSymbol,
) -> Result<TokenListEntry, TokenListError> {
    match hits.len() {
        0 => Err(TokenListError::NotFound {
            symbol: symbol.clone(),
            chain_id,
        }),
        1 => Ok(hits.remove(0)),
        _ => {
            let mut exact = hits
                .iter()
                .enumerate()
                .filter(|(_, e)| e.symbol == *symbol)
                .map(|(i, _)| i);
            if let (Some(i), None) = (exact.next(), exact.next()) {
                return Ok(hits.remove(i));
            }
            let candidates = hits
                .iter()
                .map(|e| format!("{} ({})", e.address, e.name))
                .collect::<Vec<_>>()
                .join(", ");
            Err(TokenListError::Ambiguous {
                symbol: symbol.clone(),
                chain_id,
                candidates,
            })
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // A trimmed-down tokens.uniswap.org response: one mainnet ERC20, one
    // Solana mint (base58 address — must be skipped), extra fields ignored.
    const WIRE_FIXTURE: &str = r#"{
        "name": "Uniswap Labs Default",
        "timestamp": "2025-06-01T00:00:00.000Z",
        "version": { "major": 22, "minor": 5, "patch": 0 },
        "tokens": [
            {
                "chainId": 1,
                "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "name": "USDCoin",
                "symbol": "USDC",
                "decimals": 6,
                "logoURI": "https://example.test/usdc.png",
                "extensions": { "bridgeInfo": {} }
            },
            {
                "chainId": 501000101,
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "name": "USD Coin (Solana)",
                "symbol": "USDC",
                "decimals": 6
            }
        ]
    }"#;

    fn entry(chain: u64, byte: u8, symbol: &str) -> TokenListEntry {
        TokenListEntry {
            chain_id: ChainId::new(chain),
            address: Address::repeat_byte(byte),
            symbol: TokenSymbol::new(symbol),
            name: format!("{symbol} Coin"),
            decimals: 6,
        }
    }

    fn sym(s: &str) -> TokenSymbol {
        TokenSymbol::new(s)
    }

    #[test]
    fn wire_fixture_parses_and_skips_non_evm_entries() {
        let wire: WireTokenList = serde_json::from_str(WIRE_FIXTURE).expect("fixture parses");
        assert_eq!(wire.name, "Uniswap Labs Default");
        assert_eq!(wire.version.to_string(), "22.5.0");

        let entries = entries_from_wire(wire.tokens);
        assert_eq!(entries.len(), 1, "the Solana mint must be skipped");
        assert_eq!(entries[0].chain_id, ChainId::new(1));
        assert_eq!(entries[0].symbol, sym("USDC"));
        assert_eq!(entries[0].decimals, 6);
        assert_eq!(
            entries[0].address,
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                .parse::<Address>()
                .expect("static address")
        );
    }

    #[test]
    fn staleness_thresholds() {
        let meta = TokenListMeta {
            url: default_token_list_url(),
            name: "L".to_owned(),
            version: TokenListVersion {
                major: 1,
                minor: 0,
                patch: 0,
            },
            fetched_at: Timestamp::new(1_000_000),
        };
        let at = |secs_later: u64| Timestamp::new(1_000_000 + secs_later);
        let ttl = REFRESH_INTERVAL.as_secs();

        assert!(is_stale(None, at(0)), "no snapshot is always stale");
        assert!(!is_stale(Some(&meta), at(0)));
        assert!(!is_stale(Some(&meta), at(ttl - 1)));
        assert!(is_stale(Some(&meta), at(ttl)));
        // A future-dated snapshot (clock skew) is not stale.
        assert!(!is_stale(Some(&meta), Timestamp::new(999_999)));
    }

    #[test]
    fn pick_single_hit_wins() {
        let e = entry(1, 0x11, "USDC");
        let got = pick(vec![e.clone()], ChainId::new(1), &sym("usdc")).expect("resolves");
        assert_eq!(got, e);
    }

    #[test]
    fn pick_empty_is_not_found() {
        let err = pick(Vec::new(), ChainId::new(1), &sym("NOPE")).expect_err("must fail");
        assert!(matches!(err, TokenListError::NotFound { .. }), "{err}");
    }

    #[test]
    fn pick_prefers_unique_exact_case_among_collisions() {
        let upper = entry(1, 0x11, "LIT");
        let lower = entry(1, 0x22, "lit");
        let got = pick(vec![upper, lower.clone()], ChainId::new(1), &sym("lit"))
            .expect("exact case resolves");
        assert_eq!(got, lower);
    }

    #[test]
    fn pick_same_case_collision_is_ambiguous() {
        let a = entry(1, 0x11, "WBTC");
        let b = entry(1, 0x22, "WBTC");
        let err = pick(vec![a, b], ChainId::new(1), &sym("WBTC")).expect_err("must fail");
        match &err {
            TokenListError::Ambiguous { candidates, .. } => {
                assert!(candidates.contains("WBTC Coin"), "{candidates}");
            }
            other => panic!("expected Ambiguous, got {other}"),
        }
        // Both candidate addresses are named in the message.
        let msg = err.to_string();
        assert!(msg.contains("0x1111") && msg.contains("0x2222"), "{msg}");
    }

    #[test]
    fn pick_case_insensitive_query_with_no_exact_match_is_ambiguous() {
        let a = entry(1, 0x11, "LIT");
        let b = entry(1, 0x22, "lit");
        let err = pick(vec![a, b], ChainId::new(1), &sym("Lit")).expect_err("must fail");
        assert!(matches!(err, TokenListError::Ambiguous { .. }), "{err}");
    }
}
