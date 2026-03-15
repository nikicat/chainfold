//! ERC20 utility functions.
//!
//! Reusable primitives for ERC20 tokens: Transfer event decoding,
//! on-chain metadata fetching (Multicall3 → JSON-RPC batch fallback),
//! and position comparison.

use std::cmp::Ordering;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};

use crate::arch::evm::{EvmEvent, EvmPosition};
use crate::arch::HistoryError;

use super::log_cache::LogCache;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function name() external view returns (string);
    function symbol() external view returns (string);
    function decimals() external view returns (uint8);
}

/// Keccak256 of `Transfer(address,address,uint256)`.
pub const TRANSFER_TOPIC: [u8; 32] = Transfer::SIGNATURE_HASH.0;

/// On-chain ERC20 metadata.
pub struct Erc20Metadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

/// Fetch name/symbol/decimals with cache-first, Multicall3, and batch fallback.
///
/// 1. Check SQLite cache (instant, no RPC)
/// 2. Try Multicall3 `aggregate` (1 eth_call, 1 HTTP request)
/// 3. Fall back to JSON-RPC batch (3 eth_calls, 1 HTTP request)
/// 4. Write result to cache for next time
///
/// Handles bytes32-returning tokens (MKR, SAI) in the batch fallback path.
pub async fn fetch_erc20_metadata<P: Provider<Ethereum>>(
    provider: &P,
    address: [u8; 20],
    chain_id: u64,
    cache: &LogCache,
) -> Result<Erc20Metadata, HistoryError> {
    // 1. Cache check
    if let Some(cached) = cache
        .get_erc20_metadata(&address, chain_id)
        .map_err(|e| HistoryError::internal(format!("cache read: {e}")))?
    {
        tracing::debug!(addr = %Address::from_slice(&address), "erc20 metadata cache hit");
        return Ok(Erc20Metadata {
            name: cached.name,
            symbol: cached.symbol,
            decimals: cached.decimals,
        });
    }

    // 2. Fetch from chain: Multicall3 → batch fallback
    let meta = match fetch_metadata_multicall(provider, address).await {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!(error = %e, "multicall failed, falling back to batch");
            fetch_metadata_batch(provider, address).await?
        }
    };

    // 3. Write to cache
    cache
        .insert_erc20_metadata(&address, chain_id, &meta.name, &meta.symbol, meta.decimals)
        .map_err(|e| HistoryError::internal(format!("cache write: {e}")))?;

    Ok(meta)
}

/// Fetch metadata via Multicall3 `aggregate` (single eth_call).
///
/// Fails if Multicall3 isn't deployed or if the token returns bytes32
/// instead of string (rare tokens like MKR).
async fn fetch_metadata_multicall<P: Provider<Ethereum>>(
    provider: &P,
    address: [u8; 20],
) -> Result<Erc20Metadata, HistoryError> {
    use alloy::providers::CallItem;

    let addr = Address::from_slice(&address);
    tracing::debug!(%addr, "fetching erc20 metadata via multicall3");

    let (name, symbol, decimals) = provider
        .multicall()
        .add_call(CallItem::<nameCall>::new(
            addr,
            Bytes::from(nameCall {}.abi_encode()),
        ))
        .add_call(CallItem::<symbolCall>::new(
            addr,
            Bytes::from(symbolCall {}.abi_encode()),
        ))
        .add_call(CallItem::<decimalsCall>::new(
            addr,
            Bytes::from(decimalsCall {}.abi_encode()),
        ))
        .aggregate()
        .await
        .map_err(|e| HistoryError::internal(format!("multicall3: {e}")))?;

    tracing::debug!(%addr, %name, %symbol, decimals, "multicall3 success");
    Ok(Erc20Metadata {
        name,
        symbol,
        decimals,
    })
}

/// Fetch metadata via JSON-RPC batch (3 eth_calls in 1 HTTP request).
///
/// Handles bytes32-returning tokens via `decode_string_or_bytes32`.
async fn fetch_metadata_batch<P: Provider<Ethereum>>(
    provider: &P,
    address: [u8; 20],
) -> Result<Erc20Metadata, HistoryError> {
    use alloy::network::TransactionBuilder;
    use alloy::rpc::types::TransactionRequest;

    let addr = Address::from_slice(&address);
    tracing::debug!(%addr, "fetching erc20 metadata via json-rpc batch");

    let name_tx = TransactionRequest::default()
        .with_to(addr)
        .with_input(Bytes::from(nameCall {}.abi_encode()));
    let symbol_tx = TransactionRequest::default()
        .with_to(addr)
        .with_input(Bytes::from(symbolCall {}.abi_encode()));
    let decimals_tx = TransactionRequest::default()
        .with_to(addr)
        .with_input(Bytes::from(decimalsCall {}.abi_encode()));

    let mut batch = alloy::rpc::client::BatchRequest::new(provider.client());
    let name_fut = batch
        .add_call::<_, Bytes>("eth_call", &(name_tx, BlockNumberOrTag::Latest))
        .map_err(|e| HistoryError::internal(format!("batch build: {e}")))?;
    let symbol_fut = batch
        .add_call::<_, Bytes>("eth_call", &(symbol_tx, BlockNumberOrTag::Latest))
        .map_err(|e| HistoryError::internal(format!("batch build: {e}")))?;
    let decimals_fut = batch
        .add_call::<_, Bytes>("eth_call", &(decimals_tx, BlockNumberOrTag::Latest))
        .map_err(|e| HistoryError::internal(format!("batch build: {e}")))?;

    batch
        .send()
        .await
        .map_err(|e| HistoryError::internal(format!("batch send: {e}")))?;

    let name_bytes = name_fut
        .await
        .map_err(|e| HistoryError::internal(format!("batch name: {e}")))?;
    let symbol_bytes = symbol_fut
        .await
        .map_err(|e| HistoryError::internal(format!("batch symbol: {e}")))?;
    let decimals_bytes = decimals_fut
        .await
        .map_err(|e| HistoryError::internal(format!("batch decimals: {e}")))?;

    let name = decode_string_or_bytes32(&name_bytes)?;
    let symbol = decode_string_or_bytes32(&symbol_bytes)?;
    let decimals = decimalsCall::abi_decode_returns(&decimals_bytes)
        .map_err(|e| HistoryError::internal(format!("decimals decode: {e}")))?;

    tracing::debug!(%addr, %name, %symbol, decimals, "batch success");
    Ok(Erc20Metadata {
        name,
        symbol,
        decimals,
    })
}

/// Try ABI string decode, then bytes32 fallback (MKR, SAI, etc.).
fn decode_string_or_bytes32(data: &[u8]) -> Result<String, HistoryError> {
    // Standard ABI string decode
    if let Ok(s) = nameCall::abi_decode_returns(data) {
        return Ok(s);
    }
    // bytes32 fallback: first 32 bytes, null-trimmed
    if data.len() >= 32 {
        let s = String::from_utf8_lossy(&data[..32])
            .trim_end_matches('\0')
            .trim()
            .to_string();
        if !s.is_empty() {
            return Ok(s);
        }
    }
    Err(HistoryError::internal(
        "failed to decode string return value",
    ))
}

/// Decoded ERC20 Transfer event data.
pub struct TransferData {
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub amount: U256,
}

/// Decode a raw EvmEvent as an ERC20 Transfer. Returns None if not a valid Transfer.
pub fn decode_transfer(event: &EvmEvent) -> Option<TransferData> {
    if event.topics.len() < 3 || event.topics[0] != TRANSFER_TOPIC {
        return None;
    }
    let mut from = [0u8; 20];
    from.copy_from_slice(&event.topics[1][12..32]);
    let mut to = [0u8; 20];
    to.copy_from_slice(&event.topics[2][12..32]);
    let amount = if event.data.len() >= 32 {
        U256::from_be_slice(&event.data[..32])
    } else {
        U256::ZERO
    };
    Some(TransferData { from, to, amount })
}

/// Standard comparator for merging same-chain EVM histories.
pub fn evm_pos_cmp(a: &EvmPosition, b: &EvmPosition) -> Ordering {
    assert_eq!(
        a.chain_id, b.chain_id,
        "comparing positions from different chains"
    );
    (a.block_number, a.log_index).cmp(&(b.block_number, b.log_index))
}
