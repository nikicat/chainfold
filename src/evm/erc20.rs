//! ERC20 utility functions.
//!
//! Reusable primitives for ERC20 tokens: Transfer event decoding,
//! on-chain metadata fetching (Multicall3 → JSON-RPC batch fallback),
//! and a pure balance-fold helper.
//!
//! Layer note: no `Reducer` trait dependency. Balance tracking is
//! performed by the application layer (or, eventually, a generic
//! `Reducer` rebuilt in Phase 6) by folding [`apply_transfer`] over a
//! stream of [`EvmEvent`]s yielded by a `HistStore`.

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;

use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};

use crate::arch::HistoryError;

use super::cache::Erc20Cache;
use super::types::{ChainId, EvmEvent, Topic};
use crate::observe::{CacheLayer, CacheOutcome, cache_event};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function name() external view returns (string);
    function symbol() external view returns (string);
    function decimals() external view returns (uint8);
    function balanceOf(address account) external view returns (uint256);
}

/// Keccak256 of `Transfer(address,address,uint256)`.
pub const TRANSFER_TOPIC: Topic = Topic::new(Transfer::SIGNATURE_HASH);

/// On-chain ERC20 metadata.
pub struct Erc20Metadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

/// Fetch name/symbol/decimals with cache-first, Multicall3, and batch fallback.
///
/// 1. Check `SQLite` cache (instant, no RPC)
/// 2. Try Multicall3 `aggregate` (1 `eth_call`, 1 HTTP request)
/// 3. Fall back to JSON-RPC batch (3 `eth_calls`, 1 HTTP request)
/// 4. Write result to cache for next time
///
/// Handles bytes32-returning tokens (MKR, SAI) in the batch fallback path.
pub async fn fetch_erc20_metadata<P: Provider<Ethereum>>(
    provider: &P,
    address: Address,
    chain_id: ChainId,
    cache: &Erc20Cache,
) -> Result<Erc20Metadata, HistoryError> {
    if let Some(cached) = cache
        .get_erc20_metadata(&address, chain_id)
        .map_err(|e| HistoryError::internal(format!("cache read: {e}")))?
    {
        cache_event(CacheLayer::Erc20Metadata, CacheOutcome::Hit);
        return Ok(Erc20Metadata {
            name: cached.name,
            symbol: cached.symbol,
            decimals: cached.decimals,
        });
    }
    cache_event(CacheLayer::Erc20Metadata, CacheOutcome::Miss);

    let meta = match fetch_metadata_multicall(provider, address).await {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!(error = %e, "multicall failed, falling back to batch");
            fetch_metadata_batch(provider, address).await?
        }
    };

    cache
        .insert_erc20_metadata(&address, chain_id, &meta.name, &meta.symbol, meta.decimals)
        .map_err(|e| HistoryError::internal(format!("cache write: {e}")))?;

    Ok(meta)
}

async fn fetch_metadata_multicall<P: Provider<Ethereum>>(
    provider: &P,
    address: Address,
) -> Result<Erc20Metadata, HistoryError> {
    use alloy::providers::CallItem;

    let addr = address;
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

async fn fetch_metadata_batch<P: Provider<Ethereum>>(
    provider: &P,
    address: Address,
) -> Result<Erc20Metadata, HistoryError> {
    use alloy::network::TransactionBuilder;
    use alloy::rpc::types::TransactionRequest;

    let addr = address;
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

    if name_bytes.is_empty() && symbol_bytes.is_empty() && decimals_bytes.is_empty() {
        return Err(HistoryError::internal(format!(
            "{address:#x} returned no data for name()/symbol()/decimals() — \
             is this an ERC20 contract on this chain? \
             (passing an EOA/wallet address here will fail this way)",
        )));
    }

    let name = decode_string_or_bytes32(&name_bytes)
        .map_err(|_| decode_err("name", &address, &name_bytes))?;
    let symbol = decode_string_or_bytes32(&symbol_bytes)
        .map_err(|_| decode_err("symbol", &address, &symbol_bytes))?;
    let decimals = decimalsCall::abi_decode_returns(&decimals_bytes).map_err(|e| {
        HistoryError::internal(format!(
            "decimals() decode for {address:#x}: {e} (raw: 0x{})",
            hex::encode(&decimals_bytes),
        ))
    })?;

    tracing::debug!(%addr, %name, %symbol, decimals, "batch success");
    Ok(Erc20Metadata {
        name,
        symbol,
        decimals,
    })
}

fn decode_string_or_bytes32(data: &[u8]) -> Result<String, HistoryError> {
    if let Ok(s) = nameCall::abi_decode_returns(data) {
        return Ok(s);
    }
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

fn decode_err(field: &str, address: &Address, data: &[u8]) -> HistoryError {
    HistoryError::internal(format!(
        "{field}() decode for {address:#x} failed (raw: 0x{}) — \
         not a standard ERC20 (or wrong chain)?",
        hex::encode(data),
    ))
}

/// Decoded ERC20 Transfer event data.
pub struct TransferData {
    pub from: Address,
    pub to: Address,
    pub amount: U256,
}

/// Decode a raw [`EvmEvent`] as an ERC20 Transfer. Returns `None` if not a
/// valid Transfer.
pub fn decode_transfer(event: &EvmEvent) -> Option<TransferData> {
    if event.topics.len() < 3 || event.topics[0] != TRANSFER_TOPIC {
        return None;
    }
    let from = event.topics[1].address();
    let to = event.topics[2].address();
    let amount = if event.data.len() >= 32 {
        U256::from_be_slice(&event.data[..32])
    } else {
        U256::ZERO
    };
    Some(TransferData { from, to, amount })
}

/// Apply a Transfer event to `owner`'s running balance.
///
/// * `from == to` → balance unchanged (a self-transfer moves nothing).
/// * `to == owner` → balance grows by `amount`.
/// * `from == owner` → balance shrinks by `amount` (saturating at zero).
/// * otherwise → balance unchanged.
///
/// Non-Transfer events are returned unchanged. Pure / no I/O — fold this
/// over a `HistStore` stream to derive a running balance.
pub fn apply_transfer(owner: Address, balance: U256, event: &EvmEvent) -> U256 {
    let Some(t) = decode_transfer(event) else {
        return balance;
    };
    if t.from == t.to {
        balance
    } else if t.to == owner {
        balance.wrapping_add(t.amount)
    } else if t.from == owner {
        balance.checked_sub(t.amount).unwrap_or(U256::ZERO)
    } else {
        balance
    }
}

/// On-chain `balanceOf(owner)` at `block`, with `SQLite` caching in
/// `erc20_balances`.
pub async fn fetch_balance_at<P: Provider<Ethereum>>(
    provider: &P,
    cache: &Erc20Cache,
    chain_id: ChainId,
    token: Address,
    owner: Address,
    block: u64,
) -> Result<U256, HistoryError> {
    if let Some(cached) = cache
        .get_erc20_balance(chain_id, &token, &owner, block)
        .map_err(|e| HistoryError::internal(format!("balance cache read: {e}")))?
    {
        return Ok(cached);
    }

    let calldata = balanceOfCall { account: owner }.abi_encode();

    use alloy::network::TransactionBuilder;
    use alloy::rpc::types::TransactionRequest;

    let tx = TransactionRequest::default()
        .with_to(token)
        .with_input(Bytes::from(calldata));

    let result = provider
        .call(tx)
        .block(BlockNumberOrTag::Number(block).into())
        .await
        .map_err(|e| HistoryError::internal(format!("balanceOf call: {e}")))?;

    let balance = balanceOfCall::abi_decode_returns(&result)
        .map_err(|e| HistoryError::internal(format!("balanceOf decode: {e}")))?;

    cache
        .insert_erc20_balance(chain_id, &token, &owner, block, balance)
        .map_err(|e| HistoryError::internal(format!("balance cache write: {e}")))?;

    Ok(balance)
}

/// Look up the closest cached balance at or before `block`. No RPC.
pub fn cached_balance_floor(
    cache: &Erc20Cache,
    chain_id: ChainId,
    token: Address,
    owner: Address,
    block: u64,
) -> Option<(u64, U256)> {
    cache
        .get_erc20_balance_floor(chain_id, &token, &owner, block)
        .ok()
        .flatten()
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Timestamp;
    use crate::chain::BlockNumber;
    use crate::evm::types::{BlockHash, LogIdx, TxHash};
    use alloy::primitives::B256;
    use std::sync::Arc;

    fn make_transfer_event(from: Address, to: Address, amount: U256) -> EvmEvent {
        let mut data = vec![0u8; 32];
        amount
            .to_be_bytes::<32>()
            .iter()
            .enumerate()
            .for_each(|(i, b)| data[i] = *b);

        EvmEvent {
            contract_address: Address::repeat_byte(0x42),
            block_number: BlockNumber::new(100),
            timestamp: Timestamp::new(1000),
            block_hash: BlockHash::ZERO,
            log_index: LogIdx::ZERO,
            tx_hash: TxHash::ZERO,
            topics: vec![TRANSFER_TOPIC, from.into(), to.into()],
            data,
        }
    }

    #[test]
    fn apply_incoming_adds() {
        let owner = Address::repeat_byte(0x01);
        let other = Address::repeat_byte(0x02);
        let event = make_transfer_event(other, owner, U256::from(100));
        assert_eq!(
            apply_transfer(owner, U256::from(50), &event),
            U256::from(150)
        );
    }

    #[test]
    fn apply_outgoing_subtracts() {
        let owner = Address::repeat_byte(0x01);
        let other = Address::repeat_byte(0x02);
        let event = make_transfer_event(owner, other, U256::from(30));
        assert_eq!(
            apply_transfer(owner, U256::from(50), &event),
            U256::from(20)
        );
    }

    #[test]
    fn apply_underflow_saturates_to_zero() {
        let owner = Address::repeat_byte(0x01);
        let other = Address::repeat_byte(0x02);
        let event = make_transfer_event(owner, other, U256::from(100));
        assert_eq!(apply_transfer(owner, U256::from(50), &event), U256::ZERO);
    }

    #[test]
    fn apply_self_transfer_is_balance_neutral() {
        // owner == from == to: nothing moves. Guards against the to-branch
        // winning and crediting the amount.
        let owner = Address::repeat_byte(0x01);
        let event = make_transfer_event(owner, owner, U256::from(100));
        assert_eq!(
            apply_transfer(owner, U256::from(50), &event),
            U256::from(50)
        );
    }

    #[test]
    fn apply_non_transfer_is_noop() {
        let owner = Address::repeat_byte(0x01);
        let event = EvmEvent {
            contract_address: Address::repeat_byte(0x42),
            block_number: BlockNumber::new(100),
            timestamp: Timestamp::new(1000),
            block_hash: BlockHash::ZERO,
            log_index: LogIdx::ZERO,
            tx_hash: TxHash::ZERO,
            topics: vec![Topic::new(B256::repeat_byte(0xFF))],
            data: vec![],
        };
        assert_eq!(
            apply_transfer(owner, U256::from(50), &event),
            U256::from(50)
        );
    }

    #[test]
    fn apply_unrelated_transfer_is_noop() {
        let owner = Address::repeat_byte(0x01);
        let other_a = Address::repeat_byte(0x02);
        let other_b = Address::repeat_byte(0x03);
        let event = make_transfer_event(other_a, other_b, U256::from(100));
        assert_eq!(
            apply_transfer(owner, U256::from(50), &event),
            U256::from(50)
        );
    }

    #[test]
    fn cached_balance_floor_walks_back() {
        let owner = Address::repeat_byte(0x01);
        let token = Address::repeat_byte(0x42);
        let cache = Arc::new(Erc20Cache::open_in_memory().unwrap());

        cache
            .insert_erc20_balance(ChainId::new(1), &token, &owner, 100, U256::from(500))
            .unwrap();
        cache
            .insert_erc20_balance(ChainId::new(1), &token, &owner, 200, U256::from(1000))
            .unwrap();

        let (b, v) = cached_balance_floor(&cache, ChainId::new(1), token, owner, 150).unwrap();
        assert_eq!((b, v), (100, U256::from(500)));

        let (b, v) = cached_balance_floor(&cache, ChainId::new(1), token, owner, 250).unwrap();
        assert_eq!((b, v), (200, U256::from(1000)));

        assert!(cached_balance_floor(&cache, ChainId::new(1), token, owner, 50).is_none());
    }
}
