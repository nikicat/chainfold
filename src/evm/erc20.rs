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

use super::log_store::LogStore;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function name() external view returns (string);
    function symbol() external view returns (string);
    function decimals() external view returns (uint8);
    function balanceOf(address account) external view returns (uint256);
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
    cache: &LogStore,
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

// ============================================================
// Erc20BalanceReducer
// ============================================================

use std::sync::Arc;

use crate::provider::Reducer;

/// Reducer that tracks an ERC20 token balance for a specific owner.
///
/// - `apply`: adds incoming amounts, subtracts outgoing amounts
/// - `fetch_state`: calls `balanceOf(owner)` at the given block via RPC,
///   with SQLite caching in the `erc20_balances` table
pub struct Erc20BalanceReducer<P> {
    pub owner: [u8; 20],
    pub token_address: [u8; 20],
    pub chain_id: u64,
    pub provider: P,
    pub cache: Arc<LogStore>,
}

impl<P> Erc20BalanceReducer<P> {
    pub fn new(
        owner: [u8; 20],
        token_address: [u8; 20],
        chain_id: u64,
        provider: P,
        cache: Arc<LogStore>,
    ) -> Self {
        Self {
            owner,
            token_address,
            chain_id,
            provider,
            cache,
        }
    }

    /// Look up the closest cached balance at or before `pos`. No Provider needed.
    pub fn cached_balance_floor(&self, pos: &EvmPosition) -> Option<(EvmPosition, U256)> {
        let (block, balance) = self
            .cache
            .get_erc20_balance_floor(
                self.chain_id,
                &self.token_address,
                &self.owner,
                pos.block_number,
            )
            .ok()??;
        let evm_pos = EvmPosition {
            block_number: block,
            log_index: 0,
            side: crate::arch::Side::After,
            ..pos.clone()
        };
        Some((evm_pos, balance))
    }

    /// Apply a transfer event to a balance. Usable without Provider bound.
    pub fn apply_transfer(&self, balance: U256, event: &EvmEvent) -> U256 {
        if let Some(t) = decode_transfer(event) {
            if t.to == self.owner {
                balance.wrapping_add(t.amount)
            } else if t.from == self.owner {
                balance.checked_sub(t.amount).unwrap_or(U256::ZERO)
            } else {
                balance
            }
        } else {
            balance
        }
    }
}

#[async_trait::async_trait]
impl<P: Provider<Ethereum> + Clone + Send + Sync + 'static> Reducer<EvmPosition>
    for Erc20BalanceReducer<P>
{
    type Event = EvmEvent;
    type State = U256;

    fn apply(&self, balance: U256, event: &EvmEvent) -> U256 {
        self.apply_transfer(balance, event)
    }

    async fn fetch_state(&self, pos: &EvmPosition) -> Result<U256, HistoryError> {
        // 1. Check erc20_balances cache
        if let Some(cached) = self
            .cache
            .get_erc20_balance(self.chain_id, &self.token_address, &self.owner, pos.block_number)
            .map_err(|e| HistoryError::internal(format!("balance cache read: {e}")))?
        {
            return Ok(cached);
        }

        // 2. eth_call balanceOf(owner) at pos.block_number
        let addr = Address::from_slice(&self.token_address);
        let owner_addr = Address::from_slice(&self.owner);
        let calldata = balanceOfCall {
            account: owner_addr,
        }
        .abi_encode();

        use alloy::network::TransactionBuilder;
        use alloy::rpc::types::TransactionRequest;

        let tx = TransactionRequest::default()
            .with_to(addr)
            .with_input(Bytes::from(calldata));

        let block = BlockNumberOrTag::Number(pos.block_number);
        let result = self
            .provider
            .call(tx)
            .block(block.into())
            .await
            .map_err(|e| HistoryError::internal(format!("balanceOf call: {e}")))?;

        let balance = balanceOfCall::abi_decode_returns(&result)
            .map_err(|e| HistoryError::internal(format!("balanceOf decode: {e}")))?;

        // 3. Cache the result
        self.cache
            .insert_erc20_balance(
                self.chain_id,
                &self.token_address,
                &self.owner,
                pos.block_number,
                balance,
            )
            .map_err(|e| HistoryError::internal(format!("balance cache write: {e}")))?;

        Ok(balance)
    }

    fn find_cached_state_floor(&self, pos: &EvmPosition) -> Option<(EvmPosition, U256)> {
        self.cached_balance_floor(pos)
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arch::Side;

    fn make_transfer_event(from: [u8; 20], to: [u8; 20], amount: U256) -> EvmEvent {
        let mut topic1 = [0u8; 32];
        topic1[12..32].copy_from_slice(&from);
        let mut topic2 = [0u8; 32];
        topic2[12..32].copy_from_slice(&to);
        let mut data = vec![0u8; 32];
        amount.to_be_bytes::<32>().iter().enumerate().for_each(|(i, b)| data[i] = *b);

        EvmEvent {
            contract_address: [0x42; 20],
            block_number: 100,
            timestamp: 1000,
            block_hash: [0; 32],
            log_index: 0,
            tx_hash: [0; 32],
            topics: vec![TRANSFER_TOPIC, topic1, topic2],
            data,
        }
    }

    #[test]
    fn apply_incoming_adds() {
        let owner = [0x01u8; 20];
        let other = [0x02u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let reducer = Erc20BalanceReducer {
            owner,
            token_address: [0x42; 20],
            chain_id: 1,
            provider: (), // unused in apply
            cache,
        };
        let event = make_transfer_event(other, owner, U256::from(100));
        assert_eq!(reducer.apply_transfer(U256::from(50), &event), U256::from(150));
    }

    #[test]
    fn apply_outgoing_subtracts() {
        let owner = [0x01u8; 20];
        let other = [0x02u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let reducer = Erc20BalanceReducer {
            owner,
            token_address: [0x42; 20],
            chain_id: 1,
            provider: (),
            cache,
        };
        let event = make_transfer_event(owner, other, U256::from(30));
        assert_eq!(reducer.apply_transfer(U256::from(50), &event), U256::from(20));
    }

    #[test]
    fn apply_underflow_saturates_to_zero() {
        let owner = [0x01u8; 20];
        let other = [0x02u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let reducer = Erc20BalanceReducer {
            owner,
            token_address: [0x42; 20],
            chain_id: 1,
            provider: (),
            cache,
        };
        let event = make_transfer_event(owner, other, U256::from(100));
        assert_eq!(reducer.apply_transfer(U256::from(50), &event), U256::ZERO);
    }

    #[test]
    fn apply_non_transfer_is_noop() {
        let owner = [0x01u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let reducer = Erc20BalanceReducer {
            owner,
            token_address: [0x42; 20],
            chain_id: 1,
            provider: (),
            cache,
        };
        // Event with wrong topic0
        let event = EvmEvent {
            contract_address: [0x42; 20],
            block_number: 100,
            timestamp: 1000,
            block_hash: [0; 32],
            log_index: 0,
            tx_hash: [0; 32],
            topics: vec![[0xFF; 32]],
            data: vec![],
        };
        assert_eq!(reducer.apply_transfer(U256::from(50), &event), U256::from(50));
    }

    #[test]
    fn apply_unrelated_transfer_is_noop() {
        let owner = [0x01u8; 20];
        let other_a = [0x02u8; 20];
        let other_b = [0x03u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());
        let reducer = Erc20BalanceReducer {
            owner,
            token_address: [0x42; 20],
            chain_id: 1,
            provider: (),
            cache,
        };
        let event = make_transfer_event(other_a, other_b, U256::from(100));
        assert_eq!(reducer.apply_transfer(U256::from(50), &event), U256::from(50));
    }

    #[test]
    fn find_cached_state_floor_from_sqlite() {
        let owner = [0x01u8; 20];
        let token = [0x42u8; 20];
        let cache = Arc::new(LogStore::open_in_memory().unwrap());

        // Insert balances at blocks 100 and 200
        cache.insert_erc20_balance(1, &token, &owner, 100, U256::from(500)).unwrap();
        cache.insert_erc20_balance(1, &token, &owner, 200, U256::from(1000)).unwrap();

        let reducer = Erc20BalanceReducer {
            owner,
            token_address: token,
            chain_id: 1,
            provider: (),
            cache,
        };

        // Query at block 150 → should return block 100, balance 500
        let pos = EvmPosition {
            chain_id: 1, block_number: 150, log_index: 0,
            block_hash: [0; 32], timestamp: 1500, side: Side::Before,
        };
        let (floor_pos, balance) = reducer.cached_balance_floor(&pos).unwrap();
        assert_eq!(floor_pos.block_number, 100);
        assert_eq!(balance, U256::from(500));

        // Query at block 250 → should return block 200, balance 1000
        let pos2 = EvmPosition {
            chain_id: 1, block_number: 250, log_index: 0,
            block_hash: [0; 32], timestamp: 2500, side: Side::Before,
        };
        let (floor_pos2, balance2) = reducer.cached_balance_floor(&pos2).unwrap();
        assert_eq!(floor_pos2.block_number, 200);
        assert_eq!(balance2, U256::from(1000));

        // Query at block 50 → no entry before, None
        let pos3 = EvmPosition {
            chain_id: 1, block_number: 50, log_index: 0,
            block_hash: [0; 32], timestamp: 500, side: Side::Before,
        };
        assert!(reducer.cached_balance_floor(&pos3).is_none());
    }
}
