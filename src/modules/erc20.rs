//! ERC20 balance tracker module.
//!
//! Provides domain types, state machine, codec, and an alloy-based EventSource
//! for tracking ERC20 Transfer events on EVM chains.
//!
//! Requires the `evm` feature flag.

use std::collections::{BTreeMap, HashMap};

use alloy::primitives::{FixedBytes, U256};
use alloy::rpc::types::{Filter, Log};

pub use alloy::primitives::Address;
pub use alloy::providers::{Provider, ProviderBuilder};

use crate::block::Block;
use crate::coordinator::Codec;
use crate::cursor::EvmCursor;
use crate::event::Event;
use crate::machine::StateMachine;
use crate::source::EventSource;

// ============================================================
// Domain types
// ============================================================

/// ERC20 Transfer event payload.
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub amount: u128,
}

/// Balance state: address -> token balance.
pub type Balances = HashMap<[u8; 20], u128>;

/// keccak256("Transfer(address,address,uint256)")
pub const TRANSFER_TOPIC: FixedBytes<32> = FixedBytes::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37,
    0x8d, 0xaa, 0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d,
    0xf5, 0x23, 0xb3, 0xef,
]);

/// Filter mode for Erc20Source queries.
#[derive(Debug, Clone)]
pub enum Erc20Filter {
    /// Fetch all Transfer events for the token contract.
    AllTransfers,
    /// Fetch Transfer events where the given address appears as sender or receiver.
    Account([u8; 20]),
}

// ============================================================
// StateMachine
// ============================================================

/// Reduces Transfer events into a balance map.
pub struct Erc20Machine;

impl StateMachine for Erc20Machine {
    type State = Balances;
    type Event = Transfer;
    type Cursor = EvmCursor;

    fn apply(&self, mut state: Balances, event: &Event<EvmCursor, Transfer>) -> Balances {
        let transfer = &event.data;
        // Debit sender (skip zero address — mints have from=0x0)
        if transfer.from != [0u8; 20] {
            let bal = state.entry(transfer.from).or_insert(0);
            *bal = bal.saturating_sub(transfer.amount);
        }
        // Credit receiver
        *state.entry(transfer.to).or_insert(0) += transfer.amount;
        state
    }
}

// ============================================================
// Codec
// ============================================================

/// Serializable wrapper for Transfer ([u8; 20] needs Vec<u8> for serde).
#[derive(serde::Serialize, serde::Deserialize)]
struct TransferSerde {
    from: Vec<u8>,
    to: Vec<u8>,
    amount: u128,
}

/// Serializable wrapper for Balances.
#[derive(serde::Serialize, serde::Deserialize)]
struct BalancesSerde(Vec<(Vec<u8>, u128)>);

/// JSON-based codec for Transfer events and Balances state.
pub struct Erc20Codec;

impl Codec for Erc20Codec {
    type Cursor = EvmCursor;
    type Event = Transfer;
    type State = Balances;

    fn encode_event(&self, event: &Transfer) -> Vec<u8> {
        serde_json::to_vec(&TransferSerde {
            from: event.from.to_vec(),
            to: event.to.to_vec(),
            amount: event.amount,
        })
        .unwrap()
    }

    fn decode_event(&self, payload: &[u8]) -> Option<Transfer> {
        let s: TransferSerde = serde_json::from_slice(payload).ok()?;
        Some(Transfer {
            from: s.from.try_into().ok()?,
            to: s.to.try_into().ok()?,
            amount: s.amount,
        })
    }

    fn encode_state(&self, state: &Balances) -> Vec<u8> {
        let entries: Vec<(Vec<u8>, u128)> =
            state.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
        serde_json::to_vec(&BalancesSerde(entries)).unwrap()
    }

    fn decode_state(&self, bytes: &[u8]) -> Option<Balances> {
        let s: BalancesSerde = serde_json::from_slice(bytes).ok()?;
        let mut map = HashMap::new();
        for (k, v) in s.0 {
            let addr: [u8; 20] = k.try_into().ok()?;
            map.insert(addr, v);
        }
        Some(map)
    }
}

// ============================================================
// EventSource
// ============================================================

/// Error type for the alloy-based ERC20 event source.
#[derive(Debug, thiserror::Error)]
pub enum Erc20SourceError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("parse error: {0}")]
    Parse(String),
}

/// Alloy-based EventSource for ERC20 Transfer events with filter pushdown.
///
/// Supports two filter modes via `Erc20Filter`:
/// - `AllTransfers`: single eth_getLogs per chunk (topic0 = Transfer)
/// - `Account(addr)`: two eth_getLogs per chunk (topic1=addr OR topic2=addr),
///   results merged and deduplicated by (block_number, log_index)
pub struct Erc20Source<P: Provider> {
    provider: P,
    token: [u8; 20],
    filter: Erc20Filter,
    chunk_size: u64,
    from_block: u64,
}

impl<P: Provider> Erc20Source<P> {
    /// Create a new ERC20 event source.
    ///
    /// - `provider`: alloy provider (HTTP, WS, etc.)
    /// - `token`: ERC20 contract address as raw bytes
    /// - `filter`: which transfers to fetch (all or account-scoped)
    /// - `chunk_size`: max block range per eth_getLogs call
    /// - `from_block`: earliest block to query (skips anything below)
    pub fn new(
        provider: P,
        token: [u8; 20],
        filter: Erc20Filter,
        chunk_size: u64,
        from_block: u64,
    ) -> Self {
        Self {
            provider,
            token,
            filter,
            chunk_size,
            from_block,
        }
    }
}

/// Left-pad a 20-byte address to a 32-byte EVM log topic.
fn address_to_topic(addr: &[u8; 20]) -> FixedBytes<32> {
    let mut topic = [0u8; 32];
    topic[12..32].copy_from_slice(addr);
    FixedBytes::new(topic)
}

/// Parse a Transfer event from a raw EVM log.
fn parse_transfer_log(log: &Log) -> Option<(EvmCursor, Transfer)> {
    let topics = log.topics();
    if topics.len() < 3 {
        return None;
    }

    let block_number = log.block_number.unwrap_or(0);
    let log_index = log.log_index.unwrap_or(0) as u32;

    let from: [u8; 20] = topics[1].as_slice()[12..32].try_into().ok()?;
    let to: [u8; 20] = topics[2].as_slice()[12..32].try_into().ok()?;

    let amount = if log.data().data.len() >= 32 {
        U256::from_be_slice(&log.data().data[..32]).to::<u128>()
    } else {
        0u128
    };

    Some((
        EvmCursor {
            block_number,
            log_index,
        },
        Transfer { from, to, amount },
    ))
}

impl<P: Provider + Clone> Erc20Source<P> {
    /// Fetch Transfer logs in a closed block range [from, to].
    ///
    /// For `AllTransfers`: single query with topic0=Transfer.
    /// For `Account(addr)`: two queries (topic1=addr, topic2=addr),
    /// deduplicated by (block_number, log_index) via BTreeMap.
    async fn fetch_logs_in_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, Erc20SourceError> {
        let token_addr = Address::from_slice(&self.token);

        match &self.filter {
            Erc20Filter::AllTransfers => {
                let filter = Filter::new()
                    .address(token_addr)
                    .event_signature(TRANSFER_TOPIC)
                    .from_block(from)
                    .to_block(to);
                self.provider
                    .get_logs(&filter)
                    .await
                    .map_err(|e| Erc20SourceError::Rpc(e.to_string()))
            }
            Erc20Filter::Account(addr) => {
                let addr_topic = address_to_topic(addr);

                // Query 1: address as sender (topic1)
                let filter_from = Filter::new()
                    .address(token_addr)
                    .event_signature(TRANSFER_TOPIC)
                    .topic1(addr_topic)
                    .from_block(from)
                    .to_block(to);

                // Query 2: address as receiver (topic2)
                let filter_to = Filter::new()
                    .address(token_addr)
                    .event_signature(TRANSFER_TOPIC)
                    .topic2(addr_topic)
                    .from_block(from)
                    .to_block(to);

                let (res_from, res_to) = tokio::join!(
                    self.provider.get_logs(&filter_from),
                    self.provider.get_logs(&filter_to),
                );

                let logs_from = res_from.map_err(|e| Erc20SourceError::Rpc(e.to_string()))?;
                let logs_to = res_to.map_err(|e| Erc20SourceError::Rpc(e.to_string()))?;

                // Deduplicate by (block_number, log_index) — a self-transfer
                // appears in both result sets.
                let mut deduped: BTreeMap<(u64, u32), Log> = BTreeMap::new();
                for log in logs_from.into_iter().chain(logs_to) {
                    let bn = log.block_number.unwrap_or(0);
                    let li = log.log_index.unwrap_or(0) as u32;
                    deduped.entry((bn, li)).or_insert(log);
                }

                Ok(deduped.into_values().collect())
            }
        }
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone> EventSource for Erc20Source<P> {
    type Cursor = EvmCursor;
    type Event = Transfer;
    type Error = Erc20SourceError;

    async fn fetch_state(&self, _at: &EvmCursor) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<EvmCursor, Transfer>>, Self::Error> {
        // Clamp to configured floor.
        let from_height = from_height.max(self.from_block);
        if from_height >= to_height {
            return Ok(Vec::new());
        }

        // Chunk into batches to stay within RPC provider limits.
        // [from_height, to_height) is half-open, so inclusive end = to_height - 1.
        let end = to_height.saturating_sub(1);
        let mut all_logs = Vec::new();
        let mut cursor = from_height;
        while cursor <= end {
            let chunk_end = end.min(cursor + self.chunk_size - 1);
            let chunk_logs = self.fetch_logs_in_range(cursor, chunk_end).await?;
            #[cfg(feature = "evm")]
            {
                tracing::debug!(
                    from = cursor,
                    to = chunk_end,
                    events = chunk_logs.len(),
                    "eth_getLogs"
                );
            }
            all_logs.extend(chunk_logs);
            cursor = chunk_end + 1;
        }

        // Group logs by block_number, parse transfers.
        let mut blocks_map: BTreeMap<u64, Vec<Event<EvmCursor, Transfer>>> = BTreeMap::new();
        for log in &all_logs {
            if let Some((evm_cursor, transfer)) = parse_transfer_log(log) {
                blocks_map
                    .entry(evm_cursor.block_number)
                    .or_default()
                    .push(Event {
                        cursor: evm_cursor,
                        data: transfer,
                    });
            }
        }

        // Build Block vec with real block hashes from RPC.
        let mut result = Vec::new();
        for (height, events) in blocks_map {
            let block = self
                .provider
                .get_block_by_number(height.into())
                .await
                .map_err(|e| Erc20SourceError::Rpc(e.to_string()))?;

            let (block_hash, parent_hash) = match block {
                Some(b) => {
                    let bh: [u8; 32] = b.header.hash.0.into();
                    let ph: [u8; 32] = b.header.parent_hash.0.into();
                    (bh, ph)
                }
                None => {
                    // Fallback: synthetic hashes from height (should not happen with a real RPC).
                    let mut bh = [0u8; 32];
                    bh[0..8].copy_from_slice(&height.to_le_bytes());
                    let mut ph = [0u8; 32];
                    ph[0..8].copy_from_slice(&height.saturating_sub(1).to_le_bytes());
                    (bh, ph)
                }
            };

            result.push(Block {
                block_hash,
                parent_hash,
                height,
                events,
            });
        }

        Ok(result)
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<EvmCursor, Transfer>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        // No live subscription — returns empty stream. Documented limitation.
        Ok(Box::new(futures_util::stream::empty()))
    }
}
