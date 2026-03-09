//! ERC20 balance tracker module.
//!
//! Provides domain types, state machine, codec, and an alloy-based EventSource
//! for tracking ERC20 Transfer events on EVM chains.
//!
//! Requires the `evm` feature flag.

use std::collections::HashMap;

use alloy::primitives::FixedBytes;

pub use alloy::primitives::Address;
pub use alloy::providers::{Provider, ProviderBuilder};

use crate::coordinator::Codec;
use crate::cursor::EvmCursor;
use crate::event::Event;
use crate::machine::StateMachine;

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
