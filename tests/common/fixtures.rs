//! Pinned mainnet addresses and blocks used across integration tests.
//!
//! Picked once so changes here can be reviewed alongside test failures.

use alloy::primitives::{address, Address};

/// USDC token contract on Ethereum mainnet. Stable since 2018.
pub const USDC: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

/// Wrapped Ether contract on Ethereum mainnet.
pub const WETH: Address = address!("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");

/// USDC `Transfer(address,address,uint256)` event topic.
/// `keccak256("Transfer(address,address,uint256)")`.
pub const TRANSFER_TOPIC: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];

/// Block at which USDC was deployed (`tx 0x9d6...`).
pub const USDC_DEPLOY_BLOCK: u64 = 6_082_465;

/// A recent-but-stable post-merge block with hundreds of USDC transfers.
/// Used as the default fork target for integration tests; replace cautiously,
/// since several tests depend on this exact block hash being stable.
pub const FORK_BLOCK: u64 = 18_000_000;

/// Number of blocks before [`FORK_BLOCK`] over which integration tests sweep
/// for USDC transfers. Small enough to stay under public-RPC rate limits.
pub const FORK_SWEEP_BLOCKS: u64 = 50;
