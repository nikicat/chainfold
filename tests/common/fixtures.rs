//! Pinned mainnet addresses and blocks used across integration tests.
//!
//! Picked once so changes here can be reviewed alongside test failures.

use alloy::primitives::{Address, B256, address, b256};
use chainfold::evm::{BlockCount, BlockNumber};

/// USDC token contract on Ethereum mainnet. Stable since 2018.
pub const USDC: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

/// Wrapped Ether contract on Ethereum mainnet.
pub const WETH: Address = address!("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");

/// USDC `Transfer(address,address,uint256)` event topic.
/// `keccak256("Transfer(address,address,uint256)")`.
pub const TRANSFER_TOPIC: B256 =
    b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

/// Block at which USDC was deployed (`tx 0x9d6...`).
pub const USDC_DEPLOY_BLOCK: BlockNumber = BlockNumber::new(6_082_465);

/// A recent-but-stable post-merge block with hundreds of USDC transfers.
/// Used as the default fork target for integration tests; replace cautiously,
/// since several tests depend on this exact block hash being stable.
pub const FORK_BLOCK: BlockNumber = BlockNumber::new(18_000_000);

/// Number of blocks before [`FORK_BLOCK`] over which integration tests sweep
/// for USDC transfers. Small enough to stay under public-RPC rate limits.
pub const FORK_SWEEP_BLOCKS: BlockCount = BlockCount::new(50);
