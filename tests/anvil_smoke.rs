//! Smoke tests for the anvil test harness (CCP-1).
//!
//! The "no fork" smoke test runs by default and just confirms a local
//! anvil starts and answers an RPC call. The "fork mainnet" smoke test
//! is `#[ignore]` because it spins up anvil with a real RPC; run with
//! `cargo test --test anvil_smoke -- --ignored`.

mod common;

use alloy::{
    primitives::U256,
    providers::{Provider, ProviderBuilder},
    sol,
};

use common::{
    anvil::{fork_mainnet, local, skip_no_anvil},
    fixtures::{FORK_BLOCK, USDC},
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function totalSupply() external view returns (uint256);
    }
}

#[tokio::test]
async fn local_anvil_reaches_block_zero() {
    let Some(instance) = local() else {
        skip_no_anvil("local_anvil_reaches_block_zero");
        return;
    };

    let provider = ProviderBuilder::new().connect_http(instance.endpoint_url());
    let block_number = provider
        .get_block_number()
        .await
        .expect("get_block_number should succeed against local anvil");

    assert_eq!(block_number, 0, "fresh local anvil starts at block 0");
}

#[tokio::test]
#[ignore = "spawns anvil with a real fork — slow, network-bound"]
async fn fork_mainnet_reads_usdc_total_supply() {
    let Some(instance) = fork_mainnet(FORK_BLOCK) else {
        skip_no_anvil("fork_mainnet_reads_usdc_total_supply");
        return;
    };

    let provider = ProviderBuilder::new().connect_http(instance.endpoint_url());

    let block_number = provider
        .get_block_number()
        .await
        .expect("anvil fork should answer get_block_number");
    assert_eq!(
        block_number,
        FORK_BLOCK.get(),
        "anvil fork should be pinned at the requested block"
    );

    let usdc = IERC20::new(USDC, &provider);
    let supply = usdc
        .totalSupply()
        .call()
        .await
        .expect("USDC.totalSupply() should succeed on a mainnet fork");

    assert!(
        supply > U256::ZERO,
        "USDC totalSupply at block {FORK_BLOCK} should be non-zero (got {supply})"
    );
}
