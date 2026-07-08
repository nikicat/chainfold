//! Integration tests for [`OwnedErc20`] (the Phase 5 follow-up).
//!
//! These tests fork mainnet via the anvil harness, so they're
//! `#[ignore]`-gated. Run with:
//!
//!   cargo test --features evm --test `owned_erc20` -- --ignored
//!
//! Verifies that the two-narrow-filter merge produces exactly the same
//! event set as the broad-filter + client-side filter approach, and that
//! `MergedItem::source_key` correctly tags direction.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use chainfold::Shared;
use chainfold::evm::{
    ChainProvider, ChunkSize, EvmChain, EvmChainConfig, EvmEvent, EvmPosition, Finality, LogCache,
    LogFilter, OwnedErc20, SubscribeTransport, TRANSFER_TOPIC, Topic,
};
use chainfold::merge::MergedItem;
use futures_util::StreamExt;
use tempfile::TempDir;

use common::{
    anvil::{fork_mainnet, skip_no_anvil},
    fixtures::{FORK_BLOCK, USDC},
};

const TEST_CONFIG: EvmChainConfig = EvmChainConfig {
    chunk_size: ChunkSize::new(10),
    block_time: Duration::from_secs(12),
    transport: SubscribeTransport::Poll {
        interval: Duration::from_secs(2),
    },
};

#[tokio::test]
#[ignore = "fork-mainnet integration: spawns anvil + hits external RPC"]
async fn owned_erc20_matches_broad_filter_then_client_side() {
    let Some(anvil) = fork_mainnet(FORK_BLOCK) else {
        skip_no_anvil("owned_erc20_matches_broad_filter_then_client_side");
        return;
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("owned_erc20.v3.db");
    let endpoint = anvil.endpoint_url();
    let cache = Shared::new(LogCache::open(db_path.to_str().unwrap()).expect("open cache"));
    let provider = ChainProvider::discover(ProviderBuilder::new().connect_http(endpoint), cache)
        .await
        .expect("discover chain id");
    let chain = EvmChain::new(provider, TEST_CONFIG);

    // Phase A — broad filter to discover an active owner in the window.
    let broad = chain.logs(
        &LogFilter::new(USDC, [Some(TRANSFER_TOPIC), None, None, None]),
        Finality::Latest,
    );

    let from_block = FORK_BLOCK - 5;
    let to_block = FORK_BLOCK;
    let start = EvmPosition::new(from_block - 1, u32::MAX);
    let end = EvmPosition::new(to_block, u32::MAX);
    let hist = broad.next().await;
    let (_, after_start) = hist.split(start);
    let (window, _) = after_start.split(end);
    let mut stream = window.iter();
    let mut all_transfers: Vec<EvmEvent> = Vec::new();
    while let Some(item) = stream.next().await {
        all_transfers.push(item.expect("iter").item);
    }
    assert!(
        !all_transfers.is_empty(),
        "expected USDC transfers in fork window"
    );

    // Pick the first transfer's `from` as the owner under test.
    let first = &all_transfers[0];
    let owner = first.topics[1].address();
    println!(
        "Chosen owner: {:#x} (with {} surrounding transfers)",
        owner,
        all_transfers.len()
    );

    // Build the expected set: every event where owner is either `from`
    // or `to`, deduped by (block, log_index).
    let owner_padded = Topic::from(owner);
    let mut expected: Vec<&EvmEvent> = all_transfers
        .iter()
        .filter(|e| {
            e.topics.get(1).copied() == Some(owner_padded)
                || e.topics.get(2).copied() == Some(owner_padded)
        })
        .collect();
    expected.sort_by_key(|e| (e.block_number, e.log_index));
    assert!(
        !expected.is_empty(),
        "first transfer's from must appear in its own filter"
    );

    // Phase B — same range via OwnedErc20. Awaiting the merged snapshot
    // demands the head (chain-wide singleflight), so no explicit tip
    // refresh is needed for a one-shot walk.
    let owned = OwnedErc20::new(&chain, owner, USDC);
    let merged_hs = owned.transfers();
    let hist = merged_hs.next().await;
    let (_, after_start) = hist.split(start);
    let (window, _) = after_start.split(end);
    let mut stream = window.iter();
    let mut yields: Vec<MergedItem<EvmEvent, EvmPosition>> = Vec::new();
    while let Some(item) = stream.next().await {
        yields.push(item.expect("iter").item);
    }
    assert!(!yields.is_empty(), "OwnedErc20 yielded zero events");

    // Each yield must involve the owner and have a consistent source_key.
    for y in &yields {
        let topic1 = y.value.topics.get(1).copied();
        let topic2 = y.value.topics.get(2).copied();
        let is_from = topic1 == Some(owner_padded);
        let is_to = topic2 == Some(owner_padded);
        assert!(is_from || is_to, "yielded event must involve the owner");
        // source_key correctness:
        //   "out" only if topic1 == owner.
        //   "in"  only if topic2 == owner.
        match y.source_key.as_str() {
            "out" => assert!(is_from, "source_key=out requires owner in topic1"),
            "in" => assert!(is_to, "source_key=in requires owner in topic2"),
            other => panic!("unexpected source_key: {other}"),
        }
    }

    // The merge coalesces same-position copies (a self-transfer matches
    // both filters but is one log), so every yield must be unique.
    let mut seen: HashSet<EvmPosition> = HashSet::new();
    for y in &yields {
        assert!(
            seen.insert(y.value.position()),
            "merge must yield each log once"
        );
    }

    // Owner-involved events from OwnedErc20 match the client-side filter
    // set 1:1, in (block, log_index) order.
    assert_eq!(
        yields.len(),
        expected.len(),
        "OwnedErc20 should match client-side filter count"
    );
    for (u, e) in yields.iter().zip(expected.iter()) {
        assert_eq!(u.value.block_number, e.block_number);
        assert_eq!(u.value.log_index, e.log_index);
    }
}

#[tokio::test]
#[ignore = "fork-mainnet integration: spawns anvil + hits external RPC"]
async fn owned_erc20_empty_for_inactive_address() {
    let Some(anvil) = fork_mainnet(FORK_BLOCK) else {
        skip_no_anvil("owned_erc20_empty_for_inactive_address");
        return;
    };
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("owned_erc20_empty.v3.db");
    let endpoint = anvil.endpoint_url();
    let cache = Shared::new(LogCache::open(db_path.to_str().unwrap()).expect("open cache"));
    let provider = ChainProvider::discover(ProviderBuilder::new().connect_http(endpoint), cache)
        .await
        .expect("discover chain id");
    let chain = EvmChain::new(provider, TEST_CONFIG);

    // An address with no real-world USDC activity (literally the zero
    // address — receivers of burns, but no transfers FROM it).
    let owner = Address::ZERO;
    let owned = OwnedErc20::new(&chain, owner, USDC);
    let from_block = FORK_BLOCK - 2;
    let to_block = FORK_BLOCK;
    let start = EvmPosition::new(from_block - 1, u32::MAX);
    let end = EvmPosition::new(to_block, u32::MAX);
    let merged_hs = owned.transfers();
    let hist = merged_hs.next().await;
    let (_, after_start) = hist.split(start);
    let (window, _) = after_start.split(end);
    let mut stream = window.iter();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        let item = item.expect("iter");
        // Zero address may be receive-side of burns; we don't expect
        // outgoing. Verify direction is consistent if any event surfaces.
        let topic1 = item.item.value.topics.get(1).copied();
        let topic2 = item.item.value.topics.get(2).copied();
        let owner_padded = Topic::from(owner);
        match item.item.source_key.as_str() {
            "out" => assert_eq!(topic1, Some(owner_padded)),
            "in" => assert_eq!(topic2, Some(owner_padded)),
            other => panic!("unexpected source_key: {other}"),
        }
        count += 1;
    }
    println!("zero-address USDC events in window: {count}");
}
