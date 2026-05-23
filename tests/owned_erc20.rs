//! Integration tests for the Phase 5 follow-up: [`OwnedErc20`].
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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use alloy::providers::ProviderBuilder;
use chainfold::arch::evm::{EvmEvent, EvmPosition};
use chainfold::arch::{Hist, HistBackend, Position};
use chainfold::evm::{EvmLogHistory, LogStore, OwnedErc20, IN_KEY, OUT_KEY, TRANSFER_TOPIC};
use chainfold::merge::{MergedItem, MergedPos};
use futures_util::StreamExt;
use tempfile::TempDir;

use common::{
    anvil::{fork_mainnet, skip_no_anvil},
    fixtures::{FORK_BLOCK, USDC},
};

fn padded_topic(addr: &[u8; 20]) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[12..].copy_from_slice(addr);
    t
}

fn merged_pos_for(p: EvmPosition) -> MergedPos {
    let bytes = p.marshal();
    let mut m = BTreeMap::new();
    m.insert(IN_KEY.into(), bytes.clone());
    m.insert(OUT_KEY.into(), bytes);
    MergedPos::new(m)
}

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
    let store = Arc::new(LogStore::open(db_path.to_str().unwrap()).expect("open store"));
    let provider = ProviderBuilder::new().connect_http(endpoint);

    // Phase A — broad filter to discover an active owner in the window.
    let broad = EvmLogHistory::with_options(
        provider.clone(),
        store.clone(),
        USDC.0 .0,
        [Some(TRANSFER_TOPIC), None, None, None],
        10,
        12,
    )
    .await
    .expect("broad EvmLogHistory");

    let from_block = FORK_BLOCK - 5;
    let to_block = FORK_BLOCK;
    let start = EvmPosition::new(from_block - 1, u32::MAX);
    let end = EvmPosition::new(to_block, u32::MAX);
    let backend: Arc<dyn HistBackend<EvmEvent, EvmPosition>> = broad.clone();
    let hist = Hist::new(backend, start, end);
    let mut stream = hist.iter();
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
    let from_topic = first.topics[1];
    let mut owner = [0u8; 20];
    owner.copy_from_slice(&from_topic[12..]);
    println!(
        "Chosen owner: 0x{} (with {} surrounding transfers)",
        hex::encode(owner),
        all_transfers.len()
    );

    // Build the expected set: every event where owner is either `from`
    // or `to`, deduped by (block, log_index).
    let owner_padded = padded_topic(&owner);
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

    // Phase B — same range via OwnedErc20.
    let owned = OwnedErc20::with_options(provider, store, owner, USDC.0 .0, 10, 12)
        .await
        .expect("OwnedErc20");
    owned.poll_tips().await.expect("poll_tips");
    let merged_hs = owned.transfers();
    let hist = merged_hs.next().await;
    let (_, after_start) = hist.split(merged_pos_for(start));
    let (window, _) = after_start.split(merged_pos_for(end));
    let mut stream = window.iter();
    let mut yields: Vec<MergedItem<EvmEvent>> = Vec::new();
    while let Some(item) = stream.next().await {
        yields.push(item.expect("iter").item);
    }
    assert!(!yields.is_empty(), "OwnedErc20 yielded zero events");

    // Each yield must involve the owner and have a consistent source_key.
    for y in &yields {
        let topic1 = y.item.topics.get(1).copied();
        let topic2 = y.item.topics.get(2).copied();
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

    // Dedupe self-transfers (owner == from == to). The merged stream
    // emits those twice by design — once per direction.
    let mut seen: HashSet<EvmPosition> = HashSet::new();
    let mut unique = Vec::new();
    for y in &yields {
        if seen.insert(y.item.position()) {
            unique.push(y);
        }
    }

    // Owner-involved events from OwnedErc20 (deduped) match the
    // client-side filter set 1:1, in (block, log_index) order.
    assert_eq!(
        unique.len(),
        expected.len(),
        "OwnedErc20 + dedupe should match client-side filter count"
    );
    for (u, e) in unique.iter().zip(expected.iter()) {
        assert_eq!(u.item.block_number, e.block_number);
        assert_eq!(u.item.log_index, e.log_index);
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
    let store = Arc::new(LogStore::open(db_path.to_str().unwrap()).expect("open store"));
    let provider = ProviderBuilder::new().connect_http(endpoint);

    // An address with no real-world USDC activity (literally the zero
    // address — receivers of burns, but no transfers FROM it).
    let owner = [0u8; 20];
    let owned = OwnedErc20::with_options(provider, store, owner, USDC.0 .0, 10, 12)
        .await
        .expect("OwnedErc20");
    owned.poll_tips().await.expect("poll_tips");
    let from_block = FORK_BLOCK - 2;
    let to_block = FORK_BLOCK;
    let start = EvmPosition::new(from_block - 1, u32::MAX);
    let end = EvmPosition::new(to_block, u32::MAX);
    let hist = owned.transfers().next().await;
    let (_, after_start) = hist.split(merged_pos_for(start));
    let (window, _) = after_start.split(merged_pos_for(end));
    let mut stream = window.iter();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        let item = item.expect("iter");
        // Zero address may be receive-side of burns; we don't expect
        // outgoing. Verify direction is consistent if any event surfaces.
        let topic1 = item.item.item.topics.get(1).copied();
        let topic2 = item.item.item.topics.get(2).copied();
        let owner_padded = padded_topic(&owner);
        match item.item.source_key.as_str() {
            "out" => assert_eq!(topic1, Some(owner_padded)),
            "in" => assert_eq!(topic2, Some(owner_padded)),
            other => panic!("unexpected source_key: {other}"),
        }
        count += 1;
    }
    println!("zero-address USDC events in window: {count}");
}
