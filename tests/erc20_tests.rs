/// ERC20 balance tracker integration tests.
///
/// Proves the full chainfold stack with a realistic ERC20 Transfer state machine:
/// - Cold start: processes blocks, arrives at correct balances
/// - Restart recovery: recovers from cache, shows same balance
/// - Reorg handling: balance reflects new canonical chain
/// - Store composition: ReducedStore works independently of Engine
/// - OnChange: subscribe receives balance transitions
/// - Historical query: reconstruct_at returns balance at specific height

use std::sync::Arc;

use chainfold::coordinator::recovery::recover;
use chainfold::cursor::EvmCursor;
use chainfold::modules::erc20::{Balances, Erc20Codec, Erc20Machine, Transfer};
use chainfold::storage::InMemoryStorage;
use chainfold::store::query::reconstruct_at;
use chainfold::store::Store;
use chainfold::{Block, Engine, EngineConfig, Event, ReducerCore, ReorgEvent};

// ============================================================
// Mock source for ERC20 tests
// ============================================================

struct MockErc20Source {
    blocks: Vec<Block<EvmCursor, Transfer>>,
}

impl MockErc20Source {
    fn new(blocks: Vec<Block<EvmCursor, Transfer>>) -> Self {
        Self { blocks }
    }
}

#[async_trait::async_trait]
impl chainfold::EventSource for MockErc20Source {
    type Cursor = EvmCursor;
    type Event = Transfer;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &EvmCursor) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<EvmCursor, Transfer>>, Self::Error> {
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.height >= from_height && b.height < to_height)
            .cloned()
            .collect())
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
        Ok(Box::new(futures_util::stream::empty()))
    }
}

// ============================================================
// Test helpers
// ============================================================

/// Create an address from a single byte (for test readability).
fn addr(n: u8) -> [u8; 20] {
    let mut a = [0u8; 20];
    a[0] = n;
    a
}

/// The zero address (mint source).
const ZERO_ADDR: [u8; 20] = [0u8; 20];

/// Create a block hash where byte[0] = n.
fn make_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// Genesis sentinel.
const GENESIS_PARENT: [u8; 32] = [0u8; 32];

/// Build a block with Transfer events.
fn make_transfer_block(
    hash_byte: u8,
    parent_byte: u8,
    height: u64,
    transfers: Vec<(u8, u8, u128)>, // (from_addr_byte, to_addr_byte, amount)
) -> Block<EvmCursor, Transfer> {
    let events = transfers
        .into_iter()
        .enumerate()
        .map(|(i, (from, to, amount))| Event {
            cursor: EvmCursor {
                block_number: height,
                log_index: i as u32,
            },
            data: Transfer {
                from: if from == 0 { ZERO_ADDR } else { addr(from) },
                to: addr(to),
                amount,
            },
        })
        .collect();

    Block {
        block_hash: make_hash(hash_byte),
        parent_hash: if parent_byte == 0 {
            GENESIS_PARENT
        } else {
            make_hash(parent_byte)
        },
        height,
        events,
    }
}

/// Create an engine with InMemoryStorage and the Erc20 state machine.
fn make_erc20_engine(
    storage: Arc<InMemoryStorage>,
    snapshot_every_n: usize,
) -> Engine<Erc20Machine, InMemoryStorage, Erc20Codec> {
    let sm = Arc::new(Erc20Machine);
    let (core, _rx) = ReducerCore::new(sm, Balances::new());
    let config = EngineConfig::new("erc20-test", snapshot_every_n);
    Engine::new(core, storage, Erc20Codec, config)
}

// ============================================================
// Tests
// ============================================================

/// Cold start: engine processes 10 blocks of Transfer events,
/// store.get() returns correct balances.
#[test]
fn test_erc20_cold_start() {
    let storage = Arc::new(InMemoryStorage::new());
    let mut engine = make_erc20_engine(storage, 1000);

    // Build 10 blocks:
    // Block 0: mint 1000 to addr(1)
    // Block 1: mint 2000 to addr(2)
    // Block 2: addr(1) sends 300 to addr(3)
    // Block 3: addr(2) sends 500 to addr(3)
    // Block 4: mint 100 to addr(4)
    // Block 5: addr(3) sends 200 to addr(1)
    // Block 6: mint 50 to addr(5)
    // Block 7: addr(1) sends 100 to addr(5)
    // Block 8: addr(4) sends 50 to addr(2)
    // Block 9: addr(5) sends 25 to addr(1)
    let blocks = vec![
        make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]),
        make_transfer_block(2, 1, 1, vec![(0, 2, 2000)]),
        make_transfer_block(3, 2, 2, vec![(1, 3, 300)]),
        make_transfer_block(4, 3, 3, vec![(2, 3, 500)]),
        make_transfer_block(5, 4, 4, vec![(0, 4, 100)]),
        make_transfer_block(6, 5, 5, vec![(3, 1, 200)]),
        make_transfer_block(7, 6, 6, vec![(0, 5, 50)]),
        make_transfer_block(8, 7, 7, vec![(1, 5, 100)]),
        make_transfer_block(9, 8, 8, vec![(4, 2, 50)]),
        make_transfer_block(10, 9, 9, vec![(5, 1, 25)]),
    ];

    for block in &blocks {
        engine.process_block(block).expect("process_block");
    }

    let balances = engine.state();

    // addr(1): +1000 -300 +200 -100 +25 = 825
    assert_eq!(balances.get(&addr(1)), Some(&825u128), "addr(1) balance");
    // addr(2): +2000 -500 +50 = 1550
    assert_eq!(balances.get(&addr(2)), Some(&1550u128), "addr(2) balance");
    // addr(3): +300 +500 -200 = 600
    assert_eq!(balances.get(&addr(3)), Some(&600u128), "addr(3) balance");
    // addr(4): +100 -50 = 50
    assert_eq!(balances.get(&addr(4)), Some(&50u128), "addr(4) balance");
    // addr(5): +50 +100 -25 = 125
    assert_eq!(balances.get(&addr(5)), Some(&125u128), "addr(5) balance");
}

/// Restart recovery: process blocks, drop engine, create new engine with
/// same storage, recover(), verify balance matches.
#[tokio::test]
async fn test_erc20_restart_recovery() {
    let storage = Arc::new(InMemoryStorage::new());

    // Phase 1: process blocks.
    let mut engine1 = make_erc20_engine(storage.clone(), 3);

    let blocks = vec![
        make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]),
        make_transfer_block(2, 1, 1, vec![(0, 2, 2000)]),
        make_transfer_block(3, 2, 2, vec![(1, 3, 300)]),
        make_transfer_block(4, 3, 3, vec![(2, 3, 500)]),
        make_transfer_block(5, 4, 4, vec![(0, 4, 100)]),
    ];

    for block in &blocks {
        engine1.process_block(block).expect("process_block");
    }

    let original_balances = engine1.state();
    drop(engine1);

    // Phase 2: new engine, same storage, recover.
    let mut engine2 = make_erc20_engine(storage.clone(), 3);
    assert!(engine2.state().is_empty(), "new engine should start empty");

    let source = MockErc20Source::new(blocks);
    recover(&mut engine2, &source, 4).await.expect("recovery");

    let recovered_balances = engine2.state();
    assert_eq!(
        original_balances, recovered_balances,
        "recovered balances must match original"
    );
}

/// Reorg handling: process blocks on branch A, feed reorg block on branch B,
/// verify balance reflects branch B transfers, not branch A.
#[test]
fn test_erc20_reorg_handling() {
    let storage = Arc::new(InMemoryStorage::new());
    let mut engine = make_erc20_engine(storage, 1000);

    // Chain A: block 0 (mint 1000 to addr(1)), block 1 (mint 2000 to addr(2)),
    //          block 2 (addr(1)->addr(2) 500) [branch A]
    let block_0 = make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]);
    let block_1 = make_transfer_block(2, 1, 1, vec![(0, 2, 2000)]);
    let block_2a = make_transfer_block(3, 2, 2, vec![(1, 2, 500)]); // branch A

    let mut no_reorg = |_: ReorgEvent| {};
    engine.process_or_reorg(&block_0, &mut no_reorg).unwrap();
    engine.process_or_reorg(&block_1, &mut no_reorg).unwrap();
    engine.process_or_reorg(&block_2a, &mut no_reorg).unwrap();

    // Before reorg: addr(1)=500, addr(2)=2500
    let pre = engine.state();
    assert_eq!(pre.get(&addr(1)), Some(&500), "pre-reorg addr(1)");
    assert_eq!(pre.get(&addr(2)), Some(&2500), "pre-reorg addr(2)");

    // Reorg: block 2b (addr(2)->addr(1) 300) replaces block 2a
    let block_2b = make_transfer_block(4, 2, 2, vec![(2, 1, 300)]); // branch B

    let mut reorg_count = 0;
    engine
        .process_or_reorg(&block_2b, &mut |_| reorg_count += 1)
        .unwrap();

    assert_eq!(reorg_count, 1, "reorg should fire once");

    // After reorg: addr(1)=1000+300=1300, addr(2)=2000-300=1700
    let post = engine.state();
    assert_eq!(post.get(&addr(1)), Some(&1300), "post-reorg addr(1)");
    assert_eq!(post.get(&addr(2)), Some(&1700), "post-reorg addr(2)");
}

/// Store composition: ReducedStore tracks balance independently of Engine lifecycle.
#[test]
fn test_erc20_store_composition() {
    let storage = Arc::new(InMemoryStorage::new());
    let mut engine = make_erc20_engine(storage, 1000);

    // Get store BEFORE processing blocks.
    let store = engine.reduced_store();
    assert!(store.get().is_empty(), "store should start empty");

    // Process blocks.
    let block_0 = make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]);
    let block_1 = make_transfer_block(2, 1, 1, vec![(1, 2, 400)]);
    engine.process_block(&block_0).unwrap();
    engine.process_block(&block_1).unwrap();

    // Store reflects updated balance without calling engine.state().
    let balances = store.get();
    assert_eq!(balances.get(&addr(1)), Some(&600), "store addr(1)");
    assert_eq!(balances.get(&addr(2)), Some(&400), "store addr(2)");
}

/// OnChange: subscribe to balance store, process blocks, verify each
/// balance transition received.
#[tokio::test]
async fn test_erc20_on_change() {
    let storage = Arc::new(InMemoryStorage::new());
    let mut engine = make_erc20_engine(storage, 1000);

    let store = engine.reduced_store();
    let mut rx = store.subscribe();

    // Process block 0: mint 1000 to addr(1)
    let block_0 = make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]);
    engine.process_block(&block_0).unwrap();

    rx.changed().await.expect("should see block 0");
    let snap1 = rx.borrow().clone();
    assert_eq!(snap1.get(&addr(1)), Some(&1000), "after block 0");

    // Process block 1: addr(1) sends 300 to addr(2)
    let block_1 = make_transfer_block(2, 1, 1, vec![(1, 2, 300)]);
    engine.process_block(&block_1).unwrap();

    rx.changed().await.expect("should see block 1");
    let snap2 = rx.borrow().clone();
    assert_eq!(snap2.get(&addr(1)), Some(&700), "after block 1 addr(1)");
    assert_eq!(snap2.get(&addr(2)), Some(&300), "after block 1 addr(2)");
}

/// Historical query: after processing blocks, reconstruct_at(height=5)
/// returns balance as of block 5 only.
#[test]
fn test_erc20_historical_at() {
    let storage = Arc::new(InMemoryStorage::new());
    let mut engine = make_erc20_engine(storage.clone(), 1000);

    // 10 blocks with known transfers
    let blocks = vec![
        make_transfer_block(1, 0, 0, vec![(0, 1, 1000)]),
        make_transfer_block(2, 1, 1, vec![(0, 2, 2000)]),
        make_transfer_block(3, 2, 2, vec![(1, 3, 300)]),
        make_transfer_block(4, 3, 3, vec![(2, 3, 500)]),
        make_transfer_block(5, 4, 4, vec![(0, 4, 100)]),
        make_transfer_block(6, 5, 5, vec![(3, 1, 200)]),  // block 5
        make_transfer_block(7, 6, 6, vec![(0, 5, 50)]),
        make_transfer_block(8, 7, 7, vec![(1, 5, 100)]),
        make_transfer_block(9, 8, 8, vec![(4, 2, 50)]),
        make_transfer_block(10, 9, 9, vec![(5, 1, 25)]),
    ];

    for block in &blocks {
        engine.process_block(block).expect("process_block");
    }

    // Reconstruct at height 5 (through block 5, which is the 6th block).
    // Blocks 0-5:
    // addr(1): +1000 -300 +200 = 900
    // addr(2): +2000 -500 = 1500
    // addr(3): +300 +500 -200 = 600
    // addr(4): +100
    let state_at_5 = reconstruct_at(
        storage.as_ref(),
        &Erc20Codec,
        &Erc20Machine,
        &Balances::new(),
        "erc20-test",
        5,
    )
    .unwrap();

    assert_eq!(state_at_5.get(&addr(1)), Some(&900), "at(5) addr(1)");
    assert_eq!(state_at_5.get(&addr(2)), Some(&1500), "at(5) addr(2)");
    assert_eq!(state_at_5.get(&addr(3)), Some(&600), "at(5) addr(3)");
    assert_eq!(state_at_5.get(&addr(4)), Some(&100), "at(5) addr(4)");
    // addr(5) doesn't exist yet at height 5
    assert_eq!(state_at_5.get(&addr(5)), None, "at(5) addr(5) should not exist");
}
