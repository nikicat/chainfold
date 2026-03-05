/// Unit tests for InMemoryStorage — block DAG semantics.
///
/// These tests validate that InMemoryStorage correctly implements the Storage trait:
/// block-level deduplication, canonical tip tracking, parent pointer traversal,
/// snapshot finding, chain collection, and source isolation.
use chainfold::storage::InMemoryStorage;
use chainfold::storage::Storage;

// ============================================================
// Test helpers
// ============================================================

/// Create a block hash where byte[0] is `n`. All other bytes are 0.
fn make_block_hash(n: u8) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = n;
    h
}

/// Genesis sentinel: the all-zeros hash marks "no parent" (genesis block).
const GENESIS: [u8; 32] = [0u8; 32];

// ============================================================
// write_block / get_canonical_tip
// ============================================================

#[test]
fn test_write_block_inserts_and_returns_true() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    let result = storage
        .write_block("src1", &hash_a, &GENESIS, 0, &[])
        .expect("write_block should not fail");

    assert!(result, "new block should return true");

    let tip = storage
        .get_canonical_tip("src1")
        .expect("get_canonical_tip should not fail");
    assert_eq!(tip, Some(hash_a), "canonical tip should be updated to the written block");
}

#[test]
fn test_write_block_duplicate_returns_false() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    let first = storage
        .write_block("src1", &hash_a, &GENESIS, 0, &[])
        .expect("first write should not fail");
    let second = storage
        .write_block("src1", &hash_a, &GENESIS, 0, &[])
        .expect("second write should not fail");

    assert!(first, "first write should return true");
    assert!(!second, "duplicate write should return false");
}

#[test]
fn test_canonical_tip_none_when_empty() {
    let storage = InMemoryStorage::new();

    let tip = storage
        .get_canonical_tip("src1")
        .expect("get_canonical_tip should not fail");
    assert_eq!(tip, None, "empty storage should return None for canonical tip");
}

#[test]
fn test_canonical_tip_updates_on_each_block() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);

    storage
        .write_block("src1", &hash_a, &GENESIS, 0, &[])
        .expect("write block A");
    let tip_after_a = storage
        .get_canonical_tip("src1")
        .expect("tip after A");
    assert_eq!(tip_after_a, Some(hash_a));

    storage
        .write_block("src1", &hash_b, &hash_a, 1, &[])
        .expect("write block B");
    let tip_after_b = storage
        .get_canonical_tip("src1")
        .expect("tip after B");
    assert_eq!(tip_after_b, Some(hash_b), "canonical tip should advance to B");
}

// ============================================================
// get_block_height
// ============================================================

#[test]
fn test_get_block_height() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let unknown = make_block_hash(99);

    storage
        .write_block("src1", &hash_a, &GENESIS, 42, &[])
        .expect("write block");

    let height = storage.get_block_height(&hash_a).expect("get_block_height");
    assert_eq!(height, Some(42), "should return the stored height");

    let none = storage.get_block_height(&unknown).expect("unknown hash");
    assert_eq!(none, None, "unknown block_hash should return None");
}

// ============================================================
// write_snapshot / find_nearest_snapshot
// ============================================================

#[test]
fn test_write_and_find_snapshot() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);
    let hash_c = make_block_hash(3);

    // Chain: genesis <- A <- B <- C
    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_block("src1", &hash_b, &hash_a, 1, &[]).unwrap();
    storage.write_block("src1", &hash_c, &hash_b, 2, &[]).unwrap();

    let snapshot_payload = b"state-at-B";
    storage
        .write_snapshot("src1", &hash_b, snapshot_payload)
        .expect("write_snapshot");

    let found = storage
        .find_nearest_snapshot("src1", &hash_c)
        .expect("find_nearest_snapshot");

    assert!(found.is_some(), "should find a snapshot walking back from C");
    let (found_hash, found_payload) = found.unwrap();
    assert_eq!(found_hash, hash_b, "snapshot should be at block B");
    assert_eq!(found_payload, snapshot_payload);
}

#[test]
fn test_find_nearest_snapshot_none() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);

    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_block("src1", &hash_b, &hash_a, 1, &[]).unwrap();

    // No snapshots written
    let found = storage
        .find_nearest_snapshot("src1", &hash_b)
        .expect("find_nearest_snapshot");
    assert_eq!(found, None, "should return None when no snapshot exists");
}

#[test]
fn test_find_nearest_snapshot_walks_past_blocks() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);
    let hash_c = make_block_hash(3);
    let hash_d = make_block_hash(4);
    let hash_e = make_block_hash(5);

    // Chain: genesis <- A <- B <- C <- D <- E
    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_block("src1", &hash_b, &hash_a, 1, &[]).unwrap();
    storage.write_block("src1", &hash_c, &hash_b, 2, &[]).unwrap();
    storage.write_block("src1", &hash_d, &hash_c, 3, &[]).unwrap();
    storage.write_block("src1", &hash_e, &hash_d, 4, &[]).unwrap();

    // Only B has a snapshot
    storage.write_snapshot("src1", &hash_b, b"state-at-B").unwrap();

    let found = storage
        .find_nearest_snapshot("src1", &hash_e)
        .expect("find_nearest_snapshot");

    assert!(found.is_some());
    let (found_hash, _) = found.unwrap();
    assert_eq!(found_hash, hash_b, "should walk D->C->B and find snapshot at B");
}

#[test]
fn test_find_nearest_snapshot_at_tip_itself() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_snapshot("src1", &hash_a, b"state-at-A").unwrap();

    let found = storage
        .find_nearest_snapshot("src1", &hash_a)
        .expect("find_nearest_snapshot");

    assert!(found.is_some());
    let (found_hash, _) = found.unwrap();
    assert_eq!(found_hash, hash_a, "should find snapshot at the tip itself");
}

// ============================================================
// collect_canonical_chain
// ============================================================

#[test]
fn test_collect_canonical_chain_forward_order() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);
    let hash_c = make_block_hash(3);
    let hash_d = make_block_hash(4);

    // Chain: genesis <- A <- B <- C <- D
    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_block("src1", &hash_b, &hash_a, 1, &[]).unwrap();
    storage.write_block("src1", &hash_c, &hash_b, 2, &[]).unwrap();
    storage.write_block("src1", &hash_d, &hash_c, 3, &[]).unwrap();

    let chain = storage
        .collect_canonical_chain(&hash_a, &hash_d)
        .expect("collect_canonical_chain");

    // from_hash (A) is exclusive, to_hash (D) is inclusive
    assert_eq!(chain, vec![hash_b, hash_c, hash_d]);
}

#[test]
fn test_collect_canonical_chain_adjacent() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);

    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();
    storage.write_block("src1", &hash_b, &hash_a, 1, &[]).unwrap();

    let chain = storage
        .collect_canonical_chain(&hash_a, &hash_b)
        .expect("collect_canonical_chain");

    assert_eq!(chain, vec![hash_b]);
}

#[test]
fn test_collect_canonical_chain_same_hash() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    storage.write_block("src1", &hash_a, &GENESIS, 0, &[]).unwrap();

    let chain = storage
        .collect_canonical_chain(&hash_a, &hash_a)
        .expect("collect_canonical_chain");

    assert_eq!(chain, Vec::<[u8; 32]>::new(), "from == to should return empty vec");
}

// ============================================================
// load_block_events
// ============================================================

#[test]
fn test_load_block_events_returns_events() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    let events: &[(&[u8], &[u8])] = &[
        (b"cursor-1", b"payload-1"),
        (b"cursor-2", b"payload-2"),
        (b"cursor-3", b"payload-3"),
    ];

    storage
        .write_block("src1", &hash_a, &GENESIS, 0, events)
        .expect("write_block with events");

    let loaded = storage
        .load_block_events(&hash_a)
        .expect("load_block_events");

    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[0], (b"cursor-1".to_vec(), b"payload-1".to_vec()));
    assert_eq!(loaded[1], (b"cursor-2".to_vec(), b"payload-2".to_vec()));
    assert_eq!(loaded[2], (b"cursor-3".to_vec(), b"payload-3".to_vec()));
}

#[test]
fn test_load_block_events_empty_for_unknown_block() {
    let storage = InMemoryStorage::new();
    let unknown = make_block_hash(99);

    let loaded = storage
        .load_block_events(&unknown)
        .expect("load_block_events");

    assert_eq!(loaded, vec![], "unknown block should return empty vec");
}

// ============================================================
// Source isolation
// ============================================================

#[test]
fn test_source_isolation() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);
    let hash_b = make_block_hash(2);

    // Source 1 writes block A
    storage
        .write_block("source-1", &hash_a, &GENESIS, 0, &[])
        .expect("write block A for source-1");

    // Source 2 writes block B
    storage
        .write_block("source-2", &hash_b, &GENESIS, 0, &[])
        .expect("write block B for source-2");

    let tip_1 = storage.get_canonical_tip("source-1").unwrap();
    let tip_2 = storage.get_canonical_tip("source-2").unwrap();

    assert_eq!(tip_1, Some(hash_a), "source-1 tip should be A");
    assert_eq!(tip_2, Some(hash_b), "source-2 tip should be B, not A");
    assert_ne!(tip_1, tip_2, "sources should be isolated");

    // Source-1 has no snapshot for the snapshot source-2 wrote
    storage
        .write_snapshot("source-2", &hash_b, b"state-2")
        .unwrap();
    let snap_1 = storage
        .find_nearest_snapshot("source-1", &hash_a)
        .unwrap();
    assert_eq!(snap_1, None, "source-1 should not see source-2's snapshot");
}

// ============================================================
// Block-level deduplication (INGEST-04)
// ============================================================

#[test]
fn test_block_level_dedup_skips_all_events() {
    let storage = InMemoryStorage::new();
    let hash_a = make_block_hash(1);

    let first_events: &[(&[u8], &[u8])] = &[
        (b"cursor-1", b"original-payload"),
    ];
    let second_events: &[(&[u8], &[u8])] = &[
        (b"cursor-2", b"replacement-payload"),
    ];

    // First write succeeds
    let first = storage
        .write_block("src1", &hash_a, &GENESIS, 0, first_events)
        .expect("first write");
    assert!(first);

    // Second write with same block_hash but different events — should be a no-op
    let second = storage
        .write_block("src1", &hash_a, &GENESIS, 0, second_events)
        .expect("second write");
    assert!(!second, "duplicate should return false");

    // Events should be from the first write only
    let loaded = storage.load_block_events(&hash_a).expect("load_block_events");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].1, b"original-payload".to_vec(),
        "second write should be entirely skipped, original events preserved");
}
