/// Storage integration tests for both SqliteStorage and InMemoryStorage.
///
/// Task 1 tests: Type construction (RED → verify types exist before impl)
/// Task 2 tests: Storage operations (coverage, events, snapshots, anchors)
use chainfold::storage::{Anchor, InMemoryStorage, Snapshot, SqliteStorage, Storage, StoredEvent};

// ============================================================
// Task 1: Type construction tests
// ============================================================

#[test]
fn stored_event_construction() {
    // EvmCursor maps to (major=block_number as i64, minor=log_index as i64)
    let event = StoredEvent {
        source_id: "evm:mainnet".to_string(),
        cursor_major: 18_000_000,
        cursor_minor: 5,
        payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
    };
    assert_eq!(event.source_id, "evm:mainnet");
    assert_eq!(event.cursor_major, 18_000_000);
    assert_eq!(event.cursor_minor, 5);
    assert_eq!(event.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn snapshot_construction() {
    let snap = Snapshot {
        source_id: "evm:mainnet".to_string(),
        cursor_major: 18_000_000,
        cursor_minor: 0,
        payload: vec![1, 2, 3],
        created_at: 1_700_000_000,
    };
    assert_eq!(snap.source_id, "evm:mainnet");
    assert_eq!(snap.cursor_major, 18_000_000);
    assert_eq!(snap.created_at, 1_700_000_000);
}

#[test]
fn anchor_construction() {
    let anchor = Anchor {
        source_id: "evm:mainnet".to_string(),
        name: "finalized_head".to_string(),
        cursor_major: 17_999_000,
        cursor_minor: 0,
    };
    assert_eq!(anchor.source_id, "evm:mainnet");
    assert_eq!(anchor.name, "finalized_head");
    assert_eq!(anchor.cursor_major, 17_999_000);
}

// ============================================================
// Task 2: Storage operation tests — parameterized over backend
// ============================================================

/// Run all storage tests against a given implementation.
fn run_storage_tests<S>(storage: S)
where
    S: Storage,
    S::Error: std::fmt::Debug,
{
    test_write_events_returns_count(&storage);
    test_write_events_dedup_returns_zero(&storage);
    test_events_in_range(&storage);
    test_snapshot_round_trip(&storage);
    test_snapshots_before(&storage);
    test_anchor_round_trip_and_upsert(&storage);
    test_coverage_disjoint_ranges(&storage);
    test_coverage_overlapping_merges(&storage);
    test_coverage_adjacent_merges(&storage);
    test_is_fully_cached(&storage);
    test_cached_tip(&storage);
}

fn test_write_events_returns_count<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    let events = vec![
        StoredEvent {
            source_id: "src_count".to_string(),
            cursor_major: 1,
            cursor_minor: 0,
            payload: vec![1],
        },
        StoredEvent {
            source_id: "src_count".to_string(),
            cursor_major: 2,
            cursor_minor: 0,
            payload: vec![2],
        },
    ];
    let inserted = s.write_events("src_count", &events).unwrap();
    assert_eq!(inserted, 2, "should insert 2 new events");
}

fn test_write_events_dedup_returns_zero<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // Write an event
    let events = vec![StoredEvent {
        source_id: "src_dedup".to_string(),
        cursor_major: 10,
        cursor_minor: 0,
        payload: vec![0xAA],
    }];
    let first = s.write_events("src_dedup", &events).unwrap();
    assert_eq!(first, 1);

    // Write same event again — duplicate (source_id, cursor_major, cursor_minor)
    let events2 = vec![StoredEvent {
        source_id: "src_dedup".to_string(),
        cursor_major: 10,
        cursor_minor: 0,
        payload: vec![0xBB], // different payload, same cursor — still a dup
    }];
    let second = s.write_events("src_dedup", &events2).unwrap();
    assert_eq!(second, 0, "duplicate event should return 0 inserted count");
}

fn test_events_in_range<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // Insert events at cursors (100,0), (200,0), (300,0), (400,0)
    let events: Vec<StoredEvent> = [100i64, 200, 300, 400]
        .iter()
        .map(|&major| StoredEvent {
            source_id: "src_range".to_string(),
            cursor_major: major,
            cursor_minor: 0,
            payload: vec![major as u8],
        })
        .collect();
    s.write_events("src_range", &events).unwrap();

    // Query [200, 400) — should return 200 and 300 only (exclusive end)
    let result = s
        .events_in_range("src_range", (200, 0), (400, 0))
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].cursor_major, 200);
    assert_eq!(result[1].cursor_major, 300);
}

fn test_snapshot_round_trip<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    let snap = Snapshot {
        source_id: "src_snap".to_string(),
        cursor_major: 5000,
        cursor_minor: 0,
        payload: vec![0x01, 0x02, 0x03],
        created_at: 9999,
    };
    s.write_snapshot(&snap).unwrap();
    let latest = s.latest_snapshot("src_snap").unwrap();
    assert!(latest.is_some(), "should return stored snapshot");
    let got = latest.unwrap();
    assert_eq!(got.cursor_major, 5000);
    assert_eq!(got.payload, vec![0x01, 0x02, 0x03]);
    assert_eq!(got.created_at, 9999);
}

fn test_snapshots_before<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // Write snapshots at (1000,0), (2000,0), (3000,0)
    for &major in &[1000i64, 2000, 3000] {
        s.write_snapshot(&Snapshot {
            source_id: "src_snap_before".to_string(),
            cursor_major: major,
            cursor_minor: 0,
            payload: vec![major as u8],
            created_at: major,
        })
        .unwrap();
    }
    // snapshots_before((2500, 0)) should return (2000,0) and (1000,0) in reverse order
    let result = s
        .snapshots_before("src_snap_before", (2500, 0))
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].cursor_major, 2000, "most recent first");
    assert_eq!(result[1].cursor_major, 1000);
}

fn test_anchor_round_trip_and_upsert<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // Write anchor
    let anchor = Anchor {
        source_id: "src_anchor".to_string(),
        name: "head".to_string(),
        cursor_major: 1000,
        cursor_minor: 0,
    };
    s.write_anchor(&anchor).unwrap();

    let got = s.read_anchor("src_anchor", "head").unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().cursor_major, 1000);

    // Upsert: write same (source_id, name) with new cursor
    let updated = Anchor {
        source_id: "src_anchor".to_string(),
        name: "head".to_string(),
        cursor_major: 2000,
        cursor_minor: 5,
    };
    s.write_anchor(&updated).unwrap();

    let got2 = s.read_anchor("src_anchor", "head").unwrap();
    assert!(got2.is_some());
    let got2 = got2.unwrap();
    assert_eq!(got2.cursor_major, 2000, "upsert should update cursor");
    assert_eq!(got2.cursor_minor, 5);
}

fn test_coverage_disjoint_ranges<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // Add two disjoint ranges — they should remain separate
    s.add_coverage("src_cov_disjoint", (100, 0), (200, 0))
        .unwrap();
    s.add_coverage("src_cov_disjoint", (300, 0), (400, 0))
        .unwrap();

    // Range [150, 160) is cached — inside first interval
    assert!(
        s.is_fully_cached("src_cov_disjoint", (150, 0), (160, 0))
            .unwrap()
    );
    // Range [250, 260) is NOT cached — in the gap
    assert!(
        !s.is_fully_cached("src_cov_disjoint", (250, 0), (260, 0))
            .unwrap()
    );
    // Range [100, 400) spans both — NOT fully cached due to gap
    assert!(
        !s.is_fully_cached("src_cov_disjoint", (100, 0), (400, 0))
            .unwrap()
    );
}

fn test_coverage_overlapping_merges<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // [100,300) then [200,400) — overlap, should merge to [100,400)
    s.add_coverage("src_cov_overlap", (100, 0), (300, 0))
        .unwrap();
    s.add_coverage("src_cov_overlap", (200, 0), (400, 0))
        .unwrap();

    // Entire merged range should be cached
    assert!(
        s.is_fully_cached("src_cov_overlap", (100, 0), (400, 0))
            .unwrap()
    );
}

fn test_coverage_adjacent_merges<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // [100,200) then [200,300) — adjacent (end of first == start of second), merge to [100,300)
    s.add_coverage("src_cov_adjacent", (100, 0), (200, 0))
        .unwrap();
    s.add_coverage("src_cov_adjacent", (200, 0), (300, 0))
        .unwrap();

    assert!(
        s.is_fully_cached("src_cov_adjacent", (100, 0), (300, 0))
            .unwrap()
    );
}

fn test_is_fully_cached<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    s.add_coverage("src_cached", (1000, 0), (2000, 0)).unwrap();

    // Range within coverage → true
    assert!(
        s.is_fully_cached("src_cached", (1200, 0), (1800, 0))
            .unwrap()
    );
    // Range extending beyond coverage end → false
    assert!(
        !s.is_fully_cached("src_cached", (1500, 0), (2500, 0))
            .unwrap()
    );
    // Range before coverage start → false
    assert!(
        !s.is_fully_cached("src_cached", (500, 0), (1500, 0))
            .unwrap()
    );
    // No coverage for different source → false
    assert!(
        !s.is_fully_cached("other_source", (1000, 0), (2000, 0))
            .unwrap()
    );
}

fn test_cached_tip<S: Storage>(s: &S)
where
    S::Error: std::fmt::Debug,
{
    // No coverage yet → None
    assert!(s.cached_tip("src_tip_none").unwrap().is_none());

    s.add_coverage("src_tip", (100, 0), (500, 0)).unwrap();
    s.add_coverage("src_tip", (700, 0), (900, 0)).unwrap();

    let tip = s.cached_tip("src_tip").unwrap();
    assert_eq!(tip, Some((900i64, 0i64)), "tip should be the highest range_end");
}

// ============================================================
// Parameterized runners for both backends
// ============================================================

#[test]
fn sqlite_storage_all_tests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let storage = SqliteStorage::open(path.to_str().unwrap()).unwrap();
    run_storage_tests(storage);
}

#[test]
fn in_memory_storage_all_tests() {
    let storage = InMemoryStorage::new();
    run_storage_tests(storage);
}
