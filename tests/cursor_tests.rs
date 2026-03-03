use chainfold::cursor::{BtcCursor, CexCursor, ChainCursor, EvmCursor};

// Compile-time assertions that all cursor types implement ChainCursor bounds.
fn assert_chain_cursor<T: ChainCursor>() {}

#[test]
fn cursor_types_satisfy_chain_cursor_bounds() {
    assert_chain_cursor::<EvmCursor>();
    assert_chain_cursor::<BtcCursor>();
    assert_chain_cursor::<CexCursor>();
}

// --- EvmCursor ordering tests ---

#[test]
fn evm_cursor_same_block_orders_by_log_index() {
    let a = EvmCursor {
        block_number: 100,
        log_index: 5,
    };
    let b = EvmCursor {
        block_number: 100,
        log_index: 10,
    };
    assert!(a < b, "same block: lower log_index must sort before higher");
    assert!(b > a);
    assert_ne!(a, b);
}

#[test]
fn evm_cursor_higher_block_always_greater() {
    let a = EvmCursor {
        block_number: 100,
        log_index: 10,
    };
    let b = EvmCursor {
        block_number: 101,
        log_index: 0,
    };
    assert!(
        a < b,
        "higher block must sort after lower block regardless of log_index"
    );
}

#[test]
fn evm_cursor_equality() {
    let a = EvmCursor {
        block_number: 42,
        log_index: 7,
    };
    let b = EvmCursor {
        block_number: 42,
        log_index: 7,
    };
    assert_eq!(a, b);
}

// --- BtcCursor ordering tests ---

#[test]
fn btc_cursor_same_block_orders_by_tx_index() {
    let a = BtcCursor {
        block_height: 800_000,
        tx_index: 0,
    };
    let b = BtcCursor {
        block_height: 800_000,
        tx_index: 1,
    };
    assert!(a < b, "same block: lower tx_index must sort before higher");
}

#[test]
fn btc_cursor_higher_block_always_greater() {
    let a = BtcCursor {
        block_height: 800_000,
        tx_index: 999,
    };
    let b = BtcCursor {
        block_height: 800_001,
        tx_index: 0,
    };
    assert!(a < b);
}

// --- CexCursor ordering tests ---

#[test]
fn cex_cursor_orders_by_seq_id_not_timestamp() {
    // seq_id=2 sorts after seq_id=1 even when timestamp_ms is earlier.
    // This is the critical correctness property: exchange timestamps are not monotonic.
    let a = CexCursor {
        seq_id: 1,
        timestamp_ms: 1000,
    };
    let b = CexCursor {
        seq_id: 2,
        timestamp_ms: 999,
    };
    assert!(a < b, "ordering must be by seq_id, not timestamp_ms");
}

#[test]
fn cex_cursor_same_seq_id_is_equal_regardless_of_timestamp() {
    let a = CexCursor {
        seq_id: 5,
        timestamp_ms: 100,
    };
    let b = CexCursor {
        seq_id: 5,
        timestamp_ms: 200,
    };
    assert_eq!(
        a, b,
        "same seq_id must be equal regardless of timestamp_ms"
    );
    assert!(!(a < b), "equal cursors must not compare less-than");
    assert!(!(a > b), "equal cursors must not compare greater-than");
}

#[test]
fn cex_cursor_higher_seq_id_is_always_greater() {
    let a = CexCursor {
        seq_id: 10,
        timestamp_ms: 9999,
    };
    let b = CexCursor {
        seq_id: 20,
        timestamp_ms: 1,
    };
    assert!(a < b);
}

// --- Serde round-trip tests ---

#[test]
fn evm_cursor_serde_roundtrip() {
    let cursor = EvmCursor {
        block_number: 12_345_678,
        log_index: 42,
    };
    let json = serde_json::to_string(&cursor).unwrap();
    let restored: EvmCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cursor, restored);
}

#[test]
fn btc_cursor_serde_roundtrip() {
    let cursor = BtcCursor {
        block_height: 800_000,
        tx_index: 17,
    };
    let json = serde_json::to_string(&cursor).unwrap();
    let restored: BtcCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cursor, restored);
}

#[test]
fn cex_cursor_serde_roundtrip() {
    let cursor = CexCursor {
        seq_id: 9_876_543_210,
        timestamp_ms: -500,
    };
    let json = serde_json::to_string(&cursor).unwrap();
    let restored: CexCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cursor, restored);
}

// Verify serde preserves the timestamp_ms even though it doesn't affect ordering.
#[test]
fn cex_cursor_serde_preserves_timestamp_ms() {
    let cursor = CexCursor {
        seq_id: 1,
        timestamp_ms: 42_000,
    };
    let json = serde_json::to_string(&cursor).unwrap();
    let restored: CexCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.timestamp_ms, 42_000);
    assert_eq!(restored.seq_id, 1);
}
