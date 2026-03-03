/// Cursor for centralized exchange (CEX) event streams.
///
/// Ordering: by seq_id ONLY. timestamp_ms is metadata and does NOT affect comparison.
///
/// CRITICAL: Exchange timestamps are not monotonic (clock skew, corrections, historical backfill).
/// Ordering by timestamp would produce incorrect sort results. seq_id is the authoritative
/// ordering key assigned by the exchange's event sequencing system.
///
/// Equality: two CexCursors with the same seq_id are equal regardless of timestamp_ms.
/// This is intentional — seq_id uniquely identifies a position in the event stream.
///
/// Ord and PartialOrd are implemented manually to enforce the seq_id-only invariant.
/// Deriving Ord would risk subtle breakage if fields are reordered during refactoring.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CexCursor {
    pub seq_id: u64,
    pub timestamp_ms: i64,
}

impl PartialEq for CexCursor {
    fn eq(&self, other: &Self) -> bool {
        self.seq_id == other.seq_id
    }
}

impl Eq for CexCursor {}

impl PartialOrd for CexCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CexCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // timestamp_ms deliberately excluded — only seq_id determines order.
        self.seq_id.cmp(&other.seq_id)
    }
}
