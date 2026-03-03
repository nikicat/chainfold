/// In-memory storage implementation for testing.
///
/// Provides identical semantics to SqliteStorage — same deduplication,
/// same coverage merge logic, same ordering — without filesystem I/O.
///
/// Use this as a test double when testing the recovery pipeline and coordinator.
use std::collections::HashMap;
use std::sync::Mutex;

use super::{Anchor, Snapshot, Storage, StoredEvent};

// ============================================================
// Error type
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum MemoryStorageError {
    #[error("internal lock poisoned")]
    Poisoned,
}

// ============================================================
// Internal state
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoverageInterval {
    source_id: String,
    start: (i64, i64),
    end: (i64, i64),
}

#[derive(Debug, Default)]
struct Inner {
    events: Vec<StoredEvent>,
    snapshots: Vec<Snapshot>,
    anchors: HashMap<(String, String), Anchor>,
    coverage: Vec<CoverageInterval>,
}

// ============================================================
// InMemoryStorage
// ============================================================

pub struct InMemoryStorage {
    inner: Mutex<Inner>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for InMemoryStorage {
    type Error = MemoryStorageError;

    fn write_events(&self, _source_id: &str, events: &[StoredEvent]) -> Result<usize, Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        let mut inserted = 0usize;
        for ev in events {
            // Deduplicate on (source_id, cursor_major, cursor_minor)
            let is_dup = inner.events.iter().any(|e| {
                e.source_id == ev.source_id
                    && e.cursor_major == ev.cursor_major
                    && e.cursor_minor == ev.cursor_minor
            });
            if !is_dup {
                inner.events.push(ev.clone());
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    fn events_in_range(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        let mut result: Vec<StoredEvent> = inner
            .events
            .iter()
            .filter(|e| {
                let cursor = (e.cursor_major, e.cursor_minor);
                e.source_id == source_id && cursor >= from && cursor < to
            })
            .cloned()
            .collect();
        result.sort_by(|a, b| {
            (a.cursor_major, a.cursor_minor).cmp(&(b.cursor_major, b.cursor_minor))
        });
        Ok(result)
    }

    fn write_snapshot(&self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        inner.snapshots.push(snapshot.clone());
        Ok(())
    }

    fn latest_snapshot(&self, source_id: &str) -> Result<Option<Snapshot>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        let best = inner
            .snapshots
            .iter()
            .filter(|s| s.source_id == source_id)
            .max_by_key(|s| (s.cursor_major, s.cursor_minor));
        Ok(best.cloned())
    }

    fn snapshots_before(
        &self,
        source_id: &str,
        cursor: (i64, i64),
    ) -> Result<Vec<Snapshot>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        let mut result: Vec<Snapshot> = inner
            .snapshots
            .iter()
            .filter(|s| {
                s.source_id == source_id
                    && (s.cursor_major, s.cursor_minor) <= cursor
            })
            .cloned()
            .collect();
        // Newest first
        result.sort_by(|a, b| {
            (b.cursor_major, b.cursor_minor).cmp(&(a.cursor_major, a.cursor_minor))
        });
        Ok(result)
    }

    fn write_anchor(&self, anchor: &Anchor) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        inner
            .anchors
            .insert((anchor.source_id.clone(), anchor.name.clone()), anchor.clone());
        Ok(())
    }

    fn read_anchor(&self, source_id: &str, name: &str) -> Result<Option<Anchor>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        Ok(inner
            .anchors
            .get(&(source_id.to_string(), name.to_string()))
            .cloned())
    }

    fn add_coverage(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        merge_coverage_interval(&mut inner.coverage, source_id, from, to);
        Ok(())
    }

    fn is_fully_cached(
        &self,
        source_id: &str,
        from: (i64, i64),
        to: (i64, i64),
    ) -> Result<bool, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        Ok(inner.coverage.iter().any(|c| {
            c.source_id == source_id && c.start <= from && c.end >= to
        }))
    }

    fn cached_tip(&self, source_id: &str) -> Result<Option<(i64, i64)>, Self::Error> {
        let inner = self.inner.lock().map_err(|_| MemoryStorageError::Poisoned)?;
        let tip = inner
            .coverage
            .iter()
            .filter(|c| c.source_id == source_id)
            .max_by_key(|c| c.end);
        Ok(tip.map(|c| c.end))
    }
}

/// Merge a new interval [from, to) into the coverage list for the given source_id,
/// combining overlapping/adjacent intervals.
///
/// An interval [a, b) overlaps or is adjacent to [from, to) when:
///   a <= to  AND  b >= from
fn merge_coverage_interval(
    coverage: &mut Vec<CoverageInterval>,
    source_id: &str,
    from: (i64, i64),
    to: (i64, i64),
) {
    let mut merged_start = from;
    let mut merged_end = to;

    // Collect indices of intervals for this source that overlap or touch [from, to)
    let mut to_remove: Vec<usize> = Vec::new();
    for (i, c) in coverage.iter().enumerate() {
        if c.source_id == source_id && c.start <= to && c.end >= from {
            to_remove.push(i);
            if c.start < merged_start {
                merged_start = c.start;
            }
            if c.end > merged_end {
                merged_end = c.end;
            }
        }
    }

    // Remove in reverse order to preserve indices
    for &i in to_remove.iter().rev() {
        coverage.remove(i);
    }

    coverage.push(CoverageInterval {
        source_id: source_id.to_string(),
        start: merged_start,
        end: merged_end,
    });
}
