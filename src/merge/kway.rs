//! Shared k-way scaffolding: the head-buffered per-source stream and error
//! propagation. Winner-pick and position assembly specialize per variant
//! (see `ordered` / `interleave`).

use crate::arch::{HistoryError, ItemStream, Position, Positioned};

use super::types::SourceKey;

/// One source's head-buffered stream inside a k-way merge.
pub(crate) struct MergeStream<T, P: Position> {
    pub(crate) key: SourceKey,
    pub(crate) stream: ItemStream<T, P>,
    pub(crate) head: Option<Result<Positioned<T, P>, HistoryError>>,
    pub(crate) exhausted: bool,
    /// The window's upper cut for this source — where the vector clock lands
    /// when the stream finishes (interleave only; the ordered merge derives
    /// its anchors from item positions alone).
    pub(crate) end_pos: P,
}

/// Take the first head that is an error and mark that stream exhausted
/// so the caller doesn't keep polling it.
pub(crate) fn take_first_error<T, P: Position>(
    streams: &mut [MergeStream<T, P>],
) -> Option<HistoryError> {
    let i = streams
        .iter()
        .position(|s| matches!(&s.head, Some(Err(_))))?;
    let err = streams[i]
        .head
        .take()
        .expect("position checked head is Some")
        .err()
        .expect("position checked head is Err");
    streams[i].exhausted = true;
    Some(err)
}
