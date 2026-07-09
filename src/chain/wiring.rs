//! The shared per-store wiring task: one per reactive store, driving its
//! `on_bump` entry point off the chain's head channel. Source-specific
//! wiring (e.g. the EVM ws log session) lives with its source.
//!
//! The task holds only a [`Weak`] reference to its store (the store owns
//! the task via `AbortOnDropHandle`, so a strong reference would leak
//! both) and exits when the store goes away.

use std::sync::Weak;

use tokio::sync::watch;

use crate::chain::head::HeadEvent;
use crate::chain::{BlockNumber, BlockRange};
use crate::range_index::{RangeIndexStore, RangeSpec};

/// Follow the head channel: eagerly gap-fill `(last_tip, head]` through
/// the store's own fetcher (singleflight, chunked, persisted inline), then
/// bump the tip. The first observed head only bumps — there is no baseline
/// to fill from; iteration falls back to lazy fill. A fill failure still
/// bumps — the tree keeps its gap, so no false coverage is possible.
pub async fn follow_head<S: RangeSpec>(
    mut head: watch::Receiver<Option<HeadEvent>>,
    store: Weak<RangeIndexStore<S>>,
) {
    let mut last_tip: Option<BlockNumber> = None;
    loop {
        // Read-then-wait: the channel may already hold an observation from
        // before this store subscribed.
        let observed = *head.borrow_and_update();
        if let Some(h) = observed
            && last_tip.is_none_or(|tip| h.number > tip)
        {
            let Some(store) = store.upgrade() else { return };
            if let Some(tip) = last_tip
                && let Err(e) = store
                    .index()
                    .ensure_cached(BlockRange::new(tip + 1, h.number))
                    .await
            {
                tracing::warn!(from = %(tip + 1), to = %h.number, error = %e, "head gap-fill");
            }
            store.on_bump(S::head_pos(h.number));
            last_tip = Some(h.number);
        }
        if head.changed().await.is_err() {
            // Head channel closed — chain and every other holder gone.
            return;
        }
    }
}
