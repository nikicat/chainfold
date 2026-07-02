//! Per-store wiring tasks — one per [`EvmChain::logs`](super::EvmChain::logs)
//! store, driving its `on_events` / `on_bump` entry points off the chain's
//! head clock (plus, in ws follow mode, a narrow per-filter log
//! subscription).
//!
//! Every task holds only a [`Weak`] reference to its store (the store owns
//! the task via `AbortOnDropHandle`, so a strong reference would leak both)
//! and exits when the store or the chain goes away.
//!
//! ## Why ws coverage and log delivery share one session
//!
//! A head advance is recorded as an *empty coverage page* — the claim
//! "these blocks were scanned; no logs matched". That claim is only sound
//! while this filter's log subscription is actually delivering. Splitting
//! the two (heads healthy, logs dead) would persist false coverage that a
//! later gap-fill trusts and skips. So [`ws_log_session`] interleaves both
//! in one `select!`; when the log subscription drops, coverage stops too,
//! and [`ws_follow`] fetch-fills `(last_tip, head]` before trusting push
//! delivery again.

use std::sync::Weak;

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::time::sleep;

use super::config::HeadEvent;
use super::{INITIAL_BACKOFF, MAX_BACKOFF, tip_pos};
use crate::arch::HistoryError;
use crate::evm::chain_provider::ChainProvider;
use crate::evm::range_index::{LogPage, LogSpec, RangeIndexStore};
use crate::evm::types::{BlockNumber, BlockRange};

type WeakLogStore = Weak<RangeIndexStore<LogSpec>>;

/// Non-follow mode: mirror head advances into the store tip. Gaps fill
/// lazily when a consumer iterates. Ends when the chain or store drops.
pub(super) async fn mirror_head(mut head: watch::Receiver<HeadEvent>, store: WeakLogStore) {
    while head.changed().await.is_ok() {
        let n = head.borrow_and_update().number;
        let Some(store) = store.upgrade() else { return };
        store.on_bump(tip_pos(n));
    }
}

/// Poll-follow mode: eagerly gap-fill `(last_tip, head]` through the
/// store's own fetcher (singleflight, chunked, persisted inline), then
/// bump the tip. A fill failure still bumps — the tree keeps its gap and
/// iteration falls back to lazy fill, so no false coverage is possible.
pub(super) async fn poll_follow(
    mut head: watch::Receiver<HeadEvent>,
    store: WeakLogStore,
    mut last_tip: BlockNumber,
) {
    while head.changed().await.is_ok() {
        let n = head.borrow_and_update().number;
        if n <= last_tip {
            continue;
        }
        let Some(store) = store.upgrade() else { return };
        if let Err(e) = store
            .index()
            .ensure_cached(BlockRange::new(last_tip + 1, n))
            .await
        {
            tracing::warn!(from = %(last_tip + 1), to = %n, error = %e, "poll gap-fill");
        }
        store.on_bump(tip_pos(n));
        last_tip = n;
    }
}

/// Ws-follow mode: run [`ws_log_session`]s forever, gap-filling after
/// every disconnect *before* re-entering push delivery, with exponential
/// backoff on transport errors.
pub(super) async fn ws_follow<P>(
    provider: ChainProvider<P>,
    filter: Filter,
    head: watch::Receiver<HeadEvent>,
    store: WeakLogStore,
    mut last_tip: BlockNumber,
) where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut first = true;
    loop {
        if !first {
            // Reconnect gap-fill: fetch everything push delivery may have
            // missed while the session was down. Must succeed before any
            // new empty coverage page is recorded — retry on failure
            // rather than falling through to the session.
            let Some(s) = store.upgrade() else { return };
            let current = head.borrow().number;
            if current > last_tip {
                if let Err(e) = s
                    .index()
                    .ensure_cached(BlockRange::new(last_tip + 1, current))
                    .await
                {
                    tracing::warn!(
                        from = %(last_tip + 1),
                        to = %current,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "reconnect gap-fill failed, retrying"
                    );
                    drop(s);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                s.on_bump(tip_pos(current));
                last_tip = current;
            }
        }
        first = false;

        match ws_log_session(&provider, &filter, head.clone(), &store, &mut last_tip).await {
            Ok(()) => return, // store or chain dropped
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "log subscription error, will reconnect"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// One ws session: narrow log pushes interleaved with head-driven empty
/// coverage. `Ok(())` = orderly shutdown (store/chain gone); `Err` =
/// transport failure, the caller gap-fills and reconnects.
async fn ws_log_session<P>(
    provider: &ChainProvider<P>,
    filter: &Filter,
    mut head: watch::Receiver<HeadEvent>,
    store: &WeakLogStore,
    last_tip: &mut BlockNumber,
) -> Result<(), HistoryError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let sub = provider
        .subscribe_logs(filter)
        .await
        .map_err(|e| HistoryError::internal(format!("subscribe_logs: {e}")))?;
    let mut logs = sub.into_stream();

    loop {
        tokio::select! {
            log = logs.next() => {
                let Some(log) = log else {
                    return Err(HistoryError::internal("logs subscription closed"));
                };
                let Some(s) = store.upgrade() else { return Ok(()) };
                // Headers-present invariant: resolve() reads the timestamp
                // from the cached header, synchronously. The head clock
                // usually beat us to it; fall back to one RPC when the log
                // outruns its head event.
                if log.block_timestamp.is_none()
                    && let Some(bn) = log.block_number
                {
                    provider.ensure_block_header(BlockNumber::new(bn)).await?;
                }
                let bn = BlockNumber::new(log.block_number.unwrap_or(0));
                let bh = log.block_hash.unwrap_or(B256::ZERO);
                // Single-block page covering exactly this block. Late
                // arrivals relative to head coverage are absorbed by the
                // tree's late-item rules.
                s.on_events(&LogPage {
                    from_block: bn,
                    to_block: bn,
                    items: vec![log],
                    end_block_hash: bh,
                });
                *last_tip = (*last_tip).max(bn);
            }

            changed = head.changed() => {
                if changed.is_err() {
                    // Chain dropped — no more head events. Keep the store
                    // serving cached state, stop the live wiring.
                    return Ok(());
                }
                let h = *head.borrow_and_update();
                if h.number <= *last_tip {
                    continue;
                }
                let Some(s) = store.upgrade() else { return Ok(()) };
                // Empty coverage page for (last_tip, head]: "scanned, no
                // logs (yet)". Sound only because this session's log
                // subscription is live — see the module docs.
                s.on_events(&LogPage {
                    from_block: *last_tip + 1,
                    to_block: h.number,
                    items: vec![],
                    end_block_hash: h.hash,
                });
                *last_tip = h.number;
            }
        }
    }
}
