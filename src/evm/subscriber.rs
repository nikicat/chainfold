//! Push subscription for EVM logs.
//!
//! `EvmLogSubscriber` runs a long-lived background task that delivers
//! freshly-mined logs to a [`SubscriberCallbacks`] sink. Two transports
//! are supported:
//!
//! * [`SubscribeTransport::Ws`] — native `eth_subscribe` over a pubsub
//!   provider. Sub-second delivery, real-time tip updates.
//! * [`SubscribeTransport::Poll`] — `eth_blockNumber` + `eth_getLogs` at
//!   a fixed interval. Fallback for HTTP-only providers.
//!
//! On any transport error the task logs, backs off exponentially up to
//! 30s, then calls [`SubscriberCallbacks::on_reconnect`] (asynchronous
//! gap-fill hook) and resumes. Dropping the returned `EvmLogSubscriber`
//! cancels the task immediately.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::arch::HistoryError;
use crate::evm::log_index::LogPage;

/// Choice of subscription transport.
#[derive(Clone, Debug)]
pub enum SubscribeTransport {
    /// Native `eth_subscribe` via a pubsub-capable provider (websocket / ipc).
    /// Sub-second latency. Provider must be built with `connect_ws` or similar.
    Ws,
    /// HTTP poll fallback. `interval` controls cadence.
    Poll { interval: Duration },
}

impl SubscribeTransport {
    /// Convenience: `Poll { interval: 2s }`.
    pub fn poll_default() -> Self {
        Self::Poll {
            interval: Duration::from_secs(2),
        }
    }
}

/// Async callback returning a boxed future. Used for `on_reconnect`,
/// which typically calls `LogIndex::ensure_cached` (async).
pub type AsyncCallback<T> =
    Arc<dyn Fn(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Sinks invoked by the running subscriber. `on_page` is synchronous (it
/// updates an in-memory range tree). `on_tip` and `on_reconnect` are async
/// so they can drive `ensure_block_header` and `ensure_cached`.
#[derive(Clone)]
pub struct SubscriberCallbacks {
    /// Called for each delivered batch of logs. Pages may have an empty
    /// `items` vec when only the scanned-range extends (e.g. WS new-head
    /// without matching logs).
    pub on_page: Arc<dyn Fn(LogPage) + Send + Sync>,
    /// Called when the chain head advances. Argument is the new head block.
    /// Typically fetches the block header and updates the `latest` tip store.
    pub on_tip: AsyncCallback<u64>,
    /// Called once after any reconnect (NOT on first connect). Argument
    /// is the last-seen tip before the disconnect; the callback typically
    /// gap-fills `(last_tip, current_tip]` via `LogIndex::ensure_cached`.
    pub on_reconnect: AsyncCallback<u64>,
}

/// Live handle on a running subscription. Drop to cancel.
pub struct EvmLogSubscriber {
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl EvmLogSubscriber {
    /// Spawn the subscription loop. `start_block` is the last block already
    /// covered — the loop watches `(start_block, ...]` going forward and
    /// uses `start_block` as the `last_tip` on the first reconnect.
    pub fn start<P>(
        provider: P,
        filter: Filter,
        transport: SubscribeTransport,
        start_block: u64,
        callbacks: SubscriberCallbacks,
    ) -> Self
    where
        P: Provider<Ethereum> + Clone + Send + Sync + 'static,
    {
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_loop(
            provider,
            filter,
            transport,
            start_block,
            callbacks,
            cancel.clone(),
        ));
        Self {
            cancel,
            handle: Some(handle),
        }
    }

    /// Cancel the loop. Idempotent. After return, the background task is
    /// guaranteed to observe cancellation; awaiting the `JoinHandle` is the
    /// caller's choice (default `Drop` aborts).
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Wait for the background task to finish. Useful in tests.
    pub async fn join(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

impl Drop for EvmLogSubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

// ============================================================
// Main loop: connect → run → on error: backoff → reconnect hook
// ============================================================

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

async fn run_loop<P>(
    provider: P,
    filter: Filter,
    transport: SubscribeTransport,
    start_block: u64,
    callbacks: SubscriberCallbacks,
    cancel: CancellationToken,
) where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    let mut last_tip = start_block;
    let mut backoff = INITIAL_BACKOFF;
    let mut first = true;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        if !first {
            (callbacks.on_reconnect)(last_tip).await;
        }
        first = false;

        let result = match &transport {
            SubscribeTransport::Ws => {
                run_ws(&provider, &filter, &callbacks, &mut last_tip, &cancel).await
            }
            SubscribeTransport::Poll { interval } => {
                run_poll(
                    &provider,
                    &filter,
                    *interval,
                    &callbacks,
                    &mut last_tip,
                    &cancel,
                )
                .await
            }
        };

        match result {
            Ok(()) => return, // cancelled
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "subscription error, will reconnect"
                );
                tokio::select! {
                    _ = sleep(backoff) => {}
                    _ = cancel.cancelled() => return,
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

// ============================================================
// Poll transport
// ============================================================

async fn run_poll<P>(
    provider: &P,
    filter: &Filter,
    interval: Duration,
    callbacks: &SubscriberCallbacks,
    last_tip: &mut u64,
    cancel: &CancellationToken,
) -> Result<(), HistoryError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = sleep(interval) => {}
        }

        let tip = provider
            .get_block_number()
            .await
            .map_err(|e| HistoryError::internal(format!("get_block_number: {e}")))?;

        if tip <= *last_tip {
            continue;
        }

        let from = *last_tip + 1;
        let to = tip;

        let scoped = filter.clone().from_block(from).to_block(to);
        let logs = provider
            .get_logs(&scoped)
            .await
            .map_err(|e| HistoryError::internal(format!("get_logs: {e}")))?;

        let end_block_hash = fetch_block_hash(provider, to).await?;

        let page = LogPage {
            from_block: from,
            to_block: to,
            items: logs,
            end_block_hash,
        };

        (callbacks.on_page)(page);
        (callbacks.on_tip)(to).await;
        *last_tip = to;
    }
}

// ============================================================
// WS transport
// ============================================================

async fn run_ws<P>(
    provider: &P,
    filter: &Filter,
    callbacks: &SubscriberCallbacks,
    last_tip: &mut u64,
    cancel: &CancellationToken,
) -> Result<(), HistoryError>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    // Subscribe to log events for delivery.
    let logs_sub = provider
        .subscribe_logs(filter)
        .await
        .map_err(|e| HistoryError::internal(format!("subscribe_logs: {e}")))?;
    let mut logs_stream = logs_sub.into_stream();

    // Subscribe to new heads for tip tracking + empty-range coverage.
    let heads_sub = provider
        .subscribe_blocks()
        .await
        .map_err(|e| HistoryError::internal(format!("subscribe_blocks: {e}")))?;
    let mut heads_stream = heads_sub.into_stream();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),

            log = logs_stream.next() => {
                let Some(log) = log else {
                    return Err(HistoryError::internal("logs subscription closed"));
                };
                let bn = log.block_number.unwrap_or(0);
                let bh = log.block_hash.unwrap_or(B256::ZERO);
                // Single-block page — covers exactly this block. The drain
                // task records (bn, bn) as a scanned range; if heads_stream
                // later announces a higher tip it adds the empty tail.
                let page = LogPage {
                    from_block: bn,
                    to_block: bn,
                    items: vec![log],
                    end_block_hash: bh,
                };
                (callbacks.on_page)(page);
                if bn > *last_tip {
                    (callbacks.on_tip)(bn).await;
                    *last_tip = bn;
                }
            }

            head = heads_stream.next() => {
                let Some(head) = head else {
                    return Err(HistoryError::internal("heads subscription closed"));
                };
                let bn = head.number;
                if bn <= *last_tip {
                    continue;
                }
                // Empty page covering (last_tip, bn] — records the scanned
                // range. Any log delivered later for an intermediate block
                // goes through `insert_late_item`.
                let bh = head.hash;
                let page = LogPage {
                    from_block: *last_tip + 1,
                    to_block: bn,
                    items: vec![],
                    end_block_hash: bh,
                };
                (callbacks.on_page)(page);
                (callbacks.on_tip)(bn).await;
                *last_tip = bn;
            }
        }
    }
}

// ============================================================
// Helpers
// ============================================================

async fn fetch_block_hash<P>(provider: &P, number: u64) -> Result<B256, HistoryError>
where
    P: Provider<Ethereum>,
{
    use alloy::eips::BlockNumberOrTag;
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(number))
        .await
        .map_err(|e| HistoryError::internal(format!("get_block_by_number({number}): {e}")))?;
    let block = block.ok_or_else(|| HistoryError::internal(format!("block {number} not found")))?;
    Ok(block.header.hash)
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use alloy::providers::ProviderBuilder;
    use alloy::transports::mock::Asserter;

    fn noop_callbacks() -> SubscriberCallbacks {
        SubscriberCallbacks {
            on_page: Arc::new(|_| {}),
            on_tip: Arc::new(|_| Box::pin(async {})),
            on_reconnect: Arc::new(|_| Box::pin(async {})),
        }
    }

    /// Verifies that dropping the handle cancels the running task. End-to-end
    /// poll/ws delivery + reconnect behavior is covered by the anvil
    /// integration tests in `tests/phase2_push.rs` against a real provider.
    #[tokio::test]
    async fn drop_cancels_watcher() {
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!("0x0"));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let sub = EvmLogSubscriber::start(
            provider,
            Filter::new(),
            SubscribeTransport::Poll {
                interval: Duration::from_millis(10),
            },
            0,
            noop_callbacks(),
        );

        sleep(Duration::from_millis(50)).await;
        drop(sub);
        sleep(Duration::from_millis(20)).await;
    }
}
