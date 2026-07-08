//! Tower layers for the alloy client stack — the cross-cutting concerns
//! every chainfold RPC client is built with.
//!
//! Install order matters: [`RpcTimingLayer`] goes **outermost** (added first
//! to the `ClientBuilder`), then [`rpc_retry_layer`], then the transport —
//! so one `rpc` span covers each logical request end-to-end, retries and
//! backoff included:
//!
//! ```ignore
//! let client = ClientBuilder::default()
//!     .layer(RpcTimingLayer)
//!     .layer(rpc_retry_layer())
//!     .transport(transport, is_local);
//! ```

use std::task::{Context, Poll};

use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use alloy::transports::layers::{OrRetryPolicyFn, RateLimitRetryPolicy, RetryBackoffLayer};
use alloy::transports::{RpcError, TransportError, TransportFut};
use tracing::{Instrument, Level};

/// Number of retry attempts for transient/rate-limited RPC errors.
const MAX_RETRIES: u32 = 3;
/// Initial backoff between retries, in milliseconds (grows on each attempt).
const INITIAL_BACKOFF_MS: u64 = 500;
/// Compute-units-per-second budget the backoff layer paces requests against.
const COMPUTE_UNITS_PER_SECOND: u64 = 100;

/// Returns a `RetryBackoffLayer` that retries transient RPC errors.
///
/// Extends alloy's default rate-limit retry policy with:
/// - JSON-RPC `-32603` (internal error) and `-32000` (server error)
/// - All transport-level errors (connection reset, timeout, etc.)
///
/// Config: see [`MAX_RETRIES`], [`INITIAL_BACKOFF_MS`],
/// [`COMPUTE_UNITS_PER_SECOND`].
pub fn rpc_retry_layer() -> RetryBackoffLayer<OrRetryPolicyFn> {
    let policy = RateLimitRetryPolicy::default().or(|e: &TransportError| match e {
        RpcError::ErrorResp(payload) => matches!(payload.code, -32603 | -32000),
        RpcError::Transport(_) => true,
        _ => false,
    });
    RetryBackoffLayer::new_with_policy(
        MAX_RETRIES,
        INITIAL_BACKOFF_MS,
        COMPUTE_UNITS_PER_SECOND,
        policy,
    )
}

/// Tower layer that times every JSON-RPC request under a
/// [`RPC_TARGET`](crate::evm::observe::RPC_TARGET) span carrying the wire
/// method name in its `method` field (see the contract in
/// [`observe`](crate::evm::observe)).
///
/// Timing at the transport boundary means every request the client sends is
/// measured: a new RPC call site cannot be forgotten.
///
/// A batch is one wire request, so it gets one span. Chainfold only issues
/// homogeneous batches; the span is labelled with their shared method, so
/// per-method accounting (and the zero-RPC test guards) see through
/// batching. A mixed batch would fall back to the literal `batch`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RpcTimingLayer;

impl<S> tower_layer::Layer<S> for RpcTimingLayer {
    type Service = RpcTiming<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcTiming { inner }
    }
}

/// The service [`RpcTimingLayer`] produces: forwards to `inner`,
/// instrumenting each request future with a method-labelled RPC span.
///
/// Pinned to alloy's transport signature (alloy's `Transport` trait demands
/// the boxed `TransportFut` future, so the instrumented future is re-boxed
/// rather than staying generic like
/// [`ConnectTiming`](crate::evm::observe::ConnectTiming)'s).
#[derive(Debug, Clone)]
pub struct RpcTiming<S> {
    inner: S,
}

impl<S> tower_service::Service<RequestPacket> for RpcTiming<S>
where
    S: tower_service::Service<RequestPacket, Response = ResponsePacket, Error = TransportError>,
    S::Future: Send + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let span = {
            let mut names = req.method_names();
            let first = names.next().unwrap_or("empty");
            let method = if names.all(|n| n == first) {
                first
            } else {
                "batch"
            };
            tracing::span!(target: "chainfold::rpc", Level::DEBUG, "rpc", method = method)
        };
        Box::pin(self.inner.call(req).instrument(span))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use alloy::rpc::json_rpc::{Id, Request, SerializedRequest};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id as SpanId};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    use crate::evm::observe::RPC_TARGET;

    /// One rpc span the layer emitted: its target and `method` field.
    #[derive(Debug, PartialEq, Eq)]
    struct CapturedSpan {
        target: &'static str,
        method: Option<String>,
    }

    /// Records emitted spans so the tests can assert them without depending
    /// on the CLI's aggregating layer.
    #[derive(Clone, Default)]
    struct Capture {
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    #[derive(Default)]
    struct MethodField(Option<String>);
    impl Visit for MethodField {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "method" {
                self.0 = Some(value.to_owned());
            }
        }
        fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
    }

    impl<S> Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &SpanId, _ctx: Context<'_, S>) {
            let mut method = MethodField::default();
            attrs.record(&mut method);
            self.spans.lock().unwrap().push(CapturedSpan {
                target: attrs.metadata().target(),
                method: method.0,
            });
        }
    }

    /// Push one packet through a [`RpcTimingLayer`]-wrapped dummy service.
    fn drive(packet: RequestPacket) {
        use alloy::transports::TransportErrorKind;
        use futures_util::FutureExt;
        use tower_layer::Layer as _;
        use tower_service::Service as _;

        /// Stand-in for a transport: always ready, resolves immediately.
        /// Answering would mean fabricating a `ResponsePacket`; erroring is
        /// just as good — the test only observes the span.
        struct DummyTransport;
        impl tower_service::Service<RequestPacket> for DummyTransport {
            type Response = ResponsePacket;
            type Error = TransportError;
            type Future = std::future::Ready<Result<ResponsePacket, TransportError>>;

            fn poll_ready(
                &mut self,
                _cx: &mut std::task::Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: RequestPacket) -> Self::Future {
                std::future::ready(Err(TransportErrorKind::custom_str("dummy transport")))
            }
        }

        let mut svc = RpcTimingLayer.layer(DummyTransport);
        svc.call(packet)
            .now_or_never()
            .expect("ready future completes in one poll")
            .expect_err("dummy transport always errors");
    }

    fn request(method: &'static str, id: u64) -> SerializedRequest {
        Request::new(method, Id::Number(id), ())
            .serialize()
            .expect("unit params serialize")
    }

    /// A single request through [`RpcTimingLayer`] emits one span on
    /// [`RPC_TARGET`] whose `method` field is the wire method name.
    #[test]
    fn rpc_timing_layer_labels_span_with_method() {
        let cap = Capture::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        with_default(subscriber, || {
            drive(RequestPacket::Single(request("eth_getLogs", 1)));
        });
        let spans = cap.spans.lock().unwrap();
        assert!(spans.contains(&CapturedSpan {
            target: RPC_TARGET,
            method: Some("eth_getLogs".to_owned()),
        }));
    }

    /// A homogeneous batch — the only kind chainfold issues — is labelled
    /// with its shared method, so per-method accounting sees through
    /// batching; a mixed batch falls back to `batch`.
    #[test]
    fn rpc_timing_layer_labels_batches() {
        let cap = Capture::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        with_default(subscriber, || {
            drive(RequestPacket::Batch(vec![
                request("eth_getBlockByNumber", 1),
                request("eth_getBlockByNumber", 2),
            ]));
            drive(RequestPacket::Batch(vec![
                request("eth_getBlockByNumber", 3),
                request("eth_call", 4),
            ]));
        });
        let spans = cap.spans.lock().unwrap();
        assert!(spans.contains(&CapturedSpan {
            target: RPC_TARGET,
            method: Some("eth_getBlockByNumber".to_owned()),
        }));
        assert!(spans.contains(&CapturedSpan {
            target: RPC_TARGET,
            method: Some("batch".to_owned()),
        }));
    }
}
