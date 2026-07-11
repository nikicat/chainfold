//! The alloy client stack's wire layer — the HTTP transport itself plus the
//! tower layers for the cross-cutting concerns every chainfold RPC client
//! is built with.
//!
//! Install order matters: [`RpcTimingLayer`] goes **outermost** (added first
//! to the `ClientBuilder`), then [`rpc_retry_layer`], then [`HttpTransport`]
//! — so one `rpc` span covers each logical request end-to-end, retries and
//! backoff included, with one `attempt` span per wire try inside it:
//!
//! ```ignore
//! let client = ClientBuilder::default()
//!     .layer(RpcTimingLayer)
//!     .layer(rpc_retry_layer())
//!     .transport(HttpTransport::new(http_client, url), is_local);
//! ```

use std::task::{Context, Poll};

use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use alloy::transports::layers::{OrRetryPolicyFn, RateLimitRetryPolicy, RetryBackoffLayer};
use alloy::transports::utils::guess_local_url;
use alloy::transports::{RpcError, TransportError, TransportErrorKind, TransportFut};
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

/// The HTTP JSON-RPC wire transport — chainfold's stand-in for the
/// `Service` impl of alloy's `Http<reqwest::Client>` (everything above it
/// stays alloy's). Owned so the endpoint URL — which embeds the provider
/// API key for hosted RPCs — never reaches telemetry or error text: alloy's
/// transport records the full URL on its per-request span, and reqwest
/// errors carry it in their `Display`. Here the per-attempt span carries
/// only the redacted `endpoint` (scheme + host + port — never the path or
/// query, where hosted providers put the key), and reqwest errors are
/// stripped with [`reqwest::Error::without_url`].
///
/// Each call gets an [`ATTEMPT_TARGET`](crate::observe::ATTEMPT_TARGET)
/// span: the `rpc` span above covers the logical request end-to-end,
/// retries and backoff included; one attempt span covers one wire try.
///
/// Response mapping mirrors alloy's transport exactly — the policy in
/// [`rpc_retry_layer`] classifies errors by shape, so the mapping is a
/// contract (pinned by the `http_transport_*` tests):
/// * a non-2xx body that parses as a JSON-RPC error is `Ok` — hosted
///   providers report rate limiting there, and the response, not the HTTP
///   status, carries the actionable error;
/// * any other non-2xx becomes `TransportErrorKind::http_error` (the
///   status code is what the rate-limit retry policy matches on);
/// * a 2xx body that fails to parse becomes `TransportError::deser_err`.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    url: reqwest::Url,
    /// The span-safe endpoint identity, computed once at construction.
    endpoint: String,
}

impl HttpTransport {
    /// Bundle the HTTP client with the endpoint. Rides whatever client the
    /// caller built — the CLI passes one with the connect-timing layer
    /// installed, so connection setup gets its own `connect` span (see
    /// [`ConnectTimingLayer`](crate::observe::ConnectTimingLayer)).
    pub fn new(client: reqwest::Client, url: reqwest::Url) -> Self {
        let endpoint = redacted_endpoint(&url);
        Self {
            client,
            url,
            endpoint,
        }
    }

    /// Whether the endpoint looks like a local node (alloy's heuristic) —
    /// feeds the `is_local` flag of `ClientBuilder::transport`.
    pub fn guess_local(&self) -> bool {
        guess_local_url(self.url.as_str())
    }

    async fn send(self, req: RequestPacket) -> Result<ResponsePacket, TransportError> {
        if tracing::enabled!(Level::TRACE)
            && let Ok(body) = serde_json::to_string(&req)
        {
            tracing::trace!(body = %body, "request body");
        }
        let resp = self
            .client
            .post(self.url)
            .json(&req)
            .headers(req.headers())
            .send()
            .await
            .map_err(|e| TransportErrorKind::custom(e.without_url()))?;
        let status = resp.status();
        tracing::debug!(%status, "response received");
        let body = resp
            .bytes()
            .await
            .map_err(|e| TransportErrorKind::custom(e.without_url()))?;
        if tracing::enabled!(Level::TRACE) {
            tracing::trace!(body = %String::from_utf8_lossy(&body), "response body");
        } else {
            tracing::debug!(bytes = body.len(), "response body received");
        }
        if !status.is_success() {
            if let Some(response) = json_rpc_error_response(&body) {
                return Ok(response);
            }
            return Err(TransportErrorKind::http_error(
                status.as_u16(),
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }
        serde_json::from_slice(&body)
            .map_err(|err| TransportError::deser_err(err, String::from_utf8_lossy(&body)))
    }
}

/// The part of an RPC URL that is identity, not secret: scheme + host +
/// (non-default) port. Path, query, and userinfo are dropped wholesale —
/// hosted providers put API keys there, and guessing *which* segment is
/// secret per provider is a losing game.
fn redacted_endpoint(url: &reqwest::Url) -> String {
    use std::fmt::Write;

    let mut endpoint = format!("{}://{}", url.scheme(), url.host_str().unwrap_or("-"));
    if let Some(port) = url.port() {
        write!(endpoint, ":{port}").expect("writing to a String cannot fail");
    }
    endpoint
}

/// A non-2xx body that is nonetheless a well-formed JSON-RPC *error*
/// response.
fn json_rpc_error_response(body: &[u8]) -> Option<ResponsePacket> {
    let response = serde_json::from_slice::<ResponsePacket>(body).ok()?;
    response.is_error().then_some(response)
}

impl tower_service::Service<RequestPacket> for HttpTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // reqwest imposes no readiness gate.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone();
        let span = tracing::span!(
            target: "chainfold::attempt", Level::DEBUG, "attempt",
            endpoint = %this.endpoint
        );
        Box::pin(this.send(req).instrument(span.or_current()))
    }
}

/// Tower layer that times every JSON-RPC request under a
/// [`RPC_TARGET`](crate::observe::RPC_TARGET) span carrying the wire
/// method name in its `method` field (see the contract in
/// [`observe`](crate::observe)).
///
/// Timing at the transport boundary means every request the client sends is
/// measured: a new RPC call site cannot be forgotten.
///
/// A batch is one wire request, so it gets one span. Chainfold only issues
/// homogeneous batches; the span is labelled with their shared method, so
/// per-method accounting (and the zero-RPC test guards) see through
/// batching. A mixed batch would fall back to the literal `batch`. The
/// span's `reqs` field carries the packet's request count (1 for singles),
/// so consumers can report logical JSON-RPC calls alongside wire
/// round-trips.
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
/// [`ConnectTiming`](crate::observe::ConnectTiming)'s).
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
            let reqs = match &req {
                RequestPacket::Single(_) => 1,
                RequestPacket::Batch(batch) => batch.len() as u64,
            };
            tracing::span!(target: "chainfold::rpc", Level::DEBUG, "rpc", method = method, reqs = reqs)
        };
        Box::pin(self.inner.call(req).instrument(span))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use alloy::rpc::json_rpc::{Id, Request, SerializedRequest};
    use alloy::transports::layers::RetryPolicy;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id as SpanId};
    use tracing::subscriber::{set_default, with_default};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    use crate::observe::{ATTEMPT_TARGET, RPC_TARGET};

    /// One rpc span the layer emitted: its target and `method`/`reqs` fields.
    #[derive(Debug, PartialEq, Eq)]
    struct CapturedSpan {
        target: &'static str,
        method: Option<String>,
        reqs: Option<u64>,
    }

    /// Records emitted spans so the tests can assert them without depending
    /// on the CLI's aggregating layer.
    #[derive(Clone, Default)]
    struct Capture {
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    #[derive(Default)]
    struct SpanFields {
        method: Option<String>,
        reqs: Option<u64>,
    }
    impl Visit for SpanFields {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "method" {
                self.method = Some(value.to_owned());
            }
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "reqs" {
                self.reqs = Some(value);
            }
        }
        fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
    }

    impl<S> Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &SpanId, _ctx: Context<'_, S>) {
            let mut fields = SpanFields::default();
            attrs.record(&mut fields);
            self.spans.lock().unwrap().push(CapturedSpan {
                target: attrs.metadata().target(),
                method: fields.method,
                reqs: fields.reqs,
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
    /// [`RPC_TARGET`] whose `method` field is the wire method name and whose
    /// `reqs` field is 1.
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
            reqs: Some(1),
        }));
    }

    /// A homogeneous batch — the only kind chainfold issues — is labelled
    /// with its shared method and carries the packet's request count in
    /// `reqs`, so per-method accounting sees through batching; a mixed batch
    /// falls back to `batch`.
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
            reqs: Some(2),
        }));
        assert!(spans.contains(&CapturedSpan {
            target: RPC_TARGET,
            method: Some("batch".to_owned()),
            reqs: Some(2),
        }));
    }

    // ---- HttpTransport wire tests: real sockets, one-shot server ----

    /// The secret path segment (a stand-in API key) the telemetry and error
    /// assertions sweep for.
    const SECRET: &str = "supersecretapikey";

    /// One-shot HTTP/1.1 responder: accepts a single connection, reads one
    /// full request (headers, then `content-length` body — answering before
    /// the body is read risks a reset racing the response), replies with
    /// `status` + `body`, and closes. Just enough server to drive the real
    /// wire path without mocking the client side.
    async fn serve_once(status: &'static str, body: &'static str) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut buf = [0u8; 4096];
            let body_start = loop {
                let n = sock.read(&mut buf).await.unwrap();
                assert!(n > 0, "client closed before finishing headers");
                req.extend_from_slice(&buf[..n]);
                if let Some(end) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = String::from_utf8_lossy(&req[..body_start]).to_ascii_lowercase();
            let content_length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .map_or(0, |v| v.trim().parse().unwrap());
            while req.len() < body_start + content_length {
                let n = sock.read(&mut buf).await.unwrap();
                assert!(n > 0, "client closed mid-body");
                req.extend_from_slice(&buf[..n]);
            }
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len(),
            );
            sock.write_all(response.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        });
        addr
    }

    /// Captures the target/name of every span plus every span/event field,
    /// stringified — the no-secret assertions sweep all of it.
    #[derive(Clone, Default)]
    struct TelemetryDump {
        spans: Arc<Mutex<Vec<(String, String)>>>,
        fields: Arc<Mutex<Vec<String>>>,
    }

    struct DumpFields<'a>(&'a mut Vec<String>);

    impl Visit for DumpFields<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push(format!("{}={value:?}", field.name()));
        }
    }

    impl<S> Layer<S> for TelemetryDump
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &SpanId, _ctx: Context<'_, S>) {
            let mut fields = self.fields.lock().unwrap();
            attrs.record(&mut DumpFields(&mut fields));
            self.spans.lock().unwrap().push((
                attrs.metadata().target().to_owned(),
                attrs.metadata().name().to_owned(),
            ));
        }

        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut fields = self.fields.lock().unwrap();
            event.record(&mut DumpFields(&mut fields));
        }
    }

    /// A transport pointed at `addr` with the secret riding in the URL path,
    /// the way hosted providers carry API keys.
    fn secret_transport(addr: std::net::SocketAddr) -> HttpTransport {
        let url = format!("http://{addr}/v2/{SECRET}").parse().unwrap();
        HttpTransport::new(reqwest::Client::new(), url)
    }

    /// One `eth_chainId` call against `addr` with the telemetry dump
    /// installed (unfiltered, so the TRACE body-dump path runs too).
    async fn call_http(
        addr: std::net::SocketAddr,
    ) -> (Result<ResponsePacket, TransportError>, TelemetryDump) {
        use tower_service::Service as _;

        let dump = TelemetryDump::default();
        let _guard = set_default(tracing_subscriber::registry().with(dump.clone()));
        let mut transport = secret_transport(addr);
        let result = transport
            .call(RequestPacket::Single(request("eth_chainId", 1)))
            .await;
        (result, dump)
    }

    /// Success path: the response parses, the wire try gets one `attempt`
    /// span on [`ATTEMPT_TARGET`], and no span or event field anywhere
    /// carries the URL.
    #[tokio::test]
    async fn http_transport_roundtrips_and_keeps_url_out_of_telemetry() {
        let addr = serve_once("200 OK", r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).await;
        let (result, dump) = call_http(addr).await;
        assert!(matches!(result.unwrap(), ResponsePacket::Single(_)));

        let spans = dump.spans.lock().unwrap();
        assert!(
            spans
                .iter()
                .any(|(target, name)| target == ATTEMPT_TARGET && name == "attempt"),
            "attempt span missing: {spans:?}"
        );
        let fields = dump.fields.lock().unwrap();
        for text in fields.iter() {
            assert!(!text.contains(SECRET), "secret leaked into telemetry: {text}");
        }
        // The endpoint identity survives redaction: scheme + host + port,
        // no path.
        let expected = format!("endpoint=http://{addr}");
        assert!(
            fields.contains(&expected),
            "endpoint field missing or wrong: {fields:?}"
        );
    }

    /// The redaction rule: path/query/userinfo dropped wholesale, default
    /// ports elided, explicit ports kept.
    #[test]
    fn redacted_endpoint_keeps_identity_drops_secrets() {
        let redact = |s: &str| redacted_endpoint(&s.parse().unwrap());
        assert_eq!(
            redact("https://eth-mainnet.g.alchemy.com/v2/supersecret"),
            "https://eth-mainnet.g.alchemy.com"
        );
        assert_eq!(
            redact("https://rpc.example.com/?dkey=supersecret"),
            "https://rpc.example.com"
        );
        assert_eq!(
            redact("http://user:hunter2@localhost:8545/"),
            "http://localhost:8545"
        );
    }

    /// A non-2xx status whose body is a JSON-RPC error parses as a
    /// *response* (alloy-2 semantics): hosted providers report rate limits
    /// that way, and the JSON-RPC error carries the actionable code.
    #[tokio::test]
    async fn http_transport_parses_json_rpc_error_body_on_error_status() {
        let addr = serve_once(
            "429 Too Many Requests",
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limited"}}"#,
        )
        .await;
        let (result, _) = call_http(addr).await;
        assert!(result.unwrap().is_error());
    }

    /// A non-2xx status with a non-JSON-RPC body maps to
    /// `TransportErrorKind::HttpError` carrying the status code — the shape
    /// the rate-limit retry policy classifies on. The policy assertion pins
    /// the transport↔retry contract: if the error mapping drifts, retries
    /// silently stop firing.
    #[tokio::test]
    async fn http_transport_maps_plain_error_status_for_the_retry_policy() {
        let addr = serve_once("429 Too Many Requests", "too many requests").await;
        let (result, _) = call_http(addr).await;
        let err = result.unwrap_err();
        match &err {
            RpcError::Transport(TransportErrorKind::HttpError(http)) => {
                assert_eq!(http.status, 429);
            }
            other => panic!("expected HttpError, got {other:?}"),
        }
        assert!(RateLimitRetryPolicy::default().should_retry(&err));
    }

    /// A 2xx body that isn't JSON-RPC maps to a deser error (alloy parity;
    /// not retried).
    #[tokio::test]
    async fn http_transport_maps_garbage_success_body_to_deser_error() {
        let addr = serve_once("200 OK", "<html>totally not json</html>").await;
        let (result, _) = call_http(addr).await;
        assert!(matches!(result.unwrap_err(), RpcError::DeserError { .. }));
    }

    /// Wire failures keep the URL out of the whole error chain: reqwest's
    /// `Display` embeds the request URL ("error sending request for url
    /// (…)"), which `without_url` strips before the error is wrapped.
    #[tokio::test]
    async fn http_transport_redacts_url_from_wire_errors() {
        use tokio::net::TcpListener;
        use tower_service::Service as _;

        // Bind-then-drop reserves a port nothing listens on, so the request
        // fails at connect — the error path that carries the URL.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut transport = secret_transport(addr);
        let err = transport
            .call(RequestPacket::Single(request("eth_chainId", 1)))
            .await
            .unwrap_err();
        let mut chain = Vec::new();
        let mut source: Option<&dyn std::error::Error> = Some(&err);
        while let Some(e) = source {
            chain.push(e.to_string());
            source = e.source();
        }
        assert!(
            chain.iter().all(|text| !text.contains(SECRET)),
            "secret leaked into the error chain: {chain:?}"
        );
    }
}
