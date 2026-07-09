//! The bitcoind JSON-RPC wire transport: a reqwest-backed implementation of
//! `jsonrpc`'s async [`Transport`] plus the [`TracingTransport`] decorator
//! that times every wire request.
//!
//! The decorator emits the same `chainfold::rpc` span contract as the EVM
//! side's `RpcTimingLayer` (see [`crate::observe`]): one span per wire
//! request named `rpc`, carrying `method` (the shared name for homogeneous
//! batches, `"batch"` for mixed ones) and `reqs` (the number of JSON-RPC
//! calls it carries). Stats consumers aggregate by `(target, name)` +
//! fields, so the Bitcoin transport participates in `--stats` and
//! RPC-count test guards with zero consumer changes.

use std::path::PathBuf;

use jsonrpc::client_async::{BoxFuture, Transport};
use jsonrpc::{Request, Response};
use reqwest::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;
use tracing::{Instrument, Level};

/// How the client authenticates to bitcoind. A closed enum funneled once at
/// the construction boundary (CLI flags / config resolve into this).
#[derive(Debug, Clone)]
pub enum BtcAuth {
    /// Read `user:pass` from a Core cookie file (`<datadir>/.cookie`) at
    /// transport construction time.
    Cookie(PathBuf),
    /// Explicit `rpcuser`/`rpcpassword` credentials.
    UserPass { user: String, pass: String },
    /// No authentication (nonstandard proxies only — Core always wants
    /// credentials).
    None,
}

impl BtcAuth {
    /// Resolve to concrete basic-auth credentials. The one fallible / IO
    /// step (reading the cookie file), paid when the transport is built —
    /// never on the request path, and never inside `BitcoinChain::new`.
    pub fn credentials(&self) -> std::io::Result<Option<(String, String)>> {
        match self {
            Self::None => Ok(None),
            Self::UserPass { user, pass } => Ok(Some((user.clone(), pass.clone()))),
            Self::Cookie(path) => {
                let contents = std::fs::read_to_string(path)?;
                let (user, pass) = contents.trim().split_once(':').ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("malformed bitcoind cookie file {}", path.display()),
                    )
                })?;
                Ok(Some((user.to_owned(), pass.to_owned())))
            }
        }
    }
}

/// Async JSON-RPC-over-HTTP transport on the crate's existing [`reqwest`]
/// stack. Rides whatever client the caller built — the CLI passes one with
/// the connect-timing layer installed, so connection setup gets its own
/// `chainfold::connect` span exactly like the EVM path.
pub struct ReqwestTransport {
    client: reqwest::Client,
    url: reqwest::Url,
    credentials: Option<(String, String)>,
}

impl ReqwestTransport {
    /// Bundle the HTTP client with the endpoint and resolved credentials.
    /// Reads the cookie file when `auth` is [`BtcAuth::Cookie`] — the one
    /// fallible step, paid here rather than per-request.
    pub fn new(
        client: reqwest::Client,
        url: reqwest::Url,
        auth: &BtcAuth,
    ) -> std::io::Result<Self> {
        Ok(Self {
            client,
            url,
            credentials: auth.credentials()?,
        })
    }

    /// POST `body` and hand back the response bytes. Non-2xx statuses are
    /// not short-circuited: legacy (pre-2.0) bitcoind returns RPC errors
    /// with 4xx/5xx statuses and a JSON-RPC error body — the caller parses
    /// the body first and only falls back to the status.
    async fn post_json(&self, body: Vec<u8>) -> Result<(reqwest::StatusCode, Vec<u8>), Error> {
        let mut req = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        if let Some((user, pass)) = &self.credentials {
            req = req.basic_auth(user, Some(pass));
        }
        let resp = req.send().await.map_err(transport_error)?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(transport_error)?;
        Ok((status, bytes.to_vec()))
    }

    /// Parse a response body, surfacing the HTTP status when the body is
    /// not the expected JSON shape (auth failures return HTML/plain text).
    fn parse<T: DeserializeOwned>(status: reqwest::StatusCode, bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|e| {
            if status.is_success() {
                Error::Json(e)
            } else {
                transport_error(format!(
                    "HTTP {status}: {}",
                    String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
                ))
            }
        })
    }
}

type Error = jsonrpc::Error;

fn transport_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Transport(e.into())
}

impl Transport for ReqwestTransport {
    fn send_request<'a>(&'a self, req: Request<'a>) -> BoxFuture<'a, Result<Response, Error>> {
        Box::pin(async move {
            let body = serde_json::to_vec(&req)?;
            let (status, bytes) = self.post_json(body).await?;
            Self::parse(status, &bytes)
        })
    }

    fn send_batch<'a>(
        &'a self,
        reqs: &'a [Request<'a>],
    ) -> BoxFuture<'a, Result<Vec<Response>, Error>> {
        Box::pin(async move {
            let body = serde_json::to_vec(&reqs)?;
            let (status, bytes) = self.post_json(body).await?;
            Self::parse(status, &bytes)
        })
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

/// Decorator timing every wire request with a `chainfold::rpc` span — the
/// bitcoind analog of the EVM `RpcTimingLayer`. Wrap the innermost
/// transport so one span covers each wire request end-to-end.
pub struct TracingTransport<T>(pub T);

impl<T: Transport> Transport for TracingTransport<T> {
    fn send_request<'a>(&'a self, req: Request<'a>) -> BoxFuture<'a, Result<Response, Error>> {
        let span = tracing::span!(
            target: "chainfold::rpc", Level::DEBUG, "rpc",
            method = req.method, reqs = 1u64
        );
        Box::pin(self.0.send_request(req).instrument(span))
    }

    fn send_batch<'a>(
        &'a self,
        reqs: &'a [Request<'a>],
    ) -> BoxFuture<'a, Result<Vec<Response>, Error>> {
        let mut names = reqs.iter().map(|r| r.method);
        let first = names.next().unwrap_or("empty");
        let method = if names.all(|n| n == first) {
            first
        } else {
            "batch"
        };
        let span = tracing::span!(
            target: "chainfold::rpc", Level::DEBUG, "rpc",
            method = method, reqs = reqs.len() as u64
        );
        Box::pin(self.0.send_batch(reqs).instrument(span))
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt_target(f)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id as SpanId};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    use super::*;
    use crate::observe::RPC_TARGET;

    /// One rpc span the decorator emitted: its target and `method`/`reqs`
    /// fields (the same capture shape as the EVM transport tests).
    #[derive(Debug, PartialEq, Eq)]
    struct CapturedSpan {
        target: String,
        method: Option<String>,
        reqs: Option<u64>,
    }

    #[derive(Default)]
    struct FieldVisitor {
        method: Option<String>,
        reqs: Option<u64>,
    }

    impl Visit for FieldVisitor {
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
        fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<CapturedSpan>>>);

    impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &SpanId, _cx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            self.0.lock().unwrap().push(CapturedSpan {
                target: attrs.metadata().target().to_owned(),
                method: visitor.method,
                reqs: visitor.reqs,
            });
        }
    }

    /// A transport answering every request with its own id and a null
    /// result — enough to drive the decorator.
    struct Echo;

    impl Transport for Echo {
        fn send_request<'a>(&'a self, req: Request<'a>) -> BoxFuture<'a, Result<Response, Error>> {
            Box::pin(async move {
                Ok(Response {
                    result: None,
                    error: None,
                    id: req.id.clone(),
                    jsonrpc: Some("2.0".to_owned()),
                })
            })
        }

        fn send_batch<'a>(
            &'a self,
            reqs: &'a [Request<'a>],
        ) -> BoxFuture<'a, Result<Vec<Response>, Error>> {
            Box::pin(async move {
                Ok(reqs
                    .iter()
                    .map(|r| Response {
                        result: None,
                        error: None,
                        id: r.id.clone(),
                        jsonrpc: Some("2.0".to_owned()),
                    })
                    .collect())
            })
        }

        fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "echo")
        }
    }

    fn req(method: &'static str) -> Request<'static> {
        Request {
            method,
            params: None,
            id: serde_json::Value::from(1),
            jsonrpc: Some("2.0"),
        }
    }

    #[tokio::test]
    async fn single_request_spans_method_and_one_req() {
        let capture = Capture::default();
        let spans = capture.0.clone();
        let subscriber = tracing_subscriber::registry().with(capture);
        with_default(subscriber, || {
            let t = TracingTransport(Echo);
            futures_util::FutureExt::now_or_never(t.send_request(req("getblockcount")))
                .expect("echo resolves immediately")
                .unwrap();
        });
        assert_eq!(
            *spans.lock().unwrap(),
            vec![CapturedSpan {
                target: RPC_TARGET.to_owned(),
                method: Some("getblockcount".to_owned()),
                reqs: Some(1),
            }]
        );
    }

    #[tokio::test]
    async fn homogeneous_batch_spans_shared_method_and_count() {
        let capture = Capture::default();
        let spans = capture.0.clone();
        let subscriber = tracing_subscriber::registry().with(capture);
        with_default(subscriber, || {
            let t = TracingTransport(Echo);
            let reqs = vec![req("getblockhash"), req("getblockhash")];
            futures_util::FutureExt::now_or_never(t.send_batch(&reqs))
                .expect("echo resolves immediately")
                .unwrap();
        });
        assert_eq!(
            *spans.lock().unwrap(),
            vec![CapturedSpan {
                target: RPC_TARGET.to_owned(),
                method: Some("getblockhash".to_owned()),
                reqs: Some(2),
            }]
        );
    }

    #[tokio::test]
    async fn mixed_batch_spans_batch_label() {
        let capture = Capture::default();
        let spans = capture.0.clone();
        let subscriber = tracing_subscriber::registry().with(capture);
        with_default(subscriber, || {
            let t = TracingTransport(Echo);
            let reqs = vec![req("getblockhash"), req("getblockheader")];
            futures_util::FutureExt::now_or_never(t.send_batch(&reqs))
                .expect("echo resolves immediately")
                .unwrap();
        });
        assert_eq!(
            *spans.lock().unwrap(),
            vec![CapturedSpan {
                target: RPC_TARGET.to_owned(),
                method: Some("batch".to_owned()),
                reqs: Some(2),
            }]
        );
    }

    #[test]
    fn cookie_auth_parses_user_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, "__cookie__:s3cret\n").unwrap();
        let creds = BtcAuth::Cookie(path).credentials().unwrap();
        assert_eq!(creds, Some(("__cookie__".to_owned(), "s3cret".to_owned())));
    }

    #[test]
    fn malformed_cookie_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cookie");
        std::fs::write(&path, "no-colon-here").unwrap();
        assert!(BtcAuth::Cookie(path).credentials().is_err());
    }
}
