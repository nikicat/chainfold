//! Observability contract — the structured `tracing` spans and events that
//! chainfold emits at its RPC and cache boundaries.
//!
//! This is a stable *contract*, not incidental logging. Two consumers depend
//! on it:
//!
//! * The CLI `--stats` summary (`src/bin/chainfold/stats.rs`) installs a
//!   [`tracing_subscriber::Layer`] that tallies these into an end-of-run
//!   report: RPC calls by method (+ summed wall-time), `SQLite` read time, and
//!   cache hit/miss counts. This is how the "minimize RPC" guarantee stays
//!   visible — and assertable in tests — as the code grows.
//! * A server-side daemon embedding chainfold routes the same spans to an
//!   OTLP exporter (Tempo/Jaeger). RPC spans become child spans of whatever
//!   request span the daemon opened — no chainfold-specific glue needed.
//!
//! ## The contract
//!
//! | Kind | target | span name | fields |
//! |---|---|---|---|
//! | RPC call (span) | `chainfold::rpc` | `rpc` | `method: &str` |
//! | Connection setup (span) | `chainfold::connect` | `connect` | — |
//! | `SQLite` read (span) | `chainfold::sqlite` | `sqlite` | `op: &str` |
//! | Cache hit/miss (event) | `chainfold::cache` | — | `layer: &str`, `outcome: &str` |
//!
//! Spans carry duration (open → close); a consumer sums it per `method`/`op`.
//! Cache events are instantaneous counters keyed by `layer` + `outcome`.
//!
//! Connect spans ([`ConnectTimingLayer`]) time connection establishment
//! (DNS + TCP + TLS). A connection is opened lazily by the first request that
//! needs it, so a connect span *nests inside* that request's RPC span —
//! consumers must report it separately, never add it to an RPC total (that
//! would double-count the handshake).
//!
//! The wire-level field values are strings (what `tracing`/OTLP want), but the
//! *emission API* is typed: [`RpcMethod`], [`SqliteOp`], [`CacheLayer`], and
//! [`CacheOutcome`] are closed sets, so call sites can't emit a stray method
//! name or flip a hit/miss boolean by accident. Everything is emitted at DEBUG
//! (spans) / TRACE (events) and costs nothing when no interested subscriber is
//! installed.

use std::task::{Context, Poll};

use tracing::instrument::Instrumented;
use tracing::{Instrument, Level, Span};

/// Target for per-RPC-call spans. Kept in sync by hand with the literal in
/// [`rpc_span`] — `tracing`'s `target:` slot only accepts a string literal,
/// so the const exists for the *subscriber* side to match against.
pub const RPC_TARGET: &str = "chainfold::rpc";
/// Target for connection-establishment spans (see [`connect_span`]).
pub const CONNECT_TARGET: &str = "chainfold::connect";
/// Target for durable SQLite-read spans (see [`sqlite_span`]).
pub const SQLITE_TARGET: &str = "chainfold::sqlite";
/// Target for cache hit/miss events (see [`cache_event`]).
pub const CACHE_TARGET: &str = "chainfold::cache";

/// A JSON-RPC method chainfold issues. Closed set: every wire call the library
/// makes maps to one of these, so the stats summary and any trace exporter see
/// a bounded, typed vocabulary instead of free-form method strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RpcMethod {
    GetLogs,
    GetBlockByNumber,
    BlockNumber,
    ChainId,
    Call,
}

impl RpcMethod {
    /// The wire method name, used both as the span field value and in the
    /// stats summary.
    pub const fn as_str(self) -> &'static str {
        match self {
            RpcMethod::GetLogs => "eth_getLogs",
            RpcMethod::GetBlockByNumber => "eth_getBlockByNumber",
            RpcMethod::BlockNumber => "eth_blockNumber",
            RpcMethod::ChainId => "eth_chainId",
            RpcMethod::Call => "eth_call",
        }
    }

    /// Inverse of [`Self::as_str`] — recover the method from a span's wire
    /// label so a subscriber can key by the typed value, not a raw string.
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "eth_getLogs" => RpcMethod::GetLogs,
            "eth_getBlockByNumber" => RpcMethod::GetBlockByNumber,
            "eth_blockNumber" => RpcMethod::BlockNumber,
            "eth_chainId" => RpcMethod::ChainId,
            "eth_call" => RpcMethod::Call,
            _ => return None,
        })
    }
}

/// A durable `SQLite` read worth timing. Closed set for the same reason as
/// [`RpcMethod`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SqliteOp {
    GetLogsInRange,
    GetBlocksInRange,
    GetScannedRanges,
}

impl SqliteOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            SqliteOp::GetLogsInRange => "get_logs_in_range",
            SqliteOp::GetBlocksInRange => "get_blocks_in_range",
            SqliteOp::GetScannedRanges => "get_scanned_ranges",
        }
    }

    /// Inverse of [`Self::as_str`].
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "get_logs_in_range" => SqliteOp::GetLogsInRange,
            "get_blocks_in_range" => SqliteOp::GetBlocksInRange,
            "get_scanned_ranges" => SqliteOp::GetScannedRanges,
            _ => return None,
        })
    }
}

/// A cache whose hit/miss ratio is worth reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CacheLayer {
    /// Per-block header lookup (`cached_block_headers`).
    BlockHeader,
    /// ERC20 name/symbol/decimals (`erc20_metadata`).
    Erc20Metadata,
    /// Event-signature / ABI resolver cache.
    Signature,
    /// Singleflight coalescing: a [`CacheOutcome::Hit`] here means a concurrent
    /// caller was joined onto an in-flight fetch, i.e. an RPC was *saved*.
    Singleflight,
}

impl CacheLayer {
    pub const fn as_str(self) -> &'static str {
        match self {
            CacheLayer::BlockHeader => "block_header",
            CacheLayer::Erc20Metadata => "erc20_metadata",
            CacheLayer::Signature => "signature",
            CacheLayer::Singleflight => "singleflight",
        }
    }

    /// Inverse of [`Self::as_str`].
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "block_header" => CacheLayer::BlockHeader,
            "erc20_metadata" => CacheLayer::Erc20Metadata,
            "signature" => CacheLayer::Signature,
            "singleflight" => CacheLayer::Singleflight,
            _ => return None,
        })
    }
}

/// Whether a cache lookup was served locally ([`CacheOutcome::Hit`]) or fell
/// through to the network / a fresh fetch ([`CacheOutcome::Miss`]). Replaces a
/// blind `bool` so call sites read `CacheOutcome::Hit`, not `true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
}

impl CacheOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            CacheOutcome::Hit => "hit",
            CacheOutcome::Miss => "miss",
        }
    }

    /// Inverse of [`Self::as_str`].
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "hit" => CacheOutcome::Hit,
            "miss" => CacheOutcome::Miss,
            _ => return None,
        })
    }
}

/// Open a span around one JSON-RPC call. Attach it to the call's future with
/// [`tracing::Instrument::instrument`] so its open → close duration measures
/// real RPC latency:
///
/// ```ignore
/// use tracing::Instrument;
/// let logs = provider.get_logs(&filter).instrument(rpc_span(RpcMethod::GetLogs)).await?;
/// ```
pub fn rpc_span(method: RpcMethod) -> Span {
    tracing::span!(target: "chainfold::rpc", Level::DEBUG, "rpc", method = method.as_str())
}

/// Open a span around one connection establishment (DNS + TCP + TLS).
/// Emitted by [`ConnectTimingLayer`]; its open → close duration is the
/// handshake cost that is otherwise invisible inside the first RPC span on a
/// fresh connection.
pub fn connect_span() -> Span {
    tracing::span!(target: "chainfold::connect", Level::DEBUG, "connect")
}

/// Open a span around one durable `SQLite` read. Enter it with [`Span::entered`]
/// around the synchronous read so its duration is captured.
pub fn sqlite_span(op: SqliteOp) -> Span {
    tracing::span!(target: "chainfold::sqlite", Level::DEBUG, "sqlite", op = op.as_str())
}

/// Record the outcome of a cache lookup against `layer`.
pub fn cache_event(layer: CacheLayer, outcome: CacheOutcome) {
    tracing::event!(
        target: "chainfold::cache",
        Level::TRACE,
        layer = layer.as_str(),
        outcome = outcome.as_str(),
    );
}

/// Tower layer that times connection establishment under a [`connect_span`].
///
/// Wrap a client's *connector* with it — e.g.
/// `reqwest::ClientBuilder::connector_layer(ConnectTimingLayer)` — so every
/// new connection's DNS + TCP + TLS cost is measured. Pooled requests reuse
/// established connections and never reach the connector, so the span fires
/// exactly once per physical connection.
///
/// Deliberately reqwest-agnostic: it wraps any [`tower_service::Service`], so
/// the same layer instruments whatever HTTP stack a consumer builds.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectTimingLayer;

impl<S> tower_layer::Layer<S> for ConnectTimingLayer {
    type Service = ConnectTiming<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectTiming { inner }
    }
}

/// The service [`ConnectTimingLayer`] produces: forwards to `inner`,
/// instrumenting each connect future with a [`connect_span`].
#[derive(Debug, Clone)]
pub struct ConnectTiming<S> {
    inner: S,
}

impl<S, Req> tower_service::Service<Req> for ConnectTiming<S>
where
    S: tower_service::Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.inner.call(req).instrument(connect_span())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    /// One span the contract emitted: which boundary it marks (`target`, e.g.
    /// [`RPC_TARGET`]) and its label — the `method` field for an RPC span, the
    /// `op` field for a `SQLite` span.
    #[derive(Debug, PartialEq, Eq)]
    struct CapturedSpan {
        target: &'static str,
        label: String,
    }

    /// One cache hit/miss event the contract emitted.
    #[derive(Debug, PartialEq, Eq)]
    struct CapturedEvent {
        layer: String,
        outcome: String,
    }

    /// Records the emitted contract so the tests can assert it without
    /// depending on the CLI's aggregating layer.
    #[derive(Clone, Default)]
    struct Capture {
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    #[derive(Default)]
    struct FieldGrab {
        method: Option<String>,
        op: Option<String>,
        layer: Option<String>,
        outcome: Option<String>,
    }
    impl Visit for FieldGrab {
        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "method" => self.method = Some(value.to_owned()),
                "op" => self.op = Some(value.to_owned()),
                "layer" => self.layer = Some(value.to_owned()),
                "outcome" => self.outcome = Some(value.to_owned()),
                _ => {}
            }
        }
        fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
    }

    impl<S> Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
            let mut g = FieldGrab::default();
            attrs.record(&mut g);
            let label = g.method.or(g.op).unwrap_or_default();
            self.spans.lock().unwrap().push(CapturedSpan {
                target: attrs.metadata().target(),
                label,
            });
        }

        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != CACHE_TARGET {
                return;
            }
            let mut g = FieldGrab::default();
            event.record(&mut g);
            if let (Some(layer), Some(outcome)) = (g.layer, g.outcome) {
                self.events
                    .lock()
                    .unwrap()
                    .push(CapturedEvent { layer, outcome });
            }
        }
    }

    #[test]
    fn targets_match_literals() {
        assert_eq!(RPC_TARGET, "chainfold::rpc");
        assert_eq!(CONNECT_TARGET, "chainfold::connect");
        assert_eq!(SQLITE_TARGET, "chainfold::sqlite");
        assert_eq!(CACHE_TARGET, "chainfold::cache");
    }

    /// `from_wire` is the exact inverse of `as_str` for every variant — so a
    /// subscriber that keys by the parsed enum can never silently drop an
    /// emitted label.
    #[test]
    fn wire_labels_round_trip() {
        for m in [
            RpcMethod::GetLogs,
            RpcMethod::GetBlockByNumber,
            RpcMethod::BlockNumber,
            RpcMethod::ChainId,
            RpcMethod::Call,
        ] {
            assert_eq!(RpcMethod::from_wire(m.as_str()), Some(m));
        }
        for op in [
            SqliteOp::GetLogsInRange,
            SqliteOp::GetBlocksInRange,
            SqliteOp::GetScannedRanges,
        ] {
            assert_eq!(SqliteOp::from_wire(op.as_str()), Some(op));
        }
        for layer in [
            CacheLayer::BlockHeader,
            CacheLayer::Erc20Metadata,
            CacheLayer::Signature,
            CacheLayer::Singleflight,
        ] {
            assert_eq!(CacheLayer::from_wire(layer.as_str()), Some(layer));
        }
        for outcome in [CacheOutcome::Hit, CacheOutcome::Miss] {
            assert_eq!(CacheOutcome::from_wire(outcome.as_str()), Some(outcome));
        }
    }

    #[test]
    fn emits_rpc_and_sqlite_spans_with_labels() {
        let cap = Capture::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        with_default(subscriber, || {
            drop(rpc_span(RpcMethod::GetLogs).entered());
            drop(sqlite_span(SqliteOp::GetLogsInRange).entered());
        });
        let spans = cap.spans.lock().unwrap();
        assert!(spans.contains(&CapturedSpan {
            target: RPC_TARGET,
            label: RpcMethod::GetLogs.as_str().to_owned(),
        }));
        assert!(spans.contains(&CapturedSpan {
            target: SQLITE_TARGET,
            label: SqliteOp::GetLogsInRange.as_str().to_owned(),
        }));
    }

    /// The connector layer emits one connect span per service call — driving
    /// a wrapped dummy connector to completion must leave a captured span on
    /// [`CONNECT_TARGET`].
    #[test]
    fn connect_timing_layer_emits_connect_span() {
        use futures_util::FutureExt;

        /// Stand-in for a connector: always ready, resolves immediately.
        struct DummyConnector;
        impl tower_service::Service<()> for DummyConnector {
            type Response = ();
            type Error = std::convert::Infallible;
            type Future = std::future::Ready<Result<(), Self::Error>>;

            fn poll_ready(
                &mut self,
                _cx: &mut std::task::Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, (): ()) -> Self::Future {
                std::future::ready(Ok(()))
            }
        }

        let cap = Capture::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        with_default(subscriber, || {
            use tower_layer::Layer as _;
            use tower_service::Service as _;
            let mut svc = ConnectTimingLayer.layer(DummyConnector);
            svc.call(())
                .now_or_never()
                .expect("ready future completes in one poll")
                .expect("dummy connector is infallible");
        });
        let spans = cap.spans.lock().unwrap();
        assert!(spans.contains(&CapturedSpan {
            target: CONNECT_TARGET,
            label: String::new(),
        }));
    }

    #[test]
    fn emits_cache_hit_and_miss_events() {
        let cap = Capture::default();
        let subscriber = tracing_subscriber::registry().with(cap.clone());
        with_default(subscriber, || {
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
            cache_event(CacheLayer::Erc20Metadata, CacheOutcome::Miss);
        });
        let events = cap.events.lock().unwrap();
        assert!(events.contains(&CapturedEvent {
            layer: CacheLayer::BlockHeader.as_str().to_owned(),
            outcome: CacheOutcome::Hit.as_str().to_owned(),
        }));
        assert!(events.contains(&CapturedEvent {
            layer: CacheLayer::Erc20Metadata.as_str().to_owned(),
            outcome: CacheOutcome::Miss.as_str().to_owned(),
        }));
    }
}
