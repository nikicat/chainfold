//! `--stats` session summary.
//!
//! A [`tracing_subscriber::Layer`] that aggregates the observability contract
//! the library emits (`chainfold::evm::observe`) into an end-of-run report.
//! These are the same spans a server daemon would export to OTLP; here we
//! just tally them so a human (or a test) can confirm the cache is doing its
//! job and see where wall-time went.
//!
//! Aggregation is generic: any span under a `chainfold::*` target is tallied
//! by `(target, row label)` — no vocabulary to keep in sync, so a span added
//! anywhere in the library or CLI shows up here without a code change. The
//! row label is the span's static name, except on `chainfold::rpc` where it
//! is the `method` field (the method is dynamic wire data; span names can't
//! carry it).
//!
//! Per the contract's summing rule, wall-time reconciliation counts *root*
//! spans only: a nested span (the connect handshake inside its first RPC)
//! keeps its own row but contributes nothing to the tracked total, which
//! would otherwise double-count its parent's time.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chainfold::evm::{
    CACHE_TARGET, CONNECT_TARGET, CacheLayer, CacheOutcome, RPC_TARGET, SETUP_TARGET, SQLITE_TARGET,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Everything the library emits lives under this prefix; the layer tracks
/// all of it and abstains from everything else.
const TARGET_PREFIX: &str = "chainfold::";

/// Section order and headers for the targets we know today. Spans under any
/// other `chainfold::*` target still print — appended after these, headed by
/// the target's suffix — so a new span kind can't silently vanish.
const SECTIONS: &[(&str, &str)] = &[
    (SETUP_TARGET, "setup"),
    (RPC_TARGET, "rpc calls"),
    (CONNECT_TARGET, "connections"),
    (SQLITE_TARGET, "sqlite reads"),
];

/// Call count + summed wall-time for one row (an RPC method, a `SQLite`
/// query, a setup phase).
#[derive(Default)]
struct Tally {
    count: u64,
    total: Duration,
    /// Wall-time of the *root* occurrences only. Spans are mostly sequential
    /// in the CLI, so summing this across all rows reconciles `duration`;
    /// with concurrent fetches the remainder is an upper bound on what's
    /// truly untracked (overlapping root spans double-count out).
    root_total: Duration,
}

/// Hit/miss counts for one cache layer.
#[derive(Default)]
struct HitMiss {
    hits: u64,
    misses: u64,
}

impl HitMiss {
    fn total(&self) -> u64 {
        self.hits + self.misses
    }

    /// Hit fraction in `[0, 1]`, or `None` when nothing was recorded.
    #[allow(
        clippy::cast_precision_loss,
        reason = "hit/miss counts in a CLI run never approach f64 mantissa limits"
    )]
    fn hit_rate(&self) -> Option<f64> {
        let total = self.total();
        (total > 0).then(|| self.hits as f64 / total as f64)
    }
}

/// Rows of one section: row label → tally. Labels are static span names
/// except the RPC method strings captured from span fields.
type Rows = BTreeMap<Cow<'static, str>, Tally>;

#[derive(Default)]
struct Inner {
    spans: BTreeMap<&'static str, Rows>,
    cache_by_layer: BTreeMap<CacheLayer, HitMiss>,
}

/// Installable handle: clone one into the subscriber, keep one to
/// [`print_summary`](Stats::print_summary) at the end of the run.
#[derive(Clone, Default)]
pub struct Stats {
    inner: Arc<Mutex<Inner>>,
}

/// Stashed in a tracked span's extensions at open, read back at close.
struct SpanTiming {
    start: Instant,
    /// No `chainfold::*` ancestor — this span's time enters the tracked sum.
    root: bool,
    /// Row-label override captured at open (the RPC span's `method` field).
    label: Option<String>,
}

/// Pulls the `method` field off an RPC span.
#[derive(Default)]
struct MethodField(Option<String>);
impl Visit for MethodField {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "method" {
            self.0 = Some(value.to_owned());
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

/// Pulls the `layer`/`outcome` off a cache event's fields.
#[derive(Default)]
struct CacheFields {
    layer: Option<String>,
    outcome: Option<String>,
}
impl Visit for CacheFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "layer" => self.layer = Some(value.to_owned()),
            "outcome" => self.outcome = Some(value.to_owned()),
            _ => {}
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

fn is_ours(target: &str) -> bool {
    target.starts_with(TARGET_PREFIX)
}

impl<S> Layer<S> for Stats
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    // Force chainfold's targets enabled regardless of RUST_LOG / other layers,
    // and abstain from everything else so the stats layer adds no overhead to
    // unrelated logging.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if is_ours(metadata.target()) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        is_ours(metadata.target())
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let label = (attrs.metadata().target() == RPC_TARGET).then(|| {
            let mut method = MethodField::default();
            attrs.record(&mut method);
            method.0
        });
        // Scope walks leaf → root; skip(1) drops the span itself. Foreign
        // ancestors (another layer's spans in the same registry) don't count.
        let root = !ctx
            .span_scope(id)
            .into_iter()
            .flatten()
            .skip(1)
            .any(|ancestor| is_ours(ancestor.metadata().target()));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanTiming {
                start: Instant::now(),
                root,
                label: label.flatten(),
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let timing = span.extensions_mut().remove::<SpanTiming>();
        let Some(timing) = timing else { return };
        let elapsed = timing.start.elapsed();
        let meta = span.metadata();
        let label: Cow<'static, str> = match timing.label {
            Some(method) => Cow::Owned(method),
            None => Cow::Borrowed(meta.name()),
        };
        let mut inner = self.inner.lock().unwrap();
        let tally = inner
            .spans
            .entry(meta.target())
            .or_default()
            .entry(label)
            .or_default();
        tally.count += 1;
        tally.total += elapsed;
        if timing.root {
            tally.root_total += elapsed;
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != CACHE_TARGET {
            return;
        }
        let mut fields = CacheFields::default();
        event.record(&mut fields);
        let layer = fields.layer.as_deref().and_then(CacheLayer::from_wire);
        let outcome = fields.outcome.as_deref().and_then(CacheOutcome::from_wire);
        let (Some(layer), Some(outcome)) = (layer, outcome) else {
            return;
        };
        let mut inner = self.inner.lock().unwrap();
        let hm = inner.cache_by_layer.entry(layer).or_default();
        match outcome {
            CacheOutcome::Hit => hm.hits += 1,
            CacheOutcome::Miss => hm.misses += 1,
        }
    }
}

impl Stats {
    /// Print the end-of-run report to stderr (keeps stdout clean for piped
    /// data). `wall` is the whole-invocation wall-clock.
    pub fn print_summary(&self, wall: Duration) {
        let inner = self.inner.lock().unwrap();
        eprintln!();
        eprintln!("── chainfold session summary ──");
        eprintln!("  {:<22}{:>12}", "duration", fmt_dur(wall));

        for &(target, header) in SECTIONS {
            let rows = inner.spans.get(target);
            // A zero on "rpc calls" is the headline warm-cache number —
            // always print it. Other absent sections just don't apply.
            if rows.is_none() && target != RPC_TARGET {
                continue;
            }
            print_section(header, rows);
        }
        for (target, rows) in &inner.spans {
            if SECTIONS.iter().any(|(known, _)| known == target) {
                continue;
            }
            let header = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
            print_section(header, Some(rows));
        }

        if !inner.cache_by_layer.is_empty() {
            eprintln!("  cache");
            for (layer, hm) in &inner.cache_by_layer {
                let rate = hm.hit_rate().unwrap_or(0.0) * 100.0;
                eprintln!(
                    "    {:<22}{:>4} hit / {:>4} miss  ({rate:>5.1}%)",
                    layer.as_str(),
                    hm.hits,
                    hm.misses
                );
            }
        }

        // Root spans only — the contract's summing rule (a nested connect
        // already sits inside its RPC span's duration).
        let tracked: Duration = inner
            .spans
            .values()
            .flat_map(BTreeMap::values)
            .map(|t| t.root_total)
            .sum();
        eprintln!(
            "  {:<22}{:>12}   (process init, gaps)",
            "untracked",
            fmt_dur(wall.saturating_sub(tracked))
        );
        eprintln!();
    }
}

/// One section: header line with the total call count, then a row per label.
fn print_section(header: &str, rows: Option<&Rows>) {
    let count: u64 = rows.into_iter().flatten().map(|(_, t)| t.count).sum();
    eprintln!("  {header:<22}{count:>12}");
    for (label, tally) in rows.into_iter().flatten() {
        eprintln!(
            "    {:<22}{:>5}  {:>12}",
            label,
            tally.count,
            fmt_dur(tally.total)
        );
    }
}

/// Human-readable duration: `s` / `ms` / `µs` by magnitude.
#[allow(
    clippy::cast_precision_loss,
    reason = "wall-clock microseconds in a CLI run never approach f64 mantissa limits"
)]
fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us >= 1_000_000 {
        format!("{:.2}s", d.as_secs_f64())
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}µs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainfold::evm::{cache_event, connect_span};
    use tracing::Level;
    use tracing::subscriber::with_default;
    use tracing_subscriber::prelude::*;

    /// What the library's `RpcTimingLayer` emits — a span named `rpc` on the
    /// rpc target with the wire method in the `method` field.
    macro_rules! rpc_span {
        ($method:literal) => {
            tracing::span!(target: "chainfold::rpc", Level::DEBUG, "rpc", method = $method)
        };
    }

    #[test]
    fn tallies_spans_by_target_and_label_and_cache_events() {
        let stats = Stats::default();
        let subscriber = tracing_subscriber::registry().with(stats.clone());
        with_default(subscriber, || {
            drop(rpc_span!("eth_getLogs").entered());
            drop(rpc_span!("eth_getLogs").entered());
            drop(rpc_span!("eth_getBlockByNumber").entered());
            // Span names this file has never heard of land in their target's
            // section — the vocabulary is open.
            drop(tracing::span!(target: "chainfold::setup", Level::DEBUG, "db_open").entered());
            drop(
                tracing::span!(target: "chainfold::sqlite", Level::DEBUG, "get_logs_in_range")
                    .entered(),
            );
            cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
        });

        let inner = stats.inner.lock().unwrap();
        assert_eq!(inner.spans[RPC_TARGET]["eth_getLogs"].count, 2);
        assert_eq!(inner.spans[RPC_TARGET]["eth_getBlockByNumber"].count, 1);
        assert_eq!(inner.spans[SETUP_TARGET]["db_open"].count, 1);
        assert_eq!(inner.spans[SQLITE_TARGET]["get_logs_in_range"].count, 1);

        let singleflight = &inner.cache_by_layer[&CacheLayer::Singleflight];
        assert_eq!((singleflight.hits, singleflight.misses), (1, 0));
        let header = &inner.cache_by_layer[&CacheLayer::BlockHeader];
        assert_eq!((header.hits, header.misses), (1, 1));
    }

    /// The contract's summing rule: a nested span (connect inside its RPC)
    /// keeps its row but contributes nothing to the root total; root spans
    /// contribute everything.
    #[test]
    fn nested_spans_are_tallied_but_excluded_from_root_total() {
        let stats = Stats::default();
        let subscriber = tracing_subscriber::registry().with(stats.clone());
        with_default(subscriber, || {
            let rpc = rpc_span!("eth_getLogs").entered();
            drop(connect_span().entered());
            drop(rpc);
        });

        let inner = stats.inner.lock().unwrap();
        let connect = &inner.spans[CONNECT_TARGET]["connect"];
        assert_eq!(connect.count, 1);
        assert_eq!(
            connect.root_total,
            Duration::ZERO,
            "nested connect must not enter the tracked sum"
        );
        let rpc = &inner.spans[RPC_TARGET]["eth_getLogs"];
        assert_eq!(
            rpc.root_total, rpc.total,
            "root spans enter the tracked sum in full"
        );
    }
}
