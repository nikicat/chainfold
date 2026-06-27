//! `--stats` session summary.
//!
//! A [`tracing_subscriber::Layer`] that aggregates the observability contract
//! the library emits (`chainfold::evm::observe`) — RPC spans, SQLite-read
//! spans, and cache hit/miss events — into an end-of-run report. These are the
//! same spans a server daemon would export to OTLP; here we just tally them so
//! a human (or a test) can confirm the cache is doing its job and see where
//! wall-time went.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chainfold::evm::{
    CACHE_TARGET, CacheLayer, CacheOutcome, RPC_TARGET, RpcMethod, SQLITE_TARGET, SqliteOp,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Call count + summed wall-time for one labelled operation (an RPC method or
/// a `SQLite` query).
#[derive(Default)]
struct Tally {
    count: u64,
    total: Duration,
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

#[derive(Default)]
struct Inner {
    rpc_by_method: BTreeMap<RpcMethod, Tally>,
    sqlite_by_op: BTreeMap<SqliteOp, Tally>,
    cache_by_layer: BTreeMap<CacheLayer, HitMiss>,
}

/// Installable handle: clone one into the subscriber, keep one to
/// [`print_summary`](Stats::print_summary) at the end of the run.
#[derive(Clone, Default)]
pub struct Stats {
    inner: Arc<Mutex<Inner>>,
}

/// A timed span's typed identity — what to credit the duration to at close.
#[derive(Clone, Copy)]
enum TimedOp {
    Rpc(RpcMethod),
    Sqlite(SqliteOp),
}

/// Stashed in a tracked span's extensions at open, read back at close.
struct SpanTiming {
    op: TimedOp,
    start: Instant,
}

/// Pulls the `method`/`op` label off a span's fields.
#[derive(Default)]
struct SpanLabel {
    method: Option<String>,
    op: Option<String>,
}
impl Visit for SpanLabel {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "method" => self.method = Some(value.to_owned()),
            "op" => self.op = Some(value.to_owned()),
            _ => {}
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
    target == RPC_TARGET || target == SQLITE_TARGET || target == CACHE_TARGET
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
        let mut label = SpanLabel::default();
        attrs.record(&mut label);
        let op = match attrs.metadata().target() {
            RPC_TARGET => label
                .method
                .as_deref()
                .and_then(RpcMethod::from_wire)
                .map(TimedOp::Rpc),
            SQLITE_TARGET => label
                .op
                .as_deref()
                .and_then(SqliteOp::from_wire)
                .map(TimedOp::Sqlite),
            _ => None,
        };
        let Some(op) = op else { return };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanTiming {
                op,
                start: Instant::now(),
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let timing = span.extensions_mut().remove::<SpanTiming>();
        let Some(timing) = timing else { return };
        let elapsed = timing.start.elapsed();
        let mut inner = self.inner.lock().unwrap();
        let tally = match timing.op {
            TimedOp::Rpc(method) => inner.rpc_by_method.entry(method).or_default(),
            TimedOp::Sqlite(op) => inner.sqlite_by_op.entry(op).or_default(),
        };
        tally.count += 1;
        tally.total += elapsed;
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
        eprintln!("  duration              {:>12}", fmt_dur(wall));

        let rpc_total: u64 = inner.rpc_by_method.values().map(|t| t.count).sum();
        eprintln!("  rpc calls             {rpc_total:>12}");
        for (method, tally) in &inner.rpc_by_method {
            eprintln!(
                "    {:<22}{:>5}  {:>12}",
                method.as_str(),
                tally.count,
                fmt_dur(tally.total)
            );
        }

        if !inner.sqlite_by_op.is_empty() {
            let sqlite_total: u64 = inner.sqlite_by_op.values().map(|t| t.count).sum();
            eprintln!("  sqlite reads          {sqlite_total:>12}");
            for (op, tally) in &inner.sqlite_by_op {
                eprintln!(
                    "    {:<22}{:>5}  {:>12}",
                    op.as_str(),
                    tally.count,
                    fmt_dur(tally.total)
                );
            }
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
        eprintln!();
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
    use chainfold::evm::{cache_event, rpc_span, sqlite_span};
    use tracing::subscriber::with_default;
    use tracing_subscriber::prelude::*;

    #[test]
    fn tallies_rpc_sqlite_spans_and_cache_events() {
        let stats = Stats::default();
        let subscriber = tracing_subscriber::registry().with(stats.clone());
        with_default(subscriber, || {
            drop(rpc_span(RpcMethod::GetLogs).entered());
            drop(rpc_span(RpcMethod::GetLogs).entered());
            drop(rpc_span(RpcMethod::GetBlockByNumber).entered());
            drop(sqlite_span(SqliteOp::GetLogsInRange).entered());
            cache_event(CacheLayer::Singleflight, CacheOutcome::Hit);
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Miss);
            cache_event(CacheLayer::BlockHeader, CacheOutcome::Hit);
        });

        let inner = stats.inner.lock().unwrap();
        assert_eq!(inner.rpc_by_method[&RpcMethod::GetLogs].count, 2);
        assert_eq!(inner.rpc_by_method[&RpcMethod::GetBlockByNumber].count, 1);
        assert_eq!(inner.sqlite_by_op[&SqliteOp::GetLogsInRange].count, 1);

        let singleflight = &inner.cache_by_layer[&CacheLayer::Singleflight];
        assert_eq!((singleflight.hits, singleflight.misses), (1, 0));
        let header = &inner.cache_by_layer[&CacheLayer::BlockHeader];
        assert_eq!((header.hits, header.misses), (1, 1));
    }
}
