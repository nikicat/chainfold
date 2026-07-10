//! `--stats` session summary.
//!
//! A [`tracing_subscriber::Layer`] that aggregates the observability contract
//! the library emits (`chainfold::observe`) into an end-of-run report.
//! These are the same spans a server daemon would export to OTLP; here we
//! just tally them so a human (or a test) can confirm the cache is doing its
//! job and see where wall-time went.
//!
//! Aggregation is generic: any span under a `chainfold::*` target is tallied
//! by `(target, row label)` — no vocabulary to keep in sync, so a span added
//! anywhere in the library or CLI shows up here without a code change. The
//! row label is the span's static name, except on `chainfold::rpc` where it
//! is the `method` field (the method is dynamic wire data; span names can't
//! carry it). RPC spans also carry a `reqs` field — one wire request may
//! batch many JSON-RPC calls — so where batching makes them differ, the
//! report shows the logical call count as the headline and annotates the
//! wire round-trips.
//!
//! Per the contract's summing rule, wall-time reconciliation counts *root*
//! spans only: a nested span (the connect handshake inside its first RPC)
//! keeps its own row but contributes nothing to the tracked total, which
//! would otherwise double-count its parent's time.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chainfold::observe::{
    CACHE_TARGET, CONNECT_TARGET, CacheLayer, CacheOutcome, RPC_TARGET, SETUP_TARGET, SQLITE_TARGET,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::table::{Cell, Column, Out, TableBuf, TableSpec};

/// Everything the library emits lives under this prefix; the layer tracks
/// all of it and abstains from everything else.
const TARGET_PREFIX: &str = "chainfold::";

/// Presentation of one known span target: its summary-section header and
/// what a row label *means* there (the row-label column's header).
struct Section {
    target: &'static str,
    header: &'static str,
    /// An RPC row is labeled by method, a setup row by phase, …
    label: &'static str,
}

/// Section order for the targets we know today. Spans under any other
/// `chainfold::*` target still print — appended after these, headed by the
/// target's suffix with a generic `span` label column — so a new span kind
/// can't silently vanish.
const SECTIONS: &[Section] = &[
    Section {
        target: SETUP_TARGET,
        header: "setup",
        label: "phase",
    },
    Section {
        target: RPC_TARGET,
        header: "rpc calls",
        label: "method",
    },
    Section {
        target: CONNECT_TARGET,
        header: "connections",
        label: "connection",
    },
    Section {
        target: SQLITE_TARGET,
        header: "sqlite reads",
        label: "query",
    },
];

/// Call count + summed wall-time for one row (an RPC method, a `SQLite`
/// query, a setup phase).
#[derive(Default)]
struct Tally {
    count: u64,
    /// Logical requests: RPC spans contribute their `reqs` field (the wire
    /// packet's batch length), every other span contributes 1 — so
    /// `reqs == count` exactly when nothing was batched.
    reqs: u64,
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
    /// Logical requests this span covers (the RPC span's `reqs` field; 1
    /// everywhere else).
    reqs: u64,
}

/// Pulls the `method` and `reqs` fields off an RPC span.
#[derive(Default)]
struct RpcFields {
    method: Option<String>,
    reqs: Option<u64>,
}
impl Visit for RpcFields {
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
        let (label, reqs) = if attrs.metadata().target() == RPC_TARGET {
            let mut fields = RpcFields::default();
            attrs.record(&mut fields);
            (fields.method, fields.reqs.unwrap_or(1))
        } else {
            (None, 1)
        };
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
                label,
                reqs,
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
        tally.reqs += timing.reqs;
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
    /// Print the end-of-run report to `out` (stderr, so stdout stays clean
    /// for piped data). `wall` is the whole-invocation wall-clock.
    pub fn print_summary(&self, wall: Duration, out: Out) {
        let inner = self.inner.lock().unwrap();
        out.blank_line();
        let mut head = TableBuf::new(kv_spec());
        head.title("chainfold session summary");
        head.push(kv_row("duration", wall, ""));
        out.table(&head);

        for section in SECTIONS {
            let rows = inner.spans.get(section.target);
            // A zero on "rpc calls" is the headline warm-cache number —
            // always print it. Other absent sections just don't apply.
            if rows.is_none() && section.target != RPC_TARGET {
                continue;
            }
            print_section(out, section.header, section.label, rows);
        }
        for (target, rows) in &inner.spans {
            if SECTIONS.iter().any(|known| known.target == *target) {
                continue;
            }
            let header = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
            print_section(out, header, "span", Some(rows));
        }

        if !inner.cache_by_layer.is_empty() {
            let mut table = TableBuf::new(TableSpec::new(vec![
                Column::left("layer").min(22),
                Column::right("hits"),
                Column::right("misses"),
                Column::right("rate"),
            ]));
            table.title("cache");
            for (layer, hm) in &inner.cache_by_layer {
                let rate = hm
                    .hit_rate()
                    .map_or_else(|| "-".to_string(), |r| format!("{:.1}%", r * 100.0));
                table.push(vec![
                    Cell::new(layer.as_str()),
                    Cell::new(hm.hits.to_string()),
                    Cell::new(hm.misses.to_string()),
                    Cell::new(rate),
                ]);
            }
            out.blank_line();
            out.table(&table);
        }

        // Root spans only — the contract's summing rule (a nested connect
        // already sits inside its RPC span's duration).
        let tracked: Duration = inner
            .spans
            .values()
            .flat_map(BTreeMap::values)
            .map(|t| t.root_total)
            .sum();
        let mut tail = TableBuf::new(kv_spec());
        tail.push(kv_row(
            "untracked",
            wall.saturating_sub(tracked),
            "(process init, gaps)",
        ));
        out.blank_line();
        out.table(&tail);
        out.blank_line();
    }
}

/// Label / time / note rows for the flat summary lines (`duration`,
/// `untracked`) — self-labeled, so headerless (and thus content-width).
fn kv_spec() -> TableSpec {
    TableSpec::new(vec![
        Column::left("label"),
        Column::right("time"),
        Column::left("note"),
    ])
    .no_header()
}

fn kv_row(label: &str, time: Duration, note: &str) -> Vec<Cell> {
    vec![Cell::new(label), Cell::new(fmt_dur(time)), Cell::new(note)]
}

/// One section as its own table: the title carries the section's total
/// logical-call count (plus the wire total where batching made them
/// differ), and each row labels its numbers — CALLS is logical calls, WIRE
/// (present only in batched sections) the wire round-trips they rode in,
/// TIME the summed wall-time.
fn print_section(out: Out, header: &str, label_header: &'static str, rows: Option<&Rows>) {
    let (count, reqs) = rows
        .into_iter()
        .flatten()
        .fold((0u64, 0u64), |(c, r), (_, t)| (c + t.count, r + t.reqs));
    let title = format!("{header}: {reqs}{}", wire_note(count, reqs));
    out.blank_line();
    let Some(rows) = rows else {
        // The rpc-zero headline (warm cache) — nothing to tabulate.
        out.heading(&title);
        return;
    };
    let batched = count != reqs;
    let mut cols = vec![
        Column::left(label_header).min(22),
        Column::right("calls").min(5),
    ];
    if batched {
        cols.push(Column::right("wire"));
    }
    cols.push(Column::right("time").min(9));
    let mut table = TableBuf::new(TableSpec::new(cols));
    table.title(title);
    for (label, tally) in rows {
        let mut row = vec![Cell::new(label.as_ref()), Cell::new(tally.reqs.to_string())];
        if batched {
            row.push(Cell::new(tally.count.to_string()));
        }
        row.push(Cell::new(fmt_dur(tally.total)));
        table.push(row);
    }
    out.table(&table);
}

/// ` (N wire requests)` when batching packed several JSON-RPC calls into
/// fewer wire round-trips; empty when the counts match (the unbatched case).
fn wire_note(count: u64, reqs: u64) -> String {
    if reqs == count {
        String::new()
    } else {
        format!(" ({count} wire requests)")
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
    use chainfold::observe::{cache_event, connect_span};
    use tracing::Level;
    use tracing::subscriber::with_default;
    use tracing_subscriber::prelude::*;

    /// What the library's `RpcTimingLayer` emits — a span named `rpc` on the
    /// rpc target with the wire method in the `method` field and the
    /// packet's request count in `reqs`. The one-argument form leaves `reqs`
    /// off to pin the visitor's default of 1.
    macro_rules! rpc_span {
        ($method:literal) => {
            tracing::span!(target: "chainfold::rpc", Level::DEBUG, "rpc", method = $method)
        };
        ($method:literal, reqs = $reqs:expr) => {
            tracing::span!(
                target: "chainfold::rpc", Level::DEBUG, "rpc",
                method = $method, reqs = $reqs
            )
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

    /// Batched RPC spans carry the packet's request count in `reqs`; the
    /// tally sums it separately from the span count so the summary can show
    /// logical JSON-RPC calls alongside wire round-trips. Spans without the
    /// field count as one request each, so `reqs == count` off the RPC path.
    #[test]
    fn batched_rpc_spans_sum_logical_requests() {
        let stats = Stats::default();
        let subscriber = tracing_subscriber::registry().with(stats.clone());
        with_default(subscriber, || {
            drop(rpc_span!("eth_getBlockByNumber", reqs = 100u64).entered());
            drop(rpc_span!("eth_getBlockByNumber", reqs = 100u64).entered());
            drop(rpc_span!("eth_getLogs").entered());
            drop(tracing::span!(target: "chainfold::setup", Level::DEBUG, "db_open").entered());
        });

        let inner = stats.inner.lock().unwrap();
        let blocks = &inner.spans[RPC_TARGET]["eth_getBlockByNumber"];
        assert_eq!((blocks.count, blocks.reqs), (2, 200));
        let logs = &inner.spans[RPC_TARGET]["eth_getLogs"];
        assert_eq!((logs.count, logs.reqs), (1, 1));
        let setup = &inner.spans[SETUP_TARGET]["db_open"];
        assert_eq!((setup.count, setup.reqs), (1, 1));
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
