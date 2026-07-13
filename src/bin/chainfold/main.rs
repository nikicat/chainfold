mod bitcoin;
mod cache;
mod chains;
mod cli;
mod config;
mod evm;
mod fmt;
mod signature_cache;
mod stats;
mod table;
mod theme;
mod tokens;
mod util;

use std::process::ExitCode;
use std::time::Instant;

use chains::{ChainResolver, ChainlistEntry, ListFilter};
use clap::Parser;
use cli::{ChainsCommand, Cli, Command, ConfigCommand, Verbosity};
use config::Config;
use stats::Stats;
use table::{Cell, Column, HeaderCase, Out, TableBuf, TableSpec};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{Directive, LevelFilter};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::layer as fmt_layer;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> ExitCode {
    let started = Instant::now();
    let cli = Cli::parse();

    // Stats are opt-in via `--stats` or `CHAINFOLD_STATS`. When on, install
    // an aggregating layer alongside the normal fmt logger; the RUST_LOG/-v
    // filter gates only the fmt layer, so the summary is independent of it.
    // Logs go to stderr — stdout is reserved for command output. Warnings
    // are visible by default; setting RUST_LOG replaces that baseline, and
    // -v layers extra directives on top of either. Span close events render
    // the boundary spans (rpc, connect, sqlite, setup) as log records, so
    // -v shows each wire request with its fields and timing.
    let stats = cli.stats.then(Stats::default);
    let verbosity = Verbosity::from_count(cli.verbose);
    let mut log_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .from_env_lossy();
    if let Some(directive) = verbosity_directive(verbosity) {
        log_filter = log_filter.add_directive(directive);
    }
    // Span lifecycle events inherit the span's own level, so `new` can't be
    // demoted below `close`; gate it on the trace tier instead. `close`
    // alone carries the span's fields plus timing — `new` adds only the
    // start timestamp, which earns its noise once requests may hang.
    let span_events = match verbosity {
        Verbosity::AllTrace => FmtSpan::NEW | FmtSpan::CLOSE,
        _ => FmtSpan::CLOSE,
    };
    tracing_subscriber::registry()
        .with(
            fmt_layer()
                .with_writer(std::io::stderr)
                .with_span_events(span_events)
                .with_filter(log_filter),
        )
        .with(stats.clone())
        .init();

    let result = run(&cli).await;

    // The summary goes to stderr — its own stream, its own mode.
    if let Some(stats) = &stats {
        stats.print_summary(started.elapsed(), Out::stderr());
    }

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // Render via `Display` (the `#[error(...)]` strings), not the
            // derived `Debug`. Our error types embed their `source` into their
            // Display message, so the top-level line is the full, readable
            // report — walking `.source()` here would only repeat that text.
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// The extra `EnvFilter` directive a [`Verbosity`] level layers over
/// `RUST_LOG` (both apply; on a tie the flag wins).
fn verbosity_directive(verbosity: Verbosity) -> Option<Directive> {
    match verbosity {
        Verbosity::Baseline => None,
        Verbosity::OwnDebug => Some("chainfold=debug".parse().expect("static directive parses")),
        Verbosity::AllDebug => Some(LevelFilter::DEBUG.into()),
        Verbosity::AllTrace => Some(LevelFilter::TRACE.into()),
    }
}

async fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Command output goes to stdout; `Out` binds that stream to the render
    // mode sniffed from it, once, here at the edge (the boundary-funnel —
    // see `table.rs`).
    let out = Out::stdout();
    match &cli.command {
        Command::Config { action } => {
            let (config, source) =
                Config::load_resolved(cli.config.as_deref().map(std::path::Path::new))?;
            match action {
                ConfigCommand::Show => {
                    config.show(source.as_deref());
                }
                ConfigCommand::Check => {
                    config.show(source.as_deref());
                    println!("\nConfig is valid.");
                    // Could add RPC connectivity test here later
                }
            }
        }
        Command::Chains { action } => match action {
            ChainsCommand::List(list_args) => {
                let resolver = ChainResolver::load_or_fetch().await?;
                let filter = ListFilter {
                    include_testnets: list_args.testnets,
                    provider: list_args.provider,
                };
                print_chain_list(&resolver.list(filter), out);
            }
        },
        Command::Tokens { action } => tokens::run(cli.db.as_deref(), action, out).await?,
        Command::Evm(evm_args) => {
            let config = Config::load_optional(cli.config.as_deref().map(std::path::Path::new))?;
            evm::run(&config, cli.db.as_deref(), evm_args, out).await?;
        }
        Command::Bitcoin(bitcoin_args) => {
            let config = Config::load_optional(cli.config.as_deref().map(std::path::Path::new))?;
            bitcoin::run(&config, cli.db.as_deref(), bitcoin_args, out).await?;
        }
        Command::Cache(cache_args) => cache::run(cli.db.as_deref(), cache_args, out)?,
    }

    Ok(())
}

/// Print the `chains list` table to stdout: `chainId · shortName · chainSlug ·
/// name`, one row per resolvable chain. A missing `chainSlug` renders as `-`.
fn print_chain_list(entries: &[&ChainlistEntry], out: Out) {
    if entries.is_empty() {
        eprintln!("no chains match the given filters.");
        return;
    }
    // chainlist.org field identifiers — their casing is meaningful, keep it.
    let mut table = TableBuf::new(
        TableSpec::new(vec![
            Column::right("chainId").min(12),
            Column::left("shortName").min(14),
            Column::left("chainSlug").min(18),
            Column::left("name"),
        ])
        .header_case(HeaderCase::AsIs),
    );
    for e in entries {
        table.push(vec![
            Cell::new(e.chain_id.to_string()),
            Cell::new(e.short_name.as_str()),
            Cell::new(e.chain_slug.as_deref().unwrap_or("-")),
            Cell::new(e.name.as_str()),
        ]);
    }
    out.table(&table);
}
