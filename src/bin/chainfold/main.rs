mod bitcoin;
mod cache;
mod chains;
mod cli;
mod config;
mod evm;
mod fmt;
mod signature_cache;
mod stats;
mod util;

use std::process::ExitCode;
use std::time::Instant;

use chains::{ChainResolver, ChainlistEntry, ListFilter};
use clap::Parser;
use cli::{ChainsCommand, Cli, Command, ConfigCommand, Direction, FollowMode, Verbosity};
use config::Config;
use stats::Stats;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{Directive, LevelFilter};
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
    // -v layers extra directives on top of either.
    let stats = cli.stats.then(Stats::default);
    let mut log_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .from_env_lossy();
    if let Some(directive) = verbosity_directive(Verbosity::from_count(cli.verbose)) {
        log_filter = log_filter.add_directive(directive);
    }
    tracing_subscriber::registry()
        .with(
            fmt_layer()
                .with_writer(std::io::stderr)
                .with_filter(log_filter),
        )
        .with(stats.clone())
        .init();

    let result = run(&cli).await;

    if let Some(stats) = &stats {
        stats.print_summary(started.elapsed());
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
                    alchemy_only: list_args.alchemy_supported,
                };
                print_chain_list(&resolver.list(filter));
            }
        },
        Command::Evm(evm_args) => {
            let config = Config::load_optional(cli.config.as_deref().map(std::path::Path::new))?;
            evm::run(
                &config,
                evm_args,
                Direction::from_reverse(evm_args.reverse),
                FollowMode::from_follow(evm_args.follow),
            )
            .await?;
        }
        Command::Bitcoin(bitcoin_args) => {
            let config = Config::load_optional(cli.config.as_deref().map(std::path::Path::new))?;
            bitcoin::run(
                &config,
                bitcoin_args,
                Direction::from_reverse(bitcoin_args.reverse),
                FollowMode::from_follow(bitcoin_args.follow),
            )
            .await?;
        }
        Command::Cache(cache_args) => cache::run(cache_args)?,
    }

    Ok(())
}

/// Print the `chains list` table to stdout: `chainId · shortName · chainSlug ·
/// name`, one row per resolvable chain. A missing `chainSlug` renders as `-`.
fn print_chain_list(entries: &[&ChainlistEntry]) {
    if entries.is_empty() {
        eprintln!("no chains match the given filters.");
        return;
    }
    println!(
        "{:>12}  {:<14}  {:<18}  name",
        "chainId", "shortName", "chainSlug"
    );
    for e in entries {
        println!(
            "{:>12}  {:<14}  {:<18}  {}",
            e.chain_id,
            e.short_name,
            e.chain_slug.as_deref().unwrap_or("-"),
            e.name,
        );
    }
}
