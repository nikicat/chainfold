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
use cli::{ChainsCommand, Cli, Command, ConfigCommand, Direction, FollowMode};
use config::Config;
use stats::Stats;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::layer as fmt_layer;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> ExitCode {
    let started = Instant::now();
    let cli = Cli::parse();

    // Stats are opt-in via `--stats` or `CHAINFOLD_STATS` (any non-empty,
    // non-"0" value). When on, install an aggregating layer alongside the
    // normal fmt logger; the env filter gates only the fmt layer, so the
    // summary is independent of RUST_LOG.
    let stats = (cli.stats || stats_env_enabled()).then(Stats::default);
    tracing_subscriber::registry()
        .with(fmt_layer().with_filter(EnvFilter::from_default_env()))
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

/// `CHAINFOLD_STATS` is truthy when set to any value other than empty or `0`.
fn stats_env_enabled() -> bool {
    std::env::var("CHAINFOLD_STATS")
        .ok()
        .is_some_and(|v| !v.is_empty() && v != "0")
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
