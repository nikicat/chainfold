mod chains;
mod cli;
mod config;
mod evm;
mod fmt;
mod signature_cache;
mod stats;
mod util;

use std::time::Instant;

use clap::Parser;
use cli::{Cli, Command, ConfigCommand};
use config::Config;
use stats::Stats;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::layer as fmt_layer;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    result
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
            let config = Config::load(cli.config.as_deref().map(std::path::Path::new))?;
            match action {
                ConfigCommand::Show => {
                    config.show();
                }
                ConfigCommand::Check => {
                    config.show();
                    println!("\nConfig is valid.");
                    // Could add RPC connectivity test here later
                }
            }
        }
        Command::Evm(evm_args) => {
            let config = Config::load_optional(cli.config.as_deref().map(std::path::Path::new))?;
            evm::run(&config, evm_args, cli.reverse, cli.follow).await?;
        }
    }

    Ok(())
}
