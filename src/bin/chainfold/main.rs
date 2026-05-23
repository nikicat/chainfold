mod cli;
mod config;
mod evm;
mod fmt;
mod signature_cache;
mod util;

use clap::Parser;
use cli::{Cli, Command, ConfigCommand};
use config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

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
            let config = config.unwrap_or_else(|| {
                // No config file — require --rpc-url
                Config {
                    provider: None,
                    etherscan: None,
                }
            });
            evm::run(&config, evm_args, cli.reverse, cli.follow).await?;
        }
    }

    Ok(())
}
