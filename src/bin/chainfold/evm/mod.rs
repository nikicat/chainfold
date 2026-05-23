pub mod erc20;
pub mod events;

use std::sync::Arc;

use alloy::providers::Provider;
use chainfold::evm::{rpc_retry_layer, versioned_path, LogStore};

use crate::cli::{EvmArgs, EvmCommand};
use crate::config::Config;

pub async fn run(
    config: &Config,
    args: &EvmArgs,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = config.rpc_url(args.rpc_url.as_deref(), args.chain_id)?;

    let url: alloy::transports::http::reqwest::Url = rpc_url.parse()?;
    let client = alloy::rpc::client::ClientBuilder::default()
        .layer(rpc_retry_layer())
        .http(url);
    let provider = alloy::providers::ProviderBuilder::new().connect_client(client);

    let db_path = versioned_path(&args.db);
    let cache = Arc::new(LogStore::open(&db_path)?);

    let chain_id = provider.get_chain_id().await?;
    if chain_id != args.chain_id {
        eprintln!(
            "warning: --chain-id {} but RPC reports chain {}",
            args.chain_id, chain_id
        );
    }

    match &args.command {
        EvmCommand::Erc20(erc20_args) => {
            erc20::run(
                &provider, &cache, &db_path, args, erc20_args, chain_id, reverse, follow,
            )
            .await
        }
        EvmCommand::Events(events_args) => {
            events::run(
                &provider,
                &cache,
                &db_path,
                config,
                args,
                events_args,
                chain_id,
                reverse,
                follow,
            )
            .await
        }
    }
}
