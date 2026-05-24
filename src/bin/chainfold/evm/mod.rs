pub mod erc20;
pub mod events;

use std::sync::Arc;

use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::reqwest::Url;
use chainfold::evm::{rpc_retry_layer, versioned_path, LogStore};

use crate::chains::ChainResolver;
use crate::cli::{EvmArgs, EvmCommand};
use crate::config::Config;

pub async fn run(
    config: &Config,
    args: &EvmArgs,
    reverse: bool,
    follow: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let chain_id = match (args.chain_id, args.chain.as_deref()) {
        (Some(id), _) => id,
        (None, Some(name)) => ChainResolver::load_or_fetch().await?.resolve(name)?,
        (None, None) => unreachable!("clap enforces required_unless_present"),
    };

    let rpc_url = config.rpc_url(args.rpc_url.as_deref(), chain_id)?;

    let url: Url = rpc_url.parse()?;
    let client = ClientBuilder::default().layer(rpc_retry_layer()).http(url);
    let provider = ProviderBuilder::new().connect_client(client);

    let db_path = versioned_path(&args.db);
    let cache = Arc::new(LogStore::open(&db_path)?);

    let rpc_chain_id = provider.get_chain_id().await?;
    if rpc_chain_id != chain_id {
        eprintln!("warning: requested chain {chain_id} but RPC reports chain {rpc_chain_id}");
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
