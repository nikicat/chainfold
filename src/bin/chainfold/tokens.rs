//! `chainfold tokens` — the cached token list (symbol → address): list the
//! snapshot, force a refresh. Like `cache`, it needs no chain or RPC — only
//! the DB path — which is why it is a top-level command and not part of the
//! `evm` subtree.

use std::path::Path;

use chainfold::cache::CacheDb;
use chainfold::evm::tokenlist::{default_token_list_url, refresh};
use chainfold::evm::{TokenListCache, TokenListMeta};
use chrono::DateTime;

use crate::chains::ChainResolver;
use crate::cli::{TokensCommand, TokensListArgs};
use crate::table::{Cell, Column, Out, TableBuf, TableSpec};
use crate::util::resolved_db_path;

pub async fn run(
    db: Option<&Path>,
    command: &TokensCommand,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = resolved_db_path(db);
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let cache = CacheDb::open(&db_path)?.token_list();
    match command {
        TokensCommand::List(list_args) => list(&cache, list_args, out).await,
        TokensCommand::Refresh => refresh_and_report(&cache).await,
    }
}

async fn refresh_and_report(cache: &TokenListCache) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = refresh(cache, &default_token_list_url()).await?;
    println!(
        "Fetched {} — {} tokens from {}",
        list_label(&outcome.meta),
        outcome.tokens,
        outcome.meta.url,
    );
    Ok(())
}

async fn list(
    cache: &TokenListCache,
    args: &TokensListArgs,
    out: Out,
) -> Result<(), Box<dyn std::error::Error>> {
    // First use fetches the list; from then on `list` shows the snapshot
    // as-is — only `refresh` (and staleness during symbol resolution)
    // replaces it.
    let meta = if let Some(meta) = cache.meta()? {
        meta
    } else {
        refresh_and_report(cache).await?;
        cache
            .meta()?
            .ok_or("token-list refresh did not persist its snapshot")?
    };

    let chain_id = match &args.chain {
        Some(handle) => Some(ChainResolver::load_or_fetch().await?.resolve(handle)?),
        None => args.chain_id,
    };
    let entries = cache.list(chain_id)?;
    if entries.is_empty() {
        eprintln!("no cached tokens match the given filters.");
        return Ok(());
    }

    let mut table = TableBuf::new(TableSpec::new(vec![
        Column::right("chain").min(10),
        Column::left("symbol").min(10),
        Column::left("name").min(24),
        Column::right("decimals").min(8),
        Column::left("address"),
    ]));
    table.title(format!(
        "{} · fetched {} from {}",
        list_label(&meta),
        DateTime::from_timestamp(meta.fetched_at.get() as i64, 0).map_or_else(
            || meta.fetched_at.get().to_string(),
            |dt| dt.format("%Y-%m-%d").to_string()
        ),
        meta.url,
    ));
    for e in &entries {
        table.push(vec![
            Cell::new(e.chain_id.to_string()),
            Cell::new(e.symbol.to_string()),
            Cell::new(e.name.as_str()),
            Cell::new(e.decimals.to_string()),
            Cell::new(e.address.to_string()),
        ]);
    }
    out.table(&table);
    println!("\n{} tokens.", entries.len());
    Ok(())
}

/// `Name vX.Y.Z` — how the snapshot introduces itself in titles and reports.
fn list_label(meta: &TokenListMeta) -> String {
    format!("{} v{}", meta.name, meta.version)
}
