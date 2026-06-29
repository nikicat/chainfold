use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Iteration direction — funneled from the global `--reverse` flag, so the two
/// run-fn plumbing args (`Direction`, [`FollowMode`]) can't be silently swapped
/// the way two adjacent `bool`s could.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Oldest → newest (default).
    Forward,
    /// Newest → oldest (`--reverse`).
    Backward,
}

impl Direction {
    /// From the `--reverse` flag: `true` → [`Self::Backward`].
    pub fn from_reverse(reverse: bool) -> Self {
        if reverse {
            Self::Backward
        } else {
            Self::Forward
        }
    }
}

/// Whether to block at the tip for live events — funneled from the global
/// `--follow` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowMode {
    /// Walk the requested range once and stop (default).
    Once,
    /// After the range, block at the tip and stream new events (`--follow`).
    Follow,
}

impl FollowMode {
    /// From the `--follow` flag: `true` → [`Self::Follow`].
    pub fn from_follow(follow: bool) -> Self {
        if follow { Self::Follow } else { Self::Once }
    }

    pub fn is_follow(self) -> bool {
        matches!(self, Self::Follow)
    }
}

#[derive(Parser)]
#[command(name = "chainfold", about = "Blockchain history explorer")]
pub struct Cli {
    /// Path to config file
    #[arg(long, global = true)]
    pub config: Option<String>,

    /// Navigate backwards instead of forwards
    #[arg(long, global = true)]
    pub reverse: bool,

    /// Follow (block at tip for new events)
    #[arg(long, global = true)]
    pub follow: bool,

    /// Print a session summary at exit: RPC calls by method (+ wall-time),
    /// `SQLite` read time, and cache hit/miss counts. Also enabled by setting
    /// `CHAINFOLD_STATS=1`.
    #[arg(long, global = true)]
    pub stats: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Config management
    Config {
        #[command(subcommand)]
        action: ConfigCommand,
    },
    /// Chain registry queries (cached chainlist.org data)
    Chains {
        #[command(subcommand)]
        action: ChainsCommand,
    },
    /// EVM chain commands
    Evm(EvmArgs),
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Print resolved config (API key masked)
    Show,
    /// Validate config and test RPC connectivity
    Check,
}

#[derive(Subcommand)]
pub enum ChainsCommand {
    /// List resolvable `--chain` names from the cached chainlist.org data
    /// (chainId, shortName, chainSlug, name).
    List(ChainsListArgs),
}

#[derive(Parser)]
pub struct ChainsListArgs {
    /// Show only chains flagged as testnets.
    #[arg(long)]
    pub testnet: bool,

    /// Show only chains Alchemy hosts (those with an Alchemy URL slug).
    #[arg(long)]
    pub alchemy_supported: bool,
}

#[derive(Parser)]
pub struct EvmArgs {
    /// Chain name. Matched against chainlist.org's shortName or chainSlug.
    /// Example: --chain ethereum, --chain arbitrum, --chain bnb
    #[arg(
        long,
        required_unless_present = "chain_id",
        conflicts_with = "chain_id"
    )]
    pub chain: Option<String>,

    /// Numeric chain ID. Use for chains not listed in chainlist.org.
    /// Example: --chain-id 56
    #[arg(long, required_unless_present = "chain", conflicts_with = "chain")]
    pub chain_id: Option<u64>,

    /// RPC endpoint URL (overrides config)
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// `SQLite` cache path. Defaults to `$XDG_CACHE_HOME/chainfold/chainfold.db`
    /// (typically `~/.cache/chainfold/chainfold.db`).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Block chunk size for `eth_getLogs`
    #[arg(long, default_value = "1000")]
    pub chunk_size: u64,

    /// Start date, UTC (e.g. 2025-01-15 or 2025-01-15T12:00:00)
    #[arg(long, value_name = "DATE")]
    pub from: Option<String>,

    /// End date, UTC (e.g. 2025-01-16 or 2025-01-16T12:00:00)
    #[arg(long, value_name = "DATE")]
    pub to: Option<String>,

    /// Starting block number
    #[arg(long)]
    pub from_block: Option<u64>,

    /// Number of blocks to scan (used with --from-block)
    #[arg(long, default_value = "100")]
    pub num_blocks: u64,

    /// Max events to print
    #[arg(long, default_value = "20")]
    pub limit: usize,

    #[command(subcommand)]
    pub command: EvmCommand,
}

#[derive(Subcommand)]
pub enum EvmCommand {
    /// ERC20 token commands
    Erc20(Erc20Args),
    /// Show all logs for one event of one contract
    Events(EventsArgs),
}

#[derive(Parser)]
pub struct EventsArgs {
    /// Contract address (0x-prefixed hex).
    #[arg(long, required = true)]
    pub address: String,

    /// Event name — looked up against Sourcify / Etherscan to derive
    /// the canonical signature and topic0. Cached in `SQLite`.
    /// Example: --event Transfer
    #[arg(long, conflicts_with_all = ["event_sig", "topic0"])]
    pub event: Option<String>,

    /// Canonical event signature. No network calls; topic0 is computed
    /// locally via keccak256.
    /// Example: --event-sig 'Transfer(address,address,uint256)'
    #[arg(long, conflicts_with_all = ["event", "topic0"])]
    pub event_sig: Option<String>,

    /// Raw topic0 hex (32 bytes). Bypasses any lookup.
    /// Example: --topic0 0xddf252ad...
    #[arg(long, conflicts_with_all = ["event", "event_sig"])]
    pub topic0: Option<String>,

    /// Print full topic and data hex instead of the truncated preview.
    #[arg(long)]
    pub full: bool,

    /// Decode topics and data using the event ABI (requires `--event`,
    /// which fetches and caches the matching ABI item from Sourcify or
    /// Etherscan).
    #[arg(long)]
    pub decode: bool,
}

#[derive(Parser)]
pub struct Erc20Args {
    /// Token contract address(es) (0x-prefixed hex). Repeat for multiple tokens.
    #[arg(long, required = true)]
    pub address: Vec<String>,

    /// Owner address to track (0x-prefixed hex)
    #[arg(long)]
    pub owner: String,

    #[command(subcommand)]
    pub command: Erc20Command,
}

#[derive(Subcommand)]
pub enum Erc20Command {
    /// Show ERC20 transfers (no balance tracking)
    Transfers,
    /// Show ERC20 transfers with running balance column
    Balances,
}
