use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

/// Iteration direction — funneled from the `evm` `--reverse` flag, so the two
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

/// Whether to block at the tip for live events — funneled from the `evm`
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

/// Diagnostic-log verbosity — funneled from the repeatable `-v` flag's raw
/// count, so consumers match a closed ladder instead of comparing a bare
/// `u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verbosity {
    /// No `-v`: warnings everywhere (or whatever `RUST_LOG` says when set).
    Baseline,
    /// `-v`: chainfold's own logs, at debug.
    OwnDebug,
    /// `-vv`: debug for every crate.
    AllDebug,
    /// `-vvv` (or more): trace for every crate.
    AllTrace,
}

impl Verbosity {
    /// From the `-v` occurrence count; saturates at [`Self::AllTrace`].
    pub fn from_count(count: u8) -> Self {
        match count {
            0 => Self::Baseline,
            1 => Self::OwnDebug,
            2 => Self::AllDebug,
            _ => Self::AllTrace,
        }
    }
}

#[derive(Parser)]
#[command(name = "chainfold", about = "Blockchain history explorer")]
pub struct Cli {
    /// Path to config file
    #[arg(long, global = true)]
    pub config: Option<String>,

    /// Print a session summary at exit: RPC calls by method (+ wall-time),
    /// `SQLite` read time, and cache hit/miss counts. Also enabled by setting
    /// `CHAINFOLD_STATS=1`.
    #[arg(long, global = true)]
    pub stats: bool,

    /// Log diagnostics to stderr: `-v` shows chainfold's own logs (debug),
    /// `-vv` debug for all crates, `-vvv` trace. Warnings are always on;
    /// layered over `RUST_LOG`.
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

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
    /// Bitcoin chain commands (bitcoind JSON-RPC)
    Bitcoin(BitcoinArgs),
    /// Local `SQLite` cache management
    Cache(CacheArgs),
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Validate and print the effective config as TOML (defaults filled in,
    /// API keys masked) — what an actual run would use.
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
    /// Include chains flagged as testnets (excluded by default).
    #[arg(long)]
    pub testnets: bool,

    /// Show only chains Alchemy hosts (those with an Alchemy URL slug).
    #[arg(long)]
    pub alchemy_supported: bool,
}

#[derive(Parser)]
pub struct CacheArgs {
    /// `SQLite` cache path. Defaults to `$XDG_CACHE_HOME/chainfold/chainfold.db`
    /// (typically `~/.cache/chainfold/chainfold.db`). `global` scopes the
    /// flag to the `cache` subtree, so it may follow the subcommand.
    #[arg(long, global = true)]
    pub db: Option<PathBuf>,

    #[command(subcommand)]
    pub command: CacheCommand,
}

#[derive(Subcommand)]
pub enum CacheCommand {
    /// Show per-table entry counts and the on-disk size of the cache DB
    Size,
    /// Delete cached data. With no flags, wipes everything; category flags
    /// narrow the purge and are combinable.
    Purge(CachePurgeArgs),
}

#[derive(Parser)]
pub struct CachePurgeArgs {
    /// Purge log/block scan history (cached logs and blocks, scanned ranges,
    /// reorg tombstones, block headers)
    #[arg(long)]
    pub history: bool,

    /// Purge ERC20 token metadata and balances
    #[arg(long)]
    pub erc20: bool,

    /// Purge resolved event signatures
    #[arg(long)]
    pub signatures: bool,
}

#[derive(Parser)]
pub struct EvmArgs {
    /// Chain name. Matched against chainlist.org's shortName or chainSlug.
    /// Example: --chain ethereum, --chain arbitrum, --chain bnb. When neither
    /// --chain nor --chain-id is given, falls back to `$CHAINFOLD_CHAIN`, then
    /// the config `[evm] default_chain`, then `ethereum`.
    #[arg(long, conflicts_with = "chain_id")]
    pub chain: Option<String>,

    /// Numeric chain ID. Use for chains not listed in chainlist.org.
    /// Example: --chain-id 56
    #[arg(long, conflicts_with = "chain")]
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

    #[command(flatten)]
    pub window: WindowArgs,

    /// Navigate backwards instead of forwards. `global` here scopes the flag
    /// to the `evm` subtree — direction means nothing to `chains`/`config`.
    #[arg(long, global = true)]
    pub reverse: bool,

    /// Follow (block at tip for new events). Scoped to the `evm` subtree,
    /// like `--reverse`.
    #[arg(long, global = true)]
    pub follow: bool,

    #[command(subcommand)]
    pub command: EvmCommand,
}

#[derive(Subcommand)]
pub enum EvmCommand {
    /// Block history commands
    Blocks(BlocksArgs),
    /// ERC20 token commands
    Erc20(Erc20Args),
    /// Show all logs for one event of one contract
    Events(EventsArgs),
}

#[derive(Parser)]
pub struct BlocksArgs {
    #[command(subcommand)]
    pub command: BlocksCommand,
}

#[derive(Subcommand)]
pub enum BlocksCommand {
    /// List blocks in a window as a table, or --follow the live tip.
    /// Window flags (--from/--to/--from-block/--num-blocks) come from `evm`;
    /// the default window is the latest --num-blocks ending at the chain
    /// head.
    List(BlocksListArgs),
}

#[derive(Parser)]
pub struct BlocksListArgs {
    /// Add the MINER and PARENT columns and print full hashes.
    #[arg(long)]
    pub full: bool,
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

/// The chain-agnostic window/limit flags, shared by every chain family's
/// subtree (flattened into [`EvmArgs`] and [`BitcoinArgs`]).
#[derive(Parser)]
pub struct WindowArgs {
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

    /// Max rows to print
    #[arg(long, default_value = "20")]
    pub limit: usize,
}

#[derive(Parser)]
pub struct BitcoinArgs {
    /// bitcoind JSON-RPC endpoint URL. Overrides the config `[bitcoin]`
    /// `rpc_url` (default `http://127.0.0.1:8332`).
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// Core cookie file for RPC auth (`<datadir>/.cookie`). When neither
    /// cookie nor user/pass is given, falls back to the config `[bitcoin]`
    /// auth, then — for loopback endpoints only — `~/.bitcoin/.cookie` if
    /// it exists.
    #[arg(long, conflicts_with_all = ["rpc_user", "rpc_pass"])]
    pub rpc_cookie_file: Option<PathBuf>,

    /// RPC username (`rpcuser`); requires --rpc-pass.
    #[arg(long, requires = "rpc_pass")]
    pub rpc_user: Option<String>,

    /// RPC password (`rpcpassword`); requires --rpc-user.
    #[arg(long, requires = "rpc_user")]
    pub rpc_pass: Option<String>,

    /// `SQLite` cache path. Defaults to `$XDG_CACHE_HOME/chainfold/chainfold.db`
    /// (typically `~/.cache/chainfold/chainfold.db`).
    #[arg(long)]
    pub db: Option<PathBuf>,

    /// Block chunk size for gap-fill alignment
    #[arg(long, default_value = "1000")]
    pub chunk_size: u64,

    #[command(flatten)]
    pub window: WindowArgs,

    /// Navigate backwards instead of forwards. Scoped to the `bitcoin`
    /// subtree, like `evm --reverse`.
    #[arg(long, global = true)]
    pub reverse: bool,

    /// Follow (block at tip for new blocks). Scoped to the `bitcoin`
    /// subtree.
    #[arg(long, global = true)]
    pub follow: bool,

    #[command(subcommand)]
    pub command: BitcoinCommand,
}

#[derive(Subcommand)]
pub enum BitcoinCommand {
    /// Block history commands
    Blocks(BtcBlocksArgs),
}

#[derive(Parser)]
pub struct BtcBlocksArgs {
    #[command(subcommand)]
    pub command: BtcBlocksCommand,
}

#[derive(Subcommand)]
pub enum BtcBlocksCommand {
    /// List blocks in a window as a table, or --follow the live tip.
    /// Window flags (--from/--to/--from-block/--num-blocks) come from
    /// `bitcoin`; the default window is the latest --num-blocks ending at
    /// the chain head.
    List(BtcBlocksListArgs),
}

#[derive(Parser)]
pub struct BtcBlocksListArgs {
    /// Add the PARENT column and print full hashes.
    #[arg(long)]
    pub full: bool,
}
