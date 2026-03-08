# EVM ERC20 Module — Library-level EventSource with Filter Pushdown

**Origin:** Post-phase-4 testing revealed the ERC20 example fetches ALL token transfers even when only one account is needed. Filter pushdown to the RPC layer is essential.

## Problem

`AlloyErc20Source` in the example hardcodes fetching every Transfer event for a token. When tracking one account's balance history over 70 days (~500k blocks), this downloads millions of irrelevant events. The filter should be pushed down to `eth_getLogs` topics.

## Design

### New module: `src/evm/erc20.rs` (behind `evm` feature flag)

```rust
// Cargo.toml
[features]
default = []
evm = ["alloy", "tracing"]

[dependencies]
alloy = { version = "1", features = ["provider-http", "rpc-types", "sol-types"], optional = true }
```

### Core types (move from example into library)

```rust
/// ERC20 Transfer event.
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub amount: u128,
}

/// Balances state: address -> balance.
pub type Balances = HashMap<[u8; 20], u128>;

/// StateMachine that applies Transfer events to Balances.
pub struct Erc20Machine;

/// Codec for Transfer/Balances serialization.
pub struct Erc20Codec;

/// Filter for narrowing Transfer event queries at the RPC level.
pub enum Erc20Filter {
    /// All transfers for the token (no topic filter on from/to).
    AllTransfers,
    /// Only transfers involving this account (from OR to).
    /// Makes two eth_getLogs calls per chunk and merges results.
    Account([u8; 20]),
}

/// Ready-to-use EventSource for any ERC20 token.
pub struct Erc20Source { ... }
```

### Builder API

```rust
use chainfold::evm::erc20::{Erc20Source, Erc20Filter};

// All USDC transfers
let source = Erc20Source::new(rpc_url, usdc_address)
    .chunk_size(1000);

// Only transfers for a specific account
let source = Erc20Source::new(rpc_url, usdc_address)
    .filter(Erc20Filter::Account(account_addr))
    .chunk_size(1000);
```

### Filter implementation in `fetch_blocks`

When `Erc20Filter::Account(addr)` is set, each chunk makes **two** `eth_getLogs` calls:

1. `topics: [TRANSFER_SIG, addr_padded32, null]` — transfers FROM account
2. `topics: [TRANSFER_SIG, null, addr_padded32]` — transfers TO account

Results are merged and deduplicated by `(block_number, log_index)` to handle self-transfers.

When `Erc20Filter::AllTransfers`, single call per chunk (current behavior):
- `topics: [TRANSFER_SIG]`

### Why two queries, not one

`eth_getLogs` topics are AND between positions. `from` is topic[1], `to` is topic[2]. There's no way to express "from=X OR to=X" in a single query — you need two queries with different topic positions.

### What moves from example to library

| Current location | New location |
|---|---|
| `examples/erc20_tracker.rs` Transfer struct | `src/evm/erc20.rs` |
| `examples/erc20_tracker.rs` Erc20Machine | `src/evm/erc20.rs` |
| `examples/erc20_tracker.rs` Erc20Codec | `src/evm/erc20.rs` |
| `examples/erc20_tracker.rs` AlloyErc20Source | `src/evm/erc20.rs` as `Erc20Source` |
| `examples/erc20_tracker.rs` TRANSFER_TOPIC | `src/evm/erc20.rs` |
| `examples/erc20_tracker.rs` SourceErr | `src/evm/erc20.rs` |
| `examples/erc20_tracker.rs` log parsing | `src/evm/erc20.rs` |

### Example becomes thin CLI

```rust
// examples/erc20_tracker.rs — after refactor
use chainfold::evm::erc20::{Erc20Source, Erc20Filter, Erc20Machine, Erc20Codec, Balances};

#[tokio::main]
async fn main() {
    let source = Erc20Source::new(&args.rpc_url, token)
        .chunk_size(args.chunk_size)
        .filter(match &args.account {
            Some(addr) => Erc20Filter::Account(parse_addr(addr)),
            None => Erc20Filter::AllTransfers,
        });
    // ... Engine setup, recover, display results
}
```

### Additional fixes from testing session

These are already implemented in the example and should carry over:

1. **`--days` with auto block time detection** — `estimate_blocks_per_day()` samples two block timestamps to compute blocks/day for any chain
2. **`--last N`** — only show last N balance changes
3. **`--chunk-size`** — configurable max block range per `eth_getLogs` call
4. **`i128` balance tracking** — partial history shows negative deltas for outflows before inflows
5. **`from_block` floor in source** — clamps fetch_blocks to configured start, prevents fetching from genesis
6. **tracing for RPC logging** — `tracing::debug!` on each eth_getLogs with block range and event count

## Dependencies

- alloy 1.x (behind `evm` feature flag) — currently dev-dependency, becomes optional dependency
- tracing (behind `evm` feature flag) — for RPC request logging

## Not in scope

- Generic filter trait on `EventSource` — filters are source-specific (EVM topics vs CEX API params)
- Non-ERC20 EVM events — future `evm::custom` module could support arbitrary event signatures
- CEX adapter — different model entirely, no blocks/reorgs
