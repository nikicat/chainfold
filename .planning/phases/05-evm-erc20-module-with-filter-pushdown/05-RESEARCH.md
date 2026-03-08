# Phase 5: EVM ERC20 Module with Filter Pushdown - Research

**Researched:** 2026-03-09
**Domain:** Rust module extraction, alloy EVM RPC, Cargo feature flags
**Confidence:** HIGH

## Summary

This phase extracts existing ERC20 PoC code (duplicated across `tests/erc20_tests.rs` and `examples/erc20_tracker.rs`) into a reusable library module at `src/modules/erc20.rs`, gated behind a `evm` Cargo feature. The domain types (Transfer, Erc20Machine, Erc20Codec) are straightforward moves. The substantive new work is Erc20Source — an `EventSource` impl wrapping alloy's Provider that supports filter pushdown via an `Erc20Filter` enum, enabling account-scoped `eth_getLogs` queries through topic filtering.

The existing example code (`AlloyErc20Source`) is a solid reference implementation with chunked `eth_getLogs`, block hash fetching, and Transfer log parsing. The main additions are: (1) the filter pushdown mechanism using alloy's `Filter::topic1()`/`topic2()` for indexed address parameters, (2) proper error types, and (3) re-exporting alloy provider types so downstream users don't need alloy as a direct dependency.

**Primary recommendation:** Extract domain types first, then Erc20Source with filter pushdown, then update tests and example to import from the module. Use `Box<dyn Provider>` (trait object) for the provider parameter to keep the public API simple.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Full module including Erc20Source (EventSource impl) -- not just domain types
- Erc20Source accepts an alloy Provider directly (user constructs provider, passes it in)
- alloy provider types re-exported from the module so users don't need alloy as a direct dep
- Module lives at `src/modules/erc20.rs` (generic modules namespace, leaves room for future non-EVM modules)
- Example binary thinned to CLI-only: imports Transfer, Erc20Machine, Erc20Codec, Erc20Source from chainfold::modules::erc20
- Erc20Filter enum: `AllTransfers` | `Account([u8; 20])`
- Filter set at construction time, immutable -- different filter = new source
- Account filter fires two eth_getLogs queries per chunk (from=addr, to=addr), merges and deduplicates internally by (block_number, log_index). Self-transfers handled via dedup.
- Filter lives in Source only -- Erc20Machine stays a pure reducer that applies all events it receives
- One source per token, one account per filter. Multiple tokens or accounts = multiple sources.
- Single `evm` feature: `features = { evm = ["alloy", "tracing"] }`
- alloy and tracing become optional dependencies (currently dev-dependencies)
- alloy provider types re-exported from modules::erc20 for convenience
- Existing tests in tests/erc20_tests.rs migrated to import from chainfold::modules::erc20 (MockErc20Source stays in tests)
- State type is always `HashMap<[u8; 20], u128>` regardless of filter -- Account filter just means fewer entries
- Filter enum is sufficient API -- no typed constructors or separate builder types

### Claude's Discretion
- Internal chunking strategy for eth_getLogs (reuse existing chunk_size pattern from example)
- How to structure the alloy provider generic parameter (concrete type vs trait object)
- Error type design for Erc20Source
- Whether Erc20Codec uses serde_json (like PoC) or postcard for production
- Block hash fetching strategy (batch vs per-block)
- How to handle the subscribe() method for live streaming with filters

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| alloy | 1.7.x | EVM RPC client (eth_getLogs, getBlock) | Already used in example; standard Rust EVM toolkit |
| tracing | 0.1 | Structured logging for RPC calls | Already in dev-deps; standard Rust observability |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| serde_json | 1 | Codec serialization for Transfer/Balances | Already a dependency; keep for PoC-grade codec |
| hex | 0.4 | Address formatting in errors/debug | Already a dev-dep; thin convenience |

### Feature Flag Configuration

```toml
[features]
evm = ["dep:alloy", "dep:tracing"]

[dependencies]
alloy = { version = "1", features = ["provider-http", "rpc-types", "sol-types"], optional = true }
tracing = { version = "0.1", optional = true }
```

**Key detail:** alloy and tracing move from `[dev-dependencies]` to `[dependencies]` with `optional = true`. They remain in `[dev-dependencies]` too (for tests/examples that don't enable the feature).

## Architecture Patterns

### Module Structure
```
src/
  modules/
    mod.rs           # #[cfg(feature = "evm")] pub mod erc20;
    erc20.rs         # Transfer, Erc20Machine, Erc20Codec, Erc20Source, Erc20Filter
  lib.rs             # + #[cfg(feature = "evm")] pub mod modules;
```

### Pattern 1: Feature-Gated Module
**What:** Entire `modules` directory gated behind `evm` feature
**When to use:** When the module pulls in heavy optional dependencies (alloy is ~50 crates)

```rust
// src/lib.rs
#[cfg(feature = "evm")]
pub mod modules;
```

```rust
// src/modules/mod.rs
#[cfg(feature = "evm")]
pub mod erc20;
```

Note: The double `#[cfg(feature = "evm")]` is intentional redundancy -- `modules/mod.rs` gate protects against accidental imports if someone adds a non-evm module later.

### Pattern 2: Provider as Generic with Default
**What:** Erc20Source is generic over Provider but provides a concrete convenience constructor for HTTP.

The alloy `Provider` trait in v1.x has signature: `pub trait Provider<N = Ethereum>: Send + Sync where N: Network`. The simplest approach for the module API:

```rust
use alloy::providers::Provider;

pub struct Erc20Source<P: Provider> {
    provider: P,
    token: [u8; 20],
    filter: Erc20Filter,
    chunk_size: u64,
    from_block: u64,
}
```

**Recommendation:** Use a generic `P: Provider` parameter rather than `Box<dyn Provider>`. Rationale:
- alloy's Provider trait is object-safe but boxing adds indirection
- Generic preserves zero-cost abstraction
- Users construct the provider once and pass it in -- no runtime polymorphism needed
- The existing example already uses concrete `ProviderBuilder::new().connect_http(url)` which returns a specific type

### Pattern 3: Filter Pushdown via Topic Parameters
**What:** Account filter translates to eth_getLogs topic1/topic2 constraints
**How it works:**

ERC20 Transfer event topic layout:
- `topics[0]` = event signature hash (TRANSFER_TOPIC)
- `topics[1]` = `from` address (left-padded to 32 bytes)
- `topics[2]` = `to` address (left-padded to 32 bytes)

For `Erc20Filter::Account(addr)`:
- Query 1: `topic1 = addr` (transfers FROM this account)
- Query 2: `topic2 = addr` (transfers TO this account)
- Merge results, dedup by `(block_number, log_index)`

This is correct because eth_getLogs uses AND between topic positions -- you cannot OR topic1 and topic2 in a single query.

```rust
// alloy Filter API for topic-based filtering
use alloy::rpc::types::Filter;
use alloy::primitives::FixedBytes;

fn address_to_topic(addr: &[u8; 20]) -> FixedBytes<32> {
    let mut topic = [0u8; 32];
    topic[12..32].copy_from_slice(addr);
    FixedBytes::new(topic)
}

// Query 1: transfers FROM account
let filter_from = Filter::new()
    .address(token_address)
    .event_signature(TRANSFER_TOPIC)
    .topic1(address_to_topic(&account))
    .from_block(start)
    .to_block(end);

// Query 2: transfers TO account
let filter_to = Filter::new()
    .address(token_address)
    .event_signature(TRANSFER_TOPIC)
    .topic2(address_to_topic(&account))
    .from_block(start)
    .to_block(end);
```

### Pattern 4: Deduplication by (block_number, log_index)
**What:** After merging two query results, remove duplicates (self-transfers appear in both)
**Implementation:**

```rust
use std::collections::BTreeMap;

// Key: (block_number, log_index) -- naturally ordered
let mut deduped: BTreeMap<(u64, u32), Log> = BTreeMap::new();
for log in from_logs.into_iter().chain(to_logs.into_iter()) {
    let key = (log.block_number.unwrap_or(0), log.log_index.unwrap_or(0) as u32);
    deduped.entry(key).or_insert(log);
}
// BTreeMap iteration is sorted -- events arrive in correct order
```

### Anti-Patterns to Avoid
- **Putting filter logic in Erc20Machine:** The machine is a pure reducer. Filter belongs in Source only.
- **Building a provider inside Erc20Source:** User constructs and owns the provider. Source borrows or takes ownership.
- **Single query with OR topics:** eth_getLogs ANDs topic positions; you need two separate queries for account filter.
- **Using HashSet for dedup:** Loses ordering. Use BTreeMap keyed on (block_number, log_index).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Topic hash computation | Manual keccak256 | Pre-computed TRANSFER_TOPIC constant | Already exists in example; never changes |
| Address topic encoding | Custom padding logic | `address_to_topic()` helper (12-byte left-pad) | EVM ABI standard, easy to get wrong |
| Log deduplication | Custom Vec scanning | BTreeMap<(block_number, log_index), Log> | O(n log n) vs O(n^2), preserves order |
| Provider construction | Inside the module | User passes provider in | Module doesn't know auth, endpoints, retry config |

## Common Pitfalls

### Pitfall 1: Feature Flag Dependency Leakage
**What goes wrong:** Types from optional dependencies appear in public API without `#[cfg(feature = "evm")]`
**Why it happens:** Easy to forget cfg gates on re-exports
**How to avoid:** Every `pub use` of alloy types must be inside `#[cfg(feature = "evm")]` blocks. Test with `cargo check` (no features) and `cargo check --features evm`.
**Warning signs:** Build failures when compiling without `--features evm`

### Pitfall 2: alloy dev-dependency vs dependency conflict
**What goes wrong:** Having alloy in both `[dependencies]` (optional) and `[dev-dependencies]` can cause version conflicts or unexpected feature unification
**Why it happens:** Cargo unifies features across dependency types within a build
**How to avoid:** Keep alloy in `[dependencies]` with `optional = true`. In `[dev-dependencies]`, if needed for tests/examples that always want it, just enable the `evm` feature on the crate itself: `chainfold = { path = ".", features = ["evm"] }` -- or simpler, add to `[dev-dependencies]` separately (Cargo handles this fine as long as versions match).
**Warning signs:** Feature unification warnings, unexpected deps being pulled in

### Pitfall 3: Self-Transfer Double Counting
**What goes wrong:** Account filter returns a self-transfer (from=to=account) in both queries, gets applied twice
**Why it happens:** Same log matches topic1=addr AND topic2=addr
**How to avoid:** Dedup by (block_number, log_index) BEFORE converting to Transfer events. BTreeMap insert ignores duplicates.
**Warning signs:** Account balance inflated after self-transfers

### Pitfall 4: Block Hash Fetching N+1 Problem
**What goes wrong:** Fetching block hash individually for each block with events results in many RPC calls
**Why it happens:** The existing example does per-block `get_block_by_number` for each block with events
**How to avoid:** Accept this for v1 -- block hash fetching is inherently per-block in standard JSON-RPC. Some providers support batch RPC, but alloy's batch API varies. The chunk_size already limits the number of distinct blocks per chunk.
**Warning signs:** Slow performance on ranges with many active blocks (e.g., USDC on mainnet)

### Pitfall 5: Empty Stream for subscribe()
**What goes wrong:** subscribe() needs to return a stream but live subscription with filters is non-trivial
**Why it happens:** WebSocket `eth_subscribe("logs", filter)` supports topic filters, but implementation is complex
**How to avoid:** For this phase, subscribe() returns an empty stream (matching PoC behavior). Live streaming with filters is additive future work.
**Warning signs:** N/A -- documented limitation

### Pitfall 6: Alloy Reqwest URL Type in Public API
**What goes wrong:** Exposing `alloy::transports::http::reqwest::Url` as a constructor parameter leaks internal alloy deps
**Why it happens:** The example passes a URL string and parses it internally
**How to avoid:** Erc20Source constructor takes a `P: Provider` generic, not a URL string. Provider construction is the caller's responsibility.

## Code Examples

### Erc20Filter Enum
```rust
/// Filter for ERC20 Transfer event queries.
/// Set at construction time; immutable. Different filter = new source.
#[derive(Debug, Clone)]
pub enum Erc20Filter {
    /// Fetch all Transfer events for the token contract.
    AllTransfers,
    /// Fetch only transfers involving this account (as sender or receiver).
    Account([u8; 20]),
}
```

### Erc20Source Construction
```rust
pub struct Erc20Source<P: Provider> {
    provider: P,
    token: [u8; 20],
    filter: Erc20Filter,
    chunk_size: u64,
    from_block: u64,
}

impl<P: Provider> Erc20Source<P> {
    pub fn new(
        provider: P,
        token: [u8; 20],
        filter: Erc20Filter,
        chunk_size: u64,
        from_block: u64,
    ) -> Self {
        Self { provider, token, filter, chunk_size, from_block }
    }
}
```

### Re-exports for Convenience
```rust
// src/modules/erc20.rs -- re-export alloy types users need
pub use alloy::providers::{Provider, ProviderBuilder};
pub use alloy::primitives::Address;
```

### Transfer Type (moved from test/example)
```rust
/// ERC20 Transfer event.
#[derive(Debug, Clone)]
pub struct Transfer {
    pub from: [u8; 20],
    pub to: [u8; 20],
    pub amount: u128,
}

/// Balance state: address -> balance.
pub type Balances = HashMap<[u8; 20], u128>;

/// TRANSFER_TOPIC: keccak256("Transfer(address,address,uint256)")
pub const TRANSFER_TOPIC: FixedBytes<32> = FixedBytes::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37,
    0x8d, 0xaa, 0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d,
    0xf5, 0x23, 0xb3, 0xef,
]);
```

### Account Filter -- Dual Query Pattern
```rust
async fn fetch_logs_filtered(
    &self,
    from: u64,
    to: u64,
) -> Result<Vec<Log>, Erc20SourceError> {
    match &self.filter {
        Erc20Filter::AllTransfers => {
            let filter = Filter::new()
                .address(Address::from(self.token))
                .event_signature(TRANSFER_TOPIC)
                .from_block(from)
                .to_block(to);
            Ok(self.provider.get_logs(&filter).await?)
        }
        Erc20Filter::Account(addr) => {
            let topic = address_to_topic(addr);
            // Query 1: transfers FROM account
            let filter_from = Filter::new()
                .address(Address::from(self.token))
                .event_signature(TRANSFER_TOPIC)
                .topic1(topic)
                .from_block(from)
                .to_block(to);
            // Query 2: transfers TO account
            let filter_to = Filter::new()
                .address(Address::from(self.token))
                .event_signature(TRANSFER_TOPIC)
                .topic2(topic)
                .from_block(from)
                .to_block(to);

            let (from_logs, to_logs) = tokio::join!(
                self.provider.get_logs(&filter_from),
                self.provider.get_logs(&filter_to),
            );

            // Merge and dedup by (block_number, log_index)
            let mut deduped: BTreeMap<(u64, u32), Log> = BTreeMap::new();
            for log in from_logs?.into_iter().chain(to_logs?.into_iter()) {
                let key = (log.block_number.unwrap_or(0), log.log_index.unwrap_or(0) as u32);
                deduped.entry(key).or_insert(log);
            }
            Ok(deduped.into_values().collect())
        }
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| alloy 0.x (ethers-rs successor) | alloy 1.x (stable) | May 2025 | Simplified Provider trait (no Transport generic in public API) |
| `ethers::providers::Provider` | `alloy::providers::Provider<N = Ethereum>` | alloy 1.0 | Network-generic, simpler bounds |

**Note:** alloy 1.x requires rustc 1.91+ (stable toolchain). Project already handles this per decision **04-03**.

## Discretionary Recommendations

These are areas marked as Claude's Discretion. Research-based recommendations:

### 1. Chunking Strategy
**Recommendation:** Reuse the existing pattern from the example -- configurable `chunk_size` parameter, default 1000. Loop with cursor advancement. This is proven code.

### 2. Provider Generic Parameter
**Recommendation:** Use generic `P: Provider` (not trait object). Reasoning:
- Zero-cost, no boxing overhead
- User always has a concrete provider type
- alloy Provider is Send + Sync already
- Trait objects would require `Box<dyn Provider>` which has lifetime/async complications

### 3. Error Type
**Recommendation:** A simple `Erc20SourceError` enum wrapping alloy transport errors:
```rust
#[derive(Debug, thiserror::Error)]
pub enum Erc20SourceError {
    #[error("RPC error: {0}")]
    Rpc(#[from] alloy::transports::TransportError),
    #[error("parse error: {0}")]
    Parse(String),
}
```

### 4. Codec Format
**Recommendation:** Keep serde_json for now. Rationale:
- Consistency with PoC (no gratuitous change)
- Human-readable for debugging
- Performance is not the bottleneck (RPC latency dominates)
- Users can implement their own Codec with postcard if they need speed

### 5. Block Hash Fetching
**Recommendation:** Keep per-block `get_block_by_number` (existing pattern). Batch RPC is provider-specific and alloy's batch API would add complexity. The number of blocks with events in a chunk is typically small.

### 6. subscribe() with Filters
**Recommendation:** Return empty stream for now (matching PoC). Live streaming with topic-filtered WebSocket subscriptions is non-trivial and additive. Document as known limitation.

## Open Questions

1. **alloy feature unification in Cargo.toml**
   - What we know: alloy is currently in `[dev-dependencies]` with specific features. Moving to `[dependencies]` with `optional = true` requires the same features.
   - What's unclear: Whether having it in both sections causes issues. Cargo should handle this, but worth testing.
   - Recommendation: Move to `[dependencies]` only; remove from `[dev-dependencies]`. Tests and examples that need it should use `#[cfg(feature = "evm")]` or the dev-dependency section can just list `chainfold = { path = ".", features = ["evm"] }`.

2. **Example binary Cargo features**
   - What we know: Examples in Cargo can't declare their own features. The example needs alloy.
   - What's unclear: How to make `cargo run --example erc20_tracker` automatically enable the `evm` feature.
   - Recommendation: Use `required-features = ["evm"]` in the `[[example]]` section. Users run with `cargo run --features evm --example erc20_tracker`.

## Sources

### Primary (HIGH confidence)
- Existing codebase: `tests/erc20_tests.rs`, `examples/erc20_tracker.rs` -- reference implementations
- Existing codebase: `src/source.rs` (EventSource trait), `src/machine.rs` (StateMachine trait)
- [alloy Provider trait docs](https://docs.rs/alloy/latest/alloy/providers/trait.Provider.html) -- confirmed `Provider<N = Ethereum>: Send + Sync` signature
- alloy 1.7.3 in `Cargo.lock` -- verified version

### Secondary (MEDIUM confidence)
- [alloy v1.0 announcement](https://www.paradigm.xyz/2025/05/introducing-alloy-v1-0) -- Transport generic removal from public API
- [alloy PR #1859](https://github.com/alloy-rs/alloy/pull/1859) -- removal of T: Transport from public APIs

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- alloy 1.7.3 already in use, all deps known
- Architecture: HIGH -- extracting existing code with well-defined additions
- Pitfalls: HIGH -- most pitfalls identified from reading existing code and EVM RPC behavior
- Filter pushdown: HIGH -- eth_getLogs topic filtering is well-documented EVM standard

**Research date:** 2026-03-09
**Valid until:** 2026-04-09 (stable -- alloy 1.x is mature, project patterns established)
