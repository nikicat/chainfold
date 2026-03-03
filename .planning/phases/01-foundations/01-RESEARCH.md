# Phase 1: Foundations - Research

**Researched:** 2026-03-03
**Domain:** Rust trait design — ChainCursor ordering, EventSource abstraction, StateMachine reducer, in-memory state
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Trait granularity:**
- One `EventSource` trait combining `subscribe()`, `fetch_events(range)`, and `fetch_state(at)`. One impl per chain, discoverable API.
- `Apply` is a separate trait from EventSource — pure reducer logic is cleanly separated from I/O operations.
- FetchState and FetchEvents live on EventSource — all chain interaction through one trait.
- Same Apply trait for both reduced and accumulated state. State shape is the caller's concern, not the trait's.

**Error model:**
- Apply is infallible — always returns a new state. If an event is irrelevant, state doesn't change. No error path on the hot path.
- EventSource methods return errors via a generic wrapper around `Box<dyn Error>`. Adapters don't need to conform to a library-defined error enum.
- No event validation — events are opaque structs defined by the caller. Library never inspects event contents.
- Snapshot serialization is client-owned. Library stores and loads opaque bytes (`Vec<u8>`). Client implements their own marshal/unmarshal.

**Async boundary:**
- EventSource trait methods are async (`async fn fetch_events`, `async fn fetch_state`, `async fn subscribe`). Natural Rust async, requires tokio.
- OnChange callback is sync: `Fn(&S)`. Runs inline on the event processing path. If caller needs async work, they spawn it themselves.
- Library requires tokio directly — not runtime-agnostic. Use `tokio::spawn`, tokio channels, tokio timers. Pragmatic choice for the blockchain ecosystem.
- Engine is caller-driven: `Engine::run()` is an async fn the caller `.awaits` or spawns. Caller controls task lifecycle and shutdown.

**State access:**
- Reduced state accessed via `tokio::sync::watch` channel. Caller gets a `Receiver` that always has the latest value. Lock-free reads, automatic change notification.
- Accumulated state accessed via lazy cursor iterator over a range. Events come from memory or SQLite. No full-result buffering.
- State type bounds are minimal: `Clone + Send`. `Serialize/Deserialize` bounds only on snapshot-related methods, not the core trait.
- Library wraps user events in an envelope: `Event<E> { cursor: C, data: E, block_hash: Option<Hash> }`. Library owns metadata (cursor, block_hash), user owns payload (E).

### Claude's Discretion

- Exact cursor type definitions (EvmCursor fields, BtcCursor fields, CexCursor fields)
- Internal Engine architecture (channels, task structure)
- Test harness design for mock EventSource implementations
- Whether to use associated types or generic parameters on traits

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CORE-01 | Library exposes a generic `EventSource` trait abstracting over EVM, Bitcoin, and CEX ordering schemes (blockNum+logIndex, blockHeight+txIndex, timestamp+seqId) | Trait design patterns, `async fn in trait` with `trait_variant` for Send bounds, alloy 1.x Provider API as reference |
| CORE-02 | Library exposes a `StateMachine` trait where client provides FetchState(at cursor), FetchEvents(from, to cursor), and Apply(state, event) as a pure reducer | These live on `EventSource` (locked decision); Apply is infallible and sync; `tokio::sync::watch` for state access |
| CORE-03 | Cursor types implement `Ord + Clone + Serialize + Deserialize` enabling generic ordering across all source types | Derive macros for `Ord`, `serde` for serialization; `ChainCursor` supertrait bounds; three concrete cursor structs |
| CORE-04 | Library supports reduced state — events fold into a compact in-memory value (e.g., balance, pool reserves) | `tokio::sync::watch::Sender<S>` holds latest state; `Apply` fn drives updates; no persistence in Phase 1 |
| CORE-05 | Library supports accumulated state — events stored on disk, accessed via cursors and anchors | In Phase 1: in-memory `Vec<Event<E>>` accumulator; disk storage is Phase 2; cursor iteration interface defined here |
</phase_requirements>

---

## Summary

Phase 1 is pure Rust trait design with no I/O. The deliverable is a compilable type system: `ChainCursor` trait, three concrete cursor structs (`EvmCursor`, `BtcCursor`, `CexCursor`), `EventSource` trait with async methods, `StateMachine` trait with a sync `apply`, an `Event<E>` envelope, and an in-memory `ReducerCore` that drives state forward. No network calls, no SQLite — just types and pure logic that downstream phases build on.

The primary technical challenge is Rust's `async fn in trait` with `Send` bounds. Since `EventSource` methods are `async` and implementations will be used with `tokio::spawn` (which requires `Send` futures), the traits need the `trait_variant` approach (or `#[async_trait]`) to declare a `Send`-bounded variant. This is a known, solved problem in the ecosystem — the solution is straightforward but must be chosen upfront because it affects trait ergonomics.

The second challenge is cursor design. EVM's `(block_number, log_index)` tuple, Bitcoin's `(block_height, tx_index)`, and CEX's `(timestamp_ms, seq_id)` need distinct types that each implement `Ord` (via derived lexicographic ordering on their fields), `Clone`, and `serde::Serialize + Deserialize`. The `ChainCursor` supertrait collects these bounds. The `Event<E>` envelope carries the cursor alongside the user's event payload and an optional `block_hash` for reorg detection in later phases.

**Primary recommendation:** Use `#[async_trait]` or `trait_variant` for `EventSource`; derive `Ord`/`PartialOrd`/`Eq`/`PartialEq` on cursor structs (field order determines comparison semantics — most significant field first); use `serde` with `derive` for cursor serialization. Keep Phase 1 purely in-memory; no I/O of any kind.

---

## Standard Stack

### Core (Phase 1 only needs these)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `tokio` | 1.x (1.44+ as of Mar 2026) | Async runtime, `watch` channel for state | No alternative; entire blockchain ecosystem assumes tokio |
| `serde` | 1.x | Cursor serialization / deserialization | Non-negotiable; `derive` macros make it zero-friction |
| `serde_json` | 1.x | Cursor round-trip tests (human-readable for test assertions) | Standard; also needed for later alloy integration |
| `trait-variant` | 0.1.x | Generate `Send`-bounded variant of async traits | Official Rust team crate; the recommended solution for `async fn in trait` + `Send` |
| `thiserror` | 2.x | Error types for `EventSource` error associated type | Ergonomic `derive`-based errors; `#[from]` for wrapping |

### Supporting (for tests only in Phase 1)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `tokio` with `macros` + `rt` features | 1.x | `#[tokio::test]` for async test cases | Every async test |
| `serde_json` | 1.x | Cursor round-trip assertions in tests | Serialize cursor → JSON → deserialize → compare |

### Phase 1 Does NOT Need

The following are needed in later phases but must NOT be pulled in during Phase 1:

- `alloy` — Phase 2+ (EVM adapter). Phase 1 defines the `EventSource` interface only.
- `rusqlite` — Phase 2+ (persistence).
- Snapshot serialization crate — Phase 2+. Phase 1 stores `Vec<u8>` (opaque bytes) as a placeholder.
- `tokio-tungstenite` — Phase 3+ (live WebSocket subscriptions).

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `trait-variant` | `#[async_trait]` macro | `async_trait` uses heap allocation (boxes the future); `trait_variant` is zero-cost. `async_trait` is more ergonomic and widely used; either works for Phase 1. Choose `async_trait` if trait ergonomics > micro-perf during prototype. |
| `serde` | Manual `From`/`Into` for cursor bytes | Serde derive is 3 lines per struct; manual is 50+; no contest |
| `thiserror` | `anyhow` | `thiserror` for library error types (caller can handle); `anyhow` for application error types. Library must use `thiserror`. |

**Installation (Phase 1 Cargo.toml):**
```toml
[dependencies]
tokio = { version = "1", features = ["sync"] }  # watch channel; add rt-multi-thread in later phases
serde = { version = "1", features = ["derive"] }
serde_json = "1"
trait-variant = "0.1"   # OR async-trait = "0.1" — see note below
thiserror = "2"

[dev-dependencies]
tokio = { version = "1", features = ["rt", "macros"] }
```

**Version note:** All versions verified against current crates.io as of 2026-03-03. `tokio` 1.x is at 1.44+; `serde` 1.x is stable and unchanged; `trait-variant` 0.1.x is stable.

---

## Architecture Patterns

### Recommended Project Structure (Phase 1 scope)

```
src/
├── lib.rs              # pub re-exports of public types
├── cursor/
│   ├── mod.rs          # ChainCursor trait + re-exports
│   ├── evm.rs          # EvmCursor { block_number: u64, log_index: u32 }
│   ├── btc.rs          # BtcCursor { block_height: u64, tx_index: u32 }
│   └── cex.rs          # CexCursor { timestamp_ms: i64, seq_id: u64 }
├── event.rs            # Event<E, C> envelope struct
├── source.rs           # EventSource trait
├── machine.rs          # StateMachine trait + ReducerCore
└── error.rs            # SourceError type

tests/
├── cursor_ordering.rs  # Ord + round-trip serialization tests
├── reducer_core.rs     # In-memory accumulation with mock source
└── mock_source.rs      # MockEventSource implementing EventSource
```

### Pattern 1: ChainCursor Supertrait

**What:** A supertrait that collects the bounds every cursor must satisfy. Concrete cursor types derive all required traits.

**When to use:** Define once; reference everywhere via `C: ChainCursor`.

```rust
// Source: Rust trait design patterns (standard Rust idiom)
use serde::{Serialize, de::DeserializeOwned};

pub trait ChainCursor:
    Ord + Clone + Send + Sync + 'static
    + Serialize + DeserializeOwned
{
}

// EvmCursor — field order determines Ord: block_number compared first, then log_index
// This is correct: log_index is block-scoped, so (block=100, log=5) < (block=101, log=0)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvmCursor {
    pub block_number: u64,
    pub log_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BtcCursor {
    pub block_height: u64,
    pub tx_index: u32,
}

// CexCursor: seq_id is the authoritative ordering key; timestamp_ms is metadata ONLY
// seq_id must come FIRST in field order to drive the Ord derivation correctly
// OR implement Ord manually to sort by seq_id only
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CexCursor {
    pub seq_id: u64,      // authoritative ordering — MUST be primary sort key
    pub timestamp_ms: i64, // metadata only
}

impl PartialOrd for CexCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CexCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq_id.cmp(&other.seq_id) // timestamp_ms deliberately excluded
    }
}

// Blanket impl: any type satisfying the bounds is a ChainCursor
impl<T> ChainCursor for T
where
    T: Ord + Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{}
```

**CRITICAL — `CexCursor` ordering:** Do NOT derive `Ord` for `CexCursor` with `timestamp_ms` as the first field. Exchange timestamps are not monotonic (clock skew, corrections, historical backfill). Ordering MUST be by `seq_id` only. If derived `Ord` is used, `seq_id` must be listed first in the struct. Manual `Ord` is safer and self-documenting.

### Pattern 2: EventSource Trait with async fn

**What:** The single trait that all chain adapters implement. Methods are async. Send bounds are handled via `trait_variant`.

**When to use:** One impl per chain source. Callers construct it and hand it to the Engine.

```rust
// Source: Rust 1.75+ async fn in trait + trait_variant pattern
// Reference: https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/

use futures_core::Stream;

#[trait_variant::make(EventSource: Send)]
pub trait LocalEventSource {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch the initial state snapshot at a specific cursor position.
    /// Called once on cold start. Returns opaque bytes (client owns serialization).
    async fn fetch_state(&self, at: &Self::Cursor) -> Result<Vec<u8>, Self::Error>;

    /// Fetch events in the half-open range [from, to).
    /// Must return events in cursor order.
    async fn fetch_events(
        &self,
        from: &Self::Cursor,
        to: &Self::Cursor,
    ) -> Result<Vec<Event<Self::Event, Self::Cursor>>, Self::Error>;

    /// Subscribe to live events starting from `from` cursor.
    /// Returns a stream; implementor owns the connection lifecycle.
    async fn subscribe(
        &self,
        from: &Self::Cursor,
    ) -> Result<
        impl Stream<Item = Result<Event<Self::Event, Self::Cursor>, Self::Error>> + Send,
        Self::Error,
    >;
}
```

**Note on `trait_variant`:** The macro generates two traits: `LocalEventSource` (no Send bounds, for single-threaded use) and `EventSource` (with `Send` bounds on futures). The Engine will use the `EventSource` bound. Implementations that are `Send` implement both automatically.

**Alternative — `#[async_trait]`:** If `impl Trait` in return position causes issues (e.g., for object safety or complex bounds), use `#[async_trait]`:

```rust
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    type Cursor: ChainCursor;
    type Event: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn fetch_state(&self, at: &Self::Cursor) -> Result<Vec<u8>, Self::Error>;
    async fn fetch_events(&self, from: &Self::Cursor, to: &Self::Cursor)
        -> Result<Vec<Event<Self::Event, Self::Cursor>>, Self::Error>;
    // subscribe uses Box<dyn Stream> with async_trait
    async fn subscribe(&self, from: &Self::Cursor)
        -> Result<Box<dyn Stream<Item = Result<Event<Self::Event, Self::Cursor>, Self::Error>> + Unpin + Send>, Self::Error>;
}
```

`async_trait` allocates a `Box<dyn Future>` per call (negligible for Phase 1; avoid in hot path once identified). It is simpler to use and more widely understood. **Recommendation: start with `async_trait` for Phase 1 (simpler); migrate to `trait_variant` when the hot path is profiled in Phase 3.**

### Pattern 3: Event Envelope

**What:** Library-owned wrapper around the user's event payload. Library adds metadata; user owns payload.

```rust
/// Library-owned event envelope.
/// C = cursor type (source-specific), E = user's event type (opaque to library).
#[derive(Debug, Clone)]
pub struct Event<E, C: ChainCursor> {
    pub cursor: C,
    pub data: E,
    pub block_hash: Option<[u8; 32]>, // None for CEX (no blocks); Some for EVM/BTC
}
```

### Pattern 4: StateMachine Trait + ReducerCore

**What:** The Apply side of the library. Apply is sync and infallible. `ReducerCore` holds state and drives updates.

```rust
/// Client implements this. Apply must be pure: no I/O, no side effects, deterministic.
pub trait StateMachine: Send + Sync + 'static {
    type State: Clone + Send + 'static;
    type Event: Send + 'static;
    type Cursor: ChainCursor;

    /// Pure reducer. Given current state and an event, return next state.
    /// Infallible: if the event is irrelevant, return `state` unchanged.
    fn apply(&self, state: Self::State, event: &Event<Self::Event, Self::Cursor>) -> Self::State;
}

/// In-memory state holder. Drives Apply and notifies watchers via watch channel.
pub struct ReducerCore<SM: StateMachine> {
    sm: Arc<SM>,
    sender: tokio::sync::watch::Sender<SM::State>,
}

impl<SM: StateMachine> ReducerCore<SM> {
    pub fn new(sm: Arc<SM>, initial_state: SM::State) -> (Self, tokio::sync::watch::Receiver<SM::State>) {
        let (sender, receiver) = tokio::sync::watch::channel(initial_state);
        (Self { sm, sender }, receiver)
    }

    /// Apply a single event to the current state.
    pub fn apply(&self, event: &Event<SM::Event, SM::Cursor>) {
        self.sender.send_modify(|state| {
            // Temporarily replace state with Default or a sentinel...
            // Cleaner: use send_replace
        });
        let current = self.sender.borrow().clone();
        let next = self.sm.apply(current, event);
        let _ = self.sender.send(next);
    }

    /// Get a receiver for watching state changes.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<SM::State> {
        self.sender.subscribe()
    }
}
```

**Note on `watch::send_modify` vs `send`:** `tokio::sync::watch::Sender::send` replaces the value and notifies. `send_modify` takes a `&mut T` closure — avoids a clone but requires in-place mutation. For `apply(state) -> state` (move semantics), using `send` with a `borrow().clone()` pattern is cleaner:

```rust
pub fn apply(&self, event: &Event<SM::Event, SM::Cursor>) {
    let current = self.sender.borrow().clone(); // one clone
    let next = self.sm.apply(current, event);
    let _ = self.sender.send(next);
}
```

### Pattern 5: Associated Types vs Generic Parameters

**Decision area (Claude's discretion):** The CONTEXT.md leaves this to implementation. Research recommendation:

**Use associated types on `StateMachine` and `EventSource`.** Reasoning:
- Each `StateMachine` implementation has exactly one `State`, one `Event`, and one `Cursor` — associated types express "one canonical choice per impl"
- Generic parameters would require callers to specify all three at every use site: `Engine<SM, SM::State, SM::Event, SM::Cursor>` — redundant and noisy
- Associated types are the idiomatic Rust choice for traits where one impl = one configuration

**Exception:** `EventSource` could reasonably use associated types only. The `Engine` generic bound becomes `ES: EventSource` — clean.

### Anti-Patterns to Avoid

- **`AnyCursor` enum**: `enum AnyCursor { Evm(EvmCursor), Btc(BtcCursor), Cex(CexCursor) }` — forces match arms everywhere, breaks `Ord` cleanly, couples all source types. Use `C: ChainCursor` generics instead.
- **Apply with `Result`**: `fn apply(...) -> Result<State, Error>` — the locked decision is infallible Apply. Remove error paths entirely.
- **async Apply**: `async fn apply(...)` — Apply must be sync. If it's async it cannot be called on the hot path without spawning, which adds latency and complexity. The `StateMachine` trait must make this impossible at the type level.
- **Serializing events on the hot path**: Events flow as native Rust types. Do not add `Serialize` bounds to `Event::data` in Phase 1 — that forces client events to implement serde even when only in-memory accumulation is needed.
- **`block_number` alone as cursor equality**: `EvmCursor { block_number, log_index }` — block_number alone is not unique (multiple logs per block). The `Eq` implementation must compare both fields. Derived `PartialEq`/`Eq` on the full struct is correct.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| async fn in trait + Send bounds | Manual `Box<dyn Future + Send>` wrapper | `trait_variant` or `async_trait` | Standard pattern; hand-rolling futures boxing is error-prone |
| State change notification | Custom `Arc<Mutex<Vec<Sender>>>` broadcast | `tokio::sync::watch` | Watch is exactly "latest value + notify on change" — zero extra complexity |
| Cursor serialization | Manual byte encoding (`write_u64`, etc.) | `serde` + `serde_json` (tests) / `serde` + `postcard` (later persistence) | Derive macros; serde handles endianness, field ordering, schema evolution |
| Error type wrapping | `String`-based errors | `thiserror` | Ergonomic derive, `#[from]` for composition, library-appropriate (not `anyhow`) |

**Key insight:** Phase 1 is type definitions and trait contracts. The complexity is in choosing the RIGHT abstractions, not in implementation volume. Resist the urge to add infrastructure — if a component isn't needed to satisfy a Phase 1 success criterion, it doesn't belong here.

---

## Common Pitfalls

### Pitfall 1: CexCursor Field Order in Derived Ord

**What goes wrong:** Developer writes `struct CexCursor { timestamp_ms: i64, seq_id: u64 }` and derives `Ord`. Rust's derived `Ord` compares fields in declaration order. Timestamps are not monotonic from CEX APIs (clock skew, historical fill timestamps). Ordering by timestamp first produces incorrect sort results for CEX events.

**Why it happens:** It's natural to put the human-readable timestamp first. The ordering consequence of field order in derived `Ord` is subtle.

**How to avoid:** Either (a) put `seq_id` first in struct declaration so derived `Ord` sorts by it, or (b) implement `Ord` manually. Manual implementation is self-documenting and immune to refactoring that reorders fields.

**Warning signs:** Struct has `timestamp_ms` as the first field AND derives `Ord` without comment explaining the field order is intentional.

### Pitfall 2: async fn in trait Send Bound Compile Errors

**What goes wrong:** `EventSource` has `async fn fetch_events(...)`. The Engine tries to use `ES: EventSource` in a `tokio::spawn` context, which requires `Send`. Compiler error: "future returned by `fetch_events` is not `Send`".

**Why it happens:** Rust's `async fn in trait` stabilization (Rust 1.75, Dec 2023) does not automatically make futures `Send`. The `Send`-ness depends on whether the implementation captures non-Send types.

**How to avoid:** Use `trait_variant` to generate a `Send`-bounded trait variant:
```rust
#[trait_variant::make(EventSource: Send)]
pub trait LocalEventSource { ... }
```
Or use `#[async_trait]` which handles this implicitly.

**Warning signs:** Trait has `async fn` methods and is used with `tokio::spawn` or `tokio::task::spawn` without one of these solutions.

### Pitfall 3: `ReducerCore::apply` Double-Clone

**What goes wrong:** Naive implementation: `let state = self.sender.borrow().clone(); let next = self.sm.apply(state, event); self.sender.send(next)`. This is correct but performs one clone. Premature optimization attempts (using `send_modify`) can introduce borrow checker fights when `apply` needs owned `State`.

**Why it happens:** `tokio::sync::watch::Sender::send_modify` takes `&mut T` — useful for mutation in place, but `StateMachine::apply` takes `State` by value (move), not `&mut State`. The pattern doesn't fit directly.

**How to avoid:** Stick with the one-clone pattern for Phase 1. Profile later. The clone is on the state type — for simple reduced state (`u128` balance), it's a copy. For complex state, this is where optimization would matter — but that's Phase 3+.

### Pitfall 4: Snapshot Bytes Confusion in Phase 1

**What goes wrong:** Phase 1 includes `fetch_state` returning `Vec<u8>` (opaque bytes). Developer tries to deserialize these bytes in the library with `bincode` or similar, but the locked decision is that serialization is client-owned. Library only stores and retrieves the bytes.

**How to avoid:** `fetch_state` returns `Vec<u8>`. `ReducerCore` accepts `Vec<u8>` and passes it to the client's "restore from snapshot" function. Library never calls `deserialize`. This boundary must be clear in the trait docs.

### Pitfall 5: Including Persistence in Phase 1

**What goes wrong:** Developer sees CORE-05 ("supports accumulated state — events stored on disk") and immediately adds `rusqlite` dependency and disk writes to Phase 1.

**How to avoid:** Phase 1's success criterion for CORE-05 is: "An in-memory reducer accumulates events correctly in the order they are applied, producing a deterministic final state verifiable by a unit test." Disk storage is Phase 2. Accumulated state in Phase 1 = `Vec<Event<E, C>>` in memory.

---

## Code Examples

Verified patterns from official sources:

### ChainCursor Blanket Implementation

```rust
// Standard Rust supertrait pattern
// Source: Rust Reference - Supertraits
pub trait ChainCursor:
    Ord + Clone + Send + Sync + 'static
    + serde::Serialize + serde::de::DeserializeOwned
{}

// Blanket impl: any type satisfying all bounds is automatically a ChainCursor
impl<T> ChainCursor for T
where
    T: Ord + Clone + Send + Sync + 'static
        + serde::Serialize + serde::de::DeserializeOwned,
{}
```

### tokio::sync::watch for State

```rust
// Source: https://docs.rs/tokio/latest/tokio/sync/watch/index.html
use tokio::sync::watch;

let initial_balance: u128 = 0;
let (tx, rx) = watch::channel(initial_balance);

// Writer (ReducerCore):
let new_balance = old_balance + transfer_amount;
tx.send(new_balance).unwrap();

// Reader (caller, anywhere):
let current = *rx.borrow(); // lock-free read of latest value
// Or watch for changes:
// rx.changed().await
```

### alloy Provider::subscribe_logs (for reference — Phase 2+ EVM adapter)

```rust
// Source: https://alloy.rs/examples/subscriptions/subscribe_logs/
// This is reference only — Phase 1 does not use alloy
use alloy::{
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
};
use futures_util::stream::StreamExt;

let ws = WsConnect::new("wss://...");
let provider = ProviderBuilder::new().connect_ws(ws).await?;

let filter = Filter::new()
    .address(contract_address)
    .event("Transfer(address,address,uint256)")
    .from_block(BlockNumberOrTag::Latest);

let sub = provider.subscribe_logs(&filter).await?;
let mut stream = sub.into_stream();

while let Some(log) = stream.next().await {
    // log is alloy::rpc::types::Log
}
```

### Cursor Round-Trip Test Pattern

```rust
// Standard serde round-trip test
#[test]
fn evm_cursor_roundtrip() {
    let cursor = EvmCursor { block_number: 12_345_678, log_index: 42 };
    let json = serde_json::to_string(&cursor).unwrap();
    let restored: EvmCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cursor, restored);
}

#[test]
fn evm_cursor_ordering() {
    let a = EvmCursor { block_number: 100, log_index: 5 };
    let b = EvmCursor { block_number: 100, log_index: 10 };
    let c = EvmCursor { block_number: 101, log_index: 0 };
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
}

#[test]
fn cex_cursor_ordering_by_seq_id_not_timestamp() {
    // seq_id=2 is after seq_id=1 even if timestamp_ms is earlier
    let a = CexCursor { seq_id: 1, timestamp_ms: 1000 };
    let b = CexCursor { seq_id: 2, timestamp_ms: 999 }; // earlier timestamp, higher seq
    assert!(a < b, "ordering must be by seq_id, not timestamp_ms");
}
```

### Mock EventSource for Testing

```rust
// Standard Rust test pattern: struct implementing the trait under test
pub struct MockEventSource<C: ChainCursor, E: Send + 'static> {
    events: Vec<Event<E, C>>,
    state_bytes: Vec<u8>,
}

#[async_trait::async_trait]
impl<C, E> EventSource for MockEventSource<C, E>
where
    C: ChainCursor,
    E: Clone + Send + 'static,
{
    type Cursor = C;
    type Event = E;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &C) -> Result<Vec<u8>, Self::Error> {
        Ok(self.state_bytes.clone())
    }

    async fn fetch_events(&self, from: &C, to: &C)
        -> Result<Vec<Event<E, C>>, Self::Error>
    {
        Ok(self.events.iter()
            .filter(|e| &e.cursor >= from && &e.cursor < to)
            .cloned()
            .collect())
    }

    async fn subscribe(&self, _from: &C)
        -> Result<Box<dyn futures_core::Stream<Item = Result<Event<E, C>, Self::Error>> + Unpin + Send>, Self::Error>
    {
        use futures_util::stream;
        Ok(Box::new(stream::empty()))
    }
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `async-trait` crate (boxes futures) | `trait_variant` or native `async fn in trait` | Rust 1.75 (Dec 2023) | Zero-cost async traits; `async_trait` still valid but no longer required |
| `ethers-rs` | `alloy` 1.x | alloy stable May 2025, alloy 1.5.2 as of Mar 2026 | Same authors; alloy has richer type system; ethers-rs maintenance-only since late 2023 |
| `bincode` v2 | `postcard` 1.1.3 | bincode archived Aug 2025, 3.0.0 tombstone released Dec 2025 | **bincode is dead** — use `postcard` for binary serialization. But Phase 1 avoids serialization crates entirely (snapshot bytes are opaque). |
| `rusqlite` ~0.32 | `rusqlite` 0.38.0 | Incremental; current as of Mar 2026 | API-compatible within 0.x; bundled feature unchanged |

**Deprecated/outdated:**

- **`bincode`**: The original maintainer was doxxed; development ceased; `bincode` 3.0.0 (Dec 2025) is a tombstone release that produces a compiler error. **Do not add `bincode` as a dependency.** Use `postcard` for Phase 2 snapshots (if the library chooses binary over JSON). `postcard` 1.1.3 is actively maintained, stable wire format since 1.0, serde-compatible, 16M+ downloads.
- **`ethers-rs`**: Maintenance-only since late 2023. All new code should use `alloy`.
- **`async-trait` (for new traits)**: Still works and is widely deployed, but for new trait definitions `trait_variant` is preferred for zero-cost async dispatch. `async-trait` is not wrong, just not optimal.

---

## Open Questions

1. **`async_trait` vs `trait_variant` for `EventSource`**
   - What we know: Both work. `async_trait` allocates one `Box<dyn Future>` per call (~10-50ns overhead). `trait_variant` is zero-cost. Phase 1 has no hot path (no live subscriptions).
   - What's unclear: Whether the `impl Trait` return position in `subscribe` (which returns a `Stream`) creates additional complexity with `trait_variant` in practice.
   - Recommendation: Start with `async_trait` for Phase 1 (simpler, more ergonomic, no gotchas). Plan to migrate `EventSource::subscribe` to `trait_variant` in Phase 3 when the hot path is identified.

2. **`Event<E, C>` generic parameter order**
   - What we know: Either `Event<E, C>` or `Event<C, E>` is valid Rust. Convention varies.
   - What's unclear: Whether having `C` as the second parameter creates inference issues at common call sites.
   - Recommendation: Put `C: ChainCursor` first since it's the more constrained type: `Event<C, E>`. This matches how alloy types list the most-constrained parameter first.

3. **`StateMachine` trait name vs the locked decisions**
   - What we know: CONTEXT.md uses "Apply" as the trait name in some places, "StateMachine" in others. REQUIREMENTS.md says "StateMachine trait."
   - What's unclear: Is the client-facing trait named `StateMachine` with an `apply` method, or named `Apply`?
   - Recommendation: Name the trait `StateMachine` (matches REQUIREMENTS.md CORE-02). Name the method `apply`. This is consistent with the CONTEXT.md "Apply is a separate trait" language (referring to the concept, not the Rust identifier).

---

## Sources

### Primary (HIGH confidence)
- CONTEXT.md (`/home/nb/src/chainfold/.planning/phases/01-foundations/01-CONTEXT.md`) — locked decisions, all binding
- REQUIREMENTS.md (`/home/nb/src/chainfold/.planning/REQUIREMENTS.md`) — CORE-01 through CORE-05 definitions
- Prior project research (`/home/nb/src/chainfold/.planning/research/`) — STACK.md, ARCHITECTURE.md, PITFALLS.md, all authored 2026-03-02
- Rust Blog: https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/ — async fn in trait stabilization (Rust 1.75)
- alloy docs.rs: https://docs.rs/alloy/latest/alloy/ — alloy 1.5.2 module structure verified
- alloy examples: https://alloy.rs/examples/subscriptions/subscribe_logs/ — subscribe_logs API pattern
- tokio docs (stable 1.x): https://docs.rs/tokio/latest/tokio/sync/watch/ — watch channel semantics

### Secondary (MEDIUM confidence)
- WebSearch results confirming alloy 1.5.2 as current version (crates.io via docs.rs redirect)
- WebSearch results confirming rusqlite 0.38.0 as current version
- WebSearch results confirming tokio-tungstenite 0.28.0 as current version
- bincode tombstone notice via docs.rs (https://docs.rs/crate/bincode/latest) — confirmed unmaintained
- postcard 1.1.3 confirmed active (16M+ downloads, serde-compatible, stable wire format)

### Tertiary (LOW confidence — verify before use)
- `trait_variant` crate ergonomics with `impl Stream` return types — verify in practice; may need `async_trait` as fallback
- `bincode` 2.0.1 as last usable bincode version — confirmed by release history; but given unmaintained status, prefer `postcard` for Phase 2

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — tokio, serde, thiserror are stable and unambiguous; async fn in trait approach verified against Rust blog
- Architecture: HIGH — trait design patterns are well-established Rust idioms; cursor field ordering is a known gotcha fully documented
- Pitfalls: HIGH — CEX cursor ordering and async Send bounds are verified failure modes from Rust ecosystem
- Version numbers: HIGH for major choices (alloy 1.5.2, rusqlite 0.38.0, postcard 1.1.3); verified via WebSearch against crates.io/docs.rs

**Research date:** 2026-03-03
**Valid until:** 2026-06-03 (90 days — stable Rust ecosystem; async fn in trait is stable, no breaking changes expected)

**Critical new finding vs prior research:** bincode is unmaintained (tombstone release Dec 2025). Prior research flagged "verify bincode v2 left RC" — the answer is: v2 released as 2.0.1 (stable), then v3.0.0 tombstone. Do not use bincode. Phase 1 is unaffected (no serialization crates needed). Phase 2 snapshot decision should default to `postcard`.
