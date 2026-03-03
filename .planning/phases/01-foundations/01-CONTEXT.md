# Phase 1: Foundations - Context

**Gathered:** 2026-03-03
**Status:** Ready for planning

<domain>
## Phase Boundary

Core type system — cursor types with generic ordering, EventSource trait abstracting chain interaction, Apply trait as a pure reducer, and in-memory state management for both reduced and accumulated state shapes. No I/O, no persistence, no live subscriptions — those are Phase 2+.

</domain>

<decisions>
## Implementation Decisions

### Trait granularity
- One `EventSource` trait combining subscribe(), fetch_events(range), and fetch_state(at). One impl per chain, discoverable API.
- `Apply` is a separate trait from EventSource — pure reducer logic is cleanly separated from I/O operations.
- FetchState and FetchEvents live on EventSource — all chain interaction through one trait.
- Same Apply trait for both reduced and accumulated state. State shape is the caller's concern, not the trait's.

### Error model
- Apply is infallible — always returns a new state. If an event is irrelevant, state doesn't change. No error path on the hot path.
- EventSource methods return errors via a generic wrapper around Box<dyn Error>. Adapters don't need to conform to a library-defined error enum.
- No event validation — events are opaque structs defined by the caller. Library never inspects event contents.
- Snapshot serialization is client-owned. Library stores and loads opaque bytes (Vec<u8>). Client implements their own marshal/unmarshal.

### Async boundary
- EventSource trait methods are async (async fn fetch_events, async fn fetch_state, async fn subscribe). Natural Rust async, requires tokio.
- OnChange callback is sync: Fn(&S). Runs inline on the event processing path. If caller needs async work, they spawn it themselves. Preserves hot-path latency.
- Library requires tokio directly — not runtime-agnostic. Use tokio::spawn, tokio channels, tokio timers. Pragmatic choice for the blockchain ecosystem.
- Engine is caller-driven: Engine::run() is an async fn the caller .awaits or spawns. Caller controls task lifecycle and shutdown.

### State access
- Reduced state accessed via tokio::sync::watch channel. Caller gets a Receiver that always has the latest value. Lock-free reads, automatic change notification. Idiomatic tokio.
- Accumulated state accessed via lazy cursor iterator over a range. Events come from memory or SQLite. No full-result buffering.
- State type bounds are minimal: Clone + Send. Serialize/Deserialize bounds only on snapshot-related methods, not the core trait.
- Library wraps user events in an envelope: Event<E> { cursor: C, data: E, block_hash: Option<Hash> }. Library owns metadata (cursor, block_hash), user owns payload (E).

### Claude's Discretion
- Exact cursor type definitions (EvmCursor fields, BtcCursor fields, CexCursor fields)
- Internal Engine architecture (channels, task structure)
- Test harness design for mock EventSource implementations
- Whether to use associated types or generic parameters on traits

</decisions>

<specifics>
## Specific Ideas

- The design doc mentions three ordering schemes: (blockNumber, logIndex) for EVM, (blockHeight, txIndex) for Bitcoin, (timestamp, sequenceId) for CEX
- Apply is described as a "forward-only pure reducer" — no Unapply, no side effects
- The library should "fold events into state" — the name "chainfold" reflects this core abstraction
- Event<E> envelope allows library to handle cursor comparison and reorg detection without knowing about E

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- None — greenfield project

### Established Patterns
- None yet — this phase establishes the patterns

### Integration Points
- Cursor types will be used by every subsequent phase (persistence, subscriptions, queries)
- EventSource trait is the extension point for EVM/Bitcoin/CEX adapters
- Apply trait + watch channel pattern flows directly into Phase 3 (live subscriptions) and Phase 4 (query API)

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-foundations*
*Context gathered: 2026-03-03*
