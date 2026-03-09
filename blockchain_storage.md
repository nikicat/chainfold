Usage scenarios for blockchain event store.

1. State reconstruction.

Typical example - ERC20 address's balance. But it generalizes to any EVM contract and even non-EVM. Current approach: client obtains contract state at block X. Then subscribes and processes all required events (even for ERC20 we need to watch 2 filters - transfers from address and transfers to address) since that block X. On each event client increases/decreases the balance. In mroe complicated cases (like AMM pool DEX) there could be much more event types and more complicated state calculation. In some cases when particular event occurs state must be just refetched again.
Difficulties with this approach:
 - gap between state fetching and subscription
 - eth_ rpc doesn't allow to subscribe from arbitraty block in the past, one needs to first fetch history, then subscribe. Again, gap can occur between fetching filtring history and subscribing to new events, so after subscription there is a need to refetch missed blocks again. 
 - in a generic case not so easy to go back in time

2. History comparison

Typical example - crosschain bridge. There are 2 corresponding event histories on 2 chains, the bridge daemon need to track these histories on both chains, after each update find difference and process all events that exist in source chain and absent in destination chain.
Difficulties with traditional RPCs:
 - need to combine history fetching (eth_getLogs, eth_filterLogs) and subscription to new events
 - need to store anchors - positions inside histories on both chains to avoid scanning though all the history
 - need to handle reconnects, refetch missed history parts etc.

Also both scenario involves repeated RPC fetches for the same block ranges and filters, so they required extensive caching to avoid hitting RPC rate limits.

Now lets discuss how to implement it. What's on my mind:
 - SQL to express blockchain filters. Sounds good, but SQL not so good for state reconstruction and live state updates. Also, in general, query "give me all erc20 transfer for this address" involves scanning till contract creation block (potentially millions of blocks), so results can be returned in a single response - cursor is needed. But this cursor should not consume a lot of resources, it should be lightweight pointer to a (block,log index). SQL advantages: it has JOINs. So we can SELECT erc20.*, block_timestamp FROM erc20 JOIN blocks USING (block_number) WHERE token = xxx AND ("from" = yyy OR "to" = yyy). Also SQL has filters: SELECT * FROM sourc_bridge WHERE crosschain_id NOT IN (SELECT crosschain_id FROM destination_bridge). Dune works this way - SQL is really good for analytics. But it's not suited very well for live data and subscriptions - we don't want to refetch ton of events on each update, we need only difference.

 - server-side filters. That's how eth_getFilterChanges works. The main drawback - it has no explicit anchoring, it just returns events since last poll. But last poll payload could require re-processing. Also, filters live on a node, so client can't switch node. If node goes offline, client need to somehow migrate it's state to another node, so it essentially need to manage it's state.

 - I don't event sure that this chain state tracked should be implemented as a server - a lot of operations more natural to do in-process, inside the same address space - think of iteration.

 - Caching. I want to emphasize, that effective working heavily relies on caching of all fetched data. Ideally every chain event or contract state or block timestamp should be fetched only once, and be fetched within large enough batch, i.e. not one by one. In and ideal world with unlimited-size server we could just download all the blockchains locally, but that would require petabyte ram for effective work.

---

## Design Summary (from brainstorm session)

### What It Is

A library that hides the complexity of reconstructing and maintaining blockchain state from heterogeneous data sources. The client defines *what* state it cares about and *how* events affect that state. The library handles the entire lifecycle: initial state fetch, historical replay, live tracking, caching, gap recovery, and restart resilience.

### Core Abstraction

Every state is a function of: **an initial snapshot + ordered events applied forward.**

The client provides three primitives as plugins in their native language:

- `FetchState(at: Cursor) → S` — RPC call for authoritative state at a point
- `FetchEvents(from, to: Cursor) → []E` — get events in a range
- `Apply(state: S, event: E) → S` — forward-only pure reducer

The library provides:

- `Current() → S` — live-tracked state at chain tip
- `At(cursor) → S` — historical state (library picks cheapest reconstruction path)
- `Range(from, to) → Iterator<S>` — materialized state across a range
- `OnChange(callback)` — live notifications, µs-order

### Two State Shapes

**Reduced state** — events fold into a compact value (balance, pool reserves, position size). Held in memory, accessed as a single value.

**Accumulated state** — events ARE the state (transfer history, order fills). Stored on disk, accessed via cursors and anchors.

Both share the same event ingestion and caching infrastructure.

### Data Sources

The library is source-agnostic. The client implements an `EventSource` interface:

| Source Type | Ordering | Examples |
|---|---|---|
| EVM chains | (blockNumber, logIndex) | Ethereum, Arbitrum, Base via eth_subscribe / eth_getLogs |
| Bitcoin | (blockHeight, txIndex) | bitcoind RPC, Electrum protocol |
| CEX-like APIs | (timestamp, sequenceId) | Hyperliquid, dYdX, Binance WebSocket + REST |

### Architecture Constraints

1. **Hot path is in-process.** Events flow WebSocket → Apply(state, event) → callbacks without leaving the process. No IPC, no serialization on the critical path. ms-order latency preserved.

2. **SQLite is cache, not transport.** Events written to SQLite asynchronously after state is already updated in memory. SQLite only read during cold start reconstruction.

3. **App owns the connection.** The library doesn't connect to chains/exchanges. The app implements EventSource using its own direct, co-located connections.

4. **Forward replay only.** Backward scrolling done by fetching state at an earlier point and replaying forward. No generic Unapply.

5. **Library picks cheapest reconstruction path.** For historical queries: cached snapshot + event replay (0 RPC), partial replay + gap fetch (1 RPC), or direct FetchState (1 RPC).

### Persistence

SQLite with WAL mode. Four tables:

- **events** — cached events, keyed by (source, sequence, sub_index)
- **snapshots** — periodic state snapshots (opaque bytes), for reduced states
- **anchors** — named cursor positions with optional metadata
- **coverage** — which block/sequence ranges are fully cached per source

On restart: load snapshot → replay cached events → catch up via FetchEvents → switch to live.

### Language Strategy

**Go first** — primary development, battle-tested with Go services.

For Python/TS consumers, decision pending:
- (a) Rust core with PyO3 + napi-rs (clean FFI, requires Rust)
- (b) Per-language reimplementation (~450 lines each, sharing SQLite schema)
- (c) Go for Go apps + Rust for Python/TS (two impls, best FFI for each)

SQLite schema is the stable cross-language contract regardless of option.

### Existing Solutions Evaluated

- **The Graph**: Indexing/query platform, not event sourcing. No live push, no anchors, no cross-chain diff, no CEX support. Good for analytics, wrong abstraction for operational tracking.
- **Substreams**: Fast parallel historical extraction (10-100x vs RPC). But remote execution only, Rust-only modules, no in-process usage, no CEX support. Could serve as a backend data source for EVM backfill but doesn't replace the library.
- Both add latency vs direct eth_subscribe (~50-200ms overhead minimum), disqualifying them from the hot path given ms-order latency requirement.

### Open Design Questions

1. Generics vs `any` for state type parameter in Go
2. Snapshot serialization ownership (marshal/unmarshal)
3. Reorg handling — rollback to previous snapshot + replay, or notify client
4. Secondary indexes on accumulated state (e.g., crosschain_id for bridge diff)
5. Snapshot density strategy — fixed interval vs adaptive
6. Writer coordination when multiple apps share a SQLite cache
