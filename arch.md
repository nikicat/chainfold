chainfold allows it's clients to:
 - read history of chain/cex events with caching - i.e. given time range first time it fetches events from source (EVM RPC or CEX API), and afterward the same time range is read from cache only - no new remote requests.
 - automatically updates caches - if requested range is only partially cached then it deduces what ranges are missing and fetches them only
 - handles connections/reconnections - client just receives events sooner or later, all connection-related stuff is handled by chainfold. No network errors is ever returned to client.
 - allows to read event histories incrementally - when client requests range (2020-01-01; 2026-01-01) it doesn't wait a month until the lib fetches gazillion of events, it returns an object that represents the history of requested events in the specified range. Client can iterate over it, obtain positions while iterating, compare positions across histories, etc.
 - allows to subscribe to new events and receive them instantly - i.e. within 1-2 ms - since the moment they are received through RPC/API. Live events are delivered from an in-memory range index without going through the persistent cache on the critical path.
 - allows to subscribe to events beginning at some point in the past, like from (2026-01-01; unbounded). That means the library returns an iterator that yields events starting at some point in the past, and then, when it yielded the last existing event, it waits until upstream (RPC or API) notifies about a new event and then yields it to the client.
 - allows to specify ranges for histories not only as timestamps but as chain-specific Positions, like (block_number, log_index) for EVM, or (block_number, tx_index, input_index) for BTC. These Positions are exposed on items, can be persisted, and used later to request other Hist ranges. This allows resumption from where processing last stopped.
 - allows not only to obtain event Hists but also StateHists. States are like events - have a Position and some data - but they are constructed (reduced) from events. Typical examples of States are Balances (e.g. History of Balances of owner of ERC20 token), or Transfers (e.g. History of Transfers — each item is a pointer into the linked list of all past Transfers for a specified account). To avoid huge memory copying on each new event, each Transfers object is just a pointer to a linked list of Transfer objects, so the History effectively is a list of such pointers. The linked list is virtual - pointers may target States or Events that have not yet been fetched and will be lazy-loaded from the event source.
 - States are cached too, much like Events.

In the end this library should allow clients to work with chains like they work with a local full-node, but without the need to download the full blockchain, because usually an app needs to work only with a small set of accounts/contracts/bitcoin addresses and a full local blockchain would be overkill for that.

# Core abstractions

The API rests on five shapes:

- **Position** — a separator dividing a history into "before" and "after". Carries a `side` (Before/After) so two distinct positions can sit on either side of the same item. Positions are bound to the history that produced them and are serializable for persistence (a Position doubles as an `Anchor`).
- **Hist<T, P>** — a *finite, immutable* snapshot of a history segment. Has a `start` Position and an `end` Position. Iterable forward; splittable at a Position into two adjacent Hists. Iteration is lazy — items are fetched on demand and cached.
- **HistStore<T, P>** — a reactive stream of `Hist` snapshots. Each time the tip advances (a new event arrives, a new block mines, finalization advances), the store yields a fresh `Hist` whose `end` reflects the new tip. The store owns the live subscription; consumers drive iteration off snapshots.
- **Store<T>** — a reactive value with `subscribe()` semantics (inspired by Svelte/Zustand). `HistStore` exposes three of them: `latest` (mempool), `best` (mined), `safe` (finalized).
- **Reducer / Matcher** — combinators that build derived histories from one or more source HistStores. Reducer is for monotonic folds (running balance, cumulative volume); Matcher is for stateless src↔dst correlation (bridge tracking, deposit/withdraw pairing). Both ship.

The library used to also have a `Cursor` type for navigation. It's superseded: items expose their Position directly (`item.pos`), and "next item" / "previous item" is just `hist.split(pos)` plus forward iteration.

Typescript-like pseudocode:

```lang=typescript

// Each history/chain implements its own Position.
// Position is a separator of a Hist - it does not point to a specific event or state,
// and it's not equivalent to any item handle (e.g. an empty Hist can have no items but still has positions).
// A Position divides a Hist into two parts - before and after - that can be iterated.
// Either part can be empty - in that case the Position points to the beginning or the end.
// Implementations usually store positions as pointers to an event plus a Side field indicating
// whether the position sits just before or just after that event.
// That means that between two adjacent items there always are 2 equivalent positions:
// one just after the former and one just before the latter.
// This allows callers to include/exclude an event when querying by position.
// Positions are bound to the Hist - using a position from one Hist on another Hist is undefined.
// Positions are not always linearly ordered (because of reorgs); generally they can only be compared
// for equivalence via isEquiv().
// A Position also serves as an Anchor - it is serializable for persistence.
interface Position {
  // Bytes for persistence. Pair with HistStore.load() to reconstruct.
  marshal(): bytes

  // Which side of the underlying event coordinate this position sits on.
  side: "before" | "after"
  withSide(s: "before" | "after"): Position
}

// Item-level type. Each event/state carries its own Position - the position just after it
// (i.e. between this item and the next). To get the position just before, use pos.withSide("before").
interface Positioned<P extends Position> {
  readonly pos: P
}

// Reactive value - inspired by Svelte/Zustand stores, but minimal.
class Store<T> {
  subscribe(): AsyncGenerator<T>
  get(): T
}

// A finite, immutable history segment. Lazy: iter() fetches items on demand,
// caching them in the underlying LogIndex. start..end bound the segment.
class Hist<T extends Positioned<P>, P extends Position> {
  readonly start: P
  readonly end: P

  // Yield items in forward order from start to end.
  iter(): AsyncGenerator<T>

  // Split this Hist at position `a` into (notAfter, after).
  // Clamps `a` to [start, end] - splitting outside the window yields one empty
  // side and one identical side. This is important when composing combinators
  // over a previously-narrowed Hist.
  split(a: P): readonly [Hist<T, P>, Hist<T, P>]

  // Return a Position such that splitting on it skips exactly `items` from the head.
  // Returns undefined if `items` does not form a valid prefix of this Hist.
  // Used by combinators (Filtered, Mapped) and Matcher to advance anchors past
  // a processed batch.
  posAfter(items: readonly T[]): Promise<P | undefined>
}

// Reactive stream of Hist snapshots. Owns the live subscription to the upstream source.
// The first snapshot is yielded as soon as the store is initialized; subsequent snapshots
// are yielded whenever the tip advances.
class HistStore<T extends Positioned<P>, P extends Position> {
  // Yields a fresh Hist each time the tip advances. Never ends.
  gen(): AsyncGenerator<Hist<T, P>, never>
  // Promise of the current or next available Hist.
  next(): Promise<Hist<T, P>>
  // Subscribe via callback. Returns a disposable to cancel.
  subscribe(cb: (h: Hist<T, P>) => void): AsyncDisposable

  // Reactive tip positions.
  // `latest` updates as new events arrive (may include mempool/unconfirmed).
  // `best` updates as events are mined into blocks.
  // `safe` updates as blocks finalize (no further reorgs possible).
  // For CEX sources all three may collapse to the same store.
  readonly latest: Store<P>
  readonly best: Store<P>
  readonly safe: Store<P>

  // Sentinel positions. earliest.before() is always null; never.after() is always null.
  readonly earliest: P
  readonly never: P

  // Approximate position at a wall-clock time on the canonical chain.
  // Useful as an anchor for time-bounded queries; does not need to be exact.
  at(time: Timestamp): Promise<P>

  // Deserialize a Position previously persisted via pos.marshal().
  load(data: bytes): P

  // True iff a and b are equivalent positions - i.e. they sit in the same gap
  // between two adjacent items (or at the same boundary).
  isEquiv(a: P, b: P): Promise<boolean>

  // Latest common ancestor on the canonical chain. Always exists because
  // `earliest` is an ancestor of every position. Use this to roll back from a
  // stale (reorged) position to a canonical one, then replay forward.
  lca(a: P, b: P): Promise<P>
}

// Convenience: yield all items from `store` strictly after `start`. Never ends.
// If `start` is omitted, begins from the current tip. Centralizes the
// "snapshot + split + iter tail" loop most consumers want.
async function* watchHist<T extends Positioned<P>, P extends Position>(
  store: HistStore<T, P>,
  start?: P,
): AsyncGenerator<T, never> { /* ... */ }

// Convenience: snapshot the tip, run `op`, await the first item matching `predicate`.
// The snapshot happens BEFORE op so a fast op surfacing in history immediately
// doesn't race the watcher.
async function actAndWait<T extends Positioned<P>, P extends Position, R>(
  store: HistStore<T, P>,
  op: () => Promise<R>,
  predicate: (item: T, result: R) => boolean,
): Promise<T> { /* ... */ }

// Represents an EVM chain. Constructed by users with an RPC endpoint.
class EvmChain {
  readonly blocks: HistStore<EvmBlock, EvmPosition>
  readonly transactions: HistStore<EvmTransaction, EvmPosition>
  contract(address: Address): Promise<EvmContract>
  erc20(address: Address): Promise<Erc20>
}

class EvmPosition implements Position {
  blockNumber: bigint
  logIndex: number       // index of event inside the block (or -1 for block-level positions)
  blockHash: Hash        // disambiguates forks during reorgs
  timestamp: Timestamp   // block timestamp; convenience for at()
  side: "before" | "after"
}

type EvmHist<T> = Hist<T, EvmPosition>
type EvmHistStore<T> = HistStore<T, EvmPosition>

type ContractState = bytes

class EvmContract {
  address: Address
  chain: EvmChain

  // Generic contract states - plain bytes derived from on-chain reads.
  states: EvmHistStore<ContractStateItem>

  // The block at which the contract was created. Useful as a default start position.
  createdAt: EvmPosition
}

interface Asset {
  symbol: string         // USDC, USDT, WBTC, ...
  name: string
}

class Erc20 extends EvmContract implements Asset {
  symbol: string
  name: string
  decimals: number

  // ERC20 event histories.
  transfers(from?: Address, to?: Address): EvmHistStore<Erc20Transfer>
  mints(to: Address): EvmHistStore<Erc20Mint>
  burns(from: Address): EvmHistStore<Erc20Burn>

  // Memoized per owner.
  ownedBy(owner: Address): OwnedErc20
}

class EvmEvent implements Positioned<EvmPosition> {
  contract: EvmContract
  blockNumber: bigint
  timestamp: Timestamp
  blockHash: Hash
  logIndex: number
  txId: TxId
  args: EvmEventArgs
  pos: EvmPosition       // position just after this event
}

class Erc20Transfer implements Positioned<EvmPosition> {
  event: EvmEvent
  token: Erc20
  from: Address
  to: Address
  amount: Amount
  pos: EvmPosition
}

// ERC20 events + states scoped to a single owner.
// Built from combinators - no bespoke implementation.
class OwnedErc20 {
  token: Erc20
  owner: Address

  // Internally: merge of two filtered transfer stores (from=owner ∪ to=owner).
  // Each underlying source fetches only its slice at the RPC level - this is not
  // a post-filter over all transfers.
  transfers: EvmHistStore<Erc20Transfer>

  mints: EvmHistStore<Erc20Mint>
  burns: EvmHistStore<Erc20Burn>

  // Reduced state stream. When iterating forward it's enough to obtain a state
  // once and apply transfer/mint/burn events. When iterating backward the
  // implementation fetches state from some blocks ago and reconstructs the range
  // forward - this avoids an eth_call per item. Reduced states are cached.
  balances: EvmHistStore<Erc20Balance>
}

// Reduced state - reconstructible from a past state + events. For Erc20 the
// reducer applies Transfer/Mint/Burn events. For complex contracts (AMM pools,
// lending protocols) the reducer can be arbitrarily expensive but still doable.
// Can be configured to periodically refetch on-chain state as a verification.
class Erc20Balance implements Positioned<EvmPosition> {
  erc20: Erc20
  owner: Address
  balance: Amount
  pos: EvmPosition
}

// Asset-aware fixed-point amount. Ensures same-asset operations only.
class Amount {
  raw: bigint
  decimals: number
  asset: Asset
  toString(): string
}


// ----- Examples -----

// Reorg-safe processing loop. Persists an anchor between runs.
async function processTransfersLoop(token: Erc20, owner: Address) {
  const transfers = token.ownedBy(owner).transfers
  const anchor: EvmPosition = loadAnchor() ?? transfers.earliest
  // Roll back reorged tail to the latest common ancestor on the canonical chain.
  const lca = await transfers.lca(anchor, transfers.safe.get())
  const head = await transfers.next()
  const [_, stale] = head.split(lca)
  for await (const t of stale.iter()) revertTransfer(t)
  // Replay forward, persisting the anchor as we go.
  for await (const t of watchHist(transfers, lca)) {
    await processTransfer(t)
    storeAnchor(t.pos.marshal())
  }
}

// Send a transfer on the source chain, wait for the corresponding bridge-in
// on the destination chain. Returns the destination transfer.
async function bridgeToken(
  source: Erc20, destination: Erc20, owner: Address, amount: Amount,
): Promise<Erc20Transfer> {
  const arrivals = destination.transfers({ to: owner })
  return actAndWait(
    arrivals,
    () => source.transfer({ from: owner, to: BRIDGE_ADDR, amount }),
    (t) => t.from === BRIDGE_ADDR && t.amount === amount,
  )
}

// Live balances across multiple tokens.
async function liveBalances(tokens: Erc20[], owner: Address) {
  const stores = tokens.map((t) => t.ownedBy(owner).balances)
  // mergeStores combines reactive stores into one stream of latest snapshots.
  for await (const balances of mergeStores(stores)) {
    for (const [token, b] of zip(tokens, balances)) {
      console.log(`${owner} ${token.name}: ${b.amount} ${token.symbol}`)
    }
  }
}
```

# Implementation layering

Live events must reach consumers in <10ms. Persistence runs behind that on its own schedule. Three layers:

```
┌──────────────────────────────────────────────────┐
│ consumer                                         │
│   for await item of watchHist(store, anchor)     │
├──────────────────────────────────────────────────┤
│ in-memory range index (LogIndex / per filter)    │
│   - BTreeMap<Position, Range>                    │
│   - Ranges hold either items or empty tombstones │
│   - fetchPromise per range = singleflight        │
│   - subscribe() inserts here synchronously       │
├──────────────────────────────────────────────────┤
│ durable backfill cache (LogStore / shared)       │
│   - SQLite: cached_logs, scanned_ranges,         │
│     reorged_blocks, block headers, erc20 meta    │
│   - consulted on cold miss + restart only        │
│   - written from a background drain task         │
├──────────────────────────────────────────────────┤
│ source (alloy Provider / CEX API)                │
│   - log filter watcher (websocket or poll)       │
│   - eth_getLogs for backfill                     │
└──────────────────────────────────────────────────┘
```

Key properties:

- **SQLite is never on the live-event path.** A subscribed log goes: watcher → LogIndex (synchronous BTree insert) → consumer wakeup. SQLite write is enqueued and drained asynchronously.
- **Scanned-ness is a first-class invariant.** Empty ranges (we scanned, found nothing) live alongside filled ranges in the same BTree. Iterating into an empty range yields nothing; iterating into a *missing* range triggers RPC. Pruning the in-memory index drops items but keeps the empty-range tombstone, so the gap-fill invariant survives memory pressure.
- **Aligned chunks.** RPC ranges are snapped to `batchSize` boundaries before being cached. Two consumers asking slightly different ranges hit the same cache entries; restart picks up cleanly.
- **Singleflight.** Each Range carries an optional `fetchPromise`. The first consumer hitting a cold gap inserts a placeholder and fires the RPC; concurrent consumers await the placeholder instead of firing duplicate RPCs.
- **Reorgs.** Logs are append-only. Reorged block hashes go into a side table; queries filter them out. Scanned ranges whose end_block_hash is reorged become invalid and get refetched. Compaction of scanned ranges only runs below the `safe` tip so a tip reorg can't invalidate a huge merged range.

Late-item insertion (a reconnect dump that overlaps cached history) follows four cases:
1. Item position in unscanned gap → insert filled range.
2. Item position equals existing empty range end → upgrade range to filled.
3. Item position inside existing range → split the range around the item.
4. Item position equals existing filled range end → duplicate, ignore.

These rules let a reconnection that overlaps with cached history glue back cleanly without losing or duplicating events.

# Combinators

Hists and HistStores compose. Each combinator preserves the `Hist`/`HistStore` interface, so layers stack arbitrarily.

```typescript
// Project each item through a transform. The wrapper preserves the source
// item internally so posAfter can reconstruct the upstream prefix.
function mapHistStore<Src, Dst, P extends Position>(
  store: HistStore<Src, P>,
  f: (item: Src) => Dst | Promise<Dst>,
): HistStore<MappedItem<Src, Dst>, P>

// Keep only items matching `pred`. Each yielded item carries the run of
// preceding filtered-out items on `.skipped`, so posAfter can reconstruct
// the upstream prefix.
function filterHistStore<T extends Positioned<P>, P extends Position>(
  store: HistStore<T, P>,
  pred: (item: T) => boolean,
): HistStore<FilteredItem<T>, P>

// Yield only items at or after `cut`. Implemented by pre-splitting every
// emitted Hist at the upstream level - this is what makes wrapping
// expensive mappers between sinceHistStore and source efficient: the mapper
// never sees pre-cut items.
function sinceHistStore<T extends Positioned<P>, P extends Position>(
  store: HistStore<T, P>,
  cut: P,
): HistStore<T, P>

// Merge N HistStores of the same item/position type into one.
// Items are interleaved by position order; duplicates at the same position
// are deduplicated naturally.
function mergeHistStores<T extends Positioned<P>, P extends Position>(
  sources: HistStore<T, P>[],
  cmp: (a: P, b: P) => number,
): HistStore<T, P>

// Cross-source merge: sources can have different item types and different
// position types. Output position is keyed by source name, so consumers
// can resume each source independently.
function mergeCrossSource<T>(
  sources: Record<string, HistStore<unknown, Position>>,
  project: (key: string, item: unknown) => T,
  cmp: (a: MergedItem<T>, b: MergedItem<T>) => number,
): HistStore<MergedItem<T>, MergedPos>

class MergedPos implements Position {
  // Keys are source names; values are marshalled per-source positions.
  positions: Record<string, bytes>
  marshal(): bytes
}

interface MergedItem<T> extends Positioned<MergedPos> {
  sourceKey: string
  item: T
}
```

Cross-source merge lets you compose, e.g., `EvmHistStore<Burn>` on Ethereum with `EvmHistStore<Mint>` on Polygon into one chronologically ordered stream. The keyed MergedPos round-trips through JSON and survives source additions/removals across restarts.

# Reducer

A Reducer is a *stateful, monotonic fold* over N source HistStores producing one output HistStore. The output position is a vector of source positions — one per input. Reduced state is cached; resolving state at an arbitrary position uses cost-based query planning (replay-from-floor vs. fetch-and-replay) over the in-memory cache, the reducer's external cache (e.g. `erc20_balances`), and the source's own log cache.

```typescript
interface Reducer<S, In, Out, P extends Position> {
  // Pure function. Called once per source event during forward iteration.
  apply(state: S, sourceIdx: number, event: In): S

  // Optional. Fetch state directly from the source (e.g. eth_call balanceOf)
  // at a given position. Used as a checkpoint when replay would be too expensive.
  fetchState?(pos: P): Promise<S>

  // Optional. Look up the closest cached state at or before `pos` from
  // a durable side cache.
  cachedStateFloor?(pos: P): { pos: P; state: S } | null
}

function reduce<S, In, Out, P extends Position>(
  sources: HistStore<In, P>[],
  reducer: Reducer<S, In, Out, P>,
  initial: S,
  project: (state: S, event: In) => Out,
): HistStore<Out, ReducerPos<P>>

// Vector of one source position per input.
class ReducerPos<P extends Position> implements Position {
  components: P[]
}
```

Use Reducer when the output is a function of the *cumulative history*: running balance, total volume, contract state. Don't use Reducer for problems where state items become obsolete and disappear from the output once consumed — use Matcher.

# Matcher

A Matcher is a *stateless, pure correlation* between two source HistStores (src and dst). Anchors are owned by the caller; `computeState(anchors)` reads each source's tail since its anchor and produces a snapshot of `{ matched, pending, orphans }`. There is no internal accumulating state — matched pairs disappear from the output once the caller advances both anchors past them.

```typescript
interface MatchInput<Src, Dst extends Positioned<Position>> {
  srcHist: Hist<Src, Position>
  dstHist: Hist<Dst, Position>
  srcItems: readonly Src[]
  dstItems: readonly Dst[]
  anchors: { src: Position; dst: Position }
}

interface MatchOutput<Src, Dst> {
  matched: readonly { src: Src; dst: Dst }[]
  pending: readonly Src[]   // src items with no dst match yet
  orphans: readonly Dst[]   // dst items with no src match
}

type MatchFn<Src, Dst, Out = MatchOutput<Src, Dst>> =
  (input: MatchInput<Src, Dst>) => Out | Promise<Out>

// Pure compute. No internal state. compute(anchors) is referentially transparent
// given fixed source tails. The wake signal merges both source tips so consumers
// recompute exactly when either side advances.
class BaseMatcher<Src, Dst extends Positioned<Position>, Out = MatchOutput<Src, Dst>> {
  constructor(
    srcStore: HistStore<Src, Position>,
    dstStore: HistStore<Dst, Position>,
    match: MatchFn<Src, Dst, Out>,
  )
  computeState(anchors: { src: Position; dst: Position }): Promise<Out>
  changes(): Store<unknown>
}

// Adds anchor persistence on top of BaseMatcher. Anchors are loaded lazily on
// first use, cached in memory, and persisted to the side cache on setAnchors.
class AnchoredMatcher<Src, Dst extends Positioned<Position>, Out = MatchOutput<Src, Dst>>
  extends BaseMatcher<Src, Dst, Out>
{
  currentState(): Promise<Out>
  iter(): AsyncGenerator<Out, never>
  setAnchors(next: { src: Position; dst: Position }): Promise<void>
  getHistory(since: Timestamp): Promise<Out>  // ad-hoc time-bounded query
}
```

Use Matcher when the output is a *current correlation* between two streams: bridge initiator ↔ completion, deposit ↔ withdraw, quote issued ↔ redeemed. Matched pairs are transient — they surface in the output once or a few times, then disappear once anchors advance past them. Unmatched items remain in `pending` / `orphans` until they get matched or until the operator decides to act on them (refund, escalate).

# Reducer vs Matcher — when to use which

| Property | Reducer | Matcher |
|---|---|---|
| Output | History of states, one per event | Current snapshot of correlation |
| State growth | Accumulates over time | Bounded by unmatched items |
| Sources | N (same or different P) | 2 (different P typical) |
| State backing | In-memory + durable side cache | Anchors only |
| Replay | Fetch + replay from checkpoint | Pure compute from src/dst tails |
| Position | Vector of source positions | Pair `{ src, dst }` of source positions |
| Example | Running balance | Bridge tracking |

Both ship; they're orthogonal abstractions, not alternatives.
