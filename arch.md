chainfold allows it's clients to:
 - read history of chain/cex events with caching - i.e. given time range first time it fetches events from source (EVM RPC or CEX API), and afterward the same time range is read from cache only - no new remote requests.
 - automatically updates caches - if requested range is only partially cached then it deduces what ranges are missing and fetches them only
 - handles conenctions/reconnections - client just receives events sooner or later, all connection-related stuff is handled by chainfold. No network errors is ever returned to client.
 - allows to read event histories incrementally - when client requests range (2020-01-01; 2026-01-01) it doesn't wait a month until the lib fetches gazillion of events, it returns an object that represents the history requested events in the specified range. Client can iterate over it, obtain positions while iterating, compare positions in two histories, etc.
 - allows to subscribe to new events and receive them instantly - i.e. in a 1-2 ms - since the moment they are received through RPC/API
 - allows to subscribe to events beginning at some point in the past, like from (2026-01-01; unbounded). That means that library returns iterator that yield events starting at some point in the past, and then, when it yielded last existing event, it waits until upstream (RPC or API) notifies about new event and then yields it to client.
 - allows to specify ranges for histories not only as timestamps but as chain-specific Positions, like (block_number, log_index) for EVM, or (block_number, tx_index, input_index) for BTC. These Positions can be extracted from Cursors returned by range requests or subscriptions, persisted and used later to request another History ranges. That will allow to continue processing from the positions of last stop.
 - allows not only to obtain event Histories, but also StateHistories. States are like events - have a Position and some data - but they are just constructed (reduced) from events. Typical examples of States are Balances (e.g. History of Balances of owner of ERC20 token), or Transfers (e.g. History of Transfers - meaning each item of the History is a list of all past Transfers of specified account). To avoid huge memory copying on each new event each Transfers object actually is just a pointer to a linked list of Transfer objects, so History effectively a list of such pointers (i.e. a linked list too). It's just not a "regular" linked list with c-style memory pointers - it's pointers to States or Events that could be not yet fetched, i.e. are lazy-fetched from the event source (EVM RPC or CEX api).
 - States are cached too much like Events.

In the end this library should allow client to work with chains like they work with a local full-node, but without the need to download full blockchain, because usually app needs to work only with a small set of accounts/contracts/bitcoin addresses and full local blockchain would be an overkill for that.

Typescript-like pseudocode:

```lang=typescript

// Each history/chain implements it's own Position.
// Position is a separator of History - it does not point to specific Event or State (this is Cursor's job),
// and it's not a pointer to a Cursor (because e.g. empty History can have no Cursors, but still has Positions).
// Position just divides History to two parts - before and after - that can be iterated over. One of part can be emtpy - in this case Position points to beginning or the end of the history.
// Implementations will usually store positions as pointers to an event (like a Cursor) but with a field specifying if it's just before the event or right after the event.
// That means that between two adjacent Cursors there always are 2 positions - one is just after the former and one is just before the latter.
// This allows to include/exclude the source event (i.e. Cursor) when querying using the Position later.
// Positions are bound to the History - i.e. you can't use position from one History on another History.
// Positions are not always ordered (because of reorgs), and generally can only be compared for equivalence using isEquiv().
interface Position {
  history: History
  // Shorthand for p.history.before(p) - returns Cursor just before this
  async before(cutoff: Position = this.history.earlist): Promise<Cursor|null>
  // Shorthand for p.history.after(p) - returns Cursor just after this
  async after(curoff: Position = this.history.never): Promise<Cursor|null>

  // Shorthand for this.history.isEquiv(this, p)
  async isEquiv(p: P): Promise<boolean>

  // Shorthand for this.history.marshal(this)
  marshal(): bytes
}

// Cursor points to an item of type T
// Cursors and Positions work in pair and usually implemented around the same underlying data.
// They are distinct because Positions are needed to produce ranges of Histories (represented by Cursors) and
// also Positions can be marshalled/unmarshalled to be restored after process restart.
class Cursor<T, P extends Position> {
  // Event or State this Cursor points to
  value: T
  // Link to parent History
  history: History<T, P>
  // Position just before or after the Event or State occurred - i.e. History.after(Cursor.before) returns the same Cursor
  before: P
  // Position just after this Cursor - i.e. History.before(Cursor.after) returns the same Cursor
  after: P
  // Returns Cursor to a previous element in history. If element is cached - returns cached version. If not - fetches next batch of history, caches it and then returns.
  // Shorthand for c.history.before(c.before)
  async prev(): Promise<Cursor>
  // Returns Cursor to a next element the same as for prev(), but also waits for a new element to arrive if we are already at a tip (end of History).
  // In case of reconnection still iterates over all missing elements while being disconnected.
  // Shorthand for c.history.after(c.after)
  async next(): Promise<Cursor>

  // Returns true if this Cursor is after the Position p - i.e. in a prev-chain (this, this.prev(), this.prev().prev(), ...) there exists a Cursor p.after()
  // Note that this is not the actual implementation algorithm, as this can be calculated much faster if we take into account all non-canonical chains (which are usually absent) and just compare positions.
  async isAfter(p: P): Promise<boolean>

  // Returns true if this Cursor is before the Position p - i.e. in a next-chain (this, this.next(), this.next().next(), ...) there exists a Cursor p.before()
  async isBefore(p: P): Promise<boolean>
}

// Store - inspired by React/Svelte/Zustand stores, but even simpler.
class Store<T> {
  subscribe(): AsyncGenerator<T>
}

class History<T, P extends Position> {
  // This store updates as new events arrive from upstream (EVM RPC, CEX API) - event or state could still be in mempool
  latest: Store<Position>
  // This store updates as events/states being mined/added to a block.
  best: Store<Position>
  // This store updates as blocks with corresponding events/states become finalized - no reorg could happen
  safe: Store<Position>
  // All 3 stores could be effectively the same for CEXes

  // Position to obtain the earliest Cursor in history (via h.after(h.earliest)). .earliest.before() is always null.
  // This Position can be used as a default one when iterating over all the History.
  earliest: P

  // Position such that for any cursor c c.after().isEquiv(c.history.never) == false. .never.after() is always null.
  never: P 

  // Return position at a specific point in time in a canonical chain - usefull for cutoffs in after()/before().
  at(time: Timestamp): P

  // Returns true if two Positions a and b are equivalent - meaning that a.after() === b.after() and a.before() === b.before() (including nulls)
  async isEquiv(a: P, b: P): Promise<boolean>

  // Returns latest common ancestor.
  // Common ancestor is a such Position that it's equivalent to one of {a.before().after(), a.before().prev().after(), a.before().prev().prev().after(), ...}
  // - i.e. is among all positions going back from a (including a) - and at the same time equivalent ot the same set for b.
  // Latest common ancestor is a common ancestor for with a.after() is null or a.after().after() is not a common ancestor - i.e. it's the latest.
  // LCA is useful to rollback stale (reorged) chain and start iterating forward on a canonical chain.
  // Note, that LCA always exists because .earliest is a common ancestor for all positions.
  async lca(a: P, b: P): Promise<P>

  // Returns Cursor to a first item after specific Position, but not later than cutoff.
  // Returns null if there is no such cursor.
  async after(position: P, cutoff: P = History.never): Promise<Cursor<T, P>|null>

  // Returns Cursor to a first item before specific Position, but not earlier than cutoff.
  // Returns null if there is no such cursor.
  async before(position: P, cutoff: P = History.earliest): Promise<Cursor<T, P>|null>

  // For save/load functionality
  unmarshal(data: bytes): P
  marshal(position: P): bytes
}

// Represents EVM chain
class EvmChain {
  blocks: History<Block>
  transactions: History<Transaction>
  async contract(address: Address): Promise<EvmContract>
  async erc20(address: Address): Promise<Erc20>
}

class EvmPosition implements Position {
  get time() { ... }
  blockNumber: bigint
  logIndex: number // index of event inside block
  blockHash: Hash // In case of reorg blockNumber and logIndex can be the same for different block - this field allows to distinguish
}

type EvmCursor<T> = Cursor<T, EvmPosition>
type EvmHistory<T> = History<T, EvmPosition>

type ContractState = bytes

class EvmContract {
  address: Address
  chain: EvmChain

  // Generic contract states - plain bytes
  states: EvmHistory<ContractState>

  // Usefull if we want to iterate over contract states/events starting from it's creation
  createdAt: EvmPosition
}

interface Asset {
  symbol: string // like USDC, USDT, WBTC etc.
  name: string
}

// Represents ERC20 token
class Erc20 extends EvmContract implements Asset {
  symbol: string
  name: string
  decimals: number

  // These Histories match ERC20 signatures
  transfers(from?: Address, to?: Address): EvmHistory<Erc20Transfer>
  mints(to: Address): EvmHistory<Erc20Mint>
  burns(from: Address): EvmHistory<Erc20Burn>

  // Return OwnedErc20 object. For each owner can exist only one OwnedErc20 object, and it's returned every time.
  ownedBy(owner: Address): OwnedErc20
}

class EvmEvent {
  contract: EvmContract
  blockNumber: bigint
  // Timestamp of the block
  timestamp: Timestamp
  blockHash: Hash
  logIndex: number
  txId: TxId
  args: EvmEventArgs
}

class Erc20Transfer {
  event: EvmEvent
  token: Erc20
  from: Address
  to: Address
  amount: Amount
}

// Represents part of ERC20 events/states related to a specific owner
class OwnedErc20 {
  token: Erc20
  owner: Address

  // Combines 2 Histories - transfers in and out
  transfers: EvmHistory<Erc20Transfer>

  mints: EvmHistory<Erc20Mint>
  burns: EvmHistory<Erc20Burn>

  // Reduced states. When iterating forward it's enough to obtain a state one time and then apply transfer/mint/burn events.
  // When iterating backward implementation needs to obtain state from some blockrange ago, then apply all events from that range effectively reconstructing part of History
  //  - this is needed to avoid fetching state on each Cursor iteration.
  // Already reduced states are stored in cache.
  balances: EvmHistory<Erc20Balance>
}

// Reduced state - it can be directly queried from blockchain but also can be reconstructed from a past state + set of events occured afterwards.
// For Erc20 it's enough to apply Transfer/Mint/Burn events.
// For complex contracts like AMM Pools there could be much more computation, but still doable.
// Can be configured to periodically refetch state from blockchain even when not needed just to ensure that state reduction works correctly.
class Erc20Balance {
  erc20: Erc20
  owner: Addresss
  balance: Amount
}

// Abstracts-out fixed-point logic, but also ensures that only same-asset Amounts can be added/subtracted
class Amount {
  raw: bigint
  decimals: number
  asset: Asset
  toString(): string // returns amount in human-readable form
}


// Example code that processes all transfers to/from an address
async function processTransfersLoop(token: Erc20, owner: Address) {
  const owned = token.ownedBy(owner)
  const anchor = loadAnchor()
  let pos: Position = owned.transfers.unmarshal(anchor)
  const lca: Position = await owner.transfers.lca(startPos, owner.transfers.latest)
  // Rollback reorged transfers
  for (let curs = await pos.before(); await curs.before.isEquiv(lca); curs = await curs.prev()) {
    revertTransfer(curs.value)
  }
  // Go forward processing transfers
  for (let curs = await lca.after(); ; curs = await curs.next()) {
    await processTransfer(curs.value)
    // Store anchor so that in case of process stop/crash we continue from the position we've stopped
    storeAnchor(curs.after.marshal())
  }
}

async function doAndWaitFor<T, P: Position>(gen: AsyncGenerator<P>, action: async () => Promsie<void>, condition: (t: T) => bool): Promise<T> {
  let pos = await gen.next() // first next() call always returns current state, in our case - Position
  await action()
  for (let curs = await pos.after();; curs = await curs.next()) {
    if (condition(curs.value)) return curs.value
  }
}

// This example sends a bridge-out operation and waits for bridge-in on other side (chain). Returns erc20 transfer on destination
async function bridgeToken(source: Erc20Token, destination: Erc20Token, owner: Address, amount: Amount): Promise<Erc20Transfer> {
  const gen = destination.transfers({to: owner}).latest()
  return await doAndWaitFor(gen, () => source.transfer({from: owner, to: BRIDGE_ADDR, amount}), t => t.from === BRIDGE_ADDR && t.amount === amount)
}


// This example just shows current balances
async function balances(tokens: Erc20[], owner: Address) {
  const balanceGenerators = tokens.map((t) => ([t, t.ownedBy(owner).balances.tip.subscribe()]))
  async for (const [token, balance] of combineGenerators(balanceGenerators)) {
    console.log(`balance of ${owner} ${token.name}: ${balance} ${token.symbol}`)
  }
}
```

Implementation recommendation: store in cache not only events/states, but also empty ranges between them.

## Reducer architecture

A Reducer is the general-purpose combinator that consumes N source Histories and produces an output History. Merge (interleaving N streams) is a special case of Reducer with no state and identity apply.

### Core insight: vector positions

A Reducer's output position is a **vector of N source positions** — one per input History. This captures exactly "I've consumed up to here in each source." Benefits:

- **Deterministic ordering**: no timestamp ambiguity, no tiebreakers needed. The position encodes exactly which events have been consumed.
- **Resumability**: marshal/unmarshal the vector, resume each source from its last position.
- **Cross-chain support**: sources can have completely different position types (EvmPosition on Ethereum vs EvmPosition on Polygon vs CexPosition on Binance). Each component of the vector is typed independently.
- **`at(time)` bridges**: delegate to each source's `at(time)`, assemble the vector. This maps wall-clock time back to concrete positions across heterogeneous chains.

### Comparison

Position A is "before" position B if the reducer produced A before B — the reducer imposes a total order on its output, and the vector is the proof. For partial comparison (e.g. "has source 2 advanced?"), compare individual components.

### Reducer vs Merge

```
Reducer<S, O> {
    sources: [History<I_0, P_0>, History<I_1, P_1>, ..., History<I_n, P_n>]
    state: S
    apply(state: &S, source_idx: usize, event: I_k) → S
    output: History<O, ReducerPosition<P_0, P_1, ..., P_n>>
}

// Merge is a Reducer where:
//   S = ()
//   apply((), source_idx, event) = event  (pass-through)
//   O = enum of source event types (or unified type if all sources share one)
```

### Advancing

The reducer's `after()` implementation:
1. Query each source for its next event after the corresponding component position
2. Pick the globally earliest (by timestamp, with source_idx as tiebreaker)
3. Call `apply(state, source_idx, event)` to produce new output state
4. Return cursor with new state and updated vector position

### Example: crosschain bridge state

```
Sources:
  [0] = EvmLogHistory on Ethereum (Burn events)
  [1] = EvmLogHistory on Polygon (Mint events)

State = Map<BridgeId, BridgeStatus>

apply(state, 0, BurnEvent) → insert(bridge_id, Pending { amount, recipient })
apply(state, 1, MintEvent) → update(bridge_id, Completed { ... })

Output: History<Map<BridgeId, BridgeStatus>, ReducerPosition<EvmPosition, EvmPosition>>
```

### Example: OwnedErc20 transfers (merge)

```
Sources:
  [0] = EvmLogHistory for Transfer(from=owner, *, *)  — outgoing
  [1] = EvmLogHistory for Transfer(*, to=owner, *)    — incoming

// Merge (identity reducer, no state)
apply((), _, transfer) = transfer

Output: History<Erc20Transfer, ReducerPosition<EvmPosition, EvmPosition>>
```

Both sources are on the same chain and share the same position type, but the vector still has two components because they track independent cursors into independent cached histories.
