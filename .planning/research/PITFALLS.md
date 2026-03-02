# Domain Pitfalls: Blockchain Event Sourcing / State Reconstruction

**Domain:** Blockchain event sourcing, state reconstruction library (Rust, SQLite, EVM + Bitcoin + CEX)
**Researched:** 2026-03-02
**Confidence:** HIGH — well-established domain, pitfalls are battle-tested patterns from production indexers (The Graph, Ponder, reth, ethers-rs users, bridge post-mortems)

---

## Critical Pitfalls

These mistakes cause silent state corruption, data loss, or require full re-indexing from scratch.

---

### Pitfall 1: Premature Finality — Treating Unfinalized Blocks as Final

**What goes wrong:**
The library stores events from block N as "final", a consumer builds state on them, then a reorg replaces block N. The old events remain in the cache; the new canonical events are fetched but conflict. State diverges from chain truth silently — balances are wrong, bridge proofs reference non-existent txs.

**Why it happens:**
- EVM: `eth_getBlockByNumber("latest")` returns the current tip, which can be orphaned. Ethereum PoS soft-finality is ~12 slots (~144s); hard finality is 2 epochs (~12.8 min). Many devs assume "latest" = safe.
- Bitcoin: 1-conf blocks are routinely reorged; 6-conf is convention but not guaranteed.
- CEX timestamps: exchange REST APIs return "fill time" with no block concept; the same fill can be revised on reconnect.

**Consequences:**
- Balance tracker shows stale balance for minutes without error signal
- Bridge daemon submits proof for tx on orphaned block → proof rejected → bridge stuck
- Coverage ranges in SQLite record orphaned block as "covered" → gap recovery never re-fetches it

**Prevention:**
- Tag every stored event with a `finality` field: `Unfinalized | Probabilistic(confs) | Hard`.
- Maintain a "safe tip" cursor separate from "latest tip". The safe tip trails by N blocks (configurable per chain — 12 for ETH mainnet, 6 for Bitcoin).
- Never mark a coverage range as final until the block behind it clears the safe-tip threshold.
- The reorg notification path must fire before coverage is marked final.
- Store the block hash alongside every event row. On restart, re-validate that stored block hashes still exist on the canonical chain before trusting the cache.

**Detection (warning signs):**
- Your coverage table has blocks at `latest - 0` marked final
- Reorg tests are absent or only test single-block reorgs
- EventSource returns events without a `block_hash` field

**Phase mapping:** Core architecture (Phase 1 / cursor design). This cannot be bolted on later.

---

### Pitfall 2: Snapshot Taken Mid-Reorg

**What goes wrong:**
The FetchState primitive is called to get the initial snapshot. Between the moment FetchState is issued and the moment the subscription begins, a reorg happens that affects the blocks the snapshot is based on. The snapshot is stale at the moment of capture, but the library trusts it. Gap recovery fills from `snapshot_block + 1` forward, missing the reorg correction.

**Why it happens:**
Snapshot and subscription are not atomic — they can never be, because chains are external. The gap between them is the fundamental problem this library exists to solve. The pitfall is not recognizing when that gap straddles a reorg.

**Consequences:**
- Initial state is wrong; all subsequent Apply calls compound the error
- No error is surfaced — the library thinks state is consistent

**Prevention:**
- After fetching the snapshot, verify the anchor block hash is still canonical before trusting it: `eth_getBlockByHash(anchor_hash)` must return non-null.
- If the anchor hash is orphaned, re-fetch the snapshot (retry loop with backoff).
- Store the snapshot with both `block_number` AND `block_hash`. During gap recovery, validate the hash before replaying.
- Expose a `StateAnchor { block_number, block_hash, timestamp }` type — never let block_number alone be the anchor.

**Detection:**
- FetchState returns only a block number, not a hash
- Gap recovery logic doesn't re-validate the snapshot anchor after fetching

**Phase mapping:** Gap recovery implementation (Phase 2). Must be in the core gap recovery design, not an afterthought.

---

### Pitfall 3: Off-by-One Errors at Coverage Range Boundaries

**What goes wrong:**
The library stores coverage as `[from_block, to_block]` inclusive ranges. When stitching historical fetch + live subscription, the code fetches `[snapshot+1, sub_start]` to fill the gap. But the subscription delivers events starting at `sub_start`, causing double-processing of `sub_start`. Or the fetch is `[snapshot+1, sub_start-1]` and the subscription starts at `sub_start+1`, leaving `sub_start` as a permanent gap.

**Why it happens:**
- `eth_getLogs` range semantics: both `fromBlock` and `toBlock` are inclusive.
- WebSocket subscriptions: `eth_subscribe("newHeads")` delivers the *next* block after subscription, not the one at subscription time. If you subscribed at block 100, your first event is at block 101.
- Confusion about whether "last fetched" means "last fetched and applied" or "last fetched and stored."

**Consequences:**
- Double-application corrupts accumulated state (transfer history shows duplicate transfers)
- Gap in coverage causes missed events that are never detected (the coverage table says "covered")

**Prevention:**
- Define coverage semantics precisely and enforce them in one place: `coverage.contains(block)` checks `[from, to]` inclusive. All range math goes through this type.
- The subscription start cursor must be stored atomically with the last coverage range from historical fetch. Use a SQLite transaction: `INSERT coverage (from, to, hash) + UPDATE anchor` in one commit.
- Write property-based tests (proptest) that generate random `[snapshot_block, sub_start]` pairs and verify coverage is exactly contiguous with no gaps or overlaps.
- Log every range operation at DEBUG level with the full `[from, to]` bounds.

**Detection:**
- Range math is inline (`block + 1`, `block - 1`) scattered across multiple functions
- No test covering the exact boundary where historical fetch hands off to live subscription
- `eth_getLogs` calls use `toBlock: latest` instead of a specific block number

**Phase mapping:** Gap recovery (Phase 2). Property tests should be written before implementation.

---

### Pitfall 4: EVM logIndex Is Per-Block, Not Per-Transaction

**What goes wrong:**
Code sorts events by `(block_number, transaction_index, log_index)` but treats `log_index` as relative to the transaction. In EVM, `logIndex` in a log receipt is the index within the *entire block*, not within the transaction. Two logs from different transactions in the same block have monotonically increasing `logIndex` values. If you sort by `(block_number, tx_index, log_index)` you are sorting correctly (because logIndex already encodes the full block ordering). But if you truncate and re-index logIndex per-transaction, you break global ordering within a block.

**Why it happens:**
The EVM Yellow Paper specifies log indices as block-level, but the JSON-RPC field name `logIndex` is ambiguous. Many devs assume it's per-transaction because that would "make more sense."

**Consequences:**
- Events within a block are reordered, causing Apply to produce wrong state for intra-block dependencies (e.g., two ERC20 transfers in the same block affecting the same account)
- Rare but catastrophic — only triggers when multiple tracked events share a block

**Prevention:**
- Use `(block_number, log_index)` as the canonical EVM sort key. `log_index` is sufficient to order all events within a block without `tx_index`.
- Never re-number or re-index log positions.
- The `EventSource` trait should expose a `GlobalSequence` type that encodes `(block_number, log_index)` for EVM, validated in the EVM adapter.
- Add a test: two logs in the same block from different transactions — verify Apply receives them in log_index order.

**Detection:**
- Sort code uses `(block_number, tx_index, log_index)` with `log_index` reset per-tx
- No test for multiple events in the same block across different transactions

**Phase mapping:** EventSource trait design (Phase 1). The type system should make this impossible to get wrong.

---

### Pitfall 5: SQLite WAL Mode Busy Timeouts and Journal Growth

**What goes wrong:**
SQLite in WAL mode allows one writer and many readers. If the library's background writer thread and a caller of `At(cursor)` (reader) both open connections without setting `busy_timeout`, the reader gets `SQLITE_BUSY` immediately and panics or returns an error, breaking historical queries. Separately, WAL mode accumulates a WAL file that grows without bound if checkpointing is never triggered — common when write throughput is low but reads are frequent (WAL checkpoint requires a moment with no readers).

**Why it happens:**
- `rusqlite` default `busy_timeout` is 0ms — immediate failure on contention.
- WAL checkpoint is triggered by SQLite automatically at 1000 pages, but only if no readers are active. A long-running read transaction blocks checkpointing indefinitely.
- Applications often open one connection total, not understanding that WAL requires separate reader/writer connections for concurrency.

**Consequences:**
- Intermittent `SQLITE_BUSY` errors under concurrent access — hard to reproduce in tests
- WAL file grows to gigabytes over days, eventually exhausting disk
- On-disk state becomes unrecoverable without manual `PRAGMA wal_checkpoint(TRUNCATE)`

**Prevention:**
- Set `PRAGMA busy_timeout = 5000` (5 seconds) on every connection at open time.
- Use a connection pool with separate pools for readers and the single writer: `r2d2-sqlite` with `max_size: 1` for writer pool, higher for readers.
- Set `PRAGMA wal_autocheckpoint = 1000` and run explicit `PRAGMA wal_checkpoint(PASSIVE)` after each write batch.
- Cap reader transaction duration — `At(cursor)` queries must not hold a read transaction open across async awaits.
- In tests, use `journal_mode = DELETE` for simplicity unless WAL behavior is specifically being tested.

**Detection:**
- `busy_timeout` is 0 or not set
- `wal_checkpoint` is never called explicitly
- Connection pool has a single shared connection for both reads and writes
- WAL file size not monitored in production

**Phase mapping:** SQLite layer (Phase 2 / persistence). Must be correct from the first implementation — WAL bugs only appear under load.

---

### Pitfall 6: WebSocket Reconnection Race — Duplicate or Missed Events at Seam

**What goes wrong:**
A WebSocket subscription drops. The library reconnects and re-subscribes. During reconnection, blocks were produced. The library fetches missed blocks via `eth_getLogs` to fill the gap. But the new subscription starts delivering blocks from the moment of subscription — if the backfill and the subscription overlap, the same blocks are processed twice. If they don't overlap (backfill ends at block 100, subscription first delivers 102), block 101 is missed permanently.

**Why it happens:**
- The "last seen block" cursor and the "subscription resumed at" block are tracked separately and compared incorrectly.
- The reconnect backfill issues `eth_getLogs(from: last_seen+1, to: latest)` but `latest` has moved by the time the call returns.
- The subscription's first event block is not recorded at subscription time, only at first event delivery.

**Consequences:**
- Missed events are never detected — coverage table says "covered" but events at the seam are absent
- Duplicate events corrupt accumulated state

**Prevention:**
- At reconnection: (1) Note the `last_confirmed_block` before initiating subscription. (2) Subscribe. (3) Record the block number of the **first event delivered by the new subscription** (call it `live_start`). (4) Backfill `[last_confirmed+1, live_start-1]` via REST. (5) Apply backfill, then apply live events from `live_start` forward.
- This is identical to the initial gap recovery pattern — reuse the same code path.
- The subscription must buffer incoming events while backfill is in progress, not discard them.
- Implement a deduplication layer: before Apply, check if `(block_number, log_index)` already exists in the events table.

**Detection:**
- Reconnect code does not record the first block of the new subscription before backfilling
- No test simulating a reconnect at a specific block number and verifying no gap or duplicate
- Backfill and live subscription run concurrently without synchronization

**Phase mapping:** Subscription lifecycle (Phase 3). Test with a simulated WebSocket that drops mid-stream.

---

### Pitfall 7: Reorg Notification Is Insufficient Without Depth Information

**What goes wrong:**
The library detects a reorg and calls the client's reorg callback. The callback receives the new tip but not the depth of the reorg (how many blocks were replaced). The client cannot determine which previously "confirmed" state is now invalid. With only the new tip, the client cannot roll back to a safe point — it must either re-sync from genesis or trust that the reorg was shallow.

**Why it happens:**
The simplest reorg detection compares `latest_block_hash != parent_of_previous_tip`. This fires correctly but loses the divergence point.

**Consequences:**
- Client bridge daemon receives reorg notification but cannot identify which swap proofs are now invalid
- Library discards reorg depth information, forcing client to do a full RPC poll to discover the safe ancestor

**Prevention:**
- Reorg notification must include: `ReorgEvent { old_tip: BlockRef, new_tip: BlockRef, common_ancestor: BlockRef, depth: u32 }`.
- Finding `common_ancestor` requires walking back through stored block headers. Store block headers (number + hash + parent_hash) in SQLite for at least the last N blocks (N = max expected reorg depth, 128 for Ethereum).
- Never fire a reorg callback without a known `common_ancestor`. If you can't determine the common ancestor (e.g., headers weren't stored far enough back), fire a `DeepReorg` variant that signals "re-sync required."
- Coverage ranges that overlap `[common_ancestor+1, old_tip]` must be invalidated in the same transaction as the reorg event.

**Detection:**
- Reorg callback signature is `fn on_reorg(new_tip: BlockRef)` without depth/ancestor
- Block headers are not stored in SQLite — only event data is
- No test for 5+ block reorg

**Phase mapping:** Reorg detection (Phase 2 / core state machine). Designing the callback signature correctly in Phase 1 prevents API-breaking changes later.

---

### Pitfall 8: Non-Monotonic Cursors for CEX Events

**What goes wrong:**
For CEX (exchange) event sources, the library uses exchange-provided timestamps as cursors. Exchange timestamps are not monotonic: REST endpoints may return events out of order, clocks differ between exchange microservices, and historical backfill returns timestamps that overlap with live feed timestamps. Applying events by timestamp causes out-of-order Apply calls that corrupt state.

**Why it happens:**
Block numbers are a natural monotonic sequence for EVM/Bitcoin. CEX APIs have no equivalent. The `EventSource` trait is designed with block numbers in mind; CEX adapters shoehorn timestamps into the cursor type.

**Consequences:**
- A fill at timestamp T=100 is applied, then a fill at T=99 arrives from backfill → state goes backward
- Idempotency breaks: re-fetching a time range returns overlapping events because exchange APIs use half-open intervals inconsistently

**Prevention:**
- CEX adapters must use exchange-provided **sequence IDs or trade IDs** as cursors, never timestamps. Timestamps are metadata only.
- If an exchange provides no sequence ID, use a composite `(timestamp_ms, trade_id)` with explicit deduplication by `trade_id`.
- The `EventSource` trait cursor should be a generic `Cursor: Ord + Clone` — do not assume it's a block number.
- The gap recovery logic must use `>=` on the lower cursor bound with deduplication, not `>`, to handle half-open interval ambiguity.
- For backfill, always fetch one page *before* the desired start and deduplicate against already-stored events.

**Detection:**
- CEX adapter uses `timestamp` as the cursor type
- No test for out-of-order event delivery from CEX source
- Gap recovery uses `cursor > last_seen` without deduplication

**Phase mapping:** EventSource abstraction (Phase 1). The trait definition must accommodate CEX semantics from the start.

---

### Pitfall 9: Unbounded Event Buffer During Backfill

**What goes wrong:**
During initial historical replay or gap recovery, the library fetches all events in the gap and holds them in memory before beginning Apply. For a large gap (e.g., re-syncing from block 0 for an active contract), this is millions of events and gigabytes of heap allocation. The process OOMs or swaps, slowing to a crawl.

**Why it happens:**
The intuitive implementation of "fetch all events, then apply all events" is correct for small gaps. Nobody tests with a 10M-block gap.

**Consequences:**
- OOM crash during initial sync
- If not OOM, GC/memory pressure causes latency spikes on the hot path

**Prevention:**
- Gap recovery must be streaming: fetch events in pages (e.g., 10,000 blocks per `eth_getLogs` call), apply each page, commit coverage, then fetch the next page.
- The event buffer between fetch and Apply must have a bounded capacity (`tokio::sync::mpsc` with `buffer: 1024` or similar). Backpressure propagates to the fetcher.
- Snapshot accumulation: take a new snapshot every N events applied (e.g., every 50,000 events) during backfill to checkpoint progress. On restart, resume from the most recent snapshot rather than from the original anchor.
- Never call `collect::<Vec<_>>()` on an event stream during gap recovery.

**Detection:**
- Gap recovery code calls `eth_getLogs` once with `from: snapshot_block, to: current_tip`
- No upper bound on the range passed to `eth_getLogs` (Infura/Alchemy cap this at 2,000-10,000 blocks)
- No streaming/pagination in the fetch path

**Phase mapping:** Gap recovery and backfill (Phase 2). Performance test with a 1M-block synthetic gap before shipping.

---

### Pitfall 10: SQLite Write Amplification From Per-Event Transactions

**What goes wrong:**
Each event is inserted in its own SQLite transaction (or each `Apply` call triggers a commit). At 100+ events/second, this is 100+ `fsync` calls per second. SQLite in WAL mode serializes writes through the write-ahead log; each commit flushes the WAL. This saturates disk I/O on spinning disks and causes measurable latency on SSDs.

**Why it happens:**
The simplest correct code commits after every insert. Batching is an optimization that's deferred and then forgotten.

**Consequences:**
- Write throughput limited to ~100-300 events/second on a local SSD, ~20-50 on spinning disk
- Latency for the writer thread blocks the read path due to WAL lock contention

**Prevention:**
- Batch all events from one fetched page into a single SQLite transaction. One transaction per `FetchEvents` page, not per event.
- Use prepared statements, not ad-hoc SQL strings, for event inserts.
- Use `PRAGMA synchronous = NORMAL` (not FULL) in WAL mode — WAL mode provides crash safety without `synchronous = FULL`.
- Set `PRAGMA cache_size = -65536` (64MB page cache) to reduce redundant reads on replay.
- For accumulated state (transfer history), use `INSERT OR IGNORE` rather than checking existence first.

**Detection:**
- Event insert loop has `connection.execute("BEGIN")` and `connection.execute("COMMIT")` inside the loop
- `PRAGMA synchronous` is not set explicitly (defaults to FULL, unnecessary in WAL mode)
- No prepared statement reuse for hot-path inserts

**Phase mapping:** SQLite schema and write path (Phase 2). Measure write throughput before declaring the persistence layer done.

---

### Pitfall 11: Snapshot Accumulation Without Pruning

**What goes wrong:**
The library takes periodic snapshots to speed up restart. It never prunes old snapshots. After weeks of operation, the snapshots table contains thousands of rows, each a full serialized state blob. The SQLite file bloats. On restart, the "load most recent snapshot" query scans a large table. Worse, old snapshots reference coverage ranges that have been superseded, creating confusion during debugging.

**Why it happens:**
Snapshot writes are easy; snapshot pruning requires a retention policy decision that's deferred indefinitely.

**Consequences:**
- SQLite file grows by `snapshot_size * snapshot_frequency` unboundedly
- VACUUM is never run; deleted rows don't reclaim space without it
- Historical `At(cursor)` queries accidentally find stale snapshots

**Prevention:**
- After writing a new snapshot, delete all snapshots older than the second-most-recent. Keep only 2-3 snapshots (most recent + one prior for safety).
- Run `PRAGMA auto_vacuum = INCREMENTAL` and call `PRAGMA incremental_vacuum(N)` after each pruning pass.
- Snapshots table schema must include `created_at`, `block_number`, and `block_hash` for proper ordering and validation.
- Test: after 1,000 snapshot cycles, assert the snapshots table has <= 3 rows.

**Detection:**
- No DELETE from snapshots table anywhere in the codebase
- Snapshots table has no retention policy
- `auto_vacuum` is not enabled

**Phase mapping:** Persistence layer (Phase 2), but must be designed into the schema from the start.

---

### Pitfall 12: Testing Reorgs Without a Real Chain Simulator

**What goes wrong:**
Reorg handling is tested with hand-crafted mock data: "here's block 100A, now here's block 100B." This misses the temporal sequence: reorgs are discovered by polling or subscription, not by receiving a "reorg happened" signal directly. Tests that bypass the discovery mechanism don't exercise the detection logic, only the handling logic.

**Why it happens:**
Real reorg simulation requires either a live devnet (Anvil/Hardhat) or a mock `eth_getBlockByNumber` that returns different blocks on different calls. The latter requires stateful mocks, which are more complex than simple fixtures.

**Consequences:**
- Reorg detection bugs only surface in production
- The reconnect-at-reorg scenario (WebSocket drops during a reorg) is never tested

**Prevention:**
- Build a `MockChain` test harness that can:
  1. Advance block number
  2. Fork at a given block (replace blocks above fork point with different hashes)
  3. Deliver the fork to subscribers via the `EventSource` trait
- The `EventSource` trait must be designed for testability: accept `Box<dyn ChainClient>` or use generics so tests can inject `MockChain`.
- Test matrix: 1-block reorg, 3-block reorg, reorg exactly at finality boundary, reorg during backfill.
- Use Anvil (from the Foundry suite) for integration tests that need real EVM behavior.

**Detection:**
- Reorg tests use pre-constructed fixture data, not a simulated chain that can fork dynamically
- `EventSource` implementation is not generic over the chain client — can't inject mocks
- No test for reconnection during a reorg

**Phase mapping:** Phase 1 (design `EventSource` for testability) and Phase 3 (write the reorg test suite).

---

## Moderate Pitfalls

---

### Pitfall 13: Missing `block_hash` in Event Storage

**What goes wrong:**
Events are stored with `block_number` only. After a reorg, you cannot distinguish which events came from the orphaned chain and which from the canonical chain without re-fetching from the RPC.

**Prevention:**
- Store `(block_number, block_hash, tx_hash, log_index)` for every event. On reorg, invalidate all events where `block_number >= reorg_depth AND block_hash != canonical_hash`.
- The canonical hash is verified by calling `eth_getBlockByNumber(block_number, false)` and comparing hashes.

**Phase mapping:** Schema design (Phase 1/2).

---

### Pitfall 14: `eth_getLogs` Response Size Limits From RPC Providers

**What goes wrong:**
Infura, Alchemy, and most public RPC providers cap `eth_getLogs` responses at 10,000 logs or 2,000 blocks per request. Requests exceeding these limits return an error, not a truncated result. Code that issues a single `eth_getLogs` call for a large range silently fails or panics on the error.

**Prevention:**
- Always implement binary search pagination for `eth_getLogs`: if a range returns an error, split it in half and retry each half.
- Log the actual range sizes being fetched at TRACE level for debugging.
- Test against a mock RPC that returns errors for ranges > N blocks.

**Phase mapping:** EventSource EVM adapter (Phase 2).

---

### Pitfall 15: Apply Is Not Pure — Side Effects in the Reducer

**What goes wrong:**
The client's `Apply(state, event) -> state` function makes RPC calls, writes to a database, or mutates shared state. This breaks the library's ability to replay events (replay would re-execute the side effects), take snapshots (snapshot doesn't capture side-effect state), and test correctness (tests must mock the side effects).

**Prevention:**
- Enforce purity at the trait level: `Apply` takes `&mut Self` (state) and `&Event` and returns nothing beyond state mutation. No async in Apply.
- Document this constraint prominently: "Apply must be a pure function of (state, event). All side effects belong in the OnChange callback."
- Add a linting note: if Apply signature includes `async`, reject it at compile time.

**Phase mapping:** Core trait design (Phase 1).

---

### Pitfall 16: State Deserialization Breaking on Schema Evolution

**What goes wrong:**
Snapshots are serialized with `serde` + `bincode` or `postcard`. The state struct gains a new field. Old snapshots stored in SQLite cannot be deserialized into the new struct. On restart, the library fails to load the snapshot and either panics or falls back to full re-sync.

**Prevention:**
- Use `serde` with `#[serde(default)]` on all new fields so old snapshots remain deserializable.
- Store the snapshot schema version alongside each snapshot row. On mismatch, discard the snapshot and re-sync (with a warning log, not a panic).
- Consider using JSON or a self-describing format (not bincode) for snapshots to allow schema evolution. Accept the size tradeoff for correctness.
- Treat snapshot format as a versioned public interface with migration support.

**Phase mapping:** Persistence layer (Phase 2).

---

### Pitfall 17: Cursor Comparison Across Sources With Different Semantics

**What goes wrong:**
The `EventSource` trait uses a generic `Cursor` type, but the code has a hidden assumption that cursors from different sources are comparable. A bridge daemon tracking events from Ethereum (cursor = block number) and a CEX (cursor = trade ID) tries to determine "which is newer" — the comparison is meaningless.

**Prevention:**
- Make `Cursor` source-scoped. Each `EventSource` instance produces cursors that are only comparable to other cursors from the same instance.
- The library's internal state machine should never compare cursors from different sources.
- If a use case requires "time-aligning" events from multiple sources, that is the client's responsibility via the `Apply` function, not the library's.

**Phase mapping:** EventSource trait design (Phase 1).

---

## Minor Pitfalls

---

### Pitfall 18: VACUUM Blocking Writers

**What goes wrong:**
SQLite `VACUUM` requires an exclusive lock and cannot run concurrently with any other connection. Running `VACUUM` on a large database blocks the write path for seconds.

**Prevention:**
- Use `PRAGMA auto_vacuum = INCREMENTAL` + `PRAGMA incremental_vacuum(N)` (small N per call) in the write path instead of full `VACUUM`.
- Never run full `VACUUM` in the hot path. Schedule it only at startup before the event processing loop begins.

---

### Pitfall 19: Tokio Runtime Conflicts With Blocking SQLite Calls

**What goes wrong:**
`rusqlite` is synchronous. Calling it from an async Tokio task without `spawn_blocking` blocks the Tokio thread pool, stalling all other tasks on that thread.

**Prevention:**
- All SQLite operations must go through `tokio::task::spawn_blocking`. Use `tokio-rusqlite` crate which provides the async wrapper.
- Alternatively, dedicate a single non-Tokio thread to all SQLite operations and communicate via channels.

---

### Pitfall 20: Logging at Wrong Granularity

**What goes wrong:**
DEBUG logs for every event (millions/day) fill disks. No logging on gap recovery start/end makes production debugging impossible.

**Prevention:**
- ERROR: schema migrations, reorgs, gap recovery failures, snapshot failures
- WARN: RPC errors (with retry), WAL growth above threshold
- INFO: startup (snapshot loaded, coverage ranges), gap recovery start/end, reorg detected
- DEBUG: individual fetched ranges, coverage range writes
- TRACE: individual events applied

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| EventSource trait design | CEX cursor semantics forced into block number type | Define `Cursor: Ord + Clone` generic from day one |
| EventSource trait design | Apply not enforced as pure | Make Apply synchronous at the trait level |
| EventSource trait design | logIndex misunderstood | Validate EVM ordering in adapter tests |
| Gap recovery design | Off-by-one at historical/live seam | Property tests before implementation |
| Gap recovery design | Snapshot anchor not hash-validated | Require block_hash in StateAnchor type |
| Gap recovery implementation | eth_getLogs range too large | Binary search pagination from first implementation |
| Gap recovery implementation | Unbounded memory on large gaps | Streaming/paged fetch with backpressure |
| SQLite layer | Busy timeout = 0 | Set PRAGMA busy_timeout=5000 at connection open |
| SQLite layer | Per-event transactions | Batch per fetch page, not per event |
| SQLite layer | WAL growth without checkpoint | Explicit checkpoint after write batches |
| SQLite layer | Snapshot bloat | Retention policy: keep 2-3 snapshots max |
| Reorg detection | Reorg depth not communicated | ReorgEvent must include common_ancestor |
| Reorg detection | Block headers not stored | Store headers for last 128 blocks (ETH) |
| Reorg detection | No test harness | Design EventSource for mock injection from Phase 1 |
| Subscription lifecycle | Reconnect gap or duplicate at seam | Reuse gap recovery path for reconnection |
| Subscription lifecycle | First event block not recorded | Record sub_start before backfill begins |
| Finality | Unfinalized blocks marked as final | Separate safe-tip cursor from latest-tip cursor |
| Finality | Snapshot on orphaned block | Re-validate anchor hash after FetchState |

---

## Sources

**Confidence: HIGH** — This pitfall catalogue is drawn from:

- Production post-mortems in the EVM indexer ecosystem (The Graph, Ponder, ethers-rs issue trackers through Aug 2025)
- SQLite WAL mode documentation and `rusqlite` codebase behavior (well-documented and stable)
- Ethereum JSON-RPC specification for log ordering semantics (EIP-3 and Yellow Paper, stable)
- Bridge daemon failure patterns at Symbiosis (direct operational context from PROJECT.md)
- Tokio async/blocking interaction patterns (stable, documented in Tokio book)

No external web searches were possible in this research session. All findings are based on well-established, stable knowledge in the blockchain infrastructure and SQLite domains. Claims about RPC provider limits (Infura/Alchemy 2,000-10,000 block caps) should be verified against current provider documentation before implementation.
