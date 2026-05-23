# Chainfold refactoring plan

> **Status as of 2026-05-23.** Phases 0–6 shipped; the post-Phase-5 OwnedErc20
> follow-up also shipped. Phase 7 (pruning + best/safe wiring) is the next
> eligible chunk. Currently-deferred follow-ups: real bridge decoder for
> `bridge_matcher`, same-`P` typed merge alias, `Reducer` rebuild.
> See [`arch.md`](./arch.md) for the *shipped* design and
> `~/.claude-personal/projects/-home-nb-src-chainfold/memory/` for per-phase
> notes (`phase5_merge.md`, `phase6_matcher.md`, etc).
>
> | Phase | Status | Lib tests | Integration | Notes |
> |---|---|---|---|---|
> | 0 — Housekeeping + CCPs | ✅ Done (2026-05-21) | — | anvil_smoke, mock_provider_smoke | Dead pre-arch code deleted; anvil + mock harnesses landed. |
> | 1 — LogStore + LogIndex | ✅ Done (2026-05-21) | 41 (26 + 15) | phase1_layering (#[ignore]) | Layered cache split; 98.5× warm-vs-cold on fork. |
> | 2 — Push subscription | ✅ Done (2026-05-22) | +1 | phase2_push (3 × #[ignore]) | WS latency measured ~3 ms on local anvil. |
> | 3 — Hist / HistStore split | ✅ Done (2026-05-22) | 64 | phase3_hist_store (9) | Full cleanup (no facade); `Cursor`/`History` removed. |
> | 4 — map / filter / since | ✅ Done (2026-05-22) | 76 (+12) | phase4_combinators (6) | Pure proxies; no I/O. |
> | 5 — Heterogeneous merge | ✅ Done (2026-05-22) | 84 (+8) | phase5_merge (8) | `MergedPos`, `ErasedHistStore`; clamp_pos override. |
> | 5b — OwnedErc20 follow-up | ✅ Done (2026-05-23) | 95 (+2) | owned_erc20 (2 × #[ignore]) | Two narrow filters + merge; `erc20_explorer --owner` rewired. |
> | 6 — Matcher | ✅ Done (2026-05-23) | 95 (+9) | phase6_matcher (6) | `BaseMatcher` / `AnchoredMatcher` / `AnchorStore` (in-mem + SQLite). |
> | 7 — Pruning + best/safe + facade removal | ⏳ Pending | — | phase7_soak (planned) | Not started. |
> | Bridge decoder | ⏳ Deferred | — | — | Generic `--match-on` extractor ships; ABI-aware impls per bridge are app-specific. |
> | Same-`P` merge alias | ⏳ Deferred | — | — | Not load-bearing; `MergedItem.source_key` is useful as-is. |
> | Reducer | ⏳ Deferred (Phase 6 spec) | — | — | Sibling of Matcher; `Erc20Balance` rebuild gates on it. |
>
> **Test totals after Phase 6 + OwnedErc20:** 95 lib + 31 active integration (anvil_smoke + mock_provider_smoke + 9 phase3 + 6 phase4 + 8 phase5 + 6 phase6) = 126 active, 7 ignored (anvil/fork-only).

---

A phased refactor aligning chainfold with the reference implementation's production-validated
layering. Built on three locked-in structural shifts:

1. **Cache layering** — in-memory hot path above SQLite backfill. Mirrors
   the reference implementation's `RangeTree` over `SqliteStore`. SQLite never sits on the
   live-event critical path.
2. **Snapshot + tip-stream split** — `Hist<T,P,A>` (finite, immutable
   snapshot) and `HistStore<T,P,A>` (yields fresh `Hist` as tip advances).
   This is proven in the reference implementation and locked in.
3. **Two reducer shapes** — keep `Reducer` (monotonic fold over one
   stream) and add `Matcher` (pure correlation over src + dst tails). They
   address different problems; both stay.

## Reference reading for agents (read before touching code)

Project context:
- `arch.md` — canonical vision; this plan realizes parts of it
- `src/arch.rs` — current public API (Position, Cursor, History, Store, HistoryBackend)
- `src/merge.rs`, `src/reduce.rs` — current combinators
- `src/evm/history.rs`, `src/evm/log_cache.rs` — restructured in Phase 1
- `src/evm/erc20.rs` — current `Erc20BalanceReducer`
- `examples/history_explorer.rs`, `examples/erc20_explorer.rs` — current CLI shape

Production patterns to translate:
- `../reference/src/history.ts` — `RangeTree` (758–951), `HistImpl` (683–724),
  `HistStoreImpl` (978–1067), `ProxyHist`/`ProxyHistStore` (191–298),
  `MergedPos`/`MergedHist`/`MergedHistStore` (399–591)
- `../reference/src/erc20.ts` — `Erc20TransferSource` (499–603),
  `fetchTransferLogsCached` (295–311), `alignedBlockRanges` (608–620),
  `watchAddress` (342–361)
- `../reference/src/matcher.ts` — `BaseMatcher` (83–166), `AnchoredMatcher`
  (180–215), `Matcher` (241–270), `MatchOutput` (225–232)
- `../reference/src/cache.ts` — `SqliteStore` (133–184), the
  "slurp-to-Map-at-init" pattern
- `../reference/src/store.ts` — `StoreImpl`, `MergedStore` (159–187),
  `storeFromGen` (76–84)

User preferences from memory (apply throughout):
- Discuss architectural shifts before implementing
- No hacks that create future bugs
- Clean abstractions; avoid leaky generics
- Concrete EVM impl first, then extract traits

## Conventions

- Each phase produces something runnable and visible via the CLI.
- Tests live alongside code as `#[cfg(test)] mod tests` for unit, and in
  `tests/` for integration.
- CLI binaries live in `examples/` and use real RPC by default, with `--anvil`
  or `--mock` flags where useful.
- Use `tracing` for logs, not `println!`.
- Type signatures sketched below are guidance — adapt to keep the codebase
  consistent. Don't introduce traits speculatively.

## Cross-cutting prerequisites (do once, before Phase 1)

### CCP-1: Anvil-based integration harness

**Why:** every phase has integration tests; without this they stay vague.

**Where:** `tests/common/anvil.rs` (new), `tests/` root.

**What:**
- `pub async fn fork_mainnet(block: u64) -> AnvilInstance` — spins anvil
  forking mainnet at a fixed block via env var `MAINNET_RPC_URL`. Skip the
  test if env unset (do not hard-fail CI).
- `pub async fn local_anvil() -> AnvilInstance` — plain local chain for
  push-subscription tests.
- A fixtures file (`tests/common/fixtures.rs`) listing pinned blocks:
  one block with USDC transfers, one block with reorg-adjacent state if
  reachable, one empty block, the WETH and USDC contract addresses.

**Acceptance:**
- `cargo test --test integration -- --ignored` runs against anvil if
  `MAINNET_RPC_URL` set, otherwise prints "skipped: no rpc".
- One smoke test in `tests/anvil_smoke.rs` confirming the harness reaches
  a block and reads a known balance.

### CCP-2: Mock RPC asserter helpers

**Why:** Phase 1+ unit tests need to confirm RPC call counts (singleflight,
cache hits).

**Where:** `tests/common/mock_provider.rs` (new) wrapping `alloy::transports::mock::Asserter`.

**What:**
- Helper `expect_get_logs(times: usize)` that fails the test if eth_getLogs
  count diverges from `times`.
- Used by Phase 1's singleflight test and Phase 2's reconnection tests.

---

## Phase 0 — Housekeeping

**Estimated:** half a day. No design decisions.

**Deliverables:**
- Delete `src/modules/` and `src/storage/` (per memory: not compiled).
- Move current `LogCache` to `src/evm/log_cache_legacy.rs` *unchanged*. Re-export
  from `evm::mod` as `pub use log_cache_legacy::LogCache;` so the existing
  examples keep working during Phase 1 cut-over.
- Audit `src/bin/` (the gitignored sandbox) — fine to leave alone.

**Acceptance:**
- `cargo build --features evm` clean.
- `cargo test --features evm` passes the existing 42 tests with no edits.
- `cargo run --features evm --example erc20_explorer -- --help` still works.

---

## Phase 1 — Layered cache: `LogStore` + `LogIndex`

**Estimated:** 1.5–2 weeks. **Foundation phase. Do not parallelize with later phases.**

**Prereqs:** Phase 0, CCP-1, CCP-2.

### Goal

Push SQLite below the navigation hot path. Search reads from an in-memory
`BTreeMap`; SQLite is consulted only on cold gap or restart.

### Deliverables

**`src/evm/log_store.rs`** (new) — durable SQLite layer:
- Owns `cached_logs`, `scanned_ranges`, `reorged_blocks`, `cached_block_headers`,
  `erc20_metadata`, `erc20_balances` tables (migrate from `log_cache_legacy`).
- Public API only:
  - `get_logs_in_range(filter, from, to) -> Vec<Log>`
  - `record_scanned_range(filter, start, end, end_block_hash)`
  - `mark_reorged(&[(hash, num)])`
  - `compact_ranges(filter, up_to_block)`
  - `get_block_header(num)`, `insert_block_header(...)`
  - existing erc20 metadata + balance methods
- **Removed:** `first_log_after`, `last_log_before`, `first_log_at_or_after`,
  `last_log_at_or_before`. These move to `LogIndex`. If callers in tests/examples
  use them, update those callers.

**`src/evm/log_index.rs`** (new) — in-memory range structure:
```rust
pub struct LogIndex {
    store: Arc<LogStore>,
    filter_hash: [u8; 32],
    tree: Mutex<BTreeMap<u64, Range>>,  // key = range.end (block number)
    loaded: AtomicBool,  // first-touch lazy load
}

struct Range {
    start: u64,
    end: u64,
    // None = empty range (scanned, no logs). Some = LogRef points into LogStore.
    item: Option<LogRef>,
    // Some(...) while a peer is filling this range. Wait on this to dedup RPCs.
    fetch: Option<Shared<oneshot::Receiver<Result<(), HistoryError>>>>,
}

struct LogRef { block_number: u64, log_index: u32, block_hash: [u8; 32] }
```

Methods:
- `pub async fn first_at_or_after(&self, block: u64, idx: u32) -> Option<Log>`
- `pub async fn first_after(&self, block: u64, idx: u32) -> Option<Log>`
- `pub async fn last_at_or_before(&self, block: u64, idx: u32) -> Option<Log>`
- `pub async fn last_before(&self, block: u64, idx: u32) -> Option<Log>`
- `pub async fn items(&self, from: u64, to: u64) -> impl Stream<Item = Log>` — async
  generator over `(from, to]`, fills gaps inline, awaits peers' fetches.
- `pub async fn ensure_cached(&self, from: u64, to: u64) -> Result<(), HistoryError>`
  — singleflight per gap.
- `pub async fn insert_page(&self, page: LogPage)` — for Phase 2 push delivery
  (insert items + scanned range, late-item rules apply).
- `fn ensure_loaded(&self) -> Result<()>` — first-call lazy load of all scanned
  ranges + their item refs into the BTree. Items themselves stay in SQLite;
  the BTree holds `LogRef`s.

Rules (from the reference `RangeTree`):
- Empty `Range(start, end, None)` is a first-class entry. Means "we scanned
  this gap and found nothing." Preserved on prune.
- Adjacent ranges must abut exactly (`r1.end == r2.start`). No overlap.
- Concurrent gap fills: first hitter inserts a placeholder `Range` with
  `fetch = Some(rx)`; later hitters see it and `await fetch.clone()` instead
  of firing eth_getLogs.

**`src/evm/aligned_ranges.rs`** (new) — port `alignedBlockRanges`:
```rust
pub fn aligned_block_ranges(start: u64, end: u64, batch: u64)
    -> impl Iterator<Item = AlignedRange>;

pub struct AlignedRange { pub from: u64, pub to: u64, pub is_aligned: bool }
```
Used by `LogIndex::ensure_cached` when recording scanned ranges so cache keys
collide across processes.

**`src/evm/history.rs`** — rewire `EvmLogHistory`:
- Constructor takes `Arc<LogStore>` (was `LogCache`); internally builds an
  `Arc<LogIndex>` per `(filter_hash)`.
- All navigation methods (`run_search_after`, `run_search_before`) consult
  `LogIndex` only. No direct SQL access.
- `at(time)`, `lca`, `is_equiv` unchanged.

### Test plan

**Unit (`src/evm/log_index.rs` tests):**
- `insert_range_in_gap_creates_three_entries`
- `insert_range_at_existing_end_extends_not_duplicates`
- `late_item_inside_existing_range_splits_it`
- `late_item_at_empty_range_end_upgrades_to_filled`
- `late_item_duplicate_is_ignored`
- `items_walks_filled_ranges_yields_in_order`
- `items_into_uncached_gap_calls_store_and_caches`
- `prune_keeps_empty_range_tombstone` (forward-compat for Phase 7)
- `is_loaded_lazy_first_access_only`

**Unit (singleflight, in `src/evm/log_index.rs` tests):**
- `concurrent_searches_in_same_gap_fire_one_rpc` — spawn 5 `ensure_cached`
  futures on the same uncached gap; use `mock_provider::expect_get_logs(1)`.

**Unit (port from `log_cache.rs` to `log_store.rs`):**
- All 20 existing log_cache tests run against `LogStore` with the slimmer
  API surface.

**Integration (`tests/phase1_layering.rs`):**
- `warm_store_then_drop_index_then_reopen` — fork mainnet at block N. Walk
  range [N-100, N], fully populating LogStore and LogIndex. Drop the
  `EvmLogHistory` and `LogIndex`. Open fresh ones over the same `LogStore`.
  Walk same range. Assert zero RPC calls (via Asserter rejecting any).
- `cold_walk_then_warm_walk_latency` — measure µs for warm vs cold; assert
  warm walk is at least 100× faster than cold.

### CLI deliverable

Extend `examples/history_explorer.rs`:
- `--measure` flag — for each chunk, log `cold_ms` (RPC + sqlite write),
  `warm_us` (sqlite read into BTree), `hot_us` (BTree-only lookup).
- Manual smoke: run twice against a small block range on a public RPC,
  confirm second run is fast and prints `(0 RPCs, N sqlite reads, M BTree
  lookups)`.

### Acceptance

- All Phase 0 acceptance still holds.
- Phase 1 tests above pass.
- `history_explorer --measure` shows the three latency tiers visually
  separated by orders of magnitude.
- `LogCache` (legacy) is unused in `src/evm/history.rs`. Mark it
  `#[deprecated]`. Plan to delete it after Phase 3.

---

## Phase 2 — Push subscription path

**Estimated:** 1 week.

**Prereqs:** Phase 1.

### Goal

Live events delivered to consumers in <10ms, without SQLite on the path.

### Deliverables

**`src/evm/subscriber.rs`** (new) — alloy log filter watcher:
- `EvmLogSubscriber<P>` owns a websocket or polled filter, a `tokio::JoinHandle`,
  and a reconnect loop.
- Exposes:
  - `start(callbacks: SubscriberCallbacks) -> AsyncDisposable` (drop cancels)
  - Reconnect detection via watcher error or timeout; on reconnect, calls
    `callbacks.on_reconnect(last_known_tip)` so the orchestrator can gap-fill.
- Internal: catches network errors, never propagates them; backs off
  exponentially up to 30s. Logs via `tracing::warn!`.

**`src/evm/history.rs`** — add subscription:
```rust
impl<P: Provider + ...> EvmLogHistory<P> {
    pub async fn subscribe(self: &Arc<Self>) -> Result<AsyncDisposable, HistoryError>;
}
```
Wires `EvmLogSubscriber` to `LogIndex::insert_page` and the `latest_store`
tip update. Started automatically if `start_tip_tracking` is also called
(or replaces it — see decision below).

**Decision to confirm at start of Phase 2:** does `subscribe()` replace
`start_tip_tracking`, or coexist? Recommendation: replace. The polling
tip tracker only exists because there's no push path; once push exists,
poll is a fallback for chains without websockets and can be a flag.

**`src/evm/log_index.rs`** — extend `insert_page` with the four late-item cases
(from the reference `RangeTree.insertLateItem`):
1. Item in unscanned gap → insert new `(rangeStart, item.pos, item)`.
2. Item position equals existing empty range end → upgrade range to filled.
3. Item position inside existing range → split range around item.
4. Item position equals existing filled range end → no-op (duplicate).

Write a tiny background drain task: `LogIndex::insert_page` updates the
in-memory tree synchronously AND pushes the page into a tokio mpsc that
a background task drains into `LogStore.insert_logs` + `record_scanned_range`.
The drain channel buffers — if it fills, log a warning and apply backpressure
(slow path acceptable for live events, never block hot path).

**Reconnection handling:**
- `on_reconnect(last_tip)` calls `LogIndex::ensure_cached(last_tip+1,
  current_tip)`. This reuses existing search infrastructure — no new code path.
- Late-item rules handle any overlap from the resubscribe dump.

### Test plan

**Unit (`src/evm/log_index.rs` tests):**
- One test per late-item case:
  - `late_item_in_gap_inserts_filled_range`
  - `late_item_at_empty_range_end_upgrades`
  - `late_item_in_filled_range_splits`
  - `late_item_duplicate_no_op`
- `insert_page_drains_to_store_asynchronously` — assert the in-memory tree
  is updated immediately, sqlite write happens after a `tokio::yield_now`.

**Unit (`src/evm/subscriber.rs` tests):**
- `drop_cancels_watcher` — start subscriber, drop, assert background task
  count returns to baseline within 100ms.
- `error_triggers_reconnect_with_backoff` — mock provider returns error
  three times then succeeds; assert reconnect attempts happen with backoff.

**Integration (`tests/phase2_push.rs`):**
- `live_event_latency_under_10ms` — local anvil. Subscribe to a token's
  Transfer. Send a transfer. Measure from `block.timestamp*1000` (or tx
  submit time) to consumer receiving the event. Assert <10ms (anvil is
  fast; mainnet would be RPC-bound, not library-bound).
- `reconnect_gap_fill_no_duplicates` — subscribe, mine block A with one
  transfer, kill the websocket, mine blocks B, C with transfers, restore
  websocket, assert consumer sees A, B, C exactly once each.
- `crash_consistency` — subscribe, receive some events, drop the
  `EvmLogHistory` *before* sqlite flush completes (force using
  `tokio::time::pause`). Reopen. Walk the range. Assert events are
  re-fetched from RPC (acceptable bounded loss).

### CLI deliverable

`examples/erc20_explorer.rs`:
- `--follow` now uses `subscribe()` instead of polling.
- Print `push_latency_ms = block_arrival_now - event_block_timestamp` per
  live event.
- Soak test (manual): run for 1h against mainnet USDC; confirm latency
  stays <100ms p99 (network-bound).

### Acceptance

- Tests above pass.
- `cargo run --example erc20_explorer -- --owner $ADDR --follow` shows
  events arriving sub-second on mainnet.
- Killing the network mid-stream and restoring it produces no duplicates
  and no gaps visible in the CLI output.

---

## Phase 3 — `Hist` / `HistStore` split

**Estimated:** 2 weeks. **Locked in** (no decision gate).

**Prereqs:** Phase 1. Phase 2 helpful but not strictly required.

### Goal

Replace `History<T,P>` with two types:
- `Hist<T,P,A>` — finite, immutable snapshot. Has `start`/`end`. Iterable
  forward. Splittable at an anchor.
- `HistStore<T,P,A>` — yields fresh `Hist` as the tip advances. Owns
  subscribe lifecycle.

This is the canonical reference shape; it's a substantial API shift but
unlocks Phase 4 combinators and Phase 6 Matcher.

### Deliverables

**`src/arch.rs`** — new types (old `History` stays as `#[deprecated]` facade
for one release, then removed in Phase 7):

```rust
/// Serializable cut point used to split a history into "not after" and "after".
pub trait Anchor: Send + Sync + 'static {
    fn marshal(&self) -> Vec<u8>;
}

pub struct Hist<T, P: Position, A: Anchor> {
    backend: Arc<dyn HistBackend<T, P, A>>,
    start: P,
    end: P,
}

impl<T, P, A> Hist<T, P, A> {
    pub fn start(&self) -> &P;
    pub fn end(&self) -> &P;
    pub fn iter(&self) -> impl Stream<Item = Result<T, HistoryError>>;
    pub fn split(&self, a: A) -> (Self, Self);
    pub async fn pos_after(&self, items: &[T]) -> Result<Option<A>, HistoryError>;
}

pub struct HistStore<T, P: Position, A: Anchor> {
    backend: Arc<dyn HistStoreBackend<T, P, A>>,
}

impl<T, P, A> HistStore<T, P, A> {
    /// Yields a fresh Hist every time the tip advances. First yield is the current state.
    pub fn gen(&self) -> impl Stream<Item = Hist<T, P, A>>;
    /// Snapshot the current head — Future of the next-or-current Hist.
    pub async fn next(&self) -> Hist<T, P, A>;
    /// Subscribe to a callback for each new Hist.
    pub fn subscribe(&self, cb: impl Fn(Hist<T, P, A>)) -> AsyncDisposable;
    /// Build an anchor for a wall-clock time.
    pub async fn time_pos(&self, time: Timestamp) -> Result<A, HistoryError>;
    /// Deserialize a marshalled anchor.
    pub fn load(&self, bytes: &[u8]) -> Result<A, HistoryError>;
}
```

For EVM, `A = EvmPosition` and `Position == Anchor` (the side-aware position
serves as both). For other source types (Lighter API) they may diverge.

**`src/arch.rs`** — `watch_hist` free function:
```rust
/// Yield all items from `store` after `start`. Never ends.
pub async fn watch_hist<T, P, A>(
    store: &HistStore<T, P, A>,
    start: A,
) -> impl Stream<Item = Result<T, HistoryError>>;
```
Direct port of the reference `watchHist` (history.ts:178–189).

**`src/arch.rs`** — `act_and_wait` helper:
```rust
/// Snapshot tip, run op, wait for first item matching predicate.
pub async fn act_and_wait<T, P, A, R>(
    store: &HistStore<T, P, A>,
    op: impl FnOnce() -> impl Future<Output = R>,
    predicate: impl Fn(&T, &R) -> bool,
) -> Result<T, HistoryError>;
```
Centralizes the "snapshot before op" race fix from the reference
(history.ts:160–172).

**`src/evm/history.rs`** — `EvmLogHistory` implements `HistStoreBackend` directly.
- `gen()` yields a new `Hist` every time `latest_store` ticks (or on the
  first `next()` call).
- `Hist::iter()` walks `LogIndex::items(start, end)`.
- `Hist::split(p)` clamps `p` to `[hist.start, hist.end]` and produces two
  `Hist`s sharing the same backend (the reference `HistImpl.split` clamp comment is
  the spec).

**Old `History`/`Cursor`:**
- Keep `Cursor::next/prev` working as `store.next().split(anchor).iter().next()`
  thin facade for one release.
- Mark `History::new`, `History::after`, `History::before`,
  `History::search_after`, `History::search_before` as `#[deprecated]` with
  pointers to the new API.

### Test plan

**Unit (`src/arch.rs` and `src/evm/history.rs` tests):**
- `hist_split_round_trips_via_marshalled_anchor`
- `hist_split_clamps_to_window` — `split(p < hist.start)` produces `(empty,
  full)`, not a wider after-side. Spec: reference history.ts:693–705.
- `hist_iter_stops_at_end`
- `hist_store_gen_yields_current_then_on_tip_advance`
- `watch_hist_yields_only_post_start`
- `act_and_wait_snapshot_before_op_not_after` — explicit race test: op
  completes "immediately" (via `tokio::yield_now`), still detected.

**Integration (`tests/phase3_hist_store.rs`):**
- `forward_walk_multiple_snapshots` — start a `HistStore`, snapshot, iter
  partway, snapshot again (new tip), iter rest. Assert all items visible
  exactly once, no duplicates across snapshots.
- `resume_from_marshalled_anchor` — iter halfway, marshal `anchor =
  cursor.after`, drop process state, reopen, `store.load(anchor)`, split,
  iter rest. Assert continuation is correct.

### CLI deliverable

Rewrite `examples/history_explorer.rs` over the new `HistStore.gen()` loop:
```rust
let mut snapshots = store.gen();
let mut anchor = load_or_start_anchor();
while let Some(hist) = snapshots.next().await {
    let (_, tail) = hist.split(anchor);
    while let Some(item) = tail.iter().next().await {
        process(item);
        anchor = item.after.clone();
        save_anchor(&anchor);
    }
}
```
Should be a small diff from the current `Cursor::next` loop; validates the
API ergonomics.

### Acceptance

- Tests above pass.
- `history_explorer` works identically to current behavior, code measurably
  shorter.
- Existing examples and tests compile against the `#[deprecated]` facade.

---

## Phase 4 — Proxy combinators: Map / Filter / Since

**Estimated:** 1 week.

**Prereqs:** Phase 3.

### Goal

Make `OwnedErc20::transfers = filter(token.transfers, owner)` natural. Add
the standard combinator family.

### Deliverables

**`src/combinators/mapped.rs`** (new):
```rust
pub fn map_hist_store<Src, Dst, P, A>(
    store: HistStore<Src, P, A>,
    f: impl Fn(Src) -> Dst + Send + Sync + 'static,
) -> HistStore<MappedItem<Src, Dst>, P, A>;

pub struct MappedItem<Src, Dst> { item: Dst, source: Src }
```
The `source: Src` field is what `pos_after` uses to reconstruct the
upstream prefix.

**`src/combinators/filtered.rs`** (new):
```rust
pub fn filter_hist_store<Item, P, A>(
    store: HistStore<Item, P, A>,
    pred: impl Fn(&Item) -> bool + Send + Sync + 'static,
) -> HistStore<FilteredItem<Item>, P, A>;

pub struct FilteredItem<Item> { item: Item, skipped: Vec<Item> }
```
`skipped` carries the run of filtered-out items between this yield and the
previous one, so `pos_after` reconstructs upstream. Spec: reference history.ts:308–345.

**`src/combinators/since.rs`** (new):
```rust
pub fn since_hist_store<Item, P, A>(
    store: HistStore<Item, P, A>,
    cut: A,
) -> HistStore<Item, P, A>;
```
Pre-splits every emitted Hist at `cut` — cuts at upstream `split`, NOT
inside the iterator. This is the key reference rationale (history.ts:367–397):
expensive mappers wrapped between `since` and source only run for post-cut
items.

**`src/evm/owned_erc20.rs`** (new) — rebuild OwnedErc20 over combinators:
```rust
impl OwnedErc20 {
    pub fn transfers(&self) -> HistStore<Erc20Transfer, EvmPosition, EvmPosition> {
        let outgoing = self.token.transfers(Some(self.owner), None);
        let incoming = self.token.transfers(None, Some(self.owner));
        merge_hist_stores(vec![outgoing, incoming], evm_pos_cmp)
    }
}
```

### Test plan

**Unit (`src/combinators/*` tests):**
- `filter_preserves_anchor_round_trip` — filter a stream, marshal an anchor,
  reload, iter again, assert items match.
- `filter_pos_after_reconstructs_upstream_prefix` — feed known src items,
  filter half, query `pos_after`, assert the returned anchor sits where
  expected in the upstream.
- `map_composes_with_filter` — `map(filter(src, p), f)` and `filter(map(src,
  f), p ∘ f)` semantically equivalent for `p` that doesn't depend on `f`.
- `since_skips_mapper_on_pre_cut_items` — wrap a mapper with a counter; cut
  the stream; iter; assert the counter only increments for post-cut items.

**Integration (`tests/phase4_owned_erc20.rs`):**
- `owned_erc20_matches_current_impl` — pin a small block range with known
  transfers. Current explorer output bytewise-equal to new `OwnedErc20`
  output via combinators.

### CLI deliverable

`examples/erc20_explorer.rs --owner` uses the new combinator pipeline. No
user-visible change is the point. Diff `current_output_fixture.txt` and
`new_output_fixture.txt`, assert empty.

### Acceptance

- Combinator tests pass.
- `erc20_explorer --owner` produces bytewise-identical output to pre-Phase-4.

---

## Phase 5 — Heterogeneous merge + keyed `MergedPos`

**Estimated:** 1.5 weeks.

**Prereqs:** Phase 3 (uses `HistStore`).

### Goal

Cross-source merge: combine histories with different `P` and `T` types.
Realizes arch.md's "Reducer architecture / Example: crosschain bridge state."

### Deliverables

**`src/merge.rs`** — replace current `MergedHistory` (same-P only) with:

```rust
pub struct MergedPos {
    /// keys are source names ("eth", "polygon", "lighter", ...)
    /// values are marshalled per-source anchors
    pub positions: BTreeMap<String, Vec<u8>>,
}

impl Anchor for MergedPos {
    fn marshal(&self) -> Vec<u8> { postcard::to_allocvec(self).unwrap() }
}

pub struct MergedItem<T> {
    pub item: T,
    pub source_key: String,
    pub source_pos: Vec<u8>,
}

pub fn merge_hist_stores<T: 'static>(
    sources: HashMap<String, Box<dyn AnySourceErased<T>>>,
    cmp: impl Fn(&MergedItem<T>, &MergedItem<T>) -> Ordering + Send + Sync + 'static,
) -> HistStore<MergedItem<T>, MergedPos, MergedPos>;
```

`AnySourceErased<T>` is an internal trait erasing per-source `P, A` types
while still permitting `marshal`/`unmarshal` of each component anchor. This
is the Rust translation of the reference implementation's `Record<string, HistStore<Item, Pos>>`
(history.ts:553–582).

Note: when all sources are same-P (most current uses), keep the existing
typed `merge` as a convenience alias. The new merge is for the
heterogeneous case.

### Test plan

**Unit (`src/merge.rs` tests):**
- `merged_pos_marshal_round_trip_across_heterogeneous_anchors`
- `merge_orders_by_provided_cmp`
- `merge_deduplicates_same_source_key_same_pos` — same as current test,
  ported to new API.
- `merge_reverse_walk` — port from current merge tests.
- `merged_pos_handles_added_source` — marshal a `MergedPos` with two
  sources, deserialize, add a third source key with `time_pos(genesis)`,
  iter — should not lose existing-source progress.

**Integration (`tests/phase5_cross_chain.rs`):**
- `merge_two_chains_orders_by_timestamp` — fork mainnet AND fork Optimism
  (CCP-1 extended). Build `EvmLogHistory` over USDC on each. Merge by
  timestamp. Walk; assert events from both chains interleaved chronologically.

### CLI deliverable

New `examples/bridge_explorer.rs`:
- Args: `--source-chain <name> --source-rpc <url> --source-address <addr>`
  (repeatable for multiple sources).
- Prints events in merged order with `[chain:block:idx] event...` prefix.
- Manual smoke: USDC on Ethereum + USDC on Optimism, observe interleaved
  flow.

### Acceptance

- Tests above pass.
- `bridge_explorer --source-chain eth ... --source-chain optimism ...` runs
  and prints chronologically-ordered events from both chains.

---

## Phase 6 — `Matcher` abstraction

**Estimated:** 1.5 weeks.

**Prereqs:** Phase 3, Phase 5 (uses cross-source anchors).

### Goal

Pure correlation over src + dst tails. Add alongside `Reducer` — they
target different problems and both stay.

### Deliverables

**`src/matcher.rs`** (new):

```rust
pub struct MatchOutput<Src, Dst> {
    pub matched: Vec<(Src, Dst)>,
    pub pending: Vec<Src>,
    pub orphans: Vec<Dst>,
}

pub struct MatcherAnchors<SA: Anchor, DA: Anchor> { pub src: SA, pub dst: DA }

#[async_trait]
pub trait MatchFn<Src, Dst, Output>: Send + Sync {
    async fn matchup(&self, src_items: Vec<Src>, dst_items: Vec<Dst>) -> Output;
}

/// Pure compute. No internal state. Spec: reference matcher.ts:83-166.
pub struct BaseMatcher<Src, Dst, Output, SA, DA, M>
where M: MatchFn<Src, Dst, Output> + ...
{
    src_store: HistStore<Src, _, SA>,
    dst_store: HistStore<Dst, _, DA>,
    match_fn: M,
}

impl ... {
    pub async fn compute_state(
        &self,
        anchors: MatcherAnchors<SA, DA>,
    ) -> Result<Output, HistoryError>;

    /// Wake on either source advancing.
    pub fn changes(&self) -> Store<()>;

    pub async fn get_history(
        &self,
        since: Timestamp,
    ) -> Result<Output, HistoryError>;
}

/// Adds anchor persistence on top of BaseMatcher.
pub struct AnchoredMatcher<...> {
    base: BaseMatcher<...>,
    persistence: Arc<dyn AnchorStore>,
    cached: tokio::sync::Mutex<Option<MatcherAnchors<SA, DA>>>,
}

impl ... {
    pub async fn current_state(&self) -> Result<Output, HistoryError>;
    pub async fn iter(&self) -> impl Stream<Item = Result<Output, HistoryError>>;
    pub async fn set_anchors(&self, next: MatcherAnchors<SA, DA>) -> Result<()>;
}
```

`AnchorStore` is a trait extending `LogStore` with a small kv table for
matcher anchor persistence. One impl, backed by SQLite.

**`src/matcher/bridge_example.rs`** (new) — reference implementation:
- Match `Burn(from, amount, bridge_id)` events on chain A against `Mint(to,
  amount, bridge_id)` events on chain B by `bridge_id`.
- Output: `matched: Vec<(Burn, Mint)>`, `pending: Vec<Burn>`, `orphans: Vec<Mint>`.

### Test plan

**Unit (`src/matcher.rs` tests):**
- `compute_state_is_pure` — call twice on unchanged inputs, identical
  output. No mutation between calls.
- `advance_anchors_removes_matched_from_output` — match a pair, advance
  both anchors past it, recompute — pair gone from `matched`, *also* gone
  from `pending`/`orphans`.
- `asymmetric_anchor_advance_demotes_match_to_orphan` — advance only `src`
  past a matched pair (not `dst`) — pair moves from `matched` to `orphans`
  on next compute. Pin this semantic.
- `get_history_uses_time_anchors_not_persisted` — call `get_history(t)`
  doesn't touch the persistence layer.

**Integration (`tests/phase6_matcher.rs`):**
- `bridge_matcher_pairs_burn_mint` — fork mainnet at a block with known
  bridge events, replay through matcher, assert expected pairs.
- `anchored_matcher_persists_across_restart` — advance anchors, drop
  matcher, reopen with same `AnchorStore`, recompute, assert state
  consistent with where we left off.

### CLI deliverable

New `examples/bridge_matcher.rs`:
- Args: `--chain-a / --chain-b / --bridge-a-contract / --bridge-b-contract`.
- Live mode prints `matched: N | pending: M | orphans: K` table that
  refreshes on each `changes()` tick.
- Manual smoke: Optimism canonical bridge, verify a known recent matched
  pair appears.

### Acceptance

- Tests above pass.
- `bridge_matcher` against a real bridge correctly identifies a known matched
  pair (manual verification via etherscan).

---

## Phase 7 — Pruning, polish, best/safe wiring

**Estimated:** 1 week. Opportunistic.

**Prereqs:** Phase 1 + Phase 2 (uses LogIndex and subscriptions).

### Goal

Bound long-running memory. Wire the `best`/`safe` stores that have been
stubbed all along.

### Deliverables

**`src/evm/log_index.rs`** — pruning:
- `prune_older_than(threshold: Duration)` — for `Range`s last-touched
  before `now - threshold`, drop `item: Option<LogRef>` (set to None).
  **Crucially:** keep the `Range` entry; only items get evicted. Empty
  ranges are tombstones.
- `compact_in_memory(up_to_block)` — merge adjacent empty ranges (run
  periodically; safety: only on ranges entirely below `safe` tip).
- On iter into a pruned range, repopulate items from `LogStore`. Cache-aside.

**`src/evm/history.rs`** — `best`/`safe`:
- `best`: poll `eth_getBlockByNumber("latest", false)` minus a
  configurable confirmation depth (default 12 for mainnet).
- `safe`: poll `eth_getBlockByNumber("finalized", false)` if chain supports
  it (post-Merge ETH, most L2s); fall back to `latest - 64` otherwise.
- On `safe` advance, kick off background `LogStore::compact_ranges` for
  ranges entirely below the new safe tip.

**Delete the deprecated `History`/`Cursor` facade** — Phase 3 left it as
a compatibility shim; remove it now.

### Test plan

**Unit (`src/evm/log_index.rs` tests):**
- `prune_drops_items_keeps_range`
- `prune_then_iter_repopulates_from_store`
- `compact_in_memory_merges_adjacent_empty`
- `compact_skips_above_safe_tip`

**Integration (`tests/phase7_soak.rs`):**
- `long_running_follow_memory_bounded` — `#[ignore]`-flagged 30-minute
  test (run manually): subscribe to USDC, prune every 30s, assert RSS
  plateaus after warmup.

### CLI deliverable

`examples/erc20_explorer.rs`:
- Add `--prune-after <duration>` flag.
- Add `--measure-memory` flag that emits RSS once per minute.
- Manual: 24h soak against mainnet USDC, RSS chart plateaus.

### Acceptance

- Tests pass.
- Long-running follow no longer grows RSS without bound.
- `History`/`Cursor` deprecated facade removed; codebase only references
  `Hist`/`HistStore`.

---

## What success looks like (after all phases)

- Live event delivery <10ms (chainfold internal latency; network is what
  it is).
- Cold-start of `erc20_explorer` reads cached events from SQLite in <50ms
  before showing the first frame.
- Reorgs detected, scanned ranges invalidated automatically, no duplicate
  events delivered.
- `bridge_explorer` and `bridge_matcher` run against real cross-chain
  setups without bespoke per-bridge code (just a `match_fn`).
- Memory bounded under indefinite follow.
- The library can be used as a dependency with these primary types:
  `Hist`, `HistStore`, `Position`, `Anchor`, `Reducer`, `Matcher`,
  `merge_hist_stores`, `map_hist_store`, `filter_hist_store`,
  `since_hist_store`, `watch_hist`, `act_and_wait`.

## How to use this plan

- **Each phase is a unit of work.** Pick the lowest-numbered incomplete
  phase. Phases 1–3 are sequential; Phase 4 needs 3; Phase 5 needs 3;
  Phase 6 needs 3+5; Phase 7 needs 1+2.
- **Read the references first.** The reference source files listed at the
  top contain the production-tested versions of every pattern below.
  Don't reinvent — translate.
- **Per-phase "decision to confirm" lines** are checkpoints with the user
  before implementation, NOT after. Memory: "Discuss architectural changes
  BEFORE implementing."
- **Tests are non-negotiable.** Every phase ships unit + integration. If
  the integration test needs `MAINNET_RPC_URL` and CI doesn't have it,
  mark `#[ignore]` and document in the phase acceptance.
- **CLI is the validation surface.** If you can't drive the change
  end-to-end via the example binary against real RPC, it's not done.
