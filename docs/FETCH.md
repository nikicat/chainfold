# Chainfold — Adaptive provider fetch (design)

> **This document** is the agreed design for the adaptive fetch layer:
> the streaming page contract, the slicing executor, and provider
> adaptors. Sequencing, test plan and acceptance criteria live in
> [`ROADMAP.md` Phase 8](./ROADMAP.md#phase-8--adaptive-provider-fetch-next-up--scheduled-ahead-of-phase-7);
> the as-built reference is [`ARCHITECTURE.md`](./ARCHITECTURE.md) (this
> design folds into it as the pieces ship); conventions in
> [`STYLE.md`](./STYLE.md) apply to the sketches below as much as to code.

## 1. Problem

`eth_getLogs` limits differ per provider and are **multi-dimensional**:
range-width caps (from a few hundred blocks on free public endpoints to
effectively unlimited on a local reth/erigon), result-count caps (Infura
and Alchemy accept *any* range as long as ≤10k logs come back), response
byte and compute-time caps. The result-count dimension makes success
**content-dependent** — a width that works over sparse history fails at a
busy deployment block — so no single discovered constant is correct.

The current fetch path has neither adaptation nor recovery:

- One gap = one `eth_getLogs`, sized by `ChunkSize::chunk_end` (default
  1000 blocks). A provider that rejects that request fails the gap; the
  placeholder is dropped and the **retry refetches the same range** —
  a permanent failure. The only lever is a global `--chunk-size` restart,
  and content-dependent caps have no one right value.
- Against generous endpoints the fixed 1000 wastes calls — often 10× the
  requests the provider would accept.
- All-or-nothing gap results: a failure discards every log already
  received for that gap, and a large gap yields nothing to consumers until
  the whole fetch returns.
- Config is a full RPC URL per chain, with any embedded API key exposed to
  logging.

Three layers fix this, each at its own seam: a **streaming page contract**
in the source-agnostic core (§2), a **slicing executor** in the EVM source
layer (§3), and **provider adaptors** at the endpoint boundary (§4).

## 2. Streaming page contract (range-index core)

The `Fetcher<Page>` closure alias (`src/range_index/spec.rs`) becomes a
named trait per [`STYLE.md`](./STYLE.md#reactive-not-poll) (traits over
`Fn`-pointer aliases), returning a **stream of pages** instead of one
page:

```rust
/// Streaming gap filler. Pages for `range` arrive in ascending order,
/// contiguous from `range.from`.
pub trait PageFetcher<Page>: Send + Sync + 'static {
    fn fetch(&self, range: BlockRange) -> PageStream<Page>;
}

pub type PageStream<Page> =
    Pin<Box<dyn Stream<Item = Result<Page, HistoryError>> + Send>>;
```

**Contract invariants:**

1. Pages arrive **in ascending order, contiguous from `range.from`** —
   each page's `page_bounds` starts one block after the previous page
   ended; the first starts at `range.from`. No page extends past
   `range.to`.
2. A stream that ends after a page reaching `range.to` is a **success**.
3. An `Err` item ends the stream: the delivered prefix is a valid
   coverage claim; the suffix past the last delivered page **stays a
   gap**.

Invariant 1 is the *intermediate* contract — the long-term direction
relaxes it to out-of-order pages with placeholder carving (§5,
decision 1).

**Gap-fill mechanics** (`src/range_index/gap_fill.rs`): the walk still
installs one placeholder per gap, but finalization goes per-page — insert
the page's items, persist it, **shrink the placeholder from the left**
(replace `(cursor, gap_end]` with `(page_end, gap_end]`), and
`notify_waiters`. On an `Err` item, drop the remaining placeholder,
notify, surface the error. The `Range::empty` tail-stamp in
`finalize_gap_fill` becomes per-page-correct because each page's
`page_bounds` *is* its sub-range — partial progress needs no separate
mechanism.

**What stays unchanged:** the walk/singleflight semantics (waiters already
re-walk on notify — they now simply proceed incrementally); the
fetched-at-most-once guarantee (pages only ever cover the gap);
`RangeSpec::persist` per page (`scanned_ranges` already records per-fetch
rows and compacts later — `Coverage::compact_ranges`); the reactive tip
(`on_bump` is live-path-only). Bitcoin adapts as a single-page stream, so
the seam stays source-agnostic.

**Consumer effect:** `items()` over a cold 100k-block gap yields its first
item as soon as the first page lands, while later slices are still in
flight — time-to-first-item ≈ one slice latency, which is what frees
`chunk_size` to grow.

**`ChunkSize`'s role shifts:** it no longer bounds any RPC. It keeps two
jobs — `load_window` alignment, and how much one walk step claims as a
placeholder (stream length). It becomes the *ceiling* the working size
(§3) grows toward, and can be raised aggressively once this ships.

## 3. Slicing executor (EVM source layer)

The EVM `PageFetcher` impl (new `src/evm/chain/fetch.rs`, replacing the
body of `build_fetcher`) plans and runs slices:

- **Slice plan** — split the gap by the endpoint's current *working size*
  (a `BlockCount`). A 100k gap at a learned 2k limit is 50 slices.
- **Bounded-parallel execution** under a **per-endpoint budget shared by
  every filter store on the chain** — it lives in `ChainProvider` beside
  `HeaderSingleflight`, for the same reason that exists: every filter's
  gap-fill task hits the same endpoint, and without a shared limiter
  N filters × M slices thundering-herd it.
- **In-order release** — slices run concurrently but their results are
  released into the `PageStream` in ascending order through a bounded
  reorder buffer (TCP-style). See decision 1 in §5.

```rust
/// Per-endpoint adaptive state, shared by every store on the chain.
struct EndpointBudget {
    inflight: Semaphore,        // shrinks on RateLimited, recovers when quiet
    working: AtomicBlockCount,  // halves on RangeTooLarge, regrows additively
    generation: AtomicU64,      // stamps slices; one halve per generation
}
```

**Two adaptive knobs, driven by error class** (classification is the
adaptor's job, §4):

- `RangeTooLarge { suggested }` → adopt the provider-suggested range if
  present, else **halve the working size**, and re-slice the failed
  remainder. Multiplicative decrease applies **once per generation**:
  slices are stamped with the working-size generation they were issued
  under, and only the first failure of a generation halves — otherwise 20
  concurrent failures collapse 2000 → 1 in one round trip.
- `RateLimited { retry_after }` → the size is fine; back off and **shrink
  concurrency**. Shrinking the range here would be actively wrong — it
  raises total request count against an endpoint asking you to slow down.
- `Transient` → bounded retry of the same slice. For the generic adaptor
  this is also the bisection driver: retry smaller, and only surface the
  error when the minimum-size slice still fails.
- `Fatal` → end the stream with the error (earlier in-flight slices still
  release first, so their coverage persists).

**Growth** is additive: after K clean slices at the current size, step
back up toward the `ChunkSize` ceiling. Concurrency recovers likewise
after a quiet period.

**Headers ride the same budget.** The fetcher's post-`get_logs` header
fill (`ensure_block_header` per log-bearing block, plus each page's
`end_block_hash`) is serial today (`src/evm/chain/logs.rs`) and would
dominate large backfills on providers that omit `block_timestamp`; it runs
bounded-parallel under the same semaphore — the existing singleflight gate
makes that safe.

**Observability:** split/halve/regrow decisions are `tracing` events at
the decision site (diagnostics ride tracing, not return types); RPC
counting already rides the transport layer, so zero-RPC test guards keep
working unchanged.

**Batch JSON-RPC is deferred** behind the same execution seam (decision
6): most providers meter batch items individually and cap batch length,
HTTP/2 keep-alive already amortizes transport overhead, and a
transport-level batch failure couples many slices to one timeout — worse
for the AIMD signal. If a target provider demonstrably rewards batches,
the strategy drops in without touching the plan or the contract.

## 4. Provider adaptors

Per-provider adaptors (Alchemy, Infura, dRPC, GetBlock, Tenderly, …) plus
a generic fallback, behind one trait. The intelligence lives here, not in
a universal classifier:

```rust
pub trait ProviderAdaptor: Send + Sync {
    /// Strict per-provider error taxonomy. The generic impl returns
    /// `Transient` and lets the executor bisect.
    fn classify(&self, error: &TransportError) -> ErrorClass;
    /// Known limits, so the AIMD starts converged instead of probing.
    fn seeds(&self, chain: ChainId) -> FetchSeeds;
    /// Recognize this provider's URL shape; extract the key.
    fn detect(&self, url: &Url) -> Option<ApiKey>;
    /// Endpoint templates: chain + key → URL, http and ws.
    fn endpoint(&self, chain: ChainId, key: &ApiKey, kind: RpcTransport)
    -> Option<Url>;
}

pub enum ErrorClass {
    RangeTooLarge { suggested: Option<BlockRange> },
    RateLimited { retry_after: Option<Duration> },
    Transient,
    Fatal,
}
```

- **Classify** — provider impls match exact codes/payloads (Infura's
  `-32005` carries the suggested range in `data`; Alchemy's message states
  the 2k-blocks/10k-logs rule) and are tested against **recorded real
  error payloads** as fixtures. The generic adaptor keeps the
  bisect-on-anything heuristic, so an unknown provider degrades to
  discovery, never to failure.
- **Seeds** — known max range / max logs / batch policy / sane
  concurrency per provider (`FetchSeeds`, all newtyped quantities).
- **Endpoints** — `detect` serves the existing `--rpc-url` path (registry
  scans known URL shapes); `endpoint` serves autoconfig: provider + key +
  chain resolves both the http and the **ws** URL — one key configures
  `ws_follow` too, which shrinks config further than the http side alone.
- **`ApiKey` is a redacting newtype** (`Debug`/`Display` never print the
  secret). Because the adaptor knows which URL segment is the key, URL
  rendering in logs/errors redacts it — today a key embedded in
  `--rpc-url` leaks into any span that carries the URL.

**Config surface:** `--api-key <provider>:<KEY>` (plus per-provider env
vars) with chains resolved by id or name; explicit `--rpc-url` + generic
adaptor stays first-class and is the escape hatch when a provider's URL
scheme drifts. Per-provider network-name tables are kept small and
data-driven — they *will* drift; the fallback must never be more than one
flag away.

**Placement:** EVM-side (`src/evm/provider/`) for now. GetBlock and dRPC
serve Bitcoin too, so the concept may move up a layer — but per
concrete-first, not before a Bitcoin client needs it.

## 5. Decisions locked in

1. **In-order contiguous page release — as the *intermediate* contract.**
   Slices *run* out of order; they *release* in ascending order through a
   bounded reorder buffer. This keeps the placeholder bookkeeping monotone
   (shrink from the left) and serves the common consumer — a stream from
   the gap start — at no latency cost. Accepted limitations, to revisit
   long-term: a completed later slice is discarded when an earlier slice
   terminally fails (the retry refetches it), and a waiter whose window
   sits deep inside the gap waits for delivery to reach it even if its own
   sub-range completed early. The upgrade path is out-of-order placeholder
   **carving** — split the in-flight range around each landed page —
   which relaxes contract invariant 1 (§2) and touches only the gap-fill
   internals; the executor and adaptors are unaffected.
2. **Parallelism lives below the `RangeIndex`.** The walk stays
   sequential, one placeholder per gap; all fan-out is inside the
   `PageFetcher`. Cross-chunk parallelism is achieved by raising
   `chunk_size`, not by teaching the walk to drive multiple gaps.
3. **Two adaptive knobs, never conflated:** range errors shrink the
   *size*, rate limits shrink the *concurrency*.
4. **Multiplicative decrease once per generation** — concurrent failures
   from the same working size count as one congestion event.
5. **Classification is an adaptor method consumed by the executor**, not
   another transport layer. `RpcTimingLayer` is unaffected.
6. **Parallel-single requests by default; batch JSON-RPC deferred** behind
   the execution-strategy seam (§3).
7. **`ChunkSize` stays the alignment + placeholder granularity** and the
   working-size ceiling; it no longer bounds RPCs.
8. **Adaptors are EVM-side until Bitcoin needs them**; the generic
   adaptor is always available.

## 6. Accepted costs

- One `end_block_hash` header fetch per slice boundary instead of per gap
  — singleflighted and cached, so marginal.
- More `scanned_ranges` rows between compactions — schema-native;
  `compact_ranges` already merges them.
- Waiters wake once per page instead of once per gap — they re-walk, which
  is the existing (cheap) semantics.
- Reorder-buffer memory bounded by concurrency × page size.
