# Chainfold — Coding style & design preferences

> **This document** records the project's standing preferences — the
> high-level architectural rules and the low-level code conventions that
> reviews keep enforcing. They apply to plans and sketches as much as to
> code: a design draft that models a block number as `u64` is as wrong as
> the code would be. The as-built design is
> [`ARCHITECTURE.md`](./ARCHITECTURE.md); the plan is
> [`ROADMAP.md`](./ROADMAP.md).

## Process

- **Discuss architectural changes before implementing.** Structural shifts
  (new traits, new modules, changed ownership, changed public API shape) get
  a "decision to confirm" checkpoint first; mechanical work inside an agreed
  shape doesn't.
- **No hacks that create future bugs.** A shortcut that works today but
  plants a footgun (silent truncation, a poll loop that busy-spins on an
  edge, a table shared without a partition key) is rejected even when tests
  pass.
- **Clean abstractions; no leaky generics.** If a generic layer needs to
  know something source-specific to work, the abstraction boundary is in the
  wrong place — move the concern, don't parameterize around it.
- **Prefer the native mechanism over hand-rolling.** When a dependency, the
  stdlib, or an existing module already covers a need, use it. Examples:
  clap's `env = "CHAINFOLD_STATS"` attribute (self-documenting in `--help`,
  flag-beats-env precedence) replaced a hand-written `std::env::var` check
  beside the arg it duplicated; `Duration::from_hours(24)` over
  `from_secs(24 * 60 * 60)`.

---

## High-level (architecture)

### The source-agnostic core names no source

Generic types and combinators (`Position`, `Hist`, `HistStore`, `Signal`,
merge, matcher, `map`/`filter`/`since`) must not reference EVM, Bitcoin,
Solana, CEX, or any concrete source — neither in implementation nor in tests.
EVM is one source among many, not the canonical one.

- Tests for generic components use synthetic fixtures (`MockPos`, `MockSpec`)
  — never `EvmPosition`.
- The `Position` trait stays minimal: total order + marshal. No `side()`, no
  `source_id()`, no `block_hash` — those belong one layer up, on the source's
  store/event types.
- When tempted to special-case a generic type for "what EVM needs," push the
  concern down into the EVM module instead.

### Concrete first, traits later

Don't abstract until 1–2 implementations *and a client that needs the
abstraction* exist. `EvmChain` and `BitcoinChain` are deliberate concrete
siblings — three `RangeSpec` impls and two chains still haven't produced a
client demanding a `Chain` trait, so none exists (decision #6). Speculative
traits are a review flag.

### Reactive, not poll

The product thesis is reactive histories; poll-shaped APIs and hidden RPC
undermine it.

1. **Polling only in low-level RPC code.** Retry/backoff/sleep loops live in
   the source-side task next to the transport (head clock, ws sessions).
   Generic/reactive layers (`RangeIndexStore`, combinators, merge) only wait
   on channels. Demand signals are synchronous fire-and-forget
   (`TipSeed::request()`); delivery flows back through normal wiring.
2. **Traits over `Fn`-pointer aliases** for injection seams (`TimePos`,
   `TipSeed`, `HeadClock`, `BlockTimeSource`) — a named trait with an
   implementing struct, not a closure type alias.
3. **No poll-shaped public APIs for reactive facts.** Before adding a
   fetch-and-return method, check whether an existing store/snapshot already
   carries the fact. The head is the `end()` of a snapshot — that's why there
   is no `head()` getter ("who needs a bare head?"); "last N blocks" is
   `blocks().next()` then `split(tip − N)`.

Reactive-stream footguns (watch-coalescing, emit-on-advance) are documented
in [`ARCHITECTURE.md` §14](./ARCHITECTURE.md#14-ownership--lifetime).

### No dyn erasure in public composition

Cross-source composition stays fully typed: different position types unify
via an **app-defined closed enum** lifted with `map_positions`, not via
`Box<dyn Position>`. Chain *sets* are runtime-dynamic (source keys); chain
*families* are compile-time-bounded by which sources are compiled in — the
enum costs no real flexibility.

### Observability is a contract, not logging

Use `tracing`, never `println!` (demos/CLI may print their *output* to
stdout; diagnostics go through tracing). The span contract
([`ARCHITECTURE.md` §15](./ARCHITECTURE.md#15-observability-contract)):

- **Instrument boundaries, not call sites.** RPC spans are emitted by the
  transport layer (`RpcTimingLayer`, `TracingTransport`) so a new call site
  cannot be forgotten and zero-RPC test guards see every request.
- **Open vocabulary.** Span targets/names are plain strings aggregated
  generically by `(target, name)` — no per-label enums, no consumer changes
  to add a span. (A deliberate exemption from the no-blind-primitives rule:
  span labels aren't domain values — nothing parses them except the
  aggregator, and enum ceremony taxes instrumenting liberally. `CacheLayer`/
  `CacheOutcome` remain closed enums because they *are* parsed back.)
- **Dynamic labels are fields.** `tracing` span names are static callsite
  metadata — the RPC method travels in a `method` field on a span named
  `rpc`.
- Single-call-site span helpers are local `macro_rules!` living next to their
  emitters (`sqlite_span!`, `setup_span!`), not in a central module.
- **Diagnostics ride tracing, not return types.** Don't thread a count or
  label through an API purely to decorate a report line (a refresh's
  skipped-entry count is a `tracing::debug!` at the skip site, not a struct
  field). If no caller branches on a value, it isn't a return value.

---

## Low-level (code)

### No blind primitives

Model bounded vocabularies and domain quantities as closed enums or newtypes
— never bare `bool`/`String`/`&str`/`u64`/`u32`. This applies at APIs, in
`BTreeMap`/`HashMap` keys, in test fixtures, and in plans/sketches.

- **Numbers:** `BlockNumber`, `LogIdx`, `TxCount`, `Gas`, `ChunkSize`,
  `Timestamp`, … A block number can't be swapped with a chain id; block
  arithmetic is the affine algebra (`block ± u64 = block`; `block − block =
  u64` via `.get()`).
- **Strings:** `SourceKey`, `ChainHandle`, `Url`, event names… If a string
  has a vocabulary, it's an enum; if it has an identity, it's a newtype.
- **Hashes:** `BlockHash`/`TxHash` are real newtypes (EVM-specific);
  cross-family code uses the opaque `CoverageHash`. `FilterHash` is not an
  EVM hash — separate newtype.
- **Paths are `Path`/`PathBuf`**, not `String` (a string-typed path already
  caused a real bug: `rsplit('.')` corrupting dotted directory names). APIs
  take `impl AsRef<Path>`.
- **Byte counts are `bytesize::ByteSize`** — a type with a formatting trait,
  not a `u64` + `format_bytes()` helper.
- **SQL:** `FromSql`/`ToSql` impls live in the **cache layer**, not on the
  domain type (the type stays persistence-free); row reads are name-based,
  not positional.

**The boundary-funnel pattern.** Raw primitives are fine *at* the boundary
(RPC wire structs, SQLite columns) — they're name-bound at both ends there.
The rule's force applies at the funnel: convert **once** into the domain
type of the joint decision, exhaustively — and put the funnel at the
**earliest seam available**.

For clap that seam is the arg itself: value-taking fields carry domain
types via `value_parser`, so the raw `String`/`u64` field never exists —
`Option<ChainHandle>`, `Option<ChainId>`, `Vec<TokenRef>` (an
address-or-symbol enum: the parser commits `0x…` to the address arm, so bad
hex errors instead of leaking through as a "symbol"). Parser **error types
are domain types too**: reuse the upstream error (`FromHexError`,
`ParseIntError`) rather than `String` — invent a wrapper only when no
existing error fits. A flag shared by several subtrees is one top-level
`global = true` arg (`--db`), not per-subtree copies.

Clap `bool` *presence* flags are the exception — `value_parser` never sees
an absent flag, so they stay raw and funnel right after clap:
`Direction::from_reverse(bool)`, `FollowMode::from_follow(bool)`, purge
flags → `Vec<CacheSection>` via a per-section match (so a new section can't
be silently ignored), `HexDetail::from_full(bool)` for the `--full`
value-detail flag; an `Enable/Disable` enum would rename the bool without
adding swap-safety. Non-clap funnels work the same way: `BtcAuth` funneling
four auth flags/configs, `Out::stdout()`/`Out::stderr()` sniffing "is this
stream a TTY?" once and binding the resolved `RenderMode` to its stream (no
ad-hoc `is_terminal()` downstream).

**Low-churn tactics:** coordinate constructors keep raw params and wrap
internally (`EvmPosition::new(u64, u32)` — position literals in tests don't
churn); `#[serde(transparent)]` on wire-visible newtypes keeps marshals (and
saved resume anchors) byte-identical to the pre-newtype encoding; closed-enum
`ALL` consts are `&[Self]` slices (length inferred from the value), not
`[Self; N]` arrays that hand-count variants.

### Shared ownership is spelled `share()`

`Arc` refcount bumps are written `x.share()` (the `Share` extension trait,
`src/share.rs`), never `x.clone()` — a `.clone()` is too easy to misread as
a deep copy. Enforced by `clippy::clone_on_ref_ptr` in `Cargo.toml`.
Composite `#[derive(Clone)]` structs still clone normally.

### Lints are load-bearing

`Cargo.toml` sets `unsafe_code = "deny"`, clippy `all` + `pedantic` at warn
(CI runs `-D warnings`), plus targeted lints that encode past review
feedback: `absolute_paths` (write a `use`, not a `crate::a::b::c` chain),
`redundant_clone`, `needless_pass_by_value`, `clone_on_ref_ptr`,
`unused_async`, explicit-iter loops. Don't `#[allow]` around a lint the
table turned on without a comment saying why.

### Comments and docs are concise and DRY

State a fact once, at its owning site (the arg's doc comment, the type doc,
the relevant `docs/` section); elsewhere point, don't paraphrase. A comment
restating semantics already documented on the item beside it is rot waiting
to happen. Keep only what the code can't show.

### Tests

- Unit tests live alongside code as `#[cfg(test)] mod tests`; shared in-src
  fixtures as `tests_common.rs` modules; integration tests in `tests/`.
- Tests needing external processes or paid RPC (`anvil`, fork,
  regtest-bitcoind) are `#[ignore]`-gated; the fast suite must stay ~1 s.
- Generic-layer tests use `MockPos`/`MockSpec`, never a concrete source's
  types (see the source-agnostic rule above).
- Assert *behavior through the contract* where possible — RPC-count guards
  ride the observability spans (`RpcCounter`), so "warm walk = 0
  `eth_getLogs`" is test-enforced, not aspirational.
- A behavioral footgun fix gets a regression test pinned to the failure mode
  (and should fail against the pre-fix code — verify that once).
