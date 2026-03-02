# Technology Stack

**Project:** Chainfold — Rust blockchain event sourcing / state reconstruction library
**Researched:** 2026-03-02
**Researcher note:** External network tools (WebSearch, WebFetch, Context7) were unavailable during this research session. All findings are based on training data through August 2025. Confidence levels reflect this constraint. Version numbers MUST be verified against crates.io before pinning in Cargo.toml.

---

## Recommended Stack

### EVM Client Layer

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `alloy` | ~0.11 (verify) | EVM RPC, types, ABI, event decoding | Active successor to ethers-rs; maintained by the same core team. Provider abstraction is cleaner, type system is stronger, and it is the direction the reth ecosystem has standardized on. |
| `alloy-rpc-types` | bundled with alloy | Ethereum RPC type definitions | Part of alloy workspace; use instead of rolling your own. |
| `alloy-sol-types` | bundled with alloy | Solidity ABI encoding/decoding, log parsing | Compile-time ABI via `sol!` macro — zero-cost event decoding at the hot path. |

**Why alloy over ethers-rs:** ethers-rs entered maintenance-only mode in late 2023. The alloy crate was created by the same authors as the explicit migration target. As of mid-2025, alloy has reached production stability. The reth node itself uses alloy primitives, so the type compatibility story is excellent.

**Why alloy over reth primitives directly:** reth primitives (`reth-primitives`) are node-internal types tied to reth's full-node data model. Using them would create an unnecessary coupling to a node implementation. alloy provides the same EVM type system without the node dependency baggage.

**Confidence: HIGH** — this migration/succession is well-documented and community consensus is clear as of August 2025.

---

### Bitcoin Client Layer

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `bitcoincore-rpc` | ~0.19 (verify) | Bitcoin Core JSON-RPC client | The canonical crate maintained under the `rust-bitcoin` umbrella. Wraps the Bitcoin Core RPC API with typed structs. It is the standard choice — no credible alternative exists. |
| `bitcoin` | ~0.32 (verify) | Bitcoin primitive types (Transaction, Block, Script, etc.) | Also from the `rust-bitcoin` project. `bitcoincore-rpc` depends on it; use it directly for any type-level work. |

**Why bitcoincore-rpc over rolling your own:** Bitcoin Core's RPC API is large and idiosyncratic. `bitcoincore-rpc` handles authentication, batch requests, and deserialization of the full RPC surface. Rolling your own for a library would be a significant maintenance burden.

**CEX-specific note:** Bitcoin has no native WebSocket subscription API at the protocol level. Block arrival notifications come via ZMQ (`zmq` crate) or polling. If Chainfold needs live Bitcoin block events, document that `EventSource` implementors for Bitcoin will poll or use ZMQ — not WebSocket — as the transport.

**Confidence: HIGH** — `rust-bitcoin` / `bitcoincore-rpc` is the unambiguous standard.

---

### CEX WebSocket/REST Client Patterns

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `tokio-tungstenite` | ~0.26 (verify) | Async WebSocket client | The standard tokio-native WebSocket library. Used by essentially every Rust CEX client implementation. Wraps `tungstenite` with tokio I/O. |
| `reqwest` | ~0.12 (verify) | Async HTTP/REST client | The de facto standard Rust async HTTP client. Tokio-native, TLS support via rustls or native-tls, good streaming support. |
| `serde` + `serde_json` | ~1.x | JSON serialization for REST/WS messages | Non-negotiable. Every CEX API speaks JSON. |

**Pattern recommendation:** Chainfold's `EventSource` trait is source-agnostic by design. CEX implementors should be thin adapters — `reqwest` for REST snapshot fetch, `tokio-tungstenite` for live WebSocket events — that translate exchange-specific messages into the library's event type. Do not add an exchange-specific SDK dependency to the library's core; those belong in a separate `chainfold-cex-adapters` crate or the user's application.

**What NOT to use:**
- `hyper` directly — lower-level than needed; use `reqwest` which wraps it correctly.
- `async-tungstenite` — prefer `tokio-tungstenite` to stay within the tokio ecosystem and avoid feature flag confusion.

**Confidence: HIGH** — `tokio-tungstenite` + `reqwest` is the settled community standard.

---

### Async Runtime

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `tokio` | ~1.x (verify current 1.x patch) | Async runtime, timers, channels, I/O | No real alternative for Rust production async. The entire blockchain ecosystem (alloy, reqwest, tungstenite, sqlx) assumes tokio. Using anything else creates friction. |
| `tokio::sync::broadcast` / `tokio::sync::watch` | bundled | Event fan-out for `OnChange` callbacks | `broadcast` for multi-consumer live event delivery; `watch` for single-value "current state" subscriptions. Both fit Chainfold's `OnChange` and `Current()` APIs directly. |

**Feature flags:** Enable `tokio = { features = ["full"] }` during development, then prune to `["rt-multi-thread", "sync", "time", "io-util"]` before 1.0. This avoids pulling in the `tokio::process` and `tokio::net` listeners the library doesn't need.

**Confidence: HIGH** — tokio 1.x is the only serious choice.

---

### SQLite / Persistence Layer

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `rusqlite` | ~0.32 (verify) | SQLite with WAL mode, low-level control | Best fit for Chainfold's requirements. Thin, synchronous bindings give precise control over WAL mode, journal, PRAGMA tuning, and transaction boundaries. No macro magic, no ORM overhead. |
| `rusqlite` with `bundled` feature | same | Bundles SQLite amalgamation | Eliminates system SQLite version dependency — critical for a library that will be used across environments. |

**Why rusqlite over sqlx:**
- sqlx's async SQLite driver serializes all operations through a single thread internally anyway (SQLite is not truly concurrent). The async abstraction adds overhead without benefit for a library whose SQLite writes are on a background task.
- sqlx requires compile-time query checking against a live database, which complicates the build setup for a library.
- rusqlite gives direct access to `PRAGMA wal_checkpoint`, `PRAGMA journal_mode=WAL`, and `PRAGMA synchronous=NORMAL` — exactly what's needed for Chainfold's write patterns.

**Why rusqlite over diesel:**
- Diesel is an ORM. Chainfold's schema is intentional and controlled — it does not benefit from ORM abstractions.
- Diesel adds proc-macro complexity that is unnecessary here.

**Connection strategy:** One `rusqlite::Connection` per thread (rusqlite is `!Send`). Wrap in `Arc<Mutex<Connection>>` or run SQLite operations on a dedicated `std::thread` accessed via a channel. Do NOT use `tokio::task::spawn_blocking` for every query — instead, dedicate a single background thread to SQLite and send work to it via `tokio::sync::mpsc`. This avoids spawning a new thread per operation.

**Confidence: HIGH** for rusqlite choice. MEDIUM for specific version (verify on crates.io).

---

### Serialization for State Snapshots

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| `bincode` v2 | ~2.0 (verify) | Binary snapshot serialization | Fast, compact, zero-copy capable. Well-maintained. The v2 API is significantly cleaner than v1. Good fit for in-process snapshots where schema evolution is controlled by the library author. |

**Why bincode over alternatives:**

- **msgpack (`rmp-serde`):** Cross-language compatible but adds a dependency and is slower than bincode for pure Rust use. Chainfold's snapshots are internal (not cross-language yet). Revisit if/when Python/TS bindings are added.
- **borsh:** Designed for blockchain (Near Protocol). Good binary format, deterministic. Slightly less ergonomic than bincode v2 for ad-hoc struct serialization. Worth considering if Chainfold snapshots ever need cross-language determinism guarantees — but not needed for v1.
- **postcard:** Excellent for embedded/no_std. Not a priority here.
- **protobuf / flatbuffers:** Schema evolution is their strength, but the tooling overhead (separate .proto files, codegen) is overkill for internal snapshots in v1.

**Schema evolution note:** Bincode has no built-in schema evolution. Add a snapshot version field (`snapshot_version: u32`) to the root type so the library can detect and reject stale snapshots on restart rather than silently deserializing garbage. Document this as a required pattern.

**Confidence: MEDIUM** — bincode v2 is well-suited but verify it has reached stable release (it was in RC as of early 2025).

---

### Testing: Blockchain Integration Testing

| Technology | Version (verify) | Purpose | Why |
|------------|-----------------|---------|-----|
| Rust built-in `#[test]` + `tokio::test` | — | Unit and async integration tests | `#[tokio::test]` from the `tokio` crate handles async test setup. No separate test runner needed. |
| `anvil` (from Foundry) | latest | Local EVM node for integration tests | Industry standard for EVM testing. Fast forking, instant block production, deterministic state. Used via `anvil` binary or `alloy`'s `AnvilInstance` wrapper. |
| `tempfile` | ~3.x (verify) | Temporary SQLite files in tests | Critical: never hardcode `/tmp/test.db`. `tempfile::NamedTempFile` gives a unique, auto-deleted file per test. Required by security policy. |
| `wiremock` | ~0.6 (verify) | Mock HTTP server for CEX REST tests | Record/replay HTTP exchanges. Avoids live exchange dependency in CI. |
| `mockall` | ~0.13 (verify) | Mock trait implementations | For unit-testing library internals against mock `EventSource` implementations without live data. |
| `proptest` or `quickcheck` | ~1.x (verify) | Property-based testing | Gap recovery and reorg detection logic has complex invariants. Property tests are the right tool: "apply events in any order, state must always converge." |

**Integration test strategy:**
1. **Unit tests** — pure reducer logic (`Apply`), snapshot round-trips, gap detection algorithms. No I/O.
2. **SQLite integration tests** — schema migrations, WAL mode behavior, snapshot storage/retrieval. Use `tempfile`.
3. **EVM integration tests** — spin up `anvil`, deploy an ERC20, emit transfers, verify Chainfold reconstructs balance correctly. Slow tests, gated behind `#[cfg(feature = "integration")]`.
4. **CEX mock tests** — use `wiremock` + a fake WebSocket server to test the gap-stitching and restart-resilience logic without live exchange access.

**Confidence: HIGH** for tokio::test + anvil + tempfile. MEDIUM for wiremock/mockall (verify current versions).

---

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| EVM library | `alloy` | `ethers-rs` | Maintenance-only since late 2023; alloy is the explicit successor |
| EVM library | `alloy` | `reth-primitives` | Node-internal types; couples library to reth implementation details |
| SQLite | `rusqlite` | `sqlx` | Async SQLite in sqlx is fake async (serialized internally); adds build complexity with compile-time query checking |
| SQLite | `rusqlite` | `diesel` | ORM overhead; macro complexity; no benefit for a controlled internal schema |
| Snapshots | `bincode` v2 | `msgpack` | Cross-language not needed in v1; bincode faster for pure Rust |
| Snapshots | `bincode` v2 | `borsh` | Good alternative, but less ergonomic for ad-hoc structs; revisit if determinism matters |
| WebSocket | `tokio-tungstenite` | `async-tungstenite` | Prefer single ecosystem; tokio-tungstenite is more widely used |
| HTTP | `reqwest` | `hyper` | Hyper is lower-level than needed; reqwest wraps it correctly |
| HTTP | `reqwest` | `ureq` | ureq is sync-only; incompatible with tokio-native design |

---

## Installation (Cargo.toml sketch)

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time", "io-util"] }

# EVM
alloy = { version = "0.11", features = ["providers", "rpc-types", "sol-types"] }  # verify version

# Bitcoin
bitcoin = "0.32"           # verify version
bitcoincore-rpc = "0.19"  # verify version

# CEX transports
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-native-roots"] }  # verify version

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bincode = "2"  # verify stable release

# SQLite
rusqlite = { version = "0.32", features = ["bundled"] }  # verify version

[dev-dependencies]
tempfile = "3"
mockall = "0.13"   # verify version
wiremock = "0.6"   # verify version
proptest = "1"
```

**TLS note:** Prefer `rustls-tls` over `native-tls` everywhere (reqwest, tungstenite). `native-tls` links against system OpenSSL which creates environment-specific build failures. `rustls` is pure Rust and ships deterministically.

---

## Version Verification Checklist

Before finalizing Cargo.toml, verify current versions at crates.io:

- [ ] `alloy` — was ~0.8-0.11 range as of August 2025; check for 1.0 release
- [ ] `bitcoincore-rpc` — check 0.19.x vs any 0.20 release
- [ ] `bitcoin` — check 0.32.x stability
- [ ] `tokio-tungstenite` — check current 0.2x patch
- [ ] `reqwest` — check 0.12.x current patch
- [ ] `bincode` — confirm v2 has left RC and is stable
- [ ] `rusqlite` — check 0.32.x vs any 0.33 release
- [ ] `mockall`, `wiremock` — check current minor versions

---

## Sources

All findings based on training data through August 2025. No external sources were accessible during this research session (network tools blocked). Confidence levels are assigned conservatively:

- **HIGH**: Well-established ecosystem facts with strong consensus (tokio dominance, ethers-rs deprecation, alloy succession, rusqlite for WAL control)
- **MEDIUM**: Directionally correct but version numbers may have moved (bincode v2 stability, specific crate versions)
- **LOW**: None assigned — rather than speculate at LOW confidence, those areas are flagged for manual verification

**Official sources to verify against:**
- https://crates.io — version numbers for all crates
- https://alloy.rs — alloy documentation and migration guides
- https://github.com/rust-bitcoin/rust-bitcoincore-rpc — bitcoincore-rpc releases
- https://github.com/foundry-rs/foundry — anvil documentation
- https://docs.rs/bincode/latest/bincode/ — bincode v2 API status
