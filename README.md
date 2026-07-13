# chainfold

[![CI](https://github.com/nikicat/chainfold/actions/workflows/ci.yml/badge.svg)](https://github.com/nikicat/chainfold/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/nikicat/chainfold/branch/main/graph/badge.svg)](https://codecov.io/gh/nikicat/chainfold)

**Treat blockchain event histories like local data.** Chainfold gives you the
illusion of working against a local full node — caching backfill, an in-memory
range index, sub-10 ms live push, resumable positions, and cross-source merge /
correlation — without ever downloading a chain.

One engine, three surfaces: an embeddable **Rust library**, a `cast`-like
**CLI**, and a transparent caching-RPC **daemon** (`chainfoldd`, planned).

> **Status:** under active development. The library and CLI ship today; the
> daemon is planned. Two source families are live — EVM (logs + blocks) and
> Bitcoin (blocks) — over one shared engine, shaped to take Solana and
> CEX/perp-DEX sources next.

## Two guarantees

1. **Minimal RPC.** Any block/time range is fetched *at most once*. Later reads
   hit local SQLite; partial overlaps fetch only the gap; concurrent readers of
   the same cold range are coalesced into one call. A warm walk issues zero
   RPC and is orders of magnitude faster — compare two `--stats` runs of the
   same query.
2. **Real-time delivery.** Live logs reach the consumer in single-digit ms
   (~3 ms over WebSocket on local anvil); the internal hop is sub-millisecond
   because the live path never touches SQLite.

## What you get

- **Cached backfill** — ask for a range once; later reads are local and offline-capable.
- **Lazy iteration** — a wide range returns a snapshot object, not a buffered `Vec`; cancel by dropping the stream.
- **Resumable positions** — every item carries a serializable position; persist it, restart, resume.
- **Live push** — `subscribe()` delivers WebSocket logs into memory synchronously; reconnect gap-fill is invisible.
- **Subscribe-from-the-past** — iterate from an old anchor and roll transparently into the live tip.
- **Merge & correlate** — combine N sources into one chronological stream; pair `src ⇄ dst` into `{ matched, pending, orphans }`.

## Quickstart

```bash
cargo build --features evm
cargo test  --all-features    # full suite, ~1s
```

Walk recent USDC transfers for an owner (latest 200 blocks), with a running
balance column:

```bash
cargo run --features cli -- evm --num-blocks 200 erc20 \
  --token USDC \
  --owner 0xYourAddressHere \
  balances                  # add --follow to stream live
```

`--token` takes a symbol (resolved via the cached [token
list](https://tokenlists.org) — see `chainfold tokens list`) or a raw
`0x…` contract address.

As a dependency:

```toml
[dependencies]
chainfold = { version = "0.1", features = ["evm"] }
```

## Features

| Feature | Default | Pulls in |
|---|---|---|
| `evm` | off | `alloy` (HTTP + WS) |
| `bitcoin` | off | `jsonrpc` (framing only; the wire transport is reqwest) |
| `cli` | off | implies `evm` + `bitcoin`; adds `clap`, `tracing-subscriber`, `chrono`, `toml`, `dirs` |

## Supported sources

| Source | Status |
|---|---|
| EVM (any chain via `alloy`) — logs + blocks | ✅ Tested on Ethereum mainnet, Arbitrum |
| Bitcoin (via bitcoind JSON-RPC) — blocks | ✅ Tested against regtest bitcoind |
| Solana / CEX / perp-DEX | ⏳ Not started; the backend shape is ready to accept them |

Adding a source means implementing one backend trait; combinators, merge, and
matcher then work automatically.

## Documentation

| Doc | What |
|---|---|
| [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) | As-built architecture reference — diagrams, the client/backend trait layering, data flow, storage model, observability, locked-in design decisions. |
| [`docs/ROADMAP.md`](./docs/ROADMAP.md) | Forward plan — the named phases (adaptive fetch, finality), deferred features, product-surface tracks, backlog. |
| [`docs/STYLE.md`](./docs/STYLE.md) | Coding style & design preferences — high-level architectural rules and low-level code conventions. |
| [`docs/DEVELOPMENT.md`](./docs/DEVELOPMENT.md) | Dev setup — test suites and their external dependencies, pre-push CI gate, coverage. |

The `chainfold` CLI also ships a `--stats` flag that reports RPC counts and
cache hit/miss at exit — see [ARCHITECTURE.md](./docs/ARCHITECTURE.md#15-observability-contract) —
and a repeatable `-v`/`--verbose` flag for diagnostic logs on stderr (warnings
are on by default; `-v` chainfold debug, `-vv` all-crate debug, `-vvv` trace;
layered over `RUST_LOG`).

## Running tests

```bash
cargo test --features evm                # fast lib + source-agnostic suite (~1s)
cargo test --all-features                # + bitcoin + CLI-bin unit tests
cargo test --all-features -- --ignored   # anvil / fork / regtest suites
```

The `#[ignore]`-gated suites need external processes (`anvil`, regtest
`bitcoind`) and, for fork tests, an archive `MAINNET_RPC_URL` in a gitignored
`.env` — see [`docs/DEVELOPMENT.md`](./docs/DEVELOPMENT.md).

## License

TBD.
