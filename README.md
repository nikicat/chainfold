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
> daemon is planned. EVM is the first backend; the source-agnostic core is
> shaped to take Solana, Bitcoin, and CEX/perp-DEX sources.

## Two guarantees

1. **Minimal RPC.** Any block/time range is fetched *at most once*. Later reads
   hit local SQLite; partial overlaps fetch only the gap; concurrent readers of
   the same cold range are coalesced into one call. A warm walk is ~98× faster
   than the cold one on a mainnet fork.
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

Walk recent USDC transfers for an owner, then follow live:

```bash
cargo run --features evm --example erc20_explorer -- \
  --address 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
  --owner   0xYourAddressHere \
  --num-blocks 200          # add --follow to stream live
```

As a dependency:

```toml
[dependencies]
chainfold = { version = "0.1", features = ["evm"] }
```

## Features

| Feature | Default | Pulls in |
|---|---|---|
| `evm` | off | `alloy` (HTTP + WS), `blake3`, `tracing` |
| `cli` | off | implies `evm`; adds `clap`, `tracing-subscriber`, `chrono`, `toml`, `dirs` |

## Supported sources

| Source | Status |
|---|---|
| EVM (any chain via `alloy`) | ✅ Tested on Ethereum mainnet, Arbitrum |
| Solana / Bitcoin / CEX / perp-DEX | ⏳ Not started; the backend shape is ready to accept them |

Adding a source means implementing one backend trait; combinators, merge, and
matcher then work automatically.

## Documentation

| Doc | What |
|---|---|
| [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) | Architecture reference — diagrams, the client/backend trait layering, data flow, storage model, observability. |
| [`docs/refactor.md`](./docs/refactor.md) | Phase status and roadmap (Phase 7 + deferrals). |
| [`docs/TODO.md`](./docs/TODO.md) | Active backlog, incl. the reactive-core refactor plan (R-series). |
| [`docs/blockchain_storage.md`](./docs/blockchain_storage.md) | Original pre-refactor design brainstorm (historical). |

The `chainfold` CLI also ships a `--stats` flag that reports RPC counts and
cache hit/miss at exit — see [ARCHITECTURE.md](./docs/ARCHITECTURE.md#15-observability-contract).

## Running tests

```bash
cargo test --features evm            # fast lib + source-agnostic suite (~1s)
cargo test --all-features            # + CLI-bin unit tests
cargo test --features evm -- --ignored   # anvil / fork tests (need `anvil`; MAINNET_RPC_URL in .env)
```

A gitignored `.env` with `MAINNET_RPC_URL=https://…` supplies your archive RPC
for fork tests; the harness falls back to a public endpoint if unset.

## License

TBD.
