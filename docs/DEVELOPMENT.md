# Chainfold — Development setup

> Building, testing, and shipping changes. Design docs:
> [`ARCHITECTURE.md`](./ARCHITECTURE.md) · [`ROADMAP.md`](./ROADMAP.md) ·
> [`STYLE.md`](./STYLE.md).

## Build & test

```bash
cargo build --features evm
cargo test  --features evm               # fast lib + source-agnostic suite (~1s)
cargo test  --all-features               # + bitcoin + CLI-bin unit tests
cargo test  --all-features -- --ignored  # anvil / fork / regtest suites (see below)
just                                     # list all recipes (test, coverage, …)
```

## External-process test suites (`#[ignore]`-gated)

| Suite | Needs |
|---|---|
| `tests/anvil_smoke.rs`, `tests/blocks_history.rs`, `tests/live_follow.rs` | `anvil` on `PATH` (foundry) |
| `tests/cache_layering.rs`, `tests/owned_erc20.rs`, the fork case of `anvil_smoke` | `anvil` + `MAINNET_RPC_URL` (archive node — see below) |
| `tests/bitcoin_blocks_history.rs` | `bitcoind` via `$BITCOIND_EXE` or `PATH` (regtest through `corepc-node`; no network) |
| the `#[ignore]` smokes in `tests/adaptive_fetch.rs`; the live pairing probes in `src/evm/provider/` | `ALCHEMY_KEY` (+ `INFURA_KEY` for the Infura probe) in the env or `.env`. The rest of `tests/adaptive_fetch.rs` is hermetic (in-process mock) and runs in the normal suite. |

**`MAINNET_RPC_URL`:** fork tests pin `FORK_BLOCK = 18_000_000`
(`tests/common/fixtures.rs`), which needs an **archive** endpoint — free
public RPCs (llamarpc, publicnode, cloudflare-eth) don't serve archival state
that deep; a free-tier Alchemy key works. Put it in a gitignored `.env` at
the repo root:

```
MAINNET_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<key>
ALCHEMY_KEY=<key>
INFURA_KEY=<key>
```

The bare provider keys feed the CLI's endpoint autoconfig, the
adaptive-fetch smokes, and the network-table probes/generator
(`just refresh-provider-networks`).

`tests/common/anvil.rs::load_dotenv()` loads it via `dotenvy` on the first
`fork_mainnet()` call — no shell exports needed. The harness falls back to a
public endpoint if unset (expect flakiness/rate limits). If fork tests fail
with "historical state … is not available", the endpoint isn't archival.

## Before pushing — run the CI gate locally

Pre-commit hooks are **per-clone** (`prek install` once after cloning); a
clone without them accumulates latent style drift that then fails CI in
cascades. The full gate:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
taplo fmt --check && taplo lint
cargo test --all-features --locked
```

CI (`.github/workflows/ci.yml`) runs the same, plus a parallel coverage job.

**Everything is version-pinned.** The Rust toolchain is pinned in
`rust-toolchain.toml` (channel + components) — rustup resolves local builds
and both CI jobs through it, so local and CI stay in lockstep by
construction; bumping it is a one-line edit (rustup auto-installs the new
version on the next `cargo` invocation). Both CI jobs run on stock
`ubuntu-latest` runners; in `ci.yml`, every action is pinned by commit SHA
(`# vX.Y.Z` comment alongside) and every gate tool by version
(taplo/just/shellcheck/cargo-llvm-cov via `taiki-e/install-action`,
actionlint by checksummed release tarball, yamllint via pipx), so CI only
moves when a pin is bumped — a new stable's clippy lints land when *you*
bump, not on whoever pushes next. To bump: take new action SHAs and tool
versions from each repo's tags (and the actionlint tarball's published
sha256).

**Pushing workflow changes:** a `gh`-authenticated push touching
`.github/workflows/*` needs the `workflow` OAuth scope — the default `repo`
scope is rejected by GitHub. Fix interactively with
`gh auth refresh -h github.com -s workflow` (don't recreate the repo; the
scope is the only missing piece).

## Coverage

`cargo-llvm-cov` locally, Codecov in CI (repo is private —
`CODECOV_TOKEN` is mandatory; tokenless OIDC upload does not work for
private repos).

```bash
just coverage        # terminal summary
just coverage-html   # target/llvm-cov/html/index.html
```

- The ignore list (CLI glue + test helpers: `examples/`, `src/bin/`,
  `tests/common/`, `tests_common.rs`) is kept in sync across `justfile`,
  `ci.yml`, and `codecov.yml`.
- Gates (`codecov.yml`): **patch ≥ 80 %** (new-code gate), **project
  threshold 1 %** (trend gate). If the patch gate becomes painful on small
  diffs, lower `coverage.status.patch.default.target`.
- Anvil/fork suites are excluded from CI coverage (they're `#[ignore]`d);
  counting them would need a second CI job with `--include-ignored` + anvil
  + a `MAINNET_RPC_URL` secret.
