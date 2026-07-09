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

**`MAINNET_RPC_URL`:** fork tests pin `FORK_BLOCK = 18_000_000`
(`tests/common/fixtures.rs`), which needs an **archive** endpoint — free
public RPCs (llamarpc, publicnode, cloudflare-eth) don't serve archival state
that deep; a free-tier Alchemy key works. Put it in a gitignored `.env` at
the repo root:

```
MAINNET_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<key>
```

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
