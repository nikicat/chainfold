# Project recipes. Run `just` to list, `just <recipe>` to invoke.
#
# Coverage uses cargo-llvm-cov (LLVM source-based coverage).
# Install once:  cargo install cargo-llvm-cov
# Or via pacman: pacman -S cargo-llvm-cov

# Filename regex passed to llvm-cov to drop CLI glue + test helpers from
# the report. We want coverage of the *library*, not of clap arg parsing
# or shared anvil/fixture plumbing. `tests_common\.rs` matches in-src
# #[cfg(test)] fixture modules (e.g. src/combinators/tests_common.rs).
ignore_regex := '(examples/|src/bin/|tests/common/|tests_common\.rs)'

# List available recipes.
default:
    @just --list

# Run the full fast test suite (matches CI; skips #[ignore]'d anvil tests).
test:
    cargo test --all-features --locked

# Run all #[ignore]'d tests too (needs anvil on PATH and MAINNET_RPC_URL for fork tests).
test-all:
    cargo test --all-features --locked -- --include-ignored

# Coverage summary in the terminal.
coverage:
    cargo llvm-cov --all-features --ignore-filename-regex '{{ ignore_regex }}'

# HTML report at target/llvm-cov/html/index.html.
coverage-html:
    cargo llvm-cov --all-features --html --ignore-filename-regex '{{ ignore_regex }}'

# HTML report + open in browser.
coverage-open:
    cargo llvm-cov --all-features --html --open --ignore-filename-regex '{{ ignore_regex }}'

# lcov.info for Codecov upload or external tools.
coverage-lcov:
    cargo llvm-cov --all-features --lcov --output-path lcov.info \
        --ignore-filename-regex '{{ ignore_regex }}'

# Drop cached .profraw / .profdata / HTML report.
coverage-clean:
    cargo llvm-cov clean --workspace
