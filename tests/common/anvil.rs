//! Spawn [`anvil`](https://book.getfoundry.sh/anvil) instances for integration
//! tests.
//!
//! Two flavors:
//! - [`local()`] — plain local chain (no fork). Fast (~500 ms) and offline.
//! - [`fork_mainnet()`] — forks Ethereum mainnet at a pinned block via
//!   `MAINNET_RPC_URL` (falls back to a public RPC). Slower; gated on
//!   `#[ignore]` in the smoke test and on the per-phase integration tests.
//!
//! Both helpers return [`None`] if the `anvil` binary isn't on `PATH`, letting
//! callers print a "skipped" message rather than failing the suite on a
//! contributor's machine without foundry installed.

use std::process::Command;
use std::sync::Once;

use alloy_node_bindings::{Anvil, AnvilInstance};
use chainfold::chain::BlockNumber;

/// Load a gitignored `.env` from the repo root on first call. Contributors
/// keep their `MAINNET_RPC_URL` (Alchemy etc.) there instead of editing
/// shell profiles. Idempotent and silent if no `.env` exists.
fn load_dotenv() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = dotenvy::dotenv();
    });
}

/// Fallback public RPC used by [`fork_mainnet`] when `MAINNET_RPC_URL` is
/// unset. Rate-limited; do not hammer in CI. Switched from llamarpc to
/// publicnode after observing 525 errors from Cloudflare-fronted endpoints
/// during anvil's startup chain-id probe.
pub const DEFAULT_FORK_RPC: &str = "https://ethereum-rpc.publicnode.com";

/// Return `true` if the `anvil` binary is on `PATH`. Cheap check.
pub fn anvil_available() -> bool {
    Command::new("anvil")
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
}

/// Spawn a plain local anvil instance. Returns `None` if anvil isn't on
/// `PATH` — callers should print a skip message and return early.
pub fn local() -> Option<AnvilInstance> {
    if !anvil_available() {
        return None;
    }
    Some(Anvil::new().spawn())
}

/// Spawn an anvil instance forking mainnet at `block`. RPC source comes
/// from `MAINNET_RPC_URL` if set, else [`DEFAULT_FORK_RPC`].
///
/// Returns `None` if anvil isn't on `PATH`. Panics with a clear message if
/// spawning fails (e.g. RPC unreachable). The 60s startup timeout is
/// deliberately generous because the public fallback RPC can be slow.
pub fn fork_mainnet(block: BlockNumber) -> Option<AnvilInstance> {
    if !anvil_available() {
        return None;
    }
    load_dotenv();
    let rpc = std::env::var("MAINNET_RPC_URL").unwrap_or_else(|_| DEFAULT_FORK_RPC.to_string());
    let instance = Anvil::new()
        .fork(rpc.clone())
        .fork_block_number(block.get())
        .timeout(60_000)
        .try_spawn()
        .unwrap_or_else(|e| {
            panic!(
                "failed to spawn anvil fork (block={block}, rpc={rpc}): {e}. \
                 Set MAINNET_RPC_URL to a faster endpoint if the public fallback is rate-limited."
            )
        });
    Some(instance)
}

/// Print a "skipped: anvil not on PATH" line and return — call this in
/// tests when [`local`] or [`fork_mainnet`] returns `None`.
pub fn skip_no_anvil(test_name: &str) {
    eprintln!("skipped: {test_name} (anvil binary not on PATH)");
}
