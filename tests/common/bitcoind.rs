//! Spawn regtest [`bitcoind`] instances for integration tests via
//! `corepc-node` — the Bitcoin analog of [`anvil`](super::anvil).
//!
//! [`local()`] starts a throwaway regtest node (fresh datadir, cookie auth,
//! default wallet created, killed on drop). The binary is resolved from
//! `$BITCOIND_EXE`, falling back to `bitcoind` on `PATH` — no auto-download
//! (hermetic builds; matches the anvil-on-PATH convention). Returns [`None`]
//! when no binary is available, letting callers print a "skipped" message
//! rather than failing the suite on a machine without Bitcoin Core.

use std::process::Command;

use corepc_node::Node;

/// The env var overriding which bitcoind binary to launch.
const EXE_ENV: &str = "BITCOIND_EXE";

/// The bitcoind binary to use: `$BITCOIND_EXE`, else `bitcoind` (PATH).
fn exe() -> String {
    std::env::var(EXE_ENV).unwrap_or_else(|_| "bitcoind".to_owned())
}

/// Return `true` if the resolved bitcoind binary runs. Cheap check.
pub fn bitcoind_available() -> bool {
    Command::new(exe())
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

/// Start a throwaway regtest node, or [`None`] when no binary is available.
pub fn local() -> Option<Node> {
    if !bitcoind_available() {
        return None;
    }
    Some(Node::new(exe()).expect("start regtest bitcoind"))
}

/// Print the standard "skipped" note for a bitcoind-gated test.
pub fn skip_no_bitcoind(test_name: &str) {
    eprintln!("skipping {test_name}: bitcoind not found (set {EXE_ENV} or add it to PATH)");
}
