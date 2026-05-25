//! Thin wrapper around [`alloy::transports::mock::Asserter`] for tests that
//! need to pin RPC-call counts (singleflight, reconnection paths, etc.).
//!
//! Asserter is a FIFO of pre-queued responses with no built-in call counter.
//! The contract here: tests pre-push exactly N responses for the call they
//! expect to be made N times, then [`remaining`] is `0` if and only if all N
//! were consumed. If the code under test issues an unexpected call, the
//! Asserter returns nothing and the call fails — which surfaces in the test
//! as a clear error rather than a silent miscount.

use std::sync::Arc;

use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::Log,
    transports::mock::Asserter,
};

/// Build a fresh [`Asserter`] with `times` empty `eth_getLogs` responses
/// already queued. Pair with [`remaining`] at end-of-test.
pub fn expect_get_logs(times: usize) -> Arc<Asserter> {
    let asserter = Arc::new(Asserter::new());
    let empty: Vec<Log> = Vec::new();
    for _ in 0..times {
        asserter.push_success(&empty);
    }
    asserter
}

/// Number of unconsumed responses remaining in the queue. `0` means all
/// expected calls fired; non-zero means fewer calls than expected.
pub fn remaining(asserter: &Asserter) -> usize {
    asserter.read_q().len()
}

/// Wire a mocked transport on top of an [`Asserter`] and return the
/// resulting [`Provider`]. Tests should hold the Arc<Asserter> separately
/// so they can introspect [`remaining`] after exercising the code under
/// test.
pub fn mock_provider(asserter: &Arc<Asserter>) -> impl Provider + Clone + 'static + use<> {
    ProviderBuilder::new().connect_mocked_client((**asserter).clone())
}
