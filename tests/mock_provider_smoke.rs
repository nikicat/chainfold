//! Smoke tests for the mock-provider helper (CCP-2).
//!
//! Confirms the Asserter wrapping and `remaining()` semantics work as
//! Phase 1's singleflight test will rely on them.

mod common;

use alloy::providers::Provider;

use common::mock_provider::{expect_get_logs, mock_provider, remaining};

#[tokio::test]
async fn mock_provider_consumes_queued_responses() {
    let asserter = expect_get_logs(2);
    let provider = mock_provider(asserter.clone());

    assert_eq!(
        remaining(&asserter),
        2,
        "asserter starts with 2 pre-queued responses"
    );

    let filter = alloy::rpc::types::eth::Filter::new();
    let logs1 = provider
        .get_logs(&filter)
        .await
        .expect("first mocked get_logs should succeed");
    assert!(logs1.is_empty());

    assert_eq!(remaining(&asserter), 1, "one response consumed");

    let logs2 = provider
        .get_logs(&filter)
        .await
        .expect("second mocked get_logs should succeed");
    assert!(logs2.is_empty());

    assert_eq!(
        remaining(&asserter),
        0,
        "all queued responses consumed; future calls would fail"
    );
}
