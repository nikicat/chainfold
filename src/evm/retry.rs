use alloy::transports::{
    RpcError, TransportError,
    layers::{OrRetryPolicyFn, RateLimitRetryPolicy, RetryBackoffLayer},
};

/// Returns a `RetryBackoffLayer` that retries transient RPC errors.
///
/// Extends alloy's default rate-limit retry policy with:
/// - JSON-RPC `-32603` (internal error) and `-32000` (server error)
/// - All transport-level errors (connection reset, timeout, etc.)
///
/// Config: 3 retries, 500ms initial backoff, 100 CU/s budget.
pub fn rpc_retry_layer() -> RetryBackoffLayer<OrRetryPolicyFn> {
    let policy = RateLimitRetryPolicy::default().or(|e: &TransportError| match e {
        RpcError::ErrorResp(payload) => matches!(payload.code, -32603 | -32000),
        RpcError::Transport(_) => true,
        _ => false,
    });
    RetryBackoffLayer::new_with_policy(3, 500, 100, policy)
}
