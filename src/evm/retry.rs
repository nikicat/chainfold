use alloy::transports::{
    RpcError, TransportError,
    layers::{OrRetryPolicyFn, RateLimitRetryPolicy, RetryBackoffLayer},
};

/// Number of retry attempts for transient/rate-limited RPC errors.
const MAX_RETRIES: u32 = 3;
/// Initial backoff between retries, in milliseconds (grows on each attempt).
const INITIAL_BACKOFF_MS: u64 = 500;
/// Compute-units-per-second budget the backoff layer paces requests against.
const COMPUTE_UNITS_PER_SECOND: u64 = 100;

/// Returns a `RetryBackoffLayer` that retries transient RPC errors.
///
/// Extends alloy's default rate-limit retry policy with:
/// - JSON-RPC `-32603` (internal error) and `-32000` (server error)
/// - All transport-level errors (connection reset, timeout, etc.)
///
/// Config: see [`MAX_RETRIES`], [`INITIAL_BACKOFF_MS`],
/// [`COMPUTE_UNITS_PER_SECOND`].
pub fn rpc_retry_layer() -> RetryBackoffLayer<OrRetryPolicyFn> {
    let policy = RateLimitRetryPolicy::default().or(|e: &TransportError| match e {
        RpcError::ErrorResp(payload) => matches!(payload.code, -32603 | -32000),
        RpcError::Transport(_) => true,
        _ => false,
    });
    RetryBackoffLayer::new_with_policy(
        MAX_RETRIES,
        INITIAL_BACKOFF_MS,
        COMPUTE_UNITS_PER_SECOND,
        policy,
    )
}
