//! The always-available fallback adaptor for unrecognized endpoints.

use alloy::transports::TransportError;
use reqwest::Url;

use super::{
    ApiKey, Concurrency, ErrorClass, FetchSeeds, ProviderAdaptor, RpcTransport,
    classify_http_status,
};
use crate::evm::types::ChainId;

/// Knows nothing about the endpoint: classifies protocol-level signals
/// only and returns [`ErrorClass::Transient`] for everything else, so the
/// executor's bounded-retry-then-bisect handling degrades an unknown
/// provider to discovery, never to failure. No URL shape, no key.
#[derive(Debug)]
pub(super) struct GenericAdaptor;

impl ProviderAdaptor for GenericAdaptor {
    fn classify(&self, error: &TransportError) -> ErrorClass {
        classify_http_status(error).unwrap_or(ErrorClass::Transient)
    }

    fn seeds(&self, _chain: ChainId) -> FetchSeeds {
        FetchSeeds {
            max_range: None,
            max_logs: None,
            concurrency: Concurrency::new(4),
        }
    }

    fn detect(&self, _url: &Url) -> Option<ApiKey> {
        None
    }

    fn endpoint(&self, _chain: ChainId, _key: &ApiKey, _kind: RpcTransport) -> Option<Url> {
        None
    }
}

#[cfg(test)]
mod tests {
    use alloy::transports::TransportErrorKind;

    use super::*;

    #[test]
    fn classifies_unknown_as_transient_and_429_as_rate_limited() {
        let adaptor = GenericAdaptor;

        let rate_limited = TransportErrorKind::http_error(429, "too many requests".into());
        assert_eq!(
            adaptor.classify(&rate_limited),
            ErrorClass::RateLimited { retry_after: None },
            "bisecting on a rate limit would raise request count — decision 3"
        );

        let auth = TransportErrorKind::http_error(401, "no".into());
        assert_eq!(adaptor.classify(&auth), ErrorClass::Fatal);

        let unknown_payload = super::super::error_from_fixture(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"whatever"}}"#,
        );
        assert_eq!(adaptor.classify(&unknown_payload), ErrorClass::Transient);

        let wire = TransportErrorKind::custom_str("connection reset");
        assert_eq!(adaptor.classify(&wire), ErrorClass::Transient);
    }
}
