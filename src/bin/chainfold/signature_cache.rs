//! Adapter: implements [`SignatureCache`] over [`LogStore`] so the CLI
//! reuses the same `SQLite` file for event-signature caching.

use std::sync::Arc;

use async_trait::async_trait;
use chainfold::evm::LogStore;
use chainfold::signatures::{SignatureCache, SignatureError};

pub struct LogStoreSignatureCache {
    store: Arc<LogStore>,
}

impl LogStoreSignatureCache {
    pub fn new(store: Arc<LogStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SignatureCache for LogStoreSignatureCache {
    async fn get(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
    ) -> Result<Option<(String, [u8; 32])>, SignatureError> {
        self.store
            .get_event_signature(chain_id, &address, event_name)
            .map_err(|e| SignatureError::Cache(e.to_string()))
    }

    async fn put(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
        signature: &str,
        topic0: [u8; 32],
    ) -> Result<(), SignatureError> {
        self.store
            .insert_event_signature(chain_id, &address, event_name, signature, &topic0)
            .map_err(|e| SignatureError::Cache(e.to_string()))
    }
}
