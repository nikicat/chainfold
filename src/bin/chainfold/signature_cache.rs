//! Adapter: implements [`SignatureCache`] over [`LogStore`] so the CLI
//! reuses the same `SQLite` file for event-signature caching.

use std::sync::Arc;

use async_trait::async_trait;
use chainfold::evm::LogStore;
use chainfold::signatures::{ResolvedEvent, SignatureCache, SignatureError};

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
    ) -> Result<Option<ResolvedEvent>, SignatureError> {
        self.store
            .get_event_signature(chain_id, &address, event_name)
            .map(|opt| {
                opt.map(|(signature, topic0, abi_json)| ResolvedEvent {
                    signature,
                    topic0,
                    abi_json,
                })
            })
            .map_err(|e| SignatureError::Cache(e.to_string()))
    }

    async fn put(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
        entry: &ResolvedEvent,
    ) -> Result<(), SignatureError> {
        self.store
            .insert_event_signature(
                chain_id,
                &address,
                event_name,
                &entry.signature,
                &entry.topic0,
                entry.abi_json.as_deref(),
            )
            .map_err(|e| SignatureError::Cache(e.to_string()))
    }
}
