//! Adapter: implements the [`SignatureCache`] resolver trait over the
//! SQLite-backed [`EventSignatureCache`] so the CLI reuses the same cache
//! file for event-signature caching.

use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chainfold::evm::{ChainId, EventSignatureCache};
use chainfold::signatures::{ResolvedEvent, SignatureCache, SignatureError};

pub struct SqliteSignatureCache {
    cache: EventSignatureCache,
}

impl SqliteSignatureCache {
    pub fn new(cache: EventSignatureCache) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl SignatureCache for SqliteSignatureCache {
    async fn get(
        &self,
        chain_id: u64,
        address: [u8; 20],
        event_name: &str,
    ) -> Result<Option<ResolvedEvent>, SignatureError> {
        self.cache
            .get_event_signature(ChainId::new(chain_id), &Address::from(address), event_name)
            .map(|opt| {
                opt.map(|(signature, topic0, abi_json)| ResolvedEvent {
                    signature,
                    topic0: topic0.into(),
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
        self.cache
            .insert_event_signature(
                ChainId::new(chain_id),
                &Address::from(address),
                event_name,
                &entry.signature,
                &B256::from(entry.topic0),
                entry.abi_json.as_deref(),
            )
            .map_err(|e| SignatureError::Cache(e.to_string()))
    }
}
