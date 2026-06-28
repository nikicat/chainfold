//! [`EventSignatureCache`] — durable cache for the event-signature resolver.
//!
//! A cheap handle over the shared connection vended by
//! [`CacheDb`](super::CacheDb). Stores resolved
//! `(chain, address, event name) → (signature, topic0, abi)` rows so repeated
//! lookups stay zero-network. Named `EventSignatureCache` (not
//! `SignatureCache`) to avoid colliding with the
//! [`SignatureCache`](crate::signatures::SignatureCache) resolver trait.

use alloy::primitives::{Address, B256};

use super::{CacheError, SharedConn};
use crate::evm::types::{ChainId, Topic};

/// A single resolved row from `event_signatures`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSignatureRow {
    /// Canonical event signature, e.g. `Transfer(address,address,uint256)`.
    pub signature: String,
    /// `topic0` — the keccak256 hash of the signature.
    pub topic0: Topic,
    /// JSON of the matched ABI event item, when populated.
    pub abi_json: Option<String>,
}

/// Durable SQLite-backed event-signature cache.
pub struct EventSignatureCache {
    conn: SharedConn,
}

impl EventSignatureCache {
    pub(super) fn from_conn(conn: SharedConn) -> Self {
        Self { conn }
    }

    /// Open a standalone signature cache at the given path. Creates the DB and
    /// runs migrations if needed. Equivalent to
    /// `CacheDb::open(path)?.signatures()`.
    pub fn open(path: &str) -> Result<Self, CacheError> {
        Ok(super::CacheDb::open(path)?.signatures())
    }

    /// Open a standalone in-memory signature cache. Useful for tests.
    pub fn open_in_memory() -> Result<Self, CacheError> {
        Ok(super::CacheDb::open_in_memory()?.signatures())
    }

    pub fn get_event_signature(
        &self,
        chain_id: ChainId,
        address: &Address,
        event_name: &str,
    ) -> Result<Option<EventSignatureRow>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT signature, topic0, abi_json FROM event_signatures
             WHERE chain_id = ?1 AND address = ?2 AND event_name = ?3",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![chain_id, address.as_slice(), event_name],
            |row| {
                let sig: String = row.get(0)?;
                let topic: Vec<u8> = row.get(1)?;
                let abi: Option<String> = row.get(2)?;
                Ok((sig, topic, abi))
            },
        )?;
        match rows.next() {
            Some(Ok((sig, topic, abi))) if topic.len() == 32 => Ok(Some(EventSignatureRow {
                signature: sig,
                topic0: Topic::from(B256::from_slice(&topic)),
                abi_json: abi,
            })),
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn insert_event_signature(
        &self,
        chain_id: ChainId,
        address: &Address,
        event_name: &str,
        signature: &str,
        topic0: Topic,
        abi_json: Option<&str>,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO event_signatures
             (chain_id, address, event_name, signature, topic0, abi_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                chain_id,
                address.as_slice(),
                event_name,
                signature,
                topic0.as_slice(),
                abi_json,
            ],
        )?;
        Ok(())
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    const ADDR: Address = Address::repeat_byte(0x42);
    const TOPIC0: B256 = B256::repeat_byte(0xDD);

    #[test]
    fn signature_round_trip_with_abi() {
        let cache = EventSignatureCache::open_in_memory().unwrap();
        cache
            .insert_event_signature(
                ChainId::new(1),
                &ADDR,
                "Transfer",
                "Transfer(address,address,uint256)",
                Topic::from(TOPIC0),
                Some("{\"type\":\"event\"}"),
            )
            .unwrap();
        let row = cache
            .get_event_signature(ChainId::new(1), &ADDR, "Transfer")
            .unwrap()
            .unwrap();
        assert_eq!(row.signature, "Transfer(address,address,uint256)");
        assert_eq!(row.topic0, Topic::from(TOPIC0));
        assert_eq!(row.abi_json.as_deref(), Some("{\"type\":\"event\"}"));
    }

    #[test]
    fn signature_missing_returns_none() {
        let cache = EventSignatureCache::open_in_memory().unwrap();
        assert!(
            cache
                .get_event_signature(ChainId::new(1), &ADDR, "Nope")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn signature_nullable_abi() {
        let cache = EventSignatureCache::open_in_memory().unwrap();
        cache
            .insert_event_signature(
                ChainId::new(1),
                &ADDR,
                "Approval",
                "Approval()",
                Topic::from(TOPIC0),
                None,
            )
            .unwrap();
        let row = cache
            .get_event_signature(ChainId::new(1), &ADDR, "Approval")
            .unwrap()
            .unwrap();
        assert!(row.abi_json.is_none());
    }
}
