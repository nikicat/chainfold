//! [`EventSignatureCache`] — durable cache for the event-signature resolver.
//!
//! A cheap handle over the shared connection vended by
//! [`CacheDb`](super::CacheDb). Stores resolved
//! `(chain, address, event name) → (signature, topic0, abi)` rows so repeated
//! lookups stay zero-network. Named `EventSignatureCache` (not
//! `SignatureCache`) to avoid colliding with the
//! [`SignatureCache`](crate::signatures::SignatureCache) resolver trait.

use super::{CacheError, SharedConn};
use crate::evm::types::ChainId;

/// A single row from `event_signatures`: canonical signature, topic0,
/// and (when populated) the matched ABI event item JSON.
pub type EventSignatureRow = (String, [u8; 32], Option<String>);

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
        address: &[u8; 20],
        event_name: &str,
    ) -> Result<Option<EventSignatureRow>, CacheError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT signature, topic0, abi_json FROM event_signatures
             WHERE chain_id = ?1 AND address = ?2 AND event_name = ?3",
        )?;
        let mut rows = stmt.query_map(
            rusqlite::params![chain_id.get() as i64, &address[..], event_name],
            |row| {
                let sig: String = row.get(0)?;
                let topic: Vec<u8> = row.get(1)?;
                let abi: Option<String> = row.get(2)?;
                Ok((sig, topic, abi))
            },
        )?;
        match rows.next() {
            Some(Ok((sig, topic, abi))) if topic.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&topic);
                Ok(Some((sig, arr, abi)))
            }
            Some(Err(e)) => Err(e.into()),
            _ => Ok(None),
        }
    }

    pub fn insert_event_signature(
        &self,
        chain_id: ChainId,
        address: &[u8; 20],
        event_name: &str,
        signature: &str,
        topic0: &[u8; 32],
        abi_json: Option<&str>,
    ) -> Result<(), CacheError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO event_signatures
             (chain_id, address, event_name, signature, topic0, abi_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                chain_id.get() as i64,
                &address[..],
                event_name,
                signature,
                &topic0[..],
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

    const ADDR: [u8; 20] = [0x42; 20];
    const TOPIC0: [u8; 32] = [0xDD; 32];

    #[test]
    fn signature_round_trip_with_abi() {
        let cache = EventSignatureCache::open_in_memory().unwrap();
        cache
            .insert_event_signature(
                ChainId::new(1),
                &ADDR,
                "Transfer",
                "Transfer(address,address,uint256)",
                &TOPIC0,
                Some("{\"type\":\"event\"}"),
            )
            .unwrap();
        let (sig, topic, abi) = cache
            .get_event_signature(ChainId::new(1), &ADDR, "Transfer")
            .unwrap()
            .unwrap();
        assert_eq!(sig, "Transfer(address,address,uint256)");
        assert_eq!(topic, TOPIC0);
        assert_eq!(abi.as_deref(), Some("{\"type\":\"event\"}"));
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
                &TOPIC0,
                None,
            )
            .unwrap();
        let (_, _, abi) = cache
            .get_event_signature(ChainId::new(1), &ADDR, "Approval")
            .unwrap()
            .unwrap();
        assert!(abi.is_none());
    }
}
