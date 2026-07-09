//! [`BtcRpcClient`] — the narrow bitcoind JSON-RPC surface the block
//! history needs: tip height, batched hash-by-height, batched
//! header-by-hash.
//!
//! Batching: bitcoind processes a JSON-RPC batch sequentially on one RPC
//! thread and buffers the whole response, so batches stay in the
//! low hundreds (the fetcher's sub-batch size) rather than thousands. The
//! `jsonrpc` client matches batch responses to requests by id, so callers
//! always get results in request order regardless of server reordering.
//!
//! Responses are decoded through thin local serde structs (unknown fields
//! ignored), documented against the Core RPC help — deliberately not a
//! typed-client dependency: the three methods here need eight fields
//! total, and the alternative (`corepc-types`) pulls the full `bitcoin`
//! crate stack.

use std::sync::Arc;

use jsonrpc::client_async::{Client, Transport};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use super::types::{BtcBlock, BtcBlockHash, CompactTarget};
use crate::arch::{HistoryError, Timestamp};
use crate::chain::{BlockNumber, BlockRange, TxCount};

/// Cheap-`Clone` handle over the async JSON-RPC client.
#[derive(Clone)]
pub struct BtcRpcClient {
    client: Arc<Client>,
}

impl BtcRpcClient {
    /// Wrap a wire transport (typically
    /// `TracingTransport(ReqwestTransport)` — see
    /// [`transport`](super::transport)).
    pub fn new(transport: impl Transport) -> Self {
        Self {
            client: Arc::new(Client::with_transport(transport)),
        }
    }

    /// `getblockcount` — the current tip height.
    pub async fn get_block_count(&self) -> Result<BlockNumber, HistoryError> {
        let n: u64 = self
            .client
            .call("getblockcount", None)
            .await
            .map_err(|e| rpc_error("getblockcount", &e))?;
        Ok(BlockNumber::new(n))
    }

    /// Batched `getblockhash(height)` over `range` (inclusive), ascending.
    pub async fn get_block_hashes(
        &self,
        range: BlockRange,
    ) -> Result<Vec<BtcBlockHash>, HistoryError> {
        let (from, to) = range.as_u64();
        let params: Vec<Box<RawValue>> = (from..=to).map(|h| jsonrpc::arg([h])).collect();
        let results: Vec<String> = self.batch("getblockhash", &params).await?;
        results
            .iter()
            .map(|s| {
                s.parse::<BtcBlockHash>()
                    .map_err(|e| HistoryError::internal(format!("getblockhash: {e}")))
            })
            .collect()
    }

    /// Batched `getblockheader(hash, verbose=true)`, in input order.
    pub async fn get_block_headers(
        &self,
        hashes: &[BtcBlockHash],
    ) -> Result<Vec<BtcBlock>, HistoryError> {
        let params: Vec<Box<RawValue>> = hashes
            .iter()
            .map(|h| jsonrpc::arg((h.to_string(), true)))
            .collect();
        let results: Vec<RawBlockHeader> = self.batch("getblockheader", &params).await?;
        results.iter().map(RawBlockHeader::to_block).collect()
    }

    /// One id-matched batch of `method` calls, decoded per element. Returns
    /// results in `params` order (the `jsonrpc` client re-orders by id).
    async fn batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &[Box<RawValue>],
    ) -> Result<Vec<T>, HistoryError> {
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let reqs: Vec<jsonrpc::Request> = params
            .iter()
            .map(|p| self.client.build_request(method, Some(p)))
            .collect();
        let resps = self
            .client
            .send_batch(&reqs)
            .await
            .map_err(|e| rpc_error(method, &e))?;
        resps
            .into_iter()
            .map(|resp| {
                resp.ok_or_else(|| {
                    HistoryError::internal(format!("{method}: missing batch response"))
                })?
                .result::<T>()
                .map_err(|e| rpc_error(method, &e))
            })
            .collect()
    }
}

fn rpc_error(method: &str, e: &jsonrpc::Error) -> HistoryError {
    HistoryError::internal(format!("{method}: {e}"))
}

/// The `getblockheader` (verbose) fields the block history consumes, per
/// `bitcoin-cli help getblockheader` — stable since Core 0.17 (`nTx` and
/// all fields below predate it); newer Cores' extra fields (e.g. v29's
/// `target`) are ignored.
#[derive(Debug, serde::Deserialize)]
struct RawBlockHeader {
    /// Display-order hex.
    hash: String,
    height: u64,
    /// Seconds since epoch (header `time`).
    time: u64,
    /// The number of transactions in the block.
    #[serde(rename = "nTx")]
    n_tx: u32,
    /// Compact target, hex (`"1d00ffff"`).
    bits: String,
    /// Absent at genesis.
    #[serde(rename = "previousblockhash")]
    previous_block_hash: Option<String>,
}

impl RawBlockHeader {
    fn to_block(&self) -> Result<BtcBlock, HistoryError> {
        let bad = |field: &str, e: &dyn std::fmt::Display| {
            HistoryError::internal(format!("getblockheader: bad {field}: {e}"))
        };
        Ok(BtcBlock {
            height: BlockNumber::new(self.height),
            hash: self.hash.parse().map_err(|e| bad("hash", &e))?,
            prev_hash: match &self.previous_block_hash {
                Some(s) => s.parse().map_err(|e| bad("previousblockhash", &e))?,
                None => BtcBlockHash::ZERO,
            },
            time: Timestamp::new(self.time),
            tx_count: TxCount::new(self.n_tx),
            bits: CompactTarget::from_hex(&self.bits).map_err(|e| bad("bits", &e))?,
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use jsonrpc::client_async::BoxFuture;
    use jsonrpc::error::RpcError;
    use jsonrpc::{Request, Response};

    use super::*;

    /// Scripted transport: pops one canned reply (a JSON value or an RPC
    /// error) per incoming call, pairing it with the request's id. Batch
    /// replies are deliberately reversed to prove the client re-orders by
    /// id. Panics (loudly failing the test) when the script runs dry — the
    /// bitcoin analog of the EVM mock provider.
    #[derive(Default)]
    pub(in crate::bitcoin) struct ScriptedTransport {
        replies: Mutex<VecDeque<Result<serde_json::Value, RpcError>>>,
    }

    impl ScriptedTransport {
        /// Queue a successful result for the next call.
        pub(in crate::bitcoin) fn push_ok(&self, value: serde_json::Value) {
            self.replies.lock().unwrap().push_back(Ok(value));
        }

        /// Queue an RPC error for the next call.
        pub(in crate::bitcoin) fn push_err(&self, code: i32, message: &str) {
            self.replies.lock().unwrap().push_back(Err(RpcError {
                code,
                message: message.to_owned(),
                data: None,
            }));
        }

        fn reply_to(&self, req: &Request) -> Response {
            let scripted = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("ScriptedTransport ran out of replies");
            match scripted {
                Ok(v) => Response {
                    result: Some(RawValue::from_string(v.to_string()).unwrap()),
                    error: None,
                    id: req.id.clone(),
                    jsonrpc: Some("2.0".to_owned()),
                },
                Err(e) => Response {
                    result: None,
                    error: Some(e),
                    id: req.id.clone(),
                    jsonrpc: Some("2.0".to_owned()),
                },
            }
        }
    }

    impl Transport for ScriptedTransport {
        fn send_request<'a>(
            &'a self,
            req: Request<'a>,
        ) -> BoxFuture<'a, Result<Response, jsonrpc::Error>> {
            Box::pin(async move { Ok(self.reply_to(&req)) })
        }

        fn send_batch<'a>(
            &'a self,
            reqs: &'a [Request<'a>],
        ) -> BoxFuture<'a, Result<Vec<Response>, jsonrpc::Error>> {
            Box::pin(async move {
                let mut out: Vec<Response> = reqs.iter().map(|r| self.reply_to(r)).collect();
                // Reverse to prove the client re-orders by id.
                out.reverse();
                Ok(out)
            })
        }

        fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "scripted")
        }
    }

    fn hash_hex(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    #[tokio::test]
    async fn block_count_decodes() {
        let t = ScriptedTransport::default();
        t.push_ok(serde_json::json!(840_000));
        let client = BtcRpcClient::new(t);
        assert_eq!(
            client.get_block_count().await.unwrap(),
            BlockNumber::new(840_000)
        );
    }

    #[tokio::test]
    async fn block_hashes_come_back_in_height_order() {
        let t = ScriptedTransport::default();
        t.push_ok(serde_json::json!(hash_hex(0xA0)));
        t.push_ok(serde_json::json!(hash_hex(0xA1)));
        t.push_ok(serde_json::json!(hash_hex(0xA2)));
        let client = BtcRpcClient::new(t);
        // The scripted batch reply is reversed on the wire; id-matching
        // must still hand results back in request (= height) order.
        let hashes = client
            .get_block_hashes(BlockRange::from_u64(100, 102))
            .await
            .unwrap();
        assert_eq!(
            hashes,
            vec![
                BtcBlockHash::repeat_byte(0xA0),
                BtcBlockHash::repeat_byte(0xA1),
                BtcBlockHash::repeat_byte(0xA2),
            ]
        );
    }

    #[tokio::test]
    async fn headers_decode_and_genesis_has_zero_prev() {
        let t = ScriptedTransport::default();
        // Genesis: no previousblockhash; v29-style extra fields ignored.
        t.push_ok(serde_json::json!({
            "hash": hash_hex(0xB0),
            "confirmations": 3,
            "height": 0,
            "version": 1,
            "versionHex": "00000001",
            "merkleroot": hash_hex(0xCC),
            "time": 1_231_006_505,
            "mediantime": 1_231_006_505,
            "nonce": 2_083_236_893,
            "bits": "1d00ffff",
            "target": hash_hex(0x00),
            "difficulty": 1.0,
            "chainwork": hash_hex(0x00),
            "nTx": 1,
            "nextblockhash": hash_hex(0xB1),
        }));
        t.push_ok(serde_json::json!({
            "hash": hash_hex(0xB1),
            "confirmations": 2,
            "height": 1,
            "version": 1,
            "versionHex": "00000001",
            "merkleroot": hash_hex(0xCD),
            "time": 1_231_469_665,
            "mediantime": 1_231_469_665,
            "nonce": 2_573_394_689u32,
            "bits": "1d00ffff",
            "difficulty": 1.0,
            "chainwork": hash_hex(0x00),
            "nTx": 7,
            "previousblockhash": hash_hex(0xB0),
        }));
        let client = BtcRpcClient::new(t);
        let blocks = client
            .get_block_headers(&[
                BtcBlockHash::repeat_byte(0xB0),
                BtcBlockHash::repeat_byte(0xB1),
            ])
            .await
            .unwrap();
        assert_eq!(
            blocks,
            vec![
                BtcBlock {
                    height: BlockNumber::ZERO,
                    hash: BtcBlockHash::repeat_byte(0xB0),
                    prev_hash: BtcBlockHash::ZERO,
                    time: Timestamp::new(1_231_006_505),
                    tx_count: TxCount::new(1),
                    bits: CompactTarget::new(0x1d00_ffff),
                },
                BtcBlock {
                    height: BlockNumber::new(1),
                    hash: BtcBlockHash::repeat_byte(0xB1),
                    prev_hash: BtcBlockHash::repeat_byte(0xB0),
                    time: Timestamp::new(1_231_469_665),
                    tx_count: TxCount::new(7),
                    bits: CompactTarget::new(0x1d00_ffff),
                },
            ]
        );
    }

    #[tokio::test]
    async fn rpc_error_surfaces_with_method() {
        let t = ScriptedTransport::default();
        t.push_err(-8, "Block height out of range");
        let client = BtcRpcClient::new(t);
        let err = client
            .get_block_hashes(BlockRange::from_u64(1_000_000_000, 1_000_000_000))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("getblockhash") && msg.contains("out of range"),
            "error should name the method and carry the RPC message: {msg}"
        );
    }

    #[tokio::test]
    async fn empty_range_is_zero_rpc() {
        // An empty hash list must not build an (invalid) empty batch. The
        // scripted transport would panic on any wire call.
        let client = BtcRpcClient::new(ScriptedTransport::default());
        assert_eq!(client.get_block_headers(&[]).await.unwrap(), vec![]);
    }
}
