//! Capped mock JSON-RPC server for the adaptive-fetch integration tests: a
//! real HTTP endpoint (axum on `127.0.0.1:0`) serving a deterministic chain
//! whose `eth_getLogs` enforces provider-style caps. Violations answer with
//! the *real error shape* of the chosen provider — suggested range spliced
//! in — so the production client stack exercises the same classify →
//! resize/backoff/bisect paths a hosted endpoint would drive.
//!
//! Deliberately light on lib types: block numbers are raw `u64`s and
//! payloads are `serde_json` values; only feature-independent helpers
//! (`Share`, `topic_from_signature`) are borrowed, so the module compiles
//! under any feature set.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use chainfold::Share;
use chainfold::signatures::{TRANSFER_SIGNATURE, topic_from_signature};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The contract address stamped on every generated log — what tests pass
/// as the filter/CLI `--address`.
pub const LOG_ADDRESS: &str = "0x4242424242424242424242424242424242424242";

/// topic0 stamped on every generated log: the canonical
/// [`TRANSFER_SIGNATURE`]'s keccak selector, derived via the same helper
/// the CLI's `--event-sig` path uses — so tests select the logs with a
/// filter (or `--event-sig`) built from that same string.
pub fn log_topic0() -> String {
    format!(
        "0x{}",
        hex::encode(topic_from_signature(TRANSFER_SIGNATURE))
    )
}

/// The deterministic chain the mock serves. Headers exist for every height
/// up to `head` (hash/parent/timestamp are pure functions of the number);
/// logs exist only where `logs_at` says so.
#[derive(Clone, Debug)]
pub struct MockChainData {
    pub chain_id: u64,
    pub head: u64,
    pub genesis_timestamp: u64,
    pub block_time_secs: u64,
    /// Log count per block; absent blocks have none. Density is what makes
    /// content-dependent caps (`max_logs`) bite.
    pub logs_at: BTreeMap<u64, u32>,
}

impl Default for MockChainData {
    fn default() -> Self {
        Self {
            chain_id: 31_337,
            head: 1_000,
            genesis_timestamp: 1_600_000_000,
            block_time_secs: 12,
            logs_at: BTreeMap::new(),
        }
    }
}

impl MockChainData {
    /// Total logs in `from..=to` — the tests' expected-count oracle.
    pub fn logs_in(&self, from: u64, to: u64) -> usize {
        self.logs_at
            .range(from..=to)
            .map(|(_, &n)| n as usize)
            .sum()
    }
}

/// Which provider's error dialect the mock speaks when a cap is violated.
#[derive(Clone, Copy, Debug)]
pub enum ErrorStyle {
    /// `-32005` with a `data.from`/`data.to` suggested window (the
    /// result-window face of the dual-use code; see
    /// `src/evm/provider/fixtures/infura_32005_result_window.json`).
    Infura,
    /// `-32602` with "this block range should work: [0x…, 0x…]" in the
    /// message (see `fixtures/alchemy_range_error.json`). Hard failures
    /// use the fatal "is not enabled for this app" shape.
    Alchemy,
    /// A plain HTTP 500 with a non-JSON-RPC body — the shape the generic
    /// adaptor classifies as `Transient`, driving bisection discovery.
    GenericHttp500,
}

/// The provider limits the mock enforces on `eth_getLogs`.
#[derive(Clone, Copy, Debug)]
pub struct ProviderCaps {
    /// Reject requests wider than this many blocks.
    pub max_range: Option<u64>,
    /// Reject requests whose range holds more than this many logs.
    pub max_logs: Option<usize>,
    pub style: ErrorStyle,
}

impl ProviderCaps {
    /// No caps — every well-formed request succeeds.
    pub fn unrestricted(style: ErrorStyle) -> Self {
        Self {
            max_range: None,
            max_logs: None,
            style,
        }
    }
}

struct MockState {
    data: MockChainData,
    caps: ProviderCaps,
    calls: Mutex<Vec<String>>,
    get_logs_ranges: Mutex<Vec<(u64, u64)>>,
    /// One-shot fault: the next `eth_getLogs` covering this block gets the
    /// style's hard failure, then the fault disarms.
    fail_once_at: Mutex<Option<u64>>,
    /// Latency gate: `eth_getLogs` requests reaching at or past this block
    /// stall until [`MockRpc::release`].
    hold_from: Mutex<Option<u64>>,
    released: watch::Sender<bool>,
}

/// Handle to a spawned mock server. Dropping it aborts the serve task.
pub struct MockRpc {
    addr: SocketAddr,
    state: Arc<MockState>,
    server: JoinHandle<()>,
}

impl MockRpc {
    pub async fn spawn(data: MockChainData, caps: ProviderCaps) -> Self {
        let (released, _) = watch::channel(false);
        let state = Arc::new(MockState {
            data,
            caps,
            calls: Mutex::new(Vec::new()),
            get_logs_ranges: Mutex::new(Vec::new()),
            fail_once_at: Mutex::new(None),
            hold_from: Mutex::new(None),
            released,
        });
        // Both routes hit the same handler: providers key endpoints by
        // path (`/v2/{key}`), and the planted-key CLI test relies on the
        // mock accepting whatever path the URL carries.
        let app = Router::new()
            .route("/", post(handle))
            .route("/{*path}", post(handle))
            .with_state(state.share());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock rpc listener");
        let addr = listener.local_addr().expect("mock rpc local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("mock rpc serves");
        });
        Self {
            addr,
            state,
            server,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// The endpoint with a caller-chosen path — e.g. a planted
    /// `/v2/{key}` to sweep the CLI's output for.
    pub fn url_with_path(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Every JSON-RPC method received, in arrival order.
    pub fn calls(&self) -> Vec<String> {
        self.state.calls.lock().unwrap().clone()
    }

    /// Every `eth_getLogs` `(from, to)` received, in arrival order.
    pub fn get_logs_ranges(&self) -> Vec<(u64, u64)> {
        self.state.get_logs_ranges.lock().unwrap().clone()
    }

    /// Arm the one-shot fault: the next `eth_getLogs` whose range covers
    /// `block` gets the style's hard failure (fatal-shaped for Alchemy,
    /// HTTP 500 otherwise).
    pub fn fail_once_at(&self, block: u64) {
        *self.state.fail_once_at.lock().unwrap() = Some(block);
    }

    /// Close the latency gate: `eth_getLogs` requests whose range reaches
    /// `block` stall until [`Self::release`].
    pub fn hold_logs_from(&self, block: u64) {
        *self.state.hold_from.lock().unwrap() = Some(block);
    }

    /// Open the latency gate; held requests complete normally.
    pub fn release(&self) {
        *self.state.hold_from.lock().unwrap() = None;
        self.state.released.send_replace(true);
    }
}

impl Drop for MockRpc {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// One JSON-RPC reply: a `result` payload, an `error` object, or an
/// HTTP-level failure (non-JSON-RPC body) that aborts the whole exchange.
enum Reply {
    Result(Value),
    Error(Value),
    Http(StatusCode, &'static str),
}

async fn handle(State(state): State<Arc<MockState>>, Json(body): Json<Value>) -> Response {
    match body {
        Value::Array(batch) => {
            let mut replies = Vec::with_capacity(batch.len());
            for req in &batch {
                match dispatch(&state, req).await {
                    Reply::Http(status, text) => return (status, text).into_response(),
                    reply => replies.push(envelope(&req["id"], reply)),
                }
            }
            Json(Value::Array(replies)).into_response()
        }
        req => match dispatch(&state, &req).await {
            Reply::Http(status, text) => (status, text).into_response(),
            reply => Json(envelope(&req["id"], reply)).into_response(),
        },
    }
}

fn envelope(id: &Value, reply: Reply) -> Value {
    match reply {
        Reply::Result(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Reply::Error(error) => json!({"jsonrpc": "2.0", "id": id, "error": error}),
        Reply::Http(..) => unreachable!("HTTP-level failures are handled by the caller"),
    }
}

async fn dispatch(state: &MockState, req: &Value) -> Reply {
    let method = req["method"].as_str().unwrap_or_default().to_owned();
    state.calls.lock().unwrap().push(method.clone());
    match method.as_str() {
        "eth_chainId" => Reply::Result(quantity(state.data.chain_id)),
        "eth_blockNumber" => Reply::Result(quantity(state.data.head)),
        "eth_getBlockByNumber" => serve_block(state, req),
        "eth_getLogs" => serve_logs(state, req).await,
        other => Reply::Error(
            json!({"code": -32601, "message": format!("the method {other} does not exist")}),
        ),
    }
}

/// `eth_getBlockByNumber`: a hex quantity answers that height; tags
/// (latest/safe/…) answer the head.
fn serve_block(state: &MockState, req: &Value) -> Reply {
    let param = req["params"][0].as_str().unwrap_or("latest");
    let number = match param.strip_prefix("0x") {
        Some(hex) => match u64::from_str_radix(hex, 16) {
            Ok(n) => n,
            Err(_) => {
                return Reply::Error(json!({"code": -32602, "message": "invalid block number"}));
            }
        },
        None => state.data.head,
    };
    Reply::Result(block_json(&state.data, number))
}

async fn serve_logs(state: &MockState, req: &Value) -> Reply {
    let filter = &req["params"][0];
    let (Some(from), Some(to)) = (
        parse_hex(&filter["fromBlock"]),
        parse_hex(&filter["toBlock"]),
    ) else {
        return Reply::Error(
            json!({"code": -32602, "message": "mock serves numeric block ranges only"}),
        );
    };
    state.get_logs_ranges.lock().unwrap().push((from, to));

    // Latency gate first: held requests are recorded (arrival order is
    // what the tests assert on) but answer only after release().
    // `wait_for` checks the current value before parking, so a release()
    // racing this subscribe is never missed.
    let held = state.hold_from.lock().unwrap().is_some_and(|h| to >= h);
    if held {
        let mut released = state.released.subscribe();
        let _ = released.wait_for(|open| *open).await;
    }

    let fault = {
        let mut armed = state.fail_once_at.lock().unwrap();
        match *armed {
            Some(block) if from <= block && block <= to => {
                *armed = None;
                true
            }
            _ => false,
        }
    };
    if fault {
        return hard_failure(state.caps.style);
    }

    if let Some(width) = state.caps.max_range
        && to - from + 1 > width
    {
        return range_rejection(state, from, from + width - 1);
    }
    if let Some(max_logs) = state.caps.max_logs
        && state.data.logs_in(from, to) > max_logs
    {
        return range_rejection(state, from, fit_end(&state.data, from, to, max_logs));
    }
    Reply::Result(Value::Array(logs_json(&state.data, from, to)))
}

/// The style's cap-violation answer, `[from, to]` spliced in as the
/// suggested range (mirroring the recorded fixtures the adaptors'
/// classifiers are pinned to).
fn range_rejection(state: &MockState, from: u64, to: u64) -> Reply {
    let limit = state.caps.max_logs.unwrap_or(10_000);
    match state.caps.style {
        // The message always claims a results overflow (like the real
        // payload) even when the mock's trigger was width — classification
        // keys on the structured data hint, not the prose.
        ErrorStyle::Infura => Reply::Error(json!({
            "code": -32005,
            "message": format!(
                "query returned more than {limit} results. Try with this block range [{from:#X}, {to:#X}]."
            ),
            "data": {
                "from": format!("{from:#x}"),
                "limit": limit,
                "to": format!("{to:#x}"),
            },
        })),
        ErrorStyle::Alchemy => Reply::Error(json!({
            "code": -32602,
            "message": format!(
                "Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response. Based on your parameters and the response size limit, this block range should work: [{from:#X}, {to:#X}]"
            ),
        })),
        ErrorStyle::GenericHttp500 => {
            Reply::Http(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// The style's hard failure ([`MockRpc::fail_once_at`]): fatal-shaped for
/// Alchemy ("is not enabled for this app" — no retry or resize helps),
/// a transient-shaped HTTP 500 for the others.
fn hard_failure(style: ErrorStyle) -> Reply {
    match style {
        ErrorStyle::Alchemy => Reply::Error(json!({
            "code": -32600,
            "message": "this network is not enabled for this app",
        })),
        ErrorStyle::Infura | ErrorStyle::GenericHttp500 => {
            Reply::Http(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// Largest `end` in `from..=to` with `logs_in(from, end) <= max_logs`.
/// Degenerates to `from` when the first block alone overflows — test data
/// must keep per-block density under the cap or the client would loop.
fn fit_end(data: &MockChainData, from: u64, to: u64, max_logs: usize) -> u64 {
    let mut cumulative = 0usize;
    let mut end = from;
    for block in from..=to {
        cumulative += data.logs_at.get(&block).copied().unwrap_or(0) as usize;
        if cumulative > max_logs {
            break;
        }
        end = block;
    }
    end
}

/// A JSON-RPC `QUANTITY`: a `u64` as 0x-prefixed hex.
fn quantity(n: u64) -> Value {
    json!(format!("{n:#x}"))
}

fn parse_hex(v: &Value) -> Option<u64> {
    let hex = v.as_str()?.strip_prefix("0x")?;
    u64::from_str_radix(hex, 16).ok()
}

/// Deterministic 32-byte hex, unique per `(seed, tag)` — hashes never
/// collide across kinds (blocks vs transactions) or heights.
fn hash32(seed: u64, tag: u64) -> String {
    format!("0x{tag:016x}{seed:016x}{seed:016x}{seed:016x}")
}

pub fn block_hash(number: u64) -> String {
    hash32(number, 0xb10c)
}

fn logs_json(data: &MockChainData, from: u64, to: u64) -> Vec<Value> {
    let topic0 = log_topic0();
    let mut logs = Vec::new();
    for (&block, &count) in data.logs_at.range(from..=to) {
        for idx in 0..u64::from(count) {
            // No blockTimestamp: real providers commonly omit it, and its
            // absence is what routes the client through the in-slice
            // header fill.
            logs.push(json!({
                "address": LOG_ADDRESS,
                "topics": [&topic0],
                "data": "0x",
                "blockNumber": quantity(block),
                "blockHash": block_hash(block),
                "transactionHash": hash32(block, 0x7a00 + idx),
                "transactionIndex": "0x0",
                "logIndex": quantity(idx),
                "removed": false,
            }));
        }
    }
    logs
}

/// An `eth_getBlockByNumber` response body. The client's header fill
/// reads only `hash`, `parentHash`, and `timestamp`
/// (`ChainProvider::fetch_and_persist_header`); every other field exists
/// solely because alloy's `Block` deserializer requires it, zeroed where
/// possible and mainnet-plausible where a value must be non-trivial —
/// nothing validates them.
fn block_json(data: &MockChainData, number: u64) -> Value {
    const ZERO32: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let parent = if number == 0 {
        ZERO32.to_owned()
    } else {
        block_hash(number - 1)
    };
    json!({
        "hash": block_hash(number),
        "parentHash": parent,
        // Roots, bloom, and the beneficiary are unread filler: 32-byte
        // zeros (a real chain would carry keccak commitments here), a
        // 256-byte zero bloom, a zero address.
        "sha3Uncles": ZERO32,
        "miner": "0x0000000000000000000000000000000000000000",
        "stateRoot": ZERO32,
        "transactionsRoot": ZERO32,
        "receiptsRoot": ZERO32,
        "logsBloom": format!("0x{}", "00".repeat(256)),
        // Post-merge consensus fields: difficulty/mixHash/nonce are zero
        // on mainnet since PoS (nonce is a fixed 8-byte field).
        "difficulty": "0x0",
        "number": quantity(number),
        "gasLimit": "0x1c9c380",     // 30_000_000 — mainnet's gas limit
        "gasUsed": "0x0",            // the mock's blocks are empty
        "timestamp": quantity(data.genesis_timestamp + number * data.block_time_secs),
        "extraData": "0x",
        "mixHash": ZERO32,
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x3b9aca00", // 1 gwei
        "size": "0x0",
        "transactions": [],
        "uncles": [],
    })
}
