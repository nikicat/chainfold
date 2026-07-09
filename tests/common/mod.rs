//! Shared test scaffolding. Each `tests/*.rs` file is its own crate; this
//! module is included via `mod common;` and gives them a common surface for
//! anvil management, fixtures, and mock RPC.

#![allow(dead_code)]

pub mod anvil;
pub mod bitcoind;
pub mod fixtures;
pub mod mock_provider;
