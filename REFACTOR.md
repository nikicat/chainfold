# Refactoring inventory

Snapshot of structural debt in the tree, ordered by suggested execution. "Safe" items have local blast radius and need no design discussion; "architectural" items touch module boundaries and should be agreed before starting.

## Safe — local, low-risk

### 1. Stale doc references to `HistStore::gen`

`src/arch.rs:7,50,207,393` — comments call the method `gen`, but it was renamed to `snapshots` (Rust 2024 keyword). Outdated immediately on read. Find/replace.

### 2. De-dupe `resolve_range` in the CLI

`src/bin/chainfold/evm/events.rs:176` and `src/bin/chainfold/evm/erc20.rs:187` carry the same `match (&args.from, &args.to, args.from_block)` body with the same three arms and the same hard-coded `22_000_000u64` default start block. Hoist to `src/bin/chainfold/util.rs` (or a new `range.rs`). The erc20 variant takes a `&Arc<EvmLogHistory<P>>`; the events variant takes a `&HistStore<EvmEvent, EvmPosition>` — unify on `&HistStore<…>`.

### 3. Extract `apply_topics_to_filter`

`src/evm/history.rs:316` `build_filter` and `src/evm/history.rs:555` `build_fetcher` both walk `self.topics[0..3]` calling `.event_signature` / `.topic1` / `.topic2` / `.topic3` on an alloy `Filter`. Same 18 lines in two places. One helper.

### 4. Drop stale "Phase" / "reference" citations

- `src/evm/log_index.rs:5,6,338,416,901` cite "reference history.ts" line numbers — readers don't have access to the reference.
- `src/evm/log_index.rs:20` "Wiring into `EvmLogHistory` lands in Phase 1c." — Phase 1 is complete (project memory).
- `src/evm/log_store.rs:5,12,265` "Phase 1 of the refactor" — similar.
- `src/matcher.rs:5,11` "Two layers …" header refs are fine; "Phase 6 deferred" notes in `MEMORY.md` are OK there.

Citations that belong in commit messages, not source files.

### 5. De-dupe follow vs one-shot loop in `erc20::run`

`src/bin/chainfold/evm/erc20.rs:22` `run` (~150 lines) has two near-identical inner loops (lines 102-135 and 136-170): same `apply_transfer`, same `last_pos` dedup, same `print_balance` / `print_transfer` branch. Extract `print_one_transfer(item, balance_state, count, ctx)` and the dedup check; let both follow / one-shot drive it.

### 6. Stub fields in `EvmLogHistory`

`src/evm/history.rs:67` — `best_tx` / `safe_tx` are `#[allow(dead_code)]` "stubs that mirror `latest` until Phase 7." The `best_store` / `safe_store` they feed never advance. Either delete until Phase 7 lands, or extract the tip state into `struct TipTracker { latest_store, latest_tx, best_store, best_tx, safe_store, safe_tx }`. Removing the stubs is the smaller move; reintroducing them when Phase 7 needs them costs nothing.

### 7. Misleading single-purpose names

- `EvmLogHistory::poll_tip` (`history.rs:233`) does an RPC fetch and updates the watch channel. Suggests pull-based loop. Rename `fetch_and_update_tip` (or just `update_tip`).
- `Hist::pos_after(items: &[Positioned<T,P>])` (`arch.rs:295`) returns `items.last().map(|p| p.pos.clone())`. The name suggests "position after these items"; inline the one-liner at the few call sites.
- `MergedItem::item` / `MappedItem::item` / `FilteredItem::item` produce `items[0].item.item.label` in tests. Rename inner generic field to `value` (or `inner`).

### 8. `alchemy_chain_slug` doesn't belong in `config.rs`

`src/bin/chainfold/config.rs:285` hardcodes 8 Alchemy chain slugs. The project already has a `ChainResolver` (`src/bin/chainfold/chains.rs`) that handles chain identity / endpoint selection. Move the slug map to `chains.rs` — `config.rs` shouldn't know about Alchemy URL shape, just expose `alchemy_api_key()`.

## Function-level — larger but mechanical

### 9. Unify `find_next_gap_or_wait` and `snapshot_walk`

`src/evm/log_index.rs:762` and `src/evm/log_index.rs:975` are ~90% duplicated. Both walk the tree from a cursor looking for next gap / in-flight fetch / completion. The only delta is that `snapshot_walk` also surfaces filled items via `WalkStep::Yield`. The `FillGap` placeholder install (Arc::new(Notify), insert into tree) is repeated **5 times** across the two functions. Unify into one walk primitive that returns a `Step` enum with `Yield` always present; callers that don't yield items can ignore that arm. Or extract `install_gap_placeholder(tree, from, to) -> Arc<Notify>` and let both functions call it.

### 10. Split the giant `cached_navigate`

`src/evm/log_index.rs:587` is 115 lines, switching on `NavDir` then nesting two near-identical loops (forward / backward), each with `unreachable!()` branches because the dispatch was already done. Extract `cached_navigate_forward` and `cached_navigate_backward`; `NavDir` becomes two booleans (`forward: bool, inclusive: bool`) or is removed entirely once each branch is its own function. The four public wrappers (`first_at_or_after` / `first_after` / `last_at_or_before` / `last_before`) become trivial.

### 11. Decompose `merge_step`

`src/merge.rs:597` is ~90 lines with three phases: fill heads, surface errors, k-way min. Extract `fill_heads(&mut state)`, `take_first_error(&mut state)`, `pick_winner(&state) -> Option<usize>`; `merge_step` becomes a 20-line orchestrator.

### 12. Parameterize `ErasedSource` over `TipKind`

`src/merge.rs:135` `ErasedSource` has 9 methods, 6 of them are 3x duplicates: `latest_pos` / `best_pos` / `safe_pos` and `subscribe_latest` / `subscribe_best` / `subscribe_safe`. `build_tip_store` (`src/merge.rs:304`) is called 3x with the same shape. Add `enum TipKind { Latest, Best, Safe }` and collapse to `fn tip_pos(&self, kind: TipKind)` + `fn subscribe_tip(&self, kind: TipKind)`.

### 13. `signatures.rs::event_from_abi` mixes parsing + matching + hashing

`src/signatures.rs:261` — JSON parse + find-by-name + canonicalize signature + keccak + re-serialize the matched item. Split into `find_event_item(abi_json, name) -> Result<Value>`, `canonical_signature(item) -> String`, then call the existing `keccak256`. Lower priority — already reasonably layered.

## Architectural — needs agreement before touching

### A. Split `LogStore` into per-table caches

`src/evm/log_store.rs:266` — a single `LogStore` owns five unrelated tables: `cached_logs` + `scanned_ranges` + `reorged_blocks` + `cached_block_headers` + `erc20_metadata` + `erc20_balances` + `event_signatures`. Module doc admits it: "log + range + header + erc20 store." Name is misleading.

Suggested split:
- `LogCache` — logs + scanned ranges + reorged blocks (the actual EVM log cache).
- `BlockHeaderCache` — `cached_block_headers`.
- `Erc20Cache` — metadata + balances.
- `SignatureCache` — event_signatures (this is what `signatures::SignatureCache` trait wants).

All can share an `Arc<Mutex<Connection>>` so the SQLite file stays single. Migrations would localize per cache. Big diff, but each consumer (`LogIndex`, `EvmLogHistory`, `erc20::*`, `LogStoreSignatureCache`) only needs one narrow surface today.

### B. Move `arch::evm` out of `arch.rs`

`src/arch.rs:502` defines a 210-line `pub mod evm { … }` with `EvmPosition`, `EvmEvent`, `EvmBlock`, `EvmTransaction`, `Amount`, `Asset`, `Erc20`, `Erc20Transfer`, `Erc20Mint`, `Erc20Burn`, `Erc20Balance`. Yet the module doc at line 11 says: "Generic core (this module) knows nothing about EVM." Move to `src/evm/types.rs` and re-export under `crate::evm::types`. Project's locked-in design rule #1 calls source-agnostic core out explicitly.

### C. Move `SqliteAnchorStore` out of `matcher.rs`

`src/matcher.rs:434` drags `rusqlite` into the source-agnostic matcher module. Move to `src/matcher/anchor_store_sqlite.rs` (gated behind a feature flag) or under `src/evm/` alongside the SQLite-aware code. Module doc claims source-agnosticism — implementation contradicts it.

### D. Split `src/evm/log_index.rs` (1669 lines)

Owns: `BlockPos`, `LogRef`, `LogPage`, `Range`, `Fetcher` type alias, `LogIndex`, four step enums, free helpers, ~500 lines of tests. Suggested layout:
- `block_pos.rs` — `BlockPos`, `LogRef`, `LogPage` (data types).
- `range_tree.rs` — `Range`, the `BTreeMap` operations (insert_page, insert_late_item).
- `nav.rs` — `cached_navigate` and its public wrappers.
- `gap_fill.rs` — `ensure_cached`, `items()`, fetcher singleflight.

Mostly a file move; minor visibility plumbing.

## Things I checked and left alone

- `src/combinators/{filtered,mapped,since}.rs` — clean, single-purpose, no debt.
- `src/evm/aligned_ranges.rs` — small, focused, tested.
- `src/evm/subscriber.rs` — clear two-transport split (Ws/Poll), reasonable structure.
- `src/bin/chainfold/chains.rs` — `#[allow(dead_code)]` on >half of `ChainlistEntry` fields is speculative API surface, but already tracked in `TODO.md`.
