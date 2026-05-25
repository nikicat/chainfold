# Refactoring inventory

Snapshot of structural debt in the tree. Items marked ✅ landed in commits on `main`; ⏳ items are still open. Architectural items touch module boundaries and should be agreed before starting.

## ✅ Done (this branch)

| # | Commit | Item |
|---|--------|------|
| 1 | `9395b5a` | Rename stale `HistStore::gen` doc refs to `::snapshots` (`arch.rs`, `evm/history.rs`). |
| 2 | `beb4fe3` | Hoist `resolve_range` into `bin/chainfold/util.rs` — was duplicated in `events.rs` + `erc20.rs`. |
| 3 | `aad77d5` | Extract `apply_topics` in `evm/history.rs` — `build_filter` and `build_fetcher` shared 18 lines. |
| 4 | `1a84e00` | Drop reference citations and Phase-1/1b/1c/7 tags from `evm/log_index.rs`, `evm/log_store.rs`, `evm/aligned_ranges.rs`. |
| 5 | `f7dc0f3` | Extract `render_transfer` in `bin/chainfold/evm/erc20.rs` — follow vs one-shot loops now share the per-item body. |
| 6 | `66e3f4e` | Drop unused `best_tx` / `safe_tx` stub fields from `EvmLogHistory`; channels close cleanly until finality wiring lands. |
| 8 | `0092d4b` | Move `alchemy_chain_slug` + URL builder from `config.rs` to `chains.rs`; config now exposes `alchemy_api_key()`. |
| 9 (partial) | `b84a4d9` | Extract `install_gap_placeholder` in `evm/log_index.rs` — 5 dupes collapsed to one helper. Deeper unification of `find_next_gap_or_wait` vs. `snapshot_walk` deferred (see ⏳ below). |
| 10 | `2d3bd93` | Split `cached_navigate` into `cached_navigate_forward` / `cached_navigate_backward`; drop `NavDir` enum. Doc comments now call out the asymmetry between forward (requires contiguity) and backward (yields nearest preceding). |
| 11 | `3426648` | Decompose `merge_step` into `fill_heads` / `take_first_error` / `pick_winner`; `merge_step` becomes a 20-line orchestrator. |
| 12 | `c51be9a` | Collapse `latest_pos` / `best_pos` / `safe_pos` (and the subscribe trio) in `ErasedSource` to `tip_pos(TipKind)` / `subscribe_tip(TipKind)`; `build_tip_store` no longer needs closures. |
| B | `8e4432b` | Move `arch::evm` (`EvmPosition`, `EvmEvent`, `EvmBlock`, `EvmTransaction`, `Amount`, `Asset`, `Erc20`, `Erc20Transfer`/`Mint`/`Burn`, `Erc20Balance`) out of `arch.rs` into `src/evm/types.rs`. Source-agnostic core no longer references EVM. |
| 13 | _pending_ | Extract `find_event_item` in `signatures.rs` — `event_from_abi` now locates the ABI event item, then canonicalizes/hashes/serializes; parse+find separated from signature derivation. |
| 7 | `6c8cb47` | Rename `EvmLogHistory::poll_tip` → `update_tip` (and `OwnedErc20::poll_tips` → `update_tips`); delete dead `Hist::pos_after` (zero callers); rename inner `item` field of `MergedItem` / `MappedItem` / `FilteredItem` to `value` — collapses `.item.item.label` to `.item.value.label`. |

Also: `4a6f35e` added this file.

## ⏳ Open — function-level

### 9 (remaining). Deeper unification of the tree-walk primitives

`evm/log_index.rs` — `find_next_gap_or_wait` (~70 lines) and `snapshot_walk` (~75 lines) still share their entire structure: both walk the BTreeMap from a cursor, classify each visited range as gap / in-flight / cached, and either install a placeholder, return a wait, or advance. The only difference is `snapshot_walk` additionally yields filled items. Could unify behind one walk primitive returning a `Step` enum; callers ignore the `Yield` arm if they only care about gap-filling. Lower priority now that the placeholder dupes are gone.

## ⏳ Open — architectural (need agreement first)

### A. Split `LogStore` into per-table caches

`src/evm/log_store.rs:266` — a single `LogStore` owns seven unrelated tables: `cached_logs` + `scanned_ranges` + `reorged_blocks` + `cached_block_headers` + `erc20_metadata` + `erc20_balances` + `event_signatures`. Module doc admits it: "log + range + header + erc20 store." Name is misleading.

Suggested split:
- `LogCache` — logs + scanned ranges + reorged blocks (the actual EVM log cache).
- `BlockHeaderCache` — `cached_block_headers`.
- `Erc20Cache` — metadata + balances.
- `SignatureCache` — event_signatures (this is what `signatures::SignatureCache` trait wants).

Open design question: do they share a `Arc<Mutex<Connection>>` (one SQLite file) or each own their own (one file per concern)? Migrations would localize per cache either way. Big diff but each consumer (`LogIndex`, `EvmLogHistory`, `erc20::*`, `LogStoreSignatureCache`) only needs one narrow surface today.

### C. Move `SqliteAnchorStore` out of `matcher.rs`

`src/matcher.rs:434` drags `rusqlite` into the source-agnostic matcher module. Move to `src/matcher/anchor_store_sqlite.rs` (gated behind a feature flag) or under `src/evm/` alongside other SQLite-aware code. Module doc claims source-agnosticism; implementation contradicts it.

Smaller scope than (A) but same flavor.

### D. Split `src/evm/log_index.rs` (1669 lines)

Owns: `BlockPos`, `LogRef`, `LogPage`, `Range`, `Fetcher` type alias, `LogIndex`, three step enums, free helpers, ~500 lines of tests. Suggested layout:
- `block_pos.rs` — `BlockPos`, `LogRef`, `LogPage` (data types).
- `range_tree.rs` — `Range`, the `BTreeMap` operations (insert_page, insert_late_item, install_gap_placeholder).
- `nav.rs` — `cached_navigate_forward` / `cached_navigate_backward` and their wrappers.
- `gap_fill.rs` — `ensure_cached`, `items()`, fetcher singleflight, `finalize_gap_fill`.

Mostly a file move; minor visibility plumbing (the helpers stay `pub(super)` inside the module tree).

## Things I checked and left alone

- `src/combinators/{filtered,mapped,since}.rs` — clean, single-purpose, no debt.
- `src/evm/aligned_ranges.rs` — small, focused, tested.
- `src/evm/subscriber.rs` — clear two-transport split (Ws / Poll), reasonable structure.
- `src/bin/chainfold/chains.rs` — `#[allow(dead_code)]` on >half of `ChainlistEntry` fields is speculative API surface, but already tracked in `TODO.md`.
