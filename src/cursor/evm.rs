/// Cursor for EVM chains (Ethereum, Polygon, etc.).
///
/// Ordering: lexicographic on (block_number, log_index).
/// block_number is the more significant field — a higher block always comes after a lower one.
/// log_index is block-scoped: multiple events per block are ordered within that block.
///
/// Field order in the struct determines the derived Ord comparison order.
/// block_number is listed first so it is compared first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct EvmCursor {
    pub block_number: u64,
    pub log_index: u32,
}
