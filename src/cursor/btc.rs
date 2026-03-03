/// Cursor for Bitcoin-style chains.
///
/// Ordering: lexicographic on (block_height, tx_index).
/// block_height is the more significant field.
/// tx_index is block-scoped.
///
/// Field order determines derived Ord: block_height first (most significant).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct BtcCursor {
    pub block_height: u64,
    pub tx_index: u32,
}
