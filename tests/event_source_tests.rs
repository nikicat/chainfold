use chainfold::block::Block;
use chainfold::cursor::EvmCursor;
use chainfold::event::Event;
use chainfold::source::EventSource;

// --- MockBlockSource ---

/// A simple mock implementing EventSource for testing.
/// Holds a fixed set of blocks and returns them filtered by height range.
struct MockBlockSource {
    blocks: Vec<Block<EvmCursor, u64>>,
    state_bytes: Vec<u8>,
}

#[async_trait::async_trait]
impl EventSource for MockBlockSource {
    type Cursor = EvmCursor;
    type Event = u64;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &EvmCursor) -> Result<Vec<u8>, Self::Error> {
        Ok(self.state_bytes.clone())
    }

    async fn fetch_blocks(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<Block<EvmCursor, u64>>, Self::Error> {
        let filtered = self
            .blocks
            .iter()
            .filter(|b| b.height >= from_height && b.height < to_height)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn subscribe(
        &self,
        _from_height: u64,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Block<EvmCursor, u64>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        Ok(Box::new(futures_util::stream::empty()))
    }
}

// Helper: create a block with a single event.
fn make_block(height: u64, data: u64) -> Block<EvmCursor, u64> {
    let mut hash = [0u8; 32];
    hash[0] = (height & 0xff) as u8;
    let mut parent_hash = [0u8; 32];
    if height > 0 {
        parent_hash[0] = ((height - 1) & 0xff) as u8;
    }
    Block {
        block_hash: hash,
        parent_hash,
        height,
        events: vec![Event {
            cursor: EvmCursor {
                block_number: height,
                log_index: 0,
            },
            data,
        }],
    }
}

// --- Tests ---

#[tokio::test]
async fn test_fetch_blocks_returns_blocks_in_range() {
    let blocks: Vec<Block<EvmCursor, u64>> = (0..5).map(|h| make_block(h, h * 10)).collect();
    let source = MockBlockSource {
        blocks,
        state_bytes: vec![],
    };

    // Fetch range [1, 3) — should return blocks at height 1 and 2.
    let result = source.fetch_blocks(1, 3).await.unwrap();
    assert_eq!(result.len(), 2, "expected 2 blocks in range [1, 3)");
    assert_eq!(result[0].height, 1);
    assert_eq!(result[1].height, 2);
}

#[tokio::test]
async fn test_fetch_blocks_empty_range() {
    let blocks: Vec<Block<EvmCursor, u64>> = (0..5).map(|h| make_block(h, h * 10)).collect();
    let source = MockBlockSource {
        blocks,
        state_bytes: vec![],
    };

    // Fetch a range beyond the available blocks — should return empty.
    let result = source.fetch_blocks(100, 200).await.unwrap();
    assert!(result.is_empty(), "expected empty Vec for out-of-range fetch");
}

#[tokio::test]
async fn test_fetch_state_returns_bytes() {
    let source = MockBlockSource {
        blocks: vec![],
        state_bytes: vec![1, 2, 3, 4],
    };
    let at = EvmCursor {
        block_number: 0,
        log_index: 0,
    };
    let bytes = source.fetch_state(&at).await.unwrap();
    assert_eq!(bytes, vec![1, 2, 3, 4]);
}

#[test]
fn test_block_construction() {
    let block: Block<EvmCursor, u64> = Block {
        block_hash: [0xab; 32],
        parent_hash: [0x00; 32],
        height: 42,
        events: vec![Event {
            cursor: EvmCursor {
                block_number: 42,
                log_index: 0,
            },
            data: 99_u64,
        }],
    };

    assert_eq!(block.block_hash, [0xab; 32]);
    assert_eq!(block.parent_hash, [0x00; 32]);
    assert_eq!(block.height, 42);
    assert_eq!(block.events.len(), 1);
    assert_eq!(block.events[0].data, 99);
    assert_eq!(block.events[0].cursor.block_number, 42);
}

#[test]
fn test_event_no_block_hash() {
    // Compile-time check: Event<C, E> has only cursor and data fields (no block_hash).
    let event: Event<EvmCursor, u64> = Event {
        cursor: EvmCursor {
            block_number: 10,
            log_index: 0,
        },
        data: 42_u64,
    };
    assert_eq!(event.data, 42);
    assert_eq!(event.cursor.block_number, 10);
    // If this compiles without block_hash, the field has been successfully removed.
}
