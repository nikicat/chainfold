use chainfold::cursor::{CexCursor, EvmCursor};
use chainfold::event::Event;
use chainfold::source::EventSource;

// --- Event envelope tests ---

#[test]
fn event_with_block_hash_can_be_constructed() {
    let cursor = EvmCursor {
        block_number: 100,
        log_index: 0,
    };
    let block_hash = [0xab_u8; 32];
    let event: Event<EvmCursor, u64> = Event {
        cursor,
        data: 42_u64,
        block_hash: Some(block_hash),
    };
    assert_eq!(event.data, 42);
    assert_eq!(event.block_hash, Some([0xab; 32]));
    assert_eq!(event.cursor.block_number, 100);
}

#[test]
fn event_without_block_hash_can_be_constructed() {
    // CEX events have no blocks, so block_hash = None.
    let cursor = CexCursor {
        seq_id: 1,
        timestamp_ms: 1000,
    };
    let event: Event<CexCursor, String> = Event {
        cursor,
        data: "fill".to_string(),
        block_hash: None,
    };
    assert_eq!(event.data, "fill");
    assert!(event.block_hash.is_none());
}

#[test]
fn event_is_clone() {
    let cursor = EvmCursor {
        block_number: 5,
        log_index: 1,
    };
    let event: Event<EvmCursor, u32> = Event {
        cursor,
        data: 99,
        block_hash: None,
    };
    let cloned = event.clone();
    assert_eq!(cloned.data, 99);
    assert_eq!(cloned.cursor.block_number, 5);
}

// --- MockEventSource ---

/// A simple mock implementing EventSource for testing.
/// Holds a fixed set of events and returns them filtered by cursor range.
struct MockEventSource<C: chainfold::cursor::ChainCursor, E: Clone + Send + Sync + 'static> {
    events: Vec<Event<C, E>>,
    state_bytes: Vec<u8>,
}

#[async_trait::async_trait]
impl<C, E> EventSource for MockEventSource<C, E>
where
    C: chainfold::cursor::ChainCursor,
    E: Clone + Send + Sync + 'static,
{
    type Cursor = C;
    type Event = E;
    type Error = std::convert::Infallible;

    async fn fetch_state(&self, _at: &C) -> Result<Vec<u8>, Self::Error> {
        Ok(self.state_bytes.clone())
    }

    async fn fetch_events(
        &self,
        from: &C,
        to: &C,
    ) -> Result<Vec<Event<C, E>>, Self::Error> {
        let filtered = self
            .events
            .iter()
            .filter(|e| &e.cursor >= from && &e.cursor < to)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn subscribe(
        &self,
        _from: &C,
    ) -> Result<
        Box<
            dyn futures_core::Stream<
                    Item = Result<Event<C, E>, Self::Error>,
                > + Unpin
                + Send,
        >,
        Self::Error,
    > {
        Ok(Box::new(futures_util::stream::empty()))
    }
}

// --- MockEventSource tests ---

#[tokio::test]
async fn fetch_state_returns_configured_bytes() {
    let source: MockEventSource<EvmCursor, u64> = MockEventSource {
        events: vec![],
        state_bytes: vec![1, 2, 3, 4],
    };
    let at = EvmCursor {
        block_number: 0,
        log_index: 0,
    };
    let bytes = source.fetch_state(&at).await.unwrap();
    assert_eq!(bytes, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn fetch_events_returns_events_in_cursor_range() {
    let events = vec![
        Event {
            cursor: EvmCursor {
                block_number: 10,
                log_index: 0,
            },
            data: 100_u64,
            block_hash: None,
        },
        Event {
            cursor: EvmCursor {
                block_number: 20,
                log_index: 0,
            },
            data: 200_u64,
            block_hash: None,
        },
        Event {
            cursor: EvmCursor {
                block_number: 30,
                log_index: 0,
            },
            data: 300_u64,
            block_hash: None,
        },
    ];
    let source = MockEventSource {
        events,
        state_bytes: vec![],
    };

    // Range [block 10, block 25) — should include block 10 and 20, not 30.
    let from = EvmCursor {
        block_number: 10,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 25,
        log_index: 0,
    };
    let result = source.fetch_events(&from, &to).await.unwrap();
    assert_eq!(result.len(), 2, "expected 2 events in range [10, 25)");
    assert_eq!(result[0].data, 100);
    assert_eq!(result[1].data, 200);
}

#[tokio::test]
async fn fetch_events_excludes_events_at_to_cursor() {
    let events = vec![Event {
        cursor: EvmCursor {
            block_number: 10,
            log_index: 0,
        },
        data: 42_u64,
        block_hash: None,
    }];
    let source = MockEventSource {
        events,
        state_bytes: vec![],
    };

    // Range [block 10, block 10) is empty (from == to, half-open range).
    let from = EvmCursor {
        block_number: 10,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 10,
        log_index: 0,
    };
    let result = source.fetch_events(&from, &to).await.unwrap();
    assert_eq!(result.len(), 0, "half-open range [from, from) should be empty");
}

#[tokio::test]
async fn fetch_events_empty_when_no_events_in_range() {
    let events: Vec<Event<EvmCursor, u64>> = vec![];
    let source = MockEventSource {
        events,
        state_bytes: vec![],
    };
    let from = EvmCursor {
        block_number: 0,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 100,
        log_index: 0,
    };
    let result = source.fetch_events(&from, &to).await.unwrap();
    assert!(result.is_empty());
}

/// Verify EventSource is usable with tokio::spawn (requires Send bounds on futures).
/// This test proves the trait satisfies Send requirements for multi-threaded use.
#[tokio::test]
async fn event_source_futures_are_send() {
    let source = MockEventSource::<EvmCursor, u64> {
        events: vec![],
        state_bytes: vec![],
    };
    let from = EvmCursor {
        block_number: 0,
        log_index: 0,
    };
    let to = EvmCursor {
        block_number: 100,
        log_index: 0,
    };
    // If EventSource futures are Send, this tokio::spawn will compile.
    let handle = tokio::spawn(async move {
        source.fetch_events(&from, &to).await.unwrap()
    });
    let result = handle.await.unwrap();
    assert!(result.is_empty());
}
