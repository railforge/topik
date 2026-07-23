use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::broadcast;
use topik_core::TopikError;
use topik_core::protocol::Protocol;
use topik_core::transport::{MessageStream, RawMessage, Transport};

use super::matching::matches_pattern;

const CHANNEL_CAPACITY: usize = 1024;

struct InMemoryInner {
    sender: broadcast::Sender<RawMessage>,
}

/// An in-memory pub/sub transport for testing.
///
/// Implements the full [`Transport`] contract without a real broker.
/// Use this in tests.
///
/// # Example
///
/// ```rust
/// use topik::transport::InMemoryTransport;
/// use topik::protocol::Mqtt;
/// use topik::TopikClient;
///
/// let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
/// ```
pub struct InMemoryTransport<P: Protocol> {
    inner: Arc<InMemoryInner>,
    _protocol: PhantomData<P>,
}

impl<P: Protocol> InMemoryTransport<P> {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        InMemoryTransport {
            inner: Arc::new(InMemoryInner { sender }),
            _protocol: PhantomData,
        }
    }
}

impl<P: Protocol> Default for InMemoryTransport<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Protocol> Clone for InMemoryTransport<P> {
    fn clone(&self) -> Self {
        InMemoryTransport {
            inner: Arc::clone(&self.inner),
            _protocol: PhantomData,
        }
    }
}

/// A stream of messages for a specific subscription pattern.
pub struct InMemoryStream {
    receiver: broadcast::Receiver<RawMessage>,
    pattern: String,
    sep: char,
    single: &'static str,
    multi: &'static str,
}

impl MessageStream for InMemoryStream {
    async fn next(&mut self) -> Option<RawMessage> {
        loop {
            match self.receiver.recv().await {
                Ok(msg) => {
                    if matches_pattern(&msg.topic, &self.pattern, self.sep, self.single, self.multi)
                    {
                        return Some(msg);
                    }
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }
}

impl<P: Protocol + Send + Sync> Transport for InMemoryTransport<P> {
    type Protocol = P;
    type Stream = InMemoryStream;

    async fn publish(&self, topic: String, payload: Bytes) -> Result<(), TopikError> {
        let msg = RawMessage { topic, payload };
        let _ = self.inner.sender.send(msg);
        Ok(())
    }

    async fn subscribe(&self, pattern: String) -> Result<Self::Stream, TopikError> {
        let receiver = self.inner.sender.subscribe();
        Ok(InMemoryStream {
            receiver,
            pattern,
            sep: P::SEPARATOR,
            single: P::SINGLE_WILDCARD,
            multi: P::MULTI_WILDCARD,
        })
    }

    async fn unsubscribe(&self, _pattern: String) -> Result<(), TopikError> {
        Ok(())
    }
}
