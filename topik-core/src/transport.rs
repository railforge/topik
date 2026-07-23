use crate::error::TopikError;
use crate::protocol::Protocol;
use bytes::Bytes;

/// A raw message received from the broker.
///
/// Contains the topic string exactly as received from the broker
/// and the raw payload bytes. The `TopikClient` layer converts
/// these into typed topics via `TopicWire::parse` and `Encoding::decode`.
#[derive(Debug, Clone)]
pub struct RawMessage {
    /// The topic string as received from the broker.
    /// e.g. "sensors/42/temperature"
    pub topic: String,

    /// The raw payload bytes.
    pub payload: Bytes,
}

/// A stream of raw messages from the broker.
///
/// Each transport implements this for its own message delivery mechanism.
/// `TopikClient::subscribe` returns a typed `Subscriber` that wraps this internally.
pub trait MessageStream: Send {
    /// Wait for the next message from the broker.
    ///
    /// Returns `None` when the stream is closed. Either the connection
    /// dropped or the subscription was explicitly cancelled.
    fn next(&mut self) -> impl Future<Output = Option<RawMessage>> + Send;
}

/// Abstraction over a pub/sub transport.
///
/// Implementations wrap a real broker client and translate between topik's generic
/// message model and the broker's specific API.
pub trait Transport: Send + Sync {
    type Protocol: Protocol;
    /// The stream type returned by `subscribe`.
    type Stream: MessageStream;

    /// Publish raw bytes to a topic string.
    fn publish(
        &self,
        topic: String,
        payload: Bytes,
    ) -> impl Future<Output = Result<(), TopikError>> + Send;

    /// Subscribe to a topic pattern and return a stream of raw messages.
    fn subscribe(
        &self,
        pattern: String,
    ) -> impl Future<Output = Result<Self::Stream, TopikError>> + Send;

    /// Unsubscribe from a topic pattern.
    fn unsubscribe(&self, pattern: String) -> impl Future<Output = Result<(), TopikError>> + Send;
}
