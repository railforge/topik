use std::marker::PhantomData;
use topik_core::__private::{MessageStream, TopicWire, Transport};
use topik_core::protocol::Protocol;
use topik_core::{Encoding, TopikError};

/// A typed subscriber for a specific topic type.
///
/// Returned by [`TopikClient::subscribe`]. Receives messages from the broker,
/// parses the topic string, and decodes the payload automatically.
///
/// # Example
///
/// ```ignore
/// let mut sub = client.subscribe::<TemperatureReading>().await?;
///
/// while let Some(msg) = sub.next().await {
///     println!("device {} sent {:?}", msg.device_id, msg.data);
/// }
///
/// sub.unsubscribe().await?;
/// ```
pub struct Subscriber<T: Transport + Clone, M: TopicWire> {
    pub(crate) stream: T::Stream,
    pub(crate) pattern: String,
    pub(crate) transport: T,
    pub(crate) _topic: PhantomData<M>,
}

impl<T: Transport + Clone, M: TopicWire> Subscriber<T, M> {
    /// Wait for the next typed message from the broker.
    ///
    /// Parses the topic string and decodes the payload automatically.
    /// Silently skips messages that fail to parse or decode — these are
    /// likely from legacy publishers on the same topic pattern.
    ///
    /// Returns `None` when the stream is closed.
    pub async fn next(&mut self) -> Option<M> {
        loop {
            let raw = self.stream.next().await?;

            let key = match M::parse(&raw.topic, T::Protocol::SEPARATOR) {
                Ok(key) => key,
                Err(_) => continue, // skip unparseable topics
            };

            let payload = match M::Encoding::decode(raw.payload) {
                Ok(payload) => payload,
                Err(_) => continue, // skip undecodeable payloads
            };

            return Some(M::from_key_and_payload(key, payload));
        }
    }

    /// Explicitly unsubscribe from the topic pattern.
    ///
    /// Consumes the subscriber — it cannot be used after unsubscribing.
    /// If you drop the subscriber without calling this, the subscription
    /// may remain active until the connection closes.
    pub async fn unsubscribe(self) -> Result<(), TopikError> {
        self.transport.unsubscribe(self.pattern).await
    }

    /// Returns the subscription pattern string.
    ///
    /// ```ignore
    /// let sub = client.subscribe::<TemperatureReading>().await?;
    /// println!("{}", sub.pattern());
    /// // MQTT → "sensors/+/temperature"
    /// // NATS → "sensors.*.temperature"
    /// ```
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}
