use std::marker::PhantomData;
use topik_core::__private::{TopicWire, Transport};
use topik_core::protocol::Protocol;
use topik_core::{Encoding, TopikError};

use crate::subscriber::Subscriber;

/// The main entry point for typed pub/sub messaging.
///
/// `TopikClient` wraps a [`Transport`] and provides a typed API for
/// publishing and subscribing to topics defined with `#[derive(Topic)]`.
///
/// # Example
///
/// ```rust
/// use topik::{TopikClient, Topic};
/// use topik::encoding::RawEncoding;
/// use topik::protocol::Mqtt;
/// use bytes::Bytes;
///
/// #[derive(Topic)]
/// #[topic(segments("sensors", device_id), encoding = RawEncoding)]
/// pub struct TemperatureReading {
///     pub device_id: u64,
///     #[payload]
///     pub data: Bytes,
/// }
///
/// // connect
/// let client = TopikClient::connect(
///     Mqtt::builder()
///         .url("mqtt://localhost:1883")
///         .client_id("my-service")
///         .build()
/// ).await?;
///
/// // publish
/// client.publish(TemperatureReading {
///     device_id: 42,
///     data: Bytes::from("23.5"),
/// }).await?;
///
/// // subscribe
/// let mut sub = client.subscribe::<TemperatureReading>().await?;
/// while let Some(msg) = sub.next().await {
///     println!("device {} sent {:?}", msg.device_id, msg.data);
/// }
/// ```
pub struct TopikClient<T: Transport> {
    pub(crate) transport: T,
}

impl<T: Transport> TopikClient<T> {
    /// Create a new client from a transport.
    ///
    /// Prefer using [`TopikClient::connect`] with a builder for
    /// protocol-specific configuration.
    pub fn new(transport: T) -> Self {
        TopikClient { transport }
    }

    /// Publish a typed topic message to the broker.
    ///
    /// Renders the topic string using the transport's protocol separator
    /// and encodes the payload using the topic's declared encoding.
    pub async fn publish<M: TopicWire>(&self, topic: M) -> Result<(), TopikError> {
        let topic_str = topic.render(T::Protocol::SEPARATOR);
        let payload = M::Encoding::encode(topic.payload())?;
        self.transport.publish(topic_str, payload).await
    }

    /// Subscribe to all messages matching this topic type.
    ///
    /// Returns a [`Subscriber`] that yields typed messages as they arrive.
    /// The subscription pattern is generated automatically from the topic
    /// definition using the transport's wildcard tokens.
    pub async fn subscribe<M: TopicWire>(&self) -> Result<Subscriber<T, M>, TopikError>
    where
        T: Clone,
    {
        let pattern = M::wildcard_pattern(
            T::Protocol::SEPARATOR,
            T::Protocol::SINGLE_WILDCARD,
            T::Protocol::MULTI_WILDCARD,
        );
        let stream = self.transport.subscribe(pattern.clone()).await?;
        Ok(Subscriber {
            stream,
            pattern,
            transport: self.transport.clone(),
            _topic: PhantomData,
        })
    }

    /// Returns the topic string for a message using this client's protocol separator.
    ///
    /// ```rust
    /// let reading = TemperatureReading { device_id: 42, data: Bytes::new() };
    /// println!("{}", client.display(&reading));
    /// // MQTT → "sensors/42/temperature"
    /// // NATS → "sensors.42.temperature"
    /// ```
    pub fn display<M: TopicWire>(&self, topic: &M) -> String {
        topic.render(T::Protocol::SEPARATOR)
    }
}

impl<T: Transport + Clone> Clone for TopikClient<T> {
    fn clone(&self) -> Self {
        TopikClient {
            transport: self.transport.clone(),
        }
    }
}
