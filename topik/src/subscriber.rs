use std::marker::PhantomData;
use tokio::sync::mpsc;
use topik_core::__private::{MessageStream, TopicEnum, TopicWire, Transport};
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
    /// Silently skips messages that fail to parse or decode. These are
    /// likely from legacy publishers on the same topic pattern.
    ///
    /// Returns `None` when the stream is closed.
    pub async fn next(&mut self) -> Option<M> {
        loop {
            let raw = self.stream.next().await?;

            let key = match M::parse(&raw.topic, T::Protocol::SEPARATOR) {
                Ok(key) => key,
                Err(_) => continue,
            };

            let payload = match M::Encoding::decode(raw.payload) {
                Ok(payload) => payload,
                Err(_) => continue,
            };

            return Some(M::from_key_and_payload(key, payload));
        }
    }

    /// Explicitly unsubscribe from the topic pattern.
    ///
    /// Consumes the subscriber. Cannot be used after unsubscribing.
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

/// A typed subscriber for a group of topic types defined by a [`TopicEnum`].
///
/// Returned by [`TopikClient::subscribe_many`]. Receives messages from all
/// topic patterns covered by the enum and dispatches them as typed enum
/// variants through a single channel.
///
/// Internally each topic pattern gets its own task that forwards decoded
/// messages into a shared `mpsc` channel. This means all topic streams
/// are polled concurrently — no one stream blocks another.
///
/// # Example
///
/// ```ignore
/// let mut sub = client.subscribe_many::<SensorTopics>().await?;
///
/// while let Some(event) = sub.next().await {
///     match event {
///         SensorTopics::Temperature(msg) => println!("{}°C", msg.data),
///         SensorTopics::Humidity(msg) => println!("{}%", msg.data),
///         SensorTopics::Reboot(msg) => println!("reboot {}", msg.device_id),
///     }
/// }
/// ```
///
/// # In-process pub/sub
///
/// `EnumSubscriber` with [`InMemoryTransport`] works as a typed in-process
/// event bus — no broker needed, same API as production:
///
/// ```ignore
/// let transport = InMemoryTransport::<Mqtt>::new();
///
/// let producer = TopikClient::new(transport.clone());
/// let consumer = TopikClient::new(transport.clone());
///
/// tokio::spawn(async move {
///     producer.publish(TemperatureReading { device_id: 1, data: 23.5 }).await?;
/// });
///
/// let mut sub = consumer.subscribe_many::<SensorTopics>().await?;
/// while let Some(event) = sub.next().await {
///     match event { ... }
/// }
/// ```
pub struct EnumSubscriber<E: TopicEnum> {
    receiver: mpsc::Receiver<E>,
}

impl<E: TopicEnum> EnumSubscriber<E> {
    pub(crate) fn new<T: Transport + Clone + 'static>(streams: Vec<T::Stream>, sep: char) -> Self
    where
        T::Stream: Send + 'static,
    {
        let (tx, rx) = mpsc::channel(256);

        for mut stream in streams {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(raw) = stream.next().await {
                    if let Ok(event) = E::try_from_raw(&raw.topic, &raw.payload, sep) {
                        if tx.send(event).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }

        EnumSubscriber { receiver: rx }
    }

    /// Wait for the next typed message from any of the subscribed topics.
    ///
    /// Returns `None` when all streams are closed.
    pub async fn next(&mut self) -> Option<E> {
        self.receiver.recv().await
    }
}
