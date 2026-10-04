use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use topik_core::__private::{SubscribeBuilder, TopicEnum, TopicWire, Transport};
use topik_core::protocol::Protocol;
use topik_core::{Encoding, TopikError};

use crate::subscriber::{EnumSubscriber, Subscriber};

/// Topik's typed client. A thin wrapper around a pub/sub transport.
///
/// `TopikClient` sits in front of any [`Transport`] implementation and adds
/// compile-time type safety for topic definitions, payload encoding, and
/// subscription patterns. The underlying protocol client is provided by the transport.
///
/// The protocol is inferred from the transport's associated [`Protocol`] type.
///
/// # Example
///
/// ```ignore
/// use topik::TopikClient;
/// use topik::transport::InMemoryTransport;
/// use topik::protocol::Mqtt;
///
/// let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
///
/// client.publish(TemperatureReading { device_id: 42, data: 23.5 }).await?;
///
/// let mut sub = client.subscribe::<TemperatureReading>().await?;
/// while let Some(msg) = sub.next().await {
///     println!("device {} -> {}°C", msg.device_id, msg.data);
/// }
/// ```
pub struct TopikClient<T: Transport> {
    transport: T,
}

impl<T: Transport> TopikClient<T> {
    /// Wrap a transport in a typed topik client.
    pub fn new(transport: T) -> Self {
        TopikClient { transport }
    }

    /// Returns the subscription pattern for a single topic type, using this client's protocol.
    ///
    /// ```ignore
    /// println!("{}", client.pattern::<TemperatureReading>());
    /// // MQTT -> "sensors/+/temperature"
    /// // NATS -> "sensors.*.temperature"
    /// ```
    pub fn pattern<M: TopicWire>(&self) -> String {
        M::wildcard_pattern_for::<T::Protocol>()
    }

    /// Returns all subscription patterns this enum covers, using this client's protocol.
    ///
    /// Useful for logging or passing patterns to external systems.
    ///
    /// ```ignore
    /// for pattern in client.patterns::<SensorTopics>() {
    ///     println!("{}", pattern);
    /// }
    /// ```
    pub fn patterns<E: TopicEnum>(&self) -> Vec<String> {
        E::patterns_for::<T::Protocol>()
    }

    /// Publish a typed topic message.
    ///
    /// Renders the topic string using the protocol's separator and
    /// encodes the payload using the topic's declared encoding.
    /// Wrong type or wrong topic structure is a compile error.
    pub async fn publish<M: TopicWire>(&self, topic: M) -> Result<(), TopikError> {
        let topic_str = topic.render(T::Protocol::SEPARATOR);
        let payload = M::Encoding::encode(topic.payload())?;
        self.transport.publish(topic_str, payload).await
    }

    /// Subscribe to messages matching this topic type.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // all devices
    /// let mut sub = client.subscribe::<TemperatureReading>().await?;
    ///
    /// // only device 42
    /// let mut sub = client
    ///     .subscribe::<TemperatureReading>()
    ///     .pin(|b| b.device_id(42))
    ///     .await?;
    /// ```
    pub fn subscribe<M: TopicWire>(&self) -> TopikSubscribeBuilder<'_, T, M>
    where
        T: Clone,
    {
        TopikSubscribeBuilder {
            client: self,
            inner: M::subscribe_builder(),
        }
    }

    /// Subscribe to all topics covered by a [`TopicEnum`].
    ///
    /// Returns an [`EnumSubscriber`] that yields typed enum variants
    /// as messages arrive. All patterns in the enum are subscribed to concurrently.
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
    pub async fn subscribe_many<E: TopicEnum>(&self) -> Result<EnumSubscriber<E>, TopikError>
    where
        T: Clone + 'static,
        T::Stream: Send + 'static,
    {
        let patterns = E::patterns(
            T::Protocol::SEPARATOR,
            T::Protocol::SINGLE_WILDCARD,
            T::Protocol::MULTI_WILDCARD,
        );

        let mut streams = Vec::new();
        for pattern in patterns {
            let stream = self.transport.subscribe(pattern).await?;
            streams.push(stream);
        }

        Ok(EnumSubscriber::new::<T>(streams, T::Protocol::SEPARATOR))
    }

    /// Returns the topic string for a message using this client's protocol separator.
    ///
    /// Useful for logging and debugging. Shows exactly what topic string
    /// will be sent to or received from the broker.
    ///
    /// ```ignore
    /// let reading = TemperatureReading { device_id: 42, data: 23.5 };
    /// println!("{}", client.display(&reading));
    /// // MQTT -> "sensors/42/temperature"
    /// // NATS -> "sensors.42.temperature"
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

/// A builder for typed subscriptions.
///
/// Returned by [`TopikClient::subscribe`]. Await directly for a wildcard
/// subscription, or pin specific segments before awaiting.
///
/// # Example
///
/// ```ignore
/// // wildcard (all devices)
/// let mut sub = client.subscribe::<TemperatureReading>().await?;
///
/// // pinned (only device 42)
/// let mut sub = client
///     .subscribe::<TemperatureReading>()
///     .pin(|b| b.device_id(42))
///     .await?;
/// ```
pub struct TopikSubscribeBuilder<'a, T: Transport + Clone, M: TopicWire> {
    client: &'a TopikClient<T>,
    inner: M::SubscribeBuilder,
}

impl<'a, T: Transport + Clone, M: TopicWire> TopikSubscribeBuilder<'a, T, M> {
    /// Pin specific segment values before subscribing.
    ///
    /// Unpinned segments become wildcards in the subscription pattern.
    /// Call multiple times or chain setters inside the closure to pin
    /// multiple segments.
    ///
    /// ```ignore
    /// client.subscribe::<FactoryReading>()
    ///     .pin(|b| b.device_id(42).kind("temperature".to_string()))
    ///     .await?;
    /// ```
    pub fn pin<F>(mut self, f: F) -> Self
    where
        F: FnOnce(M::SubscribeBuilder) -> M::SubscribeBuilder,
    {
        self.inner = f(self.inner);
        self
    }
}

impl<'a, T: Transport + Clone, M: TopicWire> IntoFuture for TopikSubscribeBuilder<'a, T, M> {
    type Output = Result<Subscriber<T, M>, TopikError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let pattern = self
                .inner
                .build_pattern(T::Protocol::SEPARATOR, T::Protocol::SINGLE_WILDCARD);
            let stream = self.client.transport.subscribe(pattern.clone()).await?;

            Ok(Subscriber {
                stream,
                pattern,
                transport: self.client.transport.clone(),
                _topic: PhantomData,
            })
        })
    }
}
