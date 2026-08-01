#[cfg(feature = "mqtt")]
mod mqtt_impl {
    use bytes::Bytes;
    use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
    use std::time::Duration;
    use topik_core::__private::{TopicEnum, TopicWire};
    use topik_core::protocol::{Mqtt, Protocol};
    use topik_core::{Encoding, TopikError};

    /// Builder for [`MqttClient`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (client, eventloop) = MqttClient::builder()
    ///     .url("localhost", 1883)
    ///     .client_id("my-service")
    ///     .keep_alive(30)
    ///     .build();
    /// ```
    pub struct MqttClientBuilder {
        client_id: String,
        host: String,
        port: u16,
        keep_alive: Duration,
        channel_capacity: usize,
    }

    impl MqttClientBuilder {
        pub fn client_id(mut self, id: impl Into<String>) -> Self {
            self.client_id = id.into();
            self
        }

        pub fn url(mut self, host: impl Into<String>, port: u16) -> Self {
            self.host = host.into();
            self.port = port;
            self
        }

        pub fn keep_alive(mut self, secs: u64) -> Self {
            self.keep_alive = Duration::from_secs(secs);
            self
        }

        pub fn channel_capacity(mut self, capacity: usize) -> Self {
            self.channel_capacity = capacity;
            self
        }

        /// Build the client and event loop.
        ///
        /// Returns `(MqttClient, EventLoop)`
        pub fn build(self) -> (MqttClient, EventLoop) {
            let mut options = MqttOptions::new(&self.client_id, &self.host, self.port);
            options.set_keep_alive(self.keep_alive);

            let (client, eventloop) = AsyncClient::new(options, self.channel_capacity);
            (MqttClient { inner: client }, eventloop)
        }
    }

    /// MQTT client with typed topic support.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (client, mut eventloop) = MqttClient::builder()
    ///     .url("localhost", 1883)
    ///     .client_id("my-service")
    ///     .build();
    ///
    /// client.subscribe::<TemperatureReading>().await?;
    ///
    /// client.publish(TemperatureReading { device_id: 42, data: 23.5 }).await?;
    ///
    /// while let Ok(event) = eventloop.poll().await {
    ///     if let Event::Incoming(Packet::Publish(p)) = event {
    ///         match client.parse::<SensorTopics>(&p.topic, &p.payload)? {
    ///             SensorTopics::Temperature(msg) => handle_temp(msg),
    ///             SensorTopics::Reboot(msg) => handle_reboot(msg),
    ///         }
    ///     }
    /// }
    /// ```
    #[derive(Clone)]
    pub struct MqttClient {
        inner: AsyncClient,
    }

    impl MqttClient {
        pub fn builder() -> MqttClientBuilder {
            MqttClientBuilder {
                client_id: "topik-client".to_string(),
                host: "localhost".to_string(),
                port: 1883,
                keep_alive: Duration::from_secs(30),
                channel_capacity: 10,
            }
        }

        /// Access the underlying rumqttc AsyncClient.
        ///
        /// Use this for protocol-specific features not covered by topik
        /// (QoS configuration, LWT, retained messages, TLS etc.)
        pub fn inner(&self) -> &AsyncClient {
            &self.inner
        }

        /// Subscribe to all messages matching this topic type.
        ///
        /// Sends a SUBSCRIBE packet with the correct wildcard pattern.
        /// Default QoS is AtLeastOnce.
        pub async fn subscribe<M: TopicWire>(&self) -> Result<(), TopikError> {
            let pattern =
                M::wildcard_pattern(Mqtt::SEPARATOR, Mqtt::SINGLE_WILDCARD, Mqtt::MULTI_WILDCARD);
            self.inner
                .subscribe(pattern, QoS::AtLeastOnce)
                .await
                .map_err(|e| TopikError::Encoding(Box::new(e)))
        }

        /// Subscribe to all topics covered by a TopicEnum.
        ///
        /// Sends SUBSCRIBE packets for all patterns in the enum.
        pub async fn subscribe_many<E: TopicEnum>(&self) -> Result<(), TopikError> {
            let patterns =
                E::patterns(Mqtt::SEPARATOR, Mqtt::SINGLE_WILDCARD, Mqtt::MULTI_WILDCARD);
            for pattern in patterns {
                self.inner
                    .subscribe(pattern, QoS::AtLeastOnce)
                    .await
                    .map_err(|e| TopikError::Encoding(Box::new(e)))?;
            }
            Ok(())
        }

        /// Publish a typed topic message.
        ///
        /// Await directly for default settings (QoS::AtLeastOnce, retain false),
        /// or chain options before awaiting:
        ///
        /// ```ignore
        /// // default
        /// client.publish(TemperatureReading { device_id: 42, data: 23.5 }).await?;
        ///
        /// // with options
        /// client.publish(TemperatureReading { device_id: 42, data: 23.5 })
        ///     .qos(QoS::AtMostOnce)
        ///     .retain(true)
        ///     .await?;
        /// ```
        pub fn publish<M: TopicWire>(&self, topic: M) -> MqttPublishBuilder<M> {
            MqttPublishBuilder {
                client: self.inner.clone(),
                topic,
                qos: QoS::AtLeastOnce,
                retain: false,
            }
        }

        /// Parse an incoming MQTT publish packet into a typed TopicEnum variant.
        ///
        /// Call this in your event loop after receiving a Packet::Publish.
        ///
        /// # Example
        ///
        /// ```ignore
        /// while let Ok(event) = eventloop.poll().await {
        ///     if let Event::Incoming(Packet::Publish(p)) = event {
        ///         match client.parse::<SensorTopics>(&p.topic, &p.payload)? {
        ///             SensorTopics::Temperature(msg) => handle_temp(msg),
        ///             SensorTopics::Reboot(msg) => handle_reboot(msg),
        ///         }
        ///     }
        /// }
        /// ```
        pub fn parse<E: TopicEnum>(&self, topic: &str, payload: &[u8]) -> Result<E, TopikError> {
            E::try_from_raw(topic, payload, Mqtt::SEPARATOR)
        }

        /// Parse an incoming publish packet into a single typed topic.
        ///
        /// Returns None if the topic doesn't match this type.
        pub fn parse_topic<M: TopicWire>(
            &self,
            topic: &str,
            payload: &[u8],
        ) -> Result<Option<M>, TopikError> {
            match M::parse(topic, Mqtt::SEPARATOR) {
                Ok(key) => {
                    let data = M::Encoding::decode(Bytes::copy_from_slice(payload))?;
                    Ok(Some(M::from_key_and_payload(key, data)))
                }
                Err(_) => Ok(None),
            }
        }

        /// Unsubscribe from a topic pattern.
        pub async fn unsubscribe<M: TopicWire>(&self) -> Result<(), TopikError> {
            let pattern =
                M::wildcard_pattern(Mqtt::SEPARATOR, Mqtt::SINGLE_WILDCARD, Mqtt::MULTI_WILDCARD);
            self.inner
                .unsubscribe(pattern)
                .await
                .map_err(|e| TopikError::Encoding(Box::new(e)))
        }

        /// Returns the topic string for a message using MQTT separator.
        ///
        /// Useful for logging and debugging.
        ///
        /// ```ignore
        /// let reading = TemperatureReading { device_id: 42, data: 23.5 };
        /// println!("{}", client.display(&reading));
        /// // -> "sensors/42/temperature"
        /// ```
        pub fn display<M: TopicWire>(&self, topic: &M) -> String {
            topic.render(Mqtt::SEPARATOR)
        }
    }

    pub struct MqttPublishBuilder<M: TopicWire> {
        client: AsyncClient,
        topic: M,
        qos: QoS,
        retain: bool,
    }

    impl<M: TopicWire> MqttPublishBuilder<M> {
        /// Set the QoS level for this publish.
        ///
        /// Default is `QoS::AtLeastOnce`.
        pub fn qos(mut self, qos: QoS) -> Self {
            self.qos = qos;
            self
        }

        /// Set the retain flag for this publish.
        ///
        /// Default is `false`.
        pub fn retain(mut self, retain: bool) -> Self {
            self.retain = retain;
            self
        }
    }

    impl<M: TopicWire + Send + 'static> std::future::IntoFuture for MqttPublishBuilder<M> {
        type Output = Result<(), TopikError>;
        type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

        fn into_future(self) -> Self::IntoFuture {
            Box::pin(async move {
                let topic_str = self.topic.render(Mqtt::SEPARATOR);
                let payload = M::Encoding::encode(self.topic.payload())?;
                self.client
                    .publish(topic_str, self.qos, self.retain, payload.to_vec())
                    .await
                    .map_err(|e| TopikError::Encoding(Box::new(e)))
            })
        }
    }
}

#[cfg(feature = "mqtt")]
pub use mqtt_impl::{MqttClient, MqttClientBuilder, MqttPublishBuilder};
