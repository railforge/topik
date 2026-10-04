#[cfg(feature = "rumqttc")]
mod transport {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use topik_core::__private::Transport;
    use topik_core::TopikError;
    use topik_core::protocol::{Mqtt, Protocol};
    use topik_core::transport::{MessageStream, RawMessage};

    use crate::transport::inmemory::matches_pattern;

    struct SubEntry {
        pattern: String,
        sender: mpsc::Sender<RawMessage>,
    }

    struct MqttTransportInner {
        client: AsyncClient,
        subs: Mutex<Vec<SubEntry>>,
    }

    /// A [`Transport`] implementation backed by an MQTT broker via rumqttc.
    ///
    /// The event loop runs in a background task spawned at build time. Use this
    /// with [`TopikClient`] for the full typed builder API.
    ///
    /// For direct access to rumqttc's `AsyncClient` and `EventLoop` (custom QoS,
    /// LWT, retained messages, TLS, tight event loop control), use [`MqttClient`].
    #[derive(Clone)]
    pub struct MqttTransport {
        inner: Arc<MqttTransportInner>,
    }

    impl MqttTransport {
        /// Returns a builder for configuring the transport.
        pub fn builder() -> MqttTransportBuilder {
            MqttTransportBuilder {
                client_id: "topik-client".to_string(),
                host: "localhost".to_string(),
                port: 1883,
                keep_alive: Duration::from_secs(30),
                channel_capacity: 10,
                clean_session: true,
                credentials: None,
                last_will: None,
                options_modifier: None,
            }
        }
    }

    impl Transport for MqttTransport {
        type Protocol = Mqtt;
        type Stream = MqttStream;

        async fn publish(&self, topic: String, payload: Bytes) -> Result<(), TopikError> {
            self.inner
                .client
                .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
                .await
                .map_err(|e| TopikError::Encoding(Box::new(e)))
        }

        async fn subscribe(&self, pattern: String) -> Result<Self::Stream, TopikError> {
            let (tx, rx) = mpsc::channel(256);

            {
                let mut subs = self.inner.subs.lock().unwrap();
                subs.push(SubEntry {
                    pattern: pattern.clone(),
                    sender: tx,
                });
            }

            self.inner
                .client
                .subscribe(&pattern, QoS::AtLeastOnce)
                .await
                .map_err(|e| TopikError::Encoding(Box::new(e)))?;

            Ok(MqttStream { receiver: rx })
        }

        async fn unsubscribe(&self, pattern: String) -> Result<(), TopikError> {
            {
                let mut subs = self.inner.subs.lock().unwrap();
                subs.retain(|e| e.pattern != pattern);
            }

            self.inner
                .client
                .unsubscribe(&pattern)
                .await
                .map_err(|e| TopikError::Encoding(Box::new(e)))
        }
    }

    /// A stream of messages for a single subscription on [`MqttTransport`].
    ///
    /// Receives messages forwarded by the transport's background reactor task.
    pub struct MqttStream {
        receiver: mpsc::Receiver<RawMessage>,
    }

    impl MessageStream for MqttStream {
        async fn next(&mut self) -> Option<RawMessage> {
            self.receiver.recv().await
        }
    }

    /// Builder for [`MqttTransport`].
    ///
    /// Created via [`MqttTransport::builder()`].
    pub struct MqttTransportBuilder {
        client_id: String,
        host: String,
        port: u16,
        keep_alive: Duration,
        channel_capacity: usize,
        clean_session: bool,
        credentials: Option<(String, String)>,
        last_will: Option<rumqttc::LastWill>,
        options_modifier: Option<Box<dyn FnOnce(MqttOptions) -> MqttOptions>>,
    }

    impl MqttTransportBuilder {
        /// Set the MQTT client ID.
        pub fn client_id(mut self, id: impl Into<String>) -> Self {
            self.client_id = id.into();
            self
        }

        /// Set the broker host and port.
        pub fn url(mut self, host: impl Into<String>, port: u16) -> Self {
            self.host = host.into();
            self.port = port;
            self
        }

        /// Set the keep alive interval in seconds. Default is 30.
        pub fn keep_alive(mut self, secs: u64) -> Self {
            self.keep_alive = Duration::from_secs(secs);
            self
        }

        /// Set the request channel capacity. Default is 10.
        pub fn channel_capacity(mut self, capacity: usize) -> Self {
            self.channel_capacity = capacity;
            self
        }

        /// Set the clean session flag. Default is `true`.
        pub fn clean_session(mut self, clean: bool) -> Self {
            self.clean_session = clean;
            self
        }

        /// Set username and password credentials.
        pub fn credentials(
            mut self,
            username: impl Into<String>,
            password: impl Into<String>,
        ) -> Self {
            self.credentials = Some((username.into(), password.into()));
            self
        }

        /// Set the Last Will and Testament message.
        pub fn last_will(mut self, will: rumqttc::LastWill) -> Self {
            self.last_will = Some(will);
            self
        }

        /// Apply a custom modifier to the underlying `MqttOptions`.
        ///
        /// Use for advanced configuration not exposed by the builder
        /// (TLS, websockets, proxy settings, etc.)
        pub fn with_options<F>(mut self, f: F) -> Self
        where
            F: FnOnce(MqttOptions) -> MqttOptions + 'static,
        {
            self.options_modifier = Some(Box::new(f));
            self
        }

        /// Build the transport and start the background reactor task.
        ///
        /// Spawns a tokio task that drives the rumqttc event loop and fans
        /// incoming publish packets to the matching subscriber channels.
        /// The task exits when the transport is dropped (connection closes).
        pub async fn build(self) -> Result<MqttTransport, TopikError> {
            let mut options = MqttOptions::new(&self.client_id, &self.host, self.port);
            options.set_keep_alive(self.keep_alive);
            options.set_clean_session(self.clean_session);

            if let Some((username, password)) = self.credentials {
                options.set_credentials(username, password);
            }
            if let Some(will) = self.last_will {
                options.set_last_will(will);
            }
            if let Some(modifier) = self.options_modifier {
                options = modifier(options);
            }

            let (client, mut eventloop) = AsyncClient::new(options, self.channel_capacity);

            let inner = Arc::new(MqttTransportInner {
                client,
                subs: Mutex::new(Vec::new()),
            });

            let inner_task = Arc::clone(&inner);
            tokio::spawn(async move {
                loop {
                    match eventloop.poll().await {
                        Ok(Event::Incoming(Packet::Publish(p))) => {
                            let msg = RawMessage {
                                topic: p.topic.clone(),
                                payload: Bytes::from(p.payload.to_vec()),
                            };
                            let subs = inner_task.subs.lock().unwrap();
                            for entry in subs.iter() {
                                if matches_pattern(
                                    &p.topic,
                                    &entry.pattern,
                                    Mqtt::SEPARATOR,
                                    Mqtt::SINGLE_WILDCARD,
                                    Mqtt::MULTI_WILDCARD,
                                ) {
                                    let _ = entry.sender.try_send(msg.clone());
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });

            Ok(MqttTransport { inner })
        }
    }
}

#[cfg(feature = "rumqttc")]
mod client {
    use bytes::Bytes;
    use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
    use std::time::Duration;
    use topik_core::__private::{TopicEnum, TopicWire};
    use topik_core::protocol::{Mqtt, Protocol};
    use topik_core::{Encoding, TopikError};

    /// Builder for [`MqttClient`].
    ///
    /// Created via [`MqttClient::builder()`]. Configure the connection
    /// then call [`build`](MqttClientBuilder::build) to get the client
    /// and event loop.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rumqttc::{LastWill, QoS};
    ///
    /// let (client, mut eventloop) = MqttClient::builder()
    ///     .url("localhost", 1883)
    ///     .client_id("my-service")
    ///     .keep_alive(30)
    ///     .clean_session(true)
    ///     .credentials("user", "password")
    ///     .last_will(LastWill::new(
    ///         "devices/my-service/status",
    ///         "offline",
    ///         QoS::AtLeastOnce,
    ///         true,
    ///     ))
    ///     .build();
    ///
    /// // for TLS or other advanced config use with_options:
    /// let (client, mut eventloop) = MqttClient::builder()
    ///     .url("localhost", 8883)
    ///     .client_id("my-service")
    ///     .with_options(|mut opts| {
    ///         opts.set_transport(rumqttc::Transport::tls_with_config(tls_config.into()));
    ///         opts
    ///     })
    ///     .build();
    /// ```
    pub struct MqttClientBuilder {
        client_id: String,
        host: String,
        port: u16,
        keep_alive: Duration,
        channel_capacity: usize,
        clean_session: bool,
        credentials: Option<(String, String)>,
        last_will: Option<rumqttc::LastWill>,
        options_modifier: Option<Box<dyn FnOnce(MqttOptions) -> MqttOptions>>,
    }

    impl MqttClientBuilder {
        /// Set the MQTT client ID.
        pub fn client_id(mut self, id: impl Into<String>) -> Self {
            self.client_id = id.into();
            self
        }

        /// Set the broker host and port.
        pub fn url(mut self, host: impl Into<String>, port: u16) -> Self {
            self.host = host.into();
            self.port = port;
            self
        }

        /// Set the keep alive interval in seconds.
        ///
        /// The broker disconnects the client if no message is received
        /// within 1.5x this interval. Default is 30 seconds.
        pub fn keep_alive(mut self, secs: u64) -> Self {
            self.keep_alive = Duration::from_secs(secs);
            self
        }

        /// Set the request channel capacity. Default is 10.
        pub fn channel_capacity(mut self, capacity: usize) -> Self {
            self.channel_capacity = capacity;
            self
        }

        /// Set the clean session flag.
        ///
        /// When `true` the broker clears all state on disconnect.
        /// When `false` the broker holds state for reconnection with
        /// the same `client_id`. Requires a non-empty `client_id`.
        /// Default is `true`.
        pub fn clean_session(mut self, clean: bool) -> Self {
            self.clean_session = clean;
            self
        }

        /// Set username and password credentials.
        pub fn credentials(
            mut self,
            username: impl Into<String>,
            password: impl Into<String>,
        ) -> Self {
            self.credentials = Some((username.into(), password.into()));
            self
        }

        /// Set Last Will and Testament.
        ///
        /// The broker publishes this message if the client disconnects
        /// unexpectedly. Build a `LastWill` using rumqttc directly:
        ///
        /// ```ignore
        /// use rumqttc::{LastWill, QoS};
        ///
        /// MqttClient::builder()
        ///     .url("localhost", 1883)
        ///     .client_id("my-service")
        ///     .last_will(LastWill::new(
        ///         "devices/my-service/status",
        ///         "offline",
        ///         QoS::AtLeastOnce,
        ///         true,
        ///     ))
        ///     .build();
        /// ```
        pub fn last_will(mut self, will: rumqttc::LastWill) -> Self {
            self.last_will = Some(will);
            self
        }

        /// Apply a custom modifier to the underlying `MqttOptions`.
        ///
        /// Use this for advanced configuration not exposed by the builder
        /// (e.g., TLS, websockets, proxy settings etc.)
        ///
        /// ```ignore
        /// use rumqttc::Transport;
        ///
        /// MqttClient::builder()
        ///     .url("localhost", 8883)
        ///     .client_id("my-service")
        ///     .with_options(|mut opts| {
        ///         opts.set_transport(Transport::tls_with_config(tls_config.into()));
        ///         opts
        ///     })
        ///     .build();
        /// ```
        pub fn with_options<F>(mut self, f: F) -> Self
        where
            F: FnOnce(MqttOptions) -> MqttOptions + 'static,
        {
            self.options_modifier = Some(Box::new(f));
            self
        }

        /// Build the client and event loop.
        pub fn build(self) -> (MqttClient, EventLoop) {
            let mut options = MqttOptions::new(&self.client_id, &self.host, self.port);
            options.set_keep_alive(self.keep_alive);
            options.set_clean_session(self.clean_session);

            if let Some((username, password)) = self.credentials {
                options.set_credentials(username, password);
            }

            if let Some(will) = self.last_will {
                options.set_last_will(will);
            }

            if let Some(modifier) = self.options_modifier {
                options = modifier(options);
            }

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
                clean_session: true,
                credentials: None,
                last_will: None,
                options_modifier: None,
            }
        }

        /// Access the underlying rumqttc AsyncClient.
        ///
        /// Use this for protocol-specific features not covered by topik
        /// (QoS configuration, LWT, retained messages, TLS etc.)
        pub fn inner(&self) -> &AsyncClient {
            &self.inner
        }

        /// Returns the MQTT subscription pattern for a single topic type.
        ///
        /// Useful for logging or waiting for a SubAck packet.
        ///
        /// ```ignore
        /// println!("{}", client.pattern::<TemperatureReading>());
        /// // -> "sensors/+/temperature"
        /// ```
        pub fn pattern<M: TopicWire>(&self) -> String {
            M::wildcard_pattern_for::<Mqtt>()
        }

        /// Returns all MQTT subscription patterns this enum covers.
        ///
        /// Useful for logging or waiting for SubAck packets in the event loop.
        ///
        /// ```ignore
        /// for pattern in client.patterns::<SensorTopics>() {
        ///     println!("{}", pattern);
        /// }
        /// ```
        pub fn patterns<E: TopicEnum>(&self) -> Vec<String> {
            E::patterns_for::<Mqtt>()
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
            let patterns = E::patterns_for::<Mqtt>();
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

#[cfg(feature = "rumqttc")]
pub use client::{MqttClient, MqttClientBuilder, MqttPublishBuilder};

#[cfg(feature = "rumqttc")]
pub use transport::{MqttStream, MqttTransport, MqttTransportBuilder};
