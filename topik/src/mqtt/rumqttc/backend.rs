use crate::mqtt::Mqtt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::sync::mpsc;
use topik_core::__private::Transport;
use topik_core::transport::{MessageStream, RawMessage};
use topik_core::{TopicSyntax, TopikError};

use crate::inmemory::matches_pattern;

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
    type Syntax = Mqtt;
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
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
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
