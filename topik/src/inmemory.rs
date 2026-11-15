//! In-memory pub/sub transport.
//!
//! [`InMemoryTransport`] provides a fully featured pub/sub implementation
//! that runs without any broker, network, or container.
//!
//! It serves two distinct purposes:
//!
//! ## 1. Testing without a broker
//!
//! Write unit tests for your pub/sub logic against real protocol semantics.
//! Swap `InMemoryTransport` for a real `transport` in production without
//! changing any application code:
//!
//! ```ignore
//! // in tests
//! let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
//!
//! // in application
//! let (mqtt_client, eventloop) = MqttClient::builder()
//!     .url("localhost", 1883)
//!     .build();
//! ```
//!
//! The protocol type parameter determines which wildcard semantics are used:
//!
//! ```ignore
//! // MQTT: `+` single level, `#` multi level
//! let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
//! ```
//!
//! This means your tests verify the exact same matching behavior
//! your production broker uses.
//!
//! ## 2. Typed in-process pub/sub
//!
//! Use as a typed event bus between tasks in the same process.
//!
//! ```ignore
//! let transport = InMemoryTransport::<Mqtt>::new();
//!
//! let producer = TopikClient::new(transport.clone());
//! tokio::spawn(async move {
//!     producer.publish(TemperatureReading { device_id: 1, data: 23.5 }).await?;
//! });
//!
//! let consumer = TopikClient::new(transport.clone());
//! let mut sub = consumer.subscribe_many::<SensorTopics>().await?;
//! while let Some(event) = sub.next().await {
//!     match event {
//!         SensorTopics::Temperature(msg) => handle_temp(msg),
//!         SensorTopics::Humidity(msg) => handle_humidity(msg),
//!     }
//! }
//! ```
//!
//! ## Wildcard matching
//!
//! `InMemoryTransport` implements broker-side pattern matching in pure Rust.
//!
//! Implementing the [`Transport`] trait lets you bring your own broker backend
//! with its own matching semantics.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::broadcast;
use topik_core::transport::{MessageStream, RawMessage, Transport};
use topik_core::{TopicSyntax, TopikError};

const CHANNEL_CAPACITY: usize = 1024;

// Wildcard matching

/// Match a concrete topic string against a subscription pattern.
pub(crate) fn matches_pattern(
    topic: &str,
    pattern: &str,
    sep: char,
    single: &str,
    multi: &str,
) -> bool {
    // fast path (exact match)
    if topic == pattern {
        return true;
    }

    let topic_segments: Vec<&str> = topic.split(sep).collect();
    let pattern_segments: Vec<&str> = pattern.split(sep).collect();

    match_segments(&topic_segments, &pattern_segments, single, multi)
}

fn match_segments(topic: &[&str], pattern: &[&str], single: &str, multi: &str) -> bool {
    match (topic, pattern) {
        // both exhausted
        ([], []) => true,

        // pattern has multi wildcard as last segment
        (_, [p]) if *p == multi => !topic.is_empty(),

        // both have segments
        ([t, topic_rest @ ..], [p, pattern_rest @ ..]) => {
            let head_matches = *p == single || *p == *t;
            head_matches && match_segments(topic_rest, pattern_rest, single, multi)
        }

        // lengths don't match and no multi wildcard
        _ => false,
    }
}

// Transport

struct InMemoryInner {
    sender: broadcast::Sender<RawMessage>,
}

/// An in-memory pub/sub transport for testing.
///
/// Implements the full [`Transport`] contract without a real broker.
///
/// # Example
///
/// ```ignore
/// use topik::transport::InMemoryTransport;
/// use topik::protocol::Mqtt;
/// use topik::TopikClient;
///
/// let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
/// ```
pub struct InMemoryTransport<S: TopicSyntax> {
    inner: Arc<InMemoryInner>,
    _protocol: PhantomData<S>,
}

impl<S: TopicSyntax> InMemoryTransport<S> {
    /// Create a new in-memory transport.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        InMemoryTransport {
            inner: Arc::new(InMemoryInner { sender }),
            _protocol: PhantomData,
        }
    }
}

impl<S: TopicSyntax> Default for InMemoryTransport<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: TopicSyntax> Clone for InMemoryTransport<S> {
    fn clone(&self) -> Self {
        InMemoryTransport {
            inner: Arc::clone(&self.inner),
            _protocol: PhantomData,
        }
    }
}

/// A message stream for a single subscription on [`InMemoryTransport`].
///
/// Wraps a `broadcast::Receiver` and filters messages by pattern
/// using the protocol's wildcard semantics.
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

impl<S: TopicSyntax + Send + Sync> Transport for InMemoryTransport<S> {
    type Syntax = S;
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
            sep: S::SEPARATOR,
            single: S::SINGLE_WILDCARD,
            multi: S::MULTI_WILDCARD,
        })
    }

    async fn unsubscribe(&self, _pattern: String) -> Result<(), TopikError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_exact_match() {
        assert!(matches_pattern(
            "sensors/42/temperature",
            "sensors/42/temperature",
            '/',
            "+",
            "#"
        ));
    }

    #[test]
    fn mqtt_single_wildcard() {
        assert!(matches_pattern(
            "sensors/42/temperature",
            "sensors/+/temperature",
            '/',
            "+",
            "#"
        ));
    }

    #[test]
    fn mqtt_single_wildcard_no_match() {
        assert!(!matches_pattern(
            "sensors/42/temperature",
            "sensors/+/humidity",
            '/',
            "+",
            "#"
        ));
    }

    #[test]
    fn mqtt_multi_wildcard() {
        assert!(matches_pattern(
            "sensors/42/temperature",
            "sensors/#",
            '/',
            "+",
            "#"
        ));
    }

    #[test]
    fn mqtt_multi_wildcard_deep() {
        assert!(matches_pattern(
            "sensors/42/temperature/raw",
            "sensors/#",
            '/',
            "+",
            "#"
        ));
    }

    #[test]
    fn mqtt_multi_wildcard_no_match_empty() {
        assert!(!matches_pattern("sensors", "sensors/#", '/', "+", "#"));
    }

    #[test]
    fn mqtt_multiple_single_wildcards() {
        assert!(matches_pattern(
            "sensors/42/temperature",
            "sensors/+/+",
            '/',
            "+",
            "#"
        ));
    }
}
