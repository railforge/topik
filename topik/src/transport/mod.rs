mod inmemory;

#[cfg(feature = "rumqttc")]
mod mqtt;

pub use self::inmemory::InMemoryTransport;
pub use topik_core::transport::{MessageStream, RawMessage, Transport};

#[cfg(feature = "rumqttc")]
pub use self::mqtt::{MqttStream, MqttTransport, MqttTransportBuilder};

/// Direct access to the rumqttc driver layer.
///
/// Use [`MqttTransport`] for most cases. Import from here when you need
/// direct control over the event loop, per-publish QoS, or retained messages.
#[cfg(feature = "rumqttc")]
pub mod rumqttc {
    pub use super::mqtt::{MqttClient, MqttClientBuilder, MqttPublishBuilder};
}
