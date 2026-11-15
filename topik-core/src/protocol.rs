/// Defines the wire format conventions for a pub/sub protocol.
///
/// Each protocol has its own topic separator and wildcard tokens.
///
/// # Example
///
/// ```ignore
/// use topik::TopikClient;
/// use topik::protocol::Mqtt;
///
/// // Mqtt marker tells TopikClient to use '/' as separator
/// // and '+' / '#' as wildcards
/// let client = TopikClient::connect(
///     Mqtt::builder()
///         .url("mqtt://localhost:1883")
///         .client_id("my-service")
///         .build()
/// ).await?;
/// ```
pub trait Protocol: Send + Sync + 'static {
    /// Character that separates segements of a topic.
    /// ```text
    /// MQTT: '/'   sensors/42/temperature
    /// NATS: '.'   sensors.42.temperature
    /// Redis: ':'   sensors:42:temperature
    /// ```
    const SEPARATOR: char;

    /// Exactly one wildcard character.
    ///
    /// ```text
    /// MQTT  -> "+"   sensors/+/temperature
    /// NATS  -> "*"   sensors.*.temperature
    /// Redis -> "*"   sensors:*:temperature
    /// ```
    const SINGLE_WILDCARD: &'static str;

    /// One or more wildcard character.
    ///
    /// ```text
    /// MQTT  -> "#"   sensors/#
    /// NATS  -> ">"   sensors.>
    /// Redis -> "*"   sensors:*  (glob only, no true multi-level)
    /// ```
    const MULTI_WILDCARD: &'static str;
}

/// MQTT protocol marker.
///
/// Compatible with MQTT 3.1.1 and MQTT 5.0.
pub struct Mqtt;

impl Protocol for Mqtt {
    const SEPARATOR: char = '/';
    const SINGLE_WILDCARD: &'static str = "+";
    const MULTI_WILDCARD: &'static str = "#";
}

/// NATS protocol marker.
///
/// multi-token wildcards.
pub struct Nats;

impl Protocol for Nats {
    const SEPARATOR: char = '.';
    const SINGLE_WILDCARD: &'static str = "*";
    const MULTI_WILDCARD: &'static str = ">";
}

/// Redis pub/sub protocol marker.
//
/// There is no true multi-level wildcard; `*` serves both roles.
pub struct Redis;

impl Protocol for Redis {
    const SEPARATOR: char = ':';
    const SINGLE_WILDCARD: &'static str = "*";
    const MULTI_WILDCARD: &'static str = "*";
}
