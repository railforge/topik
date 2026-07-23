/// Defines the wire format conventions for a pub/sub protocol.
///
/// Each protocol has its own topic separator and wildcard tokens.
/// Implementing this trait on a zero-sized marker type is how topik
/// knows how to render topic strings and subscription patterns for
/// a given broker.
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
    /// The character used to separate topic segments on the wire.
    ///
    /// ```text
    /// MQTT  -> '/'   sensors/42/temperature
    /// NATS  -> '.'   sensors.42.temperature
    /// Redis -> ':'   sensors:42:temperature
    /// ```
    const SEPARATOR: char;

    /// The wildcard token matching exactly one topic segment.
    ///
    /// ```text
    /// MQTT  -> "+"   sensors/+/temperature
    /// NATS  -> "*"   sensors.*.temperature
    /// Redis -> "*"   sensors:*:temperature
    /// ```
    const SINGLE_WILDCARD: &'static str;

    /// The wildcard token matching one or more trailing segments.
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
/// Uses `/` as separator, `+` for single-level wildcards, `#` for
/// multi-level wildcards.
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
/// Uses `.` as separator, `*` for single-token wildcards, `>` for
/// multi-token wildcards.
pub struct Nats;

impl Protocol for Nats {
    const SEPARATOR: char = '.';
    const SINGLE_WILDCARD: &'static str = "*";
    const MULTI_WILDCARD: &'static str = ">";
}

/// Redis pub/sub protocol marker.
///
/// Uses `:` as separator. Redis uses glob-style pattern matching —
/// `*` matches any sequence of characters within a segment.
/// There is no true multi-level wildcard; `*` serves both roles.
pub struct Redis;

impl Protocol for Redis {
    const SEPARATOR: char = ':';
    const SINGLE_WILDCARD: &'static str = "*";
    const MULTI_WILDCARD: &'static str = "*";
}
