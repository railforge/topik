/// Defines which protocol convention your topik follows.
///
/// Each protocol has its own topic separator and wildcard tokens.
/// ```
pub trait Protocol: Send + Sync + 'static {
    /// Character that separates segments of a topic.
    /// (e.g., MQTT: `'/'`)
    const SEPARATOR: char;

    /// Exactly one wildcard character.
    ///(e.g., MQTT: `'+'`)
    const SINGLE_WILDCARD: &'static str;

    /// One or more wildcard character.
    ///(e.g., MQTT: `'#'`)
    const MULTI_WILDCARD: &'static str;
}
