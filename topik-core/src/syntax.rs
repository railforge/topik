/// Defines the characters used to define a pub/sub topic.
///
/// In particular, the character used to provide the hierarchical
/// split of parts of the topic (`SEPERATOR`), a single wildcard part
/// (`SINGLE_WILDCARD`), and the character for the "one or more" wildcard
/// (`MULTI_WILDCARD`).
pub trait TopicSyntax: Send + Sync + 'static {
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
