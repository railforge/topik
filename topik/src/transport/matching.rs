/// Match a concrete topic string against a subscription pattern.
///
/// Handles wildcard matching for all supported protocols:
///
/// | Protocol | Separator | Single | Multi |
/// |----------|-----------|--------|-------|
/// | MQTT     | `/`       | `+`    | `#`   |
/// | NATS     | `.`       | `*`    | `>`   |
/// | Redis    | `:`       | `*`    | `*`   |
///
/// # Examples
///
/// ```
/// // MQTT
/// assert!(matches_pattern("sensors/42/temperature", "sensors/+/temperature", '/', "+", "#"));
/// assert!(matches_pattern("sensors/42/temperature", "sensors/#", '/', "+", "#"));
///
/// // NATS
/// assert!(matches_pattern("sensors.42.temperature", "sensors.*.temperature", '.', "*", ">"));
/// assert!(matches_pattern("sensors.42.temperature", "sensors.>", '.', "*", ">"));
///
/// // Redis
/// assert!(matches_pattern("sensors:42:temperature", "sensors:*:temperature", ':', "*", "*"));
/// ```
pub fn matches_pattern(topic: &str, pattern: &str, sep: char, single: &str, multi: &str) -> bool {
    // fast path — exact match
    if topic == pattern {
        return true;
    }

    let topic_segments: Vec<&str> = topic.split(sep).collect();
    let pattern_segments: Vec<&str> = pattern.split(sep).collect();

    match_segments(&topic_segments, &pattern_segments, single, multi)
}

fn match_segments(topic: &[&str], pattern: &[&str], single: &str, multi: &str) -> bool {
    match (topic, pattern) {
        // both exhausted — full match
        ([], []) => true,

        // pattern has multi wildcard as last segment — matches everything remaining
        (_, [p]) if *p == multi => !topic.is_empty(),

        // both have segments — check head and recurse
        ([t, topic_rest @ ..], [p, pattern_rest @ ..]) => {
            let head_matches = *p == single || *p == *t;
            head_matches && match_segments(topic_rest, pattern_rest, single, multi)
        }

        // lengths don't match and no multi wildcard — no match
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MQTT ---
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

    // --- NATS ---
    #[test]
    fn nats_single_wildcard() {
        assert!(matches_pattern(
            "sensors.42.temperature",
            "sensors.*.temperature",
            '.',
            "*",
            ">"
        ));
    }

    #[test]
    fn nats_multi_wildcard() {
        assert!(matches_pattern(
            "sensors.42.temperature",
            "sensors.>",
            '.',
            "*",
            ">"
        ));
    }

    #[test]
    fn nats_multi_wildcard_deep() {
        assert!(matches_pattern(
            "sensors.42.temperature.raw",
            "sensors.>",
            '.',
            "*",
            ">"
        ));
    }

    #[test]
    fn nats_multiple_wildcards() {
        assert!(matches_pattern(
            "sensors.42.temperature",
            "sensors.*.*",
            '.',
            "*",
            ">"
        ));
    }

    #[test]
    fn nats_no_match() {
        assert!(!matches_pattern(
            "sensors.42.temperature",
            "commands.*.*",
            '.',
            "*",
            ">"
        ));
    }

    // --- Redis ---
    #[test]
    fn redis_single_wildcard() {
        assert!(matches_pattern(
            "sensors:42:temperature",
            "sensors:*:temperature",
            ':',
            "*",
            "*"
        ));
    }

    #[test]
    fn redis_wildcard_no_match() {
        assert!(!matches_pattern(
            "sensors:42:temperature",
            "sensors:*:humidity",
            ':',
            "*",
            "*"
        ));
    }

    #[test]
    fn redis_exact() {
        assert!(matches_pattern(
            "sensors:42:temperature",
            "sensors:42:temperature",
            ':',
            "*",
            "*"
        ));
    }
}
