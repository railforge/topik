#![allow(dead_code)]

use bytes::Bytes;
use topik::Topic;
use topik::encoding::RawEncoding;
use topik_core::__private::TopicWire;

// multiple literals, multiple dynamic segments, literal in the middle
#[derive(Topic)]
#[topic(segments("factory", "v2", device_id, "readings", kind), encoding = RawEncoding)]
struct FactoryReading {
    device_id: u64,
    kind: String,
    #[payload]
    data: Bytes,
}

// all literals, one dynamic segment at the end
#[derive(Topic)]
#[topic(segments("eu", "west", "factory", "sensors", device_id), encoding = RawEncoding)]
struct DeepNestedSensor {
    device_id: u64,
    #[payload]
    data: Bytes,
}

// dynamic segment first, literals after
#[derive(Topic)]
#[topic(segments(device_id, "metrics", "cpu"), encoding = RawEncoding)]
struct CpuMetric {
    device_id: u64,
    #[payload]
    data: Bytes,
}

// --- FactoryReading tests ---

#[test]
fn render_literal_in_middle_mqtt() {
    let topic = FactoryReading {
        device_id: 99,
        kind: "temperature".to_string(),
        data: Bytes::new(),
    };
    assert_eq!(topic.render('/'), "factory/v2/99/readings/temperature");
}

#[test]
fn render_literal_in_middle_nats() {
    let topic = FactoryReading {
        device_id: 99,
        kind: "humidity".to_string(),
        data: Bytes::new(),
    };
    assert_eq!(topic.render('.'), "factory.v2.99.readings.humidity");
}

#[test]
fn parse_literal_in_middle() {
    let key = FactoryReading::parse("factory/v2/99/readings/temperature", '/').unwrap();
    assert_eq!(key.device_id, 99);
    assert_eq!(key.kind, "temperature");
}

#[test]
fn parse_literal_mismatch_first() {
    let err = FactoryReading::parse("wrong/v2/99/readings/temperature", '/').unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::LiteralMismatch { position: 0, .. }
    ));
}

#[test]
fn parse_literal_mismatch_middle() {
    let err = FactoryReading::parse("factory/v2/99/wrong/temperature", '/').unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::LiteralMismatch { position: 3, .. }
    ));
}

#[test]
fn parse_literal_mismatch_second() {
    let err = FactoryReading::parse("factory/v1/99/readings/temperature", '/').unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::LiteralMismatch { position: 1, .. }
    ));
}

#[test]
fn parse_missing_dynamic_segment() {
    let err = FactoryReading::parse("factory/v2/99/readings", '/').unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::MissingSegment { position: 4, .. }
    ));
}

#[test]
fn wildcard_pattern_skips_literals() {
    assert_eq!(
        FactoryReading::wildcard_pattern('/', "+", "#"),
        "factory/v2/+/readings/+"
    );
}

#[test]
fn round_trip_multiple_segments() {
    let topic = FactoryReading {
        device_id: 7,
        kind: "pressure".to_string(),
        data: Bytes::new(),
    };
    let rendered = topic.render('/');
    let key = FactoryReading::parse(&rendered, '/').unwrap();
    assert_eq!(key.device_id, 7);
    assert_eq!(key.kind, "pressure");
}

// --- DeepNestedSensor tests ---

#[test]
fn render_deep_nesting() {
    let topic = DeepNestedSensor {
        device_id: 42,
        data: Bytes::new(),
    };
    assert_eq!(topic.render('/'), "eu/west/factory/sensors/42");
}

#[test]
fn parse_deep_nesting() {
    let key = DeepNestedSensor::parse("eu/west/factory/sensors/42", '/').unwrap();
    assert_eq!(key.device_id, 42);
}

#[test]
fn wildcard_deep_nesting() {
    assert_eq!(
        DeepNestedSensor::wildcard_pattern('/', "+", "#"),
        "eu/west/factory/sensors/+"
    );
}

// --- CpuMetric tests ---

#[test]
fn render_dynamic_segment_first() {
    let topic = CpuMetric {
        device_id: 10,
        data: Bytes::new(),
    };
    assert_eq!(topic.render('/'), "10/metrics/cpu");
}

#[test]
fn parse_dynamic_segment_first() {
    let key = CpuMetric::parse("10/metrics/cpu", '/').unwrap();
    assert_eq!(key.device_id, 10);
}

#[test]
fn parse_literal_mismatch_after_dynamic() {
    let err = CpuMetric::parse("10/wrong/cpu", '/').unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::LiteralMismatch { position: 1, .. }
    ));
}
