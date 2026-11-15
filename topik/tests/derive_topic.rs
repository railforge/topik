use bytes::Bytes;

use topik::Topic;
use topik::protocol::{Mqtt, Redis};
use topik_core::__private::TopicWire;
use topik_core::Nats;

#[derive(Topic)]
#[topic(segments("sensor", device_id))]
struct TemperatureSensor {
    device_id: u64,
    #[payload]
    data: Bytes,
}

#[test]
fn render_nats_separator() {
    let topic = TemperatureSensor {
        device_id: 42,
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Nats>(), "sensor.42");
}

#[test]
fn render_mqtt_separator() {
    let topic = TemperatureSensor {
        device_id: 42,
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "sensor/42");
}

#[test]
fn render_redis_separator() {
    let topic = TemperatureSensor {
        device_id: 42,
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Redis>(), "sensor:42");
}

#[test]
fn parse_round_trip() {
    let key = TemperatureSensor::parse::<Mqtt>("sensor/42").unwrap();
    assert_eq!(key.device_id, 42);
}

#[test]
fn parse_literal_mismatch() {
    let err = TemperatureSensor::parse::<Mqtt>("wrong/42").unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::LiteralMismatch { position: 0, .. }
    ));
}

#[test]
fn parse_missing_segment() {
    let err = TemperatureSensor::parse::<Mqtt>("sensor").unwrap_err();
    assert!(matches!(
        err,
        topik_core::TopikError::MissingSegment { position: 1, .. }
    ));
}

#[test]
fn parse_invalid_segment() {
    let err = TemperatureSensor::parse::<Mqtt>("sensor/notanumber").unwrap_err();
    assert!(matches!(err, topik_core::TopikError::ParseError { .. }));
}

#[test]
fn wildcard_pattern_nats() {
    assert_eq!(TemperatureSensor::wildcard_pattern::<Nats>(), "sensor.*");
}

#[test]
fn wildcard_pattern_mqtt() {
    assert_eq!(TemperatureSensor::wildcard_pattern::<Mqtt>(), "sensor/+");
}

#[test]
fn name_constant() {
    assert_eq!(TemperatureSensor::NAME, "TemperatureSensor");
}

#[test]
fn from_key_and_payload() {
    let key = TemperatureSensor::parse::<Mqtt>("sensor/42").unwrap();
    let payload = Bytes::from("hello");
    let msg = TemperatureSensor::from_key_and_payload(key, payload.clone());
    assert_eq!(msg.device_id, 42);
    assert_eq!(msg.data, payload);
}
