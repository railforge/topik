#![allow(dead_code)]

use bytes::Bytes;
use topik::encoding::{F32Encoding, RawEncoding};
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;
use topik::{Topic, TopicEnum, TopikClient};
use topik_core::Encoding;

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
struct TemperatureReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "humidity"), encoding = F32Encoding)]
struct HumidityReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "reboot"), encoding = RawEncoding)]
struct RebootCommand {
    device_id: u64,
    #[payload]
    data: Bytes,
}

#[derive(TopicEnum, Debug)]
enum SensorTopics {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
    Reboot(RebootCommand),
}

// --- patterns() ---

#[test]
fn patterns_mqtt() {
    let patterns = SensorTopics::patterns('/', "+", "#");
    assert_eq!(patterns.len(), 3);
    assert_eq!(patterns[0], "sensors/+/temperature");
    assert_eq!(patterns[1], "sensors/+/humidity");
    assert_eq!(patterns[2], "devices/+/reboot");
}

#[test]
fn patterns_nats() {
    let patterns = SensorTopics::patterns('.', "*", ">");
    assert_eq!(patterns[0], "sensors.*.temperature");
    assert_eq!(patterns[1], "sensors.*.humidity");
    assert_eq!(patterns[2], "devices.*.reboot");
}

// --- try_parse() ---

#[test]
fn try_parse_temperature() {
    let payload = 23.5f32.to_string();
    let result = SensorTopics::try_parse("sensors/42/temperature", payload.as_bytes(), '/');
    assert!(matches!(result, Ok(SensorTopics::Temperature(msg)) if msg.device_id == 42));
}

#[test]
fn try_parse_humidity() {
    let payload = 65.0f32.to_string();
    let result = SensorTopics::try_parse("sensors/42/humidity", payload.as_bytes(), '/');
    assert!(matches!(result, Ok(SensorTopics::Humidity(msg)) if msg.device_id == 42));
}

#[test]
fn try_parse_reboot() {
    let result = SensorTopics::try_parse("devices/99/reboot", b"", '/');
    assert!(matches!(result, Ok(SensorTopics::Reboot(msg)) if msg.device_id == 99));
}

#[test]
fn try_parse_no_match() {
    let result = SensorTopics::try_parse("unknown/42/topic", b"", '/');
    assert!(matches!(
        result,
        Err(topik_core::TopikError::ParseError { .. })
    ));
}

#[test]
fn try_parse_wrong_separator() {
    let payload = 23.5f32.to_string();
    let result = SensorTopics::try_parse("sensors.42.temperature", payload.as_bytes(), '.');
    assert!(matches!(result, Ok(SensorTopics::Temperature(msg)) if msg.device_id == 42));
}

// --- integration with TopikClient ---

#[tokio::test]
async fn topic_enum_with_client() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut temp_sub = client.subscribe::<TemperatureReading>().await.unwrap();
    let mut humidity_sub = client.subscribe::<HumidityReading>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 1,
            data: 23.5,
        })
        .await
        .unwrap();
    client
        .publish(HumidityReading {
            device_id: 2,
            data: 65.0,
        })
        .await
        .unwrap();

    let temp = temp_sub.next().await.unwrap();
    let humidity = humidity_sub.next().await.unwrap();

    assert_eq!(temp.device_id, 1);
    assert_eq!(humidity.device_id, 2);

    // manually parse into enum
    let temp_payload = F32Encoding::encode(&temp.data).unwrap();
    let topic_str = client.display(&temp);
    let parsed = SensorTopics::try_parse(&topic_str, &temp_payload, '/').unwrap();
    assert!(matches!(parsed, SensorTopics::Temperature(_)));
}
