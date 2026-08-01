#![allow(dead_code)]

use bytes::Bytes;
use topik::encoding::{F32Encoding, RawEncoding};
use topik::protocol::{Mqtt, Nats};
use topik::transport::InMemoryTransport;
use topik::{Topic, TopicEnum, TopikClient};
use topik_core::TopicEnum as TopicEnumTrait;

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

// --- try_from_raw() ---

#[test]
fn try_from_raw_temperature() {
    let payload = 23.5f32.to_string();
    let result = SensorTopics::try_from_raw("sensors/42/temperature", payload.as_bytes(), '/');
    assert!(matches!(result, Ok(SensorTopics::Temperature(msg)) if msg.device_id == 42));
}

#[test]
fn try_from_raw_humidity() {
    let payload = 65.0f32.to_string();
    let result = SensorTopics::try_from_raw("sensors/42/humidity", payload.as_bytes(), '/');
    assert!(matches!(result, Ok(SensorTopics::Humidity(msg)) if msg.device_id == 42));
}

#[test]
fn try_from_raw_reboot() {
    let result = SensorTopics::try_from_raw("devices/99/reboot", b"", '/');
    assert!(matches!(result, Ok(SensorTopics::Reboot(msg)) if msg.device_id == 99));
}

#[test]
fn try_from_raw_no_match() {
    let result = SensorTopics::try_from_raw("unknown/42/topic", b"", '/');
    assert!(matches!(
        result,
        Err(topik_core::TopikError::ParseError { .. })
    ));
}

#[test]
fn try_from_raw_nats_separator() {
    let payload = 23.5f32.to_string();
    let result = SensorTopics::try_from_raw("sensors.42.temperature", payload.as_bytes(), '.');
    assert!(matches!(result, Ok(SensorTopics::Temperature(msg)) if msg.device_id == 42));
}

// --- subscribe_many ---

#[tokio::test]
async fn subscribe_many_receives_all_variants() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe_many::<SensorTopics>().await.unwrap();

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
    client
        .publish(RebootCommand {
            device_id: 3,
            data: Bytes::from("restart"),
        })
        .await
        .unwrap();

    let msg1 = sub.next().await.unwrap();
    let msg2 = sub.next().await.unwrap();
    let msg3 = sub.next().await.unwrap();

    assert!(matches!(msg1, SensorTopics::Temperature(msg) if msg.device_id == 1));
    assert!(matches!(msg2, SensorTopics::Humidity(msg) if msg.device_id == 2));
    assert!(matches!(msg3, SensorTopics::Reboot(msg) if msg.device_id == 3));
}

#[tokio::test]
async fn subscribe_many_mqtt_and_nats() {
    // same enum works with any protocol
    let mqtt_client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
    let nats_client = TopikClient::new(InMemoryTransport::<Nats>::new());

    let mut mqtt_sub = mqtt_client.subscribe_many::<SensorTopics>().await.unwrap();
    let mut nats_sub = nats_client.subscribe_many::<SensorTopics>().await.unwrap();

    mqtt_client
        .publish(TemperatureReading {
            device_id: 1,
            data: 20.0,
        })
        .await
        .unwrap();
    nats_client
        .publish(TemperatureReading {
            device_id: 2,
            data: 21.0,
        })
        .await
        .unwrap();

    let mqtt_msg = mqtt_sub.next().await.unwrap();
    let nats_msg = nats_sub.next().await.unwrap();

    assert!(matches!(mqtt_msg, SensorTopics::Temperature(msg) if msg.device_id == 1));
    assert!(matches!(nats_msg, SensorTopics::Temperature(msg) if msg.device_id == 2));
}

#[tokio::test]
async fn subscribe_many_pinned() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    // subscribe to all via enum
    let mut sub = client.subscribe_many::<SensorTopics>().await.unwrap();

    // publish from two different devices
    client
        .publish(TemperatureReading {
            device_id: 42,
            data: 23.5,
        })
        .await
        .unwrap();
    client
        .publish(TemperatureReading {
            device_id: 99,
            data: 18.0,
        })
        .await
        .unwrap();

    // both received since subscribe_many wildcards all segments
    let msg1 = sub.next().await.unwrap();
    let msg2 = sub.next().await.unwrap();

    assert!(matches!(msg1, SensorTopics::Temperature(msg) if msg.device_id == 42));
    assert!(matches!(msg2, SensorTopics::Temperature(msg) if msg.device_id == 99));
}
