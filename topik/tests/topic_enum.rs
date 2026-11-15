#![allow(dead_code)]

use bytes::Bytes;
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;
use topik::{Topic, TopicEnum, TopikClient};

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"))]
struct TemperatureReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "humidity"))]
struct HumidityReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "reboot"))]
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

#[test]
fn patterns_mqtt() {
    let patterns = SensorTopics::patterns::<Mqtt>();
    assert_eq!(patterns.len(), 3);
    assert_eq!(patterns[0], "sensors/+/temperature");
    assert_eq!(patterns[1], "sensors/+/humidity");
    assert_eq!(patterns[2], "devices/+/reboot");
}

#[test]
fn try_from_raw_temperature() {
    let payload = 23.5f32.to_string();
    let result = SensorTopics::try_from_raw::<Mqtt>("sensors/42/temperature", payload.as_bytes());
    assert!(matches!(result, Ok(SensorTopics::Temperature(msg)) if msg.device_id == 42));
}

#[test]
fn try_from_raw_humidity() {
    let payload = 65.0f32.to_string();
    let result = SensorTopics::try_from_raw::<Mqtt>("sensors/42/humidity", payload.as_bytes());
    assert!(matches!(result, Ok(SensorTopics::Humidity(msg)) if msg.device_id == 42));
}

#[test]
fn try_from_raw_reboot() {
    let result = SensorTopics::try_from_raw::<Mqtt>("devices/99/reboot", b"");
    assert!(matches!(result, Ok(SensorTopics::Reboot(msg)) if msg.device_id == 99));
}

#[test]
fn try_from_raw_no_match() {
    let result = SensorTopics::try_from_raw::<Mqtt>("unknown/42/topic", b"");
    assert!(matches!(
        result,
        Err(topik_core::TopikError::ParseError { .. })
    ));
}

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
