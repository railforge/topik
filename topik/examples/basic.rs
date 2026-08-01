//! Basic example: define a typed topic, publish and subscribe.
//!
//! Run with:
//!   cargo run --example basic

use bytes::Bytes;
use topik::encoding::RawEncoding;
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;
use topik::{Topic, TopikClient};

/// A temperature reading from a sensor device.
///
/// Renders as:
///   MQTT  -> "sensors/{device_id}/temperature"
///   NATS  -> "sensors.{device_id}.temperature"
///   Redis -> "sensors:{device_id}:temperature"
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = RawEncoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: Bytes,
}

#[tokio::main]
async fn main() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();
    println!("Subscribed to: {}", sub.pattern());

    let reading = TemperatureReading {
        device_id: 42,
        data: Bytes::from("23.5"),
    };
    println!("Publishing to: {}", client.display(&reading));
    client.publish(reading).await.unwrap();

    let msg = sub.next().await.unwrap();
    println!(
        "Received from {}: {}",
        client.display(&msg),
        String::from_utf8_lossy(&msg.data),
    );

    let pattern = sub.pattern().to_string();
    sub.unsubscribe().await.unwrap();
    println!("Unsubscribed from: {}", pattern);
}
