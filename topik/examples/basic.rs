//! Basic example: define a typed topic, publish and subscribe.
//!
//! This example uses InMemoryTransport so no broker is needed.
//! Swap InMemoryTransport for MqttTransport or NatsTransport
//! to connect to a real broker without changing any other code.
//!
//! Run with:
//!   cargo run --example basic

use topik::Topic;
use topik::TopikClient;
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;
use topik_core::F32Encoding;

/// A temperature reading from a sensor device.
///
/// Topic structure:
///   MQTT  -> "sensors/{device_id}/temperature"
///   NATS  -> "sensors.{device_id}.temperature"
///   Redis -> "sensors:{device_id}:temperature"
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

#[tokio::main]
async fn main() {
    // Create a client with in-memory transport using MQTT protocol semantics.
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();
    println!("Subscribed to: {}", sub.pattern());

    // Publish a message from device 42.
    let reading1 = TemperatureReading {
        device_id: 42,
        data: 23.5,
    };
    println!("Publishing to: {}", client.display(&reading1));
    client.publish(reading1).await.unwrap();

    // Publish another message from a different device.
    let reading2 = TemperatureReading {
        device_id: 99,
        data: 18.1,
    };
    println!("Publishing to: {}", client.display(&reading2));
    client.publish(reading2).await.unwrap();

    // Receive both messages (wildcard subscription catches all device IDs).
    let msg1 = sub.next().await.unwrap();
    println!("Received from {}: {}", client.display(&msg1), &msg1.data,);

    let msg2 = sub.next().await.unwrap();
    println!("Received from {}: {}", client.display(&msg2), &msg2.data,);

    println!("Unsubscribed from: {}", sub.pattern());
    sub.unsubscribe().await.unwrap();
}
