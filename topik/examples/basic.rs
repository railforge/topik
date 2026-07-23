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

    // wildcard subscription: receives from ALL devices
    let mut all_sub = client.subscribe::<TemperatureReading>().await.unwrap();
    println!("Subscribed to all devices: {}", all_sub.pattern());

    // pinned subscription: receives ONLY from device 42
    let mut device_sub = client
        .subscribe::<TemperatureReading>()
        .pin(|builder| builder.device_id(42))
        .await
        .unwrap();
    println!("Subscribed to device 42 only: {}", device_sub.pattern());

    // Publish a message from device 42.
    let reading42 = TemperatureReading {
        device_id: 42,
        data: 23.5,
    };
    println!("Publishing to: {}", client.display(&reading42));
    client.publish(reading42).await.unwrap();

    // Publish another message from a different device.
    let reading99 = TemperatureReading {
        device_id: 99,
        data: 18.1,
    };
    println!("Publishing to: {}", client.display(&reading99));
    client.publish(reading99).await.unwrap();

    // wildcard subscriber receives both
    println!("\n--- Wildcard subscriber ---");
    let msg1 = all_sub.next().await.unwrap();
    println!("Received from {}: {}", client.display(&msg1), &msg1.data,);
    let msg2 = all_sub.next().await.unwrap();
    println!("Received from {}: {}", client.display(&msg2), &msg2.data,);

    // pinned subscriber only receives device 42
    println!("\n--- Pinned subscriber (device 42 only) ---");
    let msg = device_sub.next().await.unwrap();
    println!("Received from {}: {}", client.display(&msg), &msg.data,);

    let pattern = all_sub.pattern().to_string();
    all_sub.unsubscribe().await.unwrap();
    println!("\nUnsubscribed from: {}", pattern);

    let pattern = device_sub.pattern().to_string();
    device_sub.unsubscribe().await.unwrap();
    println!("Unsubscribed from: {}", pattern);
}
