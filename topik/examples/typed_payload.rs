//! Typed payload example (numeric and float payloads).
//!
//! Run with:
//!   cargo run --example typed_payload

use topik::Topic;
use topik::TopikClient;
use topik::encoding::F32Encoding;
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

#[tokio::main]
async fn main() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();
    println!("Subscribed to: {}", sub.pattern());

    let reading1 = TemperatureReading {
        device_id: 42,
        data: 23.5,
    };
    println!("Publishing to: {}", client.display(&reading1));
    client.publish(reading1).await.unwrap();

    let reading2 = TemperatureReading {
        device_id: 99,
        data: 18.1,
    };
    println!("Publishing to: {}", client.display(&reading2));
    client.publish(reading2).await.unwrap();

    let msg1 = sub.next().await.unwrap();
    println!(
        "Received from {}: {:.1}°C",
        client.display(&msg1),
        msg1.data
    );

    let msg2 = sub.next().await.unwrap();
    println!(
        "Received from {}: {:.1}°C",
        client.display(&msg2),
        msg2.data
    );

    let pattern = sub.pattern().to_string();
    sub.unsubscribe().await.unwrap();
    println!("Unsubscribed from: {}", pattern);
}
