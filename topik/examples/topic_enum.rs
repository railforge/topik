//! TopicEnum example: subscribe to multiple topic types in one call.
//!
//! Shows how to use TopicEnum with subscribe_many to handle multiple
//! topic types through a single subscriber — no manual pattern
//! construction, no separators, no tokio::select! boilerplate.
//!
//! Run with:
//!   cargo run --example topic_enum

use bytes::Bytes;
use topik::encoding::{F32Encoding, RawEncoding};
use topik::protocol::Mqtt;
use topik::transport::InMemoryTransport;
use topik::{Topic, TopicEnum, TopikClient};

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "humidity"), encoding = F32Encoding)]
pub struct HumidityReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "reboot"), encoding = RawEncoding)]
pub struct RebootCommand {
    pub device_id: u64,
    #[payload]
    pub data: Bytes,
}

#[derive(TopicEnum, Debug)]
pub enum SensorTopics {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
    Reboot(RebootCommand),
}

#[tokio::main]
async fn main() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    // subscribe to all topics in the enum — one call, no separators
    let mut sub = client.subscribe_many::<SensorTopics>().await.unwrap();

    // publish one of each
    client
        .publish(TemperatureReading {
            device_id: 42,
            data: 23.5,
        })
        .await
        .unwrap();
    client
        .publish(HumidityReading {
            device_id: 42,
            data: 65.0,
        })
        .await
        .unwrap();
    client
        .publish(RebootCommand {
            device_id: 99,
            data: Bytes::from("graceful"),
        })
        .await
        .unwrap();

    // receive and match — exhaustive, compiler catches missing variants
    for _ in 0..3 {
        match sub.next().await.unwrap() {
            SensorTopics::Temperature(msg) => {
                println!(
                    "Temperature -> device {} sent {:.1}°C",
                    msg.device_id, msg.data
                )
            }
            SensorTopics::Humidity(msg) => {
                println!(
                    "Humidity    -> device {} sent {:.1}%",
                    msg.device_id, msg.data
                )
            }
            SensorTopics::Reboot(msg) => {
                println!(
                    "Reboot      -> device {} requested {}",
                    msg.device_id,
                    String::from_utf8_lossy(&msg.data)
                )
            }
        }
    }
}
