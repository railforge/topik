//! TopicEnum example: separate subscribers for different topic contexts.
//!
//! Shows how to use multiple TopicEnums to group topics by context,
//! each with its own independent subscriber. This is the recommended
//! pattern when different parts of your application care about different
//! topic namespaces.
//!
//! Run with:
//!   cargo run --example topic_enum

use bytes::Bytes;
use topik::prelude::*;

// --- Sensor telemetry topics ---

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"))]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "humidity"))]
pub struct HumidityReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

/// Groups all sensor telemetry topics.
/// Subscriber receives messages on `sensors/+/temperature` and `sensors/+/humidity`.
#[derive(TopicEnum, Debug)]
pub enum SensorTopics {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
}

// --- Device control topics ---

#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "reboot"))]
pub struct RebootCommand {
    pub device_id: u64,
    #[payload]
    pub data: Bytes,
}

#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "config"))]
pub struct ConfigUpdate {
    pub device_id: u64,
    #[payload]
    pub data: String,
}

/// Groups all device control topics.
/// Subscriber receives messages on `devices/+/reboot` and `devices/+/config`.
#[derive(TopicEnum, Debug)]
pub enum DeviceCommands {
    Reboot(RebootCommand),
    Config(ConfigUpdate),
}

#[tokio::main]
async fn main() {
    let transport = InMemoryTransport::<Mqtt>::new();

    let publisher = TopikClient::new(transport.clone());
    let sensor_client = TopikClient::new(transport.clone());
    let command_client = TopikClient::new(transport);

    // Each context gets its own subscriber, scoped to its topic namespace.
    let mut sensor_sub = sensor_client
        .subscribe_many::<SensorTopics>()
        .await
        .unwrap();
    let mut command_sub = command_client
        .subscribe_many::<DeviceCommands>()
        .await
        .unwrap();

    println!("Sensor subscriber patterns:");
    for pattern in publisher.patterns::<SensorTopics>() {
        println!("  {}", pattern);
    }
    println!("Command subscriber patterns:");
    for pattern in publisher.patterns::<DeviceCommands>() {
        println!("  {}", pattern);
    }
    println!();

    // Publish sensor telemetry
    publisher
        .publish(TemperatureReading {
            device_id: 42,
            data: 23.5,
        })
        .await
        .unwrap();
    publisher
        .publish(HumidityReading {
            device_id: 42,
            data: 65.0,
        })
        .await
        .unwrap();

    // Publish device commands
    publisher
        .publish(RebootCommand {
            device_id: 99,
            data: Bytes::from("graceful"),
        })
        .await
        .unwrap();
    publisher
        .publish(ConfigUpdate {
            device_id: 7,
            data: "polling_interval=30".to_string(),
        })
        .await
        .unwrap();

    // Sensor subscriber only sees its namespace
    println!("Sensor events:");
    for _ in 0..2 {
        match sensor_sub.next().await.unwrap() {
            SensorTopics::Temperature(msg) => println!(
                "  Temperature -> device {} sent {:.1}°C",
                msg.device_id, msg.data
            ),
            SensorTopics::Humidity(msg) => println!(
                "  Humidity    -> device {} sent {:.1}%",
                msg.device_id, msg.data
            ),
        }
    }

    // Command subscriber only sees its namespace
    println!("\nCommand events:");
    for _ in 0..2 {
        match command_sub.next().await.unwrap() {
            DeviceCommands::Reboot(msg) => println!(
                "  Reboot      -> device {} ({})",
                msg.device_id,
                String::from_utf8_lossy(&msg.data)
            ),
            DeviceCommands::Config(msg) => {
                println!("  Config      -> device {} : {}", msg.device_id, msg.data)
            }
        }
    }
}
