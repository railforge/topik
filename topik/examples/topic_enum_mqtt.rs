//! TopicEnum with MQTT: typed multi-topic dispatch against a real broker.
//!
//! Shows how the same `subscribe_many` / match pattern works with a real MQTT broker.
//!
//! Requires a running MQTT broker on localhost:1883.
//!
//! Start one with Docker:
//!   docker run -it -p 1883:1883 eclipse-mosquitto
//!
//! Run with:
//!   cargo run --example topic_enum_mqtt --features rumqttc

#[cfg(feature = "rumqttc")]
mod example {
    use topik::prelude::*;

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

    #[derive(TopicEnum, Debug)]
    pub enum SensorTopics {
        Temperature(TemperatureReading),
        Humidity(HumidityReading),
    }

    pub async fn run() {
        // One transport per logical role
        let sub = TopikClient::new(
            MqttTransport::builder()
                .url("localhost", 1883)
                .client_id("topik-enum-sub")
                .build()
                .await
                .unwrap(),
        );

        let pub_ = TopikClient::new(
            MqttTransport::builder()
                .url("localhost", 1883)
                .client_id("topik-enum-pub")
                .build()
                .await
                .unwrap(),
        );

        // Subscribe to all patterns the enum covers
        let mut stream = sub.subscribe_many::<SensorTopics>().await.unwrap();

        // Publish one of each type.
        pub_.publish(TemperatureReading {
            device_id: 42,
            data: 23.5,
        })
        .await
        .unwrap();
        pub_.publish(HumidityReading {
            device_id: 42,
            data: 65.0,
        })
        .await
        .unwrap();

        // Receive and dispatch by type
        for _ in 0..2 {
            match stream.next().await.unwrap() {
                SensorTopics::Temperature(msg) => println!(
                    "Temperature -> device {} sent {:.1}°C",
                    msg.device_id, msg.data
                ),
                SensorTopics::Humidity(msg) => println!(
                    "Humidity    -> device {} sent {:.1}%",
                    msg.device_id, msg.data
                ),
            }
        }
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "rumqttc")]
    example::run().await;

    #[cfg(not(feature = "rumqttc"))]
    println!("Run with --features rumqttc to enable this example.");
}
