//! MQTT example: typed topics with a real MQTT broker.
//!
//! Connects via the default [`MqttTransport`].
//!
//! Requires a running MQTT broker on localhost:1883.
//!
//! Start one with Docker:
//!   docker run -it -p 1883:1883 eclipse-mosquitto
//!
//! Run with:
//!   cargo run --example mqtt --features rumqttc

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
        // Each connection needs a unique client_id.
        // The event loop runs in a background task.
        let sub = TopikClient::new(
            MqttTransport::builder()
                .url("localhost", 1883)
                .client_id("topik-example-sub")
                .build()
                .await
                .unwrap(),
        );

        let pub_client = TopikClient::new(
            MqttTransport::builder()
                .url("localhost", 1883)
                .client_id("topik-example-pub")
                .build()
                .await
                .unwrap(),
        );

        // Subscribe to all topics covered by the enum.
        let mut stream = sub.subscribe_many::<SensorTopics>().await.unwrap();

        println!("Subscribed to:");
        for pattern in sub.patterns::<SensorTopics>() {
            println!("  {}", pattern);
        }

        // Publish typed messages.
        println!(
            "\nPublishing to: {}",
            pub_client.display(&TemperatureReading {
                device_id: 42,
                data: 23.5
            })
        );
        pub_client
            .publish(TemperatureReading {
                device_id: 42,
                data: 23.5,
            })
            .await
            .unwrap();

        println!(
            "Publishing to: {}",
            pub_client.display(&HumidityReading {
                device_id: 42,
                data: 65.0
            })
        );
        pub_client
            .publish(HumidityReading {
                device_id: 42,
                data: 65.0,
            })
            .await
            .unwrap();

        // Receive and dispatch
        println!("\nReceived messages:");
        for _ in 0..2 {
            match stream.next().await.unwrap() {
                SensorTopics::Temperature(msg) => {
                    println!(
                        "  Temperature -> device {} sent {:.1}°C",
                        msg.device_id, msg.data
                    );
                }
                SensorTopics::Humidity(msg) => {
                    println!(
                        "  Humidity    -> device {} sent {:.1}%",
                        msg.device_id, msg.data
                    );
                }
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
