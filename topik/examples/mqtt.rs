//! MQTT example: typed topics with a real MQTT broker.
//!
//! Requires a running MQTT broker. By default connects to localhost:1883.
//!
//! Start a broker with Docker:
//!   docker run -it -p 1883:1883 eclipse-mosquitto
//!
//! Run with:
//!   cargo run --example mqtt --features mqtt

#[cfg(feature = "mqtt")]
mod example {
    use rumqttc::{Event, Packet};
    use topik::encoding::F32Encoding;
    use topik::{MqttClient, Topic, TopicEnum};

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

    #[derive(TopicEnum, Debug)]
    pub enum SensorTopics {
        Temperature(TemperatureReading),
        Humidity(HumidityReading),
    }

    pub async fn run() {
        // subscriber client
        let (sub_client, mut eventloop) = MqttClient::builder()
            .url("localhost", 1883)
            .client_id("topik-example-sub")
            .build();

        // publisher client — separate client_id required by MQTT
        let (pub_client, mut pub_eventloop) = MqttClient::builder()
            .url("localhost", 1883)
            .client_id("topik-example-pub")
            .build();

        // poll publisher event loop in background
        tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

        // subscribe to all topics in the enum
        sub_client.subscribe_many::<SensorTopics>().await.unwrap();
        println!("Subscribed to: {:?}", SensorTopics::patterns('/', "+", "#"));

        // publish some messages
        pub_client
            .publish(TemperatureReading {
                device_id: 42,
                data: 23.5,
            })
            .await
            .unwrap();
        println!("Published: sensors/42/temperature → 23.5°C");

        pub_client
            .publish(HumidityReading {
                device_id: 42,
                data: 65.0,
            })
            .await
            .unwrap();
        println!("Published: sensors/42/humidity → 65.0%");

        // receive and parse — user owns the event loop
        let mut received = 0;
        println!("\nReceived messages:");
        while received < 2 {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::Publish(p)) => {
                    match sub_client.parse::<SensorTopics>(&p.topic, &p.payload) {
                        Ok(SensorTopics::Temperature(msg)) => {
                            println!(
                                "  Temperature → device {} sent {:.1}°C",
                                msg.device_id, msg.data
                            );
                            received += 1;
                        }
                        Ok(SensorTopics::Humidity(msg)) => {
                            println!(
                                "  Humidity    → device {} sent {:.1}%",
                                msg.device_id, msg.data
                            );
                            received += 1;
                        }
                        Err(_) => {}
                    }
                }
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "mqtt")]
    example::run().await;

    #[cfg(not(feature = "mqtt"))]
    println!("Run with --features mqtt to enable this example.");
}
