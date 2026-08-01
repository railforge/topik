//! MQTT example: typed topics with a real MQTT broker.
//!
//! Requires a running MQTT broker on localhost:1883.
//!
//! Start one with Docker:
//!   docker run -it -p 1883:1883 eclipse-mosquitto
//!
//! Run with:
//!   cargo run --example mqtt --features mqtt

#[cfg(feature = "mqtt")]
mod example {
    use rumqttc::{Event, Packet, QoS};
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
        // Two clients (MQTT requires unique client_id per connection)
        // Full builder options shown here (uncomment as needed)
        let (sub_client, mut eventloop) = MqttClient::builder()
            .url("localhost", 1883)
            .client_id("topik-example-sub")
            .keep_alive(30)
            .clean_session(true)
            // .credentials("user", "password")
            // .last_will(rumqttc::LastWill::new(
            //     "devices/topik-example-sub/status",
            //     "offline",
            //     rumqttc::QoS::AtLeastOnce,
            //     true,
            // ))
            // .with_options(|mut opts| {
            //     // TLS, websockets, proxy etc
            //     opts
            // })
            .build();

        let (pub_client, mut pub_eventloop) = MqttClient::builder()
            .url("localhost", 1883)
            .client_id("topik-example-pub")
            .build();

        // poll publisher event loop in background
        tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

        // Subscribing
        sub_client.subscribe_many::<SensorTopics>().await.unwrap();
        println!("Subscribed to:");
        for pattern in SensorTopics::patterns('/', "+", "#") {
            println!("  {}", pattern);
        }

        // Publishing with display
        let reading1 = TemperatureReading {
            device_id: 42,
            data: 23.5,
        };
        println!("\nPublishing to: {}", pub_client.display(&reading1));
        pub_client.publish(reading1).await.unwrap();

        // publish with explicit QoS and retain chained
        let reading2 = HumidityReading {
            device_id: 42,
            data: 65.0,
        };
        println!(
            "Publishing to: {} (QoS::AtLeastOnce, retain=false)",
            pub_client.display(&reading2)
        );
        pub_client
            .publish(reading2)
            .qos(QoS::AtLeastOnce)
            .retain(false)
            .await
            .unwrap();

        // Receiving with parse
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

        // parse_topic for a single topic type
        let reading3 = TemperatureReading {
            device_id: 99,
            data: 18.0,
        };
        println!("\nPublishing to: {}", pub_client.display(&reading3));
        pub_client.publish(reading3).await.unwrap();

        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::Publish(p)) => {
                    if let Some(msg) = sub_client
                        .parse_topic::<TemperatureReading>(&p.topic, &p.payload)
                        .unwrap()
                    {
                        if msg.device_id == 99 {
                            println!(
                                "parse_topic → device {} sent {:.1}°C",
                                msg.device_id, msg.data
                            );
                            break;
                        }
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
