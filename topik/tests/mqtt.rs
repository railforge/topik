#![cfg(feature = "mqtt")]

use bytes::Bytes;
use rumqttc::{Event, Packet};
use testcontainers_modules::{mosquitto::Mosquitto, testcontainers::runners::AsyncRunner};
use topik::encoding::F32Encoding;
use topik::{MqttClient, Topic, TopicEnum};

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
struct TemperatureReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("sensors", device_id, "humidity"), encoding = F32Encoding)]
struct HumidityReading {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(TopicEnum, Debug)]
enum SensorTopics {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
}

async fn start_broker() -> (
    testcontainers_modules::testcontainers::ContainerAsync<Mosquitto>,
    String,
    u16,
) {
    let container = Mosquitto::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(1883).await.unwrap();
    (container, host, port)
}

#[tokio::test]
async fn publish_and_parse() {
    let (_container, host, port) = start_broker().await;

    // subscriber client
    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-test-sub")
        .build();

    // publisher client
    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-test-pub")
        .build();

    // need to poll eventloops to establish connections
    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    // subscribe
    sub_client.subscribe::<TemperatureReading>().await.unwrap();

    // publish
    pub_client
        .publish(TemperatureReading {
            device_id: 42,
            data: 23.5,
        })
        .await
        .unwrap();

    // poll and parse
    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::Publish(p)) => {
                if let Some(msg) = sub_client
                    .parse_topic::<TemperatureReading>(&p.topic, &p.payload)
                    .unwrap()
                {
                    assert_eq!(msg.device_id, 42);
                    assert!((msg.data - 23.5).abs() < f32::EPSILON);
                    break;
                }
            }
            _ => continue,
        }
    }
}

#[tokio::test]
async fn subscribe_many_and_parse_enum() {
    let (_container, host, port) = start_broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-test-sub-many")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-test-pub-many")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    // subscribe to all topics in enum
    sub_client.subscribe_many::<SensorTopics>().await.unwrap();

    // publish temperature
    pub_client
        .publish(TemperatureReading {
            device_id: 1,
            data: 20.0,
        })
        .await
        .unwrap();

    // publish humidity
    pub_client
        .publish(HumidityReading {
            device_id: 2,
            data: 65.0,
        })
        .await
        .unwrap();

    let mut received = Vec::new();
    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::Publish(p)) => {
                if let Ok(event) = sub_client.parse::<SensorTopics>(&p.topic, &p.payload) {
                    received.push(event);
                    if received.len() == 2 {
                        break;
                    }
                }
            }
            _ => continue,
        }
    }

    assert!(matches!(received[0], SensorTopics::Temperature(ref msg) if msg.device_id == 1));
    assert!(matches!(received[1], SensorTopics::Humidity(ref msg) if msg.device_id == 2));
}
