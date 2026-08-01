#![cfg(feature = "mqtt")]

use rumqttc::{Event, Packet, QoS};
use testcontainers_modules::{mosquitto::Mosquitto, testcontainers::runners::AsyncRunner};
use tokio::sync::OnceCell;
use topik::encoding::F32Encoding;
use topik::{MqttClient, Topic, TopicEnum};

static BROKER: OnceCell<(String, u16)> = OnceCell::const_new();

async fn broker() -> (String, u16) {
    BROKER
        .get_or_init(|| async {
            let container = Box::leak(Box::new(Mosquitto::default().start().await.unwrap()));
            let host = container.get_host().await.unwrap().to_string();
            let port = container.get_host_port_ipv4(1883).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            (host, port)
        })
        .await
        .clone()
}

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

#[tokio::test]
async fn publish_and_parse() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-pub-parse-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-pub-parse-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe::<TemperatureReading>().await.unwrap();

    pub_client
        .publish(TemperatureReading {
            device_id: 100,
            data: 23.5,
        })
        .await
        .unwrap();

    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::Publish(p)) => {
                if let Some(msg) = sub_client
                    .parse_topic::<TemperatureReading>(&p.topic, &p.payload)
                    .unwrap()
                {
                    if msg.device_id == 100 {
                        assert!((msg.data - 23.5).abs() < f32::EPSILON);
                        break;
                    }
                }
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn subscribe_many_and_parse_enum() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-many-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-many-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe_many::<SensorTopics>().await.unwrap();

    pub_client
        .publish(TemperatureReading {
            device_id: 200,
            data: 20.0,
        })
        .await
        .unwrap();

    pub_client
        .publish(HumidityReading {
            device_id: 201,
            data: 65.0,
        })
        .await
        .unwrap();

    let mut received = Vec::new();
    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::Publish(p)) => {
                if let Ok(event) = sub_client.parse::<SensorTopics>(&p.topic, &p.payload) {
                    let relevant = match &event {
                        SensorTopics::Temperature(msg) if msg.device_id == 200 => true,
                        SensorTopics::Humidity(msg) if msg.device_id == 201 => true,
                        _ => false,
                    };
                    if relevant {
                        received.push(event);
                        if received.len() == 2 {
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    assert!(matches!(received[0], SensorTopics::Temperature(ref msg) if msg.device_id == 200));
    assert!(matches!(received[1], SensorTopics::Humidity(ref msg) if msg.device_id == 201));
}

#[tokio::test]
async fn publish_with_qos_and_retain() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-qos-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-qos-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe::<TemperatureReading>().await.unwrap();

    pub_client
        .publish(TemperatureReading {
            device_id: 300,
            data: 23.5,
        })
        .qos(QoS::AtMostOnce)
        .retain(false)
        .await
        .unwrap();

    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::Publish(p)) => {
                if let Some(msg) = sub_client
                    .parse_topic::<TemperatureReading>(&p.topic, &p.payload)
                    .unwrap()
                {
                    if msg.device_id == 300 {
                        assert!((msg.data - 23.5).abs() < f32::EPSILON);
                        assert_eq!(p.qos, QoS::AtMostOnce);
                        break;
                    }
                }
            }
            _ => {}
        }
    }
}
