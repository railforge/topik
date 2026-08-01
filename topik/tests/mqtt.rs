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

/// Wait for broker to acknowledge subscription before publishing.
/// Without this there is a race condition where the publish arrives
/// before the subscription is registered.
async fn wait_for_suback(eventloop: &mut rumqttc::EventLoop) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::SubAck(_)) => break,
                _ => {}
            }
        }
    })
    .await
    .expect("suback timed out");
}

// Each test uses its own topic types with unique prefixes
// so subscriptions never overlap between parallel tests.

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("t1", device_id, "temp"), encoding = F32Encoding)]
struct T1Temperature {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("t2", device_id, "temp"), encoding = F32Encoding)]
struct T2Temperature {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("t2", device_id, "humidity"), encoding = F32Encoding)]
struct T2Humidity {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("t3", device_id, "temp"), encoding = F32Encoding)]
struct T3Temperature {
    device_id: u64,
    #[payload]
    data: f32,
}

#[derive(TopicEnum, Debug)]
enum T2Topics {
    Temperature(T2Temperature),
    Humidity(T2Humidity),
}

#[tokio::test]
async fn publish_and_parse() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t1-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t1-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe::<T1Temperature>().await.unwrap();
    wait_for_suback(&mut eventloop).await;

    pub_client
        .publish(T1Temperature {
            device_id: 1,
            data: 23.5,
        })
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::Publish(p)) => {
                    if let Some(msg) = sub_client
                        .parse_topic::<T1Temperature>(&p.topic, &p.payload)
                        .unwrap()
                    {
                        assert_eq!(msg.device_id, 1);
                        assert!((msg.data - 23.5).abs() < f32::EPSILON);
                        break;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn subscribe_many_and_parse_enum() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t2-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t2-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe_many::<T2Topics>().await.unwrap();

    // wait for both subacks — subscribe_many sends two SUBSCRIBE packets
    wait_for_suback(&mut eventloop).await;
    wait_for_suback(&mut eventloop).await;

    pub_client
        .publish(T2Temperature {
            device_id: 1,
            data: 20.0,
        })
        .await
        .unwrap();

    pub_client
        .publish(T2Humidity {
            device_id: 2,
            data: 65.0,
        })
        .await
        .unwrap();

    let mut received = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::Publish(p)) => {
                    if let Ok(event) = sub_client.parse::<T2Topics>(&p.topic, &p.payload) {
                        received.push(event);
                        if received.len() == 2 {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("test timed out");

    assert!(matches!(received[0], T2Topics::Temperature(ref msg) if msg.device_id == 1));
    assert!(matches!(received[1], T2Topics::Humidity(ref msg) if msg.device_id == 2));
}

#[tokio::test]
async fn publish_with_qos_and_retain() {
    let (host, port) = broker().await;

    let (sub_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t3-sub")
        .build();

    let (pub_client, mut pub_eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-t3-pub")
        .build();

    tokio::spawn(async move { while pub_eventloop.poll().await.is_ok() {} });

    sub_client.subscribe::<T3Temperature>().await.unwrap();
    wait_for_suback(&mut eventloop).await;

    pub_client
        .publish(T3Temperature {
            device_id: 1,
            data: 23.5,
        })
        .qos(QoS::AtMostOnce)
        .retain(false)
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::Publish(p)) => {
                    if let Some(msg) = sub_client
                        .parse_topic::<T3Temperature>(&p.topic, &p.payload)
                        .unwrap()
                    {
                        assert_eq!(msg.device_id, 1);
                        assert!((msg.data - 23.5).abs() < f32::EPSILON);
                        assert_eq!(p.qos, QoS::AtMostOnce);
                        break;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn builder_with_credentials_and_clean_session() {
    let (host, port) = broker().await;

    let (_client, mut eventloop) = MqttClient::builder()
        .url(&host, port)
        .client_id("topik-creds-test")
        .clean_session(true)
        .credentials("user", "password")
        .build();

    // just verify connection establishes without panic
    eventloop.poll().await.unwrap();
}
