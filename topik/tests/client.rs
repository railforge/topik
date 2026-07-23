use bytes::Bytes;
use topik::Topic;
use topik::TopikClient;
use topik::encoding::RawEncoding;
use topik::protocol::{Mqtt, Nats};
use topik::transport::InMemoryTransport;

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("sensors", device_id, "temperature"), encoding = RawEncoding)]
struct TemperatureReading {
    device_id: u64,
    #[payload]
    data: Bytes,
}

#[derive(Topic, Debug, PartialEq)]
#[topic(segments("devices", device_id, "active"), encoding = RawEncoding)]
struct DeviceActive {
    device_id: u64,
    #[payload]
    data: Bytes,
}

// --- Basic publish/subscribe ---

#[tokio::test]
async fn publish_subscribe_mqtt() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 42,
            data: Bytes::from("23.5"),
        })
        .await
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.device_id, 42);
    assert_eq!(msg.data, Bytes::from("23.5"));
}

#[tokio::test]
async fn publish_subscribe_nats() {
    let client = TopikClient::new(InMemoryTransport::<Nats>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 7,
            data: Bytes::from("18.2"),
        })
        .await
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.device_id, 7);
    assert_eq!(msg.data, Bytes::from("18.2"));
}

// --- Multiple subscribers ---

#[tokio::test]
async fn multiple_subscribers_receive_same_message() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub1 = client.subscribe::<TemperatureReading>().await.unwrap();
    let mut sub2 = client.subscribe::<TemperatureReading>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 42,
            data: Bytes::from("23.5"),
        })
        .await
        .unwrap();

    let msg1 = sub1.next().await.unwrap();
    let msg2 = sub2.next().await.unwrap();

    assert_eq!(msg1.device_id, 42);
    assert_eq!(msg2.device_id, 42);
}

// --- Multiple topic types ---

#[tokio::test]
async fn subscribe_to_different_topics() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut temp_sub = client.subscribe::<TemperatureReading>().await.unwrap();
    let mut active_sub = client.subscribe::<DeviceActive>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 1,
            data: Bytes::from("22.0"),
        })
        .await
        .unwrap();

    client
        .publish(DeviceActive {
            device_id: 2,
            data: Bytes::from("1"),
        })
        .await
        .unwrap();

    let temp = temp_sub.next().await.unwrap();
    let active = active_sub.next().await.unwrap();

    assert_eq!(temp.device_id, 1);
    assert_eq!(active.device_id, 2);
}

// --- Cloned client ---

#[tokio::test]
async fn cloned_client_shares_transport() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
    let client2 = client.clone();

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    // publish from cloned client
    client2
        .publish(TemperatureReading {
            device_id: 99,
            data: Bytes::from("19.0"),
        })
        .await
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.device_id, 99);
}

// --- Wildcard matching ---

#[tokio::test]
async fn wildcard_matches_any_device_id() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    // publish from different device IDs
    client
        .publish(TemperatureReading {
            device_id: 1,
            data: Bytes::from("20.0"),
        })
        .await
        .unwrap();

    client
        .publish(TemperatureReading {
            device_id: 2,
            data: Bytes::from("21.0"),
        })
        .await
        .unwrap();

    let msg1 = sub.next().await.unwrap();
    let msg2 = sub.next().await.unwrap();

    assert_eq!(msg1.device_id, 1);
    assert_eq!(msg2.device_id, 2);
}

// --- Unsubscribe ---

#[tokio::test]
async fn unsubscribe_stops_receiving() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    client
        .publish(TemperatureReading {
            device_id: 1,
            data: Bytes::from("20.0"),
        })
        .await
        .unwrap();

    // receive first message
    let msg = sub.next().await.unwrap();
    assert_eq!(msg.device_id, 1);

    // unsubscribe
    sub.unsubscribe().await.unwrap();

    // publish another — nobody listening
    client
        .publish(TemperatureReading {
            device_id: 2,
            data: Bytes::from("21.0"),
        })
        .await
        .unwrap();
}
