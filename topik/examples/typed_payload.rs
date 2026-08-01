//! Typed payload example: shows all available encodings.
//!
//! Run with:
//!   cargo run --example typed_payload

use bytes::Bytes;
use topik::encoding::{
    BoolEncoding, F32Encoding, I32Encoding, RawEncoding, StringEncoding, U64Encoding,
};
use topik::protocol::Mqtt;
use topik::segment::OneZero;
use topik::transport::InMemoryTransport;
use topik::{Topic, TopikClient};

// float payload
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}

// String payload
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "status"), encoding = StringEncoding)]
pub struct DeviceStatus {
    pub device_id: u64,
    #[payload]
    pub data: String,
}

// bool payload
// BoolEncoding is configurable — choose how true/false are encoded:
//   BoolEncoding<TrueFalse> -> "true" / "false"  (default)
//   BoolEncoding<OneZero>   -> "1" / "0"          (legacy systems)
//   BoolEncoding<YesNo>     -> "yes" / "no"
//   BoolEncoding<OnOff>     -> "on" / "off"       (IoT/home automation)
#[derive(Topic, Debug)]
#[topic(segments("devices", device_id, "active"), encoding = BoolEncoding<OneZero>)]
pub struct DeviceActive {
    pub device_id: u64,
    #[payload]
    pub data: bool,
}

// Numeric payload
// Supported: U8Encoding, U16Encoding, U32Encoding, U64Encoding,
//            I32Encoding, I64Encoding, F32Encoding, F64Encoding
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "count"), encoding = U64Encoding)]
pub struct MessageCount {
    pub device_id: u64,
    #[payload]
    pub data: u64,
}

// Signed integer payload
#[derive(Topic, Debug)]
#[topic(segments("sensors", device_id, "offset"), encoding = I32Encoding)]
pub struct TemperatureOffset {
    pub device_id: u64,
    #[payload]
    pub data: i32,
}

// Raw bytes payload
// Use `RawEncoding` when the payload schema is unknown or for legacy systems.
// Migrate to a typed encoding once the schema is understood.
#[derive(Topic, Debug)]
#[topic(segments("legacy", device_id, "raw"), encoding = RawEncoding)]
pub struct LegacyReading {
    pub device_id: u64,
    #[payload]
    pub data: Bytes,
}

#[tokio::main]
async fn main() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    // float
    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();
    client
        .publish(TemperatureReading {
            device_id: 1,
            data: 23.5,
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("f32     -> device {} sent {:.1}°C", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();

    // String
    let mut sub = client.subscribe::<DeviceStatus>().await.unwrap();
    client
        .publish(DeviceStatus {
            device_id: 2,
            data: "online".to_string(),
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("String  -> device {} is {}", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();

    // bool (OneZero)
    let mut sub = client.subscribe::<DeviceActive>().await.unwrap();
    client
        .publish(DeviceActive {
            device_id: 3,
            data: true,
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("bool    -> device {} active: {}", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();

    // u64
    let mut sub = client.subscribe::<MessageCount>().await.unwrap();
    client
        .publish(MessageCount {
            device_id: 4,
            data: 1000,
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("u64     -> device {} count: {}", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();

    // i32
    let mut sub = client.subscribe::<TemperatureOffset>().await.unwrap();
    client
        .publish(TemperatureOffset {
            device_id: 5,
            data: -3,
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("i32     -> device {} offset: {}°C", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();

    // Raw bytes
    let mut sub = client.subscribe::<LegacyReading>().await.unwrap();
    client
        .publish(LegacyReading {
            device_id: 6,
            data: Bytes::from("unknown_format"),
        })
        .await
        .unwrap();
    let msg = sub.next().await.unwrap();
    println!("Bytes   -> device {} raw: {:?}", msg.device_id, msg.data);
    sub.unsubscribe().await.unwrap();
}
