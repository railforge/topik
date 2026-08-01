//! Typed pub/sub topics for Rust.
//!
//! Define your topics once, get compile-time guarantees everywhere.
//!
//! # Quick start
//!
//! ```ignore
//! use topik::{Topic, TopicEnum, TopikClient};
//! use topik::encoding::F32Encoding;
//! use topik::protocol::Mqtt;
//! use topik::transport::InMemoryTransport;
//!
//! #[derive(Topic)]
//! #[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
//! pub struct TemperatureReading {
//!     pub device_id: u64,
//!     #[payload]
//!     pub data: f32,
//! }
//!
//! #[derive(TopicEnum)]
//! pub enum SensorTopics {
//!     Temperature(TemperatureReading),
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
//!
//!     client.publish(TemperatureReading {
//!         device_id: 42,
//!         data: 23.5,
//!     }).await?;
//!
//!     let mut sub = client.subscribe_many::<SensorTopics>().await?;
//!     while let Some(event) = sub.next().await {
//!         match event {
//!             SensorTopics::Temperature(msg) => {
//!                 println!("device {} → {}°C", msg.device_id, msg.data);
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

mod client;
mod subscriber;

pub use client::TopikClient;
pub use subscriber::{EnumSubscriber, Subscriber};
pub use topik_core::{Topic, TopicEnum, TopikError};
pub use topik_macros::{Topic, TopicEnum};

pub mod encoding {
    pub use topik_core::{
        BoolEncoding, Encoding, F32Encoding, F64Encoding, I32Encoding, I64Encoding, RawEncoding,
        StringEncoding, U8Encoding, U16Encoding, U32Encoding, U64Encoding,
    };
}

pub mod segment {
    pub use topik_core::{
        BinaryBool, BoolRepr, BoolSegment, OnOff, OnOffBool, OneZero, StandardBool, TrueFalse,
        YesNo, YesNoBool,
    };
}

pub mod protocol {
    pub use topik_core::protocol::{Mqtt, Nats, Protocol, Redis};
}

pub mod transport;

#[doc(hidden)]
pub mod __private {
    pub use topik_core::__private::*;
}
