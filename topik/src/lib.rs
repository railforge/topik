//! Typed pub/sub topics for Rust.
//!
//! Define your topics once, get compile-time guarantees everywhere.
//!
//! # Quick start
//!
//! ```rust
//! use topik::Topic;
//! use topik::encoding::RawEncoding;
//! use topik::protocol::Mqtt;
//! use bytes::Bytes;
//!
//! #[derive(Topic)]
//! #[topic(segments("sensors", device_id), encoding = RawEncoding)]
//! pub struct TemperatureReading {
//!     pub device_id: u64,
//!     #[payload]
//!     pub data: Bytes,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = TopikClient::connect(
//!         Mqtt::builder()
//!             .url("mqtt://localhost:1883")
//!             .client_id("my-service")
//!             .build()
//!     ).await?;
//!
//!     client.publish(TemperatureReading {
//!         device_id: 42,
//!         data: Bytes::from("23.5"),
//!     }).await?;
//!
//!     let mut sub = client.subscribe::<TemperatureReading>().await?;
//!     while let Some(msg) = sub.next().await {
//!         println!("device {} sent {:?}", msg.device_id, msg.data);
//!     }
//!
//!     Ok(())
//! }
//! ```

mod client;
mod subscriber;

pub use client::TopikClient;
pub use subscriber::Subscriber;
pub use topik_core::{Topic, TopikError};
pub use topik_macros::Topic;

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
