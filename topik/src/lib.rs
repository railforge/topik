//! Typed pub/sub topics for Rust.
//!
//! Define your topics once, get compile-time guarantees everywhere.
//!
//! # Quick start
//!
//! ```ignore
//! use topik::prelude::*;
//!
//! #[derive(Topic)]
//! #[topic(segments("sensors", device_id, "temperature"))]
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
//!     // publish
//!     client.publish(TemperatureReading { device_id: 42, data: 23.5 }).await?;
//!
//!     // subscribe to a single topic type
//!     let mut sub = client.subscribe::<TemperatureReading>().await?;
//!     while let Some(msg) = sub.next().await {
//!         println!("device {} -> {}°C", msg.device_id, msg.data);
//!     }
//!
//!     // or subscribe to multiple topic types at once
//!     let mut sub = client.subscribe_many::<SensorTopics>().await?;
//!     while let Some(event) = sub.next().await {
//!         match event {
//!             SensorTopics::Temperature(msg) => {
//!                 println!("device {} -> {}°C", msg.device_id, msg.data);
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

mod client;
pub mod inmemory;
pub mod mqtt;
mod subscriber;

pub use client::TopikClient;
pub use subscriber::{EnumSubscriber, Subscriber};
// Re-export traits from topik-core
pub use topik_core::{Topic, TopicEnum, TopikError};
// Re-export derive macros
// Users write #[derive(Topic)] and the macro generates the trait impl
pub use topik_macros::{Topic, TopicEnum};

pub mod prelude {
    pub use crate::encoding::{
        BoolEncoding, F32Encoding, F64Encoding, I32Encoding, I64Encoding, RawEncoding,
        StringEncoding, U8Encoding, U16Encoding, U32Encoding, U64Encoding,
    };
    pub use crate::inmemory::InMemoryTransport;
    pub use crate::mqtt::Mqtt;
    pub use crate::segment::{BoolRepr, BoolSegment, OnOff, OneZero, TrueFalse, YesNo};
    pub use crate::{EnumSubscriber, Subscriber, Topic, TopicEnum, TopikClient, TopikError};

    #[cfg(feature = "rumqttc")]
    pub use crate::mqtt::rumqttc::{MqttClient, MqttTransport, MqttTransportBuilder};
}

pub mod encoding {
    pub use topik_core::{
        BoolEncoding, Encoding, F32Encoding, F64Encoding, I32Encoding, I64Encoding, RawEncoding,
        StringEncoding, U8Encoding, U16Encoding, U32Encoding, U64Encoding,
    };
}

pub mod segment {
    pub use topik_core::{BoolRepr, BoolSegment, OnOff, OneZero, Segment, TrueFalse, YesNo};
}

pub mod syntax {
    pub use topik_core::TopicSyntax;
}

pub mod transport {
    pub use topik_core::transport::{MessageStream, RawMessage, Transport};
}

#[doc(hidden)]
pub mod __private {
    pub use topik_core::__private::*;
}
