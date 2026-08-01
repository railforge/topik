//! Core traits and types for [topik](https://docs.rs/topik), typed pub/sub topics for Rust.
//!
//! This crate provides the foundational abstractions that everything else
//! builds on. Most users should depend on [`topik`](https://docs.rs/topik)
//! directly rather than this crate.
//!
//! # Crate contents
//!
//! - [`TopikError`] error type for all topik operations
//! - [`Topic`] the core trait users derive on their topic structs
//! - [`TopicEnum`] groups multiple topic types for unified subscription
//! - [`Encoding`] trait for payload serialization strategies
//! - [`BoolRepr`] trait for configurable boolean segment representations
//! - [`Protocol`] trait for protocol wire format conventions
//! - [`Transport`] trait for broker transport implementations
mod encoding;
mod error;
pub mod protocol;
mod segments;
mod subscribe;
mod topic;
pub mod transport;

pub use encoding::{
    BoolEncoding, Encoding, F32Encoding, F64Encoding, I32Encoding, I64Encoding, RawEncoding,
    StringEncoding, U8Encoding, U16Encoding, U32Encoding, U64Encoding,
};
pub use error::TopikError;
pub use protocol::{Mqtt, Nats, Protocol, Redis};
pub use segments::{
    BinaryBool, BoolRepr, BoolSegment, OnOff, OnOffBool, OneZero, StandardBool, TrueFalse, YesNo,
    YesNoBool,
};
pub use subscribe::SubscribeBuilder;
pub use topic::{Topic, TopicEnum};

#[doc(hidden)]
pub mod __private {
    pub use crate::segments::Segment;
    pub use crate::subscribe::SubscribeBuilder;
    pub use crate::topic::private::Sealed;
    pub use crate::topic::{TopicEnum, TopicWire};
    pub use crate::transport::{MessageStream, RawMessage, Transport};
}
