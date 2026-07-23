//! Core traits and types for [topik](https://docs.rs/topik), typed pub/sub topics for Rust.
//!
//! This crate provides internal foundational abstractions. Most users should depend on [`topik`](https://docs.rs/topik)
//! directly rather than this crate.
//!
//! # Crate contents
//!
//! - [`TopikError`]: error type for all topik operations
//! - [`BoolRepr`]: trait for configurable boolean segment representations
//! - [`Encoding`]: trait for payload serialization strategies
//! - [`RawEncoding`]: raw bytes encoding for unknown or legacy payloads
//! - [`Topic`]: the core trait users derive on their topic structs

mod encoding;
mod error;
mod segments;
mod topic;

pub use encoding::{
    BoolEncoding, Encoding, I32Encoding, I64Encoding, RawEncoding, StringEncoding, U8Encoding,
    U16Encoding, U32Encoding, U64Encoding,
};
pub use error::TopikError;
pub use segments::{
    BinaryBool, BoolRepr, BoolSegment, OnOff, OnOffBool, OneZero, StandardBool, TrueFalse, YesNo,
    YesNoBool,
};
pub use topic::Topic;

#[doc(hidden)]
pub mod __private {
    pub use crate::segments::Segment;
    pub use crate::topic::TopicWire;
    pub use crate::topic::private::Sealed;
}
