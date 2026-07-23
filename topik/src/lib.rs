//! Typed pub/sub topics for Rust.
//!
//! Define your topics once, get compile-time guarantees everywhere.
//!
//! # Quick start
//!
//! ```rust
//! use topik::Topic;
//! use topik::encoding::RawEncoding;
//! use bytes::Bytes;
//!
//! #[derive(Topic)]
//! #[topic(segments("factory", "v2", device_id), encoding = RawEncoding)]
//! pub struct SensorReading {
//!     pub device_id: u64,
//!     #[payload]
//!     pub data: Bytes,
//! }
//! ```

pub use topik_core::{Topic, TopikError};
pub use topik_macros::Topic;

pub mod encoding {
    pub use topik_core::{
        BoolEncoding, Encoding, I32Encoding, I64Encoding, RawEncoding, StringEncoding, U8Encoding,
        U16Encoding, U32Encoding, U64Encoding,
    };
}

pub mod segment {
    pub use topik_core::{
        BinaryBool, BoolRepr, BoolSegment, OnOff, OnOffBool, OneZero, StandardBool, TrueFalse,
        YesNo, YesNoBool,
    };
}

#[doc(hidden)]
pub mod __private {
    pub use topik_core::__private::*;
}
