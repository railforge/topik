//! Core traits and types for [topik](https://docs.rs/topik) — typed pub/sub topics for Rust.
//!
//! This crate provides the foundational abstractions that everything else
//! builds on. Most users should depend on [`topik`](https://docs.rs/topik)
//! directly rather than this crate.
//!
//! # Crate contents
//!
//! - [`TopikError`] — error type for all topik operations
//! - [`Segment`] — trait for types that appear as topic path segments
//! - [`Encoding`] — trait for payload serialization strategies
//! - [`RawEncoding`] — raw bytes encoding for unknown or legacy payloads
//! - [`Topic`] — the core trait users derive on their topic structs

mod encoding;
mod error;
mod segments;
mod topic;

pub use encoding::{Encoding, RawEncoding};
pub use error::TopikError;
pub use segments::{
    BinaryBool, BoolRepr, BoolSegment, OnOff, OnOffBool, OneZero, Segment, StandardBool, TrueFalse,
    YesNo, YesNoBool,
};
pub use topic::Topic;

/// Internal module for backend implementors.
///
/// This module is not part of the public API and may change at any time.
/// It exists to give backend crates access to [`TopicWire`] without
/// exposing it to end users.
///
/// If you are implementing a backend crate, depend on `topik-core` and
/// access `TopicWire` via this module:
///
/// ```rust
/// use topik_core::__private::TopicWire;
/// ```
#[doc(hidden)]
pub mod __private {
    pub use crate::topic::TopicWire;
}
