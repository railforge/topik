mod bool;
mod numeric;
mod raw;

use bytes::Bytes;

use crate::TopikError;
pub use bool::BoolEncoding;
pub use numeric::{
    F32Encoding, F64Encoding, I32Encoding, I64Encoding, U8Encoding, U16Encoding, U32Encoding,
    U64Encoding,
};
pub use raw::{RawEncoding, StringEncoding};

/// Abstraction over payload serialization and deserialization.
///
/// Topik is encoding-agnostic — JSON, Protobuf, raw strings, or raw bytes
/// all plug in through this trait. Implementations are stateless marker
/// types, meaning the encoding strategy is part of the type itself rather
/// than runtime configuration.
///
/// # Implementing a custom encoding
///
/// ```ignore
/// use topik_core::{Encoding, TopikError};
/// use bytes::Bytes;
///
/// struct MyEncoding;
///
/// impl Encoding<MyType> for MyEncoding {
///     fn encode(value: &MyType) -> Result<Bytes, TopikError> {
///         todo!()
///     }
///     fn decode(bytes: Bytes) -> Result<MyType, TopikError> {
///         todo!()
///     }
/// }
/// ```
///
/// # Provided implementations
///
/// | Encoding | Type | Feature |
/// |----------|------|---------|
/// | [`RawEncoding`] | `Bytes` | always |
/// | [`StringEncoding`] | `String` | always |
/// | [`BoolEncoding`] | `bool` | always |
/// | `U8Encoding` through `I64Encoding` | numeric primitives | always |
/// | `JsonEncoding` | any `serde::Serialize + DeserializeOwned` | `json` |
/// | `ProtobufEncoding` | any `prost::Message` | `protobuf` |
pub trait Encoding<T> {
    /// Serialize a value into raw bytes for transmission over the wire.
    fn encode(value: &T) -> Result<Bytes, TopikError>;

    /// Deserialize raw bytes received from the wire into a value.
    fn decode(bytes: Bytes) -> Result<T, TopikError>;
}
