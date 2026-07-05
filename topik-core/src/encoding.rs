use crate::TopikError;
use bytes::Bytes;

/// Abstraction over payload serialization and deserialization.
///
/// Topik is encoding-agnostic. Implementations are stateless marker
/// types, meaning the encoding strategy is part of the type itself rather
/// than runtime configuration.
///
/// # Implementing a custom encoding
///
/// ```rust
/// use topik_core::{Encoding, TopikError};
/// use bytes::Bytes;
///
/// struct MessagePackEncoding;
///
/// impl Encoding<MySensorReading> for MessagePackEncoding {
///     fn encode(value: &MySensorReading) -> Result<Bytes, TopikError> {
///         // serialize with messagepack
///         todo!()
///     }
///
///     fn decode(bytes: Bytes) -> Result<MySensorReading, TopikError> {
///         // deserialize with messagepack
///         todo!()
///     }
/// }
/// ```
///
/// # Provided implementations
///
/// | Encoding | Crate | Feature flag |
/// |----------|-------|--------------|
/// | [`RawEncoding`] | `topik-core` | always available |
/// | `JsonEncoding` | `topik` | `json` (default) |
/// | `ProtobufEncoding` | `topik` | `protobuf` |
pub trait Encoding<T> {
    /// Serialize a value into raw bytes for transmission over the wire.
    fn encode(value: &T) -> Result<Bytes, TopikError>;

    /// Deserialize raw bytes received from the wire into a value.
    fn decode(bytes: Bytes) -> Result<T, TopikError>;
}

/// Raw bytes encoding.
///
/// Use this when the payload schema is not yet known, or when interoperating
/// with legacy systems where your service only forwards raw bytes without
/// inspecting them.
///
/// This is the starting point for the migration story. Define your topic
/// structure first, type the payload later:
///
/// ```rust
/// use topik_core::RawEncoding;
/// use bytes::Bytes;
///
/// #[derive(Topic)]
/// #[topic(segments("legacy", "v1", device_id, kind))]
/// #[topic(encoding = RawEncoding)]
/// struct LegacySensor {
///     device_id: u64,
///     kind: String,
///     #[payload]
///     data: Bytes,  // don't know the schema yet, that's fine
/// }
///
/// // later, once you understand the payload:
/// // #[topic(encoding = JsonEncoding)]
/// // data: SensorReading,
/// ```
pub struct RawEncoding;

impl Encoding<Bytes> for RawEncoding {
    fn encode(value: &Bytes) -> Result<Bytes, TopikError> {
        Ok(value.clone())
    }

    fn decode(bytes: Bytes) -> Result<Bytes, TopikError> {
        Ok(bytes)
    }
}
