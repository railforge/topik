use bytes::Bytes;

use super::Encoding;
use crate::TopikError;

/// Raw bytes encoding.
///
/// Use this when the payload schema is not yet known, or when forwarding
/// messages without inspecting them.
///
/// ```ignore
/// #[derive(Topic)]
/// #[topic(segments("legacy", device_id), encoding = RawEncoding)]
/// pub struct LegacyTopic {
///     pub device_id: u64,
///     #[payload]
///     pub data: Bytes,
/// }
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

/// UTF-8 string encoding.
///
/// Use when the payload is a raw string.
///
/// ```ignore
/// #[derive(Topic)]
/// #[topic(segments("legacy", device_id), encoding = StringEncoding)]
/// pub struct LegacyReading {
///     pub device_id: u64,
///     #[payload]
///     pub data: String,
/// }
/// ```
pub struct StringEncoding;

impl Encoding<String> for StringEncoding {
    fn encode(value: &String) -> Result<Bytes, TopikError> {
        Ok(Bytes::from(value.clone()))
    }

    fn decode(bytes: Bytes) -> Result<String, TopikError> {
        String::from_utf8(bytes.into()).map_err(|e| TopikError::Encoding(Box::new(e)))
    }
}
