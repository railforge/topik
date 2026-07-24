use super::Encoding;
use crate::{BoolRepr, TopikError};
use bytes::Bytes;
use std::marker::PhantomData;

/// Boolean payload encoding with configurable string representation.
///
/// Reuses [`BoolRepr`] for the same representation you use for a bool
/// topic segment can be used for a bool payload.
///
/// ```ignore
/// use topik_core::encoding::BoolEncoding;
/// use topik_core::segment::OneZero;
///
/// // payload is "1" for true, "0" for false on the wire
/// #[derive(Topic)]
/// #[topic(segments("device", device_id, "active"), encoding = BoolEncoding<OneZero>)]
/// pub struct DeviceActive {
///     pub device_id: u64,
///     #[payload]
///     pub data: bool,
/// }
/// ```
pub struct BoolEncoding<R: BoolRepr>(PhantomData<R>);

impl<R: BoolRepr> Encoding<bool> for BoolEncoding<R> {
    fn encode(value: &bool) -> Result<Bytes, TopikError> {
        let s = if *value { R::TRUE } else { R::FALSE };
        Ok(Bytes::from(s))
    }

    fn decode(bytes: Bytes) -> Result<bool, TopikError> {
        let s = std::str::from_utf8(&bytes).map_err(|e| TopikError::Encoding(Box::new(e)))?;

        if s == R::TRUE {
            Ok(true)
        } else if s == R::FALSE {
            Ok(false)
        } else {
            Err(TopikError::EncodingMessage(format!(
                "expected '{}' or '{}', got '{}'",
                R::TRUE,
                R::FALSE,
                s
            )))
        }
    }
}
