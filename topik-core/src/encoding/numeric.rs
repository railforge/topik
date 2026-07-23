use super::Encoding;
use bytes::Bytes;

use crate::TopikError;

macro_rules! impl_numeric_encoding {
    ($name:ident, $ty:ty, $doc:literal) => {
        #[doc = $doc]
        pub struct $name;

        impl Encoding<$ty> for $name {
            fn encode(value: &$ty) -> Result<Bytes, TopikError> {
                Ok(Bytes::from(value.to_string()))
            }

            fn decode(bytes: Bytes) -> Result<$ty, TopikError> {
                let s =
                    std::str::from_utf8(&bytes).map_err(|e| TopikError::Encoding(Box::new(e)))?;
                s.parse::<$ty>()
                    .map_err(|e| TopikError::Encoding(Box::new(e)))
            }
        }
    };
}

impl_numeric_encoding!(U8Encoding, u8, "Raw `u8` payload encoding.");
impl_numeric_encoding!(U16Encoding, u16, "Raw `u16` payload encoding.");
impl_numeric_encoding!(U32Encoding, u32, "Raw `u32` payload encoding.");
impl_numeric_encoding!(U64Encoding, u64, "Raw `u64` payload encoding.");
impl_numeric_encoding!(I32Encoding, i32, "Raw `i32` payload encoding.");
impl_numeric_encoding!(I64Encoding, i64, "Raw `i64` payload encoding.");
impl_numeric_encoding!(F32Encoding, f32, "Raw `f32` payload encoding.");
impl_numeric_encoding!(F64Encoding, f64, "Raw `f64` payload encoding.");
