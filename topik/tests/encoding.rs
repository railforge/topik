#![allow(dead_code)]

use bytes::Bytes;
use topik::Topic;
use topik::encoding::{
    BoolEncoding, I32Encoding, I64Encoding, RawEncoding, StringEncoding, U8Encoding, U16Encoding,
    U32Encoding, U64Encoding,
};
use topik::segment::OneZero;
use topik_core::__private::TopicWire;
use topik_core::Encoding;

// --- StringEncoding topic ---

#[derive(Topic)]
#[topic(segments("sensor", device_id), encoding = StringEncoding)]
struct StringSensor {
    device_id: u64,
    #[payload]
    data: String,
}

// --- RawEncoding topic ---

#[derive(Topic)]
#[topic(segments("sensor", device_id), encoding = RawEncoding)]
struct RawSensor {
    device_id: u64,
    #[payload]
    data: Bytes,
}

// --- U64Encoding topic ---

#[derive(Topic)]
#[topic(segments("counter", device_id), encoding = U64Encoding)]
struct CounterReading {
    device_id: u64,
    #[payload]
    data: u64,
}

// --- BoolEncoding topic ---

#[derive(Topic)]
#[topic(segments("device", device_id, "active"), encoding = BoolEncoding<OneZero>)]
struct DeviceActive {
    device_id: u64,
    #[payload]
    data: bool,
}

// --- StringEncoding tests ---

#[test]
fn string_encoding_encode() {
    let bytes = StringEncoding::encode(&"hello".to_string()).unwrap();
    assert_eq!(bytes, Bytes::from("hello"));
}

#[test]
fn string_encoding_decode() {
    let s = StringEncoding::decode(Bytes::from("hello")).unwrap();
    assert_eq!(s, "hello");
}

#[test]
fn string_encoding_decode_invalid_utf8() {
    let err = StringEncoding::decode(Bytes::from(vec![0xFF, 0xFE])).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

#[test]
fn string_encoding_from_key_and_payload() {
    let key = StringSensor::parse("sensor/42", '/').unwrap();
    let msg = StringSensor::from_key_and_payload(key, "hello world".to_string());
    assert_eq!(msg.device_id, 42);
    assert_eq!(msg.data, "hello world");
}

// --- RawEncoding tests ---

#[test]
fn raw_encoding_encode() {
    let bytes = Bytes::from("raw");
    let encoded = RawEncoding::encode(&bytes).unwrap();
    assert_eq!(encoded, bytes);
}

#[test]
fn raw_encoding_decode() {
    let bytes = Bytes::from("raw");
    let decoded = RawEncoding::decode(bytes.clone()).unwrap();
    assert_eq!(decoded, bytes);
}

#[test]
fn raw_encoding_from_key_and_payload() {
    let key = RawSensor::parse("sensor/42", '/').unwrap();
    let payload = Bytes::from("raw payload");
    let msg = RawSensor::from_key_and_payload(key, payload.clone());
    assert_eq!(msg.device_id, 42);
    assert_eq!(msg.data, payload);
}

// --- Numeric encoding tests ---

#[test]
fn u64_encoding_encode() {
    let bytes = U64Encoding::encode(&42u64).unwrap();
    assert_eq!(bytes, Bytes::from("42"));
}

#[test]
fn u64_encoding_decode() {
    let val = U64Encoding::decode(Bytes::from("42")).unwrap();
    assert_eq!(val, 42u64);
}

#[test]
fn u64_encoding_decode_invalid() {
    let err = U64Encoding::decode(Bytes::from("notanumber")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

#[test]
fn u64_encoding_from_key_and_payload() {
    let key = CounterReading::parse("counter/1", '/').unwrap();
    let msg = CounterReading::from_key_and_payload(key, 999u64);
    assert_eq!(msg.device_id, 1);
    assert_eq!(msg.data, 999u64);
}

#[test]
fn i32_encoding_negative() {
    let bytes = I32Encoding::encode(&-42i32).unwrap();
    assert_eq!(bytes, Bytes::from("-42"));
    let val = I32Encoding::decode(bytes).unwrap();
    assert_eq!(val, -42i32);
}

#[test]
fn u8_encoding_round_trip() {
    let bytes = U8Encoding::encode(&255u8).unwrap();
    assert_eq!(bytes, Bytes::from("255"));
    let val = U8Encoding::decode(bytes).unwrap();
    assert_eq!(val, 255u8);
}

#[test]
fn u8_encoding_decode_invalid() {
    let err = U8Encoding::decode(Bytes::from("256")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

#[test]
fn u16_encoding_round_trip() {
    let bytes = U16Encoding::encode(&65535u16).unwrap();
    assert_eq!(bytes, Bytes::from("65535"));
    let val = U16Encoding::decode(bytes).unwrap();
    assert_eq!(val, 65535u16);
}

#[test]
fn u16_encoding_decode_invalid() {
    let err = U16Encoding::decode(Bytes::from("65536")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

#[test]
fn u32_encoding_round_trip() {
    let bytes = U32Encoding::encode(&4294967295u32).unwrap();
    assert_eq!(bytes, Bytes::from("4294967295"));
    let val = U32Encoding::decode(bytes).unwrap();
    assert_eq!(val, 4294967295u32);
}

#[test]
fn u32_encoding_decode_invalid() {
    let err = U32Encoding::decode(Bytes::from("4294967296")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

#[test]
fn i64_encoding_round_trip() {
    let bytes = I64Encoding::encode(&-9223372036854775808i64).unwrap();
    assert_eq!(bytes, Bytes::from("-9223372036854775808"));
    let val = I64Encoding::decode(bytes).unwrap();
    assert_eq!(val, i64::MIN);
}

#[test]
fn i64_encoding_decode_invalid() {
    let err = I64Encoding::decode(Bytes::from("notanumber")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::Encoding(_)));
}

// --- BoolEncoding tests ---

#[test]
fn bool_encoding_onezero_encode_true() {
    let bytes = BoolEncoding::<OneZero>::encode(&true).unwrap();
    assert_eq!(bytes, Bytes::from("1"));
}

#[test]
fn bool_encoding_onezero_encode_false() {
    let bytes = BoolEncoding::<OneZero>::encode(&false).unwrap();
    assert_eq!(bytes, Bytes::from("0"));
}

#[test]
fn bool_encoding_onezero_decode_true() {
    let val = BoolEncoding::<OneZero>::decode(Bytes::from("1")).unwrap();
    assert!(val);
}

#[test]
fn bool_encoding_onezero_decode_false() {
    let val = BoolEncoding::<OneZero>::decode(Bytes::from("0")).unwrap();
    assert!(!val);
}

#[test]
fn bool_encoding_onezero_decode_invalid() {
    let err = BoolEncoding::<OneZero>::decode(Bytes::from("yes")).unwrap_err();
    assert!(matches!(err, topik_core::TopikError::EncodingMessage(_)));
}

#[test]
fn bool_encoding_from_key_and_payload() {
    let key = DeviceActive::parse("device/42/active", '/').unwrap();
    let msg = DeviceActive::from_key_and_payload(key, true);
    assert_eq!(msg.device_id, 42);
    assert!(msg.data);
}
