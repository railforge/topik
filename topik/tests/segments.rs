#![allow(dead_code)]

use bytes::Bytes;

use topik::Topic;
use topik::mqtt::Mqtt;
use topik::segment::{BoolSegment, OnOff, OneZero, TrueFalse, YesNo};
use topik_core::__private::TopicWire;

#[derive(Topic)]
#[topic(segments("device", device_id, "active", state))]
struct DeviceState {
    device_id: u64,
    state: BoolSegment<OneZero>,
    #[payload]
    data: Bytes,
}

#[derive(Topic)]
#[topic(segments("home", room, "light", state))]
struct LightState {
    room: String,
    state: BoolSegment<TrueFalse>,
    #[payload]
    data: Bytes,
}

#[derive(Topic)]
#[topic(segments("switch", device_id, state))]
struct SwitchState {
    device_id: u64,
    state: BoolSegment<YesNo>,
    #[payload]
    data: Bytes,
}

#[derive(Topic)]
#[topic(segments("relay", device_id, state))]
struct RelayState {
    device_id: u64,
    state: BoolSegment<OnOff>,
    #[payload]
    data: Bytes,
}

// --- BoolSegment<OneZero> ---

#[test]
fn bool_onezero_renders_true() {
    let topic = DeviceState {
        device_id: 1,
        state: BoolSegment::from(true),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "device/1/active/1");
}

#[test]
fn bool_onezero_renders_false() {
    let topic = DeviceState {
        device_id: 1,
        state: BoolSegment::from(false),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "device/1/active/0");
}

#[test]
fn bool_onezero_parses_true() {
    let key = DeviceState::parse::<Mqtt>("device/1/active/1").unwrap();
    assert!(key.state.as_bool());
}

#[test]
fn bool_onezero_parses_false() {
    let key = DeviceState::parse::<Mqtt>("device/1/active/0").unwrap();
    assert!(!key.state.as_bool());
}

#[test]
fn bool_onezero_parse_invalid() {
    let err = DeviceState::parse::<Mqtt>("device/1/active/yes").unwrap_err();
    assert!(matches!(err, topik_core::TopikError::ParseError { .. }));
}

// --- BoolSegment<TrueFalse> ---

#[test]
fn bool_truefalse_renders_true() {
    let topic = LightState {
        room: "living".to_string(),
        state: BoolSegment::from(true),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "home/living/light/true");
}

#[test]
fn bool_truefalse_parses_true() {
    let key = LightState::parse::<Mqtt>("home/living/light/true").unwrap();
    assert!(key.state.as_bool());
}

#[test]
fn bool_truefalse_parses_false() {
    let key = LightState::parse::<Mqtt>("home/living/light/false").unwrap();
    assert!(!key.state.as_bool());
}

// --- BoolSegment<YesNo> ---

#[test]
fn bool_yesno_renders_true() {
    let topic = SwitchState {
        device_id: 5,
        state: BoolSegment::from(true),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "switch/5/yes");
}

#[test]
fn bool_yesno_parses_true() {
    let key = SwitchState::parse::<Mqtt>("switch/5/yes").unwrap();
    assert!(key.state.as_bool());
}

#[test]
fn bool_yesno_parse_invalid() {
    let err = SwitchState::parse::<Mqtt>("switch/5/1").unwrap_err();
    assert!(matches!(err, topik_core::TopikError::ParseError { .. }));
}

// --- BoolSegment<OnOff> ---

#[test]
fn bool_onoff_renders_true() {
    let topic = RelayState {
        device_id: 3,
        state: BoolSegment::from(true),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "relay/3/on");
}

#[test]
fn bool_onoff_parses_false() {
    let key = RelayState::parse::<Mqtt>("relay/3/off").unwrap();
    assert!(!key.state.as_bool());
}

// --- String segment ---

#[test]
fn string_segment_renders() {
    let topic = LightState {
        room: "kitchen".to_string(),
        state: BoolSegment::from(false),
        data: Bytes::new(),
    };
    assert_eq!(topic.render::<Mqtt>(), "home/kitchen/light/false");
}

#[test]
fn string_segment_parses() {
    let key = LightState::parse::<Mqtt>("home/kitchen/light/true").unwrap();
    assert_eq!(key.room, "kitchen");
}

#[test]
fn string_segment_round_trip() {
    let topic = LightState {
        room: "bedroom".to_string(),
        state: BoolSegment::from(true),
        data: Bytes::new(),
    };
    let rendered = topic.render::<Mqtt>();
    let key = LightState::parse::<Mqtt>(&rendered).unwrap();
    assert_eq!(key.room, "bedroom");
    assert!(key.state.as_bool());
}
