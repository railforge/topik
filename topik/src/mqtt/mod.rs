use topik_core::protocol::Protocol;

pub struct Mqtt;

impl Protocol for Mqtt {
    const SEPARATOR: char = '/';
    const SINGLE_WILDCARD: &'static str = "+";
    const MULTI_WILDCARD: &'static str = "#";
}

#[cfg(feature = "rumqttc")]
pub mod rumqttc;
