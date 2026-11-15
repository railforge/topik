use topik_core::syntax::TopicSyntax;

pub struct Mqtt;

impl TopicSyntax for Mqtt {
    const SEPARATOR: char = '/';
    const SINGLE_WILDCARD: &'static str = "+";
    const MULTI_WILDCARD: &'static str = "#";
}

#[cfg(feature = "rumqttc")]
pub mod rumqttc;
