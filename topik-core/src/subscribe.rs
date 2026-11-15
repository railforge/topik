use super::protocol::Protocol;

/// Trait for typed subscription builders.
pub trait SubscribeBuilder: Send + 'static {
    /// Build the subscription pattern string.
    fn build_pattern<P: Protocol>(&self) -> String;
}
