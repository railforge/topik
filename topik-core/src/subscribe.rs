use super::syntax::TopicSyntax;

/// Trait for typed subscription builders.
pub trait SubscribeBuilder: Send + 'static {
    /// Build the subscription pattern string.
    fn build_pattern<S: TopicSyntax>(&self) -> String;
}
