/// Trait for typed subscription builders.
///
/// Implemented by generated `{Topic}SubscribeBuilder` structs.
/// Allows pinning specific topic segments while wildcarding others.
///
/// Users never implement this directly.
pub trait SubscribeBuilder: Send + 'static {
    /// Build the subscription pattern string.
    ///
    /// Dynamic segments that have been pinned use their concrete value.
    /// Dynamic segments that have not been pinned use `single` wildcard.
    fn build_pattern(&self, sep: char, single: &'static str) -> String;
}
