use proc_macro::TokenStream;

mod generate;
mod parse;

/// Derive macro for typed pub/sub topics.
///
/// Generates implementations of `Topic`, `TopicWire`, and the associated
/// `Key` struct for the annotated struct.
///
/// # Required attributes
///
/// - `#[topic(segments(...))]` — ordered list of topic path segments
/// - `#[topic(encoding = ...)]` — payload encoding type
/// - `#[payload]` — marks exactly one field as the message payload
///
/// # Example
///
/// ```ignore
/// use topik::Topic;
///
/// #[derive(Topic)]
/// #[topic(segments("factory", "v2", device_id, kind), encoding = JsonEncoding)]
/// pub struct SensorReading {
///     pub device_id: u64,
///     pub kind: SensorKind,
///     #[payload]
///     pub data: TemperaturePayload,
/// }
/// ```
#[proc_macro_derive(Topic, attributes(topic, payload))]
pub fn derive_topic(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match parse::parse_topic_input(input) {
        Ok(topic_input) => generate::generate(topic_input).into(),
        Err(err) => err.to_compile_error().into(),
    }
}
