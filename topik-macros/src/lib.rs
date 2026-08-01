use proc_macro::TokenStream;

mod generate;
mod parse;

/// Derive macro for typed pub/sub topics.
///
/// Generates implementations of `Topic`, `TopicWire`, and the associated
/// `Key` and `SubscribeBuilder` structs for the annotated struct.
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
/// use topik::encoding::F32Encoding;
///
/// #[derive(Topic)]
/// #[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
/// pub struct TemperatureReading {
///     pub device_id: u64,
///     #[payload]
///     pub data: f32,
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

/// Derive macro for grouping multiple topic types into a single enum.
///
/// Generates a `patterns()` method returning all subscription patterns
/// and a `try_parse()` method for matching incoming raw messages against
/// all variants.
///
/// Each variant must be a tuple variant containing exactly one type
/// that implements `Topic`.
///
/// # Example
///
/// ```ignore
/// use topik::TopicEnum;
///
/// #[derive(TopicEnum)]
/// enum SensorTopics {
///     Temperature(TemperatureReading),
///     Humidity(HumidityReading),
///     Reboot(RebootCommand),
/// }
///
/// // subscribe to all patterns at once
/// let patterns = SensorTopics::patterns('/', "+", "#");
///
/// // parse an incoming message
/// match SensorTopics::try_parse(&topic, &payload, '/') {
///     Ok(SensorTopics::Temperature(msg)) => handle_temp(msg),
///     Ok(SensorTopics::Humidity(msg)) => handle_humidity(msg),
///     Ok(SensorTopics::Reboot(msg)) => handle_reboot(msg),
///     Err(e) => eprintln!("no match: {}", e),
/// }
/// ```
#[proc_macro_derive(TopicEnum)]
pub fn derive_topic_enum(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match parse::parse_topic_enum_input(input) {
        Ok(topic_enum) => generate::generate_topic_enum(topic_enum).into(),
        Err(err) => err.to_compile_error().into(),
    }
}
