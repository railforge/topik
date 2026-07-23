use thiserror::Error;

#[derive(Debug, Error)]
pub enum TopikError {
    #[error("parse error on topic '{topic}': {reason}")]
    ParseError { topic: String, reason: String },

    #[error("segment missing at position {position} in topic '{topic}'")]
    MissingSegment { position: usize, topic: String },

    #[error("literal mismatch at position {position}: expected '{expected}', got '{got}'")]
    LiteralMismatch {
        position: usize,
        expected: String,
        got: String,
    },

    /// Wraps an underlying error from a real error type.
    #[error("encoding error")]
    Encoding(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// String-only encoding error for cases where there is no underlying
    /// error to wrap, such as invalid bool representations or unexpected values.
    #[error("encoding error: {0}")]
    EncodingMessage(String),
}

// Compile-time assertion: TopikError must be Send + Sync.
fn _assert_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<TopikError>();
}
