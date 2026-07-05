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

    #[error("encoding error")]
    Encoding(#[source] Box<dyn std::error::Error + Send + Sync>),
}
