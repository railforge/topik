use std::borrow::Cow;

use crate::TopikError;

/// Trait for types that can appear as segments in a topic path.
///
/// Implemented for common primitives out of the box. Custom types
/// can implement this manually, or use the `#[derive(TopicSegment)]`
/// macro (coming in `topik-macros`).
///
/// This trait is used internally by the `Topic` derive macro to render
/// and parse topic strings. Users rarely interact with it directly.
pub trait Segment: Sized {
    /// Render this value as a topic segment string.
    fn render(&self) -> Cow<'_, str>;

    /// Parse a topic segment string back into this type.
    fn parse(s: &str) -> Result<Self, TopikError>;
}

impl Segment for u8 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected u8, got '{}'", s),
        })
    }
}

impl Segment for u16 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected u16, got '{}'", s),
        })
    }
}

impl Segment for u32 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected u32, got '{}'", s),
        })
    }
}

impl Segment for u64 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected u64, got '{}'", s),
        })
    }
}

impl Segment for i32 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected i32, got '{}'", s),
        })
    }
}

impl Segment for i64 {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        s.parse().map_err(|_| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected i64, got '{}'", s),
        })
    }
}

impl Segment for String {
    fn render(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        Ok(s.to_string())
    }
}

#[cfg(feature = "uuid")]
impl Segment for uuid::Uuid {
    fn render(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        uuid::Uuid::parse_str(s).map_err(|e| TopikError::ParseError {
            topic: s.to_string(),
            reason: format!("expected UUID, got '{}': {}", s, e),
        })
    }
}
