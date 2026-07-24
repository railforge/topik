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

/// Defines the string representation of a boolean topic segment.
///
/// Implement this on a unit struct to define what strings represent
/// `true` and `false` in your topic. This is useful for legacy systems
/// that use non-standard boolean representations.
///
/// # Example
///
/// ```ignore
/// use topik_core::{BoolRepr, BoolSegment, Segment};
///
/// // your legacy system uses "active" and "inactive"
/// struct ActiveInactive;
///
/// impl BoolRepr for ActiveInactive {
///     const TRUE: &'static str = "active";
///     const FALSE: &'static str = "inactive";
/// }
///
/// type ActiveFlag = BoolSegment<ActiveInactive>;
///
/// // renders as "active" on the wire
/// let active = ActiveFlag::from(true);
/// assert_eq!(active.render(), "active");
///
/// // parses "inactive" back to false
/// let inactive = ActiveFlag::parse("inactive").unwrap();
/// assert_eq!(inactive.as_bool(), false);
/// ```
///
/// # Provided implementations
///
/// Topik ships common representations out of the box:
///
/// ```ignore
/// use topik_core::{StandardBool, BinaryBool, YesNoBool, OnOffBool};
///
/// // "true" / "false" default, most common
/// type MyBool = StandardBool;
///
/// // "1" / "0" common in industrial/legacy systems
/// type MyFlag = BinaryBool;
///
/// // "yes" / "no"
/// type MyAnswer = YesNoBool;
///
/// // "on" / "off" common in IoT/home automation
/// type MySwitch = OnOffBool;
/// ```
pub trait BoolRepr {
    const TRUE: &'static str;
    const FALSE: &'static str;
}

/// A boolean topic segment with a configurable string representation.
///
/// The representation is defined by a [`BoolRepr`] implementation,
/// making the true/false strings part of the type.
///
/// # Example
///
/// ```ignore
/// use topik_core::{BoolSegment, BoolRepr, Segment};
///
/// struct OnOff;
/// impl BoolRepr for OnOff {
///     const TRUE: &'static str = "on";
///     const FALSE: &'static str = "off";
/// }
///
/// #[derive(Topic)]
/// #[topic(segments("home", room, "light", state))]
/// struct LightState {
///     room: String,
///     state: BoolSegment<OnOff>,
/// }
///
/// // subscriber receives typed state
/// client.subscribe::<LightState>(|msg| async move {
///     if msg.state.as_bool() {
///         println!("light in {} is on", msg.room);
///     }
/// }).await?;
/// ```
pub struct BoolSegment<R: BoolRepr>(bool, std::marker::PhantomData<R>);

impl<R: BoolRepr> BoolSegment<R> {
    /// Returns the inner bool value.
    pub fn as_bool(&self) -> bool {
        self.0
    }
}

impl<R: BoolRepr> From<bool> for BoolSegment<R> {
    fn from(b: bool) -> Self {
        BoolSegment(b, std::marker::PhantomData)
    }
}

impl<R: BoolRepr> std::fmt::Debug for BoolSegment<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoolSegment").field(&self.0).finish()
    }
}

impl<R: BoolRepr> Clone for BoolSegment<R> {
    fn clone(&self) -> Self {
        BoolSegment(self.0, std::marker::PhantomData)
    }
}

impl<R: BoolRepr> Segment for BoolSegment<R> {
    fn render(&self) -> Cow<'_, str> {
        if self.0 {
            Cow::Borrowed(R::TRUE)
        } else {
            Cow::Borrowed(R::FALSE)
        }
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        if s == R::TRUE {
            Ok(BoolSegment(true, std::marker::PhantomData))
        } else if s == R::FALSE {
            Ok(BoolSegment(false, std::marker::PhantomData))
        } else {
            Err(TopikError::ParseError {
                topic: s.to_string(),
                reason: format!("expected '{}' or '{}', got '{}'", R::TRUE, R::FALSE, s),
            })
        }
    }
}

// --- Provided BoolRepr implementations ---

/// Standard boolean representation: `"true"` / `"false"`.
pub struct TrueFalse;
impl BoolRepr for TrueFalse {
    const TRUE: &'static str = "true";
    const FALSE: &'static str = "false";
}

/// Binary boolean representation: `"1"` / `"0"`.
/// Common in industrial and legacy systems.
pub struct OneZero;
impl BoolRepr for OneZero {
    const TRUE: &'static str = "1";
    const FALSE: &'static str = "0";
}

/// Yes/No boolean representation: `"yes"` / `"no"`.
pub struct YesNo;
impl BoolRepr for YesNo {
    const TRUE: &'static str = "yes";
    const FALSE: &'static str = "no";
}

/// On/Off boolean representation: `"on"` / `"off"`.
/// Common in IoT and home automation systems.
pub struct OnOff;
impl BoolRepr for OnOff {
    const TRUE: &'static str = "on";
    const FALSE: &'static str = "off";
}

// --- Type aliases for convenience ---

/// Boolean segment using `"true"` / `"false"`. The default choice.
pub type StandardBool = BoolSegment<TrueFalse>;

/// Boolean segment using `"1"` / `"0"`.
pub type BinaryBool = BoolSegment<OneZero>;

/// Boolean segment using `"yes"` / `"no"`.
pub type YesNoBool = BoolSegment<YesNo>;

/// Boolean segment using `"on"` / `"off"`.
pub type OnOffBool = BoolSegment<OnOff>;

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
