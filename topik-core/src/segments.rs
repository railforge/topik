use std::borrow::Cow;
use std::marker::PhantomData;

use crate::TopikError;

/// A single typed part of a topic name.
///
/// A topic is a string made of ordered parts joined by a separator.
/// (e.g. `hello/world/123` with `/` being the separator.)
/// These parts often represent fields of a certain type.
///
/// With `Segment`, we refer to a typed representation of such part.
///
/// This trait allows implementing segments for custom types.
///
/// Use it to implement your own custom types. A custom type must implement
/// `Debug`, `Clone`, `Send` and `'static` to be able to be used as a field
/// of a `#[derive(Topic)]` struct.
///
/// # Example
///
/// ```
/// use std::borrow::Cow;
/// use topik_core::{Segment, TopikError};
///
/// #[derive(Debug, Clone, PartialEq)]
/// enum Room {
///     Kitchen,
///     Garage,
/// }
///
/// impl Segment for Room {
///     fn render(&self) -> Cow<'_, str> {
///         match self {
///             Room::Kitchen => Cow::Borrowed("kitchen"),
///             Room::Garage => Cow::Borrowed("garage"),
///         }
///     }
///
///     fn parse(s: &str) -> Result<Self, TopikError> {
///         match s {
///             "kitchen" => Ok(Room::Kitchen),
///             "garage" => Ok(Room::Garage),
///             _ => Err(TopikError::ParseError {
///                 topic: s.to_string(),
///                 reason: format!("expected 'kitchen' or 'garage', got '{s}'"),
///             }),
///         }
///     }
/// }
///
/// assert_eq!(Room::Kitchen.render(), "kitchen");
/// assert_eq!(Room::parse("garage").unwrap(), Room::Garage);
/// assert!(Room::parse("attic").is_err());
/// ```
/// # Provided implementations
///
/// `u8`, `u16`, `u32`, `u64`, `i32`, `i64`, `String`, [`BoolSegment`] for
/// booleans with a chosen text form, and `uuid::Uuid` with the `uuid` feature.
///
pub trait Segment: Sized {
    /// Render this value as a topic segment string.
    fn render(&self) -> Cow<'_, str>;

    /// Parse a topic segment string back into this type.
    fn parse(s: &str) -> Result<Self, TopikError>;
}

macro_rules! impl_segment_for_number {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Segment for $ty {
                fn render(&self) -> Cow<'_, str> {
                    Cow::Owned(self.to_string())
                }

                fn parse(s: &str) -> Result<Self, TopikError> {
                    s.parse().map_err(|_| TopikError::ParseError {
                        topic: s.to_string(),
                        reason: format!("expected {}, got '{}'", stringify!($ty), s),
                    })
                }
            }
        )+
    };
}

impl_segment_for_number!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

impl Segment for String {
    fn render(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }

    fn parse(s: &str) -> Result<Self, TopikError> {
        Ok(s.to_string())
    }
}

/// Defines the string representation for a boolean type.
///
/// # Example
///
/// ```
/// use topik_core::{BoolRepr, BoolSegment, Segment};
///
/// // a legacy system using "active" / "inactive"
/// struct ActiveInactive;
///
/// impl BoolRepr for ActiveInactive {
///     const TRUE: &'static str = "active";
///     const FALSE: &'static str = "inactive";
/// }
///
/// let flag = BoolSegment::<ActiveInactive>::from(true);
/// assert_eq!(flag.render(), "active");
///
/// let parsed = BoolSegment::<ActiveInactive>::parse("inactive").unwrap();
/// assert!(!parsed.as_bool());
/// ```
pub trait BoolRepr {
    const TRUE: &'static str;
    const FALSE: &'static str;
}

/// A boolean topic segment typed as a [`BoolRepr`].
///
/// Default: [`TrueFalse`]
pub struct BoolSegment<R: BoolRepr>(bool, PhantomData<R>);

impl<R: BoolRepr> BoolSegment<R> {
    pub fn as_bool(&self) -> bool {
        self.0
    }
}

impl<R: BoolRepr> From<bool> for BoolSegment<R> {
    fn from(b: bool) -> Self {
        BoolSegment(b, PhantomData)
    }
}

impl<R: BoolRepr> std::fmt::Debug for BoolSegment<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoolSegment").field(&self.0).finish()
    }
}

impl<R: BoolRepr> Clone for BoolSegment<R> {
    fn clone(&self) -> Self {
        BoolSegment(self.0, PhantomData)
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
            Ok(BoolSegment(true, PhantomData))
        } else if s == R::FALSE {
            Ok(BoolSegment(false, PhantomData))
        } else {
            Err(TopikError::ParseError {
                topic: s.to_string(),
                reason: format!("expected '{}' or '{}', got '{}'", R::TRUE, R::FALSE, s),
            })
        }
    }
}

/// Standard boolean representation: `"true"` / `"false"`.
pub struct TrueFalse;
impl BoolRepr for TrueFalse {
    const TRUE: &'static str = "true";
    const FALSE: &'static str = "false";
}

/// Binary boolean representation: `"1"` / `"0"`.
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
pub struct OnOff;
impl BoolRepr for OnOff {
    const TRUE: &'static str = "on";
    const FALSE: &'static str = "off";
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
