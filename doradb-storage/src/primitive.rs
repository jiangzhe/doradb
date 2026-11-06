//! Fixed-width scalar representations shared by row pages and column codecs.

use crate::serde::Serde;
use crate::value::{Val, ValKind};
use std::ops::Range;
use zerocopy::byteorder::little_endian::{
    F32 as LeF32, F64 as LeF64, I16 as LeI16, I32 as LeI32, I64 as LeI64, U16 as LeU16,
    U32 as LeU32, U64 as LeU64,
};
use zerocopy::{Immutable, IntoBytes};

// The physical type family. Callbacks generate complete items or matches;
// algorithms and variable-byte policies stay in ordinary Rust functions.
macro_rules! primitive_types {
    ($callback:ident) => {
        $callback! {
            integers {
                I8: i8, Signed, i64;
                U8: u8, Unsigned, u64;
                I16: i16, Signed, i64;
                U16: u16, Unsigned, u64;
                I32: i32, Signed, i64;
                U32: u32, Unsigned, u64;
                I64: i64, Signed, i64;
                U64: u64, Unsigned, u64;
            }
            floats { F32: f32; F64: f64; }
        }
    };
}
pub(crate) use primitive_types;

/// Native fixed-width scalar with an exact little-endian wire representation.
pub(crate) trait Primitive: Copy + IntoBytes + Immutable + Into<Val> {
    /// Reads exactly one scalar from its validated little-endian bytes.
    fn read_le(bytes: &[u8]) -> Self;

    /// Writes one scalar, retaining all floating-point payload bits.
    fn write_le<S: Serde + ?Sized>(self, output: &mut S, start: usize) -> usize;
}

/// Conversion from a borrowed row-page scalar to its native representation.
pub(crate) trait PageScalar: Primitive {
    /// Endian-aware scalar representation used in row pages.
    type Stored: Copy;

    /// Loads one page value without changing its bits.
    fn load(value: Self::Stored) -> Self;

    /// Appends a contiguous page slice, using bulk copy for native byte types.
    #[inline]
    fn append_slice(target: &mut Vec<Self>, source: &[Self::Stored]) {
        target.extend(source.iter().copied().map(Self::load));
    }
}

/// Copies selected source runs into a native vector without an intermediate buffer.
#[inline]
pub(crate) fn append_page_runs<T: PageScalar>(
    target: &mut Vec<T>,
    source: &[T::Stored],
    runs: impl Iterator<Item = Range<usize>>,
) {
    for range in runs {
        T::append_slice(target, &source[range]);
    }
}

macro_rules! define_primitives {
    (integers { $($int:ident: $it:ty, $category:ident, $wide:ty;)* }
     floats { $($float:ident: $ft:ty;)* }) => {
        /// Native fixed-width column storage for LWC staging.
        pub(crate) enum PrimitiveBuffer {
            $($int(Vec<$it>),)*
            $($float(Vec<$ft>),)*
        }

        impl PrimitiveBuffer {
            /// Creates a native vector for a fixed-width kind.
            #[inline]
            pub(crate) fn new(kind: ValKind) -> Self {
                match kind {
                    $(ValKind::$int => Self::$int(Vec::new()),)*
                    $(ValKind::$float => Self::$float(Vec::new()),)*
                    ValKind::VarByte => unreachable!("primitive buffer requires a fixed-width kind"),
                }
            }

            /// Returns the number of physical scalar slots.
            #[inline]
            pub(crate) fn len(&self) -> usize {
                match self {
                    $(Self::$int(values) => values.len(),)*
                    $(Self::$float(values) => values.len(),)*
                }
            }

            /// Appends a validated scalar or a zero placeholder for a logical null.
            #[inline]
            pub(crate) fn append(&mut self, value: &Val) {
                match (self, value) {
                    $((Self::$int(values), Val::$int(value)) => values.push(*value),
                      (Self::$int(values), Val::Null) => values.push(0),)*
                    $((Self::$float(values), Val::$float(value)) => values.push(value.0),
                      (Self::$float(values), Val::Null) => values.push(0.0),)*
                    _ => unreachable!("primitive buffer and validated scalar kinds must agree"),
                }
            }
        }

        /// Decodes a fixed-width value after its containing layout is validated.
        #[inline]
        pub(crate) fn decode_primitive(kind: ValKind, bytes: &[u8]) -> Val {
            match kind {
                $(ValKind::$int => <$it>::read_le(bytes).into(),)*
                $(ValKind::$float => <$ft>::read_le(bytes).into(),)*
                ValKind::VarByte => unreachable!("variable bytes require their container layout"),
            }
        }

        $(impl_primitive!($it);)*
        $(impl_primitive!($ft);)*
    };
}

macro_rules! impl_primitive {
    ($ty:ty) => {
        impl Primitive for $ty {
            #[inline]
            fn read_le(bytes: &[u8]) -> Self {
                // The caller has validated the fixed-width scalar layout.
                Self::from_le_bytes(bytes.try_into().expect("validated primitive byte width"))
            }

            #[inline]
            fn write_le<S: Serde + ?Sized>(self, output: &mut S, start: usize) -> usize {
                output.ser_byte_slice(start, &self.to_le_bytes())
            }
        }
    };
}

primitive_types!(define_primitives);

macro_rules! impl_page_scalar {
    ($($ty:ty => $stored:ty),* $(,)?) => {
        $(impl PageScalar for $ty {
            type Stored = $stored;

            #[inline]
            fn load(value: Self::Stored) -> Self {
                value.get()
            }
        })*
    };
}

impl_page_scalar!(i16 => LeI16, u16 => LeU16, i32 => LeI32, u32 => LeU32,
    i64 => LeI64, u64 => LeU64, f32 => LeF32, f64 => LeF64);

macro_rules! impl_native_page_scalar {
    ($($ty:ty),*) => {
        $(impl PageScalar for $ty {
            type Stored = Self;

            #[inline]
            fn load(value: Self) -> Self {
                value
            }

            #[inline]
            fn append_slice(target: &mut Vec<Self>, source: &[Self]) {
                target.extend_from_slice(source);
            }
        })*
    };
}

impl_native_page_scalar!(i8, u8);
