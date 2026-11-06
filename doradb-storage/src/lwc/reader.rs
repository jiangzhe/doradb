//! Checked point readers sharing structural parsing with fully validated prepared decoders.

use super::codec::{IntegerMinimum, LwcLayout, ValidatedLwcLayout, ValueLayout};
use super::{LwcBytes, LwcPrimitiveData};
use crate::compression::*;
use crate::error::DataIntegrityResult;
use crate::layout;
use crate::primitive::Primitive;
use crate::value::{Val, ValKind};
use std::marker::PhantomData;
use std::mem;

/// Owned decoder metadata cached beside a readonly buffer guard.
pub(crate) type PreparedLwcData = ValidatedLwcLayout;

/// Borrowed scalar readers that check byte-offset pairs when accessing values.
pub(crate) enum LwcData<'a> {
    Primitive(LwcPrimitive<'a>),
    Bytes(LwcBytes<'a>),
}

impl<'a> LwcData<'a> {
    /// Validates payload framing in constant time; byte offsets are checked on access.
    pub(crate) fn from_bytes(kind: ValKind, input: &'a [u8]) -> DataIntegrityResult<Self> {
        let parsed = LwcLayout::from_bytes(kind, input)?;
        Ok(match parsed.encoding {
            ValueLayout::FlatVarByte {
                offsets_start,
                data_start,
            } => Self::Bytes(LwcBytes {
                // Validation establishes exactly N+1 complete four-byte offsets.
                offsets: layout::slice_from_bytes(&input[offsets_start..data_start]),
                data: &input[data_start..],
            }),
            _ => Self::Primitive(LwcPrimitive::from_layout(&parsed, input)),
        })
    }

    /// Returns the validated physical element count.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Primitive(values) => values.len(),
            Self::Bytes(values) => values.len(),
        }
    }

    /// Decodes one physical slot; its column owns logical null handling.
    /// Returns None for an out-of-range index or invalid offsets of the selected value.
    #[inline]
    pub(crate) fn value(&self, idx: usize) -> Option<Val> {
        match self {
            Self::Primitive(values) => values.value(idx),
            Self::Bytes(values) => values.value(idx).map(Val::VarByte),
        }
    }
}

/// Borrowed flat values, decoded without alignment assumptions.
pub(crate) struct FlatReader<'a, T> {
    bytes: &'a [u8],
    marker: PhantomData<T>,
}

impl<'a, T: Primitive> FlatReader<'a, T> {
    #[inline]
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            marker: PhantomData,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes.len() / mem::size_of::<T>()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<T> {
        if idx >= self.len() {
            return None;
        }
        let start = idx * mem::size_of::<T>();
        Some(T::read_le(&self.bytes[start..start + mem::size_of::<T>()]))
    }
}

// A single runtime discriminant avoids a second type/codec dispatch per value.
// The codec/type matrix below generates the forwarding code; the packing
// kernels remain generic over their native scalar type.
macro_rules! packed_reader {
    ($kernel:ident, subbyte, $len:expr, $min:expr, $data:expr) => {
        $kernel {
            len: $len,
            min: $min,
            data: $data,
        }
    };
    ($kernel:ident, byte, $len:expr, $min:expr, $data:expr) => {
        $kernel {
            min: $min,
            data: $data,
        }
    };
    ($kernel:ident, units, $len:expr, $min:expr, $data:expr) => {
        $kernel {
            min: $min,
            data: layout::slice_from_bytes($data),
        }
    };
}

macro_rules! primitive_readers {
    (integers { $( $kind:ident: $ty:ty, $category:ident, $flat:ident {
        $($bits:literal: $packed:ident, $kernel:ident, $units:ident;)*
    } )* }
    floats { $( $float:ident: $ft:ty; )* }) => {
        /// Borrowed primitive reader with generated scalar/codec dispatch.
        pub(crate) enum LwcPrimitive<'a> {
            $( $flat(FlatReader<'a, $ty>), $($packed($kernel<'a, $ty>),)* )*
            $( $float(FlatReader<'a, $ft>), )*
        }

        impl<'a> LwcPrimitive<'a> {
            fn from_layout(parsed: &LwcLayout, input: &'a [u8]) -> Self {
                match (parsed.kind, parsed.encoding) {
                    $((ValKind::$kind, ValueLayout::FlatPrimitive { data_start, .. }) => Self::$flat(FlatReader::new(&input[data_start..])),)*
                    $((ValKind::$kind, ValueLayout::ForBitpacking { data_start, n_bits, min: IntegerMinimum::$category(min) }) => {
                        let min = min as $ty;
                        let data = &input[data_start..];
                        match n_bits {
                            $($bits => Self::$packed(packed_reader!($kernel, $units, parsed.len, min, data)),)*
                            _ => unreachable!("validated LWC packing width agrees with its scalar kind"),
                        }
                    },)*
                    $((ValKind::$float, ValueLayout::FlatPrimitive { data_start, .. }) => Self::$float(FlatReader::new(&input[data_start..])),)*
                    _ => unreachable!("validated LWC primitive layout agrees with its scalar kind"),
                }
            }

            #[inline]
            fn len(&self) -> usize {
                match self {
                    $(Self::$flat(values) => values.len(), $(Self::$packed(values) => values.len(),)*)*
                    $(Self::$float(values) => values.len(),)*
                }
            }

            #[inline]
            fn value(&self, idx: usize) -> Option<Val> {
                match self {
                    $(Self::$flat(values) => values.value(idx).map(Val::from),
                      $(Self::$packed(values) => values.value(idx).map(Val::from),)*)*
                    $(Self::$float(values) => values.value(idx).map(Val::from),)*
                }
            }
        }
    };
}

primitive_readers! {
    integers {
        I8: i8, Signed, FlatI8 {
            1: B1I8, ForBitpacking1, subbyte;
            2: B2I8, ForBitpacking2, subbyte;
            4: B4I8, ForBitpacking4, subbyte;
        }
        U8: u8, Unsigned, FlatU8 {
            1: B1U8, ForBitpacking1, subbyte;
            2: B2U8, ForBitpacking2, subbyte;
            4: B4U8, ForBitpacking4, subbyte;
        }
        I16: i16, Signed, FlatI16 {
            1: B1I16, ForBitpacking1, subbyte;
            2: B2I16, ForBitpacking2, subbyte;
            4: B4I16, ForBitpacking4, subbyte;
            8: B8I16, ForBitpacking8, byte;
        }
        U16: u16, Unsigned, FlatU16 {
            1: B1U16, ForBitpacking1, subbyte;
            2: B2U16, ForBitpacking2, subbyte;
            4: B4U16, ForBitpacking4, subbyte;
            8: B8U16, ForBitpacking8, byte;
        }
        I32: i32, Signed, FlatI32 {
            1: B1I32, ForBitpacking1, subbyte;
            2: B2I32, ForBitpacking2, subbyte;
            4: B4I32, ForBitpacking4, subbyte;
            8: B8I32, ForBitpacking8, byte;
            16: B16I32, ForBitpacking16, units;
        }
        U32: u32, Unsigned, FlatU32 {
            1: B1U32, ForBitpacking1, subbyte;
            2: B2U32, ForBitpacking2, subbyte;
            4: B4U32, ForBitpacking4, subbyte;
            8: B8U32, ForBitpacking8, byte;
            16: B16U32, ForBitpacking16, units;
        }
        I64: i64, Signed, FlatI64 {
            1: B1I64, ForBitpacking1, subbyte;
            2: B2I64, ForBitpacking2, subbyte;
            4: B4I64, ForBitpacking4, subbyte;
            8: B8I64, ForBitpacking8, byte;
            16: B16I64, ForBitpacking16, units;
            32: B32I64, ForBitpacking32, units;
        }
        U64: u64, Unsigned, FlatU64 {
            1: B1U64, ForBitpacking1, subbyte;
            2: B2U64, ForBitpacking2, subbyte;
            4: B4U64, ForBitpacking4, subbyte;
            8: B8U64, ForBitpacking8, byte;
            16: B16U64, ForBitpacking16, units;
            32: B32U64, ForBitpacking32, units;
        }
    }
    floats { F32: f32; F64: f64; }
}
