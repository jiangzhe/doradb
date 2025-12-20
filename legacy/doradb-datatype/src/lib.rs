pub mod align;
pub mod error;
pub mod konst;
pub mod memcmp;

pub use align::*;
pub use konst::*;

pub use error::{Error, Result};
pub use fxd::{Error as DecimalError, FixedDecimal as Decimal};
pub use time::format_description::{self, FormatItem};
pub use time::PrimitiveDateTime as Datetime;
pub use time::{error::Parse as DatetimeParseError, Error as DatetimeError};
pub use time::{Date, Time};

use static_init::dynamic;
use std::borrow::Cow;
use std::io;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PreciseType {
    /// code=0
    /// Most expressions are initialized with unknown type.
    /// After type inference, the precise type will be assigned.
    #[default]
    Unknown,
    /// code=1
    /// Only constant null will have null type.
    Null,
    /// code=2
    /// Integer type.
    /// First argument is byte number.
    /// Second argument is unsigned flag.
    Int(u8, bool),
    /// code=3
    /// Fixed point decimal.
    /// Precision and fraction are specified.
    Decimal(u8, u8),
    /// code=4
    /// Float type.
    /// Bytes specified, only 4 and 8 are valid.
    Float(u8),
    /// code=5
    Bool,
    /// code=6
    Date,
    /// code=7
    Time(u8),
    /// code=8
    Datetime(u8),
    /// code=9
    Interval,
    /// code=10
    /// Note: Char and Varchar length is not same as bytes.
    /// It depends on collation, e.g. commonly used utf8mb4
    /// uses at most 4 bytes to store single character.
    Char(u16, Collation),
    /// code=11
    Varchar(u16, Collation),
    /// code=12
    /// Compound type, currently not support.
    Compound,
}

impl PreciseType {
    #[inline]
    pub const fn null() -> Self {
        PreciseType::Null
    }

    #[inline]
    pub const fn bool() -> Self {
        PreciseType::Bool
    }

    #[inline]
    pub fn int(bytes: u8, unsigned: bool) -> Self {
        PreciseType::Int(bytes, unsigned)
    }

    #[inline]
    pub const fn i32() -> Self {
        PreciseType::Int(4, false)
    }

    #[inline]
    pub const fn u32() -> Self {
        PreciseType::Int(4, true)
    }

    #[inline]
    pub const fn i64() -> Self {
        PreciseType::Int(8, false)
    }

    #[inline]
    pub const fn u64() -> Self {
        PreciseType::Int(8, true)
    }

    #[inline]
    pub const fn f32() -> Self {
        PreciseType::Float(4)
    }

    #[inline]
    pub const fn f64() -> Self {
        PreciseType::Float(8)
    }

    #[inline]
    pub fn decimal(max_prec: u8, max_frac: u8) -> Self {
        PreciseType::Decimal(max_prec, max_frac)
    }

    #[inline]
    pub fn char(len: u16, collation: Collation) -> Self {
        PreciseType::Char(len, collation)
    }

    #[inline]
    pub fn varchar(max_len: u16, collation: Collation) -> Self {
        PreciseType::Varchar(max_len, collation)
    }

    #[inline]
    pub fn ascii(len: u16) -> Self {
        PreciseType::Char(len, Collation::Ascii)
    }

    #[inline]
    pub fn var_ascii(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Ascii)
    }

    #[inline]
    pub fn utf8(len: u16) -> Self {
        PreciseType::Char(len, Collation::Utf8mb4)
    }

    #[inline]
    pub fn var_utf8(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Utf8mb4)
    }

    #[inline]
    pub fn bytes(len: u16) -> Self {
        PreciseType::Char(len, Collation::Binary)
    }

    #[inline]
    pub fn var_bytes(max_len: u16) -> Self {
        PreciseType::Varchar(max_len, Collation::Binary)
    }

    #[inline]
    pub const fn date() -> Self {
        PreciseType::Date
    }

    #[inline]
    pub fn time(frac: u8) -> Self {
        PreciseType::Time(frac)
    }

    #[inline]
    pub fn datetime(frac: u8) -> Self {
        PreciseType::Datetime(frac)
    }

    #[inline]
    pub const fn interval() -> Self {
        PreciseType::Interval
    }

    #[inline]
    pub fn runtime_ty(&self) -> RuntimeType {
        match self {
            PreciseType::Int(n, false) => match n {
                1..=4 => RuntimeType::I32,
                8 => RuntimeType::I64,
                _ => unreachable!(),
            },
            PreciseType::Int(n, true) => match n {
                1..=4 => RuntimeType::U32,
                8 => RuntimeType::U64,
                _ => unreachable!(),
            },
            PreciseType::Float(n) => match n {
                4 => RuntimeType::F32,
                8 => RuntimeType::F64,
                _ => unreachable!(),
            },
            PreciseType::Decimal { .. } => RuntimeType::Decimal,
            PreciseType::Bool => RuntimeType::Bool,
            PreciseType::Char(_, Collation::Binary)
            | PreciseType::Varchar(_, Collation::Binary) => RuntimeType::Bytes,
            PreciseType::Char(..) | PreciseType::Varchar(..) => RuntimeType::String,
            PreciseType::Date => RuntimeType::Date,
            PreciseType::Time(..) => RuntimeType::Time,
            PreciseType::Datetime(..) => RuntimeType::Datetime,
            PreciseType::Interval => RuntimeType::Interval,
            PreciseType::Null => RuntimeType::Null,
            PreciseType::Unknown | PreciseType::Compound => RuntimeType::Unknown,
        }
    }

    #[inline]
    pub fn is_unknown(&self) -> bool {
        matches!(self, PreciseType::Unknown)
    }

    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, PreciseType::Bool)
    }

    #[inline]
    pub fn to_lower(&self) -> Cow<'_, str> {
        match self {
            PreciseType::Unknown => Cow::Borrowed("unknown"),
            PreciseType::Null => Cow::Borrowed("null"),
            PreciseType::Int(bytes, unsigned) => {
                if *unsigned {
                    Cow::Owned(format!("uint({bytes})"))
                } else {
                    Cow::Owned(format!("int({bytes})"))
                }
            }
            PreciseType::Decimal(max_prec, max_frac) => {
                Cow::Owned(format!("decimal({max_prec}, {max_frac})"))
            }
            PreciseType::Float(bytes) => Cow::Owned(format!("float({bytes})")),
            PreciseType::Bool => Cow::Borrowed("bool"),
            PreciseType::Date => Cow::Borrowed("date"),
            PreciseType::Time(frac) => Cow::Owned(format!("time({frac})")),
            PreciseType::Datetime(frac) => Cow::Owned(format!("datetime({frac})")),
            PreciseType::Interval => Cow::Borrowed("interval"),
            PreciseType::Char(n, c) => Cow::Owned(format!("char({n}, {c:?})")),
            PreciseType::Varchar(n, c) => Cow::Owned(format!("varchar({n}, {c:?})")),
            PreciseType::Compound => Cow::Borrowed("compound"),
        }
    }

    /// Returns value length.
    #[inline]
    pub fn val_len(&self) -> Option<usize> {
        match self {
            PreciseType::Unknown => None,
            PreciseType::Null => None,
            PreciseType::Int(bytes, _) => Some(*bytes as usize),
            PreciseType::Decimal(..) => {
                todo!("decimal not supported")
            }
            PreciseType::Float(bytes) => Some(*bytes as usize),
            PreciseType::Bool => todo!("bool length"), // todo: bool does not have own length, but be compacted as bitmap
            PreciseType::Date => Some(4),
            PreciseType::Time(_) | PreciseType::Datetime(_) => Some(12),
            PreciseType::Interval => None,
            PreciseType::Char(..) | PreciseType::Varchar(..) => None,
            PreciseType::Compound => None,
        }
    }

    /// Write the type in byte format.
    #[inline]
    pub fn write_to<W: io::Write>(self, writer: &mut W) -> Result<usize> {
        let buf: [u8; 4] = self.into();
        writer.write_all(&buf).map_err(|_| Error::IOError)?;
        Ok(4)
    }
}

impl TryFrom<&[u8]> for PreciseType {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        if src.len() < 4 {
            return Err(Error::InvalidFormat);
        }
        let res = match src[0] {
            0 => PreciseType::Unknown,
            1 => PreciseType::Null,
            2 => {
                let bytes = src[1];
                let neg = match src[2] {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::InvalidFormat),
                };
                PreciseType::Int(bytes, neg)
            }
            3 => {
                let prec = src[1];
                let frac = src[2];
                PreciseType::Decimal(prec, frac)
            }
            4 => {
                let bytes = src[1];
                PreciseType::Float(bytes)
            }
            5 => PreciseType::Bool,
            6 => PreciseType::Date,
            7 => {
                let frac = src[1];
                PreciseType::Time(frac)
            }
            8 => {
                let frac = src[1];
                PreciseType::Datetime(frac)
            }
            9 => PreciseType::Interval,
            10 => {
                let len = src[1] as u16 + ((src[2] as u16) << 8);
                let collation = Collation::try_from(src[3])?;
                PreciseType::Char(len, collation)
            }
            11 => {
                let len = src[1] as u16 + ((src[2] as u16) << 8);
                let collation = Collation::try_from(src[3])?;
                PreciseType::Varchar(len, collation)
            }
            12 => PreciseType::Compound,
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

impl From<PreciseType> for u32 {
    #[inline]
    fn from(src: PreciseType) -> Self {
        let buf: [u8; 4] = src.into();
        u32::from_le_bytes(buf)
    }
}

impl From<PreciseType> for [u8; 4] {
    #[inline]
    fn from(src: PreciseType) -> Self {
        let mut tgt = [0u8; 4];
        match src {
            PreciseType::Unknown => (),
            PreciseType::Null => tgt[0] = 1,
            PreciseType::Int(bytes, neg) => {
                tgt[0] = 2;
                tgt[1] = bytes;
                tgt[2] = u8::from(neg);
            }
            PreciseType::Decimal(prec, frac) => {
                tgt[0] = 3;
                tgt[1] = prec;
                tgt[2] = frac;
            }
            PreciseType::Float(bytes) => {
                tgt[0] = 4;
                tgt[1] = bytes;
            }
            PreciseType::Bool => tgt[0] = 5,
            PreciseType::Date => tgt[0] = 6,
            PreciseType::Time(frac) => {
                tgt[0] = 7;
                tgt[1] = frac;
            }
            PreciseType::Datetime(frac) => {
                tgt[0] = 8;
                tgt[1] = frac;
            }
            PreciseType::Interval => tgt[0] = 9,
            PreciseType::Char(len, collation) => {
                tgt[0] = 10;
                // little endian
                tgt[1] = (len & 0xff) as u8;
                tgt[2] = (len >> 8) as u8;
                tgt[3] = collation as u8;
            }
            PreciseType::Varchar(len, collation) => {
                tgt[0] = 11;
                // little endian
                tgt[1] = (len & 0xff) as u8;
                tgt[2] = (len >> 8) as u8;
                tgt[3] = collation as u8;
            }
            PreciseType::Compound => tgt[0] = 12,
        }
        tgt
    }
}

impl From<[u8; 4]> for PreciseType {
    #[inline]
    fn from(src: [u8; 4]) -> Self {
        PreciseType::try_from(&src[..]).unwrap()
    }
}

impl From<u32> for PreciseType {
    #[inline]
    fn from(src: u32) -> Self {
        let buf: [u8; 4] = src.to_le_bytes();
        PreciseType::try_from(&buf[..]).unwrap()
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum RuntimeType {
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Decimal,
    Bool,
    String,
    Bytes,
    Date,
    Time,
    Datetime,
    Interval,
    Null,
    Unknown,
}

pub trait StaticTyped {
    /// Returns static precise type
    fn static_pty() -> PreciseType;
}

macro_rules! impl_static_typed {
    ($ty:ty, $expr:expr) => {
        impl StaticTyped for $ty {
            #[inline]
            fn static_pty() -> PreciseType {
                $expr
            }
        }
    };
}

impl_static_typed!(i32, PreciseType::i32());
impl_static_typed!(i64, PreciseType::i64());
impl_static_typed!(u64, PreciseType::u64());

pub trait Typed {
    /// Returns precise type
    fn pty(&self) -> PreciseType;
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Microsecond = 1,
    Second = 2,
    Minute = 3,
    Hour = 4,
    Day = 5,
    Week = 6,
    Month = 7,
    Quarter = 8,
    Year = 9,
}

impl TimeUnit {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            TimeUnit::Microsecond => "microsecond",
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Week => "week",
            TimeUnit::Month => "month",
            TimeUnit::Quarter => "quarter",
            TimeUnit::Year => "year",
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Collation {
    Ascii = 1,
    Utf8mb4 = 2,
    Binary = 3,
}

impl TryFrom<u8> for Collation {
    type Error = Error;
    #[inline]
    fn try_from(src: u8) -> Result<Self> {
        let res = match src {
            1 => Collation::Ascii,
            2 => Collation::Utf8mb4,
            3 => Collation::Binary,
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    pub value: i32,
    pub unit: TimeUnit,
}

#[dynamic]
pub static DEFAULT_DATE_FORMAT: Vec<FormatItem<'static>> =
    format_description::parse("[year]-[month]-[day]").unwrap();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_precise_type() {
        use std::mem::size_of;
        println!("size of PreciseType is {}", size_of::<PreciseType>());
    }

    #[test]
    fn test_serde_precise_type() {
        for (ty, bytes) in vec![
            (PreciseType::Unknown, [0u8; 4]),
            (PreciseType::Null, [1u8, 0, 0, 0]),
            (PreciseType::Int(4, false), [2u8, 4, 0, 0]),
            (PreciseType::Int(4, true), [2u8, 4, 1, 0]),
            (PreciseType::Int(8, false), [2u8, 8, 0, 0]),
            (PreciseType::Decimal(10, 0), [3u8, 10, 0, 0]),
            (PreciseType::Decimal(18, 2), [3u8, 18, 2, 0]),
            (PreciseType::Float(4), [4u8, 4, 0, 0]),
            (PreciseType::Float(8), [4u8, 8, 0, 0]),
            (PreciseType::Bool, [5u8, 0, 0, 0]),
            (PreciseType::Date, [6u8, 0, 0, 0]),
            (PreciseType::Time(0), [7u8, 0, 0, 0]),
            (PreciseType::Time(6), [7u8, 6, 0, 0]),
            (PreciseType::Datetime(0), [8u8, 0, 0, 0]),
            (PreciseType::Datetime(3), [8u8, 3, 0, 0]),
            (PreciseType::Interval, [9u8, 0, 0, 0]),
            (PreciseType::Char(1, Collation::Ascii), [10u8, 1, 0, 1]),
            (PreciseType::Char(1024, Collation::Utf8mb4), [10u8, 0, 4, 2]),
            (
                PreciseType::Varchar(4096, Collation::Binary),
                [11u8, 0, 16, 3],
            ),
            (PreciseType::Varchar(16, Collation::Ascii), [11u8, 16, 0, 1]),
            (PreciseType::Compound, [12u8, 0, 0, 0]),
        ] {
            let new_bs = <[u8; 4]>::from(ty);
            assert_eq!(bytes, new_bs);
            let new_ty = PreciseType::try_from(&bytes[..]).unwrap();
            assert_eq!(ty, new_ty);
        }
    }
}
