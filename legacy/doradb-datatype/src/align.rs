use crate::RuntimeType;
use std::cmp::Ordering;

pub trait AlignPartialOrd<Rhs: ?Sized = Self> {
    fn align_partial_cmp(&self, other: &Rhs) -> Option<Ordering>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlignType {
    Identical,
    F64,
    Bytes,
    Datetime,
}

impl AlignType {
    /// Align two runtime types to single type for comparison.
    ///
    /// All numeric types are aligned to f64, which can cover most cases
    /// but has some issues on precison, especially for i64, u64, decimal.
    /// This is the default behavior of MySQL and we choose to follow it.
    #[inline]
    pub fn cmp_align(this: RuntimeType, that: RuntimeType) -> Option<Self> {
        use RuntimeType::*;
        if this == that {
            return Some(AlignType::Identical);
        }
        let res = match this {
            I32 | U32 | I64 | U64 | F32 | F64 | Decimal | Bool => match that {
                Interval | Null | Unknown => return None,
                _ => AlignType::F64,
            },
            String | Bytes => match that {
                I32 | U32 | I64 | U64 | F32 | F64 | Decimal | Bool => AlignType::F64,
                String | Bytes | Time => AlignType::Bytes,
                Date | Datetime => AlignType::Datetime,
                Interval | Null | Unknown => return None,
            },
            Date => match that {
                I32 | U32 | I64 | U64 | F32 | F64 | Decimal | Bool => AlignType::F64,
                String | Bytes => AlignType::Datetime,
                Date | Datetime => AlignType::Datetime,
                Time => AlignType::Bytes,
                Interval | Null | Unknown => return None,
            },
            Time => match that {
                I32 | U32 | I64 | U64 | F32 | F64 | Decimal | Bool => AlignType::F64,
                String | Bytes | Date | Datetime | Time => AlignType::Bytes,
                Interval | Null | Unknown => return None,
            },
            Datetime => match that {
                I32 | U32 | I64 | U64 | F32 | F64 | Decimal | Bool => AlignType::F64,
                String | Bytes | Date | Datetime => AlignType::Datetime,
                Time => AlignType::Bytes,
                Interval | Null | Unknown => return None,
            },
            Interval | Null | Unknown => return None,
        };
        Some(res)
    }
}
