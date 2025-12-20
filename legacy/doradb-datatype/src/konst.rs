use crate::align::{AlignPartialOrd, AlignType};
use crate::Collation;
use crate::{Date, Datetime, Decimal, Interval, PreciseType, RuntimeType, Time, Typed};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum Const {
    #[default]
    Null,
    I64(i64),
    U64(u64),
    F64(ValidF64),
    Decimal(Decimal),
    Date(Date),
    Time(Time),
    Datetime(Datetime),
    Interval(Interval),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
    Bool(bool),
}

impl Typed for Const {
    #[inline]
    fn pty(&self) -> PreciseType {
        use Const::*;
        match self {
            I64(_) => PreciseType::i64(),
            U64(_) => PreciseType::u64(),
            F64(_) => PreciseType::f64(),
            Decimal(dec) => PreciseType::Decimal((dec.intg() + dec.frac()) as u8, dec.frac() as u8),
            Date(_) => PreciseType::date(),
            Time(tm) => {
                let micros = tm.microsecond();
                if micros == 0 {
                    PreciseType::Time(0)
                } else {
                    let millis = tm.millisecond();
                    if millis as u32 * 1000 == micros {
                        PreciseType::Time(3)
                    } else {
                        PreciseType::Time(6)
                    }
                }
            }
            Datetime(dm) => {
                let micros = dm.microsecond();
                if micros == 0 {
                    PreciseType::Datetime(0)
                } else {
                    let millis = dm.millisecond();
                    if millis as u32 * 1000 == micros {
                        PreciseType::Datetime(3)
                    } else {
                        PreciseType::Datetime(6)
                    }
                }
            }
            Interval(_) => PreciseType::interval(),
            String(s) => {
                if s.is_ascii() {
                    PreciseType::char(s.chars().count() as u16, Collation::Ascii)
                } else {
                    PreciseType::char(s.chars().count() as u16, Collation::Utf8mb4)
                }
            }
            Bytes(b) => PreciseType::bytes(b.len() as u16),
            Bool(_) => PreciseType::bool(),
            Null => PreciseType::null(),
        }
    }
}

impl Const {
    #[inline]
    pub fn new_f64(v: f64) -> Option<Self> {
        ValidF64::new(v).map(Const::F64)
    }

    #[inline]
    pub fn is_zero(&self) -> Option<bool> {
        let res = match self {
            Const::I64(i) => *i == 0,
            Const::U64(u) => *u == 0,
            Const::F64(f) => f.value() == 0.0,
            Const::Decimal(d) => d.is_zero(),
            Const::Bool(b) => !*b, // treat false as zero
            Const::Null => return None,
            // date and datetime cannot be zero
            Const::Date(_) | Const::Datetime(_) => false,
            Const::Time(tm) => {
                tm.hour() == 0 && tm.minute() == 0 && tm.second() == 0 && tm.nanosecond() == 0
            }
            Const::Interval(Interval { value, .. }) => *value == 0,
            other => return other.cast_to_f64().map(|v| v == 0.0),
        };
        Some(res)
    }

    #[inline]
    pub fn runtime_ty(&self) -> RuntimeType {
        use Const::*;
        match self {
            I64(_) => RuntimeType::I64,
            U64(_) => RuntimeType::U64,
            F64(_) => RuntimeType::F64,
            Decimal(_) => RuntimeType::Decimal,
            Date(_) => RuntimeType::Date,
            Time(_) => RuntimeType::Time,
            Datetime(_) => RuntimeType::Datetime,
            Interval(_) => RuntimeType::Interval,
            String(_) => RuntimeType::String,
            Bytes(_) => RuntimeType::Bytes,
            Bool(_) => RuntimeType::Bool,
            Null => RuntimeType::Null,
        }
    }

    #[inline]
    pub fn cast_to_f64(&self) -> Option<f64> {
        let res = match self {
            Const::I64(v) => *v as f64,
            Const::U64(v) => *v as f64,
            Const::F64(v) => v.value(),
            Const::Decimal(v) => return v.to_string(-1).parse().ok(),
            Const::Bool(v) => {
                if *v {
                    1.0
                } else {
                    0.0
                }
            }
            Const::Date(_) | Const::Time(_) | Const::Datetime(_) | Const::Interval(_) => todo!(),
            // In MySQL, string is parsed to f64 in "best effort",
            // here we just truncate it to zero if it's not complete valid numerical text.
            Const::String(v) => return v.parse().ok().or(Some(0.0)),
            Const::Bytes(v) => {
                return std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse().ok().or(Some(0.0)))
            }
            Const::Null => return None,
        };
        Some(res)
    }

    #[inline]
    pub fn cast_to_bytes(&self) -> Option<Arc<[u8]>> {
        let res: Arc<[u8]> = match self {
            Const::I64(v) => Arc::from(v.to_string().into_bytes()),
            Const::U64(v) => Arc::from(v.to_string().into_bytes()),
            Const::F64(v) => Arc::from(v.value().to_string().into_bytes()),
            Const::Decimal(v) => Arc::from(v.to_string(-1).into_bytes()),
            Const::Bool(v) => {
                if *v {
                    Arc::from([b'1'])
                } else {
                    Arc::from([b'0'])
                }
            }
            Const::Date(_) | Const::Time(_) | Const::Datetime(_) | Const::Interval(_) => todo!(),
            Const::String(v) => Arc::from(v.as_bytes()),
            Const::Bytes(v) => Arc::clone(v),
            Const::Null => return None,
        };
        Some(res)
    }

    #[inline]
    pub fn cast_to_datetime(&self) -> Option<Datetime> {
        todo!()
    }
}

impl AlignPartialOrd for Const {
    fn align_partial_cmp(&self, other: &Self) -> Option<Ordering> {
        AlignType::cmp_align(self.runtime_ty(), other.runtime_ty()).and_then(|ty| match ty {
            AlignType::F64 => match (self.cast_to_f64(), other.cast_to_f64()) {
                (Some(v0), Some(v1)) => v0.partial_cmp(&v1),
                _ => None,
            },
            AlignType::Bytes => match (self.cast_to_bytes(), other.cast_to_bytes()) {
                (Some(v0), Some(v1)) => v0.partial_cmp(&v1),
                _ => None,
            },
            AlignType::Datetime => match (self.cast_to_datetime(), other.cast_to_datetime()) {
                (Some(v0), Some(v1)) => v0.partial_cmp(&v1),
                _ => None,
            },
            AlignType::Identical => match (self, other) {
                (Const::I64(v0), Const::I64(v1)) => v0.partial_cmp(v1),
                (Const::U64(v0), Const::U64(v1)) => v0.partial_cmp(v1),
                (Const::F64(v0), Const::F64(v1)) => v0.value().partial_cmp(&v1.value()),
                (Const::Decimal(v0), Const::Decimal(v1)) => v0.partial_cmp(v1),
                (Const::Bool(v0), Const::Bool(v1)) => v0.partial_cmp(v1),
                (Const::Date(v0), Const::Date(v1)) => v0.partial_cmp(v1),
                (Const::Time(v0), Const::Time(v1)) => v0.partial_cmp(v1),
                (Const::Datetime(v0), Const::Datetime(v1)) => v0.partial_cmp(v1),
                (Const::Interval(_v0), Const::Interval(_v1)) => todo!(),
                (Const::String(v0), Const::String(v1)) => v0.partial_cmp(v1),
                (Const::Bytes(v0), Const::Bytes(v1)) => v0.partial_cmp(v1),
                (Const::Null, Const::Null) => todo!(),
                _ => None,
            },
        })
    }
}

pub const F64_ZERO: ValidF64 = ValidF64(0.0);
pub const F64_ONE: ValidF64 = ValidF64(1.0);

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ValidF64(f64);

impl ValidF64 {
    #[inline]
    pub fn new(value: f64) -> Option<Self> {
        if value.is_infinite() || value.is_nan() {
            None
        } else {
            Some(ValidF64(value))
        }
    }

    #[inline]
    pub const fn value(&self) -> f64 {
        self.0
    }
}

impl PartialEq for ValidF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

// we must ensure f64 is valid for equality check
impl Eq for ValidF64 {}

impl PartialOrd for ValidF64 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidF64 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl Hash for ValidF64 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits())
    }
}

impl Deref for ValidF64 {
    type Target = f64;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub const F32_ZERO: ValidF32 = ValidF32(0.0);
pub const F32_ONE: ValidF32 = ValidF32(1.0);

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ValidF32(f32);

impl ValidF32 {
    #[inline]
    pub fn new(value: f32) -> Option<Self> {
        if value.is_infinite() || value.is_nan() {
            None
        } else {
            Some(ValidF32(value))
        }
    }

    #[inline]
    pub const fn value(&self) -> f32 {
        self.0
    }
}

impl PartialEq for ValidF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for ValidF32 {}

impl PartialOrd for ValidF32 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidF32 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl Hash for ValidF32 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0.to_bits())
    }
}

impl Deref for ValidF32 {
    type Target = f32;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
