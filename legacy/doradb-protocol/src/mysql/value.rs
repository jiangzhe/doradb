use crate::mysql::time::{MyDatetime, MyTime};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Tiny(u8),
    Short(u16),
    Long(u32),
    Float(f32),
    Double(f64),
    Timestamp(MyDatetime),
    LongLong(u64),
    Int24(u32),
    Date { year: u16, month: u8, day: u8 },
    Time(MyTime),
    DateTime(MyDatetime),
    Year(u16),
    Bit(Box<[u8]>),
    NewDecimal(Box<str>),
    Blob(Box<[u8]>),
    VarString(Box<str>),
    String(Box<str>),
    Geometry(Box<[u8]>),
}
