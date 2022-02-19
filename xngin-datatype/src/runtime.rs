#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum RuntimeType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Decimal,
    Bool,
    String,
    Char,
    Bytes,
    Date,
    Time,
    Datetime,
    Interval,
    Null,
}