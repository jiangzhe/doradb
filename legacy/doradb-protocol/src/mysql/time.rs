#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MyTime {
    pub negative: bool,
    pub days: u32,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro_second: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MyDatetime {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro_second: u32,
}
