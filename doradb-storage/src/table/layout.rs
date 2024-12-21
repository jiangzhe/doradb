/// Layout defines the memory layout of columns
/// stored in row page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Layout {
    Byte1, // i8, u8, bool, char(1) with ascii/latin charset
    Byte2, // i16, u16, decimal(1-2)
    Byte4, // i32, u32, f32, decimal(3-8)
    Byte8, // i64, u64, f64, decimal(9-18)
    // Byte16,  // decimal(19-38)
    VarByte, // bytes, string, no more than 60k.
}

impl Layout {
    #[inline]
    pub const fn fix_len(&self) -> usize {
        match self {
            Layout::Byte1 => 1,
            Layout::Byte2 => 2,
            Layout::Byte4 => 4,
            Layout::Byte8 => 8,
            // Layout::Byte16 => 16,
            // 2-byte len, 2-byte offset, 4-byte prefix
            // or inline version, 2-byte len, at most 6 inline bytes
            Layout::VarByte => 8,
        }
    }
}
