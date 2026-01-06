//! This module contains definition and functions of LWC(Lightweight Compression) Block.

use crate::buffer::page::BufferPage;
use crate::error::{Error, Result};
use crate::file::table_file::TABLE_FILE_PAGE_SIZE;
use crate::lwc::{
    FlatU64, ForBitpacking1, ForBitpacking2, ForBitpacking4, ForBitpacking8, ForBitpacking16,
    ForBitpacking32, LwcData, LwcPrimitive, LwcPrimitiveData, SortedPosition,
};
use crate::row::RowID;
use crate::serde::{Ser, SerdeCtx};
use crate::value::ValKind;
use std::mem;

const LWC_PAGE_FOOTER_OFFSET: usize = TABLE_FILE_PAGE_SIZE - mem::size_of::<LwcPageHeader>() - 32;

/// LwcPage stores compressioned data on disk.
/// Its size is same as in-memory row page.
/// The differences between LwcPage and row(PAX) page
/// are:
/// 1. Fields in row page is well aligned, so transmute
///    is safe to performed to access single field.
///    Fields in lwc page is not aligned, and often compressed
///    via dict/bitpacking. So access single field is often directly
///    performed on compressed data.
/// 2. Row page is mutable so there is always a hybried lock associated
///    to it and a row-level lock held by undo entry. There is also
///    delete bit for each row.
///    Lwc page is immutable and values are compressed. Delete bitmap is
///    separated in a standalone page. Row id is sparse and row access
///    requires first binary search on compressed row id array, instead
///    of simple offset calculation in row page.
///
/// Header:
///
/// ```text
/// |-------------------------|-----------|
/// | field                   | length(B) |
/// |-------------------------|-----------|
/// | first_row_id            | 8         |
/// | last_row_id             | 8         |
/// | row_count               | 2         |
/// | col_count               | 2         |
/// | first_col_offset        | 2         |
/// | padding                 | 2         |
/// |-------------------------|-----------|
/// ```
///
/// Body:
///
/// ```text
/// |------------------|------------------------------------------------|
/// | field            | length(B)                                      |
/// |------------------|------------------------------------------------|
/// | col_offsets      | 2 * col_count                                  |
/// | row_id           | col_offsets[0] - first_col_offset              |
/// | c_0              | col_offsets[1] - col_offsets[0]                |
/// | ...              | ...                                            |
/// | c_n              | col_offsets[n] - col_offsets[n-1]              |
/// | padding          | rest of the page except checksum               |
/// |------------------|------------------------------------------------|
/// ```
///
/// Footer:
///
/// ```text
/// |-------------------------|-----------|
/// | field                   | length(B) |
/// |-------------------------|-----------|
/// | b3sum                   | 32        |
/// |-------------------------|-----------|
/// ```
pub struct LwcPage {
    // The conversion from disk page to mem page is not safe.
    // We should use Ser and Deser for endianess safety.
    pub header: LwcPageHeader,
    pub body: [u8; TABLE_FILE_PAGE_SIZE - mem::size_of::<LwcPageHeader>()],
}

impl LwcPage {
    pub const BODY_SIZE: usize = TABLE_FILE_PAGE_SIZE - mem::size_of::<LwcPageHeader>();

    /// Read row from this page.
    #[inline]
    pub fn row_id_exists(&self, row_id: RowID) -> Result<bool> {
        if row_id < self.header.first_row_id() {
            return Ok(false);
        }
        if row_id > self.header.last_row_id() {
            return Ok(false);
        }
        let row_id_set = self.row_id_set()?;
        let res = row_id_set.position(row_id).is_some();
        Ok(res)
    }

    #[inline]
    fn row_id_set(&self) -> Result<RowIDSet<'_>> {
        let start_idx = self.header.col_count() as usize * mem::size_of::<u16>();
        let end_idx = self.header.first_col_offset() as usize;
        let input = &self.body[start_idx..end_idx];
        RowIDSet::from_bytes(input)
    }

    /// Returns column end offset array.
    #[inline]
    fn col_offsets(&self) -> ColOffsets<'_> {
        let col_count = self.header.col_count() as usize;
        let end_idx = col_count * mem::size_of::<u16>();
        let raw = &self.body[..end_idx];
        let offsets = bytemuck::cast_slice::<u8, [u8; 2]>(raw);
        ColOffsets {
            first_col_offset: self.header.first_col_offset() as usize, // columns follows offset array.
            offsets,
        }
    }
}

impl BufferPage for LwcPage {}

pub struct ColOffsets<'a> {
    first_col_offset: usize,
    offsets: &'a [[u8; 2]],
}

impl ColOffsets<'_> {
    /// Get the data range for given column.
    /// Note: RowID is not included in this function call.
    /// So idx=0 represents actually *second* column in this page.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<(usize, usize)> {
        if idx >= self.offsets.len() {
            return None;
        }
        let start_idx = if idx == 0 {
            self.first_col_offset
        } else {
            u16::from_le_bytes(self.offsets[idx - 1]) as usize
        };
        let end_idx = u16::from_le_bytes(self.offsets[idx]) as usize;
        Some((start_idx, end_idx))
    }
}

const LWC_PAGE_HEADER_SIZE: usize = 24;
const _: () = assert!(mem::size_of::<LwcPageHeader>() == LWC_PAGE_HEADER_SIZE);

/// Header of Lwc Page.
/// The fields are all defined as byte array
/// to avoid endianess mistake in serialization
/// and deserialization.
#[repr(C)]
#[derive(Clone)]
pub struct LwcPageHeader {
    /// Id of first row.
    first_row_id: [u8; 8],
    /// Id of last row.
    last_row_id: [u8; 8],
    /// Row count in this page.
    row_count: [u8; 2],
    /// Column count in this page.
    /// Row ID is excluded.
    col_count: [u8; 2],
    /// Start offset of first column.
    /// Row ID is excluded.
    /// This is actually the end of Row ID column.
    first_col_offset: [u8; 2],
    /// padding for alignment.
    padding: [u8; 2],
}

impl LwcPageHeader {
    #[inline]
    pub fn new(
        first_row_id: u64,
        last_row_id: u64,
        row_count: u16,
        col_count: u16,
        first_col_offset: u16,
    ) -> Self {
        LwcPageHeader {
            first_row_id: first_row_id.to_le_bytes(),
            last_row_id: last_row_id.to_le_bytes(),
            row_count: row_count.to_le_bytes(),
            col_count: col_count.to_le_bytes(),
            first_col_offset: first_col_offset.to_le_bytes(),
            padding: [0u8; 2],
        }
    }

    #[inline]
    pub fn first_row_id(&self) -> u64 {
        u64::from_le_bytes(self.first_row_id)
    }

    #[inline]
    pub fn last_row_id(&self) -> u64 {
        u64::from_le_bytes(self.last_row_id)
    }

    #[inline]
    pub fn row_count(&self) -> u16 {
        u16::from_le_bytes(self.row_count)
    }

    #[inline]
    pub fn col_count(&self) -> u16 {
        u16::from_le_bytes(self.col_count)
    }

    #[inline]
    pub fn first_col_offset(&self) -> u16 {
        u16::from_le_bytes(self.first_col_offset)
    }
}

impl Ser<'_> for LwcPageHeader {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<LwcPageHeader>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_byte_array(out, start_idx, &self.first_row_id);
        let idx = ctx.ser_byte_array(out, idx, &self.last_row_id);
        let idx = ctx.ser_byte_array(out, idx, &self.row_count);
        let idx = ctx.ser_byte_array(out, idx, &self.col_count);
        let idx = ctx.ser_byte_array(out, idx, &self.first_col_offset);
        ctx.ser_byte_array(out, idx, &self.padding)
    }
}

pub enum RowIDSet<'a> {
    B1(ForBitpacking1<'a, RowID>),
    B2(ForBitpacking2<'a, RowID>),
    B4(ForBitpacking4<'a, RowID>),
    B8(ForBitpacking8<'a, RowID>),
    B16(ForBitpacking16<'a, RowID>),
    B32(ForBitpacking32<'a, RowID>),
    Flat(FlatU64<'a>),
}

impl<'a> RowIDSet<'a> {
    /// Parse row id set from bytes.
    #[inline]
    pub fn from_bytes(input: &'a [u8]) -> Result<Self> {
        let res = match LwcData::from_bytes(ValKind::U64, input)? {
            LwcData::Primitive(LwcPrimitive::ForBp1U64(b1)) => RowIDSet::B1(b1),
            LwcData::Primitive(LwcPrimitive::ForBp2U64(b2)) => RowIDSet::B2(b2),
            LwcData::Primitive(LwcPrimitive::ForBp4U64(b4)) => RowIDSet::B4(b4),
            LwcData::Primitive(LwcPrimitive::ForBp8U64(b8)) => RowIDSet::B8(b8),
            LwcData::Primitive(LwcPrimitive::ForBp16U64(b16)) => RowIDSet::B16(b16),
            LwcData::Primitive(LwcPrimitive::ForBp32U64(b32)) => RowIDSet::B32(b32),
            LwcData::Primitive(LwcPrimitive::FlatU64(f)) => RowIDSet::Flat(f),
            _ => return Err(Error::InvalidCompressedData),
        };
        Ok(res)
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            RowIDSet::B1(f) => f.len(),
            RowIDSet::B2(f) => f.len(),
            RowIDSet::B4(f) => f.len(),
            RowIDSet::B8(f) => f.len(),
            RowIDSet::B16(f) => f.len(),
            RowIDSet::B32(f) => f.len(),
            RowIDSet::Flat(f) => f.len(),
        }
    }

    /// Find the position of given row id.
    /// For bits=1, 2, 4. There are only a few values
    /// so perform sequential search.
    /// For bits=8, 16, 32 and flat, perform binary search.
    #[inline]
    pub fn position(&self, row_id: RowID) -> Option<usize> {
        match self {
            RowIDSet::B1(f) => f.iter().position(|v| v == row_id),
            RowIDSet::B2(f) => f.iter().position(|v| v == row_id),
            RowIDSet::B4(f) => f.iter().position(|v| v == row_id),
            RowIDSet::B8(f) => f.sorted_position(row_id),
            RowIDSet::B16(f) => f.sorted_position(row_id),
            RowIDSet::B32(f) => f.sorted_position(row_id),
            RowIDSet::Flat(f) => f.sorted_position(row_id),
        }
    }

    /// Extend all row ids to given vec.
    #[inline]
    pub fn extend_to(&self, res: &mut Vec<RowID>) {
        match self {
            RowIDSet::B1(f) => f.extend_to(res),
            RowIDSet::B2(f) => f.extend_to(res),
            RowIDSet::B4(f) => f.extend_to(res),
            RowIDSet::B8(f) => f.extend_to(res),
            RowIDSet::B16(f) => f.extend_to(res),
            RowIDSet::B32(f) => f.extend_to(res),
            RowIDSet::Flat(f) => f.extend_to(res),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lwc::LwcPrimitiveSer;
    use crate::serde::SerdeCtx;

    #[test]
    fn test_row_id_set() {
        let mut buffer = vec![];

        let input = vec![1u64];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(1), Some(0));
        assert_eq!(rs.position(0), None);
        assert_eq!(rs.position(3), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 3];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(3), Some(1));
        assert_eq!(rs.position(2), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 3, 5, 6];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(6), Some(3));
        assert_eq!(rs.position(4), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 3, 6, 10, 11, 12, 13, 14, 15];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(14), Some(7));
        assert_eq!(rs.position(9), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 5, 10, 168, 199, 200, 201, 250, 251, 252, 253];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(200), Some(5));
        assert_eq!(rs.position(202), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 50, 200, 10000, 11000];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(11000), Some(4));
        assert_eq!(rs.position(10090), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);

        let input = vec![1u64, 50, 200, 10000, 11000, 1000000, 1000001];
        let rs = create_row_id_set(&input, &mut buffer);
        assert_eq!(rs.position(1000000), Some(5));
        assert_eq!(rs.position(999999), None);
        let mut output = vec![];
        rs.extend_to(&mut output);
        assert_eq!(input, output);
    }

    fn create_row_id_set<'a>(row_ids: &[RowID], buffer: &'a mut Vec<u8>) -> RowIDSet<'a> {
        buffer.clear();
        let mut ctx = SerdeCtx::default();
        let lwc_ser = LwcPrimitiveSer::new_u64(row_ids);
        let ser_len = lwc_ser.ser_len(&ctx);
        buffer.resize(ser_len, 0);
        let ser_idx = lwc_ser.ser(&mut ctx, buffer, 0);
        debug_assert!(ser_len == ser_idx);
        RowIDSet::from_bytes(buffer).unwrap()
    }

    #[test]
    fn test_lwc_page() {
        let mut bytes = [0u8; TABLE_FILE_PAGE_SIZE];
        let page = unsafe { std::mem::transmute::<&mut [u8; 65536], &mut LwcPage>(&mut bytes) };
        page.header = LwcPageHeader::new(100, 200, 50, 2, 312);
        assert!(page.header.first_row_id() == 100);
        assert!(page.header.last_row_id() == 200);
        assert!(page.header.row_count() == 50);
        assert!(page.header.col_count() == 2);
        assert!(page.header.first_col_offset() == 312);
        let ctx = SerdeCtx::default();
        let mut header_vec = vec![0u8; page.header.ser_len(&ctx)];
        let ser_idx = page.header.ser(&ctx, &mut header_vec, 0);
        assert!(ser_idx == header_vec.len());
        assert_eq!(&header_vec, &bytes[..header_vec.len()]);
    }
}
