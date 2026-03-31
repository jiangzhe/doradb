//! This module contains definition and functions of LWC(Lightweight Compression) Block.

use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::{Page, PageID};
use crate::catalog::TableMetadata;
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::page_integrity::{
    LWC_PAGE_SPEC, PAGE_INTEGRITY_HEADER_SIZE, max_payload_len, validate_page,
};
use crate::lwc::{
    FlatU64, ForBitpacking1, ForBitpacking2, ForBitpacking4, ForBitpacking8, ForBitpacking16,
    ForBitpacking32, LwcData, LwcNullBitmap, LwcPrimitive, LwcPrimitiveData, SortedPosition,
};
use crate::row::RowID;
use crate::serde::{Ser, Serde};
use crate::value::{Val, ValKind};
use bytemuck::{Pod, Zeroable};
use std::mem;

/// Size in bytes of one validated persisted LWC payload, excluding the shared page envelope.
pub const LWC_PAGE_PAYLOAD_SIZE: usize = max_payload_len(COW_FILE_PAGE_SIZE);

/// LwcPage stores the payload of one immutable checksummed LWC page.
///
/// The surrounding page-integrity envelope is validated before this payload is
/// interpreted. The differences between LwcPage and row(PAX) page
/// are:
/// 1. Fields in row page are well aligned for typed access.
///    Fields in lwc page are not aligned, and often compressed
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
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LwcPage {
    // The conversion from raw bytes to payload view is not endianness-safe for
    // arbitrary external input. Persisted callers must validate the outer page
    // envelope first and only then cast the fixed-size payload bytes.
    pub header: LwcPageHeader,
    pub body: [u8; LWC_PAGE_PAYLOAD_SIZE - mem::size_of::<LwcPageHeader>()],
}

// SAFETY: `LwcPage` is `repr(C)` and consists only of byte-array based fields.
// Any bit pattern is valid and there is no interior mutability or drop glue.
unsafe impl Zeroable for LwcPage {}
// SAFETY: same reasoning as above; plain immutable byte data layout.
unsafe impl Pod for LwcPage {}

impl LwcPage {
    pub const BODY_SIZE: usize = LWC_PAGE_PAYLOAD_SIZE - mem::size_of::<LwcPageHeader>();

    #[inline]
    pub fn try_from_bytes(input: &[u8]) -> Result<&Self> {
        bytemuck::try_from_bytes::<Self>(input).map_err(|_| Error::InvalidCompressedData)
    }

    #[inline]
    pub fn try_from_bytes_mut(input: &mut [u8]) -> Result<&mut Self> {
        bytemuck::try_from_bytes_mut::<Self>(input).map_err(|_| Error::InvalidCompressedData)
    }

    /// Validates a full persisted LWC page image and returns its payload view.
    #[inline]
    pub fn try_from_persisted_bytes(
        input: &[u8],
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<&Self> {
        let payload = validate_page(input, LWC_PAGE_SPEC).map_err(|cause| {
            Error::persisted_page_corrupted(file_kind, PersistedPageKind::LwcPage, page_id, cause)
        })?;
        Self::try_from_bytes(payload)
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))
    }

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

    /// Returns row index for given row id.
    #[inline]
    pub fn row_idx(&self, row_id: RowID) -> Result<Option<usize>> {
        if row_id < self.header.first_row_id() || row_id > self.header.last_row_id() {
            return Ok(None);
        }
        let row_id_set = self.row_id_set()?;
        Ok(row_id_set.position(row_id))
    }

    /// Returns row index for a persisted page row id and maps payload failures
    /// to contextual persisted-page corruption.
    #[inline]
    pub fn find_persisted_row_idx(
        &self,
        row_id: RowID,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Option<usize>> {
        self.row_idx(row_id)
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))
    }

    /// Returns the row id at one ordinal position.
    #[inline]
    pub fn row_id_at(&self, row_idx: usize) -> Result<Option<RowID>> {
        Ok(self.row_id_set()?.value(row_idx))
    }

    #[inline]
    fn row_id_set(&self) -> Result<RowIDSet<'_>> {
        let start_idx = self.header.col_count() as usize * mem::size_of::<u16>();
        let end_idx = self.header.first_col_offset() as usize;
        if end_idx > self.body.len() || start_idx > end_idx {
            return Err(Error::InvalidCompressedData);
        }
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

    /// Returns column data for given column index based on metadata.
    #[inline]
    pub fn column<'a>(
        &'a self,
        metadata: &'a TableMetadata,
        col_idx: usize,
    ) -> Result<LwcColumn<'a>> {
        if col_idx >= metadata.col_count() {
            return Err(Error::IndexOutOfBound);
        }
        let (start_idx, end_idx) = self
            .col_offsets()
            .get(col_idx)
            .ok_or(Error::IndexOutOfBound)?;
        if end_idx > self.body.len() || start_idx > end_idx {
            return Err(Error::InvalidCompressedData);
        }
        let data = &self.body[start_idx..end_idx];
        let row_count = self.header.row_count() as usize;
        let kind = metadata.val_kind(col_idx);
        if metadata.nullable(col_idx) {
            let (bitmap, values) = LwcNullBitmap::from_bytes(data)?;
            let required = row_count.div_ceil(8);
            if bitmap.len() < required {
                return Err(Error::InvalidCompressedData);
            }
            Ok(LwcColumn {
                kind,
                row_count,
                null_bitmap: Some(bitmap),
                values,
            })
        } else {
            Ok(LwcColumn {
                kind,
                row_count,
                null_bitmap: None,
                values: data,
            })
        }
    }

    /// Decodes the full sorted row-id list stored in this page payload.
    #[inline]
    pub fn decode_row_ids(&self) -> Result<Vec<RowID>> {
        let row_count = self.header.row_count() as usize;
        let row_id_set = self.row_id_set()?;
        if row_id_set.len() != row_count {
            return Err(Error::InvalidCompressedData);
        }
        let mut row_ids = Vec::with_capacity(row_count);
        row_id_set.extend_to(&mut row_ids);
        Ok(row_ids)
    }

    /// Decodes all row ids from a persisted page and maps payload failures to
    /// contextual persisted-page corruption.
    #[inline]
    pub fn decode_persisted_row_ids(
        &self,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Vec<RowID>> {
        self.decode_row_ids()
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))
    }

    /// Decodes selected values from one persisted page row in the requested
    /// column order and maps payload failures to contextual persisted-page
    /// corruption.
    #[inline]
    pub fn decode_persisted_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        read_set: &[usize],
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Vec<Val>> {
        self.decode_persisted_row_values_inner(
            metadata,
            row_idx,
            read_set.iter().copied(),
            read_set.len(),
            file_kind,
            page_id,
        )
    }

    /// Decodes all values from one persisted page row and maps payload
    /// failures to contextual persisted-page corruption.
    #[inline]
    pub fn decode_persisted_full_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Vec<Val>> {
        self.decode_persisted_row_values_inner(
            metadata,
            row_idx,
            0..metadata.col_count(),
            metadata.col_count(),
            file_kind,
            page_id,
        )
    }

    #[inline]
    fn decode_persisted_row_values_inner(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        col_indices: impl Iterator<Item = usize>,
        capacity: usize,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Vec<Val>> {
        let mut vals = Vec::with_capacity(capacity);
        for col_idx in col_indices {
            vals.push(self.decode_persisted_value(metadata, row_idx, col_idx, file_kind, page_id)?);
        }
        Ok(vals)
    }

    #[inline]
    fn decode_persisted_value(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        col_idx: usize,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Val> {
        let column = self
            .column(metadata, col_idx)
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))?;
        if column.is_null(row_idx) {
            return Ok(Val::Null);
        }
        let data = column
            .data()
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))?;
        data.value(row_idx)
            .ok_or(Error::InvalidCompressedData)
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))
    }
}

/// Validates one persisted LWC page image for readonly-cache residency.
#[inline]
pub(crate) fn validate_persisted_lwc_page(
    input: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<()> {
    LwcPage::try_from_persisted_bytes(input, file_kind, page_id).map(|_| ())
}

/// Borrowed validated persisted LWC page backed by a readonly-cache guard.
pub(crate) struct PersistedLwcPage {
    guard: PageSharedGuard<Page>,
    file_kind: PersistedFileKind,
    page_id: PageID,
}

impl PersistedLwcPage {
    /// Loads one persisted LWC page through the validated readonly-cache path.
    #[inline]
    pub async fn load(disk_pool: &ReadonlyBufferPool, page_id: PageID) -> Result<Self> {
        let file_kind = disk_pool.persisted_file_kind();
        let guard = disk_pool
            .get_validated_page_shared(page_id, validate_persisted_lwc_page)
            .await?;
        Ok(PersistedLwcPage {
            guard,
            file_kind,
            page_id,
        })
    }

    #[inline]
    fn page(&self) -> &LwcPage {
        let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        LwcPage::try_from_bytes(&self.guard.page()[payload_start..payload_end])
            .expect("validated readonly LWC page must have a valid payload layout")
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.page().header.row_count() as usize
    }

    #[inline]
    #[allow(dead_code)]
    pub fn find_row_idx(&self, row_id: RowID) -> Result<Option<usize>> {
        self.page()
            .find_persisted_row_idx(row_id, self.file_kind, self.page_id)
    }

    #[inline]
    pub fn row_id_at(&self, row_idx: usize) -> Result<Option<RowID>> {
        self.page()
            .row_id_at(row_idx)
            .map_err(|err| map_persisted_lwc_error(self.file_kind, self.page_id, err))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn decode_row_ids(&self) -> Result<Vec<RowID>> {
        self.page()
            .decode_persisted_row_ids(self.file_kind, self.page_id)
    }

    #[inline]
    pub fn decode_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        read_set: &[usize],
    ) -> Result<Vec<Val>> {
        self.page().decode_persisted_row_values(
            metadata,
            row_idx,
            read_set,
            self.file_kind,
            self.page_id,
        )
    }

    #[inline]
    pub fn decode_full_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
    ) -> Result<Vec<Val>> {
        self.page().decode_persisted_full_row_values(
            metadata,
            row_idx,
            self.file_kind,
            self.page_id,
        )
    }
}

#[derive(Debug)]
pub struct LwcColumn<'a> {
    kind: ValKind,
    row_count: usize,
    null_bitmap: Option<LwcNullBitmap<'a>>,
    values: &'a [u8],
}

impl<'a> LwcColumn<'a> {
    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        if row_idx >= self.row_count {
            return false;
        }
        self.null_bitmap
            .as_ref()
            .map(|bitmap| bitmap.is_null(row_idx))
            .unwrap_or(false)
    }

    #[inline]
    pub fn data(&self) -> Result<LwcData<'a>> {
        LwcData::from_bytes(self.kind, self.values)
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

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
const _: () = assert!(mem::size_of::<LwcPage>() == LWC_PAGE_PAYLOAD_SIZE);

/// Header of Lwc Page.
/// The fields are all defined as byte array
/// to avoid endianess mistake in serialization
/// and deserialization.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
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
    fn ser_len(&self) -> usize {
        mem::size_of::<LwcPageHeader>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.first_row_id);
        let idx = out.ser_byte_array(idx, &self.last_row_id);
        let idx = out.ser_byte_array(idx, &self.row_count);
        let idx = out.ser_byte_array(idx, &self.col_count);
        let idx = out.ser_byte_array(idx, &self.first_col_offset);
        out.ser_byte_array(idx, &self.padding)
    }
}

#[inline]
pub(crate) fn map_persisted_lwc_error(
    file_kind: PersistedFileKind,
    page_id: PageID,
    err: Error,
) -> Error {
    match err {
        Error::InvalidCompressedData | Error::InvalidFormat | Error::NotSupported(_) => {
            Error::persisted_page_corrupted(
                file_kind,
                PersistedPageKind::LwcPage,
                page_id,
                PersistedPageCorruptionCause::InvalidPayload,
            )
        }
        other => other,
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

    #[allow(clippy::len_without_is_empty)]
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

    /// Returns the row id at the given ordinal position.
    #[inline]
    pub fn value(&self, idx: usize) -> Option<RowID> {
        match self {
            RowIDSet::B1(f) => f.value(idx),
            RowIDSet::B2(f) => f.value(idx),
            RowIDSet::B4(f) => f.value(idx),
            RowIDSet::B8(f) => f.value(idx),
            RowIDSet::B16(f) => f.value(idx),
            RowIDSet::B32(f) => f.value(idx),
            RowIDSet::Flat(f) => f.value(idx),
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
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::error::{PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind};
    use crate::file::page_integrity::{
        PAGE_INTEGRITY_HEADER_SIZE, write_page_checksum, write_page_header,
    };
    use crate::io::DirectBuf;
    use crate::lwc::{LwcBuilder, LwcCode, LwcPrimitiveSer};
    use crate::row::{InsertRow, RowPage};
    use crate::value::Val;

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
        let lwc_ser = LwcPrimitiveSer::new_u64(row_ids);
        let ser_len = lwc_ser.ser_len();
        buffer.resize(ser_len, 0);
        let ser_idx = lwc_ser.ser(&mut buffer[..], 0);
        debug_assert!(ser_len == ser_idx);
        RowIDSet::from_bytes(buffer).unwrap()
    }

    fn build_persisted_lwc_page() -> DirectBuf {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), LWC_PAGE_SPEC);
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        let page =
            LwcPage::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();
        page.header = LwcPageHeader::new(11, 19, 2, 1, 32);
        page.body[0] = 0xAB;
        page.body[1] = 0xCD;
        write_page_checksum(buf.data_mut());
        buf
    }

    fn build_valid_persisted_lwc_page() -> (TableMetadata, DirectBuf) {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I16, ColumnAttributes::NULLABLE),
            ],
            vec![],
        );
        let mut page = RowPage::new_test_page();
        page.init(100, 8, &metadata);
        assert!(matches!(
            page.insert(&metadata, &[Val::U8(10), Val::I16(20)]),
            InsertRow::Ok(_)
        ));
        assert!(matches!(
            page.insert(&metadata, &[Val::U8(11), Val::Null]),
            InsertRow::Ok(_)
        ));
        assert!(matches!(
            page.insert(&metadata, &[Val::U8(12), Val::I16(22)]),
            InsertRow::Ok(_)
        ));
        let buf = {
            let mut builder = LwcBuilder::new(&metadata);
            assert!(builder.append_row_page(&page).unwrap());
            builder.build().unwrap()
        };
        (metadata, buf)
    }

    #[test]
    fn test_lwc_page() {
        let mut buf = DirectBuf::zeroed(LWC_PAGE_PAYLOAD_SIZE);
        let page = LwcPage::try_from_bytes_mut(buf.data_mut()).unwrap();
        page.header = LwcPageHeader::new(100, 200, 50, 2, 312);
        assert!(page.header.first_row_id() == 100);
        assert!(page.header.last_row_id() == 200);
        assert!(page.header.row_count() == 50);
        assert!(page.header.col_count() == 2);
        assert!(page.header.first_col_offset() == 312);
        let mut header_vec = vec![0u8; page.header.ser_len()];
        let ser_idx = page.header.ser(&mut header_vec[..], 0);
        assert!(ser_idx == header_vec.len());
        assert_eq!(&header_vec, bytemuck::bytes_of(&page.header));
    }

    #[test]
    fn test_lwc_page_nullable_column() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U8,
                ColumnAttributes::NULLABLE,
            )],
            vec![],
        );
        let row_ids = [1u64, 2, 3, 4];
        let row_id_ser = LwcPrimitiveSer::new_u64(&row_ids);
        let mut row_id_bytes = vec![0u8; row_id_ser.ser_len()];
        row_id_ser.ser(&mut row_id_bytes[..], 0);
        let values = [10u8, 20, 30, 40];
        let lwc_ser = LwcPrimitiveSer::new_u8(&values);
        let mut values_bytes = vec![0u8; lwc_ser.ser_len()];
        lwc_ser.ser(&mut values_bytes[..], 0);

        let null_bytes = [0b0000_1010u8];
        let null_ser = crate::lwc::LwcNullBitmapSer::new(&null_bytes).unwrap();
        let mut column_bytes = vec![0u8; null_ser.ser_len() + values_bytes.len()];
        let idx = null_ser.ser(&mut column_bytes[..], 0);
        column_bytes[idx..].copy_from_slice(&values_bytes);

        let mut buf = DirectBuf::zeroed(LWC_PAGE_PAYLOAD_SIZE);
        let page = LwcPage::try_from_bytes_mut(buf.data_mut()).unwrap();
        let col_offsets_len = mem::size_of::<u16>();
        let col_start = col_offsets_len + row_id_bytes.len();
        let col_end = col_start + column_bytes.len();
        page.header = LwcPageHeader::new(1, 4, values.len() as u16, 1, col_start as u16);
        page.body[..col_offsets_len].copy_from_slice(&(col_end as u16).to_le_bytes());
        page.body[col_offsets_len..col_start].copy_from_slice(&row_id_bytes);
        page.body[col_start..col_end].copy_from_slice(&column_bytes);

        let column = page.column(&metadata, 0).unwrap();
        assert_eq!(column.row_count(), values.len());
        assert!(!column.is_null(0));
        assert!(column.is_null(1));
        assert!(!column.is_null(2));
        assert!(column.is_null(3));

        let lwc_data = column.data().unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_u8().unwrap());
        }
        assert_eq!(output, values);
    }

    #[test]
    fn test_lwc_page_column_metadata_mismatch() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U8,
                ColumnAttributes::empty(),
            )],
            vec![],
        );
        let mut buf = DirectBuf::zeroed(LWC_PAGE_PAYLOAD_SIZE);
        let page = LwcPage::try_from_bytes_mut(buf.data_mut()).unwrap();
        let col_offsets_len = mem::size_of::<u16>() * 2;
        let end_offset = col_offsets_len as u16;
        page.header = LwcPageHeader::new(1, 1, 0, 2, end_offset);
        page.body[..2].copy_from_slice(&end_offset.to_le_bytes());
        page.body[2..4].copy_from_slice(&end_offset.to_le_bytes());

        let err = page.column(&metadata, 1);
        assert!(matches!(err, Err(Error::IndexOutOfBound)));
    }

    #[test]
    fn test_lwc_page_try_from_bytes_invalid_len() {
        let bytes = [0u8; LWC_PAGE_PAYLOAD_SIZE - 1];
        let err = LwcPage::try_from_bytes(&bytes);
        assert!(matches!(err, Err(Error::InvalidCompressedData)));
    }

    #[test]
    fn test_lwc_page_try_from_bytes_mut_roundtrip() {
        let mut buf = DirectBuf::zeroed(LWC_PAGE_PAYLOAD_SIZE);
        {
            let page = LwcPage::try_from_bytes_mut(buf.data_mut()).unwrap();
            page.header = LwcPageHeader::new(11, 19, 2, 1, 32);
            page.body[0] = 0xAB;
            page.body[1] = 0xCD;
        }
        let page = LwcPage::try_from_bytes(buf.data()).unwrap();
        assert_eq!(page.header.first_row_id(), 11);
        assert_eq!(page.header.last_row_id(), 19);
        assert_eq!(page.header.row_count(), 2);
        assert_eq!(page.header.col_count(), 1);
        assert_eq!(page.header.first_col_offset(), 32);
        assert_eq!(page.body[0], 0xAB);
        assert_eq!(page.body[1], 0xCD);
    }

    #[test]
    fn test_lwc_page_rejects_persisted_checksum_corruption() {
        let mut buf = build_persisted_lwc_page();
        let last_idx = buf.data().len() - 1;
        buf.data_mut()[last_idx] ^= 0xFF;

        let err =
            match LwcPage::try_from_persisted_bytes(buf.data(), PersistedFileKind::TableFile, 7) {
                Ok(_) => panic!("expected LWC checksum corruption"),
                Err(err) => err,
            };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id: 7,
                cause: PersistedPageCorruptionCause::ChecksumMismatch,
            }
        ));
    }

    #[test]
    fn test_lwc_page_decode_persisted_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_page();
        let page =
            LwcPage::try_from_persisted_bytes(buf.data(), PersistedFileKind::TableFile, 8).unwrap();
        let row_idx = page
            .find_persisted_row_idx(101, PersistedFileKind::TableFile, 8)
            .unwrap()
            .unwrap();
        let vals = page
            .decode_persisted_row_values(
                &metadata,
                row_idx,
                &[1, 0],
                PersistedFileKind::TableFile,
                8,
            )
            .unwrap();
        assert_eq!(vals, vec![Val::Null, Val::U8(11)]);
        assert_eq!(
            page.find_persisted_row_idx(999, PersistedFileKind::TableFile, 8)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_lwc_page_decode_persisted_full_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_page();
        let page =
            LwcPage::try_from_persisted_bytes(buf.data(), PersistedFileKind::TableFile, 8).unwrap();
        assert_eq!(
            page.decode_persisted_row_ids(PersistedFileKind::TableFile, 8)
                .unwrap(),
            vec![100, 101, 102]
        );
        assert_eq!(
            page.decode_persisted_full_row_values(&metadata, 0, PersistedFileKind::TableFile, 8,)
                .unwrap(),
            vec![Val::U8(10), Val::I16(20)]
        );
    }

    #[test]
    fn test_lwc_page_maps_persisted_row_id_not_supported_to_corruption() {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), LWC_PAGE_SPEC);
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        let page =
            LwcPage::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();

        let mut row_id_bytes = vec![LwcCode::ForBitpacking as u8, 3];
        row_id_bytes.extend_from_slice(&1u64.to_le_bytes());
        row_id_bytes.extend_from_slice(&0u64.to_le_bytes());
        row_id_bytes.push(0);

        page.header = LwcPageHeader::new(0, 0, 1, 0, row_id_bytes.len() as u16);
        page.body[..row_id_bytes.len()].copy_from_slice(&row_id_bytes);
        write_page_checksum(buf.data_mut());

        let page =
            LwcPage::try_from_persisted_bytes(buf.data(), PersistedFileKind::TableFile, 9).unwrap();
        let err = match page.find_persisted_row_idx(0, PersistedFileKind::TableFile, 9) {
            Ok(_) => panic!("expected unsupported persisted row-id codec"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id: 9,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            }
        ));
    }

    #[test]
    fn test_lwc_page_maps_persisted_value_decode_error_to_corruption() {
        let (metadata, mut buf) = build_valid_persisted_lwc_page();
        let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        {
            let page = LwcPage::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end])
                .unwrap();
            let (start_idx, end_idx) = page.col_offsets().get(0).unwrap();
            let column = &mut page.body[start_idx..end_idx];
            let data_len = column.len().saturating_sub(11);
            let len = if data_len == 0 {
                0
            } else {
                (((data_len - 1) * 8) / 3 + 1) as u64
            };
            column[0] = LwcCode::ForBitpacking as u8;
            column[1] = 3;
            column[2..10].copy_from_slice(&len.to_le_bytes());
            column[10] = 0;
            column[11..].fill(0);
        }
        write_page_checksum(buf.data_mut());

        let page = LwcPage::try_from_persisted_bytes(buf.data(), PersistedFileKind::TableFile, 10)
            .unwrap();
        let err = page
            .decode_persisted_full_row_values(&metadata, 0, PersistedFileKind::TableFile, 10)
            .unwrap_err();
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id: 10,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            }
        ));
    }
}
