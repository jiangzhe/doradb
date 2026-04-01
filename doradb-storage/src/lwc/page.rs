//! This module contains definition and functions of LWC(Lightweight Compression) Block.

use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::catalog::TableMetadata;
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::{BlockID, COW_FILE_PAGE_SIZE};
use crate::file::page_integrity::{
    LWC_PAGE_SPEC, PAGE_INTEGRITY_HEADER_SIZE, max_payload_len, validate_page,
};
use crate::lwc::{LwcData, LwcNullBitmap};
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
///    separated in a standalone page. Cold-row identity is owned by the
///    column-block index, so persisted LWC page access uses ordinal positions
///    instead of page-local row-id lookup.
///
/// Header:
///
/// ```text
/// |-------------------------|-----------|
/// | field                   | length(B) |
/// |-------------------------|-----------|
/// | row_shape_fingerprint   | 16        |
/// | row_count               | 2         |
/// | col_count               | 2         |
/// | flags                   | 2         |
/// | reserved                | 10        |
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
/// | c_0              | col_offsets[0] - 2 * col_count                 |
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
        let page =
            bytemuck::try_from_bytes::<Self>(input).map_err(|_| Error::InvalidCompressedData)?;
        page.validate_structure()?;
        Ok(page)
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
        page_id: BlockID,
    ) -> Result<&Self> {
        let payload = validate_page(input, LWC_PAGE_SPEC).map_err(|cause| {
            Error::persisted_page_corrupted(file_kind, PersistedPageKind::LwcPage, page_id, cause)
        })?;
        Self::try_from_bytes(payload)
            .map_err(|err| map_persisted_lwc_error(file_kind, page_id, err))
    }

    #[inline]
    pub fn row_shape_fingerprint(&self) -> u128 {
        self.header.row_shape_fingerprint()
    }

    #[inline]
    fn validate_structure(&self) -> Result<()> {
        self.col_offsets()?.validate(self.body.len())
    }

    /// Returns column end offset array.
    #[inline]
    fn col_offsets(&self) -> Result<ColOffsets<'_>> {
        let col_count = self.header.col_count() as usize;
        let end_idx = col_count * mem::size_of::<u16>();
        if end_idx > self.body.len() {
            return Err(Error::InvalidCompressedData);
        }
        let raw = &self.body[..end_idx];
        let offsets = bytemuck::cast_slice::<u8, [u8; 2]>(raw);
        Ok(ColOffsets {
            data_start: end_idx,
            offsets,
        })
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
            .and_then(|offsets| offsets.get(col_idx).ok_or(Error::InvalidCompressedData))?;
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
        page_id: BlockID,
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
        page_id: BlockID,
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
        page_id: BlockID,
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
        page_id: BlockID,
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

/// Validates one persisted LWC block image for readonly-cache residency.
#[inline]
pub(crate) fn validate_persisted_lwc_block(
    input: &[u8],
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<()> {
    LwcPage::try_from_persisted_bytes(input, file_kind, page_id).map(|_| ())
}

/// Borrowed validated persisted LWC block backed by a readonly-cache guard.
pub(crate) struct PersistedLwcBlock {
    guard: ReadonlyBlockGuard,
    file_kind: PersistedFileKind,
    page_id: BlockID,
}

impl PersistedLwcBlock {
    /// Loads one persisted LWC block through the validated readonly-cache path.
    #[inline]
    pub async fn load(
        disk_pool: &ReadonlyBufferPool,
        disk_pool_guard: &PoolGuard,
        page_id: BlockID,
    ) -> Result<Self> {
        let file_kind = disk_pool.persisted_file_kind();
        let guard = disk_pool
            .read_validated_block(disk_pool_guard, page_id, validate_persisted_lwc_block)
            .await?;
        Ok(PersistedLwcBlock {
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
    pub fn row_shape_fingerprint(&self) -> u128 {
        self.page().row_shape_fingerprint()
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
    data_start: usize,
    offsets: &'a [[u8; 2]],
}

impl ColOffsets<'_> {
    #[inline]
    pub fn validate(&self, body_len: usize) -> Result<()> {
        let mut start_idx = self.data_start;
        if start_idx > body_len {
            return Err(Error::InvalidCompressedData);
        }
        for offset in self.offsets {
            let end_idx = u16::from_le_bytes(*offset) as usize;
            if end_idx > body_len || start_idx > end_idx {
                return Err(Error::InvalidCompressedData);
            }
            start_idx = end_idx;
        }
        Ok(())
    }

    /// Get the data range for given column.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<(usize, usize)> {
        if idx >= self.offsets.len() {
            return None;
        }
        let start_idx = if idx == 0 {
            self.data_start
        } else {
            u16::from_le_bytes(self.offsets[idx - 1]) as usize
        };
        let end_idx = u16::from_le_bytes(self.offsets[idx]) as usize;
        Some((start_idx, end_idx))
    }
}

const LWC_PAGE_HEADER_SIZE: usize = 32;
const _: () = assert!(mem::size_of::<LwcPageHeader>() == LWC_PAGE_HEADER_SIZE);
const _: () = assert!(mem::size_of::<LwcPage>() == LWC_PAGE_PAYLOAD_SIZE);

/// Header of Lwc Page.
/// The fields are all defined as byte array
/// to avoid endianess mistake in serialization
/// and deserialization.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct LwcPageHeader {
    /// Canonical row-shape fingerprint sourced from column-block index shape.
    row_shape_fingerprint: [u8; 16],
    /// Row count in this page.
    row_count: [u8; 2],
    /// Column count in this page.
    col_count: [u8; 2],
    /// Reserved header flags for future format extensions.
    flags: [u8; 2],
    /// Reserved bytes kept deterministic in freshly written pages.
    reserved: [u8; 10],
}

impl LwcPageHeader {
    #[inline]
    pub fn new(row_shape_fingerprint: u128, row_count: u16, col_count: u16, flags: u16) -> Self {
        LwcPageHeader {
            row_shape_fingerprint: row_shape_fingerprint.to_le_bytes(),
            row_count: row_count.to_le_bytes(),
            col_count: col_count.to_le_bytes(),
            flags: flags.to_le_bytes(),
            reserved: [0u8; 10],
        }
    }

    #[inline]
    pub fn row_shape_fingerprint(&self) -> u128 {
        u128::from_le_bytes(self.row_shape_fingerprint)
    }

    #[inline]
    pub fn row_count(&self) -> u16 {
        u16::from_le_bytes(self.row_count)
    }

    #[inline]
    pub fn col_count(&self) -> u16 {
        u16::from_le_bytes(self.col_count)
    }
}

impl Ser<'_> for LwcPageHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<LwcPageHeader>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.row_shape_fingerprint);
        let idx = out.ser_byte_array(idx, &self.row_count);
        let idx = out.ser_byte_array(idx, &self.col_count);
        let idx = out.ser_byte_array(idx, &self.flags);
        out.ser_byte_array(idx, &self.reserved)
    }
}

#[inline]
pub(crate) fn map_persisted_lwc_error(
    file_kind: PersistedFileKind,
    page_id: BlockID,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::error::{PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind};
    use crate::file::page_integrity::{
        PAGE_INTEGRITY_HEADER_SIZE, write_page_checksum, write_page_header,
    };
    use crate::index::ColumnBlockEntryShape;
    use crate::io::DirectBuf;
    use crate::lwc::{LwcBuilder, LwcCode, LwcPrimitiveSer};
    use crate::row::{InsertRow, RowPage};
    use crate::value::Val;

    fn row_shape_fingerprint_for(row_ids: &[u64]) -> u128 {
        let start_row_id = *row_ids.first().unwrap();
        let end_row_id = row_ids.last().unwrap().saturating_add(1);
        ColumnBlockEntryShape::new(start_row_id, end_row_id, row_ids.to_vec(), Vec::new())
            .unwrap()
            .row_shape_fingerprint()
    }

    fn build_persisted_lwc_page() -> DirectBuf {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), LWC_PAGE_SPEC);
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        let page =
            LwcPage::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();
        page.header = LwcPageHeader::new(11, 1, 1, 0);
        page.body[..2].copy_from_slice(&(2u16).to_le_bytes());
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
            let fingerprint = row_shape_fingerprint_for(builder.row_ids());
            builder.build(fingerprint).unwrap()
        };
        (metadata, buf)
    }

    #[test]
    fn test_lwc_page() {
        let mut buf = DirectBuf::zeroed(LWC_PAGE_PAYLOAD_SIZE);
        let page = LwcPage::try_from_bytes_mut(buf.data_mut()).unwrap();
        page.header = LwcPageHeader::new(100, 50, 2, 7);
        assert!(page.header.row_shape_fingerprint() == 100);
        assert!(page.header.row_count() == 50);
        assert!(page.header.col_count() == 2);
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
        let col_start = col_offsets_len;
        let col_end = col_start + column_bytes.len();
        page.header = LwcPageHeader::new(1, values.len() as u16, 1, 0);
        page.body[..col_offsets_len].copy_from_slice(&(col_end as u16).to_le_bytes());
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
        page.header = LwcPageHeader::new(1, 0, 2, 0);
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
            page.header = LwcPageHeader::new(11, 2, 1, 0);
            page.body[..2].copy_from_slice(&(3u16).to_le_bytes());
            page.body[2] = 0xAB;
        }
        let page = LwcPage::try_from_bytes(buf.data()).unwrap();
        assert_eq!(page.header.row_shape_fingerprint(), 11);
        assert_eq!(page.header.row_count(), 2);
        assert_eq!(page.header.col_count(), 1);
        assert_eq!(page.body[..2], (3u16).to_le_bytes());
        assert_eq!(page.body[2], 0xAB);
    }

    #[test]
    fn test_lwc_page_rejects_persisted_checksum_corruption() {
        let mut buf = build_persisted_lwc_page();
        let last_idx = buf.data().len() - 1;
        buf.data_mut()[last_idx] ^= 0xFF;

        let err = match LwcPage::try_from_persisted_bytes(
            buf.data(),
            PersistedFileKind::TableFile,
            BlockID::from(7),
        ) {
            Ok(_) => panic!("expected LWC checksum corruption"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id,
                cause: PersistedPageCorruptionCause::ChecksumMismatch,
            } if page_id == BlockID::from(7)
        ));
    }

    #[test]
    fn test_lwc_page_decode_persisted_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_page();
        let page = LwcPage::try_from_persisted_bytes(
            buf.data(),
            PersistedFileKind::TableFile,
            BlockID::from(8),
        )
        .unwrap();
        let vals = page
            .decode_persisted_row_values(
                &metadata,
                1,
                &[1, 0],
                PersistedFileKind::TableFile,
                BlockID::from(8),
            )
            .unwrap();
        assert_eq!(vals, vec![Val::Null, Val::U8(11)]);
        assert_eq!(
            page.row_shape_fingerprint(),
            row_shape_fingerprint_for(&[100, 101, 102])
        );
    }

    #[test]
    fn test_lwc_page_decode_persisted_full_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_page();
        let page = LwcPage::try_from_persisted_bytes(
            buf.data(),
            PersistedFileKind::TableFile,
            BlockID::from(8),
        )
        .unwrap();
        assert_eq!(
            page.decode_persisted_full_row_values(
                &metadata,
                0,
                PersistedFileKind::TableFile,
                BlockID::from(8),
            )
            .unwrap(),
            vec![Val::U8(10), Val::I16(20)]
        );
    }

    #[test]
    fn test_lwc_page_rejects_invalid_offsets_as_persisted_corruption() {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), LWC_PAGE_SPEC);
        let payload_end = payload_start + LWC_PAGE_PAYLOAD_SIZE;
        let page =
            LwcPage::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();
        let invalid_end = (page.body.len() as u16).saturating_add(1);
        page.header = LwcPageHeader::new(0, 1, 1, 0);
        page.body[..2].copy_from_slice(&invalid_end.to_le_bytes());
        write_page_checksum(buf.data_mut());

        let err = match LwcPage::try_from_persisted_bytes(
            buf.data(),
            PersistedFileKind::TableFile,
            BlockID::from(9),
        ) {
            Ok(_) => panic!("expected invalid persisted offsets"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            } if page_id == BlockID::from(9)
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
            let (start_idx, end_idx) = page.col_offsets().unwrap().get(0).unwrap();
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

        let page = LwcPage::try_from_persisted_bytes(
            buf.data(),
            PersistedFileKind::TableFile,
            BlockID::from(10),
        )
        .unwrap();
        let err = page
            .decode_persisted_full_row_values(
                &metadata,
                0,
                PersistedFileKind::TableFile,
                BlockID::from(10),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            } if page_id == BlockID::from(10)
        ));
    }
}
