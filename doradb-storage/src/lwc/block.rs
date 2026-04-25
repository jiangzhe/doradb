//! This module contains definition and functions of LWC(Lightweight Compression) Block.

use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::catalog::TableMetadata;
use crate::error::{DataIntegrityError, Error, FileKind, OperationError, Result};
use crate::file::SparseFile;
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, LWC_BLOCK_SPEC, max_payload_len, validate_block,
};
use crate::file::cow_file::{BlockID, COW_FILE_PAGE_SIZE};
use crate::lwc::{LwcData, LwcNullBitmap};
use crate::quiescent::QuiescentGuard;
use crate::serde::{Ser, Serde};
use crate::value::{Val, ValKind};
use bytemuck::{Pod, Zeroable};
use error_stack::{Report, ResultExt};
use std::mem;
use std::sync::Arc;

/// Size in bytes of one validated persisted LWC payload, excluding the shared block envelope.
pub const LWC_BLOCK_PAYLOAD_SIZE: usize = max_payload_len(COW_FILE_PAGE_SIZE);

#[inline]
fn invalid_lwc_payload(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

#[inline]
fn persisted_lwc_payload_error(
    file_kind: FileKind,
    block_id: BlockID,
    message: impl Into<String>,
) -> Error {
    let message = message.into();
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(format!(
            "file={file_kind}, block=lwc-block, block_id={block_id}, {message}"
        ))
        .into()
}

/// LwcBlock stores the payload of one immutable checksummed LWC block.
///
/// The surrounding block-integrity envelope is validated before this payload is
/// interpreted. The differences between LwcBlock and row(PAX) page
/// are:
/// 1. Fields in row page are well aligned for typed access.
///    Fields in an LWC block are not aligned, and often compressed
///    via dict/bitpacking. So access single field is often directly
///    performed on compressed data.
/// 2. Row page is mutable so there is always a hybried lock associated
///    to it and a row-level lock held by undo entry. There is also
///    delete bit for each row.
///    LWC block is immutable and values are compressed. Delete bitmap is
///    separated in a standalone block. Cold-row identity is owned by the
///    column-block index, so persisted LWC block access uses ordinal positions
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
pub struct LwcBlock {
    // The conversion from raw bytes to payload view is not endianness-safe for
    // arbitrary external input. Persisted callers must validate the outer page
    // envelope first and only then cast the fixed-size payload bytes.
    pub header: LwcBlockHeader,
    pub body: [u8; LWC_BLOCK_PAYLOAD_SIZE - mem::size_of::<LwcBlockHeader>()],
}

// SAFETY: `LwcBlock` is `repr(C)` and consists only of byte-array based fields.
// Any bit pattern is valid and there is no interior mutability or drop glue.
unsafe impl Zeroable for LwcBlock {}
// SAFETY: same reasoning as above; plain immutable byte data layout.
unsafe impl Pod for LwcBlock {}

impl LwcBlock {
    pub const BODY_SIZE: usize = LWC_BLOCK_PAYLOAD_SIZE - mem::size_of::<LwcBlockHeader>();

    #[inline]
    pub fn try_from_bytes(input: &[u8]) -> Result<&Self> {
        let block = bytemuck::try_from_bytes::<Self>(input).map_err(|_| {
            invalid_lwc_payload(format!(
                "LWC block payload has invalid length {}, expected {}",
                input.len(),
                LWC_BLOCK_PAYLOAD_SIZE
            ))
        })?;
        block.validate_structure()?;
        Ok(block)
    }

    #[inline]
    pub fn try_from_bytes_mut(input: &mut [u8]) -> Result<&mut Self> {
        let input_len = input.len();
        bytemuck::try_from_bytes_mut::<Self>(input).map_err(|_| {
            invalid_lwc_payload(format!(
                "mutable LWC block payload has invalid length {input_len}, expected {LWC_BLOCK_PAYLOAD_SIZE}"
            ))
        })
    }

    /// Validates a full persisted LWC block image and returns its payload view.
    #[inline]
    pub fn try_from_persisted_bytes(
        input: &[u8],
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<&Self> {
        let payload = validate_block(input, LWC_BLOCK_SPEC)
            .attach_with(|| format!("file={file_kind}, block=lwc-block, block_id={block_id}"))
            .map_err(Error::from)?;
        Self::try_from_bytes(payload)
            .map_err(|err| map_persisted_lwc_error(file_kind, block_id, err))
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
            return Err(invalid_lwc_payload(format!(
                "LWC column offset table length {end_idx} exceeds body length {}",
                self.body.len()
            )));
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
            return Err(Error::index_out_of_bound());
        }
        let (start_idx, end_idx) = self.col_offsets().and_then(|offsets| {
            offsets.get(col_idx).ok_or_else(|| {
                invalid_lwc_payload(format!("LWC column index {col_idx} is out of range"))
            })
        })?;
        if end_idx > self.body.len() || start_idx > end_idx {
            return Err(invalid_lwc_payload(format!(
                "invalid LWC column slice start={start_idx}, end={end_idx}, body_len={}",
                self.body.len()
            )));
        }
        let data = &self.body[start_idx..end_idx];
        let row_count = self.header.row_count() as usize;
        let kind = metadata.val_kind(col_idx);
        if metadata.nullable(col_idx) {
            let (bitmap, values) = LwcNullBitmap::from_bytes(data)?;
            let required = row_count.div_ceil(8);
            if bitmap.len() < required {
                return Err(invalid_lwc_payload(format!(
                    "LWC null bitmap length {} is shorter than required {required}",
                    bitmap.len()
                )));
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

    /// Decodes selected values from one persisted block row in the requested
    /// column order and maps payload failures to contextual persisted-block
    /// corruption.
    #[inline]
    pub fn decode_persisted_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        read_set: &[usize],
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<Vec<Val>> {
        self.decode_persisted_row_values_inner(
            metadata,
            row_idx,
            read_set.iter().copied(),
            read_set.len(),
            file_kind,
            block_id,
        )
    }

    /// Decodes all values from one persisted block row and maps payload
    /// failures to contextual persisted-block corruption.
    #[inline]
    pub fn decode_persisted_full_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<Vec<Val>> {
        self.decode_persisted_row_values_inner(
            metadata,
            row_idx,
            0..metadata.col_count(),
            metadata.col_count(),
            file_kind,
            block_id,
        )
    }

    #[inline]
    fn decode_persisted_row_values_inner(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        col_indices: impl Iterator<Item = usize>,
        capacity: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<Vec<Val>> {
        let mut vals = Vec::with_capacity(capacity);
        for col_idx in col_indices {
            vals.push(
                self.decode_persisted_value(metadata, row_idx, col_idx, file_kind, block_id)?,
            );
        }
        Ok(vals)
    }

    #[inline]
    fn decode_persisted_value(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        col_idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<Val> {
        let column = self
            .column(metadata, col_idx)
            .map_err(|err| map_persisted_lwc_error(file_kind, block_id, err))?;
        if column.is_null(row_idx) {
            return Ok(Val::Null);
        }
        let data = column
            .data()
            .map_err(|err| map_persisted_lwc_error(file_kind, block_id, err))?;
        data.value(row_idx)
            .ok_or_else(|| invalid_lwc_payload(format!("LWC row index {row_idx} is out of range")))
            .map_err(|err| map_persisted_lwc_error(file_kind, block_id, err))
    }
}

/// Validates one persisted LWC block image for readonly-cache residency.
#[inline]
pub(crate) fn validate_persisted_lwc_block(
    input: &[u8],
    file_kind: FileKind,
    block_id: BlockID,
) -> Result<()> {
    LwcBlock::try_from_persisted_bytes(input, file_kind, block_id).map(|_| ())
}

/// Borrowed validated persisted LWC block backed by a readonly-cache guard.
pub(crate) struct PersistedLwcBlock {
    guard: ReadonlyBlockGuard,
    file_kind: FileKind,
    block_id: BlockID,
}

impl PersistedLwcBlock {
    /// Loads one persisted LWC block through the validated readonly-cache path.
    #[inline]
    pub async fn load(
        file_kind: FileKind,
        file: &Arc<SparseFile>,
        disk_pool: &QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &PoolGuard,
        block_id: BlockID,
    ) -> Result<Self> {
        let guard = disk_pool
            .read_validated_block(
                file_kind,
                file,
                disk_pool_guard,
                block_id,
                validate_persisted_lwc_block,
            )
            .await?;
        Ok(PersistedLwcBlock {
            guard,
            file_kind,
            block_id,
        })
    }

    #[inline]
    fn block(&self) -> &LwcBlock {
        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        LwcBlock::try_from_bytes(&self.guard.page()[payload_start..payload_end])
            .expect("validated readonly LWC block must have a valid payload layout")
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.block().header.row_count() as usize
    }

    #[inline]
    pub fn row_shape_fingerprint(&self) -> u128 {
        self.block().row_shape_fingerprint()
    }

    #[inline]
    pub fn decode_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
        read_set: &[usize],
    ) -> Result<Vec<Val>> {
        self.block().decode_persisted_row_values(
            metadata,
            row_idx,
            read_set,
            self.file_kind,
            self.block_id,
        )
    }

    #[inline]
    pub fn decode_full_row_values(
        &self,
        metadata: &TableMetadata,
        row_idx: usize,
    ) -> Result<Vec<Val>> {
        self.block().decode_persisted_full_row_values(
            metadata,
            row_idx,
            self.file_kind,
            self.block_id,
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
            return Err(invalid_lwc_payload(format!(
                "LWC data start {start_idx} exceeds body length {body_len}"
            )));
        }
        for offset in self.offsets {
            let end_idx = u16::from_le_bytes(*offset) as usize;
            if end_idx > body_len || start_idx > end_idx {
                return Err(invalid_lwc_payload(format!(
                    "invalid LWC column offset start={start_idx}, end={end_idx}, body_len={body_len}"
                )));
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

const LWC_BLOCK_HEADER_SIZE: usize = 32;
const _: () = assert!(mem::size_of::<LwcBlockHeader>() == LWC_BLOCK_HEADER_SIZE);
const _: () = assert!(mem::size_of::<LwcBlock>() == LWC_BLOCK_PAYLOAD_SIZE);

/// Header of one LWC block.
/// The fields are all defined as byte array
/// to avoid endianess mistakes in serialization and deserialization.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct LwcBlockHeader {
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

impl LwcBlockHeader {
    #[inline]
    pub fn new(row_shape_fingerprint: u128, row_count: u16, col_count: u16, flags: u16) -> Self {
        LwcBlockHeader {
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

impl Ser<'_> for LwcBlockHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<LwcBlockHeader>()
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
pub(crate) fn map_persisted_lwc_error(file_kind: FileKind, block_id: BlockID, err: Error) -> Error {
    if err.data_integrity_error().is_some()
        || err.operation_error() == Some(OperationError::NotSupported)
    {
        persisted_lwc_payload_error(file_kind, block_id, "invalid persisted LWC payload")
    } else {
        err
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::error::{DataIntegrityError, Error, FileKind};
    use crate::file::block_integrity::{
        BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum, write_block_header,
    };
    use crate::file::test_block_id;
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

    fn assert_lwc_data_integrity(
        err: Error,
        block_id: crate::file::cow_file::BlockID,
        expected: DataIntegrityError,
    ) {
        assert_eq!(err.data_integrity_error(), Some(expected));
        let report = format!("{err:?}");
        assert!(report.contains("table-file"), "{report}");
        assert!(report.contains("lwc-block"), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    fn build_persisted_lwc_block() -> DirectBuf {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), LWC_BLOCK_SPEC);
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        let page =
            LwcBlock::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();
        page.header = LwcBlockHeader::new(11, 1, 1, 0);
        page.body[..2].copy_from_slice(&(2u16).to_le_bytes());
        write_block_checksum(buf.data_mut());
        buf
    }

    fn build_valid_persisted_lwc_block() -> (TableMetadata, DirectBuf) {
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
    fn test_lwc_block() {
        let mut buf = DirectBuf::zeroed(LWC_BLOCK_PAYLOAD_SIZE);
        let page = LwcBlock::try_from_bytes_mut(buf.data_mut()).unwrap();
        page.header = LwcBlockHeader::new(100, 50, 2, 7);
        assert!(page.header.row_shape_fingerprint() == 100);
        assert!(page.header.row_count() == 50);
        assert!(page.header.col_count() == 2);
        let mut header_vec = vec![0u8; page.header.ser_len()];
        let ser_idx = page.header.ser(&mut header_vec[..], 0);
        assert!(ser_idx == header_vec.len());
        assert_eq!(&header_vec, bytemuck::bytes_of(&page.header));
    }

    #[test]
    fn test_lwc_block_nullable_column() {
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

        let mut buf = DirectBuf::zeroed(LWC_BLOCK_PAYLOAD_SIZE);
        let page = LwcBlock::try_from_bytes_mut(buf.data_mut()).unwrap();
        let col_offsets_len = mem::size_of::<u16>();
        let col_start = col_offsets_len;
        let col_end = col_start + column_bytes.len();
        page.header = LwcBlockHeader::new(1, values.len() as u16, 1, 0);
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
    fn test_lwc_block_column_metadata_mismatch() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U8,
                ColumnAttributes::empty(),
            )],
            vec![],
        );
        let mut buf = DirectBuf::zeroed(LWC_BLOCK_PAYLOAD_SIZE);
        let page = LwcBlock::try_from_bytes_mut(buf.data_mut()).unwrap();
        let col_offsets_len = mem::size_of::<u16>() * 2;
        let end_offset = col_offsets_len as u16;
        page.header = LwcBlockHeader::new(1, 0, 2, 0);
        page.body[..2].copy_from_slice(&end_offset.to_le_bytes());
        page.body[2..4].copy_from_slice(&end_offset.to_le_bytes());

        let err = page.column(&metadata, 1);
        assert!(
            err.as_ref()
                .is_err_and(|err| err.is_code(crate::error::ErrorCode::IndexOutOfBound))
        );
    }

    #[test]
    fn test_lwc_block_try_from_bytes_invalid_len() {
        let bytes = [0u8; LWC_BLOCK_PAYLOAD_SIZE - 1];
        let err = LwcBlock::try_from_bytes(&bytes);
        assert!(err.as_ref().is_err_and(
            |err| err.data_integrity_error() == Some(DataIntegrityError::InvalidPayload)
        ));
    }

    #[test]
    fn test_lwc_block_try_from_bytes_mut_roundtrip() {
        let mut buf = DirectBuf::zeroed(LWC_BLOCK_PAYLOAD_SIZE);
        {
            let page = LwcBlock::try_from_bytes_mut(buf.data_mut()).unwrap();
            page.header = LwcBlockHeader::new(11, 2, 1, 0);
            page.body[..2].copy_from_slice(&(3u16).to_le_bytes());
            page.body[2] = 0xAB;
        }
        let page = LwcBlock::try_from_bytes(buf.data()).unwrap();
        assert_eq!(page.header.row_shape_fingerprint(), 11);
        assert_eq!(page.header.row_count(), 2);
        assert_eq!(page.header.col_count(), 1);
        assert_eq!(page.body[..2], (3u16).to_le_bytes());
        assert_eq!(page.body[2], 0xAB);
    }

    #[test]
    fn test_lwc_block_rejects_persisted_checksum_corruption() {
        let mut buf = build_persisted_lwc_block();
        let last_idx = buf.data().len() - 1;
        buf.data_mut()[last_idx] ^= 0xFF;

        let err = match LwcBlock::try_from_persisted_bytes(
            buf.data(),
            FileKind::TableFile,
            test_block_id(7),
        ) {
            Ok(_) => panic!("expected LWC checksum corruption"),
            Err(err) => err,
        };
        assert_lwc_data_integrity(err, test_block_id(7), DataIntegrityError::ChecksumMismatch);
    }

    #[test]
    fn test_lwc_block_decode_persisted_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_block();
        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(8))
                .unwrap();
        let vals = page
            .decode_persisted_row_values(
                &metadata,
                1,
                &[1, 0],
                FileKind::TableFile,
                test_block_id(8),
            )
            .unwrap();
        assert_eq!(vals, vec![Val::Null, Val::U8(11)]);
        assert_eq!(
            page.row_shape_fingerprint(),
            row_shape_fingerprint_for(&[100, 101, 102])
        );
    }

    #[test]
    fn test_lwc_block_decode_persisted_full_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_block();
        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(8))
                .unwrap();
        assert_eq!(
            page.decode_persisted_full_row_values(
                &metadata,
                0,
                FileKind::TableFile,
                test_block_id(8),
            )
            .unwrap(),
            vec![Val::U8(10), Val::I16(20)]
        );
    }

    #[test]
    fn test_lwc_block_rejects_invalid_offsets_as_persisted_corruption() {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), LWC_BLOCK_SPEC);
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        let page =
            LwcBlock::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]).unwrap();
        let invalid_end = (page.body.len() as u16).saturating_add(1);
        page.header = LwcBlockHeader::new(0, 1, 1, 0);
        page.body[..2].copy_from_slice(&invalid_end.to_le_bytes());
        write_block_checksum(buf.data_mut());

        let err = match LwcBlock::try_from_persisted_bytes(
            buf.data(),
            FileKind::TableFile,
            test_block_id(9),
        ) {
            Ok(_) => panic!("expected invalid persisted offsets"),
            Err(err) => err,
        };
        assert_lwc_data_integrity(err, test_block_id(9), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_lwc_block_maps_persisted_value_decode_error_to_corruption() {
        let (metadata, mut buf) = build_valid_persisted_lwc_block();
        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        {
            let page =
                LwcBlock::try_from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end])
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
        write_block_checksum(buf.data_mut());

        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(10))
                .unwrap();
        let err = page
            .decode_persisted_full_row_values(&metadata, 0, FileKind::TableFile, test_block_id(10))
            .unwrap_err();
        assert_lwc_data_integrity(err, test_block_id(10), DataIntegrityError::InvalidPayload);
    }
}
