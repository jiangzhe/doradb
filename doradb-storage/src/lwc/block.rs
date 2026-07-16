//! This module contains definition and functions of LWC(Lightweight Compression) Block.

use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableColumnLayout};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, FileKind, InternalError, InternalResult, Result,
};
use crate::file::SparseFile;
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, LWC_BLOCK_SPEC, max_payload_len, validate_block,
};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::id::BlockID;
use crate::layout;
use crate::lwc::{LwcData, LwcNullBitmap};
use crate::quiescent::QuiescentGuard;
use crate::serde::{Ser, Serde};
use crate::value::{Val, ValKind};
use error_stack::{Report, ResultExt};
use std::mem;
use std::sync::Arc;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Size in bytes of one validated persisted LWC payload, excluding the shared block envelope.
pub(crate) const LWC_BLOCK_PAYLOAD_SIZE: usize = max_payload_len(COW_FILE_PAGE_SIZE);

const LWC_BLOCK_HEADER_SIZE: usize = 32;
const _: () = assert!(mem::size_of::<LwcBlockHeader>() == LWC_BLOCK_HEADER_SIZE);
const _: () = assert!(mem::size_of::<LwcBlock>() == LWC_BLOCK_PAYLOAD_SIZE);
const _: () = assert!(mem::align_of::<LwcBlock>() == 1);

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
#[derive(Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub(crate) struct LwcBlock {
    /// Fixed block payload header.
    pub(crate) header: LwcBlockHeader,
    /// Serialized column offset table, column payloads, and deterministic padding.
    pub(crate) body: [u8; LWC_BLOCK_PAYLOAD_SIZE - mem::size_of::<LwcBlockHeader>()],
}

impl LwcBlock {
    /// Validates and borrows one raw LWC payload image.
    #[inline]
    pub(crate) fn try_from_bytes(input: &[u8]) -> DataIntegrityResult<&Self> {
        let block = layout::try_ref_from_bytes::<Self>(input)
            .change_context(DataIntegrityError::InvalidPayload)
            .attach_with(|| {
                format!(
                    "LWC block payload has invalid length {}, expected {}",
                    input.len(),
                    LWC_BLOCK_PAYLOAD_SIZE
                )
            })?;
        block.validate_structure()?;
        Ok(block)
    }

    /// Borrows an LWC payload that has already passed persisted-block validation.
    ///
    /// # Safety
    ///
    /// `input` must point to exactly one complete [`LwcBlock`] payload that has
    /// passed [`validate_persisted_lwc_block`]. The bytes must remain immutable
    /// and alive for the returned reference lifetime.
    #[inline]
    unsafe fn from_bytes_unchecked(input: &[u8]) -> &Self {
        // SAFETY: the caller guarantees a complete validated payload, `LwcBlock`
        // has alignment one, every field accepts every byte pattern, and the
        // returned reference inherits the input slice's lifetime.
        unsafe { &*input.as_ptr().cast::<Self>() }
    }

    /// Mutably borrows one raw LWC payload image with the expected payload size.
    #[inline]
    pub(crate) fn try_from_bytes_mut(input: &mut [u8]) -> InternalResult<&mut Self> {
        let input_len = input.len();
        layout::try_mut_from_bytes::<Self>(input)
            .change_context(InternalError::LwcBlockEncodingInvariant)
            .attach_with(|| {
                format!(
                    "mutable LWC block payload has invalid length {input_len}, expected {LWC_BLOCK_PAYLOAD_SIZE}"
                )
            })
    }

    /// Validates a full persisted LWC block image and returns its payload view.
    #[inline]
    pub(crate) fn try_from_persisted_bytes(
        input: &[u8],
        file_kind: FileKind,
        block_id: BlockID,
    ) -> DataIntegrityResult<&Self> {
        let payload = validate_block(input, LWC_BLOCK_SPEC)
            .attach_with(|| format!("file={file_kind}, block=lwc_block, block_id={block_id}"))?;
        Self::try_from_bytes(payload)
            .attach_with(|| format!("file={file_kind}, block=lwc_block, block_id={block_id}"))
    }

    /// Returns the canonical row-shape fingerprint stored in this block.
    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        self.header.row_shape_fingerprint()
    }

    /// Returns the number of rows encoded in this block.
    #[inline]
    pub(crate) fn row_count(&self) -> usize {
        self.header.row_count() as usize
    }

    #[inline]
    fn validate_structure(&self) -> DataIntegrityResult<()> {
        self.col_offsets()?.validate(self.body.len())
    }

    /// Returns column end offset array.
    #[inline]
    fn col_offsets(&self) -> DataIntegrityResult<ColOffsets<'_>> {
        let col_count = self.header.col_count() as usize;
        let end_idx = col_count * mem::size_of::<u16>();
        if end_idx > self.body.len() {
            return Err(invalid_lwc_payload(format!(
                "LWC column offset table length {end_idx} exceeds body length {}",
                self.body.len()
            )));
        }
        let raw = &self.body[..end_idx];
        let offsets = layout::slice_from_bytes::<[u8; 2]>(raw);
        Ok(ColOffsets {
            data_start: end_idx,
            offsets,
        })
    }

    /// Returns column data for given column index based on column layout.
    #[inline]
    pub(crate) fn column<'a>(
        &'a self,
        col_layout: &'a TableColumnLayout,
        col_idx: usize,
    ) -> DataIntegrityResult<LwcColumn<'a>> {
        if col_idx >= col_layout.col_count() {
            return Err(invalid_lwc_payload(format!(
                "LWC column metadata mismatch: col_idx={col_idx}, col_count={}",
                col_layout.col_count()
            )));
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
        let kind = col_layout.val_kind(col_idx);
        if col_layout.nullable(col_idx) {
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

    /// Decodes selected values from one block row in the requested column order.
    #[inline]
    pub(crate) fn decode_row_values(
        &self,
        col_layout: &TableColumnLayout,
        row_idx: usize,
        read_set: &[usize],
    ) -> DataIntegrityResult<Vec<Val>> {
        self.decode_row_values_inner(
            col_layout,
            row_idx,
            read_set.iter().copied(),
            read_set.len(),
        )
    }

    /// Decodes the index-key columns from one block row.
    #[inline]
    pub(crate) fn decode_index_key_values(
        &self,
        col_layout: &TableColumnLayout,
        index_spec: &IndexSpec,
        row_idx: usize,
    ) -> DataIntegrityResult<Vec<Val>> {
        self.decode_row_values_inner(
            col_layout,
            row_idx,
            index_spec.cols.iter().map(|key| key.col_no as usize),
            index_spec.cols.len(),
        )
    }

    /// Decodes all values from one block row.
    #[inline]
    pub(crate) fn decode_full_row_values(
        &self,
        col_layout: &TableColumnLayout,
        row_idx: usize,
    ) -> DataIntegrityResult<Vec<Val>> {
        self.decode_row_values_inner(
            col_layout,
            row_idx,
            0..col_layout.col_count(),
            col_layout.col_count(),
        )
    }

    #[inline]
    fn decode_row_values_inner(
        &self,
        col_layout: &TableColumnLayout,
        row_idx: usize,
        col_indices: impl Iterator<Item = usize>,
        capacity: usize,
    ) -> DataIntegrityResult<Vec<Val>> {
        let mut vals = Vec::with_capacity(capacity);
        for col_idx in col_indices {
            vals.push(self.decode_value(col_layout, row_idx, col_idx)?);
        }
        Ok(vals)
    }

    /// Decodes one value from a block row.
    #[inline]
    pub(crate) fn decode_value(
        &self,
        col_layout: &TableColumnLayout,
        row_idx: usize,
        col_idx: usize,
    ) -> DataIntegrityResult<Val> {
        let column = self.column(col_layout, col_idx)?;
        if column.is_null(row_idx) {
            return Ok(Val::Null);
        }
        let data = column.data()?;
        data.value(row_idx).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("LWC row index {row_idx} is out of range"))
        })
    }
}

/// Borrowed validated persisted LWC block backed by a readonly-cache guard.
pub(crate) struct PersistedLwcBlock {
    guard: ReadonlyBlockGuard,
}

impl PersistedLwcBlock {
    /// Loads one persisted LWC block through the validated readonly-cache path.
    #[inline]
    pub(crate) async fn load(
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
        Ok(PersistedLwcBlock { guard })
    }

    /// Returns a borrowed view of the validated LWC block payload.
    #[inline]
    pub(crate) fn block(&self) -> &LwcBlock {
        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        let payload = &self.guard.page()[payload_start..payload_end];
        // SAFETY: `load` obtains the immutable shared guard through
        // `read_validated_block` with `validate_persisted_lwc_block`, and the
        // fixed payload range has the exact size and alignment asserted above.
        unsafe { LwcBlock::from_bytes_unchecked(payload) }
    }
}

/// Borrowed view of one decoded LWC column payload.
#[derive(Debug)]
pub(crate) struct LwcColumn<'a> {
    kind: ValKind,
    row_count: usize,
    null_bitmap: Option<LwcNullBitmap<'a>>,
    values: &'a [u8],
}

impl<'a> LwcColumn<'a> {
    /// Returns whether the row at `row_idx` is marked null in this column.
    #[inline]
    pub(crate) fn is_null(&self, row_idx: usize) -> bool {
        if row_idx >= self.row_count {
            return false;
        }
        self.null_bitmap
            .as_ref()
            .map(|bitmap| bitmap.is_null(row_idx))
            .unwrap_or(false)
    }

    /// Parses the compressed value payload for this column.
    #[inline]
    pub(crate) fn data(&self) -> DataIntegrityResult<LwcData<'a>> {
        LwcData::from_bytes(self.kind, self.values)
    }

    /// Returns the number of rows represented by this column.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved row_count"))]
    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }
}

/// Borrowed column end-offset table from an LWC block body.
pub(crate) struct ColOffsets<'a> {
    data_start: usize,
    offsets: &'a [[u8; 2]],
}

impl ColOffsets<'_> {
    /// Validates that all column ranges are monotonic and within the body.
    #[inline]
    pub(crate) fn validate(&self, body_len: usize) -> DataIntegrityResult<()> {
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
    pub(crate) fn get(&self, idx: usize) -> Option<(usize, usize)> {
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

/// Header of one LWC block.
/// The fields are all defined as byte array
/// to avoid endianess mistakes in serialization and deserialization.
#[repr(C)]
#[derive(Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub(crate) struct LwcBlockHeader {
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
    /// Creates a deterministic block header from logical header fields.
    #[inline]
    pub(crate) fn new(
        row_shape_fingerprint: u128,
        row_count: u16,
        col_count: u16,
        flags: u16,
    ) -> Self {
        LwcBlockHeader {
            row_shape_fingerprint: row_shape_fingerprint.to_le_bytes(),
            row_count: row_count.to_le_bytes(),
            col_count: col_count.to_le_bytes(),
            flags: flags.to_le_bytes(),
            reserved: [0u8; 10],
        }
    }

    /// Returns the canonical row-shape fingerprint.
    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        u128::from_le_bytes(self.row_shape_fingerprint)
    }

    /// Returns the number of rows encoded in the block.
    #[inline]
    pub(crate) fn row_count(&self) -> u16 {
        u16::from_le_bytes(self.row_count)
    }

    /// Returns the number of columns encoded in the block.
    #[inline]
    pub(crate) fn col_count(&self) -> u16 {
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

/// Validates one persisted LWC block image for readonly-cache residency.
#[inline]
pub(crate) fn validate_persisted_lwc_block(
    input: &[u8],
    file_kind: FileKind,
    block_id: BlockID,
) -> DataIntegrityResult<()> {
    LwcBlock::try_from_persisted_bytes(input, file_kind, block_id).map(|_| ())
}

#[inline]
fn invalid_lwc_payload(message: impl Into<String>) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload).attach(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::error::{DataIntegrityError, Error, FileKind};
    use crate::file::block_integrity::{
        BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum, write_block_header,
    };
    use crate::file::test_block_id;
    use crate::id::RowID;
    use crate::index::ColumnBlockEntryShape;
    use crate::io::DirectBuf;
    use crate::lwc::{LwcBuilder, LwcCode, LwcNullBitmapSer, LwcPrimitiveSer};
    use crate::row::{InsertRow, RowPage};
    use crate::value::Val;
    use error_stack::ResultExt;

    fn row_shape_fingerprint_for(row_ids: &[RowID]) -> u128 {
        let start_row_id = row_ids.first().unwrap().as_u64();
        let end_row_id = row_ids.last().unwrap().as_u64().saturating_add(1);
        ColumnBlockEntryShape::new(
            RowID::new(start_row_id),
            RowID::new(end_row_id),
            row_ids.to_vec(),
            Vec::new(),
        )
        .unwrap()
        .row_shape_fingerprint()
    }

    fn assert_lwc_data_integrity(err: Error, block_id: BlockID, expected: DataIntegrityError) {
        assert_eq!(err.data_integrity_error(), Some(expected));
        let report = format!("{err:?}");
        assert!(report.contains("table_file"), "{report}");
        assert!(report.contains("lwc_block"), "{report}");
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
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I16, ColumnAttributes::NULLABLE),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 8, metadata.col.as_ref());
        assert!(matches!(
            page.insert(metadata.col.as_ref(), &[Val::U8(10), Val::I16(20)]),
            InsertRow::Ok(_)
        ));
        assert!(matches!(
            page.insert(metadata.col.as_ref(), &[Val::U8(11), Val::Null]),
            InsertRow::Ok(_)
        ));
        assert!(matches!(
            page.insert(metadata.col.as_ref(), &[Val::U8(12), Val::I16(22)]),
            InsertRow::Ok(_)
        ));
        let buf = {
            let mut builder = LwcBuilder::new(metadata.col.as_ref());
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
        assert_eq!(&header_vec, layout::bytes_of(&page.header));
    }

    #[test]
    fn test_lwc_block_nullable_column() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U8,
                ColumnAttributes::NULLABLE,
            )],
            vec![],
        )
        .expect("valid table metadata");
        let values = [10u8, 20, 30, 40];
        let lwc_ser = LwcPrimitiveSer::new_u8(&values);
        let mut values_bytes = vec![0u8; lwc_ser.ser_len()];
        lwc_ser.ser(&mut values_bytes[..], 0);

        let null_bytes = [0b0000_1010u8];
        let null_ser = LwcNullBitmapSer::new(&null_bytes).unwrap();
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

        let column = page.column(metadata.col.as_ref(), 0).unwrap();
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
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U8,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut buf = DirectBuf::zeroed(LWC_BLOCK_PAYLOAD_SIZE);
        let page = LwcBlock::try_from_bytes_mut(buf.data_mut()).unwrap();
        let col_offsets_len = mem::size_of::<u16>() * 2;
        let end_offset = col_offsets_len as u16;
        page.header = LwcBlockHeader::new(1, 0, 2, 0);
        page.body[..2].copy_from_slice(&end_offset.to_le_bytes());
        page.body[2..4].copy_from_slice(&end_offset.to_le_bytes());

        let err = page.column(metadata.col.as_ref(), 1);
        assert!(
            err.as_ref()
                .is_err_and(|err| *err.current_context() == DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_lwc_block_try_from_bytes_invalid_len() {
        let bytes = [0u8; LWC_BLOCK_PAYLOAD_SIZE - 1];
        let err = LwcBlock::try_from_bytes(&bytes);
        assert!(
            err.as_ref()
                .is_err_and(|err| *err.current_context() == DataIntegrityError::InvalidPayload)
        );
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
        assert_lwc_data_integrity(
            Error::from(err),
            test_block_id(7),
            DataIntegrityError::ChecksumMismatch,
        );
    }

    #[test]
    fn test_lwc_block_decode_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_block();
        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(8))
                .unwrap();
        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        // SAFETY: `try_from_persisted_bytes` above validated this exact immutable
        // payload, including its fixed size and LWC structure.
        let unchecked_page =
            unsafe { LwcBlock::from_bytes_unchecked(&buf.data()[payload_start..payload_end]) };
        assert!(std::ptr::eq(page, unchecked_page));

        assert_eq!(
            unchecked_page
                .decode_value(metadata.col.as_ref(), 1, 0)
                .unwrap(),
            Val::U8(11)
        );
        assert_eq!(
            unchecked_page
                .decode_value(metadata.col.as_ref(), 1, 1)
                .unwrap(),
            Val::Null
        );
        let vals = page
            .decode_row_values(metadata.col.as_ref(), 1, &[1, 0])
            .unwrap();
        assert_eq!(vals, vec![Val::Null, Val::U8(11)]);
        assert_eq!(
            page.row_shape_fingerprint(),
            row_shape_fingerprint_for(&[RowID::new(100), RowID::new(101), RowID::new(102)])
        );

        let err = page.decode_value(metadata.col.as_ref(), 3, 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);

        let err = page.decode_value(metadata.col.as_ref(), 0, 2).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_lwc_block_decode_full_row_values() {
        let (metadata, buf) = build_valid_persisted_lwc_block();
        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(8))
                .unwrap();
        assert_eq!(
            page.decode_full_row_values(metadata.col.as_ref(), 0)
                .unwrap(),
            vec![Val::U8(10), Val::I16(20)]
        );
    }

    #[test]
    fn test_lwc_block_decode_stable_across_index_only_metadata_changes() {
        let (metadata, buf) = build_valid_persisted_lwc_block();
        let (index_no, indexed_metadata) = metadata
            .try_with_created_index(IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK))
            .unwrap();
        let dropped_metadata = indexed_metadata.try_without_index(index_no).unwrap();
        let page =
            LwcBlock::try_from_persisted_bytes(buf.data(), FileKind::TableFile, test_block_id(8))
                .unwrap();

        let expected = vec![Val::U8(11), Val::Null];
        assert_eq!(
            page.decode_full_row_values(metadata.col.as_ref(), 1)
                .unwrap(),
            expected
        );
        assert_eq!(
            page.decode_full_row_values(indexed_metadata.col.as_ref(), 1)
                .unwrap(),
            expected
        );
        assert_eq!(
            page.decode_row_values(dropped_metadata.col.as_ref(), 1, &[1, 0])
                .unwrap(),
            vec![Val::Null, Val::U8(11)]
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
        assert_lwc_data_integrity(
            Error::from(err),
            test_block_id(9),
            DataIntegrityError::InvalidPayload,
        );
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
        let err = page.decode_value(metadata.col.as_ref(), 0, 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        let err = page
            .decode_value(metadata.col.as_ref(), 0, 0)
            .attach_with(|| {
                format!(
                    "file={}, block=lwc_block, block_id={}",
                    FileKind::TableFile,
                    test_block_id(10)
                )
            })
            .unwrap_err();
        let report = format!("{err:?}");
        assert_eq!(report.matches("block_id=10").count(), 1, "{report}");
        assert_lwc_data_integrity(
            err.into(),
            test_block_id(10),
            DataIntegrityError::InvalidPayload,
        );
    }
}
