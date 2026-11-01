use crate::buffer::page::PAGE_SIZE;
use crate::catalog::{IndexSlot, PrimaryKeyMatchError, TableIndexMetadata, TableMetadata};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::row::{RowRead, RowValues, UpdateValues};
use crate::value::{MemVar, PAGE_VAR_LEN_INLINE, Val, ValRef, checked_var_len};
use error_stack::Report;
use std::ops::{Bound, RangeBounds};
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

/// DML validation failures before a caller assigns foreground or recovery context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum DmlValidationError {
    /// A full row does not match the table's row shape.
    #[error("invalid DML row shape")]
    RowShape,
    /// The complete logical row cannot fit a fresh page or singleton block.
    #[error("DML row exceeds physical capacity")]
    RowCapacity,
    /// A sparse update does not match the table's update contract.
    #[error("invalid sparse DML update")]
    SparseUpdate,
    /// An index reference or key does not match table index metadata.
    #[error("invalid DML index key")]
    IndexKey,
    /// A projection read set is invalid.
    #[error("invalid DML read set")]
    ReadSet,
    /// A primary-key reference does not match table metadata.
    #[error("invalid DML primary key")]
    PrimaryKey,
}

/// Result carrying caller-neutral DML validation reports.
pub(crate) type DmlValidationResult<T> = StdResult<T, Report<DmlValidationError>>;

/// Variable-byte measurement shared by mandatory admission and page planning.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowSize {
    total_var_bytes: usize,
    /// Variable bytes stored outside the RowPage's inline descriptors.
    pub(crate) outlined_var_bytes: usize,
}

/// Validates DML payloads against one table's metadata.
pub(crate) struct DmlValidator<'m> {
    metadata: &'m TableMetadata,
}

impl<'m> DmlValidator<'m> {
    /// Creates a validator for one table layout.
    #[inline]
    pub(crate) fn new(metadata: &'m TableMetadata) -> Self {
        Self { metadata }
    }

    /// Enforces aggregate physical capacity independently of optional shape validation.
    #[inline]
    pub(crate) fn validate_row_capacity<R: RowValues + ?Sized>(
        &self,
        vals: &R,
    ) -> DmlValidationResult<RowSize> {
        self.measure_capacity(|col| variable_len(vals.value(col)))
    }

    /// Checks a complete decoded row with a sorted sparse overlay, without copying payloads.
    #[inline]
    pub(crate) fn validate_updated_row_capacity<R: RowValues + ?Sized, U: UpdateValues + ?Sized>(
        &self,
        old: &R,
        update: &U,
    ) -> DmlValidationResult<RowSize> {
        self.measure_updated_capacity(update, |col| variable_len(old.value(col)))
    }

    /// Checks the latest owned hot image, ignoring stale variable fields beneath null bits.
    #[inline]
    pub(crate) fn validate_hot_update_capacity<R: RowRead, U: UpdateValues + ?Sized>(
        &self,
        old: &R,
        update: &U,
    ) -> DmlValidationResult<RowSize> {
        self.measure_updated_capacity(update, |col| {
            if old.is_null(&self.metadata.col, col) {
                0
            } else {
                old.var(col).len()
            }
        })
    }

    #[inline]
    fn measure_updated_capacity<U: UpdateValues + ?Sized>(
        &self,
        update: &U,
        mut old_len: impl FnMut(usize) -> usize,
    ) -> DmlValidationResult<RowSize> {
        let mut position = 0;
        self.measure_capacity(|col| {
            while position < update.len() {
                let (updated_col, value) = update.value(position);
                if updated_col > col {
                    break;
                }
                position += 1;
                if updated_col == col {
                    return variable_len(value);
                }
            }
            old_len(col)
        })
    }

    #[inline]
    fn measure_capacity(
        &self,
        mut length: impl FnMut(usize) -> usize,
    ) -> DmlValidationResult<RowSize> {
        let mut size = RowSize::default();
        for &col in self.metadata.col.var_cols() {
            let len = length(col);
            size.total_var_bytes = size
                .total_var_bytes
                .checked_add(len)
                .ok_or_else(|| self.capacity_error(size, "variable-byte sum overflow"))?;
            if len > PAGE_VAR_LEN_INLINE {
                size.outlined_var_bytes = size
                    .outlined_var_bytes
                    .checked_add(len)
                    .ok_or_else(|| self.capacity_error(size, "outlined-byte sum overflow"))?;
            }
        }
        let layout = self.metadata.col.row_size();
        if layout
            .row_page_bytes(size.outlined_var_bytes)
            .is_none_or(|bytes| bytes > PAGE_SIZE)
        {
            return Err(self.capacity_error(size, "RowPage bound or size arithmetic overflow"));
        }
        if layout
            .lwc_bytes(size.total_var_bytes)
            .is_none_or(|bytes| bytes > COW_FILE_PAGE_SIZE)
        {
            return Err(self.capacity_error(size, "LWC bound or size arithmetic overflow"));
        }
        Ok(size)
    }

    #[cold]
    fn capacity_error(&self, size: RowSize, reason: &str) -> Report<DmlValidationError> {
        let layout = self.metadata.col.row_size();
        Report::new(DmlValidationError::RowCapacity).attach(format!(
            "row_page_bytes={:?}, row_page_capacity={PAGE_SIZE}, lwc_bytes={:?}, lwc_capacity={COW_FILE_PAGE_SIZE}, reason={reason}",
            layout.row_page_bytes(size.outlined_var_bytes),
            layout.lwc_bytes(size.total_var_bytes),
        ))
    }

    /// Validates a repeatable full-row input without taking ownership of values.
    #[inline]
    pub(crate) fn validate_full_row<R: RowValues + ?Sized>(
        &self,
        vals: &R,
    ) -> DmlValidationResult<()> {
        if vals.len() != self.metadata.col.col_count() {
            return Err(Report::new(DmlValidationError::RowShape).attach(format!(
                "row value count mismatch: actual={}, expected={}",
                vals.len(),
                self.metadata.col.col_count()
            )));
        }
        for col_no in 0..vals.len() {
            let val = vals.value(col_no);
            if !self.metadata.col.col_type_match_ref(col_no, val) {
                return Err(Report::new(DmlValidationError::RowShape).attach(format!(
                    "row value type mismatch: column_no={col_no}, expected={:?}, actual={val:?}",
                    self.metadata.col.col_type(col_no)
                )));
            }
            if let ValRef::VarByte(bytes) = val
                && checked_var_len(bytes.len()).is_none()
            {
                return Err(Report::new(DmlValidationError::RowShape).attach(format!(
                    "row byte length exceeds limit: column_no={col_no}, len={}, max={}",
                    bytes.len(),
                    MemVar::MAX_LEN
                )));
            }
        }
        Ok(())
    }

    /// Validates sparse input in its original order, including duplicate ordinals.
    #[inline]
    pub(crate) fn validate_sparse_update<U: UpdateValues + ?Sized>(
        &self,
        update: &U,
    ) -> DmlValidationResult<()> {
        let mut last_idx = None;
        for position in 0..update.len() {
            let (col_idx, val) = update.value(position);
            if col_idx >= self.metadata.col.col_count() {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update column out of range: column_no={}, column_count={}",
                        col_idx,
                        self.metadata.col.col_count()
                    )),
                );
            }
            if last_idx.is_some_and(|idx| col_idx <= idx) {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update columns not strictly ordered: column_no={}",
                        col_idx
                    )),
                );
            }
            if !self.metadata.col.col_type_match_ref(col_idx, val) {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update column type mismatch: column_no={}, expected={:?}, actual={:?}",
                        col_idx,
                        self.metadata.col.col_type(col_idx),
                        val
                    )),
                );
            }
            if let ValRef::VarByte(bytes) = val
                && checked_var_len(bytes.len()).is_none()
            {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update byte length exceeds limit: column_no={col_idx}, len={}, max={}",
                        bytes.len(),
                        MemVar::MAX_LEN
                    )),
                );
            }
            last_idx = Some(col_idx);
        }
        Ok(())
    }

    /// Validates a unique-index DML key against table index metadata.
    #[inline]
    pub(crate) fn validate_unique_key(
        &self,
        index_slot: IndexSlot,
        key_vals: &[Val],
    ) -> DmlValidationResult<&'m TableIndexMetadata> {
        let index_spec = self.validate_unique_index(index_slot)?;
        self.validate_index_values(index_slot, index_spec, key_vals)?;
        Ok(index_spec)
    }

    /// Validates that a DML operation targets an active unique index.
    #[inline]
    pub(crate) fn validate_unique_index(
        &self,
        index_slot: IndexSlot,
    ) -> DmlValidationResult<&'m TableIndexMetadata> {
        let Some(index_spec) = self.metadata.idx.index_spec(index_slot) else {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "unique index not found: index_slot={}, index_slot_count={}",
                index_slot,
                self.metadata.idx.index_slot_count()
            )));
        };
        if !index_spec.unique() {
            return Err(Report::new(DmlValidationError::IndexKey)
                .attach(format!("index is not unique: index_slot={index_slot}")));
        }
        Ok(index_spec)
    }

    /// Validates a secondary-index scan request against table metadata.
    #[inline]
    pub(crate) fn validate_index_scan<'r, R>(
        &self,
        index_slot: IndexSlot,
        range: &R,
        read_set: &[usize],
    ) -> DmlValidationResult<()>
    where
        R: RangeBounds<&'r [Val]> + ?Sized,
    {
        self.validate_index_range(index_slot, range)?;
        self.validate_projection(read_set)?;
        Ok(())
    }

    /// Validates an index range without requiring a projection read set.
    #[inline]
    pub(crate) fn validate_index_range<'r, R>(
        &self,
        index_slot: IndexSlot,
        range: &R,
    ) -> DmlValidationResult<()>
    where
        R: RangeBounds<&'r [Val]> + ?Sized,
    {
        let Some(index_spec) = self.metadata.idx.index_spec(index_slot) else {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "index not found: index_slot={}, index_slot_count={}",
                index_slot,
                self.metadata.idx.index_slot_count()
            )));
        };
        self.validate_index_bound(index_slot, index_spec, range.start_bound())?;
        self.validate_index_bound(index_slot, index_spec, range.end_bound())?;
        Ok(())
    }

    #[inline]
    fn validate_index_bound(
        &self,
        index_slot: IndexSlot,
        index_spec: &TableIndexMetadata,
        bound: Bound<&&[Val]>,
    ) -> DmlValidationResult<()> {
        match bound {
            Bound::Unbounded => Ok(()),
            Bound::Included(vals) | Bound::Excluded(vals) => {
                self.validate_index_values(index_slot, index_spec, vals)
            }
        }
    }

    /// Validates a projection read set against table column metadata.
    #[inline]
    pub(crate) fn validate_projection(&self, read_set: &[usize]) -> DmlValidationResult<()> {
        if read_set.is_empty() {
            return Err(
                Report::new(DmlValidationError::ReadSet).attach("read set must not be empty")
            );
        }
        let mut last = None;
        for col_no in read_set {
            if *col_no >= self.metadata.col.col_count() {
                return Err(Report::new(DmlValidationError::ReadSet).attach(format!(
                    "read column out of range: column_no={}, column_count={}",
                    col_no,
                    self.metadata.col.col_count()
                )));
            }
            if last.is_some_and(|last| *col_no <= last) {
                return Err(Report::new(DmlValidationError::ReadSet).attach(format!(
                    "read columns not strictly ordered: column_no={col_no}"
                )));
            }
            last = Some(*col_no);
        }
        Ok(())
    }

    /// Validates a primary-key DML key against table metadata.
    #[inline]
    pub(crate) fn validate_primary_key(
        &self,
        index_slot: IndexSlot,
        key_vals: &[Val],
    ) -> DmlValidationResult<()> {
        let Some(primary_key) = self.metadata.primary_key() else {
            return Err(Report::new(DmlValidationError::PrimaryKey).attach("primary key not found"));
        };
        match primary_key.validate_key(index_slot, key_vals) {
            Ok(()) => Ok(()),
            Err(PrimaryKeyMatchError::IndexSlot { actual, expected }) => {
                Err(Report::new(DmlValidationError::PrimaryKey).attach(format!(
                    "key is not primary key: index_slot={actual}, primary_key_index_slot={expected}"
                )))
            }
            Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => {
                Err(Report::new(DmlValidationError::PrimaryKey).attach(format!(
                    "key value count {actual} does not match primary key column count {expected}"
                )))
            }
            Err(PrimaryKeyMatchError::Type { index_slot }) => {
                Err(Report::new(DmlValidationError::PrimaryKey)
                    .attach(format!("key type mismatch: index_slot={index_slot}")))
            }
        }
    }

    #[inline]
    fn validate_index_values(
        &self,
        index_slot: IndexSlot,
        index_spec: &TableIndexMetadata,
        vals: &[Val],
    ) -> DmlValidationResult<()> {
        if vals.len() != index_spec.keys.len() {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "key value count mismatch: index_slot={index_slot}, actual={}, expected={}",
                vals.len(),
                index_spec.keys.len()
            )));
        }
        for (key_pos, (index_key, val)) in index_spec.keys.iter().zip(vals).enumerate() {
            let col_no = usize::from(index_key.column_ordinal);
            if !self.metadata.col.col_type_match(col_no, val) {
                return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                    "key value type mismatch: index_slot={index_slot}, key_pos={key_pos}, column_no={col_no}, expected={:?}, actual={val:?}",
                    self.metadata.col.col_type(col_no)
                )));
            }
        }
        Ok(())
    }
}

#[inline]
fn variable_len(value: ValRef<'_>) -> usize {
    match value {
        ValRef::VarByte(bytes) => bytes.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{DmlValidationError, DmlValidationResult, DmlValidator};
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::row::ops::UpdateCol;
    use crate::row::tests::BufferValues;
    use crate::value::{Val, ValKind, ValRef};
    use error_stack::Report;
    use std::{mem, slice};

    fn assert_validation_parity(
        owned: DmlValidationResult<()>,
        borrowed: DmlValidationResult<()>,
        expected: Option<DmlValidationError>,
        fact: &str,
    ) {
        assert_eq!(
            owned.as_ref().err().map(|err| *err.current_context()),
            expected
        );
        assert_eq!(
            borrowed.as_ref().err().map(|err| *err.current_context()),
            expected
        );
        if let (Err(owned), Err(borrowed)) = (owned, borrowed) {
            // Compare caller-owned diagnostic facts, excluding source locations.
            let facts = |report: &Report<DmlValidationError>| {
                report
                    .frames()
                    .filter_map(|frame| frame.downcast_ref::<String>().cloned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(facts(&owned), facts(&borrowed));
            assert!(format!("{borrowed:?}").contains(fact), "{borrowed:?}");
        }
    }

    fn bytes(len: usize) -> Val {
        Val::try_from(vec![0x5a; len]).unwrap()
    }

    fn capacity_metadata(kinds: &[(ValKind, bool)]) -> TableMetadata {
        TableMetadata::try_new(
            kinds
                .iter()
                .map(|&(kind, nullable)| {
                    StorageColumnSpec::new(
                        kind,
                        if nullable {
                            StorageColumnFlags::NULLABLE
                        } else {
                            StorageColumnFlags::empty()
                        },
                    )
                })
                .collect(),
            vec![],
        )
        .unwrap()
    }

    fn assert_capacity_formats(metadata: &TableMetadata, row: &[Val]) {
        use crate::buffer::page::PAGE_SIZE;
        use crate::file::FileKind;
        use crate::file::block_integrity::{
            BLOCK_INTEGRITY_HEADER_SIZE, BLOCK_INTEGRITY_TRAILER_SIZE,
        };
        use crate::id::{BlockID, RowID};
        use crate::io::IOBuf;
        use crate::lwc::{LwcBlock, LwcBuilder};
        use crate::row::ops::InsertRow;
        use crate::row::{RowPage, RowRead};
        use std::sync::Arc;

        let validator = DmlValidator::new(metadata);
        let size = validator.validate_row_capacity(row).unwrap();
        let borrowed =
            BufferValues::new(row.iter().enumerate().map(|(i, value)| (i, value.view())));
        assert_eq!(size, validator.validate_row_capacity(&borrowed).unwrap());
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(0), 1, &metadata.col);
        let before = page.header.var_field_offset();
        assert!(matches!(page.insert(&metadata.col, row), InsertRow::Ok(_)));
        assert_eq!(page.row(0).clone_vals(&metadata.col), row);
        let actual_page =
            PAGE_SIZE - page.data.len() + usize::from(page.header.fix_field_end) + before
                - page.header.var_field_offset();
        assert_eq!(
            metadata
                .col
                .row_size()
                .row_page_bytes(size.outlined_var_bytes)
                .unwrap(),
            actual_page
        );
        let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
        assert!(builder.append_row_values(RowID::new(0), row));
        let encoded = builder.build(1).unwrap();
        let block = LwcBlock::try_from_persisted_bytes(
            encoded.as_bytes(),
            FileKind::TableFile,
            BlockID::new(1),
        )
        .unwrap();
        let end = metadata.col.col_count() * 2;
        let body_bytes = usize::from(u16::from_le_bytes(
            block.body[end - 2..end].try_into().unwrap(),
        ));
        let actual_lwc = BLOCK_INTEGRITY_HEADER_SIZE
            + BLOCK_INTEGRITY_TRAILER_SIZE
            + mem::size_of_val(&block.header)
            + body_bytes;
        assert_eq!(
            metadata
                .col
                .row_size()
                .lwc_bytes(size.total_var_bytes)
                .unwrap(),
            actual_lwc
        );
        assert_eq!(block.decode_full_row_values(&metadata.col, 0).unwrap(), row);
    }

    /// Purpose: Compare singleton admission with actual initialized pages and serialized blocks for every value kind.
    /// Expected: Owned and borrowed measurements equal physical offsets and all nullable, inline, and outlined values round-trip.
    #[test]
    fn test_row_capacity_matches_physical_formats() {
        let values = vec![
            Val::I8(-1),
            Val::U8(2),
            Val::I16(-3),
            Val::U16(4),
            Val::I32(-5),
            Val::U32(6),
            Val::from(1.5f32),
            Val::I64(-7),
            Val::U64(8),
            Val::from(2.5f64),
            bytes(0),
            bytes(6),
            bytes(7),
        ];
        for nullable in [false, true] {
            let kinds: Vec<_> = values
                .iter()
                .map(|v| (v.kind().unwrap(), nullable))
                .collect();
            let metadata = capacity_metadata(&kinds);
            for value in &values {
                let single = capacity_metadata(&[(value.kind().unwrap(), nullable)]);
                assert_capacity_formats(&single, slice::from_ref(value));
                if nullable {
                    assert_capacity_formats(&single, &[Val::Null]);
                }
            }
            assert_capacity_formats(&metadata, &values);
            if nullable {
                assert_capacity_formats(&metadata, &vec![Val::Null; values.len()]);
            }
        }
    }

    /// Purpose: Protect exact RowPage and singleton LWC boundaries, aggregate bytes, and impossible schema overhead.
    /// Expected: Exact fits encode successfully, one byte over fails the owning bound, and multiple variable values share one limit.
    #[test]
    fn test_row_capacity_physical_boundaries() {
        let single = capacity_metadata(&[(ValKind::VarByte, false)]);
        assert_capacity_formats(&single, &[bytes(65_461)]);
        let err = DmlValidator::new(&single)
            .validate_row_capacity(&[bytes(65_462)][..])
            .unwrap_err();
        assert_eq!(*err.current_context(), DmlValidationError::RowCapacity);
        assert!(format!("{err:?}").contains("LWC bound"));
        let two = capacity_metadata(&[(ValKind::VarByte, false); 2]);
        assert_capacity_formats(&two, &[bytes(32_721), bytes(32_721)]);
        assert!(
            DmlValidator::new(&two)
                .validate_row_capacity(&[bytes(32_721), bytes(32_722)][..])
                .is_err()
        );
        let mut kinds = vec![(ValKind::U8, true); 3000];
        kinds.push((ValKind::VarByte, false));
        let metadata = capacity_metadata(&kinds);
        let mut row = vec![Val::Null; 3000];
        row.push(bytes(11_464));
        assert_capacity_formats(&metadata, &row);
        row[3000] = bytes(11_465);
        let err = DmlValidator::new(&metadata)
            .validate_row_capacity(row.as_slice())
            .unwrap_err();
        assert!(format!("{err:?}").contains("RowPage bound"));
        for (columns, nullable) in [(6000, false), (4000, true)] {
            let metadata = capacity_metadata(&vec![(ValKind::U8, nullable); columns]);
            assert!(
                DmlValidator::new(&metadata)
                    .validate_row_capacity(vec![Val::U8(0); columns].as_slice())
                    .is_err()
            );
        }
    }

    /// Purpose: Measure complete sparse-update images across null and inline boundaries without reading stale null descriptors.
    /// Expected: Hot and decoded overlays agree, oversized unchanged-plus-new payloads fail, and shrinking or nulling values succeeds.
    #[test]
    fn test_row_capacity_update_overlay() {
        use crate::id::RowID;
        use crate::row::ops::InsertRow;
        use crate::row::{RowPage, RowRead};
        let metadata = capacity_metadata(&[(ValKind::VarByte, true); 2]);
        let validator = DmlValidator::new(&metadata);
        for old in [Val::Null, bytes(0), bytes(6), bytes(7), bytes(32_000)] {
            let row = vec![bytes(32_000), old];
            let mut page = RowPage::new_test_page();
            page.init(RowID::new(0), 1, &metadata.col);
            assert!(matches!(page.insert(&metadata.col, &row), InsertRow::Ok(_)));
            for new in [Val::Null, bytes(0), bytes(6), bytes(7), bytes(34_000)] {
                let update = [UpdateCol {
                    idx: 1,
                    val: new.clone(),
                }];
                let expected = validator.validate_row_capacity(&[row[0].clone(), new][..]);
                let decoded = validator.validate_updated_row_capacity(row.as_slice(), &update[..]);
                let hot = validator.validate_hot_update_capacity(&page.row(0), &update[..]);
                match expected {
                    Ok(size) => {
                        assert_eq!(decoded.unwrap(), size);
                        assert_eq!(hot.unwrap(), size);
                    }
                    Err(_) => {
                        assert!(decoded.is_err());
                        assert!(hot.is_err());
                    }
                }
            }
            // Null writes retain the previous descriptor. Admission must inspect
            // the null bit before borrowing that stale variable field.
            let mut writer = page.row_mut_exclusive(0, 0, 0);
            writer.update_col(&metadata.col, 0, ValRef::Null, true);
            writer.finish_update();
            assert!(page.row(0).is_null(&metadata.col, 0));
            let update = [UpdateCol {
                idx: 1,
                val: bytes(40_000),
            }];
            let size = validator
                .validate_hot_update_capacity(&page.row(0), &update[..])
                .unwrap();
            assert_eq!(size.total_var_bytes, 40_000);
            assert_eq!(size.outlined_var_bytes, 40_000);
        }
    }

    /// Purpose: Bound borrowed row and update bytes without changing existing validation precedence.
    /// Expected: Representable binary inputs pass; oversized bytes retain column diagnostics and caller-specific error domains after shape, order, and type checks.
    #[test]
    fn test_borrowed_byte_length_validation() {
        use crate::error::{DataIntegrityError, OperationError};
        use crate::value::{MemVar, ValRef};
        use error_stack::ResultExt;

        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        let validator = DmlValidator::new(&metadata);
        for len in [
            0,
            6,
            7,
            14,
            15,
            MemVar::MAX_LEN,
            MemVar::MAX_LEN + 1,
            MemVar::MAX_LEN + 16,
        ] {
            let bytes = vec![0xff; len];
            let row = BufferValues::new([(0, ValRef::I32(1)), (1, ValRef::VarByte(&bytes))]);
            let update = BufferValues::new([(1, ValRef::VarByte(&bytes))]);
            let valid = len <= MemVar::MAX_LEN;
            for (result, context) in [
                (
                    validator.validate_full_row(&row),
                    DmlValidationError::RowShape,
                ),
                (
                    validator.validate_sparse_update(&update),
                    DmlValidationError::SparseUpdate,
                ),
            ] {
                assert_eq!(result.is_ok(), valid, "len={len}, result={result:?}");
                if let Err(err) = result {
                    assert_eq!(err.current_context(), &context);
                    let report = format!("{err:?}");
                    for fact in [
                        "column_no=1".to_owned(),
                        format!("len={len}"),
                        "max=65535".to_owned(),
                    ] {
                        assert!(report.contains(&fact), "{report}");
                    }
                }
            }
            if valid {
                let owned = [Val::I32(1), Val::try_from(bytes.as_slice()).unwrap()];
                assert_validation_parity(
                    validator.validate_full_row(&owned[..]),
                    validator.validate_full_row(&row),
                    None,
                    "",
                );
            } else {
                let foreground = validator
                    .validate_full_row(&row)
                    .change_context(OperationError::InvalidDmlInput)
                    .unwrap_err();
                assert_eq!(
                    foreground.current_context(),
                    &OperationError::InvalidDmlInput
                );
                assert_eq!(
                    foreground.downcast_ref::<DmlValidationError>(),
                    Some(&DmlValidationError::RowShape)
                );
                let recovery = validator
                    .validate_sparse_update(&update)
                    .change_context(DataIntegrityError::InvalidPayload)
                    .unwrap_err();
                assert_eq!(
                    recovery.current_context(),
                    &DataIntegrityError::InvalidPayload
                );
                assert_eq!(
                    recovery.downcast_ref::<DmlValidationError>(),
                    Some(&DmlValidationError::SparseUpdate)
                );
            }
        }
        let oversized = vec![0xff; MemVar::MAX_LEN + 1];
        for (values, fact) in [
            (vec![(0, ValRef::VarByte(&oversized))], "count mismatch"),
            (
                vec![(0, ValRef::VarByte(&oversized)), (1, ValRef::VarByte(b""))],
                "type mismatch",
            ),
        ] {
            let err = validator
                .validate_full_row(&BufferValues::new(values))
                .unwrap_err();
            assert!(format!("{err:?}").contains(fact), "{err:?}");
        }
        for (values, fact) in [
            (vec![(2, ValRef::VarByte(&oversized))], "out of range"),
            (vec![(0, ValRef::VarByte(&oversized))], "type mismatch"),
            (
                vec![
                    (1, ValRef::VarByte(b"ok")),
                    (1, ValRef::VarByte(&oversized)),
                ],
                "not strictly ordered",
            ),
        ] {
            let err = validator
                .validate_sparse_update(&BufferValues::new(values))
                .unwrap_err();
            assert!(format!("{err:?}").contains(fact), "{err:?}");
        }
    }

    /// Purpose: Protect sparse-update validation at ordering, column, type, and nullability
    /// boundaries.
    /// Expected: Valid updates pass and malformed updates return the sparse-update error
    /// classification.
    #[test]
    fn test_sparse_update_validates_order_bounds_and_types() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .unwrap();
        let validator = DmlValidator::new(&metadata);
        let cases = [
            ("empty", vec![], true),
            (
                "ordered",
                vec![(0, Val::from(10i32)), (2, Val::from(42u64))],
                true,
            ),
            (
                "unordered",
                vec![(2, Val::from(42u64)), (0, Val::from(10i32))],
                false,
            ),
            (
                "duplicate column",
                vec![(0, Val::from(10i32)), (0, Val::from(11i32))],
                false,
            ),
            ("out of range", vec![(3, Val::from(42u64))], false),
            (
                "type mismatch",
                vec![(0, Val::try_from("not an i32").unwrap())],
                false,
            ),
            ("non-nullable column", vec![(0, Val::Null)], false),
        ];
        for (case, cols, valid) in cases {
            let update: Vec<UpdateCol> = cols
                .into_iter()
                .map(|(idx, val)| UpdateCol { idx, val })
                .collect();
            let result = validator.validate_sparse_update(update.as_slice());
            assert_eq!(result.is_ok(), valid, "case={case}, result={result:?}");
            if let Err(err) = result {
                assert_eq!(
                    err.current_context(),
                    &DmlValidationError::SparseUpdate,
                    "case={case}"
                );
            }
        }
    }

    /// Purpose: Protect equivalent validation of owned and borrowed row inputs.
    /// Expected: Both forms enforce the same shape and update rules with matching error
    /// context and diagnostics.
    #[test]
    fn test_borrowed_validation_parity() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .unwrap();
        let validator = DmlValidator::new(&metadata);
        let rows = [
            (vec![Val::I32(1), Val::try_from("bytes").unwrap()], true, ""),
            (vec![Val::I32(1), Val::Null], true, ""),
            (vec![Val::I32(1)], false, "actual=1, expected=2"),
            (
                vec![Val::I32(1), Val::Null, Val::Null],
                false,
                "actual=3, expected=2",
            ),
            (
                vec![Val::Null, Val::try_from("").unwrap()],
                false,
                "column_no=0",
            ),
            (
                vec![Val::U32(1), Val::try_from("").unwrap()],
                false,
                "actual=u32(1)",
            ),
            (vec![Val::I32(1), Val::I8(1)], false, "column_no=1"),
        ];
        for (owned, valid, fact) in rows {
            let borrowed =
                BufferValues::new(owned.iter().enumerate().map(|(idx, val)| (idx, val.view())));
            assert_validation_parity(
                validator.validate_full_row(owned.as_slice()),
                validator.validate_full_row(&borrowed),
                (!valid).then_some(DmlValidationError::RowShape),
                fact,
            );
        }
        let updates = [
            (vec![], true, ""),
            (
                vec![(0, Val::I32(2)), (1, Val::try_from("new").unwrap())],
                true,
                "",
            ),
            (vec![(1, Val::Null)], true, ""),
            (vec![(0, Val::Null)], false, "type mismatch"),
            (vec![(0, Val::U32(2))], false, "actual=u32(2)"),
            (
                vec![(1, Val::Null), (0, Val::I32(2))],
                false,
                "not strictly ordered",
            ),
            // Ordering wins over type checking at a duplicate ordinal.
            (
                vec![(0, Val::I32(2)), (0, Val::Null)],
                false,
                "not strictly ordered",
            ),
            (vec![(2, Val::Null)], false, "out of range"),
        ];
        for (entries, valid, fact) in updates {
            let owned: Vec<_> = entries
                .into_iter()
                .map(|(idx, val)| UpdateCol { idx, val })
                .collect();
            let borrowed = BufferValues::new(owned.iter().map(|col| (col.idx, col.val.view())));
            assert_validation_parity(
                validator.validate_sparse_update(owned.as_slice()),
                validator.validate_sparse_update(&borrowed),
                (!valid).then_some(DmlValidationError::SparseUpdate),
                fact,
            );
        }
    }
}
