use crate::catalog::{IndexSlot, PrimaryKeyMatchError, TableIndexMetadata, TableMetadata};
use crate::row::{RowValues, UpdateValues};
use crate::value::Val;
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

#[cfg(test)]
mod tests {
    use super::{DmlValidationError, DmlValidationResult, DmlValidator};
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::row::ops::UpdateCol;
    use crate::row::tests::BufferValues;
    use crate::value::{Val, ValKind};
    use error_stack::Report;

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
            ("type mismatch", vec![(0, Val::from("not an i32"))], false),
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
            (vec![Val::I32(1), Val::from("bytes")], true, ""),
            (vec![Val::I32(1), Val::Null], true, ""),
            (vec![Val::I32(1)], false, "actual=1, expected=2"),
            (
                vec![Val::I32(1), Val::Null, Val::Null],
                false,
                "actual=3, expected=2",
            ),
            (vec![Val::Null, Val::from("")], false, "column_no=0"),
            (vec![Val::U32(1), Val::from("")], false, "actual=u32(1)"),
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
            (vec![(0, Val::I32(2)), (1, Val::from("new"))], true, ""),
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
