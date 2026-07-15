use crate::catalog::{IndexSpec, PrimaryKeyMatchError, TableMetadata};
use crate::error::{DataIntegrityError, DataIntegrityResult, OperationError, OperationResult};
use crate::id::TableID;
use crate::row::ops::{RowUpdateView, UpdateCol};
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
    /// A secondary-index read set is invalid.
    #[error("invalid DML read set")]
    ReadSet,
    /// A primary-key reference does not match table metadata.
    #[error("invalid DML primary key")]
    PrimaryKey,
}

/// Result carrying caller-neutral DML validation reports.
pub(crate) type DmlValidationResult<T> = StdResult<T, Report<DmlValidationError>>;

/// Caller-owned conversion from neutral DML validation into an operation domain.
pub(crate) trait DmlValidationResultExt<T> {
    /// Classifies caller-supplied DML as invalid foreground input.
    fn with_foreground_context(
        self,
        operation: &'static str,
        table_id: TableID,
    ) -> OperationResult<T>;

    /// Classifies replay-sourced DML as an invalid persisted payload.
    fn with_recovery_context(
        self,
        operation: &'static str,
        table_id: TableID,
    ) -> DataIntegrityResult<T>;
}

impl<T> DmlValidationResultExt<T> for DmlValidationResult<T> {
    #[inline]
    fn with_foreground_context(
        self,
        operation: &'static str,
        table_id: TableID,
    ) -> OperationResult<T> {
        self.map_err(|report| {
            report
                .change_context(OperationError::InvalidDmlInput)
                .attach(format!("operation={operation}, table_id={table_id}"))
        })
    }

    #[inline]
    fn with_recovery_context(
        self,
        operation: &'static str,
        table_id: TableID,
    ) -> DataIntegrityResult<T> {
        self.map_err(|report| {
            report
                .change_context(DataIntegrityError::InvalidPayload)
                .attach(format!("operation={operation}, table_id={table_id}"))
        })
    }
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

    /// Validates a full-row DML payload against table column metadata.
    #[inline]
    pub(crate) fn validate_full_row(&self, vals: &[Val]) -> DmlValidationResult<()> {
        if vals.len() != self.metadata.col.col_count() {
            return Err(Report::new(DmlValidationError::RowShape).attach(format!(
                "row value count mismatch: actual={}, expected={}",
                vals.len(),
                self.metadata.col.col_count()
            )));
        }
        for (col_no, val) in vals.iter().enumerate() {
            if !self.metadata.col.col_type_match(col_no, val) {
                return Err(Report::new(DmlValidationError::RowShape).attach(format!(
                    "row value type mismatch: column_no={col_no}, expected={:?}, actual={val:?}",
                    self.metadata.col.col_type(col_no)
                )));
            }
        }
        Ok(())
    }

    /// Validates a sparse update DML payload against table column metadata.
    #[inline]
    pub(crate) fn validate_sparse_update(&self, update: &[UpdateCol]) -> DmlValidationResult<()> {
        let mut last_idx = None;
        for update_col in update {
            if update_col.idx >= self.metadata.col.col_count() {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update column out of range: column_no={}, column_count={}",
                        update_col.idx,
                        self.metadata.col.col_count()
                    )),
                );
            }
            if last_idx.is_some_and(|idx| update_col.idx <= idx) {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update columns not strictly ordered: column_no={}",
                        update_col.idx
                    )),
                );
            }
            if !self
                .metadata
                .col
                .col_type_match(update_col.idx, &update_col.val)
            {
                return Err(
                    Report::new(DmlValidationError::SparseUpdate).attach(format!(
                        "update column type mismatch: column_no={}, expected={:?}, actual={:?}",
                        update_col.idx,
                        self.metadata.col.col_type(update_col.idx),
                        update_col.val
                    )),
                );
            }
            last_idx = Some(update_col.idx);
        }
        debug_assert!(RowUpdateView::Sparse(update).is_valid_for(self.metadata.col.as_ref()));
        Ok(())
    }

    /// Validates a unique-index DML key against table index metadata.
    #[inline]
    pub(crate) fn validate_unique_key(
        &self,
        index_no: usize,
        key_vals: &[Val],
    ) -> DmlValidationResult<&'m IndexSpec> {
        let index_spec = self.validate_unique_index(index_no)?;
        self.validate_index_values(index_no, index_spec, key_vals)?;
        Ok(index_spec)
    }

    /// Validates that a DML operation targets an active unique index.
    #[inline]
    pub(crate) fn validate_unique_index(
        &self,
        index_no: usize,
    ) -> DmlValidationResult<&'m IndexSpec> {
        let Some(index_spec) = self.metadata.idx.index_spec(index_no) else {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "unique index not found: index_no={}, index_slot_count={}",
                index_no,
                self.metadata.idx.index_slot_count()
            )));
        };
        if !index_spec.unique() {
            return Err(Report::new(DmlValidationError::IndexKey)
                .attach(format!("index is not unique: index_no={index_no}")));
        }
        Ok(index_spec)
    }

    /// Validates a secondary-index scan request against table metadata.
    #[inline]
    pub(crate) fn validate_index_scan<'r, R>(
        &self,
        index_no: usize,
        range: &R,
        read_set: &[usize],
    ) -> DmlValidationResult<()>
    where
        R: RangeBounds<&'r [Val]> + ?Sized,
    {
        let Some(index_spec) = self.metadata.idx.index_spec(index_no) else {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "index not found: index_no={}, index_slot_count={}",
                index_no,
                self.metadata.idx.index_slot_count()
            )));
        };
        self.validate_index_bound(index_no, index_spec, range.start_bound())?;
        self.validate_index_bound(index_no, index_spec, range.end_bound())?;
        self.validate_read_set(read_set)?;
        Ok(())
    }

    #[inline]
    fn validate_index_bound(
        &self,
        index_no: usize,
        index_spec: &IndexSpec,
        bound: Bound<&&[Val]>,
    ) -> DmlValidationResult<()> {
        match bound {
            Bound::Unbounded => Ok(()),
            Bound::Included(vals) | Bound::Excluded(vals) => {
                self.validate_index_values(index_no, index_spec, vals)
            }
        }
    }

    #[inline]
    fn validate_read_set(&self, read_set: &[usize]) -> DmlValidationResult<()> {
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
        index_no: usize,
        key_vals: &[Val],
    ) -> DmlValidationResult<()> {
        let Some(primary_key) = self.metadata.primary_key() else {
            return Err(Report::new(DmlValidationError::PrimaryKey).attach("primary key not found"));
        };
        match primary_key.validate_key(index_no, key_vals) {
            Ok(()) => Ok(()),
            Err(PrimaryKeyMatchError::IndexNo { actual, expected }) => {
                Err(Report::new(DmlValidationError::PrimaryKey).attach(format!(
                    "key is not primary key: index_no={actual}, primary_key_index_no={expected}"
                )))
            }
            Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => {
                Err(Report::new(DmlValidationError::PrimaryKey).attach(format!(
                    "key value count {actual} does not match primary key column count {expected}"
                )))
            }
            Err(PrimaryKeyMatchError::Type { index_no }) => {
                Err(Report::new(DmlValidationError::PrimaryKey)
                    .attach(format!("key type mismatch: index_no={index_no}")))
            }
        }
    }

    #[inline]
    fn validate_index_values(
        &self,
        index_no: usize,
        index_spec: &IndexSpec,
        vals: &[Val],
    ) -> DmlValidationResult<()> {
        if vals.len() != index_spec.cols.len() {
            return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                "key value count mismatch: index_no={index_no}, actual={}, expected={}",
                vals.len(),
                index_spec.cols.len()
            )));
        }
        for (key_pos, (index_key, val)) in index_spec.cols.iter().zip(vals).enumerate() {
            let col_no = usize::from(index_key.col_no);
            if !self.metadata.col.col_type_match(col_no, val) {
                return Err(Report::new(DmlValidationError::IndexKey).attach(format!(
                    "key value type mismatch: index_no={index_no}, key_pos={key_pos}, column_no={col_no}, expected={:?}, actual={val:?}",
                    self.metadata.col.col_type(col_no)
                )));
            }
        }
        Ok(())
    }
}
