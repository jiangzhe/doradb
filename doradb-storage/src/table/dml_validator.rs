use crate::catalog::{IndexSpec, PrimaryKeyMatchError, TableMetadata};
use crate::error::{DataIntegrityError, Error, OperationError, Result};
use crate::id::TableID;
use crate::row::ops::{RowUpdateView, UpdateCol};
use crate::value::Val;
use error_stack::Report;
use std::ops::{Bound, RangeBounds};

/// Error domain used for DML shape/type/nullability validation.
#[derive(Clone, Copy)]
pub(crate) enum DmlValidationDomain {
    /// Caller supplied foreground operation input.
    Foreground,
    /// Persisted or recovery-sourced DML payload.
    Recovery,
}

impl DmlValidationDomain {
    #[inline]
    fn error(self, message: impl Into<String>) -> Error {
        match self {
            DmlValidationDomain::Foreground => Report::new(OperationError::InvalidDmlInput)
                .attach(message.into())
                .into(),
            DmlValidationDomain::Recovery => Report::new(DataIntegrityError::InvalidPayload)
                .attach(message.into())
                .into(),
        }
    }
}

/// Validates DML payloads against one table's metadata.
pub(crate) struct DmlValidator<'m> {
    metadata: &'m TableMetadata,
    table_id: TableID,
    operation: &'static str,
    domain: DmlValidationDomain,
}

impl<'m> DmlValidator<'m> {
    /// Creates a validator for one table and foreground or recovery operation.
    #[inline]
    pub(crate) fn new(
        metadata: &'m TableMetadata,
        table_id: TableID,
        operation: &'static str,
        domain: DmlValidationDomain,
    ) -> Self {
        Self {
            metadata,
            table_id,
            operation,
            domain,
        }
    }

    /// Validates a full-row DML payload against table column metadata.
    #[inline]
    pub(crate) fn validate_full_row(&self, vals: &[Val]) -> Result<()> {
        if vals.len() != self.metadata.col.col_count() {
            return Err(self.domain.error(format!(
                "{} row value count mismatch: actual={}, expected={}",
                self.context(),
                vals.len(),
                self.metadata.col.col_count()
            )));
        }
        for (col_no, val) in vals.iter().enumerate() {
            if !self.metadata.col.col_type_match(col_no, val) {
                return Err(self.domain.error(format!(
                    "{} row value type mismatch: column_no={col_no}, expected={:?}, actual={val:?}",
                    self.context(),
                    self.metadata.col.col_type(col_no)
                )));
            }
        }
        Ok(())
    }

    /// Validates a sparse update DML payload against table column metadata.
    #[inline]
    pub(crate) fn validate_sparse_update(&self, update: &[UpdateCol]) -> Result<()> {
        let mut last_idx = None;
        for update_col in update {
            if update_col.idx >= self.metadata.col.col_count() {
                return Err(self.domain.error(format!(
                    "{} update column out of range: column_no={}, column_count={}",
                    self.context(),
                    update_col.idx,
                    self.metadata.col.col_count()
                )));
            }
            if last_idx.is_some_and(|idx| update_col.idx <= idx) {
                return Err(self.domain.error(format!(
                    "{} update columns not strictly ordered: column_no={}",
                    self.context(),
                    update_col.idx
                )));
            }
            if !self
                .metadata
                .col
                .col_type_match(update_col.idx, &update_col.val)
            {
                return Err(self.domain.error(format!(
                    "{} update column type mismatch: column_no={}, expected={:?}, actual={:?}",
                    self.context(),
                    update_col.idx,
                    self.metadata.col.col_type(update_col.idx),
                    update_col.val
                )));
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
    ) -> Result<&'m IndexSpec> {
        let index_spec = self.validate_unique_index(index_no)?;
        self.validate_index_values(index_no, index_spec, key_vals)?;
        Ok(index_spec)
    }

    /// Validates that a DML operation targets an active unique index.
    #[inline]
    pub(crate) fn validate_unique_index(&self, index_no: usize) -> Result<&'m IndexSpec> {
        let Some(index_spec) = self.metadata.idx.index_spec(index_no) else {
            return Err(self.domain.error(format!(
                "{} unique index not found: index_no={}, index_slot_count={}",
                self.context(),
                index_no,
                self.metadata.idx.index_slot_count()
            )));
        };
        if !index_spec.unique() {
            return Err(self.domain.error(format!(
                "{} index is not unique: index_no={index_no}",
                self.context()
            )));
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
    ) -> Result<()>
    where
        R: RangeBounds<&'r [Val]> + ?Sized,
    {
        let Some(index_spec) = self.metadata.idx.index_spec(index_no) else {
            return Err(self.domain.error(format!(
                "{} index not found: index_no={}, index_slot_count={}",
                self.context(),
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
    ) -> Result<()> {
        match bound {
            Bound::Unbounded => Ok(()),
            Bound::Included(vals) | Bound::Excluded(vals) => {
                self.validate_index_values(index_no, index_spec, vals)
            }
        }
    }

    #[inline]
    fn validate_read_set(&self, read_set: &[usize]) -> Result<()> {
        if read_set.is_empty() {
            return Err(self
                .domain
                .error(format!("{} read set must not be empty", self.context())));
        }
        let mut last = None;
        for col_no in read_set {
            if *col_no >= self.metadata.col.col_count() {
                return Err(self.domain.error(format!(
                    "{} read column out of range: column_no={}, column_count={}",
                    self.context(),
                    col_no,
                    self.metadata.col.col_count()
                )));
            }
            if last.is_some_and(|last| *col_no <= last) {
                return Err(self.domain.error(format!(
                    "{} read columns not strictly ordered: column_no={col_no}",
                    self.context()
                )));
            }
            last = Some(*col_no);
        }
        Ok(())
    }

    /// Validates a primary-key DML key against table metadata.
    #[inline]
    pub(crate) fn validate_primary_key(&self, index_no: usize, key_vals: &[Val]) -> Result<()> {
        let Some(primary_key) = self.metadata.primary_key() else {
            return Err(self
                .domain
                .error(format!("{} primary key not found", self.context())));
        };
        match primary_key.validate_key(index_no, key_vals) {
            Ok(()) => Ok(()),
            Err(PrimaryKeyMatchError::IndexNo { actual, expected }) => {
                Err(self.domain.error(format!(
                    "{} key is not primary key: index_no={actual}, primary_key_index_no={expected}",
                    self.context()
                )))
            }
            Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => {
                Err(self.domain.error(format!(
                    "{} key value count {actual} does not match primary key column count {expected}",
                    self.context()
                )))
            }
            Err(PrimaryKeyMatchError::Type { index_no }) => Err(self.domain.error(format!(
                "{} key type mismatch: index_no={index_no}",
                self.context()
            ))),
        }
    }

    #[inline]
    fn validate_index_values(
        &self,
        index_no: usize,
        index_spec: &IndexSpec,
        vals: &[Val],
    ) -> Result<()> {
        if vals.len() != index_spec.cols.len() {
            return Err(self.domain.error(format!(
                "{} key value count mismatch: index_no={index_no}, actual={}, expected={}",
                self.context(),
                vals.len(),
                index_spec.cols.len()
            )));
        }
        for (key_pos, (index_key, val)) in index_spec.cols.iter().zip(vals).enumerate() {
            let col_no = usize::from(index_key.col_no);
            if !self.metadata.col.col_type_match(col_no, val) {
                return Err(self.domain.error(format!(
                    "{} key value type mismatch: index_no={index_no}, key_pos={key_pos}, column_no={col_no}, expected={:?}, actual={val:?}",
                    self.context(),
                    self.metadata.col.col_type(col_no)
                )));
            }
        }
        Ok(())
    }

    #[inline]
    fn context(&self) -> String {
        format!("operation={}, table_id={}", self.operation, self.table_id)
    }
}
