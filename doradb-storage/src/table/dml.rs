use crate::catalog::{IndexSpec, PrimaryKeyMatchError, TableMetadata};
use crate::error::{DataIntegrityError, Error, OperationError, Result};
use crate::id::TableID;
use crate::row::ops::{RowUpdateView, SelectKey, UpdateCol};
use crate::value::Val;
use error_stack::Report;

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

/// Validates a full-row DML payload against table column metadata.
#[inline]
pub(crate) fn validate_dml_full_row(
    metadata: &TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    vals: &[Val],
    domain: DmlValidationDomain,
) -> Result<()> {
    if vals.len() != metadata.col.col_count() {
        return Err(domain.error(format!(
            "{} row value count mismatch: actual={}, expected={}",
            dml_context(operation, table_id),
            vals.len(),
            metadata.col.col_count()
        )));
    }
    for (col_no, val) in vals.iter().enumerate() {
        if !metadata.col.col_type_match(col_no, val) {
            return Err(domain.error(format!(
                "{} row value type mismatch: column_no={col_no}, expected={:?}, actual={val:?}",
                dml_context(operation, table_id),
                metadata.col.col_type(col_no)
            )));
        }
    }
    Ok(())
}

/// Validates a sparse update DML payload against table column metadata.
#[inline]
pub(crate) fn validate_dml_sparse_update(
    metadata: &TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    update: &[UpdateCol],
    domain: DmlValidationDomain,
) -> Result<()> {
    let mut last_idx = None;
    for update_col in update {
        if update_col.idx >= metadata.col.col_count() {
            return Err(domain.error(format!(
                "{} update column out of range: column_no={}, column_count={}",
                dml_context(operation, table_id),
                update_col.idx,
                metadata.col.col_count()
            )));
        }
        if last_idx.is_some_and(|idx| update_col.idx <= idx) {
            return Err(domain.error(format!(
                "{} update columns not strictly ordered: column_no={}",
                dml_context(operation, table_id),
                update_col.idx
            )));
        }
        if !metadata.col.col_type_match(update_col.idx, &update_col.val) {
            return Err(domain.error(format!(
                "{} update column type mismatch: column_no={}, expected={:?}, actual={:?}",
                dml_context(operation, table_id),
                update_col.idx,
                metadata.col.col_type(update_col.idx),
                update_col.val
            )));
        }
        last_idx = Some(update_col.idx);
    }
    debug_assert!(RowUpdateView::Sparse(update).is_valid_for(metadata.col.as_ref()));
    Ok(())
}

/// Validates a unique-index DML key against table index metadata.
#[inline]
pub(crate) fn validate_dml_unique_key<'a>(
    metadata: &'a TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    key: &SelectKey,
    domain: DmlValidationDomain,
) -> Result<&'a IndexSpec> {
    let index_spec =
        validate_dml_unique_index(metadata, table_id, operation, key.index_no, domain)?;
    validate_index_values(
        metadata,
        table_id,
        operation,
        key.index_no,
        index_spec,
        &key.vals,
        domain,
    )?;
    Ok(index_spec)
}

/// Validates that a DML operation targets an active unique index.
#[inline]
pub(crate) fn validate_dml_unique_index<'a>(
    metadata: &'a TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    index_no: usize,
    domain: DmlValidationDomain,
) -> Result<&'a IndexSpec> {
    let Some(index_spec) = metadata.idx.index_spec(index_no) else {
        return Err(domain.error(format!(
            "{} unique index not found: index_no={}, index_slot_count={}",
            dml_context(operation, table_id),
            index_no,
            metadata.idx.index_slot_count()
        )));
    };
    if !index_spec.unique() {
        return Err(domain.error(format!(
            "{} index is not unique: index_no={index_no}",
            dml_context(operation, table_id)
        )));
    }
    Ok(index_spec)
}

/// Validates a primary-key DML key against table metadata.
#[inline]
pub(crate) fn validate_dml_primary_key(
    metadata: &TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    key: &SelectKey,
    domain: DmlValidationDomain,
) -> Result<()> {
    let Some(primary_key) = metadata.primary_key() else {
        return Err(domain.error(format!(
            "{} primary key not found",
            dml_context(operation, table_id)
        )));
    };
    match primary_key.validate_key(key) {
        Ok(()) => Ok(()),
        Err(PrimaryKeyMatchError::IndexNo { actual, expected }) => Err(domain.error(format!(
            "{} key is not primary key: index_no={actual}, primary_key_index_no={expected}",
            dml_context(operation, table_id)
        ))),
        Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => Err(domain.error(format!(
            "{} key value count {actual} does not match primary key column count {expected}",
            dml_context(operation, table_id)
        ))),
        Err(PrimaryKeyMatchError::Type { index_no }) => Err(domain.error(format!(
            "{} key type mismatch: index_no={index_no}",
            dml_context(operation, table_id)
        ))),
    }
}

#[inline]
fn validate_index_values(
    metadata: &TableMetadata,
    table_id: Option<TableID>,
    operation: &'static str,
    index_no: usize,
    index_spec: &IndexSpec,
    vals: &[Val],
    domain: DmlValidationDomain,
) -> Result<()> {
    if vals.len() != index_spec.cols.len() {
        return Err(domain.error(format!(
            "{} key value count mismatch: index_no={index_no}, actual={}, expected={}",
            dml_context(operation, table_id),
            vals.len(),
            index_spec.cols.len()
        )));
    }
    for (key_pos, (index_key, val)) in index_spec.cols.iter().zip(vals).enumerate() {
        let col_no = usize::from(index_key.col_no);
        if !metadata.col.col_type_match(col_no, val) {
            return Err(domain.error(format!(
                "{} key value type mismatch: index_no={index_no}, key_pos={key_pos}, column_no={col_no}, expected={:?}, actual={val:?}",
                dml_context(operation, table_id),
                metadata.col.col_type(col_no)
            )));
        }
    }
    Ok(())
}

#[inline]
fn dml_context(operation: &'static str, table_id: Option<TableID>) -> String {
    match table_id {
        Some(table_id) => format!("operation={operation}, table_id={table_id}"),
        None => format!("operation={operation}"),
    }
}
