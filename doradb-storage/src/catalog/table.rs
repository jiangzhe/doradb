use crate::catalog::spec::{ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexNo, IndexSpec};
use crate::error::{ConfigError, Error, InternalError, OperationError, Result};
use crate::lock::{
    FreshLockGuard, LockGrant, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource,
};
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::serde::{Deser, Ser, Serde};
use crate::value::{Val, ValKind, ValType};
use error_stack::Report;
use semistr::SemiStr;
use std::collections::HashSet;
use std::mem;
use std::ops::Index;
use std::sync::Arc;

#[inline]
fn invalid_table_metadata(message: impl Into<String>) -> Error {
    Report::new(InternalError::Generic)
        .attach(message.into())
        .into()
}

#[inline]
fn invalid_index_spec(message: impl Into<String>) -> Error {
    Report::new(ConfigError::InvalidIndexSpec)
        .attach(message.into())
        .into()
}

#[inline]
fn index_not_found(message: impl Into<String>) -> Error {
    Report::new(OperationError::IndexNotFound)
        .attach(message.into())
        .into()
}

#[inline]
fn validate_index_spec(index_no: usize, spec: &IndexSpec, col_count: usize) -> Result<()> {
    if spec.cols.is_empty() {
        return Err(invalid_index_spec(format!(
            "index_no {index_no} has no key columns"
        )));
    }
    for key in &spec.cols {
        let col_no = key.col_no as usize;
        if col_no >= col_count {
            return Err(invalid_index_spec(format!(
                "index_no {index_no} references column {col_no} outside column count {col_count}"
            )));
        }
    }
    Ok(())
}

#[inline]
pub(crate) async fn acquire_table_ddl_locks<'a>(
    lock_manager: &'a LockManager,
    table_id: crate::catalog::TableID,
    owner: LockOwner,
    owner_group: LockOwnerGroup,
) -> Result<ScopedTableDdlLocks<'a>> {
    let metadata_resource = LockResource::TableMetadata(table_id);
    let metadata_grant = lock_manager
        .acquire_grouped_with_grant(metadata_resource, LockMode::Exclusive, owner, owner_group)
        .await?;
    let mut metadata_guard =
        FreshLockGuard::new(lock_manager, metadata_resource, owner, metadata_grant);

    let data_resource = LockResource::TableData(table_id);
    let data_grant = lock_manager
        .acquire_grouped_with_grant(data_resource, LockMode::Exclusive, owner, owner_group)
        .await?;
    if let Some(guard) = metadata_guard.as_mut() {
        guard.disarm();
    }

    Ok(ScopedTableDdlLocks {
        lock_manager,
        table_id,
        owner,
        metadata_fresh: metadata_grant == LockGrant::Fresh,
        data_fresh: data_grant == LockGrant::Fresh,
        fail_waiters: None,
    })
}

#[inline]
pub(crate) fn reject_table_ddl_explicit_session_lock(
    lock_manager: &LockManager,
    table_id: crate::catalog::TableID,
    owner: LockOwner,
    operation: &'static str,
) -> Result<()> {
    // Table DDL uses the session owner for scoped DDL locks. If an explicit
    // session table lock already exists, reusing that owner would become a
    // same-owner conversion and scoped cleanup could not distinguish the DDL
    // lock from the user-held session lock.
    let metadata_locked = lock_manager.owner_holds(
        LockResource::TableMetadata(table_id),
        owner,
        LockMode::Shared,
    );
    let data_locked = lock_manager.owner_holds(
        LockResource::TableData(table_id),
        owner,
        LockMode::IntentShared,
    );
    if !metadata_locked && !data_locked {
        return Ok(());
    }
    Err(Report::new(OperationError::LockOwnerGroupConflict)
        .attach(format!(
            "{operation} while session owns explicit table lock: table_id={table_id}, owner={owner:?}"
        ))
        .into())
}

pub(crate) struct ScopedTableDdlLocks<'a> {
    lock_manager: &'a LockManager,
    table_id: crate::catalog::TableID,
    owner: LockOwner,
    metadata_fresh: bool,
    data_fresh: bool,
    fail_waiters: Option<OperationError>,
}

impl ScopedTableDdlLocks<'_> {
    #[inline]
    pub(crate) fn fail_waiters_on_release(&mut self, error: OperationError) {
        self.fail_waiters = Some(error);
    }
}

impl Drop for ScopedTableDdlLocks<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.data_fresh {
            let resource = LockResource::TableData(self.table_id);
            if let Some(error) = self.fail_waiters {
                self.lock_manager
                    .release_and_fail_waiters(resource, self.owner, error);
            } else {
                self.lock_manager.release(resource, self.owner);
            }
        }
        if self.metadata_fresh {
            let resource = LockResource::TableMetadata(self.table_id);
            if let Some(error) = self.fail_waiters {
                self.lock_manager
                    .release_and_fail_waiters(resource, self.owner, error);
            } else {
                self.lock_manager.release(resource, self.owner);
            }
        }
    }
}

/// Sparse secondary-index metadata slots keyed by stable table-local index number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexSpecs {
    slots: Vec<Option<IndexSpec>>,
    active_count: usize,
}

impl IndexSpecs {
    #[inline]
    fn try_from_active(
        next_index_no: IndexNo,
        active_index_specs: Vec<ActiveIndexSpec>,
        col_count: usize,
    ) -> Result<Self> {
        let mut slots = vec![None; next_index_no as usize];
        let mut active_count = 0usize;
        for active_index_spec in active_index_specs {
            let index_no = active_index_spec.index_no as usize;
            if index_no >= next_index_no as usize {
                return Err(invalid_table_metadata(format!(
                    "index_no {index_no} must be less than next_index_no {next_index_no}"
                )));
            }
            if slots[index_no].is_some() {
                return Err(invalid_table_metadata(format!(
                    "duplicate index_no {index_no}"
                )));
            }
            validate_index_spec(index_no, &active_index_spec.spec, col_count)?;
            slots[index_no] = Some(active_index_spec.spec);
            active_count += 1;
        }
        Ok(Self {
            slots,
            active_count,
        })
    }

    /// Returns the sparse slot count, equal to table metadata `next_index_no`.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns whether there are no active secondary indexes.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.active_count == 0
    }

    /// Returns the number of active secondary indexes.
    #[inline]
    pub(crate) fn active_count(&self) -> usize {
        self.active_count
    }

    /// Returns active secondary indexes with their stable slot numbers.
    #[inline]
    pub(crate) fn active_indexes(&self) -> impl Iterator<Item = (usize, &IndexSpec)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(index_no, spec)| spec.as_ref().map(|spec| (index_no, spec)))
    }

    /// Returns active secondary-index specs only.
    #[inline]
    pub(crate) fn values(&self) -> impl Iterator<Item = &IndexSpec> {
        self.slots.iter().filter_map(Option::as_ref)
    }

    /// Returns one active secondary-index spec by stable slot number.
    #[inline]
    pub(crate) fn get(&self, index_no: usize) -> Option<&IndexSpec> {
        self.slots.get(index_no).and_then(Option::as_ref)
    }
}

impl Index<usize> for IndexSpecs {
    type Output = IndexSpec;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap_or_else(|| {
            panic!(
                "active index spec missing: index_no={index}, slot_count={}",
                self.len()
            )
        })
    }
}

/// Immutable physical column layout used to interpret row pages, LWC blocks,
/// and undo row bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableColumnLayout {
    pub(crate) col_names: Vec<SemiStr>,
    pub(crate) col_types: Vec<ValType>,
    pub(crate) col_attrs: Vec<ColumnAttributes>,
    // fix length is the total inline length of all columns.
    fix_len: usize,
    // index of var-length columns.
    var_cols: Vec<usize>,
    // number of nullable columns.
    nullable_cols: usize,
    // scan sums of null bitmap, it can locate null bitmap
    // in row page.
    null_scan_sums: Vec<usize>,
}

impl TableColumnLayout {
    /// Try to create a physical column layout from column specifications.
    #[inline]
    pub(crate) fn try_new(column_specs: Vec<ColumnSpec>) -> Result<Self> {
        if column_specs.is_empty() {
            return Err(invalid_table_metadata(
                "table column layout requires columns",
            ));
        }
        let col_names: Vec<_> = column_specs.iter().map(|c| c.column_name.clone()).collect();
        let col_attrs: Vec<_> = column_specs.iter().map(|c| c.column_attributes).collect();
        let col_types: Vec<_> = column_specs
            .iter()
            .map(|c| {
                let nullable = c.column_attributes.contains(ColumnAttributes::NULLABLE);
                ValType {
                    kind: c.column_type,
                    nullable,
                }
            })
            .collect();
        Self::try_create(col_names, col_types, col_attrs)
    }

    #[inline]
    fn try_create(
        col_names: Vec<SemiStr>,
        col_types: Vec<ValType>,
        col_attrs: Vec<ColumnAttributes>,
    ) -> Result<Self> {
        if col_names.len() != col_types.len() || col_names.len() != col_attrs.len() {
            return Err(invalid_table_metadata(format!(
                "column metadata length mismatch: names={}, types={}, attrs={}",
                col_names.len(),
                col_types.len(),
                col_attrs.len()
            )));
        }
        for (idx, ((col_name, col_type), col_attr)) in
            col_names.iter().zip(&col_types).zip(&col_attrs).enumerate()
        {
            let type_nullable = col_type.nullable;
            let attr_nullable = col_attr.contains(ColumnAttributes::NULLABLE);
            if type_nullable != attr_nullable {
                return Err(invalid_table_metadata(format!(
                    "column nullability metadata mismatch: column_index={idx}, column_name={}, type_nullable={type_nullable}, attr_nullable={attr_nullable}",
                    col_name.as_str()
                )));
            }
        }
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in col_types.iter().enumerate() {
            fix_len += ty.kind.inline_len();
            if !ty.kind.is_fixed() {
                var_cols.push(idx);
            }
        }
        // calculate column null bitmap offsets.
        let mut nullable_cols = 0usize;
        let mut null_scan_sums = vec![];
        for ty in &col_types {
            null_scan_sums.push(nullable_cols);
            nullable_cols += if ty.nullable { 1 } else { 0 };
        }
        Ok(Self {
            col_names,
            col_types,
            col_attrs,
            fix_len,
            var_cols,
            nullable_cols,
            null_scan_sums,
        })
    }

    /// Returns column count of this layout.
    #[inline]
    pub(crate) fn col_count(&self) -> usize {
        self.col_types.len()
    }

    /// Returns layouts of all columns.
    #[inline]
    pub(crate) fn col_types(&self) -> &[ValType] {
        &self.col_types
    }

    /// Returns column type of given position.
    #[inline]
    pub(crate) fn col_type(&self, col_idx: usize) -> ValType {
        self.col_types[col_idx]
    }

    /// Returns value kind of given column.
    #[inline]
    pub(crate) fn val_kind(&self, col_idx: usize) -> ValKind {
        self.col_type(col_idx).kind
    }

    /// Returns whether the given column is nullable.
    #[inline]
    pub(crate) fn nullable(&self, col_idx: usize) -> bool {
        self.col_type(col_idx).nullable
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub(crate) fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        val.matches_kind(self.col_type(col_idx).kind)
    }

    /// Returns current column offset, compared to all nullable columns.
    #[inline]
    pub(crate) fn null_offset(&self, col_idx: usize) -> usize {
        self.null_scan_sums[col_idx]
    }

    /// Returns column names in physical order.
    #[inline]
    pub(crate) fn col_names(&self) -> &[SemiStr] {
        &self.col_names
    }

    /// Returns column attributes in physical order.
    #[inline]
    pub(crate) fn col_attrs(&self) -> &[ColumnAttributes] {
        &self.col_attrs
    }

    /// Returns variable-length column positions.
    #[inline]
    pub(crate) fn var_cols(&self) -> &[usize] {
        &self.var_cols
    }

    /// Returns the total inline length of one logical row.
    #[inline]
    pub(crate) fn fix_len(&self) -> usize {
        self.fix_len
    }

    /// Returns the number of nullable columns.
    #[inline]
    pub(crate) fn nullable_col_count(&self) -> usize {
        self.nullable_cols
    }
}

/// Immutable sparse secondary-index layout for one table metadata envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableIndexLayout {
    // next table-local index number to allocate.
    next_index_no: IndexNo,
    // secondary index slots keyed by stable table-local index number.
    index_specs: IndexSpecs,
    // columns that are included in any index.
    index_cols: HashSet<usize>,
}

impl TableIndexLayout {
    #[inline]
    fn try_create(
        column_layout: &TableColumnLayout,
        index_specs: Vec<ActiveIndexSpec>,
        next_index_no: IndexNo,
    ) -> Result<Self> {
        let index_specs =
            IndexSpecs::try_from_active(next_index_no, index_specs, column_layout.col_count())?;
        let mut index_cols = HashSet::new();
        for index_spec in index_specs.values() {
            for key in &index_spec.cols {
                index_cols.insert(key.col_no as usize);
            }
        }
        Ok(Self {
            next_index_no,
            index_specs,
            index_cols,
        })
    }

    /// Returns the next table-local index number to allocate.
    #[inline]
    pub(crate) fn next_index_no(&self) -> IndexNo {
        self.next_index_no
    }

    /// Allocates the next table-local index number and returns an index layout with
    /// the new active index appended in the corresponding sparse slot.
    #[inline]
    fn try_with_created_index(
        &self,
        column_layout: &TableColumnLayout,
        index_spec: IndexSpec,
    ) -> Result<(IndexNo, Self)> {
        let index_no = self.next_index_no;
        validate_index_spec(index_no as usize, &index_spec, column_layout.col_count())?;
        let next_index_no = index_no
            .checked_add(1)
            .ok_or_else(|| invalid_index_spec("next_index_no overflow"))?;
        let mut index_specs = self
            .active_indexes()
            .map(|(index_no, spec)| ActiveIndexSpec::new(index_no as IndexNo, spec.clone()))
            .collect::<Vec<_>>();
        index_specs.push(ActiveIndexSpec::new(index_no, index_spec));
        let index_layout = Self::try_create(column_layout, index_specs, next_index_no)?;
        Ok((index_no, index_layout))
    }

    /// Returns an index layout with one active index slot made inactive.
    #[inline]
    fn try_without_index(
        &self,
        column_layout: &TableColumnLayout,
        index_no: IndexNo,
    ) -> Result<Self> {
        let index_no_usize = usize::from(index_no);
        if index_no_usize >= self.index_slot_count() {
            return Err(index_not_found(format!(
                "drop index out of range: index_no={index_no}, next_index_no={}",
                self.next_index_no
            )));
        }
        if self.index_spec(index_no_usize).is_none() {
            return Err(index_not_found(format!(
                "drop index inactive slot: index_no={index_no}, next_index_no={}",
                self.next_index_no
            )));
        }

        let index_specs = self
            .active_indexes()
            .filter(|(active_index_no, _)| *active_index_no != index_no_usize)
            .map(|(active_index_no, spec)| {
                ActiveIndexSpec::new(active_index_no as IndexNo, spec.clone())
            })
            .collect::<Vec<_>>();
        Self::try_create(column_layout, index_specs, self.next_index_no)
    }

    /// Returns the sparse secondary-index slot count.
    #[inline]
    pub(crate) fn index_slot_count(&self) -> usize {
        self.next_index_no as usize
    }

    /// Returns the active secondary-index count.
    #[inline]
    pub(crate) fn active_index_count(&self) -> usize {
        self.index_specs.active_count()
    }

    /// Returns active secondary indexes with their stable slot numbers.
    #[inline]
    pub(crate) fn active_indexes(&self) -> impl Iterator<Item = (usize, &IndexSpec)> {
        self.index_specs.active_indexes()
    }

    /// Returns one active secondary-index spec by stable index number.
    #[inline]
    pub(crate) fn index_spec(&self, index_no: usize) -> Option<&IndexSpec> {
        self.index_specs.get(index_no)
    }

    /// Requires one active secondary-index spec by stable index number.
    #[inline]
    pub(crate) fn require_index_spec(&self, index_no: usize) -> Result<&IndexSpec> {
        self.index_spec(index_no).ok_or_else(|| {
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach(format!(
                    "index_no={index_no}, index_slot_count={}",
                    self.index_slot_count()
                ))
                .into()
        })
    }

    /// Returns the sparse secondary-index specs.
    #[inline]
    pub(crate) fn index_specs(&self) -> &IndexSpecs {
        &self.index_specs
    }

    /// Returns columns included in any active secondary index.
    #[inline]
    pub(crate) fn index_columns(&self) -> &HashSet<usize> {
        &self.index_cols
    }

    /// Returns whether input values matches given index.
    #[inline]
    pub(crate) fn index_type_match(
        &self,
        column_layout: &TableColumnLayout,
        index_no: usize,
        vals: &[Val],
    ) -> bool {
        let Some(index) = self.index_spec(index_no) else {
            return false;
        };
        if index.cols.len() != vals.len() {
            return false;
        }
        index
            .cols
            .iter()
            .map(|k| column_layout.col_type(k.col_no as usize).kind)
            .zip(vals)
            .all(|(kind, val)| val.matches_kind(kind))
    }

    /// Returns index keys of a new row.
    #[inline]
    pub(crate) fn keys_for_insert(&self, row: &[Val]) -> Vec<SelectKey> {
        self.active_indexes()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .cols
                    .iter()
                    .map(|k| row[k.col_no as usize].clone())
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    /// Returns index keys of deletion of a row.
    #[inline]
    pub(crate) fn keys_for_delete(
        &self,
        column_layout: &TableColumnLayout,
        row: Row<'_>,
    ) -> Vec<SelectKey> {
        self.active_indexes()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .cols
                    .iter()
                    .map(|k| row.val(column_layout, k.col_no as usize))
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    /// Returns whether key matches given row.
    #[inline]
    pub(crate) fn match_key(&self, key: &SelectKey, row: &[Val]) -> bool {
        let Some(keys) = self.index_spec(key.index_no).map(|spec| &spec.cols) else {
            return false;
        };
        debug_assert!(keys.len() == key.vals.len());
        keys.iter()
            .zip(&key.vals)
            .all(|(key, val)| &row[key.col_no as usize] == val)
    }
}

/// Table metadata including column layout and index layout.
/// Constraints and other advanced configurations are not implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMetadata {
    pub(crate) col: Arc<TableColumnLayout>,
    pub(crate) idx: TableIndexLayout,
}

impl TableMetadata {
    /// Try to create metadata of a new table.
    #[inline]
    pub(crate) fn try_new(
        column_specs: Vec<ColumnSpec>,
        index_specs: Vec<IndexSpec>,
    ) -> Result<Self> {
        let next_index_no = IndexNo::try_from(index_specs.len()).map_err(|_| {
            invalid_table_metadata("next_index_no overflow while deriving table metadata")
        })?;
        let col_count = column_specs.len();
        let active_index_specs = index_specs
            .into_iter()
            .enumerate()
            .map(|(index_no, spec)| {
                if col_count > 0 {
                    validate_index_spec(index_no, &spec, col_count)?;
                }
                Ok(ActiveIndexSpec::new(index_no as IndexNo, spec))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::try_new_with_next_index_no(column_specs, active_index_specs, next_index_no)
    }

    /// Try to create metadata with an explicit durable next index number.
    #[inline]
    pub(crate) fn try_new_with_next_index_no(
        column_specs: Vec<ColumnSpec>,
        index_specs: Vec<ActiveIndexSpec>,
        next_index_no: IndexNo,
    ) -> Result<Self> {
        let column_layout = Arc::new(TableColumnLayout::try_new(column_specs)?);
        let index_layout =
            TableIndexLayout::try_create(&column_layout, index_specs, next_index_no)?;
        Ok(Self {
            col: column_layout,
            idx: index_layout,
        })
    }

    #[inline]
    fn try_create(
        col_names: Vec<SemiStr>,
        col_types: Vec<ValType>,
        col_attrs: Vec<ColumnAttributes>,
        index_specs: Vec<ActiveIndexSpec>,
        next_index_no: IndexNo,
    ) -> Result<Self> {
        let column_layout = Arc::new(TableColumnLayout::try_create(
            col_names, col_types, col_attrs,
        )?);
        let index_layout =
            TableIndexLayout::try_create(&column_layout, index_specs, next_index_no)?;
        Ok(Self {
            col: column_layout,
            idx: index_layout,
        })
    }

    /// Allocates the next table-local index number and returns metadata with
    /// the new active index appended in the corresponding sparse slot.
    #[inline]
    pub(crate) fn try_with_created_index(&self, index_spec: IndexSpec) -> Result<(IndexNo, Self)> {
        let (index_no, index_layout) = self.idx.try_with_created_index(&self.col, index_spec)?;
        let metadata = Self {
            col: Arc::clone(&self.col),
            idx: index_layout,
        };
        Ok((index_no, metadata))
    }

    /// Returns metadata with one active index slot made inactive.
    #[inline]
    pub(crate) fn try_without_index(&self, index_no: IndexNo) -> Result<Self> {
        let index_no_usize = usize::from(index_no);
        if index_no_usize >= self.idx.index_slot_count() {
            return Err(index_not_found(format!(
                "drop index out of range: index_no={index_no}, next_index_no={}",
                self.idx.next_index_no()
            )));
        }
        if self.idx.index_spec(index_no_usize).is_none() {
            return Err(index_not_found(format!(
                "drop index inactive slot: index_no={index_no}, next_index_no={}",
                self.idx.next_index_no()
            )));
        }

        let index_layout = self.idx.try_without_index(&self.col, index_no)?;
        Ok(Self {
            col: Arc::clone(&self.col),
            idx: index_layout,
        })
    }

    /// Create a view for serialization.
    #[inline]
    pub(crate) fn ser_view(&self) -> TableBriefMetadataSerView<'_> {
        TableBriefMetadataSerView {
            col_names: self.col.col_names(),
            col_types: self.col.col_types(),
            col_attrs: self.col.col_attrs(),
            next_index_no: self.idx.next_index_no(),
            index_specs: self.idx.index_specs(),
        }
    }
}

impl TryFrom<TableBriefMetadata> for TableMetadata {
    type Error = Error;

    #[inline]
    fn try_from(value: TableBriefMetadata) -> Result<Self> {
        TableMetadata::try_create(
            value.col_names,
            value.col_types,
            value.col_attrs,
            value.index_specs,
            value.next_index_no,
        )
    }
}

/// View of necessary information to recover table
/// metadata.
/// It's used for serialization.
pub(crate) struct TableBriefMetadataSerView<'a> {
    pub(crate) col_names: &'a [SemiStr],
    pub(crate) col_types: &'a [ValType],
    pub(crate) col_attrs: &'a [ColumnAttributes],
    pub(crate) next_index_no: IndexNo,
    pub(crate) index_specs: &'a IndexSpecs,
}

impl<'a> Ser<'a> for TableBriefMetadataSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.col_names.ser_len()
            + self.col_types.ser_len()
            + self.col_attrs.ser_len()
            + mem::size_of::<IndexNo>()
            + mem::size_of::<u64>()
            + self
                .index_specs
                .active_indexes()
                .map(|(_, index_spec)| mem::size_of::<IndexNo>() + index_spec.ser_len())
                .sum::<usize>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.col_names.ser(out, start_idx);
        let idx = self.col_types.ser(out, idx);
        let idx = self.col_attrs.ser(out, idx);
        let mut idx = out.ser_u16(idx, self.next_index_no);
        idx = out.ser_u64(idx, self.index_specs.active_count() as u64);
        for (index_no, index_spec) in self.index_specs.active_indexes() {
            idx = out.ser_u16(idx, index_no as IndexNo);
            idx = index_spec.ser(out, idx);
        }
        idx
    }
}

/// Brief metadata of a table.
/// It's used as a deserialization container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableBriefMetadata {
    pub(crate) col_names: Vec<SemiStr>,
    pub(crate) col_types: Vec<ValType>,
    pub(crate) col_attrs: Vec<ColumnAttributes>,
    pub(crate) next_index_no: IndexNo,
    pub(crate) index_specs: Vec<ActiveIndexSpec>,
}

impl Deser for TableBriefMetadata {
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, col_names) = <Vec<SemiStr>>::deser(input, start_idx)?;
        let (idx, col_types) = <Vec<ValType>>::deser(input, idx)?;
        let (idx, col_attrs) = <Vec<ColumnAttributes>>::deser(input, idx)?;
        let (idx, next_index_no) = input.deser_u16(idx)?;
        let (idx, index_specs) = <Vec<ActiveIndexSpec>>::deser(input, idx)?;
        Ok((
            idx,
            TableBriefMetadata {
                col_names,
                col_types,
                col_attrs,
                next_index_no,
                index_specs,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnSpec, IndexAttributes, IndexKey};

    fn assert_invalid_index_spec(err: Error, expected_message: &str) {
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidIndexSpec)
        );
        let report = format!("{err:?}");
        assert!(report.contains(expected_message), "{report}");
    }

    #[test]
    fn test_table_metadata_serde() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .expect("valid table metadata");

        let ser_view = metadata.ser_view();

        let len = ser_view.ser_len();
        let mut vec = vec![0u8; len];
        let idx = ser_view.ser(&mut vec[..], 0);
        assert_eq!(idx, vec.len());
        let (idx, brief) = TableBriefMetadata::deser(&vec[..], 0).unwrap();
        assert_eq!(idx, vec.len());
        assert_eq!(metadata.col.col_names, brief.col_names);
        assert_eq!(metadata.col.col_types, brief.col_types);
        assert_eq!(metadata.col.col_attrs, brief.col_attrs);
        assert_eq!(metadata.idx.next_index_no(), brief.next_index_no);
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(index_no, spec)| ActiveIndexSpec::new(index_no as IndexNo, spec.clone()))
                .collect::<Vec<_>>(),
            brief.index_specs
        );
    }

    #[test]
    fn test_table_metadata_dense_indexes_derive_next_index_no() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
            ],
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ],
        )
        .expect("valid table metadata");
        assert_eq!(metadata.idx.next_index_no(), 2);
        assert_eq!(metadata.idx.index_slot_count(), 2);
        assert_eq!(metadata.idx.active_index_count(), 2);
    }

    #[test]
    fn test_table_metadata_index_only_changes_share_column_layout() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::VarByte, ColumnAttributes::NULLABLE),
            ],
            vec![],
        )
        .expect("valid table metadata");
        assert_eq!(metadata.col.col_count(), 2);
        assert_eq!(
            metadata.col.fix_len(),
            ValKind::U32.inline_len() + ValKind::VarByte.inline_len()
        );
        assert_eq!(metadata.col.var_cols(), &[1]);
        assert_eq!(metadata.col.nullable_col_count(), 1);
        assert_eq!(metadata.col.null_offset(0), 0);
        assert_eq!(metadata.col.null_offset(1), 0);

        let (index_no, created) = metadata
            .try_with_created_index(IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK))
            .unwrap();
        let dropped = created.try_without_index(index_no).unwrap();

        assert!(Arc::ptr_eq(&metadata.col, &created.col));
        assert!(Arc::ptr_eq(&metadata.col, &dropped.col));
        assert_eq!(created.idx.active_index_count(), 1);
        assert_eq!(dropped.idx.active_index_count(), 0);
    }

    #[test]
    fn test_table_metadata_rejects_inconsistent_column_nullability() {
        let brief = TableBriefMetadata {
            col_names: vec![SemiStr::new("c0")],
            col_types: vec![ValType::new(ValKind::U32, true)],
            col_attrs: vec![ColumnAttributes::empty()],
            next_index_no: 0,
            index_specs: vec![],
        };

        let err = TableMetadata::try_from(brief).unwrap_err();
        let report = format!("{err:?}");
        assert!(report.contains("column_index=0"), "{report}");
        assert!(report.contains("column_name=c0"), "{report}");
        assert!(report.contains("type_nullable=true"), "{report}");
        assert!(report.contains("attr_nullable=false"), "{report}");
    }

    #[test]
    fn test_table_metadata_sparse_active_indexes_preserve_index_no() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c2", ValKind::U32, ColumnAttributes::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    2,
                    IndexSpec::new(vec![IndexKey::new(2)], IndexAttributes::empty()),
                ),
            ],
            3,
        )
        .unwrap();

        assert_eq!(metadata.idx.next_index_no(), 3);
        assert_eq!(metadata.idx.index_slot_count(), 3);
        assert!(metadata.idx.index_spec(1).is_none());
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(index_no, _)| index_no)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let keys =
            metadata
                .idx
                .keys_for_insert(&[Val::from(11u32), Val::from(22u64), Val::from(33u32)]);
        assert_eq!(keys[0].index_no, 0);
        assert_eq!(keys[1].index_no, 2);
    }

    #[test]
    fn test_table_metadata_rejects_invalid_index_slots() {
        let columns = vec![ColumnSpec::new(
            "c0",
            ValKind::U32,
            ColumnAttributes::empty(),
        )];
        assert!(
            TableMetadata::try_new_with_next_index_no(
                columns.clone(),
                vec![ActiveIndexSpec::new(
                    1,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                )],
                1,
            )
            .is_err()
        );
        assert!(
            TableMetadata::try_new_with_next_index_no(
                columns.clone(),
                vec![
                    ActiveIndexSpec::new(
                        0,
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    ),
                    ActiveIndexSpec::new(
                        0,
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty()),
                    ),
                ],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn test_table_metadata_rejects_invalid_index_specs_as_config_errors() {
        let columns = vec![ColumnSpec::new(
            "c0",
            ValKind::U32,
            ColumnAttributes::empty(),
        )];

        let err = TableMetadata::try_new(
            columns.clone(),
            vec![IndexSpec::new(vec![], IndexAttributes::PK)],
        )
        .unwrap_err();
        assert_invalid_index_spec(err, "index_no 0 has no key columns");

        let err = TableMetadata::try_new_with_next_index_no(
            columns.clone(),
            vec![ActiveIndexSpec::new(
                1,
                IndexSpec::new(vec![], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap_err();
        assert_invalid_index_spec(err, "index_no 1 has no key columns");

        let err = TableMetadata::try_new(
            columns,
            vec![IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::PK)],
        )
        .unwrap_err();
        assert_invalid_index_spec(err, "index_no 0 references column 1 outside column count 1");
    }

    #[test]
    fn test_table_metadata_create_index_allocates_sparse_next_slot() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c2", ValKind::U32, ColumnAttributes::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    2,
                    IndexSpec::new(vec![IndexKey::new(2)], IndexAttributes::empty()),
                ),
            ],
            3,
        )
        .unwrap();

        let (index_no, metadata) = metadata
            .try_with_created_index(IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK))
            .unwrap();

        assert_eq!(index_no, 3);
        assert_eq!(metadata.idx.next_index_no(), 4);
        assert_eq!(metadata.idx.index_slot_count(), 4);
        assert!(metadata.idx.index_spec(1).is_none());
        assert!(metadata.idx.index_spec(3).unwrap().unique());
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(index_no, _)| index_no)
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn test_table_metadata_create_index_rejects_invalid_spec() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");

        assert!(
            metadata
                .try_with_created_index(IndexSpec::new(vec![], IndexAttributes::UK))
                .is_err()
        );
        assert!(
            metadata
                .try_with_created_index(
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK,)
                )
                .is_err()
        );
    }

    #[test]
    fn test_table_metadata_create_index_rejects_next_index_overflow() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![],
            IndexNo::MAX,
        )
        .unwrap();

        assert!(
            metadata
                .try_with_created_index(IndexSpec::new(
                    vec![IndexKey::new(0)],
                    IndexAttributes::empty(),
                ))
                .is_err()
        );
    }

    #[test]
    fn test_table_metadata_drop_index_preserves_sparse_allocation() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c2", ValKind::U32, ColumnAttributes::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    2,
                    IndexSpec::new(vec![IndexKey::new(2)], IndexAttributes::empty()),
                ),
            ],
            4,
        )
        .unwrap();

        let dropped = metadata.try_without_index(2).unwrap();

        assert_eq!(dropped.idx.next_index_no(), 4);
        assert_eq!(dropped.idx.index_slot_count(), 4);
        assert_eq!(dropped.idx.active_index_count(), 1);
        assert!(dropped.idx.index_spec(0).is_some());
        assert!(dropped.idx.index_spec(1).is_none());
        assert!(dropped.idx.index_spec(2).is_none());
        assert!(dropped.idx.index_spec(3).is_none());
        assert_eq!(dropped.idx.index_cols, HashSet::from([0]));
    }

    #[test]
    fn test_table_metadata_drop_index_rejects_inactive_and_out_of_range() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();

        let inactive = metadata.try_without_index(1).unwrap_err();
        assert_eq!(
            inactive.operation_error(),
            Some(OperationError::IndexNotFound)
        );
        let out_of_range = metadata.try_without_index(2).unwrap_err();
        assert_eq!(
            out_of_range.operation_error(),
            Some(OperationError::IndexNotFound)
        );
    }
}
