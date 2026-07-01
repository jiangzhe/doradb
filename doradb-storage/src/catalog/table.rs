use crate::buffer::PoolGuards;
use crate::catalog::spec::{ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexNo, IndexSpec};
use crate::catalog::{ColumnObject, IndexColumnObject, IndexObject, TableObject, is_user_table};
use crate::engine::EngineRef;
use crate::error::{ConfigError, Error, FatalError, InternalError, OperationError, Result};
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::id::{TableID, TrxID};
use crate::index::BlockIndex;
use crate::log::redo::DDLRedo;
use crate::map::FastHashSet;
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::serde::{Deser, MinBytesHint, Ser, Serde, min_bytes_hint};
use crate::session::{SessionDdlContext, SessionPin};
use crate::table::{Table, TableRedoReplayFloor};
use crate::trx::Transaction;
use crate::value::{Val, ValKind, ValType};
use error_stack::Report;
use semistr::SemiStr;
use std::mem;
use std::ops::Index;
use std::result::Result as StdResult;
use std::sync::Arc;
#[cfg(test)]
use tests::{
    CreateTableTestFailure, maybe_fail_create_table,
    maybe_poison_before_create_table_catalog_commit,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateTablePhase {
    Init,
    CatalogStaged,
    FilePublished,
    RuntimeBuilt,
    CatalogCommitted,
    Installed,
    Aborted,
}

impl CreateTablePhase {
    #[inline]
    fn is_terminal(self) -> bool {
        matches!(self, Self::Installed | Self::Aborted)
    }
}

struct CreateTableProgress {
    table_id: TableID,
    phase: CreateTablePhase,
    mutable_file: Option<MutableTableFile>,
    trx: Option<Transaction>,
    table_file: Option<Arc<TableFile>>,
    staged_table: Option<Arc<Table>>,
}

impl CreateTableProgress {
    #[inline]
    fn new(table_id: TableID, mutable_file: MutableTableFile) -> Self {
        Self {
            table_id,
            phase: CreateTablePhase::Init,
            mutable_file: Some(mutable_file),
            trx: None,
            table_file: None,
            staged_table: None,
        }
    }

    #[inline]
    fn set_catalog_transaction(&mut self, trx: Transaction) {
        debug_assert!(self.trx.is_none());
        self.trx = Some(trx);
    }

    #[inline]
    fn mark_catalog_staged(&mut self) {
        debug_assert_eq!(self.phase, CreateTablePhase::Init);
        self.phase = CreateTablePhase::CatalogStaged;
    }

    #[inline]
    async fn publish_file(&mut self, engine: &EngineRef) -> Result<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::CatalogStaged);
        let root_ts = self
            .trx
            .as_ref()
            .expect("catalog transaction is staged before file publish")
            .sts();
        let mutable_file = self
            .mutable_file
            .take()
            .expect("mutable create-table file is present before publish");
        let table_file = engine
            .trx_sys
            .publish_table_file_root(mutable_file, root_ts, true)
            .await?;
        self.table_file = Some(table_file);
        self.phase = CreateTablePhase::FilePublished;
        Ok(())
    }

    #[inline]
    async fn build_runtime(&mut self, guards: &PoolGuards, engine: &EngineRef) -> Result<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::FilePublished);
        let table_file = Arc::clone(
            self.table_file
                .as_ref()
                .expect("published table file is present before runtime build"),
        );
        let active_root = table_file.active_root_unchecked();
        let blk_idx = BlockIndex::new(
            engine.meta_pool.clone_inner(),
            guards.meta_guard(),
            active_root.pivot_row_id,
            active_root.column_block_index_root,
        )
        .await?;
        let table = Arc::new(
            Table::new(
                engine.mem_pool.clone_inner(),
                engine.index_pool.clone_inner(),
                guards.index_guard(),
                self.table_id,
                blk_idx,
                table_file,
                engine.disk_pool.clone_inner(),
            )
            .await?,
        );
        self.staged_table = Some(table);
        self.phase = CreateTablePhase::RuntimeBuilt;
        Ok(())
    }

    #[inline]
    async fn commit_catalog(&mut self) -> Result<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::RuntimeBuilt);
        let trx = self
            .trx
            .take()
            .expect("catalog transaction is present before commit");
        trx.commit().await?;
        self.phase = CreateTablePhase::CatalogCommitted;
        Ok(())
    }

    #[inline]
    fn install_runtime(&mut self, engine: &EngineRef) -> Result<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::CatalogCommitted);
        let table = Arc::clone(
            self.staged_table
                .as_ref()
                .expect("staged table runtime is present before install"),
        );
        engine.catalog().insert_user_table(table)?;
        let _ = self.staged_table.take();
        self.phase = CreateTablePhase::Installed;
        Ok(())
    }

    #[inline]
    fn delete_provisional_file(&mut self, engine: &EngineRef) -> Result<()> {
        if let Some(mutable_file) = self.mutable_file.take() {
            let _ = mutable_file.try_delete();
        }
        let _ = self.table_file.take();
        engine.table_fs.delete_user_table_file(self.table_id)
    }

    async fn destroy_staged_runtime(&mut self, guards: &PoolGuards) -> Result<()> {
        let Some(table) = self.staged_table.take() else {
            return Ok(());
        };
        let table = Arc::try_unwrap(table).map_err(|table| {
            Report::new(InternalError::Generic).attach(format!(
                "staged create-table runtime still referenced during cleanup: table_id={}, strong_count={}",
                self.table_id,
                Arc::strong_count(&table)
            ))
        })?;
        table.destroy_dropped_runtime(guards).await
    }

    async fn abort_before_catalog_commit(
        &mut self,
        engine: &EngineRef,
        guards: &PoolGuards,
        operation: &'static str,
        source: Error,
    ) -> Error {
        let source_debug = format!("{source:?}");
        let mut cleanup_error = None;
        if let Err(err) = self.destroy_staged_runtime(guards).await {
            cleanup_error = Some(
                poison_create_table_cleanup_with_source(
                    engine,
                    self.table_id,
                    operation,
                    "runtime destroy",
                    &source_debug,
                    err,
                )
                .into(),
            );
        }
        if let Some(trx) = self.trx.take()
            && trx.engine().is_some()
            && let Err(err) = trx.rollback().await
            && cleanup_error.is_none()
        {
            cleanup_error = Some(
                poison_create_table_rollback_with_source(
                    engine,
                    self.table_id,
                    operation,
                    &source_debug,
                    err,
                )
                .into(),
            );
        }
        if let Err(err) = self.delete_provisional_file(engine)
            && cleanup_error.is_none()
        {
            cleanup_error = Some(err);
        }
        self.phase = CreateTablePhase::Aborted;
        cleanup_error.unwrap_or(source)
    }

    async fn abort_after_root_publish_commit_error(
        &mut self,
        engine: &EngineRef,
        guards: &PoolGuards,
        operation: &'static str,
        source: Error,
    ) -> Error {
        let source_debug = format!("{source:?}");
        if let Err(err) = self.destroy_staged_runtime(guards).await {
            self.phase = CreateTablePhase::Aborted;
            return poison_create_table_cleanup_with_source(
                engine,
                self.table_id,
                operation,
                "runtime destroy after root publish",
                &source_debug,
                err,
            )
            .into();
        }
        self.phase = CreateTablePhase::Aborted;
        poison_create_table_after_root_publish_with_source(engine, self.table_id, operation, source)
            .into()
    }
}

impl Drop for CreateTableProgress {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(
            self.phase.is_terminal(),
            "create-table progress dropped in non-terminal phase: table_id={}, phase={:?}",
            self.table_id,
            self.phase
        );
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
    /// Column names in physical column order.
    pub(crate) col_names: Vec<SemiStr>,
    /// Column value types in physical column order.
    pub(crate) col_types: Vec<ValType>,
    /// Column attributes in physical column order.
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
        if col_names.is_empty() || col_types.is_empty() || col_attrs.is_empty() {
            return Err(invalid_table_metadata(
                "table column layout requires columns",
            ));
        }
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
        let col_type = self.col_type(col_idx);
        if matches!(val, Val::Null) {
            col_type.nullable
        } else {
            val.matches_kind(col_type.kind)
        }
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
    index_cols: FastHashSet<usize>,
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
        validate_primary_key_contract(column_layout, &index_specs)?;
        let mut index_cols = FastHashSet::default();
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

    /// Returns the primary-key index number and spec when this table has one.
    #[inline]
    pub(crate) fn primary_key_index(&self) -> Option<(usize, &IndexSpec)> {
        self.active_indexes()
            .find(|(_, index_spec)| index_spec.primary_key())
    }

    /// Returns the sparse secondary-index specs.
    #[inline]
    pub(crate) fn index_specs(&self) -> &IndexSpecs {
        &self.index_specs
    }

    /// Returns columns included in any active secondary index.
    #[inline]
    pub(crate) fn index_columns(&self) -> &FastHashSet<usize> {
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
            .zip(vals)
            .all(|(key, val)| column_layout.col_type_match(usize::from(key.col_no), val))
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

/// Borrowed primary-key metadata view with enough context to validate keys.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PrimaryKeySpec<'a> {
    index_no: usize,
    index_spec: &'a IndexSpec,
    column_layout: &'a TableColumnLayout,
}

impl<'a> PrimaryKeySpec<'a> {
    /// Returns the stable table-local primary-key index number.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        self.index_no
    }

    /// Returns the primary-key index specification.
    #[inline]
    pub(crate) fn spec(&self) -> &'a IndexSpec {
        self.index_spec
    }

    /// Returns whether the input key targets and matches this primary key.
    #[inline]
    pub(crate) fn matches_key(&self, key: &SelectKey) -> bool {
        self.validate_key(key).is_ok()
    }

    /// Validates that the input key targets this primary key and matches its
    /// column shape.
    #[inline]
    pub(crate) fn validate_key(&self, key: &SelectKey) -> StdResult<(), PrimaryKeyMatchError> {
        if key.index_no != self.index_no {
            return Err(PrimaryKeyMatchError::IndexNo {
                actual: key.index_no,
                expected: self.index_no,
            });
        }
        if key.vals.len() != self.index_spec.cols.len() {
            return Err(PrimaryKeyMatchError::ValueCount {
                actual: key.vals.len(),
                expected: self.index_spec.cols.len(),
            });
        }
        if !self
            .index_spec
            .cols
            .iter()
            .zip(&key.vals)
            .all(|(index_key, val)| {
                self.column_layout
                    .col_type_match(usize::from(index_key.col_no), val)
            })
        {
            return Err(PrimaryKeyMatchError::Type {
                index_no: key.index_no,
            });
        }
        Ok(())
    }
}

/// Why an input [`SelectKey`] does not match a primary-key specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrimaryKeyMatchError {
    IndexNo { actual: usize, expected: usize },
    ValueCount { actual: usize, expected: usize },
    Type { index_no: usize },
}

/// Table metadata including column layout and index layout.
/// Constraints and other advanced configurations are not implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMetadata {
    /// Physical column layout.
    pub(crate) col: Arc<TableColumnLayout>,
    /// Sparse secondary-index layout.
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

    /// Returns the primary-key metadata view when this table has one.
    #[inline]
    pub(crate) fn primary_key(&self) -> Option<PrimaryKeySpec<'_>> {
        self.idx
            .primary_key_index()
            .map(|(index_no, index_spec)| PrimaryKeySpec {
                index_no,
                index_spec,
                column_layout: self.col.as_ref(),
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
    /// Column names in physical column order.
    pub(crate) col_names: &'a [SemiStr],
    /// Column value types in physical column order.
    pub(crate) col_types: &'a [ValType],
    /// Column attributes in physical column order.
    pub(crate) col_attrs: &'a [ColumnAttributes],
    /// Next table-local secondary-index number.
    pub(crate) next_index_no: IndexNo,
    /// Active sparse secondary-index specs.
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
    /// Column names in physical column order.
    pub(crate) col_names: Vec<SemiStr>,
    /// Column value types in physical column order.
    pub(crate) col_types: Vec<ValType>,
    /// Column attributes in physical column order.
    pub(crate) col_attrs: Vec<ColumnAttributes>,
    /// Next table-local secondary-index number.
    pub(crate) next_index_no: IndexNo,
    /// Active sparse secondary-index specs.
    pub(crate) index_specs: Vec<ActiveIndexSpec>,
}

impl Deser for TableBriefMetadata {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(
        mem::size_of::<u64>() * 4 // four vector length prefixes
            + mem::size_of::<u16>(), // next_index_no
    );

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

/// Create a new user table for a session-level DDL request.
pub(crate) async fn create_table_for_session(
    session: SessionPin,
    table_spec: super::TableSpec,
    index_specs: Vec<IndexSpec>,
) -> Result<TableID> {
    let ctx = SessionDdlContext::new(&session)?;
    let engine = ctx.engine.clone();
    let guards = ctx.pool_guards.clone();
    reject_user_table_primary_key_indexes(&index_specs, "create table")?;
    let _namespace_lock = engine
        .lock_manager()
        .acquire_catalog_namespace_lock(ctx.owner, ctx.owner_group)
        .await?;

    let table_id = engine.catalog().next_table_id();
    let metadata = Arc::new(TableMetadata::try_new(
        table_spec.columns.clone(),
        index_specs.clone(),
    )?);
    let uninit_table_file =
        engine
            .table_fs
            .create_table_file(table_id, Arc::clone(&metadata), false)?;

    let table_object = TableObject {
        table_id,
        next_index_no: metadata.idx.next_index_no(),
    };
    let column_objects: Vec<_> = table_spec
        .columns
        .iter()
        .enumerate()
        .map(|(col_no, col_spec)| ColumnObject {
            table_id,
            column_no: col_no as u16,
            column_name: col_spec.column_name.clone(),
            column_type: col_spec.column_type,
            column_attributes: col_spec.column_attributes,
        })
        .collect();

    let mut index_objects = Vec::new();
    let mut index_column_objects = Vec::new();
    for (index_no, index_spec) in metadata.idx.active_indexes() {
        index_objects.push(IndexObject {
            table_id,
            index_no: index_no as u16,
            index_attributes: index_spec.attributes,
        });
        for (index_column_no, ik) in index_spec.cols.iter().enumerate() {
            index_column_objects.push(IndexColumnObject {
                table_id,
                index_no: index_no as u16,
                index_column_no: index_column_no as u16,
                column_no: ik.col_no,
                index_order: ik.order,
            });
        }
    }

    let mut progress = CreateTableProgress::new(table_id, uninit_table_file);
    let mut trx = match session.begin_trx("begin transaction") {
        Ok(trx) => trx,
        Err(err) => {
            let delete_res = progress.delete_provisional_file(&engine);
            progress.phase = CreateTablePhase::Aborted;
            delete_res?;
            return Err(err);
        }
    };

    let exec_res = execute_create_table_catalog_staging(
        &engine,
        &mut trx,
        table_id,
        table_object,
        column_objects,
        index_objects,
        index_column_objects,
    )
    .await;
    progress.set_catalog_transaction(trx);
    if let Err(err) = exec_res {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "catalog staging", err)
            .await);
    }
    progress.mark_catalog_staged();

    #[cfg(test)]
    if let Err(err) = maybe_fail_create_table(CreateTableTestFailure::AfterCatalogStaged) {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "test after catalog staging", err)
            .await);
    }

    if let Err(err) = progress.publish_file(&engine).await {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "file publish", err)
            .await);
    }

    #[cfg(test)]
    if let Err(err) = maybe_fail_create_table(CreateTableTestFailure::AfterFilePublished) {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "test after file publish", err)
            .await);
    }

    if let Err(err) = progress.build_runtime(&guards, &engine).await {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "runtime build", err)
            .await);
    }

    #[cfg(test)]
    if let Err(err) = maybe_fail_create_table(CreateTableTestFailure::AfterRuntimeBuilt) {
        return Err(progress
            .abort_before_catalog_commit(&engine, &guards, "test after runtime build", err)
            .await);
    }

    #[cfg(test)]
    maybe_poison_before_create_table_catalog_commit(&engine);

    if let Err(err) = progress.commit_catalog().await {
        return Err(progress
            .abort_after_root_publish_commit_error(&engine, &guards, "catalog commit", err)
            .await);
    }

    if let Err(err) = progress.install_runtime(&engine) {
        return Err(progress
            .abort_after_root_publish_commit_error(&engine, &guards, "runtime install", err)
            .await);
    }

    Ok(table_id)
}

/// Logically drop an existing user table for a session-level DDL request.
pub(crate) async fn drop_table_for_session(session: SessionPin, table_id: TableID) -> Result<()> {
    let ctx = SessionDdlContext::new(&session)?;
    let engine = ctx.engine.clone();
    let lock_manager = engine.lock_manager();
    // Keep this guard alive until the catalog entry is transitioned to dropped
    // state so table identity changes remain namespace-serialized.
    let _namespace_lock = lock_manager
        .acquire_catalog_namespace_lock(ctx.owner, ctx.owner_group)
        .await?;

    let table = validated_drop_table_target(&ctx.pool_guards, &engine, table_id).await?;
    lock_manager.reject_table_ddl_explicit_session_lock(table_id, ctx.owner, "drop table")?;
    let mut table_locks = lock_manager
        .acquire_table_ddl_locks(table_id, ctx.owner, ctx.owner_group)
        .await?;
    engine.trx_sys.ensure_runtime_healthy()?;

    let mut trx = session.begin_trx("begin transaction")?;

    if let Err(err) = table.begin_drop_lifecycle().await {
        trx.rollback().await?;
        return Err(err);
    }
    table_locks.fail_waiters_on_release(OperationError::TableDropping);

    let metadata = table.metadata().clone();
    let exec_res = execute_drop_table_catalog_cascade(&engine, &mut trx, table_id, &metadata).await;
    if let Err(err) = exec_res {
        // `trx.exec` may have already discarded the transaction after a fatal
        // statement-rollback failure. In either case the drop gate has been
        // crossed, so preserve the poison outcome below.
        let _ = trx.rollback().await;
        return Err(poison_drop_table_after_gate_with_source(
            &engine,
            table_id,
            "catalog cascade",
            err,
        )
        .into());
    }

    let drop_cts = match trx.commit().await {
        Ok(drop_cts) => drop_cts,
        Err(err) => {
            return Err(
                poison_drop_table_after_gate_with_source(&engine, table_id, "commit", err).into(),
            );
        }
    };

    let replay_floor = engine
        .catalog()
        .effective_user_table_redo_replay_floor(table_id, table.redo_replay_floor_snapshot());
    finish_drop_table_runtime_retention(&engine, table_id, table, drop_cts, replay_floor)?;
    table_locks.fail_waiters_on_release(OperationError::TableNotFound);
    // Foreground DROP TABLE stops at logical removal. The catalog map retains
    // the dropped runtime and replay floor until purge and catalog checkpoint
    // finish the physical cleanup obligations.
    engine.trx_sys.request_dropped_table_purge();
    Ok(())
}

/// Reject table ids outside user-managed catalog space.
#[inline]
pub(crate) fn reject_non_user_table_id(table_id: TableID, operation: &'static str) -> Result<()> {
    if is_user_table(table_id) {
        return Ok(());
    }
    Err(Report::new(OperationError::TableNotFound)
        .attach(format!(
            "{operation} requires user table id: table_id={table_id}"
        ))
        .into())
}

/// Ensure the user-table catalog row exists for a DDL operation.
#[inline]
pub(crate) async fn ensure_user_table_catalog_row(
    guards: &PoolGuards,
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
) -> Result<()> {
    if engine
        .catalog()
        .storage
        .tables()
        .find_uncommitted_by_id(guards, table_id)
        .await?
        .is_some()
    {
        return Ok(());
    }
    Err(Report::new(OperationError::TableNotFound)
        .attach(format!("{operation} catalog lookup: table_id={table_id}"))
        .into())
}

/// Precheck that a user table is a valid index-DDL target.
pub(crate) async fn precheck_index_ddl_target(
    guards: &PoolGuards,
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
) -> Result<()> {
    let _ = validated_index_ddl_target(guards, engine, table_id, operation).await?;
    Ok(())
}

/// Return the validated runtime table for an index-DDL target.
pub(crate) async fn validated_index_ddl_target(
    guards: &PoolGuards,
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
) -> Result<Arc<Table>> {
    reject_non_user_table_id(table_id, operation)?;
    let table = engine
        .catalog()
        .validate_user_table_live(table_id, operation)
        .await?;
    ensure_user_table_catalog_row(guards, engine, table_id, operation).await?;
    Ok(table)
}

/// Reject primary-key flags in public user-table DDL for now.
#[inline]
pub(crate) fn reject_user_table_primary_key_index(
    index_spec: &IndexSpec,
    operation: &'static str,
) -> Result<()> {
    if !index_spec.primary_key() {
        return Ok(());
    }
    Err(invalid_index_spec(format!(
        "{operation} does not support user-table primary keys"
    )))
}

#[inline]
fn reject_user_table_primary_key_indexes(
    index_specs: &[IndexSpec],
    operation: &'static str,
) -> Result<()> {
    for index_spec in index_specs {
        reject_user_table_primary_key_index(index_spec, operation)?;
    }
    Ok(())
}

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

async fn validated_drop_table_target(
    guards: &PoolGuards,
    engine: &EngineRef,
    table_id: TableID,
) -> Result<Arc<Table>> {
    reject_non_user_table_id(table_id, "drop table")?;
    let Some(table) = engine.catalog().get_table(table_id).await else {
        return Err(Report::new(OperationError::TableNotFound)
            .attach(format!("drop table runtime lookup: table_id={table_id}"))
            .into());
    };
    ensure_user_table_catalog_row(guards, engine, table_id, "drop table").await?;
    Ok(table)
}

#[inline]
async fn execute_create_table_catalog_staging(
    engine: &EngineRef,
    trx: &mut Transaction,
    table_id: TableID,
    table_object: TableObject,
    column_objects: Vec<ColumnObject>,
    index_objects: Vec<IndexObject>,
    index_column_objects: Vec<IndexColumnObject>,
) -> Result<()> {
    trx.exec(async |stmt| {
        let inserted = engine
            .catalog()
            .storage
            .tables()
            .insert(stmt, &table_object)
            .await;
        if !inserted {
            return Err(Report::new(OperationError::TableAlreadyExists)
                .attach(format!("create table catalog object: table_id={table_id}"))
                .into());
        }

        for column_object in column_objects {
            let inserted = engine
                .catalog()
                .storage
                .columns()
                .insert(stmt, &column_object)
                .await;
            debug_assert!(inserted);
        }
        for index_object in index_objects {
            let inserted = engine
                .catalog()
                .storage
                .indexes()
                .insert(stmt, &index_object)
                .await;
            debug_assert!(inserted);
        }
        for index_column_object in index_column_objects {
            let inserted = engine
                .catalog()
                .storage
                .index_columns()
                .insert(stmt, &index_column_object)
                .await;
            debug_assert!(inserted);
        }

        let res = stmt
            .effects_mut()
            .set_ddl_redo(DDLRedo::CreateTable(table_id));
        debug_assert!(res.is_none());
        Ok(())
    })
    .await
}

#[inline]
async fn execute_drop_table_catalog_cascade(
    engine: &EngineRef,
    trx: &mut Transaction,
    table_id: TableID,
    metadata: &TableMetadata,
) -> Result<()> {
    trx.exec(async |stmt| {
        let index_columns_deleted = engine
            .catalog()
            .storage
            .index_columns()
            .delete_by_table_id(stmt, table_id)
            .await?;
        let indexes_deleted = engine
            .catalog()
            .storage
            .indexes()
            .delete_by_table_id(stmt, table_id)
            .await?;
        let columns_deleted = engine
            .catalog()
            .storage
            .columns()
            .delete_by_table_id(stmt, table_id)
            .await?;
        let table_deleted = engine
            .catalog()
            .storage
            .tables()
            .delete_by_id(stmt, table_id)
            .await;
        if !table_deleted {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("drop table catalog row: table_id={table_id}"))
                .into());
        }
        engine
            .catalog()
            .storage
            .table_replay_silent_watermarks()
            .delete_by_table_id(stmt, table_id)
            .await?;

        validate_drop_catalog_delete_counts(
            table_id,
            metadata,
            columns_deleted,
            indexes_deleted,
            index_columns_deleted,
        )?;

        let res = stmt
            .effects_mut()
            .set_ddl_redo(DDLRedo::DropTable(table_id));
        debug_assert!(res.is_none());
        Ok(())
    })
    .await
}

#[inline]
fn validate_drop_catalog_delete_counts(
    table_id: TableID,
    metadata: &TableMetadata,
    columns_deleted: usize,
    indexes_deleted: usize,
    index_columns_deleted: usize,
) -> Result<()> {
    let expected_index_columns = metadata
        .idx
        .active_indexes()
        .map(|(_, spec)| spec.cols.len())
        .sum::<usize>();
    if columns_deleted == metadata.col.col_count()
        && indexes_deleted == metadata.idx.active_index_count()
        && index_columns_deleted == expected_index_columns
    {
        return Ok(());
    }
    Err(Report::new(InternalError::Generic)
        .attach(format!(
            "drop table catalog cascade count mismatch: table_id={table_id}, columns_deleted={columns_deleted}, expected_columns={}, indexes_deleted={indexes_deleted}, expected_indexes={}, index_columns_deleted={index_columns_deleted}, expected_index_columns={expected_index_columns}",
            metadata.col.col_count(),
            metadata.idx.active_index_count(),
        ))
        .into())
}

#[inline]
fn finish_drop_table_runtime_retention(
    engine: &EngineRef,
    table_id: TableID,
    table: Arc<Table>,
    drop_cts: TrxID,
    replay_floor: TableRedoReplayFloor,
) -> Result<()> {
    if let Err(_err) = table.mark_dropped_lifecycle() {
        return Err(poison_drop_table_after_gate(engine, table_id, "mark dropped").into());
    }
    if engine
        .catalog()
        .mark_user_table_dropped_runtime(table_id, table, drop_cts, replay_floor)
    {
        return Ok(());
    }
    Err(poison_drop_table_after_gate(engine, table_id, "runtime retention").into())
}

#[inline]
fn poison_create_table_after_root_publish_with_source(
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
    source: Error,
) -> Report<FatalError> {
    let poison = engine.trx_sys.poison_storage(FatalError::Poisoned);
    source
        .into_report()
        .change_context(*poison.current_context())
        .attach(format!(
            "create table failed after table-root publish: table_id={table_id}, operation={operation}"
        ))
}

#[inline]
fn poison_create_table_rollback_with_source(
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
    source_debug: &str,
    rollback_err: Error,
) -> Report<FatalError> {
    let poison = engine.trx_sys.poison_storage(FatalError::RollbackAccess);
    rollback_err
        .into_report()
        .change_context(*poison.current_context())
        .attach(format!(
            "create table rollback cleanup failed: table_id={table_id}, operation={operation}, source_error={source_debug}"
        ))
}

#[inline]
fn poison_create_table_cleanup_with_source(
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
    cleanup_operation: &'static str,
    source_debug: &str,
    cleanup_err: Error,
) -> Report<FatalError> {
    let poison = engine.trx_sys.poison_storage(FatalError::Poisoned);
    cleanup_err
        .into_report()
        .change_context(*poison.current_context())
        .attach(format!(
            "create table cleanup failed: table_id={table_id}, operation={operation}, cleanup_operation={cleanup_operation}, source_error={source_debug}"
        ))
}

#[inline]
fn poison_drop_table_after_gate(
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
) -> Report<FatalError> {
    // Once `begin_drop_lifecycle` succeeds, the table's checkpoint publish gate
    // is closed and the operation cannot be safely retried as an ordinary DDL
    // failure. Poison admission so future work sees the fatal state; explicit
    // engine shutdown remains responsible for stopping background workers.
    engine
        .trx_sys
        .poison_storage(FatalError::Poisoned)
        .attach(drop_table_after_gate_message(table_id, operation))
}

#[inline]
fn poison_drop_table_after_gate_with_source(
    engine: &EngineRef,
    table_id: TableID,
    operation: &'static str,
    source: Error,
) -> Report<FatalError> {
    let poison = poison_drop_table_after_gate(engine, table_id, operation);
    source
        .into_report()
        .change_context(*poison.current_context())
        .attach(drop_table_after_gate_message(table_id, operation))
}

#[inline]
fn drop_table_after_gate_message(table_id: TableID, operation: &'static str) -> String {
    format!("drop table failed after lifecycle gate: table_id={table_id}, operation={operation}")
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
fn validate_primary_key_contract(
    column_layout: &TableColumnLayout,
    index_specs: &IndexSpecs,
) -> Result<()> {
    let mut primary_key_index_no = None;
    for (index_no, index_spec) in index_specs.active_indexes() {
        if !index_spec.primary_key() {
            continue;
        }
        if let Some(existing_index_no) = primary_key_index_no {
            return Err(invalid_index_spec(format!(
                "multiple primary keys: index_no {existing_index_no} and index_no {index_no}"
            )));
        }
        for key in &index_spec.cols {
            let col_no = usize::from(key.col_no);
            if column_layout.nullable(col_no) {
                return Err(invalid_index_spec(format!(
                    "primary key index_no {index_no} references nullable column {col_no}"
                )));
            }
        }
        primary_key_index_no = Some(index_no);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        CatalogCheckpointScanStopReason, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey,
        IndexSpec, TableSpec,
    };
    use crate::error::{
        CompletionErrorKind, ConfigError, Error, ErrorKind, FatalError, InternalError,
        OperationError,
    };
    use crate::id::{SessionID, TableID, TrxID};
    use crate::io::install_storage_backend_test_hook;
    use crate::lock::tests::{LockDebugEntryState, try_acquire};
    use crate::lock::{LockMode, LockOwner, LockOwnerGroup, LockResource};
    use crate::log::redo::DDLRedo;
    use crate::session::tests::SessionTestExt;
    use crate::table::TableLifecycleState;
    use crate::table::tests::*;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::tests as trx_tests;
    use crate::value::{Val, ValKind};
    use std::cell::Cell;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum CreateTableTestFailure {
        AfterCatalogStaged,
        AfterFilePublished,
        AfterRuntimeBuilt,
        PoisonBeforeCatalogCommit,
    }

    thread_local! {
        static CREATE_TABLE_FAILURE: Cell<Option<CreateTableTestFailure>> = const { Cell::new(None) };
    }

    fn set_create_table_failure(failure: Option<CreateTableTestFailure>) {
        CREATE_TABLE_FAILURE.with(|slot| slot.set(failure));
    }

    pub(super) fn maybe_fail_create_table(failure: CreateTableTestFailure) -> Result<()> {
        if CREATE_TABLE_FAILURE.with(|slot| slot.get()) == Some(failure) {
            return Err(Report::new(InternalError::InjectedTestFailure).into());
        }
        Ok(())
    }

    pub(super) fn maybe_poison_before_create_table_catalog_commit(engine: &EngineRef) {
        if CREATE_TABLE_FAILURE.with(|slot| slot.get())
            == Some(CreateTableTestFailure::PoisonBeforeCatalogCommit)
        {
            let _ = engine.trx_sys.poison_storage(FatalError::Poisoned);
        }
    }

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
        let primary_key = metadata
            .primary_key()
            .expect("metadata should expose primary key index");
        assert_eq!(primary_key.index_no(), 0);
        assert_eq!(primary_key.spec().cols, vec![IndexKey::new(0)]);
    }

    #[test]
    fn test_primary_key_spec_validates_select_key() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
            ],
        )
        .expect("valid table metadata");
        let primary_key = metadata.primary_key().unwrap();

        assert!(primary_key.matches_key(&SelectKey::new(0, vec![Val::from(42u32)])));
        assert_eq!(
            primary_key.validate_key(&SelectKey::new(1, vec![Val::from(42u32)])),
            Err(PrimaryKeyMatchError::IndexNo {
                actual: 1,
                expected: 0
            })
        );
        assert_eq!(
            primary_key.validate_key(&SelectKey::new(0, vec![Val::from(42u32), Val::from(99u64)])),
            Err(PrimaryKeyMatchError::ValueCount {
                actual: 2,
                expected: 1
            })
        );
        assert_eq!(
            primary_key.validate_key(&SelectKey::new(0, vec![Val::from(42u64)])),
            Err(PrimaryKeyMatchError::Type { index_no: 0 })
        );
        assert_eq!(
            primary_key.validate_key(&SelectKey::new(0, vec![Val::Null])),
            Err(PrimaryKeyMatchError::Type { index_no: 0 })
        );
        assert!(
            metadata
                .idx
                .index_type_match(metadata.col.as_ref(), 1, &[Val::Null])
        );
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
    fn test_table_metadata_rejects_deserialized_empty_columns() {
        let brief = TableBriefMetadata {
            col_names: vec![],
            col_types: vec![],
            col_attrs: vec![],
            next_index_no: 0,
            index_specs: vec![],
        };

        let err = TableMetadata::try_from(brief).unwrap_err();
        let report = format!("{err:?}");
        assert!(
            report.contains("table column layout requires columns"),
            "{report}"
        );
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
    fn test_table_metadata_rejects_multiple_primary_keys() {
        let columns = vec![
            ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
            ColumnSpec::new("c1", ValKind::U32, ColumnAttributes::empty()),
        ];

        let err = TableMetadata::try_new(
            columns,
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::PK),
            ],
        )
        .unwrap_err();

        assert_invalid_index_spec(err, "multiple primary keys");
    }

    #[test]
    fn test_table_metadata_rejects_sparse_multiple_primary_keys() {
        let columns = vec![
            ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
            ColumnSpec::new("c1", ValKind::U32, ColumnAttributes::empty()),
        ];

        let err = TableMetadata::try_new_with_next_index_no(
            columns,
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    2,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::PK),
                ),
            ],
            3,
        )
        .unwrap_err();

        assert_invalid_index_spec(err, "multiple primary keys");
    }

    #[test]
    fn test_table_metadata_rejects_nullable_primary_key_column() {
        let err = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::NULLABLE,
            )],
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .unwrap_err();

        assert_invalid_index_spec(err, "primary key index_no 0 references nullable column 0");
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
        assert_eq!(
            dropped.idx.index_cols,
            [0].into_iter().collect::<FastHashSet<_>>()
        );
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

    #[test]
    fn test_statement_read_takes_metadata_lock_only() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;

            let stmt_owner = Cell::new(None);
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let owner = stmt_tests::lock_owner(stmt);
                stmt_owner.set(Some(owner));
                let selected = stmt
                    .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                    .await?;
                assert!(selected.is_found());
                let repeated = stmt
                    .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                    .await?;
                assert!(repeated.is_found());
                assert_eq!(lock_entry_count(&engine, owner), 1);
                assert!(has_lock_entry(
                    &engine,
                    owner,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                ));
                assert!(!has_lock_resource(
                    &engine,
                    owner,
                    LockResource::TableData(table_id),
                ));
                Ok(())
            })
            .await
            .unwrap();

            let owner = stmt_owner.get().unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_statement_write_locks_are_transaction_owned_and_cached() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = trx_tests::lock_owner(&trx).unwrap();

            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(10i32), Val::from("a")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Granted,
            ));
            assert_eq!(lock_entry_count(&engine, owner), 2);

            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(11i32), Val::from("b")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 2);

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_create_table_waits_on_catalog_namespace_lock() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let _ = create_table2_for_test(&engine).await;
            let blocker = LockOwner::Session(SessionID::new(91_400));
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    LockResource::CatalogNamespace,
                    LockMode::Exclusive,
                    blocker,
                )
                .unwrap()
            );

            let mut session = engine.new_session().unwrap();
            let waiting_owner = LockOwner::Session(session.id());
            let create_task = smol::spawn(async move {
                session
                    .create_table(
                        TableSpec::new(vec![ColumnSpec::new(
                            "id",
                            crate::value::ValKind::I32,
                            ColumnAttributes::empty(),
                        )]),
                        vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
                    )
                    .await
            });

            wait_for_lock_entry(
                &engine,
                waiting_owner,
                LockResource::CatalogNamespace,
                LockMode::Exclusive,
                LockDebugEntryState::Waiting,
            )
            .await;

            assert_eq!(engine.lock_manager().release_owner(blocker), 1);
            let table_id = create_task.await.unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_some());
            assert!(!has_lock_resource(
                &engine,
                waiting_owner,
                LockResource::CatalogNamespace,
            ));
        });
    }

    #[test]
    fn test_create_table_rejects_invalid_metadata_before_file_creation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_invalid_metadata")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            let err = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        ValKind::I32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![], IndexAttributes::UK)],
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::InvalidIndexSpec)
            );
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_rejects_primary_key_before_file_creation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_pk_rejected")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            let err = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        ValKind::I32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap_err();

            assert_invalid_index_spec(err, "create table does not support user-table primary keys");
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_catalog_staging_failure_rolls_back_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_fail_catalog")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(Some(CreateTableTestFailure::AfterCatalogStaged));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_file_publish_failure_rolls_back_catalog_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_fail_publish")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let hook = Arc::new(FailingFirstWriteHook::new(table_file_path.clone()));
            let _install = install_storage_backend_test_hook(hook.clone());
            let (table_spec, index_specs) = drop_table_test_spec();

            let err = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap_err();

            assert!(err.is_kind(ErrorKind::Io), "{err:?}");
            assert!(hook.call_count() > 0);
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_after_file_published_failure_rolls_back_catalog_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_fail_after_file")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(Some(CreateTableTestFailure::AfterFilePublished));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_runtime_failure_after_file_publish_rolls_back_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_fail_runtime")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(Some(CreateTableTestFailure::AfterRuntimeBuilt));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_catalog_commit_error_after_file_publish_poisons_and_keeps_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "create_fail_commit")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(Some(CreateTableTestFailure::PoisonBeforeCatalogCommit));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::Poisoned)
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::Poisoned)
            );
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!session.in_trx().unwrap());
            assert!(Path::new(&table_file_path).exists());
        });
    }

    #[test]
    fn test_explicit_table_locks_reject_intent_modes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            for mode in [LockMode::IntentShared, LockMode::IntentExclusive] {
                let err = session.lock_table(table_id, mode).await.unwrap_err();
                assert_eq!(err.operation_error(), Some(OperationError::InvalidLockMode));
            }

            let mut trx = session.begin_trx().unwrap();
            for mode in [LockMode::IntentShared, LockMode::IntentExclusive] {
                let err = trx.lock_table(table_id, mode).await.unwrap_err();
                assert_eq!(err.operation_error(), Some(OperationError::InvalidLockMode));
            }
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_transaction_shared_table_lock_blocks_external_row_writer() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = trx_tests::lock_owner(&trx).unwrap();

            trx.lock_table(table_id, LockMode::Shared).await.unwrap();
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableData(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));

            let engine_ref = engine.new_ref().unwrap();
            let (owner_tx, owner_rx) = flume::bounded(1);
            let writer = smol::spawn(async move {
                let mut writer_session = engine_ref.new_session().unwrap();
                let mut writer_trx = writer_session.begin_trx().unwrap();
                owner_tx
                    .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                    .await
                    .unwrap();
                trx_insert_row_by_id(
                    &mut writer_trx,
                    table_id,
                    vec![Val::from(31_001i32), Val::from("blocked")],
                )
                .await?;
                writer_trx.commit().await?;
                Ok::<(), Error>(())
            });
            let writer_owner = owner_rx.recv_async().await.unwrap();
            wait_for_lock_entry(
                &engine,
                writer_owner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Waiting,
            )
            .await;

            trx.rollback().await.unwrap();
            writer.await.unwrap();
        });
    }

    #[test]
    fn test_transaction_exclusive_table_lock_uses_cache_and_releases_on_commit() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = trx_tests::lock_owner(&trx).unwrap();

            trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();
            trx.lock_table(table_id, LockMode::Shared).await.unwrap();
            trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();

            assert_eq!(lock_entry_count(&engine, owner), 2);
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_session_shared_table_lock_allows_reads_but_rejects_same_session_writes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 0, 1, "name").await;

            let mut session = engine.new_session().unwrap();
            session
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap();

            let mut read_trx = session.begin_trx().unwrap();
            read_trx
                .exec(async |stmt| {
                    let selected = stmt
                        .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                        .await?;
                    assert!(selected.is_found());
                    Ok(())
                })
                .await
                .unwrap();
            read_trx.commit().await.unwrap();

            let mut write_trx = session.begin_trx().unwrap();
            let err = trx_insert_row_by_id(
                &mut write_trx,
                table_id,
                vec![Val::from(31_101i32), Val::from("same-session-s")],
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockOwnerGroupConflict)
            );
            assert!(!has_lock_entry(
                &engine,
                trx_tests::lock_owner(&write_trx).unwrap(),
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Waiting,
            ));
            write_trx.rollback().await.unwrap();

            session.unlock_table(table_id).unwrap();
        });
    }

    #[test]
    fn test_session_table_lock_failure_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_owner = LockOwner::Session(session.id());
            let mut trx = session.begin_trx().unwrap();

            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(31_301i32), Val::from("same-session-ix")],
            )
            .await
            .unwrap();

            let err = session
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockOwnerGroupConflict)
            );
            assert!(!has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableData(table_id),
            ));

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_table_lock_cancellation_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let blocker = LockOwner::Transaction(TrxID::new(91_301));
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    LockResource::TableData(table_id),
                    LockMode::Exclusive,
                    blocker,
                )
                .unwrap()
            );

            let session = engine.new_session().unwrap();
            let session_owner = LockOwner::Session(session.id());
            let mut lock_fut = Box::pin(session.lock_table(table_id, LockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                session_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                session_owner,
                LockResource::TableData(table_id),
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(lock_fut);
            wait_for_no_lock_resource(
                &engine,
                session_owner,
                LockResource::TableMetadata(table_id),
            )
            .await;
            wait_for_no_lock_resource(&engine, session_owner, LockResource::TableData(table_id))
                .await;
            assert_eq!(
                engine
                    .lock_manager()
                    .release(LockResource::TableData(table_id), blocker),
                1
            );
        });
    }

    #[test]
    fn test_transaction_table_lock_failure_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_owner = LockOwner::Session(session.id());
            session
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();

            let err = trx
                .lock_table(table_id, LockMode::Exclusive)
                .await
                .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockOwnerGroupConflict)
            );
            assert!(!has_lock_resource(
                &engine,
                trx_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(
                !trx_tests::cached_transaction_lock_covers(
                    &trx,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared
                )
                .unwrap()
            );
            assert!(has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableMetadata(table_id),
            ));

            trx.rollback().await.unwrap();
            session.unlock_table(table_id).unwrap();
        });
    }

    #[test]
    fn test_transaction_table_lock_cancellation_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let blocker = LockOwner::Transaction(TrxID::new(91_302));
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    LockResource::TableData(table_id),
                    LockMode::Exclusive,
                    blocker,
                )
                .unwrap()
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();
            let mut lock_fut = Box::pin(trx.lock_table(table_id, LockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                trx_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                trx_owner,
                LockResource::TableData(table_id),
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(lock_fut);
            wait_for_no_lock_resource(&engine, trx_owner, LockResource::TableMetadata(table_id))
                .await;
            wait_for_no_lock_resource(&engine, trx_owner, LockResource::TableData(table_id)).await;
            assert!(
                !trx_tests::cached_transaction_lock_covers(
                    &trx,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared
                )
                .unwrap()
            );
            assert_eq!(
                engine
                    .lock_manager()
                    .release(LockResource::TableData(table_id), blocker),
                1
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_exclusive_table_lock_covers_same_session_writer() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_owner = LockOwner::Session(session.id());
            session
                .lock_table(table_id, LockMode::Exclusive)
                .await
                .unwrap();

            let engine_ref = engine.new_ref().unwrap();
            let (owner_tx, owner_rx) = flume::bounded(1);
            let external_writer = smol::spawn(async move {
                let mut writer_session = engine_ref.new_session().unwrap();
                let mut writer_trx = writer_session.begin_trx().unwrap();
                owner_tx
                    .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                    .await
                    .unwrap();
                trx_insert_row_by_id(
                    &mut writer_trx,
                    table_id,
                    vec![Val::from(31_201i32), Val::from("external")],
                )
                .await?;
                writer_trx.commit().await?;
                Ok::<(), Error>(())
            });
            let external_owner = owner_rx.recv_async().await.unwrap();
            wait_for_lock_entry(
                &engine,
                external_owner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Waiting,
            )
            .await;

            let mut same_session_trx = session.begin_trx().unwrap();
            let same_session_owner = trx_tests::lock_owner(&same_session_trx).unwrap();
            trx_insert_row_by_id(
                &mut same_session_trx,
                table_id,
                vec![Val::from(31_202i32), Val::from("covered")],
            )
            .await
            .unwrap();
            assert!(has_lock_entry(
                &engine,
                same_session_owner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Granted,
            ));

            let err = session.unlock_table(table_id).unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));

            same_session_trx.commit().await.unwrap();
            assert!(has_lock_entry(
                &engine,
                session_owner,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                external_owner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
                LockDebugEntryState::Waiting,
            ));

            session.unlock_table(table_id).unwrap();
            assert!(!has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableData(table_id),
            ));
            external_writer.await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_rejects_already_dropping_lifecycle_without_poison() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            table_for_internal_assertion(&engine, table_id)
                .begin_drop_lifecycle()
                .await
                .unwrap();

            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .lifecycle
                    .state(),
                TableLifecycleState::Dropping
            );
            assert!(!session.in_trx().unwrap());
            assert!(engine.inner().trx_sys.storage_poison_error().is_none());
        });
    }

    #[test]
    fn test_drop_table_rejects_active_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_returns_not_found_for_missing_table() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let err = session.drop_table(TableID::new(0)).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));

            let missing_user_table_id = table_id + 1000;
            let err = session.drop_table(missing_user_table_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        });
    }

    #[test]
    fn test_drop_table_rejects_runtime_missing_catalog_row_before_gate() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut corrupt_session = engine.new_session().unwrap();
            let mut corrupt_trx = corrupt_session.begin_trx().unwrap();

            corrupt_trx
                .exec(async |stmt| {
                    let deleted = engine
                        .catalog()
                        .storage
                        .tables()
                        .delete_by_id(stmt, table_id)
                        .await;
                    assert!(deleted);
                    let old = stmt
                        .effects_mut()
                        .set_ddl_redo(DDLRedo::DropTable(table_id));
                    debug_assert!(old.is_none());
                    Ok(())
                })
                .await
                .unwrap();
            corrupt_trx.commit().await.unwrap();

            let mut drop_session = engine.new_session().unwrap();
            let err = drop_session.drop_table(table_id).await.unwrap_err();

            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .lifecycle
                    .state(),
                TableLifecycleState::Live
            );
            assert!(!drop_session.in_trx().unwrap());
            assert!(engine.inner().trx_sys.storage_poison_error().is_none());
            assert!(engine.catalog().get_table(table_id).await.is_some());
        });
    }

    #[test]
    fn test_drop_table_rejects_same_session_explicit_table_lock() {
        smol::block_on(async {
            for mode in [LockMode::Shared, LockMode::Exclusive] {
                let temp_dir = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
                let table_id = create_table2_for_test(&engine).await;
                let mut session = engine.new_session().unwrap();
                let owner = LockOwner::Session(session.id());

                session.lock_table(table_id, mode).await.unwrap();
                let err = session.drop_table(table_id).await.unwrap_err();
                assert_eq!(
                    err.operation_error(),
                    Some(OperationError::LockOwnerGroupConflict)
                );

                assert_eq!(
                    table_for_internal_assertion(&engine, table_id)
                        .lifecycle
                        .state(),
                    TableLifecycleState::Live
                );
                assert!(engine.catalog().get_table(table_id).await.is_some());
                assert!(!has_lock_resource(
                    &engine,
                    owner,
                    LockResource::CatalogNamespace,
                ));
                assert!(has_lock_entry(
                    &engine,
                    owner,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                ));
                assert!(has_lock_entry(
                    &engine,
                    owner,
                    LockResource::TableData(table_id),
                    mode,
                    LockDebugEntryState::Granted,
                ));

                session.unlock_table(table_id).unwrap();
                assert!(!has_lock_resource(
                    &engine,
                    owner,
                    LockResource::TableMetadata(table_id),
                ));
                assert!(!has_lock_resource(
                    &engine,
                    owner,
                    LockResource::TableData(table_id),
                ));
                session.drop_table(table_id).await.unwrap();
            }
        });
    }

    #[test]
    fn test_drop_table_fails_waiting_session_table_lock() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
            let publish_lease = table.try_begin_checkpoint_publish().unwrap();
            let mut drop_session = engine.new_session().unwrap();
            let drop_owner = LockOwner::Session(drop_session.id());
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            let lock_session = engine.new_session().unwrap();
            let lock_owner = LockOwner::Session(lock_session.id());
            let err = lock_session
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
            ));

            drop(publish_lease);
            drop_fut.await.unwrap();
            drop(root_lease);
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableData(table_id),
            ));
        });
    }

    #[test]
    fn test_drop_table_fails_waiting_transaction_table_lock() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
            let publish_lease = table.try_begin_checkpoint_publish().unwrap();
            let mut drop_session = engine.new_session().unwrap();
            let drop_owner = LockOwner::Session(drop_session.id());
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            let mut lock_session = engine.new_session().unwrap();
            let mut trx = lock_session.begin_trx().unwrap();
            let lock_owner = trx_tests::lock_owner(&trx).unwrap();
            let err = trx
                .lock_table(table_id, LockMode::Exclusive)
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
            ));

            drop(publish_lease);
            drop_fut.await.unwrap();
            drop(root_lease);
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                lock_owner,
                LockResource::TableData(table_id),
            ));

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_explicit_table_lock_after_drop_returns_not_found_without_locks() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut drop_session = engine.new_session().unwrap();
            drop_session.drop_table(table_id).await.unwrap();

            let lock_session = engine.new_session().unwrap();
            let session_owner = LockOwner::Session(lock_session.id());
            let err = lock_session
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            assert!(!has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                session_owner,
                LockResource::TableData(table_id),
            ));

            let mut trx_session = engine.new_session().unwrap();
            let mut trx = trx_session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();
            let err = trx
                .lock_table(table_id, LockMode::Exclusive)
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            assert!(!has_lock_resource(
                &engine,
                trx_owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                trx_owner,
                LockResource::TableData(table_id),
            ));
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_logical_cascade() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let mut session = engine.new_session().unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("drop-me")],
            )
            .await;
            let (other_spec, other_indexes) = drop_table_test_spec();
            let other_table_id = session
                .create_table(other_spec, other_indexes)
                .await
                .unwrap();
            let owner = LockOwner::Session(session.id());

            assert!(Path::new(&table_file_path).exists());
            session.drop_table(table_id).await.unwrap();

            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::CatalogNamespace,
            ));
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::TableData(table_id),
            ));
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .index_columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(&session.pool_guards(), other_table_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                !engine
                    .catalog()
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(&session.pool_guards(), other_table_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(Path::new(&table_file_path).exists());

            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));

            let mut stale_read = session.begin_trx().unwrap();
            let err = trx_select_row_mvcc_by_id(&mut stale_read, table_id, &single_key(1), &[0, 1])
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            assert_eq!(stale_read.commit().await.unwrap(), TrxID::new(0));

            let mut stale_write = session.begin_trx().unwrap();
            let err = trx_insert_row_by_id(
                &mut stale_write,
                table_id,
                vec![Val::from(2), Val::from("blocked")],
            )
            .await
            .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            assert_eq!(stale_write.commit().await.unwrap(), TrxID::new(0));

            let (later_spec, later_indexes) = drop_table_test_spec();
            let later_table_id = session
                .create_table(later_spec, later_indexes)
                .await
                .unwrap();
            assert!(later_table_id > table_id);
            assert!(later_table_id > other_table_id);
        });
    }

    #[test]
    fn test_drop_table_gc_deletes_file_after_catalog_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir, "drop_gc_destroy")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session.create_table(table_spec, index_specs).await.unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(11), Val::from("gc-delete")],
            )
            .await;
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            session.drop_table(table_id).await.unwrap();
            assert_eq!(
                engine.catalog().retained_dropped_table_ids_now(),
                vec![table_id]
            );
            engine.inner().trx_sys.request_dropped_table_purge();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            wait_path_exists(&table_file_path, false).await;
            assert!(engine.catalog().retained_dropped_table_ids_now().is_empty());

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_lookup_unique_mvcc(table_id, &single_key(11), &[0, 1])
                        .await
                })
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_catalog_cascade_poison_preserves_source_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut corrupt_session = engine.new_session().unwrap();
            let mut corrupt_trx = corrupt_session.begin_trx().unwrap();

            corrupt_trx
                .exec(async |stmt| {
                    let deleted = engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_index(stmt, table_id, 0)
                        .await
                        .unwrap();
                    assert_eq!(deleted, 1);
                    let old = stmt.effects_mut().set_ddl_redo(DDLRedo::DropIndex {
                        table_id,
                        index_no: 0,
                    });
                    debug_assert!(old.is_none());
                    Ok(())
                })
                .await
                .unwrap();
            corrupt_trx.commit().await.unwrap();

            let mut drop_session = engine.new_session().unwrap();
            let err = drop_session.drop_table(table_id).await.unwrap_err();
            let report = format!("{err:?}");

            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::Poisoned),
                "{report}"
            );
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::Generic),
                "{report}"
            );
            assert!(
                report.contains("drop table failed after lifecycle gate: table_id="),
                "{report}"
            );
            assert!(report.contains("operation=catalog cascade"), "{report}");
            assert!(
                report.contains("drop table catalog cascade count mismatch"),
                "{report}"
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::Poisoned)
            );
            assert!(!drop_session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_drop_table_catalog_cascade_failure_fails_queued_locks_as_dropping() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut corrupt_session = engine.new_session().unwrap();
            let mut corrupt_trx = corrupt_session.begin_trx().unwrap();

            corrupt_trx
                .exec(async |stmt| {
                    let deleted = engine
                        .catalog()
                        .storage
                        .index_columns()
                        .delete_by_index(stmt, table_id, 0)
                        .await
                        .unwrap();
                    assert_eq!(deleted, 1);
                    let old = stmt.effects_mut().set_ddl_redo(DDLRedo::DropIndex {
                        table_id,
                        index_no: 0,
                    });
                    debug_assert!(old.is_none());
                    Ok(())
                })
                .await
                .unwrap();
            corrupt_trx.commit().await.unwrap();

            let table = table_for_internal_assertion(&engine, table_id);
            let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
            let publish_lease = table.try_begin_checkpoint_publish().unwrap();
            let mut drop_session = engine.new_session().unwrap();
            let drop_owner = LockOwner::Session(drop_session.id());
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            let waiting_session_id = SessionID::new(91_501);
            let waiting_owner = LockOwner::Session(waiting_session_id);
            let waiting_group = LockOwnerGroup::Session(waiting_session_id);
            let mut lock_fut = Box::pin(engine.lock_manager().acquire_grouped_table_locks(
                table_id,
                LockMode::Shared,
                waiting_owner,
                waiting_group,
            ));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            wait_for_lock_entry(
                &engine,
                waiting_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            )
            .await;

            drop(publish_lease);
            let err = drop_fut.await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::Poisoned)
            );
            let Err(waiter_err) = lock_fut.await else {
                panic!("queued table lock waiter should fail after drop gate error");
            };
            assert_eq!(
                waiter_err.operation_error(),
                Some(OperationError::TableDropping)
            );
            drop(root_lease);
        });
    }

    #[test]
    fn test_drop_table_commit_poison_preserves_source_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let redo_file_path = temp_dir.path().join("redo_testsys_lightweight.00000000");
            let hook = Arc::new(FailingFirstWriteHook::new(redo_file_path));
            let _install = install_storage_backend_test_hook(hook.clone());
            let mut session = engine.new_session().unwrap();

            let err = session.drop_table(table_id).await.unwrap_err();
            let report = format!("{err:?}");

            assert!(hook.call_count() > 0);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite),
                "{report}"
            );
            assert_eq!(
                err.completion_error(),
                Some(CompletionErrorKind::Fatal(FatalError::RedoWrite)),
                "{report}"
            );
            assert!(
                report.contains("drop table failed after lifecycle gate: table_id="),
                "{report}"
            );
            assert!(report.contains("operation=commit"), "{report}");
            assert!(report.contains("wait for redo group commit"), "{report}");
            assert!(report.contains("propagate from other threads"), "{report}");
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_user_insert_commit_poison_rolls_back_session_before_return() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("seed")],
            )
            .await;
            let redo_file_path = temp_dir.path().join("redo_testsys_lightweight.00000000");
            let hook = Arc::new(FailingFirstWriteHook::new(redo_file_path));
            let _install = install_storage_backend_test_hook(hook.clone());

            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_insert(table_id, trx, vec![Val::from(169), Val::from("redo-fail")])
                .await;
            let err = trx.commit().await.unwrap_err();
            let report = format!("{err:?}");

            assert!(hook.call_count() > 0);
            assert!(report.contains("redo write failed"), "{report}");
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
            assert_eq!(
                engine.inner().session_registry.active_transaction_count(),
                0
            );
        });
    }

    #[test]
    fn test_drop_table_waits_for_active_metadata_reader() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut reader_session = engine.new_session().unwrap();
            let mut reader_trx = reader_session.begin_trx().unwrap();
            let (held_tx, held_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let mut reader_fut = Box::pin(reader_trx.exec(async |stmt| {
                stmt_tests::acquire_statement_lock(
                    stmt,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared,
                )
                .await?;
                held_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
                Ok(())
            }));

            loop {
                if held_rx.try_recv().is_ok() {
                    break;
                }
                assert!(matches!(
                    futures::poll!(reader_fut.as_mut()),
                    std::task::Poll::Pending
                ));
            }

            let mut drop_session = engine.new_session().unwrap();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));

            release_tx.send_async(()).await.unwrap();
            reader_fut.await.unwrap();
            assert_eq!(reader_trx.commit().await.unwrap(), TrxID::new(0));
            drop_fut.await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_waits_for_active_table_writer() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut writer_session = engine.new_session().unwrap();
            let mut writer_trx = writer_session.begin_trx().unwrap();
            trx_insert_row_by_id(
                &mut writer_trx,
                table_id,
                vec![Val::from(91), Val::from("writer")],
            )
            .await
            .unwrap();

            let mut drop_session = engine.new_session().unwrap();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));

            assert!(writer_trx.commit().await.unwrap() > TrxID::new(0));
            drop_fut.await.unwrap();
        });
    }

    #[test]
    fn test_catalog_checkpoint_scan_allows_runtime_removed_drop_table() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            session.drop_table(table_id).await.unwrap();
            let batch = engine
                .catalog()
                .scan_checkpoint_batch(&engine.inner().trx_sys)
                .await
                .unwrap();

            assert_eq!(
                batch.stop_reason,
                CatalogCheckpointScanStopReason::ReachedDurableUpper
            );
            assert_eq!(batch.catalog_ddl_txn_count, 2);
            assert!(batch.safe_cts >= batch.replay_start_ts);
        });
    }

    #[test]
    fn test_drop_table_catalog_checkpoint_cleans_absent_leftover_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir.clone(), "drop_recover_absence")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session.create_table(table_spec, index_specs).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(7), Val::from("checkpoint-covered")],
            )
            .await;
            let Ok(_) = insert else {
                panic!("insert should succeed: {insert:?}");
            };
            trx.commit().await.unwrap();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            session.drop_table(table_id).await.unwrap();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            assert!(
                engine
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
                    .unwrap()
                    .catalog_replay_start_ts
                    > TrxID::new(1)
            );
            wait_path_exists(&table_file_path, false).await;

            drop(session);
            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, "drop_recover_absence")
                .build()
                .await
                .unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert!(!Path::new(&table_file_path).exists());
        });
    }
}
