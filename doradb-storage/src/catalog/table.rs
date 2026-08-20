use crate::buffer::PoolGuards;
use crate::catalog::spec::{ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexNo, IndexSpec};
use crate::catalog::{Catalog, catalog_table_id_from_slot, is_user_table};
use crate::component::EnginePools;
use crate::engine::EngineCore;
use crate::error::{
    CompletionErrorBridge, CompletionResult, FatalError, FatalResult, InternalError,
    InternalResult, IoResult, OperationError, OperationOrRuntimeResult, OperationResult,
    RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::fs::FileSystem;
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::id::{TableID, TrxID};
use crate::index::BlockIndex;
use crate::map::FastHashSet;
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::runtime::mandatory::{AcceptedExecution, MandatoryTaskMetadata, PreparedExecution};
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use crate::session::{AcceptedDdlScope, PreparedDdlScope};
use crate::table::{Table, TableRedoReplayFloor};
use crate::trx::PrivateTransaction;
use crate::trx::sys::TransactionSystem;
use crate::value::{Val, ValKind, ValType};
use error_stack::{Report, ResultExt};
use semistr::SemiStr;
use std::any::Any;
use std::mem;
use std::ops::Index;
use std::result::Result as StdResult;
use std::sync::Arc;
#[cfg(test)]
use tests::{CreateTableTestFailure, TableDdlTestPhase};

const CREATE_TABLE_CATALOG_WRITE_TARGETS: [TableID; 4] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(1),
    catalog_table_id_from_slot(2),
    catalog_table_id_from_slot(3),
];
const DROP_TABLE_CATALOG_WRITE_TARGETS: [TableID; 5] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(1),
    catalog_table_id_from_slot(2),
    catalog_table_id_from_slot(3),
    catalog_table_id_from_slot(4),
];

/// Purely validated public CREATE TABLE input.
pub(crate) struct ValidatedCreateTable {
    metadata: Arc<TableMetadata>,
}

impl ValidatedCreateTable {
    /// Validate public metadata before reserving a session operation or table id.
    #[inline]
    pub(crate) fn try_new(
        table_spec: super::TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> OperationResult<Self> {
        reject_user_table_primary_key_indexes(&index_specs, "create_table")?;
        let metadata = Arc::new(TableMetadata::try_new(
            table_spec.columns.clone(),
            index_specs,
        )?);
        Ok(Self { metadata })
    }

    /// Bind validated metadata to one gap-tolerant allocated table id.
    #[inline]
    pub(crate) fn into_plan(self, table_id: TableID) -> CreateTablePlan {
        CreateTablePlan {
            table_id,
            metadata: self.metadata,
        }
    }
}

/// Owned CREATE TABLE execution plan transferred across mandatory acceptance.
pub(crate) struct CreateTablePlan {
    table_id: TableID,
    metadata: Arc<TableMetadata>,
}

/// Owned DROP TABLE target selected under complete target exclusion.
pub(crate) struct DropTablePlan {
    table_id: TableID,
    table: Option<Arc<Table>>,
}

impl DropTablePlan {
    /// Retain the exact current-live runtime selected during preparation.
    #[inline]
    pub(crate) fn new(table_id: TableID, table: Arc<Table>) -> Self {
        Self {
            table_id,
            table: Some(table),
        }
    }

    #[inline]
    fn take_table(&mut self) -> Arc<Table> {
        self.table.take().unwrap_or_else(|| {
            panic!(
                "drop-table plan runtime moves exactly once: table_id={}",
                self.table_id
            )
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateTablePhase {
    Prepared,
    FileCreated,
    PrivateTransactionActive,
    CatalogStaged,
    FilePublished,
    RuntimeBuilt,
    CatalogCommitted,
    Installed,
    Aborted,
}

enum CreateTableFile {
    Mutable(Box<MutableTableFile>),
    Published(Arc<TableFile>),
}

struct CreateTableProgress {
    plan: CreateTablePlan,
    table_id: TableID,
    phase: CreateTablePhase,
    file: Option<CreateTableFile>,
    trx: Option<PrivateTransaction>,
    staged_table: Option<Arc<Table>>,
}

impl CreateTableProgress {
    #[inline]
    fn new(plan: CreateTablePlan) -> Self {
        let table_id = plan.table_id;
        Self {
            plan,
            table_id,
            phase: CreateTablePhase::Prepared,
            file: None,
            trx: None,
            staged_table: None,
        }
    }

    #[inline]
    fn metadata(&self) -> &Arc<TableMetadata> {
        &self.plan.metadata
    }

    #[inline]
    fn set_provisional_file(&mut self, mutable_file: MutableTableFile) {
        assert_eq!(self.phase, CreateTablePhase::Prepared);
        self.file = Some(CreateTableFile::Mutable(Box::new(mutable_file)));
        self.phase = CreateTablePhase::FileCreated;
    }

    #[inline]
    fn set_catalog_transaction(&mut self, trx: PrivateTransaction) {
        assert_eq!(self.phase, CreateTablePhase::FileCreated);
        assert!(self.trx.is_none());
        self.trx = Some(trx);
        self.phase = CreateTablePhase::PrivateTransactionActive;
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }

    #[inline]
    fn mark_catalog_staged(&mut self) {
        assert_eq!(self.phase, CreateTablePhase::PrivateTransactionActive);
        self.phase = CreateTablePhase::CatalogStaged;
    }

    #[inline]
    async fn publish_file(&mut self, trx_sys: &TransactionSystem) -> RuntimeResult<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::CatalogStaged);
        let root_ts = self
            .trx
            .as_ref()
            .expect("catalog transaction is staged before file publish")
            .sts();
        let file = self
            .file
            .take()
            .expect("mutable create-table file is present before publish");
        let CreateTableFile::Mutable(mutable_file) = file else {
            panic!("create-table file is mutable before publish");
        };
        let table_file = trx_sys
            .publish_table_file_root(*mutable_file, root_ts, true)
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=create_table, phase=publish_file, table_id={}",
                    self.table_id
                )
            })?;
        self.file = Some(CreateTableFile::Published(table_file));
        self.phase = CreateTablePhase::FilePublished;
        Ok(())
    }

    #[inline]
    async fn build_runtime(
        &mut self,
        pools: &EnginePools,
        guards: &PoolGuards,
    ) -> RuntimeResult<()> {
        debug_assert_eq!(self.phase, CreateTablePhase::FilePublished);
        let Some(CreateTableFile::Published(table_file)) = self.file.as_ref() else {
            panic!("published table file is present before runtime build");
        };
        let table_file = Arc::clone(table_file);
        let active_root = table_file.active_root_unchecked();
        let blk_idx = BlockIndex::new(
            pools.meta.clone(),
            guards.meta_guard(),
            active_root.pivot_row_id,
            active_root.column_block_index_root,
        )
        .await
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| {
            format!(
                "operation=create_table, phase=build_block_index, table_id={}",
                self.table_id
            )
        })?;
        let table = Arc::new(
            Table::new(
                pools.mem.clone(),
                pools.index.clone(),
                guards.index_guard(),
                self.table_id,
                blk_idx,
                table_file,
                pools.disk.clone(),
            )
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=create_table, phase=build_runtime, table_id={}",
                    self.table_id
                )
            })?,
        );
        self.staged_table = Some(table);
        self.phase = CreateTablePhase::RuntimeBuilt;
        Ok(())
    }

    #[inline]
    async fn commit_catalog(&mut self) -> RuntimeOrFatalResult<TrxID> {
        debug_assert_eq!(self.phase, CreateTablePhase::RuntimeBuilt);
        let trx = self
            .trx
            .take()
            .expect("catalog transaction is present before commit");
        let create_cts = trx.commit_catalog_ddl().await?;
        self.phase = CreateTablePhase::CatalogCommitted;
        Ok(create_cts)
    }

    #[inline]
    fn install_runtime(&mut self, catalog: &Catalog, create_cts: TrxID) -> bool {
        debug_assert_eq!(self.phase, CreateTablePhase::CatalogCommitted);
        let table = Arc::clone(
            self.staged_table
                .as_ref()
                .expect("staged table runtime is present before install"),
        );
        // The table id was atomically allocated and this DDL owns the metadata
        // gate through commit, so no cache entry can exist for this runtime.
        if !catalog.insert_user_table(create_cts, table) {
            self.phase = CreateTablePhase::Aborted;
            return false;
        }
        let _ = self.staged_table.take();
        self.phase = CreateTablePhase::Installed;
        true
    }

    #[inline]
    fn delete_provisional_file(&mut self, table_fs: &FileSystem) -> IoResult<()> {
        match self.file.take() {
            Some(CreateTableFile::Mutable(mutable_file)) => {
                let _ = (*mutable_file).try_delete();
            }
            Some(CreateTableFile::Published(table_file)) => drop(table_file),
            None => {}
        }
        table_fs.delete_user_table_file(self.table_id)
    }

    async fn destroy_staged_runtime(&mut self, guards: &PoolGuards) -> RuntimeResult<()> {
        let Some(table) = self.staged_table.take() else {
            return Ok(());
        };
        let table = Arc::try_unwrap(table).unwrap_or_else(|table| {
            panic!(
                "staged create-table runtime still referenced during cleanup: table_id={}, strong_count={}",
                self.table_id,
                Arc::strong_count(&table)
            )
        });
        table.close_checkpoint_workflow_offline();
        table.destroy_dropped_runtime(guards).await
    }

    async fn abort_before_catalog_commit(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        operation: &'static str,
        source: impl Into<RuntimeOrFatalError>,
    ) -> RuntimeOrFatalError {
        let source = source.into();
        let source_debug = format!("{source:?}");
        let mut error = source;
        if let Err(err) = self.destroy_staged_runtime(guards).await {
            let cleanup = poison_error_source(
                &engine.poisoner,
                RuntimeOrFatalError::from(err),
                FatalError::Poisoned,
                format!(
                    "create table cleanup failed: table_id={}, operation={operation}, cleanup_operation=runtime_destroy, source_error={source_debug}",
                    self.table_id
                ),
            );
            error = error.merge_cleanup(cleanup);
        }
        if let Some(trx) = self.trx.take()
            && let Err(err) = trx.rollback_catalog_ddl().await
        {
            let cleanup = poison_error_source(
                &engine.poisoner,
                err,
                FatalError::RollbackAccess,
                format!(
                    "create table rollback cleanup failed: table_id={}, operation={operation}, source_error={source_debug}",
                    self.table_id
                ),
            );
            error = error.merge_cleanup(cleanup);
        }
        if let Err(err) = self.delete_provisional_file(&engine.table_fs) {
            let cleanup = RuntimeOrFatalError::from(
                err.change_context(RuntimeError::CatalogAccess)
                    .attach(format!(
                        "operation=create_table, phase=delete_provisional_file, table_id={}",
                        self.table_id
                    )),
            );
            error = error.merge_cleanup(cleanup);
        }
        self.phase = CreateTablePhase::Aborted;
        error
    }

    async fn abort_after_root_publish_commit_error(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        operation: &'static str,
        source: RuntimeOrFatalError,
    ) -> RuntimeOrFatalError {
        let source_debug = format!("{source:?}");
        if let Err(err) = self.destroy_staged_runtime(guards).await {
            self.phase = CreateTablePhase::Aborted;
            return poison_error_source(
                &engine.poisoner,
                RuntimeOrFatalError::from(err),
                FatalError::Poisoned,
                format!(
                    "create table cleanup failed: table_id={}, operation={operation}, cleanup_operation=runtime_destroy_after_root_publish, source_error={source_debug}",
                    self.table_id
                ),
            );
        }
        self.phase = CreateTablePhase::Aborted;
        poison_error_source(
            &engine.poisoner,
            source,
            FatalError::Poisoned,
            format!(
                "create table failed after table-root publish: table_id={}, operation={operation}",
                self.table_id
            ),
        )
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
    ) -> OperationResult<Self> {
        let mut slots = vec![None; next_index_no as usize];
        let mut active_count = 0usize;
        for active_index_spec in active_index_specs {
            let index_no = active_index_spec.index_no as usize;
            if index_no >= next_index_no as usize {
                return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                    "index_no {index_no} must be less than next_index_no {next_index_no}"
                )));
            }
            if slots[index_no].is_some() {
                return Err(Report::new(OperationError::InvalidMetadata)
                    .attach(format!("duplicate index_no {index_no}")));
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
    pub(crate) fn try_new(column_specs: Vec<ColumnSpec>) -> OperationResult<Self> {
        if column_specs.is_empty() {
            return Err(Report::new(OperationError::InvalidMetadata)
                .attach("table column layout requires columns"));
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
    ) -> OperationResult<Self> {
        if col_names.is_empty() || col_types.is_empty() || col_attrs.is_empty() {
            return Err(Report::new(OperationError::InvalidMetadata)
                .attach("table column layout requires columns"));
        }
        if col_names.len() != col_types.len() || col_names.len() != col_attrs.len() {
            return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
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
                return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
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
    ) -> OperationResult<Self> {
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
    ) -> OperationResult<(IndexNo, Self)> {
        let index_no = self.next_index_no;
        validate_index_spec(index_no as usize, &index_spec, column_layout.col_count())?;
        let next_index_no = index_no.checked_add(1).ok_or_else(|| {
            Report::new(OperationError::InvalidMetadata).attach("next_index_no overflow")
        })?;
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
    fn without_index(&self, column_layout: &TableColumnLayout, index_no: IndexNo) -> Self {
        let index_no_usize = usize::from(index_no);
        assert!(
            index_no_usize < self.index_slot_count(),
            "drop-index metadata invariant violated: index_no={index_no}, next_index_no={}",
            self.next_index_no
        );
        assert!(
            self.index_spec(index_no_usize).is_some(),
            "drop-index metadata invariant violated: inactive index_no={index_no}, next_index_no={}",
            self.next_index_no
        );

        let index_specs = self
            .active_indexes()
            .filter(|(active_index_no, _)| *active_index_no != index_no_usize)
            .map(|(active_index_no, spec)| {
                ActiveIndexSpec::new(active_index_no as IndexNo, spec.clone())
            })
            .collect::<Vec<_>>();
        Self::try_create(column_layout, index_specs, self.next_index_no).unwrap_or_else(|err| {
            panic!(
                "drop-index metadata rebuild invariant violated: index_no={index_no}, next_index_no={}, error={err:?}",
                self.next_index_no
            )
        })
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
    pub(crate) fn require_index_spec(&self, index_no: usize) -> InternalResult<&IndexSpec> {
        self.index_spec(index_no).ok_or_else(|| {
            Report::new(InternalError::SecondaryIndexOutOfBounds).attach(format!(
                "index_no={index_no}, index_slot_count={}",
                self.index_slot_count()
            ))
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
    pub(crate) fn match_key(&self, index_no: usize, key_vals: &[Val], row: &[Val]) -> bool {
        let Some(keys) = self.index_spec(index_no).map(|spec| &spec.cols) else {
            return false;
        };
        if keys.len() != key_vals.len() {
            return false;
        }
        keys.iter()
            .zip(key_vals)
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

    /// Validates that the input key targets this primary key and matches its
    /// column shape.
    #[inline]
    pub(crate) fn validate_key(
        &self,
        index_no: usize,
        key_vals: &[Val],
    ) -> StdResult<(), PrimaryKeyMatchError> {
        if index_no != self.index_no {
            return Err(PrimaryKeyMatchError::IndexNo {
                actual: index_no,
                expected: self.index_no,
            });
        }
        if key_vals.len() != self.index_spec.cols.len() {
            return Err(PrimaryKeyMatchError::ValueCount {
                actual: key_vals.len(),
                expected: self.index_spec.cols.len(),
            });
        }
        if !self
            .index_spec
            .cols
            .iter()
            .zip(key_vals)
            .all(|(index_key, val)| {
                self.column_layout
                    .col_type_match(usize::from(index_key.col_no), val)
            })
        {
            return Err(PrimaryKeyMatchError::Type { index_no });
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
    ) -> OperationResult<Self> {
        let next_index_no = IndexNo::try_from(index_specs.len()).map_err(|_| {
            Report::new(OperationError::InvalidMetadata)
                .attach("next_index_no overflow while deriving table metadata")
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
            .collect::<OperationResult<Vec<_>>>()?;
        Self::try_new_with_next_index_no(column_specs, active_index_specs, next_index_no)
    }

    /// Try to create metadata with an explicit durable next index number.
    #[inline]
    pub(crate) fn try_new_with_next_index_no(
        column_specs: Vec<ColumnSpec>,
        index_specs: Vec<ActiveIndexSpec>,
        next_index_no: IndexNo,
    ) -> OperationResult<Self> {
        let column_layout = Arc::new(TableColumnLayout::try_new(column_specs)?);
        let index_layout =
            TableIndexLayout::try_create(&column_layout, index_specs, next_index_no)?;
        Ok(Self {
            col: column_layout,
            idx: index_layout,
        })
    }

    /// Reconstructs metadata previously validated and persisted by the engine.
    #[inline]
    pub(crate) fn from_persisted_parts(
        column_specs: Vec<ColumnSpec>,
        index_specs: Vec<ActiveIndexSpec>,
        next_index_no: IndexNo,
    ) -> Self {
        Self::try_new_with_next_index_no(column_specs, index_specs, next_index_no).unwrap_or_else(
            |err| {
                panic!(
                    "persisted table metadata invariant violated: next_index_no={next_index_no}, error={err:?}"
                )
            },
        )
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
    ) -> OperationResult<Self> {
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
    pub(crate) fn try_with_created_index(
        &self,
        index_spec: IndexSpec,
    ) -> OperationResult<(IndexNo, Self)> {
        let (index_no, index_layout) = self.idx.try_with_created_index(&self.col, index_spec)?;
        let metadata = Self {
            col: Arc::clone(&self.col),
            idx: index_layout,
        };
        Ok((index_no, metadata))
    }

    /// Returns metadata with one active index slot made inactive.
    #[inline]
    pub(crate) fn without_index(&self, index_no: IndexNo) -> Self {
        let index_layout = self.idx.without_index(&self.col, index_no);
        Self {
            col: Arc::clone(&self.col),
            idx: index_layout,
        }
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

impl From<TableBriefMetadata> for TableMetadata {
    #[inline]
    fn from(value: TableBriefMetadata) -> Self {
        TableMetadata::try_create(
            value.col_names,
            value.col_types,
            value.col_attrs,
            value.index_specs,
            value.next_index_no,
        )
        .unwrap_or_else(|err| {
            panic!(
                "persisted table-file metadata invariant violated: next_index_no={}, error={err:?}",
                value.next_index_no
            )
        })
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

    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
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

/// Caller-prepared CREATE TABLE awaiting mandatory runtime capacity.
pub(crate) struct PreparedCreateTable {
    scope: PreparedDdlScope,
    plan: CreateTablePlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedCreateTable {
    /// Build one fully prepared CREATE TABLE carrier.
    #[inline]
    pub(crate) fn new(scope: PreparedDdlScope, plan: CreateTablePlan) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id,
        );
        Self {
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedCreateTable {
    type Output = TableID;
    type Accepted = AcceptedCreateTable;

    const LABEL: &'static str = "create_table";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id;
        AcceptedCreateTable {
            scope: scope.accept(),
            table_id,
            progress: Some(CreateTableProgress::new(plan)),
        }
    }
}

/// Mandatory-runtime owner of accepted CREATE TABLE execution.
pub(crate) struct AcceptedCreateTable {
    scope: AcceptedDdlScope,
    table_id: TableID,
    progress: Option<CreateTableProgress>,
}

impl AcceptedExecution for AcceptedCreateTable {
    type Output = TableID;

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self.execute_inner().await;
        self.scope.mark_terminal_ready();
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.progress.take());
        self.scope.finish();
    }

    #[inline]
    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
        }
        self.scope.handle_panic();
        let phase = match self.progress.as_ref() {
            Some(progress) => progress.phase,
            None => CreateTablePhase::Aborted,
        };
        CompletionErrorBridge::capture(Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "accepted CREATE TABLE panicked: table_id={}, phase={:?}",
            self.table_id, phase
        )))
    }
}

impl AcceptedCreateTable {
    async fn execute_inner(&mut self) -> CompletionResult<TableID> {
        let scope = &mut self.scope;
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| panic!("accepted CREATE progress exists during execution"));
        let engine = scope.engine().clone();
        let guards = engine.pool_guards();
        let table_id = progress.table_id;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateBeforeFirstEffect)
            .await;

        let mutable_file = engine
            .table_fs
            .create_table_file(table_id, Arc::clone(progress.metadata()), false)
            .map_err(CompletionErrorBridge::capture)?;
        progress.set_provisional_file(mutable_file);

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateFileCreated)
            .await;

        let trx = match scope.begin_private_trx() {
            Ok(trx) => trx,
            Err(err) => {
                let source_debug = format!("{err:?}");
                progress.phase = CreateTablePhase::Aborted;
                if let Err(cleanup_err) = progress.delete_provisional_file(&engine.table_fs) {
                    return Err(CompletionErrorBridge::capture(cleanup_err.attach(format!(
                    "create table provisional-file cleanup failed after transaction begin: table_id={table_id}, source_error={source_debug}"
                ))));
                }
                return Err(CompletionErrorBridge::capture(
                    err.attach("operation=create_table, phase=begin_private_transaction"),
                ));
            }
        };
        progress.set_catalog_transaction(trx);

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreatePrivateTransactionBegun)
            .await;

        let metadata = Arc::clone(progress.metadata());
        let exec_res = engine
            .catalog()
            .storage
            .stage_create_table(
                progress
                    .trx
                    .as_mut()
                    .unwrap_or_else(|| panic!("CREATE staging requires private transaction")),
                table_id,
                &metadata,
            )
            .await;
        if let Err(err) = exec_res {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "catalog_staging", err)
                    .await,
            ));
        }
        progress.mark_catalog_staged();

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateCatalogStaged)
            .await;

        #[cfg(test)]
        if let Err(err) = engine
            .table_ddl_test
            .maybe_fail_create(CreateTableTestFailure::AfterCatalogStaged)
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "test_after_catalog_staging", err)
                    .await,
            ));
        }

        if let Err(err) = progress.publish_file(&engine.trx_sys).await {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "file_publish", err)
                    .await,
            ));
        }

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateFilePublished)
            .await;

        #[cfg(test)]
        if let Err(err) = engine
            .table_ddl_test
            .maybe_fail_create(CreateTableTestFailure::AfterFilePublished)
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "test_after_file_publish", err)
                    .await,
            ));
        }

        if let Err(err) = progress.build_runtime(&engine.pools, guards).await {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "runtime_build", err)
                    .await,
            ));
        }

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateRuntimeBuilt)
            .await;

        #[cfg(test)]
        if let Err(err) = engine
            .table_ddl_test
            .maybe_fail_create(CreateTableTestFailure::AfterRuntimeBuilt)
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .abort_before_catalog_commit(&engine, guards, "test_after_runtime_build", err)
                    .await,
            ));
        }

        #[cfg(test)]
        engine
            .table_ddl_test
            .maybe_poison_before_create_commit(&engine.poisoner);

        let create_cts = match progress.commit_catalog().await {
            Ok(create_cts) => create_cts,
            Err(err) => {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                    progress
                        .abort_after_root_publish_commit_error(
                            &engine,
                            guards,
                            "catalog_commit",
                            err,
                        )
                        .await,
                ));
            }
        };

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateCatalogCommitted)
            .await;

        assert!(
            progress.install_runtime(engine.catalog(), create_cts),
            "allocated CREATE TABLE id duplicated current runtime during accepted execution: table_id={table_id}"
        );

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::CreateRuntimeInstalled)
            .await;

        Ok(table_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropTablePhase {
    Prepared,
    PrivateTransactionActive,
    LifecycleClosed,
    DrainComplete,
    CatalogStaged,
    CatalogCommitted,
    RuntimeRetained,
}

struct DropTableProgress {
    plan: DropTablePlan,
    phase: DropTablePhase,
    trx: Option<PrivateTransaction>,
}

impl DropTableProgress {
    #[inline]
    fn new(plan: DropTablePlan) -> Self {
        Self {
            plan,
            phase: DropTablePhase::Prepared,
            trx: None,
        }
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }
}

/// Caller-prepared DROP TABLE awaiting mandatory runtime capacity.
pub(crate) struct PreparedDropTable {
    scope: PreparedDdlScope,
    plan: DropTablePlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedDropTable {
    /// Build one fully prepared DROP TABLE carrier.
    #[inline]
    pub(crate) fn new(scope: PreparedDdlScope, plan: DropTablePlan) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id,
        );
        Self {
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedDropTable {
    type Output = ();
    type Accepted = AcceptedDropTable;

    const LABEL: &'static str = "drop_table";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id;
        AcceptedDropTable {
            scope: scope.accept(),
            table_id,
            progress: Some(DropTableProgress::new(plan)),
        }
    }
}

/// Mandatory-runtime owner of accepted DROP TABLE execution.
pub(crate) struct AcceptedDropTable {
    scope: AcceptedDdlScope,
    table_id: TableID,
    progress: Option<DropTableProgress>,
}

impl AcceptedExecution for AcceptedDropTable {
    type Output = ();

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self.execute_inner().await;
        self.scope.mark_terminal_ready();
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.progress.take());
        self.scope.finish();
    }

    #[inline]
    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
        }
        self.scope.handle_panic();
        let phase = match self.progress.as_ref() {
            Some(progress) => progress.phase,
            None => DropTablePhase::Prepared,
        };
        CompletionErrorBridge::capture(Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "accepted DROP TABLE panicked: table_id={}, phase={:?}",
            self.table_id, phase
        )))
    }
}

impl AcceptedDropTable {
    async fn execute_inner(&mut self) -> CompletionResult<()> {
        let scope = &mut self.scope;
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| panic!("accepted DROP progress exists during execution"));
        let engine = scope.engine().clone();
        let table_id = progress.plan.table_id;
        let table = progress.plan.take_table();

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropBeforeFirstEffect)
            .await;

        let trx = scope.begin_private_trx().map_err(|err| {
            CompletionErrorBridge::capture(
                err.attach("operation=drop_table, phase=begin_private_transaction"),
            )
        })?;
        progress.trx = Some(trx);
        progress.phase = DropTablePhase::PrivateTransactionActive;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropPrivateTransactionBegun)
            .await;

        let drain = match table.start_drop_lifecycle() {
            Ok(drain) => drain,
            Err(err) => {
                let source_debug = format!("{err:?}");
                let trx = progress.trx.take().unwrap_or_else(|| {
                    panic!("DROP lifecycle failure requires private transaction")
                });
                if let Err(rollback_err) = trx.rollback_catalog_ddl().await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                        rollback_err.attach_with(|| {
                            format!(
                                "drop table rollback failed after lifecycle rejection: table_id={table_id}, source_error={source_debug}"
                            )
                        }),
                    ));
                }
                return Err(CompletionErrorBridge::capture(err));
            }
        };
        progress.phase = DropTablePhase::LifecycleClosed;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropLifecycleClosed)
            .await;

        drain.wait().await;
        progress.phase = DropTablePhase::DrainComplete;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropDrainComplete)
            .await;

        let metadata = table.metadata().clone();
        let exec_res = engine
            .catalog()
            .storage
            .stage_drop_table(
                progress
                    .trx
                    .as_mut()
                    .unwrap_or_else(|| panic!("DROP cascade requires private transaction")),
                table_id,
                &metadata,
            )
            .await;
        if let Err(err) = exec_res {
            if let Some(trx) = progress.trx.take()
                && let Err(rollback_err) = trx.rollback_catalog_ddl().await
            {
                let rollback_err = rollback_err.attach_with(|| {
                format!(
                    "best-effort DROP TABLE rollback failed after lifecycle gate: table_id={table_id}"
                )
            });
                obs::error!(
                    "event=ddl_cleanup component=catalog action=rollback_drop_table result=error error={rollback_err:?}"
                );
            }
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                poison_error_source(
                    &engine.poisoner,
                    err,
                    FatalError::Poisoned,
                    format!(
                        "drop table failed after lifecycle gate: table_id={table_id}, operation=catalog_cascade"
                    ),
                ),
            ));
        }
        progress.phase = DropTablePhase::CatalogStaged;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropCatalogStaged)
            .await;

        let trx = progress
            .trx
            .take()
            .unwrap_or_else(|| panic!("DROP commit requires private transaction"));
        let drop_cts = match trx.commit_catalog_ddl().await {
            Ok(drop_cts) => drop_cts,
            Err(err) => {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                    poison_error_source(
                        &engine.poisoner,
                        err,
                        FatalError::Poisoned,
                        format!(
                            "drop table failed after lifecycle gate: table_id={table_id}, operation=commit"
                        ),
                    ),
                ));
            }
        };
        progress.phase = DropTablePhase::CatalogCommitted;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropCatalogCommitted)
            .await;

        let replay_floor = engine
            .catalog()
            .effective_user_table_redo_replay_floor(table_id, table.redo_replay_floor_snapshot());
        finish_drop_table_runtime_retention(&engine, table_id, table, drop_cts, replay_floor)
            .map_err(CompletionErrorBridge::capture)?;
        progress.phase = DropTablePhase::RuntimeRetained;

        #[cfg(test)]
        engine
            .table_ddl_test
            .reach_phase(TableDdlTestPhase::DropRuntimeRetained)
            .await;

        engine.trx_sys.request_dropped_table_purge();
        engine.trx_sys.request_metadata_history_purge();
        Ok(())
    }
}

/// Return the fixed catalog tables written by CREATE TABLE.
#[inline]
pub(crate) const fn create_table_catalog_write_targets() -> &'static [TableID] {
    &CREATE_TABLE_CATALOG_WRITE_TARGETS
}

/// Return the fixed catalog tables written by DROP TABLE.
#[inline]
pub(crate) const fn drop_table_catalog_write_targets() -> &'static [TableID] {
    &DROP_TABLE_CATALOG_WRITE_TARGETS
}

/// Reject table ids outside user-managed catalog space.
#[inline]
pub(crate) fn reject_non_user_table_id(
    table_id: TableID,
    operation: &'static str,
) -> OperationResult<()> {
    if is_user_table(table_id) {
        return Ok(());
    }
    Err(Report::new(OperationError::TableNotFound).attach(format!(
        "{operation} requires user table id: table_id={table_id}"
    )))
}

/// Ensure the user-table catalog row exists for a DDL operation.
#[inline]
pub(crate) async fn ensure_user_table_catalog_row(
    engine: &EngineCore,
    guards: &PoolGuards,
    table_id: TableID,
    operation: &'static str,
) -> OperationOrRuntimeResult<()> {
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

/// Return the validated runtime table for an index-DDL target.
pub(crate) async fn validated_index_ddl_target(
    engine: &EngineCore,
    guards: &PoolGuards,
    table_id: TableID,
    operation: &'static str,
) -> OperationOrRuntimeResult<Arc<Table>> {
    reject_non_user_table_id(table_id, operation)?;
    let table = engine
        .catalog()
        .validate_user_table_live(table_id)
        .await
        .attach_with(|| format!("operation={operation}"))?;
    ensure_user_table_catalog_row(engine, guards, table_id, operation).await?;
    Ok(table)
}

/// Reject primary-key flags in public user-table DDL for now.
#[inline]
pub(crate) fn reject_user_table_primary_key_index(
    index_spec: &IndexSpec,
    operation: &'static str,
) -> OperationResult<()> {
    if !index_spec.primary_key() {
        return Ok(());
    }
    Err(Report::new(OperationError::InvalidMetadata).attach(format!(
        "{operation} does not support user-table primary keys"
    )))
}

#[inline]
fn reject_user_table_primary_key_indexes(
    index_specs: &[IndexSpec],
    operation: &'static str,
) -> OperationResult<()> {
    for index_spec in index_specs {
        reject_user_table_primary_key_index(index_spec, operation)?;
    }
    Ok(())
}

#[inline]
fn finish_drop_table_runtime_retention(
    engine: &EngineCore,
    table_id: TableID,
    table: Arc<Table>,
    drop_cts: TrxID,
    replay_floor: TableRedoReplayFloor,
) -> FatalResult<()> {
    table.mark_dropped_lifecycle();
    if engine
        .catalog()
        .mark_user_table_dropped_runtime(table_id, table, drop_cts, replay_floor)
    {
        return Ok(());
    }
    Err(poison_drop_table_after_gate(
        &engine.poisoner,
        table_id,
        "runtime_retention",
    ))
}

#[inline]
fn poison_drop_table_after_gate(
    poisoner: &EnginePoisoner,
    table_id: TableID,
    operation: &'static str,
) -> Report<FatalError> {
    // Once terminal drop admission succeeds, the table's checkpoint publish gate
    // is closed and the operation cannot be safely retried as an ordinary DDL
    // failure. Poison admission so future work sees the fatal state; explicit
    // engine shutdown remains responsible for stopping background workers.
    let report = Report::new(FatalError::Poisoned).attach(format!(
        "drop table failed after lifecycle gate: table_id={table_id}, operation={operation}"
    ));
    obs::error!(
        "event=engine_poison component=catalog_table action=poison result=error error={:?}",
        report
    );
    poisoner.poison(report).into_report()
}

/// Fatalizes a typed catalog source while retaining its physical evidence.
#[inline]
fn poison_error_source(
    poisoner: &EnginePoisoner,
    source: RuntimeOrFatalError,
    reason: FatalError,
    message: String,
) -> RuntimeOrFatalError {
    let report = source.into_fatal_report(reason).attach(message);
    obs::error!(
        "event=engine_poison component=catalog_table action=poison result=error error={:?}",
        report
    );
    RuntimeOrFatalError::from(poisoner.poison(report).into_report())
}

#[inline]
fn validate_index_spec(index_no: usize, spec: &IndexSpec, col_count: usize) -> OperationResult<()> {
    if spec.cols.is_empty() {
        return Err(Report::new(OperationError::InvalidMetadata)
            .attach(format!("index_no {index_no} has no key columns")));
    }
    for key in &spec.cols {
        let col_no = key.col_no as usize;
        if col_no >= col_count {
            return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
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
) -> OperationResult<()> {
    let mut primary_key_index_no = None;
    for (index_no, index_spec) in index_specs.active_indexes() {
        if !index_spec.primary_key() {
            continue;
        }
        if let Some(existing_index_no) = primary_key_index_no {
            return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                "multiple primary keys: index_no {existing_index_no} and index_no {index_no}"
            )));
        }
        for key in &index_spec.cols {
            let col_no = usize::from(key.col_no);
            if column_layout.nullable(col_no) {
                return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                    "primary key index_no {index_no} references nullable column {col_no}"
                )));
            }
        }
        primary_key_index_no = Some(index_no);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::tests::{
        assert_dropped_table_floor, assert_dropped_table_runtime,
        assert_no_dropped_table_operational_state, wait_for_dropped_table_floor,
        wait_for_no_dropped_table_operational_state,
    };
    use crate::catalog::{
        CatalogCheckpointScanStopReason, ColumnAttributes, ColumnSpec, CurrentTableState,
        IndexAttributes, IndexKey, IndexSpec, TableMetadata, TableSpec,
    };
    use crate::engine::Engine;
    use crate::error::{
        DiscloseError, Error, ErrorKind, FatalError, IoError, LifecycleError, OperationError,
        RuntimeError,
    };
    use crate::id::{SessionID, TrxID};
    use crate::io::install_storage_backend_test_hook;
    use crate::lock::tests::{LockDebugEntryState, TestLockOwner, debug_snapshot};
    use crate::lock::{LockMode, LockOwner, LockResource, TableLockMode};
    use crate::log::redo::DDLRedo;
    use crate::session::tests::{
        SessionTestExt, active_operation_count, active_operation_snapshot, remove_session_for_test,
    };
    use crate::table::TableTerminal;
    use crate::table::tests::*;
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::trx::purge::PurgeTestEvent;
    use crate::trx::tests as trx_tests;
    use crate::value::{Val, ValKind};
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

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum TableDdlTestPhase {
        CreateBeforeFirstEffect,
        CreateFileCreated,
        CreatePrivateTransactionBegun,
        CreateCatalogStaged,
        CreateFilePublished,
        CreateRuntimeBuilt,
        CreateCatalogCommitted,
        CreateRuntimeInstalled,
        DropBeforeFirstEffect,
        DropPrivateTransactionBegun,
        DropLifecycleClosed,
        DropDrainComplete,
        DropCatalogStaged,
        DropCatalogCommitted,
        DropRuntimeRetained,
    }

    struct TableDdlTestGate {
        phase: TableDdlTestPhase,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    /// Per-engine CREATE/DROP fault controller shared across runtime threads.
    #[derive(Default)]
    pub(crate) struct TableDdlTestController {
        create_failure: parking_lot::Mutex<Option<CreateTableTestFailure>>,
        panic_phase: parking_lot::Mutex<Option<TableDdlTestPhase>>,
        gate: parking_lot::Mutex<Option<TableDdlTestGate>>,
    }

    impl TableDdlTestController {
        #[inline]
        fn set_create_failure(&self, failure: Option<CreateTableTestFailure>) {
            *self.create_failure.lock() = failure;
        }

        #[inline]
        pub(super) fn maybe_fail_create(
            &self,
            failure: CreateTableTestFailure,
        ) -> RuntimeResult<()> {
            if *self.create_failure.lock() == Some(failure) {
                return Err(Report::new(RuntimeError::CatalogAccess)
                    .attach("operation=test_create_table_phase_failure"));
            }
            Ok(())
        }

        #[inline]
        pub(super) fn maybe_poison_before_create_commit(&self, poisoner: &EnginePoisoner) {
            if *self.create_failure.lock()
                == Some(CreateTableTestFailure::PoisonBeforeCatalogCommit)
            {
                let _ = poisoner
                    .poison(Report::new(FatalError::Poisoned).attach("forced create-table poison"));
            }
        }

        fn install_gate(
            &self,
            phase: TableDdlTestPhase,
        ) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let previous = self.gate.lock().replace(TableDdlTestGate {
                phase,
                entered: entered_tx,
                release: release_rx,
            });
            assert!(
                previous.is_none(),
                "table DDL test gate is already installed"
            );
            (entered_rx, release_tx)
        }

        fn set_panic_phase(&self, phase: Option<TableDdlTestPhase>) {
            *self.panic_phase.lock() = phase;
        }

        pub(super) async fn reach_phase(&self, phase: TableDdlTestPhase) {
            let should_panic = {
                let mut panic_phase = self.panic_phase.lock();
                if *panic_phase == Some(phase) {
                    *panic_phase = None;
                    true
                } else {
                    false
                }
            };
            if should_panic {
                panic!("injected accepted table DDL panic: phase={phase:?}");
            }
            let gate = {
                let mut slot = self.gate.lock();
                if slot.as_ref().is_some_and(|gate| gate.phase == phase) {
                    slot.take()
                } else {
                    None
                }
            };
            let Some(gate) = gate else {
                return;
            };
            let _ = gate.entered.send_async(()).await;
            let _ = gate.release.recv_async().await;
        }
    }

    fn set_create_table_failure(engine: &Engine, failure: Option<CreateTableTestFailure>) {
        engine.inner().table_ddl_test.set_create_failure(failure);
    }

    fn assert_invalid_metadata(err: Error, expected_message: &str) {
        assert!(err.is_kind(crate::error::ErrorKind::Operation));
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::InvalidMetadata)
        );
        let report = format!("{err:?}");
        assert!(report.contains(expected_message), "{report}");
    }

    async fn request_and_wait_for_purge_cycle(
        engine: &Engine,
        event_rx: &flume::Receiver<PurgeTestEvent>,
    ) {
        while event_rx.try_recv().is_ok() {}
        engine.inner().trx_sys.request_dropped_table_purge();
        let mut dropped_table_started = false;
        loop {
            match event_rx.recv_async().await.unwrap() {
                PurgeTestEvent::DroppedTableStarted => dropped_table_started = true,
                PurgeTestEvent::CycleCompleted if dropped_table_started => return,
                _ => {}
            }
        }
    }

    async fn wait_for_table_terminal(table: &Table, expected: TableTerminal) {
        loop {
            let changed = table.lifecycle.listener();
            if table.lifecycle.inspect_terminal() == expected {
                return;
            }
            changed.await;
        }
    }

    fn assert_no_user_table_publication(engine: &Engine, table_id: TableID) {
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .get_table_now(table_id)
                .is_none()
        );
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .is_none()
        );
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, MAX_SNAPSHOT_TS)
                .is_none()
        );
        assert_eq!(
            engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            None
        );
        assert_no_dropped_table_operational_state(engine.inner().core.catalog(), table_id);
    }

    struct TableDdlSnapshot {
        effective_cts: TrxID,
        metadata: Arc<TableMetadata>,
        table: Arc<Table>,
        history_count: Option<usize>,
        retained_dropped_state: bool,
        file_exists: bool,
        lifecycle: TableTerminal,
        poisoned: bool,
    }

    fn table_ddl_snapshot(engine: &Engine, table_id: TableID, table: &Table) -> TableDdlSnapshot {
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            table: current_table,
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("table DDL snapshot requires a live current table");
        };
        TableDdlSnapshot {
            effective_cts,
            metadata,
            table: current_table,
            history_count: engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            retained_dropped_state: engine
                .inner()
                .core
                .catalog()
                .retained_dropped_table_ids_now()
                .contains(&table_id),
            file_exists: Path::new(&engine.inner().table_fs.user_table_file_path(table_id))
                .exists(),
            lifecycle: table.lifecycle.inspect_terminal(),
            poisoned: engine.inner().poisoner.poison_error().is_some(),
        }
    }

    fn assert_table_logical_snapshot_unchanged(
        before: &TableDdlSnapshot,
        engine: &Engine,
        table_id: TableID,
    ) {
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            table: current_table,
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("failed pre-gate DROP TABLE must keep the table live");
        };
        assert_eq!(effective_cts, before.effective_cts);
        assert!(Arc::ptr_eq(&metadata, &before.metadata));
        assert!(Arc::ptr_eq(&current_table, &before.table));
        assert_eq!(
            engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            before.history_count
        );
        assert_eq!(
            engine
                .inner()
                .core
                .catalog()
                .retained_dropped_table_ids_now()
                .contains(&table_id),
            before.retained_dropped_state
        );
        assert_eq!(
            Path::new(&engine.inner().table_fs.user_table_file_path(table_id)).exists(),
            before.file_exists
        );
    }

    fn assert_table_ddl_snapshot_unchanged(
        before: &TableDdlSnapshot,
        engine: &Engine,
        table_id: TableID,
        table: &Table,
    ) {
        assert_table_logical_snapshot_unchanged(before, engine, table_id);
        assert_eq!(table.lifecycle.inspect_terminal(), before.lifecycle);
        assert_eq!(
            engine.inner().poisoner.poison_error().is_some(),
            before.poisoned
        );
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

        assert!(primary_key.validate_key(0, &[Val::from(42u32)]).is_ok());
        assert_eq!(
            primary_key.validate_key(1, &[Val::from(42u32)]),
            Err(PrimaryKeyMatchError::IndexNo {
                actual: 1,
                expected: 0
            })
        );
        assert_eq!(
            primary_key.validate_key(0, &[Val::from(42u32), Val::from(99u64)]),
            Err(PrimaryKeyMatchError::ValueCount {
                actual: 2,
                expected: 1
            })
        );
        assert_eq!(
            primary_key.validate_key(0, &[Val::from(42u64)]),
            Err(PrimaryKeyMatchError::Type { index_no: 0 })
        );
        assert_eq!(
            primary_key.validate_key(0, &[Val::Null]),
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
        let dropped = created.without_index(index_no);

        assert!(Arc::ptr_eq(&metadata.col, &created.col));
        assert!(Arc::ptr_eq(&metadata.col, &dropped.col));
        assert_eq!(created.idx.active_index_count(), 1);
        assert_eq!(dropped.idx.active_index_count(), 0);
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

        assert_invalid_metadata(err.disclose(), "multiple primary keys");
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

        assert_invalid_metadata(err.disclose(), "multiple primary keys");
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

        assert_invalid_metadata(
            err.disclose(),
            "primary key index_no 0 references nullable column 0",
        );
    }

    #[test]
    fn test_table_metadata_rejects_invalid_index_specs_as_operation_errors() {
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
        assert_invalid_metadata(err.disclose(), "index_no 0 has no key columns");

        let err = TableMetadata::try_new_with_next_index_no(
            columns.clone(),
            vec![ActiveIndexSpec::new(
                1,
                IndexSpec::new(vec![], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap_err();
        assert_invalid_metadata(err.disclose(), "index_no 1 has no key columns");

        let err = TableMetadata::try_new(
            columns,
            vec![IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::PK)],
        )
        .unwrap_err();
        assert_invalid_metadata(
            err.disclose(),
            "index_no 0 references column 1 outside column count 1",
        );
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

        let dropped = metadata.without_index(2);

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
    fn test_first_read_acquires_metadata_lock_for_transaction_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;

            let mut trx = session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();
            let key = single_key(0i32);
            let selected = trx
                .table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                .await
                .unwrap();
            assert!(selected.is_found());
            let repeated = trx
                .table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                .await
                .unwrap();
            assert!(repeated.is_found());
            assert_eq!(lock_entry_count(&engine, trx_owner), 1);
            assert!(!has_lock_resource(
                &engine,
                trx_owner,
                LockResource::TableData(table_id),
            ));

            assert_eq!(lock_entry_count(&engine, trx_owner), 1);
            assert!(has_lock_entry(
                &engine,
                trx_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
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

            trx.table_insert_mvcc(table_id, vec![Val::from(10i32), Val::from("a")])
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

            trx.table_insert_mvcc(table_id, vec![Val::from(11i32), Val::from("b")])
                .await
                .unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 2);

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_concurrent_create_table_publishes_distinct_tables() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mut session1 = engine.new_session().unwrap();
            let mut session2 = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let create1 = smol::spawn({
                let table_spec = table_spec.clone();
                let index_specs = index_specs.clone();
                async move { session1.create_table(table_spec, index_specs).await }
            });
            let create2 =
                smol::spawn(async move { session2.create_table(table_spec, index_specs).await });

            let table_id1 = create1.await.unwrap();
            let table_id2 = create2.await.unwrap();
            assert_ne!(table_id1, table_id2);

            let verify_session = engine.new_session().unwrap();
            let guards = verify_session.pool_guards();
            for table_id in [table_id1, table_id2] {
                assert!(
                    engine
                        .inner()
                        .core
                        .catalog()
                        .get_table(table_id)
                        .await
                        .is_some()
                );
                assert!(
                    engine
                        .inner()
                        .core
                        .catalog()
                        .storage
                        .tables()
                        .find_uncommitted_by_id(&guards, table_id)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert!(
                    !engine
                        .inner()
                        .core
                        .catalog()
                        .storage
                        .columns()
                        .list_uncommitted_by_table_id(&guards, table_id)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    !engine
                        .inner()
                        .core
                        .catalog()
                        .storage
                        .indexes()
                        .list_uncommitted_by_table_id(&guards, table_id)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    !engine
                        .inner()
                        .core
                        .catalog()
                        .storage
                        .index_columns()
                        .list_uncommitted_by_table_id(&guards, table_id)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    Path::new(&engine.inner().table_fs.user_table_file_path(table_id)).exists()
                );
            }
        });
    }

    #[test]
    fn test_create_table_rejects_invalid_metadata_before_file_creation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_invalid_metadata",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
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
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidMetadata)
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableData(table_id),
            ));
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_rejects_primary_key_before_file_creation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_pk_rejected",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
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

            assert_invalid_metadata(err, "create_table does not support user-table primary keys");
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_catalog_staging_failure_rolls_back_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_fail_catalog",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(&engine, Some(CreateTableTestFailure::AfterCatalogStaged));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(&engine, None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::CatalogAccess)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("operation=create_table, phase=wait_mandatory_completion"),
                "{report}"
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableData(table_id),
            ));
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_file_publish_failure_rolls_back_catalog_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_fail_publish",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let hook = Arc::new(FailingFirstWriteHook::new(table_file_path.clone()));
            let _install = install_storage_backend_test_hook(hook.clone());
            let (table_spec, index_specs) = drop_table_test_spec();

            let err = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap_err();

            assert!(err.is_kind(ErrorKind::Runtime), "{err:?}");
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::CatalogAccess)
            );
            assert!(err.report().frames().any(|frame| {
                frame.downcast_ref::<RuntimeError>() == Some(&RuntimeError::FileRootAccess)
            }));
            assert!(err.report().downcast_ref::<IoError>().is_some());
            let report = format!("{err:?}");
            assert!(report.contains("file_kind=table_file"), "{report}");
            assert!(report.contains("phase=write_meta_block"), "{report}");
            assert!(hook.call_count() > 0);
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_after_file_published_failure_rolls_back_catalog_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_fail_after_file",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(&engine, Some(CreateTableTestFailure::AfterFilePublished));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(&engine, None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::CatalogAccess)
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_runtime_failure_after_file_publish_rolls_back_and_deletes_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_fail_runtime",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(&engine, Some(CreateTableTestFailure::AfterRuntimeBuilt));
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(&engine, None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::CatalogAccess)
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(engine.inner().poisoner.poison_error().is_none());
            assert!(!session.in_trx().unwrap());
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_create_table_catalog_commit_error_after_file_publish_poisons_and_keeps_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "create_fail_commit",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();

            set_create_table_failure(
                &engine,
                Some(CreateTableTestFailure::PoisonBeforeCatalogCommit),
            );
            let res = session.create_table(table_spec, index_specs).await;
            set_create_table_failure(&engine, None);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::Poisoned)
            );
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::Poisoned)
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(!session.in_trx().unwrap());
            assert!(Path::new(&table_file_path).exists());

            drop(session);
            drop(engine);

            let recovered = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_fail_commit",
            ))
            .await
            .unwrap();
            assert_no_user_table_publication(&recovered, table_id);
            wait_path_exists(&table_file_path, false).await;
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

            trx.lock_table(table_id, TableLockMode::Shared)
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
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));

            let mut writer_session = engine.new_session().unwrap();
            let (owner_tx, owner_rx) = flume::bounded(1);
            let writer = smol::spawn(async move {
                let mut writer_trx = writer_session.begin_trx().unwrap();
                owner_tx
                    .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                    .await
                    .unwrap();
                writer_trx
                    .table_insert_mvcc(table_id, vec![Val::from(31_001i32), Val::from("blocked")])
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

            trx.lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();
            trx.lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            trx.lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();

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
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();

            let mut read_trx = session.begin_trx().unwrap();
            let key = single_key(0i32);
            let selected = read_trx
                .table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                .await
                .unwrap();
            assert!(selected.is_found());
            read_trx.commit().await.unwrap();

            let mut write_trx = session.begin_trx().unwrap();
            let err = write_trx
                .table_insert_mvcc(
                    table_id,
                    vec![Val::from(31_101i32), Val::from("same-session-s")],
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::LockFamilyConflict)
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
    fn test_session_table_lock_rejects_active_transaction_before_acquisition() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            trx.table_insert_mvcc(
                table_id,
                vec![Val::from(31_301i32), Val::from("same-session-ix")],
            )
            .await
            .unwrap();

            let err = session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_table_lock_cancellation_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let blocker = LockOwner::transaction(SessionID::new(91_301), TrxID::new(91_301));
            let mut blocker = TestLockOwner::new(blocker);
            blocker
                .acquire(
                    engine.inner().core.lock_manager(),
                    LockResource::TableData(table_id),
                    LockMode::Exclusive,
                )
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let session_owner = LockOwner::session_explicit(session.id());
            let mut lock_fut = Box::pin(session.lock_table(table_id, TableLockMode::Shared));
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
            blocker.close(engine.inner().core.lock_manager());
        });
    }

    #[test]
    fn test_transaction_table_lock_failure_releases_fresh_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_owner = LockOwner::session_explicit(session.id());
            session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::LockFamilyConflict)
            );
            assert!(
                !trx_tests::transaction_lock_covers(
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
            let blocker = LockOwner::transaction(SessionID::new(91_302), TrxID::new(91_302));
            let mut blocker = TestLockOwner::new(blocker);
            blocker
                .acquire(
                    engine.inner().core.lock_manager(),
                    LockResource::TableData(table_id),
                    LockMode::Exclusive,
                )
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();
            let mut lock_fut = Box::pin(trx.lock_table(table_id, TableLockMode::Shared));
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
                !trx_tests::transaction_lock_covers(
                    &trx,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared
                )
                .unwrap()
            );
            blocker.close(engine.inner().core.lock_manager());
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
            let session_owner = LockOwner::session_explicit(session.id());
            session
                .lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();

            let mut writer_session = engine.new_session().unwrap();
            let (owner_tx, owner_rx) = flume::bounded(1);
            let external_writer = smol::spawn(async move {
                let mut writer_trx = writer_session.begin_trx().unwrap();
                owner_tx
                    .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                    .await
                    .unwrap();
                writer_trx
                    .table_insert_mvcc(table_id, vec![Val::from(31_201i32), Val::from("external")])
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
            same_session_trx
                .table_insert_mvcc(table_id, vec![Val::from(31_202i32), Val::from("covered")])
                .await
                .unwrap();
            assert!(has_lock_entry(
                &engine,
                same_session_owner,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            let err = session.unlock_table(table_id).unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );

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
                .start_drop_lifecycle()
                .unwrap()
                .wait()
                .await;
            let table = table_for_internal_assertion(&engine, table_id);
            let before = table_ddl_snapshot(&engine, table_id, &table);

            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableDropping)
            );
            assert_table_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert!(!session.in_trx().unwrap());
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    #[test]
    fn test_drop_table_rejects_active_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let before = table_ddl_snapshot(&engine, table_id, &table);

            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert_table_ddl_snapshot_unchanged(&before, &engine, table_id, &table);

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
            let table = table_for_internal_assertion(&engine, table_id);
            let before = table_ddl_snapshot(&engine, table_id, &table);

            let err = session.drop_table(TABLE_ID_TABLES).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            assert_no_user_table_publication(&engine, TABLE_ID_TABLES);

            let missing_user_table_id = table_id + 1000;
            let err = session.drop_table(missing_user_table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            assert_no_user_table_publication(&engine, missing_user_table_id);
            assert_table_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
        });
    }

    #[test]
    fn test_drop_table_missing_catalog_row_panics_under_mandatory_supervision() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let corrupt_session = engine.new_session().unwrap();
            let mut corrupt_trx = begin_catalog_test_trx(&corrupt_session);
            let deleted = engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .delete_by_id(corrupt_trx.trx(), table_id)
                .await
                .unwrap();
            assert!(deleted);
            corrupt_trx.commit(DDLRedo::DropTable(table_id)).await;

            let mut drop_session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let err = drop_session.drop_table(table_id).await.unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .map(|error| *error.current_context()),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);
            assert_checkpoint_workflow_closed(&table);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(table_id)
                    .await
                    .is_some()
            );
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            let shutdown_err = engine.try_shutdown().unwrap_err();
            assert_eq!(
                shutdown_err
                    .report()
                    .downcast_ref::<LifecycleError>()
                    .copied(),
                Some(LifecycleError::ShutdownBusy)
            );

            // FailedRetained deliberately keeps rollback-owned row state alive
            // and blocks destructive component teardown. This test process is
            // the final owner of the poisoned synthetic engine.
            mem::forget((engine, temp_dir, drop_session, table));
        });
    }

    #[test]
    fn test_create_table_execution_panic_before_first_effect_is_supervised() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let (table_spec, index_specs) = drop_table_test_spec();
            engine
                .inner()
                .table_ddl_test
                .set_panic_phase(Some(TableDdlTestPhase::CreateBeforeFirstEffect));

            let err = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .map(|error| *error.current_context()),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_no_user_table_publication(&engine, table_id);
            assert!(!Path::new(&engine.inner().table_fs.user_table_file_path(table_id)).exists());
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            let shutdown_err = engine.try_shutdown().unwrap_err();
            assert_eq!(
                shutdown_err
                    .report()
                    .downcast_ref::<LifecycleError>()
                    .copied(),
                Some(LifecycleError::ShutdownBusy)
            );

            remove_session_for_test(&engine.inner().session_registry, session_id);
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_create_table_execution_panic_parks_active_private_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_table_private_panic").await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let (table_spec, index_specs) = drop_table_test_spec();
            engine
                .inner()
                .table_ddl_test
                .set_panic_phase(Some(TableDdlTestPhase::CreatePrivateTransactionBegun));

            let err = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            let snapshot = active_operation_snapshot(&engine.inner().session_registry, session_id);
            assert_eq!(
                snapshot.state,
                crate::trx::SessionOperationState::FailedRetained
            );
            assert!(snapshot.trx_id.is_some());

            remove_session_for_test(&engine.inner().session_registry, session_id);
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_drop_table_rejects_same_session_explicit_table_lock() {
        smol::block_on(async {
            for (table_mode, lock_mode) in [
                (TableLockMode::Shared, LockMode::Shared),
                (TableLockMode::Exclusive, LockMode::Exclusive),
            ] {
                let temp_dir = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
                let table_id = create_table2_for_test(&engine).await;
                let mut session = engine.new_session().unwrap();
                let owner = LockOwner::session_explicit(session.id());
                let table = table_for_internal_assertion(&engine, table_id);

                session.lock_table(table_id, table_mode).await.unwrap();
                let before = table_ddl_snapshot(&engine, table_id, &table);
                let err = session.drop_table(table_id).await.unwrap_err();
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::LockFamilyConflict)
                );
                let rendered = err.to_string();
                assert_eq!(rendered.matches("operation=drop_table").count(), 1);
                assert!(rendered.contains(&format!("table_id={table_id}")));

                assert_table_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
                assert!(
                    engine
                        .inner()
                        .core
                        .catalog()
                        .get_table(table_id)
                        .await
                        .is_some()
                );
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
                    lock_mode,
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
                drop(before);
                drop(table);
                session.drop_table(table_id).await.unwrap();
            }
        });
    }

    #[test]
    fn test_drop_waiting_on_checkpoint_does_not_block_other_table_drop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let other_table_id = create_table2_for_test(&engine).await;
            let mut purge_blocker_session = engine.new_session().unwrap();
            let purge_blocker = purge_blocker_session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let (root_lease, publish_lease) = begin_checkpoint_publish_for_test(&table);

            let mut waiting_session = engine.new_session().unwrap();
            let mut waiting_drop = Box::pin(waiting_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(waiting_drop.as_mut()),
                std::task::Poll::Pending
            ));
            wait_for_table_terminal(&table, TableTerminal::Dropping).await;
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);

            let mut other_session = engine.new_session().unwrap();
            other_session.drop_table(other_table_id).await.unwrap();
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(other_table_id)
                    .await
                    .is_none()
            );
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);

            drop(root_lease);
            drop(publish_lease);
            drop(table);
            waiting_drop.await.unwrap();
            purge_blocker.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_waiting_on_checkpoint_does_not_block_create_table() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut purge_blocker_session = engine.new_session().unwrap();
            let purge_blocker = purge_blocker_session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let (root_lease, publish_lease) = begin_checkpoint_publish_for_test(&table);

            let mut waiting_session = engine.new_session().unwrap();
            let mut waiting_drop = Box::pin(waiting_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(waiting_drop.as_mut()),
                std::task::Poll::Pending
            ));
            wait_for_table_terminal(&table, TableTerminal::Dropping).await;
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);

            let mut create_session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let created_table_id = create_session
                .create_table(table_spec, index_specs)
                .await
                .unwrap();
            assert_ne!(created_table_id, table_id);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(created_table_id)
                    .await
                    .is_some()
            );
            assert!(
                Path::new(
                    &engine
                        .inner()
                        .table_fs
                        .user_table_file_path(created_table_id)
                )
                .exists()
            );
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);

            drop(root_lease);
            drop(publish_lease);
            drop(table);
            waiting_drop.await.unwrap();
            purge_blocker.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_normally_grants_waiting_session_table_lock() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut purge_blocker_session = engine.new_session().unwrap();
            let purge_blocker = purge_blocker_session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let (root_lease, publish_lease) = begin_checkpoint_publish_for_test(&table);
            let mut drop_session = engine.new_session().unwrap();
            let drop_session_id = drop_session.id();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            let drop_owner = ddl_lock_owner(
                &engine,
                drop_session_id,
                LockResource::TableMetadata(table_id),
            )
            .expect("drop DDL owner should hold metadata X");
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            let mut lock_session = engine.new_session().unwrap();
            let lock_owner = LockOwner::session_explicit(lock_session.id());
            let mut lock_fut = Box::pin(lock_session.lock_table(table_id, TableLockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(root_lease);
            drop(publish_lease);
            drop(table);
            drop_fut.await.unwrap();
            let err = lock_fut.await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
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
            purge_blocker.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_abandoned_create_future_before_first_effect_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = engine.inner().core.catalog().curr_next_table_id();
            let (entered, release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::CreateBeforeFirstEffect);
            let mut create_session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let mut create_fut = Box::pin(create_session.create_table(table_spec, index_specs));

            assert!(matches!(
                futures::poll!(create_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            drop(create_fut);
            release.send_async(()).await.unwrap();

            let mut verify_session = engine.new_session().unwrap();
            verify_session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table_now(table_id)
                    .is_some()
            );
            verify_session.unlock_table(table_id).unwrap();
            verify_session.drop_table(table_id).await.unwrap();
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    #[test]
    fn test_abandoned_drop_future_before_first_effect_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let (entered, release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::DropBeforeFirstEffect);
            let mut drop_session = engine.new_session().unwrap();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            drop(drop_fut);
            release.send_async(()).await.unwrap();

            let mut verify_session = engine.new_session().unwrap();
            let err = verify_session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    #[test]
    fn test_accepted_table_ddl_owns_exact_prepared_lock_sets() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;

            let create_table_id = engine.inner().core.catalog().curr_next_table_id();
            let (create_entered, create_release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::CreateBeforeFirstEffect);
            let mut create_session = engine.new_session().unwrap();
            let create_session_id = create_session.id();
            let (table_spec, index_specs) = drop_table_test_spec();
            let mut create_fut = Box::pin(create_session.create_table(table_spec, index_specs));
            assert!(matches!(
                futures::poll!(create_fut.as_mut()),
                std::task::Poll::Pending
            ));
            create_entered.recv_async().await.unwrap();

            let create_owner = ddl_lock_owner(
                &engine,
                create_session_id,
                LockResource::TableMetadata(create_table_id),
            )
            .expect("accepted CREATE should retain its operation owner");
            assert_eq!(lock_entry_count(&engine, create_owner), 9);
            assert!(has_lock_entry(
                &engine,
                create_owner,
                LockResource::TableMetadata(create_table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));
            for &catalog_table_id in create_table_catalog_write_targets() {
                assert!(has_lock_entry(
                    &engine,
                    create_owner,
                    LockResource::TableMetadata(catalog_table_id),
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                ));
                assert!(has_lock_entry(
                    &engine,
                    create_owner,
                    LockResource::TableData(catalog_table_id),
                    LockMode::IntentExclusive,
                    LockDebugEntryState::Granted,
                ));
            }
            let (create_staged, create_staged_release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::CreateCatalogStaged);
            create_release.send_async(()).await.unwrap();
            create_staged.recv_async().await.unwrap();
            assert_eq!(lock_entry_count(&engine, create_owner), 9);
            assert!(
                debug_snapshot(engine.inner().lock_manager())
                    .entries
                    .into_iter()
                    .filter(|entry| entry.family.session_id() == create_session_id)
                    .all(|entry| {
                        entry.state == LockDebugEntryState::Granted && entry.pending_owner.is_none()
                    }),
                "accepted CREATE must retain only physical held-family diagnostics"
            );
            create_staged_release.send_async(()).await.unwrap();
            assert_eq!(create_fut.await.unwrap(), create_table_id);

            let (drop_entered, drop_release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::DropBeforeFirstEffect);
            let mut drop_session = engine.new_session().unwrap();
            let drop_session_id = drop_session.id();
            let mut drop_fut = Box::pin(drop_session.drop_table(create_table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            drop_entered.recv_async().await.unwrap();

            let drop_owner = ddl_lock_owner(
                &engine,
                drop_session_id,
                LockResource::TableMetadata(create_table_id),
            )
            .expect("accepted DROP should retain its operation owner");
            assert_eq!(lock_entry_count(&engine, drop_owner), 12);
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableMetadata(create_table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                drop_owner,
                LockResource::TableData(create_table_id),
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));
            for &catalog_table_id in drop_table_catalog_write_targets() {
                assert!(has_lock_entry(
                    &engine,
                    drop_owner,
                    LockResource::TableMetadata(catalog_table_id),
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                ));
                assert!(has_lock_entry(
                    &engine,
                    drop_owner,
                    LockResource::TableData(catalog_table_id),
                    LockMode::IntentExclusive,
                    LockDebugEntryState::Granted,
                ));
            }
            let (drop_staged, drop_staged_release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::DropCatalogStaged);
            drop_release.send_async(()).await.unwrap();
            drop_staged.recv_async().await.unwrap();
            assert_eq!(lock_entry_count(&engine, drop_owner), 12);
            assert!(
                debug_snapshot(engine.inner().lock_manager())
                    .entries
                    .into_iter()
                    .filter(|entry| entry.family.session_id() == drop_session_id)
                    .all(|entry| {
                        entry.state == LockDebugEntryState::Granted && entry.pending_owner.is_none()
                    }),
                "accepted DROP must retain only physical held-family diagnostics"
            );
            drop_staged_release.send_async(()).await.unwrap();
            drop_fut.await.unwrap();
        });
    }

    #[test]
    fn test_abandoned_drop_future_after_acceptance_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut purge_blocker_session = engine.new_session().unwrap();
            let purge_blocker = purge_blocker_session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let (root_lease, publish_lease) = begin_checkpoint_publish_for_test(&table);
            let (lifecycle_closed, lifecycle_release) = engine
                .inner()
                .table_ddl_test
                .install_gate(TableDdlTestPhase::DropLifecycleClosed);
            let mut drop_session = engine.new_session().unwrap();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            lifecycle_closed.recv_async().await.unwrap();
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);
            assert_checkpoint_workflow_closed(&table);

            drop(drop_fut);
            assert!(
                engine.inner().poisoner.poison_error().is_none(),
                "dropping the observer must not poison accepted DDL"
            );
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);
            lifecycle_release.send_async(()).await.unwrap();

            let mut lock_session = engine.new_session().unwrap();
            let mut lock_fut = Box::pin(lock_session.lock_table(table_id, TableLockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(publish_lease);
            drop(root_lease);
            drop(table);
            let err = lock_fut.await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            assert!(
                engine.inner().poisoner.poison_error().is_none(),
                "accepted DROP should complete after its observer is dropped"
            );
            purge_blocker.rollback().await.unwrap();
            drop(drop_session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_drop_table_normally_grants_waiting_transaction_table_lock() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let (root_lease, publish_lease) = begin_checkpoint_publish_for_test(&table);
            let mut drop_session = engine.new_session().unwrap();
            let drop_session_id = drop_session.id();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            let drop_owner = ddl_lock_owner(
                &engine,
                drop_session_id,
                LockResource::TableMetadata(table_id),
            )
            .expect("drop DDL owner should hold metadata X");
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
            let mut lock_fut = Box::pin(trx.lock_table(table_id, TableLockMode::Exclusive));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                lock_owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(root_lease);
            drop(publish_lease);
            drop(table);
            drop_fut.await.unwrap();
            let err = lock_fut.await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
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
            let table = table_for_internal_assertion(&engine, table_id);
            let mut horizon_session = engine.new_session().unwrap();
            let horizon = horizon_session.begin_trx().unwrap();
            let mut drop_session = engine.new_session().unwrap();
            drop_session.drop_table(table_id).await.unwrap();
            assert_dropped_table_runtime(engine.inner().core.catalog(), table_id);

            let mut lock_session = engine.new_session().unwrap();
            let session_owner = LockOwner::session_explicit(lock_session.id());
            let err = lock_session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
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

            for err in [
                lock_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap_err(),
                lock_session.checkpoint_table(table_id).await.unwrap_err(),
            ] {
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::TableNotFound)
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
            }
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropped);

            let mut trx_session = engine.new_session().unwrap();
            let mut trx = trx_session.begin_trx().unwrap();
            let trx_owner = trx_tests::lock_owner(&trx).unwrap();
            let err = trx
                .lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
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
            drop(table);
            horizon.rollback().await.unwrap();
            wait_for_dropped_table_floor(&engine, table_id).await;
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
            let session_id = session.id();

            assert!(Path::new(&table_file_path).exists());
            session.drop_table(table_id).await.unwrap();

            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_ddl_lock_resource(
                &engine,
                session_id,
                LockResource::TableData(table_id),
            ));
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(table_id)
                    .await
                    .is_none()
            );
            assert!(
                engine
                    .inner()
                    .core
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
                    .inner()
                    .core
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
                    .inner()
                    .core
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
                    .inner()
                    .core
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
                    .inner()
                    .core
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
                    .inner()
                    .core
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
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );

            let mut stale_read = session.begin_trx().unwrap();
            let err = trx_select_row_mvcc_by_id(&mut stale_read, table_id, &single_key(1), &[0, 1])
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            assert_eq!(stale_read.commit().await.unwrap(), TrxID::new(0));

            let mut stale_write = session.begin_trx().unwrap();
            let err = stale_write
                .table_insert_mvcc(table_id, vec![Val::from(2), Val::from("blocked")])
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
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
    fn test_drop_table_first_eligible_purge_destroys_runtime_without_restore() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                Engine::bootstrap(lightweight_test_engine_config(main_dir, "drop_gc_destroy"))
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
            let (event_tx, event_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(event_tx);

            session.drop_table(table_id).await.unwrap();
            let drop_cts = session.last_cts();
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .retained_dropped_table_ids_now(),
                vec![table_id]
            );
            session.wait_for_gc_horizon_after(drop_cts).await.unwrap();
            request_and_wait_for_purge_cycle(&engine, &event_rx).await;
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id)
                    .is_none()
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .user_table_history_version_count(table_id),
                None
            );
            assert_dropped_table_floor(engine.inner().core.catalog(), table_id);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table_now(table_id)
                    .is_none()
            );
            assert!(Path::new(&table_file_path).exists());

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            wait_for_no_dropped_table_operational_state(&engine, table_id).await;
            assert!(!Path::new(&table_file_path).exists());
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .retained_dropped_table_ids_now()
                    .is_empty()
            );
            assert_no_dropped_table_operational_state(engine.inner().core.catalog(), table_id);
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_visible(table_id, MAX_SNAPSHOT_TS)
                    .is_none()
            );

            let mut trx = session.begin_trx().unwrap();
            let key = single_key(11);
            let err = trx
                .table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_table_commit_poison_preserves_source_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let before = table_ddl_snapshot(&engine, table_id, &table);
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
                err.report().downcast_ref::<IoError>().map(|_| ()),
                Some(()),
                "{report}"
            );
            assert!(
                report.contains("drop table failed after lifecycle gate: table_id="),
                "{report}"
            );
            assert!(report.contains("operation=commit"), "{report}");
            assert!(
                report.contains("operation=commit_catalog_ddl, phase=wait_redo_group"),
                "{report}"
            );
            assert!(!report.contains("propagate from other threads"), "{report}");
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
            assert_table_logical_snapshot_unchanged(&before, &engine, table_id);
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropping);
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
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
            );
            assert_eq!(active_operation_count(&engine.inner().session_registry), 0);
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
            reader_trx
                .table_scan_mvcc(table_id, &[0], |_| true)
                .await
                .unwrap();

            let mut drop_session = engine.new_session().unwrap();
            let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));

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
            writer_trx
                .table_insert_mvcc(table_id, vec![Val::from(91), Val::from("writer")])
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
            let trx_sys = &engine.inner().trx_sys;
            let batch = engine
                .inner()
                .core
                .catalog()
                .scan_checkpoint_batch(
                    trx_sys.persisted_watermark_cts(),
                    trx_sys.catalog_checkpoint_scan_config().unwrap(),
                )
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
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "drop_recover_absence",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session.create_table(table_spec, index_specs).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
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
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
                    .catalog_replay_start_ts
                    > TrxID::new(1)
            );
            wait_path_exists(&table_file_path, false).await;

            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "drop_recover_absence",
            ))
            .await
            .unwrap();
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(table_id)
                    .await
                    .is_none()
            );
            assert!(!Path::new(&table_file_path).exists());
        });
    }
}
