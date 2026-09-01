use crate::buffer::PoolGuards;
#[cfg(test)]
use crate::catalog::spec::ActiveIndexSpec;
use crate::catalog::spec::{
    IndexOrder, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexSpec,
};
use crate::catalog::{
    Catalog, ColumnID, ColumnOrdinal, ID_DOMAIN_END, IndexID, IndexRef, IndexSlot,
    catalog_table_id_from_slot,
};
use crate::component::EnginePools;
use crate::engine::EngineCore;
use crate::error::{
    CompletionErrorBridge, CompletionResult, DataIntegrityError, DataIntegrityResult, FatalError,
    FatalResult, InternalError, InternalResult, IoResult, OperationError, OperationOrRuntimeResult,
    OperationResult, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::fs::FileSystem;
use crate::file::table_file::{MutableTableFile, TableFile};
use crate::id::{BlockID, TableID, TrxID};
use crate::index::BlockIndex;
use crate::map::{FastHashMap, FastHashSet};
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
use std::any::Any;
use std::mem;
use std::num::NonZeroU64;
use std::ops::Index;
use std::result::Result as StdResult;
use std::sync::Arc;
#[cfg(test)]
use tests::{CreateTableTestFailure, TableDdlTestPhase};

const CREATE_TABLE_CATALOG_WRITE_TARGETS: [TableID; 3] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(1),
    catalog_table_id_from_slot(2),
];
const DROP_TABLE_CATALOG_WRITE_TARGETS: [TableID; 6] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(1),
    catalog_table_id_from_slot(2),
    catalog_table_id_from_slot(3),
    catalog_table_id_from_slot(4),
    catalog_table_id_from_slot(5),
];

/// Authoritative identities finalized by a successful CREATE TABLE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableOutcome {
    table_id: TableID,
    index_ids: Box<[IndexID]>,
}

impl CreateTableOutcome {
    /// Returns the newly created table identity.
    #[inline]
    pub const fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns finalized initial index identities in input-definition order.
    #[inline]
    pub fn index_ids(&self) -> &[IndexID] {
        &self.index_ids
    }

    /// Consumes the outcome into its table and initial-index identities.
    #[inline]
    pub fn into_parts(self) -> (TableID, Box<[IndexID]>) {
        (self.table_id, self.index_ids)
    }
}

/// Purely validated public CREATE TABLE input.
pub(crate) struct ValidatedCreateTable {
    metadata: Arc<TableMetadata>,
}

impl ValidatedCreateTable {
    /// Validate public metadata before reserving a session operation or table id.
    #[inline]
    pub(crate) fn try_new(
        table_spec: super::StorageTableSpec,
        index_specs: Vec<StorageIndexSpec>,
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
        let index_ids = self
            .metadata
            .idx
            .active_indexes()
            .map(|(_, index)| index.index.id())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        CreateTablePlan {
            table_id,
            metadata: self.metadata,
            outcome: CreateTableOutcome {
                table_id,
                index_ids,
            },
        }
    }
}

/// Owned CREATE TABLE execution plan transferred across mandatory acceptance.
pub(crate) struct CreateTablePlan {
    table_id: TableID,
    metadata: Arc<TableMetadata>,
    outcome: CreateTableOutcome,
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

/// Canonical metadata for one physical storage column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableColumnMetadata {
    /// Stable table-local column identity.
    pub(crate) id: ColumnID,
    /// Physical position in the stored row layout.
    pub(crate) ordinal: ColumnOrdinal,
    /// Stored value kind.
    pub(crate) value_kind: ValKind,
    /// Validated storage-column flags.
    pub(crate) flags: StorageColumnFlags,
}

/// Canonical validated key metadata shared by persistence and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableIndexKeySpec {
    /// Stable identity of the referenced column.
    pub(crate) column_id: ColumnID,
    /// Compiled physical ordinal of the referenced column.
    pub(crate) column_ordinal: ColumnOrdinal,
    /// Logical index-key ordering.
    pub(crate) order: IndexOrder,
}

/// Canonical metadata for one active exact index generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableIndexMetadata {
    /// Exact stable generation and physical slot.
    pub(crate) index: IndexRef,
    /// Validated index flags.
    pub(crate) flags: StorageIndexFlags,
    /// Ordered canonical index-key definition.
    pub(crate) keys: Box<[TableIndexKeySpec]>,
}

impl TableIndexMetadata {
    /// Returns whether this index enforces uniqueness.
    #[inline]
    pub(crate) fn unique(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK) || self.flags.contains(StorageIndexFlags::UK)
    }

    /// Returns whether this index is the table primary key.
    #[inline]
    pub(crate) fn primary_key(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK)
    }
}

/// Persisted root state of one active secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecondaryIndexRoot {
    /// The active index has no persisted DiskTree nodes.
    Empty,
    /// The active index is rooted at this nonzero table-file block.
    Present(NonZeroU64),
}

impl SecondaryIndexRoot {
    /// Converts the storage root state into an optional physical block.
    #[inline]
    pub(crate) fn block_id(self) -> Option<BlockID> {
        match self {
            Self::Empty => None,
            Self::Present(block_id) => Some(BlockID::new(block_id.get())),
        }
    }

    /// Converts an optional physical root into its persisted representation.
    ///
    /// A present root must identify a real DiskTree node rather than the table
    /// file's super block.
    #[inline]
    pub(crate) fn from_block_id(block_id: Option<BlockID>) -> Self {
        match block_id {
            None => Self::Empty,
            Some(block_id) => Self::Present(
                NonZeroU64::new(block_id.as_u64())
                    .expect("present secondary index root is the table-file super block"),
            ),
        }
    }
}

/// Persisted state of one physical table-file index slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecondaryIndexSlot {
    /// The durable root covers this slot, but no index generation was durably
    /// published in it.
    ///
    /// This can happen when recovery reserves a slot for a replay-visible
    /// CREATE whose table-root publication failed, and a later CREATE skips
    /// that reservation and durably extends the slot array. The crossed slot
    /// is persisted as vacant while the earlier CREATE remains reserved by the
    /// recovery overlay until its redo is checkpoint-covered. A failed root
    /// publication alone does not persist a vacant slot; without a later
    /// extension, the slot remains outside the durable slot count.
    Vacant,
    /// The slot currently owns this exact stable identity and DiskTree root.
    Active {
        /// Stable identity of the active generation.
        index_id: IndexID,
        /// Explicit empty or present DiskTree root state.
        root: SecondaryIndexRoot,
    },
    /// This exact stable identity was dropped and remains retired.
    Retired(IndexID),
}

impl SecondaryIndexSlot {
    /// Returns the stable identity carried by an active or retired generation.
    #[inline]
    pub(crate) fn index_id(self) -> Option<IndexID> {
        match self {
            Self::Vacant => None,
            Self::Active { index_id, .. } | Self::Retired(index_id) => Some(index_id),
        }
    }

    /// Returns the root of an active generation.
    #[inline]
    pub(crate) fn active_root(self) -> Option<SecondaryIndexRoot> {
        match self {
            Self::Active { root, .. } => Some(root),
            Self::Vacant | Self::Retired(_) => None,
        }
    }
}

/// Sparse active secondary-index metadata keyed by physical slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexSpecs {
    slots: Vec<Option<TableIndexMetadata>>,
    active_count: usize,
}

impl IndexSpecs {
    /// Returns the sparse physical slot count.
    #[inline]
    fn new(slots: Vec<Option<TableIndexMetadata>>) -> Self {
        let active_count = slots.iter().flatten().count();
        Self {
            slots,
            active_count,
        }
    }

    /// Returns the number of active index generations.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Returns the number of active index definitions.
    #[inline]
    pub(crate) fn active_count(&self) -> usize {
        self.active_count
    }

    /// Iterates active definitions with their physical slots.
    #[inline]
    pub(crate) fn active_indexes(&self) -> impl Iterator<Item = (IndexSlot, &TableIndexMetadata)> {
        self.slots.iter().enumerate().filter_map(|(slot_no, spec)| {
            spec.as_ref().map(|spec| {
                let slot = IndexSlot::try_from(slot_no)
                    .expect("validated table metadata slot is representable");
                (slot, spec)
            })
        })
    }

    /// Iterates active definitions without their slots.
    #[inline]
    pub(crate) fn values(&self) -> impl Iterator<Item = &TableIndexMetadata> {
        self.slots.iter().flatten()
    }

    /// Returns one active definition by physical slot.
    #[inline]
    pub(crate) fn get(&self, index_slot: IndexSlot) -> Option<&TableIndexMetadata> {
        self.slots
            .get(index_slot.as_usize())
            .and_then(Option::as_ref)
    }
}

impl Index<IndexSlot> for IndexSpecs {
    type Output = TableIndexMetadata;

    #[inline]
    fn index(&self, index_slot: IndexSlot) -> &Self::Output {
        self.get(index_slot).unwrap_or_else(|| {
            panic!(
                "active index spec missing: index_slot={index_slot}, slot_count={}",
                self.len()
            )
        })
    }
}

/// Immutable physical column layout and stable-ID translation map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableColumnLayout {
    next_column_id: u64,
    columns: Box<[TableColumnMetadata]>,
    ordinal_by_id: FastHashMap<ColumnID, ColumnOrdinal>,
    /// Runtime value types in physical ordinal order.
    pub(crate) col_types: Vec<ValType>,
    fix_len: usize,
    var_cols: Vec<usize>,
    nullable_cols: usize,
    null_scan_sums: Vec<usize>,
}

impl TableColumnLayout {
    /// Returns the exclusive stable column-ID allocator bound.
    #[inline]
    fn build(
        next_column_id: u64,
        mut columns: Vec<TableColumnMetadata>,
    ) -> StdResult<Self, String> {
        validate_next_id(next_column_id, "next_column_id")?;
        if columns.is_empty() {
            return Err("table column layout requires columns".to_owned());
        }
        if columns.len() > usize::from(u16::MAX) + 1 {
            return Err("column count exceeds physical ordinal domain".to_owned());
        }
        columns.sort_unstable_by_key(|column| column.ordinal);
        let mut ordinal_by_id = FastHashMap::default();
        let mut col_types = Vec::with_capacity(columns.len());
        let mut fix_len = 0usize;
        let mut var_cols = Vec::new();
        let mut nullable_cols = 0usize;
        let mut null_scan_sums = Vec::with_capacity(columns.len());
        for (expected_ordinal, column) in columns.iter().enumerate() {
            let expected = ColumnOrdinal::try_from(expected_ordinal)
                .map_err(|_| "column ordinal exceeds physical domain".to_owned())?;
            if column.ordinal != expected {
                return Err(format!(
                    "column ordinals are not dense: expected={expected}, actual={}",
                    column.ordinal
                ));
            }
            if u64::from(column.id.get()) >= next_column_id {
                return Err(format!(
                    "column id must be below next_column_id: column_id={}, next_column_id={next_column_id}",
                    column.id
                ));
            }
            if ordinal_by_id.insert(column.id, column.ordinal).is_some() {
                return Err(format!("duplicate column id {}", column.id));
            }
            let unknown = column.flags.bits() & !StorageColumnFlags::all().bits();
            if unknown != 0 {
                return Err(format!("unknown column flags: bits={unknown:#x}"));
            }
            let nullable = column.flags.contains(StorageColumnFlags::NULLABLE);
            let ty = ValType {
                kind: column.value_kind,
                nullable,
            };
            null_scan_sums.push(nullable_cols);
            nullable_cols += usize::from(nullable);
            fix_len = fix_len
                .checked_add(ty.kind.inline_len())
                .ok_or_else(|| "column fixed-length sum overflow".to_owned())?;
            if !ty.kind.is_fixed() {
                var_cols.push(expected_ordinal);
            }
            col_types.push(ty);
        }
        Ok(Self {
            next_column_id,
            columns: columns.into_boxed_slice(),
            ordinal_by_id,
            col_types,
            fix_len,
            var_cols,
            nullable_cols,
            null_scan_sums,
        })
    }

    /// Returns the exclusive stable column-ID allocator bound.
    #[inline]
    pub(crate) const fn next_column_id(&self) -> u64 {
        self.next_column_id
    }

    /// Returns canonical columns in physical ordinal order.
    #[inline]
    pub(crate) fn columns(&self) -> &[TableColumnMetadata] {
        &self.columns
    }

    /// Resolves a stable column identity to its physical ordinal.
    #[inline]
    pub(crate) fn ordinal_for_id(&self, id: ColumnID) -> Option<ColumnOrdinal> {
        self.ordinal_by_id.get(&id).copied()
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
    next_index_id: u64,
    index_slot_count: u32,
    index_specs: IndexSpecs,
    slot_by_id: FastHashMap<IndexID, IndexSlot>,
    index_cols: FastHashSet<usize>,
}

impl TableIndexLayout {
    /// Returns the exclusive stable index-ID allocator bound.
    #[inline]
    fn build(
        column_layout: &TableColumnLayout,
        next_index_id: u64,
        index_slot_count: u32,
        index_specs: Vec<TableIndexMetadata>,
    ) -> StdResult<Self, String> {
        validate_next_id(next_index_id, "next_index_id")?;
        if index_slot_count > u32::from(u16::MAX) + 1 {
            return Err(format!(
                "index_slot_count exceeds physical domain: {index_slot_count}"
            ));
        }
        let slot_count = usize::try_from(index_slot_count)
            .map_err(|_| "index_slot_count exceeds usize".to_owned())?;
        let mut slots = vec![None; slot_count];
        let mut slot_by_id = FastHashMap::default();
        let mut index_cols = FastHashSet::default();
        for index_spec in index_specs {
            let index = index_spec.index;
            if u64::from(index.id().get()) >= next_index_id {
                return Err(format!(
                    "index id must be below next_index_id: index={index}, next_index_id={next_index_id}"
                ));
            }
            if index.slot().as_usize() >= slot_count {
                return Err(format!(
                    "index slot outside index_slot_count: index={index}, index_slot_count={index_slot_count}"
                ));
            }
            validate_table_index_metadata(column_layout, &index_spec)?;
            if slot_by_id.insert(index.id(), index.slot()).is_some() {
                return Err(format!("duplicate index id {}", index.id()));
            }
            let slot = &mut slots[index.slot().as_usize()];
            if slot.is_some() {
                return Err(format!("duplicate active index slot {}", index.slot()));
            }
            for key in &index_spec.keys {
                index_cols.insert(key.column_ordinal.as_usize());
            }
            *slot = Some(index_spec);
        }
        let index_specs = IndexSpecs::new(slots);
        validate_primary_key_contract(column_layout, &index_specs)?;
        Ok(Self {
            next_index_id,
            index_slot_count,
            index_specs,
            slot_by_id,
            index_cols,
        })
    }

    /// Returns the exclusive stable index-ID allocator bound.
    #[inline]
    pub(crate) const fn next_index_id(&self) -> u64 {
        self.next_index_id
    }

    /// Returns the exclusive physical index-slot count.
    #[inline]
    pub(crate) const fn index_slot_count_u32(&self) -> u32 {
        self.index_slot_count
    }

    /// Resolves a stable index identity to its exact active generation.
    #[inline]
    pub(crate) fn resolve_index_id(&self, id: IndexID) -> Option<IndexRef> {
        self.slot_by_id
            .get(&id)
            .copied()
            .map(|slot| IndexRef::new(id, slot))
    }

    /// Returns the sparse secondary-index slot count.
    #[inline]
    pub(crate) fn index_slot_count(&self) -> usize {
        self.index_specs.len()
    }

    /// Returns the active secondary-index count.
    #[inline]
    pub(crate) fn active_index_count(&self) -> usize {
        self.index_specs.active_count()
    }

    /// Returns active secondary indexes with their stable slot numbers.
    #[inline]
    pub(crate) fn active_indexes(&self) -> impl Iterator<Item = (IndexSlot, &TableIndexMetadata)> {
        self.index_specs.active_indexes()
    }

    /// Returns one active secondary-index spec by physical slot.
    #[inline]
    pub(crate) fn index_spec(&self, slot: IndexSlot) -> Option<&TableIndexMetadata> {
        self.index_specs.get(slot)
    }

    /// Requires one active secondary-index spec by physical slot.
    #[inline]
    pub(crate) fn require_index_spec(
        &self,
        slot: IndexSlot,
    ) -> InternalResult<&TableIndexMetadata> {
        self.index_spec(slot).ok_or_else(|| {
            Report::new(InternalError::SecondaryIndexOutOfBounds).attach(format!(
                "index_slot={slot}, index_slot_count={}",
                self.index_slot_count()
            ))
        })
    }

    /// Returns the primary-key index slot and spec when this table has one.
    #[inline]
    pub(crate) fn primary_key_index(&self) -> Option<(IndexSlot, &TableIndexMetadata)> {
        self.active_indexes()
            .find(|(_, index_spec)| index_spec.primary_key())
    }

    /// Returns sparse secondary-index specs to lower-level test fixtures.
    #[cfg(test)]
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
        slot: IndexSlot,
        vals: &[Val],
    ) -> bool {
        let Some(index) = self.index_spec(slot) else {
            return false;
        };
        if index.keys.len() != vals.len() {
            return false;
        }
        index
            .keys
            .iter()
            .zip(vals)
            .all(|(key, val)| column_layout.col_type_match(key.column_ordinal.as_usize(), val))
    }

    /// Returns index keys of a new row.
    #[inline]
    pub(crate) fn keys_for_insert(&self, row: &[Val]) -> Vec<SelectKey> {
        self.active_indexes()
            .map(|(slot, is)| {
                let vals: Vec<Val> = is
                    .keys
                    .iter()
                    .map(|k| row[k.column_ordinal.as_usize()].clone())
                    .collect();
                SelectKey {
                    index_slot: slot,
                    vals,
                }
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
            .map(|(slot, is)| {
                let vals: Vec<Val> = is
                    .keys
                    .iter()
                    .map(|k| row.val(column_layout, k.column_ordinal.as_usize()))
                    .collect();
                SelectKey {
                    index_slot: slot,
                    vals,
                }
            })
            .collect()
    }

    /// Returns whether key matches given row.
    #[inline]
    pub(crate) fn match_key(&self, slot: IndexSlot, key_vals: &[Val], row: &[Val]) -> bool {
        let Some(keys) = self.index_spec(slot).map(|spec| &spec.keys) else {
            return false;
        };
        if keys.len() != key_vals.len() {
            return false;
        }
        keys.iter()
            .zip(key_vals)
            .all(|(key, val)| &row[key.column_ordinal.as_usize()] == val)
    }
}

/// Borrowed primary-key metadata view with enough context to validate keys.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PrimaryKeySpec<'a> {
    index_slot: IndexSlot,
    index_spec: &'a TableIndexMetadata,
    column_layout: &'a TableColumnLayout,
}

impl<'a> PrimaryKeySpec<'a> {
    /// Returns the physical primary-key index slot.
    #[inline]
    pub(crate) fn index_slot(&self) -> IndexSlot {
        self.index_slot
    }

    /// Returns the primary-key index specification.
    #[inline]
    pub(crate) fn spec(&self) -> &'a TableIndexMetadata {
        self.index_spec
    }

    /// Validates that the input key targets this primary key and matches its
    /// column shape.
    #[inline]
    pub(crate) fn validate_key(
        &self,
        index_slot: IndexSlot,
        key_vals: &[Val],
    ) -> StdResult<(), PrimaryKeyMatchError> {
        if index_slot != self.index_slot {
            return Err(PrimaryKeyMatchError::IndexSlot {
                actual: index_slot,
                expected: self.index_slot,
            });
        }
        if key_vals.len() != self.index_spec.keys.len() {
            return Err(PrimaryKeyMatchError::ValueCount {
                actual: key_vals.len(),
                expected: self.index_spec.keys.len(),
            });
        }
        if !self
            .index_spec
            .keys
            .iter()
            .zip(key_vals)
            .all(|(index_key, val)| {
                self.column_layout
                    .col_type_match(index_key.column_ordinal.as_usize(), val)
            })
        {
            return Err(PrimaryKeyMatchError::Type { index_slot });
        }
        Ok(())
    }
}

/// Why an input [`SelectKey`] does not match a primary-key specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrimaryKeyMatchError {
    IndexSlot {
        actual: IndexSlot,
        expected: IndexSlot,
    },
    ValueCount {
        actual: usize,
        expected: usize,
    },
    Type {
        index_slot: IndexSlot,
    },
}

/// Table metadata including column layout and index layout.
/// Constraints and other advanced configurations are not implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMetadata {
    /// Monotonic active storage-schema epoch.
    pub(crate) storage_epoch: u64,
    /// Physical column layout.
    pub(crate) col: Arc<TableColumnLayout>,
    /// Sparse secondary-index layout.
    pub(crate) idx: TableIndexLayout,
}

impl TableMetadata {
    /// Reconstructs canonical metadata from validated persisted catalog rows.
    pub(crate) fn try_from_persisted_parts(
        storage_epoch: u64,
        next_column_id: u64,
        columns: Vec<TableColumnMetadata>,
        next_index_id: u64,
        index_slot_count: u32,
        indexes: Vec<TableIndexMetadata>,
    ) -> DataIntegrityResult<Self> {
        let columns = TableColumnLayout::build(next_column_id, columns)
            .map_err(|detail| Report::new(DataIntegrityError::InvalidPayload).attach(detail))?;
        Self::build(
            storage_epoch,
            columns,
            next_index_id,
            index_slot_count,
            indexes,
        )
        .map_err(|detail| Report::new(DataIntegrityError::InvalidPayload).attach(detail))
    }

    /// Try to create metadata of a new table.
    #[inline]
    pub(crate) fn try_new(
        column_specs: Vec<StorageColumnSpec>,
        index_specs: Vec<StorageIndexSpec>,
    ) -> OperationResult<Self> {
        let mut next_column_id = 0u64;
        let columns = column_specs
            .into_iter()
            .enumerate()
            .map(|(ordinal, spec)| {
                let ordinal = ColumnOrdinal::try_from(ordinal).map_err(|_| {
                    Report::new(OperationError::InvalidMetadata)
                        .attach("column count exceeds physical ordinal domain")
                })?;
                let id = allocate_column_id(&mut next_column_id)?;
                Ok(TableColumnMetadata {
                    id,
                    ordinal,
                    value_kind: spec.value_kind,
                    flags: spec.flags,
                })
            })
            .collect::<OperationResult<Vec<_>>>()?;
        let column_layout =
            TableColumnLayout::build(next_column_id, columns).map_err(invalid_metadata)?;

        if index_specs.len() > usize::from(u16::MAX) + 1 {
            return Err(invalid_metadata(
                "index count exceeds physical slot domain".to_owned(),
            ));
        }
        let mut next_index_id = 0u64;
        let mut indexes = Vec::with_capacity(index_specs.len());
        for (slot, spec) in index_specs.into_iter().enumerate() {
            let slot = IndexSlot::try_from(slot)
                .map_err(|_| invalid_metadata("index slot overflow".to_owned()))?;
            let id = allocate_index_id(&mut next_index_id)?;
            indexes.push(compile_storage_index_spec(
                &column_layout,
                IndexRef::new(id, slot),
                spec,
            )?);
        }
        let index_slot_count = u32::try_from(indexes.len())
            .map_err(|_| invalid_metadata("index slot count overflow".to_owned()))?;
        Self::build(0, column_layout, next_index_id, index_slot_count, indexes)
            .map_err(invalid_metadata)
    }

    /// Creates metadata from explicit canonical parts for catalog bootstrap and tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_new_with_index_slot_count(
        column_specs: Vec<StorageColumnSpec>,
        index_specs: Vec<ActiveIndexSpec>,
        index_slot_count: impl Into<u32>,
    ) -> OperationResult<Self> {
        let index_slot_count = index_slot_count.into();
        let mut metadata = Self::try_new(column_specs, Vec::new())?;
        let mut next_index_id = 0u64;
        let mut indexes = Vec::with_capacity(index_specs.len());
        for active in index_specs {
            next_index_id = next_index_id.max(u64::from(active.index.id().get()) + 1);
            indexes.push(compile_storage_index_spec(
                &metadata.col,
                active.index,
                active.spec,
            )?);
        }
        metadata.idx =
            TableIndexLayout::build(&metadata.col, next_index_id, index_slot_count, indexes)
                .map_err(invalid_metadata)?;
        Ok(metadata)
    }

    /// Returns the primary-key metadata view when this table has one.
    #[inline]
    pub(crate) fn primary_key(&self) -> Option<PrimaryKeySpec<'_>> {
        self.idx
            .primary_key_index()
            .map(|(index_slot, index_spec)| PrimaryKeySpec {
                index_slot,
                index_spec,
                column_layout: self.col.as_ref(),
            })
    }

    #[inline]
    fn build(
        storage_epoch: u64,
        column_layout: TableColumnLayout,
        next_index_id: u64,
        index_slot_count: u32,
        indexes: Vec<TableIndexMetadata>,
    ) -> StdResult<Self, String> {
        let column_layout = Arc::new(column_layout);
        let index_layout =
            TableIndexLayout::build(&column_layout, next_index_id, index_slot_count, indexes)?;
        Ok(Self {
            storage_epoch,
            col: column_layout,
            idx: index_layout,
        })
    }

    /// Allocates an exact ID and append slot for a new index.
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_with_created_index(
        &self,
        index_spec: StorageIndexSpec,
    ) -> OperationResult<(IndexRef, Self)> {
        self.try_with_created_index_at(
            index_spec,
            self.idx.next_index_id,
            self.idx.index_slot_count,
        )
    }

    /// Allocates at an overlay-qualified ID and append slot.
    pub(crate) fn try_with_created_index_at(
        &self,
        index_spec: StorageIndexSpec,
        effective_next_index_id: u64,
        index_slot: u32,
    ) -> OperationResult<(IndexRef, Self)> {
        validate_next_id(effective_next_index_id, "effective_next_index_id")
            .map_err(invalid_metadata)?;
        if effective_next_index_id < self.idx.next_index_id {
            return Err(invalid_metadata(format!(
                "effective_next_index_id regressed: durable={}, effective={effective_next_index_id}",
                self.idx.next_index_id
            )));
        }
        if index_slot < self.idx.index_slot_count {
            return Err(invalid_metadata(format!(
                "CREATE INDEX slot precedes append bound: slot={index_slot}, durable_count={}",
                self.idx.index_slot_count
            )));
        }
        if index_slot > u32::from(u16::MAX) {
            return Err(invalid_metadata("index slot domain exhausted".to_owned()));
        }
        let mut next_index_id = effective_next_index_id;
        let id = allocate_index_id(&mut next_index_id)?;
        let slot = IndexSlot::new(index_slot as u16);
        let index = IndexRef::new(id, slot);
        let compiled = compile_storage_index_spec(&self.col, index, index_spec)?;
        let mut indexes = self.idx.index_specs.values().cloned().collect::<Vec<_>>();
        indexes.push(compiled);
        let index_slot_count = index_slot
            .checked_add(1)
            .ok_or_else(|| invalid_metadata("index_slot_count overflow".to_owned()))?;
        let storage_epoch = self
            .storage_epoch
            .checked_add(1)
            .ok_or_else(|| invalid_metadata("storage_epoch overflow".to_owned()))?;
        let mut metadata = Self::build(
            storage_epoch,
            (*self.col).clone(),
            next_index_id,
            index_slot_count,
            indexes,
        )
        .map_err(invalid_metadata)?;
        metadata.col = Arc::clone(&self.col);
        Ok((index, metadata))
    }

    /// Returns metadata with one exact active index generation removed.
    #[inline]
    pub(crate) fn without_index(&self, index: IndexRef) -> OperationResult<Self> {
        if self
            .idx
            .index_spec(index.slot())
            .is_none_or(|spec| spec.index != index)
        {
            return Err(Report::new(OperationError::IndexNotFound).attach(format!(
                "inactive or replaced index generation: index={index}"
            )));
        }
        let indexes = self
            .idx
            .index_specs
            .values()
            .filter(|spec| spec.index != index)
            .cloned()
            .collect();
        let storage_epoch = self
            .storage_epoch
            .checked_add(1)
            .ok_or_else(|| invalid_metadata("storage_epoch overflow".to_owned()))?;
        let mut metadata = Self::build(
            storage_epoch,
            (*self.col).clone(),
            self.idx.next_index_id,
            self.idx.index_slot_count,
            indexes,
        )
        .map_err(invalid_metadata)?;
        metadata.col = Arc::clone(&self.col);
        Ok(metadata)
    }

    /// Create a view for serialization.
    #[inline]
    pub(crate) fn ser_view(&self) -> TableBriefMetadataSerView<'_> {
        TableBriefMetadataSerView { metadata: self }
    }

    /// Computes the canonical active storage-schema fingerprint.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the canonical digest is installed now for the descriptor phase consumer"
        )
    )]
    pub(crate) fn storage_schema_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"doradb.storage-schema\0");
        hasher.update(&[1]);
        hasher.update(&(self.col.col_count() as u32).to_le_bytes());
        for column in self.col.columns() {
            hasher.update(&column.id.get().to_le_bytes());
            hasher.update(&column.ordinal.get().to_le_bytes());
            hasher.update(&(column.value_kind as u32).to_le_bytes());
            hasher.update(&column.flags.bits().to_le_bytes());
        }
        hasher.update(&(self.idx.active_index_count() as u32).to_le_bytes());
        let mut indexes = self.idx.index_specs.values().collect::<Vec<_>>();
        indexes.sort_unstable_by_key(|spec| spec.index.id());
        for index in indexes {
            hasher.update(&index.index.id().get().to_le_bytes());
            hasher.update(&index.index.slot().get().to_le_bytes());
            hasher.update(&index.flags.bits().to_le_bytes());
            hasher.update(&(index.keys.len() as u16).to_le_bytes());
            for key in &index.keys {
                hasher.update(&key.column_id.get().to_le_bytes());
                hasher.update(&[key.order as u8]);
            }
        }
        *hasher.finalize().as_bytes()
    }
}

/// View of necessary information to recover table
/// metadata.
/// It's used for serialization.
pub(crate) struct TableBriefMetadataSerView<'a> {
    /// Canonical metadata being serialized.
    pub(crate) metadata: &'a TableMetadata,
}

impl<'a> Ser<'a> for TableBriefMetadataSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() * 3
            + mem::size_of::<u32>() * 3
            + self.metadata.col.col_count() * (mem::size_of::<u32>() * 3 + mem::size_of::<u16>())
            + self
                .metadata
                .idx
                .index_specs
                .values()
                .map(|index| {
                    mem::size_of::<u32>() * 2
                        + mem::size_of::<u16>() * 2
                        + index.keys.len() * (mem::size_of::<u32>() + mem::size_of::<u8>())
                })
                .sum::<usize>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let metadata = self.metadata;
        let mut idx = out.ser_u64(start_idx, metadata.storage_epoch);
        idx = out.ser_u64(idx, metadata.col.next_column_id);
        idx = out.ser_u32(idx, metadata.col.col_count() as u32);
        for column in metadata.col.columns() {
            idx = out.ser_u32(idx, column.id.get());
            idx = out.ser_u16(idx, column.ordinal.get());
            idx = out.ser_u32(idx, column.value_kind as u32);
            idx = out.ser_u32(idx, column.flags.bits());
        }
        idx = out.ser_u64(idx, metadata.idx.next_index_id);
        idx = out.ser_u32(idx, metadata.idx.index_slot_count);
        idx = out.ser_u32(idx, metadata.idx.active_index_count() as u32);
        for index in metadata.idx.index_specs.values() {
            idx = out.ser_u32(idx, index.index.id().get());
            idx = out.ser_u16(idx, index.index.slot().get());
            idx = out.ser_u32(idx, index.flags.bits());
            idx = out.ser_u16(idx, index.keys.len() as u16);
            for key in &index.keys {
                idx = out.ser_u32(idx, key.column_id.get());
                idx = out.ser_u8(idx, key.order as u8);
            }
        }
        idx
    }
}

/// Brief metadata of a table.
/// It's used as a deserialization container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableBriefMetadata {
    /// Canonical metadata reconstructed from the persisted payload.
    pub(crate) metadata: TableMetadata,
}

impl Deser for TableBriefMetadata {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u64>() * 3 + mem::size_of::<u32>() * 3);

    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (mut idx, storage_epoch) = input.deser_u64(start_idx)?;
        let (next_idx, next_column_id) = input.deser_u64(idx)?;
        idx = next_idx;
        let (next_idx, column_count) = input.deser_u32(idx)?;
        idx = next_idx;
        if column_count == 0 || column_count > u32::from(u16::MAX) + 1 {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid table column count {column_count}")));
        }
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let (next_idx, id) = input.deser_u32(idx)?;
            let (next_idx, ordinal) = input.deser_u16(next_idx)?;
            let (next_idx, value_kind) = input.deser_u32(next_idx)?;
            let (next_idx, flags) = input.deser_u32(next_idx)?;
            idx = next_idx;
            let value_kind = ValKind::try_from(value_kind).map_err(|_| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("unknown table column value kind {value_kind}"))
            })?;
            let flags = StorageColumnFlags::from_bits(flags).ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("unknown table column flags {flags:#x}"))
            })?;
            columns.push(TableColumnMetadata {
                id: ColumnID::new(id),
                ordinal: ColumnOrdinal::new(ordinal),
                value_kind,
                flags,
            });
        }
        let column_layout = TableColumnLayout::build(next_column_id, columns)
            .map_err(|detail| Report::new(DataIntegrityError::InvalidPayload).attach(detail))?;
        let (next_idx, next_index_id) = input.deser_u64(idx)?;
        let (next_idx, index_slot_count) = input.deser_u32(next_idx)?;
        let (next_idx, active_index_count) = input.deser_u32(next_idx)?;
        idx = next_idx;
        if active_index_count > index_slot_count {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("active index count exceeds index slot count"));
        }
        let mut indexes = Vec::with_capacity(active_index_count as usize);
        for _ in 0..active_index_count {
            let (next_idx, id) = input.deser_u32(idx)?;
            let (next_idx, slot) = input.deser_u16(next_idx)?;
            let (next_idx, flags) = input.deser_u32(next_idx)?;
            let (next_idx, key_count) = input.deser_u16(next_idx)?;
            idx = next_idx;
            if key_count == 0 {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach("persisted index has no keys"));
            }
            let flags = StorageIndexFlags::from_bits(flags).ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("unknown table index flags {flags:#x}"))
            })?;
            let mut keys = Vec::with_capacity(key_count as usize);
            for _ in 0..key_count {
                let (next_idx, column_id) = input.deser_u32(idx)?;
                let (next_idx, order) = input.deser_u8(next_idx)?;
                idx = next_idx;
                let column_id = ColumnID::new(column_id);
                let column_ordinal = column_layout.ordinal_for_id(column_id).ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "index key references missing column id {column_id}"
                    ))
                })?;
                let order = IndexOrder::try_from(order).map_err(|()| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("unknown index key order {order}"))
                })?;
                keys.push(TableIndexKeySpec {
                    column_id,
                    column_ordinal,
                    order,
                });
            }
            indexes.push(TableIndexMetadata {
                index: IndexRef::new(IndexID::new(id), IndexSlot::new(slot)),
                flags,
                keys: keys.into_boxed_slice(),
            });
        }
        let metadata = TableMetadata::build(
            storage_epoch,
            column_layout,
            next_index_id,
            index_slot_count,
            indexes,
        )
        .map_err(|detail| Report::new(DataIntegrityError::InvalidPayload).attach(detail))?;
        Ok((idx, TableBriefMetadata { metadata }))
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
    type Output = CreateTableOutcome;
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
    type Output = CreateTableOutcome;

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
    async fn execute_inner(&mut self) -> CompletionResult<CreateTableOutcome> {
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

        Ok(progress.plan.outcome.clone())
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
    replay_floor: Option<TableRedoReplayFloor>,
}

impl DropTableProgress {
    #[inline]
    fn new(plan: DropTablePlan) -> Self {
        Self {
            plan,
            phase: DropTablePhase::Prepared,
            trx: None,
            replay_floor: None,
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

        progress.replay_floor =
            Some(engine.catalog().effective_user_table_redo_replay_floor(
                table_id,
                table.redo_replay_floor_snapshot(),
            ));

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

        let replay_floor = progress.replay_floor.unwrap_or_else(|| {
            panic!("DROP must capture replay floor before catalog deletion: table_id={table_id}")
        });
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
    if table_id.is_user() {
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
    index_spec: &StorageIndexSpec,
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
    index_specs: &[StorageIndexSpec],
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
fn invalid_metadata(detail: String) -> Report<OperationError> {
    Report::new(OperationError::InvalidMetadata).attach(detail)
}

#[inline]
fn validate_next_id(next_id: u64, field: &'static str) -> StdResult<(), String> {
    if next_id > ID_DOMAIN_END {
        return Err(format!(
            "{field} exceeds stable identity domain: value={next_id}, max={ID_DOMAIN_END}"
        ));
    }
    Ok(())
}

#[inline]
fn allocate_column_id(next_id: &mut u64) -> OperationResult<ColumnID> {
    validate_next_id(*next_id, "next_column_id").map_err(invalid_metadata)?;
    if *next_id == ID_DOMAIN_END {
        return Err(Report::new(OperationError::ColumnIdExhausted));
    }
    let allocated = ColumnID::new(*next_id as u32);
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| invalid_metadata("next_column_id arithmetic overflow".to_owned()))?;
    Ok(allocated)
}

#[inline]
fn allocate_index_id(next_id: &mut u64) -> OperationResult<IndexID> {
    validate_next_id(*next_id, "next_index_id").map_err(invalid_metadata)?;
    if *next_id == ID_DOMAIN_END {
        return Err(Report::new(OperationError::IndexIdExhausted));
    }
    let allocated = IndexID::new(*next_id as u32);
    *next_id = next_id
        .checked_add(1)
        .ok_or_else(|| invalid_metadata("next_index_id arithmetic overflow".to_owned()))?;
    Ok(allocated)
}

fn compile_storage_index_spec(
    columns: &TableColumnLayout,
    index: IndexRef,
    spec: StorageIndexSpec,
) -> OperationResult<TableIndexMetadata> {
    let unknown = spec.flags.bits() & !StorageIndexFlags::all().bits();
    if unknown != 0 {
        return Err(invalid_metadata(format!(
            "index {index} has unknown flags: bits={unknown:#x}"
        )));
    }
    if spec.keys.is_empty() {
        return Err(invalid_metadata(format!(
            "index {index} has no key columns"
        )));
    }
    if spec.keys.len() > usize::from(u16::MAX) {
        return Err(invalid_metadata(format!(
            "index {index} key count exceeds persisted u16 domain"
        )));
    }
    if 3usize
        .checked_add(spec.keys.len().saturating_mul(5))
        .is_none_or(|len| len > usize::from(u16::MAX))
    {
        return Err(invalid_metadata(format!(
            "index {index} key specification exceeds VarByte payload limit"
        )));
    }
    let mut seen = FastHashSet::default();
    let mut keys = Vec::with_capacity(spec.keys.len());
    for key in spec.keys {
        let ordinal = key.column_ordinal;
        let Some(column) = columns.columns().get(ordinal.as_usize()) else {
            return Err(invalid_metadata(format!(
                "index {index} references column ordinal {ordinal} outside column count {}",
                columns.col_count()
            )));
        };
        if !seen.insert(ordinal) {
            return Err(invalid_metadata(format!(
                "index {index} repeats column ordinal {ordinal}"
            )));
        }
        keys.push(TableIndexKeySpec {
            column_id: column.id,
            column_ordinal: ordinal,
            order: key.order,
        });
    }
    Ok(TableIndexMetadata {
        index,
        flags: spec.flags,
        keys: keys.into_boxed_slice(),
    })
}

fn validate_table_index_metadata(
    columns: &TableColumnLayout,
    spec: &TableIndexMetadata,
) -> StdResult<(), String> {
    let unknown = spec.flags.bits() & !StorageIndexFlags::all().bits();
    if unknown != 0 {
        return Err(format!(
            "index {} has unknown flags: bits={unknown:#x}",
            spec.index
        ));
    }
    if spec.keys.is_empty() {
        return Err(format!("index {} has no key columns", spec.index));
    }
    if 3usize
        .checked_add(spec.keys.len().saturating_mul(5))
        .is_none_or(|len| len > usize::from(u16::MAX))
    {
        return Err(format!(
            "index {} key specification exceeds VarByte payload limit",
            spec.index
        ));
    }
    let mut seen = FastHashSet::default();
    for key in &spec.keys {
        let Some(expected_ordinal) = columns.ordinal_for_id(key.column_id) else {
            return Err(format!(
                "index {} references missing column id {}",
                spec.index, key.column_id
            ));
        };
        if expected_ordinal != key.column_ordinal {
            return Err(format!(
                "index {} key translation mismatch: column_id={}, expected_ordinal={expected_ordinal}, actual_ordinal={}",
                spec.index, key.column_id, key.column_ordinal
            ));
        }
        if !seen.insert(key.column_id) {
            return Err(format!(
                "index {} repeats column id {}",
                spec.index, key.column_id
            ));
        }
    }
    Ok(())
}

#[inline]
fn validate_primary_key_contract(
    column_layout: &TableColumnLayout,
    index_specs: &IndexSpecs,
) -> StdResult<(), String> {
    let mut primary_key_index_slot = None;
    for (index_slot, index_spec) in index_specs.active_indexes() {
        if !index_spec.primary_key() {
            continue;
        }
        if let Some(existing_index_slot) = primary_key_index_slot {
            return Err(format!(
                "multiple primary keys: index_slot {existing_index_slot} and index_slot {index_slot}"
            ));
        }
        for key in &index_spec.keys {
            let col_no = key.column_ordinal.as_usize();
            if column_layout.nullable(col_no) {
                return Err(format!(
                    "primary key index_slot {index_slot} references nullable column {col_no}"
                ));
            }
        }
        primary_key_index_slot = Some(index_slot);
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
        CatalogCheckpointScanStopReason, CurrentTableState, StorageColumnFlags, StorageColumnSpec,
        StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec, TableMetadata,
    };
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, DiscloseError, Error, ErrorKind, FatalError, IoError, LifecycleError,
        OperationError, RuntimeError,
    };
    use crate::id::{SessionID, TrxID};
    use crate::io::install_storage_backend_test_hook;
    use crate::lock::tests::{LockDebugEntryState, TestLockOwner, debug_snapshot};
    use crate::lock::{LockMode, LockOwner, LockResource, TableLockMode};
    use crate::log::redo::DDLRedo;
    use crate::row::ops::ScanRowDecision;
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
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
            ],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::PK,
            )],
        )
        .expect("valid table metadata");

        let ser_view = metadata.ser_view();

        let len = ser_view.ser_len();
        let mut vec = vec![0u8; len];
        let idx = ser_view.ser(&mut vec[..], 0);
        assert_eq!(idx, vec.len());
        let (idx, brief) = TableBriefMetadata::deser(&vec[..], 0).unwrap();
        assert_eq!(idx, vec.len());
        assert_eq!(metadata, brief.metadata);
    }

    #[test]
    fn test_table_metadata_dense_indexes_derive_index_slot_count() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
            ],
            vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            ],
        )
        .expect("valid table metadata");
        assert_eq!(metadata.idx.index_slot_count_u32(), 2);
        assert_eq!(metadata.idx.index_slot_count(), 2);
        assert_eq!(metadata.idx.active_index_count(), 2);
        let primary_key = metadata
            .primary_key()
            .expect("metadata should expose primary key index");
        assert_eq!(primary_key.index_slot(), IndexSlot::new(0));
        assert_eq!(primary_key.spec().keys.len(), 1);
        assert_eq!(primary_key.spec().keys[0].column_id, ColumnID::new(0));
    }

    #[test]
    fn test_stable_id_allocators_cover_full_u32_domain() {
        let mut next_column_id = 0;
        assert_eq!(
            allocate_column_id(&mut next_column_id).unwrap(),
            ColumnID::new(0)
        );
        assert_eq!(next_column_id, 1);

        next_column_id = u64::from(u32::MAX);
        assert_eq!(
            allocate_column_id(&mut next_column_id).unwrap(),
            ColumnID::new(u32::MAX)
        );
        assert_eq!(next_column_id, ID_DOMAIN_END);
        let err = allocate_column_id(&mut next_column_id).unwrap_err();
        assert_eq!(*err.current_context(), OperationError::ColumnIdExhausted);

        let mut next_index_id = u64::from(u32::MAX);
        assert_eq!(
            allocate_index_id(&mut next_index_id).unwrap(),
            IndexID::new(u32::MAX)
        );
        assert_eq!(next_index_id, ID_DOMAIN_END);
        let err = allocate_index_id(&mut next_index_id).unwrap_err();
        assert_eq!(*err.current_context(), OperationError::IndexIdExhausted);

        let mut invalid_next_id = ID_DOMAIN_END + 1;
        let err = allocate_index_id(&mut invalid_next_id).unwrap_err();
        assert_eq!(*err.current_context(), OperationError::InvalidMetadata);
    }

    #[test]
    fn test_canonical_metadata_rejects_allocator_and_slot_boundary_violations() {
        let column = TableColumnMetadata {
            id: ColumnID::new(0),
            ordinal: ColumnOrdinal::new(0),
            value_kind: ValKind::U32,
            flags: StorageColumnFlags::empty(),
        };
        let index = TableIndexMetadata {
            index: IndexRef::new(IndexID::new(7), IndexSlot::new(0)),
            flags: StorageIndexFlags::UK,
            keys: vec![TableIndexKeySpec {
                column_id: ColumnID::new(0),
                column_ordinal: ColumnOrdinal::new(0),
                order: IndexOrder::Asc,
            }]
            .into_boxed_slice(),
        };
        assert!(
            TableMetadata::try_from_persisted_parts(0, 1, vec![column], 7, 1, vec![index]).is_err()
        );
        assert!(
            TableMetadata::try_from_persisted_parts(
                0,
                ID_DOMAIN_END + 1,
                vec![column],
                0,
                0,
                vec![],
            )
            .is_err()
        );

        let full_slot_domain = TableMetadata::try_from_persisted_parts(
            0,
            1,
            vec![column],
            0,
            u32::from(u16::MAX) + 1,
            vec![],
        )
        .unwrap();
        assert_eq!(full_slot_domain.idx.index_slot_count_u32(), 65_536);
        let err = full_slot_domain
            .try_with_created_index_at(
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                0,
                65_536,
            )
            .unwrap_err();
        assert_eq!(*err.current_context(), OperationError::InvalidMetadata);
    }

    #[test]
    fn test_storage_epoch_overflow_fails_before_metadata_change() {
        let mut metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U32,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        metadata.storage_epoch = u64::MAX;
        let err = metadata
            .try_with_created_index(StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            ))
            .unwrap_err();
        assert_eq!(*err.current_context(), OperationError::InvalidMetadata);
        assert_eq!(metadata.idx.active_index_count(), 0);
    }

    #[test]
    fn test_storage_schema_fingerprint_is_canonical() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
            ],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey {
                    column_ordinal: ColumnOrdinal::new(1),
                    order: IndexOrder::Desc,
                }],
                StorageIndexFlags::UK,
            )],
        )
        .unwrap();
        let fingerprint = metadata.storage_schema_fingerprint();
        assert_eq!(
            fingerprint,
            [
                93, 98, 157, 121, 231, 186, 186, 42, 170, 226, 50, 231, 4, 188, 50, 164, 167, 7,
                108, 60, 242, 232, 63, 176, 70, 255, 16, 249, 122, 34, 127, 207,
            ]
        );

        let canonical_clone = TableMetadata::try_from_persisted_parts(
            99,
            metadata.col.next_column_id() + 10,
            metadata.col.columns().to_vec(),
            metadata.idx.next_index_id() + 10,
            5,
            metadata
                .idx
                .active_indexes()
                .map(|(_, index)| index.clone())
                .collect(),
        )
        .unwrap();
        assert_eq!(
            fingerprint,
            canonical_clone.storage_schema_fingerprint(),
            "epoch and allocator/slot high-water fields are not part of active schema"
        );

        let reordered = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
            ],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(1)],
                StorageIndexFlags::UK,
            )],
        )
        .unwrap();
        assert_ne!(fingerprint, reordered.storage_schema_fingerprint());
    }

    #[test]
    fn test_primary_key_spec_validates_select_key() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
            ],
            vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
            ],
        )
        .expect("valid table metadata");
        let primary_key = metadata.primary_key().unwrap();

        assert!(
            primary_key
                .validate_key(IndexSlot::new(0), &[Val::from(42u32)])
                .is_ok()
        );
        assert_eq!(
            primary_key.validate_key(IndexSlot::new(1), &[Val::from(42u32)]),
            Err(PrimaryKeyMatchError::IndexSlot {
                actual: IndexSlot::new(1),
                expected: IndexSlot::new(0)
            })
        );
        assert_eq!(
            primary_key.validate_key(IndexSlot::new(0), &[Val::from(42u32), Val::from(99u64)]),
            Err(PrimaryKeyMatchError::ValueCount {
                actual: 2,
                expected: 1
            })
        );
        assert_eq!(
            primary_key.validate_key(IndexSlot::new(0), &[Val::from(42u64)]),
            Err(PrimaryKeyMatchError::Type {
                index_slot: IndexSlot::new(0)
            })
        );
        assert_eq!(
            primary_key.validate_key(IndexSlot::new(0), &[Val::Null]),
            Err(PrimaryKeyMatchError::Type {
                index_slot: IndexSlot::new(0)
            })
        );
        assert!(metadata.idx.index_type_match(
            metadata.col.as_ref(),
            IndexSlot::new(1),
            &[Val::Null]
        ));
    }

    #[test]
    fn test_table_metadata_index_only_changes_share_column_layout() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
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

        let (index, created) = metadata
            .try_with_created_index(StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            ))
            .unwrap();
        let dropped = created.without_index(index).unwrap();

        assert!(Arc::ptr_eq(&metadata.col, &created.col));
        assert!(Arc::ptr_eq(&metadata.col, &dropped.col));
        assert_eq!(created.idx.active_index_count(), 1);
        assert_eq!(dropped.idx.active_index_count(), 0);
    }

    #[test]
    fn test_table_metadata_sparse_active_indexes_preserve_index_slot() {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(2)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(3),
        )
        .unwrap();

        assert_eq!(metadata.idx.index_slot_count_u32(), 3);
        assert_eq!(metadata.idx.index_slot_count(), 3);
        assert!(metadata.idx.index_spec(IndexSlot::new(1)).is_none());
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(index_slot, _)| index_slot.get())
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let keys =
            metadata
                .idx
                .keys_for_insert(&[Val::from(11u32), Val::from(22u64), Val::from(33u32)]);
        assert_eq!(keys[0].index_slot, IndexSlot::new(0));
        assert_eq!(keys[1].index_slot, IndexSlot::new(2));
    }

    #[test]
    fn test_table_metadata_rejects_invalid_index_slots() {
        let columns = vec![StorageColumnSpec::new(
            ValKind::U32,
            StorageColumnFlags::empty(),
        )];
        assert!(
            TableMetadata::try_new_with_index_slot_count(
                columns.clone(),
                vec![ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                )],
                IndexSlot::new(1),
            )
            .is_err()
        );
        assert!(
            TableMetadata::try_new_with_index_slot_count(
                columns.clone(),
                vec![
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                    ),
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(0)],
                            StorageIndexFlags::empty()
                        ),
                    ),
                ],
                IndexSlot::new(1),
            )
            .is_err()
        );
    }

    #[test]
    fn test_table_metadata_rejects_multiple_primary_keys() {
        let columns = vec![
            StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
        ];

        let err = TableMetadata::try_new(
            columns,
            vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::PK),
            ],
        )
        .unwrap_err();

        assert_invalid_metadata(err.disclose(), "multiple primary keys");
    }

    #[test]
    fn test_table_metadata_rejects_sparse_multiple_primary_keys() {
        let columns = vec![
            StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
        ];

        let err = TableMetadata::try_new_with_index_slot_count(
            columns,
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::PK),
                ),
            ],
            IndexSlot::new(3),
        )
        .unwrap_err();

        assert_invalid_metadata(err.disclose(), "multiple primary keys");
    }

    #[test]
    fn test_table_metadata_rejects_nullable_primary_key_column() {
        let err = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U32,
                StorageColumnFlags::NULLABLE,
            )],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::PK,
            )],
        )
        .unwrap_err();

        assert_invalid_metadata(
            err.disclose(),
            "primary key index_slot 0 references nullable column 0",
        );
    }

    #[test]
    fn test_table_metadata_rejects_invalid_index_specs_as_operation_errors() {
        let columns = vec![StorageColumnSpec::new(
            ValKind::U32,
            StorageColumnFlags::empty(),
        )];

        let err = TableMetadata::try_new(
            columns.clone(),
            vec![StorageIndexSpec::new(vec![], StorageIndexFlags::PK)],
        )
        .unwrap_err();
        assert_invalid_metadata(
            err.disclose(),
            "index IndexRef(id=0, slot=0) has no key columns",
        );

        let err = TableMetadata::try_new_with_index_slot_count(
            columns.clone(),
            vec![ActiveIndexSpec::new(
                IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                StorageIndexSpec::new(vec![], StorageIndexFlags::PK),
            )],
            IndexSlot::new(2),
        )
        .unwrap_err();
        assert_invalid_metadata(
            err.disclose(),
            "index IndexRef(id=1, slot=1) has no key columns",
        );

        let err = TableMetadata::try_new(
            columns,
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(1)],
                StorageIndexFlags::PK,
            )],
        )
        .unwrap_err();
        assert_invalid_metadata(
            err.disclose(),
            "index IndexRef(id=0, slot=0) references column ordinal 1 outside column count 1",
        );
    }

    #[test]
    fn test_table_metadata_create_index_allocates_sparse_next_slot() {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(2)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(3),
        )
        .unwrap();

        let (index_slot, metadata) = metadata
            .try_with_created_index(StorageIndexSpec::new(
                vec![StorageIndexKey::new(1)],
                StorageIndexFlags::UK,
            ))
            .unwrap();

        assert_eq!(index_slot.slot(), IndexSlot::new(3));
        assert_eq!(metadata.idx.index_slot_count_u32(), 4);
        assert_eq!(metadata.idx.index_slot_count(), 4);
        assert!(metadata.idx.index_spec(IndexSlot::new(1)).is_none());
        assert!(metadata.idx.index_spec(IndexSlot::new(3)).unwrap().unique());
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(index_slot, _)| index_slot.get())
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn test_table_metadata_create_index_rejects_invalid_spec() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U32,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");

        assert!(
            metadata
                .try_with_created_index(StorageIndexSpec::new(vec![], StorageIndexFlags::UK))
                .is_err()
        );
        assert!(
            metadata
                .try_with_created_index(StorageIndexSpec::new(
                    vec![StorageIndexKey::new(1)],
                    StorageIndexFlags::UK,
                ))
                .is_err()
        );
    }

    #[test]
    fn test_table_metadata_create_index_rejects_next_index_overflow() {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            vec![StorageColumnSpec::new(
                ValKind::U32,
                StorageColumnFlags::empty(),
            )],
            vec![],
            u32::from(u16::MAX) + 1,
        )
        .unwrap();

        assert!(
            metadata
                .try_with_created_index(StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::empty(),
                ))
                .is_err()
        );
    }

    #[test]
    fn test_table_metadata_drop_index_preserves_sparse_allocation() {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            ],
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(2)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(4),
        )
        .unwrap();

        let dropped = metadata
            .without_index(IndexRef::new(IndexID::new(2), IndexSlot::new(2)))
            .unwrap();

        assert_eq!(dropped.idx.index_slot_count_u32(), 4);
        assert_eq!(dropped.idx.index_slot_count(), 4);
        assert_eq!(dropped.idx.active_index_count(), 1);
        assert!(dropped.idx.index_spec(IndexSlot::new(0)).is_some());
        assert!(dropped.idx.index_spec(IndexSlot::new(1)).is_none());
        assert!(dropped.idx.index_spec(IndexSlot::new(2)).is_none());
        assert!(dropped.idx.index_spec(IndexSlot::new(3)).is_none());
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
                .table_lookup_unique_mvcc(
                    crate::TableIndex(table_id, IndexID::new(0)),
                    &key.vals,
                    &[0, 1],
                )
                .await
                .unwrap();
            assert!(selected.is_found());
            let repeated = trx
                .table_lookup_unique_mvcc(
                    crate::TableIndex(table_id, IndexID::new(0)),
                    &key.vals,
                    &[0, 1],
                )
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

            let table_id1 = create1.await.unwrap().table_id();
            let table_id2 = create2.await.unwrap().table_id();
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
                    Path::new(&engine.inner().table_fs.user_table_file_path(table_id)).exists()
                );
            }
        });
    }

    #[test]
    fn test_create_table_outcome_returns_finalized_index_ids_in_input_order() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create-table-outcome").await;
            let mut session = engine.new_session().unwrap();

            let empty = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::U32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![],
                )
                .await
                .unwrap();
            assert!(empty.index_ids().is_empty());

            let outcome = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    ]),
                    vec![
                        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(0)],
                            StorageIndexFlags::empty(),
                        ),
                    ],
                )
                .await
                .unwrap();
            assert_eq!(outcome.index_ids(), [IndexID::new(0), IndexID::new(1)]);
            let table = table_for_internal_assertion(&engine, outcome.table_id());
            let metadata = table.metadata();
            for (input_ordinal, index_id) in outcome.index_ids().iter().copied().enumerate() {
                let index = metadata
                    .idx
                    .resolve_index_id(index_id)
                    .expect("outcome index id must resolve in installed metadata");
                assert_eq!(index.slot().as_usize(), input_ordinal);
            }

            let (table_id, index_ids) = outcome.into_parts();
            assert_eq!(table_id, table.table_id());
            assert_eq!(&*index_ids, [IndexID::new(0), IndexID::new(1)]);
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
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![StorageIndexSpec::new(vec![], StorageIndexFlags::UK)],
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
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::PK,
                    )],
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
                .table_lookup_unique_mvcc(
                    crate::TableIndex(table_id, IndexID::new(0)),
                    &key.vals,
                    &[0, 1],
                )
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
    fn test_drop_table_missing_catalog_row_returns_typed_integrity_and_poisons() {
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
                Some(FatalError::Poisoned)
            );
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            assert_eq!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .map(|error| *error.current_context()),
                Some(FatalError::Poisoned)
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
            assert_eq!(active_operation_count(&engine.inner().session_registry), 0);
            assert!(!drop_session.in_trx().unwrap());
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
                .unwrap()
                .table_id();
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
            assert_eq!(lock_entry_count(&engine, create_owner), 7);
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
            assert_eq!(lock_entry_count(&engine, create_owner), 7);
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
            assert_eq!(create_fut.await.unwrap().table_id(), create_table_id);

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
            assert_eq!(lock_entry_count(&engine, drop_owner), 14);
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
            assert_eq!(lock_entry_count(&engine, drop_owner), 14);
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
                .unwrap()
                .table_id();
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
                .unwrap()
                .table_id();
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
            let table_id = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
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
                .table_lookup_unique_mvcc(
                    crate::TableIndex(table_id, IndexID::new(0)),
                    &key.vals,
                    &[0, 1],
                )
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
            let reader_stream = reader_trx
                .table_scan_mvcc_stream(table_id, &[0], |_| Ok(ScanRowDecision::Include))
                .await
                .unwrap();
            drop(reader_stream);

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
            let table_id = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
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
