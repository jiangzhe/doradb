use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards};
use crate::catalog::{
    Catalog, CatalogDefinitionEffects, IndexID, IndexRef, IndexSlot, SecondaryIndexRoot,
    SecondaryIndexSlot, TableIndexMetadata, TableMetadata, catalog_table_id_from_slot,
};
use crate::engine::EngineCore;
use crate::error::{
    CompletionErrorBridge, CompletionResult, DataIntegrityError, DataIntegrityResult, FatalError,
    OperationError, OperationOrRuntimeResult, OperationResult, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::meta_block::validate_secondary_index_state;
use crate::file::table_file::{ActiveRoot, MutableTableFile};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::disk_tree::{NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedPut};
use crate::index::{
    BTreeKey, BTreeKeyEncoder, ColumnBlockIndex, IndexInsert, NonUniqueMemIndex,
    SecondaryDiskTreeRuntime, SecondaryIndex, UniqueMemIndex,
};
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::row::RowRead;
use crate::runtime::mandatory::{AcceptedExecution, MandatoryTaskMetadata, PreparedExecution};
use crate::runtime::{POLL_BUDGET, yield_now};
use crate::session::{AcceptedDdlScope, PreparedDdlScope};
use crate::table::{
    CreateIndexPlan, DeleteMarker, DropIndexPlan, RuntimeIndexEntry, Table, TableRuntimeLayout,
    secondary_disk_tree_encoder,
};
use crate::trx::{PrivateTransaction, trx_is_committed};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;
#[cfg(test)]
pub(crate) use tests::IndexDdlTestController;
#[cfg(test)]
use tests::{CreateIndexTestFailure, IndexDdlTestPhase};

const CREATE_INDEX_CATALOG_WRITE_TARGETS: [TableID; 3] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(2),
    catalog_table_id_from_slot(3),
];
const DROP_INDEX_CATALOG_WRITE_TARGETS: [TableID; 3] = [
    catalog_table_id_from_slot(0),
    catalog_table_id_from_slot(2),
    catalog_table_id_from_slot(3),
];

/// Index DDL operation kind used for root-publish durability proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlKind {
    /// CREATE INDEX DDL marker.
    Create,
    /// DROP INDEX DDL marker.
    Drop,
}

/// Root-publish proof for one index DDL redo marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlRootProof {
    /// The active table root does not prove the DDL durable.
    Provisional,
    /// The root proves the created index remains active.
    DurableFinalCreate,
    /// The root proves the index number was allocated, but a later root dropped it.
    DurableAllocationOnly,
    /// The root proves the dropped index is inactive and its root slot is empty.
    DurableFinalDrop,
}

/// Generation-qualified index DDL marker known to be visible at replay floor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplayVisibleIndexDdl {
    index: IndexRef,
    cts: TrxID,
}

impl ReplayVisibleIndexDdl {
    /// Qualifies an exact redo generation after replay visibility was established.
    #[inline]
    pub(crate) fn from_replay_visible(
        index: IndexRef,
        cts: TrxID,
        catalog_replay_start_ts: TrxID,
    ) -> Self {
        assert!(
            cts >= catalog_replay_start_ts,
            "index DDL carrier requires replay-visible CTS: cts={cts}, catalog_replay_start_ts={catalog_replay_start_ts}"
        );
        Self { index, cts }
    }

    #[inline]
    fn index(self) -> IndexRef {
        self.index
    }

    #[inline]
    fn cts(self) -> TrxID {
        self.cts
    }
}

/// Transferable admission to the table and catalog metadata-change gates.
pub(crate) struct IndexDdlGateScope {
    table: Arc<Table>,
    catalog: QuiescentGuard<Catalog>,
    table_active: bool,
    catalog_active: bool,
}

impl IndexDdlGateScope {
    /// Acquires table admission first and catalog admission second.
    pub(crate) async fn acquire(
        table: Arc<Table>,
        catalog: QuiescentGuard<Catalog>,
    ) -> OperationResult<Self> {
        table.acquire_index_metadata_change().await?;
        let mut scope = Self {
            table,
            catalog,
            table_active: true,
            catalog_active: false,
        };
        scope.catalog.acquire_index_metadata_change().await;
        scope.catalog_active = true;
        Ok(scope)
    }
}

impl Drop for IndexDdlGateScope {
    #[inline]
    fn drop(&mut self) {
        if self.catalog_active {
            self.catalog_active = false;
            self.catalog.release_index_metadata_change();
        }
        if self.table_active {
            self.table_active = false;
            self.table.release_index_metadata_change();
        }
    }
}

#[derive(Clone, Debug)]
struct CreateIndexRowEntry {
    key: BTreeKey,
    row_id: RowID,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CreateIndexKeyValidator {
    Unique,
    NonUnique,
}

impl CreateIndexKeyValidator {
    #[inline]
    fn new(index_spec: &TableIndexMetadata) -> Self {
        if index_spec.unique() {
            Self::Unique
        } else {
            Self::NonUnique
        }
    }

    fn prepare_cold(self, rows: &mut [CreateIndexRowEntry]) -> OperationResult<()> {
        rows.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if self == Self::NonUnique {
            return Ok(());
        }
        for pair in rows.windows(2) {
            if pair[0].key != pair[1].key {
                continue;
            }
            return Err(Report::new(OperationError::DuplicateKey).attach(format!(
                "create unique index found duplicate cold key: row_id={}",
                pair[1].row_id
            )));
        }
        Ok(())
    }

    fn prepare_hot(
        self,
        rows: &mut [CreateIndexRowEntry],
        cold_rows: &[CreateIndexRowEntry],
    ) -> OperationResult<()> {
        if self == Self::NonUnique {
            return Ok(());
        }
        rows.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if let Some(pair) = rows.windows(2).find(|pair| pair[0].key == pair[1].key) {
            return Err(Report::new(OperationError::DuplicateKey).attach(format!(
                "create unique index found duplicate hot key: row_id={}",
                pair[1].row_id
            )));
        }
        let mut cold_pos = 0;
        for row in rows {
            while cold_pos < cold_rows.len() && cold_rows[cold_pos].key < row.key {
                cold_pos += 1;
            }
            if cold_pos < cold_rows.len() && cold_rows[cold_pos].key == row.key {
                return Err(Report::new(OperationError::DuplicateKey).attach(format!(
                    "create unique index found duplicate cold/hot key: row_id={}",
                    row.row_id
                )));
            }
        }
        Ok(())
    }
}

struct CreateIndexCollector<'a> {
    table: &'a Table,
    guards: &'a PoolGuards,
    layout: &'a TableRuntimeLayout,
    index_spec: &'a TableIndexMetadata,
    key_encoder: BTreeKeyEncoder,
    column_block_index_root: BlockID,
    pivot_row_id: RowID,
}

impl<'a> CreateIndexCollector<'a> {
    #[inline]
    fn new(
        table: &'a Table,
        guards: &'a PoolGuards,
        layout: &'a TableRuntimeLayout,
        index_spec: &'a TableIndexMetadata,
        active_root: &ActiveRoot,
    ) -> Self {
        let column_block_index_root = active_root.column_block_index_root;
        let pivot_row_id = active_root.pivot_row_id;
        assert_create_index_block_index_snapshot(
            table.table_id(),
            (pivot_row_id, column_block_index_root),
            table.mem.blk_idx().column_route_snapshot(),
        );
        let key_encoder =
            secondary_disk_tree_encoder(layout.metadata(), index_spec, !index_spec.unique());
        Self {
            table,
            guards,
            layout,
            index_spec,
            key_encoder,
            column_block_index_root,
            pivot_row_id,
        }
    }

    // Future improvement: stream/batch and parallelize this cold-row build to
    // avoid materializing every persisted row. See docs/backlogs/000104.
    async fn collect_current_cold(&self) -> OperationOrRuntimeResult<Vec<CreateIndexRowEntry>> {
        let table = self.table;
        let guards = self.guards;
        let metadata = self.layout.metadata();
        let index_spec = self.index_spec;
        let column_block_index_root = self.column_block_index_root;
        let pivot_row_id = self.pivot_row_id;
        if !create_index_cold_root_has_rows(column_block_index_root, pivot_row_id) {
            return Ok(Vec::new());
        }
        let disk_guard = guards.disk_guard();
        let column_index = ColumnBlockIndex::new(
            column_block_index_root,
            pivot_row_id,
            table.file().file_kind(),
            table.file().sparse_file(),
            table.disk_pool(),
            disk_guard,
        );
        let read_set = index_spec
            .keys
            .iter()
            .map(|index_key| index_key.column_ordinal.as_usize())
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        let file_kind = table.file().file_kind();
        for entry in column_index.collect_leaf_entries().await? {
            let (delete_deltas, row_ids) =
                column_index.load_delete_deltas_and_row_ids(&entry).await?;
            let block_id = entry.block_id();
            let persisted = table.storage.load_lwc_block(disk_guard, block_id).await?;
            let block = persisted.block();
            if usize::from(entry.row_count()) != row_ids.len() || block.row_count() != row_ids.len()
            {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, create index LWC row count mismatch: entry_rows={}, block_rows={}, row_ids={}",
                    entry.row_count(),
                    block.row_count(),
                    row_ids.len()
                ))
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=create_index, phase=validate_index_build_input")
                .into());
            }
            if block.row_shape_fingerprint() != entry.row_shape_fingerprint() {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, create index LWC row shape mismatch"
                ))
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=create_index, phase=validate_index_build_input")
                .into());
            }

            let mut durable_deleted = BTreeSet::new();
            for delta in delete_deltas {
                let row_id = entry
                    .start_row_id
                    .checked_add(u64::from(delta))
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach(format!(
                                "file={file_kind}, block=lwc_block, block_id={block_id}, create index delete delta overflows row id: start_row_id={}, delta={delta}",
                                entry.start_row_id
                            ))
                            .change_context(RuntimeError::CatalogAccess)
                            .attach("operation=create_index, phase=validate_index_build_input")
                    })?;
                durable_deleted.insert(row_id);
            }

            for (row_idx, row_id) in row_ids.into_iter().enumerate() {
                if durable_deleted.contains(&row_id) {
                    continue;
                }
                if create_index_current_cold_row_is_deleted(table, row_id)? {
                    continue;
                }
                let key_vals = block
                    .decode_row_values(&metadata.col, row_idx, &read_set)
                    .attach_with(|| {
                        format!("file={file_kind}, block=lwc_block, block_id={block_id}")
                    })
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=create_index, phase=decode_cold_row, table_id={}, row_id={row_id}",
                            table.table_id()
                        )
                    })?;
                let key = self.encode_key(&key_vals, row_id);
                rows.push(CreateIndexRowEntry { key, row_id });
            }
            yield_now().await;
        }
        Ok(rows)
    }

    async fn collect_current_hot(&self) -> RuntimeResult<Vec<CreateIndexRowEntry>> {
        let mut rows = Vec::new();
        self.table
            .accessor_with_layout(self.layout)
            .mem_scan_uncommitted_from(self.guards, self.pivot_row_id, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                let row_id = row.row_id();
                let key_vals = self
                    .index_spec
                    .keys
                    .iter()
                    .map(|index_key| row.val(col_layout, index_key.column_ordinal.as_usize()))
                    .collect::<Vec<_>>();
                let key = self.encode_key(&key_vals, row_id);
                rows.push(CreateIndexRowEntry { key, row_id });
                true
            })
            .await?;
        Ok(rows)
    }

    #[inline]
    fn encode_key(&self, key_vals: &[Val], row_id: RowID) -> BTreeKey {
        if self.index_spec.unique() {
            self.key_encoder.encode(key_vals)
        } else {
            self.key_encoder.encode_pair(key_vals, Val::from(row_id))
        }
    }
}

struct CreateIndexRuntimeBuilder<'a> {
    index_pool: QuiescentGuard<EvictableBufferPool>,
    index_guard: &'a PoolGuard,
    metadata: &'a TableMetadata,
    index_spec: &'a TableIndexMetadata,
    build_ts: TrxID,
    #[cfg(test)]
    test: tests::IndexDdlTestController,
}

impl<'a> CreateIndexRuntimeBuilder<'a> {
    #[inline]
    fn new(
        engine: &'a EngineCore,
        guards: &'a PoolGuards,
        metadata: &'a TableMetadata,
        index_spec: &'a TableIndexMetadata,
        build_ts: TrxID,
    ) -> Self {
        Self {
            index_pool: engine.pools.index.clone(),
            index_guard: guards.index_guard(),
            metadata,
            index_spec,
            build_ts,
            #[cfg(test)]
            test: engine.index_ddl_test.clone(),
        }
    }

    async fn build_unique(
        self,
        disk_runtime: SecondaryDiskTreeRuntime,
        hot_rows: Vec<CreateIndexRowEntry>,
    ) -> OperationOrRuntimeResult<SecondaryIndex<EvictableBufferPool>> {
        let Self {
            index_pool,
            index_guard,
            metadata,
            index_spec,
            build_ts,
            #[cfg(test)]
            test,
        } = self;
        let ty_infer = |col_no| metadata.col.col_type(col_no);
        let mem =
            UniqueMemIndex::new(index_pool, index_guard, index_spec, ty_infer, build_ts).await?;
        let insert_res = insert_create_index_unique_hot_rows(
            &mem,
            index_guard,
            &hot_rows,
            build_ts,
            #[cfg(test)]
            &test,
        )
        .await;
        if let Err(err) = insert_res {
            if let Err(report) = mem.destroy(index_guard).await {
                let report = report.attach(format!(
                    "operation=rollback_create_unique_index_build, index_slot={}",
                    disk_runtime.index_slot()
                ));
                obs::error!(
                    "event=index_ddl_cleanup component=catalog_index action=destroy_unpublished result=error error={report:?}"
                );
            }
            return Err(err);
        }
        Ok(SecondaryIndex::Unique {
            mem,
            disk: disk_runtime,
        })
    }

    async fn build_non_unique(
        self,
        disk_runtime: SecondaryDiskTreeRuntime,
        hot_rows: Vec<CreateIndexRowEntry>,
    ) -> OperationOrRuntimeResult<SecondaryIndex<EvictableBufferPool>> {
        #[cfg(test)]
        use tests::CreateIndexTestFailure;

        let Self {
            index_pool,
            index_guard,
            metadata,
            index_spec,
            build_ts,
            #[cfg(test)]
            test,
        } = self;
        let ty_infer = |col_no| metadata.col.col_type(col_no);
        let mem =
            NonUniqueMemIndex::new(index_pool, index_guard, index_spec, ty_infer, build_ts).await?;
        #[cfg(test)]
        let forced_population_failure =
            test.maybe_fail_create(CreateIndexTestFailure::PopulateNonUnique);
        #[cfg(not(test))]
        let forced_population_failure: RuntimeResult<()> = Ok(());
        let insert_res = match forced_population_failure {
            Ok(()) => {
                insert_create_index_non_unique_hot_rows(
                    &mem,
                    index_guard,
                    &hot_rows,
                    build_ts,
                    #[cfg(test)]
                    &test,
                )
                .await
            }
            Err(err) => Err(err),
        };
        if let Err(err) = insert_res {
            if let Err(report) = mem.destroy(index_guard).await {
                let report = report.attach(format!(
                    "operation=rollback_create_non_unique_index_build, index_slot={}",
                    disk_runtime.index_slot()
                ));
                obs::error!(
                    "event=index_ddl_cleanup component=catalog_index action=destroy_unpublished result=error error={report:?}"
                );
            }
            return Err(err.into());
        }
        Ok(SecondaryIndex::NonUnique {
            mem,
            disk: disk_runtime,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateIndexBuildPhase {
    Building,
    RuntimeStaged,
    LayoutStaged,
    CatalogCommitted,
    Installed,
    Aborted,
}

struct CreateIndexProgress {
    table_id: TableID,
    index: IndexRef,
    build_ts: TrxID,
    phase: CreateIndexBuildPhase,
    trx: Option<PrivateTransaction>,
    staged_index: Option<Arc<SecondaryIndex<EvictableBufferPool>>>,
    new_layout: Option<TableRuntimeLayout>,
}

impl CreateIndexProgress {
    #[inline]
    fn new(table_id: TableID, index: IndexRef, trx: PrivateTransaction) -> Self {
        let build_ts = trx.sts();
        Self {
            table_id,
            index,
            build_ts,
            phase: CreateIndexBuildPhase::Building,
            trx: Some(trx),
            staged_index: None,
            new_layout: None,
        }
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }

    #[inline]
    fn build_ts(&self) -> TrxID {
        self.build_ts
    }

    #[inline]
    fn stage_runtime_index(&mut self, index: SecondaryIndex<EvictableBufferPool>) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::Building);
        debug_assert!(self.staged_index.is_none());
        self.staged_index = Some(Arc::new(index));
        self.phase = CreateIndexBuildPhase::RuntimeStaged;
    }

    #[inline]
    fn clone_staged_index_for_layout(&self) -> Arc<SecondaryIndex<EvictableBufferPool>> {
        assert_eq!(
            self.phase,
            CreateIndexBuildPhase::RuntimeStaged,
            "create-index progress invariant violated: staged runtime requested in phase {:?}",
            self.phase
        );
        self.staged_index
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| {
                panic!(
                    "create-index progress invariant violated: staged runtime index is missing, table_id={}, index={}",
                    self.table_id, self.index
                )
            })
    }

    #[inline]
    fn stage_layout(&mut self, layout: TableRuntimeLayout) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::RuntimeStaged);
        debug_assert!(self.new_layout.is_none());
        self.new_layout = Some(layout);
        self.phase = CreateIndexBuildPhase::LayoutStaged;
    }

    async fn execute_catalog_update(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let trx = self.trx.as_mut().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: transaction is missing before catalog update, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        let res = engine
            .catalog()
            .storage
            .stage_create_index(
                trx,
                self.table_id,
                self.index,
                new_metadata,
                definition_effects,
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(cleanup) = self.rollback_before_catalog_commit(guards).await {
                    return Err(err.merge_cleanup(cleanup.attach_with(|| {
                        format!(
                            "operation=create_index, phase=rollback_before_catalog_commit, table_id={}, index={}",
                            self.table_id, self.index
                        )
                    })));
                }
                Err(err)
            }
        }
    }

    async fn commit_catalog(&mut self, guards: &PoolGuards) -> RuntimeOrFatalResult<TrxID> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let trx = self.trx.take().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: transaction is missing before commit, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        match trx.commit_catalog_ddl().await {
            Ok(cts) => {
                self.phase = CreateIndexBuildPhase::CatalogCommitted;
                Ok(cts)
            }
            Err(err) => {
                self.cleanup_staged_runtime(guards).await;
                self.phase = CreateIndexBuildPhase::Aborted;
                Err(err)
            }
        }
    }

    fn take_layout_for_install(&mut self) -> TableRuntimeLayout {
        assert_eq!(
            self.phase,
            CreateIndexBuildPhase::CatalogCommitted,
            "create-index progress invariant violated: layout install requested in phase {:?}",
            self.phase
        );
        self.new_layout.take().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: runtime layout is missing before install, table_id={}, index={}",
                self.table_id, self.index
            )
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::CatalogCommitted);
        self.staged_index = None;
        self.phase = CreateIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit(
        &mut self,
        guards: &PoolGuards,
    ) -> RuntimeOrFatalResult<()> {
        self.cleanup_staged_runtime(guards).await;
        let rollback_res = rollback_active_ddl_trx(&mut self.trx).await;
        self.phase = CreateIndexBuildPhase::Aborted;
        rollback_res?;
        Ok(())
    }

    async fn cleanup_after_catalog_commit_failure(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        operation: &'static str,
        source: RuntimeOrFatalError,
    ) -> RuntimeOrFatalError {
        self.cleanup_staged_runtime(guards).await;
        self.phase = CreateIndexBuildPhase::Aborted;
        poison_index_after_catalog_commit_with_source(
            &engine.poisoner,
            IndexDdlKind::Create,
            self.table_id,
            self.index,
            operation,
            source,
        )
    }

    async fn cleanup_staged_runtime(&mut self, guards: &PoolGuards) {
        self.new_layout = None;
        if let Some(index) = self.staged_index.take() {
            // Preserve the existing best-effort cleanup policy. A destroy
            // failure is observed but does not replace the DDL source.
            if let Err(report) = destroy_uninstalled_staged_index(index, guards).await {
                let report = report.attach(format!(
                    "operation=cleanup_create_index_staged_runtime, table_id={}, index={}",
                    self.table_id, self.index
                ));
                obs::error!(
                    "event=index_ddl_cleanup component=catalog_index action=destroy_staged result=error error={report:?}"
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DropIndexBuildPhase {
    LayoutStaged,
    CatalogCommitted,
    Installed,
    Aborted,
}

struct DropIndexProgress {
    table_id: TableID,
    index: IndexRef,
    phase: DropIndexBuildPhase,
    trx: Option<PrivateTransaction>,
    new_layout: Option<TableRuntimeLayout>,
}

impl DropIndexProgress {
    #[inline]
    fn new(table_id: TableID, index: IndexRef, trx: PrivateTransaction) -> Self {
        Self {
            table_id,
            index,
            phase: DropIndexBuildPhase::LayoutStaged,
            trx: Some(trx),
            new_layout: None,
        }
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }

    #[inline]
    fn stage_layout(&mut self, layout: TableRuntimeLayout) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        debug_assert!(self.new_layout.is_none());
        self.new_layout = Some(layout);
    }

    async fn execute_catalog_update(
        &mut self,
        catalog: &Catalog,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let trx = self.trx.as_mut().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: transaction is missing before catalog update, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        let res = catalog
            .storage
            .stage_drop_index(
                trx,
                self.table_id,
                self.index,
                new_metadata,
                definition_effects,
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(cleanup) = self.rollback_before_catalog_commit().await {
                    return Err(err.merge_cleanup(cleanup.attach_with(|| {
                        format!(
                            "operation=drop_index, phase=rollback_before_catalog_commit, table_id={}, index={}",
                            self.table_id, self.index
                        )
                    })));
                }
                Err(err)
            }
        }
    }

    async fn commit_catalog(&mut self) -> RuntimeOrFatalResult<TrxID> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let trx = self.trx.take().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: transaction is missing before commit, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        match trx.commit_catalog_ddl().await {
            Ok(cts) => {
                self.phase = DropIndexBuildPhase::CatalogCommitted;
                Ok(cts)
            }
            Err(err) => {
                self.new_layout = None;
                self.phase = DropIndexBuildPhase::Aborted;
                Err(err)
            }
        }
    }

    fn take_layout_for_install(&mut self) -> TableRuntimeLayout {
        assert_eq!(
            self.phase,
            DropIndexBuildPhase::CatalogCommitted,
            "drop-index progress invariant violated: layout install requested in phase {:?}",
            self.phase
        );
        self.new_layout.take().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: runtime layout is missing before install, table_id={}, index={}",
                self.table_id, self.index
            )
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::CatalogCommitted);
        self.phase = DropIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit(&mut self) -> RuntimeOrFatalResult<()> {
        self.new_layout = None;
        let rollback_res = rollback_active_ddl_trx(&mut self.trx).await;
        self.phase = DropIndexBuildPhase::Aborted;
        rollback_res?;
        Ok(())
    }

    async fn cleanup_after_catalog_commit_failure(
        &mut self,
        poisoner: &EnginePoisoner,
        operation: &'static str,
        source: RuntimeOrFatalError,
    ) -> RuntimeOrFatalError {
        self.new_layout = None;
        self.phase = DropIndexBuildPhase::Aborted;
        poison_index_after_catalog_commit_with_source(
            poisoner,
            IndexDdlKind::Drop,
            self.table_id,
            self.index,
            operation,
            source,
        )
    }
}

/// Caller-prepared CREATE INDEX awaiting mandatory runtime capacity.
pub(crate) struct PreparedCreateIndex {
    gates: IndexDdlGateScope,
    scope: PreparedDdlScope,
    plan: CreateIndexPlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedCreateIndex {
    /// Builds one fully prepared CREATE INDEX carrier.
    #[inline]
    pub(crate) fn new(
        gates: IndexDdlGateScope,
        scope: PreparedDdlScope,
        plan: CreateIndexPlan,
    ) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id(),
        );
        Self {
            gates,
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedCreateIndex {
    type Output = IndexID;
    type Accepted = AcceptedCreateIndex;

    const LABEL: &'static str = "create_index";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            gates,
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id();
        let index = plan.index();
        AcceptedCreateIndex {
            gates: Some(gates),
            scope: scope.accept(),
            table_id,
            index,
            plan: Some(plan),
            progress: None,
        }
    }
}

/// Mandatory-runtime owner of accepted CREATE INDEX execution.
pub(crate) struct AcceptedCreateIndex {
    gates: Option<IndexDdlGateScope>,
    scope: AcceptedDdlScope,
    table_id: TableID,
    index: IndexRef,
    plan: Option<CreateIndexPlan>,
    progress: Option<CreateIndexProgress>,
}

impl AcceptedExecution for AcceptedCreateIndex {
    type Output = IndexID;

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self.execute_inner().await;
        self.scope.mark_terminal_ready();
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.progress.take());
        drop(self.plan.take());
        drop(self.gates.take());
        self.scope.finish();
    }

    #[inline]
    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
        }
        self.scope.handle_panic();
        let phase = self
            .progress
            .as_ref()
            .map_or(CreateIndexBuildPhase::Building, |progress| progress.phase);
        CompletionErrorBridge::capture(Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "accepted CREATE INDEX panicked: table_id={}, index={}, phase={phase:?}",
            self.table_id, self.index
        )))
    }
}

impl AcceptedCreateIndex {
    async fn execute_inner(&mut self) -> CompletionResult<IndexID> {
        let mut plan = self.plan.take().unwrap_or_else(|| {
            panic!("accepted CREATE INDEX invariant violated: execution plan is missing")
        });
        let runtime = self.scope.engine().clone();
        let engine = runtime.core();
        let guards = runtime.pool_guards();
        let table_id = plan.table_id();
        let index = plan.index();
        let index_slot = index.slot();
        let mut secondary_index_slots = plan.take_secondary_index_slots();

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateBeforeFirstEffect)
            .await;

        let trx = self.scope.begin_private_trx().map_err(|err| {
            CompletionErrorBridge::capture(
                err.attach("operation=create_index, phase=begin_private_transaction"),
            )
        })?;
        self.progress = Some(CreateIndexProgress::new(table_id, index, trx));
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| {
                panic!(
                    "accepted CREATE INDEX invariant violated: progress state is missing after transaction initialization"
                )
            });

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreatePrivateTransactionBegun)
            .await;

        let build_ts = progress.build_ts();
        let key_validator = CreateIndexKeyValidator::new(plan.new_index_spec());
        let collector = CreateIndexCollector::new(
            plan.table(),
            guards,
            plan.old_layout().as_ref(),
            plan.new_index_spec(),
            plan.active_root(),
        );
        let mut mutable_file = MutableTableFile::fork(
            plan.table().file(),
            engine.table_fs.background_writes(),
            plan.table().disk_pool().clone(),
            guards.disk_guard().clone(),
        );
        let disk_runtime = match SecondaryDiskTreeRuntime::new(
            index_slot,
            Arc::clone(plan.new_metadata()),
            Arc::clone(plan.table().file()),
            plan.table().disk_pool().clone(),
        ) {
            Ok(runtime) => runtime,
            Err(err) => {
                if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
                }
                return Err(CompletionErrorBridge::capture(err));
            }
        };

        let mut cold_rows = match collector.collect_current_cold().await {
            Ok(cold_rows) => cold_rows,
            Err(err) => {
                if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
                }
                return Err(CompletionErrorBridge::capture_operation_or_runtime(err));
            }
        };
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateColdCollectionComplete)
            .await;
        if let Err(err) = key_validator.prepare_cold(&mut cold_rows) {
            if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
            }
            return Err(CompletionErrorBridge::capture(err));
        }

        let cold_root = match build_create_index_disk_tree(
            &mut mutable_file,
            &disk_runtime,
            guards,
            plan.new_index_spec(),
            &cold_rows,
            build_ts,
        )
        .await
        {
            Ok(root) => root,
            Err(err) => {
                if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
                }
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(err));
            }
        };
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateDiskTreeBuilt)
            .await;
        secondary_index_slots[index_slot.as_usize()] = SecondaryIndexSlot::Active {
            index_id: plan.index().id(),
            root: SecondaryIndexRoot::from_block_id(cold_root),
        };
        mutable_file.replace_metadata_and_secondary_index_slots(
            Arc::clone(plan.new_metadata()),
            secondary_index_slots,
        );

        let runtime_builder = CreateIndexRuntimeBuilder::new(
            engine,
            guards,
            plan.new_metadata().as_ref(),
            plan.new_index_spec(),
            build_ts,
        );
        let mut hot_rows = match collector.collect_current_hot().await {
            Ok(hot_rows) => hot_rows,
            Err(err) => {
                if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
                }
                return Err(CompletionErrorBridge::capture(err));
            }
        };
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateHotCollectionComplete)
            .await;
        if let Err(err) = key_validator.prepare_hot(&mut hot_rows, &cold_rows) {
            if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
            }
            return Err(CompletionErrorBridge::capture(err));
        }
        let runtime_index = if plan.new_index_spec().unique() {
            runtime_builder.build_unique(disk_runtime, hot_rows).await
        } else {
            runtime_builder
                .build_non_unique(disk_runtime, hot_rows)
                .await
        };
        match runtime_index {
            Ok(index) => progress.stage_runtime_index(index),
            Err(err) => {
                if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                    return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
                }
                return Err(CompletionErrorBridge::capture_operation_or_runtime(err));
            }
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateRuntimeStaged)
            .await;

        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_create(CreateIndexTestFailure::AfterRuntimeStaged)
        {
            if let Err(cleanup) = progress.rollback_before_catalog_commit(guards).await {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
            }
            return Err(CompletionErrorBridge::capture(err));
        }

        let new_layout = build_created_index_runtime_layout(
            plan.old_layout(),
            Arc::clone(plan.new_metadata()),
            index,
            progress.clone_staged_index_for_layout(),
        );
        progress.stage_layout(new_layout);

        if let Err(err) = progress
            .execute_catalog_update(
                engine,
                guards,
                plan.new_metadata().as_ref(),
                plan.definition_effects(),
            )
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(err));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateCatalogStaged)
            .await;
        let create_cts = progress
            .commit_catalog(guards)
            .await
            .map_err(CompletionErrorBridge::capture_runtime_or_fatal)?;
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateCatalogCommitted)
            .await;

        if let Err(err) = engine
            .trx_sys
            .publish_table_file_root(mutable_file, create_cts, false)
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .cleanup_after_catalog_commit_failure(
                        engine,
                        guards,
                        "table_root_publish",
                        RuntimeOrFatalError::from(err),
                    )
                    .await,
            ));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateRootPublished)
            .await;

        let new_layout = progress.take_layout_for_install();
        if engine
            .catalog()
            .install_created_index_layout_and_publish_history(
                create_cts,
                &plan,
                new_layout,
                #[cfg(test)]
                &engine.index_ddl_test,
            )
            .is_none()
        {
            progress.cleanup_staged_runtime(guards).await;
            progress.phase = CreateIndexBuildPhase::Aborted;
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                poison_index_publication_invariant(
                    &engine.poisoner,
                    IndexDdlKind::Create,
                    table_id,
                    index,
                ),
            ));
        }
        progress.mark_installed();
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateLayoutHistoryPublished)
            .await;
        engine.trx_sys.request_metadata_history_purge();
        Ok(index.id())
    }
}

/// Caller-prepared DROP INDEX awaiting mandatory runtime capacity.
pub(crate) struct PreparedDropIndex {
    gates: IndexDdlGateScope,
    scope: PreparedDdlScope,
    plan: DropIndexPlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedDropIndex {
    /// Builds one fully prepared DROP INDEX carrier.
    #[inline]
    pub(crate) fn new(
        gates: IndexDdlGateScope,
        scope: PreparedDdlScope,
        plan: DropIndexPlan,
    ) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id(),
        );
        Self {
            gates,
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedDropIndex {
    type Output = ();
    type Accepted = AcceptedDropIndex;

    const LABEL: &'static str = "drop_index";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            gates,
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id();
        let index = plan.index();
        AcceptedDropIndex {
            gates: Some(gates),
            scope: scope.accept(),
            table_id,
            index,
            plan: Some(plan),
            progress: None,
        }
    }
}

/// Mandatory-runtime owner of accepted DROP INDEX execution.
pub(crate) struct AcceptedDropIndex {
    gates: Option<IndexDdlGateScope>,
    scope: AcceptedDdlScope,
    table_id: TableID,
    index: IndexRef,
    plan: Option<DropIndexPlan>,
    progress: Option<DropIndexProgress>,
}

impl AcceptedExecution for AcceptedDropIndex {
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
        drop(self.plan.take());
        drop(self.gates.take());
        self.scope.finish();
    }

    #[inline]
    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
        }
        self.scope.handle_panic();
        let phase = self
            .progress
            .as_ref()
            .map_or(DropIndexBuildPhase::LayoutStaged, |progress| progress.phase);
        CompletionErrorBridge::capture(Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "accepted DROP INDEX panicked: table_id={}, index={}, phase={phase:?}",
            self.table_id, self.index
        )))
    }
}

impl AcceptedDropIndex {
    async fn execute_inner(&mut self) -> CompletionResult<()> {
        let mut plan = self.plan.take().unwrap_or_else(|| {
            panic!("accepted DROP INDEX invariant violated: execution plan is missing")
        });
        let runtime = self.scope.engine().clone();
        let engine = runtime.core();
        let guards = runtime.pool_guards();
        let table_id = plan.table_id();
        let index = plan.index();
        let index_slot = index.slot();
        let secondary_index_slots = plan.take_secondary_index_slots();

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropBeforeFirstEffect)
            .await;

        let trx = self.scope.begin_private_trx().map_err(|err| {
            CompletionErrorBridge::capture(
                err.attach("operation=drop_index, phase=begin_private_transaction"),
            )
        })?;
        self.progress = Some(DropIndexProgress::new(table_id, index, trx));
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| {
                panic!(
                    "accepted DROP INDEX invariant violated: progress state is missing after transaction initialization"
                )
            });
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropPrivateTransactionBegun)
            .await;

        let mut mutable_file = MutableTableFile::fork(
            plan.table().file(),
            engine.table_fs.background_writes(),
            plan.table().disk_pool().clone(),
            guards.disk_guard().clone(),
        );
        mutable_file.replace_metadata_and_secondary_index_slots(
            Arc::clone(plan.new_metadata()),
            secondary_index_slots,
        );
        progress.stage_layout(build_dropped_index_runtime_layout(
            plan.old_layout(),
            Arc::clone(plan.new_metadata()),
            index_slot,
        ));
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRuntimeStaged)
            .await;

        if let Err(err) = progress
            .execute_catalog_update(
                engine.catalog(),
                plan.new_metadata().as_ref(),
                plan.definition_effects(),
            )
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(err));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropCatalogStaged)
            .await;
        let drop_cts = progress
            .commit_catalog()
            .await
            .map_err(CompletionErrorBridge::capture_runtime_or_fatal)?;
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropCatalogCommitted)
            .await;

        if let Err(err) = engine
            .trx_sys
            .publish_table_file_root(mutable_file, drop_cts, false)
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .cleanup_after_catalog_commit_failure(
                        &engine.poisoner,
                        "table_root_publish",
                        RuntimeOrFatalError::from(err),
                    )
                    .await,
            ));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRootPublished)
            .await;

        let new_layout = progress.take_layout_for_install();
        if engine
            .catalog()
            .install_dropped_index_layout_and_publish_history(
                drop_cts,
                &plan,
                new_layout,
                #[cfg(test)]
                &engine.index_ddl_test,
            )
            .is_none()
        {
            progress.phase = DropIndexBuildPhase::Aborted;
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                poison_index_publication_invariant(
                    &engine.poisoner,
                    IndexDdlKind::Drop,
                    table_id,
                    index,
                ),
            ));
        }
        progress.mark_installed();
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropLayoutHistoryPublished)
            .await;
        engine.trx_sys.request_metadata_history_purge();
        drop(plan);
        engine.trx_sys.request_retired_index_runtime_purge(table_id);
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRetiredCleanupComplete)
            .await;
        Ok(())
    }
}

/// Return the fixed catalog tables written by CREATE INDEX.
#[inline]
pub(crate) const fn create_index_catalog_write_targets() -> &'static [TableID] {
    &CREATE_INDEX_CATALOG_WRITE_TARGETS
}

/// Return the fixed catalog tables written by DROP INDEX.
#[inline]
pub(crate) const fn drop_index_catalog_write_targets() -> &'static [TableID] {
    &DROP_INDEX_CATALOG_WRITE_TARGETS
}

/// Classify whether an active table root proves one index DDL redo durable.
pub(crate) fn classify_index_ddl_root(
    kind: IndexDdlKind,
    table_id: TableID,
    ddl: ReplayVisibleIndexDdl,
    active_root: Option<&ActiveRoot>,
) -> DataIntegrityResult<IndexDdlRootProof> {
    let index = ddl.index();
    let index_slot = index.slot();
    let ddl_cts = ddl.cts();
    // Root proof is deliberately conservative: without an active root there is
    // no durable table state that can confirm whether this index DDL took
    // effect, so recovery must treat the DDL marker as provisional.
    let Some(active_root) = active_root else {
        return Ok(IndexDdlRootProof::Provisional);
    };
    // A root older than the DDL commit timestamp cannot include the DDL's table
    // metadata/root changes. It may still be a valid root, but it is not proof
    // for this redo marker.
    if active_root.root_ts < ddl_cts {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let metadata = &active_root.metadata;
    validate_secondary_index_state(metadata, &active_root.secondary_index_slots)
        .change_context(DataIntegrityError::InvalidRootInvariant)?;
    let root_count = active_root.secondary_index_slots.len();

    // `index_slot_count` is the allocation boundary. If the DDL's index slot is
    // still outside that boundary, the root cannot prove even allocation of the
    // slot, regardless of create/drop kind.
    if index_slot.as_usize() >= metadata.idx.index_slot_count() {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let Some(state) = active_root
        .secondary_index_slots
        .get(index_slot.as_usize())
        .copied()
    else {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "index DDL root proof missing secondary-root slot: table_id={table_id}, index={index}, root_count={root_count}, root_ts={}, ddl_cts={ddl_cts}",
            active_root.root_ts
        )));
    };

    match state {
        SecondaryIndexSlot::Vacant => Ok(IndexDdlRootProof::Provisional),
        SecondaryIndexSlot::Active { index_id, .. } => {
            if index_id != index.id() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replay-visible index DDL slot contains a different active generation: table_id={table_id}, marker={index}, durable_id={index_id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            match kind {
                IndexDdlKind::Create => Ok(IndexDdlRootProof::DurableFinalCreate),
                IndexDdlKind::Drop => Ok(IndexDdlRootProof::Provisional),
            }
        }
        SecondaryIndexSlot::Retired(id) => {
            if id != index.id() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replay-visible index DDL slot contains a different retired generation: table_id={table_id}, marker={index}, durable_id={id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            match kind {
                IndexDdlKind::Create => Ok(IndexDdlRootProof::DurableAllocationOnly),
                IndexDdlKind::Drop => Ok(IndexDdlRootProof::DurableFinalDrop),
            }
        }
    }
}

async fn rollback_active_ddl_trx(trx: &mut Option<PrivateTransaction>) -> RuntimeOrFatalResult<()> {
    let Some(trx) = trx.take() else {
        return Ok(());
    };
    trx.rollback_catalog_ddl().await?;
    Ok(())
}

#[inline]
fn create_index_cold_root_has_rows(column_block_index_root: BlockID, pivot_row_id: RowID) -> bool {
    if column_block_index_root == SUPER_BLOCK_ID {
        return false;
    }
    assert!(
        pivot_row_id != RowID::new(0),
        "create index found non-empty cold root with pivot_row_id == 0: column_block_index_root={column_block_index_root}, pivot_row_id={pivot_row_id}"
    );
    true
}

#[inline]
fn assert_create_index_block_index_snapshot(
    table_id: TableID,
    captured: (RowID, BlockID),
    runtime: (RowID, BlockID),
) {
    assert!(
        captured == runtime,
        "create-index block-index snapshot invariant violated: table_id={table_id}, captured_pivot_row_id={}, captured_column_block_index_root={}, runtime_pivot_row_id={}, runtime_column_block_index_root={}",
        captured.0,
        captured.1,
        runtime.0,
        runtime.1
    );
}

#[inline]
fn create_index_current_cold_row_is_deleted(table: &Table, row_id: RowID) -> OperationResult<bool> {
    match table.deletion_buffer().get(row_id) {
        Some(DeleteMarker::Committed(_)) => Ok(true),
        Some(DeleteMarker::Ref(status)) if trx_is_committed(status.ts()) => Ok(true),
        Some(DeleteMarker::Ref(_)) => Err(Report::new(OperationError::WriteConflict).attach(
            format!(
                "create index found uncommitted cold-row delete marker: table_id={}, row_id={row_id}",
                table.table_id()
            ),
        )),
        None => Ok(false),
    }
}

async fn build_create_index_disk_tree(
    mutable_file: &mut MutableTableFile,
    disk_runtime: &SecondaryDiskTreeRuntime,
    guards: &PoolGuards,
    index_spec: &TableIndexMetadata,
    rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
) -> RuntimeOrFatalResult<Option<BlockID>> {
    if rows.is_empty() {
        return Ok(None);
    }

    if index_spec.unique() {
        assert!(
            rows.windows(2).all(|pair| pair[0].key != pair[1].key),
            "create-index build invariant violated: unique cold keys were not validated before DiskTree construction"
        );
        let batch = rows
            .iter()
            .map(|entry| UniqueDiskTreeEncodedPut {
                key: entry.key.as_bytes(),
                row_id: entry.row_id,
            })
            .collect::<Vec<_>>();
        let tree = disk_runtime.open_unique_at(None, guards.disk_guard())?;
        let mut writer = tree.batch_writer(mutable_file, build_ts);
        writer.batch_put_encoded(&batch);
        writer.finish().await
    } else {
        let batch = rows
            .iter()
            .map(|entry| NonUniqueDiskTreeEncodedExact {
                key: entry.key.as_bytes(),
            })
            .collect::<Vec<_>>();
        let tree = disk_runtime.open_non_unique_at(None, guards.disk_guard())?;
        let mut writer = tree.batch_writer(mutable_file, build_ts);
        writer.batch_insert_encoded(&batch)?;
        writer.finish().await
    }
}

async fn insert_create_index_unique_hot_rows(
    mem: &UniqueMemIndex<EvictableBufferPool>,
    index_guard: &PoolGuard,
    hot_rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
    #[cfg(test)] test: &tests::IndexDdlTestController,
) -> OperationOrRuntimeResult<()> {
    for (row_no, row) in hot_rows.iter().enumerate() {
        match mem
            .bind(index_guard)
            .insert_encoded_if_not_exists(&row.key, row.row_id, false, build_ts)
            .await?
        {
            IndexInsert::Ok(_) => (),
            IndexInsert::DuplicateKey(..) => {
                return Err(Report::new(OperationError::DuplicateKey).attach(format!(
                    "create unique index found duplicate hot key during MemIndex build: row_id={}",
                    row.row_id
                ))
                .into());
            }
        }
        if row_no % POLL_BUDGET == POLL_BUDGET - 1 {
            #[cfg(test)]
            test.reach_phase(tests::IndexDdlTestPhase::CreateHotBuildBatchComplete)
                .await;
            yield_now().await;
        }
    }
    Ok(())
}

async fn insert_create_index_non_unique_hot_rows(
    mem: &NonUniqueMemIndex<EvictableBufferPool>,
    index_guard: &PoolGuard,
    hot_rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
    #[cfg(test)] test: &tests::IndexDdlTestController,
) -> RuntimeResult<()> {
    for (row_no, row) in hot_rows.iter().enumerate() {
        match mem
            .bind(index_guard)
            .insert_encoded_if_not_exists(&row.key, row.row_id, false, build_ts)
            .await?
        {
            IndexInsert::Ok(_) => (),
            IndexInsert::DuplicateKey(..) => {
                panic!(
                    "create-index build invariant violated: current non-unique exact key duplicated for row_id={}",
                    row.row_id
                );
            }
        }
        if row_no % POLL_BUDGET == POLL_BUDGET - 1 {
            #[cfg(test)]
            test.reach_phase(tests::IndexDdlTestPhase::CreateHotBuildBatchComplete)
                .await;
            yield_now().await;
        }
    }
    Ok(())
}

fn build_created_index_runtime_layout(
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index: IndexRef,
    staged_index: Arc<SecondaryIndex<EvictableBufferPool>>,
) -> TableRuntimeLayout {
    let index_slot = index.slot();
    let generation = old_layout.generation().checked_add(1).unwrap_or_else(|| {
        panic!(
            "create-index runtime layout generation overflow: old_generation={}",
            old_layout.generation()
        )
    });
    let mut slots = old_layout.secondary_indexes().to_vec();
    slots.resize_with(new_metadata.idx.index_slot_count(), || None);
    assert!(
        slots
            .get(index_slot.as_usize())
            .and_then(Option::as_ref)
            .is_none(),
        "create-index runtime layout invariant violated: slot is already occupied, index_slot={index_slot}"
    );
    slots[index_slot.as_usize()] = Some(RuntimeIndexEntry::new(index, staged_index));
    TableRuntimeLayout::from_entries(generation, new_metadata, slots.into_boxed_slice())
}

fn build_dropped_index_runtime_layout(
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index_slot: IndexSlot,
) -> TableRuntimeLayout {
    let generation = old_layout.generation().checked_add(1).unwrap_or_else(|| {
        panic!(
            "drop-index runtime layout generation overflow: old_generation={}",
            old_layout.generation()
        )
    });
    let mut slots = old_layout.secondary_indexes().to_vec();
    let slot = slots.get_mut(index_slot.as_usize()).unwrap_or_else(|| {
        panic!(
            "drop-index runtime layout invariant violated: slot is out of range, index_slot={index_slot}, slot_count={}",
            old_layout.index_slot_count()
        )
    });
    assert!(
        slot.is_some(),
        "drop-index runtime layout invariant violated: slot is inactive, index_slot={index_slot}"
    );
    *slot = None;
    TableRuntimeLayout::from_entries(generation, new_metadata, slots.into_boxed_slice())
}

async fn destroy_uninstalled_staged_index(
    index: Arc<SecondaryIndex<EvictableBufferPool>>,
    guards: &PoolGuards,
) -> RuntimeResult<()> {
    let Ok(index) = Arc::try_unwrap(index) else {
        // Preserve the existing best-effort cleanup policy. A surviving
        // internal reference leaves ownership with that reference.
        return Ok(());
    };
    index
        .destroy(guards.index_guard())
        .await
        .change_context(RuntimeError::CatalogAccess)
        .attach("operation=destroy_uninstalled_create_index_runtime")
}

#[inline]
fn poison_index_after_catalog_commit_with_source(
    poisoner: &EnginePoisoner,
    kind: IndexDdlKind,
    table_id: TableID,
    index: IndexRef,
    operation: &'static str,
    source: RuntimeOrFatalError,
) -> RuntimeOrFatalError {
    let operation_name = match kind {
        IndexDdlKind::Create => "create_index",
        IndexDdlKind::Drop => "drop_index",
    };
    let report = source.into_fatal_report(FatalError::Poisoned).attach(format!(
        "{operation_name} failed after catalog commit: table_id={table_id}, index={index}, operation={operation}"
    ));
    obs::error!(
        "event=engine_poison component=catalog_index action=poison result=error error={:?}",
        report
    );
    RuntimeOrFatalError::from(poisoner.poison(report).into_report())
}

#[inline]
fn poison_index_publication_invariant(
    poisoner: &EnginePoisoner,
    kind: IndexDdlKind,
    table_id: TableID,
    index: IndexRef,
) -> RuntimeOrFatalError {
    let operation = match kind {
        IndexDdlKind::Create => "create_index",
        IndexDdlKind::Drop => "drop_index",
    };
    let report = Report::new(FatalError::Poisoned).attach(format!(
        "{operation} metadata history publication disagreed after catalog/root/layout commit: table_id={table_id}, index={index}"
    ));
    obs::error!(
        "event=engine_poison component=catalog_index action=poison result=error error={:?}",
        report
    );
    RuntimeOrFatalError::from(poisoner.poison(report).into_report())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::catalog::{
        ActiveIndexSpec, CurrentTableState, ResolvedVisibleTableMetadata, SecondaryIndexSlot,
        StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableMetadata, tests::table2,
    };
    use crate::conf::{
        EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, MandatoryRuntimeConfig,
        TrxSysConfig,
    };
    use crate::engine::Engine;
    use crate::error::LifecycleError;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::ActiveRoot;
    use crate::index::IndexBatchStream;
    use crate::row::ops::{SelectKey, UpdateCol, UpdateMvcc};
    use crate::session::Session;
    use crate::session::tests::{
        SessionTestExt, active_operation_count, assert_checkpoint_published,
        remove_session_for_test,
    };
    use crate::table::tests::{
        assert_freeze_created, expect_delete_committed, insert_one_row, insert_rows,
    };
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::trx::purge::PurgeTestEvent;
    use crate::value::{Val, ValKind};
    use smol::{Timer, future::race};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::mpsc::sync_channel;
    use std::task::Poll;
    use std::thread::spawn;
    use std::time::Duration;
    use tempfile::TempDir;

    const LIGHTWEIGHT_TEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const LIGHTWEIGHT_TEST_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    const LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum CreateIndexTestFailure {
        PopulateNonUnique,
        AfterRuntimeStaged,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum IndexDdlTestPhase {
        CreateBeforeFirstEffect,
        CreatePrivateTransactionBegun,
        CreateColdCollectionComplete,
        CreateDiskTreeBuilt,
        CreateHotCollectionComplete,
        CreateHotBuildBatchComplete,
        CreateRuntimeStaged,
        CreateCatalogStaged,
        CreateCatalogCommitted,
        CreateRootPublished,
        CreateLayoutHistoryPublished,
        DropBeforeFirstEffect,
        DropPrivateTransactionBegun,
        DropRuntimeStaged,
        DropCatalogStaged,
        DropCatalogCommitted,
        DropRootPublished,
        DropLayoutHistoryPublished,
        DropRetiredCleanupComplete,
    }

    struct IndexDdlTestGate {
        phase: IndexDdlTestPhase,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    struct IndexDdlPublicationGate {
        kind: IndexDdlKind,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    #[derive(Default)]
    struct IndexDdlTestState {
        create_failure: parking_lot::Mutex<Option<CreateIndexTestFailure>>,
        panic_phase: parking_lot::Mutex<Option<IndexDdlTestPhase>>,
        gate: parking_lot::Mutex<Option<IndexDdlTestGate>>,
        publication_gate: parking_lot::Mutex<Option<IndexDdlPublicationGate>>,
    }

    /// Test fixture for index ddl test controller.
    #[derive(Clone, Default)]
    pub(crate) struct IndexDdlTestController {
        state: Arc<IndexDdlTestState>,
    }

    impl IndexDdlTestController {
        pub(super) fn maybe_fail_create(
            &self,
            failure: CreateIndexTestFailure,
        ) -> RuntimeResult<()> {
            if *self.state.create_failure.lock() == Some(failure) {
                return Err(Report::new(RuntimeError::IndexAccess)
                    .attach("operation=test_create_index_phase_failure"));
            }
            Ok(())
        }

        fn set_create_failure(&self, failure: Option<CreateIndexTestFailure>) {
            *self.state.create_failure.lock() = failure;
        }

        fn install_gate(
            &self,
            phase: IndexDdlTestPhase,
        ) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let previous = self.state.gate.lock().replace(IndexDdlTestGate {
                phase,
                entered: entered_tx,
                release: release_rx,
            });
            assert!(
                previous.is_none(),
                "index DDL test gate is already installed"
            );
            (entered_rx, release_tx)
        }

        fn set_panic_phase(&self, phase: Option<IndexDdlTestPhase>) {
            *self.state.panic_phase.lock() = phase;
        }

        fn install_publication_gate(
            &self,
            kind: IndexDdlKind,
        ) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let previous = self
                .state
                .publication_gate
                .lock()
                .replace(IndexDdlPublicationGate {
                    kind,
                    entered: entered_tx,
                    release: release_rx,
                });
            assert!(
                previous.is_none(),
                "index DDL publication test gate is already installed"
            );
            (entered_rx, release_tx)
        }

        /// Provides test-only access to `reach_publication_interval`.
        pub(crate) fn reach_publication_interval(&self, kind: IndexDdlKind) {
            let gate = {
                let mut slot = self.state.publication_gate.lock();
                if slot.as_ref().is_some_and(|gate| gate.kind == kind) {
                    slot.take()
                } else {
                    None
                }
            };
            let Some(gate) = gate else {
                return;
            };
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }

        pub(super) async fn reach_phase(&self, phase: IndexDdlTestPhase) {
            let should_panic = {
                let mut panic_phase = self.state.panic_phase.lock();
                if *panic_phase == Some(phase) {
                    *panic_phase = None;
                    true
                } else {
                    false
                }
            };
            if should_panic {
                panic!("injected accepted index DDL panic: phase={phase:?}");
            }
            let gate = {
                let mut slot = self.state.gate.lock();
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

    struct IndexDdlSnapshot {
        current_effective_cts: TrxID,
        current_metadata: Arc<TableMetadata>,
        history_count: Option<usize>,
        layout_generation: u64,
        runtime_slots: Vec<bool>,
        root: ActiveRoot,
    }

    fn index_ddl_snapshot(engine: &Engine, table_id: TableID, table: &Table) -> IndexDdlSnapshot {
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            ..
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("index DDL snapshot requires a live current table");
        };
        let layout = table.layout_snapshot();
        IndexDdlSnapshot {
            current_effective_cts: effective_cts,
            current_metadata: metadata,
            history_count: engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            layout_generation: layout.generation(),
            runtime_slots: layout
                .secondary_indexes()
                .iter()
                .map(Option::is_some)
                .collect(),
            root: table.file().active_root_unchecked().clone(),
        }
    }

    fn assert_index_ddl_snapshot_unchanged(
        before: &IndexDdlSnapshot,
        engine: &Engine,
        table_id: TableID,
        table: &Table,
    ) {
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            ..
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("failed index DDL must keep a live current table");
        };
        assert_eq!(effective_cts, before.current_effective_cts);
        assert!(Arc::ptr_eq(&metadata, &before.current_metadata));
        assert_eq!(
            engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            before.history_count
        );
        let layout = table.layout_snapshot();
        assert_eq!(layout.generation(), before.layout_generation);
        assert!(Arc::ptr_eq(layout.metadata_arc(), &before.current_metadata));
        assert_eq!(
            layout
                .secondary_indexes()
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>(),
            before.runtime_slots
        );
        assert_root_metadata_unchanged(&before.root, table);
    }

    fn columns() -> Vec<StorageColumnSpec> {
        vec![
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
        ]
    }

    fn root_with_metadata(metadata: TableMetadata, root_ts: TrxID) -> ActiveRoot {
        ActiveRoot::new(root_ts, 128, Arc::new(metadata))
    }

    async fn lightweight_test_engine(temp_dir: &TempDir, log_file_stem: &str) -> Engine {
        Engine::bootstrap(lightweight_test_engine_config(
            temp_dir.path().to_path_buf(),
            log_file_stem,
        ))
        .await
        .unwrap()
    }

    fn table_for_internal_assertion(engine: &Engine, table_id: TableID) -> Arc<Table> {
        engine
            .inner()
            .core
            .catalog()
            .get_table_now(table_id)
            .expect("test table should exist")
    }

    fn lightweight_test_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .meta_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
            .index_buffer(
                EvictableBufferPoolConfig::default()
                    .swap_file("index.swp")
                    .max_mem_size(LIGHTWEIGHT_TEST_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES),
            )
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(LIGHTWEIGHT_TEST_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES),
            )
            .trx(
                TrxSysConfig::default()
                    .log_write_io_depth(1)
                    .recovery_io_depth(1)
                    .catalog_checkpoint_scan_io_depth(1)
                    .log_file_stem(log_file_stem)
                    .purge_threads(1),
            )
            .file(
                FileSystemConfig::default()
                    .io_depth(1)
                    .readonly_buffer_size(LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES)
                    .data_dir("."),
            )
    }

    async fn update_one_row(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let result = trx
            .table_update_unique_mvcc(
                crate::TableIndex(table_id, IndexID::new(0)),
                &key.vals,
                update,
            )
            .await;
        let Ok(UpdateMvcc::Updated(row_id)) = result else {
            panic!("update should succeed: {result:?}");
        };
        trx.commit().await.unwrap();
        row_id
    }

    async fn assert_create_unique_index_rejects_cold_duplicates(
        log_file_stem: &str,
        cold_count: i32,
        hot_count: i32,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, log_file_stem).await;
        let table_id = table2(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let mut session = engine.new_session().unwrap();
        for primary_key in 0..cold_count {
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(primary_key), Val::from("dup")],
            )
            .await;
        }
        assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
        for offset in 0..hot_count {
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(100 + offset), Val::from("dup")],
            )
            .await;
        }
        assert_checkpoint_published(&mut session, table_id).await;
        let before = index_ddl_snapshot(&engine, table_id, &table);

        let err = session
            .create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
            )
            .await
            .unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::DuplicateKey)
        );
        assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
        assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
        assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
    }

    async fn assert_create_index_build_failure_cleanup(failure: CreateIndexTestFailure) {
        let temp_dir = TempDir::new().unwrap();
        let log_stem = match failure {
            CreateIndexTestFailure::PopulateNonUnique => "create_index_population_failure",
            CreateIndexTestFailure::AfterRuntimeStaged => "create_index_staged_failure",
        };
        let engine = lightweight_test_engine(&temp_dir, log_stem).await;
        let table_id = table2(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let mut session = engine.new_session().unwrap();
        insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1), Val::from("alpha")],
        )
        .await;
        let before = index_ddl_snapshot(&engine, table_id, &table);
        let allocated_before = engine.inner().pools.index.allocated();

        engine
            .inner()
            .index_ddl_test
            .set_create_failure(Some(failure));
        let result = session
            .create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            )
            .await;
        engine.inner().index_ddl_test.set_create_failure(None);
        let err = result.unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::IndexAccess)
        );
        assert_eq!(engine.inner().pools.index.allocated(), allocated_before);
        assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
        assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
        assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
    }

    async fn non_unique_mem_state(
        table: &Table,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &str,
        row_id: RowID,
    ) -> Option<bool> {
        let layout = table.layout_snapshot();
        layout
            .secondary_index(index_slot)
            .unwrap()
            .non_unique_mem()
            .unwrap()
            .bind(guards.index_guard())
            .lookup_unique(&[Val::from(key)], row_id, MAX_SNAPSHOT_TS)
            .await
            .unwrap()
    }

    fn single_key<V: Into<Val>>(value: V) -> SelectKey {
        SelectKey {
            index_slot: IndexSlot::new(0),
            vals: vec![value.into()],
        }
    }

    fn name_key(value: &str) -> SelectKey {
        SelectKey {
            index_slot: IndexSlot::new(1),
            vals: vec![Val::from(value)],
        }
    }

    fn active_secondary_root(table: &Table, index_slot: IndexSlot) -> Option<BlockID> {
        table
            .file()
            .active_root_unchecked()
            .secondary_index_root(index_slot)
    }

    async fn unique_runtime_lookup(
        table: &Table,
        index_slot: IndexSlot,
        guards: &PoolGuards,
        key: &[Val],
    ) -> Option<(RowID, bool)> {
        let root = active_secondary_root(table, index_slot);
        let layout = table.layout_snapshot();
        let index = layout
            .secondary_index(index_slot)
            .unwrap()
            .bind_unique_unchecked(guards, root)
            .unwrap();
        index.lookup(key, MAX_SNAPSHOT_TS).await.unwrap()
    }

    async fn non_unique_runtime_lookup(
        layout: &Arc<TableRuntimeLayout>,
        root: Option<BlockID>,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &[Val],
    ) -> Vec<RowID> {
        let index = layout.secondary_index(index_slot).unwrap();
        let range = index.key_encoder().encode_non_unique_equal_range(key);
        let bound = index.bind_non_unique_unchecked(guards, root).unwrap();
        let mut stream = bound
            .equal_scan_candidates(&range, MAX_SNAPSHOT_TS)
            .unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        rows
    }

    async fn non_unique_mem_index_prefix_scan(
        layout: &Arc<TableRuntimeLayout>,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &[Val],
    ) -> Vec<RowID> {
        let index = layout.secondary_index(index_slot).unwrap();
        let range = index.key_encoder().encode_non_unique_equal_range(key);
        let mem = index.non_unique_mem().unwrap().bind(guards.index_guard());
        let mut stream = mem.equal_scan_candidates(&range, MAX_SNAPSHOT_TS).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        rows
    }

    async fn non_unique_disk_tree_prefix_scan(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Vec<RowID> {
        let index_slot = key.index_slot;
        let root = active_secondary_root(table, index_slot);
        let layout = table.layout_snapshot();
        let index = layout.secondary_index(index_slot).unwrap();
        let range = index.key_encoder().encode_non_unique_equal_range(&key.vals);
        let tree = index
            .disk_runtime()
            .open_non_unique_at(root, guards.disk_guard())
            .unwrap();
        let mut stream = tree.scan_candidate_stream(&range);
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        rows
    }

    fn assert_root_metadata_unchanged(before: &ActiveRoot, table: &Table) {
        let after = table.file().active_root_unchecked();
        assert_eq!(after.slot_no, before.slot_no);
        assert_eq!(after.root_ts, before.root_ts);
        assert_eq!(after.effective_ts(), before.effective_ts());
        assert_eq!(after.meta_block_id, before.meta_block_id);
        assert_eq!(after.pivot_row_id, before.pivot_row_id);
        assert_eq!(after.heap_redo_start_ts, before.heap_redo_start_ts);
        assert_eq!(after.deletion_cutoff_ts, before.deletion_cutoff_ts);
        assert_eq!(
            after.secondary_index_slots, before.secondary_index_slots,
            "secondary index slots changed"
        );
        assert_eq!(after.alloc_map.len(), before.alloc_map.len());
        assert_eq!(after.alloc_map.allocated(), before.alloc_map.allocated());
        assert!(
            (0..before.alloc_map.len()).all(|block_id| {
                after.alloc_map.is_allocated(block_id) == before.alloc_map.is_allocated(block_id)
            }),
            "table-file allocation map changed"
        );
        assert!(Arc::ptr_eq(&after.metadata, &before.metadata));
    }

    #[test]
    fn classify_create_index_root_proof_variants() {
        let active_metadata = TableMetadata::try_new_with_index_slot_count(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(2),
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, TrxID::new(20));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalCreate
        );

        let dropped_metadata = active_root
            .metadata
            .without_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
            .unwrap();
        let mut dropped_root = root_with_metadata(dropped_metadata, TrxID::new(30));
        dropped_root.secondary_index_slots[1] = SecondaryIndexSlot::Retired(IndexID::new(1));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableAllocationOnly
        );

        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(31),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );
    }

    #[test]
    fn classify_drop_index_requires_inactive_empty_slot() {
        let active_metadata = TableMetadata::try_new_with_index_slot_count(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(2),
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, TrxID::new(20));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );

        let dropped_metadata = active_root
            .metadata
            .without_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
            .unwrap();
        let mut dropped_root = root_with_metadata(dropped_metadata, TrxID::new(20));
        dropped_root.secondary_index_slots[1] = SecondaryIndexSlot::Retired(IndexID::new(1));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalDrop
        );
    }

    #[test]
    fn create_index_cold_root_shape_accepts_empty_root() {
        assert!(!create_index_cold_root_has_rows(
            SUPER_BLOCK_ID,
            RowID::new(0)
        ));
        assert!(!create_index_cold_root_has_rows(
            SUPER_BLOCK_ID,
            RowID::new(10)
        ));
    }

    #[test]
    #[should_panic(expected = "non-empty cold root with pivot_row_id == 0")]
    fn create_index_cold_root_shape_panics_on_non_empty_root_without_pivot() {
        let _ = create_index_cold_root_has_rows(BlockID::new(99), RowID::new(0));
    }

    #[test]
    fn create_index_block_index_snapshot_accepts_exact_match() {
        assert_create_index_block_index_snapshot(
            TableID::new(42),
            (RowID::new(100), BlockID::new(10)),
            (RowID::new(100), BlockID::new(10)),
        );
    }

    #[test]
    fn create_index_block_index_snapshot_panics_with_boundary_diagnostic() {
        for runtime in [
            (RowID::new(101), BlockID::new(10)),
            (RowID::new(100), BlockID::new(11)),
        ] {
            let panic = catch_unwind(AssertUnwindSafe(|| {
                assert_create_index_block_index_snapshot(
                    TableID::new(42),
                    (RowID::new(100), BlockID::new(10)),
                    runtime,
                );
            }))
            .unwrap_err();
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(message.contains("table_id=42"), "{message}");
            assert!(message.contains("captured_pivot_row_id=100"), "{message}");
            assert!(
                message.contains("captured_column_block_index_root=10"),
                "{message}"
            );
            assert!(
                message.contains(&format!("runtime_pivot_row_id={}", runtime.0)),
                "{message}"
            );
            assert!(
                message.contains(&format!("runtime_column_block_index_root={}", runtime.1)),
                "{message}"
            );
        }
    }

    #[test]
    fn create_index_key_validator_only_sorts_non_unique_cold_entries() {
        let row = |key: &[u8], row_id| CreateIndexRowEntry {
            key: BTreeKey::from(key),
            row_id: RowID::new(row_id),
        };
        let validator = CreateIndexKeyValidator::NonUnique;
        let mut cold_rows = vec![row(b"cold-b", 2), row(b"cold-a", 1)];
        validator.prepare_cold(&mut cold_rows).unwrap();
        assert_eq!(cold_rows[0].key.as_bytes(), b"cold-a");
        assert_eq!(cold_rows[1].key.as_bytes(), b"cold-b");

        let mut hot_rows = vec![row(b"hot-b", 4), row(b"hot-a", 3)];
        validator.prepare_hot(&mut hot_rows, &cold_rows).unwrap();
        assert_eq!(hot_rows[0].key.as_bytes(), b"hot-b");
        assert_eq!(hot_rows[1].key.as_bytes(), b"hot-a");
    }

    #[test]
    fn create_index_key_validator_rejects_unique_cold_duplicate() {
        let row = |row_id| CreateIndexRowEntry {
            key: BTreeKey::from(&b"duplicate"[..]),
            row_id: RowID::new(row_id),
        };
        let mut unique_rows = vec![row(1), row(2)];
        let err = CreateIndexKeyValidator::Unique
            .prepare_cold(&mut unique_rows)
            .unwrap_err();
        assert_eq!(*err.current_context(), OperationError::DuplicateKey);
    }

    #[test]
    fn test_create_index_builds_non_unique_hot_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row1 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            let _row2 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(2), Val::from("beta")],
            )
            .await;
            let row3 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(3), Val::from("alpha")],
            )
            .await;
            let old_generation = table.layout_snapshot().generation();

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_some());
            assert_eq!(table.layout_snapshot().generation(), old_generation + 1);
            assert_eq!(active_secondary_root(&table, IndexSlot::new(1)), None);
            let table_object = engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .find_uncommitted_by_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table_object.index_slot_count, 2);

            let layout = table.layout_snapshot();
            let root = active_secondary_root(&table, IndexSlot::new(1));
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                IndexSlot::new(1),
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3]);

            let row4 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(4), Val::from("alpha")],
            )
            .await;
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                IndexSlot::new(1),
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3, row4]);
        });
    }

    #[test]
    fn test_abandoned_create_index_future_after_acceptance_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_observer_drop").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let (entered, release) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::CreateBeforeFirstEffect);
            let mut session = engine.new_session().unwrap();
            let mut create_fut = Box::pin(session.create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            ));

            assert!(matches!(
                futures::poll!(create_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            let (published, finish_publication) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::CreateLayoutHistoryPublished);
            drop(create_fut);
            release.send_async(()).await.unwrap();
            published.recv_async().await.unwrap();

            let layout = table.layout_snapshot();
            assert!(
                layout
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );
            assert!(layout.secondary_indexes()[1].is_some());
            let CurrentTableState::Live { metadata, .. } = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
            else {
                panic!("CREATE INDEX must retain a live current table");
            };
            assert!(Arc::ptr_eq(layout.metadata_arc(), &metadata));
            finish_publication.send_async(()).await.unwrap();
            let mut verify_session = engine.new_session().unwrap();
            verify_session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    #[test]
    fn test_abandoned_drop_index_future_after_acceptance_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop_index_observer_drop").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let index_slot = table
                .metadata()
                .idx
                .resolve_index_id(index_id)
                .expect("created index must be active")
                .slot();
            let (entered, release) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::DropBeforeFirstEffect);
            let mut drop_fut = Box::pin(session.drop_index(table_id, index_id));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            let (published, finish_publication) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::DropLayoutHistoryPublished);
            drop(drop_fut);
            release.send_async(()).await.unwrap();
            published.recv_async().await.unwrap();

            let layout = table.layout_snapshot();
            assert!(layout.metadata().idx.index_spec(index_slot).is_none());
            assert!(layout.secondary_indexes()[index_slot.as_usize()].is_none());
            finish_publication.send_async(()).await.unwrap();
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    #[test]
    fn test_terminal_cleanup_progresses_during_accepted_index_ddl_on_one_runner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                lightweight_test_engine_config(
                    temp_dir.path().to_path_buf(),
                    "index_ddl_cleanup_progress",
                )
                .mandatory_runtime(MandatoryRuntimeConfig::default().concurrency_limit(1)),
            )
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let mut ddl_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut ddl_session, 0, 129, "fairness").await;
            let before = ddl_session.mandatory_runtime_stats().unwrap();

            for iteration in 0..8 {
                let expected_cleanup_submitted =
                    before.transaction_cleanup.submitted_count + iteration + 1;
                race(
                    async {
                        let (entered, release) = engine
                            .inner()
                            .index_ddl_test
                            .install_gate(IndexDdlTestPhase::CreateHotBuildBatchComplete);
                        let mut create = Box::pin(ddl_session.create_index(
                            table_id,
                            StorageIndexSpec::new(
                                vec![StorageIndexKey::new(1)],
                                StorageIndexFlags::empty(),
                            ),
                        ));
                        assert!(matches!(
                            futures::poll!(create.as_mut()),
                            Poll::Pending
                        ));
                        entered.recv_async().await.unwrap();

                        let mut cleanup_session = engine.new_session().unwrap();
                        let mut rollback =
                            Box::pin(cleanup_session.begin_trx().unwrap().rollback());
                        let mut rollback_result = None;
                        loop {
                            if rollback_result.is_none()
                                && let Poll::Ready(result) = futures::poll!(rollback.as_mut())
                            {
                                rollback_result = Some(result);
                            }
                            let cleanup_submitted = engine
                                .inner()
                                .mandatory_runtime
                                .stats()
                                .transaction_cleanup
                                .submitted_count;
                            if cleanup_submitted >= expected_cleanup_submitted {
                                break;
                            }
                            assert!(
                                rollback_result.is_none(),
                                "rollback completed before mandatory cleanup submission"
                            );
                            Timer::after(Duration::from_millis(1)).await;
                        }

                        release.send_async(()).await.unwrap();
                        match rollback_result {
                            Some(result) => result,
                            None => rollback.await,
                        }
                        .unwrap();
                        cleanup_session.close().await.unwrap();

                        let index_id = create.await.unwrap();
                        ddl_session.drop_index(table_id, index_id).await.unwrap();
                    },
                    async {
                        Timer::after(Duration::from_secs(5)).await;
                        panic!(
                            "one-runner index DDL and cleanup scheduling timed out: iteration={iteration}"
                        );
                    },
                )
                .await;
            }

            let after = ddl_session.mandatory_runtime_stats().unwrap();
            assert_eq!(
                after
                    .operation
                    .submitted_count
                    .saturating_sub(before.operation.submitted_count),
                16
            );
            assert_eq!(
                after
                    .operation
                    .completed_count
                    .saturating_sub(before.operation.completed_count),
                16
            );
            assert_eq!(
                after
                    .transaction_cleanup
                    .submitted_count
                    .saturating_sub(before.transaction_cleanup.submitted_count),
                8
            );
            assert_eq!(
                after
                    .transaction_cleanup
                    .completed_count
                    .saturating_sub(before.transaction_cleanup.completed_count),
                8
            );
        });
    }

    #[test]
    fn test_create_index_execution_panic_before_first_effect_is_supervised() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_panic").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            engine
                .inner()
                .index_ddl_test
                .set_panic_phase(Some(IndexDdlTestPhase::CreateBeforeFirstEffect));

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
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
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            remove_session_for_test(&engine.inner().session_registry, session_id);
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_index_layout_history_publication_excludes_metadata_purge() {
        smol::block_on(async {
            for kind in [IndexDdlKind::Create, IndexDdlKind::Drop] {
                let temp_dir = TempDir::new().unwrap();
                let engine = lightweight_test_engine(
                    &temp_dir,
                    match kind {
                        IndexDdlKind::Create => "create_index_atomic_publication",
                        IndexDdlKind::Drop => "drop_index_atomic_publication",
                    },
                )
                .await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                let drop_index_id = if kind == IndexDdlKind::Drop {
                    Some(
                        session
                            .create_index(
                                table_id,
                                StorageIndexSpec::new(
                                    vec![StorageIndexKey::new(1)],
                                    StorageIndexFlags::empty(),
                                ),
                            )
                            .await
                            .unwrap(),
                    )
                } else {
                    None
                };
                let (publication_entered, publication_release) =
                    engine.inner().index_ddl_test.install_publication_gate(kind);
                let mut ddl_fut = Box::pin(async {
                    match kind {
                        IndexDdlKind::Create => session
                            .create_index(
                                table_id,
                                StorageIndexSpec::new(
                                    vec![StorageIndexKey::new(1)],
                                    StorageIndexFlags::empty(),
                                ),
                            )
                            .await
                            .map(|_| ()),
                        IndexDdlKind::Drop => {
                            session
                                .drop_index(
                                    table_id,
                                    drop_index_id
                                        .expect("DROP atomic-publication case has an active index"),
                                )
                                .await
                        }
                    }
                });

                assert!(matches!(
                    futures::poll!(ddl_fut.as_mut()),
                    std::task::Poll::Pending
                ));
                publication_entered.recv_async().await.unwrap();

                let catalog = engine.inner().core.catalog.clone();
                let (started_tx, started_rx) = sync_channel(1);
                let (done_tx, done_rx) = sync_channel(1);
                let purge = spawn(move || {
                    started_tx.send(()).unwrap();
                    catalog.purge_user_table_history(MAX_SNAPSHOT_TS);
                    done_tx.send(()).unwrap();
                });
                started_rx.recv().unwrap();
                assert_eq!(
                    done_rx.recv_timeout(Duration::from_millis(20)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout),
                    "metadata purge entered the catalog item during {kind:?} split publication"
                );

                publication_release.send_async(()).await.unwrap();
                done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                purge.join().unwrap();
                ddl_fut.await.unwrap();

                let layout = table.layout_snapshot();
                let CurrentTableState::Live { metadata, .. } = engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id)
                    .unwrap()
                else {
                    panic!("{kind:?} publication must retain a live current table");
                };
                assert!(Arc::ptr_eq(layout.metadata_arc(), &metadata));
            }
        });
    }

    #[test]
    fn test_create_index_build_failures_destroy_unpublished_runtime() {
        smol::block_on(async {
            assert_create_index_build_failure_cleanup(CreateIndexTestFailure::PopulateNonUnique)
                .await;
            assert_create_index_build_failure_cleanup(CreateIndexTestFailure::AfterRuntimeStaged)
                .await;
        });
    }

    #[test]
    fn test_create_non_unique_index_uses_only_current_hot_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_current_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            assert_eq!(
                update_one_row(
                    table_id,
                    &mut session,
                    &single_key(1),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("bravo"),
                    }],
                )
                .await,
                row_id
            );
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "alpha",
                    row_id,
                )
                .await,
                None
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "bravo",
                    row_id,
                )
                .await,
                Some(true)
            );
        });
    }

    #[test]
    fn test_create_non_unique_index_uses_current_cold_to_hot_replacement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_current_cold_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let cold_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let hot_row_id = update_one_row(
                table_id,
                &mut session,
                &single_key(1),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("bravo"),
                }],
            )
            .await;
            assert_ne!(hot_row_id, cold_row_id);
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &name_key("alpha"),
                )
                .await
                .is_empty()
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "alpha",
                    cold_row_id,
                )
                .await,
                None
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "bravo",
                    hot_row_id,
                )
                .await,
                Some(true)
            );
            let layout = table.layout_snapshot();
            assert!(
                non_unique_runtime_lookup(
                    &layout,
                    active_secondary_root(&table, IndexSlot::new(1)),
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    &[Val::from("alpha")],
                )
                .await
                .is_empty()
            );
            assert_eq!(
                non_unique_runtime_lookup(
                    &layout,
                    active_secondary_root(&table, IndexSlot::new(1)),
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    &[Val::from("bravo")],
                )
                .await,
                vec![hot_row_id]
            );
        });
    }

    #[test]
    fn test_create_unique_index_uses_only_current_hot_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "create_unique_index_current_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            assert_eq!(
                update_one_row(
                    table_id,
                    &mut session,
                    &single_key(1),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("bravo"),
                    }],
                )
                .await,
                row_id
            );
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("alpha")],
                )
                .await,
                None
            );
            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("bravo")],
                )
                .await,
                Some((row_id, false))
            );
        });
    }

    #[test]
    fn test_create_index_builds_non_unique_cold_disk_tree() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 10, 8, "cold").await;
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert!(active_secondary_root(&table, IndexSlot::new(1)).is_some());
            let mut rows =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key("cold"))
                    .await;
            rows.sort_unstable();
            assert_eq!(rows.len(), 8);
        });
    }

    #[test]
    fn test_create_index_uses_one_cold_hot_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_boundary").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();

            let mut cold_rows = Vec::new();
            for primary_key in 0..4 {
                cold_rows.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(primary_key), Val::from("boundary")],
                    )
                    .await,
                );
            }
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );

            let mut hot_rows = Vec::new();
            for primary_key in 100..103 {
                hot_rows.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(primary_key), Val::from("boundary")],
                    )
                    .await,
                );
            }
            assert_checkpoint_published(&mut session, table.table_id()).await;

            let captured_root = table.file().active_root_unchecked().clone();
            assert!(
                cold_rows
                    .iter()
                    .all(|row_id| *row_id < captured_root.pivot_row_id)
            );
            assert!(
                hot_rows
                    .iter()
                    .all(|row_id| *row_id >= captured_root.pivot_row_id)
            );

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            cold_rows.sort_unstable();
            hot_rows.sort_unstable();
            let key = name_key("boundary");
            let mut disk_rows =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &key).await;
            disk_rows.sort_unstable();
            assert_eq!(disk_rows, cold_rows);

            let layout = table.layout_snapshot();
            let index_slot = key.index_slot;
            let mut mem_rows = non_unique_mem_index_prefix_scan(
                &layout,
                &session.pool_guards(),
                index_slot,
                &key.vals,
            )
            .await;
            mem_rows.sort_unstable();
            assert_eq!(mem_rows, hot_rows);

            let mut expected_rows = cold_rows;
            expected_rows.extend(hot_rows);
            expected_rows.sort_unstable();
            let mut runtime_rows = non_unique_runtime_lookup(
                &layout,
                active_secondary_root(&table, index_slot),
                &session.pool_guards(),
                index_slot,
                &key.vals,
            )
            .await;
            runtime_rows.sort_unstable();
            assert_eq!(runtime_rows, expected_rows);
        });
    }

    #[test]
    fn test_create_index_retains_old_root_until_purge_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let retained_root_ptr = table.file().active_root_unchecked() as *const _ as usize;
            let drop_count_before = old_root_drop_count(retained_root_ptr);

            let mut read_session = engine.new_session().unwrap();
            let read_trx = read_session.begin_trx().unwrap();

            let mut session = engine.new_session().unwrap();
            session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let retained_root_fence = table.file().active_root_unchecked().effective_ts();
            engine.inner().trx_sys.request_table_root_retention_purge();

            // Timer audit: bounded negative assertion while an active reader pins the root.
            for _ in 0..10 {
                Timer::after(Duration::from_millis(10)).await;
                assert_eq!(
                    old_root_drop_count(retained_root_ptr),
                    drop_count_before,
                    "create index old root must stay retained while an earlier transaction is active"
                );
            }

            read_trx.commit().await.unwrap();
            session
                .wait_for_purge_completion_after(retained_root_fence)
                .await
                .unwrap();
            assert!(old_root_drop_count(retained_root_ptr) > drop_count_before);
        });
    }

    #[test]
    fn test_create_unique_index_rejects_duplicate_hot_rows_without_publish() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_one_row(table_id, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(table_id, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            let before = index_ddl_snapshot(&engine, table_id, &table);

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::DuplicateKey)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
        });
    }

    #[test]
    fn test_create_unique_index_rejects_cold_duplicates_without_publish() {
        smol::block_on(async {
            assert_create_unique_index_rejects_cold_duplicates(
                "create_unique_index_duplicate_cold_cold",
                2,
                0,
            )
            .await;
            assert_create_unique_index_rejects_cold_duplicates(
                "create_unique_index_duplicate_cold_hot",
                1,
                1,
            )
            .await;
        });
    }

    #[test]
    fn test_create_index_rejects_primary_key_without_publish() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_pk_rejected").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let mut session = engine.new_session().unwrap();

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::PK),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidMetadata)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("create_index does not support user-table primary keys"),
                "{report}"
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
        });
    }

    #[test]
    fn test_create_unique_index_skips_committed_cold_delete_marker() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row1 =
                insert_one_row(table_id, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(table_id, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;
            expect_delete_committed(table_id, &mut session, &single_key(2)).await;

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("dup")],
                )
                .await,
                Some((row1, false))
            );
        });
    }

    #[test]
    fn test_create_index_rejects_active_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_create_index_recovery_loads_published_index() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "create_index_recover",
            ))
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("persisted")],
            )
            .await;
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            drop(session);
            drop(table);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_index_recover",
            ))
            .await
            .unwrap();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_some());
            let session = engine.new_session().unwrap();
            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &SelectKey::new(IndexSlot::new(1), vec![Val::from("persisted")],),
                )
                .await,
                vec![row_id]
            );
        });
    }

    #[test]
    fn test_recovery_reconstructs_checkpoint_covered_slot_for_reuse() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_stem = "drop-index-recover";
            let engine =
                Engine::bootstrap(lightweight_test_engine_config(main_dir.clone(), log_stem))
                    .await
                    .unwrap();
            let table_id = table2(&engine).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();

            let metadata = table.metadata();
            assert_eq!(metadata.idx.index_slot_count_u32(), 2);
            assert!(metadata.idx.index_spec(IndexSlot::new(0)).is_some());
            assert!(metadata.idx.index_spec(IndexSlot::new(1)).is_none());
            let root = table.file().active_root_unchecked();
            assert_eq!(root.secondary_index_slots.len(), 2);
            assert_eq!(
                root.secondary_index_slots[1],
                SecondaryIndexSlot::Retired(crate::IndexID::new(1))
            );
            let catalog_indexes = engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap();
            assert_eq!(catalog_indexes.len(), 1);
            assert_eq!(catalog_indexes[0].index.slot(), IndexSlot::new(0));
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            drop(session);
            drop(table);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(main_dir, log_stem))
                .await
                .unwrap();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Retired(crate::IndexID::new(1))
            );

            let mut session = engine.new_session().unwrap();
            let replacement = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(replacement, crate::IndexID::new(2));
            let layout = table.layout_snapshot();
            assert_eq!(layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                layout.resolve_index_id(replacement).unwrap().slot(),
                IndexSlot::new(1)
            );
        });
    }

    #[test]
    fn test_drop_unique_and_primary_indexes_remove_uniqueness_enforcement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("same")],
            )
            .await;

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(2), Val::from("same")],
            )
            .await;

            session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("different")],
            )
            .await;
        });
    }

    #[test]
    fn test_drop_index_rejects_active_transaction_and_missing_slots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let mut horizon_session = engine.new_session().unwrap();
            let horizon_trx = horizon_session.begin_trx().unwrap();

            let trx = session.begin_trx().unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            trx.rollback().await.unwrap();

            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::IndexNotFound)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);

            session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::IndexNotFound)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            horizon_trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_index_runtime_install_retires_removed_runtime_until_pinned_layout_drops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            let old_layout = table.layout_snapshot();
            let old_generation = old_layout.generation();
            let old_pk = Arc::clone(
                old_layout.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
            );
            let mut old_session = engine.new_session().unwrap();
            let old_trx = old_session.begin_trx().unwrap();
            let retained_visible = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, old_trx.sts())
                .unwrap();
            let ResolvedVisibleTableMetadata::Live(retained_live) = &retained_visible else {
                panic!("pre-drop metadata should remain logically live");
            };
            assert!(
                retained_live
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );

            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            old_trx.rollback().await.unwrap();

            let installed = table.layout_snapshot();
            assert_eq!(installed.generation(), old_generation + 1);
            assert!(installed.secondary_indexes()[1].is_none());
            assert!(Arc::ptr_eq(
                installed.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
                &old_pk
            ));
            assert!(table.has_retired_secondary_indexes());
            assert_eq!(
                table
                    .cleanup_retired_secondary_indexes(&session.pool_guards())
                    .await
                    .unwrap(),
                0
            );
            drop(old_layout);
            assert!(
                retained_live
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );
            assert_eq!(
                table
                    .cleanup_retired_secondary_indexes(&session.pool_guards())
                    .await
                    .unwrap(),
                1
            );
            assert!(!table.has_retired_secondary_indexes());
        });
    }

    #[test]
    fn test_maintenance_and_ddl_use_current_layout_with_retained_predecessor_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "current_layout_maintenance").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut old_session = engine.new_session().unwrap();
            let old_trx = old_session.begin_trx().unwrap();
            let retained_visible = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, old_trx.sts())
                .unwrap();
            let ResolvedVisibleTableMetadata::Live(retained_live) = &retained_visible else {
                panic!("pre-DDL metadata should remain logically live");
            };
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);

            let mut session = engine.new_session().unwrap();
            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(index_id, IndexID::new(1));
            let current_layout = table.layout_snapshot();
            assert_eq!(current_layout.metadata().idx.active_index_count(), 2);
            assert!(current_layout.secondary_indexes()[1].is_some());
            assert!(Arc::ptr_eq(
                current_layout.metadata_arc(),
                &table.file().active_root_unchecked().metadata
            ));

            old_trx.rollback().await.unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("current")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            assert!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_root(IndexSlot::new(1))
                    .is_some()
            );
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);

            session.drop_index(table_id, index_id).await.unwrap();
            let installed = table.layout_snapshot();
            assert_eq!(installed.generation(), current_layout.generation() + 1);
            assert!(
                installed
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_none()
            );
            assert!(installed.secondary_indexes()[1].is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Retired(index_id)
            );
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);
        });
    }

    #[test]
    fn test_checkpoint_and_scheduled_cleanup_gate_lowest_slot_reuse() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_slot_reuse").await;
            let table_id = table2(&engine).await;
            let unrelated_table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let old_layout = table.layout_snapshot();
            let old_index = old_layout.secondary_indexes()[0]
                .as_ref()
                .unwrap()
                .index_ref();
            let mut session = engine.new_session().unwrap();

            session.drop_index(table_id, old_index.id()).await.unwrap();
            assert!(table.has_retired_secondary_indexes());
            session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            let appended = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(appended, IndexID::new(1));
            let appended_layout = table.layout_snapshot();
            assert_eq!(appended_layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                appended_layout.resolve_index_id(appended).unwrap().slot(),
                IndexSlot::new(1)
            );

            let (purge_tx, purge_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(purge_tx);
            drop(old_layout);
            engine.inner().trx_sys.request_retired_index_runtime_retry();
            let mut cleanup_started = false;
            let mut cleanup_tables = Vec::new();
            loop {
                match purge_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::RetiredIndexRuntimeStarted => cleanup_started = true,
                    PurgeTestEvent::RetiredIndexRuntimeTableStarted {
                        table_id: cleanup_table_id,
                    } => cleanup_tables.push(cleanup_table_id),
                    PurgeTestEvent::CycleCompleted if cleanup_started => break,
                    _ => {}
                }
            }
            assert_eq!(cleanup_tables, vec![table_id]);
            assert!(!cleanup_tables.contains(&unrelated_table_id));
            assert!(!table.has_retired_secondary_indexes());

            engine.inner().trx_sys.request_retired_index_runtime_retry();
            let mut retry_started = false;
            loop {
                match purge_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::RetiredIndexRuntimeStarted => retry_started = true,
                    PurgeTestEvent::RetiredIndexRuntimeTableStarted { table_id } => {
                        panic!("completed runtime candidate was retried: table_id={table_id}")
                    }
                    PurgeTestEvent::CycleCompleted if retry_started => break,
                    _ => {}
                }
            }

            let reused = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(reused, IndexID::new(2));
            let reused_layout = table.layout_snapshot();
            assert_eq!(reused_layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                reused_layout.resolve_index_id(reused).unwrap().slot(),
                IndexSlot::new(0)
            );
            assert_eq!(reused_layout.resolve_index_id(old_index.id()), None);
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[0].index_id(),
                Some(reused)
            );
        });
    }

    #[test]
    fn test_runtime_cleanup_before_checkpoint_still_blocks_slot_reuse() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_slot_runtime_first").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let old_index = table.layout_snapshot().secondary_indexes()[0]
                .as_ref()
                .unwrap()
                .index_ref();
            let mut session = engine.new_session().unwrap();

            session.drop_index(table_id, old_index.id()).await.unwrap();
            let (purge_tx, purge_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(purge_tx);
            engine.inner().trx_sys.request_retired_index_runtime_retry();
            let mut cleanup_started = false;
            loop {
                match purge_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::RetiredIndexRuntimeStarted => cleanup_started = true,
                    PurgeTestEvent::CycleCompleted if cleanup_started => break,
                    _ => {}
                }
            }
            assert!(!table.has_retired_secondary_indexes());

            let appended = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(
                table
                    .layout_snapshot()
                    .resolve_index_id(appended)
                    .unwrap()
                    .slot(),
                IndexSlot::new(1),
                "runtime cleanup alone must not clear durable retirement"
            );

            session.checkpoint_catalog().await.unwrap();
            let reused = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let layout = table.layout_snapshot();
            assert_eq!(
                layout.resolve_index_id(reused).unwrap().slot(),
                IndexSlot::new(0)
            );
            assert_eq!(layout.metadata().idx.index_slot_count_u32(), 2);
        });
    }
}
