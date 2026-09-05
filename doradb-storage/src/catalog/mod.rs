mod checkpoint;
mod definition;
mod history;
pub(crate) mod index;
mod index_ref;
pub(crate) mod spec;
pub(crate) mod storage;
pub(crate) mod table;

pub(crate) use checkpoint::*;
pub use checkpoint::{
    CatalogCheckpointOutcome, CatalogCheckpointReport, CatalogTableCheckpointChange,
    CatalogTableCheckpointIoStats,
};
pub(crate) use definition::*;
pub use definition::{
    BindingNamespaceID, DescriptorUpdate, MAX_TABLE_BINDING_KEY_BYTES, MAX_TABLE_DESCRIPTOR_BYTES,
    ManagedCreateTableDefinition, ManagedDdlError, ManagedDdlResult,
    ManagedTableDefinitionSnapshot, ManagedTableInterpreter, ResolvedTableBinding, TableBinding,
    TableDefinitionVersion,
};
pub(crate) use history::*;
pub(crate) use index::*;
pub(crate) use index_ref::*;
pub use index_ref::{
    ColumnID, ColumnOrdinal, ID_DOMAIN_END, IndexID, ResolvedTableIndex, TableIndex,
    TableIndexArgument, TableIndexSelector,
};
#[cfg(test)]
pub(crate) use spec::ActiveIndexSpec;
pub use spec::{
    CreateIndexDefinition, CreateTableDefinition, DropIndexDefinition, IndexOrder,
    StorageColumnDefinition, StorageColumnFlags, StorageColumnSpec, StorageIndexDefinition,
    StorageIndexFlags, StorageIndexKey, StorageIndexKeyByColumnId, StorageIndexSpec,
    StorageTableDefinition, StorageTableSpec,
};
pub(crate) use storage::layout::{catalog_table_id_from_slot, catalog_table_slot};
pub(crate) use storage::*;
pub use table::CreateTableOutcome;
pub(crate) use table::*;

use crate::DiskPool;
use crate::buffer::page::VersionedPageID;
use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuard, PoolGuards, PoolRole,
    ReadonlyBufferPool,
};
use crate::component::{Component, ComponentRegistry, MetaPool, ShelfScope};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, FatalError, OperationError, OperationResult,
    RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::fs::FileSystem;
use crate::id::{RowID, TableID, TrxID};
use crate::index::BlockIndex;
use crate::map::{FastDashMap, FastHashMap, FastHashSet};
use crate::poison::EnginePoisoner;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::row::Row;
use crate::table::{
    CreateIndexPlan, DropIndexPlan, IndexLookupCriteria, LiveTableRedoReplayFloor, MemTable, Table,
    TableDefinitionKind, TableRedoReplayFloor, TableRuntimeLayout,
};
use crate::trx::retention::PendingDroppedTableRedoFloor;
use crate::trx::undo::IndexUndo;
use crate::trx::{MIN_SNAPSHOT_TS, PrivateTransaction};
use dashmap::mapref::entry::Entry::{Occupied, Vacant};
use error_stack::{Report, ResultExt};
use std::collections::hash_map::Entry;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// First table id allocated to user-managed tables.
pub(crate) const USER_TABLE_ID_START: TableID = TableID::new(0);

/// First table id reserved for built-in catalog tables.
pub(crate) const CATALOG_TABLE_ID_START: TableID = TableID::CATALOG_START;

/// Exclusive upper bound of the user table-id range.
pub(crate) const USER_TABLE_ID_LIMIT: TableID = CATALOG_TABLE_ID_START;

/// Dedicated runtime wrapper for catalog logical tables.
pub(crate) struct CatalogTable {
    /// In-memory row store for this catalog table.
    pub(crate) mem: MemTable<FixedBufferPool, FixedBufferPool>,
}

impl CatalogTable {
    /// Build a catalog table runtime from catalog-specific construction inputs.
    #[inline]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<FixedBufferPool>,
        meta_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> RuntimeResult<Self> {
        let mem = MemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
            mem_pool,
            PoolRole::Meta,
            meta_pool_guard,
            table_id,
            metadata,
            blk_idx,
            MIN_SNAPSHOT_TS,
        )
        .await
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| format!("operation=create_catalog_table, table_id={table_id}"))?;
        Ok(CatalogTable { mem })
    }

    /// Visits raw current rows selected through one catalog index while the
    /// owning private DDL transaction retains stabilizing catalog locks.
    pub(crate) async fn index_lookup_current_locked<F>(
        &self,
        trx: &PrivateTransaction,
        index_slot: CatalogIndexNo,
        criteria: IndexLookupCriteria<'_>,
        row_action: F,
    ) -> RuntimeResult<()>
    where
        F: for<'m, 'p> FnMut(&'m TableColumnLayout, Row<'p>) -> bool,
    {
        if !trx.has_catalog_current_lock_authority(self.table_id()) {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "locked current catalog lookup lacks metadata-S or data-IX authority: table_id={}, index_slot={index_slot}",
                    self.table_id()
                ))
                .change_context(RuntimeError::CatalogAccess));
        }
        self.mem
            .catalog_index_lookup_current(trx.pool_guards(), index_slot, criteria, row_action)
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=index_lookup_current_locked, table_id={}, index_slot={index_slot}",
                    self.table_id()
                )
            })
    }
}

impl Deref for CatalogTable {
    type Target = MemTable<FixedBufferPool, FixedBufferPool>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}

/// Catalog startup options that must be available before transaction-system build.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CatalogConfig {
    /// Disable DML payload validation while bootstrapping checkpointed catalog rows.
    pub(crate) recovery_disable_dml_validation: bool,
}

impl CatalogConfig {
    /// Build catalog startup options from the transaction-system recovery policy.
    #[inline]
    pub(crate) fn new(recovery_disable_dml_validation: bool) -> Self {
        Self {
            recovery_disable_dml_validation,
        }
    }
}

struct IndexLayoutPublication<'a> {
    effective_cts: TrxID,
    expected_table: &'a Arc<Table>,
    expected_old_layout: &'a Arc<TableRuntimeLayout>,
    new_layout: TableRuntimeLayout,
}

/// Catalog contains metadata of user tables.
pub(crate) struct Catalog {
    next_table_id: AtomicU64,
    user_tables: FastDashMap<TableID, UserTableEntry>,
    /// Engine-level fatal runtime poison state used by catalog policy boundaries.
    pub(super) poisoner: QuiescentGuard<EnginePoisoner>,
    /// Persistent storage for built-in catalog tables.
    pub(crate) storage: CatalogStorage,
    checkpoint_gate: CatalogCheckpointGate,
}

impl Catalog {
    /// Create a catalog runtime from persisted catalog storage.
    #[inline]
    pub(crate) async fn new(
        storage: CatalogStorage,
        poisoner: QuiescentGuard<EnginePoisoner>,
        config: CatalogConfig,
        bootstrap_guards: &PoolGuards,
    ) -> RuntimeResult<Self> {
        let snapshot = storage.checkpoint_snapshot();
        storage
            .bootstrap_from_checkpoint(
                &snapshot,
                bootstrap_guards,
                config.recovery_disable_dml_validation,
            )
            .await?;
        let next_table_id = storage.next_table_id();
        Ok(Catalog {
            next_table_id: AtomicU64::new(next_table_id.as_u64()),
            user_tables: FastDashMap::default(),
            poisoner,
            storage,
            checkpoint_gate: CatalogCheckpointGate::new(),
        })
    }

    /// Allocate and return the next table id.
    #[inline]
    pub(crate) fn next_table_id(&self) -> TableID {
        let table_id = TableID::new(self.next_table_id.fetch_add(1, Ordering::SeqCst));
        assert!(
            table_id < USER_TABLE_ID_LIMIT,
            "user table id allocator overflowed into catalog table range: table_id={table_id}, limit={USER_TABLE_ID_LIMIT}"
        );
        table_id
    }

    #[inline]
    fn try_update_next_table_id(&self, next_table_id: TableID) {
        self.next_table_id
            .fetch_max(next_table_id.as_u64(), Ordering::SeqCst);
    }

    /// Return the current next table id without allocating one.
    #[inline]
    pub(crate) fn curr_next_table_id(&self) -> TableID {
        TableID::new(self.next_table_id.load(Ordering::Acquire))
    }

    /// Apply one scanned catalog checkpoint batch into `catalog.mtb`.
    ///
    /// # Panics
    ///
    /// Panics if another mutable writer is already active on the shared
    /// `catalog.mtb` `MultiTableFile`. Only one checkpoint publish may be in
    /// flight at a time per shared `CatalogStorage`/`MultiTableFile`; callers
    /// are responsible for ensuring mutual exclusion at a higher level (e.g.,
    /// a single background checkpoint task).
    #[inline]
    pub(crate) async fn apply_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        disk_guard: &PoolGuard,
    ) -> RuntimeOrFatalResult<CatalogCheckpointReport> {
        let prepared = self.prepare_checkpoint_batch(batch, disk_guard).await?;
        self.commit_prepared_checkpoint(prepared).await
    }

    /// Prepare one scanned catalog checkpoint batch for a later root commit.
    #[inline]
    pub(crate) async fn prepare_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        disk_guard: &PoolGuard,
    ) -> RuntimeOrFatalResult<PreparedCatalogCheckpoint> {
        self.storage
            .prepare_checkpoint_batch(batch, self.curr_next_table_id(), disk_guard)
            .await
    }

    /// Commit a prepared catalog root and apply its Table-local lifecycle event.
    pub(crate) async fn commit_prepared_checkpoint(
        &self,
        prepared: PreparedCatalogCheckpoint,
    ) -> RuntimeOrFatalResult<CatalogCheckpointReport> {
        let report = prepared
            .commit(&self.storage)
            .await
            .map_err(RuntimeOrFatalError::from)?;
        let CatalogCheckpointOutcome::Published {
            catalog_replay_start_ts,
        } = report.outcome
        else {
            return Ok(report);
        };
        for table in self.snapshot_live_user_tables() {
            if let Err(err) = table.apply_index_lifecycle_checkpoint(catalog_replay_start_ts) {
                let report = err
                    .change_context(FatalError::CatalogWrite)
                    .attach(format!(
                        "published catalog checkpoint lifecycle transition failed: table_id={}, catalog_replay_start_ts={catalog_replay_start_ts}",
                        table.table_id()
                    ));
                return Err(RuntimeOrFatalError::from(
                    self.poisoner.poison(report).into_report(),
                ));
            }
        }
        Ok(report)
    }

    /// Reload one user table runtime from catalog metadata and table file.
    ///
    /// Returns `true` when catalog metadata exactly matches the table-file root
    /// metadata. Returns `false` when the metadata differs only by a recoverable
    /// index-DDL gap and recovery must replay catalog index-DDL rows before final
    /// metadata validation.
    pub(crate) async fn reload_create_table(
        &self,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: &FileSystem,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<bool> {
        assert!(
            !self.user_tables.contains_key(&table_id),
            "catalog reload invariant violated: table runtime already exists, table_id={table_id}"
        );
        let (table, metadata_in_catalog) = self
            .user_table_metadata_from_catalog(guards, table_id)
            .await?;
        let definition_kind = if self
            .storage
            .table_descriptors()
            .find_uncommitted_by_table_id(guards, table_id)
            .await?
            .is_some()
        {
            TableDefinitionKind::Managed
        } else {
            TableDefinitionKind::Unmanaged
        };

        // Phase 2 allocator semantics: only table ids consume the global allocator.
        self.try_update_next_table_id(table.table_id.saturating_add(1));

        let table_file = table_fs
            .open_table_file(table.table_id, disk_pool.clone(), guards.disk_guard())
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=reload_create_table, phase=open_table_file, table_id={}",
                    table.table_id
                )
            })?;
        // `catalog_load_boundary`: loading a user table binds one root for
        // metadata validation and block-index initialization.
        let active_root = table_file.active_root_unchecked();
        let metadata_in_file = &*active_root.metadata;
        let metadata_matched = if &metadata_in_catalog == metadata_in_file {
            true
        } else if index_ddl_metadata_reconcilable(
            table.table_id,
            &metadata_in_catalog,
            metadata_in_file,
        )
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| {
            format!(
                "operation=reload_create_table, phase=reconcile_metadata, table_id={}",
                table.table_id
            )
        })? {
            false
        } else {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant))
                .attach("user table metadata mismatch outside index-DDL reconciliation")
                .change_context(RuntimeError::CatalogAccess)
                .attach(format!(
                    "operation=reload_create_table, phase=validate_metadata, table_id={}",
                    table.table_id
                ));
        };

        let row_id_bound = active_root.pivot_row_id;
        let meta_pool_guard = guards.meta_guard();
        let index_pool_guard = guards.index_guard();

        let blk_idx = BlockIndex::new(
            self.storage.meta_pool.clone(),
            meta_pool_guard,
            row_id_bound,
            active_root.column_block_index_root,
        )
        .await
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| {
            format!(
                "operation=reload_create_table, phase=build_block_index, table_id={}",
                table.table_id
            )
        })?;
        let table = Arc::new(
            Table::new(
                mem_pool.clone(),
                index_pool.clone(),
                index_pool_guard,
                table.table_id,
                definition_kind,
                blk_idx,
                table_file,
                disk_pool.clone(),
            )
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=reload_create_table, phase=build_table_runtime, table_id={table_id}"
                )
            })?,
        );
        let metadata = table.metadata();
        let old = self.user_tables.insert(
            table_id,
            UserTableEntry::new_live(TrxID::new(0), metadata, table),
        );
        assert!(
            old.is_none(),
            "catalog reload invariant violated: table runtime inserted concurrently, table_id={table_id}"
        );
        Ok(metadata_matched)
    }

    /// Build user-table metadata from current in-memory catalog rows.
    pub(crate) async fn user_table_metadata_from_catalog(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<(TableObject, TableMetadata)> {
        let table = self
            .storage
            .tables()
            .find_uncommitted_by_id(guards, table_id)
            .await?
            .unwrap_or_else(|| {
                panic!(
                    "catalog reconstruction invariant violated: known table row is missing, table_id={table_id}"
                )
            });

        // todo: use secondary index to improve performance
        let columns = self
            .storage
            .columns()
            .list_uncommitted_by_table_id(guards, table_id)
            .await?;
        let indexes = self
            .storage
            .indexes()
            .list_uncommitted_by_table_id(guards, table_id)
            .await?;
        let metadata = reconstruct_user_table_metadata(&table, columns, indexes)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!("operation=reconstruct_user_table_metadata, table_id={table_id}")
            })?;
        Ok((table, metadata))
    }

    /// Validates every managed descriptor against the current numeric schema.
    pub(crate) async fn validate_live_table_descriptors(
        &self,
        guards: &PoolGuards,
    ) -> RuntimeResult<()> {
        let descriptors = self
            .storage
            .table_descriptors()
            .list_uncommitted(guards)
            .await?;
        for descriptor in descriptors {
            let table_id = descriptor.table_id;
            let (_, metadata) = self
                .user_table_metadata_from_catalog(guards, table_id)
                .await?;
            validate_table_descriptor_against_metadata(&descriptor, table_id, &metadata)
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!("operation=validate_live_table_descriptors, table_id={table_id}")
                })?;
        }
        Ok(())
    }

    /// Get a user-table runtime handle by table id.
    #[inline]
    pub(crate) async fn get_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.get_table_now(table_id)
    }

    /// Get a user-table runtime handle synchronously by table id.
    #[inline]
    pub(crate) fn get_table_now(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.current_live_user_table(table_id)
    }

    /// Resolve user-table metadata visible to one transaction snapshot.
    #[inline]
    pub(crate) fn resolve_user_table_visible(
        &self,
        table_id: TableID,
        sts: TrxID,
    ) -> Option<ResolvedVisibleTableMetadata> {
        if table_id.is_catalog() {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .and_then(|entry| entry.value().resolve_visible(sts))
    }

    /// Resolve the direct current logical state without consulting history.
    #[inline]
    pub(crate) fn resolve_user_table_current(
        &self,
        table_id: TableID,
    ) -> Option<CurrentTableState> {
        if table_id.is_catalog() {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .and_then(|entry| entry.value().resolve_current())
    }

    /// Return the direct current live runtime without consulting history.
    #[inline]
    pub(crate) fn current_live_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.resolve_user_table_current(table_id)
            .and_then(|current| current.live_table().map(Arc::clone))
    }

    /// Snapshot all current live user-table runtimes without retaining map guards.
    pub(crate) fn snapshot_live_user_tables(&self) -> Vec<Arc<Table>> {
        let mut tables = self
            .user_tables
            .iter()
            .filter_map(|entry| entry.value().current_live_table())
            .collect::<Vec<_>>();
        tables.sort_by_key(|table| table.table_id().as_u64());
        tables
    }

    /// Returns a session-owned insert-page token to the exact current runtime.
    ///
    /// The catalog entry guard prevents DROP from replacing the live runtime
    /// while the token is returned. Comparing the cached weak identity with the
    /// borrowed current runtime avoids a transient strong owner racing dropped
    /// runtime destruction.
    #[inline]
    pub(crate) fn return_session_insert_page(
        &self,
        table_id: TableID,
        expected_table: &Weak<Table>,
        page_id: VersionedPageID,
    ) {
        let Some(entry) = self.user_tables.get(&table_id) else {
            return;
        };
        let Some(table) = entry.value().current_live_table_ref() else {
            return;
        };
        if ptr::eq(expected_table.as_ptr(), table) {
            table.mem.cache_insert_page_version(page_id);
        }
    }

    /// Pins a user-table runtime for checkpoint-retirement purge.
    ///
    /// Purge may race the catalog transition from live to retained dropped
    /// state. Both variants own the same runtime identity; a dropped floor or
    /// absent entry no longer has a runtime that can safely service the batch.
    #[inline]
    pub(crate) fn pin_user_table_for_purge(&self, table_id: TableID) -> Option<Arc<Table>> {
        if table_id.is_catalog() {
            return None;
        }
        self.user_tables
            .get(&table_id)
            .and_then(|entry| entry.value().runtime_for_purge())
    }

    /// Return sorted ids for currently loaded user-table runtimes.
    #[inline]
    pub(crate) fn list_user_table_ids_now(&self) -> Vec<TableID> {
        let mut table_ids = self
            .user_tables
            .iter()
            .filter_map(|entry| {
                let table_id = *entry.key();
                entry
                    .value()
                    .current_live_table_ref()
                    .is_some()
                    .then_some(table_id)
            })
            .collect::<Vec<_>>();
        table_ids.sort_by_key(|table_id| table_id.as_u64());
        table_ids
    }

    /// Copy replay floors from live and dropped user-table catalog entries.
    #[inline]
    pub(crate) fn snapshot_user_table_redo_floors(
        &self,
        catalog_replay_start_ts: TrxID,
    ) -> (
        Vec<LiveTableRedoReplayFloor>,
        Vec<PendingDroppedTableRedoFloor>,
    ) {
        let checkpointed_silent_watermarks = self.storage.checkpointed_silent_watermarks();
        self.snapshot_user_table_redo_floors_with_silent_watermarks(
            catalog_replay_start_ts,
            &checkpointed_silent_watermarks,
        )
    }

    /// Copy replay floors using an explicit checkpoint-durable silent overlay.
    #[inline]
    pub(crate) fn snapshot_user_table_redo_floors_with_silent_watermarks(
        &self,
        catalog_replay_start_ts: TrxID,
        checkpointed_silent_watermarks: &FastHashMap<TableID, TableRedoReplayFloor>,
    ) -> (
        Vec<LiveTableRedoReplayFloor>,
        Vec<PendingDroppedTableRedoFloor>,
    ) {
        let mut live = Vec::new();
        let mut dropped = Vec::new();
        for entry in &self.user_tables {
            let table_id = *entry.key();
            if let Some(floor) = entry
                .value()
                .live_replay_floor(checkpointed_silent_watermarks.get(&table_id).copied())
            {
                live.push(LiveTableRedoReplayFloor { table_id, floor });
            }
            if let Some((drop_cts, replay_floor)) = entry.value().dropped_replay_floor()
                && catalog_replay_start_ts <= drop_cts
            {
                dropped.push(PendingDroppedTableRedoFloor::new(
                    table_id,
                    drop_cts,
                    replay_floor,
                ));
            }
        }
        live.sort_by_key(|floor| floor.table_id.as_u64());
        dropped.sort_by_key(|floor| (floor.drop_cts.as_u64(), floor.table_id.as_u64()));
        (live, dropped)
    }

    /// Compute the checkpoint-durable root-plus-overlay replay floor for one table.
    #[inline]
    pub(crate) fn effective_user_table_redo_replay_floor(
        &self,
        table_id: TableID,
        root_floor: TableRedoReplayFloor,
    ) -> TableRedoReplayFloor {
        let checkpointed_silent_watermarks = self.storage.checkpointed_silent_watermarks();
        effective_table_redo_replay_floor(
            root_floor,
            checkpointed_silent_watermarks.get(&table_id).copied(),
        )
    }

    /// Acquires transferable index-DDL metadata admission.
    #[inline]
    pub(crate) async fn acquire_index_metadata_change(&self) {
        self.checkpoint_gate.acquire_metadata_change().await;
    }

    /// Releases transferable index-DDL metadata admission.
    #[inline]
    pub(crate) fn release_index_metadata_change(&self) {
        self.checkpoint_gate.release_metadata_change();
    }

    /// Validates that a user-table runtime exists and still admits foreground work.
    #[inline]
    pub(crate) async fn validate_user_table_live(
        &self,
        table_id: TableID,
    ) -> OperationResult<Arc<Table>> {
        let table = self.get_table(table_id).await.ok_or_else(|| {
            Report::new(OperationError::TableNotFound).attach(format!("table_id={table_id}"))
        })?;
        table.check_foreground_live()?;
        Ok(table)
    }

    /// Get a catalog-table runtime handle by table id.
    #[inline]
    pub(crate) fn get_catalog_table(&self, table_id: TableID) -> Option<Arc<CatalogTable>> {
        self.storage.get_catalog_table(table_id)
    }

    /// Insert a user table runtime into the in-memory cache.
    #[inline]
    pub(crate) fn insert_user_table(&self, effective_cts: TrxID, table: Arc<Table>) -> bool {
        let table_id = table.table_id();
        let metadata = table.metadata();
        match self.user_tables.entry(table_id) {
            Vacant(entry) => {
                entry.insert(UserTableEntry::new_live(effective_cts, metadata, table));
                true
            }
            Occupied(_) => false,
        }
    }

    /// Remove one recovery-only live current entry from the in-memory cache.
    #[inline]
    pub(crate) fn remove_live_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        match self.user_tables.entry(table_id) {
            Occupied(entry) if entry.get().current_live_table_ref().is_some() => {
                Some(entry.remove().into_recovery_live_table())
            }
            Occupied(_) => None,
            Vacant(_) => None,
        }
    }

    /// Atomically installs one CREATE layout and publishes its catalog history.
    #[inline]
    pub(crate) fn install_created_index_layout_and_publish_history(
        &self,
        effective_cts: TrxID,
        plan: &CreateIndexPlan,
        new_layout: TableRuntimeLayout,
        #[cfg(test)] test_hook: &IndexDdlTestController,
    ) -> Option<Arc<TableRuntimeLayout>> {
        let publication = IndexLayoutPublication {
            effective_cts,
            expected_table: plan.table(),
            expected_old_layout: plan.old_layout(),
            new_layout,
        };
        self.install_index_layout_and_publish_history(
            publication,
            |new_layout| {
                plan.table().try_install_created_index(
                    plan.old_layout(),
                    new_layout,
                    plan.placement(),
                    plan.index(),
                )
            },
            #[cfg(test)]
            (test_hook, IndexDdlKind::Create),
        )
    }

    /// Atomically installs one DROP layout and publishes its catalog history.
    #[inline]
    pub(crate) fn install_dropped_index_layout_and_publish_history(
        &self,
        effective_cts: TrxID,
        plan: &DropIndexPlan,
        new_layout: TableRuntimeLayout,
        #[cfg(test)] test_hook: &IndexDdlTestController,
    ) -> Option<Arc<TableRuntimeLayout>> {
        let publication = IndexLayoutPublication {
            effective_cts,
            expected_table: plan.table(),
            expected_old_layout: plan.old_layout(),
            new_layout,
        };
        self.install_index_layout_and_publish_history(
            publication,
            |new_layout| {
                plan.table().try_install_dropped_index(
                    plan.old_layout(),
                    new_layout,
                    plan.index(),
                    effective_cts,
                )
            },
            #[cfg(test)]
            (test_hook, IndexDdlKind::Drop),
        )
    }

    /// Atomically coordinates typed index layout and catalog-history publication.
    ///
    /// The occupied catalog entry is held before the table layout mutex, which
    /// is the same nesting order used by metadata-history purge.
    fn install_index_layout_and_publish_history(
        &self,
        publication: IndexLayoutPublication<'_>,
        install_layout: impl FnOnce(Arc<TableRuntimeLayout>) -> Option<()>,
        #[cfg(test)] test_hook: (&IndexDdlTestController, IndexDdlKind),
    ) -> Option<Arc<TableRuntimeLayout>> {
        let IndexLayoutPublication {
            effective_cts,
            expected_table,
            expected_old_layout,
            new_layout,
        } = publication;
        let table_id = expected_table.table_id();
        new_layout.assert_valid();
        let new_layout = Arc::new(new_layout);
        match self.user_tables.entry(table_id) {
            Occupied(mut entry) => {
                let expected_metadata = expected_old_layout.metadata_arc();
                if !entry.get_mut().prepare_publish_live(
                    effective_cts,
                    expected_table,
                    expected_metadata,
                ) {
                    return None;
                }
                install_layout(Arc::clone(&new_layout))?;
                #[cfg(test)]
                test_hook.0.reach_publication_interval(test_hook.1);
                entry
                    .get_mut()
                    .commit_publish_live(effective_cts, Arc::clone(new_layout.metadata_arc()));
            }
            Vacant(_) => return None,
        }
        Some(new_layout)
    }

    /// Publish a tombstone and retain the dropped runtime operationally.
    #[inline]
    pub(crate) fn mark_user_table_dropped_runtime(
        &self,
        table_id: TableID,
        table: Arc<Table>,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    ) -> bool {
        match self.user_tables.entry(table_id) {
            Occupied(mut entry) => entry.get_mut().publish_drop(drop_cts, table, replay_floor),
            Vacant(_) => false,
        }
    }

    /// Insert a lightweight retained dropped-table replay floor.
    #[inline]
    pub(crate) fn insert_dropped_table_floor(
        &self,
        table_id: TableID,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    ) {
        match self.user_tables.entry(table_id) {
            Vacant(entry) => {
                entry.insert(UserTableEntry::new_dropped_floor(drop_cts, replay_floor));
            }
            Occupied(_) => panic!(
                "recovery dropped-floor invariant violated: table entry still exists after runtime removal, table_id={table_id}, drop_cts={drop_cts}"
            ),
        }
    }

    /// Detach purge-horizon dropped runtimes while leaving replay floors visible.
    #[inline]
    pub(crate) fn take_dropped_runtime_candidates(
        &self,
        min_active_sts: TrxID,
    ) -> Vec<DroppedTableRuntime> {
        let mut table_ids = self
            .user_tables
            .iter()
            .filter_map(|entry| {
                entry
                    .value()
                    .dropped_replay_floor()
                    .is_some_and(|(drop_cts, _)| drop_cts < min_active_sts)
                    .then_some(*entry.key())
            })
            .collect::<Vec<_>>();
        table_ids.sort_by_key(|table_id| table_id.as_u64());

        let mut candidates = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            if let Occupied(mut entry) = self.user_tables.entry(table_id) {
                let Some((table, drop_cts, replay_floor)) =
                    entry.get_mut().take_dropped_runtime(min_active_sts)
                else {
                    continue;
                };
                candidates.push(DroppedTableRuntime {
                    table_id,
                    drop_cts,
                    replay_floor,
                    table,
                });
            }
        }
        candidates
    }

    /// Snapshot retained dropped floors for purge file-cleanup queue seeding.
    ///
    /// This is the intended catalog-map scan for file cleanup: recovery and
    /// startup rebuild the lightweight purge queue from these authoritative
    /// floor entries, then normal purge wakeups work from that queue.
    #[inline]
    pub(crate) fn snapshot_dropped_table_file_cleanups(&self) -> Vec<DroppedTableFileCleanup> {
        let mut candidates = self
            .user_tables
            .iter()
            .filter_map(|entry| {
                entry
                    .value()
                    .dropped_floor()
                    .map(|(drop_cts, replay_floor)| {
                        DroppedTableFileCleanup::new(*entry.key(), drop_cts, replay_floor)
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|item| (item.drop_cts.as_u64(), item.table_id.as_u64()));
        candidates
    }

    /// Remove a dropped-floor entry after its table-file cleanup succeeds.
    #[inline]
    pub(crate) fn remove_dropped_floor(&self, item: DroppedTableFileCleanup) -> bool {
        match self.user_tables.entry(item.table_id) {
            Occupied(mut entry) => {
                if !entry
                    .get_mut()
                    .remove_dropped_floor(item.drop_cts, item.replay_floor)
                {
                    return false;
                }
                if entry.get().is_empty() {
                    let _ = entry.remove();
                }
                true
            }
            Vacant(_) => false,
        }
    }

    /// Purge metadata history against the authoritative transaction horizon.
    #[inline]
    pub(crate) fn purge_user_table_history(&self, min_active_sts: TrxID) {
        let mut table_ids = self
            .user_tables
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        table_ids.sort_by_key(|table_id| table_id.as_u64());
        for table_id in table_ids {
            if let Occupied(mut entry) = self.user_tables.entry(table_id) {
                entry.get_mut().purge_history(min_active_sts);
                if entry.get().is_empty() {
                    let _ = entry.remove();
                }
            }
        }
    }

    /// Return the retained logical metadata-version count for test assertions.
    #[cfg(test)]
    #[inline]
    pub(crate) fn user_table_history_version_count(&self, table_id: TableID) -> Option<usize> {
        self.user_tables
            .get(&table_id)
            .and_then(|entry| entry.value().history_version_count())
    }

    /// Return retained dropped table ids that should protect files from startup cleanup.
    #[inline]
    pub(crate) fn retained_dropped_table_ids_now(&self) -> Vec<TableID> {
        let mut table_ids = self
            .user_tables
            .iter()
            .filter_map(|entry| {
                entry
                    .value()
                    .has_dropped_operational_state()
                    .then_some(*entry.key())
            })
            .collect::<Vec<_>>();
        table_ids.sort_by_key(|table_id| table_id.as_u64());
        table_ids
    }
}

impl Component for Catalog {
    type Config = CatalogConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Report<RuntimeError>;

    const NAME: &'static str = "catalog";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> RuntimeResult<()> {
        let meta_pool = registry.dependency::<MetaPool>();
        let table_fs = registry.dependency::<FileSystem>();
        let disk_pool = registry.dependency::<DiskPool>();
        let poisoner = registry.dependency::<EnginePoisoner>();
        // Catalog bootstrap runs before sessions exist. Create one explicit
        // component-build root per required pool and thread that bundle through
        // file loading and catalog-table initialization.
        let bootstrap_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.create_base_guard())
            .push(PoolRole::Disk, disk_pool.create_base_guard())
            .build();
        let storage = CatalogStorage::new(
            meta_pool.clone_inner(),
            table_fs.clone(),
            disk_pool.clone_inner(),
            &bootstrap_guards,
        )
        .await?;
        registry
            .register::<Self>(Catalog::new(storage, poisoner, config, &bootstrap_guards).await?);
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {
        // Panic safety: foreground catalog users are drained before component
        // dispatch, and purge workers stop before catalog owner release.
    }
}

/// Dropped table runtime detached from the catalog map for purge destruction.
pub(crate) struct DroppedTableRuntime {
    /// Dropped user table id.
    pub(crate) table_id: TableID,
    /// Commit timestamp of the logical DROP TABLE.
    pub(crate) drop_cts: TrxID,
    /// Replay floor copied before the table can be destroyed.
    pub(crate) replay_floor: TableRedoReplayFloor,
    /// Runtime table handle retained until purge can destroy it.
    pub(crate) table: Arc<Table>,
}

/// Dropped table floor whose file can be deleted after catalog absence is durable.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DroppedTableFileCleanup {
    /// Dropped user table id.
    pub(crate) table_id: TableID,
    /// Commit timestamp of the logical DROP TABLE.
    pub(crate) drop_cts: TrxID,
    replay_floor: TableRedoReplayFloor,
}

impl DroppedTableFileCleanup {
    /// Create a dropped-table file cleanup item.
    #[inline]
    pub(crate) fn new(
        table_id: TableID,
        drop_cts: TrxID,
        replay_floor: TableRedoReplayFloor,
    ) -> Self {
        Self {
            table_id,
            drop_cts,
            replay_floor,
        }
    }
}

/// User-table cache entry used by rollback and purge paths.
///
/// Row-only paths can use the table directly. Index rollback and purge use this
/// entry to lazily pin one user-table layout snapshot for repeated same-table
/// index operations. A purge cycle may cache a layout before a later DROP INDEX
/// publishes a new inactive slot; that is safe only because RFC 0018 keeps
/// the index slot stable and non-reused. If the cached/current layout sees the slot
/// inactive, index purge is a no-op; if it still sees the old slot active, it
/// can only touch the old runtime/root identity for that same stable slot.
pub(crate) struct UserTableCacheEntry {
    table: Arc<Table>,
    user_layout: Option<Arc<TableRuntimeLayout>>,
}

impl UserTableCacheEntry {
    #[inline]
    fn new(table: Arc<Table>) -> Self {
        UserTableCacheEntry {
            table,
            user_layout: None,
        }
    }

    /// Returns the bound user-table runtime.
    #[inline]
    pub(crate) fn table(&self) -> &Table {
        self.table.as_ref()
    }

    /// Roll back one secondary-index undo entry through the bound user table.
    #[inline]
    pub(crate) async fn rollback_index_entry(
        &mut self,
        entry: &IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> RuntimeResult<()> {
        let table = &self.table;
        let layout = self
            .user_layout
            .get_or_insert_with(|| table.layout_snapshot());
        table
            .rollback_index_entry_with_layout(layout, entry, guards, ts)
            .await
    }

    /// Delete one user secondary-index entry if it is no longer needed.
    #[inline]
    pub(crate) async fn delete_index(
        &mut self,
        guards: &PoolGuards,
        key: &ResolvedIndexKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let table = &self.table;
        let layout = self
            .user_layout
            .get_or_insert_with(|| table.layout_snapshot());
        table
            .accessor_with_layout(layout.as_ref())
            .delete_index(guards, key.index, &key.vals, row_id, unique, min_active_sts)
            .await
    }
}

/// Per-operation table cache used by rollback/recovery paths.
pub(crate) struct TableCache<'a> {
    catalog: &'a Catalog,
    user_tables: FastHashMap<TableID, UserTableCacheEntry>,
    catalog_tables: FastHashMap<TableID, Arc<CatalogTable>>,
    missing: FastHashSet<TableID>,
}

impl<'a> TableCache<'a> {
    /// Create an empty table cache bound to one catalog instance.
    #[inline]
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        TableCache {
            catalog,
            user_tables: FastHashMap::default(),
            catalog_tables: FastHashMap::default(),
            missing: FastHashSet::default(),
        }
    }

    /// Returns cached user table for given id.
    ///
    /// If table is not cached, this method loads it from catalog and caches
    /// positive/negative lookup result.
    #[inline]
    pub(crate) async fn get_user_table(&mut self, table_id: TableID) -> Option<&Table> {
        self.get_user_entry_mut(table_id)
            .await
            .map(|binding| binding.table())
    }

    /// Returns cached catalog table for given id.
    ///
    /// If table is not cached, this method loads it from catalog and caches
    /// positive/negative lookup result.
    #[inline]
    pub(crate) fn get_catalog_table(&mut self, table_id: TableID) -> Option<&CatalogTable> {
        if !table_id.is_catalog() {
            return None;
        }
        match self.catalog_tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                match self.catalog.get_catalog_table(table_id) {
                    Some(table) => Some(Arc::as_ref(vac.insert(table))),
                    None => {
                        let _ = self.missing.insert(table_id);
                        None
                    }
                }
            }
            Entry::Occupied(occ) => Some(Arc::as_ref(occ.into_mut())),
        }
    }

    /// Returns cached user-table entry for given id.
    ///
    /// Index maintenance paths use this mutable entry to lazily pin one
    /// user-table layout snapshot for repeated same-table index operations.
    #[inline]
    pub(crate) async fn get_user_entry_mut(
        &mut self,
        table_id: TableID,
    ) -> Option<&mut UserTableCacheEntry> {
        if table_id.is_catalog() {
            return None;
        }
        match self.user_tables.entry(table_id) {
            Entry::Vacant(vac) => {
                if self.missing.contains(&table_id) {
                    return None;
                }
                match self.catalog.get_table(table_id).await {
                    Some(table) => {
                        let res = vac.insert(UserTableCacheEntry::new(table));
                        Some(res)
                    }
                    None => {
                        let _ = self.missing.insert(table_id);
                        None
                    }
                }
            }
            Entry::Occupied(occ) => {
                let res = occ.into_mut();
                Some(res)
            }
        }
    }

    /// Returns cached user table and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) async fn must_get_user_table(&mut self, table_id: TableID) -> &Table {
        match self.get_user_table(table_id).await {
            Some(table) => table,
            None => panic!("table {table_id} not found in catalog"),
        }
    }

    /// Returns cached catalog table and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) fn must_get_catalog_table(&mut self, table_id: TableID) -> &CatalogTable {
        match self.get_catalog_table(table_id) {
            Some(table) => table,
            None => panic!("table {table_id} not found in catalog"),
        }
    }

    /// Returns cached user-table entry and requires table to exist.
    ///
    /// This method is intended for rollback paths where table id in undo log
    /// must always map to an existing table.
    #[inline]
    pub(crate) async fn must_get_user_entry_mut(
        &mut self,
        table_id: TableID,
    ) -> &mut UserTableCacheEntry {
        match self.get_user_entry_mut(table_id).await {
            Some(entry) => entry,
            None => panic!("table {table_id} not found in catalog"),
        }
    }
}

/// Combine table-root replay bounds with a checkpoint-durable silent overlay.
#[inline]
pub(crate) fn effective_table_redo_replay_floor(
    root_floor: TableRedoReplayFloor,
    checkpointed_silent: Option<TableRedoReplayFloor>,
) -> TableRedoReplayFloor {
    checkpointed_silent.map_or(root_floor, |silent| TableRedoReplayFloor {
        heap_redo_start_ts: root_floor.heap_redo_start_ts.max(silent.heap_redo_start_ts),
        deletion_cutoff_ts: root_floor.deletion_cutoff_ts.max(silent.deletion_cutoff_ts),
    })
}

/// Reconstructs one canonical numeric schema from decoded catalog objects.
pub(crate) fn reconstruct_user_table_metadata(
    table: &TableObject,
    mut columns: Vec<ColumnObject>,
    mut indexes: Vec<IndexObject>,
) -> DataIntegrityResult<TableMetadata> {
    let table_id = table.table_id;
    columns.sort_by_key(|column| column.storage_ordinal);
    let column_metadata = columns
        .iter()
        .map(|column| TableColumnMetadata {
            id: column.column_id,
            ordinal: column.storage_ordinal,
            value_kind: column.value_kind,
            flags: column.value_flags,
        })
        .collect::<Vec<_>>();
    let ordinal_by_id = columns
        .iter()
        .map(|column| (column.column_id, column.storage_ordinal))
        .collect::<FastHashMap<_, _>>();

    indexes.sort_by_key(|index| index.index.id());
    let mut index_metadata = Vec::with_capacity(indexes.len());
    for index in indexes {
        let keys = index
            .keys
            .iter()
            .map(|key| {
                let column_ordinal = ordinal_by_id.get(&key.column_id).copied().ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "catalog index references missing column: table_id={table_id}, index={}, column_id={}",
                        index.index, key.column_id
                    ))
                })?;
                Ok(TableIndexKeySpec {
                    column_id: key.column_id,
                    column_ordinal,
                    order: key.order,
                })
            })
            .collect::<DataIntegrityResult<Vec<_>>>()?
            .into_boxed_slice();
        index_metadata.push(TableIndexMetadata {
            index: index.index,
            flags: index.index_flags,
            keys,
        });
    }
    TableMetadata::try_from_persisted_parts(
        table.storage_epoch,
        table.next_column_id,
        column_metadata,
        table.next_index_id,
        table.index_slot_count,
        index_metadata,
    )
}

#[inline]
fn index_ddl_metadata_reconcilable(
    table_id: TableID,
    catalog: &TableMetadata,
    file: &TableMetadata,
) -> DataIntegrityResult<bool> {
    // The active table root may be ahead of checkpointed catalog rows: recovery
    // can replay later index-DDL catalog rows to make catalog metadata catch up.
    // The opposite direction is unrecoverable here because replay cannot make a
    // table root that has already been opened acquire missing allocation state.
    if file.idx.index_slot_count_u32() < catalog.idx.index_slot_count_u32() {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "index-DDL reconciliation found catalog allocation ahead of table root: table_id={table_id}, catalog_index_slot_count={}, file_index_slot_count={}",
                catalog.idx.index_slot_count_u32(),
                file.idx.index_slot_count_u32()
            )));
    }
    if catalog.col != file.col {
        return Ok(false);
    }

    let max_slots = catalog
        .idx
        .index_slot_count()
        .max(file.idx.index_slot_count());
    for index_slot in 0..max_slots {
        let index_slot = IndexSlot::try_from(index_slot).unwrap_or_else(|_| {
            panic!("validated metadata index slot exceeds u16: index_slot={index_slot}")
        });
        let catalog_spec = catalog.idx.index_spec(index_slot);
        let file_spec = file.idx.index_spec(index_slot);
        if let (Some(catalog_spec), Some(file_spec)) = (catalog_spec, file_spec)
            && catalog_spec != file_spec
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::CatalogCheckpointScanStopReason;
    use crate::catalog::{
        StorageColumnFlags, StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec,
    };
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{CompletionErrorBridge, DataIntegrityError, Error};
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::index::{COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex};
    use crate::table::tests::assert_freeze_created;
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::trx::purge::PurgeTestEvent;
    use crate::value::{Val, ValKind};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// Asserts dropped table runtime in tests.
    #[inline]
    pub(crate) fn assert_dropped_table_runtime(catalog: &Catalog, table_id: TableID) {
        assert!(catalog.retained_dropped_table_ids_now().contains(&table_id));
        assert!(
            !catalog
                .snapshot_dropped_table_file_cleanups()
                .iter()
                .any(|item| item.table_id == table_id)
        );
    }

    /// Asserts dropped table floor in tests.
    #[inline]
    pub(crate) fn assert_dropped_table_floor(catalog: &Catalog, table_id: TableID) {
        assert!(catalog.retained_dropped_table_ids_now().contains(&table_id));
        assert!(
            catalog
                .snapshot_dropped_table_file_cleanups()
                .iter()
                .any(|item| item.table_id == table_id)
        );
    }

    /// Waits for one targeted purge cycle to convert a dropped runtime to a floor.
    pub(crate) async fn wait_for_dropped_table_floor(engine: &Engine, table_id: TableID) {
        let (event_tx, event_rx) = flume::unbounded();
        engine.inner().trx_sys.set_purge_test_observer(event_tx);
        if engine
            .inner()
            .core
            .catalog()
            .snapshot_dropped_table_file_cleanups()
            .iter()
            .any(|item| item.table_id == table_id)
        {
            return;
        }

        engine.inner().trx_sys.request_dropped_table_purge();
        let mut dropped_table_started = false;
        loop {
            match event_rx.recv_async().await.unwrap() {
                PurgeTestEvent::DroppedTableStarted => dropped_table_started = true,
                PurgeTestEvent::CycleCompleted if dropped_table_started => {
                    if engine
                        .inner()
                        .core
                        .catalog()
                        .snapshot_dropped_table_file_cleanups()
                        .iter()
                        .any(|item| item.table_id == table_id)
                    {
                        break;
                    }
                    dropped_table_started = false;
                    engine.inner().trx_sys.request_dropped_table_purge();
                }
                _ => {}
            }
        }
        assert_dropped_table_floor(engine.inner().core.catalog(), table_id);
    }

    /// Waits for targeted purge completion after dropped-table file cleanup becomes eligible.
    pub(crate) async fn wait_for_no_dropped_table_operational_state(
        engine: &Engine,
        table_id: TableID,
    ) {
        let (event_tx, event_rx) = flume::unbounded();
        engine.inner().trx_sys.set_purge_test_observer(event_tx);
        while engine
            .inner()
            .core
            .catalog()
            .retained_dropped_table_ids_now()
            .contains(&table_id)
        {
            engine.inner().trx_sys.request_dropped_table_purge();
            let mut dropped_table_started = false;
            loop {
                match event_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::DroppedTableStarted => dropped_table_started = true,
                    PurgeTestEvent::CycleCompleted if dropped_table_started => break,
                    _ => {}
                }
            }
        }
        assert_no_dropped_table_operational_state(engine.inner().core.catalog(), table_id);
    }

    /// Asserts no dropped table operational state in tests.
    #[inline]
    pub(crate) fn assert_no_dropped_table_operational_state(catalog: &Catalog, table_id: TableID) {
        assert!(!catalog.retained_dropped_table_ids_now().contains(&table_id));
    }

    /// Provides test-only access to `catalog_test_engine_config`.
    #[inline]
    pub(crate) fn catalog_test_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: Option<&str>,
    ) -> EngineConfig {
        let mut trx = TrxSysConfig::default();
        if let Some(log_file_stem) = log_file_stem {
            trx = trx.log_file_stem(log_file_stem);
        }
        EngineConfig::default().storage_root(main_dir).trx(trx)
    }

    /// Opens catalog test engine for tests.
    #[inline]
    pub(crate) async fn open_catalog_test_engine(
        main_dir: impl Into<PathBuf>,
        log_file_stem: Option<&str>,
    ) -> Engine {
        Engine::bootstrap(catalog_test_engine_config(main_dir, log_file_stem))
            .await
            .unwrap()
    }

    /// Executes catalog test engine error and verifies the expected test outcome.
    #[inline]
    pub(crate) async fn expect_catalog_test_engine_error(
        main_dir: impl Into<PathBuf>,
        log_file_stem: Option<&str>,
        expected_message: &str,
    ) -> Error {
        match Engine::bootstrap(catalog_test_engine_config(main_dir, log_file_stem)).await {
            Ok(_) => panic!("{expected_message}"),
            Err(err) => err,
        }
    }

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec {
                    columns: vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )],
                },
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .await
            .unwrap()
            .table_id();

        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec {
                    columns: vec![
                        StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ],
                },
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .await
            .unwrap()
            .table_id();

        drop(session);
        table_id
    }

    /// Table3 has single string key column.
    #[inline]
    pub(crate) async fn table3(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();

        let table_id = session
            .create_table(
                StorageTableSpec {
                    columns: vec![StorageColumnSpec::new(
                        ValKind::VarByte,
                        StorageColumnFlags::empty(),
                    )],
                },
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .await
            .unwrap()
            .table_id();

        drop(session);
        table_id
    }

    /// Table4 has two i32 columns.
    /// First is unique index.
    /// Second is non-unique index.
    #[inline]
    pub(crate) async fn table4(engine: &Engine) -> TableID {
        let mut session = engine.new_session().unwrap();

        let table_id = session
            .create_table(
                StorageTableSpec {
                    columns: vec![
                        StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    ],
                },
                vec![
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        // unique index.
                        StorageIndexFlags::UK,
                    ),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        // non-unique index.
                        StorageIndexFlags::empty(),
                    ),
                ],
            )
            .await
            .unwrap()
            .table_id();

        drop(session);
        table_id
    }

    fn corrupt_page_checksum(path: impl AsRef<Path>, page_id: u64) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64 + (COW_FILE_PAGE_SIZE as u64 - 1);
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();
    }

    fn rewrite_page_with_checksum(
        path: impl AsRef<Path>,
        page_id: u64,
        rewrite: impl FnOnce(&mut [u8]),
    ) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64;
        let mut page = vec![0u8; COW_FILE_PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        rewrite(&mut page);
        write_block_checksum(&mut page);
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&page).unwrap();
        file.flush().unwrap();
    }

    fn corrupt_leaf_delete_codec(path: impl AsRef<Path>, page_id: u64, prefix_idx: usize) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 35;
            page[byte_offset] = 0xFF;
        });
    }

    fn leaf_entry_payload_offset(page: &[u8], prefix_idx: usize) -> usize {
        const SEARCH_TYPE_PLAIN: u8 = 1;
        const SEARCH_TYPE_DELTA_U32: u8 = 2;
        const SEARCH_TYPE_DELTA_U16: u8 = 3;

        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let search_type = page[payload_start + COLUMN_BLOCK_HEADER_SIZE];
        let (prefix_size, entry_offset_offset) = match search_type {
            SEARCH_TYPE_PLAIN => (10usize, 8usize),
            SEARCH_TYPE_DELTA_U32 => (6usize, 4usize),
            SEARCH_TYPE_DELTA_U16 => (4usize, 2usize),
            _ => panic!("invalid leaf search type {search_type}"),
        };
        let prefix_offset =
            payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + prefix_idx * prefix_size;
        let entry_offset = u16::from_le_bytes(
            page[prefix_offset + entry_offset_offset..prefix_offset + entry_offset_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + entry_offset
    }

    fn assert_catalog_data_integrity(err: Error) {
        let report = format!("{err:?}");
        assert!(
            err.report().downcast_ref::<DataIntegrityError>().is_some(),
            "{report}"
        );
        assert!(!report.contains("propagate from other threads"), "{report}");
        assert!(
            err.report()
                .downcast_ref::<CompletionErrorBridge>()
                .is_none(),
            "{report}"
        );
    }

    #[test]
    fn test_catalog_table_id_boundary_predicates() {
        let last_user = TableID::new(USER_TABLE_ID_LIMIT.as_u64() - 1);
        assert!(USER_TABLE_ID_START.is_user());
        assert!(!USER_TABLE_ID_START.is_catalog());
        assert!(last_user.is_user());
        assert!(!last_user.is_catalog());
        assert!(!CATALOG_TABLE_ID_START.is_user());
        assert!(CATALOG_TABLE_ID_START.is_catalog());
        for (slot, table_id) in [
            TABLE_ID_TABLES,
            TABLE_ID_COLUMNS,
            TABLE_ID_INDEXES,
            TABLE_ID_TABLE_DESCRIPTORS,
            TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS,
            TABLE_ID_TABLE_BINDINGS,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(table_id, catalog_table_id_from_slot(slot));
            assert_eq!(catalog_table_slot(table_id), Some(slot));
        }
    }

    #[test]
    #[should_panic(expected = "user table id allocator overflowed into catalog table range")]
    fn test_next_table_id_panics_at_catalog_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(
                temp_dir.path().to_path_buf(),
                Some("catalog-allocator-overflow"),
            )
            .await;
            engine
                .inner()
                .core
                .catalog()
                .next_table_id
                .store(USER_TABLE_ID_LIMIT.as_u64(), Ordering::SeqCst);
            let _ = engine.inner().core.catalog().next_table_id();
        });
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_rejects_column_attribute_mismatch() {
        let catalog_metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I32,
                StorageColumnFlags::empty(),
            )],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            )],
        )
        .expect("valid table metadata");
        let file_metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I32,
                StorageColumnFlags::NULLABLE,
            )],
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            )],
        )
        .expect("valid table metadata");
        assert_ne!(catalog_metadata.col, file_metadata.col);
        assert!(
            !index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap()
        );
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_allows_file_ahead_of_catalog() {
        let columns = || {
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            ]
        };
        let primary_index =
            || StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK);
        let secondary_index =
            || StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty());

        let catalog_metadata =
            TableMetadata::try_new(columns(), vec![primary_index()]).expect("valid table metadata");
        let file_metadata =
            TableMetadata::try_new(columns(), vec![primary_index(), secondary_index()])
                .expect("valid table metadata");

        assert!(
            file_metadata.idx.index_slot_count_u32() > catalog_metadata.idx.index_slot_count_u32()
        );
        assert!(
            index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap()
        );
    }

    #[test]
    fn test_index_ddl_metadata_reconcilable_errors_when_catalog_ahead_of_file() {
        let columns = || {
            vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            ]
        };
        let primary_index =
            || StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK);
        let secondary_index =
            || StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty());

        let catalog_metadata =
            TableMetadata::try_new(columns(), vec![primary_index(), secondary_index()])
                .expect("valid table metadata");
        let file_metadata =
            TableMetadata::try_new(columns(), vec![primary_index()]).expect("valid table metadata");

        assert!(
            catalog_metadata.idx.index_slot_count_u32() > file_metadata.idx.index_slot_count_u32()
        );
        let err =
            index_ddl_metadata_reconcilable(TableID::new(42), &catalog_metadata, &file_metadata)
                .unwrap_err();
        assert_eq!(
            err.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        let report = format!("{err:?}");
        assert!(report.contains("table_id=42"), "{report}");
        assert!(report.contains("catalog_index_slot_count=2"), "{report}");
        assert!(report.contains("file_index_slot_count=1"), "{report}");
    }

    #[test]
    fn test_bootstrap_creates_catalog_mtb_without_catalog_tbl_files() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir.clone(), None).await;
            drop(engine);

            let data_dir = temp_dir.path();
            assert!(data_dir.join("catalog.mtb").exists());
            for table_id in 0..4u64 {
                assert!(!data_dir.join(format!("{table_id}.tbl")).exists());
            }
        });
    }

    #[test]
    fn test_next_table_id_monotonic_across_restart() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir.clone(), Some("catalog-allocator")).await;
            assert_eq!(
                engine.inner().core.catalog().curr_next_table_id(),
                USER_TABLE_ID_START
            );
            let mut session = engine.new_session().unwrap();
            let table_spec = StorageTableSpec::new(vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            ]);
            let index_specs = vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(1), StorageIndexKey::new(2)],
                    StorageIndexFlags::empty(),
                ),
            ];
            let table_id1 = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
            assert_eq!(
                engine.inner().core.catalog().curr_next_table_id(),
                table_id1 + 1
            );
            drop(session);
            drop(engine);

            let engine = open_catalog_test_engine(main_dir, Some("catalog-allocator")).await;
            assert_eq!(
                engine.inner().core.catalog().curr_next_table_id(),
                table_id1 + 1
            );
            let table_id2 = table1(&engine).await;
            assert!(table_id1 >= USER_TABLE_ID_START);
            assert_eq!(table_id2, table_id1 + 1);
            drop(engine);
        });
    }

    #[test]
    fn test_index_slot_count_persists_across_restart_and_catalog_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_stem = "stable-index-metadata";

            let engine = open_catalog_test_engine(main_dir.clone(), Some(log_stem)).await;
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec {
                        columns: vec![
                            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                            StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                        ],
                    },
                    vec![
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ],
                )
                .await
                .unwrap()
                .table_id();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                table
                    .metadata()
                    .idx
                    .active_indexes()
                    .map(|(index_slot, _)| index_slot.get())
                    .collect::<Vec<_>>(),
                vec![0, 1]
            );
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_slots
                    .len(),
                2
            );
            drop(table);
            drop(session);
            drop(engine);

            let engine = open_catalog_test_engine(main_dir.clone(), Some(log_stem)).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_slots
                    .len(),
                2
            );
            let indexes = engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(
                    &PoolGuards::builder()
                        .push(
                            PoolRole::Meta,
                            engine.inner().pools.meta.create_base_guard(),
                        )
                        .build(),
                    table_id,
                )
                .await
                .unwrap();
            assert_eq!(
                indexes
                    .iter()
                    .map(|index| index.index.slot())
                    .collect::<Vec<_>>(),
                vec![IndexSlot::new(0), IndexSlot::new(1)]
            );
            drop(table);
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            drop(engine);

            let engine = open_catalog_test_engine(main_dir, Some(log_stem)).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(table.metadata().idx.active_index_count(), 2);
            assert_eq!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_slots
                    .len(),
                2
            );
            drop(table);
            drop(engine);
        });
    }

    #[test]
    fn test_redo_floor_snapshot_does_not_retain_live_table_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                open_catalog_test_engine(temp_dir.path().to_path_buf(), Some("redo-floor-borrow"))
                    .await;
            let table_id = table1(&engine).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table_now(table_id)
                .unwrap();
            let owners_before = Arc::strong_count(&table);

            let (live, dropped) = engine
                .inner()
                .core
                .catalog()
                .snapshot_user_table_redo_floors(MIN_SNAPSHOT_TS);

            assert_eq!(live.len(), 1);
            assert_eq!(live[0].table_id, table_id);
            assert!(dropped.is_empty());
            assert_eq!(Arc::strong_count(&table), owners_before);

            drop(table);
            engine.shutdown();
        });
    }

    #[test]
    fn test_session_catalog_checkpoint_publish_and_noop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = open_catalog_test_engine(main_dir, Some("catalog-checkpoint-now")).await;

            let snap0 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert_eq!(snap0.catalog_replay_start_ts, MIN_SNAPSHOT_TS);
            assert!(
                snap0
                    .meta
                    .table_roots
                    .iter()
                    .all(|root| root.checkpoint_root_block_id().is_none())
            );

            let _ = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap1 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            assert_eq!(
                snap1.meta.next_table_id,
                engine.inner().core.catalog().curr_next_table_id()
            );
            assert!(
                snap1
                    .meta
                    .table_roots
                    .iter()
                    .any(|root| root.checkpoint_root_block_id().is_some())
            );
            assert!(
                snap1
                    .meta
                    .table_roots
                    .iter()
                    .any(|root| root.pivot_row_id() > RowID::new(0))
            );

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap2 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert_eq!(snap2.catalog_replay_start_ts, snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, snap1.meta.table_roots);
        });
    }

    #[test]
    fn test_catalog_bootstrap_fails_on_corrupted_checkpoint_lwc_block() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = open_catalog_test_engine(
                main_dir.clone(),
                Some("catalog-checkpoint-corrupt-bootstrap"),
            )
            .await;

            let _ = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let snap = engine.inner().core.catalog().storage.checkpoint_snapshot();
            let root = snap
                .meta
                .table_roots
                .iter()
                .copied()
                .find(|root| root.checkpoint_root_block_id().is_some())
                .expect("catalog checkpoint should publish at least one root");
            let root_block_id = root.checkpoint_root_block_id().unwrap();
            let block_id = {
                let disk_pool_guard = engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .disk_pool
                    .create_base_guard();
                let index = ColumnBlockIndex::new(
                    root_block_id,
                    root.pivot_row_id(),
                    engine.inner().core.catalog().storage.mtb.file_kind(),
                    engine.inner().core.catalog().storage.mtb.sparse_file(),
                    &engine.inner().core.catalog().storage.disk_pool,
                    &disk_pool_guard,
                );
                let entry = index
                    .collect_leaf_entries()
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("catalog checkpoint should publish at least one LWC block");
                entry.block_id()
            };
            drop(engine);

            corrupt_page_checksum(main_dir.join("catalog.mtb"), u64::from(block_id));

            let err = expect_catalog_test_engine_error(
                main_dir,
                Some("catalog-checkpoint-corrupt-bootstrap"),
                "expected catalog bootstrap corruption failure",
            )
            .await;
            assert_catalog_data_integrity(err);
        });
    }

    #[test]
    fn test_catalog_bootstrap_fails_on_invalid_v2_delete_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine = open_catalog_test_engine(
                main_dir.clone(),
                Some("catalog-checkpoint-invalid-delete-metadata"),
            )
            .await;

            let _ = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let snap = engine.inner().core.catalog().storage.checkpoint_snapshot();
            let root = snap
                .meta
                .table_roots
                .iter()
                .copied()
                .find(|root| root.checkpoint_root_block_id().is_some())
                .expect("catalog checkpoint should publish at least one root");
            let root_block_id = root.checkpoint_root_block_id().unwrap();
            let entry = {
                let disk_pool_guard = engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .disk_pool
                    .create_base_guard();
                let index = ColumnBlockIndex::new(
                    root_block_id,
                    root.pivot_row_id(),
                    engine.inner().core.catalog().storage.mtb.file_kind(),
                    engine.inner().core.catalog().storage.mtb.sparse_file(),
                    &engine.inner().core.catalog().storage.disk_pool,
                    &disk_pool_guard,
                );
                index
                    .collect_leaf_entries()
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("catalog checkpoint should publish at least one leaf entry")
            };
            drop(engine);

            corrupt_leaf_delete_codec(
                main_dir.join("catalog.mtb"),
                u64::from(entry.leaf_block_id),
                0,
            );

            let err = expect_catalog_test_engine_error(
                main_dir,
                Some("catalog-checkpoint-invalid-delete-metadata"),
                "expected catalog bootstrap invalid-metadata failure",
            )
            .await;
            assert_catalog_data_integrity(err);
        });
    }

    #[test]
    fn test_session_catalog_checkpoint_heartbeat_without_catalog_ops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-heartbeat")).await;

            let table_id = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap1 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            let roots_before = snap1.meta.table_roots;

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(table_id, vec![Val::I32(7)])
                .await
                .unwrap();
            trx.commit().await.unwrap();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap2 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap2.catalog_replay_start_ts > snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, roots_before);
            assert_eq!(snap2.meta.next_table_id, snap1.meta.next_table_id);
        });
    }

    #[test]
    fn test_catalog_checkpoint_scan_apply_full_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-batch-full-range"))
                    .await;

            let _ = table1(&engine).await;
            let _ = table2(&engine).await;

            let trx_sys = &engine.inner().trx_sys;
            let batch1 = engine
                .inner()
                .core
                .catalog()
                .scan_checkpoint_batch(
                    trx_sys.persisted_watermark_cts(),
                    trx_sys.catalog_checkpoint_scan_config().unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(batch1.catalog_ddl_txn_count, 2);
            assert_eq!(
                batch1.stop_reason,
                CatalogCheckpointScanStopReason::ReachedDurableUpper
            );
            let safe_cts_1 = batch1.safe_cts;
            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(
                    batch1,
                    engine.inner().core.pools.pool_guards().disk_guard(),
                )
                .await
                .unwrap();
            let snap1 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert_eq!(snap1.catalog_replay_start_ts, safe_cts_1 + 1);

            let batch2 = engine
                .inner()
                .core
                .catalog()
                .scan_checkpoint_batch(
                    trx_sys.persisted_watermark_cts(),
                    trx_sys.catalog_checkpoint_scan_config().unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(batch2.catalog_ddl_txn_count, 0);
            assert_eq!(batch2.safe_cts, safe_cts_1);
            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(
                    batch2,
                    engine.inner().core.pools.pool_guards().disk_guard(),
                )
                .await
                .unwrap();
            let snap2 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert_eq!(snap2.catalog_replay_start_ts, snap1.catalog_replay_start_ts);
        });
    }

    #[test]
    fn test_session_catalog_checkpoint_heartbeat_with_mixed_user_table_checkpoint_states() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();

            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-mixed-user-states"))
                    .await;

            let checkpointed_table_id = table1(&engine).await;
            let replay_only_table_id = table2(&engine).await;

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap1 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap1.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            let roots_before = snap1.meta.table_roots;

            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(checkpointed_table_id, vec![Val::I32(7)])
                .await
                .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(
                replay_only_table_id,
                vec![Val::I32(9), Val::from("replay-backed")],
            )
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let checkpointed_table = engine
                .inner()
                .core
                .catalog()
                .get_table(checkpointed_table_id)
                .await
                .unwrap();
            let replay_only_table = engine
                .inner()
                .core
                .catalog()
                .get_table(replay_only_table_id)
                .await
                .unwrap();
            assert_freeze_created(
                session
                    .freeze_table(checkpointed_table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            let checkpoint_outcome = checkpoint_session
                .checkpoint_table_with_wait(checkpointed_table.table_id())
                .await
                .unwrap();
            assert!(matches!(
                checkpoint_outcome,
                crate::table::CheckpointOutcome::Published { .. }
            ));

            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id
                    > RowID::new(0)
            );
            assert_eq!(
                replay_only_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id,
                RowID::new(0)
            );
            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .heap_redo_start_ts
                    > snap1.catalog_replay_start_ts
            );

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snap2 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap2.catalog_replay_start_ts > snap1.catalog_replay_start_ts);
            assert_eq!(snap2.meta.table_roots, roots_before);
            assert_eq!(snap2.meta.next_table_id, snap1.meta.next_table_id);
        });
    }
}
