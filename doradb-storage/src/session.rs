use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards, PoolRole};
use crate::catalog::{ColumnObject, IndexColumnObject, IndexObject, TableMetadata, TableObject};
use crate::catalog::{IndexSpec, TableID, TableSpec, is_user_obj_id};
use crate::engine::EngineRef;
use crate::error::{FatalError, InternalError, OperationError, Result};
use crate::index::BlockIndex;
use crate::lock::{
    FreshLockGuard, LockGrant, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource,
};
use crate::row::RowID;
use crate::table::Table;
use crate::trx::redo::DDLRedo;
use crate::trx::{ActiveTrx, TrxID};
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Engine-local session identity.
pub type SessionID = u64;

/// Per-client execution context bound to one engine instance.
pub struct Session {
    state: Arc<SessionState>,
}

impl Session {
    #[inline]
    pub(crate) fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        Session {
            state: Arc::new(SessionState::new(engine_ref, id)),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.state.id()
    }

    /// Returns the engine handle bound to this session.
    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.state.engine_ref
    }

    /// Returns the reusable pool guards owned by this session.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        self.state.pool_guards()
    }

    /// Returns whether the session currently owns an active transaction.
    #[inline]
    pub fn in_trx(&self) -> bool {
        self.state.in_trx.load(Ordering::Relaxed)
    }

    /// Remove and return the cached insert page for a table, if present.
    #[inline]
    pub fn load_active_insert_page(
        &mut self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.state.load_active_insert_page(table_id)
    }

    /// Cache the current insert page for a table.
    #[inline]
    pub fn save_active_insert_page(
        &mut self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.state
            .save_active_insert_page(table_id, page_id, row_id);
    }

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn try_begin_trx(&mut self) -> Result<Option<ActiveTrx>> {
        self.state.engine_ref.with_running_admission(|| {
            if !self.state.try_enter_trx() {
                return None;
            }
            Some(
                self.state
                    .engine_ref
                    .trx_sys
                    .begin_trx(Arc::clone(&self.state)),
            )
        })
    }

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        if self.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }

        let engine = self.state.engine().clone();
        let _namespace_lock = self
            .acquire_catalog_namespace_lock(engine.lock_manager())
            .await?;
        // 1. Prepare a new table file.
        //    User table file name is <table-id:016x>.tbl
        let table_id = engine.catalog().next_user_obj_id();

        let metadata = Arc::new(TableMetadata::new(
            table_spec.columns.clone(),
            index_specs.clone(),
        ));
        let uninit_table_file =
            engine
                .table_fs
                .create_table_file(table_id, Arc::clone(&metadata), false)?;

        // 2. Prepare catalog related object
        let table_object = TableObject {
            table_id,
            next_index_no: metadata.next_index_no(),
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

        let mut index_objects = vec![];
        let mut index_column_objects = vec![];

        for (index_no, index_spec) in metadata.active_indexes() {
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

        // 3. begin transaction
        let mut trx = match self.try_begin_trx() {
            Ok(Some(trx)) => trx,
            Ok(None) => unreachable!("create_table requires idle session"),
            Err(err) => {
                uninit_table_file.try_delete();
                return Err(err);
            }
        };

        // 4. insert catalog related objects.
        let exec_res = trx
            .exec(async |stmt| {
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

                // 5. add DDL redo log to redo log buffer
                let res = stmt
                    .effects_mut()
                    .set_ddl_redo(DDLRedo::CreateTable(table_id));
                debug_assert!(res.is_none());
                Ok(())
            })
            .await;
        if let Err(err) = exec_res {
            uninit_table_file.try_delete();
            if trx.engine().is_some() {
                trx.rollback().await?;
            }
            return Err(err);
        }

        // 6. commit current transaction implicitly.
        let cts = match trx.commit().await {
            Ok(cts) => cts,
            Err(e) => {
                uninit_table_file.try_delete();
                return Err(e);
            }
        };

        // 7. commit file with cts.
        let (table_file, old_root) = uninit_table_file.commit(cts, true).await?;
        debug_assert!(old_root.is_none());

        // 8. Prepare in-memory representation of new table
        let meta_pool_guard = self.pool_guards().meta_guard();
        let index_pool_guard = self.pool_guards().index_guard();
        // `catalog_load_boundary`: the file was just committed for this DDL,
        // so this root initializes runtime state rather than serving a
        // foreground transaction read.
        let active_root = table_file.active_root_unchecked();
        let blk_idx = BlockIndex::new(
            engine.meta_pool.clone_inner(),
            meta_pool_guard,
            active_root.pivot_row_id,
            active_root.column_block_index_root,
        )
        .await?;
        let disk_pool = engine.disk_pool.clone_inner();
        let table = Arc::new(
            Table::new(
                engine.mem_pool.clone_inner(),
                engine.index_pool.clone_inner(),
                index_pool_guard,
                table_id,
                blk_idx,
                table_file,
                disk_pool,
            )
            .await?,
        );

        engine.catalog().insert_user_table(table);

        Ok(table_id)
    }

    /// Logically drop an existing user table.
    #[inline]
    pub async fn drop_table(&mut self, table_id: TableID) -> Result<()> {
        if self.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }

        let engine = self.state.engine().clone();
        let owner = LockOwner::Session(self.id());
        let lock_manager = engine.lock_manager();
        let _namespace_lock = self.acquire_catalog_namespace_lock(lock_manager).await?;

        let table = self.validated_drop_table_target(&engine, table_id).await?;
        reject_drop_table_explicit_session_lock(lock_manager, table_id, owner)?;
        let mut table_locks = self
            .acquire_drop_table_locks(lock_manager, table_id, owner)
            .await?;
        engine.trx_sys.ensure_runtime_healthy()?;

        let mut trx = match self.try_begin_trx() {
            Ok(Some(trx)) => trx,
            Ok(None) => unreachable!("drop_table requires idle session"),
            Err(err) => return Err(err),
        };

        if let Err(err) = table.begin_drop_lifecycle().await {
            trx.rollback().await?;
            return Err(err);
        }

        let metadata = table.metadata().clone();
        let exec_res =
            execute_drop_table_catalog_cascade(&engine, &mut trx, table_id, &metadata).await;
        if let Err(err) = exec_res {
            // `trx.exec` may have already discarded the transaction after a
            // fatal statement-rollback failure. In either case the drop gate
            // has been crossed, so preserve the poison outcome below.
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
                return Err(poison_drop_table_after_gate_with_source(
                    &engine, table_id, "commit", err,
                )
                .into());
            }
        };

        let removed = finish_drop_table_runtime_removal(&engine, table_id, &table)?;
        table_locks.fail_waiters_on_release(OperationError::TableNotFound);
        drop(table);
        // Foreground DROP TABLE stops at logical/runtime removal. Physical
        // runtime destruction and file unlink are purge-owned so stale handles,
        // active snapshots, and catalog checkpoint durability can be honored
        // without blocking this DDL call on best-effort cleanup work.
        engine
            .trx_sys
            .enqueue_dropped_table(table_id, drop_cts, removed);
        Ok(())
    }

    /// Acquires an explicit session-lifetime table lock.
    #[inline]
    pub async fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()> {
        mode.validate_explicit_table_lock()?;
        let engine = self.state.engine().clone();
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        let lock_manager = engine.lock_manager();
        let owner = LockOwner::Session(self.id());
        let owner_group = LockOwnerGroup::Session(self.id());
        let metadata_resource = LockResource::TableMetadata(table_id);
        let metadata_grant = lock_manager
            .acquire_grouped_with_grant(metadata_resource, LockMode::Shared, owner, owner_group)
            .await?;
        let mut metadata_guard =
            FreshLockGuard::new(lock_manager, metadata_resource, owner, metadata_grant);
        let data_resource = LockResource::TableData(table_id);
        let data_grant = lock_manager
            .acquire_grouped_with_grant(data_resource, mode, owner, owner_group)
            .await?;
        let mut data_guard = FreshLockGuard::new(lock_manager, data_resource, owner, data_grant);
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        if let Some(guard) = data_guard.as_mut() {
            guard.disarm();
        }
        if let Some(guard) = metadata_guard.as_mut() {
            guard.disarm();
        }
        Ok(())
    }

    /// Releases an explicit session-lifetime table lock when no transaction is active.
    #[inline]
    pub fn unlock_table(&self, table_id: TableID) -> Result<()> {
        if self.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("unlock table while session has an active transaction")
                .into());
        }
        let owner = LockOwner::Session(self.id());
        let lock_manager = self.state.engine().lock_manager();
        lock_manager.release(LockResource::TableData(table_id), owner);
        lock_manager.release(LockResource::TableMetadata(table_id), owner);
        Ok(())
    }

    #[inline]
    async fn acquire_catalog_namespace_lock<'a>(
        &self,
        lock_manager: &'a LockManager,
    ) -> Result<ScopedSessionLock<'a>> {
        let owner = LockOwner::Session(self.id());
        let owner_group = LockOwnerGroup::Session(self.id());
        let resource = LockResource::CatalogNamespace;
        lock_manager
            .acquire_grouped(resource, LockMode::Exclusive, owner, owner_group)
            .await?;
        Ok(ScopedSessionLock {
            lock_manager,
            resource,
            owner,
        })
    }

    async fn acquire_drop_table_locks<'a>(
        &self,
        lock_manager: &'a LockManager,
        table_id: TableID,
        owner: LockOwner,
    ) -> Result<ScopedDropTableLocks<'a>> {
        let owner_group = LockOwnerGroup::Session(self.id());
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

        Ok(ScopedDropTableLocks {
            lock_manager,
            table_id,
            owner,
            metadata_fresh: metadata_grant == LockGrant::Fresh,
            data_fresh: data_grant == LockGrant::Fresh,
            fail_waiters: None,
        })
    }

    async fn validated_drop_table_target(
        &self,
        engine: &EngineRef,
        table_id: TableID,
    ) -> Result<Arc<Table>> {
        if !is_user_obj_id(table_id) {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!(
                    "drop table requires user table id: table_id={table_id}"
                ))
                .into());
        }
        let Some(table) = engine.catalog().get_table(table_id).await else {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("drop table runtime lookup: table_id={table_id}"))
                .into());
        };
        if engine
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_id(self.pool_guards(), table_id)
            .await?
            .is_none()
        {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("drop table catalog lookup: table_id={table_id}"))
                .into());
        }
        Ok(table)
    }
}

#[inline]
fn reject_drop_table_explicit_session_lock(
    lock_manager: &LockManager,
    table_id: TableID,
    owner: LockOwner,
) -> Result<()> {
    // `drop_table` uses the session owner for scoped DDL locks. If an explicit
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
            "drop table while session owns explicit table lock: table_id={table_id}, owner={owner:?}"
        ))
        .into())
}

struct ScopedSessionLock<'a> {
    lock_manager: &'a LockManager,
    resource: LockResource,
    owner: LockOwner,
}

impl Drop for ScopedSessionLock<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lock_manager.release(self.resource, self.owner);
    }
}

struct ScopedDropTableLocks<'a> {
    lock_manager: &'a LockManager,
    table_id: TableID,
    owner: LockOwner,
    metadata_fresh: bool,
    data_fresh: bool,
    fail_waiters: Option<OperationError>,
}

impl ScopedDropTableLocks<'_> {
    #[inline]
    fn fail_waiters_on_release(&mut self, error: OperationError) {
        self.fail_waiters = Some(error);
    }
}

impl Drop for ScopedDropTableLocks<'_> {
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

#[inline]
async fn execute_drop_table_catalog_cascade(
    engine: &EngineRef,
    trx: &mut ActiveTrx,
    table_id: TableID,
    metadata: &TableMetadata,
) -> Result<()> {
    trx.exec(async |stmt| {
        let index_columns_deleted = engine
            .catalog()
            .storage
            .index_columns()
            .delete_by_table_id(stmt, table_id)
            .await;
        let indexes_deleted = engine
            .catalog()
            .storage
            .indexes()
            .delete_by_table_id(stmt, table_id)
            .await;
        let columns_deleted = engine
            .catalog()
            .storage
            .columns()
            .delete_by_table_id(stmt, table_id)
            .await;
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
        .active_indexes()
        .map(|(_, spec)| spec.cols.len())
        .sum::<usize>();
    if columns_deleted == metadata.col_count()
        && indexes_deleted == metadata.active_index_count()
        && index_columns_deleted == expected_index_columns
    {
        return Ok(());
    }
    Err(Report::new(InternalError::Generic)
        .attach(format!(
            "drop table catalog cascade count mismatch: table_id={table_id}, columns_deleted={columns_deleted}, expected_columns={}, indexes_deleted={indexes_deleted}, expected_indexes={}, index_columns_deleted={index_columns_deleted}, expected_index_columns={expected_index_columns}",
            metadata.col_count(),
            metadata.active_index_count(),
        ))
        .into())
}

#[inline]
fn finish_drop_table_runtime_removal(
    engine: &EngineRef,
    table_id: TableID,
    table: &Arc<Table>,
) -> Result<Arc<Table>> {
    if let Err(_err) = table.mark_dropped_lifecycle() {
        return Err(poison_drop_table_after_gate(engine, table_id, "mark dropped").into());
    }
    match engine.catalog().remove_user_table(table_id) {
        Some(removed) if Arc::ptr_eq(&removed, table) => Ok(removed),
        Some(_) | None => {
            Err(poison_drop_table_after_gate(engine, table_id, "runtime removal").into())
        }
    }
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
    source: crate::error::Error,
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

/// Shared mutable state referenced by transactions started from one [`Session`].
pub struct SessionState {
    id: SessionID,
    engine_ref: EngineRef,
    pool_guards: PoolGuards,
    in_trx: AtomicBool,
    last_cts: AtomicU64,
    active_insert_pages: Mutex<HashMap<TableID, (VersionedPageID, RowID)>>,
}

impl SessionState {
    /// Create a new session state and populate its default pool guards.
    #[inline]
    pub fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, engine_ref.meta_pool.pool_guard())
            .push(PoolRole::Index, engine_ref.index_pool.pool_guard())
            .push(PoolRole::Mem, engine_ref.mem_pool.pool_guard())
            .push(PoolRole::Disk, engine_ref.disk_pool.pool_guard())
            .build();
        SessionState {
            id,
            engine_ref,
            pool_guards,
            in_trx: AtomicBool::new(false),
            last_cts: AtomicU64::new(0),
            active_insert_pages: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns the engine handle for this session state.
    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.engine_ref
    }

    /// Returns the guard bundle owned by this session state.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    /// Returns the last committed transaction timestamp observed by this session.
    #[inline]
    pub fn last_cts(&self) -> Option<TrxID> {
        let trx_id = self.last_cts.load(Ordering::Relaxed);
        if trx_id == 0 {
            return None;
        }
        Some(trx_id)
    }

    #[inline]
    fn try_enter_trx(&self) -> bool {
        self.in_trx
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Mark the session transaction as committed at the given CTS.
    #[inline]
    pub fn commit(&self, cts: TrxID) {
        self.last_cts.store(cts, Ordering::SeqCst);
        self.in_trx.store(false, Ordering::SeqCst);
    }

    /// Mark the session transaction as rolled back.
    #[inline]
    pub fn rollback(&self) {
        self.in_trx.store(false, Ordering::SeqCst);
    }

    /// Remove and return the cached insert page for a table, if present.
    #[inline]
    pub fn load_active_insert_page(&self, table_id: TableID) -> Option<(VersionedPageID, RowID)> {
        let mut g = self.active_insert_pages.lock();
        g.remove(&table_id)
    }

    /// Cache the active insert page for a table.
    #[inline]
    pub fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        let mut g = self.active_insert_pages.lock();
        let res = g.insert(table_id, (page_id, row_id));
        debug_assert!(res.is_none());
    }
}

impl Drop for SessionState {
    #[inline]
    fn drop(&mut self) {
        self.engine_ref
            .lock_manager()
            .release_owner(LockOwner::Session(self.id));
    }
}
