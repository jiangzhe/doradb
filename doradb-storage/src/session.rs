use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuards, PoolRole};
use crate::catalog::{ColumnObject, IndexColumnObject, IndexObject, TableMetadata, TableObject};
use crate::catalog::{IndexNo, IndexSpec, TableID, TableSpec, is_user_obj_id};
use crate::engine::EngineRef;
use crate::error::{DataIntegrityError, Error, FatalError, InternalError, OperationError, Result};
use crate::file::BlockID;
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::MutableTableFile;
use crate::index::disk_tree::{NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedPut};
use crate::index::{
    BlockIndex, ColumnBlockIndex, IndexInsert, NonUniqueIndex, NonUniqueMemIndex,
    SecondaryDiskTreeRuntime, SecondaryIndex, UniqueIndex, UniqueMemIndex,
};
use crate::lock::{
    FreshLockGuard, LockGrant, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource,
};
use crate::lwc::PersistedLwcBlock;
use crate::row::{RowID, RowRead};
use crate::table::{
    DeleteMarker, Table, TableAccess, TableRuntimeLayout, secondary_disk_tree_encoder,
};
use crate::trx::redo::DDLRedo;
use crate::trx::{ActiveTrx, TrxID, trx_is_committed};
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
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
        // Keep this guard alive through runtime catalog publication so table-id
        // allocation, catalog DML, file publish, and runtime insertion remain
        // one namespace-serialized create-table sequence.
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

    /// Build and publish a new secondary index for an existing user table.
    #[inline]
    pub async fn create_index(
        &mut self,
        table_id: TableID,
        index_spec: IndexSpec,
    ) -> Result<IndexNo> {
        if self.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }

        let engine = self.state.engine().clone();
        let owner = LockOwner::Session(self.id());
        let lock_manager = engine.lock_manager();

        // 1. Validate the target and acquire table-local DDL exclusion before
        // deriving any new metadata or touching mutable table roots.
        self.precheck_create_index_target(&engine, table_id).await?;
        reject_table_ddl_explicit_session_lock(lock_manager, table_id, owner, "create index")?;
        // Keep these DDL locks alive through root publish and runtime layout
        // install so foreground readers/writers cannot observe a partial index.
        let _table_locks = self
            .acquire_table_ddl_locks(lock_manager, table_id, owner)
            .await?;
        let table = self
            .validated_create_index_target(&engine, table_id)
            .await?;
        engine.trx_sys.ensure_runtime_healthy()?;
        table.check_foreground_live("create index")?;

        // 2. Exclude table and catalog checkpoints while catalog metadata,
        // table-file roots, and runtime layout are temporarily out of sync.
        // Keep both metadata-change leases alive until after the matching table
        // root is published and the new runtime layout is installed.
        let _table_metadata_lease = table.begin_metadata_change().await?;
        let _catalog_metadata_lease = engine.catalog().begin_metadata_change().await;

        // 3. Allocate the stable table-local index number and prepare the new
        // metadata/root shape, preserving existing sparse index slots.
        let old_layout = table.layout_snapshot();
        let old_metadata = old_layout.metadata();
        let (index_no, new_metadata_value) = old_metadata.try_with_created_index(index_spec)?;
        let new_metadata = Arc::new(new_metadata_value);
        let index_no_usize = usize::from(index_no);
        let new_index_spec = new_metadata.require_index_spec(index_no_usize)?.clone();

        let active_root = table.file().active_root_unchecked().clone();
        validate_create_index_root_shape(table_id, &active_root, old_metadata)?;
        let mut secondary_index_roots = active_root.secondary_index_roots.clone();
        secondary_index_roots.resize(new_metadata.index_slot_count(), SUPER_BLOCK_ID);
        let disk_runtime = SecondaryDiskTreeRuntime::new(
            index_no_usize,
            Arc::clone(&new_metadata),
            Arc::clone(table.file()),
            table.disk_pool().clone(),
        )?;

        // 4. Start the implicit DDL transaction and let the build state own all
        // rollback/destroy transitions from this point onward.
        let trx = match self.try_begin_trx() {
            Ok(Some(trx)) => trx,
            Ok(None) => unreachable!("create_index requires idle session"),
            Err(err) => return Err(err),
        };
        let mut builder =
            CreateIndexBuilder::new(&engine, self.pool_guards(), table_id, index_no, trx);
        let build_ts = builder.build_ts();

        let mut mutable_file =
            MutableTableFile::fork(table.file(), engine.table_fs.background_writes());

        // 5. Build the cold DiskTree from the currently persisted live rows and
        // stage the resulting root in the forked table file.
        let cold_rows = collect_create_index_cold_rows(
            &table,
            self.pool_guards(),
            old_metadata,
            &new_index_spec,
            active_root.column_block_index_root,
            active_root.pivot_row_id,
        )
        .await;
        let cold_rows = match cold_rows {
            Ok(cold_rows) => cold_rows,
            Err(err) => {
                return builder.rollback_before_catalog_commit(err).await;
            }
        };

        let (cold_root, cold_unique_keys) = match build_create_index_disk_tree(
            &mut mutable_file,
            &disk_runtime,
            self.pool_guards(),
            new_metadata.as_ref(),
            &new_index_spec,
            &cold_rows,
            build_ts,
        )
        .await
        {
            Ok(res) => res,
            Err(err) => {
                return builder.rollback_before_catalog_commit(err).await;
            }
        };
        secondary_index_roots[index_no_usize] = cold_root;
        if let Err(err) = mutable_file.replace_metadata_and_secondary_index_roots(
            Arc::clone(&new_metadata),
            secondary_index_roots,
        ) {
            return builder.rollback_before_catalog_commit(err).await;
        }

        // 6. Build the hot MemIndex from row-store rows and assemble a runtime
        // layout that future readers can install atomically.
        let hot_rows =
            collect_create_index_hot_rows(&table, &old_layout, self.pool_guards(), &new_index_spec)
                .await;
        match build_create_index_runtime_index(CreateIndexRuntimeBuild {
            engine: &engine,
            guards: self.pool_guards(),
            metadata: Arc::clone(&new_metadata),
            index_spec: &new_index_spec,
            disk_runtime,
            hot_rows,
            cold_unique_keys: &cold_unique_keys,
            build_ts,
        })
        .await
        {
            Ok(index) => {
                builder.stage_runtime_index(index);
            }
            Err(err) => {
                return builder.rollback_before_catalog_commit(err).await;
            }
        };
        let new_layout = match build_created_index_runtime_layout(
            &old_layout,
            Arc::clone(&new_metadata),
            index_no_usize,
            match builder.clone_staged_index_for_layout() {
                Ok(index) => index,
                Err(err) => {
                    return builder.rollback_before_catalog_commit(err).await;
                }
            },
        ) {
            Ok(layout) => layout,
            Err(err) => {
                return builder.rollback_before_catalog_commit(err).await;
            }
        };
        builder.stage_layout(new_layout);

        // 7. Persist catalog metadata and DDL redo in the implicit transaction.
        // Until the table root publishes below, recovery treats this redo as
        // provisional.
        builder
            .execute_catalog_update(new_metadata.as_ref(), &new_index_spec)
            .await?;

        let create_cts = builder.commit_catalog().await?;

        // 8. Publish the table root that proves the new index metadata durable.
        // Failure after catalog commit poisons storage per the RFC 0018 policy.
        let root_publish = mutable_file.commit(create_cts, false).await;
        let (_table_file, old_root) = match root_publish {
            Ok(res) => res,
            Err(err) => {
                return builder
                    .cleanup_after_catalog_commit_failure("table root publish", err)
                    .await;
            }
        };
        drop(old_root);

        // 9. Install the new runtime layout last. Existing snapshots keep their
        // old layout Arcs, while later foreground work observes the new index.
        let new_layout = match builder.take_layout_for_install() {
            Ok(layout) => layout,
            Err(err) => {
                return builder
                    .cleanup_after_catalog_commit_failure("runtime layout install", err)
                    .await;
            }
        };
        if let Err(err) = table.install_runtime_layout(old_layout.generation(), new_layout) {
            return builder
                .cleanup_after_catalog_commit_failure("runtime layout install", err)
                .await;
        }
        builder.mark_installed();

        Ok(index_no)
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
        // Keep this guard alive until runtime removal is complete so table
        // identity removal remains namespace-serialized.
        let _namespace_lock = self.acquire_catalog_namespace_lock(lock_manager).await?;

        let table = self.validated_drop_table_target(&engine, table_id).await?;
        reject_table_ddl_explicit_session_lock(lock_manager, table_id, owner, "drop table")?;
        let mut table_locks = self
            .acquire_table_ddl_locks(lock_manager, table_id, owner)
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

    async fn acquire_table_ddl_locks<'a>(
        &self,
        lock_manager: &'a LockManager,
        table_id: TableID,
        owner: LockOwner,
    ) -> Result<ScopedTableDdlLocks<'a>> {
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

        Ok(ScopedTableDdlLocks {
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

    async fn precheck_create_index_target(
        &self,
        engine: &EngineRef,
        table_id: TableID,
    ) -> Result<()> {
        let _ = self.validated_create_index_target(engine, table_id).await?;
        Ok(())
    }

    async fn validated_create_index_target(
        &self,
        engine: &EngineRef,
        table_id: TableID,
    ) -> Result<Arc<Table>> {
        if !is_user_obj_id(table_id) {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!(
                    "create index requires user table id: table_id={table_id}"
                ))
                .into());
        }
        let table = engine
            .catalog()
            .validate_user_table_live(table_id, "create index")
            .await?;
        if engine
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_id(self.pool_guards(), table_id)
            .await?
            .is_none()
        {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("create index catalog lookup: table_id={table_id}"))
                .into());
        }
        Ok(table)
    }
}

#[inline]
fn reject_table_ddl_explicit_session_lock(
    lock_manager: &LockManager,
    table_id: TableID,
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

#[derive(Clone, Debug)]
struct CreateIndexRowEntry {
    key: Vec<crate::value::Val>,
    row_id: RowID,
}

#[derive(Clone, Debug)]
struct CreateIndexEncodedRowEntry {
    key: Vec<u8>,
    row_id: RowID,
}

struct CreateIndexRuntimeBuild<'a> {
    engine: &'a EngineRef,
    guards: &'a PoolGuards,
    metadata: Arc<TableMetadata>,
    index_spec: &'a IndexSpec,
    disk_runtime: SecondaryDiskTreeRuntime,
    hot_rows: Vec<CreateIndexRowEntry>,
    cold_unique_keys: &'a BTreeSet<Vec<u8>>,
    build_ts: TrxID,
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

struct CreateIndexBuilder<'a> {
    engine: &'a EngineRef,
    guards: &'a PoolGuards,
    table_id: TableID,
    index_no: IndexNo,
    build_ts: TrxID,
    phase: CreateIndexBuildPhase,
    trx: Option<ActiveTrx>,
    staged_index: Option<Arc<SecondaryIndex<EvictableBufferPool>>>,
    new_layout: Option<TableRuntimeLayout>,
}

impl<'a> CreateIndexBuilder<'a> {
    #[inline]
    fn new(
        engine: &'a EngineRef,
        guards: &'a PoolGuards,
        table_id: TableID,
        index_no: IndexNo,
        trx: ActiveTrx,
    ) -> Self {
        let build_ts = trx.sts();
        Self {
            engine,
            guards,
            table_id,
            index_no,
            build_ts,
            phase: CreateIndexBuildPhase::Building,
            trx: Some(trx),
            staged_index: None,
            new_layout: None,
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
    fn clone_staged_index_for_layout(&self) -> Result<Arc<SecondaryIndex<EvictableBufferPool>>> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::RuntimeStaged);
        self.staged_index
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| create_index_internal("create index staged runtime index is missing"))
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
        metadata: &TableMetadata,
        index_spec: &IndexSpec,
    ) -> Result<()> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let Some(trx) = self.trx.as_mut() else {
            let err =
                create_index_internal("create index transaction is missing before catalog update");
            return self.rollback_before_catalog_commit(err).await;
        };
        let res = execute_create_index_catalog_update(
            self.engine,
            trx,
            self.table_id,
            self.index_no,
            metadata,
            index_spec,
        )
        .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => self.rollback_before_catalog_commit(err).await,
        }
    }

    async fn commit_catalog(&mut self) -> Result<TrxID> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let Some(trx) = self.trx.take() else {
            let err = create_index_internal("create index transaction is missing before commit");
            self.cleanup_staged_runtime().await;
            self.phase = CreateIndexBuildPhase::Aborted;
            return Err(err);
        };
        match trx.commit().await {
            Ok(cts) => {
                self.phase = CreateIndexBuildPhase::CatalogCommitted;
                Ok(cts)
            }
            Err(err) => {
                self.cleanup_staged_runtime().await;
                self.phase = CreateIndexBuildPhase::Aborted;
                Err(err)
            }
        }
    }

    fn take_layout_for_install(&mut self) -> Result<TableRuntimeLayout> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::CatalogCommitted);
        self.new_layout.take().ok_or_else(|| {
            create_index_internal("create index runtime layout is missing before install")
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::CatalogCommitted);
        self.staged_index = None;
        self.phase = CreateIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit<T>(&mut self, err: Error) -> Result<T> {
        self.cleanup_staged_runtime().await;
        let rollback_res = self.rollback_active().await;
        self.phase = CreateIndexBuildPhase::Aborted;
        rollback_res?;
        Err(err)
    }

    async fn cleanup_after_catalog_commit_failure<T>(
        &mut self,
        operation: &'static str,
        source: Error,
    ) -> Result<T> {
        self.cleanup_staged_runtime().await;
        self.phase = CreateIndexBuildPhase::Aborted;
        Err(poison_create_index_after_catalog_commit_with_source(
            self.engine,
            self.table_id,
            self.index_no,
            operation,
            source,
        )
        .into())
    }

    async fn cleanup_staged_runtime(&mut self) {
        self.new_layout = None;
        if let Some(index) = self.staged_index.take() {
            destroy_uninstalled_staged_index(index, self.guards).await;
        }
    }

    async fn rollback_active(&mut self) -> Result<()> {
        let Some(trx) = self.trx.take() else {
            return Ok(());
        };
        if trx.engine().is_some() {
            trx.rollback().await?;
        }
        Ok(())
    }
}

impl Drop for CreateIndexBuilder<'_> {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(
            matches!(
                self.phase,
                CreateIndexBuildPhase::Installed | CreateIndexBuildPhase::Aborted
            ),
            "create index build state dropped before terminal cleanup: {:?}",
            self.phase
        );
    }
}

#[inline]
fn create_index_duplicate_key(message: impl Into<String>) -> Error {
    Report::new(OperationError::DuplicateKey)
        .attach(message.into())
        .into()
}

#[inline]
fn create_index_invalid_payload(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

#[inline]
fn create_index_internal(message: impl Into<String>) -> Error {
    Report::new(InternalError::Generic)
        .attach(message.into())
        .into()
}

#[inline]
fn validate_create_index_root_shape(
    table_id: TableID,
    active_root: &crate::file::table_file::ActiveRoot,
    metadata: &TableMetadata,
) -> Result<()> {
    if active_root.metadata.as_ref() != metadata {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create index root metadata mismatch: table_id={table_id}"
            ))
            .into());
    }
    let expected_slots = metadata.index_slot_count();
    let actual_slots = active_root.secondary_index_roots.len();
    if actual_slots != expected_slots {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create index secondary-root slot mismatch: table_id={table_id}, actual_slots={actual_slots}, expected_slots={expected_slots}"
            ))
            .into());
    }
    Ok(())
}

// Future improvement: stream/batch and parallelize this cold-row build to avoid
// materializing every persisted row. See docs/backlogs/000104.
async fn collect_create_index_cold_rows(
    table: &Table,
    guards: &PoolGuards,
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    column_block_index_root: BlockID,
    pivot_row_id: RowID,
) -> Result<Vec<CreateIndexRowEntry>> {
    if column_block_index_root == SUPER_BLOCK_ID || pivot_row_id == 0 {
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
        .cols
        .iter()
        .map(|index_key| index_key.col_no as usize)
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for entry in column_index.collect_leaf_entries().await? {
        let (delete_deltas, row_ids) = column_index.load_delete_deltas_and_row_ids(&entry).await?;
        let block = PersistedLwcBlock::load(
            table.file().file_kind(),
            table.file().sparse_file(),
            table.disk_pool(),
            disk_guard,
            entry.block_id(),
        )
        .await?;
        if usize::from(entry.row_count()) != row_ids.len() || block.row_count() != row_ids.len() {
            return Err(create_index_invalid_payload(format!(
                "create index LWC row count mismatch: block_id={}, entry_rows={}, block_rows={}, row_ids={}",
                entry.block_id(),
                entry.row_count(),
                block.row_count(),
                row_ids.len()
            )));
        }
        if block.row_shape_fingerprint() != entry.row_shape_fingerprint() {
            return Err(create_index_invalid_payload(format!(
                "create index LWC row shape mismatch: block_id={}",
                entry.block_id()
            )));
        }

        let mut persisted_deleted = BTreeSet::new();
        for delta in delete_deltas {
            let row_id = entry
                .start_row_id
                .checked_add(RowID::from(delta))
                .ok_or_else(|| {
                    create_index_invalid_payload(format!(
                        "create index delete delta overflows row id: start_row_id={}, delta={delta}",
                        entry.start_row_id
                    ))
                })?;
            persisted_deleted.insert(row_id);
        }

        for (row_idx, row_id) in row_ids.into_iter().enumerate() {
            if persisted_deleted.contains(&row_id) {
                continue;
            }
            if create_index_cold_row_deleted_by_buffer(table, row_id)? {
                continue;
            }
            let key = block.decode_row_values(metadata, row_idx, &read_set)?;
            rows.push(CreateIndexRowEntry { key, row_id });
        }
    }
    Ok(rows)
}

#[inline]
fn create_index_cold_row_deleted_by_buffer(table: &Table, row_id: RowID) -> Result<bool> {
    match table.deletion_buffer().get(row_id) {
        Some(DeleteMarker::Committed(_)) => Ok(true),
        Some(DeleteMarker::Ref(status)) => {
            if trx_is_committed(status.ts()) {
                Ok(true)
            } else {
                Err(Report::new(OperationError::WriteConflict)
                    .attach(format!(
                        "create index found uncommitted cold-row delete marker: table_id={}, row_id={row_id}",
                        table.table_id()
                    ))
                    .into())
            }
        }
        None => Ok(false),
    }
}

async fn build_create_index_disk_tree(
    mutable_file: &mut MutableTableFile,
    disk_runtime: &SecondaryDiskTreeRuntime,
    guards: &PoolGuards,
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
) -> Result<(BlockID, BTreeSet<Vec<u8>>)> {
    if rows.is_empty() {
        return Ok((SUPER_BLOCK_ID, BTreeSet::new()));
    }

    if index_spec.unique() {
        let encoder = secondary_disk_tree_encoder(metadata, index_spec, false)?;
        let mut encoded = rows
            .iter()
            .map(|row| CreateIndexEncodedRowEntry {
                key: encoder.encode(&row.key).as_bytes().to_vec(),
                row_id: row.row_id,
            })
            .collect::<Vec<_>>();
        encoded.sort_by(|a, b| a.key.cmp(&b.key));
        let mut cold_keys = BTreeSet::new();
        for entry in &encoded {
            if !cold_keys.insert(entry.key.clone()) {
                return Err(create_index_duplicate_key(format!(
                    "create unique index found duplicate cold key: row_id={}",
                    entry.row_id
                )));
            }
        }
        let batch = encoded
            .iter()
            .map(|entry| UniqueDiskTreeEncodedPut {
                key: entry.key.as_slice(),
                row_id: entry.row_id,
            })
            .collect::<Vec<_>>();
        let tree = disk_runtime.open_unique_at(SUPER_BLOCK_ID, guards.disk_guard())?;
        let mut writer = tree.batch_writer(mutable_file, build_ts);
        writer.batch_put_encoded(&batch)?;
        let root = writer.finish().await?;
        Ok((root, cold_keys))
    } else {
        let encoder = secondary_disk_tree_encoder(metadata, index_spec, true)?;
        let mut encoded = rows
            .iter()
            .map(|row| {
                encoder
                    .encode_pair(&row.key, crate::value::Val::from(row.row_id))
                    .as_bytes()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        encoded.sort();
        if encoded.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(create_index_invalid_payload(
                "create non-unique index found duplicate cold exact key",
            ));
        }
        let batch = encoded
            .iter()
            .map(|key| NonUniqueDiskTreeEncodedExact {
                key: key.as_slice(),
            })
            .collect::<Vec<_>>();
        let tree = disk_runtime.open_non_unique_at(SUPER_BLOCK_ID, guards.disk_guard())?;
        let mut writer = tree.batch_writer(mutable_file, build_ts);
        writer.batch_insert_encoded(&batch)?;
        let root = writer.finish().await?;
        Ok((root, BTreeSet::new()))
    }
}

async fn collect_create_index_hot_rows(
    table: &Table,
    layout: &Arc<TableRuntimeLayout>,
    guards: &PoolGuards,
    index_spec: &IndexSpec,
) -> Vec<CreateIndexRowEntry> {
    let mut rows = Vec::new();
    table
        .accessor_with_layout(Arc::clone(layout))
        .table_scan_uncommitted(guards, |metadata, row| {
            if row.is_deleted() {
                return true;
            }
            let key = index_spec
                .cols
                .iter()
                .map(|index_key| row.val(metadata, index_key.col_no as usize))
                .collect();
            rows.push(CreateIndexRowEntry {
                key,
                row_id: row.row_id(),
            });
            true
        })
        .await;
    rows
}

async fn build_create_index_runtime_index(
    build: CreateIndexRuntimeBuild<'_>,
) -> Result<SecondaryIndex<EvictableBufferPool>> {
    let CreateIndexRuntimeBuild {
        engine,
        guards,
        metadata,
        index_spec,
        disk_runtime,
        hot_rows,
        cold_unique_keys,
        build_ts,
    } = build;
    let index_pool = engine.index_pool.clone_inner();
    let index_guard = guards.index_guard();
    let ty_infer = |col_no| metadata.col_type(col_no);
    if index_spec.unique() {
        validate_create_index_hot_unique_keys(
            metadata.as_ref(),
            index_spec,
            &hot_rows,
            cold_unique_keys,
        )?;
        let mem =
            UniqueMemIndex::new(index_pool, index_guard, index_spec, ty_infer, build_ts).await?;
        let insert_res =
            insert_create_index_unique_hot_rows(&mem, index_guard, &hot_rows, build_ts).await;
        if let Err(err) = insert_res {
            let _ = mem.destroy(index_guard).await;
            return Err(err);
        }
        Ok(SecondaryIndex::Unique {
            mem,
            disk: disk_runtime,
        })
    } else {
        let mem =
            NonUniqueMemIndex::new(index_pool, index_guard, index_spec, ty_infer, build_ts).await?;
        let insert_res =
            insert_create_index_non_unique_hot_rows(&mem, index_guard, &hot_rows, build_ts).await;
        if let Err(err) = insert_res {
            let _ = mem.destroy(index_guard).await;
            return Err(err);
        }
        Ok(SecondaryIndex::NonUnique {
            mem,
            disk: disk_runtime,
        })
    }
}

fn validate_create_index_hot_unique_keys(
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    hot_rows: &[CreateIndexRowEntry],
    cold_unique_keys: &BTreeSet<Vec<u8>>,
) -> Result<()> {
    let encoder = secondary_disk_tree_encoder(metadata, index_spec, false)?;
    let mut hot_keys = BTreeSet::new();
    for row in hot_rows {
        let encoded = encoder.encode(&row.key).as_bytes().to_vec();
        if cold_unique_keys.contains(&encoded) {
            return Err(create_index_duplicate_key(format!(
                "create unique index found duplicate cold/hot key: row_id={}",
                row.row_id
            )));
        }
        if !hot_keys.insert(encoded) {
            return Err(create_index_duplicate_key(format!(
                "create unique index found duplicate hot key: row_id={}",
                row.row_id
            )));
        }
    }
    Ok(())
}

async fn insert_create_index_unique_hot_rows(
    mem: &UniqueMemIndex<EvictableBufferPool>,
    index_guard: &crate::buffer::PoolGuard,
    hot_rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
) -> Result<()> {
    for row in hot_rows {
        match mem
            .insert_if_not_exists(index_guard, &row.key, row.row_id, false, build_ts)
            .await?
        {
            IndexInsert::Ok(_) => (),
            IndexInsert::DuplicateKey(..) => {
                return Err(create_index_duplicate_key(format!(
                    "create unique index found duplicate hot key during MemIndex build: row_id={}",
                    row.row_id
                )));
            }
        }
    }
    Ok(())
}

async fn insert_create_index_non_unique_hot_rows(
    mem: &NonUniqueMemIndex<EvictableBufferPool>,
    index_guard: &crate::buffer::PoolGuard,
    hot_rows: &[CreateIndexRowEntry],
    build_ts: TrxID,
) -> Result<()> {
    for row in hot_rows {
        match mem
            .insert_if_not_exists(index_guard, &row.key, row.row_id, false, build_ts)
            .await?
        {
            IndexInsert::Ok(_) => (),
            IndexInsert::DuplicateKey(..) => {
                return Err(create_index_internal(format!(
                    "create non-unique index found duplicate hot exact key: row_id={}",
                    row.row_id
                )));
            }
        }
    }
    Ok(())
}

fn build_created_index_runtime_layout(
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index_no: usize,
    staged_index: Arc<SecondaryIndex<EvictableBufferPool>>,
) -> Result<TableRuntimeLayout> {
    let generation = old_layout
        .generation()
        .checked_add(1)
        .ok_or_else(|| create_index_internal("table runtime layout generation overflow"))?;
    let mut slots = old_layout.secondary_indexes().to_vec();
    slots.resize_with(new_metadata.index_slot_count(), || None);
    if slots.get(index_no).and_then(Option::as_ref).is_some() {
        return Err(create_index_internal(format!(
            "create index runtime slot is already occupied: index_no={index_no}"
        )));
    }
    slots[index_no] = Some(staged_index);
    TableRuntimeLayout::new(generation, new_metadata, slots.into_boxed_slice())
}

async fn destroy_uninstalled_staged_index(
    index: Arc<SecondaryIndex<EvictableBufferPool>>,
    guards: &PoolGuards,
) {
    let Ok(index) = Arc::try_unwrap(index) else {
        return;
    };
    let _ = index.destroy(guards.index_guard()).await;
}

#[inline]
async fn execute_create_index_catalog_update(
    engine: &EngineRef,
    trx: &mut ActiveTrx,
    table_id: TableID,
    index_no: IndexNo,
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
) -> Result<()> {
    trx.exec(async |stmt| {
        let table_deleted = engine
            .catalog()
            .storage
            .tables()
            .delete_by_id(stmt, table_id)
            .await;
        if !table_deleted {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!("create index catalog table row: table_id={table_id}"))
                .into());
        }

        let table_inserted = engine
            .catalog()
            .storage
            .tables()
            .insert(
                stmt,
                &TableObject {
                    table_id,
                    next_index_no: metadata.next_index_no(),
                },
            )
            .await;
        if !table_inserted {
            return Err(create_index_internal(format!(
                "create index failed to update catalog table row: table_id={table_id}"
            )));
        }

        let index_inserted = engine
            .catalog()
            .storage
            .indexes()
            .insert(
                stmt,
                &IndexObject {
                    table_id,
                    index_no,
                    index_attributes: index_spec.attributes,
                },
            )
            .await;
        if !index_inserted {
            return Err(create_index_internal(format!(
                "create index failed to insert catalog index row: table_id={table_id}, index_no={index_no}"
            )));
        }

        for (index_column_no, index_key) in index_spec.cols.iter().enumerate() {
            let inserted = engine
                .catalog()
                .storage
                .index_columns()
                .insert(
                    stmt,
                    &IndexColumnObject {
                        table_id,
                        index_no,
                        index_column_no: index_column_no as u16,
                        column_no: index_key.col_no,
                        index_order: index_key.order,
                    },
                )
                .await;
            if !inserted {
                return Err(create_index_internal(format!(
                    "create index failed to insert catalog index-column row: table_id={table_id}, index_no={index_no}, index_column_no={index_column_no}"
                )));
            }
        }

        let res = stmt
            .effects_mut()
            .set_ddl_redo(DDLRedo::CreateIndex { table_id, index_no });
        debug_assert!(res.is_none());
        Ok(())
    })
    .await
}

#[inline]
fn poison_create_index_after_catalog_commit_with_source(
    engine: &EngineRef,
    table_id: TableID,
    index_no: IndexNo,
    operation: &'static str,
    source: Error,
) -> Report<FatalError> {
    let poison = engine.trx_sys.poison_storage(FatalError::Poisoned);
    source
        .into_report()
        .change_context(*poison.current_context())
        .attach(format!(
            "create index failed after catalog commit: table_id={table_id}, index_no={index_no}, operation={operation}"
        ))
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

struct ScopedTableDdlLocks<'a> {
    lock_manager: &'a LockManager,
    table_id: TableID,
    owner: LockOwner,
    metadata_fresh: bool,
    data_fresh: bool,
    fail_waiters: Option<OperationError>,
}

impl ScopedTableDdlLocks<'_> {
    #[inline]
    fn fail_waiters_on_release(&mut self, error: OperationError) {
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
