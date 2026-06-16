use super::table::{
    acquire_table_ddl_locks, precheck_index_ddl_target, reject_table_ddl_explicit_session_lock,
    validated_index_ddl_target,
};
use crate::buffer::{EvictableBufferPool, PoolGuards};
use crate::catalog::{
    IndexColumnObject, IndexNo, IndexObject, IndexSpec, TableMetadata, TableObject,
};
use crate::engine::EngineRef;
use crate::error::{DataIntegrityError, Error, FatalError, InternalError, OperationError, Result};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::{ActiveRoot, MutableTableFile};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::disk_tree::{NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedPut};
use crate::index::{
    ColumnBlockIndex, IndexInsert, NonUniqueIndex, NonUniqueMemIndex, SecondaryDiskTreeRuntime,
    SecondaryIndex, UniqueIndex, UniqueMemIndex,
};
use crate::log::redo::DDLRedo;
use crate::lwc::PersistedLwcBlock;
use crate::row::RowRead;
#[cfg(test)]
use crate::session::Session;
use crate::session::{SessionDdlContext, SessionPin};
use crate::table::{DeleteMarker, Table, TableRuntimeLayout, secondary_disk_tree_encoder};
use crate::trx::{Transaction, trx_is_committed};
use crate::value::Val;
use error_stack::Report;
use std::collections::BTreeSet;
use std::sync::Arc;

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

#[inline]
fn invalid_index_ddl_root(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(message.into())
        .into()
}

/// Build and publish a new secondary index for a user-table session request.
pub(crate) async fn create_index_for_session(
    session: SessionPin,
    table_id: TableID,
    index_spec: IndexSpec,
) -> Result<IndexNo> {
    let ctx = SessionDdlContext::new(&session)?;
    let engine = ctx.engine.clone();
    let guards = ctx.pool_guards.clone();
    let lock_manager = engine.lock_manager();

    // 1. Validate the target and acquire table-local DDL exclusion before
    // deriving any new metadata or touching mutable table roots.
    precheck_index_ddl_target(&guards, &engine, table_id, "create index").await?;
    reject_table_ddl_explicit_session_lock(lock_manager, table_id, ctx.owner, "create index")?;
    // Keep these DDL locks alive through root publish and runtime layout
    // install so foreground readers/writers cannot observe a partial index.
    let _table_locks =
        acquire_table_ddl_locks(lock_manager, table_id, ctx.owner, ctx.owner_group).await?;
    let table = validated_index_ddl_target(&guards, &engine, table_id, "create index").await?;
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
    let active_root = table.file().active_root_unchecked().clone();
    validate_create_index_root_shape(table_id, &active_root, old_metadata)?;
    let (index_no, new_metadata_value) = old_metadata.try_with_created_index(index_spec)?;
    let new_metadata = Arc::new(new_metadata_value);
    let index_no_usize = usize::from(index_no);
    let new_index_spec = new_metadata.idx.require_index_spec(index_no_usize)?.clone();

    let mut secondary_index_roots = active_root.secondary_index_roots.clone();
    secondary_index_roots.resize(new_metadata.idx.index_slot_count(), SUPER_BLOCK_ID);
    let disk_runtime = SecondaryDiskTreeRuntime::new(
        index_no_usize,
        Arc::clone(&new_metadata),
        Arc::clone(table.file()),
        table.disk_pool().clone(),
    )?;

    // 4. Start the implicit DDL transaction and let the progress state own all
    // rollback/destroy transitions from this point onward.
    let trx = session.begin_trx("begin transaction")?;
    let mut progress = CreateIndexProgress::new(&engine, &guards, table_id, index_no, trx);
    let build_ts = progress.build_ts();

    let mut mutable_file = MutableTableFile::fork(
        table.file(),
        engine.table_fs.background_writes(),
        table.disk_pool().clone(),
    );

    // 5. Build the cold DiskTree from the currently persisted live rows and
    // stage the resulting root in the forked table file.
    let cold_rows = collect_create_index_cold_rows(
        &table,
        &guards,
        old_metadata,
        &new_index_spec,
        active_root.column_block_index_root,
        active_root.pivot_row_id,
    )
    .await;
    let cold_rows = match cold_rows {
        Ok(cold_rows) => cold_rows,
        Err(err) => {
            return progress.rollback_before_catalog_commit(err).await;
        }
    };

    let (cold_root, cold_unique_keys) = match build_create_index_disk_tree(
        &mut mutable_file,
        &disk_runtime,
        &guards,
        new_metadata.as_ref(),
        &new_index_spec,
        &cold_rows,
        build_ts,
    )
    .await
    {
        Ok(res) => res,
        Err(err) => {
            return progress.rollback_before_catalog_commit(err).await;
        }
    };
    secondary_index_roots[index_no_usize] = cold_root;
    if let Err(err) = mutable_file.replace_metadata_and_secondary_index_roots(
        Arc::clone(&new_metadata),
        secondary_index_roots,
    ) {
        return progress.rollback_before_catalog_commit(err).await;
    }

    // 6. Build the hot MemIndex from row-store rows and assemble a runtime
    // layout that future readers can install atomically.
    let hot_rows =
        match collect_create_index_hot_rows(&table, &old_layout, &guards, &new_index_spec).await {
            Ok(hot_rows) => hot_rows,
            Err(err) => {
                return progress.rollback_before_catalog_commit(err).await;
            }
        };
    match build_create_index_runtime_index(CreateIndexRuntimeBuild {
        engine: &engine,
        guards: &guards,
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
            progress.stage_runtime_index(index);
        }
        Err(err) => {
            return progress.rollback_before_catalog_commit(err).await;
        }
    };
    let new_layout = match build_created_index_runtime_layout(
        &old_layout,
        Arc::clone(&new_metadata),
        index_no_usize,
        match progress.clone_staged_index_for_layout() {
            Ok(index) => index,
            Err(err) => {
                return progress.rollback_before_catalog_commit(err).await;
            }
        },
    ) {
        Ok(layout) => layout,
        Err(err) => {
            return progress.rollback_before_catalog_commit(err).await;
        }
    };
    progress.stage_layout(new_layout);

    // 7. Persist catalog metadata and DDL redo in the implicit transaction.
    // Until the table root publishes below, recovery treats this redo as
    // provisional.
    progress
        .execute_catalog_update(new_metadata.as_ref(), &new_index_spec)
        .await?;

    let create_cts = progress.commit_catalog().await?;

    // 8. Publish the table root that proves the new index metadata durable.
    // Failure after catalog commit poisons storage per the RFC 0018 policy.
    let root_publish = engine
        .trx_sys
        .publish_table_file_root(mutable_file, create_cts, false)
        .await;
    match root_publish {
        Ok(_table_file) => {}
        Err(err) => {
            return progress
                .cleanup_after_catalog_commit_failure("table root publish", err)
                .await;
        }
    }

    // 9. Install the new runtime layout last. Existing snapshots keep their old
    // layout Arcs, while later foreground work observes the new index.
    let new_layout = match progress.take_layout_for_install() {
        Ok(layout) => layout,
        Err(err) => {
            return progress
                .cleanup_after_catalog_commit_failure("runtime layout install", err)
                .await;
        }
    };
    if let Err(err) = table.install_runtime_layout(old_layout.generation(), new_layout) {
        return progress
            .cleanup_after_catalog_commit_failure("runtime layout install", err)
            .await;
    }
    progress.mark_installed();

    Ok(index_no)
}

/// Drop an active secondary index for a user-table session request.
pub(crate) async fn drop_index_for_session(
    session: SessionPin,
    table_id: TableID,
    index_no: IndexNo,
) -> Result<()> {
    let ctx = SessionDdlContext::new(&session)?;
    let engine = ctx.engine.clone();
    let guards = ctx.pool_guards.clone();
    let lock_manager = engine.lock_manager();

    precheck_index_ddl_target(&guards, &engine, table_id, "drop index").await?;
    reject_table_ddl_explicit_session_lock(lock_manager, table_id, ctx.owner, "drop index")?;
    let _table_locks =
        acquire_table_ddl_locks(lock_manager, table_id, ctx.owner, ctx.owner_group).await?;
    let table = validated_index_ddl_target(&guards, &engine, table_id, "drop index").await?;
    engine.trx_sys.ensure_runtime_healthy()?;
    table.check_foreground_live("drop index")?;

    let _table_metadata_lease = table.begin_metadata_change().await?;
    let _catalog_metadata_lease = engine.catalog().begin_metadata_change().await;

    let old_layout = table.layout_snapshot();
    let old_generation = old_layout.generation();
    let old_metadata = old_layout.metadata();
    let index_no_usize = usize::from(index_no);
    let old_index_spec = old_metadata
        .idx
        .index_spec(index_no_usize)
        .ok_or_else(|| drop_index_not_found(table_id, index_no, "inactive metadata slot"))?
        .clone();
    old_layout.secondary_index(index_no_usize)?;

    let active_root = table.file().active_root_unchecked().clone();
    validate_drop_index_root_shape(table_id, index_no_usize, &active_root, old_metadata)?;
    let new_metadata = Arc::new(old_metadata.try_without_index(index_no)?);

    let mut secondary_index_roots = active_root.secondary_index_roots.clone();
    secondary_index_roots[index_no_usize] = SUPER_BLOCK_ID;
    let mut mutable_file = MutableTableFile::fork(
        table.file(),
        engine.table_fs.background_writes(),
        table.disk_pool().clone(),
    );
    mutable_file.replace_metadata_and_secondary_index_roots(
        Arc::clone(&new_metadata),
        secondary_index_roots,
    )?;

    let new_layout = build_dropped_index_runtime_layout(
        table_id,
        &old_layout,
        Arc::clone(&new_metadata),
        index_no_usize,
    )?;

    let trx = session.begin_trx("begin transaction")?;
    let mut progress = DropIndexProgress::new(&engine, table_id, index_no, trx);
    progress.stage_layout(new_layout);
    progress.execute_catalog_update(&old_index_spec).await?;
    let drop_cts = progress.commit_catalog().await?;

    let root_publish = engine
        .trx_sys
        .publish_table_file_root(mutable_file, drop_cts, false)
        .await;
    match root_publish {
        Ok(_table_file) => {}
        Err(err) => {
            return progress
                .cleanup_after_catalog_commit_failure("table root publish", err)
                .await;
        }
    }

    let new_layout = match progress.take_layout_for_install() {
        Ok(layout) => layout,
        Err(err) => {
            return progress
                .cleanup_after_catalog_commit_failure("runtime layout install", err)
                .await;
        }
    };
    if let Err(err) = table.install_runtime_layout(old_generation, new_layout) {
        return progress
            .cleanup_after_catalog_commit_failure("runtime layout install", err)
            .await;
    }
    progress.mark_installed();
    drop(old_layout);

    if let Err(err) = table.cleanup_retired_secondary_indexes(&guards).await {
        return Err(poison_index_after_catalog_commit_with_source(
            &engine,
            IndexDdlKind::Drop,
            table_id,
            index_no,
            "retired secondary-index cleanup",
            err,
        )
        .into());
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct CreateIndexRowEntry {
    key: Vec<Val>,
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

struct CreateIndexProgress<'a> {
    engine: &'a EngineRef,
    guards: &'a PoolGuards,
    table_id: TableID,
    index_no: IndexNo,
    build_ts: TrxID,
    phase: CreateIndexBuildPhase,
    trx: Option<Transaction>,
    staged_index: Option<Arc<SecondaryIndex<EvictableBufferPool>>>,
    new_layout: Option<TableRuntimeLayout>,
}

impl<'a> CreateIndexProgress<'a> {
    #[inline]
    fn new(
        engine: &'a EngineRef,
        guards: &'a PoolGuards,
        table_id: TableID,
        index_no: IndexNo,
        trx: Transaction,
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
        let rollback_res = rollback_active_ddl_trx(&mut self.trx).await;
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
        Err(poison_index_after_catalog_commit_with_source(
            self.engine,
            IndexDdlKind::Create,
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
}

impl Drop for CreateIndexProgress<'_> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DropIndexBuildPhase {
    LayoutStaged,
    CatalogCommitted,
    Installed,
    Aborted,
}

struct DropIndexProgress<'a> {
    engine: &'a EngineRef,
    table_id: TableID,
    index_no: IndexNo,
    phase: DropIndexBuildPhase,
    trx: Option<Transaction>,
    new_layout: Option<TableRuntimeLayout>,
}

impl<'a> DropIndexProgress<'a> {
    #[inline]
    fn new(engine: &'a EngineRef, table_id: TableID, index_no: IndexNo, trx: Transaction) -> Self {
        Self {
            engine,
            table_id,
            index_no,
            phase: DropIndexBuildPhase::LayoutStaged,
            trx: Some(trx),
            new_layout: None,
        }
    }

    #[inline]
    fn stage_layout(&mut self, layout: TableRuntimeLayout) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        debug_assert!(self.new_layout.is_none());
        self.new_layout = Some(layout);
    }

    async fn execute_catalog_update(&mut self, old_index_spec: &IndexSpec) -> Result<()> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let Some(trx) = self.trx.as_mut() else {
            let err =
                drop_index_internal("drop index transaction is missing before catalog update");
            return self.rollback_before_catalog_commit(err).await;
        };
        let res = execute_drop_index_catalog_update(
            self.engine,
            trx,
            self.table_id,
            self.index_no,
            old_index_spec,
        )
        .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => self.rollback_before_catalog_commit(err).await,
        }
    }

    async fn commit_catalog(&mut self) -> Result<TrxID> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let Some(trx) = self.trx.take() else {
            self.new_layout = None;
            self.phase = DropIndexBuildPhase::Aborted;
            return Err(drop_index_internal(
                "drop index transaction is missing before commit",
            ));
        };
        match trx.commit().await {
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

    fn take_layout_for_install(&mut self) -> Result<TableRuntimeLayout> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::CatalogCommitted);
        self.new_layout.take().ok_or_else(|| {
            drop_index_internal("drop index runtime layout is missing before install")
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::CatalogCommitted);
        self.phase = DropIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit<T>(&mut self, err: Error) -> Result<T> {
        self.new_layout = None;
        let rollback_res = rollback_active_ddl_trx(&mut self.trx).await;
        self.phase = DropIndexBuildPhase::Aborted;
        rollback_res?;
        Err(err)
    }

    async fn cleanup_after_catalog_commit_failure<T>(
        &mut self,
        operation: &'static str,
        source: Error,
    ) -> Result<T> {
        self.new_layout = None;
        self.phase = DropIndexBuildPhase::Aborted;
        Err(poison_index_after_catalog_commit_with_source(
            self.engine,
            IndexDdlKind::Drop,
            self.table_id,
            self.index_no,
            operation,
            source,
        )
        .into())
    }
}

async fn rollback_active_ddl_trx(trx: &mut Option<Transaction>) -> Result<()> {
    let Some(trx) = trx.take() else {
        return Ok(());
    };
    if trx.engine().is_some() {
        trx.rollback().await?;
    }
    Ok(())
}

impl Drop for DropIndexProgress<'_> {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(
            matches!(
                self.phase,
                DropIndexBuildPhase::Installed | DropIndexBuildPhase::Aborted
            ),
            "drop index state dropped before terminal cleanup: {:?}",
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
fn drop_index_internal(message: impl Into<String>) -> Error {
    Report::new(InternalError::Generic)
        .attach(message.into())
        .into()
}

#[inline]
fn drop_index_not_found(table_id: TableID, index_no: IndexNo, reason: &'static str) -> Error {
    Report::new(OperationError::IndexNotFound)
        .attach(format!(
            "drop index target not found: table_id={table_id}, index_no={index_no}, reason={reason}"
        ))
        .into()
}

#[inline]
fn validate_create_index_root_shape(
    table_id: TableID,
    active_root: &ActiveRoot,
    metadata: &TableMetadata,
) -> Result<()> {
    if active_root.metadata.as_ref() != metadata {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create index root metadata mismatch: table_id={table_id}"
            ))
            .into());
    }
    let expected_slots = metadata.idx.index_slot_count();
    let actual_slots = active_root.secondary_index_roots.len();
    if actual_slots != expected_slots {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create index secondary-root slot mismatch: table_id={table_id}, actual_slots={actual_slots}, expected_slots={expected_slots}"
            ))
            .into());
    }
    for (index_no, root) in active_root
        .secondary_index_roots
        .iter()
        .copied()
        .enumerate()
    {
        if metadata.idx.index_spec(index_no).is_none() && root != SUPER_BLOCK_ID {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "create index inactive secondary-root slot is non-empty before sparse slot reuse: table_id={table_id}, index_no={index_no}, root_block_id={root}, expected_root_block_id={SUPER_BLOCK_ID}"
                ))
                .into());
        }
    }
    Ok(())
}

#[inline]
fn validate_drop_index_root_shape(
    table_id: TableID,
    index_no: usize,
    active_root: &ActiveRoot,
    metadata: &TableMetadata,
) -> Result<()> {
    if active_root.metadata.as_ref() != metadata {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "drop index root metadata mismatch: table_id={table_id}"
            ))
            .into());
    }
    let expected_slots = metadata.idx.index_slot_count();
    let actual_slots = active_root.secondary_index_roots.len();
    if actual_slots != expected_slots {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "drop index secondary-root slot mismatch: table_id={table_id}, actual_slots={actual_slots}, expected_slots={expected_slots}"
            ))
            .into());
    }
    if metadata.idx.index_spec(index_no).is_none() {
        return Err(drop_index_not_found(
            table_id,
            index_no as IndexNo,
            "inactive metadata slot",
        ));
    }
    for (slot_no, root) in active_root
        .secondary_index_roots
        .iter()
        .copied()
        .enumerate()
    {
        if metadata.idx.index_spec(slot_no).is_none() && root != SUPER_BLOCK_ID {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "drop index inactive secondary-root slot is non-empty: table_id={table_id}, index_no={slot_no}, root_block_id={root}, expected_root_block_id={SUPER_BLOCK_ID}"
                ))
                .into());
        }
    }
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
                .checked_add(u64::from(delta))
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
            let key = block.decode_row_values(&metadata.col, row_idx, &read_set)?;
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
                    .encode_pair(&row.key, Val::from(row.row_id))
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
) -> Result<Vec<CreateIndexRowEntry>> {
    let mut rows = Vec::new();
    table
        .accessor_with_layout(layout.as_ref())
        .mem_scan_uncommitted(guards, |col_layout, row| {
            if row.is_deleted() {
                return true;
            }
            let key = index_spec
                .cols
                .iter()
                .map(|index_key| row.val(col_layout, index_key.col_no as usize))
                .collect();
            rows.push(CreateIndexRowEntry {
                key,
                row_id: row.row_id(),
            });
            true
        })
        .await?;
    Ok(rows)
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
    let ty_infer = |col_no| metadata.col.col_type(col_no);
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
    slots.resize_with(new_metadata.idx.index_slot_count(), || None);
    if slots.get(index_no).and_then(Option::as_ref).is_some() {
        return Err(create_index_internal(format!(
            "create index runtime slot is already occupied: index_no={index_no}"
        )));
    }
    slots[index_no] = Some(staged_index);
    TableRuntimeLayout::new(generation, new_metadata, slots.into_boxed_slice())
}

fn build_dropped_index_runtime_layout(
    table_id: TableID,
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index_no: usize,
) -> Result<TableRuntimeLayout> {
    let generation = old_layout
        .generation()
        .checked_add(1)
        .ok_or_else(|| drop_index_internal("table runtime layout generation overflow"))?;
    let mut slots = old_layout.secondary_indexes().to_vec();
    let Some(slot) = slots.get_mut(index_no) else {
        return Err(drop_index_not_found(
            table_id,
            index_no as IndexNo,
            "runtime slot out of range",
        ));
    };
    if slot.is_none() {
        return Err(drop_index_not_found(
            table_id,
            index_no as IndexNo,
            "inactive runtime slot",
        ));
    }
    *slot = None;
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
async fn execute_drop_index_catalog_update(
    engine: &EngineRef,
    trx: &mut Transaction,
    table_id: TableID,
    index_no: IndexNo,
    old_index_spec: &IndexSpec,
) -> Result<()> {
    trx.exec(async |stmt| {
        let deleted_columns = engine
            .catalog()
            .storage
            .index_columns()
            .delete_by_index(stmt, table_id, index_no)
            .await?;
        if deleted_columns != old_index_spec.cols.len() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "drop index catalog index-column delete count mismatch: table_id={table_id}, index_no={index_no}, deleted={deleted_columns}, expected={}",
                    old_index_spec.cols.len()
                ))
                .into());
        }

        let index_deleted = engine
            .catalog()
            .storage
            .indexes()
            .delete_by_id(stmt, table_id, index_no)
            .await;
        if !index_deleted {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "drop index catalog index row missing: table_id={table_id}, index_no={index_no}"
                ))
                .into());
        }

        let res = stmt
            .effects_mut()
            .set_ddl_redo(DDLRedo::DropIndex { table_id, index_no });
        debug_assert!(res.is_none());
        Ok(())
    })
    .await
}

#[inline]
async fn execute_create_index_catalog_update(
    engine: &EngineRef,
    trx: &mut Transaction,
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
                    next_index_no: metadata.idx.next_index_no(),
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
fn poison_index_after_catalog_commit_with_source(
    engine: &EngineRef,
    kind: IndexDdlKind,
    table_id: TableID,
    index_no: IndexNo,
    operation: &'static str,
    source: Error,
) -> Report<FatalError> {
    let operation_name = match kind {
        IndexDdlKind::Create => "create index",
        IndexDdlKind::Drop => "drop index",
    };
    let poison = engine.trx_sys.poison_storage(FatalError::Poisoned);
    source
        .into_report()
        .change_context(*poison.current_context())
        .attach(format!(
            "{operation_name} failed after catalog commit: table_id={table_id}, index_no={index_no}, operation={operation}"
        ))
}

/// Classify whether an active table root proves one index DDL redo durable.
pub(crate) fn classify_index_ddl_root(
    kind: IndexDdlKind,
    table_id: TableID,
    index_no: u16,
    ddl_cts: TrxID,
    active_root: Option<&ActiveRoot>,
) -> Result<IndexDdlRootProof> {
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
    let root_count = active_root.secondary_index_roots.len();
    let slot_count = metadata.idx.index_slot_count();
    // Metadata and sparse secondary-root slots describe the same index-number
    // space. A mismatch means the active root itself is malformed, not merely
    // inconclusive for this DDL marker.
    if root_count != slot_count {
        return Err(invalid_index_ddl_root(format!(
            "index DDL root proof found secondary-root count mismatch: table_id={table_id}, index_no={index_no}, root_count={root_count}, metadata_slots={slot_count}, root_ts={}, ddl_cts={ddl_cts}",
            active_root.root_ts
        )));
    }

    // `next_index_no` is the allocation boundary. If the DDL's index number is
    // still outside that boundary, the root cannot prove even allocation of the
    // index number, regardless of create/drop kind.
    if metadata.idx.next_index_no() <= index_no {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let Some(root_block_id) = active_root
        .secondary_index_roots
        .get(index_no as usize)
        .copied()
    else {
        return Err(invalid_index_ddl_root(format!(
            "index DDL root proof missing secondary-root slot: table_id={table_id}, index_no={index_no}, root_count={root_count}, root_ts={}, ddl_cts={ddl_cts}",
            active_root.root_ts
        )));
    };

    // From here the root is new enough and the index number has been allocated.
    // The active metadata decides whether the final durable state keeps the
    // index active or has made the slot inactive again.
    let active = metadata.idx.index_spec(index_no as usize).is_some();
    match (kind, active) {
        // CREATE INDEX is fully durable when the later/equal root still exposes
        // the created index as an active metadata entry.
        (IndexDdlKind::Create, true) => Ok(IndexDdlRootProof::DurableFinalCreate),
        (IndexDdlKind::Create, false) => {
            // The create's index number was allocated, but a later root no
            // longer has an active spec for it. This is valid only if the
            // sparse root slot is empty, matching a subsequent durable drop.
            if root_block_id != SUPER_BLOCK_ID {
                return Err(invalid_index_ddl_root(format!(
                    "inactive created index slot has non-empty root: table_id={table_id}, index_no={index_no}, root_block_id={root_block_id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            Ok(IndexDdlRootProof::DurableAllocationOnly)
        }
        // DROP INDEX is not proven by a root that still shows the index active.
        // Recovery must leave this DDL marker provisional and use catalog redo
        // replay decisions to converge from the durable root state.
        (IndexDdlKind::Drop, true) => Ok(IndexDdlRootProof::Provisional),
        (IndexDdlKind::Drop, false) => {
            // DROP INDEX is durable when the root is new enough, the index
            // number remains inside the allocation boundary, and the final slot
            // is inactive with no remaining secondary-root block.
            if root_block_id != SUPER_BLOCK_ID {
                return Err(invalid_index_ddl_root(format!(
                    "dropped index slot has non-empty root: table_id={table_id}, index_no={index_no}, root_block_id={root_block_id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            Ok(IndexDdlRootProof::DurableFinalDrop)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
        TableMetadata, tests::table2,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::ActiveRoot;
    use crate::row::ops::{DeleteMvcc, SelectKey};
    use crate::session::tests::SessionTestExt;
    use crate::table::CheckpointOutcome;
    use crate::trx::{MAX_SNAPSHOT_TS, Transaction};
    use crate::value::{Val, ValKind};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    const LIGHTWEIGHT_TEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const LIGHTWEIGHT_TEST_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    const LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

    fn columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("value", ValKind::I32, ColumnAttributes::empty()),
        ]
    }

    fn root_with_metadata(metadata: TableMetadata, root_ts: TrxID) -> ActiveRoot {
        ActiveRoot::new(root_ts, 128, Arc::new(metadata))
    }

    #[test]
    fn classify_create_index_root_proof_variants() {
        let active_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    1,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ),
            ],
            2,
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, TrxID::new(20));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                1,
                TrxID::new(19),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalCreate
        );

        let dropped_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();
        let dropped_root = root_with_metadata(dropped_metadata, TrxID::new(30));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                1,
                TrxID::new(19),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableAllocationOnly
        );

        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                1,
                TrxID::new(31),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );
    }

    #[test]
    fn classify_drop_index_requires_inactive_empty_slot() {
        let active_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    1,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ),
            ],
            2,
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, TrxID::new(20));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                1,
                TrxID::new(19),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );

        let dropped_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();
        let dropped_root = root_with_metadata(dropped_metadata, TrxID::new(20));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                1,
                TrxID::new(19),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalDrop
        );
    }

    #[test]
    fn validate_create_index_root_shape_rejects_non_empty_inactive_slot() {
        let metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();
        let mut active_root = root_with_metadata(metadata.clone(), TrxID::new(20));
        active_root.secondary_index_roots[1] = BlockID::new(99);

        let err = validate_create_index_root_shape(TableID::new(42), &active_root, &metadata)
            .unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        let report = format!("{err:?}");
        assert!(report.contains("inactive secondary-root slot"), "{report}");
        assert!(report.contains("index_no=1"), "{report}");
        assert!(report.contains("root_block_id=99"), "{report}");
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
    fn test_create_index_builds_non_unique_hot_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row1 =
                insert_one_row(&table, &mut session, vec![Val::from(1), Val::from("alpha")]).await;
            let _row2 =
                insert_one_row(&table, &mut session, vec![Val::from(2), Val::from("beta")]).await;
            let row3 =
                insert_one_row(&table, &mut session, vec![Val::from(3), Val::from("alpha")]).await;
            let old_generation = table.layout_snapshot().generation();

            let index_no = session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();

            assert_eq!(index_no, 1);
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert!(table.metadata().idx.index_spec(1).is_some());
            assert_eq!(table.layout_snapshot().generation(), old_generation + 1);
            assert_eq!(active_secondary_root(&table, 1), SUPER_BLOCK_ID);
            let table_object = engine
                .catalog()
                .storage
                .tables()
                .find_uncommitted_by_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table_object.next_index_no, 2);

            let layout = table.layout_snapshot();
            let root = active_secondary_root(&table, 1);
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                1,
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3]);

            let row4 =
                insert_one_row(&table, &mut session, vec![Val::from(4), Val::from("alpha")]).await;
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                1,
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3, row4]);
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
            insert_rows(&table, &mut session, 10, 8, "cold").await;
            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            checkpoint_published(&table, &mut session).await;

            let index_no = session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();

            assert_eq!(index_no, 1);
            assert_ne!(active_secondary_root(&table, 1), SUPER_BLOCK_ID);
            let mut rows =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key("cold"))
                    .await;
            rows.sort_unstable();
            assert_eq!(rows.len(), 8);
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
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();
            engine.inner().trx_sys.request_table_root_retention_purge();

            for _ in 0..10 {
                smol::Timer::after(Duration::from_millis(10)).await;
                assert_eq!(
                    old_root_drop_count(retained_root_ptr),
                    drop_count_before,
                    "create index old root must stay retained while an earlier transaction is active"
                );
            }

            read_trx.commit().await.unwrap();
            engine.inner().trx_sys.request_table_root_retention_purge();
            for _ in 0..100 {
                if old_root_drop_count(retained_root_ptr) > drop_count_before {
                    return;
                }
                smol::Timer::after(Duration::from_millis(10)).await;
            }
            panic!("create index old root was not released after purge crossed the fence");
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
            insert_one_row(&table, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(&table, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            let root_before = table.file().active_root_unchecked().clone();
            let old_generation = table.layout_snapshot().generation();

            let err = session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
                )
                .await
                .unwrap_err();

            assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
            assert_root_metadata_unchanged(&root_before, &table);
            assert_eq!(table.layout_snapshot().generation(), old_generation);
            assert_eq!(table.metadata().idx.next_index_no(), 1);
            assert!(table.metadata().idx.index_spec(1).is_none());
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
                insert_one_row(&table, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(&table, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            checkpoint_published(&table, &mut session).await;
            delete_one_row(&table, &mut session, &single_key(2)).await;

            let index_no = session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
                )
                .await
                .unwrap();

            assert_eq!(index_no, 1);
            assert_eq!(
                unique_runtime_lookup(&table, 1, &session.pool_guards(), &[Val::from("dup")]).await,
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
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap_err();

            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_create_index_recovery_loads_published_index() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir.clone(), "create_index_recover")
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                &table,
                &mut session,
                vec![Val::from(1), Val::from("persisted")],
            )
            .await;
            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            checkpoint_published(&table, &mut session).await;
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    )
                    .await
                    .unwrap(),
                1
            );
            drop(session);
            drop(table);
            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, "create_index_recover")
                .build()
                .await
                .unwrap();
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert!(table.metadata().idx.index_spec(1).is_some());
            let session = engine.new_session().unwrap();
            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &SelectKey::new(1, vec![Val::from("persisted")]),
                )
                .await,
                vec![row_id]
            );
        });
    }

    #[test]
    fn test_drop_index_removes_sparse_slot_and_preserves_allocation_after_restart() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_stem = "drop-index-recover";
            let engine = lightweight_test_engine_config(main_dir.clone(), log_stem)
                .build()
                .await
                .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    )
                    .await
                    .unwrap(),
                1
            );
            session.drop_index(table_id, 1).await.unwrap();

            let metadata = table.metadata();
            assert_eq!(metadata.idx.next_index_no(), 2);
            assert!(metadata.idx.index_spec(0).is_some());
            assert!(metadata.idx.index_spec(1).is_none());
            let root = table.file().active_root_unchecked();
            assert_eq!(root.secondary_index_roots.len(), 2);
            assert_eq!(root.secondary_index_roots[1], SUPER_BLOCK_ID);
            let catalog_indexes = engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap();
            assert_eq!(catalog_indexes.len(), 1);
            assert_eq!(catalog_indexes[0].index_no, 0);
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            drop(session);
            drop(table);
            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, log_stem)
                .build()
                .await
                .unwrap();
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().idx.next_index_no(), 2);
            assert!(table.metadata().idx.index_spec(1).is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_roots[1],
                SUPER_BLOCK_ID
            );

            let mut session = engine.new_session().unwrap();
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    )
                    .await
                    .unwrap(),
                2
            );
        });
    }

    #[test]
    fn test_drop_unique_and_primary_indexes_remove_uniqueness_enforcement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_one_row(&table, &mut session, vec![Val::from(1), Val::from("same")]).await;

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
                    )
                    .await
                    .unwrap(),
                1
            );
            session.drop_index(table_id, 1).await.unwrap();
            insert_one_row(&table, &mut session, vec![Val::from(2), Val::from("same")]).await;

            session.drop_index(table_id, 0).await.unwrap();
            insert_one_row(
                &table,
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

            let trx = session.begin_trx().unwrap();
            let err = session.drop_index(table_id, 0).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
            trx.rollback().await.unwrap();

            let root_before = table.file().active_root_unchecked().clone();
            let old_generation = table.layout_snapshot().generation();
            let err = session.drop_index(table_id, 1).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::IndexNotFound));
            assert_root_metadata_unchanged(&root_before, &table);
            assert_eq!(table.layout_snapshot().generation(), old_generation);

            session.drop_index(table_id, 0).await.unwrap();
            let root_before = table.file().active_root_unchecked().clone();
            let old_generation = table.layout_snapshot().generation();
            let err = session.drop_index(table_id, 0).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::IndexNotFound));
            assert_root_metadata_unchanged(&root_before, &table);
            assert_eq!(table.layout_snapshot().generation(), old_generation);
        });
    }

    #[test]
    fn test_drop_index_runtime_install_retires_removed_runtime() {
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
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    )
                    .await
                    .unwrap(),
                1
            );
            let old_layout = table.layout_snapshot();
            let old_generation = old_layout.generation();
            let old_pk = Arc::clone(old_layout.secondary_indexes()[0].as_ref().unwrap());

            session.drop_index(table_id, 1).await.unwrap();

            let installed = table.layout_snapshot();
            assert_eq!(installed.generation(), old_generation + 1);
            assert!(installed.secondary_indexes()[1].is_none());
            assert!(Arc::ptr_eq(
                installed.secondary_indexes()[0].as_ref().unwrap(),
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

    async fn lightweight_test_engine(temp_dir: &TempDir, log_file_stem: &str) -> Engine {
        lightweight_test_engine_config(temp_dir.path().to_path_buf(), log_file_stem)
            .build()
            .await
            .unwrap()
    }

    fn table_for_internal_assertion(engine: &Engine, table_id: TableID) -> Arc<Table> {
        engine
            .catalog()
            .get_table_now(table_id)
            .expect("test table should exist")
    }

    fn lightweight_test_engine_config(
        main_dir: impl Into<std::path::PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .meta_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
            .index_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
            .index_max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(LIGHTWEIGHT_TEST_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES),
            )
            .trx(
                TrxSysConfig::default()
                    .io_depth(1)
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

    async fn trx_insert_row(trx: &mut Transaction, table: &Table, cols: Vec<Val>) -> Result<RowID> {
        trx.exec(async |stmt| stmt.table_insert_mvcc(table.table_id(), cols).await)
            .await
    }

    async fn insert_one_row(table: &Table, session: &mut Session, values: Vec<Val>) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let insert = trx_insert_row(&mut trx, table, values).await;
        let Ok(row_id) = insert else {
            panic!("insert should succeed: {insert:?}");
        };
        trx.commit().await.unwrap();
        row_id
    }

    async fn insert_rows(table: &Table, session: &mut Session, start: i32, count: i32, name: &str) {
        let mut trx = session.begin_trx().unwrap();
        for i in 0..count {
            let insert = vec![Val::from(start + i), Val::from(name)];
            let res = trx_insert_row(&mut trx, table, insert).await;
            assert!(res.is_ok());
        }
        trx.commit().await.unwrap();
    }

    async fn delete_one_row(table: &Table, session: &mut Session, key: &SelectKey) {
        let mut trx = session.begin_trx().unwrap();
        let delete = trx
            .exec(async |stmt| {
                stmt.table_delete_unique_mvcc(table.table_id(), key, false)
                    .await
            })
            .await;
        if !matches!(delete, Ok(DeleteMvcc::Deleted)) {
            panic!("delete should succeed: {delete:?}");
        }
        trx.commit().await.unwrap();
    }

    fn single_key<V: Into<Val>>(value: V) -> SelectKey {
        SelectKey {
            index_no: 0,
            vals: vec![value.into()],
        }
    }

    fn name_key(value: &str) -> SelectKey {
        SelectKey {
            index_no: 1,
            vals: vec![Val::from(value)],
        }
    }

    fn active_secondary_root(table: &Table, index_no: usize) -> BlockID {
        table.file().active_root_unchecked().secondary_index_roots[index_no]
    }

    async fn unique_runtime_lookup(
        table: &Table,
        index_no: usize,
        guards: &PoolGuards,
        key: &[Val],
    ) -> Option<(RowID, bool)> {
        let root = active_secondary_root(table, index_no);
        let layout = table.layout_snapshot();
        let index = layout
            .secondary_index(index_no)
            .unwrap()
            .bind_unique(root)
            .unwrap();
        index
            .lookup(guards.index_guard(), key, MAX_SNAPSHOT_TS)
            .await
            .unwrap()
    }

    async fn non_unique_runtime_lookup(
        layout: &Arc<TableRuntimeLayout>,
        root: BlockID,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
    ) -> Vec<RowID> {
        let index = layout
            .secondary_index(index_no)
            .unwrap()
            .bind_non_unique(root)
            .unwrap();
        let mut rows = Vec::new();
        index
            .lookup(guards.index_guard(), key, &mut rows, MAX_SNAPSHOT_TS)
            .await
            .unwrap();
        rows
    }

    async fn non_unique_disk_tree_prefix_scan(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Vec<RowID> {
        let root = active_secondary_root(table, key.index_no);
        let layout = table.layout_snapshot();
        let tree = layout
            .secondary_index(key.index_no)
            .unwrap()
            .disk_runtime()
            .open_non_unique_at(root, guards.disk_guard())
            .unwrap();
        tree.prefix_scan_entries(&key.vals)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, row_id)| row_id)
            .collect()
    }

    async fn checkpoint_published(table: &Table, session: &mut Session) -> TrxID {
        let mut last_delay = None;
        for _ in 0..50 {
            match session.checkpoint_table(table.table_id()).await.unwrap() {
                CheckpointOutcome::Published { checkpoint_ts } => {
                    return checkpoint_ts;
                }
                CheckpointOutcome::Delayed { reason } => {
                    last_delay = Some(reason);
                    smol::Timer::after(Duration::from_millis(20)).await;
                }
                CheckpointOutcome::Cancelled { reason } => {
                    panic!("checkpoint should publish, cancelled by {reason:?}")
                }
            }
        }
        panic!(
            "checkpoint should publish, delayed after retries by {:?}",
            last_delay.unwrap()
        )
    }

    fn assert_root_metadata_unchanged(before: &ActiveRoot, table: &Table) {
        let after = table.file().active_root_unchecked();
        assert_eq!(after.root_ts, before.root_ts);
        assert_eq!(after.meta_block_id, before.meta_block_id);
        assert_eq!(after.pivot_row_id, before.pivot_row_id);
        assert_eq!(after.heap_redo_start_ts, before.heap_redo_start_ts);
        assert_eq!(after.deletion_cutoff_ts, before.deletion_cutoff_ts);
        assert_eq!(
            after.secondary_index_roots, before.secondary_index_roots,
            "secondary index roots changed"
        );
        assert!(Arc::ptr_eq(&after.metadata, &before.metadata));
    }
}
