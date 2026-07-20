use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards};
use crate::catalog::{
    CatalogCheckpointOutcome, IndexNo, IndexSpec, TableSpec, create_index_for_session,
    create_table_for_session, drop_index_for_session, drop_table_for_session,
};
use crate::engine::{EngineInner, EngineRef, WeakEngineRef};
use crate::error::{
    Error, InternalError, InternalResult, LifecycleError, LifecycleResult, OperationError,
    OperationResult, Result,
};
use crate::id::{SessionID, TableID, TrxID};
use crate::lock::{LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource};
use crate::map::{FastDashMap, FastHashMap};
use crate::notify::ChangeNotifier;
use crate::quiescent::QuiescentGuard;
use crate::stats::{
    BufferPoolStats, StorageIoStats, TransactionSystemStats, buffer_pool_runtime_stats_snapshot,
    storage_io_stats_snapshot, transaction_system_stats_snapshot,
};
use crate::table::{
    CheckpointDelayReason, CheckpointOutcome, FreezeOutcome, SecondaryMemIndexCleanupStats, Table,
};
use crate::trx::{StartedTransaction, Transaction, TrxCleanupReason, TrxEntry, TrxEntryState};
use error_stack::{Report, ResultExt};
use futures::future::select_all;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// Summary returned by a redo-log truncation maintenance call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedoTruncationOutcome {
    /// Durable first retained redo file sequence observed before this call published a marker.
    pub previous_first_retained_file_seq: u32,
    /// Durable first retained redo file sequence after any marker publication in this call.
    pub new_first_retained_file_seq: u32,
    /// Number of newly planned sealed prefix files covered by marker advancement.
    pub advanced_files: usize,
    /// Number of obsolete redo files physically removed during this call.
    pub removed_files: usize,
    /// Number of obsolete redo files that disappeared before unlink completed.
    pub already_missing_files: usize,
    /// Number of obsolete redo files that could not be unlinked and remain retryable.
    pub failed_unlink_files: usize,
    /// Current blockers that prevented truncation candidate growth.
    pub blockers: Vec<RedoTruncationBlockerInfo>,
}

/// Public reason that a retained redo file could not be truncated yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedoTruncationBlockerInfo {
    /// Catalog recovery still needs redo at or above the catalog replay boundary.
    CatalogFloor {
        /// Current catalog replay boundary.
        catalog_replay_start_ts: TrxID,
    },
    /// A live user table still needs redo at one of its replay floors.
    LiveTableFloor {
        /// User table id.
        table_id: TableID,
        /// Heap replay boundary copied from the table active root.
        heap_redo_start_ts: TrxID,
        /// Cold-delete replay boundary copied from the table active root.
        deletion_cutoff_ts: TrxID,
    },
    /// A logically dropped table still needs redo until catalog absence is checkpointed.
    PendingDroppedTableFloor {
        /// Dropped user table id.
        table_id: TableID,
        /// Commit timestamp of the logical table drop.
        drop_cts: TrxID,
        /// Heap replay boundary copied before runtime destruction.
        heap_redo_start_ts: TrxID,
        /// Cold-delete replay boundary copied before runtime destruction.
        deletion_cutoff_ts: TrxID,
    },
    /// The retained prefix reached the active unsealed redo file.
    UnsealedFile {
        /// Redo file sequence of the unsealed file.
        file_seq: u32,
    },
}

/// Summary returned by combined catalog checkpoint and redo-log truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRedoMaintenanceOutcome {
    /// Outcome of the catalog checkpoint portion of the maintenance call.
    pub catalog_checkpoint: CatalogCheckpointOutcome,
    /// Outcome of the redo-log truncation portion of the maintenance call.
    pub redo_truncation: RedoTruncationOutcome,
}

/// Shared session-level DDL admission context.
pub(crate) struct SessionDdlContext {
    /// Engine handle used for catalog and table access.
    pub(crate) engine: EngineRef,
    /// Pool guards retained for DDL work.
    pub(crate) pool_guards: PoolGuards,
    /// Session-level lock owner.
    pub(crate) owner: LockOwner,
    /// Session-level lock owner group.
    pub(crate) owner_group: LockOwnerGroup,
}

impl SessionDdlContext {
    /// Creates DDL admission context from a pinned idle session.
    #[inline]
    pub(crate) fn new(session: &SessionPin) -> OperationResult<Self> {
        if session
            .in_trx()
            .change_context(OperationError::NotSupported)
            .attach_with(|| "phase=inspect_session_lifecycle")?
        {
            return Err(
                Report::new(OperationError::NotSupported).attach("implicit commit due to DDL")
            );
        }
        let id = session.id();
        let pool_guards = session.pool_guards();
        Ok(Self {
            engine: session.engine.clone(),
            pool_guards,
            owner: LockOwner::Session(id),
            owner_group: LockOwnerGroup::Session(id),
        })
    }
}

#[derive(Clone, Copy)]
enum MaintenanceBoundary {
    GcHorizon,
    PurgeCompletion,
}

impl MaintenanceBoundary {
    #[inline]
    fn name(self) -> &'static str {
        match self {
            MaintenanceBoundary::GcHorizon => "GC horizon",
            MaintenanceBoundary::PurgeCompletion => "purge completion",
        }
    }

    #[inline]
    fn observed(self, session: &SessionPin) -> TrxID {
        match self {
            MaintenanceBoundary::GcHorizon => session.engine.trx_sys.published_gc_horizon(),
            MaintenanceBoundary::PurgeCompletion => session.engine.trx_sys.global_visible_sts(),
        }
    }

    #[inline]
    fn listener(self, session: &SessionPin) -> event_listener::EventListener {
        match self {
            MaintenanceBoundary::GcHorizon => session.engine.trx_sys.gc_horizon_listener(),
            MaintenanceBoundary::PurgeCompletion => {
                session.engine.trx_sys.purge_completion_listener()
            }
        }
    }
}

/// Weak, non-cloneable public session capability bound to one engine instance.
///
/// The engine owns the strong session state in its internal session registry.
/// Public session operations upgrade weak engine reachability internally, pin
/// the registry-owned state for one operation, and release registry/admission
/// guards before async work.
pub struct Session {
    id: SessionID,
    engine: WeakEngineRef,
    closed: AtomicBool,
}

impl Session {
    /// Creates a weak public session handle.
    #[inline]
    pub(crate) fn new(engine: WeakEngineRef, id: SessionID) -> Self {
        Session {
            id,
            engine,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns a pinned operation view of this session.
    #[inline]
    pub(crate) fn pin(&self) -> LifecycleResult<SessionPin> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_session_report(self.id));
        }
        let engine = self
            .engine
            .upgrade()
            .attach_with(|| format!("session_id={}, phase=upgrade_engine_runtime", self.id))?;
        let admission = engine
            .acquire_admission()
            .attach_with(|| format!("session_id={}", self.id))?;
        let state = engine.session_registry.pin_running(self.id)?;
        drop(admission);
        Ok(SessionPin { engine, state })
    }

    #[inline]
    fn query_session(&self) -> LifecycleResult<SessionQueryPin> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_session_report(self.id));
        }
        let engine = self
            .engine
            .upgrade()
            .attach_with(|| format!("session_id={}, phase=upgrade_engine_runtime", self.id))?;
        engine.ensure_admission_open_for_query().attach_with(|| {
            format!("session_id={}, phase=check_engine_query_admission", self.id)
        })?;
        let state = engine.session_registry.pin_running(self.id)?;
        Ok(SessionQueryPin {
            engine,
            _state: state,
        })
    }

    /// Return sorted ids for currently loaded user-table runtimes.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// engine query admission is still open. It is not available after engine
    /// shutdown, session close, or registry removal. It does not expose
    /// in-flight DDL rows that have not installed a user-table runtime.
    #[inline]
    pub fn list_table_ids(&self) -> Result<Vec<TableID>> {
        let session = self.query_session().attach("operation=list_table_ids")?;
        Ok(session.engine.catalog().list_user_table_ids_now())
    }

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn begin_trx(&mut self) -> Result<Transaction> {
        let session = self.pin().attach("operation=begin_transaction")?;
        Ok(session.begin_trx().attach("operation=begin_transaction")?)
    }

    /// Close this session when it has no active transaction.
    #[inline]
    pub async fn close(&mut self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let engine = self.engine.upgrade().attach_with(|| {
            format!(
                "operation=close_session, session_id={}, phase=upgrade_engine_runtime",
                self.id
            )
        })?;
        let _admission = engine
            .acquire_admission()
            .attach_with(|| format!("operation=close_session, session_id={}", self.id))?;
        engine
            .session_registry
            .close_idle(self.id)
            .attach("operation=close_session")?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        let session = self.pin().attach("operation=create_table")?;
        create_table_for_session(session, table_spec, index_specs).await
    }

    /// Build and publish a new secondary index for an existing user table.
    #[inline]
    pub async fn create_index(
        &mut self,
        table_id: TableID,
        index_spec: IndexSpec,
    ) -> Result<IndexNo> {
        let session = self.pin().attach("operation=create_index")?;
        create_index_for_session(session, table_id, index_spec).await
    }

    /// Logically drop an active secondary index from an existing user table.
    #[inline]
    pub async fn drop_index(&mut self, table_id: TableID, index_no: IndexNo) -> Result<()> {
        let session = self.pin().attach("operation=drop_index")?;
        drop_index_for_session(session, table_id, index_no).await
    }

    /// Logically drop an existing user table.
    #[inline]
    pub async fn drop_table(&mut self, table_id: TableID) -> Result<()> {
        let session = self.pin().attach("operation=drop_table")?;
        drop_table_for_session(session, table_id).await
    }

    /// Run one online catalog checkpoint.
    ///
    /// This mutating maintenance operation uses normal healthy-runtime
    /// admission and requires the session to be idle. A successful publish also
    /// refreshes internal catalog-safe redo retention progress for future
    /// truncation planning.
    #[inline]
    pub async fn checkpoint_catalog(&mut self) -> Result<()> {
        let session = self.pin().attach("operation=checkpoint_catalog")?;
        if session.in_trx().attach("operation=checkpoint_catalog")? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("catalog checkpoint requires an idle session")
                .into());
        }
        session
            .engine
            .catalog()
            .checkpoint_now(&session.engine.trx_sys)
            .await
            .map(|_| ())
    }

    /// Run catalog checkpoint and redo-log truncation as one maintenance operation.
    ///
    /// When both catalog checkpoint metadata and the durable first-retained redo
    /// marker advance, this operation publishes them in one `catalog.mtb` root
    /// before unlinking obsolete redo files.
    #[inline]
    pub async fn checkpoint_catalog_and_truncate_redo_log(
        &mut self,
    ) -> Result<CatalogRedoMaintenanceOutcome> {
        let session = self
            .pin()
            .attach("operation=checkpoint_catalog_and_truncate_redo_log")?;
        if session
            .in_trx()
            .attach("operation=checkpoint_catalog_and_truncate_redo_log")?
        {
            return Err(Report::new(OperationError::NotSupported)
                .attach("combined catalog checkpoint and redo truncation requires an idle session")
                .into());
        }
        session
            .engine
            .trx_sys
            .checkpoint_catalog_and_truncate_redo_log()
            .await
    }

    /// Physically remove recovery-obsolete sealed redo prefix files.
    ///
    /// The operation first advances the durable first-retained redo marker when
    /// the current retention plan has eligible candidates, then unlinks present
    /// redo files below the final marker. Non-`NotFound` unlink failures are
    /// summarized in the returned outcome and can be retried by a later call.
    #[inline]
    pub async fn truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome> {
        let session = self.pin().attach("operation=truncate_redo_log")?;
        if session.in_trx().attach("operation=truncate_redo_log")? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("redo log truncation requires an idle session")
                .into());
        }
        session.engine.trx_sys.truncate_redo_log().await
    }

    /// Return a monotonic transaction-system statistics snapshot.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// engine query admission is still open. It is not available after engine
    /// shutdown, session close, or registry removal. Callers can compare
    /// snapshots to compute deltas.
    #[inline]
    pub fn transaction_system_stats(&self) -> Result<TransactionSystemStats> {
        let session = self
            .query_session()
            .attach("operation=query_transaction_system_stats")?;
        let engine = &session.engine;
        Ok(transaction_system_stats_snapshot(
            engine.trx_sys.trx_sys_stats(),
        ))
    }

    /// Return a monotonic shared-storage IO statistics snapshot.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// engine query admission is still open. It is not available after engine
    /// shutdown, session close, or registry removal. Callers can compare
    /// snapshots to compute deltas.
    #[inline]
    pub fn storage_io_stats(&self) -> Result<StorageIoStats> {
        let session = self
            .query_session()
            .attach("operation=query_storage_io_stats")?;
        let engine = &session.engine;
        Ok(storage_io_stats_snapshot(
            engine.table_fs.io_backend_stats(),
            engine.table_fs.storage_service_stats(),
        ))
    }

    /// Return point-in-time buffer-pool capacity, allocation, and counters.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// engine query admission is still open. It is not available after engine
    /// shutdown, session close, or registry removal. Counters are monotonic
    /// snapshots and callers can compare snapshots to compute deltas.
    #[inline]
    pub fn buffer_pool_stats(&self) -> Result<BufferPoolStats> {
        let session = self
            .query_session()
            .attach("operation=query_buffer_pool_stats")?;
        let engine = &session.engine;
        Ok(BufferPoolStats {
            meta: buffer_pool_runtime_stats_snapshot(
                engine.meta_pool.capacity(),
                engine.meta_pool.allocated(),
                engine.meta_pool.stats(),
            ),
            mem: buffer_pool_runtime_stats_snapshot(
                engine.mem_pool.capacity(),
                engine.mem_pool.allocated(),
                engine.mem_pool.stats(),
            ),
            index: buffer_pool_runtime_stats_snapshot(
                engine.index_pool.capacity(),
                engine.index_pool.allocated(),
                engine.index_pool.stats(),
            ),
            disk: buffer_pool_runtime_stats_snapshot(
                engine.disk_pool.capacity(),
                engine.disk_pool.allocated(),
                engine.disk_pool.stats(),
            ),
        })
    }

    /// Freeze a row-page prefix or report the existing table-owned batch.
    #[inline]
    pub async fn freeze_table(
        &mut self,
        table_id: TableID,
        max_rows: usize,
    ) -> Result<FreezeOutcome> {
        let session = self.pin().attach("operation=freeze_table")?;
        ensure_idle_maintenance_session(&session).attach("operation=freeze_table")?;
        let table = session
            .resolve_user_table(table_id)
            .await
            .attach("operation=freeze_table")?;

        // Acquire maintenance admission before entering the freeze workflow. The
        // grouped helper orders TableMetadata(S) before TableData(IS), excluding
        // metadata-X DDL and data-X full-table updates while remaining compatible
        // with ordinary IX DML. Keep the scoped guards through batch publication.
        let lock_manager = session.engine.lock_manager();
        let owner = LockOwner::Session(session.id());
        let owner_group = LockOwnerGroup::Session(session.id());
        let (_metadata_lock, _data_lock) = lock_manager
            .acquire_grouped_table_locks(table_id, LockMode::IntentShared, owner, owner_group)
            .await
            .attach_with(|| format!("operation=freeze_table, table_id={table_id}"))?;
        // Lock acquisition can wait, so the table may have started dropping since
        // resolution. Revalidate only after both maintenance locks are granted.
        table
            .check_foreground_live()
            .attach("operation=freeze_table")?;
        table.freeze(&session, max_rows).await
    }

    /// Persist eligible state using the table-owned canonical frozen batch.
    #[inline]
    pub async fn checkpoint_table(&mut self, table_id: TableID) -> Result<CheckpointOutcome> {
        let session = self.pin().attach("operation=checkpoint_table")?;
        ensure_idle_maintenance_session(&session).attach("operation=checkpoint_table")?;
        let table = session
            .resolve_existing_user_table(table_id)
            .await
            .attach("operation=checkpoint_table")?;

        // Acquire maintenance admission before entering the checkpoint workflow.
        // The grouped helper orders TableMetadata(S) before TableData(IS), excluding
        // metadata-X DDL and data-X full-table updates while remaining compatible
        // with ordinary IX DML. Keep the scoped guards through root publication.
        let lock_manager = session.engine.lock_manager();
        let owner = LockOwner::Session(session.id());
        let owner_group = LockOwnerGroup::Session(session.id());
        let (_metadata_lock, _data_lock) = lock_manager
            .acquire_grouped_table_locks(table_id, LockMode::IntentShared, owner, owner_group)
            .await
            .attach_with(|| format!("operation=checkpoint_table, table_id={table_id}"))?;
        // Lock acquisition can wait, so the table may have started dropping since
        // resolution. Revalidate only after both maintenance locks are granted.
        table
            .check_foreground_live()
            .attach("operation=checkpoint_table")?;
        table.checkpoint(&session).await
    }

    /// Wait until retry may be useful for one self-identifying checkpoint delay.
    ///
    /// Completion means the observed predicate is satisfied or obsolete; a
    /// later checkpoint attempt can still encounter a different delay.
    pub async fn wait_for_checkpoint_retry(&self, reason: CheckpointDelayReason) -> Result<()> {
        let session = self.pin().attach("operation=wait_for_checkpoint_retry")?;
        ensure_idle_maintenance_session(&session).attach("operation=wait_for_checkpoint_retry")?;
        let table_id = match reason {
            CheckpointDelayReason::ActiveRoot { table_id, .. }
            | CheckpointDelayReason::FrozenPageCutoff { table_id, .. } => table_id,
        };
        let table = match session.state.cached_user_table(table_id) {
            Some(table) => table,
            None => {
                let Some(table) = session.engine.catalog().get_table(table_id).await else {
                    return Ok(());
                };
                session.state.cache_user_table(&table);
                table
            }
        };
        table.wait_for_checkpoint_retry(&session, reason).await
    }

    /// Retry delayed table checkpoints through their exact production wait predicate.
    ///
    /// Published and cancelled outcomes are returned unchanged. Only a normal
    /// [`CheckpointOutcome::Delayed`] result is waited and retried.
    pub async fn checkpoint_table_with_wait(
        &mut self,
        table_id: TableID,
    ) -> Result<CheckpointOutcome> {
        loop {
            match self.checkpoint_table(table_id).await? {
                CheckpointOutcome::Delayed { reason } => {
                    self.wait_for_checkpoint_retry(reason).await?;
                }
                outcome => return Ok(outcome),
            }
        }
    }

    /// Wait for the purge-published active horizon to become strictly newer.
    ///
    /// This boundary is published before physical purge work completes and is
    /// suitable for checkpoint cutoff and active-snapshot readiness decisions.
    pub async fn wait_for_gc_horizon_after(&self, ts: TrxID) -> Result<TrxID> {
        let session = self.pin().attach("operation=wait_for_gc_horizon")?;
        ensure_idle_maintenance_session(&session).attach("operation=wait_for_gc_horizon")?;
        wait_for_maintenance_boundary(&session, ts, MaintenanceBoundary::GcHorizon).await
    }

    /// Wait for completed purge-horizon-cycle progress to become strictly newer.
    ///
    /// The returned boundary is published only after eligible undo/index,
    /// retired-page, retained-root, and coalesced cleanup work completes.
    pub async fn wait_for_purge_completion_after(&self, ts: TrxID) -> Result<TrxID> {
        let session = self.pin().attach("operation=wait_for_purge_completion")?;
        ensure_idle_maintenance_session(&session).attach("operation=wait_for_purge_completion")?;
        wait_for_maintenance_boundary(&session, ts, MaintenanceBoundary::PurgeCompletion).await
    }

    /// Wait until ordered commit payloads through `ts` reach purge coordination.
    #[cfg(test)]
    pub(crate) async fn wait_for_purge_handoff_for_test(&self, ts: TrxID) -> Result<()> {
        let session = self.pin().attach("operation=wait_for_purge_handoff")?;
        ensure_idle_maintenance_session(&session).attach("operation=wait_for_purge_handoff")?;
        wait_for_purge_handoff(&session, ts).await
    }

    /// Returns total number of hot row pages for an existing user table.
    #[inline]
    pub async fn total_row_pages(&self, table_id: TableID) -> Result<usize> {
        let session = self.pin().attach("operation=count_table_row_pages")?;
        let table = session
            .resolve_user_table(table_id)
            .await
            .attach("operation=count_table_row_pages")?;
        let guards = session.pool_guards();
        table.total_row_pages(&guards).await.map_err(|err| {
            err.attach(format!(
                "operation=count_table_row_pages, table_id={table_id}"
            ))
        })
    }

    /// Full-scan cleanup for an existing user table's secondary MemIndex entries.
    #[inline]
    pub async fn cleanup_secondary_mem_indexes(
        &mut self,
        table_id: TableID,
        clean_live_entries: bool,
    ) -> Result<SecondaryMemIndexCleanupStats> {
        let session = self
            .pin()
            .attach("operation=cleanup_secondary_mem_indexes")?;
        let table = session
            .resolve_user_table(table_id)
            .await
            .attach("operation=cleanup_secondary_mem_indexes")?;
        table
            .cleanup_secondary_mem_indexes(session, clean_live_entries)
            .await
    }

    /// Acquires an explicit session-lifetime table lock.
    #[inline]
    pub async fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()> {
        mode.validate_explicit_table_lock()
            .attach_with(|| format!("operation=lock_explicit_table, table_id={table_id}"))?;
        let session = self.pin().attach("operation=lock_explicit_table")?;
        Ok(session
            .lock_table(table_id, mode)
            .await
            .attach_with(|| format!("operation=lock_explicit_table, table_id={table_id}"))?)
    }

    /// Releases an explicit session-lifetime table lock when no transaction is active.
    #[inline]
    pub fn unlock_table(&self, table_id: TableID) -> Result<()> {
        let session = self.pin().attach("operation=unlock_explicit_table")?;
        Ok(session
            .unlock_table(table_id)
            .attach_with(|| format!("operation=unlock_explicit_table, table_id={table_id}"))?)
    }
}

impl Drop for Session {
    #[inline]
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(engine) = self.engine.upgrade_for_cleanup() {
            engine.session_registry.abandon(self.id);
        }
    }
}

/// Query-scoped strong pin for poison-observable public reads.
struct SessionQueryPin {
    /// Engine handle retained while the query reads shared runtime state.
    engine: EngineRef,
    /// Strong reference proving the session is still registered and running.
    _state: Arc<SessionState>,
}

/// One operation-scoped strong pin of a registry-owned session.
pub(crate) struct SessionPin {
    /// Engine handle retained for the duration of this operation.
    pub(crate) engine: EngineRef,
    /// Strong reference to registry-owned session state.
    pub(crate) state: Arc<SessionState>,
}

impl SessionPin {
    /// Returns the engine-local session identity.
    #[inline]
    pub(crate) fn id(&self) -> SessionID {
        self.state.id()
    }

    /// Returns a cloned guard bundle for this pinned operation.
    #[inline]
    pub(crate) fn pool_guards(&self) -> PoolGuards {
        self.state.pool_guards().clone()
    }

    /// Begin a new transaction from this already pinned session operation.
    #[inline]
    pub(crate) fn begin_trx(&self) -> LifecycleResult<Transaction> {
        let engine = &self.engine;
        self.state
            .begin_trx(|| engine.trx_sys.begin_trx(engine, &self.state))
    }

    /// Returns whether the pinned session is in an active transaction.
    #[inline]
    pub(crate) fn in_trx(&self) -> LifecycleResult<bool> {
        self.state.in_trx()
    }

    /// Resolve a live user table, checking cached entries first.
    #[inline]
    pub(crate) async fn resolve_user_table(
        &self,
        table_id: TableID,
    ) -> OperationResult<Arc<Table>> {
        if let Some(table) = self.state.cached_user_table(table_id) {
            table.check_foreground_live()?;
            return Ok(table);
        }
        let table = self
            .engine
            .catalog()
            .validate_user_table_live(table_id)
            .await?;
        self.state.cache_user_table(&table);
        Ok(table)
    }

    /// Resolve an existing user table without requiring it to be foreground-live.
    #[inline]
    pub(crate) async fn resolve_existing_user_table(
        &self,
        table_id: TableID,
    ) -> OperationResult<Arc<Table>> {
        if let Some(table) = self.state.cached_user_table(table_id) {
            return Ok(table);
        }
        let table = self
            .engine
            .catalog()
            .get_table(table_id)
            .await
            .ok_or_else(|| {
                Report::new(OperationError::TableNotFound).attach(format!("table_id={table_id}"))
            })?;
        self.state.cache_user_table(&table);
        Ok(table)
    }

    /// Acquires an explicit session-lifetime table lock from this pinned session.
    #[inline]
    pub(crate) async fn lock_table(
        &self,
        table_id: TableID,
        mode: LockMode,
    ) -> OperationResult<()> {
        let session_id = self.id();
        let engine = &self.engine;
        engine.catalog().validate_user_table_live(table_id).await?;
        let lock_manager = engine.lock_manager();
        let owner = LockOwner::Session(session_id);
        let owner_group = LockOwnerGroup::Session(session_id);
        let (mut metadata_guard, mut data_guard) = lock_manager
            .acquire_grouped_table_locks(table_id, mode, owner, owner_group)
            .await?;
        engine.catalog().validate_user_table_live(table_id).await?;
        if let Some(guard) = data_guard.as_mut() {
            guard.disarm();
        }
        if let Some(guard) = metadata_guard.as_mut() {
            guard.disarm();
        }
        Ok(())
    }

    /// Releases an explicit session-lifetime table lock from this pinned session.
    #[inline]
    pub(crate) fn unlock_table(&self, table_id: TableID) -> OperationResult<()> {
        if self
            .in_trx()
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| "phase=inspect_session_lifecycle")?
        {
            return Err(Report::new(OperationError::NotSupported)
                .attach("unlock table while session has an active transaction"));
        }
        let owner = LockOwner::Session(self.id());
        let lock_manager = self.engine.lock_manager();
        lock_manager.release(LockResource::TableData(table_id), owner);
        lock_manager.release(LockResource::TableMetadata(table_id), owner);
        Ok(())
    }
}

/// Engine-owned registry for strong session state.
pub(crate) struct SessionRegistry {
    entries: FastDashMap<SessionID, Arc<SessionState>>,
    trx_changes: ChangeNotifier,
}

impl SessionRegistry {
    /// Create an empty session registry.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            entries: FastDashMap::default(),
            trx_changes: ChangeNotifier::new(),
        }
    }

    /// Insert a new registry-owned session state.
    #[inline]
    pub(crate) fn insert(&self, state: Arc<SessionState>) {
        let old = self.entries.insert(state.id(), state);
        debug_assert!(old.is_none(), "session ids are monotonic and never reused");
    }

    /// Create and insert one registry-owned session state.
    #[inline]
    pub(crate) fn create_session(
        &self,
        engine: &Arc<EngineInner>,
        engine_ref: EngineRef,
        id: SessionID,
    ) -> Session {
        let state = Arc::new(SessionState::new(engine_ref, id));
        self.insert(state);
        Session::new(WeakEngineRef::new(engine), id)
    }

    /// Pin a running session state for one operation.
    #[inline]
    pub(crate) fn pin_running(&self, id: SessionID) -> LifecycleResult<Arc<SessionState>> {
        let state = self
            .session_state(id)
            .ok_or_else(|| closed_session_report(id).attach("reason=session_missing"))?;
        state
            .ensure_running()
            .attach_with(|| format!("session_id={id}"))?;
        Ok(state)
    }

    /// Close an idle session and remove it from the registry.
    #[inline]
    pub(crate) fn close_idle(&self, id: SessionID) -> LifecycleResult<()> {
        let state = self
            .session_state(id)
            .ok_or_else(|| closed_session_report(id).attach("reason=session_missing"))?;
        let removal = state
            .close_idle()
            .attach_with(|| format!("session_id={id}"))?;
        self.apply_removal(id, removal);
        Ok(())
    }

    /// Best-effort nonblocking abandonment from public session `Drop`.
    #[inline]
    pub(crate) fn abandon(&self, id: SessionID) {
        self.modify_session(id, SessionState::abandon);
        self.notify_trx_changed();
    }

    /// Apply session cleanup after a transaction commits.
    #[inline]
    pub(crate) fn finish_trx_commit(&self, id: SessionID, trx_id: TrxID, cts: TrxID) {
        self.modify_session(id, |state| state.finish_trx_commit(trx_id, cts));
        self.notify_trx_changed();
    }

    /// Apply session cleanup after a transaction rolls back.
    #[inline]
    pub(crate) fn finish_trx_rollback(&self, id: SessionID, trx_id: TrxID) {
        self.modify_session(id, |state| state.finish_trx_rollback(trx_id));
        self.notify_trx_changed();
    }

    /// Resolve an active transaction by session and transaction id.
    #[inline]
    pub(crate) fn resolve_trx(
        &self,
        session_id: SessionID,
        trx_id: TrxID,
    ) -> LifecycleResult<(Arc<TrxEntry>, Arc<SessionState>)> {
        let session = self.session_state(session_id).ok_or_else(|| {
            Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                "session_id={session_id}, trx_id={trx_id}, reason=session_missing"
            ))
        })?;
        let entry = session
            .resolve_trx(trx_id)
            .change_context(LifecycleError::TransactionDiscarded)
            .attach_with(|| {
                format!("session_id={session_id}, trx_id={trx_id}, phase=resolve_transaction_entry")
            })?;
        Ok((entry, session))
    }

    /// Mark a public transaction handle abandoned if it still names the active entry.
    #[inline]
    pub(crate) fn abandon_trx_handle(&self, session_id: SessionID, trx_id: TrxID) -> bool {
        let Some(session) = self.session_state(session_id) else {
            return false;
        };
        let abandoned = session.abandon_trx_handle(trx_id);
        if abandoned {
            self.notify_trx_changed();
        }
        abandoned
    }

    /// Queue cleanup for abandoned transactions found during shutdown scanning.
    #[inline]
    pub(crate) fn collect_shutdown_cleanup(&self) -> Vec<(SessionID, TrxID, TrxCleanupReason)> {
        let sessions = self
            .entries
            .iter()
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect::<Vec<_>>();
        let mut cleanup = Vec::new();
        for (session_id, session) in sessions {
            if let Some(trx_id) = session.shutdown_cleanup_candidate() {
                cleanup.push((session_id, trx_id, TrxCleanupReason::ShutdownDrain));
            }
        }
        if !cleanup.is_empty() {
            self.notify_trx_changed();
        }
        cleanup
    }

    /// Count registry-owned transactions that still block owner shutdown.
    #[inline]
    pub(crate) fn active_transaction_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .filter(|session| session.has_active_transaction())
            .count()
    }

    /// Returns the current transaction lifecycle change epoch.
    #[inline]
    pub(crate) fn trx_change_epoch(&self) -> u64 {
        self.trx_changes.epoch()
    }

    /// Wait for any transaction lifecycle change after the observed epoch.
    #[inline]
    pub(crate) fn wait_for_trx_change_since(&self, observed_epoch: u64) {
        self.trx_changes.wait_since(observed_epoch);
    }

    /// Wait asynchronously for a transaction lifecycle change in tests.
    #[cfg(test)]
    #[inline]
    pub(crate) async fn wait_for_trx_change_since_async(&self, observed_epoch: u64) {
        self.trx_changes.wait_since_async(observed_epoch).await;
    }

    /// Remove idle and abandoned-idle sessions during engine shutdown.
    #[inline]
    pub(crate) fn shutdown_idle(&self) {
        let sessions = self
            .entries
            .iter()
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect::<Vec<_>>();
        let removed = sessions
            .into_iter()
            .filter_map(|(id, state)| {
                state
                    .shutdown_removal()
                    .then(|| self.entries.remove(&id).map(|(_, state)| state))
                    .flatten()
            })
            .collect::<Vec<_>>();
        drop(removed);
    }

    #[inline]
    fn session_state(&self, id: SessionID) -> Option<Arc<SessionState>> {
        self.entries.get(&id).map(|entry| Arc::clone(entry.value()))
    }

    #[inline]
    fn modify_session(&self, id: SessionID, modify: impl FnOnce(&SessionState) -> SessionRemoval) {
        let Some(state) = self.session_state(id) else {
            return;
        };
        self.apply_removal(id, modify(&state));
    }

    #[inline]
    fn apply_removal(&self, id: SessionID, removal: SessionRemoval) {
        let removed = match removal {
            SessionRemoval::Remove => self.entries.remove(&id).map(|(_, state)| state),
            SessionRemoval::Keep => None,
        };
        drop(removed);
    }

    /// Notifies waiters about a transaction lifecycle change.
    #[inline]
    pub(crate) fn notify_trx_changed(&self) {
        self.trx_changes.notify();
    }
}

/// Session-local user-table runtime and insert-page state.
struct SessionTableCacheEntry {
    table: Weak<Table>,
    active_insert_page: Option<VersionedPageID>,
}

impl SessionTableCacheEntry {
    #[inline]
    fn new(table: &Arc<Table>) -> Self {
        SessionTableCacheEntry {
            table: Arc::downgrade(table),
            active_insert_page: None,
        }
    }
}

/// Shared mutable state referenced by transactions started from one [`Session`].
pub(crate) struct SessionState {
    id: SessionID,
    pool_guards: PoolGuards,
    lock_manager: QuiescentGuard<LockManager>,
    lifecycle: Mutex<SessionLifecycle>,
    last_cts: AtomicU64,
    table_cache: Mutex<FastHashMap<TableID, SessionTableCacheEntry>>,
}

impl SessionState {
    /// Create a new session state and populate its default pool guards.
    #[inline]
    pub(crate) fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        let pool_guards = engine_ref.pools().pool_guards();
        let lock_manager = engine_ref.lock_manager().clone();
        SessionState {
            id,
            pool_guards,
            lock_manager,
            lifecycle: Mutex::new(SessionLifecycle::RunningIdle),
            last_cts: AtomicU64::new(0),
            table_cache: Mutex::new(FastHashMap::default()),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns the guard bundle owned by this session state.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    #[inline]
    fn ensure_running(&self) -> LifecycleResult<()> {
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningIdle | SessionLifecycle::RunningActive { .. } => Ok(()),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_report(self.id)),
        }
    }

    #[inline]
    fn in_trx(&self) -> LifecycleResult<bool> {
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningIdle => Ok(false),
            SessionLifecycle::RunningActive { entry } => Ok(entry.in_trx()),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_report(self.id)),
        }
    }

    #[inline]
    fn active_transaction_err(&self, entry: &TrxEntry) -> Report<LifecycleError> {
        Report::new(LifecycleError::ExistingTransaction).attach(format!(
            "session_id={}, trx_id={}, state={}",
            self.id,
            entry.trx_id(),
            entry.inspect_state().label()
        ))
    }

    #[inline]
    fn begin_trx(
        &self,
        begin: impl FnOnce() -> StartedTransaction,
    ) -> LifecycleResult<Transaction> {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningIdle => {}
            SessionLifecycle::RunningActive { entry } => {
                return Err(self.active_transaction_err(entry));
            }
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => {
                return Err(closed_session_report(self.id));
            }
        }
        let trx = begin();
        *lifecycle = SessionLifecycle::RunningActive {
            entry: Arc::clone(&trx.entry),
        };
        Ok(trx.handle)
    }

    #[inline]
    fn close_idle(&self) -> LifecycleResult<SessionRemoval> {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {}
            SessionLifecycle::RunningActive { entry } => {
                return Err(self.active_transaction_err(entry));
            }
            SessionLifecycle::AbandonedActive { entry } => {
                return Err(self.active_transaction_err(entry));
            }
            SessionLifecycle::Closed => return Ok(SessionRemoval::Remove),
        }
        *lifecycle = SessionLifecycle::Closed;
        self.release_session_locks();
        Ok(SessionRemoval::Remove)
    }

    #[inline]
    fn abandon(&self) -> SessionRemoval {
        let mut lifecycle = self.lifecycle.lock();
        let entry = match &*lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::AbandonedIdle;
                self.release_session_locks();
                return SessionRemoval::Remove;
            }
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry } => Arc::clone(entry),
            SessionLifecycle::Closed => return SessionRemoval::Remove,
        };
        *lifecycle = SessionLifecycle::AbandonedActive { entry };
        SessionRemoval::Keep
    }

    /// Finish this session's active-transaction lifecycle after commit.
    #[inline]
    fn finish_trx_commit(&self, trx_id: TrxID, cts: TrxID) -> SessionRemoval {
        self.finish_trx(trx_id, || {
            self.last_cts.store(cts.as_u64(), Ordering::SeqCst);
        })
    }

    /// Finish this session's active-transaction lifecycle after rollback.
    #[inline]
    fn finish_trx_rollback(&self, trx_id: TrxID) -> SessionRemoval {
        self.finish_trx(trx_id, || {})
    }

    #[inline]
    fn finish_trx(&self, trx_id: TrxID, mut on_finish: impl FnMut()) -> SessionRemoval {
        let mut lifecycle = self.lifecycle.lock();
        enum Finish {
            Running(Arc<TrxEntry>),
            Abandoned(Arc<TrxEntry>),
            Stale,
        }
        let finish = match &*lifecycle {
            SessionLifecycle::RunningActive { entry } if entry.trx_id() == trx_id => {
                Finish::Running(Arc::clone(entry))
            }
            SessionLifecycle::AbandonedActive { entry } if entry.trx_id() == trx_id => {
                Finish::Abandoned(Arc::clone(entry))
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed
            | SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. } => Finish::Stale,
        };
        match finish {
            Finish::Running(entry) => {
                on_finish();
                entry.finish(TrxEntryState::Terminal);
                *lifecycle = SessionLifecycle::RunningIdle;
                SessionRemoval::Keep
            }
            Finish::Abandoned(entry) => {
                on_finish();
                entry.finish(TrxEntryState::Terminal);
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                SessionRemoval::Remove
            }
            Finish::Stale => SessionRemoval::Keep,
        }
    }

    #[inline]
    fn resolve_trx(&self, trx_id: TrxID) -> InternalResult<Arc<TrxEntry>> {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry }
                if entry.trx_id() == trx_id =>
            {
                Ok(Arc::clone(entry))
            }
            SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => {
                Err(Report::new(InternalError::ActiveTransactionDiscarded)
                    .attach(format!("session_id={}, trx_id={trx_id}", self.id)))
            }
        }
    }

    #[inline]
    fn abandon_trx_handle(&self, trx_id: TrxID) -> bool {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry }
                if entry.trx_id() == trx_id =>
            {
                entry.abandon()
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed
            | SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. } => false,
        }
    }

    #[inline]
    fn shutdown_cleanup_candidate(&self) -> Option<TrxID> {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry } => matches!(
                entry.inspect_state(),
                TrxEntryState::Abandoned | TrxEntryState::CheckedOutAbandoned
            )
            .then(|| entry.trx_id()),
            SessionLifecycle::AbandonedActive { entry } => entry.abandon().then(|| entry.trx_id()),
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => None,
        }
    }

    #[inline]
    fn has_active_transaction(&self) -> bool {
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry } => entry.in_trx(),
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => false,
        }
    }

    #[inline]
    fn shutdown_removal(&self) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                true
            }
            SessionLifecycle::Closed => true,
            SessionLifecycle::RunningActive { .. } | SessionLifecycle::AbandonedActive { .. } => {
                false
            }
        }
    }

    /// Upgrade a cached user-table runtime if the session weak cache still reaches it.
    #[inline]
    pub(crate) fn cached_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        let mut cache = self.table_cache.lock();
        let entry = cache.get(&table_id)?;
        match entry.table.upgrade() {
            Some(table) => Some(table),
            None => {
                cache.remove(&table_id);
                None
            }
        }
    }

    /// Remember a successfully resolved user-table runtime without extending its lifetime.
    #[inline]
    pub(crate) fn cache_user_table(&self, table: &Arc<Table>) {
        let mut cache = self.table_cache.lock();
        match cache.get_mut(&table.table_id()) {
            Some(entry) => entry.table = Arc::downgrade(table),
            None => {
                cache.insert(table.table_id(), SessionTableCacheEntry::new(table));
            }
        }
    }

    /// Remove and return the cached insert page for a table, if present.
    #[inline]
    pub fn load_active_insert_page(&self, table_id: TableID) -> Option<VersionedPageID> {
        self.table_cache
            .lock()
            .get_mut(&table_id)
            .and_then(|entry| entry.active_insert_page.take())
    }

    /// Cache the active insert page for a table.
    #[inline]
    pub fn save_active_insert_page(&self, table_id: TableID, page_id: VersionedPageID) {
        let mut cache = self.table_cache.lock();
        let entry = cache.get_mut(&table_id);
        assert!(
            entry.is_some(),
            "active insert page requires a cached user-table runtime: table_id={table_id}"
        );
        if let Some(entry) = entry {
            let previous = entry.active_insert_page.replace(page_id);
            assert!(
                previous.is_none(),
                "active insert page token already cached: table_id={table_id}"
            );
        }
    }

    #[inline]
    fn release_session_locks(&self) {
        self.lock_manager.release_owner(LockOwner::Session(self.id));
    }
}

impl Drop for SessionState {
    #[inline]
    fn drop(&mut self) {
        let table_cache = self.table_cache.get_mut();
        for (_, entry) in table_cache.drain() {
            if let (Some(page_id), Some(table)) = (entry.active_insert_page, entry.table.upgrade())
            {
                table.mem.cache_insert_page_version(page_id);
            }
        }
        self.release_session_locks();
    }
}

enum SessionLifecycle {
    RunningIdle,
    RunningActive { entry: Arc<TrxEntry> },
    AbandonedIdle,
    AbandonedActive { entry: Arc<TrxEntry> },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionRemoval {
    Keep,
    Remove,
}

/// Private transaction runtime attachment retained by checked-out transaction work.
///
/// This handle owns engine runtime liveness and session-state reachability for
/// one operation, terminal path, prepared commit handoff, or cleanup path. The
/// public transaction facade never stores it.
pub(crate) struct TrxAttachment {
    /// Strong runtime handle kept until the transaction reaches a terminal path.
    engine: EngineRef,
    /// Registry lookup key for terminal session cleanup.
    session_id: SessionID,
    /// Active transaction id used to avoid finishing a replaced session state.
    trx_id: TrxID,
    /// Claim-local reachability for session-local caches.
    session: Arc<SessionState>,
    /// Cached guards needed by transaction work even if session state is gone.
    pool_guards: PoolGuards,
}

impl TrxAttachment {
    /// Create a transaction runtime attachment without public handle ownership.
    #[inline]
    pub(crate) fn new(engine: EngineRef, session: Arc<SessionState>, trx_id: TrxID) -> Self {
        let pool_guards = session.pool_guards().clone();
        Self {
            engine,
            session_id: session.id(),
            trx_id,
            session,
            pool_guards,
        }
    }

    /// Returns the crate-private engine runtime handle.
    #[inline]
    pub(crate) fn engine(&self) -> &EngineRef {
        &self.engine
    }

    /// Returns the cloned session pool guards retained by this transaction.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    /// Upgrade a cached session-local user-table runtime if it is still alive.
    #[inline]
    pub(crate) fn cached_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.session.cached_user_table(table_id)
    }

    /// Store a weak session-local table cache entry after successful resolution.
    #[inline]
    pub(crate) fn cache_user_table(&self, table: &Arc<Table>) {
        self.session.cache_user_table(table);
    }

    /// Remove and return the cached insert page for a table, if session state remains.
    #[inline]
    pub(crate) fn load_active_insert_page(&self, table_id: TableID) -> Option<VersionedPageID> {
        self.session.load_active_insert_page(table_id)
    }

    /// Cache the active insert page if session state remains.
    #[inline]
    pub(crate) fn save_active_insert_page(&self, table_id: TableID, page_id: VersionedPageID) {
        self.session.save_active_insert_page(table_id, page_id);
    }

    /// Mark the owning session committed.
    #[inline]
    pub(crate) fn commit(&self, cts: TrxID) {
        self.engine
            .session_registry
            .finish_trx_commit(self.session_id, self.trx_id, cts);
    }

    /// Mark the owning session rolled back.
    #[inline]
    pub(crate) fn rollback(&self) {
        self.engine
            .session_registry
            .finish_trx_rollback(self.session_id, self.trx_id);
    }

    /// Queue rollback cleanup for an abandoned transaction.
    #[inline]
    pub(crate) fn request_abandoned_cleanup(&self, reason: TrxCleanupReason) {
        self.engine.trx_sys.request_abandoned_trx_cleanup(
            self.engine.clone(),
            self.session_id,
            self.trx_id,
            reason,
        );
    }
}

#[inline]
fn ensure_idle_maintenance_session(session: &SessionPin) -> OperationResult<()> {
    if session
        .in_trx()
        .change_context(OperationError::NotSupported)
        .attach_with(|| "phase=inspect_session_lifecycle")?
    {
        return Err(
            Report::new(OperationError::NotSupported).attach("maintenance requires idle session")
        );
    }
    Ok(())
}

async fn wait_for_maintenance_boundary(
    session: &SessionPin,
    ts: TrxID,
    boundary: MaintenanceBoundary,
) -> Result<TrxID> {
    let trx_sys = &session.engine.trx_sys;
    loop {
        session.engine.poisoner.ensure_healthy()?;
        if session.engine.shutdown_started() {
            return Err(maintenance_boundary_shutdown_err(boundary, ts));
        }
        let observed = boundary.observed(session);
        if observed > ts {
            return Ok(observed);
        }

        trx_sys.request_purge_observation();
        let progress_listener = boundary.listener(session);
        let poison_listener = session.engine.poisoner.listener();
        let shutdown_listener = session.engine.shutdown_listener();

        session.engine.poisoner.ensure_healthy()?;
        if session.engine.shutdown_started() {
            return Err(maintenance_boundary_shutdown_err(boundary, ts));
        }
        let observed = boundary.observed(session);
        if observed > ts {
            return Ok(observed);
        }
        select_all(vec![progress_listener, poison_listener, shutdown_listener]).await;
    }
}

#[inline]
fn maintenance_boundary_shutdown_err(boundary: MaintenanceBoundary, ts: TrxID) -> Error {
    Report::new(LifecycleError::Shutdown)
        .attach(format!(
            "maintenance progress wait observed engine shutdown: boundary={}, target_ts={ts}",
            boundary.name()
        ))
        .into()
}

#[cfg(test)]
async fn wait_for_purge_handoff(session: &SessionPin, ts: TrxID) -> Result<()> {
    let trx_sys = &session.engine.trx_sys;
    loop {
        session.engine.poisoner.ensure_healthy()?;
        if session.engine.shutdown_started() {
            return Err(Report::new(LifecycleError::Shutdown)
                .attach("completed-purge wait observed engine shutdown before ordered handoff")
                .into());
        }
        if trx_sys.purge_handoff_cts() >= ts {
            return Ok(());
        }
        let handoff_listener = trx_sys.purge_handoff_listener();
        let poison_listener = session.engine.poisoner.listener();
        let shutdown_listener = session.engine.shutdown_listener();
        session.engine.poisoner.ensure_healthy()?;
        if session.engine.shutdown_started() {
            return Err(Report::new(LifecycleError::Shutdown)
                .attach("completed-purge wait observed engine shutdown before ordered handoff")
                .into());
        }
        if trx_sys.purge_handoff_cts() >= ts {
            return Ok(());
        }
        select_all(vec![handoff_listener, poison_listener, shutdown_listener]).await;
    }
}

#[inline]
fn closed_session_report(id: SessionID) -> Report<LifecycleError> {
    Report::new(LifecycleError::SessionUnavailable).attach(format!("session_id={id}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::guard::PageGuard;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::tests::{table1, table2};
    use crate::catalog::{
        CatalogTable, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, is_catalog_table,
    };
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, Error, ErrorKind, FatalError, LifecycleError, OperationError,
        RuntimeError,
    };
    use crate::io::install_storage_backend_test_hook;
    use crate::log::LogSync;
    use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
    use crate::stats::{BufferPoolCounters, BufferPoolRuntimeStats, TransactionSystemStats};
    use crate::table::tests::{FailingFirstWriteHook, assert_freeze_created};
    use crate::trx::retention::{
        RedoTruncationBlocker, tests::install_redo_cleanup_before_unlink_hook,
    };
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, TrxInner};
    use crate::value::{Val, ValKind};
    use futures::task::noop_waker;
    use std::fs;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    const TRUNCATE_TEST_LOG_BLOCK_SIZE: usize = 4096;
    const TRUNCATE_TEST_LOG_FILE_MAX_SIZE: usize =
        REDO_DEFAULT_DATA_START_OFFSET + 4 * TRUNCATE_TEST_LOG_BLOCK_SIZE;

    #[inline]
    fn assert_runtime_unavailable_after_shutdown(err: Error) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );
    }

    #[inline]
    fn assert_runtime_unavailable_after_fatal(err: Error, fatal: FatalError) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::RuntimeUnavailable)
        );
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(fatal)
        );
    }

    #[inline]
    fn assert_transaction_system_stats_monotonic(
        before: TransactionSystemStats,
        after: TransactionSystemStats,
    ) {
        assert!(after.commit_count >= before.commit_count);
        assert!(after.trx_count >= before.trx_count);
        assert!(after.log_bytes >= before.log_bytes);
        assert!(after.sync_count >= before.sync_count);
        assert!(after.sync_nanos >= before.sync_nanos);
        assert!(after.seal_failure_count >= before.seal_failure_count);
        assert!(after.io_submit_and_wait_count >= before.io_submit_and_wait_count);
        assert!(after.io_submit_and_wait_nanos >= before.io_submit_and_wait_nanos);
        assert!(after.purge_trx_count >= before.purge_trx_count);
        assert!(after.purge_row_count >= before.purge_row_count);
        assert!(after.purge_index_count >= before.purge_index_count);
    }

    #[inline]
    fn assert_buffer_pool_stats_monotonic(before: &BufferPoolStats, after: &BufferPoolStats) {
        assert_buffer_pool_runtime_stats_monotonic(before.meta, after.meta);
        assert_buffer_pool_runtime_stats_monotonic(before.mem, after.mem);
        assert_buffer_pool_runtime_stats_monotonic(before.index, after.index);
        assert_buffer_pool_runtime_stats_monotonic(before.disk, after.disk);
    }

    #[inline]
    fn assert_buffer_pool_runtime_stats_monotonic(
        before: BufferPoolRuntimeStats,
        after: BufferPoolRuntimeStats,
    ) {
        assert_eq!(after.capacity, before.capacity);
        assert!(after.allocated >= before.allocated);
        assert_buffer_pool_counters_monotonic(before.counters, after.counters);
    }

    #[inline]
    fn assert_buffer_pool_counters_monotonic(
        before: BufferPoolCounters,
        after: BufferPoolCounters,
    ) {
        assert!(after.cache_hits >= before.cache_hits);
        assert!(after.cache_misses >= before.cache_misses);
        assert!(after.miss_joins >= before.miss_joins);
        assert!(after.queued_reads >= before.queued_reads);
        assert!(after.running_reads >= before.running_reads);
        assert!(after.completed_reads >= before.completed_reads);
        assert!(after.read_errors >= before.read_errors);
        assert!(after.queued_writes >= before.queued_writes);
        assert!(after.running_writes >= before.running_writes);
        assert!(after.completed_writes >= before.completed_writes);
        assert!(after.write_errors >= before.write_errors);
    }

    fn redo_truncation_engine_config(main_dir: &Path, log_file_stem: &str) -> EngineConfig {
        EngineConfig::default().storage_root(main_dir).trx(
            TrxSysConfig::default()
                .log_file_stem(log_file_stem)
                .log_write_io_depth(1)
                .recovery_io_depth(1)
                .catalog_checkpoint_scan_io_depth(1)
                .log_block_size(TRUNCATE_TEST_LOG_BLOCK_SIZE)
                .log_file_max_size(TRUNCATE_TEST_LOG_FILE_MAX_SIZE)
                .log_sync(LogSync::None)
                .purge_threads(1),
        )
    }

    fn redo_file_path(main_dir: &Path, log_file_stem: &str, file_seq: u32) -> PathBuf {
        main_dir.join(format!("{log_file_stem}.{file_seq:08x}"))
    }

    async fn create_rotated_redo_table(
        engine: &Engine,
        main_dir: &Path,
        log_file_stem: &str,
        target_file_seq: u32,
    ) -> TableID {
        let table_id = table2(engine).await;
        let mut session = engine.new_session().unwrap();
        let payload = [7u8; 196];
        for value in 0..256 {
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(value), Val::from(&payload[..])])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
            if redo_file_path(main_dir, log_file_stem, target_file_seq).exists() {
                return table_id;
            }
        }
        panic!("test setup did not create redo file {target_file_seq:08x}");
    }

    pub(crate) async fn assert_checkpoint_published(
        session: &mut Session,
        table_id: TableID,
    ) -> TrxID {
        let outcome = session.checkpoint_table_with_wait(table_id).await.unwrap();
        let CheckpointOutcome::Published { checkpoint_ts, .. } = outcome else {
            panic!("checkpoint should publish, got {outcome:?}");
        };
        checkpoint_ts
    }

    pub(crate) async fn wait_for_checkpoint_purge(session: &Session, redo_cts: TrxID) -> TrxID {
        session
            .wait_for_purge_handoff_for_test(redo_cts)
            .await
            .unwrap();
        session
            .wait_for_purge_completion_after(redo_cts)
            .await
            .unwrap()
    }

    pub(crate) async fn wait_for_checkpoint_root_ready(session: &Session, table_id: TableID) {
        let table = session
            .engine()
            .catalog()
            .get_table_now(table_id)
            .expect("test table should exist");
        let effective_ts = table.file().active_root_unchecked().effective_ts();
        let min_active_sts = session.engine().trx_sys.calc_min_active_sts_for_gc();
        if effective_ts < min_active_sts {
            return;
        }
        session
            .wait_for_checkpoint_retry(CheckpointDelayReason::ActiveRoot {
                table_id,
                effective_ts,
                min_active_sts,
            })
            .await
            .unwrap();
    }

    async fn commit_redo_durability_anchor(session: &mut Session, table_id: TableID) {
        let payload = [9u8; 196];
        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(
                table_id,
                vec![Val::from(10_000i32), Val::from(&payload[..])],
            )
            .await?;
            Ok(())
        })
        .await
        .unwrap();
        trx.commit().await.unwrap();
    }

    async fn create_cache_test_table(session: &mut Session) -> TableID {
        session
            .create_table(
                TableSpec::new(vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )]),
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
            )
            .await
            .unwrap()
    }

    async fn catalog_row_page_count(table: &CatalogTable, guards: &PoolGuards) -> usize {
        let mut count = 0usize;
        let pivot_row_id = table.pivot_row_id();
        let mut cursor = table.blk_idx().mem_cursor(guards.meta_guard());
        cursor.seek(pivot_row_id).await.unwrap();
        while let Some(leaf) = cursor.next().await.unwrap() {
            let guard = leaf.lock_shared_async().await.unwrap();
            count += guard
                .page()
                .leaf_entries()
                .iter()
                .filter(|entry| entry.row_id >= pivot_row_id)
                .count();
        }
        count
    }

    /// Returns the number of registry-owned sessions for tests.
    #[inline]
    pub(crate) fn session_registry_len(registry: &SessionRegistry) -> usize {
        registry.entries.len()
    }

    /// Create one registry-owned transaction with test-controlled ids.
    #[inline]
    pub(crate) fn create_test_transaction(
        registry: &SessionRegistry,
        engine: EngineRef,
        session_id: SessionID,
        trx_id: TrxID,
        sts: TrxID,
        gc_no: usize,
    ) -> (Transaction, Arc<SessionState>) {
        let state = Arc::new(SessionState::new(engine.clone(), session_id));
        let inner = TrxInner::new(trx_id, sts, gc_no, session_id);
        let entry = TrxEntry::new(inner);
        {
            let mut lifecycle = state.lifecycle.lock();
            *lifecycle = SessionLifecycle::RunningActive { entry };
        }
        registry.insert(Arc::clone(&state));
        (
            Transaction::new(engine.downgrade(), session_id, trx_id, sts),
            state,
        )
    }

    pub(crate) trait SessionTestExt {
        fn in_trx(&self) -> Result<bool>;
        fn pool_guards(&self) -> PoolGuards;
        fn engine(&self) -> EngineRef;
        fn last_cts(&self) -> TrxID;
        fn load_active_insert_page(&mut self, table_id: TableID) -> Option<VersionedPageID>;
        fn save_active_insert_page(&mut self, table_id: TableID, page_id: VersionedPageID);
    }

    impl SessionTestExt for Session {
        #[inline]
        fn in_trx(&self) -> Result<bool> {
            const OPERATION: &str = "test_query_session_transaction_state";
            let session = self
                .query_session()
                .attach_with(|| format!("operation={OPERATION}"))?;
            Ok(session
                ._state
                .in_trx()
                .attach_with(|| format!("operation={OPERATION}, session_id={}", self.id))?)
        }

        #[inline]
        fn pool_guards(&self) -> PoolGuards {
            self.pin()
                .expect("test session must be running")
                .state
                .pool_guards()
                .clone()
        }

        #[inline]
        fn engine(&self) -> EngineRef {
            self.pin().expect("test session must be running").engine
        }

        #[inline]
        fn last_cts(&self) -> TrxID {
            let session = self.pin().expect("test session must be running");
            TrxID::new(session.state.last_cts.load(Ordering::SeqCst))
        }

        #[inline]
        fn load_active_insert_page(&mut self, table_id: TableID) -> Option<VersionedPageID> {
            self.pin()
                .expect("test session must be running")
                .state
                .load_active_insert_page(table_id)
        }

        #[inline]
        fn save_active_insert_page(&mut self, table_id: TableID, page_id: VersionedPageID) {
            self.pin()
                .expect("test session must be running")
                .state
                .save_active_insert_page(table_id, page_id);
        }
    }

    #[test]
    fn test_session_table_cache_owns_active_user_insert_page() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = create_cache_test_table(&mut session).await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(1i32)])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let pin = session.pin().unwrap();
            let cached_page = {
                let cache = pin.state.table_cache.lock();
                let entry = cache.get(&table_id).unwrap();
                assert!(entry.table.upgrade().is_some());
                entry.active_insert_page.unwrap()
            };
            assert_eq!(
                pin.state.load_active_insert_page(table_id),
                Some(cached_page)
            );
            assert!(pin.state.cached_user_table(table_id).is_some());
            assert!(
                pin.state
                    .table_cache
                    .lock()
                    .get(&table_id)
                    .is_some_and(|entry| entry.active_insert_page.is_none())
            );
            pin.state.save_active_insert_page(table_id, cached_page);
            assert_eq!(
                pin.state
                    .table_cache
                    .lock()
                    .get(&table_id)
                    .and_then(|entry| entry.active_insert_page),
                Some(cached_page)
            );
        });
    }

    #[test]
    fn test_catalog_insert_pages_use_shared_free_list() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let catalog_tables = engine
                .catalog()
                .storage
                .get_catalog_table(TABLE_ID_TABLES)
                .unwrap();

            let mut session1 = engine.new_session().unwrap();
            create_cache_test_table(&mut session1).await;
            let guards1 = session1.pool_guards();
            let row_page_count = catalog_row_page_count(&catalog_tables, &guards1).await;
            assert!(row_page_count > 0);
            {
                let pin = session1.pin().unwrap();
                assert!(
                    pin.state
                        .table_cache
                        .lock()
                        .keys()
                        .all(|table_id| !is_catalog_table(*table_id))
                );
            }

            let mut session2 = engine.new_session().unwrap();
            create_cache_test_table(&mut session2).await;
            let guards2 = session2.pool_guards();
            assert_eq!(
                catalog_row_page_count(&catalog_tables, &guards2).await,
                row_page_count
            );
            let pin = session2.pin().unwrap();
            assert!(
                pin.state
                    .table_cache
                    .lock()
                    .keys()
                    .all(|table_id| !is_catalog_table(*table_id))
            );
        });
    }

    #[test]
    fn test_shutdown_cleanup_candidate_claims_abandoned_active() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = SessionState::new(engine.new_ref().unwrap(), session_id);
            let entry = TrxEntry::new(TrxInner::new(trx_id, MIN_SNAPSHOT_TS, 0, session_id));
            {
                let mut lifecycle = state.lifecycle.lock();
                *lifecycle = SessionLifecycle::AbandonedActive {
                    entry: Arc::clone(&entry),
                };
            }

            assert_eq!(state.shutdown_cleanup_candidate(), Some(trx_id));
            assert_eq!(entry.inspect_state(), TrxEntryState::Abandoned);
        });
    }

    #[test]
    fn test_shutdown_cleanup_candidate_skips_unclaimable_abandoned_active() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = SessionState::new(engine.new_ref().unwrap(), session_id);
            let entry = TrxEntry::new(TrxInner::new(trx_id, MIN_SNAPSHOT_TS, 0, session_id));
            entry.finish(TrxEntryState::Failed);
            {
                let mut lifecycle = state.lifecycle.lock();
                *lifecycle = SessionLifecycle::AbandonedActive {
                    entry: Arc::clone(&entry),
                };
            }

            assert_eq!(state.shutdown_cleanup_candidate(), None);
            assert_eq!(entry.inspect_state(), TrxEntryState::Failed);
        });
    }

    #[test]
    fn test_wait_for_trx_change_since_returns_after_prior_change() {
        let registry = SessionRegistry::new();
        let observed_epoch = registry.trx_change_epoch();

        registry.notify_trx_changed();
        registry.wait_for_trx_change_since(observed_epoch);

        assert_ne!(registry.trx_change_epoch(), observed_epoch);
    }

    #[test]
    fn test_wait_for_trx_change_since_wakes_on_change() {
        let registry = Arc::new(SessionRegistry::new());
        let observed_epoch = registry.trx_change_epoch();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let waiter = {
            let registry = Arc::clone(&registry);
            thread::spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                registry.wait_for_trx_change_since(observed_epoch);
                done_tx.send(()).expect("waiter should report completion");
            })
        };

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should start");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "waiter should block before a transaction change"
        );

        registry.notify_trx_changed();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should wake after a transaction change");
        waiter.join().expect("waiter thread should finish");
    }

    #[test]
    fn test_session_list_table_ids_empty_and_sorted() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            assert_eq!(session.list_table_ids().unwrap(), Vec::<TableID>::new());

            let table_id1 = table2(&engine).await;
            let table_id2 = table1(&engine).await;

            assert_eq!(
                session.list_table_ids().unwrap(),
                vec![table_id1, table_id2]
            );
        });
    }

    #[test]
    fn test_session_checkpoint_catalog_requires_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.checkpoint_catalog().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
            );

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_maintenance_progress_waits_and_idle_requirement() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            table2(&engine).await;
            let session = engine.new_session().unwrap();
            let target = engine.inner().trx_sys.purge_handoff_cts();

            let horizon = session.wait_for_gc_horizon_after(target).await.unwrap();
            assert!(horizon > target);
            let completed = session
                .wait_for_purge_completion_after(target)
                .await
                .unwrap();
            assert!(completed > target);

            let mut active_session = engine.new_session().unwrap();
            let trx = active_session.begin_trx().unwrap();
            let horizon_err = active_session
                .wait_for_gc_horizon_after(target)
                .await
                .unwrap_err();
            assert_eq!(
                horizon_err
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::NotSupported)
            );
            let retry_err = active_session
                .wait_for_checkpoint_retry(CheckpointDelayReason::ActiveRoot {
                    table_id: TableID::new(1),
                    effective_ts: target,
                    min_active_sts: target,
                })
                .await
                .unwrap_err();
            assert_eq!(
                retry_err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_checkpoint_catalog_persists_catalog_state() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(&main_dir)
                .build()
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();

            assert!(
                engine
                    .inner()
                    .trx_sys
                    .catalog_redo_retention_progress()
                    .is_none()
            );
            session.checkpoint_catalog().await.unwrap();
            let progress = engine
                .inner()
                .trx_sys
                .catalog_redo_retention_progress()
                .expect("catalog checkpoint publish should refresh retention progress");
            assert!(progress.catalog_replay_start_ts > MIN_SNAPSHOT_TS);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(&main_dir)
                .build()
                .await
                .unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_some());
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .catalog_redo_retention_progress()
                    .is_none()
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_requires_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.truncate_redo_log().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
            );

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_combined_catalog_redo_maintenance_requires_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
            );

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_combined_catalog_redo_maintenance_publishes_checkpoint_and_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_checkpoint_marker";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let before = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(before.meta.first_redo_log_seq, 0);

            let outcome = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            let CatalogCheckpointOutcome::Published {
                catalog_replay_start_ts,
            } = outcome.catalog_checkpoint
            else {
                panic!(
                    "combined call should publish catalog checkpoint: {:?}",
                    outcome.catalog_checkpoint
                );
            };
            assert!(catalog_replay_start_ts > before.catalog_replay_start_ts);
            assert_eq!(outcome.redo_truncation.previous_first_retained_file_seq, 0);
            assert!(
                outcome.redo_truncation.new_first_retained_file_seq > 0,
                "{outcome:?}"
            );
            assert!(
                outcome.redo_truncation.removed_files >= outcome.redo_truncation.advanced_files,
                "{outcome:?}"
            );
            assert_eq!(outcome.redo_truncation.failed_unlink_files, 0);

            let after = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(after.catalog_replay_start_ts, catalog_replay_start_ts);
            assert_eq!(
                after.meta.first_redo_log_seq,
                outcome.redo_truncation.new_first_retained_file_seq
            );
            for file_seq in 0..outcome.redo_truncation.new_first_retained_file_seq {
                assert!(
                    !redo_file_path(&main_dir, log_file_stem, file_seq).exists(),
                    "obsolete redo file {file_seq:08x} should be removed"
                );
            }
            drop(session);
            drop(engine);

            let restarted = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            assert_eq!(
                restarted.new_session().unwrap().list_table_ids().unwrap(),
                vec![table_id]
            );
        });
    }

    #[test]
    fn test_session_combined_catalog_redo_maintenance_marker_only_after_checkpoint() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_marker_only";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let checkpointed = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let outcome = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            assert_eq!(outcome.catalog_checkpoint, CatalogCheckpointOutcome::Noop);
            assert_eq!(outcome.redo_truncation.previous_first_retained_file_seq, 0);
            assert!(
                outcome.redo_truncation.new_first_retained_file_seq > 0,
                "{outcome:?}"
            );
            let after = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(
                after.catalog_replay_start_ts,
                checkpointed.catalog_replay_start_ts
            );
            assert_eq!(
                after.meta.first_redo_log_seq,
                outcome.redo_truncation.new_first_retained_file_seq
            );
        });
    }

    #[test]
    fn test_session_combined_uses_projected_silent_watermark_for_truncation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_silent_watermark";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let root_floor = table.redo_replay_floor_snapshot();
            drop(table);
            let mut session = engine.new_session().unwrap();
            session.checkpoint_catalog().await.unwrap();

            let checkpoint = session.checkpoint_table(table_id).await.unwrap();
            assert!(
                matches!(
                    checkpoint,
                    CheckpointOutcome::Published { silent: true, .. }
                ),
                "{checkpoint:?}"
            );
            let watermark = engine
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .expect("silent checkpoint should write a catalog row");
            assert!(watermark.heap_redo_start_ts > root_floor.heap_redo_start_ts);
            assert!(watermark.deletion_cutoff_ts > root_floor.deletion_cutoff_ts);
            assert!(
                engine
                    .catalog()
                    .storage
                    .checkpointed_silent_watermarks()
                    .get(&table_id)
                    .is_none()
            );

            let durable_plan = engine.inner().trx_sys.plan_redo_truncation().unwrap();
            assert!(
                durable_plan.blockers.iter().any(|blocker| matches!(
                    blocker,
                    RedoTruncationBlocker::LiveTableFloor {
                        table_id: blocked_table_id,
                        heap_redo_start_ts,
                        deletion_cutoff_ts,
                    } if *blocked_table_id == table_id
                        && *heap_redo_start_ts == root_floor.heap_redo_start_ts
                        && *deletion_cutoff_ts == root_floor.deletion_cutoff_ts
                )),
                "{durable_plan:?}"
            );

            commit_redo_durability_anchor(&mut session, table_id).await;
            let outcome = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            assert!(matches!(
                outcome.catalog_checkpoint,
                CatalogCheckpointOutcome::Published { .. }
            ));
            assert!(
                outcome
                    .redo_truncation
                    .blockers
                    .iter()
                    .any(|blocker| matches!(
                        blocker,
                        RedoTruncationBlockerInfo::LiveTableFloor {
                            table_id: blocked_table_id,
                            heap_redo_start_ts,
                            deletion_cutoff_ts,
                        } if *blocked_table_id == table_id
                            && *heap_redo_start_ts == watermark.heap_redo_start_ts
                            && *deletion_cutoff_ts == watermark.deletion_cutoff_ts
                    )),
                "{outcome:?}"
            );
            let checkpointed = engine.catalog().storage.checkpointed_silent_watermarks();
            let checkpointed_floor = checkpointed
                .get(&table_id)
                .copied()
                .expect("combined checkpoint should install silent watermark cache");
            assert_eq!(
                checkpointed_floor.heap_redo_start_ts,
                watermark.heap_redo_start_ts
            );
            assert_eq!(
                checkpointed_floor.deletion_cutoff_ts,
                watermark.deletion_cutoff_ts
            );
        });
    }

    #[test]
    fn test_session_combined_checkpoint_publish_failure_does_not_unlink() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_checkpoint_fail";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            session.checkpoint_catalog().await.unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let before = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(before.meta.first_redo_log_seq, 0);
            let plan = engine.inner().trx_sys.plan_redo_truncation().unwrap();
            assert_eq!(plan.first_retained_file_seq, 0);
            assert!(!plan.candidates.is_empty(), "{plan:?}");
            let candidate_paths = plan
                .candidates
                .iter()
                .map(|candidate| redo_file_path(&main_dir, log_file_stem, candidate.file_seq))
                .collect::<Vec<_>>();
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "test setup candidate redo file should exist before checkpoint failure: {}",
                    path.display()
                );
            }

            let catalog_path = engine.inner().table_fs.catalog_mtb_file_path();
            let publish_hook = Arc::new(FailingFirstWriteHook::new(catalog_path));
            let _publish_hook_guard = install_storage_backend_test_hook(publish_hook.clone());
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(Arc::new(
                |file_seq, path| {
                    panic!(
                        "redo cleanup must not run after combined checkpoint failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                },
            ));

            let err = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            assert!(publish_hook.call_count() > 0);
            let after = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(
                after.catalog_replay_start_ts,
                before.catalog_replay_start_ts
            );
            assert_eq!(after.meta.first_redo_log_seq, 0);
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "candidate redo file should remain after checkpoint failure: {}",
                    path.display()
                );
            }
        });
    }

    #[test]
    fn test_session_combined_marker_only_publish_failure_does_not_unlink() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_marker_fail";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let before = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(before.meta.first_redo_log_seq, 0);
            let plan = engine.inner().trx_sys.plan_redo_truncation().unwrap();
            assert_eq!(plan.first_retained_file_seq, 0);
            assert!(!plan.candidates.is_empty(), "{plan:?}");
            let candidate_paths = plan
                .candidates
                .iter()
                .map(|candidate| redo_file_path(&main_dir, log_file_stem, candidate.file_seq))
                .collect::<Vec<_>>();
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "test setup candidate redo file should exist before marker failure: {}",
                    path.display()
                );
            }

            let catalog_path = engine.inner().table_fs.catalog_mtb_file_path();
            let publish_hook = Arc::new(FailingFirstWriteHook::new(catalog_path));
            let _publish_hook_guard = install_storage_backend_test_hook(publish_hook.clone());
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(Arc::new(
                |file_seq, path| {
                    panic!(
                        "redo cleanup must not run after combined marker failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                },
            ));

            let err = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            assert!(publish_hook.call_count() > 0);
            let after = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert_eq!(
                after.catalog_replay_start_ts,
                before.catalog_replay_start_ts
            );
            assert_eq!(after.meta.first_redo_log_seq, 0);
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "candidate redo file should remain after marker failure: {}",
                    path.display()
                );
            }
        });
    }

    #[test]
    fn test_session_combined_releases_catalog_gate_before_cleanup() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_cleanup_gate";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut setup_session = engine.new_session().unwrap();
            setup_session.checkpoint_catalog().await.unwrap();
            engine
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let hook_called = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_called);
            let hook_engine = engine.new_ref().unwrap();
            let hook_guard =
                install_redo_cleanup_before_unlink_hook(Arc::new(move |file_seq, _path| {
                    if file_seq != 0 {
                        return;
                    }
                    hook_flag.store(true, Ordering::SeqCst);
                    let catalog = hook_engine.catalog();
                    let mut metadata_fut = Box::pin(catalog.begin_metadata_change());
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    let metadata_lease = match metadata_fut.as_mut().poll(&mut cx) {
                        Poll::Ready(lease) => lease,
                        Poll::Pending => {
                            panic!("catalog gate should be released before redo cleanup")
                        }
                    };
                    drop(metadata_lease);
                }));

            let mut session = engine.new_session().unwrap();
            let outcome = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            assert!(hook_called.load(Ordering::SeqCst));
            assert_eq!(outcome.catalog_checkpoint, CatalogCheckpointOutcome::Noop);
            assert_eq!(outcome.redo_truncation.previous_first_retained_file_seq, 1);
            assert_eq!(outcome.redo_truncation.new_first_retained_file_seq, 1);
            assert_eq!(outcome.redo_truncation.advanced_files, 0);
            assert_eq!(outcome.redo_truncation.removed_files, 1);
            assert_eq!(outcome.redo_truncation.failed_unlink_files, 0);
            assert!(!obsolete_path.exists());
            drop(hook_guard);
        });
    }

    #[test]
    fn test_session_combined_rechecks_poison_after_gate_wait() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_poison_wait";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut setup_session = engine.new_session().unwrap();
            setup_session.checkpoint_catalog().await.unwrap();
            engine
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let redo_retention_lease = engine.inner().trx_sys.begin_redo_retention().await;
            let mut session = engine.new_session().unwrap();
            let mut maintenance_fut = Box::pin(session.checkpoint_catalog_and_truncate_redo_log());

            assert!(matches!(
                futures::poll!(maintenance_fut.as_mut()),
                std::task::Poll::Pending
            ));

            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));
            drop(redo_retention_lease);

            let err = maintenance_fut.await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite)
            );
            assert!(
                obsolete_path.exists(),
                "obsolete redo file should not be removed after poison"
            );
            assert_eq!(
                engine
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
                    .unwrap()
                    .meta
                    .first_redo_log_seq,
                1
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_no_candidates_reports_unsealed_blocker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            let outcome = session.truncate_redo_log().await.unwrap();

            assert_eq!(outcome.previous_first_retained_file_seq, 0);
            assert_eq!(outcome.new_first_retained_file_seq, 0);
            assert_eq!(outcome.advanced_files, 0);
            assert_eq!(outcome.removed_files, 0);
            assert_eq!(outcome.already_missing_files, 0);
            assert_eq!(outcome.failed_unlink_files, 0);
            assert_eq!(
                outcome.blockers,
                vec![RedoTruncationBlockerInfo::UnsealedFile { file_seq: 0 }]
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_reports_catalog_retained_dropped_floor() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_dropped_floor";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut session = engine.new_session().unwrap();
            session.checkpoint_catalog().await.unwrap();
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let expected_floor = table.redo_replay_floor_snapshot();
            drop(table);

            session.drop_table(table_id).await.unwrap();

            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert_eq!(session.list_table_ids().unwrap(), Vec::<TableID>::new());
            let plan = engine.inner().trx_sys.plan_redo_truncation().unwrap();
            assert!(
                plan.blockers.iter().any(|blocker| matches!(
                    blocker,
                    RedoTruncationBlocker::PendingDroppedTableFloor {
                        table_id: blocked_table_id,
                        heap_redo_start_ts,
                        deletion_cutoff_ts,
                        ..
                    } if *blocked_table_id == table_id
                        && *heap_redo_start_ts == expected_floor.heap_redo_start_ts
                        && *deletion_cutoff_ts == expected_floor.deletion_cutoff_ts
                )),
                "{plan:?}"
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_waits_for_catalog_metadata_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let metadata_lease = engine.catalog().begin_metadata_change().await;
            let mut session = engine.new_session().unwrap();
            let mut truncate_fut = Box::pin(session.truncate_redo_log());

            assert!(matches!(
                futures::poll!(truncate_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_lease);
            let outcome = truncate_fut.await.unwrap();
            assert_eq!(outcome.previous_first_retained_file_seq, 0);
            assert_eq!(outcome.new_first_retained_file_seq, 0);
            assert_eq!(
                outcome.blockers,
                vec![RedoTruncationBlockerInfo::UnsealedFile { file_seq: 0 }]
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_removes_prefix_and_restart_keeps_retained_suffix_strict() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_candidate";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let outcome = session.truncate_redo_log().await.unwrap();

            assert_eq!(outcome.previous_first_retained_file_seq, 0);
            assert!(outcome.new_first_retained_file_seq > 0, "{outcome:?}");
            assert!(outcome.advanced_files > 0, "{outcome:?}");
            assert!(
                outcome.removed_files >= outcome.advanced_files,
                "{outcome:?}"
            );
            assert_eq!(outcome.failed_unlink_files, 0);
            for file_seq in 0..outcome.new_first_retained_file_seq {
                assert!(
                    !redo_file_path(&main_dir, log_file_stem, file_seq).exists(),
                    "obsolete redo file {file_seq:08x} should be removed"
                );
            }
            assert!(
                redo_file_path(
                    &main_dir,
                    log_file_stem,
                    outcome.new_first_retained_file_seq
                )
                .exists(),
                "first retained redo file must remain present"
            );
            drop(session);
            drop(engine);

            let restarted = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let session = restarted.new_session().unwrap();
            assert_eq!(session.list_table_ids().unwrap(), vec![table_id]);
            drop(session);
            drop(restarted);

            fs::remove_file(redo_file_path(
                &main_dir,
                log_file_stem,
                outcome.new_first_retained_file_seq,
            ))
            .unwrap();
            let err = match redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
            {
                Ok(_) => panic!("engine startup should reject missing first retained redo file"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Runtime, "{err:?}");
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::RedoLogDiscovery)
            );
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::RedoLogSequenceGap)
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_marker_publish_failure_does_not_unlink() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_marker_fail";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let plan = engine.inner().trx_sys.plan_redo_truncation().unwrap();
            assert_eq!(plan.first_retained_file_seq, 0);
            assert!(!plan.candidates.is_empty(), "{plan:?}");
            let candidate_paths = plan
                .candidates
                .iter()
                .map(|candidate| redo_file_path(&main_dir, log_file_stem, candidate.file_seq))
                .collect::<Vec<_>>();
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "test setup candidate redo file should exist before marker failure: {}",
                    path.display()
                );
            }

            let catalog_path = engine.inner().table_fs.catalog_mtb_file_path();
            let publish_hook = Arc::new(FailingFirstWriteHook::new(catalog_path));
            let _publish_hook_guard = install_storage_backend_test_hook(publish_hook.clone());
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(Arc::new(
                |file_seq, path| {
                    panic!(
                        "redo cleanup must not run after marker publication failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                },
            ));

            let err = session.truncate_redo_log().await.unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            assert!(publish_hook.call_count() > 0);
            assert_eq!(
                engine
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
                    .unwrap()
                    .meta
                    .first_redo_log_seq,
                0
            );
            for path in &candidate_paths {
                assert!(
                    path.exists(),
                    "candidate redo file should remain after marker failure: {}",
                    path.display()
                );
            }
        });
    }

    #[test]
    fn test_session_truncate_redo_log_releases_catalog_gate_before_cleanup() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_cleanup_gate";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let hook_called = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_called);
            let hook_engine = engine.new_ref().unwrap();
            let hook_guard =
                install_redo_cleanup_before_unlink_hook(Arc::new(move |file_seq, _path| {
                    if file_seq != 0 {
                        return;
                    }
                    hook_flag.store(true, Ordering::SeqCst);
                    let catalog = hook_engine.catalog();
                    let mut metadata_fut = Box::pin(catalog.begin_metadata_change());
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    let metadata_lease = match metadata_fut.as_mut().poll(&mut cx) {
                        Poll::Ready(lease) => lease,
                        Poll::Pending => {
                            panic!("catalog gate should be released before redo cleanup")
                        }
                    };
                    drop(metadata_lease);
                }));

            let mut session = engine.new_session().unwrap();
            let outcome = session.truncate_redo_log().await.unwrap();

            assert!(hook_called.load(Ordering::SeqCst));
            assert_eq!(outcome.previous_first_retained_file_seq, 1);
            assert_eq!(outcome.new_first_retained_file_seq, 1);
            assert_eq!(outcome.advanced_files, 0);
            assert_eq!(outcome.removed_files, 1);
            assert_eq!(outcome.failed_unlink_files, 0);
            assert!(!obsolete_path.exists());
            drop(hook_guard);
        });
    }

    #[test]
    fn test_session_truncate_redo_log_rechecks_poison_after_gate_wait() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_poison_wait";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let redo_retention_lease = engine.inner().trx_sys.begin_redo_retention().await;
            let mut session = engine.new_session().unwrap();
            let mut truncate_fut = Box::pin(session.truncate_redo_log());

            assert!(matches!(
                futures::poll!(truncate_fut.as_mut()),
                std::task::Poll::Pending
            ));

            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));
            drop(redo_retention_lease);

            let err = truncate_fut.await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite)
            );
            assert!(
                obsolete_path.exists(),
                "obsolete redo file should not be removed after poison"
            );
        });
    }

    #[test]
    fn test_session_truncate_redo_log_retries_below_marker_cleanup() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_retry";
            let engine = redo_truncation_engine_config(&main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            let hook_removed_file = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_removed_file);
            let hook_path = obsolete_path.clone();
            let hook_guard =
                install_redo_cleanup_before_unlink_hook(Arc::new(move |file_seq, path| {
                    if file_seq == 0 && path == hook_path && !hook_flag.swap(true, Ordering::SeqCst)
                    {
                        fs::remove_file(path).unwrap();
                    }
                }));

            let mut session = engine.new_session().unwrap();
            let missing = session.truncate_redo_log().await.unwrap();

            assert_eq!(missing.previous_first_retained_file_seq, 1);
            assert_eq!(missing.new_first_retained_file_seq, 1);
            assert_eq!(missing.advanced_files, 0);
            assert_eq!(missing.removed_files, 0);
            assert_eq!(missing.already_missing_files, 1);
            assert_eq!(missing.failed_unlink_files, 0);
            assert!(!obsolete_path.exists());
            drop(hook_guard);

            fs::create_dir(&obsolete_path).unwrap();

            let failed = session.truncate_redo_log().await.unwrap();

            assert_eq!(failed.previous_first_retained_file_seq, 1);
            assert_eq!(failed.new_first_retained_file_seq, 1);
            assert_eq!(failed.advanced_files, 0);
            assert_eq!(failed.removed_files, 0);
            assert_eq!(failed.failed_unlink_files, 1);
            assert!(obsolete_path.exists());

            fs::remove_dir(&obsolete_path).unwrap();
            fs::write(&obsolete_path, b"retry obsolete redo cleanup").unwrap();
            let retried = session.truncate_redo_log().await.unwrap();

            assert_eq!(retried.previous_first_retained_file_seq, 1);
            assert_eq!(retried.new_first_retained_file_seq, 1);
            assert_eq!(retried.advanced_files, 0);
            assert_eq!(retried.removed_files, 1);
            assert_eq!(retried.failed_unlink_files, 0);
            assert!(!obsolete_path.exists());
        });
    }

    #[test]
    fn test_session_overlapping_checkpoint_catalog_calls_complete() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let _table_id = table1(&engine).await;
            let mut session1 = engine.new_session().unwrap();
            let mut session2 = engine.new_session().unwrap();

            let (res1, res2) =
                futures::join!(session1.checkpoint_catalog(), session2.checkpoint_catalog());

            res1.unwrap();
            res2.unwrap();
        });
    }

    #[test]
    fn test_session_stats_snapshots_are_monotonic() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            let trx0 = session.transaction_system_stats().unwrap();
            let storage0 = session.storage_io_stats().unwrap();
            let pools0 = session.buffer_pool_stats().unwrap();
            assert_eq!(trx0.commit_count, 0);
            assert_eq!(trx0.trx_count, 0);
            assert_eq!(trx0.log_bytes, 0);
            assert!(pools0.meta.capacity > 0);
            assert!(pools0.mem.capacity > 0);
            assert!(pools0.index.capacity > 0);
            assert!(pools0.disk.capacity > 0);

            let _table_id = table1(&engine).await;
            let trx1 = session.transaction_system_stats().unwrap();
            let storage1 = session.storage_io_stats().unwrap();
            let pools1 = session.buffer_pool_stats().unwrap();
            // Commit waiters can complete before the redo thread publishes
            // aggregate stats, so this test verifies monotonic snapshots
            // rather than immediate progress from the preceding operation.
            assert_transaction_system_stats_monotonic(trx0, trx1);
            assert!(storage1.backend.submitted_ops >= storage0.backend.submitted_ops);
            assert!(storage1.table_read_requests >= storage0.table_read_requests);
            assert!(storage1.pool_read_requests >= storage0.pool_read_requests);
            assert!(storage1.background_write_requests >= storage0.background_write_requests);
            assert_buffer_pool_stats_monotonic(&pools0, &pools1);
        });
    }

    #[test]
    fn test_session_query_methods_require_registered_running_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            engine
                .inner()
                .session_registry
                .close_idle(session.id())
                .unwrap();

            for err in [
                session.list_table_ids().unwrap_err(),
                session.transaction_system_stats().unwrap_err(),
                session.storage_io_stats().unwrap_err(),
                session.buffer_pool_stats().unwrap_err(),
            ] {
                assert_eq!(err.kind(), ErrorKind::Lifecycle);
                assert_eq!(
                    err.report().downcast_ref::<LifecycleError>().copied(),
                    Some(LifecycleError::SessionUnavailable)
                );
            }
        });
    }

    #[test]
    fn test_session_query_methods_fail_after_engine_shutdown() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            engine.shutdown().unwrap();

            assert_runtime_unavailable_after_shutdown(session.list_table_ids().unwrap_err());
            assert_runtime_unavailable_after_shutdown(
                session.transaction_system_stats().unwrap_err(),
            );
            assert_runtime_unavailable_after_shutdown(session.storage_io_stats().unwrap_err());
            assert_runtime_unavailable_after_shutdown(session.buffer_pool_stats().unwrap_err());
        });
    }

    #[test]
    fn test_session_diagnostics_remain_visible_after_storage_poison() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();

            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));

            assert_eq!(session.list_table_ids().unwrap(), vec![table_id]);
            assert!(session.transaction_system_stats().is_ok());
            assert!(session.storage_io_stats().is_ok());
            assert!(session.buffer_pool_stats().is_ok());

            let err = session.truncate_redo_log().await.unwrap_err();
            assert_runtime_unavailable_after_fatal(err, FatalError::RedoWrite);

            let err = session.checkpoint_catalog().await.unwrap_err();
            assert_runtime_unavailable_after_fatal(err, FatalError::RedoWrite);
        });
    }
}
