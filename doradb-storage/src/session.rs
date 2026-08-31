use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards};
use crate::catalog::{
    Catalog, CatalogCheckpointOutcome, CatalogCheckpointScope, CreateIndexPlan, CreateTableOutcome,
    DropIndexPlan, DropTablePlan, IndexDdlGateScope, IndexID, PreparedCreateIndex,
    PreparedCreateTable, PreparedDropIndex, PreparedDropTable, StorageIndexSpec, StorageTableSpec,
    ValidatedCreateTable, create_index_catalog_write_targets, create_table_catalog_write_targets,
    drop_index_catalog_write_targets, drop_table_catalog_write_targets,
    prepare_catalog_checkpoint_operation, reject_non_user_table_id,
    reject_user_table_primary_key_index, validated_index_ddl_target,
};
use crate::engine::{EngineAdmission, EngineCore, EngineLifecycle};
use crate::error::{
    CompletionErrorBridge, CompletionResult, DiscloseError, DiscloseResultExt, FatalError,
    LifecycleError, LifecycleOrFatalError, LifecycleOrFatalResult, LifecycleResult,
    MultiDomainResultExt, OperationError, OperationOrFatalResult, OperationResult, Result,
    RuntimeError,
};
use crate::id::{OperationID, SessionID, SessionOperationKey, TableID, TrxID};
use crate::lock::{
    FamilyLockAuthority, FamilyLockState, FreshClaimsGuard, LockMode, LockOwner, LockResource,
    LockScopeState, TableLockMode,
};
use crate::map::{FastDashMap, FastHashMap};
use crate::notify::EventNotifyOnDrop;
use crate::quiescent::QuiescentGuard;
use crate::runtime::mandatory::{AcceptedExecution, MandatoryTaskMetadata, PreparedExecution};
use crate::stats::{
    BufferPoolStats, LogicalLockStats, MandatoryRuntimeStats, StorageIoStats,
    TransactionSystemStats, buffer_pool_runtime_stats_snapshot, storage_io_stats_snapshot,
    transaction_system_stats_snapshot,
};
use crate::table::{
    CheckpointDelayReason, CheckpointOutcome, CheckpointRetryObservation, FreezeOutcome,
    MemIndexCleanupOutcome, Table, prepare_checkpoint_table_operation,
    prepare_freeze_table_operation, prepare_mem_index_cleanup_operation,
};
use crate::trx::{
    FrozenReadSnapshotCore, PrivateTransaction, ReadSnapshotBuildCore, ReadSnapshotBuilder,
    ReadSnapshotDrainReason, ReadSnapshotEntry, ReadSnapshotLockOwner, ReadSnapshotPhase,
    ReadSnapshotReadyPayload, ReadSnapshotTerminalClaim, RedoRetentionScope,
    ReleasedTransactionLocks, SessionOperationEntry, SessionOperationKind, SessionOperationState,
    Transaction, TrxInner, prepare_catalog_redo_maintenance_operation,
    prepare_redo_truncation_operation,
};
use error_stack::{Report, ResultExt};
use event_listener::EventListener;
use futures::future::select_all;
use parking_lot::Mutex;
use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::mem::replace;
use std::ops::Deref;
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

/// Caller-owned DDL preparation transferred atomically at mandatory acceptance.
///
/// The foreground pin owns both the family root and the operation scope, so
/// ordinary cancellation closes claims before publishing the terminal edge.
pub(crate) struct PreparedDdlScope {
    operation: SessionOperationPin,
}

impl PreparedDdlScope {
    /// Prepare the fixed CREATE TABLE lock set in canonical resource order.
    #[inline]
    pub(crate) async fn create(
        mut operation: SessionOperationPin,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<Self> {
        operation
            .acquire_ddl_create(table_id, catalog_targets)
            .await?;
        Ok(Self { operation })
    }

    /// Prepare the fixed DROP TABLE lock set in canonical resource order.
    #[inline]
    pub(crate) async fn drop_table(
        mut operation: SessionOperationPin,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<Self> {
        operation
            .acquire_ddl_existing(table_id, catalog_targets)
            .await?;
        Ok(Self { operation })
    }

    /// Prepare the fixed CREATE INDEX lock set in canonical resource order.
    #[inline]
    pub(crate) async fn create_index(
        mut operation: SessionOperationPin,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<Self> {
        operation
            .acquire_ddl_existing(table_id, catalog_targets)
            .await?;
        Ok(Self { operation })
    }

    /// Prepare the fixed DROP INDEX lock set in canonical resource order.
    #[inline]
    pub(crate) async fn drop_index(
        mut operation: SessionOperationPin,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<Self> {
        operation
            .acquire_ddl_existing(table_id, catalog_targets)
            .await?;
        Ok(Self { operation })
    }

    /// Return the exact operation key carried into mandatory diagnostics.
    #[inline]
    pub(crate) fn key(&self) -> SessionOperationKey {
        self.operation.key()
    }

    /// Return the retained engine while caller preparation still owns the scope.
    #[inline]
    pub(crate) fn engine(&self) -> &SessionRuntime {
        &self.operation.runtime
    }

    /// Synchronously consume caller preparation into accepted authority.
    #[inline]
    pub(crate) fn accept(self) -> AcceptedDdlScope {
        AcceptedDdlScope {
            operation: self.operation.into_mandatory(),
            finish_state: DdlFinishState::Executing,
        }
    }
}

enum DdlFinishState {
    Executing,
    TerminalReady,
    FailedRetained,
}

/// Runtime-owned table-DDL operation and its transferred logical locks.
pub(crate) struct AcceptedDdlScope {
    operation: MandatoryOperationGuard,
    finish_state: DdlFinishState,
}

impl AcceptedDdlScope {
    /// Return the retained engine runtime.
    #[inline]
    pub(crate) fn engine(&self) -> &SessionRuntime {
        &self.operation.runtime
    }

    /// Start one mandatory-owned nested private transaction.
    #[inline]
    pub(crate) fn begin_private_trx(&mut self) -> LifecycleResult<PrivateTransaction> {
        self.operation.begin_private_trx()
    }

    /// Verify the nested state before returning from accepted execution.
    #[inline]
    pub(crate) fn mark_terminal_ready(&mut self) {
        self.operation.assert_finish_ready();
        self.finish_state = DdlFinishState::TerminalReady;
    }

    /// Publish normal completion or defensively retain an invalid finish state.
    #[inline]
    pub(crate) fn finish(&mut self) {
        let state = replace(&mut self.finish_state, DdlFinishState::FailedRetained);
        match state {
            DdlFinishState::TerminalReady => {
                self.operation.finish();
            }
            DdlFinishState::Executing => {
                self.operation.fail_retained();
                let report = Report::new(FatalError::MandatoryTaskPanic)
                    .attach("accepted table DDL finished without terminal-ready state");
                self.operation.runtime.poisoner.poison(report);
            }
            DdlFinishState::FailedRetained => {}
        }
    }

    /// Retain unsafe nested ownership before the supervisor publishes poison.
    #[inline]
    pub(crate) fn handle_panic(&mut self) {
        self.operation.fail_retained();
        self.finish_state = DdlFinishState::FailedRetained;
    }
}

/// Caller-owned maintenance preparation transferred atomically at acceptance.
///
/// The voluntary operation pin owns its exact lock scope through cancellation.
pub(crate) struct PreparedMaintenanceScope {
    operation: SessionOperationPin,
}

impl PreparedMaintenanceScope {
    /// Prepare one table-scoped maintenance lock set.
    pub(crate) async fn table(
        mut operation: SessionOperationPin,
        table_id: TableID,
    ) -> OperationOrFatalResult<Self> {
        operation.acquire_maintenance_table(table_id).await?;
        Ok(Self { operation })
    }

    /// Prepare one catalog/redo-wide maintenance operation.
    #[inline]
    pub(crate) fn global(operation: SessionOperationPin) -> Self {
        Self { operation }
    }

    /// Return the exact operation key carried into mandatory diagnostics.
    #[inline]
    pub(crate) fn key(&self) -> SessionOperationKey {
        self.operation.key()
    }

    /// Return the retained engine while caller preparation owns the scope.
    #[inline]
    pub(crate) fn engine(&self) -> &SessionRuntime {
        &self.operation.runtime
    }

    /// Resolve and retain the authoritative current-live table under locks.
    pub(crate) async fn resolve_user_table(
        &self,
        table_id: TableID,
    ) -> OperationResult<Arc<Table>> {
        let table = self
            .operation
            .runtime
            .catalog()
            .validate_user_table_live(table_id)
            .await?;
        self.operation.runtime.state().cache_user_table(&table);
        Ok(table)
    }

    /// Synchronously consume caller preparation and execution state into accepted authority.
    #[inline]
    fn accept<E>(self, execution: E) -> AcceptedMaintenanceScope<E>
    where
        E: MaintenanceExecution,
    {
        AcceptedMaintenanceScope {
            execution: Some(execution),
            operation: self.operation.into_mandatory(),
            finish_state: MaintenanceFinishState::Executing,
        }
    }
}

enum MaintenanceFinishState {
    Executing,
    TerminalReady,
    FailedRetained,
}

/// Runtime-owned maintenance execution and its transferred logical locks.
pub(crate) struct AcceptedMaintenanceScope<E>
where
    E: MaintenanceExecution,
{
    execution: Option<E>,
    operation: MandatoryOperationGuard,
    finish_state: MaintenanceFinishState,
}

impl<E> AcceptedExecution for AcceptedMaintenanceScope<E>
where
    E: MaintenanceExecution,
{
    type Output = E::Output;

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self
            .execution
            .as_mut()
            .unwrap_or_else(|| panic!("accepted maintenance execution is missing"))
            .execute(&self.operation.runtime)
            .await;
        self.operation.assert_finish_ready();
        self.finish_state = MaintenanceFinishState::TerminalReady;
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.execution.take());
        let state = replace(
            &mut self.finish_state,
            MaintenanceFinishState::FailedRetained,
        );
        match state {
            MaintenanceFinishState::TerminalReady => {
                self.operation.finish();
            }
            MaintenanceFinishState::Executing => {
                self.operation.fail_retained();
                let report = Report::new(FatalError::MandatoryTaskPanic)
                    .attach("accepted maintenance finished without terminal-ready state");
                self.operation.runtime.poisoner.poison(report);
            }
            MaintenanceFinishState::FailedRetained => {}
        }
    }

    #[inline]
    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        let diagnostic = self
            .execution
            .as_ref()
            .unwrap_or_else(|| panic!("accepted maintenance execution is missing"))
            .panic_diagnostic();
        drop(self.execution.take());
        self.operation.fail_retained();
        self.finish_state = MaintenanceFinishState::FailedRetained;
        CompletionErrorBridge::capture(
            Report::new(FatalError::MandatoryTaskPanic).attach(diagnostic),
        )
    }
}

/// Operation-specific state and behavior owned by one accepted maintenance scope.
pub(crate) trait MaintenanceExecution: Send + 'static {
    /// Terminal output delivered to the maintenance observer.
    type Output: Send + 'static;

    /// Stable mandatory-runtime diagnostic label.
    const LABEL: &'static str;

    /// Execute one accepted operation using its retained session runtime.
    fn execute(
        &mut self,
        runtime: &SessionRuntime,
    ) -> impl Future<Output = CompletionResult<Self::Output>> + Send;

    /// Build the diagnostic attached after an unexpected execution panic.
    fn panic_diagnostic(&self) -> String;
}

/// Shared caller-prepared carrier for one maintenance execution body.
pub(crate) struct PreparedMaintenanceExecution<E>
where
    E: MaintenanceExecution,
{
    execution: E,
    scope: PreparedMaintenanceScope,
    metadata: MandatoryTaskMetadata,
}

impl<E> PreparedMaintenanceExecution<E>
where
    E: MaintenanceExecution,
{
    /// Build one global catalog/redo maintenance operation.
    #[inline]
    pub(crate) fn global(scope: PreparedMaintenanceScope, execution: E) -> Self {
        let metadata = MandatoryTaskMetadata::operation(E::LABEL, Some(scope.key()));
        Self {
            execution,
            scope,
            metadata,
        }
    }

    /// Build one table-scoped maintenance operation.
    #[inline]
    pub(crate) fn table(scope: PreparedMaintenanceScope, execution: E, table_id: TableID) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(E::LABEL, scope.key(), table_id);
        Self {
            execution,
            scope,
            metadata,
        }
    }
}

impl<E> PreparedExecution for PreparedMaintenanceExecution<E>
where
    E: MaintenanceExecution,
{
    type Output = E::Output;
    type Accepted = AcceptedMaintenanceScope<E>;

    const LABEL: &'static str = E::LABEL;

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            execution,
            scope,
            metadata: _,
        } = self;
        scope.accept(execution)
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
    fn observed(self, session: &SessionObserverPin) -> TrxID {
        match self {
            MaintenanceBoundary::GcHorizon => session.runtime.trx_sys.published_gc_horizon(),
            MaintenanceBoundary::PurgeCompletion => session.runtime.trx_sys.global_visible_sts(),
        }
    }

    #[inline]
    fn listener(self, session: &SessionObserverPin) -> event_listener::EventListener {
        match self {
            MaintenanceBoundary::GcHorizon => session.runtime.trx_sys.gc_horizon_listener(),
            MaintenanceBoundary::PurgeCompletion => {
                session.runtime.trx_sys.purge_completion_listener()
            }
        }
    }
}

/// Live table runtime bound to one admitted session maintenance operation.
struct SessionTable<'s> {
    table: Arc<Table>,
    session: &'s SessionOperationPin,
}

impl<'s> SessionTable<'s> {
    /// Borrows the admitted runtime without exposing another strong clone.
    #[inline]
    fn table(&self) -> &Table {
        &self.table
    }

    /// Borrows the pool guards retained by the admitted session operation.
    #[inline]
    fn pool_guards(&self) -> &PoolGuards {
        self.session.pool_guards()
    }
}

impl Drop for SessionTable<'_> {
    #[inline]
    fn drop(&mut self) {
        // The Drop impl makes drop checking retain `session` until the owned
        // table runtime is released.
    }
}

/// Limited per-session façade over the engine lifecycle admission gate.
pub(crate) struct SessionAdmission {
    lifecycle: Arc<EngineLifecycle>,
}

impl SessionAdmission {
    /// Create one façade shared by a session state and its public handles.
    #[inline]
    pub(crate) fn new(lifecycle: Arc<EngineLifecycle>) -> Self {
        Self { lifecycle }
    }

    /// Acquire short-lived operation-start admission.
    #[inline]
    fn acquire(&self) -> LifecycleResult<EngineAdmission<'_>> {
        self.lifecycle
            .admit()
            .attach_with(|| "phase=acquire_engine_lifecycle_admission")
    }

    /// Returns whether owner-side shutdown has started.
    #[inline]
    pub(crate) fn shutdown_started(&self) -> bool {
        self.lifecycle.shutdown_started()
    }

    /// Register for owner-side shutdown start.
    #[inline]
    pub(crate) fn shutdown_listener(&self) -> EventListener {
        self.lifecycle.shutdown_listener()
    }
}

/// Weak reachability to one exact registered session state.
#[derive(Clone)]
pub(crate) struct WeakSessionRef {
    state: Weak<SessionState>,
    admission: Arc<SessionAdmission>,
}

impl WeakSessionRef {
    /// Create a weak session capability without retaining engine components.
    #[inline]
    fn new(state: &Arc<SessionState>) -> Self {
        Self {
            state: Arc::downgrade(state),
            admission: Arc::clone(&state.admission),
        }
    }

    /// Acquires foreground admission and upgrades the exact registered session.
    #[inline]
    pub(crate) fn upgrade(&self) -> LifecycleResult<Option<AdmittedSessionRuntime<'_>>> {
        let admission = self.admission.acquire()?;
        Ok(self
            .state
            .upgrade()
            .map(SessionRuntime)
            .map(|runtime| AdmittedSessionRuntime {
                runtime,
                _admission: admission,
            }))
    }

    /// Best-effort upgrade for terminal and cleanup ownership.
    #[inline]
    pub(crate) fn upgrade_for_terminal(&self) -> Option<SessionRuntime> {
        self.state.upgrade().map(SessionRuntime)
    }
}

/// Strong session reachability retaining its operation-start admission.
///
/// Callers release admission with [`Self::into_runtime`] only after registering
/// a stable operation or observer proof.
pub(crate) struct AdmittedSessionRuntime<'a> {
    runtime: SessionRuntime,
    _admission: EngineAdmission<'a>,
}

impl AdmittedSessionRuntime<'_> {
    /// Borrow the pinned runtime while admission remains active.
    #[inline]
    pub(crate) fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    /// Retain the runtime while releasing operation-start admission.
    #[inline]
    pub(crate) fn into_runtime(self) -> SessionRuntime {
        self.runtime
    }
}

/// Shared runtime view implemented by observer and foreground authorities.
pub(crate) trait SessionRuntimeAccess {
    /// Returns the retained exact session runtime.
    fn runtime(&self) -> &SessionRuntime;
    /// Returns immutable shared engine capabilities.
    fn engine(&self) -> &EngineCore {
        self.runtime().core()
    }
    /// Borrows the exact session's pool-guard roots.
    fn pool_guards(&self) -> &PoolGuards {
        self.runtime().pool_guards()
    }
}

/// Strong operation-local reachability to one exact session state.
///
/// This typed `Arc` wrapper pins the state reached by a public weak handle.
/// Engine capabilities are reached through the state without a separate core
/// clone. Foreground admission establishes durable operation or observer
/// ownership before yielding this plain strong attachment; the attachment is
/// not itself an admission guard.
#[derive(Clone)]
pub(crate) struct SessionRuntime(Arc<SessionState>);

impl SessionRuntime {
    /// Wrap one registered session state without another allocation.
    #[inline]
    fn new(state: Arc<SessionState>) -> Self {
        Self(state)
    }

    /// Return the exact pinned session state.
    #[inline]
    pub(crate) fn state(&self) -> &Arc<SessionState> {
        &self.0
    }

    /// Return the immutable engine capability set reached through the state.
    #[inline]
    pub(crate) fn core(&self) -> &EngineCore {
        &self.0.core
    }

    /// Create weak reachability for a public transaction facade.
    #[inline]
    pub(crate) fn downgrade(&self) -> WeakSessionRef {
        WeakSessionRef::new(&self.0)
    }

    /// Borrow the exact session's pool-guard roots.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        &self.0.pool_guards
    }

    /// Returns whether owner-side shutdown has closed operation admission.
    #[inline]
    pub(crate) fn shutdown_started(&self) -> bool {
        self.0.admission.shutdown_started()
    }

    /// Register for owner-side shutdown start through the session admission façade.
    #[inline]
    pub(crate) fn shutdown_listener(&self) -> EventListener {
        self.0.admission.shutdown_listener()
    }

    /// Clone catalog capability for an accepted ownership handoff.
    #[inline]
    pub(crate) fn catalog_guard(&self) -> QuiescentGuard<Catalog> {
        self.core().catalog.clone()
    }

    /// Begin one public transaction while foreground admission remains held.
    #[inline]
    fn begin_public_trx(&self) -> LifecycleResult<Transaction> {
        let state = self.state();
        let mut lifecycle = state.lifecycle.lock();
        lifecycle
            .admit_idle()
            .attach_with(|| format!("session_id={}", state.id))?;
        let key = SessionState::next_operation_key(&lifecycle, state.id);
        let inner = lifecycle.public_trx_cache.take().unwrap_or_else(|| {
            panic!(
                "idle session must retain one ready public transaction core: session_id={}",
                state.id
            )
        });
        let authority = lifecycle.lock_authority.take().unwrap_or_else(|| {
            panic!(
                "idle session must retain family lock authority: session_id={}, operation_key={key}",
                state.id
            )
        });
        let (trx, entry) = self
            .trx_sys
            .begin_public_trx(self.downgrade(), key, inner, authority);
        lifecycle.advance_operation_id();
        lifecycle.slot = SessionOperationSlot::Active(ActiveSessionOperation::Operation(entry));
        Ok(trx)
    }

    /// Seal an exact read snapshot and synchronously resolve checked-in cleanup.
    #[inline]
    pub(crate) fn request_read_snapshot_close(
        &self,
        key: SessionOperationKey,
        reason: ReadSnapshotDrainReason,
    ) {
        let claim = self.request_read_snapshot_drain(key, reason);
        if let Some(claim) = claim {
            claim.cleanup();
        }
    }

    /// Seal an exact snapshot and claim checked-in terminal ownership when available.
    #[inline]
    fn request_read_snapshot_drain(
        &self,
        key: SessionOperationKey,
        reason: ReadSnapshotDrainReason,
    ) -> Option<ReadSnapshotTerminalClaim> {
        let state = self.state();
        let lifecycle = state.lifecycle.lock();
        let entry = lifecycle
            .slot
            .active_read_snapshot()
            .filter(|entry| entry.key() == key)
            .cloned()?;
        entry.request_drain(reason);
        let claim = entry.claim_terminal(self.clone());
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        SessionState::notify_operation_change(notify);
        claim
    }

    /// Claim a terminal payload only while this exact typed entry remains active.
    #[inline]
    pub(crate) fn claim_read_snapshot_terminal(
        &self,
        entry: &Arc<ReadSnapshotEntry>,
    ) -> Option<ReadSnapshotTerminalClaim> {
        let state = self.state();
        let lifecycle = state.lifecycle.lock();
        let exact = lifecycle
            .slot
            .active_read_snapshot()
            .is_some_and(|active| Arc::ptr_eq(active, entry));
        let claim = exact.then(|| entry.claim_terminal(self.clone())).flatten();
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        if claim.is_some() {
            SessionState::notify_operation_change(notify);
        }
        claim
    }

    /// Resolve terminal cleanup exposed by a returned counted checkout.
    #[inline]
    pub(crate) fn return_read_snapshot_checkout(&self, entry: &Arc<ReadSnapshotEntry>) {
        let claim = self.claim_read_snapshot_terminal(entry);
        if let Some(claim) = claim {
            claim.cleanup();
        } else {
            let state = self.state();
            let lifecycle = state.lifecycle.lock();
            let notify = lifecycle.change_ev.clone();
            drop(lifecycle);
            SessionState::notify_operation_change(notify);
        }
    }

    /// Remove this state only when the registry still owns this exact Arc.
    #[inline]
    pub(crate) fn remove_if_requested(&self, remove_from_registry: bool) {
        if !remove_from_registry {
            return;
        }
        if let Some(registry) = self.core().session_registry.upgrade() {
            registry.remove_exact(self);
        }
    }
}

impl Deref for SessionRuntime {
    type Target = EngineCore;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.core()
    }
}

impl SessionRuntimeAccess for SessionRuntime {
    #[inline]
    fn runtime(&self) -> &SessionRuntime {
        self
    }
}

/// Weak, non-cloneable public session capability bound to one engine instance.
///
/// The engine owns the strong session state in its internal session registry.
/// Public session operations upgrade weak state reachability internally, pin
/// that exact state for one operation, and release admission
/// guards before async work. A session may move between threads but cannot be
/// shared between them. Lock-free observations use shared access; state
/// mutation and every logical-lock transition require mutable access. Every
/// effectful public operation requires an open idle coordinator slot; detached
/// observer reads and progress waits remain admissible while another operation
/// is active.
pub struct Session {
    id: SessionID,
    session: WeakSessionRef,
    /// Local explicit-close marker.
    ///
    /// `Cell` intentionally preserves `Send` while suppressing the `Sync`
    /// auto-implementation. Closing still requires mutable session access.
    closed: Cell<bool>,
}

impl Session {
    /// Creates a weak public session handle.
    #[inline]
    pub(crate) fn new(session: WeakSessionRef, id: SessionID) -> Self {
        Session {
            id,
            session,
            closed: Cell::new(false),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns a pinned observer view using normal healthy-runtime admission.
    ///
    /// This path rejects storage poison. Poison-observable read-only diagnostics
    /// use [`Self::pin_inspection`] instead.
    #[inline]
    pub(crate) fn pin_observer(&self) -> LifecycleOrFatalResult<SessionObserverPin> {
        if self.closed.get() {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("session_id={}", self.id))
                .into());
        }
        let admitted = self
            .session
            .upgrade()
            .attach_with(|| format!("session_id={}", self.id))?
            .ok_or_else(|| {
                Report::new(LifecycleError::SessionUnavailable)
                    .attach(format!("session_id={}, reason=session_missing", self.id))
            })?;
        admitted
            .runtime()
            .poisoner
            .ensure_healthy()
            .map_err(|error| {
                LifecycleOrFatalError::from(
                    error.attach(format!("session_id={}, phase=check_engine_health", self.id)),
                )
            })?;
        admitted
            .runtime()
            .state()
            .acquire_observer()
            .attach_with(|| format!("session_id={}", self.id))?;
        let runtime = admitted.into_runtime();
        Ok(SessionObserverPin { runtime })
    }

    /// Reserves one stable entry for an effectful public session operation.
    #[inline]
    fn pin_operation(
        &self,
        kind: SessionOperationKind,
    ) -> LifecycleOrFatalResult<SessionOperationPin> {
        if self.closed.get() {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("session_id={}", self.id))
                .into());
        }
        let admitted = self
            .session
            .upgrade()
            .attach_with(|| format!("session_id={}, kind={}", self.id, kind.label()))?
            .ok_or_else(|| {
                Report::new(LifecycleError::SessionUnavailable)
                    .attach(format!("session_id={}, reason=session_missing", self.id))
            })?;
        admitted
            .runtime()
            .poisoner
            .ensure_healthy()
            .map_err(|error| {
                LifecycleOrFatalError::from(error.attach(format!(
                    "session_id={}, kind={}, phase=check_engine_health",
                    self.id,
                    kind.label()
                )))
            })?;
        let (entry, authority) = admitted
            .runtime()
            .state()
            .reserve_operation(kind)
            .attach_with(|| format!("session_id={}, kind={}", self.id, kind.label()))?;
        let runtime = admitted.into_runtime();
        let curr_scope = matches!(
            kind,
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
        )
        .then(|| LockScopeState::new(LockOwner::operation(entry.key())));
        Ok(SessionOperationPin {
            runtime,
            entry,
            authority: Some(authority),
            curr_scope,
            armed: true,
        })
    }

    /// Returns a lifecycle-pinned view for poison-observable inspection.
    ///
    /// Unlike [`Self::pin_observer`], this path deliberately skips storage
    /// health validation. It still requires an open engine lifecycle and a
    /// registered, available session, and callers must restrict the resulting
    /// pin to read-only diagnostic snapshots.
    #[inline]
    fn pin_inspection(&self) -> LifecycleResult<SessionObserverPin> {
        if self.closed.get() {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("session_id={}", self.id)));
        }
        let admitted = self
            .session
            .upgrade()
            .attach_with(|| format!("session_id={}", self.id))?
            .ok_or_else(|| {
                Report::new(LifecycleError::SessionUnavailable)
                    .attach(format!("session_id={}, reason=session_missing", self.id))
            })?;
        admitted
            .runtime()
            .state()
            .acquire_observer()
            .attach_with(|| format!("session_id={}", self.id))?;
        let runtime = admitted.into_runtime();
        Ok(SessionObserverPin { runtime })
    }

    /// Return sorted ids for currently loaded user-table runtimes.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is still running. It is not available after engine
    /// shutdown, session close, or registry removal. It does not expose
    /// in-flight DDL rows that have not installed a user-table runtime.
    #[inline]
    pub fn list_table_ids(&self) -> Result<Vec<TableID>> {
        let session = self
            .pin_inspection()
            .attach("operation=list_table_ids")
            .disclose()?;
        Ok(session.runtime.catalog().list_user_table_ids_now())
    }

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn begin_trx(&mut self) -> Result<Transaction> {
        if self.closed.get() {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("session_id={}", self.id))
                .disclose());
        }
        let admitted = self
            .session
            .upgrade()
            .attach_with(|| format!("session_id={}", self.id))
            .disclose()?
            .ok_or_else(|| {
                Report::new(LifecycleError::SessionUnavailable)
                    .attach(format!("session_id={}, reason=session_missing", self.id))
            })
            .disclose()?;
        admitted
            .runtime()
            .poisoner
            .ensure_healthy()
            .attach_with(|| format!("session_id={}, phase=check_engine_health", self.id))
            .disclose()?;
        let trx = admitted
            .runtime()
            .begin_public_trx()
            .attach("operation=begin_transaction")
            .disclose()?;
        drop(admitted);
        Ok(trx)
    }

    /// Begin one shared read snapshot at a newly registered timestamp.
    #[inline]
    pub fn begin_read_snapshot(&mut self) -> Result<ReadSnapshotBuilder> {
        if self.closed.get() {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("session_id={}", self.id))
                .disclose());
        }
        let admitted = self
            .session
            .upgrade()
            .attach_with(|| format!("session_id={}", self.id))
            .disclose()?
            .ok_or_else(|| {
                Report::new(LifecycleError::SessionUnavailable)
                    .attach(format!("session_id={}, reason=session_missing", self.id))
            })
            .disclose()?;
        admitted
            .runtime()
            .poisoner
            .ensure_healthy()
            .attach_with(|| format!("session_id={}, phase=check_engine_health", self.id))
            .disclose()?;
        let (entry, sts) = admitted
            .runtime()
            .state()
            .reserve_read_snapshot()
            .attach_with(|| format!("session_id={}", self.id))
            .disclose()?;
        assert_eq!(
            entry.sts(),
            sts,
            "reserved snapshot entry returned mismatched STS: key={}",
            entry.key()
        );
        let builder = ReadSnapshotBuilder::new(self.session.clone(), entry.key(), sts);
        drop(admitted);
        Ok(builder)
    }

    /// Close this session when no caller-owned foreground operation is active.
    #[inline]
    pub async fn close(&mut self) -> Result<()> {
        if self.closed.get() {
            return Ok(());
        }
        // End the non-Send admitted wrapper's storage scope before the close
        // loop can await. `into_runtime` releases foreground admission while
        // retaining the runtime pin that owns asynchronous close progress.
        let runtime = {
            let admitted = self
                .session
                .upgrade()
                .attach_with(|| format!("operation=close_session, session_id={}", self.id))
                .disclose()?
                .ok_or_else(|| {
                    Report::new(LifecycleError::SessionUnavailable)
                        .attach(format!("session_id={}, reason=session_missing", self.id))
                })
                .disclose()?;
            admitted
                .runtime()
                .poisoner
                .ensure_healthy()
                .attach_with(|| {
                    format!(
                        "operation=close_session, session_id={}, phase=check_engine_health",
                        self.id
                    )
                })
                .disclose()?;
            admitted.into_runtime()
        };
        let mut initial_runtime = Some(runtime);
        loop {
            let decision = {
                let Some(runtime) = initial_runtime
                    .take()
                    .or_else(|| self.session.upgrade_for_terminal())
                else {
                    break;
                };
                let (decision, remove_from_registry) = runtime.state().request_close();
                runtime.remove_if_requested(remove_from_registry);
                // A lifecycle listener owns its wake registration without the
                // runtime. End strong reachability before a possible wait so
                // terminal session removal cannot leave a hidden shutdown owner.
                decision
            };
            match decision {
                SessionCloseDecision::Closed => break,
                SessionCloseDecision::Wait(listener) => listener.await,
                SessionCloseDecision::Rejected(err) => {
                    return Err(err.attach("operation=close_session").disclose());
                }
            }
        }
        self.closed.set(true);
        Ok(())
    }

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        table_spec: StorageTableSpec,
        index_specs: Vec<StorageIndexSpec>,
    ) -> Result<CreateTableOutcome> {
        let validated = ValidatedCreateTable::try_new(table_spec, index_specs).disclose()?;
        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=create_table")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let prepared = operation
            .prepare_create_table(validated)
            .await
            .attach("operation=create_table")
            .disclose()?;
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=create_table")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CatalogAccess))
            .attach("operation=create_table, phase=wait_mandatory_completion")
            .disclose()
    }

    /// Build and publish a new secondary index for an existing user table.
    #[inline]
    pub async fn create_index(
        &mut self,
        table_id: TableID,
        index_spec: StorageIndexSpec,
    ) -> Result<IndexID> {
        reject_user_table_primary_key_index(&index_spec, "create_index").disclose()?;
        reject_non_user_table_id(table_id, "create_index").disclose()?;
        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=create_index")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        operation
            .reject_table_ddl_explicit_session_lock(table_id)
            .attach("operation=create_index")
            .disclose()?;
        let scope = PreparedDdlScope::create_index(
            operation,
            table_id,
            create_index_catalog_write_targets(),
        )
        .await
        .attach_with(|| format!("prepare CREATE INDEX locks: table_id={table_id}"))
        .disclose()?;
        let engine = scope.engine();
        let table =
            validated_index_ddl_target(engine, engine.pool_guards(), table_id, "create_index")
                .await
                .disclose()?;
        engine.poisoner.ensure_healthy().disclose()?;
        let gates = IndexDdlGateScope::acquire(Arc::clone(&table), engine.catalog_guard())
            .await
            .attach("operation=create_index")
            .disclose()?;
        let plan = CreateIndexPlan::new(table_id, table, index_spec, &engine.catalog().storage)
            .disclose()?;
        let observer = mandatory_runtime
            .submit(PreparedCreateIndex::new(gates, scope, plan))
            .await
            .attach("operation=create_index")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::IndexAccess))
            .attach_with(|| {
                format!(
                    "operation=create_index, phase=wait_mandatory_completion, table_id={table_id}"
                )
            })
            .disclose()
    }

    /// Logically drop an active secondary index from an existing user table.
    #[inline]
    pub async fn drop_index(&mut self, table_id: TableID, index_id: IndexID) -> Result<()> {
        reject_non_user_table_id(table_id, "drop_index").disclose()?;
        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=drop_index")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        operation
            .reject_table_ddl_explicit_session_lock(table_id)
            .attach("operation=drop_index")
            .disclose()?;
        let scope =
            PreparedDdlScope::drop_index(operation, table_id, drop_index_catalog_write_targets())
                .await
                .attach_with(|| format!("prepare DROP INDEX locks: table_id={table_id}"))
                .disclose()?;
        let engine = scope.engine();
        let table =
            validated_index_ddl_target(engine, engine.pool_guards(), table_id, "drop_index")
                .await
                .disclose()?;
        engine.poisoner.ensure_healthy().disclose()?;
        let gates = IndexDdlGateScope::acquire(Arc::clone(&table), engine.catalog_guard())
            .await
            .attach("operation=drop_index")
            .disclose()?;
        let plan = DropIndexPlan::new(table_id, table, index_id).disclose()?;
        let observer = mandatory_runtime
            .submit(PreparedDropIndex::new(gates, scope, plan))
            .await
            .attach("operation=drop_index")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::IndexAccess))
            .attach_with(|| {
                format!(
                    "operation=drop_index, phase=wait_mandatory_completion, table_id={table_id}, index_id={index_id}"
                )
            })
            .disclose()
    }

    /// Logically drop an existing user table.
    #[inline]
    pub async fn drop_table(&mut self, table_id: TableID) -> Result<()> {
        reject_non_user_table_id(table_id, "drop_table").disclose()?;
        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=drop_table")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let prepared = operation
            .prepare_drop_table(table_id)
            .await
            .attach("operation=drop_table")
            .disclose()?;
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=drop_table")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CatalogAccess))
            .attach_with(|| {
                format!(
                    "operation=drop_table, phase=wait_mandatory_completion, table_id={table_id}"
                )
            })
            .disclose()
    }

    /// Run one online catalog checkpoint.
    ///
    /// This mutating maintenance operation uses normal healthy-runtime
    /// admission and requires the session to be idle. A successful publish also
    /// refreshes internal catalog-safe redo retention progress for future
    /// truncation planning.
    #[inline]
    pub async fn checkpoint_catalog(&mut self) -> Result<()> {
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=checkpoint_catalog")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::global(operation);
        let engine = scope.engine();
        let catalog_scope = CatalogCheckpointScope::acquire(engine.catalog_guard()).await;
        let redo_scope = RedoRetentionScope::acquire(engine.trx_sys.clone()).await;
        engine.poisoner.ensure_healthy().disclose()?;
        let prepared = prepare_catalog_checkpoint_operation(catalog_scope, redo_scope, scope);
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=checkpoint_catalog")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CatalogAccess))
            .attach("operation=checkpoint_catalog, phase=wait_mandatory_completion")
            .map(|_| ())
            .disclose()
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
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=checkpoint_catalog_and_truncate_redo_log")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::global(operation);
        let engine = scope.engine();
        let catalog_scope = CatalogCheckpointScope::acquire(engine.catalog_guard()).await;
        let redo_scope = RedoRetentionScope::acquire(engine.trx_sys.clone()).await;
        engine.poisoner.ensure_healthy().disclose()?;
        let prepared = prepare_catalog_redo_maintenance_operation(catalog_scope, redo_scope, scope);
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=checkpoint_catalog_and_truncate_redo_log")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CatalogAccess))
            .attach(
                "operation=checkpoint_catalog_and_truncate_redo_log, phase=wait_mandatory_completion",
            )
            .disclose()
    }

    /// Physically remove recovery-obsolete sealed redo prefix files.
    ///
    /// The operation first advances the durable first-retained redo marker when
    /// the current retention plan has eligible candidates, then unlinks present
    /// redo files below the final marker. Non-`NotFound` unlink failures are
    /// summarized in the returned outcome and can be retried by a later call.
    #[inline]
    pub async fn truncate_redo_log(&mut self) -> Result<RedoTruncationOutcome> {
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=truncate_redo_log")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::global(operation);
        let engine = scope.engine();
        let catalog_scope = CatalogCheckpointScope::acquire(engine.catalog_guard()).await;
        let redo_scope = RedoRetentionScope::acquire(engine.trx_sys.clone()).await;
        engine.poisoner.ensure_healthy().disclose()?;
        let prepared = prepare_redo_truncation_operation(catalog_scope, redo_scope, scope);
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=truncate_redo_log")
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::RedoLogAccess))
            .attach("operation=truncate_redo_log, phase=wait_mandatory_completion")
            .disclose()
    }

    /// Return a monotonic transaction-system statistics snapshot.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is still running. It is not available after engine
    /// shutdown, session close, or registry removal. Callers can compare
    /// snapshots to compute deltas.
    #[inline]
    pub fn transaction_system_stats(&self) -> Result<TransactionSystemStats> {
        let session = self
            .pin_inspection()
            .attach("operation=query_transaction_system_stats")
            .disclose()?;
        let engine = &session.runtime;
        Ok(transaction_system_stats_snapshot(
            engine.trx_sys.trx_sys_stats(),
        ))
    }

    /// Return a monotonic shared-storage IO statistics snapshot.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is still running. It is not available after engine
    /// shutdown, session close, or registry removal. Callers can compare
    /// snapshots to compute deltas.
    #[inline]
    pub fn storage_io_stats(&self) -> Result<StorageIoStats> {
        let session = self
            .pin_inspection()
            .attach("operation=query_storage_io_stats")
            .disclose()?;
        let engine = &session.runtime;
        Ok(storage_io_stats_snapshot(
            engine.table_fs.io_backend_stats(),
            engine.table_fs.storage_service_stats(),
        ))
    }

    /// Return point-in-time buffer-pool capacity, allocation, and counters.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is still running. It is not available after engine
    /// shutdown, session close, or registry removal. Counters are monotonic
    /// snapshots and callers can compare snapshots to compute deltas.
    #[inline]
    pub fn buffer_pool_stats(&self) -> Result<BufferPoolStats> {
        let session = self
            .pin_inspection()
            .attach("operation=query_buffer_pool_stats")
            .disclose()?;
        let engine = &session.runtime;
        Ok(BufferPoolStats {
            meta: buffer_pool_runtime_stats_snapshot(
                engine.pools.meta.capacity(),
                engine.pools.meta.allocated(),
                engine.pools.meta.stats(),
            ),
            mem: buffer_pool_runtime_stats_snapshot(
                engine.pools.mem.capacity(),
                engine.pools.mem.allocated(),
                engine.pools.mem.stats(),
            ),
            index: buffer_pool_runtime_stats_snapshot(
                engine.pools.index.capacity(),
                engine.pools.index.allocated(),
                engine.pools.index.stats(),
            ),
            disk: buffer_pool_runtime_stats_snapshot(
                engine.pools.disk.capacity(),
                engine.pools.disk.allocated(),
                engine.pools.disk.stats(),
            ),
        })
    }

    /// Return mandatory-runtime task and timing statistics by fixed task class.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is still running. It is not available after engine
    /// shutdown, session close, or registry removal. Monotonic fields and
    /// current active counts are independently sampled.
    #[inline]
    pub fn mandatory_runtime_stats(&self) -> Result<MandatoryRuntimeStats> {
        let session = self
            .pin_inspection()
            .attach("operation=query_mandatory_runtime_stats")
            .disclose()?;
        Ok(session.runtime.mandatory_runtime.stats())
    }

    /// Return cumulative logical-lock work and current physical-state statistics.
    ///
    /// This read-only diagnostic remains observable after storage poison while
    /// the engine lifecycle is running. Owner-local counters are aggregated
    /// when a session's final family authority closes.
    #[inline]
    pub fn logical_lock_stats(&self) -> Result<LogicalLockStats> {
        let session = self
            .pin_inspection()
            .attach("operation=query_logical_lock_stats")
            .disclose()?;
        Ok(session.runtime.lock_manager().stats())
    }

    /// Freeze a row-page prefix or report the existing table-owned batch.
    #[inline]
    pub async fn freeze_table(
        &mut self,
        table_id: TableID,
        max_rows: usize,
    ) -> Result<FreezeOutcome> {
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=freeze_table")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::table(operation, table_id)
            .await
            .attach_with(|| format!("operation=freeze_table, table_id={table_id}"))
            .disclose()?;
        let table = scope
            .resolve_user_table(table_id)
            .await
            .attach_with(|| format!("operation=freeze_table, table_id={table_id}"))
            .disclose()?;
        scope.engine().poisoner.ensure_healthy().disclose()?;
        let prepared = match prepare_freeze_table_operation(scope, table, max_rows) {
            Ok(prepared) => prepared,
            Err(outcome) => return Ok(outcome),
        };
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach_with(|| format!("operation=freeze_table, table_id={table_id}"))
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::TableAccess))
            .attach_with(|| {
                format!(
                    "operation=freeze_table, phase=wait_mandatory_completion, table_id={table_id}, max_rows={max_rows}"
                )
            })
            .disclose()
    }

    /// Persist eligible state using the table-owned canonical frozen batch.
    #[inline]
    pub async fn checkpoint_table(&mut self, table_id: TableID) -> Result<CheckpointOutcome> {
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=checkpoint_table")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::table(operation, table_id)
            .await
            .attach_with(|| format!("operation=checkpoint_table, table_id={table_id}"))
            .disclose()?;
        let table = scope
            .resolve_user_table(table_id)
            .await
            .attach_with(|| format!("operation=checkpoint_table, table_id={table_id}"))
            .disclose()?;
        scope.engine().poisoner.ensure_healthy().disclose()?;
        let prepared = match prepare_checkpoint_table_operation(scope, table) {
            Ok(prepared) => prepared,
            Err(outcome) => return Ok(outcome),
        };
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach_with(|| format!("operation=checkpoint_table, table_id={table_id}"))
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CheckpointExecution))
            .attach_with(|| {
                format!(
                    "operation=checkpoint_table, phase=wait_mandatory_completion, table_id={table_id}"
                )
            })
            .disclose()
    }

    /// Wait until retry may be useful for one self-identifying checkpoint delay.
    ///
    /// Completion means the observed predicate is satisfied or obsolete; a
    /// later checkpoint attempt can still encounter a different delay.
    pub async fn wait_for_checkpoint_retry(&mut self, reason: CheckpointDelayReason) -> Result<()> {
        let session = self
            .pin_observer()
            .attach("operation=wait_for_checkpoint_retry")
            .disclose()?;
        let table_id = match reason {
            CheckpointDelayReason::ActiveRoot { table_id, .. }
            | CheckpointDelayReason::FrozenPageCutoff { table_id, .. } => table_id,
        };
        loop {
            let Some(table) = session.runtime.catalog().current_live_user_table(table_id) else {
                return Ok(());
            };
            if table.check_foreground_live().is_err() {
                return Ok(());
            }
            session.runtime.state().cache_user_table(&table);
            let observation = table
                .checkpoint_retry_observation(&session, reason)
                .await
                .attach_with(|| format!("operation=wait_for_checkpoint_retry, table_id={table_id}"))
                .disclose()?;
            // The detached waiter owns only event-listener state.
            drop(table);
            match observation {
                CheckpointRetryObservation::Ready => return Ok(()),
                CheckpointRetryObservation::Wait(waiter) => waiter.wait().await,
            }
        }
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
            let outcome = self.checkpoint_table(table_id).await?;
            match outcome {
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
        let session = self
            .pin_observer()
            .attach("operation=wait_for_gc_horizon")
            .disclose()?;
        wait_for_maintenance_boundary(&session, ts, MaintenanceBoundary::GcHorizon)
            .await
            .disclose()
    }

    /// Wait for completed purge-horizon-cycle progress to become strictly newer.
    ///
    /// The returned boundary is published only after eligible undo/index,
    /// retired-page, retained-root, and coalesced cleanup work completes.
    pub async fn wait_for_purge_completion_after(&self, ts: TrxID) -> Result<TrxID> {
        let session = self
            .pin_observer()
            .attach("operation=wait_for_purge_completion")
            .disclose()?;
        wait_for_maintenance_boundary(&session, ts, MaintenanceBoundary::PurgeCompletion)
            .await
            .disclose()
    }

    /// Returns total number of hot row pages for an existing user table.
    #[inline]
    pub async fn total_row_pages(&mut self, table_id: TableID) -> Result<usize> {
        let mut session = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=count_table_row_pages")
            .disclose()?;
        let table = session
            .read_table(table_id)
            .await
            .attach("operation=count_table_row_pages")
            .disclose()?;
        #[cfg(test)]
        tests::run_test_total_row_pages_after_runtime_resolution_hook().await;
        table
            .table()
            .total_row_pages(table.pool_guards())
            .await
            .attach_with(|| format!("operation=count_table_row_pages, table_id={table_id}"))
            .disclose()
    }

    /// Full-scan cleanup for an existing user table's secondary MemIndex entries.
    ///
    /// The outcome always includes completed cleanup accounting. When requested
    /// live-entry removal is unsafe against the active snapshot horizon, delete
    /// overlays are still processed and `live_delay` describes when to retry.
    #[inline]
    pub async fn cleanup_secondary_mem_indexes(
        &mut self,
        table_id: TableID,
        clean_live_entries: bool,
    ) -> Result<MemIndexCleanupOutcome> {
        let operation = self
            .pin_operation(SessionOperationKind::Maintenance)
            .attach("operation=cleanup_secondary_mem_indexes")
            .disclose()?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let scope = PreparedMaintenanceScope::table(operation, table_id)
            .await
            .attach("operation=cleanup_secondary_mem_indexes")
            .disclose()?;
        let table = scope
            .resolve_user_table(table_id)
            .await
            .attach_with(|| format!("operation=cleanup_secondary_mem_indexes, table_id={table_id}"))
            .disclose()?;
        scope.engine().poisoner.ensure_healthy().disclose()?;
        let observer = mandatory_runtime
            .submit(prepare_mem_index_cleanup_operation(
                scope,
                table,
                clean_live_entries,
            ))
            .await
            .attach_with(|| format!("operation=cleanup_secondary_mem_indexes, table_id={table_id}"))
            .disclose()?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::IndexAccess))
            .attach_with(|| {
                format!(
                    "operation=cleanup_secondary_mem_indexes, phase=wait_mandatory_completion, table_id={table_id}, clean_live_entries={clean_live_entries}"
                )
            })
            .disclose()
    }

    /// Acquires an explicit session-lifetime table lock.
    #[inline]
    pub async fn lock_table(&mut self, table_id: TableID, mode: TableLockMode) -> Result<()> {
        let mode = LockMode::from(mode);
        let mut session = self
            .pin_operation(SessionOperationKind::SessionExplicitLock)
            .attach("operation=lock_explicit_table")
            .disclose()?;
        session
            .lock_table(table_id, mode)
            .await
            .attach_with(|| format!("operation=lock_explicit_table, table_id={table_id}"))
            .disclose()
    }

    /// Releases an explicit session-lifetime table lock when no transaction is active.
    #[inline]
    pub fn unlock_table(&mut self, table_id: TableID) -> Result<()> {
        let mut session = self
            .pin_operation(SessionOperationKind::SessionExplicitLock)
            .attach("operation=unlock_explicit_table")
            .disclose()?;
        session
            .unlock_table(table_id)
            .attach_with(|| format!("operation=unlock_explicit_table, table_id={table_id}"))
            .disclose()
    }
}

impl Drop for Session {
    #[inline]
    fn drop(&mut self) {
        if self.closed.get() {
            return;
        }
        if let Some(runtime) = self.session.upgrade_for_terminal() {
            let remove_from_registry = runtime.state().abandon();
            runtime.remove_if_requested(remove_from_registry);
        }
    }
}

/// One strong runtime/session pin for observer or inspection work.
///
/// The creating `Session` method establishes whether normal healthy-runtime or
/// lifecycle-only inspection admission applies.
pub(crate) struct SessionObserverPin {
    /// Exact state and engine capabilities retained for this observation.
    pub(crate) runtime: SessionRuntime,
}

impl Drop for SessionObserverPin {
    #[inline]
    fn drop(&mut self) {
        let remove_from_registry = self.runtime.state().release_observer();
        self.runtime.remove_if_requested(remove_from_registry);
    }
}

impl SessionRuntimeAccess for SessionObserverPin {
    #[inline]
    fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }
}

/// Non-cloneable foreground authority for one stable session operation.
pub(crate) struct SessionOperationPin {
    /// Exact state and engine capabilities retained while the operation blocks shutdown.
    pub(crate) runtime: SessionRuntime,
    /// Stable entry shared with transaction, cleanup, and terminal owners.
    entry: Arc<SessionOperationEntry>,
    /// The one boxed family authority taken from the idle session.
    authority: Option<Box<FamilyLockAuthority>>,
    /// Exact DDL or maintenance scope owned by this operation.
    curr_scope: Option<LockScopeState>,
    /// Whether drop must publish the foreground release edge.
    armed: bool,
}

impl SessionOperationPin {
    /// Returns this operation's exact stable key.
    #[inline]
    pub(crate) fn key(&self) -> SessionOperationKey {
        self.entry.key()
    }

    /// Returns this operation's stable purpose.
    #[inline]
    pub(crate) fn kind(&self) -> SessionOperationKind {
        self.entry.kind()
    }

    /// Returns the exact logical-lock owner for DDL or maintenance work.
    #[inline]
    pub(crate) fn operation_lock_owner(&self) -> LockOwner {
        assert!(
            matches!(
                self.kind(),
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance
            ),
            "operation lock owner requires DDL or maintenance authority: key={}, kind={}",
            self.key(),
            self.kind().label()
        );
        LockOwner::operation(self.key())
    }

    #[inline]
    fn operation_lock_parts(&mut self) -> (&EngineCore, &mut FamilyLockState, &mut LockScopeState) {
        let key = self.key();
        let expected_owner = self.operation_lock_owner();
        let engine = self.runtime.core();
        let authority = self
            .authority
            .as_deref_mut()
            .unwrap_or_else(|| panic!("operation must retain family authority: key={key}"));
        let curr_scope = self.curr_scope.as_mut().unwrap_or_else(|| {
            panic!("DDL or maintenance operation must retain curr_scope: key={key}")
        });
        assert!(
            curr_scope.owner() == expected_owner,
            "operation scope identity mismatch: key={key}, expected_owner={expected_owner}, actual_owner={}",
            curr_scope.owner()
        );
        (engine, authority.family_mut(), curr_scope)
    }

    #[inline]
    fn session_lock_parts(&mut self) -> (&EngineCore, &mut FamilyLockState, &mut LockScopeState) {
        let key = self.key();
        let engine = self.runtime.core();
        let authority = self.authority.as_deref_mut().unwrap_or_else(|| {
            panic!("explicit lock operation must retain family authority: key={key}")
        });
        let (family, session_scope) = authority.parts();
        (engine, family, session_scope)
    }

    #[inline]
    async fn acquire_ddl_create(
        &mut self,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh = FreshClaimsGuard::<16>::new(
            family,
            curr_scope,
            engine.lock_manager(),
            &engine.poisoner,
        );
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Exclusive)
            .await?;
        for &catalog_table_id in catalog_targets {
            fresh
                .acquire(
                    LockResource::TableMetadata(catalog_table_id),
                    LockMode::Shared,
                )
                .await?;
        }
        for &catalog_table_id in catalog_targets {
            fresh
                .acquire(
                    LockResource::TableData(catalog_table_id),
                    LockMode::IntentExclusive,
                )
                .await?;
        }
        fresh.disarm();
        Ok(())
    }

    #[inline]
    async fn acquire_ddl_existing(
        &mut self,
        table_id: TableID,
        catalog_targets: &[TableID],
    ) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh = FreshClaimsGuard::<16>::new(
            family,
            curr_scope,
            engine.lock_manager(),
            &engine.poisoner,
        );
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Exclusive)
            .await?;
        for &catalog_table_id in catalog_targets {
            fresh
                .acquire(
                    LockResource::TableMetadata(catalog_table_id),
                    LockMode::Shared,
                )
                .await?;
        }
        fresh
            .acquire(LockResource::TableData(table_id), LockMode::Exclusive)
            .await?;
        for &catalog_table_id in catalog_targets {
            fresh
                .acquire(
                    LockResource::TableData(catalog_table_id),
                    LockMode::IntentExclusive,
                )
                .await?;
        }
        fresh.disarm();
        Ok(())
    }

    #[inline]
    async fn acquire_maintenance_table(&mut self, table_id: TableID) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh =
            FreshClaimsGuard::<2>::new(family, curr_scope, engine.lock_manager(), &engine.poisoner);
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await?;
        fresh
            .acquire(LockResource::TableData(table_id), LockMode::IntentShared)
            .await?;
        fresh.disarm();
        Ok(())
    }

    /// Acquires maintenance read admission and returns a lifetime-bound table.
    async fn read_table(&mut self, table_id: TableID) -> OperationOrFatalResult<SessionTable<'_>> {
        assert!(
            self.kind() == SessionOperationKind::Maintenance,
            "session table requires maintenance authority: key={}, kind={}",
            self.key(),
            self.kind().label()
        );
        self.acquire_maintenance_table(table_id).await?;
        let table = self.resolve_user_table(table_id).await?;
        Ok(SessionTable {
            table,
            session: self,
        })
    }

    #[inline]
    fn reject_table_ddl_explicit_session_lock(&self, table_id: TableID) -> OperationResult<()> {
        let authority = self.authority.as_deref().unwrap_or_else(|| {
            panic!(
                "DDL operation must retain family authority: key={}",
                self.key()
            )
        });
        authority
            .family()
            .reject_table_ddl_explicit_session_lock(table_id, self.operation_lock_owner())
    }

    /// Borrows the exact session's pool-guard roots.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        self.runtime.pool_guards()
    }

    /// Consume voluntary authority at the exact mandatory ownership handoff.
    ///
    /// The lifecycle slot remains `Active` with this same entry; only the
    /// entry's owner label changes from voluntary to mandatory. No later
    /// operation can replace that active identity before terminal publication.
    #[inline]
    pub(crate) fn into_mandatory(mut self) -> MandatoryOperationGuard {
        self.runtime.state().accept_mandatory(&self.entry);
        self.armed = false;
        MandatoryOperationGuard {
            runtime: self.runtime.clone(),
            entry: Arc::clone(&self.entry),
            authority: self.authority.take(),
            curr_scope: self.curr_scope.take(),
            armed: true,
        }
    }

    /// Prepare CREATE TABLE while consuming this foreground operation.
    async fn prepare_create_table(
        self,
        validated: ValidatedCreateTable,
    ) -> OperationOrFatalResult<PreparedCreateTable> {
        let table_id = self.runtime.catalog().next_table_id();
        let plan = validated.into_plan(table_id);
        let scope = PreparedDdlScope::create(self, table_id, create_table_catalog_write_targets())
            .await
            .attach_with(|| format!("prepare CREATE TABLE locks: table_id={table_id}"))?;
        Ok(PreparedCreateTable::new(scope, plan))
    }

    /// Prepare DROP TABLE while consuming this foreground operation.
    async fn prepare_drop_table(
        self,
        table_id: TableID,
    ) -> OperationOrFatalResult<PreparedDropTable> {
        self.reject_table_ddl_explicit_session_lock(table_id)
            .attach("prepare DROP TABLE explicit-session-lock check")?;
        let scope =
            PreparedDdlScope::drop_table(self, table_id, drop_table_catalog_write_targets())
                .await
                .attach_with(|| format!("prepare DROP TABLE locks: table_id={table_id}"))?;
        let table = scope
            .engine()
            .catalog()
            .current_live_user_table(table_id)
            .ok_or_else(|| {
                Report::new(OperationError::TableNotFound).attach(format!(
                    "drop table current-live lookup: table_id={table_id}"
                ))
            })?;
        Ok(PreparedDropTable::new(
            scope,
            DropTablePlan::new(table_id, table),
        ))
    }

    /// Resolve a live user table from authoritative current catalog state.
    #[inline]
    pub(crate) async fn resolve_user_table(
        &self,
        table_id: TableID,
    ) -> OperationResult<Arc<Table>> {
        let table = self
            .runtime
            .catalog()
            .validate_user_table_live(table_id)
            .await?;
        self.runtime.state().cache_user_table(&table);
        Ok(table)
    }

    /// Acquires an explicit session-lifetime table lock from this operation.
    #[inline]
    pub(crate) async fn lock_table(
        &mut self,
        table_id: TableID,
        mode: LockMode,
    ) -> OperationOrFatalResult<()> {
        let (engine, family, session_scope) = self.session_lock_parts();
        let mut fresh = FreshClaimsGuard::<2>::new(
            family,
            session_scope,
            engine.lock_manager(),
            &engine.poisoner,
        );
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await?;
        fresh
            .acquire(LockResource::TableData(table_id), mode)
            .await?;
        engine.catalog().validate_user_table_live(table_id).await?;
        fresh.disarm();
        Ok(())
    }

    /// Releases an explicit session-lifetime table lock from this operation.
    #[inline]
    pub(crate) fn unlock_table(&mut self, table_id: TableID) -> OperationResult<()> {
        let (engine, family, session_scope) = self.session_lock_parts();
        family.release(
            session_scope,
            engine.lock_manager(),
            LockResource::TableData(table_id),
        );
        family.release(
            session_scope,
            engine.lock_manager(),
            LockResource::TableMetadata(table_id),
        );
        Ok(())
    }
}

impl SessionRuntimeAccess for SessionOperationPin {
    #[inline]
    fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }
}

impl Drop for SessionOperationPin {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            self.armed = false;
            if self.authority.is_some()
                && let Some(mut curr_scope) = self.curr_scope.take()
            {
                let lock_manager = self.runtime.lock_manager();
                let authority = self.authority.as_deref_mut().unwrap();
                authority
                    .family_mut()
                    .close_scope(&mut curr_scope, lock_manager);
            }
            let (remove_from_registry, cleanup) = self.runtime.state().finish_foreground(
                self.key(),
                self.authority.take(),
                self.curr_scope.take(),
            );
            self.runtime.remove_if_requested(remove_from_registry);
            if let Some(trx_id) = cleanup {
                self.runtime.trx_sys.request_abandoned_trx_cleanup(
                    self.runtime.clone(),
                    self.key(),
                    trx_id,
                );
            }
        }
    }
}

/// Sole terminal authority for one accepted session operation.
///
/// `state` coordinates session-wide disposition, observation, and terminal
/// slot publication. `entry` is the exact operation retained by that slot.
/// The slot identity stays stable while this guard is armed, although close or
/// abandonment may still change lifecycle disposition or install listeners.
/// Nested private-transaction state can therefore move directly through
/// `entry` without locking the outer lifecycle.
pub(crate) struct MandatoryOperationGuard {
    runtime: SessionRuntime,
    /// Intentionally redundant with the `Arc` retained by `Active(entry)`.
    ///
    /// This direct reference is the guard's exact operation authority. It
    /// avoids lifecycle relookup and lets nested transaction state move through
    /// the stable entry without taking the outer lifecycle lock.
    entry: Arc<SessionOperationEntry>,
    authority: Option<Box<FamilyLockAuthority>>,
    curr_scope: Option<LockScopeState>,
    armed: bool,
}

impl MandatoryOperationGuard {
    /// Return the accepted operation key.
    #[inline]
    pub(crate) fn key(&self) -> SessionOperationKey {
        self.entry.key()
    }

    /// Starts one private transaction owned by accepted mandatory execution.
    ///
    /// Mandatory ownership keeps the active slot bound to `entry`, so private
    /// installation needs only the entry mutex rather than the lifecycle lock.
    #[inline]
    pub(crate) fn begin_private_trx(&mut self) -> LifecycleResult<PrivateTransaction> {
        self.reclaim_transaction_authority();
        self.entry.validate_private_transaction_begin()?;
        let authority = self.authority.take().unwrap_or_else(|| {
            panic!(
                "private transaction begin requires family authority: key={}",
                self.key()
            )
        });
        begin_private_transaction(&self.runtime, &self.entry, authority)
    }

    #[inline]
    fn reclaim_transaction_authority(&mut self) {
        if self.authority.is_none() && self.entry.inspect().trx_id.is_none() {
            self.authority = Some(self.entry.take_lock_authority_return());
        }
    }

    /// Verify that accepted execution settled every nested transaction.
    ///
    /// This assertion-bearing check must run only from `AcceptedExecution::execute`.
    #[inline]
    pub(crate) fn assert_finish_ready(&mut self) {
        self.reclaim_transaction_authority();
        self.entry.assert_mandatory_finish_ready();
        assert!(
            self.authority.is_some(),
            "mandatory completion must reclaim family authority: key={}",
            self.key()
        );
    }

    /// Publish normal terminal state after transferred resources are released.
    #[inline]
    pub(crate) fn finish(&mut self) {
        if !self.armed {
            return;
        }
        self.reclaim_transaction_authority();
        let mut authority = self.authority.take().unwrap_or_else(|| {
            panic!(
                "mandatory completion requires family authority: key={}",
                self.key()
            )
        });
        if let Some(mut curr_scope) = self.curr_scope.take() {
            let lock_manager = self.runtime.lock_manager();
            authority
                .family_mut()
                .close_scope(&mut curr_scope, lock_manager);
        }
        authority.assert_idle();
        let remove_from_registry = self
            .runtime
            .state()
            .finish_mandatory(&self.entry, authority);
        self.runtime.remove_if_requested(remove_from_registry);
        self.armed = false;
    }

    /// Publish retained fatal state after domain-specific panic handling.
    #[inline]
    pub(crate) fn fail_retained(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.entry
            .retain_failed_operation_locks(self.authority.take(), self.curr_scope.take());
        self.runtime.state().fail_mandatory_retained(&self.entry);
    }
}

impl SessionRuntimeAccess for MandatoryOperationGuard {
    #[inline]
    fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }
}

impl Drop for MandatoryOperationGuard {
    #[inline]
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.entry
            .retain_failed_operation_locks(self.authority.take(), self.curr_scope.take());
        self.runtime.state().fail_mandatory_retained(&self.entry);
        let report = Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "mandatory operation authority dropped unexpectedly: operation_key={}",
            self.key()
        ));
        self.runtime.poisoner.poison(report);
    }
}

/// One session-local ownership class found by a shutdown probe.
pub(crate) enum SessionShutdownBlocker {
    /// One stable effectful operation remains active.
    Operation {
        /// Current operation ownership class.
        state: Option<SessionOperationState>,
        /// Typed read-snapshot phase when the active entry is a snapshot.
        snapshot_phase: Option<ReadSnapshotPhase>,
        /// Exact claimable transaction cleanup hint, when one exists.
        ///
        /// The tuple locates the stable outer operation entry first and
        /// identifies its currently attached transaction second.
        cleanup: Option<(SessionOperationKey, TrxID)>,
        /// Runtime captured from the registered state during the shutdown scan.
        runtime: Option<SessionRuntime>,
    },
    /// One or more standalone read-only observers remain active.
    Observer {
        /// Current nonzero observer count.
        count: usize,
    },
}

impl SessionShutdownBlocker {
    /// Returns the structured shutdown blocker label.
    #[inline]
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Operation { .. } => "operation",
            Self::Observer { .. } => "observer",
        }
    }

    /// Returns the active operation state, if this is an operation blocker.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "only shutdown diagnostics and tests inspect typed states"
        )
    )]
    pub(crate) const fn operation_state(&self) -> Option<SessionOperationState> {
        match self {
            Self::Operation { state, .. } => *state,
            Self::Observer { .. } => None,
        }
    }

    /// Returns the current active-operation state label.
    #[inline]
    pub(crate) const fn operation_state_label(&self) -> Option<&'static str> {
        match self {
            Self::Operation {
                state: Some(state), ..
            } => Some(state.label()),
            Self::Operation {
                snapshot_phase: Some(phase),
                ..
            } => Some(phase.label()),
            Self::Operation { .. } | Self::Observer { .. } => None,
        }
    }

    /// Returns the active observer count, or zero for an operation blocker.
    #[inline]
    pub(crate) const fn observer_count(&self) -> usize {
        match self {
            Self::Operation { .. } => 0,
            Self::Observer { count } => *count,
        }
    }

    /// Returns an exact claimable cleanup hint, when one exists.
    #[inline]
    pub(crate) fn into_cleanup(self) -> Option<SessionCleanupRequest> {
        match self {
            Self::Operation {
                cleanup: Some((operation_key, trx_id)),
                runtime: Some(runtime),
                ..
            } => Some(SessionCleanupRequest {
                runtime,
                operation_key,
                trx_id,
            }),
            Self::Observer { .. } => None,
            Self::Operation { .. } => None,
        }
    }

    #[inline]
    fn capture_runtime(&mut self, runtime: SessionRuntime) {
        if let Self::Operation {
            cleanup: Some(_),
            runtime: captured,
            ..
        } = self
        {
            *captured = Some(runtime);
        }
    }
}

/// Exact shutdown-discovered cleanup authority captured during registry scan.
pub(crate) struct SessionCleanupRequest {
    /// Exact registered session state and engine capabilities.
    pub(crate) runtime: SessionRuntime,
    /// Stable operation identity.
    pub(crate) operation_key: SessionOperationKey,
    /// Exact transaction identity.
    pub(crate) trx_id: TrxID,
}

/// One session-local blocker observed by blocking shutdown.
pub(crate) struct SessionShutdownWait {
    /// Listener installed before the selected blocker was re-read.
    pub(crate) listener: EventListener,
    /// Blocker classification captured by the lossless re-read.
    pub(crate) blocker: SessionShutdownBlocker,
}

/// Engine-owned registry for strong session state.
pub(crate) struct SessionRegistry {
    entries: FastDashMap<SessionID, Arc<SessionState>>,
}

impl SessionRegistry {
    /// Create an empty session registry.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            entries: FastDashMap::default(),
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
        core: Arc<EngineCore>,
        admission: Arc<SessionAdmission>,
        id: SessionID,
    ) -> Session {
        let state = Arc::new(SessionState::new(core, admission, id));
        let session = Session::new(WeakSessionRef::new(&state), id);
        self.insert(state);
        session
    }

    /// Resolve one exact operation through the registry for legacy test setup.
    #[cfg(test)]
    #[inline]
    pub(crate) fn try_resolve_operation(
        &self,
        key: SessionOperationKey,
    ) -> Option<(Arc<SessionOperationEntry>, Arc<SessionState>)> {
        let session = self.session_state(key.session_id())?;
        let entry = session.resolve_operation(key)?;
        Some((entry, session))
    }

    /// Returns the first active session operation without installing a listener.
    ///
    /// The DashMap iterator retains a shard read guard while each session takes
    /// its lifecycle and entry mutexes. Registry mutation occurs only after
    /// callers release those inner guards, so this short probe cannot form a
    /// lock cycle.
    #[inline]
    pub(crate) fn first_shutdown_blocker(&self) -> Option<SessionShutdownBlocker> {
        self.entries
            .iter()
            .find_map(|entry| entry.value().shutdown_blocker())
    }

    /// Returns the first active session operation with a lossless local listener.
    ///
    /// The returned listener and cleanup hint own no registry or state guards.
    #[inline]
    pub(crate) fn first_shutdown_wait(&self) -> Option<SessionShutdownWait> {
        self.entries
            .iter()
            .find_map(|entry| entry.value().shutdown_wait())
    }

    /// Remove idle and abandoned-idle sessions during engine shutdown.
    #[inline]
    pub(crate) fn shutdown_idle(&self) {
        let sessions = self
            .entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect::<Vec<_>>();
        for state in sessions {
            let remove_from_registry = state.shutdown_removal();
            if remove_from_registry {
                self.remove_exact(&SessionRuntime::new(state));
            }
        }
    }

    #[cfg(test)]
    #[inline]
    fn session_state(&self, id: SessionID) -> Option<Arc<SessionState>> {
        self.entries.get(&id).map(|entry| Arc::clone(entry.value()))
    }

    /// Remove only the pointer-identical registered state.
    #[inline]
    pub(crate) fn remove_exact(&self, runtime: &SessionRuntime) {
        let state = runtime.state();
        self.entries.remove_if(&state.id(), |_id, registered| {
            Arc::ptr_eq(registered, state)
        });
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
    /// Per-session roots for page-guard `Arc` clone/drop traffic.
    ///
    /// Keep this field before `core`: Rust drops fields in declaration order,
    /// so the session roots release their arena keepalives before the shared
    /// engine capabilities. Do not replace them with `EngineCore`'s canonical
    /// bundle; page lookup clones would again contend across all sessions.
    pool_guards: PoolGuards,
    core: Arc<EngineCore>,
    admission: Arc<SessionAdmission>,
    lifecycle: Mutex<SessionLifecycle>,
    last_cts: AtomicU64,
    table_cache: Mutex<FastHashMap<TableID, SessionTableCacheEntry>>,
}

impl SessionState {
    /// Create a new session state retaining one engine core and admission façade.
    #[inline]
    pub(crate) fn new(
        core: Arc<EngineCore>,
        admission: Arc<SessionAdmission>,
        id: SessionID,
    ) -> Self {
        // Four allocations and arena acquisitions are intentionally paid once
        // at session creation to shard millions of page-guard Arc operations.
        let pool_guards = core.pools.create_session_pool_guards();
        SessionState {
            id,
            pool_guards,
            core,
            admission,
            lifecycle: Mutex::new(SessionLifecycle {
                disposition: SessionDisposition::Open,
                slot: SessionOperationSlot::Idle,
                lock_authority: Some(FamilyLockAuthority::new(id)),
                observer_count: 0,
                next_operation_id: 1,
                change_ev: None,
                public_trx_cache: Some(Box::new(TrxInner::public_cached())),
            }),
            last_cts: AtomicU64::new(0),
            table_cache: Mutex::new(FastHashMap::default()),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    #[inline]
    fn acquire_observer(&self) -> LifecycleResult<()> {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.disposition == SessionDisposition::Open
            && !matches!(&lifecycle.slot, SessionOperationSlot::Closed)
        {
            assert!(
                lifecycle.observer_count < usize::MAX,
                "session observer count overflow: session_id={}",
                self.id
            );
            lifecycle.observer_count += 1;
            Ok(())
        } else {
            Err(self.unavailable_err(&lifecycle))
        }
    }

    #[inline]
    fn release_observer(&self) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        assert!(
            lifecycle.observer_count > 0,
            "session observer count underflow: session_id={}",
            self.id
        );
        lifecycle.observer_count -= 1;
        let remove_from_registry =
            lifecycle.observer_count == 0 && matches!(lifecycle.slot, SessionOperationSlot::Closed);
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        Self::notify_operation_change(notify);
        remove_from_registry
    }

    #[inline]
    fn active_operation_err(
        &self,
        disposition: SessionDisposition,
        entry: &ActiveSessionOperation,
    ) -> Report<LifecycleError> {
        let error = if entry.kind() == SessionOperationKind::PublicTransaction {
            LifecycleError::ExistingTransaction
        } else {
            LifecycleError::ExistingOperation
        };
        Report::new(error).attach(format!(
            "operation_key={}, kind={}, state={}, disposition={}, trx_id={}",
            entry.key(),
            entry.kind().label(),
            entry.state_label(),
            disposition.label(),
            entry
                .trx_id()
                .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string())
        ))
    }

    #[inline]
    fn unavailable_err(&self, lifecycle: &SessionLifecycle) -> Report<LifecycleError> {
        Report::new(LifecycleError::SessionUnavailable).attach(format!(
            "session_id={}, disposition={}, slot={}",
            self.id,
            lifecycle.disposition.label(),
            lifecycle.slot.label()
        ))
    }

    #[inline]
    fn next_operation_key(
        lifecycle: &SessionLifecycle,
        session_id: SessionID,
    ) -> SessionOperationKey {
        let operation_id = OperationID::new(lifecycle.next_operation_id);
        lifecycle
            .next_operation_id
            .checked_add(1)
            .unwrap_or_else(|| {
                panic!(
                    "session operation id exhausted before reservation: session_id={session_id}, operation_id={operation_id}"
                )
            });
        SessionOperationKey::new(session_id, operation_id)
    }

    #[inline]
    fn reserve_operation(
        &self,
        kind: SessionOperationKind,
    ) -> LifecycleResult<(Arc<SessionOperationEntry>, Box<FamilyLockAuthority>)> {
        assert!(
            kind != SessionOperationKind::PublicTransaction,
            "public transaction reservation requires transaction payload installation"
        );
        let mut lifecycle = self.lifecycle.lock();
        lifecycle
            .admit_idle()
            .attach_with(|| format!("session_id={}", self.id))?;
        let key = Self::next_operation_key(&lifecycle, self.id);
        let entry = SessionOperationEntry::new(key, kind);
        let authority = lifecycle.lock_authority.take().unwrap_or_else(|| {
            panic!(
                "idle session must retain family lock authority: session_id={}, operation_key={key}",
                self.id
            )
        });
        lifecycle.advance_operation_id();
        lifecycle.slot =
            SessionOperationSlot::Active(ActiveSessionOperation::Operation(Arc::clone(&entry)));
        Ok((entry, authority))
    }

    #[inline]
    fn reserve_read_snapshot(&self) -> LifecycleResult<(Arc<ReadSnapshotEntry>, TrxID)> {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle
            .admit_idle()
            .attach_with(|| format!("session_id={}", self.id))?;
        let key = Self::next_operation_key(&lifecycle, self.id);
        let authority = lifecycle.lock_authority.take().unwrap_or_else(|| {
            panic!(
                "idle session must retain family lock authority: session_id={}, operation_key={key}",
                self.id
            )
        });
        let registration = self.core.trx_sys.register_active_snapshot();
        let sts = registration.sts();
        let locks = ReadSnapshotLockOwner::new(authority, key);
        let core = ReadSnapshotBuildCore::new(registration, locks);
        let entry = ReadSnapshotEntry::new(key, sts, core);
        lifecycle.advance_operation_id();
        lifecycle.slot =
            SessionOperationSlot::Active(ActiveSessionOperation::ReadSnapshot(Arc::clone(&entry)));
        Ok((entry, sts))
    }

    /// Exclusively check out the exact building snapshot core.
    #[inline]
    pub(crate) fn checkout_read_snapshot_build(
        &self,
        key: SessionOperationKey,
    ) -> LifecycleResult<(Arc<ReadSnapshotEntry>, ReadSnapshotBuildCore)> {
        let lifecycle = self.lifecycle.lock();
        if lifecycle.disposition != SessionDisposition::Open || self.admission.shutdown_started() {
            return Err(
                Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                    "operation_key={key}, disposition={}, shutdown_started={}",
                    lifecycle.disposition.label(),
                    self.admission.shutdown_started()
                )),
            );
        }
        let entry = lifecycle
            .slot
            .active_read_snapshot()
            .filter(|entry| entry.key() == key)
            .cloned()
            .ok_or_else(|| {
                Report::new(LifecycleError::ReadSnapshotUnavailable)
                    .attach(format!("operation_key={key}, reason=identity_mismatch"))
            })?;
        let core = entry.take_build()?;
        Ok((entry, core))
    }

    /// Atomically publish a complete frozen snapshot when lifecycle admission still holds.
    #[inline]
    pub(crate) fn publish_read_snapshot_ready(
        &self,
        entry: &Arc<ReadSnapshotEntry>,
        payload: ReadSnapshotReadyPayload,
    ) -> bool {
        let lifecycle = self.lifecycle.lock();
        let exact = lifecycle
            .slot
            .active_read_snapshot()
            .is_some_and(|active| Arc::ptr_eq(active, entry));
        let healthy = self.core.poisoner.ensure_healthy().is_ok();
        let admit_ready = exact
            && lifecycle.disposition == SessionDisposition::Open
            && !self.admission.shutdown_started()
            && healthy;
        let published = entry.publish_ready(payload, admit_ready);
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        Self::notify_operation_change(notify);
        published
    }

    /// Count and clone the frozen core from one exact open ready snapshot.
    #[inline]
    pub(crate) fn checkout_read_snapshot_ready(
        &self,
        key: SessionOperationKey,
        facade_closed: &AtomicBool,
    ) -> LifecycleResult<(Arc<ReadSnapshotEntry>, Arc<FrozenReadSnapshotCore>)> {
        let lifecycle = self.lifecycle.lock();
        if lifecycle.disposition != SessionDisposition::Open
            || self.admission.shutdown_started()
            || facade_closed.load(Ordering::Acquire)
        {
            return Err(
                Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                    "operation_key={key}, disposition={}, shutdown_started={}, facade_closed={}",
                    lifecycle.disposition.label(),
                    self.admission.shutdown_started(),
                    facade_closed.load(Ordering::Acquire)
                )),
            );
        }
        let entry = lifecycle
            .slot
            .active_read_snapshot()
            .filter(|entry| entry.key() == key)
            .cloned()
            .ok_or_else(|| {
                Report::new(LifecycleError::ReadSnapshotUnavailable)
                    .attach(format!("operation_key={key}, reason=identity_mismatch"))
            })?;
        let read_core = entry.checkout_ready()?;
        if facade_closed.load(Ordering::Acquire) {
            // Close marked the facade after the first check but before this
            // lifecycle-serialized acceptance edge. Destroy the local pin
            // before returning the counted checkout to the entry.
            drop(read_core);
            entry.return_checkout();
            return Err(
                Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                    "operation_key={key}, reason=facade_closed_during_checkout"
                )),
            );
        }
        Ok((entry, read_core))
    }

    /// Admit a completed plan at the final edge before it escapes to the caller.
    ///
    /// Planning retains a counted checkout, but close, abandonment, shutdown,
    /// or poison may arrive after worklist capture starts. Holding the session
    /// lifecycle lock stabilizes the active entry, session disposition, and
    /// snapshot phase while this method decides whether publication won.
    #[inline]
    pub(crate) fn admit_read_snapshot_plan_publication(
        &self,
        entry: &Arc<ReadSnapshotEntry>,
        facade_closed: &AtomicBool,
    ) -> LifecycleOrFatalResult<()> {
        let lifecycle = self.lifecycle.lock();
        // Pointer identity rejects a stale checkout even if another snapshot
        // could otherwise present matching scalar identity.
        let exact = lifecycle
            .slot
            .active_read_snapshot()
            .is_some_and(|active| Arc::ptr_eq(active, entry));
        let disposition_open = lifecycle.disposition == SessionDisposition::Open;
        let phase = entry.phase();
        let ready = phase == ReadSnapshotPhase::Ready;
        let shutdown_started = self.admission.shutdown_started();
        let closed = facade_closed.load(Ordering::Acquire);
        if !exact || !disposition_open || !ready || shutdown_started || closed {
            return Err(Report::new(LifecycleError::ReadSnapshotUnavailable)
                .attach(format!(
                    "operation_key={}, exact_entry={exact}, disposition={}, snapshot_phase={}, shutdown_started={shutdown_started}, facade_closed={closed}",
                    entry.key(),
                    lifecycle.disposition.label(),
                    phase.label()
                ))
                .into());
        }
        // Preserve poison as a fatal result instead of collapsing it into
        // snapshot lifecycle unavailability.
        self.core
            .poisoner
            .ensure_healthy()
            .attach_with(|| format!("operation_key={}, phase=publish_plan", entry.key()))?;
        // Shutdown and facade close publish through atomics outside the session
        // lifecycle lock. Recheck them after the poison edge; the lock already
        // keeps the exact entry, disposition, and snapshot phase stable.
        if self.admission.shutdown_started() || facade_closed.load(Ordering::Acquire) {
            return Err(Report::new(LifecycleError::ReadSnapshotUnavailable)
                .attach(format!(
                    "operation_key={}, reason=terminal_edge_during_plan_publication, shutdown_started={}, facade_closed={}",
                    entry.key(),
                    self.admission.shutdown_started(),
                    facade_closed.load(Ordering::Acquire)
                ))
                .into());
        }
        if !entry.execution_healthy() {
            return Err(Report::new(LifecycleError::ReadSnapshotUnavailable)
                .attach(format!(
                    "operation_key={}, reason=execution_failed_during_plan_publication",
                    entry.key()
                ))
                .into());
        }
        Ok(())
    }

    /// Arm the session lifecycle listener while the exact snapshot remains active.
    #[inline]
    pub(crate) fn read_snapshot_terminal_listener(
        &self,
        key: SessionOperationKey,
    ) -> Option<EventListener> {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle
            .slot
            .active_read_snapshot()
            .filter(|entry| entry.key() == key)?;
        Some(lifecycle.change_listener())
    }

    /// Publish terminal after resource cleanup and restore or close family authority.
    #[inline]
    pub(crate) fn finish_read_snapshot_terminal(
        &self,
        entry: &Arc<ReadSnapshotEntry>,
        authority: Box<FamilyLockAuthority>,
    ) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        let exact = lifecycle
            .slot
            .active_read_snapshot()
            .is_some_and(|active| Arc::ptr_eq(active, entry));
        assert!(
            exact,
            "snapshot terminal claim lost exact active slot: key={}",
            entry.key()
        );
        entry.publish_terminal();
        let terminal = lifecycle.finalize_terminal(authority);
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        if let Some(mut authority) = terminal.close_authority {
            authority.close_session(self.core.lock_manager());
        }
        Self::notify_operation_change(notify);
        terminal.remove_from_registry
    }

    /// Recycle a cached public core or directly drop an ephemeral private core.
    #[inline]
    fn finish_trx_inner(&self, key: SessionOperationKey, trx_id: TrxID, mut inner: Box<TrxInner>) {
        if !inner.cache_on_terminal() {
            return;
        }
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.disposition != SessionDisposition::Open {
            return;
        }
        assert!(
            lifecycle.public_trx_cache.is_none(),
            "active public transaction must own the session's reusable core: \
             session_id={}, operation_key={key}, trx_id={trx_id}",
            self.id
        );
        inner.reset();
        lifecycle.public_trx_cache = Some(inner);
    }

    #[inline]
    fn request_close(self: &Arc<Self>) -> (SessionCloseDecision, bool) {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.disposition == SessionDisposition::Abandoned {
            return (
                SessionCloseDecision::Rejected(self.unavailable_err(&lifecycle)),
                false,
            );
        }
        match &lifecycle.slot {
            SessionOperationSlot::Idle => {}
            SessionOperationSlot::Closed => {
                return (SessionCloseDecision::Closed, lifecycle.observer_count == 0);
            }
            SessionOperationSlot::Active(entry) => {
                if let Some(snapshot_entry) = entry.read_snapshot().cloned() {
                    lifecycle.disposition = SessionDisposition::CloseRequested;
                    snapshot_entry.request_drain(ReadSnapshotDrainReason::SessionClose);
                    let listener = lifecycle.change_listener();
                    let claim =
                        snapshot_entry.claim_terminal(SessionRuntime::new(Arc::clone(self)));
                    drop(lifecycle);
                    if let Some(claim) = claim {
                        claim.cleanup();
                    }
                    return (SessionCloseDecision::Wait(listener), false);
                }
                let operation_entry = entry.operation_entry().unwrap_or_else(|| {
                    panic!("active operation lost typed entry: key={}", entry.key())
                });
                let snapshot = operation_entry.inspect();
                match snapshot.state {
                    SessionOperationState::Terminal => {}
                    SessionOperationState::CleanupReady
                    | SessionOperationState::Completing
                    | SessionOperationState::Mandatory(_) => {
                        lifecycle.disposition = SessionDisposition::CloseRequested;
                        let listener = lifecycle.change_listener();
                        return (SessionCloseDecision::Wait(listener), false);
                    }
                    SessionOperationState::FailedRetained => {
                        return (
                            SessionCloseDecision::Rejected(self.unavailable_err(&lifecycle)),
                            false,
                        );
                    }
                    SessionOperationState::Voluntary(_) => {
                        return (
                            SessionCloseDecision::Rejected(
                                self.active_operation_err(lifecycle.disposition, entry),
                            ),
                            false,
                        );
                    }
                }
            }
        }
        lifecycle.disposition = SessionDisposition::CloseRequested;
        lifecycle.slot = SessionOperationSlot::Closed;
        let remove_from_registry = lifecycle.observer_count == 0;
        let notify = lifecycle.change_ev.clone();
        let mut authority = lifecycle.lock_authority.take().unwrap_or_else(|| {
            panic!(
                "closing idle session must retain family authority: session_id={}",
                self.id
            )
        });
        drop(lifecycle);
        authority.close_session(self.core.lock_manager());
        Self::notify_operation_change(notify);
        (SessionCloseDecision::Closed, remove_from_registry)
    }

    #[inline]
    fn abandon(self: &Arc<Self>) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.disposition == SessionDisposition::Open {
            lifecycle.disposition = SessionDisposition::Abandoned;
        }
        let snapshot_entry = lifecycle.slot.active_read_snapshot().cloned();
        if let Some(entry) = &snapshot_entry {
            entry.request_drain(ReadSnapshotDrainReason::SessionAbandoned);
        }
        let close_authority = match lifecycle.slot {
            SessionOperationSlot::Closed => false,
            SessionOperationSlot::Idle => {
                lifecycle.slot = SessionOperationSlot::Closed;
                true
            }
            SessionOperationSlot::Active(_) => false,
        };
        let remove_from_registry =
            matches!(lifecycle.slot, SessionOperationSlot::Closed) && lifecycle.observer_count == 0;
        let notify = lifecycle.change_ev.clone();
        let authority = close_authority.then(|| {
            lifecycle.lock_authority.take().unwrap_or_else(|| {
                panic!(
                    "abandoning idle session must retain family authority: session_id={}",
                    self.id
                )
            })
        });
        let claim = snapshot_entry
            .and_then(|entry| entry.claim_terminal(SessionRuntime::new(Arc::clone(self))));
        drop(lifecycle);
        if let Some(mut authority) = authority {
            authority.close_session(self.core.lock_manager());
        }
        if let Some(claim) = claim {
            claim.cleanup();
        }
        Self::notify_operation_change(notify);
        remove_from_registry
    }

    #[inline]
    fn finish_foreground(
        &self,
        key: SessionOperationKey,
        authority: Option<Box<FamilyLockAuthority>>,
        curr_scope: Option<LockScopeState>,
    ) -> (bool, Option<TrxID>) {
        let mut lifecycle = self.lifecycle.lock();
        let Some(entry) = lifecycle.active_entry(key).cloned() else {
            return (false, None);
        };
        let release = entry.release_foreground();
        let terminal = if release.terminal {
            assert!(
                curr_scope.is_none(),
                "terminal foreground operation scope must close before publication: key={key}"
            );
            let authority = authority.unwrap_or_else(|| {
                panic!("terminal foreground operation must return family authority: key={key}")
            });
            lifecycle.finalize_terminal(authority)
        } else {
            entry.retain_failed_operation_locks(authority, curr_scope);
            SessionTerminalFinish::ACTIVE
        };
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        if let Some(mut authority) = terminal.close_authority {
            authority.close_session(self.core.lock_manager());
        }
        Self::notify_operation_change(notify);
        (terminal.remove_from_registry, release.cleanup)
    }

    /// Transfer the retained active entry to mandatory ownership.
    ///
    /// `entry` is pointer-identical to the lifecycle slot entry by reservation
    /// and pin construction. The lifecycle lock serializes the ownership edge
    /// and notification; it is not used to resolve the entry again.
    #[inline]
    fn accept_mandatory(&self, entry: &Arc<SessionOperationEntry>) {
        let lifecycle = self.lifecycle.lock();
        entry.accept_mandatory();
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        Self::notify_operation_change(notify);
    }

    /// Publish the retained mandatory entry and its outer slot atomically.
    ///
    /// The armed guard supplies the same entry still stored in `Active`. The
    /// lifecycle lock orders entry publication with concurrent close or
    /// abandonment before changing the slot to `Idle` or `Closed`.
    #[inline]
    fn finish_mandatory(
        &self,
        entry: &Arc<SessionOperationEntry>,
        authority: Box<FamilyLockAuthority>,
    ) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        entry.publish_mandatory_terminal();
        let terminal = lifecycle.finalize_terminal(authority);
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        if let Some(mut authority) = terminal.close_authority {
            authority.close_session(self.core.lock_manager());
        }
        Self::notify_operation_change(notify);
        terminal.remove_from_registry
    }

    /// Retain failure on the same stable entry while notifying lifecycle waiters.
    #[inline]
    fn fail_mandatory_retained(&self, entry: &Arc<SessionOperationEntry>) {
        let lifecycle = self.lifecycle.lock();
        entry.fail_mandatory_retained();
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        Self::notify_operation_change(notify);
    }

    /// Finish this session's exact transaction lifecycle after commit.
    #[inline]
    fn finish_trx_commit(
        &self,
        key: SessionOperationKey,
        trx_id: TrxID,
        cts: TrxID,
        authority: Box<FamilyLockAuthority>,
    ) -> bool {
        self.finish_trx(key, trx_id, authority, || {
            self.last_cts.store(cts.as_u64(), Ordering::SeqCst);
        })
    }

    /// Finish this session's active-transaction lifecycle after rollback.
    #[inline]
    fn finish_trx_rollback(
        &self,
        key: SessionOperationKey,
        trx_id: TrxID,
        authority: Box<FamilyLockAuthority>,
    ) -> bool {
        self.finish_trx(key, trx_id, authority, || {})
    }

    #[inline]
    fn finish_trx(
        &self,
        key: SessionOperationKey,
        trx_id: TrxID,
        authority: Box<FamilyLockAuthority>,
        on_finish: impl FnOnce(),
    ) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        let Some(entry) = lifecycle.active_entry(key).cloned() else {
            return false;
        };
        let Some(operation_terminal) = entry.finish_transaction(trx_id, authority) else {
            return false;
        };
        on_finish();
        if !operation_terminal {
            return false;
        }
        let authority = entry.take_lock_authority_return();
        let mut authority = authority;
        if let Some(mut curr_scope) = entry.take_retained_curr_scope() {
            authority
                .family_mut()
                .close_scope(&mut curr_scope, self.core.lock_manager());
        }
        let terminal = lifecycle.finalize_terminal(authority);
        let notify = lifecycle.change_ev.clone();
        drop(lifecycle);
        if let Some(mut authority) = terminal.close_authority {
            authority.close_session(self.core.lock_manager());
        }
        Self::notify_operation_change(notify);
        terminal.remove_from_registry
    }

    #[inline]
    fn notify_operation_change(event: Option<Arc<EventNotifyOnDrop>>) {
        if let Some(event) = event {
            event.notify(usize::MAX);
        }
    }

    /// Resolve an exact operation key directly on this pinned session state.
    #[inline]
    pub(crate) fn resolve_operation(
        &self,
        key: SessionOperationKey,
    ) -> Option<Arc<SessionOperationEntry>> {
        let lifecycle = self.lifecycle.lock();
        lifecycle.active_entry(key).cloned()
    }

    /// Abandon the exact public transaction handle when its identity matches.
    #[inline]
    pub(crate) fn abandon_trx_handle(&self, key: SessionOperationKey, trx_id: TrxID) -> bool {
        let lifecycle = self.lifecycle.lock();
        let abandoned = lifecycle
            .active_entry(key)
            .is_some_and(|entry| entry.abandon_transaction(trx_id));
        let notify = if abandoned {
            lifecycle.change_ev.clone()
        } else {
            None
        };
        drop(lifecycle);
        Self::notify_operation_change(notify);
        abandoned
    }

    /// Wakes a close or shutdown observer for this exact active key.
    #[inline]
    fn notify_operation_transition(&self, key: SessionOperationKey) {
        let lifecycle = self.lifecycle.lock();
        let notify = if lifecycle.active_entry(key).is_some() {
            lifecycle.change_ev.clone()
        } else {
            None
        };
        drop(lifecycle);
        Self::notify_operation_change(notify);
    }

    #[inline]
    fn shutdown_blocker(self: &Arc<Self>) -> Option<SessionShutdownBlocker> {
        let lifecycle = self.lifecycle.lock();
        let mut blocker = lifecycle.shutdown_blocker()?;
        let snapshot_entry = lifecycle.slot.active_read_snapshot().cloned();
        blocker.capture_runtime(SessionRuntime::new(Arc::clone(self)));
        let claim = snapshot_entry
            .and_then(|entry| entry.claim_terminal(SessionRuntime::new(Arc::clone(self))));
        drop(lifecycle);
        if let Some(claim) = claim {
            claim.cleanup_during_registry_scan();
        }
        Some(blocker)
    }

    #[inline]
    fn shutdown_wait(self: &Arc<Self>) -> Option<SessionShutdownWait> {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.shutdown_blocker()?;
        let listener = lifecycle.change_listener();
        let mut blocker = lifecycle
            .shutdown_blocker()
            .expect("session blocker cannot change while lifecycle lock is held");
        let snapshot_entry = lifecycle.slot.active_read_snapshot().cloned();
        blocker.capture_runtime(SessionRuntime::new(Arc::clone(self)));
        let claim = snapshot_entry
            .and_then(|entry| entry.claim_terminal(SessionRuntime::new(Arc::clone(self))));
        drop(lifecycle);
        if let Some(claim) = claim {
            claim.cleanup_during_registry_scan();
        }
        Some(SessionShutdownWait { listener, blocker })
    }

    #[inline]
    fn shutdown_removal(&self) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        let close_authority = match lifecycle.slot {
            SessionOperationSlot::Closed => false,
            SessionOperationSlot::Idle => {
                lifecycle.slot = SessionOperationSlot::Closed;
                true
            }
            SessionOperationSlot::Active(_) => false,
        };
        let remove_from_registry =
            matches!(lifecycle.slot, SessionOperationSlot::Closed) && lifecycle.observer_count == 0;
        let notify = lifecycle.change_ev.clone();
        let authority = close_authority.then(|| {
            lifecycle.lock_authority.take().unwrap_or_else(|| {
                panic!(
                    "shutdown of idle session must retain family authority: session_id={}",
                    self.id
                )
            })
        });
        drop(lifecycle);
        if let Some(mut authority) = authority {
            authority.close_session(self.core.lock_manager());
        }
        Self::notify_operation_change(notify);
        remove_from_registry
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
}

impl Drop for SessionState {
    #[inline]
    fn drop(&mut self) {
        let table_cache = self.table_cache.get_mut();
        for (table_id, entry) in table_cache.drain() {
            let Some(page_id) = entry.active_insert_page else {
                continue;
            };
            self.core
                .catalog()
                .return_session_insert_page(table_id, &entry.table, page_id);
        }
        if let Some(mut authority) = self.lifecycle.get_mut().lock_authority.take() {
            authority.close_session(self.core.lock_manager());
        }
    }
}

struct SessionLifecycle {
    disposition: SessionDisposition,
    slot: SessionOperationSlot,
    /// Present exactly while this session is idle.
    lock_authority: Option<Box<FamilyLockAuthority>>,
    observer_count: usize,
    next_operation_id: u64,
    change_ev: Option<Arc<EventNotifyOnDrop>>,
    /// Reusable public transaction core, checked out only by a public transaction.
    public_trx_cache: Option<Box<TrxInner>>,
}

impl SessionLifecycle {
    #[inline]
    fn admit_idle(&self) -> LifecycleResult<()> {
        if self.disposition != SessionDisposition::Open {
            return Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!("disposition={}", self.disposition.label())));
        }
        match &self.slot {
            SessionOperationSlot::Idle => Ok(()),
            SessionOperationSlot::Active(entry) => {
                let error = if entry.kind() == SessionOperationKind::PublicTransaction {
                    LifecycleError::ExistingTransaction
                } else {
                    LifecycleError::ExistingOperation
                };
                Err(Report::new(error).attach(format!(
                    "operation_key={}, kind={}, state={}, disposition={}, trx_id={}",
                    entry.key(),
                    entry.kind().label(),
                    entry.state_label(),
                    self.disposition.label(),
                    entry
                        .trx_id()
                        .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string())
                )))
            }
            SessionOperationSlot::Closed => Err(Report::new(LifecycleError::SessionUnavailable)
                .attach(format!(
                    "disposition={}, slot=closed",
                    self.disposition.label()
                ))),
        }
    }

    #[inline]
    fn advance_operation_id(&mut self) {
        self.next_operation_id = self
            .next_operation_id
            .checked_add(1)
            .expect("session operation id was checked before reservation");
    }

    #[inline]
    fn active_entry(&self, key: SessionOperationKey) -> Option<&Arc<SessionOperationEntry>> {
        self.slot.active_entry().filter(|entry| entry.key() == key)
    }

    #[inline]
    fn shutdown_blocker(&self) -> Option<SessionShutdownBlocker> {
        if let Some(operation) = self.slot.active_operation() {
            if let Some(entry) = operation.read_snapshot() {
                let phase = entry.phase();
                entry.request_drain(ReadSnapshotDrainReason::EngineShutdown);
                return Some(SessionShutdownBlocker::Operation {
                    state: None,
                    snapshot_phase: Some(phase),
                    cleanup: None,
                    runtime: None,
                });
            }
            let entry = operation.operation_entry().unwrap_or_else(|| {
                panic!(
                    "typed active operation is missing its operation entry: key={}",
                    operation.key()
                )
            });
            let state = entry.inspect().state;
            return Some(SessionShutdownBlocker::Operation {
                state: Some(state),
                snapshot_phase: None,
                cleanup: entry
                    .cleanup_candidate()
                    .map(|trx_id| (entry.key(), trx_id)),
                runtime: None,
            });
        }
        (self.observer_count != 0).then_some(SessionShutdownBlocker::Observer {
            count: self.observer_count,
        })
    }

    /// Publishes a terminal operation as idle for an open session or closed for
    /// a session whose close or abandonment is already pending.
    ///
    /// Returns the closed-session lock-release and registry-removal decisions.
    #[inline]
    fn finalize_terminal(&mut self, authority: Box<FamilyLockAuthority>) -> SessionTerminalFinish {
        authority.assert_idle();
        match self.disposition {
            SessionDisposition::Open => {
                assert!(
                    self.lock_authority.is_none(),
                    "terminal operation cannot replace idle family authority"
                );
                self.lock_authority = Some(authority);
                self.slot = SessionOperationSlot::Idle;
                SessionTerminalFinish::ACTIVE
            }
            SessionDisposition::CloseRequested | SessionDisposition::Abandoned => {
                self.slot = SessionOperationSlot::Closed;
                SessionTerminalFinish {
                    remove_from_registry: self.observer_count == 0,
                    close_authority: Some(authority),
                }
            }
        }
    }

    #[inline]
    fn change_listener(&mut self) -> EventListener {
        self.change_ev
            .get_or_insert_with(|| Arc::new(EventNotifyOnDrop::new()))
            .listen()
    }
}

struct SessionTerminalFinish {
    remove_from_registry: bool,
    close_authority: Option<Box<FamilyLockAuthority>>,
}

impl SessionTerminalFinish {
    const ACTIVE: Self = Self {
        remove_from_registry: false,
        close_authority: None,
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionDisposition {
    Open,
    CloseRequested,
    Abandoned,
}

impl SessionDisposition {
    #[inline]
    const fn label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::CloseRequested => "close_requested",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Typed pointer-stable entry stored in one active session slot.
enum ActiveSessionOperation {
    Operation(Arc<SessionOperationEntry>),
    ReadSnapshot(Arc<ReadSnapshotEntry>),
}

impl ActiveSessionOperation {
    #[inline]
    fn key(&self) -> SessionOperationKey {
        match self {
            Self::Operation(entry) => entry.key(),
            Self::ReadSnapshot(entry) => entry.key(),
        }
    }

    #[inline]
    fn kind(&self) -> SessionOperationKind {
        match self {
            Self::Operation(entry) => entry.kind(),
            Self::ReadSnapshot(_) => SessionOperationKind::ReadSnapshot,
        }
    }

    #[inline]
    fn state_label(&self) -> &'static str {
        match self {
            Self::Operation(entry) => entry.inspect().state.label(),
            Self::ReadSnapshot(entry) => entry.phase().label(),
        }
    }

    #[inline]
    fn trx_id(&self) -> Option<TrxID> {
        match self {
            Self::Operation(entry) => entry.inspect().trx_id,
            Self::ReadSnapshot(_) => None,
        }
    }

    #[inline]
    fn operation_entry(&self) -> Option<&Arc<SessionOperationEntry>> {
        match self {
            Self::Operation(entry) => Some(entry),
            Self::ReadSnapshot(_) => None,
        }
    }

    #[inline]
    fn read_snapshot(&self) -> Option<&Arc<ReadSnapshotEntry>> {
        match self {
            Self::ReadSnapshot(entry) => Some(entry),
            Self::Operation(_) => None,
        }
    }
}

/// Registry-visible ownership slot for one session's effectful operation.
enum SessionOperationSlot {
    Idle,
    /// Exact active entry, stable until its terminal lifecycle publication.
    Active(ActiveSessionOperation),
    Closed,
}

impl SessionOperationSlot {
    #[inline]
    fn active_operation(&self) -> Option<&ActiveSessionOperation> {
        match self {
            Self::Active(entry) => Some(entry),
            Self::Idle | Self::Closed => None,
        }
    }

    #[inline]
    fn active_entry(&self) -> Option<&Arc<SessionOperationEntry>> {
        self.active_operation()
            .and_then(ActiveSessionOperation::operation_entry)
    }

    #[inline]
    fn active_read_snapshot(&self) -> Option<&Arc<ReadSnapshotEntry>> {
        self.active_operation()
            .and_then(ActiveSessionOperation::read_snapshot)
    }

    #[inline]
    fn label(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active(_) => "active",
            Self::Closed => "closed",
        }
    }
}

enum SessionCloseDecision {
    Closed,
    Wait(EventListener),
    Rejected(Report<LifecycleError>),
}

/// Private transaction runtime attachment retained by checked-out transaction work.
///
/// This handle owns exact session runtime reachability for one
/// operation, terminal path, prepared commit handoff, or cleanup path. The
/// stable session operation remains the shutdown proof, and the public
/// transaction facade never stores this attachment.
pub(crate) struct TrxAttachment {
    /// Exact state and engine capabilities retained by this claim.
    runtime: SessionRuntime,
    /// Exact session-local operation key for terminal cleanup.
    operation_key: SessionOperationKey,
    /// Active transaction id used to avoid finishing a replaced session state.
    trx_id: TrxID,
}

impl TrxAttachment {
    /// Create a transaction runtime attachment without public handle ownership.
    #[inline]
    pub(crate) fn new(
        runtime: SessionRuntime,
        operation_key: SessionOperationKey,
        trx_id: TrxID,
    ) -> Self {
        assert!(
            runtime.state().id() == operation_key.session_id(),
            "transaction attachment session/key mismatch: session_id={}, operation_key={operation_key}",
            runtime.state().id()
        );
        Self {
            runtime,
            operation_key,
            trx_id,
        }
    }

    /// Returns immutable engine capabilities.
    #[inline]
    pub(crate) fn engine(&self) -> &EngineCore {
        self.runtime.core()
    }

    /// Returns the exact transaction identity carried by this attachment.
    #[inline]
    pub(crate) const fn trx_id(&self) -> TrxID {
        self.trx_id
    }

    /// Borrows the exact session's pool-guard roots.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        self.runtime.pool_guards()
    }

    /// Store a weak session-local table cache entry after successful resolution.
    #[inline]
    pub(crate) fn cache_user_table(&self, table: &Arc<Table>) {
        self.runtime.state().cache_user_table(table);
    }

    /// Remove and return the cached insert page for a table, if session state remains.
    #[inline]
    pub(crate) fn load_active_insert_page(&self, table_id: TableID) -> Option<VersionedPageID> {
        self.runtime.state().load_active_insert_page(table_id)
    }

    /// Cache the active insert page if session state remains.
    #[inline]
    pub(crate) fn save_active_insert_page(&self, table_id: TableID, page_id: VersionedPageID) {
        self.runtime
            .state()
            .save_active_insert_page(table_id, page_id);
    }

    /// Mark the owning session committed.
    #[inline]
    pub(crate) fn commit(
        &self,
        released: ReleasedTransactionLocks,
        cts: TrxID,
        inner: Box<TrxInner>,
    ) {
        let authority = released.into_authority(self.trx_id);
        self.runtime
            .state()
            .finish_trx_inner(self.operation_key, self.trx_id, inner);
        #[cfg(test)]
        tests::run_terminal_attachment_test_hook(
            self.trx_id,
            tests::TerminalAttachmentOutcome::Commit,
        );
        let remove_from_registry =
            self.runtime
                .state()
                .finish_trx_commit(self.operation_key, self.trx_id, cts, authority);
        self.runtime.remove_if_requested(remove_from_registry);
    }

    /// Mark the owning session rolled back.
    #[inline]
    pub(crate) fn rollback(&self, released: ReleasedTransactionLocks, inner: Box<TrxInner>) {
        let authority = released.into_authority(self.trx_id);
        self.runtime
            .state()
            .finish_trx_inner(self.operation_key, self.trx_id, inner);
        self.finish_rollback(authority);
    }

    /// Mark the owning session rolled back without recycling a failed core.
    #[inline]
    pub(crate) fn rollback_without_reuse(&self, released: ReleasedTransactionLocks) {
        let authority = released.into_authority(self.trx_id);
        self.finish_rollback(authority);
    }

    #[inline]
    fn finish_rollback(&self, authority: Box<FamilyLockAuthority>) {
        #[cfg(test)]
        tests::run_terminal_attachment_test_hook(
            self.trx_id,
            tests::TerminalAttachmentOutcome::Rollback,
        );
        let remove_from_registry =
            self.runtime
                .state()
                .finish_trx_rollback(self.operation_key, self.trx_id, authority);
        self.runtime.remove_if_requested(remove_from_registry);
    }

    /// Queue rollback cleanup for an abandoned transaction.
    #[inline]
    pub(crate) fn request_abandoned_cleanup(&self) {
        self.runtime.trx_sys.request_abandoned_trx_cleanup(
            self.runtime.clone(),
            self.operation_key,
            self.trx_id,
        );
    }

    /// Notifies close or shutdown only when this exact operation was armed.
    #[inline]
    pub(crate) fn notify_operation_transition(&self) {
        self.runtime
            .state()
            .notify_operation_transition(self.operation_key);
    }
}

/// Starts one private transaction under an existing DDL owner.
#[inline]
fn begin_private_transaction(
    runtime: &SessionRuntime,
    entry: &Arc<SessionOperationEntry>,
    authority: Box<FamilyLockAuthority>,
) -> LifecycleResult<PrivateTransaction> {
    let kind = entry.kind();
    assert!(
        kind == SessionOperationKind::Ddl,
        "private transaction requires DDL authority: key={}, kind={}",
        entry.key(),
        kind.label()
    );
    let inner = Box::new(TrxInner::private());
    runtime
        .trx_sys
        .begin_private_trx(runtime.clone(), entry, inner, authority)
}

async fn wait_for_maintenance_boundary(
    session: &SessionObserverPin,
    ts: TrxID,
    boundary: MaintenanceBoundary,
) -> LifecycleOrFatalResult<TrxID> {
    let trx_sys = &session.runtime.trx_sys;
    loop {
        session
            .runtime
            .poisoner
            .ensure_healthy()
            .map_err(LifecycleOrFatalError::from)
            .attach_with(|| {
                format!(
                    "maintenance progress wait observed engine poison: boundary={}, target_ts={ts}",
                    boundary.name()
                )
            })?;
        if session.runtime.state().admission.shutdown_started() {
            return Err(Report::new(LifecycleError::Shutdown)
                .attach(format!(
                    "maintenance progress wait observed engine shutdown: boundary={}, target_ts={ts}",
                    boundary.name()
                ))
                .into());
        }
        let observed = boundary.observed(session);
        if observed > ts {
            return Ok(observed);
        }

        trx_sys.request_purge_observation();
        let progress_listener = boundary.listener(session);
        let poison_listener = session.runtime.poisoner.listener();
        let shutdown_listener = session.runtime.state().admission.shutdown_listener();

        session
            .runtime
            .poisoner
            .ensure_healthy()
            .map_err(LifecycleOrFatalError::from)
            .attach_with(|| {
                format!(
                    "maintenance progress wait observed engine poison: boundary={}, target_ts={ts}",
                    boundary.name()
                )
            })?;
        if session.runtime.state().admission.shutdown_started() {
            return Err(Report::new(LifecycleError::Shutdown)
                .attach(format!(
                    "maintenance progress wait observed engine shutdown: boundary={}, target_ts={ts}",
                    boundary.name()
                ))
                .into());
        }
        let observed = boundary.observed(session);
        if observed > ts {
            return Ok(observed);
        }
        select_all(vec![progress_listener, poison_listener, shutdown_listener]).await;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::guard::PageGuard;
    use crate::buffer::{PoolRole, test_pool_guards_share_keepalive_root};
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::tests::{table1, table2, wait_for_dropped_table_floor};
    use crate::catalog::{
        CatalogTable, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
    };
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, Error, ErrorKind, FatalError, LifecycleError, RuntimeError,
    };
    use crate::io::install_storage_backend_test_hook;
    use crate::lock::tests::LockDebugEntryState;
    use crate::log::LogSync;
    use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
    use crate::stats::{
        BufferPoolCounters, BufferPoolRuntimeStats, MandatoryRuntimeStats, MandatoryTaskStats,
        TransactionSystemStats,
    };
    use crate::table::tests::{
        FailingFirstWriteHook, assert_freeze_created, has_lock_entry,
        lightweight_test_engine_config, lock_entry_count, maintenance_lock_owner,
    };
    use crate::trx::retention::{
        RedoTruncationBlocker, tests::install_redo_cleanup_before_unlink_hook,
    };
    use crate::trx::tests::{
        private_noop, private_transaction_inner_ptr, session_operation_entry_inner_ptr, trx_inner,
    };
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, SessionOperationSnapshot, TrxInner};
    use crate::value::{Val, ValKind};
    use futures::task::noop_waker;
    use std::cell::RefCell;
    use std::fs;
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, OnceLock, mpsc};
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    const TRUNCATE_TEST_LOG_BLOCK_SIZE: usize = 4096;
    const TRUNCATE_TEST_LOG_FILE_MAX_SIZE: usize =
        REDO_DEFAULT_DATA_START_OFFSET + 4 * TRUNCATE_TEST_LOG_BLOCK_SIZE;

    const _: fn() = || {
        fn assert_send<T: Send>() {}
        assert_send::<Session>();
    };

    macro_rules! assert_not_impl {
        ($ty:ty: $trait:path) => {
            const _: fn() = || {
                trait AmbiguousIfImpl<A> {
                    fn check() {}
                }
                impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
                struct Invalid;
                impl<T: ?Sized + $trait> AmbiguousIfImpl<Invalid> for T {}
                <$ty as AmbiguousIfImpl<_>>::check();
            };
        };
    }

    assert_not_impl!(Session: Sync);

    /// Terminal attachment transition observed after transaction-lock release.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum TerminalAttachmentOutcome {
        /// The attachment will finish the session with a commit timestamp.
        Commit,
        /// The attachment will finish the session with rollback-style cleanup.
        Rollback,
    }

    type TerminalAttachmentTestHook =
        Arc<dyn Fn(TrxID, TerminalAttachmentOutcome) + Send + Sync + 'static>;

    /// Guard that restores the previous terminal attachment hook on drop.
    pub(crate) struct TerminalAttachmentTestHookGuard {
        previous: Option<TerminalAttachmentTestHook>,
        _install_guard: parking_lot::MutexGuard<'static, ()>,
    }

    impl Drop for TerminalAttachmentTestHookGuard {
        #[inline]
        fn drop(&mut self) {
            *terminal_attachment_test_hook_slot().lock() = self.previous.take();
        }
    }

    type TotalRowPagesAfterRuntimeResolutionHook =
        Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + 'static>;

    thread_local! {
        static TEST_TOTAL_ROW_PAGES_AFTER_RUNTIME_RESOLUTION_HOOK:
            RefCell<Option<TotalRowPagesAfterRuntimeResolutionHook>> = RefCell::new(None);
    }

    /// Test-only extension interface for session test ext.
    pub(crate) trait SessionTestExt {
        /// Provides test-only access to `in_trx`.
        fn in_trx(&self) -> Result<bool>;
        /// Provides test-only access to `pool_guards`.
        fn pool_guards(&self) -> PoolGuards;
        /// Provides test-only access to `engine`.
        fn engine(&self) -> SessionRuntime;
        /// Provides test-only access to `last_cts`.
        fn last_cts(&self) -> TrxID;
        /// Provides test-only access to `load_active_insert_page`.
        fn load_active_insert_page(&mut self, table_id: TableID) -> Option<VersionedPageID>;
        /// Provides test-only access to `save_active_insert_page`.
        fn save_active_insert_page(&mut self, table_id: TableID, page_id: VersionedPageID);
    }

    impl SessionTestExt for Session {
        #[inline]
        fn in_trx(&self) -> Result<bool> {
            const OPERATION: &str = "test_inspect_transaction_state";
            let runtime = test_session_runtime(self)
                .attach_with(|| format!("operation={OPERATION}"))
                .disclose()?;
            inspect_session_in_trx(runtime.state())
                .attach_with(|| format!("operation={OPERATION}, session_id={}", self.id))
                .disclose()
        }

        #[inline]
        fn pool_guards(&self) -> PoolGuards {
            test_session_runtime(self)
                .expect("test session must be running")
                .pool_guards()
                .clone()
        }

        #[inline]
        fn engine(&self) -> SessionRuntime {
            test_session_runtime(self).expect("test session must be running")
        }

        #[inline]
        fn last_cts(&self) -> TrxID {
            let runtime = test_session_runtime(self).expect("test session must be running");
            TrxID::new(runtime.state().last_cts.load(Ordering::SeqCst))
        }

        #[inline]
        fn load_active_insert_page(&mut self, table_id: TableID) -> Option<VersionedPageID> {
            test_session_runtime(self)
                .expect("test session must be running")
                .state()
                .load_active_insert_page(table_id)
        }

        #[inline]
        fn save_active_insert_page(&mut self, table_id: TableID, page_id: VersionedPageID) {
            test_session_runtime(self)
                .expect("test session must be running")
                .state()
                .save_active_insert_page(table_id, page_id);
        }
    }

    /// Installs a serialized test-only terminal attachment observation hook.
    #[inline]
    pub(crate) fn install_terminal_attachment_test_hook(
        hook: TerminalAttachmentTestHook,
    ) -> TerminalAttachmentTestHookGuard {
        let install_guard = terminal_attachment_test_hook_install_lock().lock();
        let mut slot = terminal_attachment_test_hook_slot().lock();
        let previous = slot.replace(hook);
        TerminalAttachmentTestHookGuard {
            previous,
            _install_guard: install_guard,
        }
    }

    /// Runs terminal attachment test hook for tests.
    #[inline]
    pub(crate) fn run_terminal_attachment_test_hook(
        trx_id: TrxID,
        outcome: TerminalAttachmentOutcome,
    ) {
        let hook = terminal_attachment_test_hook_slot().lock().clone();
        if let Some(hook) = hook {
            hook(trx_id, outcome);
        }
    }

    /// Finishes a stale transaction directly for session-registry tests.
    #[inline]
    pub(crate) fn finish_trx_commit_for_test(
        registry: &SessionRegistry,
        session_id: SessionID,
        trx_id: TrxID,
        cts: TrxID,
    ) {
        let state = registry
            .session_state(session_id)
            .expect("test session must remain registered");
        let key = match &state.lifecycle.lock().slot {
            SessionOperationSlot::Active(entry) => entry.key(),
            SessionOperationSlot::Idle | SessionOperationSlot::Closed => {
                panic!("test transaction requires active operation slot")
            }
        };
        let remove_from_registry =
            state.finish_trx_commit(key, trx_id, cts, FamilyLockAuthority::new(session_id));
        if remove_from_registry {
            registry.entries.remove_if(&session_id, |_id, registered| {
                Arc::ptr_eq(registered, &state)
            });
        }
    }

    /// Wait asynchronously until one exact session no longer owns an operation.
    pub(crate) async fn wait_for_session_idle(registry: &SessionRegistry, session_id: SessionID) {
        loop {
            let listener = {
                let Some(state) = registry.session_state(session_id) else {
                    return;
                };
                let mut lifecycle = state.lifecycle.lock();
                match lifecycle.slot {
                    SessionOperationSlot::Active(_) => lifecycle.change_listener(),
                    SessionOperationSlot::Idle | SessionOperationSlot::Closed => return,
                }
            };
            listener.await;
        }
    }

    /// Wait until ordered commit payloads through `ts` reach purge coordination.
    pub(crate) async fn wait_for_purge_handoff(session: &Session, ts: TrxID) -> Result<()> {
        let session = session
            .pin_observer()
            .attach("operation=wait_for_purge_handoff")
            .disclose()?;
        let trx_sys = &session.runtime.trx_sys;
        loop {
            session.runtime.poisoner.ensure_healthy().disclose()?;
            if session.runtime.shutdown_started() {
                return Err(Report::new(LifecycleError::Shutdown)
                    .attach("completed-purge wait observed engine shutdown before ordered handoff")
                    .disclose());
            }
            if trx_sys.purge_handoff_cts() >= ts {
                return Ok(());
            }
            let handoff_listener = trx_sys.purge_handoff_listener();
            let poison_listener = session.runtime.poisoner.listener();
            let shutdown_listener = session.runtime.shutdown_listener();
            session.runtime.poisoner.ensure_healthy().disclose()?;
            if session.runtime.shutdown_started() {
                return Err(Report::new(LifecycleError::Shutdown)
                    .attach("completed-purge wait observed engine shutdown before ordered handoff")
                    .disclose());
            }
            if trx_sys.purge_handoff_cts() >= ts {
                return Ok(());
            }
            select_all(vec![handoff_listener, poison_listener, shutdown_listener]).await;
        }
    }

    /// Asserts checkpoint published in tests.
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

    /// Waits for checkpoint purge in tests.
    pub(crate) async fn wait_for_checkpoint_purge(session: &Session, redo_cts: TrxID) -> TrxID {
        wait_for_purge_handoff(session, redo_cts).await.unwrap();
        session
            .wait_for_purge_completion_after(redo_cts)
            .await
            .unwrap()
    }

    /// Waits for checkpoint root ready in tests.
    pub(crate) async fn wait_for_checkpoint_root_ready(session: &mut Session, table_id: TableID) {
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

    /// Returns the number of registry-owned sessions for tests.
    #[inline]
    pub(crate) fn session_registry_len(registry: &SessionRegistry) -> usize {
        registry.entries.len()
    }

    /// Count registry-owned operations that still block owner shutdown.
    #[inline]
    pub(crate) fn active_operation_count(registry: &SessionRegistry) -> usize {
        registry
            .entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .filter(|session| {
                matches!(
                    &session.lifecycle.lock().slot,
                    SessionOperationSlot::Active(_)
                )
            })
            .count()
    }

    /// Return the coherent snapshot of one test session's active operation.
    #[inline]
    pub(crate) fn active_operation_snapshot(
        registry: &SessionRegistry,
        session_id: SessionID,
    ) -> SessionOperationSnapshot {
        active_operation_entry_for_test(registry, session_id).inspect()
    }

    /// Returns whether a registered session currently owns its public transaction cache.
    #[inline]
    pub(crate) fn session_has_public_trx_cache(
        registry: &SessionRegistry,
        session_id: SessionID,
    ) -> bool {
        registry
            .session_state(session_id)
            .is_some_and(|state| state.lifecycle.lock().public_trx_cache.is_some())
    }

    /// Removes one synthetic or failed-retained test session before engine teardown.
    #[inline]
    pub(crate) fn remove_session_for_test(registry: &SessionRegistry, session_id: SessionID) {
        drop(registry.entries.remove(&session_id));
    }

    /// Create one registry-owned transaction with test-controlled ids.
    #[inline]
    pub(crate) fn create_test_transaction(
        engine: &Engine,
        session_id: SessionID,
        trx_id: TrxID,
        sts: TrxID,
        gc_no: usize,
    ) -> (Transaction, Arc<SessionState>) {
        let registry = &engine.inner().session_registry;
        let state = Arc::new(new_session_state_for_test(engine, session_id));
        let key = SessionOperationKey::new(session_id, OperationID::new(1));
        let (mut inner, authority) = {
            let mut lifecycle = state.lifecycle.lock();
            let inner = lifecycle
                .public_trx_cache
                .take()
                .expect("test session must start with one public transaction cache");
            let authority = lifecycle
                .lock_authority
                .take()
                .expect("test session must start with one family lock authority");
            (inner, authority)
        };
        inner.init(trx_id, sts, gc_no, session_id, authority);
        let entry = SessionOperationEntry::new_public_transaction(key, inner);
        {
            let mut lifecycle = state.lifecycle.lock();
            lifecycle.next_operation_id = 2;
            lifecycle.slot = SessionOperationSlot::Active(ActiveSessionOperation::Operation(entry));
        }
        let runtime = SessionRuntime::new(Arc::clone(&state));
        registry.insert(Arc::clone(&state));
        (
            Transaction::new(runtime.downgrade(), key, trx_id, sts),
            state,
        )
    }

    /// Begin one test-owned mandatory private transaction for focused catalog storage tests.
    pub(crate) fn begin_test_mandatory_private_trx(
        session: &Session,
    ) -> (MandatoryOperationGuard, PrivateTransaction) {
        let mut operation = session
            .pin_operation(SessionOperationKind::Ddl)
            .expect("catalog test operation must be admitted")
            .into_mandatory();
        let trx = operation
            .begin_private_trx()
            .expect("catalog test private transaction must begin");
        (operation, trx)
    }

    /// Asserts existing transaction error in tests.
    pub(crate) fn assert_existing_transaction_error(
        err: &Error,
        session_id: SessionID,
        trx_id: TrxID,
        state: &str,
    ) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::ExistingTransaction)
        );
        let diagnostic = format!("{:?}", err.report());
        assert!(diagnostic.contains(&format!("session_id={session_id}")));
        assert!(diagnostic.contains(&format!("trx_id={trx_id}")));
        assert!(diagnostic.contains(&format!("state={state}")));
    }

    fn terminal_attachment_test_hook_slot() -> &'static Mutex<Option<TerminalAttachmentTestHook>> {
        static HOOK: OnceLock<Mutex<Option<TerminalAttachmentTestHook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    fn terminal_attachment_test_hook_install_lock() -> &'static Mutex<()> {
        static INSTALL_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        INSTALL_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn set_test_total_row_pages_after_runtime_resolution_hook<F, Fut>(hook: F)
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        TEST_TOTAL_ROW_PAGES_AFTER_RUNTIME_RESOLUTION_HOOK.with(|slot| {
            let old = slot
                .borrow_mut()
                .replace(Box::new(move || Box::pin(hook())));
            assert!(
                old.is_none(),
                "total-row-pages runtime-resolution hook already installed"
            );
        });
    }

    pub(super) async fn run_test_total_row_pages_after_runtime_resolution_hook() {
        let hook = TEST_TOTAL_ROW_PAGES_AFTER_RUNTIME_RESOLUTION_HOOK
            .with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook().await;
        }
    }

    #[inline]
    fn assert_runtime_unavailable_after_shutdown(err: Error) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );
    }

    #[inline]
    fn assert_fatal_admission_error(err: Error, fatal: FatalError) {
        assert_eq!(err.kind(), ErrorKind::Fatal);
        assert!(err.report().downcast_ref::<LifecycleError>().is_none());
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
            trx.table_insert_mvcc(table_id, vec![Val::from(value), Val::from(&payload[..])])
                .await
                .unwrap();
            trx.commit().await.unwrap();
            if redo_file_path(main_dir, log_file_stem, target_file_seq).exists() {
                return table_id;
            }
        }
        panic!("test setup did not create redo file {target_file_seq:08x}");
    }

    async fn commit_redo_durability_anchor(session: &mut Session, table_id: TableID) {
        let payload = [9u8; 196];
        let mut trx = session.begin_trx().unwrap();
        trx.table_insert_mvcc(
            table_id,
            vec![Val::from(10_000i32), Val::from(&payload[..])],
        )
        .await
        .unwrap();
        trx.commit().await.unwrap();
    }

    async fn create_cache_test_table(session: &mut Session) -> TableID {
        session
            .create_table(
                StorageTableSpec::new(vec![StorageColumnSpec::new(
                    ValKind::I32,
                    StorageColumnFlags::empty(),
                )]),
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .await
            .unwrap()
            .table_id()
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

    #[inline]
    fn new_session_state_for_test(engine: &Engine, id: SessionID) -> SessionState {
        let seed = engine.new_session().unwrap();
        let state = seed
            .session
            .state
            .upgrade()
            .expect("new test session must remain registered");
        let synthetic =
            SessionState::new(Arc::clone(&state.core), Arc::clone(&state.admission), id);
        drop(state);
        drop(seed);
        synthetic
    }

    #[inline]
    fn active_operation_entry_for_test(
        registry: &SessionRegistry,
        session_id: SessionID,
    ) -> Arc<SessionOperationEntry> {
        let state = registry
            .session_state(session_id)
            .expect("test session must remain registered");
        let lifecycle = state.lifecycle.lock();
        match &lifecycle.slot {
            SessionOperationSlot::Active(entry) => Arc::clone(
                entry
                    .operation_entry()
                    .expect("test helper requires an existing operation entry"),
            ),
            SessionOperationSlot::Idle | SessionOperationSlot::Closed => {
                panic!("test session must have an active operation")
            }
        }
    }

    fn inspect_session_in_trx(state: &SessionState) -> LifecycleResult<bool> {
        let lifecycle = state.lifecycle.lock();
        if lifecycle.disposition != SessionDisposition::Open {
            return Err(state.unavailable_err(&lifecycle));
        }
        Ok(matches!(
            &lifecycle.slot,
            SessionOperationSlot::Active(entry)
                if entry
                    .operation_entry()
                    .is_some_and(|entry| entry.inspect().trx_id.is_some())
        ))
    }

    fn test_session_runtime(session: &Session) -> LifecycleResult<SessionRuntime> {
        let pin = session.pin_inspection()?;
        inspect_session_in_trx(pin.runtime.state())
            .attach_with(|| format!("session_id={}", session.id))?;
        Ok(pin.runtime.clone())
    }

    fn run_observer_admission_shutdown_race(inspection: bool) {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            if inspection {
                let _ = engine
                    .inner()
                    .poisoner
                    .poison(Report::new(FatalError::RedoWrite).attach("observer race poison"));
            }
            let barrier = Arc::new(Barrier::new(3));

            thread::scope(|scope| {
                let shutdown_engine = &engine;
                let shutdown_barrier = Arc::clone(&barrier);
                let shutdown = scope.spawn(move || {
                    shutdown_barrier.wait();
                    shutdown_engine.try_shutdown()
                });
                let observer_barrier = Arc::clone(&barrier);
                let observer = scope.spawn(move || {
                    observer_barrier.wait();
                    if inspection {
                        session
                            .pin_inspection()
                            .map_err(LifecycleOrFatalError::from)
                    } else {
                        session.pin_observer()
                    }
                });
                barrier.wait();

                match (shutdown.join().unwrap(), observer.join().unwrap()) {
                    (Ok(()), Err(err)) => {
                        let LifecycleOrFatalError::Lifecycle(err) = err else {
                            panic!("shutdown admission must remain Lifecycle")
                        };
                        assert_eq!(err.current_context(), &LifecycleError::Shutdown);
                    }
                    (Err(err), Ok(observer)) => {
                        assert_eq!(
                            err.report().downcast_ref::<LifecycleError>().copied(),
                            Some(LifecycleError::ShutdownBusy)
                        );
                        assert_eq!(
                            err.report().downcast_ref::<String>().map(String::as_str),
                            Some(
                                "origin=explicit, session_blocker=observer, operation_state=none, observer_count=1, cleanup_queued=false, mandatory_callers=0, mandatory_internal=0"
                            )
                        );
                        drop(observer);
                        engine.shutdown();
                    }
                    (Ok(()), Ok(observer)) => {
                        drop(observer);
                        panic!("shutdown completed after admitting an invisible observer");
                    }
                    (Err(shutdown_err), Err(observer_err)) => {
                        panic!(
                            "observer race produced two rejections: shutdown={shutdown_err:?}, observer={observer_err:?}"
                        );
                    }
                }
            });
        });
    }

    #[test]
    fn test_active_transaction_allows_observers_and_rejects_effectful_operations() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            let table_ids_before = engine.inner().core.catalog().list_user_table_ids_now();

            macro_rules! assert_rejected {
                ($result:expr) => {
                    match $result {
                        Ok(_) => panic!(
                            "active transaction unexpectedly admitted session operation: {}",
                            stringify!($result)
                        ),
                        Err(err) => {
                            assert_existing_transaction_error(
                                &err,
                                session_id,
                                trx_id,
                                "voluntary",
                            );
                        }
                    }
                };
            }

            assert_eq!(session.id(), session_id);
            assert_eq!(session.list_table_ids().unwrap(), table_ids_before);
            assert!(session.transaction_system_stats().is_ok());
            assert!(session.storage_io_stats().is_ok());
            assert!(session.buffer_pool_stats().is_ok());
            assert!(session.mandatory_runtime_stats().is_ok());
            assert!(session.logical_lock_stats().is_ok());
            assert!(
                session
                    .wait_for_checkpoint_retry(CheckpointDelayReason::ActiveRoot {
                        table_id,
                        effective_ts: TrxID::new(0),
                        min_active_sts: TrxID::new(0),
                    })
                    .await
                    .is_ok()
            );
            assert!(
                session
                    .wait_for_gc_horizon_after(TrxID::new(0))
                    .await
                    .is_ok()
            );
            assert!(
                session
                    .wait_for_purge_completion_after(TrxID::new(0))
                    .await
                    .is_ok()
            );

            assert_rejected!(session.begin_trx());
            assert_rejected!(session.close().await);
            assert_rejected!(
                session
                    .create_table(
                        StorageTableSpec::new(vec![StorageColumnSpec::new(
                            ValKind::I32,
                            StorageColumnFlags::empty(),
                        )]),
                        vec![],
                    )
                    .await
            );
            assert_rejected!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(0)],
                            StorageIndexFlags::empty(),
                        ),
                    )
                    .await
            );
            assert_rejected!(session.drop_index(table_id, crate::IndexID::new(0)).await);
            assert_rejected!(session.drop_table(table_id).await);
            assert_rejected!(session.checkpoint_catalog().await);
            assert_rejected!(session.checkpoint_catalog_and_truncate_redo_log().await);
            assert_rejected!(session.truncate_redo_log().await);
            assert_rejected!(session.freeze_table(table_id, usize::MAX).await);
            assert_rejected!(session.checkpoint_table(table_id).await);
            assert_rejected!(session.checkpoint_table_with_wait(table_id).await);
            assert_rejected!(session.total_row_pages(table_id).await);
            assert_rejected!(session.cleanup_secondary_mem_indexes(table_id, true).await);
            assert_rejected!(session.lock_table(table_id, TableLockMode::Exclusive).await);
            assert_rejected!(session.unlock_table(table_id));

            assert_eq!(
                lock_entry_count(&engine, LockOwner::session_explicit(session_id)),
                0
            );
            assert_eq!(
                engine.inner().core.catalog().list_user_table_ids_now(),
                table_ids_before
            );

            trx.rollback().await.unwrap();
            assert_eq!(session.list_table_ids().unwrap(), table_ids_before);
        });
    }

    #[test]
    fn test_observer_registration_counts_without_consuming_operation_slot() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();

            {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.observer_count, 0);
                assert_eq!(lifecycle.next_operation_id, 1);
                assert!(matches!(lifecycle.slot, SessionOperationSlot::Idle));
            }

            let trx = session.begin_trx().unwrap();
            let operation_key = {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.next_operation_id, 2);
                lifecycle.slot.active_entry().unwrap().key()
            };
            let first = session.pin_observer().unwrap();
            let second = session.pin_inspection().unwrap();
            {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.observer_count, 2);
                assert_eq!(lifecycle.next_operation_id, 2);
                assert_eq!(
                    lifecycle.slot.active_entry().map(|entry| entry.key()),
                    Some(operation_key)
                );
            }

            drop(first);
            assert_eq!(state.lifecycle.lock().observer_count, 1);
            drop(second);
            assert_eq!(state.lifecycle.lock().observer_count, 0);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_observer_counts_are_session_local() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session1 = engine.new_session().unwrap();
            let session2 = engine.new_session().unwrap();
            let state1 = engine
                .inner()
                .session_registry
                .session_state(session1.id())
                .unwrap();
            let state2 = engine
                .inner()
                .session_registry
                .session_state(session2.id())
                .unwrap();

            let observer1 = session1.pin_observer().unwrap();
            let observer2 = session2.pin_observer().unwrap();
            assert_eq!(state1.lifecycle.lock().observer_count, 1);
            assert_eq!(state2.lifecycle.lock().observer_count, 1);

            drop(observer1);
            assert_eq!(state1.lifecycle.lock().observer_count, 0);
            assert_eq!(state2.lifecycle.lock().observer_count, 1);
            drop(observer2);
            assert_eq!(state2.lifecycle.lock().observer_count, 0);
        });
    }

    #[test]
    fn test_close_retains_observed_session_until_final_observer_drop() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let observer = session.pin_observer().unwrap();
            let state = Arc::clone(observer.runtime.state());

            session.close().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.disposition, SessionDisposition::CloseRequested);
                assert!(matches!(lifecycle.slot, SessionOperationSlot::Closed));
                assert_eq!(lifecycle.observer_count, 1);
            }
            let err = match session.pin_observer() {
                Ok(_) => panic!("closed session must reject new observers"),
                Err(err) => err,
            };
            let LifecycleOrFatalError::Lifecycle(err) = err else {
                panic!("closed session must remain a Lifecycle rejection")
            };
            assert_eq!(err.current_context(), &LifecycleError::SessionUnavailable);

            drop(observer);
            assert!(
                engine
                    .inner()
                    .session_registry
                    .session_state(session_id)
                    .is_none()
            );
        });
    }

    #[test]
    fn test_abandonment_and_terminal_operation_retain_observed_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let trx = session.begin_trx().unwrap();
            let observer = session.pin_observer().unwrap();
            let state = Arc::clone(observer.runtime.state());

            drop(session);
            {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.disposition, SessionDisposition::Abandoned);
                assert!(matches!(lifecycle.slot, SessionOperationSlot::Active(_)));
                assert_eq!(lifecycle.observer_count, 1);
            }

            trx.rollback().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            {
                let lifecycle = state.lifecycle.lock();
                assert!(matches!(lifecycle.slot, SessionOperationSlot::Closed));
                assert_eq!(lifecycle.observer_count, 1);
            }

            drop(observer);
            assert!(
                engine
                    .inner()
                    .session_registry
                    .session_state(session_id)
                    .is_none()
            );
        });
    }

    #[test]
    fn test_shutdown_reports_operation_before_observers_on_same_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let observer = session.pin_observer().unwrap();
            let state = Arc::clone(observer.runtime.state());

            let blocker = state.shutdown_blocker().unwrap();
            assert_eq!(blocker.label(), "operation");
            assert_eq!(blocker.observer_count(), 0);

            trx.rollback().await.unwrap();
            let blocker = state.shutdown_blocker().unwrap();
            assert_eq!(blocker.label(), "observer");
            assert_eq!(blocker.operation_state(), None);
            assert_eq!(blocker.observer_count(), 1);

            drop(observer);
            assert!(state.shutdown_blocker().is_none());
        });
    }

    #[test]
    fn test_observer_shutdown_wait_is_woken_by_exact_release() {
        smol::block_on(async {
            const REPETITIONS: usize = 100;

            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();

            for _ in 0..REPETITIONS {
                let observer = session.pin_observer().unwrap();
                let shutdown_wait = state
                    .shutdown_wait()
                    .expect("observer must install a shutdown listener");
                assert_eq!(shutdown_wait.blocker.label(), "observer");
                assert_eq!(shutdown_wait.blocker.observer_count(), 1);

                drop(observer);
                shutdown_wait.listener.await;
                assert!(state.shutdown_blocker().is_none());
            }

            let observer = session.pin_observer().unwrap();
            drop(observer);
            assert!(
                state.shutdown_wait().is_none(),
                "release before listener installation must be visible to the predicate scan"
            );
        });
    }

    #[test]
    fn test_admitted_session_runtime_holds_admission_until_drop() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let admitted = session.session.upgrade().unwrap().unwrap();
            assert_eq!(admitted.runtime().state().id(), session.id());
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();

            thread::scope(|scope| {
                let shutdown = scope.spawn(|| {
                    started_tx.send(()).unwrap();
                    engine.shutdown();
                    done_tx.send(()).unwrap();
                });

                started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown thread should start");
                assert!(
                    done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                    "shutdown must wait while the admitted session runtime is live"
                );

                drop(admitted);
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown should complete after admitted session runtime drops");
                shutdown.join().unwrap();
            });
        });
    }

    #[test]
    fn test_normal_observer_admission_race_registers_or_rejects() {
        run_observer_admission_shutdown_race(false);
    }

    #[test]
    fn test_poison_tolerant_inspection_race_registers_or_rejects() {
        run_observer_admission_shutdown_race(true);
    }

    #[test]
    fn test_session_table_cache_owns_active_user_insert_page() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = create_cache_test_table(&mut session).await;

            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(table_id, vec![Val::from(1i32)])
                .await
                .unwrap();
            trx.commit().await.unwrap();

            let pin = session.pin_observer().unwrap();
            let cached_page = {
                let cache = pin.runtime.state().table_cache.lock();
                let entry = cache.get(&table_id).unwrap();
                assert!(entry.table.upgrade().is_some());
                entry.active_insert_page.unwrap()
            };
            assert_eq!(
                pin.runtime.state().load_active_insert_page(table_id),
                Some(cached_page)
            );
            {
                let cache = pin.runtime.state().table_cache.lock();
                let entry = cache.get(&table_id).unwrap();
                assert!(entry.table.upgrade().is_some());
                assert!(entry.active_insert_page.is_none());
            }
            pin.runtime
                .state()
                .save_active_insert_page(table_id, cached_page);
            assert_eq!(
                pin.runtime
                    .state()
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let catalog_tables = engine
                .inner()
                .core
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
                let pin = session1.pin_observer().unwrap();
                assert!(
                    pin.runtime
                        .state()
                        .table_cache
                        .lock()
                        .keys()
                        .all(|table_id| !table_id.is_catalog())
                );
            }

            let mut session2 = engine.new_session().unwrap();
            create_cache_test_table(&mut session2).await;
            let guards2 = session2.pool_guards();
            assert_eq!(
                catalog_row_page_count(&catalog_tables, &guards2).await,
                row_page_count
            );
            let pin = session2.pin_observer().unwrap();
            assert!(
                pin.runtime
                    .state()
                    .table_cache
                    .lock()
                    .keys()
                    .all(|table_id| !table_id.is_catalog())
            );
        });
    }

    #[test]
    fn test_sessions_use_independent_pool_guard_arc_roots() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session1 = engine.new_session().unwrap();
            let session2 = engine.new_session().unwrap();
            let runtime1 = test_session_runtime(&session1).unwrap();
            let runtime2 = test_session_runtime(&session2).unwrap();
            let guards1 = runtime1.pool_guards().clone();
            let guards1_clone = runtime1.pool_guards().clone();
            let guards2 = runtime2.pool_guards().clone();
            let canonical = engine.inner().core.pools.pool_guards().clone();

            for role in [
                PoolRole::Meta,
                PoolRole::Index,
                PoolRole::Mem,
                PoolRole::Disk,
            ] {
                let guard1 = guards1.guard(role);
                let guard1_clone = guards1_clone.guard(role);
                let guard2 = guards2.guard(role);
                let canonical_guard = canonical.guard(role);
                assert_eq!(guard1.identity(), guard2.identity(), "role={role:?}");
                assert_eq!(
                    guard1.identity(),
                    canonical_guard.identity(),
                    "role={role:?}"
                );
                assert!(
                    test_pool_guards_share_keepalive_root(guard1, guard1_clone),
                    "same-session clones must share one root: role={role:?}"
                );
                assert!(
                    !test_pool_guards_share_keepalive_root(guard1, guard2),
                    "different sessions must shard roots: role={role:?}"
                );
                assert!(
                    !test_pool_guards_share_keepalive_root(guard1, canonical_guard),
                    "session and canonical engine work must shard roots: role={role:?}"
                );
            }
        });
    }

    #[test]
    fn test_shutdown_inspection_collects_exact_claimable_transaction() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = Arc::new(new_session_state_for_test(&engine, session_id));
            let key = SessionOperationKey::new(session_id, OperationID::new(1));
            let entry = SessionOperationEntry::new_public_transaction(
                key,
                Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
            );
            assert!(entry.abandon_transaction(trx_id));
            {
                let mut lifecycle = state.lifecycle.lock();
                lifecycle.disposition = SessionDisposition::Abandoned;
                lifecycle.slot = SessionOperationSlot::Active(ActiveSessionOperation::Operation(
                    Arc::clone(&entry),
                ));
            }

            let blocker = state
                .shutdown_blocker()
                .expect("abandoned transaction must block shutdown");
            assert_eq!(blocker.label(), "operation");
            let cleanup = blocker
                .into_cleanup()
                .expect("abandoned transaction must be claimable");
            assert_eq!((cleanup.operation_key, cleanup.trx_id), (key, trx_id));
            assert!(Arc::ptr_eq(cleanup.runtime.state(), &state));
            assert_eq!(entry.inspect().state, SessionOperationState::CleanupReady);
        });
    }

    #[test]
    fn test_shutdown_inspection_keeps_failed_retained_operation_active() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = Arc::new(new_session_state_for_test(&engine, session_id));
            let key = SessionOperationKey::new(session_id, OperationID::new(1));
            let entry = SessionOperationEntry::new_public_transaction(
                key,
                Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
            );
            entry.fail_retained();
            {
                let mut lifecycle = state.lifecycle.lock();
                lifecycle.disposition = SessionDisposition::Abandoned;
                lifecycle.slot = SessionOperationSlot::Active(ActiveSessionOperation::Operation(
                    Arc::clone(&entry),
                ));
            }

            let blocker = state
                .shutdown_blocker()
                .expect("failed-retained operation must block shutdown");
            assert_eq!(
                blocker.operation_state(),
                Some(SessionOperationState::FailedRetained)
            );
            assert!(blocker.into_cleanup().is_none());
            assert_eq!(entry.inspect().state, SessionOperationState::FailedRetained);
        });
    }

    #[test]
    fn test_shutdown_wait_installs_listener_before_transition() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = Arc::new(new_session_state_for_test(&engine, session_id));
            let key = SessionOperationKey::new(session_id, OperationID::new(1));
            let entry = SessionOperationEntry::new_public_transaction(
                key,
                Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
            );
            state.lifecycle.lock().slot =
                SessionOperationSlot::Active(ActiveSessionOperation::Operation(Arc::clone(&entry)));

            let shutdown_wait = state
                .shutdown_wait()
                .expect("active transaction must install a shutdown listener");
            let SessionShutdownWait { blocker, listener } = shutdown_wait;
            assert!(blocker.into_cleanup().is_none());
            assert!(state.lifecycle.lock().change_ev.is_some());

            assert!(state.abandon_trx_handle(key, trx_id));
            listener.await;
            assert_eq!(entry.inspect().state, SessionOperationState::CleanupReady);
        });
    }

    #[test]
    fn test_session_change_wakes_all_installed_listeners() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = Arc::new(new_session_state_for_test(&engine, session_id));
            let key = SessionOperationKey::new(session_id, OperationID::new(1));
            let entry = SessionOperationEntry::new_public_transaction(
                key,
                Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
            );
            state.lifecycle.lock().slot =
                SessionOperationSlot::Active(ActiveSessionOperation::Operation(entry));

            let first = state
                .shutdown_wait()
                .expect("first shutdown listener must be installed");
            let second = state
                .shutdown_wait()
                .expect("second shutdown listener must reuse the event");
            assert!(state.abandon_trx_handle(key, trx_id));

            futures::join!(first.listener, second.listener);
        });
    }

    #[test]
    fn test_registry_shutdown_wait_arms_only_first_blocker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let registry = SessionRegistry::new();
            let mut states = Vec::new();
            let mut expected_cleanup = Vec::new();
            for raw_id in 1..=2 {
                let session_id = SessionID::new(raw_id);
                let trx_id = TrxID::new(MIN_ACTIVE_TRX_ID.as_u64() + raw_id);
                let state = Arc::new(new_session_state_for_test(&engine, session_id));
                let key = SessionOperationKey::new(session_id, OperationID::new(1));
                let entry = SessionOperationEntry::new_public_transaction(
                    key,
                    Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
                );
                assert!(entry.abandon_transaction(trx_id));
                {
                    let mut lifecycle = state.lifecycle.lock();
                    lifecycle.disposition = SessionDisposition::Abandoned;
                    lifecycle.slot =
                        SessionOperationSlot::Active(ActiveSessionOperation::Operation(entry));
                }
                expected_cleanup.push((key, trx_id));
                states.push(Arc::clone(&state));
                registry.insert(state);
            }

            let shutdown_wait = registry
                .first_shutdown_wait()
                .expect("one active session must block shutdown");
            let cleanup = shutdown_wait
                .blocker
                .into_cleanup()
                .expect("abandoned transaction must be claimable");
            assert!(expected_cleanup.contains(&(cleanup.operation_key, cleanup.trx_id)));
            assert_eq!(
                states
                    .iter()
                    .filter(|state| state.lifecycle.lock().change_ev.is_some())
                    .count(),
                1,
                "lazy shutdown scan must stop after arming its first blocker"
            );
        });
    }

    #[test]
    fn test_cold_removal_preserves_pointer_distinct_replacement() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let session_id = session.id();
            let registry = &engine.inner().session_registry;
            let stale = registry
                .session_state(session_id)
                .expect("new session must be registered");
            let stale_runtime = SessionRuntime::new(Arc::clone(&stale));
            let replacement = Arc::new(new_session_state_for_test(&engine, session_id));

            let displaced = registry
                .entries
                .insert(session_id, Arc::clone(&replacement))
                .expect("test replacement must displace the original state");
            assert!(Arc::ptr_eq(&displaced, &stale));

            stale_runtime.remove_if_requested(true);
            let registered = registry
                .session_state(session_id)
                .expect("pointer-distinct replacement must remain registered");
            assert!(Arc::ptr_eq(&registered, &replacement));

            remove_session_for_test(registry, session_id);
            drop(registered);
            drop(replacement);
            drop(displaced);
            drop(stale_runtime);
            drop(stale);
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_registry_shutdown_wait_lazily_drains_many_blockers() {
        smol::block_on(async {
            const SESSION_COUNT: u64 = 32;

            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let registry = SessionRegistry::new();
            let mut states = Vec::new();
            for raw_id in 1..=SESSION_COUNT {
                let session_id = SessionID::new(raw_id);
                let trx_id = TrxID::new(MIN_ACTIVE_TRX_ID.as_u64() + raw_id);
                let state = Arc::new(new_session_state_for_test(&engine, session_id));
                let key = SessionOperationKey::new(session_id, OperationID::new(1));
                let entry = SessionOperationEntry::new_public_transaction(
                    key,
                    Box::new(trx_inner(trx_id, MIN_SNAPSHOT_TS, 0, session_id)),
                );
                assert!(entry.abandon_transaction(trx_id));
                {
                    let mut lifecycle = state.lifecycle.lock();
                    lifecycle.disposition = SessionDisposition::Abandoned;
                    lifecycle.slot =
                        SessionOperationSlot::Active(ActiveSessionOperation::Operation(entry));
                }
                states.push(Arc::clone(&state));
                registry.insert(state);
            }

            let mut drained = Vec::new();
            for expected_armed in 1..=SESSION_COUNT as usize {
                let shutdown_wait = registry
                    .first_shutdown_wait()
                    .expect("one remaining active session must block shutdown");
                let cleanup = shutdown_wait
                    .blocker
                    .into_cleanup()
                    .expect("abandoned transaction must be claimable");
                let key = cleanup.operation_key;
                assert!(
                    !drained.contains(&key),
                    "a drained session must not block a later lazy pass"
                );
                assert_eq!(
                    states
                        .iter()
                        .filter(|state| state.lifecycle.lock().change_ev.is_some())
                        .count(),
                    expected_armed,
                    "each pass may arm only its first current blocker"
                );

                let state = registry
                    .session_state(key.session_id())
                    .expect("synthetic blocker must remain registered");
                let notify = {
                    let mut lifecycle = state.lifecycle.lock();
                    lifecycle.slot = SessionOperationSlot::Closed;
                    lifecycle.change_ev.clone()
                };
                SessionState::notify_operation_change(notify);
                shutdown_wait.listener.await;
                drained.push(key);
            }

            assert!(registry.first_shutdown_wait().is_none());
            assert_eq!(drained.len(), SESSION_COUNT as usize);
        });
    }

    #[test]
    fn test_shutdown_probe_does_not_install_listener() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();
            let trx = session.begin_trx().unwrap();

            assert!(
                engine
                    .inner()
                    .session_registry
                    .first_shutdown_blocker()
                    .is_some()
            );
            assert!(state.lifecycle.lock().change_ev.is_none());

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_operation_ids_are_local_monotonic_and_kind_independent() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();
            {
                let lifecycle = state.lifecycle.lock();
                assert_eq!(lifecycle.disposition, SessionDisposition::Open);
                assert!(matches!(lifecycle.slot, SessionOperationSlot::Idle));
                assert_eq!(lifecycle.next_operation_id, 1);
                assert!(lifecycle.change_ev.is_none());
            }

            let trx = session.begin_trx().unwrap();
            let public_key =
                active_operation_entry_for_test(&engine.inner().session_registry, session.id())
                    .key();
            trx.rollback().await.unwrap();

            let ddl = session.pin_operation(SessionOperationKind::Ddl).unwrap();
            let ddl_key = ddl.key();
            drop(ddl);
            let maintenance = session
                .pin_operation(SessionOperationKind::Maintenance)
                .unwrap();
            let maintenance_key = maintenance.key();
            drop(maintenance);
            let explicit = session
                .pin_operation(SessionOperationKind::SessionExplicitLock)
                .unwrap();
            let explicit_key = explicit.key();
            drop(explicit);

            assert_eq!(
                [
                    public_key.operation_id().as_u64(),
                    ddl_key.operation_id().as_u64(),
                    maintenance_key.operation_id().as_u64(),
                    explicit_key.operation_id().as_u64(),
                ],
                [1, 2, 3, 4]
            );
            assert!(
                [public_key, ddl_key, maintenance_key, explicit_key]
                    .into_iter()
                    .all(|key| key.session_id() == session.id())
            );
            let lifecycle = state.lifecycle.lock();
            assert!(matches!(lifecycle.slot, SessionOperationSlot::Idle));
            assert_eq!(lifecycle.next_operation_id, 5);
        });
    }

    #[test]
    fn test_operation_pin_consumes_into_mandatory_terminal_authority() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            let pin = session
                .pin_operation(SessionOperationKind::Maintenance)
                .unwrap();
            let key = pin.key();
            let entry = Arc::clone(&pin.entry);
            let mut mandatory = pin.into_mandatory();
            assert_eq!(mandatory.key(), key);
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Mandatory(None)
            );
            mandatory.assert_finish_ready();
            mandatory.finish();
            assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
            drop(mandatory);

            let pin = session.pin_operation(SessionOperationKind::Ddl).unwrap();
            let entry = Arc::clone(&pin.entry);
            let mut mandatory = pin.into_mandatory();
            mandatory.fail_retained();
            assert_eq!(entry.inspect().state, SessionOperationState::FailedRetained);
            drop(mandatory);

            remove_session_for_test(&engine.inner().session_registry, session.id());
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_equal_raw_operation_ids_are_isolated_by_session_family() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session1 = engine.new_session().unwrap();
            let session2 = engine.new_session().unwrap();
            let operation1 = session1
                .pin_operation(SessionOperationKind::Maintenance)
                .unwrap();
            let operation2 = session2.pin_operation(SessionOperationKind::Ddl).unwrap();

            assert_eq!(operation1.key().operation_id(), OperationID::new(1));
            assert_eq!(operation2.key().operation_id(), OperationID::new(1));
            assert_ne!(operation1.key(), operation2.key());
            assert_eq!(
                operation1.operation_lock_owner().scope(),
                operation2.operation_lock_owner().scope()
            );
            assert_ne!(
                operation1.operation_lock_owner(),
                operation2.operation_lock_owner()
            );
        });
    }

    #[test]
    fn test_lock_parts_reject_wrong_same_family_operation_scope() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let mut operation = session
                .pin_operation(SessionOperationKind::Maintenance)
                .unwrap();
            let key = operation.key();
            let wrong_key = SessionOperationKey::new(session.id(), OperationID::new(u64::MAX - 1));
            assert_ne!(wrong_key, key);
            operation.curr_scope = Some(LockScopeState::new(LockOwner::operation(wrong_key)));

            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = operation.operation_lock_parts();
            }))
            .expect_err("operation lock indexes must reject a wrong operation scope");
            let diagnostic = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&'static str>().copied())
                .expect("operation scope mismatch panic should retain its diagnostic");
            assert!(diagnostic.contains("operation scope identity mismatch"));
            assert!(diagnostic.contains(&format!("key={key}")));
            assert!(
                diagnostic.contains(&format!("actual_owner={}", LockOwner::operation(wrong_key)))
            );
        });
    }

    #[test]
    fn test_operation_id_exhaustion_panics_before_reservation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();
            state.lifecycle.lock().next_operation_id = u64::MAX;

            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = state.reserve_operation(SessionOperationKind::Maintenance);
            }))
            .expect_err("operation-id exhaustion must be a hard invariant failure");
            let diagnostic = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&'static str>().copied())
                .expect("overflow panic should retain its diagnostic");
            assert!(diagnostic.contains("session operation id exhausted"));
            let lifecycle = state.lifecycle.lock();
            assert_eq!(lifecycle.next_operation_id, u64::MAX);
            assert!(matches!(lifecycle.slot, SessionOperationSlot::Idle));
        });
    }

    #[test]
    fn test_mandatory_private_transaction_preserves_stable_entry_and_public_cache() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let operation = session.pin_operation(SessionOperationKind::Ddl).unwrap();
            let key = operation.key();
            let entry = Arc::clone(&operation.entry);
            let state = Arc::clone(operation.runtime.state());
            assert!(state.lifecycle.lock().change_ev.is_none());
            let public_cache_ptr = state
                .lifecycle
                .lock()
                .public_trx_cache
                .as_deref()
                .map(|inner| inner as *const TrxInner as usize)
                .expect("session must retain its public transaction cache");
            let mut operation = operation.into_mandatory();
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Mandatory(None)
            );
            let mut trx = operation.begin_private_trx().unwrap();
            let first_inner = private_transaction_inner_ptr(&trx);
            assert_ne!(
                first_inner, public_cache_ptr,
                "private transaction must use a core distinct from the parked public cache"
            );
            assert_eq!(
                session_operation_entry_inner_ptr(&entry),
                None,
                "running private transaction must hold its core outside the entry"
            );
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Mandatory(Some(crate::trx::InternalTrxState::Running))
            );
            let nested_begin_err = match operation.begin_private_trx() {
                Ok(_) => panic!("mandatory operation cannot start a second private transaction"),
                Err(err) => err,
            };
            assert_eq!(
                *nested_begin_err.current_context(),
                LifecycleError::ExistingTransaction
            );
            private_noop(&mut trx).await.unwrap();
            assert_eq!(private_transaction_inner_ptr(&trx), first_inner);
            assert_eq!(session_operation_entry_inner_ptr(&entry), None);
            private_noop(&mut trx).await.unwrap();
            assert_eq!(private_transaction_inner_ptr(&trx), first_inner);
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Mandatory(Some(crate::trx::InternalTrxState::Running))
            );
            assert_eq!(
                state
                    .lifecycle
                    .lock()
                    .public_trx_cache
                    .as_deref()
                    .map(|inner| inner as *const TrxInner as usize),
                Some(public_cache_ptr),
                "private begin must leave the public transaction cache untouched"
            );

            let (resolved, _) = engine
                .inner()
                .session_registry
                .try_resolve_operation(key)
                .expect("private transaction must remain registry-visible");
            assert!(Arc::ptr_eq(&resolved, &entry));
            assert_eq!(state.lifecycle.lock().next_operation_id, 2);

            trx.rollback_catalog_ddl().await.unwrap();
            let snapshot = entry.inspect();
            assert_eq!(snapshot.state, SessionOperationState::Mandatory(None));
            assert_eq!(snapshot.trx_id, None);
            {
                let lifecycle = state.lifecycle.lock();
                let SessionOperationSlot::Active(active) = &lifecycle.slot else {
                    panic!("private transaction terminal must preserve the outer operation")
                };
                assert!(
                    active
                        .operation_entry()
                        .is_some_and(|active| Arc::ptr_eq(active, &entry))
                );
                assert_eq!(lifecycle.next_operation_id, 2);
                assert_eq!(
                    lifecycle
                        .public_trx_cache
                        .as_deref()
                        .map(|inner| inner as *const TrxInner as usize),
                    Some(public_cache_ptr),
                    "private terminal must not replace the public transaction cache"
                );
            }
            assert!(state.lifecycle.lock().change_ev.is_none());

            let replacement = operation.begin_private_trx().unwrap();
            let second_inner = private_transaction_inner_ptr(&replacement);
            assert_ne!(
                second_inner, public_cache_ptr,
                "each private transaction must remain separate from the public cache"
            );
            assert_eq!(session_operation_entry_inner_ptr(&entry), None);
            assert_eq!(
                state
                    .lifecycle
                    .lock()
                    .public_trx_cache
                    .as_deref()
                    .map(|inner| inner as *const TrxInner as usize),
                Some(public_cache_ptr),
                "sequential private transactions must leave the public cache parked"
            );
            replacement.rollback_catalog_ddl().await.unwrap();

            operation.assert_finish_ready();
            operation.finish();
            assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
            drop(operation);
            assert!(matches!(
                state.lifecycle.lock().slot,
                SessionOperationSlot::Idle
            ));
        });
    }

    #[test]
    fn test_unobserved_transaction_and_statement_transitions_are_silent() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let state = engine
                .inner()
                .session_registry
                .session_state(session.id())
                .unwrap();
            assert!(state.lifecycle.lock().change_ev.is_none());

            let mut trx = session.begin_trx().unwrap();
            trx.noop().await.unwrap();
            assert!(state.lifecycle.lock().change_ev.is_none());
            trx.rollback().await.unwrap();
            assert!(state.lifecycle.lock().change_ev.is_none());

            let trx = session.begin_trx().unwrap();
            assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
            assert!(state.lifecycle.lock().change_ev.is_none());
        });
    }

    #[test]
    fn test_existing_operation_reports_exact_coordinator_identity() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let operation = session
                .pin_operation(SessionOperationKind::Maintenance)
                .unwrap();
            let key = operation.key();

            let err = match session.begin_trx() {
                Ok(_) => panic!("active maintenance operation must block transaction admission"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingOperation)
            );
            let diagnostic = format!("{:?}", err.report());
            assert!(diagnostic.contains(&format!("operation_key={key}")));
            assert!(diagnostic.contains("kind=maintenance"));
            assert!(diagnostic.contains("state=voluntary"));
            assert!(diagnostic.contains("disposition=open"));
            assert!(diagnostic.contains("trx_id=none"));

            let close_err = session.close().await.unwrap_err();
            assert_eq!(
                close_err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingOperation)
            );
            drop(operation);
            session.close().await.unwrap();
        });
    }

    #[test]
    fn test_session_list_table_ids_empty_and_sorted() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.checkpoint_catalog().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_total_row_pages_session_table_blocks_drop_until_runtime_release() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                root.path().to_path_buf(),
                "total-row-pages-drop-drain",
            ))
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let mut count_session = engine.new_session().unwrap();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_total_row_pages_after_runtime_resolution_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let mut count = Box::pin(count_session.total_row_pages(table_id));
            assert!(futures::poll!(count.as_mut()).is_pending());
            entered_rx.recv_async().await.unwrap();

            let mut drop_session = engine.new_session().unwrap();
            let mut drop_table = Box::pin(drop_session.drop_table(table_id));
            assert!(futures::poll!(drop_table.as_mut()).is_pending());

            release_tx.send_async(()).await.unwrap();
            assert_eq!(count.await.unwrap(), 0);
            drop_table.await.unwrap();
            wait_for_dropped_table_floor(&engine, table_id).await;

            engine.shutdown();
        });
    }

    #[test]
    fn test_explicit_table_lock_retains_separate_maintenance_claim() {
        smol::block_on(async {
            for (table_mode, explicit_data_mode) in [
                (TableLockMode::Shared, LockMode::Shared),
                (TableLockMode::Exclusive, LockMode::Exclusive),
            ] {
                let root = TempDir::new().unwrap();
                let engine = Engine::bootstrap(lightweight_test_engine_config(
                    root.path().to_path_buf(),
                    "explicit-maintenance-owner",
                ))
                .await
                .unwrap();
                let table_id = table2(&engine).await;
                let mut session = engine.new_session().unwrap();
                let session_id = session.id();
                let explicit_owner = LockOwner::session_explicit(session_id);
                session.lock_table(table_id, table_mode).await.unwrap();

                let (entered_tx, entered_rx) = flume::bounded(1);
                let (release_tx, release_rx) = flume::bounded(1);
                set_test_total_row_pages_after_runtime_resolution_hook(move || async move {
                    entered_tx.send_async(()).await.unwrap();
                    release_rx.recv_async().await.unwrap();
                });

                let mut count = Box::pin(session.total_row_pages(table_id));
                assert!(futures::poll!(count.as_mut()).is_pending());
                entered_rx.recv_async().await.unwrap();

                let metadata = LockResource::TableMetadata(table_id);
                let data = LockResource::TableData(table_id);
                let maintenance_owner = maintenance_lock_owner(
                    &engine,
                    session_id,
                    metadata,
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                )
                .expect("maintenance owner should retain metadata S");
                assert_eq!(maintenance_owner.family(), explicit_owner.family());
                assert!(has_lock_entry(
                    &engine,
                    explicit_owner,
                    metadata,
                    LockMode::Shared,
                    LockDebugEntryState::Granted,
                ));
                assert!(has_lock_entry(
                    &engine,
                    explicit_owner,
                    data,
                    explicit_data_mode,
                    LockDebugEntryState::Granted,
                ));
                assert!(has_lock_entry(
                    &engine,
                    maintenance_owner,
                    data,
                    explicit_data_mode,
                    LockDebugEntryState::Granted,
                ));

                release_tx.send_async(()).await.unwrap();
                assert_eq!(count.await.unwrap(), 0);
                assert_eq!(lock_entry_count(&engine, maintenance_owner), 2);
                assert_eq!(lock_entry_count(&engine, explicit_owner), 2);

                session.unlock_table(table_id).unwrap();
                engine.shutdown();
            }
        });
    }

    #[test]
    fn test_queued_explicit_lock_poison_returns_fatal_and_rolls_back_prefix() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                root.path().to_path_buf(),
                "explicit-lock-poison",
            ))
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let mut blocker = engine.new_session().unwrap();
            blocker
                .lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();

            let mut waiter = engine.new_session().unwrap();
            let waiter_owner = LockOwner::session_explicit(waiter.id());
            let blocker_owner = LockOwner::session_explicit(blocker.id());
            let mut acquire = Box::pin(waiter.lock_table(table_id, TableLockMode::Shared));
            assert!(futures::poll!(acquire.as_mut()).is_pending());
            engine.inner().poisoner.poison(
                Report::new(FatalError::StorageIo).attach("queued explicit-lock unrelated poison"),
            );

            let error = acquire.as_mut().await.unwrap_err();
            assert_fatal_admission_error(error, FatalError::StorageIo);
            drop(acquire);
            assert_eq!(lock_entry_count(&engine, waiter_owner), 0);
            assert_eq!(lock_entry_count(&engine, blocker_owner), 2);

            drop(waiter);
            drop(blocker);
            engine.shutdown();
        });
    }

    #[test]
    fn test_session_maintenance_progress_waits_are_observers() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
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
            assert!(
                active_session
                    .wait_for_gc_horizon_after(target)
                    .await
                    .is_ok()
            );
            assert!(
                active_session
                    .wait_for_purge_completion_after(target)
                    .await
                    .is_ok()
            );
            active_session
                .wait_for_checkpoint_retry(CheckpointDelayReason::ActiveRoot {
                    table_id: TableID::new(1),
                    effective_ts: target,
                    min_active_sts: target,
                })
                .await
                .unwrap();
            assert!(active_session.in_trx().unwrap());
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_maintenance_progress_wait_poison_reports_boundary_context() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let observer = session.pin_observer().unwrap();
            let target = engine.inner().trx_sys.purge_handoff_cts();
            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("maintenance wait poison"));

            for boundary in [
                MaintenanceBoundary::GcHorizon,
                MaintenanceBoundary::PurgeCompletion,
            ] {
                let error = wait_for_maintenance_boundary(&observer, target, boundary)
                    .await
                    .unwrap_err();
                let LifecycleOrFatalError::Fatal(error) = error else {
                    panic!("poisoned maintenance wait must remain Fatal")
                };
                assert_eq!(
                    error.downcast_ref::<FatalError>().copied(),
                    Some(FatalError::RedoWrite)
                );
                assert!(error.downcast_ref::<LifecycleError>().is_none());
                let report = format!("{error:?}");
                let expected = format!(
                    "maintenance progress wait observed engine poison: boundary={}, target_ts={target}",
                    boundary.name()
                );
                assert!(report.contains(&expected), "{report}");
            }
        });
    }

    #[test]
    fn test_session_checkpoint_catalog_persists_catalog_state() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(&main_dir))
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

            let engine = Engine::bootstrap(EngineConfig::default().storage_root(&main_dir))
                .await
                .unwrap();
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.truncate_redo_log().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_combined_catalog_redo_maintenance_requires_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session
                .checkpoint_catalog_and_truncate_redo_log()
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
    fn test_session_combined_catalog_redo_maintenance_publishes_checkpoint_and_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_combined_checkpoint_marker";
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let before = engine.inner().core.catalog().storage.checkpoint_snapshot();
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

            let after = engine.inner().core.catalog().storage.checkpoint_snapshot();
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

            let restarted =
                Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let checkpointed = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let after = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
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
                .inner()
                .core
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
                    .inner()
                    .core
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
            let checkpointed = engine
                .inner()
                .core
                .catalog()
                .storage
                .checkpointed_silent_watermarks();
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            session.checkpoint_catalog().await.unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let before = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(|file_seq, path| {
                    panic!(
                        "redo cleanup must not run after combined checkpoint failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                }),
            );

            let err = session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains(
                    "operation=checkpoint_catalog_and_truncate_redo_log, phase=wait_mandatory_completion"
                ),
                "{report}"
            );
            assert!(publish_hook.call_count() > 0);
            let after = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 2).await;
            let mut session = engine.new_session().unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            commit_redo_durability_anchor(&mut session, table_id).await;
            session.checkpoint_catalog().await.unwrap();

            let before = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(|file_seq, path| {
                    panic!(
                        "redo cleanup must not run after combined marker failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                }),
            );

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
            let after = engine.inner().core.catalog().storage.checkpoint_snapshot();
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut setup_session = engine.new_session().unwrap();
            setup_session.checkpoint_catalog().await.unwrap();
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let hook_called = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_called);
            let hook_catalog = engine.inner().core.catalog.clone();
            let hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(move |file_seq, _path| {
                    if file_seq != 0 {
                        return;
                    }
                    hook_flag.store(true, Ordering::SeqCst);
                    let catalog = &*hook_catalog;
                    let mut metadata_fut = Box::pin(catalog.acquire_index_metadata_change());
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    match metadata_fut.as_mut().poll(&mut cx) {
                        Poll::Ready(()) => {}
                        Poll::Pending => {
                            panic!("catalog gate should be released before redo cleanup")
                        }
                    }
                    catalog.release_index_metadata_change();
                }),
            );

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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut setup_session = engine.new_session().unwrap();
            setup_session.checkpoint_catalog().await.unwrap();
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let redo_retention_scope =
                RedoRetentionScope::acquire(engine.inner().trx_sys.clone()).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let state = engine
                .inner()
                .session_registry
                .session_state(session_id)
                .unwrap();
            let mut maintenance_fut = Box::pin(session.checkpoint_catalog_and_truncate_redo_log());

            assert!(matches!(
                futures::poll!(maintenance_fut.as_mut()),
                std::task::Poll::Pending
            ));
            let entry =
                active_operation_entry_for_test(&engine.inner().session_registry, session_id);
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert_eq!(engine.inner().mandatory_runtime.blocker_counts(), (0, 0));

            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));
            drop(redo_retention_scope);

            let err = maintenance_fut.await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite)
            );
            assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
            assert!(matches!(
                state.lifecycle.lock().slot,
                SessionOperationSlot::Idle
            ));
            assert_eq!(engine.inner().mandatory_runtime.blocker_counts(), (0, 0));
            let mut catalog_acquire = Box::pin(CatalogCheckpointScope::acquire(
                engine.inner().catalog.clone(),
            ));
            let Poll::Ready(catalog_scope) = futures::poll!(catalog_acquire.as_mut()) else {
                panic!("failed preparation must release catalog checkpoint authority")
            };
            let mut redo_acquire =
                Box::pin(RedoRetentionScope::acquire(engine.inner().trx_sys.clone()));
            let Poll::Ready(redo_scope) = futures::poll!(redo_acquire.as_mut()) else {
                panic!("failed preparation must release redo retention authority")
            };
            drop(catalog_scope);
            drop(redo_scope);
            assert!(
                obsolete_path.exists(),
                "obsolete redo file should not be removed after poison"
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            let table_id = create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            let mut session = engine.new_session().unwrap();
            session.checkpoint_catalog().await.unwrap();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();
            let expected_floor = table.redo_replay_floor_snapshot();
            drop(table);

            session.drop_table(table_id).await.unwrap();

            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table(table_id)
                    .await
                    .is_none()
            );
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let catalog = engine.inner().core.catalog();
            catalog.acquire_index_metadata_change().await;
            let mut session = engine.new_session().unwrap();
            let mut truncate_fut = Box::pin(session.truncate_redo_log());

            assert!(matches!(
                futures::poll!(truncate_fut.as_mut()),
                std::task::Poll::Pending
            ));

            catalog.release_index_metadata_change();
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
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

            let restarted =
                Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
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
            let err =
                match Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                    .await
                {
                    Ok(_) => {
                        panic!("engine startup should reject missing first retained redo file")
                    }
                    Err(err) => err,
                };
            assert_eq!(err.kind(), ErrorKind::Runtime, "{err:?}");
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::RedoLogAccess)
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
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
            let _cleanup_hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(|file_seq, path| {
                    panic!(
                        "redo cleanup must not run after marker publication failure: file_seq={file_seq}, path={}",
                        path.display()
                    );
                }),
            );

            let err = session.truncate_redo_log().await.unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Fatal);
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            assert!(publish_hook.call_count() > 0);
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .checkpoint_snapshot()
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let hook_called = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_called);
            let hook_catalog = engine.inner().core.catalog.clone();
            let hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(move |file_seq, _path| {
                    if file_seq != 0 {
                        return;
                    }
                    hook_flag.store(true, Ordering::SeqCst);
                    let catalog = &*hook_catalog;
                    let mut metadata_fut = Box::pin(catalog.acquire_index_metadata_change());
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    match metadata_fut.as_mut().poll(&mut cx) {
                        Poll::Ready(()) => {}
                        Poll::Pending => {
                            panic!("catalog gate should be released before redo cleanup")
                        }
                    }
                    catalog.release_index_metadata_change();
                }),
            );

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
    fn test_dropped_redo_truncation_observer_does_not_cancel_unlink() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let log_file_stem = "redo_truncate_observer_drop";
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(move |file_seq, _path| {
                    if file_seq == 0 {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                }),
            );

            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut truncate = Box::pin(session.truncate_redo_log());
            assert!(matches!(
                futures::poll!(truncate.as_mut()),
                std::task::Poll::Pending
            ));
            entered_rx.recv_async().await.unwrap();
            drop(truncate);

            release_tx.send_async(()).await.unwrap();
            wait_for_session_idle(&engine.inner().session_registry, session_id).await;
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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            assert!(obsolete_path.exists());

            let redo_retention_scope =
                RedoRetentionScope::acquire(engine.inner().trx_sys.clone()).await;
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
            drop(redo_retention_scope);

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
            let engine = Engine::bootstrap(redo_truncation_engine_config(&main_dir, log_file_stem))
                .await
                .unwrap();
            create_rotated_redo_table(&engine, &main_dir, log_file_stem, 1).await;
            engine
                .inner()
                .core
                .catalog()
                .storage
                .publish_first_redo_log_seq(1)
                .await
                .unwrap();
            let obsolete_path = redo_file_path(&main_dir, log_file_stem, 0);
            let hook_removed_file = Arc::new(AtomicBool::new(false));
            let hook_flag = Arc::clone(&hook_removed_file);
            let hook_path = obsolete_path.clone();
            let hook_guard = install_redo_cleanup_before_unlink_hook(
                &engine.inner().maintenance_test,
                Arc::new(move |file_seq, path| {
                    if file_seq == 0 && path == hook_path && !hook_flag.swap(true, Ordering::SeqCst)
                    {
                        fs::remove_file(path).unwrap();
                    }
                }),
            );

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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            let trx0 = session.transaction_system_stats().unwrap();
            let storage0 = session.storage_io_stats().unwrap();
            let pools0 = session.buffer_pool_stats().unwrap();
            let mandatory0 = session.mandatory_runtime_stats().unwrap();
            let logical0 = session.logical_lock_stats().unwrap();
            assert_eq!(trx0.commit_count, 0);
            assert_eq!(trx0.trx_count, 0);
            assert_eq!(trx0.log_bytes, 0);
            assert!(pools0.meta.capacity > 0);
            assert!(pools0.mem.capacity > 0);
            assert!(pools0.index.capacity > 0);
            assert!(pools0.disk.capacity > 0);
            assert_eq!(mandatory0, MandatoryRuntimeStats::default());

            let _table_id = table1(&engine).await;
            let trx1 = session.transaction_system_stats().unwrap();
            let storage1 = session.storage_io_stats().unwrap();
            let pools1 = session.buffer_pool_stats().unwrap();
            engine.inner().mandatory_runtime.drain_callers().await;
            let mandatory1 = session.mandatory_runtime_stats().unwrap();
            let logical1 = session.logical_lock_stats().unwrap();
            // Commit waiters can complete before the redo thread publishes
            // aggregate stats, so this test verifies monotonic snapshots
            // rather than immediate progress from the preceding operation.
            assert_transaction_system_stats_monotonic(trx0, trx1);
            assert!(storage1.backend.submitted_ops >= storage0.backend.submitted_ops);
            assert!(storage1.table_read_requests >= storage0.table_read_requests);
            assert!(storage1.pool_read_requests >= storage0.pool_read_requests);
            assert!(storage1.background_write_requests >= storage0.background_write_requests);
            assert_buffer_pool_stats_monotonic(&pools0, &pools1);
            assert_eq!(mandatory1.operation.submitted_count, 1);
            assert_eq!(mandatory1.operation.started_count, 1);
            assert_eq!(mandatory1.operation.completed_count, 1);
            assert_eq!(mandatory1.operation.error_count, 0);
            assert_eq!(mandatory1.operation.panic_count, 0);
            assert_eq!(mandatory1.operation.detached_observer_count, 0);
            assert_eq!(mandatory1.operation.active_count, 0);
            assert_eq!(
                mandatory1.transaction_cleanup,
                MandatoryTaskStats::default()
            );
            assert!(logical1.resource_transitions >= logical0.resource_transitions);
            assert!(
                logical1.immediate_physical_acquisitions
                    >= logical0.immediate_physical_acquisitions
            );
        });
    }

    #[test]
    fn test_session_query_methods_require_registered_running_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            remove_session_for_test(&engine.inner().session_registry, session.id());

            for err in [
                session.list_table_ids().unwrap_err(),
                session.transaction_system_stats().unwrap_err(),
                session.storage_io_stats().unwrap_err(),
                session.buffer_pool_stats().unwrap_err(),
                session.mandatory_runtime_stats().unwrap_err(),
                session.logical_lock_stats().unwrap_err(),
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
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            engine.shutdown();

            assert_runtime_unavailable_after_shutdown(session.list_table_ids().unwrap_err());
            assert_runtime_unavailable_after_shutdown(
                session.transaction_system_stats().unwrap_err(),
            );
            assert_runtime_unavailable_after_shutdown(session.storage_io_stats().unwrap_err());
            assert_runtime_unavailable_after_shutdown(session.buffer_pool_stats().unwrap_err());
            assert_runtime_unavailable_after_shutdown(
                session.mandatory_runtime_stats().unwrap_err(),
            );
            assert_runtime_unavailable_after_shutdown(session.logical_lock_stats().unwrap_err());
        });
    }

    #[test]
    fn test_session_diagnostics_remain_visible_after_storage_poison() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();

            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));

            let err = match session.pin_observer().disclose() {
                Ok(_) => panic!("normal observer admission must reject storage poison"),
                Err(err) => err,
            };
            assert_fatal_admission_error(err, FatalError::RedoWrite);

            assert_eq!(session.list_table_ids().unwrap(), vec![table_id]);
            assert!(session.transaction_system_stats().is_ok());
            assert!(session.storage_io_stats().is_ok());
            assert!(session.buffer_pool_stats().is_ok());
            assert!(session.mandatory_runtime_stats().is_ok());
            assert!(session.logical_lock_stats().is_ok());

            let err = session.truncate_redo_log().await.unwrap_err();
            assert_fatal_admission_error(err, FatalError::RedoWrite);

            let err = session.checkpoint_catalog().await.unwrap_err();
            assert_fatal_admission_error(err, FatalError::RedoWrite);
        });
    }
}
