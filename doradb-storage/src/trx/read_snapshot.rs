use super::{
    MvccReadView, MvccVisibility, ResolvedTableReadBinding, resolve_table_read_binding,
    sys::TransactionSystem,
};
use crate::buffer::PoolGuards;
use crate::catalog::ResolvedLiveMetadata;
use crate::error::{
    DiscloseError, DiscloseResultExt, LifecycleError, LifecycleOrFatalResult, LifecycleResult,
    MultiDomainResultExt, OperationError, OperationResult, QuadResult, Result,
};
use crate::id::{BlockID, RowID, SessionOperationKey, TableID, TrxID};
use crate::lock::{FamilyLockAuthority, LockMode, LockOwner, LockResource, LockScopeState};
use crate::map::{FastHashMap, FastHashSet};
use crate::quiescent::QuiescentGuard;
use crate::session::{SessionRuntime, WeakSessionRef};
use crate::table::{
    CheckedOutTableScanRoot, CompiledTableScanPlan, DmlValidator, OwnedTableScanRoot, Table,
    TableRuntimeLayout, TableScanPartitionStream, TableScanRuntime, TableScanUnit,
    compile_table_scan_plan, repartition_table_scan_offsets,
};
use error_stack::{Report, ResultExt};
use event_listener::{Event, EventListener};
use futures::future::{Either, select};
use parking_lot::Mutex;
use std::cell::Cell;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem::replace;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
pub(crate) use tests::TableScanPlanTestController;

/// One RAII registration in the transaction system's active GC horizon.
///
/// This owner carries no transaction capabilities. Its STS protects captured
/// roots and MVCC history until the registration is dropped.
pub(crate) struct ActiveSnapshotRegistration {
    trx_sys: QuiescentGuard<TransactionSystem>,
    sts: TrxID,
    gc_no: usize,
}

impl ActiveSnapshotRegistration {
    /// Return the registered snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.sts
    }
}

impl Drop for ActiveSnapshotRegistration {
    #[inline]
    fn drop(&mut self) {
        self.trx_sys.deregister_active_sts(self.gc_no, self.sts);
    }
}

/// Maintenance-only snapshot registered in the active GC horizon.
///
/// This owner carries no transaction capabilities or stable session child
/// state. Its active STS protects root snapshots borrowed from it until drop.
pub(crate) struct PrivateSnapshot(ActiveSnapshotRegistration);

impl PrivateSnapshot {
    /// Return the registered snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.0.sts()
    }
}

impl QuiescentGuard<TransactionSystem> {
    /// Register one ownerless snapshot in the active GC horizon.
    #[inline]
    pub(crate) fn register_active_snapshot(&self) -> ActiveSnapshotRegistration {
        let (gc_no, sts) = self.register_active_sts();
        ActiveSnapshotRegistration {
            trx_sys: self.clone(),
            sts,
            gc_no,
        }
    }

    /// Register one mandatory-maintenance snapshot in the active GC horizon.
    #[inline]
    pub(crate) fn register_private_snapshot(&self) -> PrivateSnapshot {
        PrivateSnapshot(self.register_active_snapshot())
    }
}

/// Why a read snapshot was sealed and moved toward terminal cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadSnapshotDrainReason {
    BuilderDropped,
    ExplicitClose,
    FinalFacadeDrop,
    SessionClose,
    SessionAbandoned,
    EngineShutdown,
    ExecutionFailed,
}

/// Registry-visible phase of one typed read-snapshot entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadSnapshotPhase {
    BuildingAvailable,
    BuildingCheckedOut,
    Ready,
    Draining,
    CompletingAvailable,
    CompletingCheckedOut,
    Terminal,
}

impl ReadSnapshotPhase {
    /// Stable diagnostic label for this snapshot phase.
    #[inline]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::BuildingAvailable => "building_available",
            Self::BuildingCheckedOut => "building_checked_out",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::CompletingAvailable => "completing_available",
            Self::CompletingCheckedOut => "completing_checked_out",
            Self::Terminal => "terminal",
        }
    }
}

/// One table binding frozen into a shared read snapshot.
struct SnapshotTableBinding {
    visible: ResolvedLiveMetadata,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    root: OwnedTableScanRoot,
}

/// Logical-lock resources retained by a building or ready snapshot.
pub(crate) struct ReadSnapshotLockOwner {
    authority: Box<FamilyLockAuthority>,
    metadata_scope: LockScopeState,
}

impl ReadSnapshotLockOwner {
    /// Create logical-lock ownership for one exact snapshot operation.
    #[inline]
    pub(crate) fn new(authority: Box<FamilyLockAuthority>, key: SessionOperationKey) -> Self {
        Self {
            authority,
            metadata_scope: LockScopeState::new(LockOwner::operation(key)),
        }
    }

    #[inline]
    fn parts(&mut self) -> (&mut FamilyLockAuthority, &mut LockScopeState) {
        (&mut self.authority, &mut self.metadata_scope)
    }

    #[inline]
    fn cleanup(mut self, runtime: &SessionRuntime) -> Box<FamilyLockAuthority> {
        self.authority
            .family_mut()
            .close_scope(&mut self.metadata_scope, runtime.lock_manager());
        self.authority.assert_idle();
        self.authority
    }
}

/// Mutable resources exclusively checked out by the consuming builder.
pub(crate) struct ReadSnapshotBuildCore {
    bindings: FastHashMap<TableID, SnapshotTableBinding>,
    read_view: MvccReadView,
    active_sts: ActiveSnapshotRegistration,
    locks: ReadSnapshotLockOwner,
}

impl ReadSnapshotBuildCore {
    /// Build an empty core after its STS registration has succeeded.
    #[inline]
    pub(crate) fn new(
        active_sts: ActiveSnapshotRegistration,
        locks: ReadSnapshotLockOwner,
    ) -> Self {
        let sts = active_sts.sts();
        Self {
            bindings: FastHashMap::default(),
            read_view: MvccReadView::ownerless(sts),
            active_sts,
            locks,
        }
    }

    #[inline]
    fn freeze(self) -> ReadSnapshotReadyPayload {
        let Self {
            bindings,
            read_view,
            active_sts,
            locks,
        } = self;
        ReadSnapshotReadyPayload {
            read_core: Arc::new(FrozenReadSnapshotCore {
                bindings,
                read_view,
                execution: SnapshotExecutionControl::new(),
                active_sts,
            }),
            locks,
        }
    }

    #[inline]
    fn cleanup(self, runtime: &SessionRuntime) -> Box<FamilyLockAuthority> {
        let Self {
            bindings,
            read_view,
            active_sts,
            locks,
        } = self;
        drop(bindings);
        drop(read_view);
        drop(active_sts);
        locks.cleanup(runtime)
    }
}

/// Immutable snapshot data shared only by counted internal checkouts.
pub(crate) struct FrozenReadSnapshotCore {
    bindings: FastHashMap<TableID, SnapshotTableBinding>,
    read_view: MvccReadView,
    execution: SnapshotExecutionControl,
    // Keep registration last so bindings and owned roots drop first.
    active_sts: ActiveSnapshotRegistration,
}

/// First execution error context shared by every stream in one snapshot.
#[derive(Clone, Copy)]
pub(crate) struct SnapshotExecutionFailure {
    /// Table whose partition produced the first error.
    pub(crate) table_id: TableID,
    /// Zero-based partition that produced the first error.
    pub(crate) partition_idx: usize,
}

struct SnapshotExecutionControl {
    failed: AtomicBool,
    first_failure: Mutex<Option<SnapshotExecutionFailure>>,
}

impl SnapshotExecutionControl {
    #[inline]
    fn new() -> Self {
        Self {
            failed: AtomicBool::new(false),
            first_failure: Mutex::new(None),
        }
    }

    #[inline]
    fn is_healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire)
    }

    #[inline]
    fn failure(&self) -> Option<SnapshotExecutionFailure> {
        if self.is_healthy() {
            return None;
        }
        Some(self.first_failure.lock().unwrap_or_else(|| {
            panic!("failed snapshot execution control is missing first-failure context")
        }))
    }

    #[inline]
    fn publish_first_failure(&self, failure: SnapshotExecutionFailure) -> bool {
        let mut first_failure = self.first_failure.lock();
        if self.failed.load(Ordering::Acquire) {
            return false;
        }
        assert!(
            first_failure.is_none(),
            "healthy snapshot execution control already contains failure context"
        );
        *first_failure = Some(failure);
        self.failed.store(true, Ordering::Release);
        true
    }
}

/// Checked-in ready payload; lock mutation remains registry-authoritative.
pub(crate) struct ReadSnapshotReadyPayload {
    read_core: Arc<FrozenReadSnapshotCore>,
    locks: ReadSnapshotLockOwner,
}

impl ReadSnapshotReadyPayload {
    #[inline]
    fn cleanup(self, runtime: &SessionRuntime) -> Box<FamilyLockAuthority> {
        let Self { read_core, locks } = self;
        assert_eq!(
            Arc::strong_count(&read_core),
            1,
            "terminal snapshot cleanup requires all checkout pins to return"
        );
        assert_eq!(
            read_core.active_sts.sts(),
            read_core.read_view.sts(),
            "frozen snapshot registration and read view must share one STS"
        );
        drop(read_core);
        locks.cleanup(runtime)
    }
}

enum ReadSnapshotTerminalPayload {
    Building(ReadSnapshotBuildCore),
    Ready(ReadSnapshotReadyPayload),
}

impl ReadSnapshotTerminalPayload {
    #[inline]
    fn cleanup(self, runtime: &SessionRuntime) -> Box<FamilyLockAuthority> {
        match self {
            Self::Building(core) => core.cleanup(runtime),
            Self::Ready(payload) => payload.cleanup(runtime),
        }
    }
}

enum ReadSnapshotEntryState {
    BuildingAvailable(ReadSnapshotBuildCore),
    BuildingCheckedOut {
        abort: Option<ReadSnapshotDrainReason>,
    },
    Ready {
        payload: ReadSnapshotReadyPayload,
        active_checkouts: usize,
    },
    Draining {
        payload: ReadSnapshotReadyPayload,
        active_checkouts: usize,
        reason: ReadSnapshotDrainReason,
    },
    CompletingAvailable(ReadSnapshotTerminalPayload),
    CompletingCheckedOut,
    Terminal,
}

impl ReadSnapshotEntryState {
    #[inline]
    const fn phase(&self) -> ReadSnapshotPhase {
        match self {
            Self::BuildingAvailable(_) => ReadSnapshotPhase::BuildingAvailable,
            Self::BuildingCheckedOut { .. } => ReadSnapshotPhase::BuildingCheckedOut,
            Self::Ready { .. } => ReadSnapshotPhase::Ready,
            Self::Draining { .. } => ReadSnapshotPhase::Draining,
            Self::CompletingAvailable(_) => ReadSnapshotPhase::CompletingAvailable,
            Self::CompletingCheckedOut => ReadSnapshotPhase::CompletingCheckedOut,
            Self::Terminal => ReadSnapshotPhase::Terminal,
        }
    }
}

struct ReadSnapshotEntryInner {
    state: ReadSnapshotEntryState,
}

/// Pointer-stable registry entry for one read snapshot operation.
pub(crate) struct ReadSnapshotEntry {
    key: SessionOperationKey,
    sts: TrxID,
    inner: Mutex<ReadSnapshotEntryInner>,
    abort_ev: Event,
}

impl ReadSnapshotEntry {
    /// Create a checked-in building snapshot entry.
    #[inline]
    pub(crate) fn new(
        key: SessionOperationKey,
        sts: TrxID,
        core: ReadSnapshotBuildCore,
    ) -> Arc<Self> {
        assert_eq!(
            sts,
            core.active_sts.sts(),
            "snapshot entry STS must match its active registration: key={key}"
        );
        Arc::new(Self {
            key,
            sts,
            inner: Mutex::new(ReadSnapshotEntryInner {
                state: ReadSnapshotEntryState::BuildingAvailable(core),
            }),
            abort_ev: Event::new(),
        })
    }

    /// Return the exact operation key.
    #[inline]
    pub(crate) const fn key(&self) -> SessionOperationKey {
        self.key
    }

    /// Return the registered snapshot timestamp.
    #[inline]
    pub(crate) const fn sts(&self) -> TrxID {
        self.sts
    }

    /// Inspect the current snapshot phase.
    #[inline]
    pub(crate) fn phase(&self) -> ReadSnapshotPhase {
        self.inner.lock().state.phase()
    }

    /// Return whether the exact ready snapshot still accepts execution work.
    #[inline]
    pub(crate) fn execution_healthy(&self) -> bool {
        let inner = self.inner.lock();
        match &inner.state {
            ReadSnapshotEntryState::Ready { payload, .. } => {
                payload.read_core.execution.is_healthy()
            }
            _ => false,
        }
    }

    /// Register the exact abort listener before rechecking sticky build abort.
    #[inline]
    fn abort_listener(&self) -> (EventListener, Option<ReadSnapshotDrainReason>) {
        let inner = self.inner.lock();
        let listener = self.abort_ev.listen();
        let ReadSnapshotEntryState::BuildingCheckedOut { abort } = &inner.state else {
            panic!(
                "snapshot build abort listener requires checked-out state: key={}, phase={}",
                self.key,
                inner.state.phase().label()
            );
        };
        (listener, *abort)
    }

    /// Return whether the checked-out build has a sticky abort request.
    #[inline]
    fn build_abort(&self) -> Option<ReadSnapshotDrainReason> {
        let inner = self.inner.lock();
        let ReadSnapshotEntryState::BuildingCheckedOut { abort } = &inner.state else {
            panic!(
                "snapshot build abort inspection requires checked-out state: key={}, phase={}",
                self.key,
                inner.state.phase().label()
            );
        };
        *abort
    }

    /// Move the checked-in build core into one exclusive checkout.
    #[inline]
    pub(crate) fn take_build(&self) -> LifecycleResult<ReadSnapshotBuildCore> {
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        match state {
            ReadSnapshotEntryState::BuildingAvailable(core) => {
                inner.state = ReadSnapshotEntryState::BuildingCheckedOut { abort: None };
                Ok(core)
            }
            state => {
                let phase = state.phase();
                inner.state = state;
                Err(
                    Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                        "operation_key={}, phase={}",
                        self.key,
                        phase.label()
                    )),
                )
            }
        }
    }

    /// Seal this snapshot and make checked-in ownership claimable when possible.
    #[inline]
    pub(crate) fn request_drain(&self, reason: ReadSnapshotDrainReason) {
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        let notify_abort = matches!(
            state,
            ReadSnapshotEntryState::BuildingCheckedOut { abort: None }
        );
        inner.state = match state {
            ReadSnapshotEntryState::BuildingAvailable(core) => {
                ReadSnapshotEntryState::CompletingAvailable(ReadSnapshotTerminalPayload::Building(
                    core,
                ))
            }
            ReadSnapshotEntryState::BuildingCheckedOut { abort } => {
                ReadSnapshotEntryState::BuildingCheckedOut {
                    abort: abort.or(Some(reason)),
                }
            }
            ReadSnapshotEntryState::Ready {
                payload,
                active_checkouts: 0,
            } => ReadSnapshotEntryState::CompletingAvailable(ReadSnapshotTerminalPayload::Ready(
                payload,
            )),
            ReadSnapshotEntryState::Ready {
                payload,
                active_checkouts,
            } => ReadSnapshotEntryState::Draining {
                payload,
                active_checkouts,
                reason,
            },
            state @ (ReadSnapshotEntryState::Draining { .. }
            | ReadSnapshotEntryState::CompletingAvailable(_)
            | ReadSnapshotEntryState::CompletingCheckedOut
            | ReadSnapshotEntryState::Terminal) => state,
        };
        drop(inner);
        if notify_abort {
            self.abort_ev.notify(usize::MAX);
        }
    }

    /// Return a cancelled or failed exclusive build into terminal ownership.
    #[inline]
    fn return_failed_build(&self, core: ReadSnapshotBuildCore) {
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        match state {
            ReadSnapshotEntryState::BuildingCheckedOut { .. } => {
                inner.state = ReadSnapshotEntryState::CompletingAvailable(
                    ReadSnapshotTerminalPayload::Building(core),
                );
            }
            state => {
                inner.state = state;
                panic!(
                    "snapshot build return requires checked-out state: key={}, phase={}",
                    self.key,
                    inner.state.phase().label()
                );
            }
        }
    }

    /// Publish the completed payload while the session lifecycle lock is held.
    #[inline]
    pub(crate) fn publish_ready(
        &self,
        payload: ReadSnapshotReadyPayload,
        admit_ready: bool,
    ) -> bool {
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        let published = matches!(
            state,
            ReadSnapshotEntryState::BuildingCheckedOut { abort: None }
        ) && admit_ready;
        inner.state = match (state, published) {
            (ReadSnapshotEntryState::BuildingCheckedOut { .. }, true) => {
                ReadSnapshotEntryState::Ready {
                    payload,
                    active_checkouts: 0,
                }
            }
            (ReadSnapshotEntryState::BuildingCheckedOut { .. }, false) => {
                ReadSnapshotEntryState::CompletingAvailable(ReadSnapshotTerminalPayload::Ready(
                    payload,
                ))
            }
            (state, _) => {
                let phase = state.phase();
                inner.state = state;
                panic!(
                    "snapshot publication requires checked-out build: key={}, phase={}",
                    self.key,
                    phase.label()
                );
            }
        };
        published
    }

    /// Count and clone the frozen core from an exact ready snapshot.
    #[inline]
    pub(crate) fn checkout_ready(&self) -> LifecycleResult<Arc<FrozenReadSnapshotCore>> {
        let mut inner = self.inner.lock();
        let ReadSnapshotEntryState::Ready {
            payload,
            active_checkouts,
        } = &mut inner.state
        else {
            return Err(
                Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                    "operation_key={}, phase={}",
                    self.key,
                    inner.state.phase().label()
                )),
            );
        };
        if !payload.read_core.execution.is_healthy() {
            return Err(
                Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                    "operation_key={}, reason=execution_failed",
                    self.key
                )),
            );
        }
        *active_checkouts = active_checkouts
            .checked_add(1)
            .unwrap_or_else(|| panic!("read snapshot checkout count overflow: key={}", self.key));
        Ok(Arc::clone(&payload.read_core))
    }

    /// Return one counted checkout and expose terminal ownership at the drain edge.
    #[inline]
    pub(crate) fn return_checkout(&self) {
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        inner.state = match state {
            ReadSnapshotEntryState::Ready {
                payload,
                active_checkouts,
            } => {
                let active_checkouts = active_checkouts.checked_sub(1).unwrap_or_else(|| {
                    panic!("read snapshot checkout count underflow: key={}", self.key)
                });
                ReadSnapshotEntryState::Ready {
                    payload,
                    active_checkouts,
                }
            }
            ReadSnapshotEntryState::Draining {
                payload,
                active_checkouts,
                reason,
            } => {
                let active_checkouts = active_checkouts.checked_sub(1).unwrap_or_else(|| {
                    panic!("read snapshot checkout count underflow: key={}", self.key)
                });
                if active_checkouts == 0 {
                    ReadSnapshotEntryState::CompletingAvailable(ReadSnapshotTerminalPayload::Ready(
                        payload,
                    ))
                } else {
                    ReadSnapshotEntryState::Draining {
                        payload,
                        active_checkouts,
                        reason,
                    }
                }
            }
            state => {
                let phase = state.phase();
                inner.state = state;
                panic!(
                    "snapshot checkout return requires ready or draining state: key={}, phase={}",
                    self.key,
                    phase.label()
                );
            }
        };
    }

    /// Claim the sole checked-in terminal payload.
    #[inline]
    pub(crate) fn claim_terminal(
        self: &Arc<Self>,
        runtime: SessionRuntime,
    ) -> Option<ReadSnapshotTerminalClaim> {
        assert!(
            runtime.state().id() == self.key.session_id(),
            "snapshot terminal claim session/key mismatch: session_id={}, operation_key={}",
            runtime.state().id(),
            self.key
        );
        let mut inner = self.inner.lock();
        let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
        let ReadSnapshotEntryState::CompletingAvailable(payload) = state else {
            inner.state = state;
            return None;
        };
        inner.state = ReadSnapshotEntryState::CompletingCheckedOut;
        Some(ReadSnapshotTerminalClaim {
            runtime,
            entry: Arc::clone(self),
            payload: Some(payload),
        })
    }

    /// Publish terminal after the complete payload has been cleaned.
    #[inline]
    pub(crate) fn publish_terminal(&self) {
        let mut inner = self.inner.lock();
        assert!(
            matches!(inner.state, ReadSnapshotEntryState::CompletingCheckedOut),
            "snapshot terminal publication requires terminal claim: key={}, phase={}",
            self.key,
            inner.state.phase().label()
        );
        inner.state = ReadSnapshotEntryState::Terminal;
    }
}

/// Sole owner that performs ordered synchronous snapshot cleanup.
#[must_use = "a read-snapshot terminal claim must complete synchronous cleanup"]
pub(crate) struct ReadSnapshotTerminalClaim {
    runtime: SessionRuntime,
    entry: Arc<ReadSnapshotEntry>,
    payload: Option<ReadSnapshotTerminalPayload>,
}

impl ReadSnapshotTerminalClaim {
    /// Drop roots and STS, close metadata scope, and publish the exact terminal slot.
    #[inline]
    pub(crate) fn cleanup(mut self) {
        self.cleanup_inner(true);
    }

    /// Complete cleanup during a registry scan without mutating that registry.
    #[inline]
    pub(crate) fn cleanup_during_registry_scan(mut self) {
        self.cleanup_inner(false);
    }

    #[inline]
    fn cleanup_inner(&mut self, remove_from_registry: bool) {
        let payload = self.payload.take().unwrap_or_else(|| {
            panic!(
                "snapshot terminal claim is missing payload: key={}",
                self.entry.key()
            )
        });
        let authority = payload.cleanup(&self.runtime);
        let remove = self
            .runtime
            .state()
            .finish_read_snapshot_terminal(&self.entry, authority);
        if remove_from_registry {
            self.runtime.remove_if_requested(remove);
        }
    }
}

impl Drop for ReadSnapshotTerminalClaim {
    #[inline]
    fn drop(&mut self) {
        if let Some(payload) = self.payload.take() {
            let mut inner = self.entry.inner.lock();
            assert!(
                matches!(inner.state, ReadSnapshotEntryState::CompletingCheckedOut),
                "unconsumed snapshot terminal claim requires checked-out completion: key={}, phase={}",
                self.entry.key(),
                inner.state.phase().label()
            );
            // Preserve the complete cleanup owner before surfacing the invariant.
            // If this drop is already unwinding another panic, that panic is the
            // owning failure and a second panic here would abort the process.
            inner.state = ReadSnapshotEntryState::CompletingAvailable(payload);
            drop(inner);
            assert!(
                std::thread::panicking(),
                "read-snapshot terminal claim dropped without synchronous cleanup: key={}",
                self.entry.key()
            );
        }
    }
}

/// Weak, one-shot owner for preparing a shared read snapshot.
pub struct ReadSnapshotBuilder {
    session: WeakSessionRef,
    key: SessionOperationKey,
    sts: TrxID,
    armed: Cell<bool>,
}

impl ReadSnapshotBuilder {
    /// Construct a builder for one already-registered stable entry.
    #[inline]
    pub(crate) fn new(session: WeakSessionRef, key: SessionOperationKey, sts: TrxID) -> Self {
        Self {
            session,
            key,
            sts,
            armed: Cell::new(true),
        }
    }

    /// Return the registered snapshot timestamp.
    #[inline]
    pub const fn sts(&self) -> TrxID {
        self.sts
    }

    /// Acquire and freeze the complete user-table set for this snapshot.
    pub async fn acquire_tables<I>(self, input_table_ids: I) -> Result<ReadSnapshot>
    where
        I: IntoIterator<Item = TableID>,
    {
        self.acquire_tables_inner(input_table_ids).await.disclose()
    }

    async fn acquire_tables_inner<I>(self, input_table_ids: I) -> QuadResult<ReadSnapshot>
    where
        I: IntoIterator<Item = TableID>,
    {
        let mut unique = FastHashSet::default();
        let mut table_ids = Vec::new();
        for table_id in input_table_ids {
            if unique.insert(table_id) {
                table_ids.push(table_id);
            }
        }
        if table_ids.is_empty() {
            return Err(Report::new(OperationError::InvalidReadSnapshotInput)
                .attach(format!("operation_key={}", self.key))
                .into());
        }
        if let Some(table_id) = table_ids.iter().copied().find(|id| id.is_catalog()) {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!(
                    "operation=acquire_read_snapshot, table_id={table_id}"
                ))
                .into());
        }

        // End the non-Send admitted wrapper's storage scope before the first
        // await. Although `into_runtime` consumes the value and releases
        // admission, keeping its binding in this async scope would make the
        // generated future conservatively retain a non-Send coroutine field.
        let (runtime, mut checkout) = {
            let admitted = self
                .session
                .upgrade()
                .attach_with(|| format!("operation_key={}", self.key))?
                .ok_or_else(|| {
                    Report::new(LifecycleError::ReadSnapshotUnavailable).attach(format!(
                        "operation_key={}, reason=session_missing",
                        self.key
                    ))
                })?;
            admitted
                .runtime()
                .poisoner
                .ensure_healthy()
                .attach_with(|| format!("operation_key={}, phase=build_checkout", self.key))?;
            let (entry, core) = admitted
                .runtime()
                .state()
                .checkout_read_snapshot_build(self.key)
                .attach_with(|| format!("operation_key={}", self.key))?;
            let runtime = admitted.into_runtime();
            self.armed.set(false);
            let checkout = ReadSnapshotBuildCheckout {
                runtime: runtime.clone(),
                entry,
                core: Some(core),
            };
            (runtime, checkout)
        };

        for table_id in table_ids {
            checkout.acquire_table(table_id).await?;
        }

        runtime
            .poisoner
            .ensure_healthy()
            .attach_with(|| format!("operation_key={}, phase=publish_ready", self.key))?;
        let payload = checkout.take_core().freeze();
        let published = runtime
            .state()
            .publish_read_snapshot_ready(&checkout.entry, payload);
        let claim = runtime.claim_read_snapshot_terminal(&checkout.entry);
        drop(checkout);
        if let Some(claim) = claim {
            claim.cleanup();
        }
        if !published {
            return Err(Report::new(LifecycleError::ReadSnapshotUnavailable)
                .attach(format!("operation_key={}, phase=publish_ready", self.key))
                .into());
        }
        let group = Arc::new(ReadSnapshotFacadeGroup {
            session: self.session.clone(),
            key: self.key,
            sts: self.sts,
            closed: AtomicBool::new(false),
        });
        Ok(ReadSnapshot { group })
    }
}

impl Drop for ReadSnapshotBuilder {
    #[inline]
    fn drop(&mut self) {
        if self.armed.replace(false)
            && let Some(runtime) = self.session.upgrade_for_terminal()
        {
            runtime.request_read_snapshot_close(self.key, ReadSnapshotDrainReason::BuilderDropped);
        }
    }
}

struct ReadSnapshotBuildCheckout {
    runtime: SessionRuntime,
    entry: Arc<ReadSnapshotEntry>,
    core: Option<ReadSnapshotBuildCore>,
}

impl ReadSnapshotBuildCheckout {
    async fn acquire_table(&mut self, table_id: TableID) -> QuadResult<()> {
        let (listener, abort) = self.entry.abort_listener();
        if abort.is_some() {
            return Err(read_snapshot_unavailable(self.entry.key(), "before_metadata_wait").into());
        }

        let engine = self.runtime.core();
        let core = self.core.as_mut().unwrap_or_else(|| {
            panic!(
                "snapshot build checkout is missing core: key={}",
                self.entry.key()
            )
        });
        let (authority, metadata_scope) = core.locks.parts();
        let acquisition = authority.family_mut().acquire(
            metadata_scope,
            engine.lock_manager(),
            &engine.poisoner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
        );
        futures::pin_mut!(acquisition);
        futures::pin_mut!(listener);
        match select(acquisition, listener).await {
            Either::Left((result, _listener)) => {
                result.attach_with(|| {
                    format!(
                        "operation=acquire_read_snapshot, operation_key={}, table_id={table_id}",
                        self.entry.key()
                    )
                })?;
            }
            Either::Right((_notification, _acquisition)) => {
                return Err(
                    read_snapshot_unavailable(self.entry.key(), "metadata_wait_aborted").into(),
                );
            }
        }

        if self.entry.build_abort().is_some() {
            return Err(read_snapshot_unavailable(self.entry.key(), "after_metadata_grant").into());
        }
        let ResolvedTableReadBinding {
            visible,
            current_effective_cts: _,
            table,
            layout,
        } = resolve_table_read_binding(
            engine,
            core.active_sts.sts(),
            table_id,
            "acquire_read_snapshot",
        )?;
        self.runtime.state().cache_user_table(&table);
        let root = table.capture_owned_scan_root(&core.active_sts);
        let previous = core.bindings.insert(
            table_id,
            SnapshotTableBinding {
                visible,
                table,
                layout,
                root,
            },
        );
        assert!(
            previous.is_none(),
            "deduplicated snapshot build replaced a table binding: key={}, table_id={table_id}",
            self.entry.key()
        );
        Ok(())
    }

    #[inline]
    fn take_core(&mut self) -> ReadSnapshotBuildCore {
        self.core.take().unwrap_or_else(|| {
            panic!(
                "snapshot build checkout is missing core: key={}",
                self.entry.key()
            )
        })
    }
}

impl Drop for ReadSnapshotBuildCheckout {
    #[inline]
    fn drop(&mut self) {
        let Some(core) = self.core.take() else {
            return;
        };
        self.entry.return_failed_build(core);
        if let Some(claim) = self.runtime.claim_read_snapshot_terminal(&self.entry) {
            claim.cleanup();
        }
    }
}

/// Cloneable weak facade over one registry-owned shared read snapshot.
#[derive(Clone)]
pub struct ReadSnapshot {
    group: Arc<ReadSnapshotFacadeGroup>,
}

impl ReadSnapshot {
    /// Return the registered snapshot timestamp.
    #[inline]
    pub fn sts(&self) -> TrxID {
        self.group.sts
    }

    /// Count and pin one immutable snapshot checkout.
    #[inline]
    pub(crate) fn checkout(&self) -> LifecycleOrFatalResult<ReadSnapshotCheckout> {
        ReadSnapshotCheckout::open(&self.group)
    }

    /// Prepare one deterministic table-scan plan at this snapshot.
    pub async fn prepare_table_scan(
        &self,
        table_id: TableID,
        options: TableScanOptions,
    ) -> Result<TableScanPlan> {
        self.prepare_table_scan_inner(table_id, options)
            .await
            .disclose()
    }

    async fn prepare_table_scan_inner(
        &self,
        table_id: TableID,
        options: TableScanOptions,
    ) -> QuadResult<TableScanPlan> {
        let checkout = self.checkout()?;
        let operation_key = checkout.entry.key();
        let config = checkout.runtime.core().table_scan_config();
        let read_view = MvccReadView::ownerless(self.sts());
        let worklist = {
            let table = checkout.table(table_id)?;
            DmlValidator::new(table.visible_metadata().metadata())
                .validate_projection(&options.projection)
                .change_context(OperationError::InvalidTableScanInput)
                .attach_with(|| {
                    format!(
                        "operation=prepare_table_scan, operation_key={operation_key}, table_id={table_id}"
                    )
                })?;
            table
                .table()
                .accessor_with_layout(table.layout())
                .table_scan_mvcc_worklist(
                    TableScanRuntime::new(checkout.runtime.pool_guards()),
                    table.root(),
                    &read_view,
                )
                .await
                .attach_with(|| {
                    format!(
                        "operation=prepare_table_scan, operation_key={operation_key}, table_id={table_id}"
                    )
                })?
        };
        #[cfg(test)]
        checkout
            .runtime
            .core()
            .table_scan_plan_test
            .after_worklist_capture()
            .await;
        let compiled = compile_table_scan_plan(worklist, config);
        let plan = TableScanPlan::new(
            Arc::clone(&self.group),
            operation_key,
            self.sts(),
            table_id,
            options.projection,
            compiled,
        );
        checkout
            .runtime
            .state()
            .admit_read_snapshot_plan_publication(&checkout.entry, &self.group.closed)
            .attach_with(|| {
                format!(
                    "operation=prepare_table_scan, operation_key={operation_key}, table_id={table_id}, phase=publish_plan"
                )
            })?;
        drop(checkout);
        Ok(plan)
    }

    /// Seal the shared facade group and wait for exact terminal cleanup.
    pub async fn close(self) -> Result<()> {
        self.group
            .request_close(ReadSnapshotDrainReason::ExplicitClose);
        loop {
            let listener = {
                let Some(runtime) = self.group.session.upgrade_for_terminal() else {
                    return Ok(());
                };
                let Some(listener) = runtime
                    .state()
                    .read_snapshot_terminal_listener(self.group.key)
                else {
                    return Ok(());
                };
                // The listener owns its wake registration independently. End
                // strong session/runtime reachability before suspending so a
                // terminal close future cannot become a hidden shutdown owner.
                listener
            };
            listener.await;
        }
    }
}

/// Projection input for deterministic shared-snapshot table-scan planning.
pub struct TableScanOptions {
    /// Strictly increasing snapshot-visible table column numbers.
    pub projection: Vec<usize>,
}

struct TableScanPlanShared {
    liveness: Arc<ReadSnapshotFacadeGroup>,
    gate: PlanFamilyGate,
    operation_key: SessionOperationKey,
    sts: TrxID,
    table_id: TableID,
    column_root: BlockID,
    pivot_row_id: RowID,
    projection: Arc<[usize]>,
    units: Arc<[TableScanUnit]>,
    weight_prefix: Arc<[u64]>,
}

/// Cloneable immutable table-scan plan for one exact snapshot and generation.
#[derive(Clone)]
pub struct TableScanPlan {
    shared: Arc<TableScanPlanShared>,
    partition_offsets: Arc<[usize]>,
    generation: u64,
}

impl Debug for TableScanPlan {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableScanPlan")
            .field("operation_key", &self.shared.operation_key)
            .field("sts", &self.shared.sts)
            .field("table_id", &self.shared.table_id)
            .field("column_root", &self.shared.column_root)
            .field("pivot_row_id", &self.shared.pivot_row_id)
            .field("generation", &self.generation)
            .field("partition_offsets", &self.partition_offsets)
            .finish_non_exhaustive()
    }
}

impl TableScanPlan {
    #[inline]
    fn new(
        liveness: Arc<ReadSnapshotFacadeGroup>,
        operation_key: SessionOperationKey,
        sts: TrxID,
        table_id: TableID,
        projection: Vec<usize>,
        compiled: CompiledTableScanPlan,
    ) -> Self {
        let CompiledTableScanPlan {
            column_root,
            pivot_row_id,
            units,
            weight_prefix,
            partition_offsets,
        } = compiled;
        Self {
            shared: Arc::new(TableScanPlanShared {
                liveness,
                gate: PlanFamilyGate::new(),
                operation_key,
                sts,
                table_id,
                column_root,
                pivot_row_id,
                projection: Arc::from(projection),
                units: Arc::from(units),
                weight_prefix: Arc::from(weight_prefix),
            }),
            partition_offsets: Arc::from(partition_offsets),
            generation: 0,
        }
    }

    /// Return the compact plan layout's logical partition count.
    #[inline]
    pub fn partition_count(&self) -> usize {
        self.partition_offsets.len() - 1
    }

    /// Open one fully owned current-generation partition stream.
    pub fn open(&self, partition_idx: usize) -> Result<TableScanPartitionStream> {
        if partition_idx >= self.partition_count() {
            return Err(Report::new(OperationError::InvalidTableScanInput)
                .attach(format!(
                    "operation=open_table_scan_partition, operation_key={}, table_id={}, generation={}, partition_idx={partition_idx}, partition_count={}",
                    self.shared.operation_key,
                    self.shared.table_id,
                    self.generation,
                    self.partition_count()
                ))
                .disclose());
        }
        let start = self.partition_offsets[partition_idx];
        let end = self.partition_offsets[partition_idx + 1];
        self.admit_open(|| {
            let checkout = ReadSnapshotExecutionCheckout::open(&self.shared.liveness)?;
            let table = checkout.table(self.shared.table_id, partition_idx, self.generation);
            Ok(TableScanPartitionStream::new(
                Arc::clone(&self.shared.units),
                start,
                end,
                Arc::clone(&self.shared.projection),
                self.shared.table_id,
                partition_idx,
                table,
                checkout,
            ))
        })
        .disclose()
    }

    /// Best-effort repartition at immutable physical-unit boundaries.
    pub fn repartition(&self, target_partitions: NonZeroUsize) -> Result<Option<Self>> {
        self.repartition_inner(target_partitions).disclose()
    }

    fn repartition_inner(&self, target_partitions: NonZeroUsize) -> OperationResult<Option<Self>> {
        let offsets =
            repartition_table_scan_offsets(self.shared.weight_prefix.as_ref(), target_partitions);

        let mut gate = self.shared.gate.inner.lock();
        if self.generation != gate.current_generation {
            return Err(Report::new(OperationError::StaleTableScanPlan)
                .attach(format!(
                    "operation=repartition_table_scan, operation_key={}, table_id={}, receiver_generation={}, current_generation={}, target_partitions={target_partitions}",
                    self.shared.operation_key,
                    self.shared.table_id,
                    self.generation,
                    gate.current_generation
                )));
        }
        if gate.opened {
            return Err(Report::new(OperationError::TableScanAlreadyOpened)
                .attach(format!(
                    "operation=repartition_table_scan, operation_key={}, table_id={}, generation={}, target_partitions={target_partitions}",
                    self.shared.operation_key, self.shared.table_id, self.generation
                )));
        }
        if offsets.as_slice() == self.partition_offsets.as_ref() {
            return Ok(None);
        }
        let Some(generation) = gate.current_generation.checked_add(1) else {
            panic!(
                "table scan invariant violated: operation=repartition_table_scan, phase=increment_generation, operation_key={}, table_id={}, generation={}",
                self.shared.operation_key, self.shared.table_id, gate.current_generation
            )
        };
        gate.current_generation = generation;
        drop(gate);
        Ok(Some(Self {
            shared: Arc::clone(&self.shared),
            partition_offsets: Arc::from(offsets),
            generation,
        }))
    }

    /// Run execution-checkout acceptance while holding the family gate.
    ///
    /// Only a successful checkout seals the family; repeatable acceptance
    /// through the current generation remains legal.
    #[inline]
    pub(crate) fn admit_open<T>(&self, accept: impl FnOnce() -> QuadResult<T>) -> QuadResult<T> {
        let mut gate = self.shared.gate.inner.lock();
        if self.generation != gate.current_generation {
            return Err(Report::new(OperationError::StaleTableScanPlan)
                .attach(format!(
                    "operation=open_table_scan_partition, operation_key={}, table_id={}, receiver_generation={}, current_generation={}",
                    self.shared.operation_key,
                    self.shared.table_id,
                    self.generation,
                    gate.current_generation
                ))
                .into());
        }
        let result = accept();
        if result.is_ok() {
            gate.opened = true;
        }
        result
    }
}

struct PlanFamilyGate {
    inner: Mutex<PlanFamilyState>,
}

impl PlanFamilyGate {
    #[inline]
    fn new() -> Self {
        Self {
            inner: Mutex::new(PlanFamilyState {
                current_generation: 0,
                opened: false,
            }),
        }
    }
}

struct PlanFamilyState {
    current_generation: u64,
    opened: bool,
}

struct ReadSnapshotFacadeGroup {
    session: WeakSessionRef,
    key: SessionOperationKey,
    sts: TrxID,
    closed: AtomicBool,
}

impl ReadSnapshotFacadeGroup {
    #[inline]
    fn request_close(&self, reason: ReadSnapshotDrainReason) {
        self.closed.store(true, Ordering::Release);
        if let Some(runtime) = self.session.upgrade_for_terminal() {
            runtime.request_read_snapshot_close(self.key, reason);
        }
    }
}

impl Drop for ReadSnapshotFacadeGroup {
    #[inline]
    fn drop(&mut self) {
        self.request_close(ReadSnapshotDrainReason::FinalFacadeDrop);
    }
}

/// Counted owner of one exact immutable frozen snapshot core.
pub(crate) struct ReadSnapshotCheckout {
    runtime: SessionRuntime,
    entry: Arc<ReadSnapshotEntry>,
    read_core: Option<Arc<FrozenReadSnapshotCore>>,
}

impl ReadSnapshotCheckout {
    #[inline]
    fn open(group: &Arc<ReadSnapshotFacadeGroup>) -> LifecycleOrFatalResult<Self> {
        if group.closed.load(Ordering::Acquire) {
            return Err(read_snapshot_unavailable(group.key, "facade_closed").into());
        }
        let admitted = group
            .session
            .upgrade()
            .attach_with(|| format!("operation_key={}", group.key))?
            .ok_or_else(|| read_snapshot_unavailable(group.key, "session_missing"))?;
        admitted
            .runtime()
            .poisoner
            .ensure_healthy()
            .attach_with(|| format!("operation_key={}, phase=checkout", group.key))?;
        let (entry, read_core) = admitted
            .runtime()
            .state()
            .checkout_read_snapshot_ready(group.key, &group.closed)
            .attach_with(|| format!("operation_key={}", group.key))?;
        let runtime = admitted.into_runtime();
        Ok(Self {
            runtime,
            entry,
            read_core: Some(read_core),
        })
    }

    /// Return a table/layout/root view borrowed from this exact checkout.
    #[inline]
    pub(crate) fn table(&self, table_id: TableID) -> OperationResult<CheckedOutSnapshotTable<'_>> {
        let read_core = self.read_core.as_ref().unwrap_or_else(|| {
            panic!(
                "live snapshot checkout is missing its read core: key={}",
                self.entry.key()
            )
        });
        let binding = read_core.bindings.get(&table_id).ok_or_else(|| {
            Report::new(OperationError::TableNotAcquired).attach(format!(
                "operation_key={}, table_id={table_id}",
                self.entry.key()
            ))
        })?;
        Ok(CheckedOutSnapshotTable {
            visible: &binding.visible,
            table: &binding.table,
            layout: &binding.layout,
            root: CheckedOutTableScanRoot::new(&binding.root),
        })
    }

    /// Borrow the ownerless MVCC identity pinned by this checkout.
    #[inline]
    pub(crate) fn read_view(&self) -> &MvccReadView {
        &self
            .read_core
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "live snapshot checkout is missing its read core: key={}",
                    self.entry.key()
                )
            })
            .read_view
    }
}

impl Drop for ReadSnapshotCheckout {
    #[inline]
    fn drop(&mut self) {
        drop(self.read_core.take());
        self.entry.return_checkout();
        self.runtime.return_read_snapshot_checkout(&self.entry);
    }
}

/// Exact table and layout pins transferred into one partition stream.
pub(crate) struct ReadSnapshotExecutionTable {
    /// Snapshot-bound user-table runtime.
    pub(crate) table: Arc<Table>,
    /// Snapshot-bound table runtime layout.
    pub(crate) layout: Arc<TableRuntimeLayout>,
}

/// Counted owner accepted for one fully owned table-scan execution.
pub(crate) struct ReadSnapshotExecutionCheckout {
    checkout: ReadSnapshotCheckout,
}

impl ReadSnapshotExecutionCheckout {
    #[inline]
    fn open(group: &Arc<ReadSnapshotFacadeGroup>) -> LifecycleOrFatalResult<Self> {
        ReadSnapshotCheckout::open(group).map(|checkout| Self { checkout })
    }

    #[inline]
    fn table(
        &self,
        table_id: TableID,
        partition_idx: usize,
        generation: u64,
    ) -> ReadSnapshotExecutionTable {
        let read_core = self.checkout.read_core.as_ref().unwrap_or_else(|| {
            panic!(
                "live snapshot execution checkout is missing its read core: key={}",
                self.checkout.entry.key()
            )
        });
        let binding = read_core.bindings.get(&table_id).unwrap_or_else(|| {
            panic!(
                "table scan execution binding invariant violated: operation_key={}, table_id={table_id}, partition_idx={partition_idx}, generation={generation}",
                self.checkout.entry.key()
            )
        });
        ReadSnapshotExecutionTable {
            table: Arc::clone(&binding.table),
            layout: Arc::clone(&binding.layout),
        }
    }

    /// Borrow the ownerless MVCC view pinned by this execution checkout.
    #[inline]
    pub(crate) fn read_view(&self) -> &MvccReadView {
        self.checkout.read_view()
    }

    /// Clone the operation's pool-guard roots for an asynchronous unit load.
    #[inline]
    pub(crate) fn pool_guards_owned(&self) -> PoolGuards {
        self.checkout.runtime.pool_guards().clone()
    }

    /// Return the first snapshot execution failure, if one was published.
    #[inline]
    pub(crate) fn failure(&self) -> Option<SnapshotExecutionFailure> {
        self.read_core().execution.failure()
    }

    /// Attempt to publish this partition as the snapshot's first failure.
    #[inline]
    pub(crate) fn publish_failure(&self, table_id: TableID, partition_idx: usize) -> bool {
        self.read_core()
            .execution
            .publish_first_failure(SnapshotExecutionFailure {
                table_id,
                partition_idx,
            })
    }

    /// Seal the exact snapshot after this checkout wins failure publication.
    #[inline]
    pub(crate) fn request_failed_drain(&self) {
        self.checkout.runtime.request_read_snapshot_close(
            self.checkout.entry.key(),
            ReadSnapshotDrainReason::ExecutionFailed,
        );
    }

    #[inline]
    fn read_core(&self) -> &FrozenReadSnapshotCore {
        self.checkout.read_core.as_deref().unwrap_or_else(|| {
            panic!(
                "live snapshot execution checkout is missing its read core: key={}",
                self.checkout.entry.key()
            )
        })
    }
}

/// Table binding whose references and root view cannot outlive a checkout.
pub(crate) struct CheckedOutSnapshotTable<'checkout> {
    visible: &'checkout ResolvedLiveMetadata,
    table: &'checkout Arc<Table>,
    layout: &'checkout Arc<TableRuntimeLayout>,
    root: CheckedOutTableScanRoot<'checkout>,
}

impl CheckedOutSnapshotTable<'_> {
    /// Return the snapshot-visible metadata version.
    #[inline]
    pub(crate) const fn visible_metadata(&self) -> &ResolvedLiveMetadata {
        self.visible
    }

    /// Borrow the bound table runtime.
    #[inline]
    pub(crate) fn table(&self) -> &Table {
        self.table
    }

    /// Borrow the bound runtime layout.
    #[inline]
    pub(crate) fn layout(&self) -> &TableRuntimeLayout {
        self.layout
    }

    /// Borrow the usable scan-root view.
    #[inline]
    pub(crate) const fn root(&self) -> &CheckedOutTableScanRoot<'_> {
        &self.root
    }
}

#[inline]
fn read_snapshot_unavailable(
    key: SessionOperationKey,
    phase: &'static str,
) -> Report<LifecycleError> {
    Report::new(LifecycleError::ReadSnapshotUnavailable)
        .attach(format!("operation_key={key}, phase={phase}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CallbackResult;
    use crate::catalog::CATALOG_TABLE_ID_START;
    use crate::catalog::tests::{table2, table3};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TableScanConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{Error, ErrorKind, FatalError, LifecycleOrFatalError, QuadError};
    use crate::id::{BlockID, OperationID, PageID, SessionID};
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};
    use crate::lock::{FamilyLockAuthority, LockMode, LockOwner, LockResource, LockScopeState};
    use crate::row::ops::ScanRowDecision;
    use crate::session::tests::assert_checkpoint_published;
    use crate::table::tests::assert_freeze_created;
    use crate::table::{RowPageDescriptor, TableScanRootView, TableScanWorklist};
    use crate::trx::tests::{active_sts_contains, active_sts_count, test_engine};
    use crate::trx::{MAX_SNAPSHOT_TS, MvccVisibility};
    use crate::value::Val;
    use std::iter::empty;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Barrier;
    use std::thread::scope;
    use tempfile::TempDir;

    const _: fn() = || {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        fn assert_clone<T: Clone>() {}

        assert_send::<ReadSnapshotBuilder>();
        assert_send::<ReadSnapshot>();
        assert_sync::<ReadSnapshot>();
        assert_clone::<ReadSnapshot>();
        assert_send::<ReadSnapshotCheckout>();
        assert_sync::<ReadSnapshotCheckout>();
        assert_send::<TableScanPlan>();
        assert_sync::<TableScanPlan>();
        assert_clone::<TableScanPlan>();
    };

    const _: fn(ReadSnapshotBuilder, TableID) = |builder, table_id| {
        fn assert_send<T: Send>(_: T) {}

        assert_send(builder.acquire_tables([table_id]));
    };

    /// Per-engine semantic pause after worklist capture and before plan publication.
    #[derive(Default)]
    pub(crate) struct TableScanPlanTestController {
        after_capture: Mutex<Option<Arc<TableScanPlanTestHook>>>,
    }

    impl TableScanPlanTestController {
        /// Arm a one-shot pause after physical worklist capture.
        #[inline]
        fn arm_after_worklist_capture(&self) -> Arc<TableScanPlanTestHook> {
            let hook = Arc::new(TableScanPlanTestHook::new());
            let previous = self.after_capture.lock().replace(Arc::clone(&hook));
            assert!(
                previous.is_none(),
                "table-scan planning test hook already armed"
            );
            hook
        }

        #[inline]
        pub(super) async fn after_worklist_capture(&self) {
            let Some(hook) = self.after_capture.lock().take() else {
                return;
            };
            hook.pause().await;
        }
    }

    /// One semantic planning pause controlled without sleeps or elapsed-time progress.
    struct TableScanPlanTestHook {
        reached: AtomicBool,
        released: AtomicBool,
        reached_event: Event,
        release_event: Event,
    }

    impl TableScanPlanTestHook {
        #[inline]
        fn new() -> Self {
            Self {
                reached: AtomicBool::new(false),
                released: AtomicBool::new(false),
                reached_event: Event::new(),
                release_event: Event::new(),
            }
        }

        #[inline]
        async fn pause(&self) {
            self.reached.store(true, Ordering::Release);
            self.reached_event.notify(usize::MAX);
            loop {
                let listener = self.release_event.listen();
                if self.released.load(Ordering::Acquire) {
                    return;
                }
                listener.await;
            }
        }

        /// Return whether planning reached the armed capture boundary.
        #[inline]
        fn reached(&self) -> bool {
            self.reached.load(Ordering::Acquire)
        }

        /// Release planning from the armed capture boundary.
        #[inline]
        fn release(&self) {
            self.released.store(true, Ordering::Release);
            self.release_event.notify(usize::MAX);
        }
    }

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

    assert_not_impl!(ReadSnapshotBuilder: Sync);
    assert_not_impl!(ReadSnapshotBuilder: Clone);

    fn assert_quad_operation(error: QuadError, expected: OperationError) {
        match error {
            QuadError::Operation(report) => assert_eq!(report.current_context(), &expected),
            error => panic!("expected Operation error, got {error:?}"),
        }
    }

    fn assert_quad_lifecycle(error: QuadError, expected: LifecycleError) {
        match error {
            QuadError::Lifecycle(report) => assert_eq!(report.current_context(), &expected),
            error => panic!("expected Lifecycle error, got {error:?}"),
        }
    }

    fn assert_operation(error: Error, expected: OperationError) {
        assert_eq!(error.operation_error(), Some(expected), "{error:?}");
    }

    fn assert_lifecycle(error: Error, expected: LifecycleError) {
        assert_eq!(error.kind(), ErrorKind::Lifecycle, "{error:?}");
        assert_eq!(
            error.report().downcast_ref::<LifecycleError>(),
            Some(&expected)
        );
    }

    fn assert_fatal(error: Error, expected: FatalError) {
        assert_eq!(error.kind(), ErrorKind::Fatal, "{error:?}");
        assert_eq!(error.report().downcast_ref::<FatalError>(), Some(&expected));
    }

    macro_rules! drive_planner_to_capture_hook {
        ($future:expr, $hook:expr) => {{
            for _ in 0..64 {
                assert!(
                    matches!(futures::poll!($future.as_mut()), std::task::Poll::Pending),
                    "planning completed before the armed capture hook"
                );
                if $hook.reached() {
                    break;
                }
            }
            assert!($hook.reached(), "planning did not reach the capture hook");
        }};
    }

    fn assert_lifecycle_or_fatal_lifecycle(error: LifecycleOrFatalError, expected: LifecycleError) {
        match error {
            LifecycleOrFatalError::Lifecycle(report) => {
                assert_eq!(report.current_context(), &expected);
            }
            error => panic!("expected Lifecycle error, got {error:?}"),
        }
    }

    fn assert_operation_report(error: Error, expected: OperationError) {
        assert_operation(error, expected);
    }

    fn synthetic_hot_plan(snapshot: &ReadSnapshot, table_id: TableID) -> TableScanPlan {
        let pivot_row_id = RowID::new(100);
        let worklist = TableScanWorklist {
            column_root: BlockID::new(7),
            pivot_row_id,
            cold_entries: Vec::new(),
            hot_pages: (0..4)
                .map(|idx| RowPageDescriptor {
                    page_id: PageID::new(idx + 1),
                    start_row_id: RowID::new(100 + idx * 2),
                    end_row_id: RowID::new(101 + idx * 2),
                })
                .collect(),
        };
        let compiled = compile_table_scan_plan(worklist, TableScanConfig::default());
        TableScanPlan::new(
            Arc::clone(&snapshot.group),
            snapshot.group.key,
            snapshot.sts(),
            table_id,
            vec![0],
            compiled,
        )
    }

    async fn insert_test_rows(engine: &Engine, table_id: TableID, count: i32) {
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        for key in 0..count {
            trx.table_insert_mvcc(table_id, vec![Val::from(key), Val::from("value")])
                .await
                .unwrap();
        }
        trx.commit().await.unwrap();
        session.close().await.unwrap();
    }

    async fn insert_large_test_rows(engine: &Engine, table_id: TableID, count: i32) {
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        for key in 0..count {
            trx.table_insert_mvcc(
                table_id,
                vec![Val::from(key), Val::from(vec![b'x'; 40 * 1024])],
            )
            .await
            .unwrap();
        }
        trx.commit().await.unwrap();
        session.close().await.unwrap();
    }

    async fn drain_partition(mut stream: TableScanPartitionStream) -> Result<Vec<Vec<Val>>> {
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    #[test]
    fn partition_streams_are_spawnable_ordered_and_repeatable() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_partition_spawnable").await;
            let table_id = table2(&engine).await;
            insert_large_test_rows(&engine, table_id, 12).await;

            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            let invalid = match plan.open(plan.partition_count()) {
                Ok(_) => panic!("out-of-range partition must fail before execution checkout"),
                Err(error) => error,
            };
            assert_operation(invalid, OperationError::InvalidTableScanInput);
            let plan = plan
                .repartition(NonZeroUsize::new(4).unwrap())
                .unwrap()
                .unwrap_or(plan);
            assert!(plan.partition_count() > 1);

            let mut tasks = Vec::with_capacity(plan.partition_count());
            for partition_idx in 0..plan.partition_count() {
                let mut stream = plan.open(partition_idx).unwrap();
                fn require_send<T: Send>(_: &T) {}
                let next = stream.next();
                require_send(&next);
                drop(next);
                tasks.push(smol::spawn(drain_partition(stream)));
            }
            drop(plan);
            let (partition_rows, close) =
                futures::join!(futures::future::join_all(tasks), snapshot.close());
            close.unwrap();
            let rows = partition_rows
                .into_iter()
                .map(|result| result.unwrap())
                .collect::<Vec<_>>();
            let concatenated = rows.into_iter().flatten().collect::<Vec<_>>();
            let mut trx = session.begin_trx().unwrap();
            let mut sequential_stream = trx
                .table_scan_mvcc_stream(table_id, &[0], |_| -> CallbackResult<_> {
                    Ok(ScanRowDecision::Include)
                })
                .await
                .unwrap();
            let mut sequential = Vec::new();
            while let Some(row) = sequential_stream.next().await.unwrap() {
                sequential.push(row);
            }
            drop(sequential_stream);
            trx.rollback().await.unwrap();
            assert_eq!(concatenated, sequential);

            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn partition_stream_scans_mixed_cold_and_hot_units() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_partition_mixed").await;
            let table_id = table2(&engine).await;
            insert_test_rows(&engine, table_id, 3).await;
            let mut maintenance = engine.new_session().unwrap();
            assert_freeze_created(
                maintenance
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut maintenance, table_id).await;
            maintenance.close().await.unwrap();

            let mut writer = engine.new_session().unwrap();
            let mut trx = writer.begin_trx().unwrap();
            for key in 3..5 {
                trx.table_insert_mvcc(table_id, vec![Val::from(key), Val::from("hot")])
                    .await
                    .unwrap();
            }
            trx.commit().await.unwrap();
            writer.close().await.unwrap();

            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            assert!(
                plan.shared
                    .units
                    .iter()
                    .any(|unit| matches!(unit, TableScanUnit::Cold(_)))
            );
            assert!(
                plan.shared
                    .units
                    .iter()
                    .any(|unit| matches!(unit, TableScanUnit::Hot(_)))
            );
            let streams = (0..plan.partition_count())
                .map(|partition_idx| plan.open(partition_idx).unwrap())
                .collect::<Vec<_>>();
            drop(plan);
            let (partitions, close) = futures::join!(
                futures::future::join_all(streams.into_iter().map(drain_partition)),
                snapshot.close()
            );
            close.unwrap();
            let rows = partitions
                .into_iter()
                .flat_map(|result| result.unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                rows,
                (0..5).map(|key| vec![Val::from(key)]).collect::<Vec<_>>()
            );

            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn opened_streams_outlive_facades_and_repeat_the_same_partition() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_partition_owned").await;
            let table_id = table2(&engine).await;
            insert_test_rows(&engine, table_id, 4).await;

            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            let first = plan.open(0).unwrap();
            let second = plan.open(0).unwrap();
            let (first, second) = futures::join!(drain_partition(first), drain_partition(second));
            let expected = first.unwrap();
            assert_eq!(expected, second.unwrap());
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));

            let third = plan.open(0).unwrap();
            drop(plan);
            drop(snapshot);
            assert_eq!(drain_partition(third).await.unwrap(), expected);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn first_execution_error_aborts_peers_only_at_unit_boundary() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_partition_failure").await;
            let table_id = table2(&engine).await;
            insert_test_rows(&engine, table_id, 5).await;

            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let valid = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            let mut peer = valid.open(0).unwrap();
            assert_eq!(peer.next().await.unwrap(), Some(vec![Val::from(0)]));

            let mut origin = valid.open(0).unwrap();
            let origin_error = origin.inject_execution_error().unwrap_err();
            assert_eq!(origin_error.kind(), ErrorKind::Runtime);
            assert_eq!(origin.next().await.unwrap(), None);

            let later_open = match valid.open(0) {
                Ok(_) => panic!("failed snapshot must reject later execution checkout"),
                Err(error) => error,
            };
            assert_lifecycle(later_open, LifecycleError::ReadSnapshotUnavailable);
            assert_lifecycle(
                snapshot
                    .prepare_table_scan(
                        table_id,
                        TableScanOptions {
                            projection: vec![0],
                        },
                    )
                    .await
                    .unwrap_err(),
                LifecycleError::ReadSnapshotUnavailable,
            );

            let mut remainder = Vec::new();
            let peer_error = loop {
                match peer.next().await {
                    Ok(Some(row)) => remainder.push(row),
                    Ok(None) => panic!("failed peer must publish its abort at the unit boundary"),
                    Err(error) => break error,
                }
            };
            assert_eq!(
                remainder,
                (1..5).map(|key| vec![Val::from(key)]).collect::<Vec<_>>()
            );
            assert_operation(peer_error, OperationError::SnapshotScanAborted);
            assert_eq!(peer.next().await.unwrap(), None);

            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn table_scan_plan_preparation_validates_projection_and_identity() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_projection").await;
            let table_id = table2(&engine).await;
            let outside = table3(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();

            for projection in [vec![], vec![2], vec![0, 0], vec![1, 0]] {
                let error = snapshot
                    .prepare_table_scan(table_id, TableScanOptions { projection })
                    .await
                    .unwrap_err();
                assert_operation(error, OperationError::InvalidTableScanInput);
            }
            let error = snapshot
                .prepare_table_scan(
                    outside,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap_err();
            assert_operation(error, OperationError::TableNotAcquired);

            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0, 1],
                    },
                )
                .await
                .unwrap();
            assert_eq!(plan.partition_count(), 1);
            assert_eq!(plan.partition_offsets.as_ref(), [0, 0]);
            assert_eq!(plan.generation, 0);
            assert_eq!(plan.shared.operation_key, snapshot.group.key);
            assert_eq!(plan.shared.sts, snapshot.sts());
            assert_eq!(plan.shared.table_id, table_id);
            assert_eq!(plan.shared.projection.as_ref(), [0, 1]);
            assert!(plan.shared.units.is_empty());
            assert_eq!(plan.shared.weight_prefix.as_ref(), [0]);
            assert_eq!(plan.shared.column_root, BlockID::new(0));
            assert_eq!(plan.shared.pivot_row_id, RowID::new(0));
            assert!(Arc::ptr_eq(&plan.shared.liveness, &snapshot.group));

            drop(plan);
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn custom_engine_scan_config_drives_real_snapshot_initial_partitions() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let scan_config = TableScanConfig::default()
                .lwc_blocks_per_partition(3)
                .row_pages_per_partition(1);
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(64usize * 1024 * 1024)
                            .max_file_size(128usize * 1024 * 1024),
                    )
                    .trx(
                        TrxSysConfig::default()
                            .purge_threads(1)
                            .log_file_stem("custom_table_scan_config"),
                    )
                    .table_scan(scan_config),
            )
            .await
            .unwrap();
            assert_eq!(engine.inner().table_scan_config(), scan_config);
            let table_id = table2(&engine).await;
            for row_no in 0..2 {
                let mut insert_session = engine.new_session().unwrap();
                let mut trx = insert_session.begin_trx().unwrap();
                trx.table_insert_mvcc(
                    table_id,
                    vec![
                        Val::from(row_no),
                        Val::from(vec![b'a' + row_no as u8; 40 * 1024]),
                    ],
                )
                .await
                .unwrap();
                trx.commit().await.unwrap();
                insert_session.close().await.unwrap();
            }

            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            assert_eq!(plan.shared.units.len(), 2);
            assert!(
                plan.shared
                    .units
                    .iter()
                    .all(|unit| matches!(unit, TableScanUnit::Hot(_)))
            );
            assert_eq!(plan.shared.weight_prefix.as_ref(), [0, 3, 6]);
            assert_eq!(plan.partition_offsets.as_ref(), [0, 1, 2]);

            drop(plan);
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn repeated_and_concurrent_preparation_is_deterministic_with_independent_gates() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_repeatable_prepare").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let concurrent = snapshot.clone();
            let (first, second) = futures::join!(
                snapshot.prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0, 1],
                    },
                ),
                concurrent.prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0, 1],
                    },
                )
            );
            let first = first.unwrap();
            let second = second.unwrap();
            assert_eq!(first.shared.operation_key, second.shared.operation_key);
            assert_eq!(first.shared.column_root, second.shared.column_root);
            assert_eq!(first.shared.pivot_row_id, second.shared.pivot_row_id);
            assert_eq!(first.shared.projection, second.shared.projection);
            assert_eq!(first.shared.units, second.shared.units);
            assert_eq!(first.shared.weight_prefix, second.shared.weight_prefix);
            assert_eq!(first.partition_offsets, second.partition_offsets);
            assert_eq!(first.generation, 0);
            assert_eq!(second.generation, 0);
            assert!(!Arc::ptr_eq(&first.shared, &second.shared));

            drop(first);
            drop(second);
            drop(concurrent);
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn table_scan_repartition_supersedes_generations_and_open_seals_family() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_generation").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();

            let original = synthetic_hot_plan(&snapshot, table_id);
            let clone = original.clone();
            assert_eq!(original.partition_offsets.as_ref(), [0, 4]);
            assert!(
                original
                    .repartition(NonZeroUsize::new(1).unwrap())
                    .unwrap()
                    .is_none()
            );
            let current = original
                .repartition(NonZeroUsize::new(2).unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(current.generation, 1);
            assert_eq!(current.partition_offsets.as_ref(), [0, 2, 4]);
            assert!(Arc::ptr_eq(&original.shared, &current.shared));
            assert_operation_report(
                original
                    .repartition(NonZeroUsize::new(3).unwrap())
                    .unwrap_err(),
                OperationError::StaleTableScanPlan,
            );
            assert_operation_report(
                clone
                    .repartition(NonZeroUsize::new(3).unwrap())
                    .unwrap_err(),
                OperationError::StaleTableScanPlan,
            );
            let open_error = original
                .admit_open(|| Ok(()))
                .expect_err("superseded generation must reject open");
            assert_quad_operation(open_error, OperationError::StaleTableScanPlan);
            drop(current);
            assert_operation_report(
                original
                    .repartition(NonZeroUsize::new(4).unwrap())
                    .unwrap_err(),
                OperationError::StaleTableScanPlan,
            );

            let failed_then_repartitioned = synthetic_hot_plan(&snapshot, table_id);
            let failed: QuadResult<()> = failed_then_repartitioned
                .admit_open(|| Err(Report::new(LifecycleError::ReadSnapshotUnavailable).into()));
            assert_quad_lifecycle(
                failed.expect_err("simulated checkout must fail"),
                LifecycleError::ReadSnapshotUnavailable,
            );
            assert!(
                failed_then_repartitioned
                    .repartition(NonZeroUsize::new(2).unwrap())
                    .unwrap()
                    .is_some()
            );

            let opened = synthetic_hot_plan(&snapshot, table_id);
            opened.admit_open(|| Ok(())).unwrap();
            opened.admit_open(|| Ok(())).unwrap();
            assert_operation_report(
                opened
                    .repartition(NonZeroUsize::new(2).unwrap())
                    .unwrap_err(),
                OperationError::TableScanAlreadyOpened,
            );

            let mut overflow = synthetic_hot_plan(&snapshot, table_id);
            overflow.generation = u64::MAX;
            overflow.shared.gate.inner.lock().current_generation = u64::MAX;
            let result = catch_unwind(AssertUnwindSafe(|| {
                overflow.repartition(NonZeroUsize::new(2).unwrap())
            }));
            assert!(result.is_err(), "generation wrap must panic");

            drop(overflow);
            drop(opened);
            drop(failed_then_repartitioned);
            drop(clone);
            drop(original);
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn table_scan_repartition_and_open_have_one_gate_winner() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_gate_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let plan = synthetic_hot_plan(&snapshot, table_id);
            let repartition_plan = plan.clone();
            let open_plan = plan.clone();
            let barrier = Barrier::new(3);
            let (repartition_result, open_result) = scope(|scope| {
                let repartition = scope.spawn(|| {
                    barrier.wait();
                    repartition_plan.repartition(NonZeroUsize::new(2).unwrap())
                });
                let open = scope.spawn(|| {
                    barrier.wait();
                    open_plan.admit_open(|| Ok(()))
                });
                barrier.wait();
                (repartition.join().unwrap(), open.join().unwrap())
            });
            match (repartition_result, open_result) {
                (Ok(Some(_)), Err(error)) => {
                    assert_quad_operation(error, OperationError::StaleTableScanPlan);
                }
                (Err(error), Ok(())) => {
                    assert_operation_report(error, OperationError::TableScanAlreadyOpened);
                }
                _ => panic!("unexpected table-scan gate race outcome"),
            }

            drop(plan);
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn table_scan_plan_liveness_is_resource_free_after_explicit_close() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_resource_free").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            snapshot.clone().close().await.unwrap();
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            assert_eq!(plan.partition_count(), 1);
            assert!(
                plan.repartition(NonZeroUsize::new(8).unwrap())
                    .unwrap()
                    .is_none()
            );
            drop(snapshot);
            drop(plan);

            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            let plan = snapshot
                .prepare_table_scan(
                    table_id,
                    TableScanOptions {
                        projection: vec![0],
                    },
                )
                .await
                .unwrap();
            drop(snapshot);
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));
            drop(plan);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn cancelled_table_scan_planning_returns_counted_checkout() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_cancel").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);
            drop(planning);

            let checkout = snapshot.checkout().unwrap();
            assert_eq!(checkout.entry.phase(), ReadSnapshotPhase::Ready);
            drop(checkout);
            hook.release();
            snapshot.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn explicit_snapshot_close_wins_against_final_plan_publication() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_explicit_close_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);

            let mut close = Box::pin(snapshot.clone().close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            hook.release();
            assert_lifecycle(
                planning.await.unwrap_err(),
                LifecycleError::ReadSnapshotUnavailable,
            );
            close.await.unwrap();
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            drop(snapshot);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn session_close_and_abandonment_win_against_plan_publication() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_session_close_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);
            let mut close = Box::pin(session.close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            hook.release();
            assert_lifecycle(
                planning.await.unwrap_err(),
                LifecycleError::ReadSnapshotUnavailable,
            );
            close.await.unwrap();
            drop(snapshot);
            engine.shutdown();
        });

        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_abandon_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);
            drop(session);
            hook.release();
            assert_lifecycle(
                planning.await.unwrap_err(),
                LifecycleError::ReadSnapshotUnavailable,
            );
            drop(snapshot);
            engine.shutdown();
        });
    }

    #[test]
    fn poison_and_shutdown_win_against_final_plan_publication() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_poison_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);
            engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test planning poison"));
            hook.release();
            assert_fatal(planning.await.unwrap_err(), FatalError::RedoWrite);
            snapshot.close().await.unwrap();
            drop(session);
            engine.shutdown();
        });

        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("table_scan_plan_shutdown_race").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let hook = engine
                .inner()
                .table_scan_plan_test
                .arm_after_worklist_capture();
            let mut planning = Box::pin(snapshot.prepare_table_scan(
                table_id,
                TableScanOptions {
                    projection: vec![0],
                },
            ));
            drive_planner_to_capture_hook!(planning, hook);
            assert!(engine.try_shutdown().is_err());
            hook.release();
            assert_lifecycle(
                planning.await.unwrap_err(),
                LifecycleError::ReadSnapshotUnavailable,
            );
            engine.try_shutdown().unwrap();
            drop(snapshot);
            drop(session);
        });
    }

    #[test]
    fn private_snapshot_registers_and_releases_active_sts() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("private_snapshot_sts").await;
            let trx_sys = engine.inner().trx_sys.clone();
            assert_eq!(active_sts_count(&trx_sys), 0);

            let first = trx_sys.register_private_snapshot();
            let first_sts = first.sts();
            assert_eq!(active_sts_count(&trx_sys), 1);
            assert_eq!(trx_sys.min_active_sts(), first_sts);

            let second = trx_sys.register_private_snapshot();
            let second_sts = second.sts();
            assert!(second_sts > first_sts);
            assert_eq!(active_sts_count(&trx_sys), 2);
            assert_eq!(trx_sys.min_active_sts(), first_sts);

            drop(first);
            assert_eq!(active_sts_count(&trx_sys), 1);
            assert_eq!(trx_sys.min_active_sts(), second_sts);
            drop(second);
            assert_eq!(active_sts_count(&trx_sys), 0);
            assert_eq!(trx_sys.min_active_sts(), MAX_SNAPSHOT_TS);

            drop(trx_sys);
            engine.shutdown();
        });
    }

    #[test]
    fn dropped_terminal_claim_restores_payload_before_asserting() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("snapshot_terminal_claim_invariant").await;
            let mut session = engine.new_session().unwrap();
            let builder = session.begin_read_snapshot().unwrap();
            let runtime = builder.session.upgrade_for_terminal().unwrap();
            drop(builder);

            let key = SessionOperationKey::new(session.id(), OperationID::new(u64::MAX));
            let registration = engine.inner().trx_sys.register_active_snapshot();
            let sts = registration.sts();
            let core = ReadSnapshotBuildCore::new(
                registration,
                ReadSnapshotLockOwner::new(FamilyLockAuthority::new(session.id()), key),
            );
            let entry = ReadSnapshotEntry::new(key, sts, core);
            let core = entry.take_build().unwrap();
            entry.inner.lock().state = ReadSnapshotEntryState::CompletingCheckedOut;
            let claim = ReadSnapshotTerminalClaim {
                runtime: runtime.clone(),
                entry: Arc::clone(&entry),
                payload: Some(ReadSnapshotTerminalPayload::Building(core)),
            };

            let dropped = catch_unwind(AssertUnwindSafe(|| drop(claim)));
            assert!(dropped.is_err());
            assert_eq!(entry.phase(), ReadSnapshotPhase::CompletingAvailable);
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));

            let payload = {
                let mut inner = entry.inner.lock();
                let state = replace(&mut inner.state, ReadSnapshotEntryState::Terminal);
                let ReadSnapshotEntryState::CompletingAvailable(payload) = state else {
                    inner.state = state;
                    panic!(
                        "dropped terminal claim did not restore its payload: key={}, phase={}",
                        entry.key(),
                        inner.state.phase().label()
                    );
                };
                payload
            };
            let mut authority = payload.cleanup(&runtime);
            authority.close_session(runtime.core().lock_manager());
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            drop(runtime);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn shared_snapshot_acquires_checks_out_reuses_and_closes() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_workflow").await;
            let first = table2(&engine).await;
            let second = table3(&engine).await;
            let mut session = engine.new_session().unwrap();
            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));
            let snapshot = builder
                .acquire_tables([first, second, first])
                .await
                .unwrap();
            assert_eq!(snapshot.sts(), sts);
            let family = session.id();
            let snapshot_locks = debug_snapshot(engine.inner().core.lock_manager())
                .entries
                .into_iter()
                .filter(|entry| entry.family.session_id() == family)
                .collect::<Vec<_>>();
            assert_eq!(snapshot_locks.len(), 2);
            assert!(snapshot_locks.iter().all(|entry| {
                matches!(entry.resource, LockResource::TableMetadata(id) if id == first || id == second)
                    && entry.mode == LockMode::Shared
                    && entry.state == LockDebugEntryState::Granted
            }));

            let checkout = snapshot.checkout().unwrap();
            assert_eq!(checkout.read_view().sts(), sts);
            let first_table = checkout.table(first).unwrap();
            assert_eq!(first_table.table().table_id(), first);
            assert!(first_table.visible_metadata().effective_cts() < sts);
            assert!(Arc::ptr_eq(
                first_table.layout().metadata_arc(),
                first_table.table().layout_snapshot().metadata_arc()
            ));
            let _pivot = first_table.root().pivot_row_id();
            let missing = match checkout.table(TableID::new(u64::MAX)) {
                Ok(_) => panic!("table outside the frozen set must be rejected"),
                Err(error) => error,
            };
            assert_eq!(missing.current_context(), &OperationError::TableNotAcquired);
            drop(checkout);

            assert!(active_sts_contains(&engine.inner().trx_sys, sts));
            let reusable = snapshot.checkout().unwrap();
            assert_eq!(reusable.table(second).unwrap().table().table_id(), second);
            drop(reusable);

            snapshot.clone().close().await.unwrap();
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| entry.family.session_id() != family)
            );
            let closed = match snapshot.checkout() {
                Ok(_) => panic!("closed snapshot must reject checkout"),
                Err(error) => error,
            };
            assert_lifecycle_or_fatal_lifecycle(closed, LifecycleError::ReadSnapshotUnavailable);
            drop(snapshot);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn empty_snapshot_input_drops_registered_sts() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_empty_input").await;
            let mut session = engine.new_session().unwrap();
            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));
            let err = match builder.acquire_tables(empty()).await {
                Ok(_) => panic!("empty snapshot input must be rejected"),
                Err(error) => error,
            };
            assert_operation(err, OperationError::InvalidReadSnapshotInput);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn builder_drop_unpolled_future_and_prefix_failure_cleanup() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_builder_cleanup").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();

            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            drop(builder);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            let future = builder.acquire_tables([table_id]);
            drop(future);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            let err = match builder
                .acquire_tables([table_id, TableID::new(u64::MAX - 1)])
                .await
            {
                Ok(_) => panic!("missing table after an accepted prefix must fail the build"),
                Err(error) => error,
            };
            assert_operation(err, OperationError::TableNotFound);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| entry.family.session_id() != session.id())
            );

            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            let err = match builder.acquire_tables([CATALOG_TABLE_ID_START]).await {
                Ok(_) => panic!("catalog table must be rejected"),
                Err(error) => error,
            };
            assert_operation(err, OperationError::TableNotFound);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn cancelled_explicit_close_stays_group_wide_and_finishes_on_checkout_return() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_cancelled_close").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let dormant = snapshot.clone();
            let sts = snapshot.sts();
            let checkout = snapshot.checkout().unwrap();

            let mut close = Box::pin(snapshot.close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            drop(close);
            assert!(dormant.checkout().is_err());
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));

            drop(checkout);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            drop(dormant);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn final_facade_drop_requests_ready_snapshot_cleanup() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_final_facade").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            drop(snapshot);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn abandonment_aborts_blocked_metadata_wait_and_cancels_pending_claim() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_wait_abort").await;
            let table_id = table2(&engine).await;
            let resource = LockResource::TableMetadata(table_id);

            let blocker_session_id = SessionID::new(u64::MAX - 7);
            let blocker_key =
                SessionOperationKey::new(blocker_session_id, OperationID::new(u64::MAX - 9));
            let mut blocker = FamilyLockAuthority::new(blocker_session_id);
            let mut blocker_scope = LockScopeState::new(LockOwner::operation(blocker_key));
            blocker
                .family_mut()
                .acquire(
                    &mut blocker_scope,
                    engine.inner().core.lock_manager(),
                    &engine.inner().core.poisoner,
                    resource,
                    LockMode::Exclusive,
                )
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let snapshot_session_id = session.id();
            let builder = session.begin_read_snapshot().unwrap();
            let sts = builder.sts();
            let mut acquire = Box::pin(builder.acquire_tables([table_id]));
            let mut observed_waiter = false;
            for _ in 0..32 {
                assert!(matches!(
                    futures::poll!(acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                observed_waiter = debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .any(|entry| {
                        entry.family.session_id() == snapshot_session_id
                            && entry.resource == resource
                            && entry.mode == LockMode::Shared
                            && entry.state == LockDebugEntryState::Waiting
                    });
                if observed_waiter {
                    break;
                }
            }
            assert!(
                observed_waiter,
                "snapshot metadata waiter was not installed"
            );

            drop(session);
            let err = match acquire.await {
                Ok(_) => panic!("abandoned blocked snapshot build must fail"),
                Err(error) => error,
            };
            assert_lifecycle(err, LifecycleError::ReadSnapshotUnavailable);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| entry.family.session_id() != snapshot_session_id),
                "snapshot pending claim must cancel before blocker release"
            );

            blocker
                .family_mut()
                .close_scope(&mut blocker_scope, engine.inner().core.lock_manager());
            blocker.close_session(engine.inner().core.lock_manager());
            engine.shutdown();
        });
    }

    #[test]
    fn session_close_waits_for_accepted_snapshot_checkout() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_session_close").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            let checkout = snapshot.checkout().unwrap();

            let mut close = Box::pin(session.close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(snapshot.checkout().is_err());
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));

            drop(checkout);
            close.await.unwrap();
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            drop(snapshot);
            engine.shutdown();
        });
    }

    #[test]
    fn notified_snapshot_close_future_retains_no_hidden_shutdown_runtime() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_close_runtime_release").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let checkout = snapshot.checkout().unwrap();

            let mut close = Box::pin(snapshot.close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            drop(checkout);

            // Keep the notified close future deliberately unpolled. Shutdown
            // must still remove the idle session and release engine components.
            engine.try_shutdown().unwrap();
            close.await.unwrap();
            drop(session);
        });
    }

    #[test]
    fn notified_session_close_future_retains_no_hidden_shutdown_runtime() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("session_close_runtime_release").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let checkout = snapshot.checkout().unwrap();

            let mut close = Box::pin(session.close());
            assert!(matches!(
                futures::poll!(close.as_mut()),
                std::task::Poll::Pending
            ));
            drop(checkout);

            // Snapshot terminal cleanup removed the close-requested session.
            // The unpolled close future must retain only its independent listener.
            engine.try_shutdown().unwrap();
            close.await.unwrap();
            drop(snapshot);
        });
    }

    #[test]
    fn session_abandonment_cleans_checked_in_snapshot() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_abandonment").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();
            assert!(active_sts_contains(&engine.inner().trx_sys, sts));

            drop(session);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            assert!(snapshot.checkout().is_err());
            drop(snapshot);
            engine.shutdown();
        });
    }

    #[test]
    fn try_shutdown_cleans_ready_snapshot_then_reports_sampled_blocker() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("shared_snapshot_try_shutdown").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let snapshot = session
                .begin_read_snapshot()
                .unwrap()
                .acquire_tables([table_id])
                .await
                .unwrap();
            let sts = snapshot.sts();

            let first = engine.try_shutdown().unwrap_err();
            assert_eq!(first.kind(), crate::error::ErrorKind::Lifecycle);
            assert!(!active_sts_contains(&engine.inner().trx_sys, sts));
            engine.try_shutdown().unwrap();
            drop(snapshot);
            drop(session);
        });
    }
}
