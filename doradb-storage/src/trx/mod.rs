//! DoraDB's concurrency control protocol is an implmementation of MVCC + MV2PL(todo).
//!
//! The basic MVCC logic is described as below.
//! 1. When starting a transaction, a snapshot timestamp(STS) is generated, and transaction id
//!    is also derived from STS by setting highest bit to 1.
//! 2. When the transaction do any insert, update or delete, an undo log is generated with
//!    RowID and stored in a page-level transaction version map(UndoMap). The undo log records
//!    current transaction id at head.
//! 3. When the transaction commits, a commit timestamp(CTS) is generated, and all undo logs of
//!    this transaction will update CTS in its head.
//! 4. When a transaction query a row in one page,
//!    a) it first look at page-level UndoMap, if the map is empty, then all data on the page
//!    are latest. So directly read data and return.
//!    b) otherwise, check if queried RowID exists in the map. if not, same as a).
//!    c) If exists, check the timestamp in entry head. If it's larger than current STS, means
//!    it's invisible, undo change and go to next version in the chain...
//!    d) If less than current STS, return current version.
mod admission;
pub(crate) mod group;
pub(crate) mod purge;
pub(crate) mod retention;
pub(crate) mod row;
pub(crate) mod stmt;
mod stream_stmt;
pub(crate) mod sys;
mod sys_trx;
pub(crate) mod undo;
pub(crate) mod ver_map;

pub(crate) use retention::{
    prepare_catalog_redo_maintenance_operation, prepare_redo_truncation_operation,
};
pub(crate) use sys::RedoRetentionScope;
pub(crate) use sys_trx::{RetiredRowPageBatch, SysTrxPayload};

use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableCache, is_catalog_table};
use crate::completion::Completion;
use crate::engine::{EngineRef, WeakEngineRef};
use crate::error::{
    CompletionErrorBridge, DiscloseError, DiscloseResultExt, Error, FatalError, LifecycleError,
    LifecycleResult, OperationResult, ResourceError, Result, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult, RuntimeResult, SharedFatalError,
};
use crate::id::{SessionID, SessionOperationKey, TableID, TrxID};
use crate::lock::{
    FreshLockGuard, LockManager, LockMode, LockOwner, LockResource, LockScope, OwnerLockState,
    StmtNo, TableLockMode,
};
use crate::log::block_group::TrxLog;
use crate::log::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind};
use crate::map::FastHashMap;
use crate::notify::EventNotifyOnDrop;
use crate::obs;
use crate::quiescent::QuiescentGuard;
use crate::session::TrxAttachment;
use crate::trx::undo::{IndexPurgeEntry, IndexUndoLogs, RowUndoHead, RowUndoLogs, UndoStatus};
use error_stack::{Report, ResultExt};
use event_listener::{Event, EventListener};
use futures::FutureExt;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem;
use std::ops::AsyncFnOnce;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::ptr::addr_eq;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(crate) use admission::TableAdmissionRequest;
pub use stmt::Statement;
use stmt::StmtState;
pub use stream_stmt::{IndexScanMvccStream, StreamStmt};
/// Minimum snapshot timestamp assigned by the transaction system.
pub(crate) const MIN_SNAPSHOT_TS: TrxID = TrxID::new(1);
/// Exclusive upper bound for snapshot timestamps.
pub(crate) const MAX_SNAPSHOT_TS: TrxID = TrxID::new(1 << 63);
/// Exclusive upper bound for commit timestamps.
pub(crate) const MAX_COMMIT_TS: TrxID = TrxID::new(1 << 63);
// As active transaction id is always greater than STS, that means
// visibility check can be simplified to "STS is larger".
/// Minimum active transaction id derived from a snapshot timestamp.
pub(crate) const MIN_ACTIVE_TRX_ID: TrxID = TrxID::new((1 << 63) + 1);

/// Proof that one transaction's owner-local logical lock state was drained.
///
/// The proof is deliberately single-use and is minted only by terminal
/// transaction cleanup after the retained lock-manager guard is also dropped.
pub(crate) struct ReleasedTransactionLocks {
    trx_id: TrxID,
}

impl ReleasedTransactionLocks {
    #[inline]
    fn new(trx_id: TrxID) -> Self {
        ReleasedTransactionLocks { trx_id }
    }

    /// Consumes and validates this proof at the terminal session boundary.
    #[inline]
    pub(crate) fn assert_validated_for(self, attachment_trx_id: TrxID) {
        assert!(
            self.trx_id == attachment_trx_id,
            "released transaction-lock proof mismatch at terminal attachment boundary: \
             proof_trx_id={}, attachment_trx_id={attachment_trx_id}",
            self.trx_id
        );
    }
}

/// Public active transaction facade.
pub struct Transaction {
    trx_id: TrxID,
    sts: TrxID,
    operation_key: SessionOperationKey,
    engine: WeakEngineRef,
    terminal_started: bool,
}

impl Transaction {
    /// Create a weak public transaction facade for a stable session entry.
    #[inline]
    pub(crate) fn new(
        engine: WeakEngineRef,
        operation_key: SessionOperationKey,
        trx_id: TrxID,
        sts: TrxID,
    ) -> Self {
        Transaction {
            trx_id,
            sts,
            operation_key,
            engine,
            terminal_started: false,
        }
    }

    /// Resolve this handle and build an operation-local runtime attachment.
    #[inline]
    fn resolve_active(&self) -> LifecycleResult<(Arc<SessionOperationEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade().attach_with(|| {
            format!(
                "operation_key={}, trx_id={}, phase=upgrade_engine_runtime",
                self.operation_key, self.trx_id
            )
        })?;
        let admission = engine.acquire_admission().attach_with(|| {
            format!(
                "operation_key={}, trx_id={}",
                self.operation_key, self.trx_id
            )
        })?;
        let (entry, session) = engine
            .session_registry
            .resolve_operation(self.operation_key)?;
        drop(admission);
        let attachment = TrxAttachment::new(engine, session, self.operation_key, self.trx_id);
        Ok((entry, attachment))
    }

    /// Resolve this handle for terminal or cleanup paths.
    #[inline]
    fn resolve_terminal(&self) -> LifecycleResult<(Arc<SessionOperationEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade_for_terminal().attach_with(|| {
            format!(
                "operation_key={}, trx_id={}, phase=upgrade_engine_runtime",
                self.operation_key, self.trx_id
            )
        })?;
        self.resolve_with_engine(engine)
    }

    #[inline]
    fn resolve_with_engine(
        &self,
        engine: EngineRef,
    ) -> LifecycleResult<(Arc<SessionOperationEntry>, TrxAttachment)> {
        let (entry, session) = engine
            .session_registry
            .resolve_operation(self.operation_key)?;
        let attachment = TrxAttachment::new(engine, session, self.operation_key, self.trx_id);
        Ok((entry, attachment))
    }

    /// Check out the mutable core for one crate-internal operation.
    #[inline]
    pub(crate) fn checkout(&mut self) -> LifecycleResult<SessionOperationCheckout> {
        let (entry, attachment) = self.resolve_active()?;
        SessionOperationCheckout::new(entry, attachment)
    }

    /// Claim this transaction for an explicit terminal operation.
    #[inline]
    pub(crate) fn claim_terminal(&self) -> LifecycleResult<SessionOperationCompletionClaim> {
        let (entry, attachment) = self.resolve_terminal()?;
        SessionOperationCompletionClaim::terminal(entry, attachment)
    }

    /// Best-effort check that the transaction can still reach its engine.
    #[inline]
    pub(crate) fn engine(&self) -> Option<EngineRef> {
        self.engine.upgrade_for_cleanup()
    }

    /// Returns this transaction's current status timestamp.
    #[inline]
    pub fn trx_id(&self) -> TrxID {
        self.trx_id
    }

    /// Returns the transaction snapshot timestamp.
    #[inline]
    pub fn sts(&self) -> TrxID {
        self.sts
    }

    /// Acquires an explicit transaction-lifetime table lock.
    #[inline]
    pub async fn lock_table(&mut self, table_id: TableID, mode: TableLockMode) -> Result<()> {
        let mode = LockMode::from(mode);
        let mut checkout = self
            .checkout()
            .attach("operation=lock_explicit_table")
            .disclose()?;
        checkout.lock_table(table_id, mode).await.disclose()
    }

    /// Creates a statement facade for public caller-driven transaction streams.
    #[inline]
    pub fn stream_stmt(&mut self) -> StreamStmt<'_> {
        StreamStmt::new(self)
    }

    /// Executes one scoped statement callback inside this active transaction.
    ///
    /// Successful callbacks merge statement-local row undo, index undo, and
    /// redo effects into the transaction. Ordinary callback errors roll back
    /// only the current statement and leave previous successful statements
    /// transaction-owned. Dropping the future before its first poll performs no
    /// checkout. Dropping it after checkout synchronously settles
    /// statement-local ownership, terminally cancels the transaction, and
    /// queues whole-transaction rollback. The public transaction facade is
    /// discarded after that cancellation.
    #[inline]
    pub async fn exec<T, F>(&mut self, f: F) -> Result<T>
    where
        F: for<'borrow> AsyncFnOnce(&'borrow mut Statement<'_>) -> Result<T>,
    {
        let checkout = self
            .checkout()
            .attach("operation=execute_statement")
            .disclose()?;
        let mut stmt_state = StmtState::public(checkout);
        enum ExecOutcome<T> {
            Success(T),
            StatementError(Error),
            FatalRollback(Report<FatalError>),
        }
        let outcome = {
            let mut stmt = stmt_state.statement();
            match f(&mut stmt).await {
                Ok(value) => {
                    stmt.merge_effects();
                    ExecOutcome::Success(value)
                }
                Err(err) => match stmt.rollback_effects().await {
                    Ok(()) => ExecOutcome::StatementError(err),
                    Err(rollback_err) => ExecOutcome::FatalRollback(rollback_err),
                },
            }
        };
        match outcome {
            ExecOutcome::Success(value) => {
                stmt_state.return_ordinary();
                Ok(value)
            }
            ExecOutcome::StatementError(err) => {
                stmt_state.return_ordinary();
                Err(err)
            }
            ExecOutcome::FatalRollback(err) => {
                stmt_state.discard_after_fatal_rollback();
                Err(err.disclose())
            }
        }
    }

    /// Stages catalog DDL under an accepted operation's prepared logical locks.
    ///
    /// This is a narrow bridge for the current exact-owner lock manager.
    /// Reacquiring the same catalog claims for the nested transaction would be
    /// correctness-safe through same-family coverage, but would add duplicate
    /// manager grants and owner-cache entries. A future exact-family lock
    /// design should unify operation and transaction claims and remove this
    /// special authority path while preserving the panic settlement below.
    ///
    /// A callback panic is settled while the statement carrier still owns its
    /// partial effects. Incomplete redo is discarded, residual undo returns to
    /// the nested transaction core, and the original unwind resumes for the
    /// mandatory supervisor.
    #[inline]
    pub(crate) async fn stage_prepared_catalog_statement<T, F>(
        &mut self,
        authority: PreparedCatalogWriteAuthority<'_>,
        f: F,
    ) -> RuntimeResult<T>
    where
        F: for<'borrow> AsyncFnOnce(&'borrow mut Statement<'_>) -> RuntimeResult<T>,
    {
        let checkout = self
            .checkout()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=stage_prepared_catalog_statement")?;
        let mut stmt_state = StmtState::private(checkout);
        let outcome = AssertUnwindSafe(async {
            let mut stmt = stmt_state.prepared_catalog_statement(authority);
            let result = f(&mut stmt).await;
            stmt.merge_effects();
            result
        })
        .catch_unwind()
        .await;
        match outcome {
            Ok(result) => {
                stmt_state.return_ordinary();
                result
            }
            Err(panic) => {
                stmt_state.return_after_mandatory_panic();
                resume_unwind(panic);
            }
        }
    }

    /// Commit the transaction.
    #[inline]
    pub async fn commit(self) -> Result<TrxID> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .attach_with(|| {
                format!(
                    "operation=commit_active_transaction, session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                    trx.operation_key.session_id(), trx.trx_id
                )
            })
            .disclose()?;
        let claim = trx
            .claim_terminal()
            .attach("operation=commit_active_transaction")
            .disclose()?;
        engine.trx_sys.commit_transaction(claim).await
    }

    /// Rollback the transaction.
    #[inline]
    pub async fn rollback(self) -> Result<()> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .attach_with(|| {
                format!(
                    "operation=rollback_active_transaction, session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                    trx.operation_key.session_id(), trx.trx_id
                )
            })
            .disclose()?;
        let claim = trx
            .claim_terminal()
            .attach("operation=rollback_active_transaction")
            .disclose()?;
        engine.trx_sys.rollback_transaction(claim).await.disclose()
    }

    /// Commit a catalog DDL transaction without crossing the public error boundary.
    #[inline]
    pub(crate) async fn commit_catalog_ddl(self) -> RuntimeOrFatalResult<TrxID> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=commit_catalog_ddl, session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                    trx.operation_key.session_id(), trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        let claim = trx
            .claim_terminal()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=commit_catalog_ddl, session_id={}, trx_id={}",
                    trx.operation_key.session_id(),
                    trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        engine.trx_sys.commit_catalog_transaction(claim).await
    }

    /// Roll back a catalog DDL transaction without crossing the public error boundary.
    #[inline]
    pub(crate) async fn rollback_catalog_ddl(self) -> RuntimeOrFatalResult<()> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=rollback_catalog_ddl, session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                    trx.operation_key.session_id(), trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        let claim = trx
            .claim_terminal()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=rollback_catalog_ddl, session_id={}, trx_id={}",
                    trx.operation_key.session_id(),
                    trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        engine.trx_sys.rollback_catalog_transaction(claim).await
    }

    /// Roll back an engine-owned table-maintenance transaction without crossing
    /// the public error boundary.
    #[inline]
    pub(crate) async fn rollback_table_maintenance(self) -> RuntimeOrFatalResult<()> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=rollback_table_maintenance, session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                    trx.operation_key.session_id(), trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        let claim = trx
            .claim_terminal()
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=rollback_table_maintenance, session_id={}, trx_id={}",
                    trx.operation_key.session_id(),
                    trx.trx_id
                )
            })
            .map_err(RuntimeOrFatalError::from)?;
        engine
            .trx_sys
            .rollback_table_maintenance_transaction(claim)
            .await
    }
}

impl Drop for Transaction {
    #[inline]
    fn drop(&mut self) {
        if self.terminal_started {
            return;
        }
        if let Some(engine) = self.engine.upgrade_for_cleanup() {
            let abandoned = engine
                .session_registry
                .abandon_trx_handle(self.operation_key, self.trx_id);
            if abandoned {
                engine.trx_sys.request_abandoned_trx_cleanup(
                    engine.clone(),
                    self.operation_key,
                    self.trx_id,
                );
            }
        }
    }
}

/// Result of registering for a transaction's prepare completion.
pub(crate) enum PrepareListenerResult {
    /// The transaction was not preparing when registration started.
    NotPreparing,
    /// Registration succeeded and the listener must be awaited.
    Registered(EventListener),
    /// Prepare completion won the race with first-listener registration.
    Completed,
}

/// Shared transaction timestamp state referenced by row undo heads.
pub(crate) struct SharedTrxStatus {
    ts: AtomicU64,
    preparing: AtomicBool,
    prepare_ev: Mutex<Option<EventNotifyOnDrop>>,
    terminal: AtomicBool,
    terminal_ev: Event,
}

impl SharedTrxStatus {
    /// Create a uniquely owned zero-valued status for a ready transaction core.
    ///
    /// It is initialized with an active transaction id before the core is
    /// installed into a session-operation entry and becomes shareable.
    #[inline]
    fn ready() -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(0),
            preparing: AtomicBool::new(false),
            prepare_ev: Mutex::new(None),
            terminal: AtomicBool::new(false),
            terminal_ev: Event::new(),
        }
    }

    /// Initialize a uniquely owned ready status with an active transaction id.
    #[inline]
    fn init(&mut self, trx_id: TrxID) {
        assert!(
            trx_id >= MIN_ACTIVE_TRX_ID,
            "ready transaction status requires an active transaction id: trx_id={trx_id}"
        );
        assert!(
            *self.ts.get_mut() == 0,
            "ready transaction status timestamp must be zero before initialization"
        );
        assert!(
            !*self.preparing.get_mut(),
            "ready transaction status cannot be preparing"
        );
        assert!(
            self.prepare_ev.get_mut().is_none(),
            "ready transaction status cannot retain a prepare notifier"
        );
        assert!(
            !*self.terminal.get_mut(),
            "ready transaction status cannot be terminal"
        );
        *self.ts.get_mut() = trx_id.as_u64();
    }

    /// Returns the timestamp of current transaction.
    #[inline]
    pub(crate) fn ts(&self) -> TrxID {
        TrxID::new(self.ts.load(Ordering::Acquire))
    }

    /// Returns whether commit or successful rollback reached its terminal boundary.
    #[inline]
    pub(crate) fn terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    /// Registers for terminal resolution, or returns `None` if it already happened.
    #[inline]
    pub(crate) fn terminal_listener(&self) -> Option<EventListener> {
        if self.terminal() {
            return None;
        }
        let listener = self.terminal_ev.listen();
        (!self.terminal()).then_some(listener)
    }

    /// Returns whether this transaction is preparing.
    #[inline]
    pub(crate) fn preparing(&self) -> bool {
        self.preparing.load(Ordering::Acquire)
    }

    /// Registers a listener if the transaction is in prepare phase.
    ///
    /// Preparing means commit ordering has started but the transaction has not
    /// reached its terminal commit or failed-precommit rollback outcome.
    /// Waiters must wake for either terminal result and recheck the shared
    /// transaction status.
    #[inline]
    pub(crate) fn prepare_listener(&self) -> PrepareListenerResult {
        if !self.preparing.load(Ordering::Acquire) {
            return PrepareListenerResult::NotPreparing;
        }
        #[cfg(test)]
        tests::run_prepare_listener_before_lock_hook();
        let mut g = self.prepare_ev.lock();
        if let Some(event) = g.as_ref() {
            // Completion must take an installed event while holding this same
            // mutex, so finding one here proves notification is still owed.
            return PrepareListenerResult::Registered(event.listen());
        }
        if !self.preparing.load(Ordering::Acquire) {
            // Completion won between the optimistic load and mutex acquisition.
            return PrepareListenerResult::Completed;
        }
        let event = EventNotifyOnDrop::new();
        let listener = event.listen();
        *g = Some(event);
        PrepareListenerResult::Registered(listener)
    }

    /// Marks the transaction as preparing without allocating a notifier.
    #[inline]
    fn mark_preparing(&self) {
        assert!(
            !self.terminal(),
            "terminal transaction cannot enter prepare"
        );
        assert!(
            self.preparing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "transaction is already preparing"
        );
    }

    /// Publish the commit timestamp and wake prepare waiters.
    #[inline]
    fn commit_prepared(&self, cts: TrxID) {
        self.ts.store(cts.as_u64(), Ordering::SeqCst);
        self.finish_terminal();
        self.finish_preparing();
    }

    /// Publishes sticky successful terminal resolution and wakes all waiters.
    #[inline]
    fn finish_terminal(&self) {
        if !self.terminal.swap(true, Ordering::AcqRel) {
            self.terminal_ev.notify(usize::MAX);
        }
    }

    /// Marks prepare complete and wakes listeners registered before completion.
    #[inline]
    fn finish_preparing(&self) {
        let notifier = {
            let mut g = self.prepare_ev.lock();
            self.preparing.store(false, Ordering::SeqCst);
            g.take()
        };
        drop(notifier);
    }
}

/// Proof that a runtime read is bound to a live transaction context.
///
/// The proof carries only the transaction-context lifetime. Callers cannot
/// construct it directly, and table runtime code uses it to gate active-root
/// binding without borrowing mutable transaction or statement effects.
pub(crate) struct TrxReadProof<'ctx> {
    _ctx: PhantomData<&'ctx TrxContext>,
}

/// Immutable transaction identity and MVCC status.
pub(crate) struct TrxContext {
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    gc_no: usize,
}

impl TrxContext {
    /// Create a uniquely owned zero-valued context for an inactive core.
    #[inline]
    fn ready() -> Self {
        TrxContext {
            status: Arc::new(SharedTrxStatus::ready()),
            sts: TrxID::new(0),
            gc_no: 0,
        }
    }

    /// Initialize a ready context before it becomes visible to transaction work.
    #[inline]
    fn init(&mut self, trx_id: TrxID, sts: TrxID, gc_no: usize) {
        let status = Arc::get_mut(&mut self.status).unwrap_or_else(|| {
            panic!(
                "ready transaction status must be uniquely owned before initialization: \
                 trx_id={trx_id}"
            )
        });
        status.init(trx_id);
        self.sts = sts;
        self.gc_no = gc_no;
    }

    /// Returns the borrowed shared transaction status handle.
    #[inline]
    pub(crate) fn status(&self) -> &Arc<SharedTrxStatus> {
        &self.status
    }

    /// Returns whether the row undo head belongs to this transaction.
    #[inline]
    pub(crate) fn is_same_trx(&self, undo_head: &RowUndoHead) -> bool {
        match &undo_head.next.main.status {
            UndoStatus::Ref(arc) => addr_eq(self.status.as_ref(), arc.as_ref()),
            _ => false,
        }
    }

    /// Returns this transaction's current status timestamp.
    #[inline]
    pub(crate) fn trx_id(&self) -> TrxID {
        self.status.ts()
    }

    /// Returns the transaction snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.sts
    }

    /// Returns the GC bucket number assigned to the transaction.
    #[inline]
    pub(crate) fn gc_no(&self) -> usize {
        self.gc_no
    }

    /// Mint a proof for runtime reads tied to this transaction context.
    #[inline]
    pub(crate) fn read_proof(&self) -> TrxReadProof<'_> {
        TrxReadProof { _ctx: PhantomData }
    }

    /// Marks the shared transaction status as preparing.
    #[inline]
    pub(crate) fn mark_preparing(&self) {
        self.status.mark_preparing();
    }
}

/// Borrowed proof that accepted table DDL prepared catalog write locks.
///
/// The enclosing operation owns metadata-S and data-IX claims for longer than
/// its nested catalog transaction. This temporary capability lets that
/// transaction reuse those covering claims without registering duplicate
/// exact-owner grants. It is deliberately not a general lock-bypass flag: the
/// borrow ties its lifetime to the prepared lock scope and every catalog-table
/// write still asserts exact coverage.
#[derive(Clone, Copy)]
pub(crate) struct PreparedCatalogWriteAuthority<'a> {
    locks: &'a OwnerLockState,
}

impl<'a> PreparedCatalogWriteAuthority<'a> {
    /// Create a borrowed proof over one accepted operation's prepared locks.
    #[inline]
    pub(crate) fn new(locks: &'a OwnerLockState) -> Self {
        Self { locks }
    }

    /// Assert that the prepared owner covers one catalog-table write.
    #[inline]
    pub(crate) fn assert_table_write(self, table_id: TableID) {
        assert!(
            self.covers_table_write(table_id),
            "prepared catalog-write authority is incomplete: table_id={table_id}, owner={}",
            self.locks.owner()
        );
    }

    /// Return whether metadata-S and data-IX are both present.
    #[inline]
    pub(crate) fn covers_table_write(self, table_id: TableID) -> bool {
        self.locks
            .cached_covers(LockResource::TableMetadata(table_id), LockMode::Shared)
            && self
                .locks
                .cached_covers(LockResource::TableData(table_id), LockMode::IntentExclusive)
    }
}

/// Operation-local transaction runtime view.
///
/// Prepared catalog authority is present only for accepted table DDL. Ordinary
/// statements continue proving writes with transaction-owned logical locks.
/// The optional authority exists only to carry the temporary operation-claim
/// proof into lower-level write assertions.
#[derive(Clone, Copy)]
pub(crate) struct TrxRuntime<'r> {
    ctx: &'r TrxContext,
    attachment: &'r TrxAttachment,
    #[cfg_attr(
        not(debug_assertions),
        expect(
            dead_code,
            reason = "prepared authority participates in debug-only lower-level lock assertions"
        )
    )]
    prepared_catalog_write: Option<PreparedCatalogWriteAuthority<'r>>,
}

impl<'r> TrxRuntime<'r> {
    /// Create an operation-local runtime view.
    #[inline]
    pub(crate) fn new(ctx: &'r TrxContext, attachment: &'r TrxAttachment) -> Self {
        Self {
            ctx,
            attachment,
            prepared_catalog_write: None,
        }
    }

    /// Create a runtime view backed by prepared operation-level catalog locks.
    #[inline]
    pub(crate) fn new_prepared_catalog(
        ctx: &'r TrxContext,
        attachment: &'r TrxAttachment,
        authority: PreparedCatalogWriteAuthority<'r>,
    ) -> Self {
        Self {
            ctx,
            attachment,
            prepared_catalog_write: Some(authority),
        }
    }

    /// Returns this runtime's immutable transaction context.
    #[inline]
    pub(crate) fn ctx(&self) -> &'r TrxContext {
        self.ctx
    }

    /// Returns the crate-private engine runtime handle.
    #[inline]
    pub(crate) fn engine(&self) -> &'r EngineRef {
        self.attachment.engine()
    }

    /// Returns the cloned session pool guards retained by this operation.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &'r PoolGuards {
        self.attachment.pool_guards()
    }

    /// Returns the borrowed shared transaction status handle.
    #[inline]
    pub(crate) fn status(&self) -> &'r Arc<SharedTrxStatus> {
        self.ctx.status()
    }

    /// Returns whether the row undo head belongs to this transaction.
    #[inline]
    pub(crate) fn is_same_trx(&self, undo_head: &RowUndoHead) -> bool {
        self.ctx.is_same_trx(undo_head)
    }

    /// Returns the transaction snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.ctx.sts()
    }

    /// Mint a proof for runtime reads tied to this transaction context.
    #[inline]
    pub(crate) fn read_proof(&self) -> TrxReadProof<'r> {
        TrxReadProof { _ctx: PhantomData }
    }

    /// Debug-asserts that this transaction owns table-write intent.
    ///
    /// Foreground row, CDB, and index write paths call this before installing
    /// lower-level ownership. Recovery and no-transaction lifecycle paths stay
    /// outside those foreground call sites.
    #[inline]
    pub(crate) fn debug_assert_table_write_lock_held(&self, table_id: TableID) {
        #[cfg(debug_assertions)]
        {
            if self
                .prepared_catalog_write
                .is_some_and(|authority| authority.covers_table_write(table_id))
            {
                return;
            }
            let resource = LockResource::TableData(table_id);
            let owner = LockOwner::transaction(self.attachment.session_id(), self.ctx.trx_id());
            let held = self.engine().lock_manager().owner_holds(
                resource,
                owner,
                LockMode::IntentExclusive,
            );
            debug_assert!(
                held,
                "transaction owner must hold TableData(IX) or stronger before row/index ownership"
            );
        }

        #[cfg(not(debug_assertions))]
        {
            let _ = table_id;
        }
    }

    /// Loads the cached active insert page through the current attachment.
    #[inline]
    pub(crate) fn load_active_insert_page(&self, table_id: TableID) -> Option<VersionedPageID> {
        self.attachment.load_active_insert_page(table_id)
    }

    /// Saves the cached active insert page through the current attachment.
    #[inline]
    pub(crate) fn save_active_insert_page(&self, table_id: TableID, page_id: VersionedPageID) {
        self.attachment.save_active_insert_page(table_id, page_id);
    }
}

/// Mutable transaction-level effects accumulated across successful statements.
pub(crate) struct TrxEffects {
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    redo: RedoLogs,
}

impl TrxEffects {
    /// Create an empty transaction effects accumulator.
    #[inline]
    pub(crate) fn empty() -> Self {
        TrxEffects {
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    /// Returns whether this transaction needs a recovery-visible log record.
    ///
    /// Durability is the persistent timestamp carrier used by recovery. Runtime
    /// effects such as undo cleanup and row-page GC must
    /// not manufacture empty redo records just to enter group commit.
    #[inline]
    pub(crate) fn require_durability(&self) -> bool {
        !self.redo.is_empty()
    }

    /// Returns whether this transaction must pass through ordered commit.
    ///
    /// Ordered commit is the runtime barrier for CTS assignment, status/session
    /// completion, and GC handoff. A transaction may require ordered commit even
    /// when it does not require durability; in that case its CTS is volatile and
    /// must not be relied on by recovery.
    #[inline]
    pub(crate) fn require_ordered_commit(&self) -> bool {
        self.require_durability() || !self.row_undo.is_empty() || !self.index_undo.is_empty()
    }

    /// Returns mutable access to transaction row undo logs.
    #[inline]
    pub(crate) fn row_undo_mut(&mut self) -> &mut RowUndoLogs {
        &mut self.row_undo
    }

    /// Returns mutable access to transaction index undo logs.
    #[inline]
    pub(crate) fn index_undo_mut(&mut self) -> &mut IndexUndoLogs {
        &mut self.index_undo
    }

    /// Merges one successful statement's effects into this transaction.
    #[inline]
    pub(crate) fn merge_statement_effects(
        &mut self,
        row_undo: &mut RowUndoLogs,
        index_undo: &mut IndexUndoLogs,
        redo: RedoLogs,
    ) {
        self.row_undo.merge(row_undo);
        self.index_undo.merge(index_undo);
        self.redo.merge(redo);
    }

    /// Builds a redo log record when this transaction requires durability.
    #[inline]
    pub(crate) fn take_log(&mut self) -> Option<TrxLog> {
        if !self.require_durability() {
            None
        } else {
            Some(TrxLog::new(
                RedoHeader {
                    cts: TrxID::new(0),
                    trx_kind: RedoTrxKind::User,
                },
                mem::take(&mut self.redo),
            ))
        }
    }

    /// Validate invariants that need both DDL and DML redo context.
    #[inline]
    fn debug_assert_redo_invariants(&self) {
        debug_assert!(
            !self
                .redo
                .dml
                .keys()
                .any(|table_id| is_catalog_table(*table_id))
                || is_catalog_metadata_ddl(self.redo.ddl.as_deref()),
            "catalog table DML must be logged by a catalog metadata DDL transaction"
        );
    }

    /// Moves transaction effects into a prepared transaction payload.
    #[inline]
    pub(crate) fn take_payload_parts(&mut self) -> (RowUndoLogs, IndexUndoLogs) {
        (
            mem::take(&mut self.row_undo),
            mem::take(&mut self.index_undo),
        )
    }

    /// Clears effects after rollback or fatal cleanup.
    #[inline]
    pub(crate) fn clear_for_rollback(&mut self) {
        self.redo.clear();
        self.row_undo = RowUndoLogs::empty();
        self.index_undo = IndexUndoLogs::empty();
    }

    /// Move remaining rollback-owned effects into fatal retention.
    #[inline]
    fn take_for_fatal_retention(&mut self) -> FatalRollbackRetention {
        self.redo.clear();
        FatalRollbackRetention::Active {
            row_undo: mem::take(&mut self.row_undo),
            index_undo: mem::take(&mut self.index_undo),
        }
    }

    /// Asserts that all transaction effects have been consumed or cleared.
    #[inline]
    fn assert_cleared(&self) {
        assert!(self.redo.is_empty(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_undo.is_empty(), "index undo should be cleared");
    }
}

/// Stable purpose of one enclosing effectful session operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOperationKind {
    /// Public transaction whose handle outlives its begin call.
    PublicTransaction,
    /// Catalog or table DDL workflow.
    Ddl,
    /// Mutating maintenance or finite maintenance-scoped observation.
    Maintenance,
    /// Explicit session-lock mutation.
    SessionExplicitLock,
}

impl SessionOperationKind {
    /// Returns the stable diagnostic label for this operation purpose.
    #[inline]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::PublicTransaction => "public_transaction",
            Self::Ddl => "ddl",
            Self::Maintenance => "maintenance",
            Self::SessionExplicitLock => "session_explicit_lock",
        }
    }
}

/// Private transaction payload position inside a voluntary or mandatory operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InternalTrxState {
    Available,
    Running,
    CleanupReady,
    Completing,
}

/// Registry-visible owner of one stable session-operation entry.
///
/// Public transaction checkout remains `Voluntary(None)` and is inferred from
/// whether `trx_inner` is checked in. The consuming acceptance edge is the only
/// `Voluntary` to `Mandatory` transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOperationState {
    /// Caller-owned preparation or foreground execution.
    Voluntary(Option<InternalTrxState>),
    /// Runtime-owned accepted execution.
    Mandatory(Option<InternalTrxState>),
    /// A checked-in abandoned transaction may be claimed for cleanup.
    CleanupReady,
    /// Cleanup, prepare, group commit, or another terminal owner is active.
    Completing,
    /// Every transaction and outer-operation obligation is complete.
    Terminal,
    /// A safe residual owner is retained after a fatal failure.
    FailedRetained,
}

impl SessionOperationState {
    /// Returns whether this state still blocks operation admission and shutdown.
    #[cfg(test)]
    #[inline]
    pub(crate) const fn active(self) -> bool {
        !matches!(self, Self::Terminal)
    }

    /// Returns the stable snake-case diagnostic label.
    #[inline]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Voluntary(_) => "voluntary",
            Self::Mandatory(_) => "mandatory",
            Self::CleanupReady => "cleanup_ready",
            Self::Completing => "completing",
            Self::Terminal => "terminal",
            Self::FailedRetained => "failed_retained",
        }
    }
}

/// Coherent registry inspection snapshot for one stable operation entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionOperationSnapshot {
    /// Stable enclosing purpose.
    pub(crate) kind: SessionOperationKind,
    /// Current outer ownership label.
    pub(crate) state: SessionOperationState,
    /// Optional public or private transaction identity.
    pub(crate) trx_id: Option<TrxID>,
    /// Whether deferred or claimable transaction cleanup was requested.
    pub(crate) cleanup_requested: bool,
}

struct SessionOperationEntryInner {
    state: SessionOperationState,
    /// Identity of the currently attached transaction, if any.
    ///
    /// One DDL or maintenance operation may run sequential private
    /// transactions, so this changes with payload installation and completion
    /// under the same mutex rather than being immutable entry identity.
    trx_id: Option<TrxID>,
    /// Heap-stable transaction core allocated once at installation.
    ///
    /// Checkout and terminal ownership transfer this box rather than copying
    /// the substantially larger core between entry and async-future storage.
    trx_inner: Option<Box<TrxInner>>,
    /// Deferred cleanup intent for the attached transaction.
    ///
    /// This may be true in a running state while `trx_inner` is checked
    /// out and cleanup cannot yet claim the core. Returning that core publishes
    /// the matching cleanup-ready state and tells the checkout to submit cleanup.
    /// The entry retains the flag through cleanup claim and clears it only when
    /// the transaction finishes.
    cleanup_requested: bool,
    /// Whether the non-cloneable outer foreground authority for a DDL,
    /// maintenance, or explicit-lock operation is still alive.
    ///
    /// Public transactions have no separate outer authority and always use
    /// `false`. For private transactions this remains separate from `state`
    /// because dropping the outer authority while the core is checked out
    /// leaves `Voluntary(Some(Running))` unchanged until that core
    /// returns. The flag then determines whether return/completion resumes the
    /// attached outer operation or publishes outer cleanup/terminal state.
    outer_foreground_alive: bool,
}

/// Result of dropping the non-cloneable foreground operation authority.
pub(crate) struct SessionOperationForegroundRelease {
    /// Whether the operation reached terminal state and may finalize its session.
    pub(crate) terminal: bool,
    /// Optional private transaction cleanup that became claimable at this edge.
    pub(crate) cleanup: Option<TrxID>,
}

/// Session-owned stable operation entry.
///
/// One compact mutex serializes both owner labels and optional transaction
/// payload movement. The entry never stores a whole operation future or a
/// strong engine runtime handle.
pub(crate) struct SessionOperationEntry {
    key: SessionOperationKey,
    kind: SessionOperationKind,
    inner: Mutex<SessionOperationEntryInner>,
}

impl SessionOperationEntry {
    /// Creates a foreground-owned DDL, maintenance, or explicit-lock entry.
    #[inline]
    pub(crate) fn new(key: SessionOperationKey, kind: SessionOperationKind) -> Arc<Self> {
        assert!(
            kind != SessionOperationKind::PublicTransaction,
            "public transaction entry requires an installed transaction payload: key={key}"
        );
        Arc::new(Self {
            key,
            kind,
            inner: Mutex::new(SessionOperationEntryInner {
                state: SessionOperationState::Voluntary(None),
                trx_id: None,
                trx_inner: None,
                cleanup_requested: false,
                outer_foreground_alive: true,
            }),
        })
    }

    /// Creates the single stable entry for one public transaction.
    #[inline]
    pub(crate) fn new_public_transaction(
        key: SessionOperationKey,
        inner: Box<TrxInner>,
    ) -> Arc<Self> {
        let trx_id = inner.trx_id();
        Arc::new(Self {
            key,
            kind: SessionOperationKind::PublicTransaction,
            inner: Mutex::new(SessionOperationEntryInner {
                state: SessionOperationState::Voluntary(None),
                trx_id: Some(trx_id),
                trx_inner: Some(inner),
                cleanup_requested: false,
                outer_foreground_alive: false,
            }),
        })
    }

    /// Returns this entry's exact enclosing operation key.
    #[inline]
    pub(crate) const fn key(&self) -> SessionOperationKey {
        self.key
    }

    /// Returns this entry's immutable purpose.
    #[inline]
    pub(crate) const fn kind(&self) -> SessionOperationKind {
        self.kind
    }

    /// Acquires the entry mutex and returns a coherent state and
    /// transaction-identity snapshot.
    #[inline]
    pub(crate) fn inspect(&self) -> SessionOperationSnapshot {
        let inner = self.inner.lock();
        SessionOperationSnapshot {
            kind: self.kind,
            state: inner.state,
            trx_id: inner.trx_id,
            cleanup_requested: inner.cleanup_requested,
        }
    }

    /// Returns the checked-in core allocation address for lifecycle tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn inner_ptr_for_test(&self) -> Option<usize> {
        self.inner
            .lock()
            .trx_inner
            .as_deref()
            .map(|inner| inner as *const TrxInner as usize)
    }

    /// Installs one private transaction inside a DDL or maintenance entry.
    #[inline]
    pub(crate) fn install_private_transaction(&self, trx_inner: Box<TrxInner>) {
        assert!(
            matches!(
                self.kind,
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance
            ),
            "private transaction requires DDL or maintenance operation: key={}, kind={}",
            self.key,
            self.kind.label()
        );
        let trx_id = trx_inner.trx_id();
        let mut inner = self.inner.lock();
        let next_state = match inner.state {
            SessionOperationState::Voluntary(None) if inner.outer_foreground_alive => {
                SessionOperationState::Voluntary(Some(InternalTrxState::Available))
            }
            SessionOperationState::Mandatory(None) if !inner.outer_foreground_alive => {
                SessionOperationState::Mandatory(Some(InternalTrxState::Available))
            }
            _ => panic!(
                "private transaction installation requires empty voluntary or mandatory authority: key={}, state={}, trx_id={:?}",
                self.key,
                inner.state.label(),
                inner.trx_id
            ),
        };
        assert!(
            inner.trx_id.is_none() && inner.trx_inner.is_none(),
            "private transaction installation requires an empty payload slot: key={}, state={}, trx_id={:?}",
            self.key,
            inner.state.label(),
            inner.trx_id
        );
        inner.state = next_state;
        inner.trx_id = Some(trx_id);
        inner.trx_inner = Some(trx_inner);
    }

    #[inline]
    fn take_for_checkout(&self, trx_id: TrxID) -> LifecycleResult<Box<TrxInner>> {
        let mut inner = self.inner.lock();
        if inner.trx_id != Some(trx_id) {
            return Err(
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "operation_key={}, expected_trx_id={trx_id}, actual_trx_id={}",
                    self.key,
                    inner
                        .trx_id
                        .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string())
                )),
            );
        }
        let next_state = match self.kind {
            SessionOperationKind::PublicTransaction
                if inner.state == SessionOperationState::Voluntary(None)
                    && inner.trx_inner.is_some() =>
            {
                None
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.state
                    == SessionOperationState::Voluntary(Some(InternalTrxState::Available)) =>
            {
                Some(SessionOperationState::Voluntary(Some(
                    InternalTrxState::Running,
                )))
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.state
                    == SessionOperationState::Mandatory(Some(InternalTrxState::Available)) =>
            {
                Some(SessionOperationState::Mandatory(Some(
                    InternalTrxState::Running,
                )))
            }
            SessionOperationKind::PublicTransaction
            | SessionOperationKind::Ddl
            | SessionOperationKind::Maintenance
            | SessionOperationKind::SessionExplicitLock => {
                return Err(session_operation_entry_state_err(
                    self.key, self.kind, &inner,
                ));
            }
        };
        let trx_inner = inner.trx_inner.take().unwrap_or_else(|| {
            panic!(
                "available operation transaction must retain its checked-in core: key={}",
                self.key
            )
        });
        if let Some(next_state) = next_state {
            inner.state = next_state;
        }
        Ok(trx_inner)
    }

    #[inline]
    fn take_for_terminal(&self, trx_id: TrxID) -> LifecycleResult<Box<TrxInner>> {
        let mut inner = self.inner.lock();
        if inner.trx_id != Some(trx_id) {
            return Err(
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "operation_key={}, expected_trx_id={trx_id}, actual_trx_id={}",
                    self.key,
                    inner
                        .trx_id
                        .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string())
                )),
            );
        }
        let next_state = match self.kind {
            SessionOperationKind::PublicTransaction
                if inner.state == SessionOperationState::Voluntary(None)
                    && inner.trx_inner.is_some() =>
            {
                SessionOperationState::Completing
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.state
                    == SessionOperationState::Voluntary(Some(InternalTrxState::Available)) =>
            {
                SessionOperationState::Voluntary(Some(InternalTrxState::Completing))
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.state
                    == SessionOperationState::Mandatory(Some(InternalTrxState::Available)) =>
            {
                SessionOperationState::Mandatory(Some(InternalTrxState::Completing))
            }
            SessionOperationKind::PublicTransaction
            | SessionOperationKind::Ddl
            | SessionOperationKind::Maintenance
            | SessionOperationKind::SessionExplicitLock => {
                return Err(session_operation_entry_state_err(
                    self.key, self.kind, &inner,
                ));
            }
        };
        let trx_inner = inner.trx_inner.take().unwrap_or_else(|| {
            panic!(
                "terminal operation claim requires checked-in transaction core: key={}",
                self.key
            )
        });
        inner.state = next_state;
        Ok(trx_inner)
    }

    #[inline]
    fn take_for_cleanup(&self, trx_id: TrxID) -> LifecycleResult<Box<TrxInner>> {
        let mut inner = self.inner.lock();
        if inner.trx_id != Some(trx_id) {
            return Err(
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "operation_key={}, expected_trx_id={trx_id}, actual_trx_id={}",
                    self.key,
                    inner
                        .trx_id
                        .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string())
                )),
            );
        }
        let next_state = match self.kind {
            SessionOperationKind::PublicTransaction
                if inner.state == SessionOperationState::CleanupReady =>
            {
                SessionOperationState::Completing
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.outer_foreground_alive
                    && inner.state
                        == SessionOperationState::Voluntary(Some(
                            InternalTrxState::CleanupReady,
                        )) =>
            {
                SessionOperationState::Voluntary(Some(InternalTrxState::Completing))
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if !inner.outer_foreground_alive
                    && inner.state == SessionOperationState::CleanupReady =>
            {
                SessionOperationState::Completing
            }
            SessionOperationKind::PublicTransaction
            | SessionOperationKind::Ddl
            | SessionOperationKind::Maintenance
            | SessionOperationKind::SessionExplicitLock => {
                return Err(session_operation_entry_state_err(
                    self.key, self.kind, &inner,
                ));
            }
        };
        let trx_inner = inner.trx_inner.take().unwrap_or_else(|| {
            panic!(
                "cleanup-ready operation must retain checked-in transaction core: key={}",
                self.key
            )
        });
        inner.state = next_state;
        Ok(trx_inner)
    }

    /// Restores one checked-out transaction core to its stable entry.
    ///
    /// Returns true only when abandonment occurred while the core was checked
    /// out and returning it made cleanup claimable. The caller then publishes
    /// the transition and submits cleanup using its authoritative
    /// [`TrxAttachment`] identity.
    #[inline]
    fn return_inner(&self, trx_inner: Box<TrxInner>) -> bool {
        let mut inner = self.inner.lock();
        let running = match self.kind {
            SessionOperationKind::PublicTransaction => {
                inner.state == SessionOperationState::Voluntary(None)
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance => {
                matches!(
                    inner.state,
                    SessionOperationState::Voluntary(Some(InternalTrxState::Running))
                        | SessionOperationState::Mandatory(Some(InternalTrxState::Running))
                )
            }
            SessionOperationKind::SessionExplicitLock => false,
        };
        assert!(
            running && inner.trx_inner.is_none(),
            "transaction lease return requires one checked-out core: key={}, state={}",
            self.key,
            inner.state.label()
        );
        let trx_id = trx_inner.trx_id();
        assert!(
            inner.trx_id == Some(trx_id),
            "transaction lease identity mismatch: key={}, expected_trx_id={:?}, returned_trx_id={trx_id}",
            self.key,
            inner.trx_id
        );
        inner.trx_inner = Some(trx_inner);
        if inner.cleanup_requested {
            inner.state = match self.kind {
                SessionOperationKind::PublicTransaction => SessionOperationState::CleanupReady,
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                    if inner.outer_foreground_alive =>
                {
                    SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady))
                }
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance => {
                    SessionOperationState::CleanupReady
                }
                SessionOperationKind::SessionExplicitLock => {
                    panic!(
                        "explicit-lock operation cannot return a transaction core: key={}",
                        self.key
                    )
                }
            };
            return true;
        }
        if self.kind != SessionOperationKind::PublicTransaction {
            inner.state = match self.kind {
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance => {
                    match inner.state {
                        SessionOperationState::Voluntary(Some(InternalTrxState::Running))
                            if inner.outer_foreground_alive =>
                        {
                            SessionOperationState::Voluntary(Some(InternalTrxState::Available))
                        }
                        SessionOperationState::Mandatory(Some(InternalTrxState::Running))
                            if !inner.outer_foreground_alive =>
                        {
                            SessionOperationState::Mandatory(Some(InternalTrxState::Available))
                        }
                        _ => panic!(
                            "private transaction return requires matching outer authority: key={}, state={}",
                            self.key,
                            inner.state.label()
                        ),
                    }
                }
                SessionOperationKind::SessionExplicitLock => {
                    panic!(
                        "explicit-lock operation cannot return a transaction core: key={}",
                        self.key
                    )
                }
                SessionOperationKind::PublicTransaction => unreachable!(),
            };
        }
        false
    }

    /// Returns a cancelled public statement directly to terminal cleanup.
    #[inline]
    fn return_cancelled(&self, trx_inner: Box<TrxInner>) {
        let mut inner = self.inner.lock();
        assert!(
            self.kind == SessionOperationKind::PublicTransaction
                && inner.state == SessionOperationState::Voluntary(None)
                && inner.trx_inner.is_none(),
            "cancelled statement return requires one checked-out public transaction core: key={}, kind={}, state={}",
            self.key,
            self.kind.label(),
            inner.state.label()
        );
        let trx_id = trx_inner.trx_id();
        assert!(
            inner.trx_id == Some(trx_id),
            "cancelled statement identity mismatch: key={}, expected_trx_id={:?}, returned_trx_id={trx_id}",
            self.key,
            inner.trx_id
        );
        inner.trx_inner = Some(trx_inner);
        inner.cleanup_requested = true;
        inner.state = SessionOperationState::CleanupReady;
    }

    /// Marks the exact attached transaction abandoned after its owning handle
    /// or outer foreground authority is dropped.
    ///
    /// A stale transaction identity is neutral.
    #[inline]
    pub(crate) fn abandon_transaction(&self, trx_id: TrxID) -> bool {
        let mut inner = self.inner.lock();
        if inner.trx_id != Some(trx_id) {
            return false;
        }
        match (self.kind, inner.state) {
            (SessionOperationKind::PublicTransaction, SessionOperationState::Voluntary(None))
                if inner.trx_inner.is_some() =>
            {
                inner.cleanup_requested = true;
                inner.state = SessionOperationState::CleanupReady;
                true
            }
            (SessionOperationKind::PublicTransaction, SessionOperationState::Voluntary(None))
            | (
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance,
                SessionOperationState::Voluntary(Some(InternalTrxState::Running)),
            ) => {
                inner.cleanup_requested = true;
                true
            }
            (
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance,
                SessionOperationState::Voluntary(Some(InternalTrxState::Available)),
            ) => {
                inner.cleanup_requested = true;
                inner.state =
                    SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady));
                true
            }
            (
                SessionOperationKind::Ddl | SessionOperationKind::Maintenance,
                SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady)),
            )
            | (
                SessionOperationKind::PublicTransaction
                | SessionOperationKind::Ddl
                | SessionOperationKind::Maintenance,
                SessionOperationState::CleanupReady,
            ) => {
                assert!(
                    inner.cleanup_requested,
                    "cleanup-ready operation requires cleanup intent: key={}",
                    self.key
                );
                true
            }
            // Keep every operation kind and outer state explicit so adding a variant
            // requires this abandonment policy to be reviewed. The nested-state
            // wildcard is intentional: positions not accepted above conservatively
            // produce no newly actionable abandonment.
            (
                SessionOperationKind::PublicTransaction
                | SessionOperationKind::Ddl
                | SessionOperationKind::Maintenance
                | SessionOperationKind::SessionExplicitLock,
                SessionOperationState::Voluntary(_)
                | SessionOperationState::CleanupReady
                | SessionOperationState::Completing
                | SessionOperationState::Mandatory(_)
                | SessionOperationState::Terminal
                | SessionOperationState::FailedRetained,
            ) => false,
        }
    }

    /// Drops the foreground outer-operation authority.
    ///
    /// The caller holds the session lifecycle mutex, preserving the global
    /// `lifecycle -> entry` lock order.
    #[inline]
    pub(crate) fn release_foreground(&self) -> SessionOperationForegroundRelease {
        assert!(
            self.kind != SessionOperationKind::PublicTransaction,
            "public transaction entry has no outer foreground authority: key={}",
            self.key
        );
        let mut inner = self.inner.lock();
        assert!(
            inner.outer_foreground_alive,
            "operation foreground authority released more than once: key={}",
            self.key
        );
        // A running private core cannot be relabeled until its lease returns,
        // so record outer detachment separately before promoting every state
        // whose current owner can already be represented by an outer variant.
        inner.outer_foreground_alive = false;
        if inner.state == SessionOperationState::FailedRetained {
            return SessionOperationForegroundRelease {
                terminal: false,
                cleanup: None,
            };
        }
        let mut cleanup = None;
        inner.state = match inner.state {
            SessionOperationState::Voluntary(None) => SessionOperationState::Terminal,
            SessionOperationState::Voluntary(Some(InternalTrxState::Available)) => {
                let trx_id = inner.trx_id.unwrap_or_else(|| {
                    panic!(
                        "available private transaction requires identity: key={}",
                        self.key
                    )
                });
                inner.cleanup_requested = true;
                cleanup = Some(trx_id);
                SessionOperationState::CleanupReady
            }
            SessionOperationState::Voluntary(Some(InternalTrxState::Running)) => {
                inner.cleanup_requested = true;
                SessionOperationState::Voluntary(Some(InternalTrxState::Running))
            }
            SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady)) => {
                SessionOperationState::CleanupReady
            }
            SessionOperationState::Voluntary(Some(InternalTrxState::Completing)) => {
                SessionOperationState::Completing
            }
            SessionOperationState::CleanupReady
            | SessionOperationState::Completing
            | SessionOperationState::Mandatory(_)
            | SessionOperationState::Terminal
            | SessionOperationState::FailedRetained => {
                panic!(
                    "foreground release requires foreground-running operation state: key={}, state={}",
                    self.key,
                    inner.state.label()
                )
            }
        };
        SessionOperationForegroundRelease {
            terminal: inner.state == SessionOperationState::Terminal,
            cleanup,
        }
    }

    /// Publishes fatal retention after transaction payload ownership moved out.
    #[inline]
    pub(crate) fn fail_retained(&self) {
        let mut inner = self.inner.lock();
        inner.trx_inner.take();
        inner.state = SessionOperationState::FailedRetained;
    }

    /// Completes the exact attached transaction at the ordered terminal edge.
    ///
    /// Returns whether the matching transaction finished and, when it did,
    /// whether the outer operation also became terminal.
    #[inline]
    pub(crate) fn finish_transaction(&self, trx_id: TrxID) -> Option<bool> {
        let mut inner = self.inner.lock();
        let completion_owned = match self.kind {
            SessionOperationKind::PublicTransaction => {
                inner.state == SessionOperationState::Completing
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.outer_foreground_alive =>
            {
                matches!(
                    inner.state,
                    SessionOperationState::Voluntary(Some(InternalTrxState::Completing))
                )
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance
                if inner.state
                    == SessionOperationState::Mandatory(Some(InternalTrxState::Completing)) =>
            {
                true
            }
            SessionOperationKind::Ddl | SessionOperationKind::Maintenance => {
                inner.state == SessionOperationState::Completing
            }
            SessionOperationKind::SessionExplicitLock => false,
        };
        if inner.trx_id != Some(trx_id) || !completion_owned {
            return None;
        }
        inner.trx_inner.take();
        inner.trx_id = None;
        inner.cleanup_requested = false;
        match inner.state {
            SessionOperationState::Mandatory(Some(InternalTrxState::Completing)) => {
                inner.state = SessionOperationState::Mandatory(None);
                Some(false)
            }
            SessionOperationState::Voluntary(Some(InternalTrxState::Completing)) => {
                inner.state = SessionOperationState::Voluntary(None);
                Some(false)
            }
            SessionOperationState::Completing => {
                inner.state = SessionOperationState::Terminal;
                Some(true)
            }
            _ => None,
        }
    }

    /// Returns whether shutdown may claim this entry's transaction cleanup.
    #[inline]
    pub(crate) fn cleanup_candidate(&self) -> Option<TrxID> {
        let inner = self.inner.lock();
        matches!(
            inner.state,
            SessionOperationState::CleanupReady
                | SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady))
        )
        .then(|| {
            assert!(
                inner.cleanup_requested,
                "cleanup-ready operation requires cleanup intent: key={}",
                self.key
            );
            inner.trx_id.unwrap_or_else(|| {
                panic!(
                    "cleanup-ready operation requires transaction identity: key={}",
                    self.key
                )
            })
        })
    }

    /// Transfer the sole empty voluntary operation authority to the runtime.
    ///
    /// The caller holds the session lifecycle mutex, preserving the global
    /// `lifecycle -> entry` lock order.
    #[inline]
    pub(crate) fn accept_mandatory(&self) {
        let mut inner = self.inner.lock();
        inner.outer_foreground_alive = false;
        inner.state = SessionOperationState::Mandatory(None);
    }

    /// Verify successful mandatory completion while execution is supervised.
    #[inline]
    pub(crate) fn assert_mandatory_finish_ready(&self) {
        let inner = self.inner.lock();
        assert!(
            inner.state == SessionOperationState::Mandatory(None)
                && inner.trx_id.is_none()
                && inner.trx_inner.is_none(),
            "mandatory completion requires empty accepted authority: key={}, state={}, trx_id={:?}",
            self.key,
            inner.state.label(),
            inner.trx_id
        );
    }

    /// Publish terminal state after execution-side validation succeeded.
    #[inline]
    pub(crate) fn publish_mandatory_terminal(&self) {
        self.inner.lock().state = SessionOperationState::Terminal;
    }

    /// Publish safe fatal retention for an unexpectedly lost mandatory owner.
    #[inline]
    pub(crate) fn fail_mandatory_retained(&self) {
        let mut inner = self.inner.lock();
        if matches!(inner.state, SessionOperationState::Mandatory(_)) {
            inner.state = SessionOperationState::FailedRetained;
        }
    }
}

/// Private RAII checkout for one non-terminal transaction operation.
///
/// `SessionOperationCheckout` is mechanical ownership plumbing: it moves the
/// heap-stable [`TrxInner`] box out of the stable entry, owns the operation-local
/// runtime attachment, and restores the same box on ordinary drop. Statement
/// ownership policy lives in
/// [`StmtState`], its callback facade is [`Statement`], and terminal commit and
/// rollback use private completion claims.
pub(crate) struct SessionOperationCheckout {
    entry: Arc<SessionOperationEntry>,
    inner: Option<Box<TrxInner>>,
    attachment: TrxAttachment,
}

impl SessionOperationCheckout {
    #[inline]
    fn new(entry: Arc<SessionOperationEntry>, attachment: TrxAttachment) -> LifecycleResult<Self> {
        let inner = entry.take_for_checkout(attachment.trx_id())?;
        Ok(Self {
            entry,
            inner: Some(inner),
            attachment,
        })
    }

    /// Returns this checkout's immutable transaction core.
    #[inline]
    pub(crate) fn inner(&self) -> &TrxInner {
        self.inner
            .as_ref()
            .expect("SessionOperationCheckout always owns an inner until fatal discard")
    }

    /// Returns this checkout's mutable transaction core.
    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut TrxInner {
        self.inner
            .as_mut()
            .expect("SessionOperationCheckout always owns an inner until fatal discard")
    }

    /// Returns mutable transaction state and the operation-local attachment.
    #[inline]
    pub(crate) fn inner_and_attachment_mut(&mut self) -> (&mut TrxInner, &TrxAttachment) {
        let inner = self
            .inner
            .as_mut()
            .expect("SessionOperationCheckout always owns an inner until fatal discard");
        (inner, &self.attachment)
    }

    /// Returns this checkout's operation-local attachment.
    #[inline]
    pub(crate) fn attachment(&self) -> &TrxAttachment {
        &self.attachment
    }

    /// Acquires an explicit transaction-lifetime table lock.
    #[inline]
    pub(crate) async fn lock_table(
        &mut self,
        table_id: TableID,
        mode: LockMode,
    ) -> OperationResult<()> {
        let Self {
            inner, attachment, ..
        } = self;
        let inner = inner
            .as_mut()
            .expect("SessionOperationCheckout always owns an inner until fatal discard");
        inner.lock_table(attachment, table_id, mode).await
    }

    /// Clear a fatally discarded inner and leave the entry impossible to reuse.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            let retention = inner.retain_and_discard_after_fatal_rollback(&self.attachment);
            self.attachment
                .engine()
                .trx_sys
                .retain_fatal_rollback(retention);
        }
        self.entry.fail_retained();
        self.attachment.notify_operation_transition();
    }

    /// Returns a cancelled public statement directly to cleanup ownership.
    #[inline]
    pub(crate) fn return_cancelled(mut self) {
        let inner = self
            .inner
            .take()
            .expect("cancelled statement checkout must retain its transaction core");
        self.entry.return_cancelled(inner);
        self.attachment.notify_operation_transition();
        self.attachment.request_abandoned_cleanup();
    }
}

impl Drop for SessionOperationCheckout {
    #[inline]
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        if self.entry.return_inner(inner) {
            self.attachment.notify_operation_transition();
            self.attachment.request_abandoned_cleanup();
        }
    }
}

/// Private ownership claim for explicit terminal and cleanup paths.
pub(crate) struct SessionOperationCompletionClaim {
    entry: Arc<SessionOperationEntry>,
    inner: Option<Box<TrxInner>>,
    attachment: Option<TrxAttachment>,
}

impl SessionOperationCompletionClaim {
    #[inline]
    fn terminal(
        entry: Arc<SessionOperationEntry>,
        attachment: TrxAttachment,
    ) -> LifecycleResult<Self> {
        let inner = entry.take_for_terminal(attachment.trx_id())?;
        Ok(Self {
            entry,
            inner: Some(inner),
            attachment: Some(attachment),
        })
    }

    /// Claim an abandoned checked-in transaction for cleanup rollback.
    #[inline]
    pub(crate) fn cleanup(
        entry: Arc<SessionOperationEntry>,
        attachment: TrxAttachment,
    ) -> LifecycleResult<Self> {
        let inner = entry.take_for_cleanup(attachment.trx_id())?;
        Ok(Self {
            entry,
            inner: Some(inner),
            attachment: Some(attachment),
        })
    }

    /// Borrow the claimed entry, mutable core, and attachment for rollback.
    #[inline]
    pub(crate) fn parts_mut(
        &mut self,
    ) -> (&Arc<SessionOperationEntry>, &mut TrxInner, &TrxAttachment) {
        let Self {
            entry,
            inner,
            attachment,
        } = self;
        (
            entry,
            inner
                .as_deref_mut()
                .expect("active completion claim retains transaction core"),
            attachment
                .as_ref()
                .expect("active completion claim retains terminal attachment"),
        )
    }

    /// Move terminal parts after borrowed rollback work succeeds.
    #[inline]
    pub(crate) fn take_parts(
        &mut self,
    ) -> (Arc<SessionOperationEntry>, Box<TrxInner>, TrxAttachment) {
        (
            Arc::clone(&self.entry),
            self.inner
                .take()
                .expect("completion claim transaction core moves exactly once"),
            self.attachment
                .take()
                .expect("completion claim attachment moves exactly once"),
        )
    }

    /// Retain all residual rollback ownership after a supervised task panic.
    #[inline]
    pub(crate) fn preserve_after_panic(&mut self) {
        let Some(inner) = self.inner.as_deref_mut() else {
            return;
        };
        if !inner.active {
            return;
        }
        let attachment = self
            .attachment
            .as_ref()
            .expect("active completion claim retains terminal attachment");
        self.entry.fail_retained();
        attachment.notify_operation_transition();
        let retention = inner.retain_and_discard_after_fatal_rollback(attachment);
        attachment.engine().trx_sys.retain_fatal_rollback(retention);
    }

    /// Consume and return claimed parts in non-supervised paths.
    #[inline]
    pub(crate) fn into_parts(
        mut self,
    ) -> (Arc<SessionOperationEntry>, Box<TrxInner>, TrxAttachment) {
        self.take_parts()
    }

    /// Returns the engine retained by this terminal or cleanup claim.
    #[inline]
    pub(crate) fn engine(&self) -> &EngineRef {
        self.attachment
            .as_ref()
            .expect("active completion claim retains terminal attachment")
            .engine()
    }

    /// Returns the exact transaction identity retained by this claim.
    #[cfg(test)]
    #[inline]
    pub(crate) const fn trx_id(&self) -> TrxID {
        self.attachment
            .as_ref()
            .expect("active completion claim retains terminal attachment")
            .trx_id()
    }
}

/// Abandoned transaction cleanup job.
///
/// The job carries an `EngineRef`, so submitted cleanup pins the runtime until
/// the mandatory-runtime task has either claimed and rolled back the abandoned
/// transaction or found that the session/transaction is no longer claimable.
/// Engine shutdown keeps scanning abandoned sessions and waits for active
/// transaction state to reach a terminal state before component teardown
/// begins.
pub(crate) struct SessionOperationCleanupJob {
    /// Engine retained until cleanup resolves this job.
    pub(crate) engine: EngineRef,
    /// Exact stable operation that owns the abandoned transaction.
    pub(crate) operation_key: SessionOperationKey,
    /// Exact transaction id validated by the entry cleanup claim.
    ///
    /// Registry lookup uses only `operation_key`; claim-time validation under
    /// the entry mutex prevents stale work from taking a replacement private
    /// transaction attached to the same operation.
    pub(crate) trx_id: TrxID,
    /// Claimed cleanup ownership retained outside the panic-caught future.
    pub(crate) claim: Option<SessionOperationCompletionClaim>,
}

/// Reason a precommit transaction has to rollback instead of commit.
#[derive(Clone, Debug)]
pub(crate) enum FailedPrecommitReason {
    /// Redo write, submit, or sync failed after precommit handoff.
    Fatal(SharedFatalError),
    /// A single precommit group exceeded an intrinsic redo resource limit.
    Resource(ResourceError),
    /// Group commit admission closed for engine shutdown.
    Shutdown,
}

impl FailedPrecommitReason {
    /// Convert this rejection at a Runtime-or-Fatal system-commit boundary.
    #[inline]
    pub(crate) fn into_runtime_or_fatal(self) -> RuntimeOrFatalError {
        match self {
            FailedPrecommitReason::Fatal(error) => RuntimeOrFatalError::from(error),
            FailedPrecommitReason::Resource(reason) => RuntimeOrFatalError::from(
                Report::new(reason).change_context(RuntimeError::SystemTransactionCommit),
            ),
            FailedPrecommitReason::Shutdown => RuntimeOrFatalError::from(
                Report::new(LifecycleError::Shutdown)
                    .change_context(RuntimeError::SystemTransactionCommit),
            ),
        }
    }

    #[inline]
    fn completion_bridge(self, message: &'static str) -> CompletionErrorBridge {
        match self {
            FailedPrecommitReason::Fatal(error) => error.into_completion_bridge(),
            FailedPrecommitReason::Resource(reason) => {
                CompletionErrorBridge::capture(Report::new(reason).attach(message))
            }
            FailedPrecommitReason::Shutdown => CompletionErrorBridge::capture(
                Report::new(LifecycleError::Shutdown).attach(message),
            ),
        }
    }
}

/// Terminal ownership outcome after attempting failed-precommit rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
enum FailedPrecommitRollbackOutcome {
    /// Rollback completed and released all transaction-owned effects.
    RolledBack,
    /// Rollback failed and all remaining effects moved to fatal retention.
    FailedRetained,
}

/// Failed-precommit rollback job submitted to the mandatory runtime.
///
/// This path is mandatory for user transactions. Once a transaction enters
/// precommit, row/index changes may already be installed in in-memory MVCC
/// structures, while redo durability can still fail. Dropping the precommit
/// payload without rollback can leave row-version links pointing at
/// transaction-owned undo memory, so failed-precommit cleanup must run before
/// the shared completion wakes commit waiters.
///
/// The mandatory runtime owns these jobs instead of the log thread so redo
/// failure handling can hand over rollback work and continue draining or
/// shutting down its own queues. `TransactionRedoWorkers` joins the log thread
/// before `MandatoryRuntimeWorkers` closes and drains internal admission, so
/// every failed-precommit job produced by redo is accepted before cleanup
/// shutdown.
pub(crate) struct FailedPrecommitCleanupJob {
    trx_list: Vec<PrecommitTrx>,
    completion: Arc<Completion<()>>,
    reason: Option<FailedPrecommitReason>,
}

impl FailedPrecommitCleanupJob {
    /// Create a failed-precommit cleanup job for one redo group result.
    #[inline]
    pub(crate) fn new(
        trx_list: Vec<PrecommitTrx>,
        completion: Arc<Completion<()>>,
        reason: FailedPrecommitReason,
    ) -> Self {
        Self {
            trx_list,
            completion,
            reason: Some(reason),
        }
    }

    #[inline]
    async fn run(&mut self) {
        while let Some(trx) = self.trx_list.last_mut() {
            match trx.rollback_failed_precommit().await {
                FailedPrecommitRollbackOutcome::RolledBack => {
                    self.trx_list.pop();
                }
                FailedPrecommitRollbackOutcome::FailedRetained => {
                    self.trx_list.pop();
                    // Once rollback access fails, storage is poisoned and
                    // continuing through older payloads is unsafe. Retain each
                    // one without applying undo so raw undo references remain
                    // valid until transaction-system teardown.
                    while let Some(trx) = self.trx_list.last_mut() {
                        trx.retain_failed_precommit_without_rollback();
                        self.trx_list.pop();
                    }
                    break;
                }
            }
        }
        // Waiters must observe the original redo/shutdown failure only after
        // rollback has released MVCC undo ownership, transaction locks, session
        // state, and prepare waiters.
        let reason = self
            .reason
            .take()
            .expect("failed-precommit cleanup runs exactly once");
        self.completion.complete(Err(reason.completion_bridge(
            "fail redo group commit waiters after failed precommit rollback",
        )));
    }

    /// Retain every current and pending precommit payload after task unwind.
    #[inline]
    pub(crate) fn preserve_after_panic(&mut self) {
        while let Some(trx) = self.trx_list.last_mut() {
            trx.retain_failed_precommit_without_rollback();
            self.trx_list.pop();
        }
        self.reason.take();
    }

    /// Wake redo waiters with the supervised mandatory-task fatal result.
    #[inline]
    pub(crate) fn publish_panic(&self, error: CompletionErrorBridge) {
        self.completion.complete(Err(error));
    }
}

/// Undo/effect ownership retained after rollback access failed fatally.
///
/// Retention is separate from storage poison. Poison prevents future admitted
/// work from entering storage paths, while this value keeps row undo memory
/// alive for any `RowUndoRef` already reachable from row-version chains. These
/// values must not own engine/session attachments, waiters, or logical locks.
pub(in crate::trx) enum FatalRollbackRetention {
    Active {
        row_undo: RowUndoLogs,
        index_undo: IndexUndoLogs,
    },
    Statement {
        row_undo: RowUndoLogs,
        index_undo: IndexUndoLogs,
    },
    Precommit(PrecommitTrxPayload),
}

impl FatalRollbackRetention {
    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            FatalRollbackRetention::Active {
                row_undo,
                index_undo,
            } => row_undo.is_empty() && index_undo.is_empty(),
            FatalRollbackRetention::Statement {
                row_undo,
                index_undo,
            } => row_undo.is_empty() && index_undo.is_empty(),
            FatalRollbackRetention::Precommit(payload) => payload.is_empty(),
        }
    }
}

#[derive(Clone, Copy)]
struct TrxTableLockResources {
    metadata: LockResource,
    data: LockResource,
}

#[derive(Clone, Copy)]
struct TrxTableLockCache {
    metadata_cached: bool,
    data_cached: bool,
}

struct TrxTableLockGuards<'lock> {
    metadata: Option<FreshLockGuard<'lock>>,
    data: Option<FreshLockGuard<'lock>>,
}

impl TrxTableLockGuards<'_> {
    #[inline]
    fn disarm_all(&mut self) {
        if let Some(guard) = self.data.as_mut() {
            guard.disarm();
        }
        if let Some(guard) = self.metadata.as_mut() {
            guard.disarm();
        }
    }
}

/// Mutable transaction core allocated once per installed transaction.
///
/// Stable entries, foreground checkouts, and terminal claims move its owning
/// box so repeated operations do not copy this payload.
pub(crate) struct TrxInner {
    ctx: TrxContext,
    effects: TrxEffects,
    table_bindings: FastHashMap<TableID, admission::TransactionTableBinding>,
    lock_state: Option<OwnerLockState>,
    next_stmt_no: StmtNo,
    active: bool,
    /// Whether successful terminal processing returns this core to the session.
    cache_on_terminal: bool,
}

impl TrxInner {
    /// Create one reusable zero-valued core for a session's public cache.
    #[inline]
    pub(crate) fn public_cached() -> Self {
        Self::ready(true)
    }

    /// Create one ephemeral zero-valued core for a private transaction.
    #[inline]
    pub(crate) fn private() -> Self {
        Self::ready(false)
    }

    #[inline]
    fn ready(cache_on_terminal: bool) -> Self {
        TrxInner {
            ctx: TrxContext::ready(),
            effects: TrxEffects::empty(),
            table_bindings: FastHashMap::default(),
            lock_state: None,
            next_stmt_no: 0,
            active: false,
            cache_on_terminal,
        }
    }

    /// Returns whether successful terminal processing returns this core to the session.
    #[inline]
    pub(crate) const fn cache_on_terminal(&self) -> bool {
        self.cache_on_terminal
    }

    /// Initialize a ready core for one new transaction.
    #[inline]
    pub(crate) fn init(&mut self, trx_id: TrxID, sts: TrxID, gc_no: usize, session_id: SessionID) {
        assert!(
            !self.active,
            "transaction initialization requires an inactive ready core"
        );
        self.effects.assert_cleared();
        assert!(
            self.table_bindings.is_empty(),
            "ready transaction core cannot retain table bindings"
        );
        assert!(
            self.lock_state.is_none(),
            "ready transaction core cannot retain transaction lock state"
        );
        assert!(
            self.next_stmt_no == 0,
            "ready transaction statement number must be zero"
        );
        self.ctx.init(trx_id, sts, gc_no);
        self.lock_state = Some(OwnerLockState::new(LockOwner::transaction(
            session_id, trx_id,
        )));
        self.next_stmt_no = 1;
        self.active = true;
    }

    /// Reset one successfully terminal public transaction into a reusable core.
    #[inline]
    pub(crate) fn reset(&mut self) {
        assert!(
            self.cache_on_terminal,
            "ephemeral private transaction core cannot enter the public cache"
        );
        assert!(
            !self.active,
            "transaction reset requires an inactive terminal core: trx_id={}",
            self.trx_id()
        );
        assert!(
            self.ctx.status.terminal(),
            "transaction reset requires terminal shared status: trx_id={}",
            self.trx_id()
        );
        assert!(
            !self.ctx.status.preparing(),
            "transaction reset cannot retain prepare state: trx_id={}",
            self.trx_id()
        );
        self.effects.assert_cleared();
        assert!(
            self.table_bindings.is_empty(),
            "transaction reset cannot retain table bindings: trx_id={}",
            self.trx_id()
        );
        if let Some(lock_state) = self.lock_state.take() {
            lock_state.assert_cleared();
        }
        // Replace containers rather than clearing them so an idle session does
        // not pin capacity accumulated by its previous transaction.
        self.effects = TrxEffects::empty();
        self.table_bindings = FastHashMap::default();
        self.ctx = TrxContext::ready();
        self.next_stmt_no = 0;
    }

    /// Mark a prepared transaction's emptied core as inactive.
    #[inline]
    fn finish_prepare(&mut self) {
        assert!(
            self.active,
            "transaction prepare completion requires an active core: trx_id={}",
            self.trx_id()
        );
        self.effects.assert_cleared();
        assert!(
            self.table_bindings.is_empty(),
            "prepared transaction cannot retain table bindings: trx_id={}",
            self.trx_id()
        );
        assert!(
            self.lock_state.is_none(),
            "prepared transaction cannot retain transaction lock state: trx_id={}",
            self.trx_id()
        );
        self.active = false;
    }

    /// Returns this transaction's immutable context.
    #[inline]
    pub(crate) fn ctx(&self) -> &TrxContext {
        &self.ctx
    }

    /// Checks transaction redo/effect invariants in debug builds.
    #[inline]
    pub(crate) fn debug_assert_redo_invariants(&self) {
        self.effects.debug_assert_redo_invariants();
    }

    /// Returns mutable access to this transaction's effects.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut TrxEffects {
        &mut self.effects
    }

    #[inline]
    fn checked_engine(&self, attachment: &TrxAttachment) -> EngineRef {
        assert!(
            self.active,
            "checked-out transaction must retain its active core and engine attachment: trx_id={}",
            self.trx_id()
        );
        attachment.engine().clone()
    }

    #[inline]
    fn clone_lock_manager_guard(
        &self,
        attachment: &TrxAttachment,
    ) -> Option<QuiescentGuard<LockManager>> {
        self.active
            .then(|| attachment.engine().lock_manager().clone())
    }

    #[inline]
    fn checked_lock_state(&self) -> &OwnerLockState {
        // A lock-state accessor is reachable only while SessionOperationCheckout or
        // SessionOperationCompletionClaim owns the checked-out core. TrxInner construction
        // installs the lock state, and prepare is the only transition that
        // takes it while consuming the core, so absence here is an invariant
        // violation rather than a recoverable stale-handle condition.
        assert!(
            self.active,
            "checked-out transaction must retain its active lock state: trx_id={}",
            self.trx_id()
        );
        self.lock_state.as_ref().unwrap_or_else(|| {
            panic!(
                "checked-out transaction must retain lock ownership: trx_id={}",
                self.trx_id()
            )
        })
    }

    #[inline]
    fn checked_lock_state_mut(&mut self) -> &mut OwnerLockState {
        let trx_id = self.trx_id();
        assert!(
            self.active,
            "checked-out transaction must retain its active lock state: trx_id={trx_id}"
        );
        self.lock_state.as_mut().unwrap_or_else(|| {
            panic!("checked-out transaction must retain lock ownership: trx_id={trx_id}")
        })
    }

    #[inline]
    fn table_lock_resources(table_id: TableID) -> TrxTableLockResources {
        TrxTableLockResources {
            metadata: LockResource::TableMetadata(table_id),
            data: LockResource::TableData(table_id),
        }
    }

    #[inline]
    fn next_stmt_no(&mut self) -> StmtNo {
        let stmt_no = self.next_stmt_no;
        self.next_stmt_no = self.next_stmt_no.checked_add(1).unwrap_or_else(|| {
            panic!(
                "transaction statement number exhausted u64 space: trx_id={}",
                self.trx_id()
            )
        });
        stmt_no
    }

    #[inline]
    fn next_statement_owner(&mut self) -> LockOwner {
        let stmt_no = self.next_stmt_no();
        self.checked_lock_state().owner().statement(stmt_no)
    }

    /// Returns this transaction's current status timestamp.
    #[inline]
    pub(crate) fn trx_id(&self) -> TrxID {
        self.ctx().trx_id()
    }

    /// Returns the transaction snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.ctx().sts()
    }

    /// Returns the GC bucket number assigned to this transaction.
    #[inline]
    pub(crate) fn gc_no(&self) -> usize {
        self.ctx().gc_no()
    }

    /// Returns mutable access to transaction row undo logs.
    #[inline]
    pub(crate) fn row_undo_mut(&mut self) -> &mut RowUndoLogs {
        self.effects.row_undo_mut()
    }

    /// Returns mutable access to transaction index undo logs.
    #[inline]
    pub(crate) fn index_undo_mut(&mut self) -> &mut IndexUndoLogs {
        self.effects.index_undo_mut()
    }

    /// Acquires an explicit transaction-lifetime table lock.
    #[inline]
    async fn lock_table(
        &mut self,
        attachment: &TrxAttachment,
        table_id: TableID,
        mode: LockMode,
    ) -> OperationResult<()> {
        let operation = "lock_explicit_table";
        let engine = self.checked_engine(attachment);
        let lock_manager = engine.lock_manager();
        let resources = Self::table_lock_resources(table_id);
        let lock_state = self.checked_lock_state();
        let owner = lock_state.owner();
        let cache = TrxTableLockCache {
            metadata_cached: lock_state.cached_covers(resources.metadata, LockMode::Shared),
            data_cached: lock_state.cached_covers(resources.data, mode),
        };
        let metadata_grant = lock_state
            .acquire_uncached(lock_manager, resources.metadata, LockMode::Shared)
            .await
            .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;
        let metadata = FreshLockGuard::new(lock_manager, resources.metadata, owner, metadata_grant);
        let data_grant = lock_state
            .acquire_uncached(lock_manager, resources.data, mode)
            .await
            .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;
        let data = FreshLockGuard::new(lock_manager, resources.data, owner, data_grant);
        let mut guards = TrxTableLockGuards { metadata, data };

        engine
            .catalog()
            .validate_user_table_live(table_id)
            .await
            .attach_with(|| format!("operation={operation}"))?;

        let lock_state = self.checked_lock_state_mut();
        if !cache.data_cached {
            lock_state.cache_granted(resources.data, mode);
        }
        if !cache.metadata_cached {
            lock_state.cache_granted(resources.metadata, LockMode::Shared);
        }
        guards.disarm_all();
        Ok(())
    }

    /// Releases every transaction-owned logical lock.
    #[inline]
    pub(crate) fn release_transaction_locks(
        &mut self,
        attachment: &TrxAttachment,
    ) -> ReleasedTransactionLocks {
        let trx_id = self.trx_id();
        assert!(
            self.active,
            "terminal transaction-lock release requires an active core: trx_id={trx_id}"
        );
        self.clear_table_bindings();
        let lock_state = self.lock_state.as_mut().unwrap_or_else(|| {
            panic!("terminal transaction-lock release requires owner state: trx_id={trx_id}")
        });
        assert!(
            lock_state.owner().scope() == LockScope::Transaction(trx_id),
            "terminal transaction-lock owner mismatch: trx_id={trx_id}, owner={}",
            lock_state.owner()
        );
        lock_state.release_all(attachment.engine().lock_manager());
        lock_state.assert_cleared();
        ReleasedTransactionLocks::new(trx_id)
    }

    /// Drops every transaction table binding before the active STS can advance.
    #[inline]
    pub(crate) fn clear_table_bindings(&mut self) {
        self.table_bindings.clear();
    }

    /// Returns whether the transaction needs a recovery-visible log record.
    #[inline]
    pub(crate) fn require_durability(&self) -> bool {
        self.effects.require_durability()
    }

    /// Returns whether the transaction must pass through ordered commit.
    #[inline]
    pub(crate) fn require_ordered_commit(&self) -> bool {
        self.effects.require_ordered_commit()
    }

    /// Retain remaining effects and roll back the attached session after rollback failure.
    #[inline]
    pub(in crate::trx) fn retain_and_discard_after_fatal_rollback(
        &mut self,
        attachment: &TrxAttachment,
    ) -> FatalRollbackRetention {
        let retention = self.effects.take_for_fatal_retention();
        let released = self.release_transaction_locks(attachment);
        self.active = false;
        attachment.rollback_without_reuse(released);
        retention
    }

    /// Prepare current transaction for committing.
    #[inline]
    fn prepare(mut self: Box<Self>, attachment: TrxAttachment) -> PreparedTrx {
        assert!(
            self.active,
            "terminal transaction claim must retain its active core through prepare: trx_id={}",
            self.trx_id()
        );
        self.clear_table_bindings();
        // fast path for readonly transactions
        if !self.require_ordered_commit() {
            let lock_manager = self.clone_lock_manager_guard(&attachment);
            // there should be no ref count of transaction status.
            debug_assert!(Arc::strong_count(&self.ctx.status) == 1);
            debug_assert!(self.effects.index_undo.is_empty());
            let payload = PreparedTrxPayload::User {
                status: Arc::clone(self.ctx.status()),
                sts: self.ctx.sts(),
                gc_no: self.ctx.gc_no(),
                row_undo: RowUndoLogs::empty(),
                index_undo: IndexUndoLogs::empty(),
            };
            let lock_state = self.lock_state.take();
            self.finish_prepare();
            return PreparedTrx {
                redo_bin: None,
                payload: Some(payload),
                attachment: Some(attachment),
                lock_manager,
                lock_state,
                trx_inner: Some(self),
            };
        }

        // change transaction status
        self.ctx.mark_preparing();
        // Use bincode to serialize redo log when durability is required.
        let redo_bin = if self.require_durability() {
            self.effects.take_log()
        } else {
            None
        };
        let (row_undo, index_undo) = self.effects.take_payload_parts();
        let lock_manager = self.clone_lock_manager_guard(&attachment);
        let payload = PreparedTrxPayload::User {
            status: Arc::clone(self.ctx.status()),
            sts: self.ctx.sts(),
            gc_no: self.ctx.gc_no(),
            row_undo,
            index_undo,
        };
        let lock_state = self.lock_state.take();
        self.finish_prepare();
        PreparedTrx {
            redo_bin,
            payload: Some(payload),
            attachment: Some(attachment),
            lock_manager,
            lock_state,
            trx_inner: Some(self),
        }
    }
}

impl Drop for TrxInner {
    #[inline]
    fn drop(&mut self) {
        self.effects.assert_cleared();
        assert!(
            self.table_bindings.is_empty(),
            "transaction table bindings should be cleared before core drop"
        );
        if let Some(lock_state) = self.lock_state.as_ref() {
            lock_state.assert_cleared();
        }
    }
}

/// Runtime effects moved out of an active transaction before CTS assignment.
pub(crate) enum PreparedTrxPayload {
    User {
        status: Arc<SharedTrxStatus>,
        sts: TrxID,
        gc_no: usize,
        row_undo: RowUndoLogs,
        index_undo: IndexUndoLogs,
    },
    System(SysTrxPayload),
}

impl PreparedTrxPayload {
    /// Returns whether this payload has runtime effects that require ordered commit.
    #[inline]
    fn require_ordered_commit(&self) -> bool {
        match self {
            PreparedTrxPayload::User {
                row_undo,
                index_undo,
                ..
            } => !row_undo.is_empty() || !index_undo.is_empty(),
            PreparedTrxPayload::System(payload) => !payload.is_empty(),
        }
    }
}

/// Transaction in the logical PreparingCommit state.
///
/// `PreparedTrx` owns all transaction effects, locks, and the terminal
/// attachment before the irreversible group-commit handoff. Dropping it is only
/// valid after a caller has consumed or explicitly cleared those fields.
pub(crate) struct PreparedTrx {
    redo_bin: Option<TrxLog>,
    payload: Option<PreparedTrxPayload>,
    /// Terminal session attachment carried until ordered commit or rollback cleanup.
    pub(crate) attachment: Option<TrxAttachment>,
    lock_manager: Option<QuiescentGuard<LockManager>>,
    lock_state: Option<OwnerLockState>,
    /// Emptied session transaction core retained until the terminal outcome.
    trx_inner: Option<Box<TrxInner>>,
}

impl PreparedTrx {
    /// Returns whether this transaction needs a recovery-visible log record.
    ///
    /// Recovery seeds the next timestamp from checkpoint metadata, table roots,
    /// and redo headers. A transaction that publishes durable state needing a
    /// stable CTS must therefore require durability and emit a real log record.
    #[inline]
    fn require_durability(&self) -> bool {
        self.redo_bin.is_some()
    }

    /// Returns whether this transaction must pass through group commit ordering.
    ///
    /// This is broader than durability: volatile runtime effects still need CTS
    /// ordering for status backfill, attachment completion, and GC handoff even
    /// when no log record should be written.
    #[inline]
    fn require_ordered_commit(&self) -> bool {
        self.require_durability()
            || self
                .payload
                .as_ref()
                .map(PreparedTrxPayload::require_ordered_commit)
                .unwrap_or(false)
    }

    /// Fill the reserved commit timestamp and enter precommit state.
    #[inline]
    pub(crate) fn fill_cts(mut self, cts: TrxID) -> PrecommitTrx {
        let redo_bin = if let Some(mut redo_bin) = self.redo_bin.take() {
            redo_bin.header.cts = cts;
            Some(redo_bin)
        } else {
            None
        };
        // CTS assignment is only an ordering reservation. Redo durability can
        // still fail after this point, so user precommit retains rollback-capable
        // index undo while system precommit retains only its GC page ownership.
        let payload = self.payload.take().map(|payload| match payload {
            PreparedTrxPayload::User {
                status,
                sts,
                gc_no,
                row_undo,
                index_undo,
            } => PrecommitTrxPayload::User {
                status,
                sts,
                gc_no,
                row_undo,
                index_undo,
            },
            PreparedTrxPayload::System(payload) => PrecommitTrxPayload::System(payload),
        });
        if payload.is_none() {
            debug_assert!(self.attachment.is_none());
        }
        PrecommitTrx {
            cts,
            redo_bin,
            payload,
            attachment: self.attachment.take(),
            lock_manager: self.lock_manager.take(),
            lock_state: self.lock_state.take(),
            trx_inner: self.trx_inner.take(),
        }
    }

    /// Releases and drops transaction-owned locks for an unordered discard path.
    #[inline]
    pub(self) fn release_transaction_locks(&mut self) -> Option<ReleasedTransactionLocks> {
        release_carried_transaction_locks(&mut self.lock_state, &mut self.lock_manager)
    }
}

impl Drop for PreparedTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.payload.is_none(), "payload should be cleared");
        assert!(self.attachment.is_none(), "attachment should be cleared");
        assert!(
            self.lock_manager.is_none(),
            "lock manager should be cleared"
        );
        assert!(self.lock_state.is_none(), "lock state should be cleared");
        assert!(
            self.trx_inner.is_none(),
            "prepared transaction core should be cleared"
        );
    }
}

/// Runtime effects retained while a precommit transaction waits for redo outcome.
pub(crate) enum PrecommitTrxPayload {
    User {
        status: Arc<SharedTrxStatus>,
        sts: TrxID,
        gc_no: usize,
        row_undo: RowUndoLogs,
        /// Rollback-capable index undo retained until redo durability succeeds.
        ///
        /// A precommit CTS is not a durability proof: failed redo write/sync must
        /// still rollback secondary-index effects before dropping transaction
        /// payload ownership.
        index_undo: IndexUndoLogs,
    },
    System(SysTrxPayload),
}

impl PrecommitTrxPayload {
    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            PrecommitTrxPayload::User {
                row_undo,
                index_undo,
                ..
            } => row_undo.is_empty() && index_undo.is_empty(),
            PrecommitTrxPayload::System(payload) => payload.is_empty(),
        }
    }

    #[inline]
    async fn rollback(&mut self, attachment: &TrxAttachment) -> RuntimeResult<()> {
        let PrecommitTrxPayload::User {
            sts,
            row_undo,
            index_undo,
            ..
        } = self
        else {
            panic!("rollback requires a user precommit payload")
        };
        let trx_sys = &attachment.engine().trx_sys;
        let pool_guards = attachment.pool_guards().clone();
        let mut table_cache = TableCache::new(&trx_sys.catalog);
        index_undo
            .rollback(&mut table_cache, &pool_guards, *sts)
            .await?;
        row_undo.rollback(&mut table_cache, &pool_guards).await
    }

    #[inline]
    fn record_rollback_for_purge(&self, attachment: &TrxAttachment) {
        let trx_sys = &attachment.engine().trx_sys;
        let PrecommitTrxPayload::User { sts, gc_no, .. } = self else {
            panic!("rollback purge record requires a user precommit payload")
        };
        trx_sys.record_rollback_for_purge(*gc_no, *sts);
    }

    #[inline]
    fn release_prepare_waiters(&self) {
        if let PrecommitTrxPayload::User { status, .. } = self {
            status.finish_preparing();
        }
    }

    #[inline]
    fn finish_successful_rollback(&self) {
        if let PrecommitTrxPayload::User { status, .. } = self {
            status.finish_terminal();
            status.finish_preparing();
        }
    }
}

/// Transaction in the logical Committing state.
///
/// Once a `PrecommitTrx` is queued in a commit group, the log thread owns the
/// terminal outcome. User waiters may observe success or failure, but they no
/// longer own session commit/rollback cleanup and cannot convert this state
/// back into an explicit rollback.
///
/// There are two kinds of PrecommitTrx. One is a user transaction containing
/// rollback-capable undo, locks, and terminal attachment. The other is an
/// attachmentless system transaction, which is directly dropped after ordered
/// completion.
pub(crate) struct PrecommitTrx {
    /// Commit timestamp reserved for this precommit transaction.
    pub(crate) cts: TrxID,
    /// Recovery-visible redo record, when this transaction requires durability.
    pub(crate) redo_bin: Option<TrxLog>,
    /// User transaction payload retained until ordered commit or rollback.
    pub(crate) payload: Option<PrecommitTrxPayload>,
    /// Terminal session attachment for user transactions.
    pub(crate) attachment: Option<TrxAttachment>,
    /// Lock manager retained to release transaction-owned locks.
    pub(crate) lock_manager: Option<QuiescentGuard<LockManager>>,
    /// Transaction-owned lock state retained until terminal cleanup.
    pub(crate) lock_state: Option<OwnerLockState>,
    /// Emptied session transaction core retained until the terminal outcome.
    pub(crate) trx_inner: Option<Box<TrxInner>>,
}

impl PrecommitTrx {
    /// Takes the recovery-visible log record, if this transaction requires one.
    #[inline]
    pub(crate) fn take_log(&mut self) -> Option<TrxLog> {
        self.redo_bin.take()
    }

    /// Commit this transaction.
    /// The method should be invoked when redo logs have been persisted to disk.
    /// It will update backfill commit timestamp and update status to committed.
    #[inline]
    pub(crate) fn commit(mut self) -> CommittedTrx {
        assert!(self.redo_bin.is_none()); // redo log should be already processed by logger.
        // release the prepare notifier in transaction status
        match self.payload.take() {
            Some(PrecommitTrxPayload::User {
                status,
                sts,
                gc_no,
                row_undo,
                mut index_undo,
            }) => {
                let index_gc = index_undo.commit_for_gc();
                status.commit_prepared(self.cts);
                let committed = CommittedTrx {
                    cts: self.cts,
                    payload: Some(CommittedTrxPayload::User {
                        sts,
                        gc_no,
                        row_undo,
                        index_gc,
                    }),
                };
                let released = self.release_transaction_locks().unwrap_or_else(|| {
                    panic!(
                        "user precommit requires released transaction-lock proof: cts={}",
                        self.cts
                    )
                });
                let attachment = self.attachment.take().unwrap_or_else(|| {
                    panic!(
                        "user precommit requires terminal attachment: cts={}",
                        self.cts
                    )
                });
                let inner = self.trx_inner.take().unwrap_or_else(|| {
                    panic!(
                        "user precommit requires reusable transaction core: cts={}",
                        self.cts
                    )
                });
                attachment.commit(released, self.cts, inner);
                committed
            }
            Some(PrecommitTrxPayload::System(payload)) => {
                assert!(
                    self.attachment.is_none(),
                    "system precommit must not carry a session attachment: cts={}",
                    self.cts
                );
                assert!(
                    self.release_transaction_locks().is_none(),
                    "system precommit must not produce a transaction-lock proof: cts={}",
                    self.cts
                );
                assert!(
                    self.trx_inner.is_none(),
                    "system precommit must not carry a session transaction core: cts={}",
                    self.cts
                );
                CommittedTrx {
                    cts: self.cts,
                    payload: Some(CommittedTrxPayload::System(payload)),
                }
            }
            None => {
                assert!(
                    self.attachment.is_none(),
                    "empty system precommit must not carry a session attachment: cts={}",
                    self.cts
                );
                assert!(
                    self.release_transaction_locks().is_none(),
                    "empty system precommit must not produce a transaction-lock proof: cts={}",
                    self.cts
                );
                assert!(
                    self.trx_inner.is_none(),
                    "empty system precommit must not carry a session transaction core: cts={}",
                    self.cts
                );
                // A system transaction without GC pages has no purge payload.
                CommittedTrx {
                    cts: self.cts,
                    payload: None,
                }
            }
        }
    }

    /// Rollback this transaction after it entered prepare but before redo durability succeeded.
    ///
    /// Failed precommit must perform rollback-equivalent cleanup before waking
    /// redo waiters. Row-version heads can point into transaction-owned row
    /// undo, so dropping the payload before rollback would leave dangling raw
    /// undo references.
    #[inline]
    async fn rollback_failed_precommit(&mut self) -> FailedPrecommitRollbackOutcome {
        self.redo_bin.take();
        let engine = self
            .attachment
            .as_ref()
            .map(|attachment| attachment.engine().clone());
        if let (Some(payload), Some(attachment)) = (self.payload.as_mut(), self.attachment.as_ref())
        {
            if let Err(err) = payload.rollback(attachment).await {
                let report = err
                    .change_context(FatalError::RollbackAccess)
                    .attach("failed-precommit rollback failed");
                obs::error!(
                    "event=engine_poison component=trx action=poison result=error error={:?}",
                    report
                );
                let _ = attachment.engine().poisoner.poison(report);
                self.finish_failed_precommit_with_retention(engine);
                return FailedPrecommitRollbackOutcome::FailedRetained;
            }
            payload.record_rollback_for_purge(attachment);
        }
        if let Some(payload) = self.payload.take() {
            payload.finish_successful_rollback();
        }
        let released = self.release_transaction_locks();
        self.finish_carried_session_rollback(released);
        FailedPrecommitRollbackOutcome::RolledBack
    }

    /// Retain this transaction without touching row/index undo after rollback became unsafe.
    #[inline]
    fn retain_failed_precommit_without_rollback(&mut self) {
        self.redo_bin.take();
        let engine = self
            .attachment
            .as_ref()
            .map(|attachment| attachment.engine().clone());
        self.finish_failed_precommit_with_retention(engine);
    }

    #[inline]
    fn finish_failed_precommit_with_retention(&mut self, engine: Option<EngineRef>) {
        let released = self.release_transaction_locks();
        self.finish_carried_session_rollback_without_reuse(released);
        if let Some(payload) = self.payload.take() {
            payload.release_prepare_waiters();
            if let Some(engine) = engine {
                engine
                    .trx_sys
                    .retain_fatal_rollback(FatalRollbackRetention::Precommit(payload));
            } else {
                debug_assert!(
                    false,
                    "precommit payload requires an attachment for fatal retention"
                );
                mem::forget(payload);
            }
        }
    }

    /// Discard an attachmentless rejected precommit transaction.
    #[inline]
    pub(crate) fn discard_rejected(mut self) {
        assert!(
            self.attachment.is_none(),
            "rejected system precommit must not carry a session attachment: cts={}",
            self.cts
        );
        if let Some(payload) = self.payload.take() {
            assert!(
                matches!(payload, PrecommitTrxPayload::System(_)),
                "rejected precommit discard requires system payload: cts={}",
                self.cts
            );
        }
        self.redo_bin.take();
        assert!(
            self.release_transaction_locks().is_none(),
            "rejected system precommit must not produce a transaction-lock proof: cts={}",
            self.cts
        );
        assert!(
            self.trx_inner.is_none(),
            "rejected system precommit must not carry a session transaction core: cts={}",
            self.cts
        );
    }

    #[inline]
    fn release_transaction_locks(&mut self) -> Option<ReleasedTransactionLocks> {
        release_carried_transaction_locks(&mut self.lock_state, &mut self.lock_manager)
    }

    #[inline]
    fn finish_carried_session_rollback(&mut self, released: Option<ReleasedTransactionLocks>) {
        match (self.attachment.take(), released, self.trx_inner.take()) {
            (Some(attachment), Some(released), Some(inner)) => attachment.rollback(released, inner),
            (None, None, None) => {
                assert!(
                    !matches!(self.payload, Some(PrecommitTrxPayload::User { .. })),
                    "user precommit requires attachment, transaction-lock state, and core: cts={}",
                    self.cts
                );
            }
            (attachment, released, inner) => {
                panic!(
                    "precommit terminal ownership mismatch: cts={}, attachment={}, released={}, \
                     trx_inner={}",
                    self.cts,
                    attachment.is_some(),
                    released.is_some(),
                    inner.is_some()
                );
            }
        }
    }

    #[inline]
    fn finish_carried_session_rollback_without_reuse(
        &mut self,
        released: Option<ReleasedTransactionLocks>,
    ) {
        let inner = self.trx_inner.take();
        match (self.attachment.take(), released) {
            (Some(attachment), Some(released)) => {
                attachment.rollback_without_reuse(released);
                drop(inner);
            }
            (None, None) => {
                assert!(
                    inner.is_none(),
                    "system precommit cannot carry a session transaction core: cts={}",
                    self.cts
                );
            }
            (attachment, released) => {
                panic!(
                    "precommit fatal terminal ownership mismatch: cts={}, attachment={}, \
                     released={}, trx_inner={}",
                    self.cts,
                    attachment.is_some(),
                    released.is_some(),
                    inner.is_some()
                );
            }
        }
    }
}

impl Drop for PrecommitTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.payload.is_none(), "payload should be cleared");
        assert!(self.attachment.is_none(), "attachment should be cleared");
        assert!(
            self.lock_manager.is_none(),
            "lock manager should be cleared"
        );
        assert!(self.lock_state.is_none(), "lock state should be cleared");
        assert!(
            self.trx_inner.is_none(),
            "precommit transaction core should be cleared"
        );
    }
}

enum CommittedTrxPayload {
    User {
        sts: TrxID,
        gc_no: usize,
        row_undo: RowUndoLogs,
        index_gc: Vec<IndexPurgeEntry>,
    },
    System(SysTrxPayload),
}

/// Transaction payload handed from ordered commit into purge coordination.
pub(crate) struct CommittedTrx {
    cts: TrxID,
    payload: Option<CommittedTrxPayload>,
}

impl CommittedTrx {
    /// Returns the transaction snapshot timestamp for GC-aware user transactions.
    #[inline]
    pub(crate) fn sts(&self) -> Option<TrxID> {
        match self.payload.as_ref() {
            Some(CommittedTrxPayload::User { sts, .. }) => Some(*sts),
            Some(CommittedTrxPayload::System(_)) | None => None,
        }
    }

    /// Returns the runtime GC bucket if this transaction carries purge work.
    ///
    /// User payloads retain their begin-time bucket. System retirement payloads
    /// derive a table-affine bucket from the supplied runtime bucket count.
    #[inline]
    pub(crate) fn gc_no(&self, gc_buckets: usize) -> Option<usize> {
        self.payload.as_ref().map(|payload| match payload {
            CommittedTrxPayload::User { gc_no, .. } => *gc_no,
            CommittedTrxPayload::System(payload) => {
                sys_trx::retirement_gc_no(payload.retired_row_pages.table_id, gc_buckets)
            }
        })
    }

    #[inline]
    fn row_undo(&self) -> Option<&RowUndoLogs> {
        match self.payload.as_ref() {
            Some(CommittedTrxPayload::User { row_undo, .. }) => Some(row_undo),
            Some(CommittedTrxPayload::System(_)) | None => None,
        }
    }

    #[inline]
    fn index_gc(&self) -> Option<&[IndexPurgeEntry]> {
        match self.payload.as_ref() {
            Some(CommittedTrxPayload::User { index_gc, .. }) => Some(index_gc),
            Some(CommittedTrxPayload::System(_)) | None => None,
        }
    }

    #[inline]
    #[cfg(test)]
    fn retired_row_pages(&self) -> Option<&RetiredRowPageBatch> {
        match self.payload.as_ref() {
            Some(CommittedTrxPayload::System(payload)) => Some(&payload.retired_row_pages),
            Some(CommittedTrxPayload::User { .. }) | None => None,
        }
    }

    #[inline]
    fn into_retired_row_pages(mut self) -> Option<RetiredRowPageBatch> {
        match self.payload.take() {
            Some(CommittedTrxPayload::System(payload)) => Some(payload.retired_row_pages),
            Some(CommittedTrxPayload::User { .. }) | None => None,
        }
    }
}

/// Returns whether the transaction timestamp is committed.
#[inline]
pub(crate) fn trx_is_committed(ts: TrxID) -> bool {
    ts < MIN_ACTIVE_TRX_ID
}

#[inline]
fn session_operation_entry_state_err(
    key: SessionOperationKey,
    kind: SessionOperationKind,
    inner: &SessionOperationEntryInner,
) -> Report<LifecycleError> {
    let trx_id = inner
        .trx_id
        .map_or_else(|| "none".to_owned(), |trx_id| trx_id.to_string());
    let error = match inner.state {
        SessionOperationState::Voluntary(_) | SessionOperationState::Mandatory(_) => {
            LifecycleError::ExistingTransaction
        }
        SessionOperationState::CleanupReady
        | SessionOperationState::Completing
        | SessionOperationState::Terminal
        | SessionOperationState::FailedRetained => LifecycleError::TransactionDiscarded,
    };
    Report::new(error).attach(format!(
        "operation_key={key}, kind={}, state={}, trx_id={trx_id}",
        kind.label(),
        inner.state.label()
    ))
}

#[inline]
fn release_carried_transaction_locks(
    lock_state: &mut Option<OwnerLockState>,
    lock_manager: &mut Option<QuiescentGuard<LockManager>>,
) -> Option<ReleasedTransactionLocks> {
    match (lock_state.take(), lock_manager.take()) {
        (Some(mut lock_state), Some(lock_manager)) => {
            let owner = lock_state.owner();
            let LockScope::Transaction(trx_id) = owner.scope() else {
                panic!("carried terminal lock state requires a transaction owner: owner={owner}")
            };
            lock_state.release_all(&lock_manager);
            lock_state.assert_cleared();
            drop(lock_state);
            drop(lock_manager);
            Some(ReleasedTransactionLocks::new(trx_id))
        }
        (None, None) => None,
        (Some(lock_state), None) => {
            panic!(
                "carried transaction lock state requires a lock-manager guard: owner={}",
                lock_state.owner()
            )
        }
        (None, Some(_)) => {
            panic!("carried transaction lock-manager guard requires owner lock state")
        }
    }
}

#[inline]
fn is_catalog_metadata_ddl(ddl: Option<&DDLRedo>) -> bool {
    matches!(
        ddl,
        Some(
            DDLRedo::CreateTable(_)
                | DDLRedo::DropTable(_)
                | DDLRedo::CreateIndex { .. }
                | DDLRedo::DropIndex { .. }
                | DDLRedo::TableReplaySilentWatermark { .. }
        )
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::buffer::frame::FrameKind;
    use crate::buffer::page::PAGE_SIZE;
    use crate::buffer::test_frame_kind;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::tests as catalog_tests;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{InternalError, OperationError};
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::{MutableTableFile, TableFile};
    use crate::id::{OperationID, PageID, RowID, SessionID};
    use crate::io::{
        IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot, try_acquire};
    use crate::log::redo::{RowRedo, RowRedoKind};
    use crate::row::ops::SelectKey;
    use crate::session::{
        Session,
        tests::{
            SessionTestExt, TerminalAttachmentOutcome, TerminalAttachmentTestHookGuard,
            active_operation_count, assert_existing_transaction_error,
            install_terminal_attachment_test_hook, remove_session_for_test,
            session_has_public_trx_cache, session_registry_len, wait_for_session_idle,
        },
    };
    use crate::table::test_user_table_id;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::sys::tests::{
        TerminalRollbackTestHookGuard, fatal_rollback_retention_count,
        install_terminal_rollback_test_hook, retains_precommit_row_undo,
    };
    use crate::trx::undo::tests::{
        index_rollback_paused, pause_next_index_rollback, pause_next_row_rollback,
        row_rollback_paused,
    };
    use crate::trx::undo::{IndexUndo, IndexUndoKind, OwnedRowUndo, RowUndoKind};
    use crate::value::{Val, ValKind};
    use event_listener::Listener;
    use smol::Timer;
    use smol::future::yield_now;
    use std::cell::Cell;
    use std::future::{Future, pending};
    use std::io::Error as IoError;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
    use std::thread::{scope, sleep, spawn};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    std::thread_local! {
        static PREPARE_LISTENER_BEFORE_LOCK_HOOK:
            std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
    }

    /// Installs a thread-local pause after the optimistic prepare load.
    #[inline]
    pub(crate) fn install_prepare_listener_before_lock_hook(hook: impl FnOnce() + 'static) {
        PREPARE_LISTENER_BEFORE_LOCK_HOOK.with(|slot| {
            assert!(
                slot.borrow_mut().replace(Box::new(hook)).is_none(),
                "prepare-listener test hook is already installed"
            );
        });
    }

    /// Runs and clears the thread-local prepare-listener pause.
    #[inline]
    pub(super) fn run_prepare_listener_before_lock_hook() {
        let hook = PREPARE_LISTENER_BEFORE_LOCK_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Create one test-controlled shared transaction status.
    #[inline]
    pub(crate) fn shared_trx_status(trx_id: TrxID) -> SharedTrxStatus {
        let terminal = trx_is_committed(trx_id);
        SharedTrxStatus {
            ts: AtomicU64::new(trx_id.as_u64()),
            preparing: AtomicBool::new(false),
            prepare_ev: parking_lot::Mutex::new(None),
            terminal: AtomicBool::new(terminal),
            terminal_ev: Event::new(),
        }
    }

    /// Publish one committed result on a test-controlled shared status.
    #[inline]
    pub(crate) fn commit_shared_trx_status(status: &SharedTrxStatus, cts: TrxID) {
        debug_assert!(trx_is_committed(cts));
        status.ts.store(cts.as_u64(), Ordering::SeqCst);
        status.finish_terminal();
    }

    /// Publish one rolled-back result on a test-controlled shared status.
    #[inline]
    pub(crate) fn rollback_shared_trx_status(status: &SharedTrxStatus) {
        status.finish_terminal();
    }

    /// Enter prepare on one test-controlled shared status.
    #[inline]
    pub(crate) fn prepare_shared_trx_status(status: &SharedTrxStatus) {
        status.mark_preparing();
    }

    /// Publish one committed prepare result on a test-controlled shared status.
    #[inline]
    pub(crate) fn commit_preparing_shared_trx_status(status: &SharedTrxStatus, cts: TrxID) {
        debug_assert!(trx_is_committed(cts));
        status.commit_prepared(cts);
    }

    /// Publish one successful rollback prepare result on a test-controlled status.
    #[inline]
    pub(crate) fn rollback_preparing_shared_trx_status(status: &SharedTrxStatus) {
        status.finish_terminal();
        status.finish_preparing();
    }

    /// Returns whether a test-controlled status has an injected prepare event.
    #[inline]
    pub(crate) fn prepare_event_is_installed(status: &SharedTrxStatus) -> bool {
        status.prepare_ev.lock().is_some()
    }

    /// Create one initialized, public-cacheable transaction core for tests.
    #[inline]
    pub(crate) fn trx_inner(
        trx_id: TrxID,
        sts: TrxID,
        gc_no: usize,
        session_id: SessionID,
    ) -> TrxInner {
        let mut inner = TrxInner::public_cached();
        inner.init(trx_id, sts, gc_no, session_id);
        inner
    }

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        test_engine_with_mem_size(log_file_stem, 64usize * 1024 * 1024).await
    }

    #[test]
    fn test_shared_status_terminal_resolution_is_sticky_and_wakeable() {
        smol::block_on(async {
            let committed = shared_trx_status(MIN_ACTIVE_TRX_ID + 1);
            let mut commit_listener = Box::pin(committed.terminal_listener().unwrap());
            assert!(futures::poll!(commit_listener.as_mut()).is_pending());
            commit_shared_trx_status(&committed, TrxID::new(10));
            commit_listener.await;
            assert!(committed.terminal());
            assert!(committed.terminal_listener().is_none());

            let rolled_back = shared_trx_status(MIN_ACTIVE_TRX_ID + 2);
            let mut rollback_listener = Box::pin(rolled_back.terminal_listener().unwrap());
            assert!(futures::poll!(rollback_listener.as_mut()).is_pending());
            rollback_shared_trx_status(&rolled_back);
            rollback_listener.await;
            assert!(rolled_back.terminal());
            assert!(rolled_back.terminal_listener().is_none());
        });
    }

    #[test]
    fn test_transaction_core_reset_installs_fresh_ready_status_before_init() {
        let session_id = SessionID::new(71);
        let first_trx_id = MIN_ACTIVE_TRX_ID + 71;
        let mut inner = trx_inner(first_trx_id, TrxID::new(71), 3, session_id);
        let old_status = Arc::clone(inner.ctx().status());
        inner.table_bindings.reserve(128);
        assert!(inner.table_bindings.capacity() >= 128);

        old_status.finish_terminal();
        inner.active = false;
        inner.reset();

        assert!(old_status.terminal());
        assert_eq!(old_status.ts(), first_trx_id);
        assert_eq!(inner.ctx.status.ts(), TrxID::new(0));
        assert!(!inner.ctx.status.terminal());
        assert!(!inner.ctx.status.preparing());
        assert_eq!(Arc::strong_count(&inner.ctx.status), 1);
        assert_eq!(inner.table_bindings.capacity(), 0);
        assert!(inner.lock_state.is_none());
        assert_eq!(inner.next_stmt_no, 0);
        assert!(!inner.active);
        let ready_status_ptr = Arc::as_ptr(&inner.ctx.status);

        let second_trx_id = MIN_ACTIVE_TRX_ID + 72;
        inner.init(second_trx_id, TrxID::new(72), 4, session_id);
        assert_eq!(Arc::as_ptr(&inner.ctx.status), ready_status_ptr);
        assert!(!Arc::ptr_eq(&old_status, &inner.ctx.status));
        assert_eq!(inner.trx_id(), second_trx_id);
        assert_eq!(inner.sts(), TrxID::new(72));
        assert_eq!(inner.gc_no(), 4);
        assert_eq!(inner.next_stmt_no, 1);
        assert!(inner.active);

        let second_status = Arc::clone(inner.ctx().status());
        second_status.finish_terminal();
        inner.active = false;
        inner.reset();
    }

    #[test]
    fn test_session_reuses_transaction_core_with_fresh_status_identity() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_session_ready_core_reuse").await;
            let mut session = engine.new_session().unwrap();

            let first = session.begin_trx().unwrap();
            let first_trx_id = first.trx_id();
            let (first_inner, first_status) = with_transaction_inner(
                &first,
                "observe_first_reusable_transaction_core",
                |inner| {
                    (
                        inner as *const TrxInner as usize,
                        Arc::clone(inner.ctx().status()),
                    )
                },
            )
            .unwrap();
            first.rollback().await.unwrap();
            assert!(first_status.terminal());
            assert_eq!(first_status.ts(), first_trx_id);
            assert!(session_has_public_trx_cache(
                &engine.inner().session_registry,
                session.id()
            ));

            let second = session.begin_trx().unwrap();
            let second_trx_id = second.trx_id();
            let (second_inner, second_status) = with_transaction_inner(
                &second,
                "observe_second_reusable_transaction_core",
                |inner| {
                    (
                        inner as *const TrxInner as usize,
                        Arc::clone(inner.ctx().status()),
                    )
                },
            )
            .unwrap();
            assert_eq!(second_inner, first_inner);
            assert!(!Arc::ptr_eq(&first_status, &second_status));
            assert_eq!(second_status.ts(), second_trx_id);
            assert!(!second_status.terminal());
            second.rollback().await.unwrap();
        });
    }

    async fn test_engine_with_mem_size(
        log_file_stem: &str,
        max_mem_size: usize,
    ) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::bootstrap(
            EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(max_mem_size)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem(log_file_stem),
                ),
        )
        .await
        .unwrap();
        (temp_dir, engine)
    }

    struct FailingPageReadHook {
        file: StorageBackendFileIdentity,
        offset: usize,
        errno: i32,
        calls: AtomicUsize,
    }

    impl FailingPageReadHook {
        #[inline]
        fn for_page(file: StorageBackendFileIdentity, page_id: PageID, errno: i32) -> Self {
            Self {
                file,
                offset: usize::from(page_id) * PAGE_SIZE,
                errno,
                calls: AtomicUsize::new(0),
            }
        }

        #[inline]
        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        #[inline]
        fn matches(&self, op: StorageBackendOp) -> bool {
            op.kind() == IOKind::Read
                && op.matches_file_identity(self.file)
                && op.offset() == self.offset
        }
    }

    impl StorageBackendTestHook for FailingPageReadHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if self.matches(op) {
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if self.matches(op) {
                *res = Err(IoError::from_raw_os_error(self.errno));
            }
        }
    }

    #[inline]
    fn resolve_active_parts_for_test(
        trx: &Transaction,
    ) -> Result<(Arc<SessionOperationEntry>, TrxAttachment)> {
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .attach_with(|| format!("operation_key={}, trx_id={}", trx.operation_key, trx.trx_id))
            .disclose()?;
        let (entry, session) = engine
            .session_registry
            .try_resolve_operation(trx.operation_key)
            .ok_or_else(|| {
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "operation_key={}, trx_id={}, reason=transaction_not_resolvable",
                    trx.operation_key, trx.trx_id
                ))
            })
            .disclose()?;
        let attachment = TrxAttachment::new(engine, session, trx.operation_key, trx.trx_id);
        Ok((entry, attachment))
    }

    #[inline]
    fn transaction_entry(trx: &Transaction) -> Arc<SessionOperationEntry> {
        resolve_active_parts_for_test(trx)
            .expect("test transaction must resolve")
            .0
    }

    #[test]
    fn test_checked_out_entry_remembers_abandonment_until_return() {
        let session_id = SessionID::new(100);
        let trx_id = MIN_ACTIVE_TRX_ID + 100;
        let entry = SessionOperationEntry::new_public_transaction(
            SessionOperationKey::new(session_id, OperationID::new(1)),
            Box::new(trx_inner(trx_id, TrxID::new(100), 0, session_id)),
        );
        let inner = entry
            .take_for_checkout(trx_id)
            .expect("active entry can be checked out");

        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(None)
        );
        assert!(entry.abandon_transaction(trx_id));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(None)
        );
        assert!(entry.inspect().cleanup_requested);

        assert!(entry.return_inner(inner));
        let snapshot = entry.inspect();
        assert_eq!(snapshot.trx_id, Some(trx_id));
        assert_eq!(snapshot.state, SessionOperationState::CleanupReady);
        assert!(snapshot.cleanup_requested);
    }

    #[test]
    fn test_cancelled_statement_return_publishes_cleanup_ready_directly() {
        for preexisting_cleanup in [false, true] {
            let id = 105 + u64::from(preexisting_cleanup);
            let session_id = SessionID::new(id);
            let trx_id = MIN_ACTIVE_TRX_ID + id;
            let entry = SessionOperationEntry::new_public_transaction(
                SessionOperationKey::new(session_id, OperationID::new(1)),
                Box::new(trx_inner(trx_id, TrxID::new(id), 0, session_id)),
            );
            let inner = entry
                .take_for_checkout(trx_id)
                .expect("active entry can be checked out");
            if preexisting_cleanup {
                assert!(entry.abandon_transaction(trx_id));
            }

            entry.return_cancelled(inner);

            let snapshot = entry.inspect();
            assert_eq!(snapshot.trx_id, Some(trx_id));
            assert_eq!(snapshot.state, SessionOperationState::CleanupReady);
            assert!(snapshot.cleanup_requested);
        }
    }

    #[test]
    fn test_private_transaction_state_is_nested_under_foreground_operation() {
        let session_id = SessionID::new(101);
        let trx_id = MIN_ACTIVE_TRX_ID + 101;
        let entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(1)),
            SessionOperationKind::Maintenance,
        );

        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(None)
        );
        entry.install_private_transaction(Box::new(trx_inner(
            trx_id,
            TrxID::new(101),
            0,
            session_id,
        )));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(Some(InternalTrxState::Available))
        );

        let inner = entry
            .take_for_checkout(trx_id)
            .expect("private transaction can be checked out");
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(Some(InternalTrxState::Running))
        );
        assert!(entry.abandon_transaction(trx_id));
        assert!(entry.return_inner(inner));
        let snapshot = entry.inspect();
        assert_eq!(snapshot.trx_id, Some(trx_id));
        assert_eq!(
            snapshot.state,
            SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady))
        );

        let _inner = entry
            .take_for_cleanup(trx_id)
            .expect("private transaction cleanup can be claimed");
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(Some(InternalTrxState::Completing))
        );
        assert_eq!(entry.finish_transaction(trx_id), Some(false));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Voluntary(None)
        );
        assert!(!entry.inspect().cleanup_requested);

        let release = entry.release_foreground();
        assert!(release.terminal);
        assert!(release.cleanup.is_none());
        assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
    }

    #[test]
    fn test_private_transaction_state_is_nested_under_mandatory_operation() {
        let session_id = SessionID::new(102);
        let trx_id = MIN_ACTIVE_TRX_ID + 102;
        let entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(1)),
            SessionOperationKind::Ddl,
        );

        entry.accept_mandatory();
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(None)
        );
        entry.install_private_transaction(Box::new(trx_inner(
            trx_id,
            TrxID::new(102),
            0,
            session_id,
        )));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(Some(InternalTrxState::Available))
        );

        let inner = entry
            .take_for_checkout(trx_id)
            .expect("mandatory private transaction can be checked out");
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(Some(InternalTrxState::Running))
        );
        assert!(!entry.return_inner(inner));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(Some(InternalTrxState::Available))
        );

        let inner = entry
            .take_for_terminal(trx_id)
            .expect("mandatory private transaction terminal can be claimed");
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(Some(InternalTrxState::Completing))
        );
        drop(inner);
        assert_eq!(entry.finish_transaction(trx_id), Some(false));
        assert_eq!(
            entry.inspect().state,
            SessionOperationState::Mandatory(None)
        );

        entry.assert_mandatory_finish_ready();
        entry.publish_mandatory_terminal();
        assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
    }

    #[test]
    fn test_stale_transaction_identity_cannot_claim_reused_operation_entry() {
        let session_id = SessionID::new(200);
        let first_trx_id = MIN_ACTIVE_TRX_ID + 200;
        let second_trx_id = MIN_ACTIVE_TRX_ID + 201;
        let entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(1)),
            SessionOperationKind::Maintenance,
        );

        entry.install_private_transaction(Box::new(trx_inner(
            first_trx_id,
            TrxID::new(200),
            0,
            session_id,
        )));
        let first_inner = entry
            .take_for_terminal(first_trx_id)
            .expect("first private transaction can be completed");
        assert_eq!(entry.finish_transaction(first_trx_id), Some(false));
        drop(first_inner);

        entry.install_private_transaction(Box::new(trx_inner(
            second_trx_id,
            TrxID::new(201),
            0,
            session_id,
        )));
        assert!(
            !entry.abandon_transaction(first_trx_id),
            "a stale handle must not abandon the replacement transaction"
        );
        assert!(entry.abandon_transaction(second_trx_id));
        assert!(
            entry.take_for_cleanup(first_trx_id).is_err(),
            "a stale cleanup message must not claim the replacement transaction"
        );
        let snapshot = entry.inspect();
        assert_eq!(snapshot.trx_id, Some(second_trx_id));
        assert_eq!(
            snapshot.state,
            SessionOperationState::Voluntary(Some(InternalTrxState::CleanupReady))
        );
        assert!(snapshot.cleanup_requested);
    }

    #[test]
    fn test_foreground_release_promotes_private_transaction_state() {
        let session_id = SessionID::new(102);

        let available_trx_id = MIN_ACTIVE_TRX_ID + 102;
        let available_entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(1)),
            SessionOperationKind::Maintenance,
        );
        available_entry.install_private_transaction(Box::new(trx_inner(
            available_trx_id,
            TrxID::new(102),
            0,
            session_id,
        )));
        let release = available_entry.release_foreground();
        assert!(!release.terminal);
        assert_eq!(release.cleanup, Some(available_trx_id));
        assert_eq!(
            available_entry.inspect().state,
            SessionOperationState::CleanupReady
        );
        assert!(available_entry.inspect().cleanup_requested);

        let terminal_trx_id = MIN_ACTIVE_TRX_ID + 103;
        let terminal_entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(2)),
            SessionOperationKind::Ddl,
        );
        terminal_entry.install_private_transaction(Box::new(trx_inner(
            terminal_trx_id,
            TrxID::new(103),
            0,
            session_id,
        )));
        let _inner = terminal_entry
            .take_for_terminal(terminal_trx_id)
            .expect("private transaction terminal ownership can be claimed");
        assert_eq!(
            terminal_entry.inspect().state,
            SessionOperationState::Voluntary(Some(InternalTrxState::Completing))
        );
        let release = terminal_entry.release_foreground();
        assert!(!release.terminal);
        assert!(release.cleanup.is_none());
        assert_eq!(
            terminal_entry.inspect().state,
            SessionOperationState::Completing
        );
        assert_eq!(
            terminal_entry.finish_transaction(terminal_trx_id),
            Some(true)
        );
        assert_eq!(
            terminal_entry.inspect().state,
            SessionOperationState::Terminal
        );

        let running_trx_id = MIN_ACTIVE_TRX_ID + 104;
        let running_entry = SessionOperationEntry::new(
            SessionOperationKey::new(session_id, OperationID::new(3)),
            SessionOperationKind::Maintenance,
        );
        running_entry.install_private_transaction(Box::new(trx_inner(
            running_trx_id,
            TrxID::new(104),
            0,
            session_id,
        )));
        let inner = running_entry
            .take_for_checkout(running_trx_id)
            .expect("private transaction can be checked out");
        let release = running_entry.release_foreground();
        assert!(!release.terminal);
        assert!(release.cleanup.is_none());
        assert_eq!(
            running_entry.inspect().state,
            SessionOperationState::Voluntary(Some(InternalTrxState::Running))
        );
        assert!(running_entry.inspect().cleanup_requested);
        assert!(running_entry.return_inner(inner));
        let snapshot = running_entry.inspect();
        assert_eq!(snapshot.trx_id, Some(running_trx_id));
        assert_eq!(snapshot.state, SessionOperationState::CleanupReady);
    }

    #[test]
    fn test_operation_state_activity_labels() {
        for state in [
            SessionOperationState::Voluntary(None),
            SessionOperationState::Voluntary(Some(InternalTrxState::Running)),
            SessionOperationState::CleanupReady,
            SessionOperationState::Completing,
            SessionOperationState::Mandatory(None),
            SessionOperationState::Mandatory(Some(InternalTrxState::Completing)),
            SessionOperationState::FailedRetained,
        ] {
            assert!(
                state.active(),
                "state should block shutdown: {}",
                state.label()
            );
        }
        assert!(!SessionOperationState::Terminal.active());
    }

    #[test]
    fn test_failed_entry_remains_an_active_operation_blocker() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                test_engine("failed_entry_remains_an_active_operation_blocker").await;
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let sts = trx.sts();
            let gc_no = transaction_gc_no(&trx);

            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            entry.fail_retained();
            assert_eq!(entry.inspect().state, SessionOperationState::FailedRetained);
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);

            engine.inner().trx_sys.record_rollback_for_purge(gc_no, sts);
            drop(trx);
            remove_session_for_test(&engine.inner().session_registry, session.id());
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_same_operation_key_rejects_wrong_transaction_identity() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                test_engine("same_operation_key_rejects_wrong_transaction_identity").await;
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let engine_ref = engine.new_ref().unwrap();
            let mut forged = Transaction::new(
                engine_ref.downgrade(),
                trx.operation_key,
                TrxID::new(trx.trx_id().as_u64() + 1),
                trx.sts(),
            );
            drop(engine_ref);

            let err = match forged.checkout() {
                Ok(_) => panic!("wrong transaction id must not claim the exact operation entry"),
                Err(err) => err,
            };
            assert_eq!(
                err.downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            let diagnostic = format!("{err:?}");
            assert!(diagnostic.contains(&format!("operation_key={}", trx.operation_key)));
            assert!(diagnostic.contains(&format!("actual_trx_id={}", trx.trx_id())));

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_checked_out_abandoned_return_notifies_session_waiter() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                test_engine("checked_out_abandoned_return_notifies_session_waiter").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            let entry = transaction_entry(&trx);
            let checkout = trx.checkout().expect("test transaction can be checked out");

            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(entry.abandon_transaction(trx_id));
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );

            let shutdown_wait = engine
                .inner()
                .session_registry
                .first_shutdown_wait()
                .expect("checked-out transaction must block shutdown");
            assert_eq!(shutdown_wait.cleanup, None);
            let (ready_tx, ready_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let waiter = spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                shutdown_wait.listener.wait();
                done_tx.send(()).expect("waiter should report completion");
            });

            ready_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("waiter should start");
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "waiter should block before checkout returns the abandoned inner"
            );

            drop(checkout);

            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("waiter should wake after checkout returns the abandoned inner");
            waiter.join().expect("waiter thread should finish");
            assert!(matches!(
                entry.inspect().state,
                SessionOperationState::CleanupReady
                    | SessionOperationState::Completing
                    | SessionOperationState::Terminal
            ));

            drop(trx);
            drop(session);
            engine.shutdown();
        });
    }

    #[inline]
    fn with_transaction_inner<T>(
        trx: &Transaction,
        operation: &'static str,
        f: impl FnOnce(&TrxInner) -> T,
    ) -> Result<T> {
        let entry = transaction_entry(trx);
        let inner_slot = entry.inner.lock();
        if inner_slot.state != SessionOperationState::Voluntary(None) {
            return Err(Report::new(LifecycleError::ExistingTransaction)
                .attach(format!(
                    "operation={operation}, operation_key={}, state={}",
                    entry.key(),
                    inner_slot.state.label()
                ))
                .disclose());
        }
        let inner = inner_slot.trx_inner.as_ref().unwrap_or_else(|| {
            panic!(
                "active test transaction must retain its checked-in core: operation={operation}, operation_key={}",
                entry.key()
            )
        });
        Ok(f(inner))
    }

    #[inline]
    fn with_transaction_inner_mut<T>(
        trx: &mut Transaction,
        operation: &'static str,
        f: impl FnOnce(&mut TrxInner) -> T,
    ) -> Result<T> {
        let mut checkout = trx
            .checkout()
            .attach_with(|| format!("operation={operation}"))
            .disclose()?;
        Ok(f(checkout.inner_mut()))
    }

    #[inline]
    fn transaction_gc_no(trx: &Transaction) -> usize {
        with_transaction_inner(trx, "query_test_transaction_gc_bucket", TrxInner::gc_no)
            .expect("test transaction must be active")
    }

    #[inline]
    fn transaction_require_durability(trx: &Transaction) -> bool {
        with_transaction_inner(
            trx,
            "query_test_transaction_durability",
            TrxInner::require_durability,
        )
        .unwrap_or(false)
    }

    #[inline]
    fn transaction_require_ordered_commit(trx: &Transaction) -> bool {
        with_transaction_inner(
            trx,
            "query_test_transaction_ordered_commit",
            TrxInner::require_ordered_commit,
        )
        .unwrap_or(false)
    }

    #[inline]
    fn prepare_transaction(mut trx: Transaction) -> Result<PreparedTrx> {
        trx.terminal_started = true;
        let claim = trx
            .claim_terminal()
            .attach("operation=prepare_active_transaction")
            .disclose()?;
        let (_entry, inner, attachment) = claim.into_parts();
        Ok(inner.prepare(attachment))
    }

    #[inline]
    fn begin_production_test_transaction(engine: &Engine) -> (Session, Transaction) {
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();
        (session, trx)
    }

    #[inline]
    fn discard_production_prepared_for_test(mut prepared: PreparedTrx) {
        if let Some(payload) = prepared.payload.take() {
            let attachment = prepared
                .attachment
                .as_ref()
                .expect("production prepared transaction must carry attachment");
            let trx_sys = &attachment.engine().trx_sys;
            let PreparedTrxPayload::User {
                status, gc_no, sts, ..
            } = payload
            else {
                panic!("production prepared transaction must carry user payload")
            };
            trx_sys.record_rollback_for_purge(gc_no, sts);
            status.finish_terminal();
            status.finish_preparing();
        }
        prepared.redo_bin.take();
        let released = prepared.release_transaction_locks();
        match (
            prepared.attachment.take(),
            released,
            prepared.trx_inner.take(),
        ) {
            (Some(attachment), Some(released), Some(inner)) => attachment.rollback(released, inner),
            (None, None, None) => {}
            _ => {
                panic!(
                    "production prepared terminal ownership requires matching attachment, \
                     transaction-lock proof, and core"
                )
            }
        }
    }

    #[inline]
    fn finish_production_committed_for_test(engine: &Engine, committed: CommittedTrx) {
        if let Some(gc_no) = committed.gc_no(engine.inner().trx_sys.gc_buckets.len()) {
            engine.inner().trx_sys.gc_buckets[gc_no].record_committed_for_purge(vec![committed]);
        }
    }

    #[inline]
    fn discard_production_transaction_after_fatal_rollback(trx: &mut Transaction) {
        let sts = trx.sts();
        let gc_no = transaction_gc_no(trx);
        let session_id = trx.operation_key.session_id();
        let engine = trx.engine().expect("test transaction must have engine");
        discard_transaction_after_fatal_rollback(trx);
        engine.trx_sys.record_rollback_for_purge(gc_no, sts);
        remove_session_for_test(&engine.session_registry, session_id);
    }

    /// Add one redo log entry for tests that need a non-readonly transaction.
    #[inline]
    pub(crate) async fn add_pseudo_redo_log_entry(trx: &mut Transaction) {
        use crate::catalog::USER_TABLE_ID_START;

        static PSEUDO_SYSBENCH_VAR1: [u8; 60] = [3; 60];
        static PSEUDO_SYSBENCH_VAR2: [u8; 120] = [4; 120];

        trx.exec(async |stmt| {
            // Simulate one sysbench record:
            // uint64 + int32 + int32 + char(60) + char(120)
            stmt.effects_mut().insert_row_redo(
                USER_TABLE_ID_START,
                RowRedo {
                    row_id: RowID::new(0),
                    kind: RowRedoKind::Insert(
                        PageID::new(0),
                        vec![
                            Val::U64(123),
                            Val::U32(1),
                            Val::U32(2),
                            Val::from(&PSEUDO_SYSBENCH_VAR1[..]),
                            Val::from(&PSEUDO_SYSBENCH_VAR2[..]),
                        ],
                    ),
                },
            );
            Ok(())
        })
        .await
        .expect("test transaction must be active")
    }

    /// Discard transaction state for tests that construct a transaction directly.
    #[inline]
    pub(crate) fn discard_transaction_after_fatal_rollback(trx: &mut Transaction) {
        let (entry, attachment) =
            resolve_active_parts_for_test(trx).expect("test transaction must be active");
        let mut checkout = SessionOperationCheckout::new(entry, attachment).expect("test checkout");
        checkout.discard_after_fatal_rollback();
    }

    #[inline]
    pub(crate) fn lock_owner(trx: &Transaction) -> Result<LockOwner> {
        with_transaction_inner(trx, "read_transaction_lock_owner", |inner| {
            Ok(inner.checked_lock_state().owner())
        })?
    }

    #[inline]
    pub(crate) fn cached_transaction_lock_covers(
        trx: &Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        with_transaction_inner(
            trx,
            "check_transaction_lock_cache",
            |inner| -> Result<bool> {
                Ok(inner.checked_lock_state().cached_covers(resource, mode))
            },
        )?
    }

    #[inline]
    pub(crate) fn try_acquire_transaction_lock(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let mut checkout = trx
            .checkout()
            .attach("operation=try_acquire_transaction_lock")
            .disclose()?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        try_acquire_owner_lock_state(inner.checked_lock_state_mut(), lock_manager, resource, mode)
    }

    #[inline]
    fn try_acquire_owner_lock_state(
        lock_state: &mut OwnerLockState,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        if lock_state.cached_covers(resource, mode) {
            return Ok(true);
        }
        let owner = lock_state.owner();
        let acquired = try_acquire(lock_manager, resource, mode, owner).disclose()?;
        if acquired {
            lock_state.cache_granted(resource, mode);
        }
        Ok(acquired)
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
    }

    #[derive(Debug)]
    struct TerminalBoundaryObservation {
        outcome: TerminalAttachmentOutcome,
        transaction_lock_entries: usize,
        session_active: bool,
        status_ts: Option<TrxID>,
        session_lock_entries: usize,
    }

    fn install_terminal_boundary_observer(
        engine: EngineRef,
        operation_key: SessionOperationKey,
        target_trx_id: TrxID,
        status: Option<Arc<SharedTrxStatus>>,
        session_owner: Option<LockOwner>,
    ) -> (
        TerminalAttachmentTestHookGuard,
        mpsc::Receiver<TerminalBoundaryObservation>,
    ) {
        let session_id = operation_key.session_id();
        let (observed_tx, observed_rx) = mpsc::channel();
        let hook = Arc::new(move |trx_id, outcome| {
            if trx_id != target_trx_id {
                return;
            }
            let snapshot = debug_snapshot(engine.lock_manager());
            let transaction_lock_entries = snapshot
                .entries
                .iter()
                .filter(|entry| entry.owner == LockOwner::transaction(session_id, trx_id))
                .count();
            let session_lock_entries = session_owner.map_or(0, |owner| {
                snapshot
                    .entries
                    .iter()
                    .filter(|entry| entry.owner == owner)
                    .count()
            });
            observed_tx
                .send(TerminalBoundaryObservation {
                    outcome,
                    transaction_lock_entries,
                    session_active: engine
                        .session_registry
                        .try_resolve_operation(operation_key)
                        .is_some(),
                    status_ts: status.as_ref().map(|status| status.ts()),
                    session_lock_entries,
                })
                .expect("terminal attachment observer should report the boundary");
        });
        (install_terminal_attachment_test_hook(hook), observed_rx)
    }

    fn recv_terminal_boundary(
        observed_rx: &mpsc::Receiver<TerminalBoundaryObservation>,
    ) -> TerminalBoundaryObservation {
        observed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("terminal attachment boundary should be observed")
    }

    fn wait_until(mut done: impl FnMut() -> bool, message: &'static str) {
        // Timer audit: engine-shutdown/test-hook state inspection watchdog.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "{message}");
            sleep(Duration::from_millis(1));
        }
    }

    fn terminal_rollback_hook_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    type TerminalRollbackRelease = Arc<(Mutex<bool>, Condvar)>;

    fn install_blocking_terminal_rollback_hook(
        target_trx_id: TrxID,
        target_operation: &'static str,
    ) -> (
        TerminalRollbackTestHookGuard,
        mpsc::Receiver<&'static str>,
        TerminalRollbackRelease,
    ) {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_release = Arc::clone(&release);
        let hook: Arc<dyn Fn(TrxID, &'static str) + Send + Sync> =
            Arc::new(move |trx_id, operation| {
                if trx_id != target_trx_id || operation != target_operation {
                    return;
                }
                started_tx
                    .send(operation)
                    .expect("terminal rollback hook should report start");
                let (released, cvar) = &*hook_release;
                let released = released
                    .lock()
                    .expect("terminal rollback release mutex should not be poisoned");
                let (released, timeout) = cvar
                    .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                    .expect("terminal rollback release wait should not be poisoned");
                assert!(
                    *released && !timeout.timed_out(),
                    "terminal rollback test hook was not released"
                );
            });
        let guard = install_terminal_rollback_test_hook(hook);
        (guard, started_rx, release)
    }

    fn release_terminal_rollback_hook(release: &TerminalRollbackRelease) {
        let (released, cvar) = &**release;
        *released
            .lock()
            .expect("terminal rollback release mutex should not be poisoned") = true;
        cvar.notify_all();
    }

    fn has_lock_entry(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
        mode: LockMode,
        state: LockDebugEntryState,
    ) -> bool {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .any(|entry| {
                entry.owner == owner
                    && entry.resource == resource
                    && entry.mode == mode
                    && entry.state == state
            })
    }

    fn has_lock_resource(engine: &Engine, owner: LockOwner, resource: LockResource) -> bool {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .any(|entry| entry.owner == owner && entry.resource == resource)
    }

    async fn publish_initial_test_root(engine: &Engine, table_id_offset: u64) -> Arc<TableFile> {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![],
            )
            .expect("valid table metadata"),
        );
        let table_id = test_user_table_id(table_id_offset);
        let mutable = engine
            .inner()
            .table_fs
            .create_table_file(table_id, metadata, false)
            .unwrap();
        engine
            .inner()
            .trx_sys
            .publish_table_file_root(mutable, TrxID::new(1), false)
            .await
            .unwrap()
    }

    #[test]
    fn test_transaction_readonly_prepare_keeps_empty_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_readonly_prepare").await;
            let (_session, trx) = begin_production_test_transaction(&engine);
            let expected_sts = trx.sts();
            let expected_gc_no = transaction_gc_no(&trx);

            let prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(!prepared.require_ordered_commit());
            let payload = prepared.payload.as_ref().unwrap();
            let PreparedTrxPayload::User {
                sts,
                gc_no,
                row_undo,
                index_undo,
                ..
            } = payload
            else {
                panic!("readonly transaction must carry user payload")
            };
            assert_eq!(*sts, expected_sts);
            assert_eq!(*gc_no, expected_gc_no);
            assert!(row_undo.is_empty());
            assert!(index_undo.is_empty());

            discard_production_prepared_for_test(prepared);
        });
    }

    #[test]
    fn test_transaction_prepare_moves_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_prepare").await;
            let (_session, mut trx) = begin_production_test_transaction(&engine);
            add_pseudo_redo_log_entry(&mut trx).await;
            with_transaction_inner_mut(&mut trx, "test_prepare_payload", |inner| {
                inner.row_undo_mut().push(OwnedRowUndo::new(
                    TableID::new(11),
                    None,
                    RowID::new(22),
                    RowUndoKind::Delete,
                ));
                inner.index_undo_mut().push(IndexUndo {
                    table_id: TableID::new(11),
                    row_id: RowID::new(22),
                    kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
                });
            })
            .unwrap();
            let expected_sts = trx.sts();
            let expected_gc_no = transaction_gc_no(&trx);

            let prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            let payload = prepared.payload.as_ref().unwrap();
            let PreparedTrxPayload::User {
                sts,
                gc_no,
                row_undo,
                index_undo,
                ..
            } = payload
            else {
                panic!("prepared transaction must carry user payload")
            };
            assert_eq!(*sts, expected_sts);
            assert_eq!(*gc_no, expected_gc_no);
            assert_eq!(row_undo.len(), 1);
            assert_eq!(index_undo.len(), 1);

            discard_production_prepared_for_test(prepared);
        });
    }

    #[test]
    fn test_precommit_retains_index_undo_until_successful_commit() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_precommit_index_undo").await;
            let (_session, mut trx) = begin_production_test_transaction(&engine);
            with_transaction_inner_mut(&mut trx, "test_precommit_index_undo", |inner| {
                inner.index_undo_mut().push(IndexUndo {
                    table_id: TableID::new(11),
                    row_id: RowID::new(22),
                    kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
                });
            })
            .unwrap();

            let precommit = prepare_transaction(trx)
                .unwrap()
                .fill_cts(TrxID::new(91_247));
            let payload = precommit.payload.as_ref().unwrap();
            let PrecommitTrxPayload::User { index_undo, .. } = payload else {
                panic!("precommit transaction must carry user payload")
            };
            assert_eq!(index_undo.len(), 1);

            let committed = precommit.commit();
            let index_gc = committed.index_gc().unwrap();
            assert_eq!(index_gc.len(), 1);
            assert_eq!(index_gc[0].table_id, TableID::new(11));
            assert_eq!(index_gc[0].row_id, RowID::new(22));
            finish_production_committed_for_test(&engine, committed);
        });
    }

    #[test]
    fn test_prepare_listener_wakes_after_precommit_commit() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_prepare_waiter_commit").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            let status =
                with_transaction_inner(&trx, "test_prepare_commit_waiter_status", |inner| {
                    Arc::clone(inner.ctx().status())
                })
                .unwrap();

            let prepared = prepare_transaction(trx).unwrap();
            assert!(status.preparing());
            let PrepareListenerResult::Registered(listener) = status.prepare_listener() else {
                panic!("preparing transaction should expose a listener");
            };
            let waiter_status = Arc::clone(&status);
            let (ready_tx, ready_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let waiter = spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                listener.wait();
                done_tx
                    .send(waiter_status.ts())
                    .expect("waiter should report observed status");
            });

            ready_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("waiter should start");
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "prepare waiter should block before commit completion"
            );

            let cts = TrxID::new(91_248);
            let mut precommit = prepared.fill_cts(cts);
            let _redo = precommit.take_log().expect("durable test transaction");
            let committed = precommit.commit();
            assert_eq!(committed.cts, cts);
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("waiter should wake after commit"),
                cts
            );
            waiter.join().expect("waiter thread should finish");
            assert_eq!(status.ts(), cts);
            assert!(!status.preparing());
            assert!(matches!(
                status.prepare_listener(),
                PrepareListenerResult::NotPreparing
            ));
        });
    }

    #[test]
    fn test_prepare_listener_is_injected_only_by_waiters() {
        let status = shared_trx_status(MIN_ACTIVE_TRX_ID + 90_000);
        status.mark_preparing();
        assert!(
            status.prepare_ev.lock().is_none(),
            "uncontended prepare must not allocate a notifier"
        );

        let PrepareListenerResult::Registered(first) = status.prepare_listener() else {
            panic!("first prepare waiter should install a listener");
        };
        let event_addr = {
            let guard = status.prepare_ev.lock();
            guard.as_ref().expect("prepare event") as *const EventNotifyOnDrop
        };
        let PrepareListenerResult::Registered(second) = status.prepare_listener() else {
            panic!("later prepare waiter should share the listener event");
        };
        let shared_event_addr = {
            let guard = status.prepare_ev.lock();
            guard.as_ref().expect("prepare event") as *const EventNotifyOnDrop
        };
        assert_eq!(event_addr, shared_event_addr);

        status.finish_preparing();
        first.wait();
        second.wait();
        assert!(!status.preparing());
        assert!(status.prepare_ev.lock().is_none());
    }

    #[test]
    fn test_prepare_without_waiters_keeps_event_slot_empty() {
        let status = shared_trx_status(MIN_ACTIVE_TRX_ID + 90_001);
        status.mark_preparing();
        assert!(status.prepare_ev.lock().is_none());
        status.finish_preparing();
        assert!(status.prepare_ev.lock().is_none());
        assert!(matches!(
            status.prepare_listener(),
            PrepareListenerResult::NotPreparing
        ));
    }

    #[test]
    fn test_prepare_completion_wins_first_listener_registration() {
        let status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 90_002));
        status.mark_preparing();
        let (loaded_tx, loaded_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter_status = Arc::clone(&status);
        let waiter = spawn(move || {
            install_prepare_listener_before_lock_hook(move || {
                loaded_tx
                    .send(())
                    .expect("waiter should report its optimistic prepare load");
                release_rx
                    .recv()
                    .expect("waiter registration should be released");
            });
            result_tx
                .send(matches!(
                    waiter_status.prepare_listener(),
                    PrepareListenerResult::Completed
                ))
                .expect("waiter should report registration outcome");
        });

        loaded_rx
            .recv()
            .expect("waiter should pause before acquiring the prepare mutex");
        status.finish_preparing();
        release_tx
            .send(())
            .expect("waiter registration should resume");
        assert!(
            result_rx.recv().expect("waiter should finish registration"),
            "completion must be distinguished from the not-preparing fast path"
        );
        waiter.join().expect("waiter thread should finish");
        assert!(status.prepare_ev.lock().is_none());
    }

    #[test]
    fn test_cancelled_prepare_listener_leaves_event_for_completion() {
        let status = shared_trx_status(MIN_ACTIVE_TRX_ID + 90_003);
        status.mark_preparing();
        let PrepareListenerResult::Registered(listener) = status.prepare_listener() else {
            panic!("preparing status should install a listener");
        };
        drop(listener);
        assert!(
            status.prepare_ev.lock().is_some(),
            "listener cancellation must not remove the shared event"
        );
        status.finish_preparing();
        assert!(status.prepare_ev.lock().is_none());
    }

    #[test]
    fn test_prepare_listener_wakes_after_failed_precommit_rollback() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_prepare_waiter_rollback").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            let status =
                with_transaction_inner(&trx, "test_prepare_rollback_waiter_status", |inner| {
                    Arc::clone(inner.ctx().status())
                })
                .unwrap();

            let prepared = prepare_transaction(trx).unwrap();
            assert!(status.preparing());
            let PrepareListenerResult::Registered(listener) = status.prepare_listener() else {
                panic!("preparing transaction should expose a listener");
            };
            let waiter_status = Arc::clone(&status);
            let (ready_tx, ready_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let waiter = spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                listener.wait();
                done_tx
                    .send(matches!(
                        waiter_status.prepare_listener(),
                        PrepareListenerResult::NotPreparing
                    ))
                    .expect("waiter should report completion state");
            });

            ready_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("waiter should start");
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "prepare waiter should block before rollback completion"
            );

            let mut precommit = prepared.fill_cts(TrxID::new(91_249));
            assert_eq!(
                precommit.rollback_failed_precommit().await,
                FailedPrecommitRollbackOutcome::RolledBack
            );
            assert!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("waiter should wake after failed-precommit rollback"),
                "late listener lookup should return none after rollback"
            );
            waiter.join().expect("waiter thread should finish");
            assert!(!status.preparing());
            assert!(matches!(
                status.prepare_listener(),
                PrepareListenerResult::NotPreparing
            ));
        });
    }

    #[test]
    fn test_statement_success_merges_statement_effects_into_transaction_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_effect_merge").await;
            let (_session, mut trx) = begin_production_test_transaction(&engine);
            trx.exec(async |stmt| {
                let effects = stmt.effects_mut();
                effects.push_row_undo(OwnedRowUndo::new(
                    TableID::new(12),
                    None,
                    RowID::new(23),
                    RowUndoKind::Delete,
                ));
                effects.push_delete_index_undo(
                    TableID::new(12),
                    RowID::new(23),
                    SelectKey::new(0, vec![]),
                    true,
                );
                effects.insert_row_redo(
                    TableID::new(12),
                    RowRedo {
                        row_id: RowID::new(23),
                        kind: RowRedoKind::Delete(Some(PageID::new(0))),
                    },
                );
                Ok(())
            })
            .await
            .unwrap();
            assert!(transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            with_transaction_inner(&trx, "check_merged_statement_effects", |inner| {
                assert_eq!(inner.effects.row_undo.len(), 1);
                assert_eq!(inner.effects.index_undo.len(), 1);
                assert!(!inner.effects.redo.is_empty());
            })
            .unwrap();

            discard_production_transaction_after_fatal_rollback(&mut trx);
        });
    }

    #[test]
    fn test_unpolled_statement_future_leaves_transaction_reusable() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_unpolled_stmt_cancel").await;
            let (_session, mut trx) = begin_production_test_transaction(&engine);
            let entry = transaction_entry(&trx);

            let exec = trx.exec(async |_| Ok::<(), Error>(()));
            drop(exec);

            let snapshot = entry.inspect();
            assert_eq!(snapshot.state, SessionOperationState::Voluntary(None));
            assert!(!snapshot.cleanup_requested);
            trx.exec(async |_| Ok::<(), Error>(())).await.unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_dropped_polled_statement_future_terminally_cancels_transaction() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_polled_stmt_cancel").await;
            let (session, mut trx) = begin_production_test_transaction(&engine);
            let session_id = session.id();
            let entry = transaction_entry(&trx);
            let mut exec = Box::pin(trx.exec(async |_| {
                pending::<()>().await;
                Ok::<(), Error>(())
            }));

            assert!(matches!(
                futures::poll!(exec.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            drop(exec);

            assert_ne!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            let err = trx.exec(async |_| Ok::<(), Error>(())).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session_id).await;
            engine.shutdown();
        });
    }

    #[test]
    fn test_dropped_effectful_statement_discards_redo_and_releases_locks() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_effectful_stmt_cancel").await;
            let (session, mut trx) = begin_production_test_transaction(&engine);
            let session_id = session.id();
            let stmt_owner = lock_owner(&trx).unwrap().statement(1);
            let resource = LockResource::TableMetadata(TableID::new(91_430));
            let mut exec = Box::pin(trx.exec(async |stmt| {
                stmt_tests::acquire_statement_lock(stmt, resource, LockMode::Shared).await?;
                stmt.effects_mut().insert_row_redo(
                    TableID::new(91_430),
                    RowRedo {
                        row_id: RowID::new(1),
                        kind: RowRedoKind::Delete(Some(PageID::new(0))),
                    },
                );
                pending::<()>().await;
                Ok::<(), Error>(())
            }));

            assert!(matches!(
                futures::poll!(exec.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(has_lock_entry(
                &engine,
                stmt_owner,
                resource,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            drop(exec);

            assert!(!has_lock_resource(&engine, stmt_owner, resource));
            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session_id).await;
            engine.shutdown();
        });
    }

    async fn poll_until_statement_rollback_pauses<F>(mut exec: Pin<&mut F>, paused: fn() -> bool)
    where
        F: Future,
    {
        for _ in 0..512 {
            assert!(matches!(
                futures::poll!(exec.as_mut()),
                std::task::Poll::Pending
            ));
            if paused() {
                return;
            }
            yield_now().await;
        }
        panic!("statement rollback did not reach the requested pause predicate");
    }

    async fn assert_dropped_statement_rollback_residuals_are_transaction_owned(
        log_file_stem: &str,
        value: i32,
        pause_index: bool,
    ) {
        let (_temp_dir, engine) = test_engine(log_file_stem).await;
        let table_id = catalog_tests::table2(&engine).await;
        let mut session = engine.new_session().unwrap();
        let session_id = session.id();
        let mut trx = session.begin_trx().unwrap();
        if pause_index {
            pause_next_index_rollback();
        } else {
            pause_next_row_rollback();
        }
        let mut exec = Box::pin(trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(value), Val::from("cancelled")])
                .await?;
            Err::<(), Error>(Report::new(OperationError::InvalidDmlInput).disclose())
        }));

        poll_until_statement_rollback_pauses(
            exec.as_mut(),
            if pause_index {
                index_rollback_paused
            } else {
                row_rollback_paused
            },
        )
        .await;
        drop(exec);

        let err = trx.exec(async |_| Ok::<(), Error>(())).await.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::TransactionDiscarded)
        );
        wait_for_session_idle(&engine.inner().session_registry, session_id).await;

        let mut verify = session.begin_trx().unwrap();
        let select = verify
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(table_id, 0, &[Val::from(value)], &[0, 1])
                    .await
            })
            .await
            .unwrap();
        assert!(
            select.not_found(),
            "whole-transaction cleanup must consume the residual statement insert"
        );
        verify.rollback().await.unwrap();
        engine.shutdown();
    }

    #[test]
    fn test_cancelled_index_rollback_folds_index_and_row_residuals() {
        smol::block_on(
            assert_dropped_statement_rollback_residuals_are_transaction_owned(
                "trx_cancel_index_rollback",
                91_433,
                true,
            ),
        );
    }

    #[test]
    fn test_cancelled_row_rollback_folds_remaining_row_residual() {
        smol::block_on(
            assert_dropped_statement_rollback_residuals_are_transaction_owned(
                "trx_cancel_row_rollback",
                91_434,
                false,
            ),
        );
    }

    async fn assert_dropped_statement_waiter_released(
        log_file_stem: &str,
        id: u64,
        promote_before_drop: bool,
    ) {
        let (_temp_dir, engine) = test_engine(log_file_stem).await;
        let (session, mut trx) = begin_production_test_transaction(&engine);
        let session_id = session.id();
        let stmt_owner = lock_owner(&trx).unwrap().statement(1);
        let resource = LockResource::TableMetadata(TableID::new(id));
        let blocker = LockOwner::transaction(SessionID::new(id), TrxID::new(id));
        assert!(
            try_acquire(
                engine.lock_manager(),
                resource,
                LockMode::Exclusive,
                blocker
            )
            .unwrap()
        );
        let mut exec = Box::pin(trx.exec(async |stmt| {
            stmt_tests::acquire_statement_lock(stmt, resource, LockMode::Shared).await?;
            Ok::<(), Error>(())
        }));

        assert!(matches!(
            futures::poll!(exec.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(has_lock_entry(
            &engine,
            stmt_owner,
            resource,
            LockMode::Shared,
            LockDebugEntryState::Waiting,
        ));
        if promote_before_drop {
            assert_eq!(engine.lock_manager().release(resource, blocker), 1);
            assert!(has_lock_entry(
                &engine,
                stmt_owner,
                resource,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
        }

        drop(exec);

        assert!(!has_lock_resource(&engine, stmt_owner, resource));
        if !promote_before_drop {
            assert_eq!(engine.lock_manager().release(resource, blocker), 1);
        }
        let err = trx.rollback().await.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::TransactionDiscarded)
        );
        wait_for_session_idle(&engine.inner().session_registry, session_id).await;
        engine.shutdown();
    }

    #[test]
    fn test_dropped_statement_removes_queued_lock_before_cleanup() {
        smol::block_on(assert_dropped_statement_waiter_released(
            "trx_queued_stmt_lock_cancel",
            91_431,
            false,
        ));
    }

    #[test]
    fn test_dropped_statement_releases_promoted_unobserved_lock_before_cleanup() {
        smol::block_on(assert_dropped_statement_waiter_released(
            "trx_promoted_stmt_lock_cancel",
            91_432,
            true,
        ));
    }

    #[test]
    #[should_panic(expected = "catalog table DML must be logged")]
    fn test_redo_invariants_debug_assert_catalog_dml_without_metadata_ddl() {
        let mut effects = TrxEffects::empty();
        effects.redo.insert_dml(
            TABLE_ID_TABLES,
            RowRedo {
                row_id: RowID::new(0),
                kind: RowRedoKind::Insert(PageID::new(0), vec![Val::U64(1)]),
            },
        );
        effects.debug_assert_redo_invariants();
    }

    #[test]
    fn test_statement_error_rolls_back_only_statement_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_error_rollback").await;
            let (_session, mut trx) = begin_production_test_transaction(&engine);

            trx.exec(async |stmt| {
                stmt.effects_mut().insert_row_redo(
                    TableID::new(12),
                    RowRedo {
                        row_id: RowID::new(23),
                        kind: RowRedoKind::Delete(Some(PageID::new(0))),
                    },
                );
                Ok(())
            })
            .await
            .unwrap();

            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt.effects_mut().insert_row_redo(
                        TableID::new(12),
                        RowRedo {
                            row_id: RowID::new(24),
                            kind: RowRedoKind::Delete(Some(PageID::new(0))),
                        },
                    );
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            let err = res.unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidDmlInput)
            );
            with_transaction_inner(&trx, "check_statement_rollback_effects", |inner| {
                let table_redo = inner.effects.redo.dml.get(&TableID::new(12)).unwrap();
                assert!(table_redo.rows.contains_key(&RowID::new(23)));
                assert!(!table_redo.rows.contains_key(&RowID::new(24)));
            })
            .unwrap();

            discard_production_transaction_after_fatal_rollback(&mut trx);
        });
    }

    #[test]
    fn test_statement_locks_release_without_releasing_transaction_locks() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_lock_release").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = lock_owner(&trx).unwrap();
            let trx_resource = LockResource::TableData(TableID::new(91_210));
            assert!(
                try_acquire_transaction_lock(&mut trx, trx_resource, LockMode::IntentExclusive)
                    .unwrap()
            );

            let first_owner = Cell::new(None);
            trx.exec(async |stmt| {
                let owner = stmt_tests::lock_owner(stmt);
                first_owner.set(Some(owner));
                stmt_tests::acquire_statement_lock(
                    stmt,
                    LockResource::TableMetadata(TableID::new(91_210)),
                    LockMode::Shared,
                )
                .await?;
                stmt_tests::acquire_statement_lock(
                    stmt,
                    LockResource::TableMetadata(TableID::new(91_210)),
                    LockMode::Shared,
                )
                .await?;
                assert_eq!(lock_entry_count(&engine, owner), 1);
                Ok(())
            })
            .await
            .unwrap();

            let second_owner = Cell::new(None);
            trx.exec(async |stmt| {
                let owner = stmt_tests::lock_owner(stmt);
                second_owner.set(Some(owner));
                assert!(stmt_tests::try_acquire_statement_lock(
                    stmt,
                    LockResource::TableMetadata(TableID::new(91_211)),
                    LockMode::Shared,
                )?);
                assert!(stmt_tests::try_acquire_statement_lock(
                    stmt,
                    LockResource::TableMetadata(TableID::new(91_211)),
                    LockMode::Shared,
                )?);
                assert_eq!(lock_entry_count(&engine, owner), 1);
                Ok(())
            })
            .await
            .unwrap();

            let error_owner = Cell::new(None);
            let res: Result<()> = trx
                .exec(async |stmt| {
                    let owner = stmt_tests::lock_owner(stmt);
                    error_owner.set(Some(owner));
                    stmt_tests::acquire_statement_lock(
                        stmt,
                        LockResource::TableMetadata(TableID::new(91_212)),
                        LockMode::Shared,
                    )
                    .await?;
                    assert_eq!(lock_entry_count(&engine, owner), 1);
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );

            let first_owner = first_owner.get().unwrap();
            let second_owner = second_owner.get().unwrap();
            let error_owner = error_owner.get().unwrap();
            assert_eq!(first_owner, trx_owner.statement(1));
            assert_eq!(second_owner, trx_owner.statement(2));
            assert_eq!(error_owner, trx_owner.statement(3));
            assert_eq!(lock_entry_count(&engine, first_owner), 0);
            assert_eq!(lock_entry_count(&engine, second_owner), 0);
            assert_eq!(lock_entry_count(&engine, error_owner), 0);
            assert_eq!(lock_entry_count(&engine, trx_owner), 1);

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, trx_owner), 0);
        });
    }

    #[test]
    fn test_transaction_lock_cache_skips_covered_requests_and_preserves_errors() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_cache").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            let data = LockResource::TableData(TableID::new(91_220));

            assert!(
                try_acquire_transaction_lock(&mut trx, data, LockMode::IntentExclusive).unwrap()
            );
            assert!(cached_transaction_lock_covers(&trx, data, LockMode::IntentShared).unwrap());
            assert!(try_acquire_transaction_lock(&mut trx, data, LockMode::IntentShared).unwrap());
            assert_eq!(lock_entry_count(&engine, owner), 1);

            let err = try_acquire_transaction_lock(&mut trx, data, LockMode::Shared).unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::LockConversionNotSupported)
            );
            assert_eq!(lock_entry_count(&engine, owner), 1);

            let metadata = LockResource::TableMetadata(TableID::new(91_221));
            assert!(try_acquire_transaction_lock(&mut trx, metadata, LockMode::Shared).unwrap());
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    metadata,
                    LockMode::Shared,
                    LockOwner::session_explicit(SessionID::new(91_221))
                )
                .unwrap()
            );
            let err =
                try_acquire_transaction_lock(&mut trx, metadata, LockMode::Exclusive).unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::LockUpgradeWouldBlock)
            );
            engine
                .lock_manager()
                .release_owner(LockOwner::session_explicit(SessionID::new(91_221)));

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_lock_table_caches_explicit_locks_and_restores_entry_state() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_lock_table_cache").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let owner = lock_owner(&trx).unwrap();
            let metadata = LockResource::TableMetadata(table_id);
            let data = LockResource::TableData(table_id);

            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            trx.lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(cached_transaction_lock_covers(&trx, metadata, LockMode::Shared).unwrap());
            assert!(cached_transaction_lock_covers(&trx, data, LockMode::Exclusive).unwrap());

            trx.lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            trx.lock_table(table_id, TableLockMode::Exclusive)
                .await
                .unwrap();

            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert_eq!(lock_entry_count(&engine, owner), 2);
            assert!(has_lock_entry(
                &engine,
                owner,
                metadata,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                data,
                LockMode::Exclusive,
                LockDebugEntryState::Granted,
            ));

            trx.rollback().await.unwrap();
            assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_pending_lock_table_checkout_restores_active_on_drop() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_lock_table_cancel_state").await;
            let table_id = catalog_tests::table2(&engine).await;
            let blocker = LockOwner::transaction(SessionID::new(91_401), TrxID::new(91_401));
            let data = LockResource::TableData(table_id);
            assert!(
                try_acquire(engine.lock_manager(), data, LockMode::Exclusive, blocker).unwrap()
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let owner = lock_owner(&trx).unwrap();
            let metadata = LockResource::TableMetadata(table_id);
            let mut lock_fut = Box::pin(trx.lock_table(table_id, TableLockMode::Shared));

            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(has_lock_entry(
                &engine,
                owner,
                metadata,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                data,
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(lock_fut);

            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(!has_lock_resource(&engine, owner, metadata));
            assert!(!has_lock_resource(&engine, owner, data));
            assert!(!cached_transaction_lock_covers(&trx, metadata, LockMode::Shared).unwrap());
            assert_eq!(engine.lock_manager().release(data, blocker), 1);

            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_lock_table_cancel_preserves_cached_metadata_only() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_lock_table_cached_metadata_cancel").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let owner = lock_owner(&trx).unwrap();
            let metadata = LockResource::TableMetadata(table_id);
            let data = LockResource::TableData(table_id);

            assert!(try_acquire_transaction_lock(&mut trx, metadata, LockMode::Shared).unwrap());
            assert!(cached_transaction_lock_covers(&trx, metadata, LockMode::Shared).unwrap());

            let blocker = LockOwner::transaction(SessionID::new(91_402), TrxID::new(91_402));
            assert!(
                try_acquire(engine.lock_manager(), data, LockMode::Exclusive, blocker).unwrap()
            );

            let mut lock_fut = Box::pin(trx.lock_table(table_id, TableLockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(has_lock_entry(
                &engine,
                owner,
                metadata,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                data,
                LockMode::Shared,
                LockDebugEntryState::Waiting,
            ));

            drop(lock_fut);

            assert_eq!(
                entry.inspect().state,
                SessionOperationState::Voluntary(None)
            );
            assert!(has_lock_entry(
                &engine,
                owner,
                metadata,
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(!has_lock_resource(&engine, owner, data));
            assert!(cached_transaction_lock_covers(&trx, metadata, LockMode::Shared).unwrap());
            assert!(!cached_transaction_lock_covers(&trx, data, LockMode::Shared).unwrap());
            assert_eq!(engine.lock_manager().release(data, blocker), 1);

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_transaction_locks_release_on_readonly_commit_rollback_and_ordered_commit() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_terminal").await;

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_230)),
                    LockMode::IntentShared,
                )
                .unwrap()
            );
            assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_231)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );
            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_232)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );
            add_pseudo_redo_log_entry(&mut trx).await;
            assert!(trx.commit().await.unwrap() > TrxID::new(0));
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_released_transaction_lock_proof_validates_identity() {
        let proof_trx_id = TrxID::new(91_501);
        ReleasedTransactionLocks::new(proof_trx_id).assert_validated_for(proof_trx_id);

        let attachment_trx_id = TrxID::new(91_502);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            ReleasedTransactionLocks::new(proof_trx_id).assert_validated_for(attachment_trx_id);
        }))
        .expect_err("mismatched transaction-lock proof must panic");
        let diagnostic = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&'static str>().copied())
            .expect("proof mismatch panic should contain a string diagnostic");
        assert!(diagnostic.contains("terminal attachment boundary"));
        assert!(diagnostic.contains(&format!("proof_trx_id={proof_trx_id}")));
        assert!(diagnostic.contains(&format!("attachment_trx_id={attachment_trx_id}")));
    }

    #[test]
    fn test_ordered_commit_releases_locks_before_session_finish() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_ordered_commit_release_boundary").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_510)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );
            add_pseudo_redo_log_entry(&mut trx).await;
            let status = with_transaction_inner(&trx, "observe_ordered_commit_status", |inner| {
                Arc::clone(inner.ctx().status())
            })
            .unwrap();
            let (hook, observed_rx) = install_terminal_boundary_observer(
                engine.new_ref().unwrap(),
                trx.operation_key,
                trx_id,
                Some(status),
                None,
            );

            let cts = trx.commit().await.unwrap();
            let observed = recv_terminal_boundary(&observed_rx);
            assert_eq!(observed.outcome, TerminalAttachmentOutcome::Commit);
            assert_eq!(observed.transaction_lock_entries, 0);
            assert!(observed.session_active);
            assert_eq!(observed.status_ts, Some(cts));
            assert!(!session.in_trx().unwrap());

            drop(hook);
            session.begin_trx().unwrap().rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unordered_commit_releases_locks_before_session_finish() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_unordered_commit_release_boundary").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_511)),
                    LockMode::IntentShared,
                )
                .unwrap()
            );
            let (hook, observed_rx) = install_terminal_boundary_observer(
                engine.new_ref().unwrap(),
                trx.operation_key,
                trx_id,
                None,
                None,
            );

            assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
            let observed = recv_terminal_boundary(&observed_rx);
            assert_eq!(observed.outcome, TerminalAttachmentOutcome::Rollback);
            assert_eq!(observed.transaction_lock_entries, 0);
            assert!(observed.session_active);
            assert!(!session.in_trx().unwrap());

            drop(hook);
            session.begin_trx().unwrap().rollback().await.unwrap();
        });
    }

    #[test]
    fn test_rollback_releases_locks_before_session_finish() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_rollback_release_boundary").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_512)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );
            let (hook, observed_rx) = install_terminal_boundary_observer(
                engine.new_ref().unwrap(),
                trx.operation_key,
                trx_id,
                None,
                None,
            );

            trx.rollback().await.unwrap();
            let observed = recv_terminal_boundary(&observed_rx);
            assert_eq!(observed.outcome, TerminalAttachmentOutcome::Rollback);
            assert_eq!(observed.transaction_lock_entries, 0);
            assert!(observed.session_active);
            assert!(!session.in_trx().unwrap());

            drop(hook);
            session.begin_trx().unwrap().rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_admission_waits_for_terminal_lifecycle_finish() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_session_admission_terminal_boundary").await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_513)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );

            let (reached_tx, reached_rx) = mpsc::channel();
            let (release_tx, release_rx) = flume::bounded(1);
            let hook =
                install_terminal_attachment_test_hook(Arc::new(move |observed_trx_id, outcome| {
                    if observed_trx_id != trx_id {
                        return;
                    }
                    reached_tx
                        .send(outcome)
                        .expect("terminal hook should report its boundary");
                    let _ = release_rx.recv();
                }));

            scope(|scope| {
                let release_tx = release_tx;
                let rollback = scope.spawn(move || smol::block_on(trx.rollback()));
                let outcome = reached_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("rollback should reach the terminal attachment boundary");
                assert!(
                    session.list_table_ids().is_ok(),
                    "read-only observation remains admissible during terminal ownership"
                );
                let admission = session.begin_trx();
                let transaction_lock_entries =
                    lock_entry_count(&engine, LockOwner::transaction(session_id, trx_id));

                release_tx
                    .send(())
                    .expect("terminal hook should remain available for release");
                rollback.join().unwrap().unwrap();

                assert_eq!(outcome, TerminalAttachmentOutcome::Rollback);
                assert_eq!(transaction_lock_entries, 0);
                let err = match admission {
                    Ok(_) => panic!("terminal transaction ownership must block replacement"),
                    Err(err) => err,
                };
                assert_existing_transaction_error(&err, session_id, trx_id, "completing");
            });

            drop(hook);
            assert!(session.list_table_ids().is_ok());
        });
    }

    #[test]
    fn test_abandoned_session_releases_transaction_locks_before_explicit_locks() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_abandoned_session_release_boundary").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let session_owner = LockOwner::session_explicit(session_id);
            session
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(table_id),
                    LockMode::IntentShared,
                )
                .unwrap()
            );
            let (hook, observed_rx) = install_terminal_boundary_observer(
                engine.new_ref().unwrap(),
                trx.operation_key,
                trx_id,
                None,
                Some(session_owner),
            );

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            trx.rollback().await.unwrap();

            let observed = recv_terminal_boundary(&observed_rx);
            assert_eq!(observed.outcome, TerminalAttachmentOutcome::Rollback);
            assert_eq!(observed.transaction_lock_entries, 0);
            assert!(observed.session_active);
            assert!(observed.session_lock_entries > 0);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            assert_eq!(lock_entry_count(&engine, session_owner), 0);
            drop(hook);
        });
    }

    #[test]
    fn test_transaction_locks_release_on_precommit_abort() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_abort").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            let owner = lock_owner(&trx).unwrap();
            assert!(
                try_acquire_transaction_lock(
                    &mut trx,
                    LockResource::TableData(TableID::new(91_240)),
                    LockMode::IntentExclusive,
                )
                .unwrap()
            );
            add_pseudo_redo_log_entry(&mut trx).await;

            let operation_key = trx.operation_key;
            let prepared = prepare_transaction(trx).unwrap();
            let mut precommit = prepared.fill_cts(TrxID::new(91_241));
            let (hook, observed_rx) = install_terminal_boundary_observer(
                engine.new_ref().unwrap(),
                operation_key,
                trx_id,
                None,
                None,
            );
            assert_eq!(
                precommit.rollback_failed_precommit().await,
                FailedPrecommitRollbackOutcome::RolledBack
            );
            let observed = recv_terminal_boundary(&observed_rx);
            assert_eq!(observed.outcome, TerminalAttachmentOutcome::Rollback);
            assert_eq!(observed.transaction_lock_entries, 0);
            assert!(observed.session_active);
            assert_eq!(lock_entry_count(&engine, owner), 0);
            assert!(!session.in_trx().unwrap());
            drop(hook);
        });
    }

    #[test]
    fn test_dropped_terminal_rollback_waiter_completes_worker_cleanup() {
        let _hook_lock = terminal_rollback_hook_test_lock().lock().unwrap();
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_terminal_rollback_cancel").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let trx_id = trx.trx_id();
            let owner = lock_owner(&trx).unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    table_id,
                    vec![Val::from(91_270i32), Val::from("terminal-rollback")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            let (_hook, started_rx, release) =
                install_blocking_terminal_rollback_hook(trx_id, "rollback active transaction");
            let mut rollback = Box::pin(trx.rollback());
            assert!(matches!(
                futures::poll!(rollback.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("terminal rollback worker should start"),
                "rollback active transaction"
            );
            assert_eq!(entry.inspect().state, SessionOperationState::Completing);
            assert!(session.in_trx().unwrap());

            drop(rollback);
            release_terminal_rollback_hook(&release);
            wait_until(
                || {
                    !session.in_trx().unwrap()
                        && entry.inspect().state == SessionOperationState::Terminal
                        && lock_entry_count(&engine, owner) == 0
                },
                "terminal rollback cleanup did not finish after waiter drop",
            );
            engine.shutdown();
        });
    }

    #[test]
    fn test_terminal_rollback_blocks_shutdown_after_waiter_drop() {
        let _hook_lock = terminal_rollback_hook_test_lock().lock().unwrap();
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_terminal_rollback_shutdown").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let trx_id = trx.trx_id();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    table_id,
                    vec![Val::from(91_271i32), Val::from("terminal-shutdown")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();

            let (_hook, started_rx, release) =
                install_blocking_terminal_rollback_hook(trx_id, "rollback active transaction");
            let mut rollback = Box::pin(trx.rollback());
            assert!(matches!(
                futures::poll!(rollback.as_mut()),
                std::task::Poll::Pending
            ));
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("terminal rollback worker should start");
            drop(rollback);

            scope(|scope| {
                let (done_tx, done_rx) = mpsc::channel();
                let shutdown_engine = &engine;
                let shutdown = scope.spawn(move || {
                    shutdown_engine.shutdown();
                    done_tx.send(()).expect("shutdown should report completion");
                });

                assert!(
                    done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                    "shutdown must wait for worker-owned terminal rollback"
                );
                release_terminal_rollback_hook(&release);
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown should finish after rollback cleanup");
                shutdown.join().unwrap();
            });
            assert_eq!(entry.inspect().state, SessionOperationState::Terminal);
            drop(session);
        });
    }

    #[test]
    fn test_duplicate_abandoned_cleanup_cannot_claim_terminal_rollback() {
        let _hook_lock = terminal_rollback_hook_test_lock().lock().unwrap();
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_terminal_rollback_duplicate").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let trx_id = trx.trx_id();
            let operation_key = trx.operation_key;
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    table_id,
                    vec![Val::from(91_272i32), Val::from("terminal-duplicate")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();

            let (_hook, started_rx, release) =
                install_blocking_terminal_rollback_hook(trx_id, "rollback active transaction");
            let mut rollback = Box::pin(trx.rollback());
            assert!(matches!(
                futures::poll!(rollback.as_mut()),
                std::task::Poll::Pending
            ));
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("terminal rollback worker should start");
            assert_eq!(entry.inspect().state, SessionOperationState::Completing);

            let engine_ref = engine.new_ref().unwrap();
            let (duplicate_entry, duplicate_session) = engine_ref
                .session_registry
                .try_resolve_operation(operation_key)
                .expect("rolling-back transaction should remain registry-visible");
            let duplicate_attachment =
                TrxAttachment::new(engine_ref, duplicate_session, operation_key, trx_id);
            assert!(
                SessionOperationCompletionClaim::cleanup(duplicate_entry, duplicate_attachment)
                    .is_err(),
                "abandoned cleanup must not claim a rolling-back terminal transaction"
            );

            drop(rollback);
            release_terminal_rollback_hook(&release);
            wait_until(
                || {
                    !session.in_trx().unwrap()
                        && entry.inspect().state == SessionOperationState::Terminal
                },
                "terminal rollback cleanup did not finish after duplicate cleanup attempt",
            );
            engine.shutdown();
        });
    }

    #[test]
    fn test_dropped_commit_waiter_after_pre_handoff_rollback_still_cleans_up() {
        let _hook_lock = terminal_rollback_hook_test_lock().lock().unwrap();
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_commit_pre_handoff_cancel").await;
            let table_id = catalog_tests::table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let trx_id = trx.trx_id();
            let owner = lock_owner(&trx).unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(
                    table_id,
                    vec![Val::from(91_273i32), Val::from("pre-handoff-rollback")],
                )
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            let (_hook, started_rx, release) =
                install_blocking_terminal_rollback_hook(trx_id, "rollback poisoned commit");
            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));
            let mut commit = Box::pin(trx.commit());
            assert!(matches!(
                futures::poll!(commit.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("poisoned commit rollback worker should start"),
                "rollback poisoned commit"
            );
            assert!(matches!(
                entry.inspect().state,
                SessionOperationState::Completing
            ));

            drop(commit);
            release_terminal_rollback_hook(&release);
            wait_until(
                || {
                    !session.in_trx().unwrap()
                        && entry.inspect().state == SessionOperationState::Terminal
                        && lock_entry_count(&engine, owner) == 0
                },
                "poisoned commit rollback cleanup did not finish after waiter drop",
            );
            engine.shutdown();
        });
    }

    #[test]
    fn test_failed_precommit_cleanup_stops_reverse_after_rollback_failure() {
        smol::block_on(async {
            let (temp_dir, engine) = test_engine_with_mem_size(
                "redo_failed_precommit_reverse_quarantine",
                9 * 1024 * 1024,
            )
            .await;
            let table_id = catalog_tests::table2(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();
            let large = "r".repeat(48 * 1024);

            fn precommit_with_cold_row_undo(
                session: &mut Session,
                table_id: TableID,
                cts: TrxID,
            ) -> PrecommitTrx {
                let mut trx = session.begin_trx().unwrap();
                with_transaction_inner_mut(
                    &mut trx,
                    "test_failed_precommit_retained_cold_row_undo",
                    |inner| {
                        inner.row_undo_mut().push(OwnedRowUndo::new(
                            table_id,
                            None,
                            RowID::new(cts.as_u64()),
                            RowUndoKind::Delete,
                        ));
                    },
                )
                .unwrap();
                prepare_transaction(trx).unwrap().fill_cts(cts)
            }

            let mut session1 = engine.new_session().unwrap();
            let mut session2 = engine.new_session().unwrap();
            let mut session3 = engine.new_session().unwrap();
            let row_id1 = RowID::new(91_261);
            let row_id2 = RowID::new(91_262);
            let precommit1 =
                precommit_with_cold_row_undo(&mut session1, table_id, TrxID::new(91_261));
            let precommit2 =
                precommit_with_cold_row_undo(&mut session2, table_id, TrxID::new(91_262));

            let mut trx3 = session3.begin_trx().unwrap();
            let row_id3 = trx3
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(
                        table_id,
                        vec![Val::from(91_263i32), Val::from(&large[..])],
                    )
                    .await
                })
                .await
                .unwrap();
            let cached_page = session3.load_active_insert_page(table_id).unwrap();
            let precommit3 = prepare_transaction(trx3)
                .unwrap()
                .fill_cts(TrxID::new(91_263));

            let mut writer = engine.new_session().unwrap();
            for i in 0..258 {
                let mut trx = writer.begin_trx().unwrap();
                trx.exec(async |stmt| {
                    stmt.table_insert_mvcc(
                        table_id,
                        vec![Val::from(92_000i32 + i), Val::from(&large[..])],
                    )
                    .await?;
                    Ok(())
                })
                .await
                .unwrap();
                trx.commit().await.unwrap();
                if test_frame_kind(&table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
                    break;
                }
            }
            // Timer audit: buffer-eviction/I/O test coordination.
            let mut evicted = false;
            for _ in 0..20 {
                if test_frame_kind(&table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
                    evicted = true;
                    break;
                }
                Timer::after(Duration::from_millis(50)).await;
            }
            assert!(evicted, "failed-precommit rollback page should be evicted");

            let mem_pool_file =
                StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp")).unwrap();
            let read_hook = Arc::new(FailingPageReadHook::for_page(
                mem_pool_file,
                cached_page.page_id,
                libc::EIO,
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());

            let poison = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));
            let completion = Arc::new(Completion::new());
            let mut job = FailedPrecommitCleanupJob::new(
                vec![precommit1, precommit2, precommit3],
                Arc::clone(&completion),
                FailedPrecommitReason::Fatal(poison),
            );

            job.run().await;

            assert!(
                read_hook.call_count() > 0,
                "latest precommit rollback should reload the evicted row page"
            );
            assert_eq!(
                fatal_rollback_retention_count(&engine.inner().trx_sys),
                3,
                "reverse cleanup should fail on the newest transaction first and retain older unprocessed payloads"
            );
            for row_id in [row_id1, row_id2, row_id3] {
                assert!(
                    retains_precommit_row_undo(&engine.inner().trx_sys, table_id, row_id),
                    "failed-precommit fatal retention must own row undo: table_id={table_id}, row_id={row_id}"
                );
            }
            assert!(!session1.in_trx().unwrap());
            assert!(!session2.in_trx().unwrap());
            assert!(!session3.in_trx().unwrap());
            assert!(completion.wait_result().await.is_err_and(|err| {
                err.downcast_ref::<FatalError>()
                    .is_some_and(|reason| *reason == FatalError::RedoWrite)
            }));
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite),
                "the initiating redo failure must remain the first stored poison reason"
            );

            let err = match session1.begin_trx() {
                Ok(_) => panic!("poisoned storage must reject new transactions"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite)
            );
        });
    }

    #[test]
    fn test_commit_and_rollback_after_fatal_discard_return_error() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_discard_errors").await;

            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            assert!(
                !session_has_public_trx_cache(&engine.inner().session_registry, session_id),
                "fatal rollback must not recycle the failed transaction core"
            );
            let replacement_err = match session.begin_trx() {
                Ok(_) => panic!("failed-retained operation must block session reuse"),
                Err(err) => err,
            };
            assert_existing_transaction_error(
                &replacement_err,
                session_id,
                trx.trx_id(),
                "failed_retained",
            );
            let err = match prepare_transaction(trx) {
                Ok(_) => panic!("discarded transaction prepare should fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert!(err.report().downcast_ref::<InternalError>().is_none());
            remove_session_for_test(&engine.inner().session_registry, session_id);

            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement_err = match session.begin_trx() {
                Ok(_) => panic!("failed-retained operation must block session reuse"),
                Err(err) => err,
            };
            assert_existing_transaction_error(
                &replacement_err,
                session_id,
                trx.trx_id(),
                "failed_retained",
            );
            let err = trx.commit().await.unwrap_err();
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert!(err.report().downcast_ref::<InternalError>().is_none());
            remove_session_for_test(&engine.inner().session_registry, session_id);

            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement_err = match session.begin_trx() {
                Ok(_) => panic!("failed-retained operation must block session reuse"),
                Err(err) => err,
            };
            assert_existing_transaction_error(
                &replacement_err,
                session_id,
                trx.trx_id(),
                "failed_retained",
            );
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert!(err.report().downcast_ref::<InternalError>().is_none());
            remove_session_for_test(&engine.inner().session_registry, session_id);
        });
    }

    #[test]
    fn test_transaction_effect_predicates_split_durability_from_ordering() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_predicates").await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            with_transaction_inner_mut(&mut trx, "test_index_undo_predicate", |inner| {
                inner.index_undo_mut().push(IndexUndo {
                    table_id: TableID::new(47),
                    row_id: RowID::new(1),
                    kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
                });
            })
            .unwrap();
            assert!(!transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            let prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            discard_production_prepared_for_test(prepared);

            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            assert!(transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            let prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            discard_production_prepared_for_test(prepared);
        });
    }

    #[test]
    fn test_published_table_root_retention_waits_for_fence_horizon() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("published_root_retention_fence").await;
            let table_file = publish_initial_test_root(&engine, 91_001).await;
            let old_root_ptr = table_file.active_root_unchecked() as *const _ as usize;
            let drop_count_before = old_root_drop_count(old_root_ptr);

            let mut session = engine.new_session().unwrap();
            let read_trx = session.begin_trx().unwrap();
            let mutable = MutableTableFile::fork(
                &table_file,
                engine.inner().table_fs.background_writes(),
                engine.inner().disk_pool.clone_inner(),
            );
            let table_file = engine
                .inner()
                .trx_sys
                .publish_table_file_root(mutable, TrxID::new(2), false)
                .await
                .unwrap();
            assert_eq!(table_file.active_root_unchecked().root_ts, TrxID::new(2));
            let retained_root_fence = table_file.active_root_unchecked().effective_ts();

            // Timer audit: bounded negative assertion while an active reader pins the root.
            for _ in 0..10 {
                Timer::after(Duration::from_millis(10)).await;
                assert_eq!(
                    old_root_drop_count(old_root_ptr),
                    drop_count_before,
                    "old root must stay retained while an earlier transaction is active"
                );
            }
            read_trx.commit().await.unwrap();
            session
                .wait_for_purge_completion_after(retained_root_fence)
                .await
                .unwrap();
            assert!(old_root_drop_count(old_root_ptr) > drop_count_before);
        });
    }
}
