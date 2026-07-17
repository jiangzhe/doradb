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

pub(crate) use sys_trx::{RetiredRowPageBatch, SysTrxPayload};

use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableCache, is_catalog_table};
use crate::engine::{EngineRef, WeakEngineRef};
use crate::error::{
    CompletionErrorKind, Error, FatalError, InternalError, InternalResult, LifecycleError,
    LifecycleResult, OperationError, ResourceError, Result,
};
use crate::id::{SessionID, TableID, TrxID};
use crate::io::Completion;
use crate::lock::{
    FreshLockGuard, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource, OwnerLockState,
    StmtNo,
};
use crate::log::block_group::TrxLog;
use crate::log::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind};
use crate::map::FastHashMap;
use crate::notify::EventNotifyOnDrop;
use crate::quiescent::QuiescentGuard;
use crate::session::TrxAttachment;
use crate::table::Table;
use crate::trx::undo::{IndexPurgeEntry, IndexUndoLogs, RowUndoHead, RowUndoLogs, UndoStatus};
use error_stack::{Report, ResultExt};
use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem;
use std::ops::AsyncFnOnce;
use std::ptr::addr_eq;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

pub use stmt::Statement;
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

/// Public active transaction facade.
pub struct Transaction {
    trx_id: TrxID,
    sts: TrxID,
    session_id: SessionID,
    engine: WeakEngineRef,
    terminal_started: bool,
}

impl Transaction {
    /// Create a weak public transaction facade for a stable session entry.
    #[inline]
    pub(crate) fn new(
        engine: WeakEngineRef,
        session_id: SessionID,
        trx_id: TrxID,
        sts: TrxID,
    ) -> Self {
        Transaction {
            trx_id,
            sts,
            session_id,
            engine,
            terminal_started: false,
        }
    }

    /// Resolve this handle and build an operation-local runtime attachment.
    #[inline]
    fn resolve_active(&self) -> LifecycleResult<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade().attach_with(|| {
            format!(
                "session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                self.session_id, self.trx_id
            )
        })?;
        let admission = engine
            .acquire_admission()
            .attach_with(|| format!("session_id={}, trx_id={}", self.session_id, self.trx_id))?;
        let (entry, session) = engine
            .session_registry
            .resolve_trx(self.session_id, self.trx_id)?;
        drop(admission);
        let attachment = TrxAttachment::new(engine, session, self.trx_id);
        Ok((entry, attachment))
    }

    /// Resolve this handle for terminal or cleanup paths.
    #[inline]
    fn resolve_terminal(&self) -> LifecycleResult<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade_for_terminal().attach_with(|| {
            format!(
                "session_id={}, trx_id={}, phase=upgrade_engine_runtime",
                self.session_id, self.trx_id
            )
        })?;
        self.resolve_with_engine(engine)
    }

    #[inline]
    fn resolve_with_engine(
        &self,
        engine: EngineRef,
    ) -> LifecycleResult<(Arc<TrxEntry>, TrxAttachment)> {
        let (entry, session) = engine
            .session_registry
            .resolve_trx(self.session_id, self.trx_id)?;
        let attachment = TrxAttachment::new(engine, session, self.trx_id);
        Ok((entry, attachment))
    }

    /// Check out the mutable core for one crate-internal operation.
    #[inline]
    pub(crate) fn checkout(&mut self) -> LifecycleResult<TrxCheckout> {
        let (entry, attachment) = self.resolve_active()?;
        TrxCheckout::new(entry, attachment)
    }

    /// Claim this transaction for an explicit terminal operation.
    #[inline]
    pub(crate) fn claim_terminal(
        &self,
        state: TrxEntryState,
    ) -> LifecycleResult<TrxCompletionClaim> {
        let (entry, attachment) = self.resolve_terminal()?;
        TrxCompletionClaim::terminal(entry, attachment, state)
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
    pub async fn lock_table(&mut self, table_id: TableID, mode: LockMode) -> Result<()> {
        let mut checkout = self.checkout().attach("operation=lock_explicit_table")?;
        checkout.lock_table(table_id, mode).await
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
    /// transaction-owned. Once the returned future has been polled, callers
    /// must drive it to completion; dropping a non-empty statement future is
    /// detected by the `StmtEffects` leak guard rather than repaired by async
    /// rollback from `Drop`.
    #[inline]
    pub async fn exec<T, F>(&mut self, f: F) -> Result<T>
    where
        F: for<'borrow> AsyncFnOnce(&'borrow mut Statement<'_>) -> Result<T>,
    {
        let mut checkout = self.checkout().attach("operation=execute_statement")?;
        let trx_id = checkout.inner().trx_id();
        let stmt_no = checkout
            .inner_mut()
            .next_stmt_no()
            .attach("operation=execute_statement")?;
        let stmt_owner = LockOwner::Statement(trx_id, stmt_no);
        enum ExecOutcome<T> {
            Success(T),
            StatementError(Error),
            FatalRollback(Error),
        }
        let outcome = {
            let (inner, attachment) = checkout.inner_and_attachment_mut();
            let mut stmt = Statement::new(inner, attachment, stmt_owner)
                .attach("operation=execute_statement")?;
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
            ExecOutcome::Success(value) => Ok(value),
            ExecOutcome::StatementError(err) => Err(err),
            ExecOutcome::FatalRollback(err) => {
                checkout.discard_after_fatal_rollback();
                Err(err)
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
                    trx.session_id, trx.trx_id
                )
            })?;
        let claim = trx
            .claim_terminal(TrxEntryState::Committing)
            .attach("operation=commit_active_transaction")?;
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
                    trx.session_id, trx.trx_id
                )
            })?;
        let claim = trx
            .claim_terminal(TrxEntryState::RollingBack)
            .attach("operation=rollback_active_transaction")?;
        engine.trx_sys.rollback_transaction(claim).await
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
                .abandon_trx_handle(self.session_id, self.trx_id);
            if abandoned {
                engine.trx_sys.request_abandoned_trx_cleanup(
                    engine.clone(),
                    self.session_id,
                    self.trx_id,
                    TrxCleanupReason::HandleDrop,
                );
            }
        }
    }
}

/// Transaction begin result with separate public handle and registry entry.
pub(crate) struct StartedTransaction {
    /// Public transaction handle returned to the session.
    pub(crate) handle: Transaction,
    /// Stable session-registry entry for the mutable transaction core.
    pub(crate) entry: Arc<TrxEntry>,
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
    /// Create a new shared transaction status for given transaction id.
    #[inline]
    pub(crate) fn new(trx_id: TrxID) -> Self {
        let terminal = trx_is_committed(trx_id);
        SharedTrxStatus {
            ts: AtomicU64::new(trx_id.as_u64()),
            preparing: AtomicBool::new(false),
            prepare_ev: Mutex::new(None),
            terminal: AtomicBool::new(terminal),
            terminal_ev: Event::new(),
        }
    }

    /// Returns the timestamp of current transaction.
    #[inline]
    pub(crate) fn ts(&self) -> TrxID {
        TrxID::new(self.ts.load(Ordering::Acquire))
    }

    /// Publishes a committed status for deterministic undo-marker tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn commit_for_test(&self, cts: TrxID) {
        debug_assert!(trx_is_committed(cts));
        self.ts.store(cts.as_u64(), Ordering::SeqCst);
        self.finish_terminal();
    }

    /// Publishes successful rollback resolution for deterministic status tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn rollback_for_test(&self) {
        self.finish_terminal();
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

    /// Returns notifier if the transaction is in prepare phase.
    ///
    /// Preparing means commit ordering has started but the transaction has not
    /// reached its terminal commit or failed-precommit rollback outcome.
    /// Waiters must wake for either terminal result and recheck the shared
    /// transaction status.
    #[inline]
    pub(crate) fn prepare_listener(&self) -> Option<EventListener> {
        let g = self.prepare_ev.lock();
        if self.preparing.load(Ordering::Acquire) {
            g.as_ref().map(|event| event.listen())
        } else {
            None
        }
    }

    /// Marks the transaction as preparing and installs a completion notifier.
    #[inline]
    fn mark_preparing(&self) {
        let mut g = self.prepare_ev.lock();
        debug_assert!(
            !self.preparing.load(Ordering::Acquire),
            "transaction is already preparing"
        );
        debug_assert!(g.is_none(), "prepare notifier should not be installed");
        *g = Some(EventNotifyOnDrop::new());
        self.preparing.store(true, Ordering::SeqCst);
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
    /// Create a transaction context from session, status, and GC bucket identity.
    #[inline]
    pub(crate) fn new(trx_id: TrxID, sts: TrxID, gc_no: usize) -> Self {
        TrxContext {
            status: Arc::new(SharedTrxStatus::new(trx_id)),
            sts,
            gc_no,
        }
    }

    /// Returns a clone of the shared transaction status handle.
    #[inline]
    pub(crate) fn status(&self) -> Arc<SharedTrxStatus> {
        self.status.clone()
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

/// Operation-local transaction runtime view.
///
/// `TrxRuntime` pairs immutable MVCC identity with the checked-out operation's
/// runtime attachment. It is borrowed from scoped foreground statement or
/// terminal work; checked-in transaction state never stores it.
#[derive(Clone, Copy)]
pub(crate) struct TrxRuntime<'r> {
    ctx: &'r TrxContext,
    attachment: &'r TrxAttachment,
}

impl<'r> TrxRuntime<'r> {
    /// Create an operation-local runtime view.
    #[inline]
    pub(crate) fn new(ctx: &'r TrxContext, attachment: &'r TrxAttachment) -> Self {
        Self { ctx, attachment }
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

    /// Returns a clone of the shared transaction status handle.
    #[inline]
    pub(crate) fn status(&self) -> Arc<SharedTrxStatus> {
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
    pub(crate) fn read_proof(&self) -> TrxReadProof<'_> {
        self.ctx.read_proof()
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
            let resource = LockResource::TableData(table_id);
            let owner = LockOwner::Transaction(self.ctx.trx_id());
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

/// Registry-visible lifecycle for a session-owned transaction entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TrxEntryState {
    /// The mutable transaction core is checked in and available for work.
    Active = 0,
    /// One operation has checked out the mutable transaction core.
    CheckedOut = 1,
    /// One operation has checked out the core after the public handle was dropped.
    CheckedOutAbandoned = 2,
    /// Commit has consumed the mutable core and is preparing or queued.
    Committing = 3,
    /// Rollback has consumed the mutable core and is running undo cleanup.
    RollingBack = 4,
    /// The public transaction handle was dropped and cleanup may claim rollback.
    Abandoned = 5,
    /// Abandoned cleanup has consumed the mutable core and is rolling back.
    CleanupRunning = 6,
    /// The transaction reached a terminal state and the session entry was cleared.
    Terminal = 7,
    /// Fatal cleanup made the transaction impossible to reuse.
    Failed = 8,
}

impl TrxEntryState {
    #[inline]
    fn to_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            value if value == TrxEntryState::Active as u8 => TrxEntryState::Active,
            value if value == TrxEntryState::CheckedOut as u8 => TrxEntryState::CheckedOut,
            value if value == TrxEntryState::CheckedOutAbandoned as u8 => {
                TrxEntryState::CheckedOutAbandoned
            }
            value if value == TrxEntryState::Committing as u8 => TrxEntryState::Committing,
            value if value == TrxEntryState::RollingBack as u8 => TrxEntryState::RollingBack,
            value if value == TrxEntryState::Abandoned as u8 => TrxEntryState::Abandoned,
            value if value == TrxEntryState::CleanupRunning as u8 => TrxEntryState::CleanupRunning,
            value if value == TrxEntryState::Terminal as u8 => TrxEntryState::Terminal,
            value if value == TrxEntryState::Failed as u8 => TrxEntryState::Failed,
            _ => panic!("invalid transaction entry state: {value}"),
        }
    }

    /// Returns whether this state still blocks the owning session as in-transaction.
    #[inline]
    pub(crate) fn in_trx(self) -> bool {
        !matches!(self, TrxEntryState::Terminal | TrxEntryState::Failed)
    }

    /// Returns the stable snake_case attachment label for this state.
    #[inline]
    pub(crate) fn label(self) -> &'static str {
        match self {
            TrxEntryState::Active => "active",
            TrxEntryState::CheckedOut => "checked_out",
            TrxEntryState::CheckedOutAbandoned => "checked_out_abandoned",
            TrxEntryState::Committing => "committing",
            TrxEntryState::RollingBack => "rolling_back",
            TrxEntryState::Abandoned => "abandoned",
            TrxEntryState::CleanupRunning => "cleanup_running",
            TrxEntryState::Terminal => "terminal",
            TrxEntryState::Failed => "failed",
        }
    }
}

/// Session-owned stable transaction entry.
///
/// The entry remains visible through session lifecycle state while the mutable
/// core is checked out, committing, rolling back, terminal, or failed. It never
/// stores a strong engine runtime handle; operation-scoped runtime reachability
/// is attached only while a [`TrxCheckout`] owns the checked-out core.
pub(crate) struct TrxEntry {
    trx_id: TrxID,
    /// Lock-free registry-visible state for session queries and diagnostics.
    ///
    /// Writers that change whether `inner` is present must hold `inner`'s mutex
    /// and then publish this state with `Release`. Readers use `Acquire`; they
    /// may observe a concurrent transition, but must never observe `Active`
    /// unless `inner` has already been restored.
    state: AtomicU8,
    /// Checked-in mutable transaction body.
    ///
    /// The mutex serializes ownership transfer of `TrxInner`. State transitions
    /// that remove or restore this slot must update `state` in the order
    /// documented on `checkout` and `return_inner`.
    inner: Mutex<Option<TrxInner>>,
}

impl TrxEntry {
    /// Create a stable transaction entry for a checked-in mutable core.
    #[inline]
    pub(crate) fn new(inner: TrxInner) -> Arc<Self> {
        let trx_id = inner.trx_id();
        Arc::new(Self {
            trx_id,
            state: AtomicU8::new(TrxEntryState::Active.to_u8()),
            inner: Mutex::new(Some(inner)),
        })
    }

    /// Returns the transaction id for this stable entry.
    #[inline]
    pub(crate) fn trx_id(&self) -> TrxID {
        self.trx_id
    }

    /// Returns a lock-free snapshot of the current registry-visible state.
    ///
    /// This is an Acquire-loaded observation only. It must not be used as a
    /// stable TOCTOU guard unless the caller also holds the entry lock, or has
    /// another ownership/lifecycle guard that prevents the relevant transition.
    #[inline]
    pub(crate) fn inspect_state(&self) -> TrxEntryState {
        TrxEntryState::from_u8(self.state.load(Ordering::Acquire))
    }

    #[inline]
    fn store_state(&self, state: TrxEntryState) {
        self.state.store(state.to_u8(), Ordering::Release);
    }

    /// Returns whether this entry still represents an active session transaction.
    #[inline]
    pub(crate) fn in_trx(&self) -> bool {
        self.inspect_state().in_trx()
    }

    #[inline]
    fn take_for_checkout(&self) -> LifecycleResult<TrxInner> {
        self.take_inner(TrxEntryState::CheckedOut)
    }

    #[inline]
    fn take_for_terminal(&self, state: TrxEntryState) -> LifecycleResult<TrxInner> {
        debug_assert!(matches!(
            state,
            TrxEntryState::Committing | TrxEntryState::RollingBack
        ));
        self.take_inner(state)
    }

    #[inline]
    fn take_for_cleanup(&self) -> LifecycleResult<TrxInner> {
        self.take_inner_from_state(TrxEntryState::Abandoned, TrxEntryState::CleanupRunning)
    }

    #[inline]
    fn take_inner(&self, next_state: TrxEntryState) -> LifecycleResult<TrxInner> {
        self.take_inner_from_state(TrxEntryState::Active, next_state)
    }

    #[inline]
    fn take_inner_from_state(
        &self,
        expected_state: TrxEntryState,
        next_state: TrxEntryState,
    ) -> LifecycleResult<TrxInner> {
        let mut inner_slot = self.inner.lock();
        // The state snapshot is stable for this transition because we hold the
        // inner mutex that serializes state/inner ownership changes.
        let state = self.inspect_state();
        if state != expected_state {
            return Err(transaction_entry_state_err(self.trx_id, state));
        }
        if inner_slot.is_none() {
            return Err(transaction_entry_state_err(self.trx_id, expected_state));
        }
        // Publish the unavailable state before taking the inner. This prevents
        // lock-free state readers from seeing an available state while the
        // checked-in body is absent or terminal/cleanup ownership has started.
        self.store_state(next_state);
        let inner = inner_slot
            .take()
            .expect("transaction entry expected checked-in inner");
        Ok(inner)
    }

    #[inline]
    fn return_inner(&self, inner: TrxInner) -> TrxEntryState {
        let mut inner_slot = self.inner.lock();
        let state = self.inspect_state();
        debug_assert!(matches!(
            state,
            TrxEntryState::CheckedOut | TrxEntryState::CheckedOutAbandoned
        ));
        debug_assert!(inner_slot.is_none());
        // Restore the inner before publishing an available state. This preserves
        // the invariant that any observed Active or Abandoned state has a
        // checked-in body.
        *inner_slot = Some(inner);
        let next_state = match state {
            TrxEntryState::CheckedOut => TrxEntryState::Active,
            TrxEntryState::CheckedOutAbandoned => TrxEntryState::Abandoned,
            _ => unreachable!("checked above"),
        };
        self.store_state(next_state);
        next_state
    }

    /// Mark this transaction as abandoned by a dropped public handle.
    #[inline]
    pub(crate) fn abandon(&self) -> bool {
        let _inner_slot = self.inner.lock();
        match self.inspect_state() {
            TrxEntryState::Active => {
                self.store_state(TrxEntryState::Abandoned);
                true
            }
            TrxEntryState::CheckedOut => {
                self.store_state(TrxEntryState::CheckedOutAbandoned);
                true
            }
            TrxEntryState::CheckedOutAbandoned | TrxEntryState::Abandoned => true,
            TrxEntryState::Committing
            | TrxEntryState::RollingBack
            | TrxEntryState::CleanupRunning
            | TrxEntryState::Terminal
            | TrxEntryState::Failed => false,
        }
    }

    /// Publish an in-progress terminal or failed state after the core was consumed.
    #[inline]
    pub(crate) fn publish_state(&self, state: TrxEntryState) {
        let inner_slot = self.inner.lock();
        debug_assert!(
            inner_slot.is_none(),
            "in-progress transaction states must not keep checked-in mutable state"
        );
        // Terminal-running states are only published after checkout consumed
        // the inner, so a Release store is enough for lock-free state readers.
        self.store_state(state);
    }

    /// Clear any checked-in core and publish a terminal session-visible state.
    #[inline]
    pub(crate) fn finish(&self, state: TrxEntryState) {
        debug_assert!(matches!(
            state,
            TrxEntryState::Terminal | TrxEntryState::Failed
        ));
        let mut inner_slot = self.inner.lock();
        debug_assert!(
            inner_slot.is_none() || state == TrxEntryState::Failed,
            "terminal transaction completion should consume the mutable core first"
        );
        inner_slot.take();
        // Clear the checked-in body before publishing the terminal state.
        self.store_state(state);
    }
}

/// Private RAII checkout for one non-terminal transaction operation.
///
/// `TrxCheckout` is mechanical ownership plumbing: it moves [`TrxInner`] out of
/// the stable entry, owns the operation-local runtime attachment, and restores
/// the core on ordinary drop. Statement semantics live in [`Statement`];
/// terminal commit and rollback use private completion claims.
pub(crate) struct TrxCheckout {
    entry: Arc<TrxEntry>,
    inner: Option<TrxInner>,
    attachment: TrxAttachment,
}

impl TrxCheckout {
    #[inline]
    fn new(entry: Arc<TrxEntry>, attachment: TrxAttachment) -> LifecycleResult<Self> {
        let inner = entry.take_for_checkout()?;
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
            .expect("TrxCheckout always owns an inner until fatal discard")
    }

    /// Returns this checkout's mutable transaction core.
    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut TrxInner {
        self.inner
            .as_mut()
            .expect("TrxCheckout always owns an inner until fatal discard")
    }

    /// Returns mutable transaction state and the operation-local attachment.
    #[inline]
    pub(crate) fn inner_and_attachment_mut(&mut self) -> (&mut TrxInner, &TrxAttachment) {
        let inner = self
            .inner
            .as_mut()
            .expect("TrxCheckout always owns an inner until fatal discard");
        (inner, &self.attachment)
    }

    /// Returns this checkout's operation-local attachment.
    #[inline]
    pub(crate) fn attachment(&self) -> &TrxAttachment {
        &self.attachment
    }

    /// Acquires an explicit transaction-lifetime table lock.
    #[inline]
    pub(crate) async fn lock_table(&mut self, table_id: TableID, mode: LockMode) -> Result<()> {
        let Self {
            inner, attachment, ..
        } = self;
        let inner = inner
            .as_mut()
            .expect("TrxCheckout always owns an inner until fatal discard");
        inner.lock_table(attachment, table_id, mode).await
    }

    /// Clear a fatally discarded inner and leave the entry impossible to reuse.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(&mut self) {
        self.entry.publish_state(TrxEntryState::Failed);
        if let Some(mut inner) = self.inner.take() {
            let retention = inner.retain_and_discard_after_fatal_rollback(&self.attachment);
            self.attachment
                .engine()
                .trx_sys
                .retain_fatal_rollback(retention);
        }
        self.entry.finish(TrxEntryState::Failed);
    }
}

impl Drop for TrxCheckout {
    #[inline]
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let state = self.entry.return_inner(inner);
        if state == TrxEntryState::Abandoned {
            self.attachment
                .engine()
                .session_registry
                .notify_trx_changed();
            self.attachment
                .request_abandoned_cleanup(TrxCleanupReason::HandleDrop);
        }
    }
}

/// Private ownership claim for explicit terminal and cleanup paths.
pub(crate) struct TrxCompletionClaim {
    entry: Arc<TrxEntry>,
    inner: TrxInner,
    attachment: TrxAttachment,
}

impl TrxCompletionClaim {
    #[inline]
    fn terminal(
        entry: Arc<TrxEntry>,
        attachment: TrxAttachment,
        state: TrxEntryState,
    ) -> LifecycleResult<Self> {
        let inner = entry.take_for_terminal(state)?;
        Ok(Self {
            entry,
            inner,
            attachment,
        })
    }

    /// Claim an abandoned checked-in transaction for cleanup rollback.
    #[inline]
    pub(crate) fn cleanup(
        entry: Arc<TrxEntry>,
        attachment: TrxAttachment,
    ) -> LifecycleResult<Self> {
        let inner = entry.take_for_cleanup()?;
        Ok(Self {
            entry,
            inner,
            attachment,
        })
    }

    /// Consume and return the claimed stable entry, mutable core, and attachment.
    #[inline]
    pub(crate) fn into_parts(self) -> (Arc<TrxEntry>, TrxInner, TrxAttachment) {
        (self.entry, self.inner, self.attachment)
    }

    /// Returns the engine retained by this terminal or cleanup claim.
    #[inline]
    pub(crate) fn engine(&self) -> &EngineRef {
        self.attachment.engine()
    }
}

/// Reason an abandoned transaction cleanup job was queued.
///
/// Abandoned cleanup is the recovery path for a transaction whose public handle
/// disappeared before an explicit terminal operation completed. It is not the
/// durability-critical precommit path: cleanup claims the still-checked-in
/// mutable transaction core and rolls it back as an ordinary active
/// transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrxCleanupReason {
    /// The public transaction handle was dropped.
    HandleDrop,
    /// Engine shutdown found an abandoned transaction.
    ShutdownDrain,
}

/// Abandoned transaction cleanup job.
///
/// The job carries an `EngineRef`, so queued cleanup pins the runtime until the
/// worker has either claimed and rolled back the abandoned transaction or found
/// that the session/transaction is no longer claimable. Engine shutdown keeps
/// scanning abandoned sessions and waits for active transaction state to reach a
/// terminal state before component teardown begins.
pub(crate) struct TrxCleanupJob {
    /// Engine retained until cleanup resolves this job.
    pub(crate) engine: EngineRef,
    /// Session that owned the abandoned transaction.
    pub(crate) session_id: SessionID,
    /// Transaction id to resolve in the session registry.
    pub(crate) trx_id: TrxID,
    /// Reason this cleanup was queued.
    pub(crate) reason: TrxCleanupReason,
}

/// Reason a precommit transaction has to rollback instead of commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailedPrecommitReason {
    /// Redo write, submit, or sync failed after precommit handoff.
    Fatal(FatalError),
    /// A single precommit group exceeded an intrinsic redo resource limit.
    Resource(ResourceError),
    /// Group commit admission closed for engine shutdown.
    Shutdown,
}

impl FailedPrecommitReason {
    /// Convert this failed-precommit reason into an operation error.
    #[inline]
    pub(crate) fn into_error(self, message: impl Into<String>) -> Error {
        match self {
            FailedPrecommitReason::Fatal(reason) => {
                Report::new(reason).attach(message.into()).into()
            }
            FailedPrecommitReason::Resource(reason) => {
                Report::new(reason).attach(message.into()).into()
            }
            FailedPrecommitReason::Shutdown => Report::new(LifecycleError::Shutdown)
                .attach(message.into())
                .into(),
        }
    }

    #[inline]
    fn completion_report(self, message: impl Into<String>) -> Report<CompletionErrorKind> {
        match self {
            FailedPrecommitReason::Fatal(reason) => {
                CompletionErrorKind::from_fatal(Report::new(reason), message)
            }
            FailedPrecommitReason::Resource(reason) => {
                CompletionErrorKind::from_resource(Report::new(reason), message)
            }
            FailedPrecommitReason::Shutdown => {
                CompletionErrorKind::from_lifecycle(Report::new(LifecycleError::Shutdown), message)
            }
        }
    }
}

/// Failed-precommit rollback job owned by the transaction cleanup worker.
///
/// This path is mandatory for user transactions. Once a transaction enters
/// precommit, row/index changes may already be installed in in-memory MVCC
/// structures, while redo durability can still fail. Dropping the precommit
/// payload without rollback can leave row-version links pointing at
/// transaction-owned undo memory, so failed-precommit cleanup must run before
/// the shared completion wakes commit waiters.
///
/// The cleanup worker owns these jobs instead of the log thread so redo
/// failure handling can hand over rollback work and continue draining/shutting
/// down its own queues. Transaction-system worker shutdown joins the log
/// thread before sending cleanup `Stop`, which keeps the cleanup receiver
/// alive for every failed-precommit job that the log thread can produce.
pub(crate) struct FailedPrecommitCleanupJob {
    trx_list: Vec<PrecommitTrx>,
    completion: Arc<Completion<()>>,
    reason: FailedPrecommitReason,
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
            reason,
        }
    }

    #[inline]
    async fn run(mut self) {
        while let Some(trx) = self.trx_list.pop() {
            if trx.rollback_failed_precommit().await {
                continue;
            }
            while let Some(trx) = self.trx_list.pop() {
                trx.fail_failed_precommit_without_rollback();
            }
            break;
        }
        // Waiters must observe the original redo/shutdown failure only after
        // rollback has released MVCC undo ownership, transaction locks, session
        // state, and prepare waiters.
        self.completion.complete(Err(self.reason.completion_report(
            "fail redo group commit waiters after failed precommit rollback",
        )));
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

/// Checked-out mutable transaction core.
pub(crate) struct TrxInner {
    ctx: TrxContext,
    effects: TrxEffects,
    table_cache: FastHashMap<TableID, Weak<Table>>,
    lock_state: Option<OwnerLockState>,
    next_stmt_no: StmtNo,
    active: bool,
}

impl TrxInner {
    /// Create a checked-in mutable transaction core.
    #[inline]
    pub(crate) fn new(trx_id: TrxID, sts: TrxID, gc_no: usize, session_id: SessionID) -> Self {
        let owner_group = LockOwnerGroup::Session(session_id);
        TrxInner {
            ctx: TrxContext::new(trx_id, sts, gc_no),
            effects: TrxEffects::empty(),
            table_cache: FastHashMap::default(),
            lock_state: Some(OwnerLockState::new_grouped(
                LockOwner::Transaction(trx_id),
                owner_group,
            )),
            next_stmt_no: 1,
            active: true,
        }
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

    // TODO(backlog 000159): prove whether active transaction attachment is
    // impossible under every cleanup/fatal schedule before replacing this
    // recoverable internal error with an assertion.
    #[inline]
    fn checked_engine(&self, attachment: &TrxAttachment) -> InternalResult<EngineRef> {
        if !self.active {
            return Err(Report::new(InternalError::ActiveTransactionDiscarded));
        }
        Ok(attachment.engine().clone())
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
    fn checked_lock_state(&self) -> LifecycleResult<&OwnerLockState> {
        if !self.active {
            return Err(
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "trx_id={}, reason=transaction_inactive",
                    self.trx_id()
                )),
            );
        }
        self.lock_state.as_ref().ok_or_else(|| {
            Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                "trx_id={}, reason=lock_state_missing",
                self.trx_id()
            ))
        })
    }

    #[inline]
    fn checked_lock_state_mut(&mut self) -> LifecycleResult<&mut OwnerLockState> {
        if !self.active {
            return Err(
                Report::new(LifecycleError::TransactionDiscarded).attach(format!(
                    "trx_id={}, reason=transaction_inactive",
                    self.trx_id()
                )),
            );
        }
        let trx_id = self.trx_id();
        self.lock_state.as_mut().ok_or_else(|| {
            Report::new(LifecycleError::TransactionDiscarded)
                .attach(format!("trx_id={trx_id}, reason=lock_state_missing"))
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
    fn next_stmt_no(&mut self) -> InternalResult<StmtNo> {
        let stmt_no = self.next_stmt_no;
        self.next_stmt_no = self.next_stmt_no.checked_add(1).ok_or_else(|| {
            Report::new(InternalError::StatementNumberOverflow)
                .attach(format!("trx_id={}", self.trx_id()))
        })?;
        Ok(stmt_no)
    }

    /// Upgrade a cached user-table runtime if this transaction weak cache can still reach it.
    #[inline]
    pub(crate) fn cached_user_table(&mut self, table_id: TableID) -> Option<Arc<Table>> {
        let weak = self.table_cache.get(&table_id)?;
        match weak.upgrade() {
            Some(table) => Some(table),
            None => {
                self.table_cache.remove(&table_id);
                None
            }
        }
    }

    /// Remember a successfully resolved user-table runtime without extending its lifetime.
    #[inline]
    pub(crate) fn cache_user_table(&mut self, table: &Arc<Table>) {
        self.table_cache
            .insert(table.table_id(), Arc::downgrade(table));
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
    ) -> Result<()> {
        let operation = "lock_explicit_table";
        mode.validate_explicit_table_lock()
            .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;
        let engine = self.checked_engine(attachment).attach_with(|| {
            format!("operation={operation}, table_id={table_id}, phase=resolve_transaction_engine")
        })?;
        engine
            .catalog()
            .validate_user_table_live(table_id)
            .await
            .attach_with(|| format!("operation={operation}"))?;

        let lock_manager = engine.lock_manager();
        let resources = Self::table_lock_resources(table_id);
        let lock_state = self
            .checked_lock_state()
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| {
                format!("operation={operation}, table_id={table_id}, phase=inspect_lock_state")
            })?;
        let owner = lock_state.owner();
        let cache = TrxTableLockCache {
            metadata_cached: lock_state
                .cached_covers(resources.metadata, LockMode::Shared)
                .attach_with(|| format!("operation={operation}, table_id={table_id}"))?,
            data_cached: lock_state
                .cached_covers(resources.data, mode)
                .attach_with(|| format!("operation={operation}, table_id={table_id}"))?,
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

        let lock_state = self
            .checked_lock_state_mut()
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| {
                format!("operation={operation}, table_id={table_id}, phase=cache_lock_grants")
            })?;
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
    pub(crate) fn release_transaction_locks(&mut self, attachment: &TrxAttachment) -> usize {
        if !self.active {
            return 0;
        }
        self.lock_state.as_mut().map_or(0, |lock_state| {
            lock_state.release_all(attachment.engine().lock_manager())
        })
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

    /// Rolls back the attached session without taking the attachment.
    #[inline]
    pub(crate) fn finish_session_rollback(&mut self, attachment: &TrxAttachment) {
        attachment.rollback();
        self.active = false;
    }

    /// Retain remaining effects and roll back the attached session after rollback failure.
    #[inline]
    pub(in crate::trx) fn retain_and_discard_after_fatal_rollback(
        &mut self,
        attachment: &TrxAttachment,
    ) -> FatalRollbackRetention {
        let retention = self.effects.take_for_fatal_retention();
        self.release_transaction_locks(attachment);
        self.finish_session_rollback(attachment);
        retention
    }

    /// Prepare current transaction for committing.
    #[inline]
    fn prepare(mut self, attachment: TrxAttachment) -> Result<PreparedTrx> {
        if !self.active {
            return Err(Report::new(InternalError::ActiveTransactionDiscarded)
                .attach("operation=prepare_active_transaction")
                .into());
        }
        // fast path for readonly transactions
        if !self.require_ordered_commit() {
            let lock_manager = self.clone_lock_manager_guard(&attachment);
            // there should be no ref count of transaction status.
            debug_assert!(Arc::strong_count(&self.ctx.status) == 1);
            debug_assert!(self.effects.index_undo.is_empty());
            return Ok(PreparedTrx {
                redo_bin: None,
                payload: Some(PreparedTrxPayload::User {
                    status: self.ctx.status(),
                    sts: self.ctx.sts(),
                    gc_no: self.ctx.gc_no(),
                    row_undo: RowUndoLogs::empty(),
                    index_undo: IndexUndoLogs::empty(),
                }),
                attachment: Some(attachment),
                lock_manager,
                lock_state: self.lock_state.take(),
            });
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
        Ok(PreparedTrx {
            redo_bin,
            payload: Some(PreparedTrxPayload::User {
                status: self.ctx.status(),
                sts: self.ctx.sts(),
                gc_no: self.ctx.gc_no(),
                row_undo,
                index_undo,
            }),
            attachment: Some(attachment),
            lock_manager,
            lock_state: self.lock_state.take(),
        })
    }
}

impl Drop for TrxInner {
    #[inline]
    fn drop(&mut self) {
        self.effects.assert_cleared();
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
        }
    }

    /// Releases and drops transaction-owned locks for an unordered discard path.
    #[inline]
    pub(crate) fn release_transaction_locks(&mut self) -> usize {
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
    async fn rollback(&mut self, attachment: &TrxAttachment) -> Result<()> {
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
        let committed = match self.payload.take() {
            Some(PrecommitTrxPayload::User {
                status,
                sts,
                gc_no,
                row_undo,
                mut index_undo,
            }) => {
                let index_gc = index_undo.commit_for_gc();
                status.commit_prepared(self.cts);
                if let Some(s) = self.attachment.take() {
                    s.commit(self.cts);
                }
                CommittedTrx {
                    cts: self.cts,
                    payload: Some(CommittedTrxPayload::User {
                        sts,
                        gc_no,
                        row_undo,
                        index_gc,
                    }),
                }
            }
            Some(PrecommitTrxPayload::System(payload)) => {
                debug_assert!(self.attachment.is_none());
                CommittedTrx {
                    cts: self.cts,
                    payload: Some(CommittedTrxPayload::System(payload)),
                }
            }
            None => {
                debug_assert!(self.attachment.is_none());
                // A system transaction without GC pages has no purge payload.
                CommittedTrx {
                    cts: self.cts,
                    payload: None,
                }
            }
        };
        self.release_transaction_locks();
        committed
    }

    /// Rollback this transaction after it entered prepare but before redo durability succeeded.
    ///
    /// Failed precommit must perform rollback-equivalent cleanup before waking
    /// redo waiters. Row-version heads can point into transaction-owned row
    /// undo, so dropping the payload before rollback would leave dangling raw
    /// undo references.
    #[inline]
    async fn rollback_failed_precommit(mut self) -> bool {
        self.redo_bin.take();
        let engine = self
            .attachment
            .as_ref()
            .map(|attachment| attachment.engine().clone());
        if let (Some(payload), Some(attachment)) = (self.payload.as_mut(), self.attachment.as_ref())
        {
            let res = payload.rollback(attachment).await;
            if res.is_err() {
                let _ = attachment
                    .engine()
                    .trx_sys
                    .poison_engine(FatalError::RollbackAccess);
                self.finish_failed_precommit_with_retention(engine);
                return false;
            }
            payload.record_rollback_for_purge(attachment);
        }
        self.release_transaction_locks();
        if let Some(s) = self.attachment.take() {
            s.rollback();
        }
        if let Some(payload) = self.payload.take() {
            payload.finish_successful_rollback();
        }
        true
    }

    /// Fail this transaction without touching row/index undo after rollback became unsafe.
    #[inline]
    fn fail_failed_precommit_without_rollback(mut self) {
        self.redo_bin.take();
        let engine = self
            .attachment
            .as_ref()
            .map(|attachment| attachment.engine().clone());
        self.finish_failed_precommit_with_retention(engine);
    }

    #[inline]
    fn finish_failed_precommit_with_retention(&mut self, engine: Option<EngineRef>) {
        self.release_transaction_locks();
        if let Some(s) = self.attachment.take() {
            s.rollback();
        }
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
        debug_assert!(self.attachment.is_none());
        if let Some(payload) = self.payload.take() {
            debug_assert!(matches!(payload, PrecommitTrxPayload::System(_)));
        }
        self.redo_bin.take();
        self.release_transaction_locks();
    }

    #[inline]
    fn release_transaction_locks(&mut self) -> usize {
        release_carried_transaction_locks(&mut self.lock_state, &mut self.lock_manager)
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
fn transaction_entry_state_err(trx_id: TrxID, state: TrxEntryState) -> Report<LifecycleError> {
    match state {
        TrxEntryState::CheckedOut
        | TrxEntryState::CheckedOutAbandoned
        | TrxEntryState::Committing
        | TrxEntryState::RollingBack
        | TrxEntryState::CleanupRunning => Report::new(LifecycleError::ExistingTransaction)
            .attach(format!("trx_id={trx_id}, state={}", state.label())),
        TrxEntryState::Abandoned | TrxEntryState::Terminal | TrxEntryState::Failed => {
            Report::new(InternalError::ActiveTransactionDiscarded)
                .change_context(LifecycleError::TransactionDiscarded)
                .attach(format!("trx_id={trx_id}, state={}", state.label()))
        }
        TrxEntryState::Active => Report::new(InternalError::ActiveTransactionDiscarded)
            .change_context(LifecycleError::TransactionDiscarded)
            .attach(format!("trx_id={trx_id}, reason=transaction_core_missing")),
    }
}

#[inline]
fn release_carried_transaction_locks(
    lock_state: &mut Option<OwnerLockState>,
    lock_manager: &mut Option<QuiescentGuard<LockManager>>,
) -> usize {
    match (lock_state.take(), lock_manager.take()) {
        (Some(mut lock_state), Some(lock_manager)) => lock_state.release_all(&lock_manager),
        (None, None) => 0,
        (Some(_), None) => {
            panic!("transaction lock state requires a lock manager guard")
        }
        (None, Some(_)) => {
            panic!("transaction lock manager guard requires lock state")
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
    use crate::error::OperationError;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::{MutableTableFile, TableFile};
    use crate::id::{PageID, RowID, SessionID};
    use crate::io::{
        IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::lock::tests::{
        LockDebugEntryState, debug_snapshot, try_acquire, try_acquire_grouped,
    };
    use crate::log::redo::{RowRedo, RowRedoKind};
    use crate::row::ops::SelectKey;
    use crate::session::{Session, tests::SessionTestExt};
    use crate::table::test_user_table_id;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::sys::tests::{
        TerminalRollbackTestHookGuard, fatal_rollback_retention_count,
        install_terminal_rollback_test_hook,
    };
    use crate::trx::undo::{IndexUndo, IndexUndoKind, OwnedRowUndo, RowUndoKind};
    use crate::value::{Val, ValKind};
    use event_listener::Listener;
    use smol::Timer;
    use std::cell::Cell;
    use std::io::Error as IoError;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
    use std::thread::{scope, sleep, spawn};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        test_engine_with_mem_size(log_file_stem, 64usize * 1024 * 1024).await
    }

    #[test]
    fn test_shared_status_terminal_resolution_is_sticky_and_wakeable() {
        smol::block_on(async {
            let committed = SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1);
            let mut commit_listener = Box::pin(committed.terminal_listener().unwrap());
            assert!(futures::poll!(commit_listener.as_mut()).is_pending());
            committed.commit_for_test(TrxID::new(10));
            commit_listener.await;
            assert!(committed.terminal());
            assert!(committed.terminal_listener().is_none());

            let rolled_back = SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 2);
            let mut rollback_listener = Box::pin(rolled_back.terminal_listener().unwrap());
            assert!(futures::poll!(rollback_listener.as_mut()).is_pending());
            rolled_back.rollback_for_test();
            rollback_listener.await;
            assert!(rolled_back.terminal());
            assert!(rolled_back.terminal_listener().is_none());
        });
    }

    async fn test_engine_with_mem_size(
        log_file_stem: &str,
        max_mem_size: usize,
    ) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
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
            )
            .build()
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
    fn resolve_active_parts_for_test(trx: &Transaction) -> Result<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = trx
            .engine
            .upgrade_for_terminal()
            .attach_with(|| format!("session_id={}, trx_id={}", trx.session_id, trx.trx_id))?;
        let (entry, session) = engine
            .session_registry
            .resolve_trx(trx.session_id, trx.trx_id)?;
        let attachment = TrxAttachment::new(engine, session, trx.trx_id);
        Ok((entry, attachment))
    }

    #[inline]
    fn transaction_entry(trx: &Transaction) -> Arc<TrxEntry> {
        resolve_active_parts_for_test(trx)
            .expect("test transaction must resolve")
            .0
    }

    #[test]
    fn test_checked_out_entry_remembers_abandonment_until_return() {
        let entry = TrxEntry::new(TrxInner::new(
            MIN_ACTIVE_TRX_ID + 100,
            TrxID::new(100),
            0,
            SessionID::new(100),
        ));
        let inner = entry
            .take_for_checkout()
            .expect("active entry can be checked out");

        assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOut);
        assert!(entry.abandon());
        assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOutAbandoned);

        assert_eq!(entry.return_inner(inner), TrxEntryState::Abandoned);
        assert_eq!(entry.inspect_state(), TrxEntryState::Abandoned);
    }

    #[test]
    fn test_failed_entry_state_is_not_in_transaction() {
        assert!(TrxEntryState::Active.in_trx());
        assert!(TrxEntryState::CheckedOut.in_trx());
        assert!(TrxEntryState::CheckedOutAbandoned.in_trx());
        assert!(TrxEntryState::Committing.in_trx());
        assert!(TrxEntryState::RollingBack.in_trx());
        assert!(TrxEntryState::Abandoned.in_trx());
        assert!(TrxEntryState::CleanupRunning.in_trx());
        assert!(!TrxEntryState::Terminal.in_trx());
        assert!(!TrxEntryState::Failed.in_trx());
    }

    #[test]
    fn test_failed_entry_does_not_count_as_active_transaction() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                test_engine("failed_entry_does_not_count_as_active_transaction").await;
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let sts = trx.sts();
            let gc_no = transaction_gc_no(&trx);

            assert_eq!(
                engine.inner().session_registry.active_transaction_count(),
                1
            );
            entry.finish(TrxEntryState::Failed);
            assert_eq!(entry.inspect_state(), TrxEntryState::Failed);
            assert_eq!(
                engine.inner().session_registry.active_transaction_count(),
                0
            );

            engine.inner().trx_sys.record_rollback_for_purge(gc_no, sts);
            engine
                .inner()
                .session_registry
                .finish_trx_rollback(trx.session_id, trx.trx_id);
            drop(trx);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_checked_out_abandoned_return_notifies_session_registry() {
        smol::block_on(async {
            let (_temp_dir, engine) =
                test_engine("checked_out_abandoned_return_notifies_session_registry").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let checkout = trx.checkout().expect("test transaction can be checked out");

            assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOut);
            assert!(entry.abandon());
            assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOutAbandoned);

            let observed_epoch = engine.inner().session_registry.trx_change_epoch();
            let engine_ref = engine.new_ref().unwrap();
            let (ready_tx, ready_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let waiter = spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                engine_ref
                    .session_registry
                    .wait_for_trx_change_since(observed_epoch);
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
                entry.inspect_state(),
                TrxEntryState::Abandoned | TrxEntryState::CleanupRunning | TrxEntryState::Terminal
            ));

            drop(trx);
            drop(session);
            engine.shutdown().unwrap();
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
        let state = entry.inspect_state();
        if state != TrxEntryState::Active {
            return Err(transaction_entry_state_err(entry.trx_id, state)
                .attach(format!("operation={operation}"))
                .into());
        }
        let inner = inner_slot.as_ref().ok_or_else(|| {
            transaction_entry_state_err(entry.trx_id, TrxEntryState::Active)
                .attach(format!("operation={operation}"))
        })?;
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
            .attach_with(|| format!("operation={operation}"))?;
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
            .claim_terminal(TrxEntryState::Committing)
            .attach("operation=prepare_active_transaction")?;
        let (_entry, inner, attachment) = claim.into_parts();
        inner.prepare(attachment)
    }

    #[inline]
    fn begin_production_test_transaction(engine: &Engine) -> (Session, Transaction) {
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();
        (session, trx)
    }

    #[inline]
    fn discard_production_prepared_for_test(mut prepared: PreparedTrx) {
        if let Some(payload) = prepared.payload.take()
            && let Some(attachment) = prepared.attachment.as_ref()
        {
            let trx_sys = &attachment.engine().trx_sys;
            let PreparedTrxPayload::User { gc_no, sts, .. } = payload else {
                panic!("production prepared transaction must carry user payload")
            };
            trx_sys.record_rollback_for_purge(gc_no, sts);
        }
        prepared.redo_bin.take();
        if let Some(attachment) = prepared.attachment.take() {
            attachment.rollback();
        }
        prepared.release_transaction_locks();
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
        let engine = trx.engine().expect("test transaction must have engine");
        discard_transaction_after_fatal_rollback(trx);
        engine.trx_sys.record_rollback_for_purge(gc_no, sts);
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
                    page_id: PageID::new(0),
                    row_id: RowID::new(0),
                    kind: RowRedoKind::Insert(vec![
                        Val::U64(123),
                        Val::U32(1),
                        Val::U32(2),
                        Val::from(&PSEUDO_SYSBENCH_VAR1[..]),
                        Val::from(&PSEUDO_SYSBENCH_VAR2[..]),
                    ]),
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
        let mut checkout = TrxCheckout::new(entry, attachment).expect("test checkout");
        checkout.discard_after_fatal_rollback();
    }

    #[inline]
    pub(crate) fn lock_owner(trx: &Transaction) -> Result<LockOwner> {
        with_transaction_inner(trx, "read_transaction_lock_owner", |inner| {
            Ok(inner
                .checked_lock_state()
                .attach("operation=read_transaction_lock_owner")?
                .owner())
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
                Ok(inner
                    .checked_lock_state()
                    .attach("operation=check_transaction_lock_cache")?
                    .cached_covers(resource, mode)?)
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
            .attach("operation=try_acquire_transaction_lock")?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        try_acquire_owner_lock_state(
            inner
                .checked_lock_state_mut()
                .change_context(OperationError::LockUnavailable)
                .attach_with(|| {
                    "operation=try_acquire_transaction_lock, phase=resolve_transaction_lock_state"
                })?,
            lock_manager,
            resource,
            mode,
        )
    }

    #[inline]
    fn try_acquire_owner_lock_state(
        lock_state: &mut OwnerLockState,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        if lock_state.cached_covers(resource, mode)? {
            return Ok(true);
        }
        let owner = lock_state.owner();
        let acquired = match lock_state.owner_group() {
            Some(owner_group) => {
                try_acquire_grouped(lock_manager, resource, mode, owner, owner_group)?
            }
            None => try_acquire(lock_manager, resource, mode, owner)?,
        };
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
                    inner.ctx().status()
                })
                .unwrap();

            let prepared = prepare_transaction(trx).unwrap();
            assert!(status.preparing());
            let listener = status
                .prepare_listener()
                .expect("preparing transaction should expose a listener");
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
            assert!(status.prepare_listener().is_none());
        });
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
                    inner.ctx().status()
                })
                .unwrap();

            let prepared = prepare_transaction(trx).unwrap();
            assert!(status.preparing());
            let listener = status
                .prepare_listener()
                .expect("preparing transaction should expose a listener");
            let waiter_status = Arc::clone(&status);
            let (ready_tx, ready_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let waiter = spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                listener.wait();
                done_tx
                    .send(waiter_status.prepare_listener().is_none())
                    .expect("waiter should report completion state");
            });

            ready_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("waiter should start");
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "prepare waiter should block before rollback completion"
            );

            let precommit = prepared.fill_cts(TrxID::new(91_249));
            assert!(precommit.rollback_failed_precommit().await);
            assert!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("waiter should wake after failed-precommit rollback"),
                "late listener lookup should return none after rollback"
            );
            waiter.join().expect("waiter thread should finish");
            assert!(!status.preparing());
            assert!(status.prepare_listener().is_none());
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
                        page_id: PageID::new(0),
                        row_id: RowID::new(23),
                        kind: RowRedoKind::Delete,
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
    #[should_panic(expected = "catalog table DML must be logged")]
    fn test_redo_invariants_debug_assert_catalog_dml_without_metadata_ddl() {
        let mut effects = TrxEffects::empty();
        effects.redo.insert_dml(
            TABLE_ID_TABLES,
            RowRedo {
                page_id: PageID::new(0),
                row_id: RowID::new(0),
                kind: RowRedoKind::Insert(vec![Val::U64(1)]),
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
                        page_id: PageID::new(0),
                        row_id: RowID::new(23),
                        kind: RowRedoKind::Delete,
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
                            page_id: PageID::new(0),
                            row_id: RowID::new(24),
                            kind: RowRedoKind::Delete,
                        },
                    );
                    Err(Report::new(OperationError::NotSupported).into())
                })
                .await;
            let err = res.unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
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
                    Err(Report::new(OperationError::NotSupported).into())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::NotSupported)
            );

            let first_owner = first_owner.get().unwrap();
            let second_owner = second_owner.get().unwrap();
            let error_owner = error_owner.get().unwrap();
            assert_eq!(first_owner, LockOwner::Statement(trx.trx_id(), 1));
            assert_eq!(second_owner, LockOwner::Statement(trx.trx_id(), 2));
            assert_eq!(error_owner, LockOwner::Statement(trx.trx_id(), 3));
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
                    LockOwner::Session(SessionID::new(91_221))
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
                .release_owner(LockOwner::Session(SessionID::new(91_221)));

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

            assert_eq!(entry.inspect_state(), TrxEntryState::Active);
            trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();
            assert_eq!(entry.inspect_state(), TrxEntryState::Active);
            assert!(cached_transaction_lock_covers(&trx, metadata, LockMode::Shared).unwrap());
            assert!(cached_transaction_lock_covers(&trx, data, LockMode::Exclusive).unwrap());

            trx.lock_table(table_id, LockMode::Shared).await.unwrap();
            trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();

            assert_eq!(entry.inspect_state(), TrxEntryState::Active);
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
            assert_eq!(entry.inspect_state(), TrxEntryState::Terminal);
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_pending_lock_table_checkout_restores_active_on_drop() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("trx_lock_table_cancel_state").await;
            let table_id = catalog_tests::table2(&engine).await;
            let blocker = LockOwner::Transaction(TrxID::new(91_401));
            let data = LockResource::TableData(table_id);
            assert!(
                try_acquire(engine.lock_manager(), data, LockMode::Exclusive, blocker).unwrap()
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let entry = transaction_entry(&trx);
            let owner = lock_owner(&trx).unwrap();
            let metadata = LockResource::TableMetadata(table_id);
            let mut lock_fut = Box::pin(trx.lock_table(table_id, LockMode::Shared));

            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOut);
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

            assert_eq!(entry.inspect_state(), TrxEntryState::Active);
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

            let blocker = LockOwner::Transaction(TrxID::new(91_402));
            assert!(
                try_acquire(engine.lock_manager(), data, LockMode::Exclusive, blocker).unwrap()
            );

            let mut lock_fut = Box::pin(trx.lock_table(table_id, LockMode::Shared));
            assert!(matches!(
                futures::poll!(lock_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOut);
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

            assert_eq!(entry.inspect_state(), TrxEntryState::Active);
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
    fn test_transaction_locks_release_on_precommit_abort() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_abort").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
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

            let prepared = prepare_transaction(trx).unwrap();
            let precommit = prepared.fill_cts(TrxID::new(91_241));
            assert!(precommit.rollback_failed_precommit().await);
            assert_eq!(lock_entry_count(&engine, owner), 0);
            assert!(!session.in_trx().unwrap());
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
            assert_eq!(entry.inspect_state(), TrxEntryState::RollingBack);
            assert!(session.in_trx().unwrap());

            drop(rollback);
            release_terminal_rollback_hook(&release);
            wait_until(
                || {
                    !session.in_trx().unwrap()
                        && entry.inspect_state() == TrxEntryState::Terminal
                        && lock_entry_count(&engine, owner) == 0
                },
                "terminal rollback cleanup did not finish after waiter drop",
            );
            engine.shutdown().unwrap();
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
                    shutdown_engine.shutdown().unwrap();
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
            assert_eq!(entry.inspect_state(), TrxEntryState::Terminal);
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
            let session_id = trx.session_id;
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
            assert_eq!(entry.inspect_state(), TrxEntryState::RollingBack);

            let engine_ref = engine.new_ref().unwrap();
            let (duplicate_entry, duplicate_session) = engine_ref
                .session_registry
                .resolve_trx(session_id, trx_id)
                .expect("rolling-back transaction should remain registry-visible");
            let duplicate_attachment = TrxAttachment::new(engine_ref, duplicate_session, trx_id);
            assert!(
                TrxCompletionClaim::cleanup(duplicate_entry, duplicate_attachment).is_err(),
                "abandoned cleanup must not claim a rolling-back terminal transaction"
            );

            drop(rollback);
            release_terminal_rollback_hook(&release);
            wait_until(
                || !session.in_trx().unwrap() && entry.inspect_state() == TrxEntryState::Terminal,
                "terminal rollback cleanup did not finish after duplicate cleanup attempt",
            );
            engine.shutdown().unwrap();
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
            let _ = engine.inner().trx_sys.poison_engine(FatalError::RedoWrite);
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
                entry.inspect_state(),
                TrxEntryState::Committing | TrxEntryState::RollingBack
            ));

            drop(commit);
            release_terminal_rollback_hook(&release);
            wait_until(
                || {
                    !session.in_trx().unwrap()
                        && entry.inspect_state() == TrxEntryState::Terminal
                        && lock_entry_count(&engine, owner) == 0
                },
                "poisoned commit rollback cleanup did not finish after waiter drop",
            );
            engine.shutdown().unwrap();
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
            let precommit1 =
                precommit_with_cold_row_undo(&mut session1, table_id, TrxID::new(91_261));
            let precommit2 =
                precommit_with_cold_row_undo(&mut session2, table_id, TrxID::new(91_262));

            let mut trx3 = session3.begin_trx().unwrap();
            let _row_id = trx3
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

            let _ = engine.inner().trx_sys.poison_engine(FatalError::RedoWrite);
            let completion = Arc::new(Completion::new());
            let job = FailedPrecommitCleanupJob::new(
                vec![precommit1, precommit2, precommit3],
                Arc::clone(&completion),
                FailedPrecommitReason::Fatal(FatalError::RedoWrite),
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
            assert!(!session1.in_trx().unwrap());
            assert!(!session2.in_trx().unwrap());
            assert!(!session3.in_trx().unwrap());
            assert!(
                completion
                    .wait_result()
                    .await
                    .is_err_and(|err| *err.current_context()
                        == CompletionErrorKind::Fatal(FatalError::RedoWrite))
            );
            assert!(
                engine
                    .inner()
                    .trx_sys
                    .poison_error()
                    .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
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
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            let err = match prepare_transaction(trx) {
                Ok(_) => panic!("discarded transaction prepare should fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            let err = trx.commit().await.unwrap_err();
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(err.kind(), crate::error::ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );
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
