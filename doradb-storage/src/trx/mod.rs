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
mod group;
pub(crate) mod log;
pub(crate) mod log_replay;
pub(crate) mod purge;
pub(crate) mod recover;
pub(crate) mod redo;
pub(crate) mod row;
pub(crate) mod stmt;
pub(crate) mod sys;
mod sys_trx;
pub(crate) mod undo;
pub(crate) mod ver_map;

use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::is_catalog_obj_id;
use crate::engine::{EngineRef, WeakEngineRef};
use crate::error::{InternalError, OperationError, Result};
use crate::id::{PageID, RowID, SessionID, TableID, TrxID};
use crate::lock::{
    FreshLockGuard, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource, OwnerLockState,
    StmtNo,
};
use crate::quiescent::QuiescentGuard;
use crate::session::TrxAttachment;
use crate::trx::log_replay::TrxLog;
use crate::trx::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind};
use crate::trx::undo::{IndexPurgeEntry, IndexUndoLogs, RowUndoHead, RowUndoLogs, UndoStatus};
use error_stack::Report;
use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem;
use std::ops::AsyncFnOnce;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

pub use stmt::Statement;
pub(crate) const MIN_SNAPSHOT_TS: TrxID = TrxID::new(1);
pub(crate) const MAX_SNAPSHOT_TS: TrxID = TrxID::new(1 << 63);
pub(crate) const MAX_COMMIT_TS: TrxID = TrxID::new(1 << 63);
// As active transaction id is always greater than STS, that means
// visibility check can be simplified to "STS is larger".
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
    fn resolve_active(&self, operation: &'static str) -> Result<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade(operation)?;
        engine.trx_sys.ensure_runtime_healthy()?;
        self.resolve_with_engine(engine, operation)
    }

    /// Resolve this handle for terminal or cleanup paths.
    #[inline]
    fn resolve_terminal(&self, operation: &'static str) -> Result<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = self.engine.upgrade_for_terminal(operation)?;
        self.resolve_with_engine(engine, operation)
    }

    #[inline]
    fn resolve_with_engine(
        &self,
        engine: EngineRef,
        operation: &'static str,
    ) -> Result<(Arc<TrxEntry>, TrxAttachment)> {
        let (entry, session) =
            engine
                .session_registry
                .resolve_trx(self.session_id, self.trx_id, operation)?;
        let attachment = TrxAttachment::new(engine, session, self.trx_id);
        Ok((entry, attachment))
    }

    /// Check out the mutable core for one crate-internal operation.
    #[inline]
    pub(crate) fn checkout(&mut self, operation: &'static str) -> Result<TrxCheckout> {
        let (entry, attachment) = self.resolve_active(operation)?;
        TrxCheckout::new(entry, attachment, operation)
    }

    #[inline]
    pub(crate) fn claim_terminal(
        &self,
        state: TrxEntryState,
        operation: &'static str,
    ) -> Result<TrxCompletionClaim> {
        let (entry, attachment) = self.resolve_terminal(operation)?;
        TrxCompletionClaim::terminal(entry, attachment, state, operation)
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
        let mut checkout = self.checkout("lock explicit table")?;
        checkout.lock_table(table_id, mode).await
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
        let mut checkout = self.checkout("execute statement")?;
        let trx_id = checkout.inner().trx_id();
        let stmt_no = checkout.inner_mut().next_stmt_no()?;
        let stmt_owner = LockOwner::Statement(trx_id, stmt_no);
        enum ExecOutcome<T> {
            Success(T),
            StatementError(crate::error::Error),
            FatalRollback(crate::error::Error),
        }
        let outcome = {
            let (inner, attachment) = checkout.inner_and_attachment_mut();
            let mut stmt = Statement::new(inner, attachment, stmt_owner)?;
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

    /// Extends row pages that should be GCed after commit.
    #[inline]
    pub(crate) fn extend_gc_row_pages(&mut self, pages: Vec<PageID>) -> Result<()> {
        let mut checkout = self.checkout("extend transaction GC row pages")?;
        checkout.inner_mut().extend_gc_row_pages(pages);
        Ok(())
    }

    /// Replace the transaction-level DDL redo marker.
    #[inline]
    pub(crate) fn set_ddl_redo(&mut self, ddl: DDLRedo) -> Result<Option<Box<DDLRedo>>> {
        let mut checkout = self.checkout("set transaction DDL redo")?;
        Ok(checkout.inner_mut().redo_mut().ddl.replace(Box::new(ddl)))
    }

    /// Commit the transaction.
    #[inline]
    pub async fn commit(self) -> Result<TrxID> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal("commit active transaction")?;
        let claim = trx.claim_terminal(TrxEntryState::Committing, "commit active transaction")?;
        engine.trx_sys.commit_transaction(claim).await
    }

    /// Rollback the transaction.
    #[inline]
    pub async fn rollback(self) -> Result<()> {
        let mut trx = self;
        trx.terminal_started = true;
        let engine = trx
            .engine
            .upgrade_for_terminal("rollback active transaction")?;
        let claim =
            trx.claim_terminal(TrxEntryState::RollingBack, "rollback active transaction")?;
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
    pub(crate) handle: Transaction,
    pub(crate) entry: Arc<TrxEntry>,
}

pub(crate) struct SharedTrxStatus {
    ts: AtomicU64,
    preparing: AtomicBool,
    prepare_ev: Mutex<Option<Event>>,
}

impl SharedTrxStatus {
    /// Create a new shared transaction status for given transaction id.
    #[inline]
    pub(crate) fn new(trx_id: TrxID) -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(trx_id.as_u64()),
            preparing: AtomicBool::new(false),
            prepare_ev: Mutex::new(None),
        }
    }

    /// Returns the timestamp of current transaction.
    #[inline]
    pub(crate) fn ts(&self) -> TrxID {
        TrxID::new(self.ts.load(Ordering::Acquire))
    }

    /// Returns whether this transaction is preparing.
    #[inline]
    pub(crate) fn preparing(&self) -> bool {
        self.preparing.load(Ordering::Acquire)
    }

    /// Returns notifier if the transaction is in prepare phase.
    /// Prepare pahse means the transaction already get a commit timestamp
    /// but not persist its log.
    /// To avoid partial read, other transaction read the data modified by
    /// this transaction should wait for
    #[inline]
    pub(crate) fn prepare_listener(&self) -> Option<EventListener> {
        let g = self.prepare_ev.lock();
        g.as_ref().map(|event| event.listen())
    }
}

/// Returns whether the transaction is committed.
#[inline]
pub(crate) fn trx_is_committed(ts: TrxID) -> bool {
    ts < MIN_ACTIVE_TRX_ID
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
    log_no: usize,
    gc_no: usize,
}

impl TrxContext {
    /// Create a transaction context from session, status, and partition identity.
    #[inline]
    pub(crate) fn new(trx_id: TrxID, sts: TrxID, log_no: usize, gc_no: usize) -> Self {
        TrxContext {
            status: Arc::new(SharedTrxStatus::new(trx_id)),
            sts,
            log_no,
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
            UndoStatus::Ref(arc) => std::ptr::addr_eq(self.status.as_ref(), arc.as_ref()),
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

    /// Returns the log partition number assigned to the transaction.
    #[inline]
    pub(crate) fn log_no(&self) -> usize {
        self.log_no
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
        let mut g = self.status.prepare_ev.lock();
        *g = Some(Event::new());
        self.status.preparing.store(true, Ordering::SeqCst);
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

    /// Returns this transaction's current status timestamp.
    #[inline]
    pub(crate) fn trx_id(&self) -> TrxID {
        self.ctx.trx_id()
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
            let owner = LockOwner::Transaction(self.trx_id());
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
    pub(crate) fn load_active_insert_page(
        &self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.attachment.load_active_insert_page(table_id)
    }

    /// Saves the cached active insert page through the current attachment.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.attachment
            .save_active_insert_page(table_id, page_id, row_id);
    }
}

/// Mutable transaction-level effects accumulated across successful statements.
pub(crate) struct TrxEffects {
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    redo: RedoLogs,
    gc_row_pages: Vec<PageID>,
}

impl TrxEffects {
    /// Create an empty transaction effects accumulator.
    #[inline]
    pub(crate) fn empty() -> Self {
        TrxEffects {
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
            gc_row_pages: Vec::new(),
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
        self.require_durability()
            || !self.row_undo.is_empty()
            || !self.index_undo.is_empty()
            || !self.gc_row_pages.is_empty()
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

    /// Returns mutable access to transaction redo logs.
    #[inline]
    pub(crate) fn redo_mut(&mut self) -> &mut RedoLogs {
        &mut self.redo
    }

    /// Extends row pages that should be GCed after commit.
    #[inline]
    pub(crate) fn extend_gc_row_pages(&mut self, pages: Vec<PageID>) {
        self.gc_row_pages.extend(pages);
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
                .any(|table_id| is_catalog_obj_id(*table_id))
                || is_catalog_metadata_ddl(self.redo.ddl.as_deref()),
            "catalog table DML must be logged by a catalog metadata DDL transaction"
        );
    }

    /// Moves transaction effects into a prepared transaction payload.
    #[inline]
    pub(crate) fn take_payload_parts(&mut self) -> (RowUndoLogs, IndexUndoLogs, Vec<PageID>) {
        (
            mem::take(&mut self.row_undo),
            mem::take(&mut self.index_undo),
            mem::take(&mut self.gc_row_pages),
        )
    }

    /// Clears effects after rollback or fatal cleanup.
    #[inline]
    pub(crate) fn clear_for_rollback(&mut self) {
        self.redo.clear();
        self.row_undo = RowUndoLogs::empty();
        self.index_undo = IndexUndoLogs::empty();
        self.gc_row_pages.clear();
    }

    /// Asserts that all transaction effects have been consumed or cleared.
    #[inline]
    fn assert_cleared(&self) {
        assert!(self.redo.is_empty(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_undo.is_empty(), "index undo should be cleared");
        assert!(
            self.gc_row_pages.is_empty(),
            "gc row pages should be cleared"
        );
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
        )
    )
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
        !matches!(self, TrxEntryState::Terminal)
    }

    #[inline]
    fn label(self) -> &'static str {
        match self {
            TrxEntryState::Active => "active",
            TrxEntryState::CheckedOut => "checked-out",
            TrxEntryState::CheckedOutAbandoned => "checked-out-abandoned",
            TrxEntryState::Committing => "committing",
            TrxEntryState::RollingBack => "rolling-back",
            TrxEntryState::Abandoned => "abandoned",
            TrxEntryState::CleanupRunning => "cleanup-running",
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
    fn take_for_checkout(&self, operation: &'static str) -> Result<TrxInner> {
        self.take_inner(TrxEntryState::CheckedOut, operation)
    }

    #[inline]
    fn take_for_terminal(&self, state: TrxEntryState, operation: &'static str) -> Result<TrxInner> {
        debug_assert!(matches!(
            state,
            TrxEntryState::Committing | TrxEntryState::RollingBack
        ));
        self.take_inner(state, operation)
    }

    #[inline]
    fn take_for_cleanup(&self, operation: &'static str) -> Result<TrxInner> {
        self.take_inner_from_state(
            TrxEntryState::Abandoned,
            TrxEntryState::CleanupRunning,
            operation,
        )
    }

    #[inline]
    fn take_inner(&self, next_state: TrxEntryState, operation: &'static str) -> Result<TrxInner> {
        self.take_inner_from_state(TrxEntryState::Active, next_state, operation)
    }

    #[inline]
    fn take_inner_from_state(
        &self,
        expected_state: TrxEntryState,
        next_state: TrxEntryState,
        operation: &'static str,
    ) -> Result<TrxInner> {
        let mut inner_slot = self.inner.lock();
        // The state snapshot is stable for this transition because we hold the
        // inner mutex that serializes state/inner ownership changes.
        let state = self.inspect_state();
        if state != expected_state {
            return Err(transaction_entry_state_err(self.trx_id, state, operation));
        }
        if inner_slot.is_none() {
            return Err(transaction_entry_state_err(
                self.trx_id,
                expected_state,
                operation,
            ));
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
    fn return_inner(&self, inner: TrxInner) {
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
    fn new(
        entry: Arc<TrxEntry>,
        attachment: TrxAttachment,
        operation: &'static str,
    ) -> Result<Self> {
        let inner = entry.take_for_checkout(operation)?;
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
    fn inner_and_attachment_mut(&mut self) -> (&mut TrxInner, &TrxAttachment) {
        let inner = self
            .inner
            .as_mut()
            .expect("TrxCheckout always owns an inner until fatal discard");
        (inner, &self.attachment)
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
            inner.discard_after_fatal_rollback(&self.attachment);
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
        self.entry.return_inner(inner);
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
        operation: &'static str,
    ) -> Result<Self> {
        let inner = entry.take_for_terminal(state, operation)?;
        Ok(Self {
            entry,
            inner,
            attachment,
        })
    }

    #[inline]
    pub(crate) fn cleanup(
        entry: Arc<TrxEntry>,
        attachment: TrxAttachment,
        operation: &'static str,
    ) -> Result<Self> {
        let inner = entry.take_for_cleanup(operation)?;
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
}

/// Reason an abandoned transaction cleanup job was queued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrxCleanupReason {
    /// The public transaction handle was dropped.
    HandleDrop,
    /// Engine shutdown found an abandoned transaction.
    ShutdownDrain,
}

/// Abandoned transaction cleanup job.
pub(crate) struct TrxCleanupJob {
    pub(crate) engine: EngineRef,
    pub(crate) session_id: SessionID,
    pub(crate) trx_id: TrxID,
    pub(crate) reason: TrxCleanupReason,
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
    lock_state: Option<OwnerLockState>,
    next_stmt_no: StmtNo,
    active: bool,
}

impl TrxInner {
    #[inline]
    pub(crate) fn new(
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
        session_id: SessionID,
    ) -> Self {
        let owner_group = LockOwnerGroup::Session(session_id);
        TrxInner {
            ctx: TrxContext::new(trx_id, sts, log_no, gc_no),
            effects: TrxEffects::empty(),
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

    #[inline]
    fn checked_engine(
        &self,
        attachment: &TrxAttachment,
        operation: &'static str,
    ) -> Result<EngineRef> {
        if !self.active {
            return Err(transaction_discarded_err(operation));
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
    fn checked_lock_state(&self, operation: &'static str) -> Result<&OwnerLockState> {
        if !self.active {
            return Err(transaction_discarded_err(operation));
        }
        self.lock_state
            .as_ref()
            .ok_or_else(|| transaction_discarded_err(operation))
    }

    #[inline]
    fn checked_lock_state_mut(&mut self, operation: &'static str) -> Result<&mut OwnerLockState> {
        if !self.active {
            return Err(transaction_discarded_err(operation));
        }
        self.lock_state
            .as_mut()
            .ok_or_else(|| transaction_discarded_err(operation))
    }

    #[inline]
    fn table_lock_resources(table_id: TableID) -> TrxTableLockResources {
        TrxTableLockResources {
            metadata: LockResource::TableMetadata(table_id),
            data: LockResource::TableData(table_id),
        }
    }

    #[inline]
    fn explicit_table_lock_owner(&self, operation: &'static str) -> Result<LockOwner> {
        Ok(self.checked_lock_state(operation)?.owner())
    }

    #[inline]
    fn inspect_explicit_table_lock_cache(
        &self,
        resources: TrxTableLockResources,
        data_mode: LockMode,
        operation: &'static str,
    ) -> Result<TrxTableLockCache> {
        let lock_state = self.checked_lock_state(operation)?;
        Ok(TrxTableLockCache {
            metadata_cached: lock_state.cached_covers(resources.metadata, LockMode::Shared)?,
            data_cached: lock_state.cached_covers(resources.data, data_mode)?,
        })
    }

    #[inline]
    async fn acquire_explicit_table_lock_guards<'lock>(
        &self,
        lock_manager: &'lock LockManager,
        resources: TrxTableLockResources,
        data_mode: LockMode,
        owner: LockOwner,
        operation: &'static str,
    ) -> Result<TrxTableLockGuards<'lock>> {
        let metadata_grant = self
            .checked_lock_state(operation)?
            .acquire_uncached(lock_manager, resources.metadata, LockMode::Shared)
            .await?;
        let metadata = FreshLockGuard::new(lock_manager, resources.metadata, owner, metadata_grant);

        let data_grant = self
            .checked_lock_state(operation)?
            .acquire_uncached(lock_manager, resources.data, data_mode)
            .await?;
        let data = FreshLockGuard::new(lock_manager, resources.data, owner, data_grant);

        Ok(TrxTableLockGuards { metadata, data })
    }

    #[inline]
    fn cache_explicit_table_lock_grants(
        &mut self,
        resources: TrxTableLockResources,
        data_mode: LockMode,
        cache: TrxTableLockCache,
        operation: &'static str,
    ) -> Result<()> {
        if !cache.data_cached {
            self.checked_lock_state_mut(operation)?
                .cache_granted(resources.data, data_mode);
        }
        if !cache.metadata_cached {
            self.checked_lock_state_mut(operation)?
                .cache_granted(resources.metadata, LockMode::Shared);
        }
        Ok(())
    }

    #[inline]
    fn next_stmt_no(&mut self) -> Result<StmtNo> {
        let stmt_no = self.next_stmt_no;
        self.next_stmt_no = self.next_stmt_no.checked_add(1).ok_or_else(|| {
            Report::new(InternalError::StatementNumberOverflow)
                .attach(format!("trx_id={}", self.trx_id()))
        })?;
        Ok(stmt_no)
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

    /// Returns the log partition number assigned to this transaction.
    #[inline]
    pub(crate) fn log_no(&self) -> usize {
        self.ctx().log_no()
    }

    /// Returns the GC bucket number assigned to this transaction.
    #[inline]
    pub(crate) fn gc_no(&self) -> usize {
        self.ctx().gc_no()
    }

    /// Returns mutable access to transaction redo logs.
    #[inline]
    pub(crate) fn redo_mut(&mut self) -> &mut RedoLogs {
        self.effects.redo_mut()
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
        let operation = "lock explicit table";
        mode.validate_explicit_table_lock()?;
        let engine = self.checked_engine(attachment, operation)?;
        engine
            .catalog()
            .validate_user_table_live(table_id, operation)
            .await?;

        let lock_manager = engine.lock_manager();
        let resources = Self::table_lock_resources(table_id);
        let owner = self.explicit_table_lock_owner(operation)?;
        let cache = self.inspect_explicit_table_lock_cache(resources, mode, operation)?;
        let mut guards = self
            .acquire_explicit_table_lock_guards(lock_manager, resources, mode, owner, operation)
            .await?;

        engine
            .catalog()
            .validate_user_table_live(table_id, operation)
            .await?;

        self.cache_explicit_table_lock_grants(resources, mode, cache, operation)?;
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

    /// Extends row pages that should be GCed after commit.
    #[inline]
    pub(crate) fn extend_gc_row_pages(&mut self, pages: Vec<PageID>) {
        self.effects.extend_gc_row_pages(pages);
    }

    /// Rolls back the attached session without taking the attachment.
    #[inline]
    pub(crate) fn finish_session_rollback(&mut self, attachment: &TrxAttachment) {
        attachment.rollback();
        self.active = false;
    }

    /// Clears effects and rolls back the attached session after rollback failure.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(&mut self, attachment: &TrxAttachment) {
        self.effects.clear_for_rollback();
        self.release_transaction_locks(attachment);
        self.finish_session_rollback(attachment);
    }

    /// Prepare current transaction for committing.
    #[inline]
    fn prepare(mut self, attachment: TrxAttachment) -> Result<PreparedTrx> {
        if !self.active {
            return Err(transaction_discarded_err("prepare active transaction"));
        }
        // fast path for readonly transactions
        if !self.require_ordered_commit() {
            let lock_manager = self.clone_lock_manager_guard(&attachment);
            // there should be no ref count of transaction status.
            debug_assert!(Arc::strong_count(&self.ctx.status) == 1);
            debug_assert!(self.effects.index_undo.is_empty());
            return Ok(PreparedTrx {
                redo_bin: None,
                payload: Some(PreparedTrxPayload {
                    status: self.ctx.status(),
                    sts: self.ctx.sts(),
                    log_no: self.ctx.log_no(),
                    gc_no: self.ctx.gc_no(),
                    row_undo: RowUndoLogs::empty(),
                    index_undo: IndexUndoLogs::empty(),
                    gc_row_pages: Vec::new(),
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
        let (row_undo, index_undo, gc_row_pages) = self.effects.take_payload_parts();
        let lock_manager = self.clone_lock_manager_guard(&attachment);
        Ok(PreparedTrx {
            redo_bin,
            payload: Some(PreparedTrxPayload {
                status: self.ctx.status(),
                sts: self.ctx.sts(),
                log_no: self.ctx.log_no(),
                gc_no: self.ctx.gc_no(),
                row_undo,
                index_undo,
                gc_row_pages,
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

#[inline]
pub(crate) fn transaction_discarded_err(operation: &'static str) -> crate::error::Error {
    Report::new(InternalError::ActiveTransactionDiscarded)
        .attach(format!("operation={operation}"))
        .into()
}

#[inline]
fn transaction_entry_state_err(
    trx_id: TrxID,
    state: TrxEntryState,
    operation: &'static str,
) -> crate::error::Error {
    match state {
        TrxEntryState::CheckedOut
        | TrxEntryState::CheckedOutAbandoned
        | TrxEntryState::Committing
        | TrxEntryState::RollingBack
        | TrxEntryState::CleanupRunning => Report::new(OperationError::ExistingTransaction)
            .attach(format!(
                "{operation}: transaction is {}: trx_id={trx_id}",
                state.label()
            ))
            .into(),
        TrxEntryState::Abandoned => transaction_discarded_err(operation),
        TrxEntryState::Terminal | TrxEntryState::Failed => transaction_discarded_err(operation),
        TrxEntryState::Active => Report::new(InternalError::ActiveTransactionDiscarded)
            .attach(format!(
                "{operation}: transaction core is missing: trx_id={trx_id}"
            ))
            .into(),
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
pub(crate) struct PreparedTrxPayload {
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    log_no: usize,
    gc_no: usize,
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    gc_row_pages: Vec<PageID>,
}

impl PreparedTrxPayload {
    /// Returns whether this payload has runtime effects that require ordered commit.
    #[inline]
    fn require_ordered_commit(&self) -> bool {
        !self.row_undo.is_empty() || !self.index_undo.is_empty() || !self.gc_row_pages.is_empty()
    }
}

/// Transaction in the logical PreparingCommit state.
///
/// `PreparedTrx` owns all transaction effects, locks, and the terminal
/// attachment before the irreversible group-commit handoff. Dropping it is only
/// valid after a caller has consumed or explicitly cleared those fields.
struct PreparedTrx {
    redo_bin: Option<TrxLog>,
    payload: Option<PreparedTrxPayload>,
    attachment: Option<TrxAttachment>,
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

    #[inline]
    fn fill_cts(mut self, cts: TrxID) -> PrecommitTrx {
        let redo_bin = if let Some(mut redo_bin) = self.redo_bin.take() {
            redo_bin.header.cts = cts;
            Some(redo_bin)
        } else {
            None
        };
        match self.payload.take() {
            Some(PreparedTrxPayload {
                status,
                sts,
                log_no: _,
                gc_no,
                row_undo,
                mut index_undo,
                gc_row_pages,
            }) => {
                // Once we get a concrete CTS, we won't rollback this transaction.
                // So we convert IndexUndo to IndexGC, and let purge threads to
                // remove unused index entries.
                let index_gc = index_undo.commit_for_gc();
                PrecommitTrx {
                    cts,
                    redo_bin,
                    payload: Some(PrecommitTrxPayload {
                        status,
                        sts,
                        gc_no,
                        row_undo,
                        index_gc,
                        gc_row_pages,
                    }),
                    attachment: self.attachment.take(),
                    lock_manager: self.lock_manager.take(),
                    lock_state: self.lock_state.take(),
                }
            }
            None => {
                debug_assert!(self.attachment.is_none());
                PrecommitTrx {
                    cts,
                    redo_bin,
                    payload: None,
                    attachment: None,
                    lock_manager: self.lock_manager.take(),
                    lock_state: self.lock_state.take(),
                }
            }
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

struct PrecommitTrxPayload {
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    gc_no: usize,
    row_undo: RowUndoLogs,
    index_gc: Vec<IndexPurgeEntry>,
    gc_row_pages: Vec<PageID>,
}

/// Transaction in the logical Committing state.
///
/// Once a `PrecommitTrx` is queued in a commit group, the redo worker owns the
/// terminal outcome. User waiters may observe success or failure, but they no
/// longer own session commit/rollback cleanup and cannot convert this state
/// back into an explicit rollback.
///
/// There are two kinds of PrecommitTrx. One is a user transaction containing
/// undo, index GC, lock, and terminal attachment. The other is an attachmentless
/// system transaction, which is directly dropped after ordered completion.
struct PrecommitTrx {
    cts: TrxID,
    redo_bin: Option<TrxLog>,
    // Payload is only for user transaction
    payload: Option<PrecommitTrxPayload>,
    attachment: Option<TrxAttachment>,
    lock_manager: Option<QuiescentGuard<LockManager>>,
    lock_state: Option<OwnerLockState>,
}

impl PrecommitTrx {
    /// Returns whether this transaction needs a recovery-visible log record.
    #[inline]
    fn require_durability(&self) -> bool {
        self.redo_bin.is_some()
    }

    /// Takes the recovery-visible log record, if this transaction requires one.
    #[inline]
    fn take_log(&mut self) -> Option<TrxLog> {
        self.redo_bin.take()
    }

    /// Commit this transaction.
    /// The method should be invoked when redo logs have been persisted to disk.
    /// It will update backfill commit timestamp and update status to committed.
    #[inline]
    fn commit(mut self) -> CommittedTrx {
        assert!(self.redo_bin.is_none()); // redo log should be already processed by logger.
        // release the prepare notifier in transaction status
        let committed = match self.payload.take() {
            Some(PrecommitTrxPayload {
                status,
                sts,
                gc_no,
                row_undo,
                index_gc,
                gc_row_pages,
            }) => {
                // For user transaction, we need to notify readers that this transaction is committed,
                // and readers can continue their work.
                {
                    // first update cts.
                    status.ts.store(self.cts.as_u64(), Ordering::SeqCst);
                    // then reset preparing.
                    status.preparing.store(false, Ordering::SeqCst);
                    // finally, drop event to notify all waiting transactions.
                    let mut g = status.prepare_ev.lock();
                    drop(g.take());
                }
                if let Some(s) = self.attachment.take() {
                    s.commit(self.cts);
                }
                CommittedTrx {
                    cts: self.cts,
                    payload: Some(CommittedTrxPayload {
                        sts,
                        gc_no,
                        row_undo,
                        index_gc,
                        gc_row_pages,
                    }),
                }
            }
            None => {
                debug_assert!(self.attachment.is_none());
                // For system transaction, there is nothing we have to keep once it's committed.
                CommittedTrx {
                    cts: self.cts,
                    payload: None,
                }
            }
        };
        self.release_transaction_locks();
        committed
    }

    /// Abort this transaction after it entered prepare but before redo durability succeeded.
    #[inline]
    fn abort(mut self) {
        self.redo_bin.take();
        if let Some(PrecommitTrxPayload { status, .. }) = self.payload.take() {
            status.preparing.store(false, Ordering::SeqCst);
            let mut g = status.prepare_ev.lock();
            drop(g.take());
        }
        if let Some(s) = self.attachment.take() {
            s.rollback();
        }
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

struct CommittedTrxPayload {
    sts: TrxID,
    gc_no: usize,
    row_undo: RowUndoLogs,
    index_gc: Vec<IndexPurgeEntry>,
    gc_row_pages: Vec<PageID>,
}

pub(crate) struct CommittedTrx {
    cts: TrxID,
    payload: Option<CommittedTrxPayload>,
}

impl CommittedTrx {
    #[inline]
    fn sts(&self) -> Option<TrxID> {
        self.payload.as_ref().map(|p| p.sts)
    }

    /// Returns gc_no if this transaction is GC-aware.
    #[inline]
    fn gc_no(&self) -> Option<usize> {
        self.payload.as_ref().map(|p| p.gc_no)
    }

    #[inline]
    fn row_undo(&self) -> Option<&RowUndoLogs> {
        self.payload.as_ref().map(|p| &p.row_undo)
    }

    #[inline]
    fn index_gc(&self) -> Option<&[IndexPurgeEntry]> {
        self.payload.as_ref().map(|p| &p.index_gc[..])
    }

    #[inline]
    fn gc_row_pages(&self) -> Option<&[PageID]> {
        self.payload.as_ref().map(|p| &p.gc_row_pages[..])
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::tests as catalog_tests;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::OperationError;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::{MutableTableFile, TableFile};
    use crate::id::SessionID;
    use crate::lock::tests::{
        LockDebugEntryState, debug_snapshot, try_acquire, try_acquire_grouped,
    };
    use crate::row::ops::SelectKey;
    use crate::session::tests as session_tests;
    use crate::table::test_user_table_id;
    use crate::trx::redo::{RowRedo, RowRedoKind};
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::undo::{IndexUndo, IndexUndoKind, OwnedRowUndo, RowUndoKind};
    use crate::value::{Val, ValKind};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path().to_path_buf())
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64usize * 1024 * 1024)
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

    fn test_transaction_with_parts(
        engine: &Engine,
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
    ) -> Transaction {
        let engine_ref = engine.new_ref().unwrap();
        let session_id = engine_ref.next_session_id();
        let (trx, _state) = session_tests::create_test_transaction(
            &engine.session_registry,
            engine_ref,
            session_id,
            trx_id,
            sts,
            log_no,
            gc_no,
        );
        trx
    }

    #[inline]
    fn resolve_active_parts_for_test(
        trx: &Transaction,
        operation: &'static str,
    ) -> Result<(Arc<TrxEntry>, TrxAttachment)> {
        let engine = trx.engine.upgrade_for_terminal(operation)?;
        let (entry, session) =
            engine
                .session_registry
                .resolve_trx(trx.session_id, trx.trx_id, operation)?;
        let attachment = TrxAttachment::new(engine, session, trx.trx_id);
        Ok((entry, attachment))
    }

    #[inline]
    fn transaction_entry(trx: &Transaction) -> Arc<TrxEntry> {
        resolve_active_parts_for_test(trx, "test resolve transaction entry")
            .expect("test transaction must resolve")
            .0
    }

    #[test]
    fn test_checked_out_entry_remembers_abandonment_until_return() {
        let entry = TrxEntry::new(TrxInner::new(
            MIN_ACTIVE_TRX_ID + 100,
            TrxID::new(100),
            0,
            0,
            SessionID::new(100),
        ));
        let inner = entry
            .take_for_checkout("test checkout")
            .expect("active entry can be checked out");

        assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOut);
        assert!(entry.abandon());
        assert_eq!(entry.inspect_state(), TrxEntryState::CheckedOutAbandoned);

        entry.return_inner(inner);

        assert_eq!(entry.inspect_state(), TrxEntryState::Abandoned);
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
            return Err(transaction_entry_state_err(entry.trx_id, state, operation));
        }
        let inner = inner_slot.as_ref().ok_or_else(|| {
            transaction_entry_state_err(entry.trx_id, TrxEntryState::Active, operation)
        })?;
        Ok(f(inner))
    }

    #[inline]
    fn with_transaction_inner_mut<T>(
        trx: &mut Transaction,
        operation: &'static str,
        f: impl FnOnce(&mut TrxInner) -> T,
    ) -> Result<T> {
        let mut checkout = trx.checkout(operation)?;
        Ok(f(checkout.inner_mut()))
    }

    #[inline]
    fn transaction_has_engine(trx: &Transaction) -> bool {
        trx.engine().is_some()
    }

    #[inline]
    fn transaction_pool_guards_attached(trx: &Transaction) -> bool {
        trx.resolve_active("test transaction pool guards")
            .and_then(|(entry, attachment)| {
                TrxCheckout::new(entry, attachment, "test transaction pool guards")
            })
            .map(|mut checkout| {
                let (inner, attachment) = checkout.inner_and_attachment_mut();
                let rt = TrxRuntime::new(inner.ctx(), attachment);
                let _ = rt.pool_guards();
                true
            })
            .unwrap_or(false)
    }

    #[inline]
    fn transaction_log_no(trx: &Transaction) -> usize {
        with_transaction_inner(
            trx,
            "query test transaction log partition",
            TrxInner::log_no,
        )
        .expect("test transaction must be active")
    }

    #[inline]
    fn transaction_gc_no(trx: &Transaction) -> usize {
        with_transaction_inner(trx, "query test transaction GC bucket", TrxInner::gc_no)
            .expect("test transaction must be active")
    }

    #[inline]
    fn transaction_require_durability(trx: &Transaction) -> bool {
        with_transaction_inner(
            trx,
            "query test transaction durability",
            TrxInner::require_durability,
        )
        .unwrap_or(false)
    }

    #[inline]
    fn transaction_require_ordered_commit(trx: &Transaction) -> bool {
        with_transaction_inner(
            trx,
            "query test transaction ordered commit",
            TrxInner::require_ordered_commit,
        )
        .unwrap_or(false)
    }

    #[inline]
    fn prepare_transaction(mut trx: Transaction) -> Result<PreparedTrx> {
        trx.terminal_started = true;
        let claim = trx.claim_terminal(TrxEntryState::Committing, "prepare active transaction")?;
        let (_entry, inner, attachment) = claim.into_parts();
        inner.prepare(attachment)
    }

    /// Add one redo log entry for tests that need a non-readonly transaction.
    #[inline]
    pub(crate) fn add_pseudo_redo_log_entry(trx: &mut Transaction) {
        use crate::catalog::USER_OBJ_ID_START;

        static PSEUDO_SYSBENCH_VAR1: [u8; 60] = [3; 60];
        static PSEUDO_SYSBENCH_VAR2: [u8; 120] = [4; 120];

        with_transaction_inner_mut(trx, "add pseudo redo log entry", |inner| {
            // Simulate one sysbench record:
            // uint64 + int32 + int32 + char(60) + char(120)
            inner.redo_mut().insert_dml(
                USER_OBJ_ID_START,
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
            )
        })
        .expect("test transaction must be active")
    }

    /// Discard transaction state for tests that construct a transaction directly.
    #[inline]
    pub(crate) fn discard_transaction_after_fatal_rollback(trx: &mut Transaction) {
        let (entry, attachment) = resolve_active_parts_for_test(trx, "discard transaction in test")
            .expect("test transaction must be active");
        let mut checkout = TrxCheckout::new(entry, attachment, "discard transaction in test")
            .expect("test checkout");
        checkout.discard_after_fatal_rollback();
    }

    #[inline]
    pub(crate) fn lock_owner(trx: &Transaction) -> Result<LockOwner> {
        with_transaction_inner(trx, "read transaction lock owner", |inner| {
            Ok(inner
                .checked_lock_state("read transaction lock owner")?
                .owner())
        })?
    }

    #[inline]
    pub(crate) fn cached_transaction_lock_covers(
        trx: &Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        with_transaction_inner(trx, "check transaction lock cache", |inner| {
            inner
                .checked_lock_state("check transaction lock cache")?
                .cached_covers(resource, mode)
        })?
    }

    #[inline]
    pub(crate) fn try_acquire_transaction_lock(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let mut checkout = trx.checkout("try acquire transaction lock")?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        try_acquire_owner_lock_state(
            inner.checked_lock_state_mut("try acquire transaction lock")?,
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

    #[inline]
    pub(crate) async fn acquire_transaction_lock(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let mut checkout = trx.checkout("acquire transaction lock")?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        inner
            .checked_lock_state_mut("acquire transaction lock")?
            .acquire(lock_manager, resource, mode)
            .await
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
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
            .table_fs
            .create_table_file(table_id, metadata, false)
            .unwrap();
        engine
            .trx_sys
            .publish_table_file_root(mutable, TrxID::new(1), false)
            .await
            .unwrap()
    }

    #[test]
    fn test_transaction_new_splits_context_and_empty_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_context_new").await;
            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 42, TrxID::new(42), 3, 5);

            let page_id = VersionedPageID {
                page_id: PageID::new(7),
                generation: 11,
            };
            {
                let mut checkout = trx
                    .checkout("test transaction context")
                    .expect("test transaction must check out");
                let (inner, attachment) = checkout.inner_and_attachment_mut();
                {
                    let rt = TrxRuntime::new(inner.ctx(), attachment);
                    rt.save_active_insert_page(TableID::new(19), page_id, RowID::new(23));
                    assert_eq!(
                        rt.load_active_insert_page(TableID::new(19)),
                        Some((page_id, RowID::new(23)))
                    );
                }
                assert!(inner.effects.row_undo.is_empty());
                assert!(inner.effects.index_undo.is_empty());
                assert!(inner.effects.redo.is_empty());
                assert!(inner.effects.gc_row_pages.is_empty());
            }
            assert!(!transaction_require_durability(&trx));
            assert!(!transaction_require_ordered_commit(&trx));
            assert_eq!(trx.sts(), TrxID::new(42));
            assert_eq!(trx.trx_id(), MIN_ACTIVE_TRX_ID + 42);
            assert_eq!(transaction_log_no(&trx), 3);
            assert_eq!(transaction_gc_no(&trx), 5);
            assert!(transaction_has_engine(&trx));
            assert!(transaction_pool_guards_attached(&trx));

            discard_transaction_after_fatal_rollback(&mut trx);
        });
    }
    #[test]
    fn test_transaction_readonly_prepare_keeps_empty_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_readonly_prepare").await;
            let trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 43, TrxID::new(43), 1, 2);

            let mut prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(!prepared.require_ordered_commit());
            let payload = prepared.payload.as_ref().unwrap();
            assert_eq!(payload.sts, TrxID::new(43));
            assert_eq!(payload.log_no, 1);
            assert_eq!(payload.gc_no, 2);
            assert!(payload.row_undo.is_empty());
            assert!(payload.index_undo.is_empty());
            assert!(payload.gc_row_pages.is_empty());

            prepared.payload.take();
            if let Some(attachment) = prepared.attachment.take() {
                attachment.rollback();
            }
            prepared.release_transaction_locks();
        });
    }

    #[test]
    fn test_transaction_prepare_moves_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_prepare").await;
            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 44, TrxID::new(44), 2, 3);
            add_pseudo_redo_log_entry(&mut trx);
            with_transaction_inner_mut(&mut trx, "test prepare payload", |inner| {
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

            let mut prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            let payload = prepared.payload.as_ref().unwrap();
            assert_eq!(payload.sts, TrxID::new(44));
            assert_eq!(payload.log_no, 2);
            assert_eq!(payload.gc_no, 3);
            assert_eq!(payload.row_undo.len(), 1);
            assert_eq!(payload.index_undo.len(), 1);
            assert!(payload.gc_row_pages.is_empty());

            prepared.redo_bin.take();
            prepared.payload.take();
            if let Some(attachment) = prepared.attachment.take() {
                attachment.rollback();
            }
            prepared.release_transaction_locks();
        });
    }

    #[test]
    fn test_statement_success_merges_statement_effects_into_transaction_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_effect_merge").await;
            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 45, TrxID::new(45), 0, 0);
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
            with_transaction_inner(&trx, "check merged statement effects", |inner| {
                assert_eq!(inner.effects.row_undo.len(), 1);
                assert_eq!(inner.effects.index_undo.len(), 1);
                assert!(!inner.effects.redo.is_empty());
            })
            .unwrap();

            discard_transaction_after_fatal_rollback(&mut trx);
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
            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 49, TrxID::new(49), 0, 0);

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

            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
            with_transaction_inner(&trx, "check statement rollback effects", |inner| {
                let table_redo = inner.effects.redo.dml.get(&TableID::new(12)).unwrap();
                assert!(table_redo.rows.contains_key(&RowID::new(23)));
                assert!(!table_redo.rows.contains_key(&RowID::new(24)));
            })
            .unwrap();

            discard_transaction_after_fatal_rollback(&mut trx);
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

            let first_owner = std::cell::Cell::new(None);
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

            let second_owner = std::cell::Cell::new(None);
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

            let error_owner = std::cell::Cell::new(None);
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
                res.unwrap_err().operation_error(),
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
                err.operation_error(),
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
                err.operation_error(),
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

            acquire_transaction_lock(&mut trx, metadata, LockMode::Shared)
                .await
                .unwrap();
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
            acquire_transaction_lock(
                &mut trx,
                LockResource::TableData(TableID::new(91_230)),
                LockMode::IntentShared,
            )
            .await
            .unwrap();
            assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            acquire_transaction_lock(
                &mut trx,
                LockResource::TableData(TableID::new(91_231)),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.begin_trx().unwrap();
            let owner = lock_owner(&trx).unwrap();
            acquire_transaction_lock(
                &mut trx,
                LockResource::TableData(TableID::new(91_232)),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            add_pseudo_redo_log_entry(&mut trx);
            assert!(trx.commit().await.unwrap() > TrxID::new(0));
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_transaction_locks_release_on_precommit_abort() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_abort").await;
            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 55, TrxID::new(55), 0, 0);
            let owner = lock_owner(&trx).unwrap();
            acquire_transaction_lock(
                &mut trx,
                LockResource::TableData(TableID::new(91_240)),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            add_pseudo_redo_log_entry(&mut trx);

            let prepared = prepare_transaction(trx).unwrap();
            let precommit = prepared.fill_cts(TrxID::new(91_241));
            precommit.abort();
            assert_eq!(lock_entry_count(&engine, owner), 0);
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
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            discard_transaction_after_fatal_rollback(&mut trx);
            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );
        });
    }

    #[test]
    fn test_transaction_effect_predicates_split_durability_from_ordering() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_predicates").await;

            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 46, TrxID::new(46), 0, 0);
            trx.extend_gc_row_pages(vec![PageID::new(46)]).unwrap();
            assert!(!transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            let mut prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.payload.take();
            if let Some(attachment) = prepared.attachment.take() {
                attachment.rollback();
            }
            prepared.release_transaction_locks();

            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 47, TrxID::new(47), 0, 0);
            with_transaction_inner_mut(&mut trx, "test index undo predicate", |inner| {
                inner.index_undo_mut().push(IndexUndo {
                    table_id: TableID::new(47),
                    row_id: RowID::new(1),
                    kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
                });
            })
            .unwrap();
            assert!(!transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            let mut prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.payload.take();
            if let Some(attachment) = prepared.attachment.take() {
                attachment.rollback();
            }
            prepared.release_transaction_locks();

            let mut trx =
                test_transaction_with_parts(&engine, MIN_ACTIVE_TRX_ID + 48, TrxID::new(48), 0, 0);
            add_pseudo_redo_log_entry(&mut trx);
            assert!(transaction_require_durability(&trx));
            assert!(transaction_require_ordered_commit(&trx));
            let mut prepared = prepare_transaction(trx).unwrap();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.redo_bin.take();
            prepared.payload.take();
            if let Some(attachment) = prepared.attachment.take() {
                attachment.rollback();
            }
            prepared.release_transaction_locks();
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
                engine.table_fs.background_writes(),
                engine.disk_pool.clone_inner(),
            );
            let table_file = engine
                .trx_sys
                .publish_table_file_root(mutable, TrxID::new(2), false)
                .await
                .unwrap();
            assert_eq!(table_file.active_root_unchecked().root_ts, TrxID::new(2));

            for _ in 0..10 {
                smol::Timer::after(std::time::Duration::from_millis(10)).await;
                assert_eq!(
                    old_root_drop_count(old_root_ptr),
                    drop_count_before,
                    "old root must stay retained while an earlier transaction is active"
                );
            }
            read_trx.commit().await.unwrap();
            engine.trx_sys.request_table_root_retention_purge();
            for _ in 0..100 {
                if old_root_drop_count(old_root_ptr) > drop_count_before {
                    return;
                }
                smol::Timer::after(std::time::Duration::from_millis(10)).await;
            }
            panic!("old root was not released after the fence crossed the purge horizon");
        });
    }
}
