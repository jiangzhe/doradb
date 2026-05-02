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
pub mod group;
pub mod log;
pub mod log_replay;
pub mod purge;
pub mod recover;
pub mod redo;
pub mod row;
pub mod stmt;
pub mod sys;
pub mod sys_trx;
pub mod undo;
pub mod ver_map;

use crate::buffer::PageID;
use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableCache, TableID};
use crate::engine::EngineRef;
use crate::error::{FatalError, InternalError, OperationError, Result};
use crate::file::table_file::OldRoot;
use crate::lock::{LockManager, LockMode, LockOwner, LockResource, StmtNo};
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::session::SessionState;
use crate::trx::log_replay::TrxLog;
use crate::trx::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind};
use crate::trx::stmt::{Statement, StmtEffects};
use crate::trx::undo::{IndexPurgeEntry, IndexUndoLogs, RowUndoHead, RowUndoLogs, UndoStatus};
use crate::value::Val;
use error_stack::Report;
use event_listener::{Event, EventListener};
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::ops::AsyncFnOnce;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub type TrxID = u64;
pub const INVALID_TRX_ID: TrxID = !0;
pub const MIN_SNAPSHOT_TS: TrxID = 1;
pub const MAX_SNAPSHOT_TS: TrxID = 1 << 63;
pub const SNAPSHOT_TS_MASK: TrxID = MAX_SNAPSHOT_TS - 1;
pub const MAX_COMMIT_TS: TrxID = 1 << 63;
// data without version chain will treated as its commit timestamp equals to 0
pub const GLOBAL_VISIBLE_COMMIT_TS: TrxID = 0;
// As active transaction id is always greater than STS, that means
// visibility check can be simplified to "STS is larger".
pub const MIN_ACTIVE_TRX_ID: TrxID = (1 << 63) + 1;

pub enum TrxStatus {
    // OCC, write-write conflict will abort .
    Uncommitted(TrxID),
    Committed(TrxID),
    Preparing(Sender<()>, Receiver<()>),
}

pub struct SharedTrxStatus {
    ts: AtomicU64,
    preparing: AtomicBool,
    prepare_ev: Mutex<Option<Event>>,
}

impl SharedTrxStatus {
    /// Create a new shared transaction status for given transaction id.
    #[inline]
    pub fn new(trx_id: TrxID) -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(trx_id),
            preparing: AtomicBool::new(false),
            prepare_ev: Mutex::new(None),
        }
    }

    /// Create a new transaction status that is globally visible for all
    /// transactions. This is used for transaction rollback.
    #[inline]
    pub fn global_visible() -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(GLOBAL_VISIBLE_COMMIT_TS),
            preparing: AtomicBool::new(false),
            prepare_ev: Mutex::new(None),
        }
    }

    /// Returns the timestamp of current transaction.
    #[inline]
    pub fn ts(&self) -> TrxID {
        self.ts.load(Ordering::Acquire)
    }

    /// Returns whether this transaction is preparing.
    #[inline]
    pub fn preparing(&self) -> bool {
        self.preparing.load(Ordering::Acquire)
    }

    /// Returns notifier if the transaction is in prepare phase.
    /// Prepare pahse means the transaction already get a commit timestamp
    /// but not persist its log.
    /// To avoid partial read, other transaction read the data modified by
    /// this transaction should wait for
    #[inline]
    pub fn prepare_listener(&self) -> Option<EventListener> {
        let g = self.prepare_ev.lock();
        g.as_ref().map(|event| event.listen())
    }
}

/// Returns whether the transaction is committed.
#[inline]
pub fn trx_is_committed(ts: TrxID) -> bool {
    ts < MIN_ACTIVE_TRX_ID
}

/// Returns true if the active transaction must not see the transaction even
/// if it's in prepare phase and may commit succeeds.
/// This is optimization to avoid blocking read thread.
#[inline]
pub fn trx_must_not_see_even_if_prepare(sts: TrxID, ts: TrxID) -> bool {
    // snapshot ts of read transaction is less than snapshot ts of committing transaction
    // means sts must less than cts.
    sts < (ts & SNAPSHOT_TS_MASK)
}

/// Proof that a runtime read is bound to a live transaction context.
///
/// The proof carries only the transaction-context lifetime. Callers cannot
/// construct it directly, and table runtime code uses it to gate active-root
/// binding without borrowing mutable transaction or statement effects.
pub struct TrxReadProof<'ctx> {
    _ctx: PhantomData<&'ctx TrxContext>,
}

/// Immutable transaction identity and runtime handles.
pub struct TrxContext {
    session: Option<Arc<SessionState>>,
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    log_no: usize,
    gc_no: usize,
}

impl TrxContext {
    /// Create a transaction context from session, status, and partition identity.
    #[inline]
    pub(crate) fn new(
        session: Arc<SessionState>,
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
    ) -> Self {
        TrxContext {
            session: Some(session),
            status: Arc::new(SharedTrxStatus::new(trx_id)),
            sts,
            log_no,
            gc_no,
        }
    }

    /// Returns reference of the storage engine.
    #[inline]
    pub(crate) fn engine(&self) -> Option<&EngineRef> {
        self.session.as_ref().map(|s| s.engine())
    }

    /// Returns the transaction session pool guards, if the context still owns a session.
    #[inline]
    pub(crate) fn pool_guards(&self) -> Option<&PoolGuards> {
        self.session.as_ref().map(|s| s.pool_guards())
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
    pub fn read_proof(&self) -> TrxReadProof<'_> {
        TrxReadProof { _ctx: PhantomData }
    }

    /// Marks the shared transaction status as preparing.
    #[inline]
    pub(crate) fn mark_preparing(&self) {
        let mut g = self.status.prepare_ev.lock();
        *g = Some(Event::new());
        self.status.preparing.store(true, Ordering::SeqCst);
    }

    /// Loads the cached active insert page from the attached session.
    #[inline]
    pub(crate) fn load_active_insert_page(
        &self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.session
            .as_ref()
            .and_then(|session| session.load_active_insert_page(table_id))
    }

    /// Saves the cached active insert page into the attached session.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        if let Some(session) = self.session.as_ref() {
            session.save_active_insert_page(table_id, page_id, row_id);
        }
    }

    /// Takes the attached session from the context.
    #[inline]
    pub(crate) fn take_session(&mut self) -> Option<Arc<SessionState>> {
        self.session.take()
    }

    /// Rolls back and clears the attached session if one remains.
    #[inline]
    pub(crate) fn rollback_session(&mut self) {
        if let Some(session) = self.session.take() {
            session.rollback();
        }
    }

    /// Asserts that terminal lifecycle code has cleared the session.
    #[inline]
    fn assert_cleared(&self) {
        assert!(self.session.is_none(), "session should be cleared");
    }
}

/// Mutable transaction-level effects accumulated across successful statements.
pub(crate) struct TrxEffects {
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    redo: RedoLogs,
    gc_row_pages: Vec<PageID>,
    old_table_root: Option<OldRoot>,
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
            old_table_root: None,
        }
    }

    /// Returns whether this transaction needs a recovery-visible log record.
    ///
    /// Durability is the persistent timestamp carrier used by recovery. Runtime
    /// effects such as undo cleanup, row-page GC, and retained table roots must
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
            || self.old_table_root.is_some()
    }

    /// Returns whether transaction effects are logically readonly.
    #[inline]
    pub(crate) fn readonly(&self) -> bool {
        !self.require_ordered_commit()
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

    /// Attach one swapped user-table root to this transaction for GC-horizon retention.
    ///
    /// Checkpoint transactions call this after publishing a new table-file root
    /// so the replaced root remains alive until the committed transaction is
    /// purged. A transaction may retain at most one table root.
    #[inline]
    pub(crate) fn retain_old_table_root(&mut self, old_root: OldRoot) -> Result<()> {
        if self.old_table_root.is_some() {
            return Err(Report::new(OperationError::OldTableRootAlreadyRetained)
                .attach("retain old table root: transaction effects already hold one old root")
                .into());
        }
        self.old_table_root = Some(old_root);
        Ok(())
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
                    cts: 0,
                    trx_kind: RedoTrxKind::User,
                },
                mem::take(&mut self.redo),
            ))
        }
    }

    /// Moves transaction effects into a prepared transaction payload.
    #[inline]
    pub(crate) fn take_payload_parts(
        &mut self,
    ) -> (RowUndoLogs, IndexUndoLogs, Vec<PageID>, Option<OldRoot>) {
        (
            mem::take(&mut self.row_undo),
            mem::take(&mut self.index_undo),
            mem::take(&mut self.gc_row_pages),
            self.old_table_root.take(),
        )
    }

    /// Clears effects after rollback or fatal cleanup.
    #[inline]
    pub(crate) fn clear_for_rollback(&mut self) {
        self.redo.clear();
        self.row_undo = RowUndoLogs::empty();
        self.index_undo = IndexUndoLogs::empty();
        self.gc_row_pages.clear();
        self.old_table_root.take();
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
        assert!(
            self.old_table_root.is_none(),
            "old table root should be cleared"
        );
    }
}

/// Mutable transaction-owned logical lock cache.
///
/// This state is separate from [`TrxContext`] and [`TrxEffects`]. It records
/// only lock-manager ownership and strongest granted modes, so it must not
/// influence redo durability, ordered commit classification, or rollbackable
/// statement effects.
pub(crate) struct TrxLockState {
    owner: LockOwner,
    held: HashMap<LockResource, LockMode>,
}

#[allow(dead_code)]
impl TrxLockState {
    /// Create an empty lock cache for one active transaction owner.
    #[inline]
    pub(crate) fn new(trx_id: TrxID) -> Self {
        TrxLockState {
            owner: LockOwner::Transaction(trx_id),
            held: HashMap::new(),
        }
    }

    /// Returns the transaction lock owner represented by this state.
    #[inline]
    pub(crate) fn owner(&self) -> LockOwner {
        self.owner
    }

    /// Returns whether the cached lock mode covers the requested mode.
    #[inline]
    pub(crate) fn cached_covers(&self, resource: LockResource, mode: LockMode) -> Result<bool> {
        match self.held.get(&resource).copied() {
            Some(held) => held.covers(resource, mode),
            None => {
                mode.validate_for(resource)?;
                Ok(false)
            }
        }
    }

    /// Attempts to acquire a transaction-owned lock without waiting.
    ///
    /// A covering cached mode skips the lock manager. Fresh conflicts return
    /// `Ok(false)`, while invalid modes and unsupported or blocking conversions
    /// propagate the lock-manager error.
    #[inline]
    pub(crate) fn try_acquire(
        &mut self,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        if self.cached_covers(resource, mode)? {
            return Ok(true);
        }
        if !lock_manager.try_acquire(resource, mode, self.owner)? {
            return Ok(false);
        }
        self.cache_granted(resource, mode);
        Ok(true)
    }

    /// Acquires a transaction-owned lock, waiting for fresh conflicts.
    ///
    /// Blocking conversion is still delegated to the lock manager and remains
    /// unsupported; the cache updates only after a successful grant.
    #[inline]
    pub(crate) async fn acquire(
        &mut self,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        if self.cached_covers(resource, mode)? {
            return Ok(());
        }
        lock_manager.acquire(resource, mode, self.owner).await?;
        self.cache_granted(resource, mode);
        Ok(())
    }

    #[inline]
    fn cache_granted(&mut self, resource: LockResource, mode: LockMode) {
        if let Some(held) = self.held.get(&resource).copied() {
            debug_assert!(
                mode.covers(resource, held).unwrap_or(false),
                "newly granted lock mode must cover the previous cached mode"
            );
        }
        self.held.insert(resource, mode);
    }

    /// Releases every transaction-owned lock and clears the cache.
    #[inline]
    pub(crate) fn release_all(&mut self, lock_manager: &LockManager) -> usize {
        let removed = lock_manager.release_owner(self.owner);
        self.held.clear();
        removed
    }

    /// Asserts that every cached transaction lock has been cleared.
    #[inline]
    fn assert_cleared(&self) {
        assert!(self.held.is_empty(), "transaction locks should be cleared");
    }
}

pub struct ActiveTrx {
    ctx: TrxContext,
    effects: TrxEffects,
    lock_state: Option<TrxLockState>,
    next_stmt_no: StmtNo,
}

impl ActiveTrx {
    /// Create a new transaction.
    #[inline]
    pub fn new(
        session: Arc<SessionState>,
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
    ) -> Self {
        ActiveTrx {
            ctx: TrxContext::new(session, trx_id, sts, log_no, gc_no),
            effects: TrxEffects::empty(),
            lock_state: Some(TrxLockState::new(trx_id)),
            next_stmt_no: 1,
        }
    }

    /// Returns this transaction's immutable context.
    #[inline]
    pub(crate) fn ctx(&self) -> &TrxContext {
        &self.ctx
    }

    /// Returns mutable access to this transaction's effects.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut TrxEffects {
        &mut self.effects
    }

    #[inline]
    fn discarded_err(operation: &'static str) -> crate::error::Error {
        Report::new(InternalError::ActiveTransactionDiscarded)
            .attach(format!("operation={operation}"))
            .into()
    }

    #[inline]
    fn checked_engine(&self, operation: &'static str) -> Result<EngineRef> {
        self.engine()
            .cloned()
            .ok_or_else(|| Self::discarded_err(operation))
    }

    #[inline]
    fn checked_pool_guards(&self, operation: &'static str) -> Result<&PoolGuards> {
        self.pool_guards()
            .ok_or_else(|| Self::discarded_err(operation))
    }

    #[inline]
    fn lock_manager_guard(&self) -> Option<QuiescentGuard<LockManager>> {
        self.engine().map(|engine| engine.lock_manager().clone())
    }

    #[inline]
    fn checked_lock_manager(&self, operation: &'static str) -> Result<QuiescentGuard<LockManager>> {
        self.lock_manager_guard()
            .ok_or_else(|| Self::discarded_err(operation))
    }

    #[inline]
    #[allow(dead_code)]
    fn checked_lock_state(&self, operation: &'static str) -> Result<&TrxLockState> {
        self.lock_state
            .as_ref()
            .ok_or_else(|| Self::discarded_err(operation))
    }

    #[inline]
    #[allow(dead_code)]
    fn checked_lock_state_mut(&mut self, operation: &'static str) -> Result<&mut TrxLockState> {
        self.lock_state
            .as_mut()
            .ok_or_else(|| Self::discarded_err(operation))
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

    /// Returns reference of the storage engine.
    #[inline]
    pub fn engine(&self) -> Option<&EngineRef> {
        self.ctx().engine()
    }

    /// Returns transaction session pool guards if the session is still attached.
    #[inline]
    pub fn pool_guards(&self) -> Option<&PoolGuards> {
        self.ctx().pool_guards()
    }

    /// Returns a clone of the shared transaction status handle.
    #[inline]
    pub fn status(&self) -> Arc<SharedTrxStatus> {
        self.ctx().status()
    }

    /// Returns whether the row undo head belongs to this transaction.
    #[inline]
    pub fn is_same_trx(&self, undo_head: &RowUndoHead) -> bool {
        self.ctx().is_same_trx(undo_head)
    }

    /// Returns this transaction's current status timestamp.
    #[inline]
    pub fn trx_id(&self) -> TrxID {
        self.ctx().trx_id()
    }

    /// Returns the transaction-owned logical lock owner.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn lock_owner(&self) -> Result<LockOwner> {
        Ok(self
            .checked_lock_state("read transaction lock owner")?
            .owner())
    }

    /// Returns the transaction snapshot timestamp.
    #[inline]
    pub fn sts(&self) -> TrxID {
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

    /// Returns whether the transaction lock cache covers the requested lock.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn cached_transaction_lock_covers(
        &self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        self.checked_lock_state("check transaction lock cache")?
            .cached_covers(resource, mode)
    }

    /// Attempts to acquire a transaction-owned logical lock without waiting.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_transaction_lock(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let lock_manager = self.checked_lock_manager("try acquire transaction lock")?;
        self.checked_lock_state_mut("try acquire transaction lock")?
            .try_acquire(&lock_manager, resource, mode)
    }

    /// Acquires a transaction-owned logical lock.
    #[inline]
    #[allow(dead_code)]
    pub(crate) async fn acquire_transaction_lock(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let lock_manager = self.checked_lock_manager("acquire transaction lock")?;
        self.checked_lock_state_mut("acquire transaction lock")?
            .acquire(&lock_manager, resource, mode)
            .await
    }

    /// Releases every transaction-owned logical lock.
    #[inline]
    pub(crate) fn release_transaction_locks(&mut self) -> usize {
        let Some(lock_manager) = self.lock_manager_guard() else {
            return 0;
        };
        self.lock_state
            .as_mut()
            .map_or(0, |lock_state| lock_state.release_all(&lock_manager))
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
        let lock_manager = self.checked_lock_manager("execute statement")?;
        let stmt_no = self.next_stmt_no()?;
        let stmt_owner = LockOwner::Statement(self.trx_id(), stmt_no);
        let mut effects = StmtEffects::empty();
        let res = {
            let mut stmt = Statement::new(self.ctx(), &mut effects, stmt_owner);
            f(&mut stmt).await
        };
        match res {
            Ok(value) => {
                effects.merge_into_trx(self);
                lock_manager.release_owner(stmt_owner);
                Ok(value)
            }
            Err(err) => {
                let rollback_res = self.rollback_statement_effects(&mut effects).await;
                lock_manager.release_owner(stmt_owner);
                rollback_res?;
                Err(err)
            }
        }
    }

    /// Returns whether the transaction is readonly.
    #[inline]
    pub fn readonly(&self) -> bool {
        self.effects.readonly()
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
    pub fn extend_gc_row_pages(&mut self, pages: Vec<PageID>) {
        self.effects.extend_gc_row_pages(pages);
    }

    /// Attach one swapped user-table root to this transaction for GC-horizon retention.
    ///
    /// Checkpoint transactions call this after publishing a new table-file root
    /// so the replaced root remains alive until the committed transaction is
    /// purged. A transaction may retain at most one table root.
    #[inline]
    pub(crate) fn retain_old_table_root(&mut self, old_root: OldRoot) -> Result<()> {
        self.effects.retain_old_table_root(old_root)
    }

    /// Merges successful statement effects into transaction effects.
    #[inline]
    pub(crate) fn merge_statement_effects(
        &mut self,
        row_undo: &mut RowUndoLogs,
        index_undo: &mut IndexUndoLogs,
        redo: RedoLogs,
    ) {
        self.effects
            .merge_statement_effects(row_undo, index_undo, redo);
    }

    /// Takes the attached session from the transaction context.
    #[inline]
    pub(crate) fn take_session(&mut self) -> Option<Arc<SessionState>> {
        self.ctx.take_session()
    }

    /// Clears effects and rolls back the attached session after rollback failure.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(&mut self) {
        self.effects.clear_for_rollback();
        self.release_transaction_locks();
        self.ctx.rollback_session();
    }

    /// Rolls back statement-local effects after an ordinary callback error.
    ///
    /// Index effects roll back before row effects so index entries stop
    /// pointing at uncommitted row state before row undo is unwound.
    #[inline]
    pub(crate) async fn rollback_statement_effects(
        &mut self,
        effects: &mut StmtEffects,
    ) -> Result<()> {
        let engine = self.checked_engine("rollback statement effects")?;
        let pool_guards = self
            .checked_pool_guards("rollback statement effects")?
            .clone();
        let mut table_cache = TableCache::new(engine.catalog());
        let sts = self.sts();
        if effects
            .rollback_index(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            effects.clear_for_discard();
            self.discard_after_fatal_rollback();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        if effects
            .rollback_row(&mut table_cache, &pool_guards, Some(sts))
            .await
            .is_err()
        {
            effects.clear_for_discard();
            self.discard_after_fatal_rollback();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        effects.clear_redo();
        Ok(())
    }

    /// Prepare current transaction for committing.
    #[inline]
    pub fn prepare(mut self) -> PreparedTrx {
        // fast path for readonly transactions
        if !self.require_ordered_commit() {
            let lock_manager = self.lock_manager_guard();
            // there should be no ref count of transaction status.
            debug_assert!(Arc::strong_count(&self.ctx.status) == 1);
            debug_assert!(self.effects.index_undo.is_empty());
            return PreparedTrx {
                redo_bin: None,
                payload: Some(PreparedTrxPayload {
                    status: self.ctx.status(),
                    sts: self.ctx.sts(),
                    log_no: self.ctx.log_no(),
                    gc_no: self.ctx.gc_no(),
                    row_undo: RowUndoLogs::empty(),
                    index_undo: IndexUndoLogs::empty(),
                    gc_row_pages: Vec::new(),
                    old_table_root: None,
                }),
                session: self.ctx.take_session(),
                lock_manager,
                lock_state: self.lock_state.take(),
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
        let (row_undo, index_undo, gc_row_pages, old_table_root) =
            self.effects.take_payload_parts();
        let lock_manager = self.lock_manager_guard();
        PreparedTrx {
            redo_bin,
            payload: Some(PreparedTrxPayload {
                status: self.ctx.status(),
                sts: self.ctx.sts(),
                log_no: self.ctx.log_no(),
                gc_no: self.ctx.gc_no(),
                row_undo,
                index_undo,
                gc_row_pages,
                old_table_root,
            }),
            session: self.ctx.take_session(),
            lock_manager,
            lock_state: self.lock_state.take(),
        }
    }

    /// Commit the transaction.
    #[inline]
    pub async fn commit(self) -> Result<TrxID> {
        let engine = self.checked_engine("commit active transaction")?;
        engine.trx_sys.commit(self).await
    }

    /// Rollback the transaction.
    #[inline]
    pub async fn rollback(self) -> Result<()> {
        let engine = self.checked_engine("rollback active transaction")?;
        engine.trx_sys.rollback(self).await
    }

    /// Add one redo log entry.
    /// This function is only use for test purpose.
    #[inline]
    pub fn add_pseudo_redo_log_entry(&mut self) {
        // self.trx_redo.push(RedoEntry{page_id: 0, row_id: 0, kind: RedoKind::Delete})
        // simulate sysbench record
        // uint64 + int32 + int32 + char(60) + char(120)
        self.redo_mut().insert_dml(
            0,
            RowRedo {
                page_id: PageID::new(0),
                row_id: 0,
                kind: RowRedoKind::Insert(vec![
                    Val::U64(123),
                    Val::U32(1),
                    Val::U32(2),
                    Val::from(&PSEUDO_SYSBENCH_VAR1[..]),
                    Val::from(&PSEUDO_SYSBENCH_VAR2[..]),
                ]),
            },
        )
    }
}

impl Drop for ActiveTrx {
    #[inline]
    fn drop(&mut self) {
        self.effects.assert_cleared();
        if let Some(lock_state) = self.lock_state.as_ref() {
            lock_state.assert_cleared();
        }
        self.ctx.assert_cleared();
    }
}

#[inline]
fn release_carried_transaction_locks(
    lock_state: &mut Option<TrxLockState>,
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

static PSEUDO_SYSBENCH_VAR1: [u8; 60] = [3; 60];
static PSEUDO_SYSBENCH_VAR2: [u8; 120] = [4; 120];

pub struct PreparedTrxPayload {
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    log_no: usize,
    gc_no: usize,
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    gc_row_pages: Vec<PageID>,
    old_table_root: Option<OldRoot>,
}

impl PreparedTrxPayload {
    /// Returns whether this payload has runtime effects that require ordered commit.
    #[inline]
    fn require_ordered_commit(&self) -> bool {
        !self.row_undo.is_empty()
            || !self.index_undo.is_empty()
            || !self.gc_row_pages.is_empty()
            || self.old_table_root.is_some()
    }
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PreparedTrx {
    redo_bin: Option<TrxLog>,
    payload: Option<PreparedTrxPayload>,
    session: Option<Arc<SessionState>>,
    lock_manager: Option<QuiescentGuard<LockManager>>,
    lock_state: Option<TrxLockState>,
}

impl PreparedTrx {
    #[inline]
    pub fn engine(&self) -> Option<&EngineRef> {
        self.session.as_ref().map(|s| s.engine())
    }

    /// Returns whether this transaction needs a recovery-visible log record.
    ///
    /// Recovery seeds the next timestamp from checkpoint metadata, table roots,
    /// and redo headers. A transaction that publishes durable state needing a
    /// stable CTS must therefore require durability and emit a real log record.
    #[inline]
    pub fn require_durability(&self) -> bool {
        self.redo_bin.is_some()
    }

    /// Returns whether this transaction must pass through group commit ordering.
    ///
    /// This is broader than durability: volatile runtime effects still need CTS
    /// ordering for status backfill, session completion, and GC handoff even
    /// when no log record should be written.
    #[inline]
    pub fn require_ordered_commit(&self) -> bool {
        self.require_durability()
            || self
                .payload
                .as_ref()
                .map(PreparedTrxPayload::require_ordered_commit)
                .unwrap_or(false)
    }

    #[inline]
    pub fn fill_cts(mut self, cts: TrxID) -> PrecommitTrx {
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
                old_table_root,
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
                        old_table_root,
                    }),
                    session: self.session.take(),
                    lock_manager: self.lock_manager.take(),
                    lock_state: self.lock_state.take(),
                }
            }
            None => {
                debug_assert!(self.session.is_none());
                PrecommitTrx {
                    cts,
                    redo_bin,
                    payload: None,
                    session: None,
                    lock_manager: self.lock_manager.take(),
                    lock_state: self.lock_state.take(),
                }
            }
        }
    }

    /// Returns whether the prepared transaction is readonly.
    #[inline]
    pub fn readonly(&self) -> bool {
        !self.require_ordered_commit()
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
        assert!(self.session.is_none(), "session should be cleared");
        assert!(
            self.lock_manager.is_none(),
            "lock manager should be cleared"
        );
        assert!(self.lock_state.is_none(), "lock state should be cleared");
    }
}

pub struct PrecommitTrxPayload {
    status: Arc<SharedTrxStatus>,
    pub sts: TrxID,
    pub gc_no: usize,
    pub row_undo: RowUndoLogs,
    pub index_gc: Vec<IndexPurgeEntry>,
    pub gc_row_pages: Vec<PageID>,
    old_table_root: Option<OldRoot>,
}

impl PrecommitTrxPayload {
    /// Returns whether this payload has runtime effects that require ordered commit.
    #[inline]
    fn require_ordered_commit(&self) -> bool {
        !self.row_undo.is_empty()
            || !self.index_gc.is_empty()
            || !self.gc_row_pages.is_empty()
            || self.old_table_root.is_some()
    }
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
/// There are two kinds of PrecommitTrx.
/// One is user transaction which will contains payload such as undo logs and index GC records
/// and session info.
/// The other is system transaction which will be directly dropped and has no other info.
pub struct PrecommitTrx {
    pub cts: TrxID,
    pub redo_bin: Option<TrxLog>,
    // Payload is only for user transaction
    pub payload: Option<PrecommitTrxPayload>,
    session: Option<Arc<SessionState>>,
    lock_manager: Option<QuiescentGuard<LockManager>>,
    lock_state: Option<TrxLockState>,
}

impl PrecommitTrx {
    /// Returns whether this transaction needs a recovery-visible log record.
    #[inline]
    pub fn require_durability(&self) -> bool {
        self.redo_bin.is_some()
    }

    /// Returns whether this transaction must pass through ordered commit.
    #[inline]
    pub fn require_ordered_commit(&self) -> bool {
        self.require_durability()
            || self
                .payload
                .as_ref()
                .map(PrecommitTrxPayload::require_ordered_commit)
                .unwrap_or(false)
    }

    /// Takes the recovery-visible log record, if this transaction requires one.
    #[inline]
    pub fn take_log(&mut self) -> Option<TrxLog> {
        self.redo_bin.take()
    }

    #[inline]
    pub fn take_session(&mut self) -> Option<Arc<SessionState>> {
        self.session.take()
    }

    /// Commit this transaction.
    /// The method should be invoked when redo logs have been persisted to disk.
    /// It will update backfill commit timestamp and update status to committed.
    #[inline]
    pub fn commit(mut self) -> CommittedTrx {
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
                old_table_root,
            }) => {
                // For user transaction, we need to notify readers that this transaction is committed,
                // and readers can continue their work.
                {
                    // first update cts.
                    status.ts.store(self.cts, Ordering::SeqCst);
                    // then reset preparing.
                    status.preparing.store(false, Ordering::SeqCst);
                    // finally, drop event to notify all waiting transactions.
                    let mut g = status.prepare_ev.lock();
                    drop(g.take());
                }
                if let Some(s) = self.session.take() {
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
                        old_table_root,
                    }),
                }
            }
            None => {
                debug_assert!(self.session.is_none());
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
    pub fn abort(mut self) {
        self.redo_bin.take();
        if let Some(PrecommitTrxPayload { status, .. }) = self.payload.take() {
            status.preparing.store(false, Ordering::SeqCst);
            let mut g = status.prepare_ev.lock();
            drop(g.take());
        }
        if let Some(s) = self.session.take() {
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
        assert!(self.session.is_none(), "session should be cleared");
        assert!(
            self.lock_manager.is_none(),
            "lock manager should be cleared"
        );
        assert!(self.lock_state.is_none(), "lock state should be cleared");
    }
}

pub struct CommittedTrxPayload {
    pub sts: TrxID,
    pub gc_no: usize,
    pub row_undo: RowUndoLogs,
    pub index_gc: Vec<IndexPurgeEntry>,
    pub gc_row_pages: Vec<PageID>,
    old_table_root: Option<OldRoot>,
}

pub struct CommittedTrx {
    pub cts: TrxID,
    pub payload: Option<CommittedTrxPayload>,
}

impl CommittedTrx {
    #[inline]
    pub fn sts(&self) -> Option<TrxID> {
        self.payload.as_ref().map(|p| p.sts)
    }

    /// Returns gc_no if this transaction is GC-aware.
    #[inline]
    pub fn gc_no(&self) -> Option<usize> {
        self.payload.as_ref().map(|p| p.gc_no)
    }

    #[inline]
    pub fn row_undo(&self) -> Option<&RowUndoLogs> {
        self.payload.as_ref().map(|p| &p.row_undo)
    }

    #[inline]
    pub fn index_gc(&self) -> Option<&[IndexPurgeEntry]> {
        self.payload.as_ref().map(|p| &p.index_gc[..])
    }

    #[inline]
    pub fn gc_row_pages(&self) -> Option<&[PageID]> {
        self.payload.as_ref().map(|p| &p.gc_row_pages[..])
    }

    /// Release the retained table root after this committed transaction reaches purge.
    ///
    /// Purge calls this only after the transaction's commit timestamp is below
    /// the active-snapshot horizon, making the previous table-file root
    /// unreachable by active readers.
    #[inline]
    pub(crate) fn release_old_table_root(&mut self) {
        if let Some(payload) = self.payload.as_mut() {
            payload.old_table_root.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::MutableTableFile;
    use crate::row::ops::SelectKey;
    use crate::trx::undo::{IndexUndo, IndexUndoKind, OwnedRowUndo, RowUndoKind};
    use crate::value::ValKind;
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

    fn test_session_state(engine: &Engine) -> Arc<SessionState> {
        let engine_ref = engine.new_ref().unwrap();
        let session_id = engine_ref.next_session_id();
        Arc::new(SessionState::new(engine_ref, session_id))
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        engine
            .lock_manager()
            .debug_snapshot()
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
    }

    async fn swapped_old_root(engine: &Engine, table_id: u64) -> (usize, OldRoot) {
        let metadata = Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U64,
                ColumnAttributes::empty(),
            )],
            vec![],
        ));
        let mutable = engine
            .table_fs
            .create_table_file(table_id, metadata, false)
            .unwrap();
        let (table_file, old_root) = mutable.commit(1, false).await.unwrap();
        assert!(old_root.is_none());

        let old_root_ptr = table_file.active_root_unchecked() as *const _ as usize;
        let mutable = MutableTableFile::fork(&table_file, engine.table_fs.background_writes());
        let (_table_file, old_root) = mutable.commit(2, false).await.unwrap();
        (old_root_ptr, old_root.unwrap())
    }

    #[test]
    fn test_active_trx_new_splits_context_and_empty_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_context_new").await;
            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 42, 42, 3, 5);

            assert!(trx.readonly());
            assert!(!trx.require_durability());
            assert!(!trx.require_ordered_commit());
            assert_eq!(trx.sts(), 42);
            assert_eq!(trx.trx_id(), MIN_ACTIVE_TRX_ID + 42);
            assert_eq!(trx.log_no(), 3);
            assert_eq!(trx.gc_no(), 5);
            assert!(trx.engine().is_some());
            assert!(trx.pool_guards().is_some());
            assert!(trx.effects.row_undo.is_empty());
            assert!(trx.effects.index_undo.is_empty());
            assert!(trx.effects.redo.is_empty());
            assert!(trx.effects.gc_row_pages.is_empty());
            assert!(trx.effects.old_table_root.is_none());

            trx.discard_after_fatal_rollback();
        });
    }

    #[test]
    fn test_active_trx_readonly_prepare_keeps_empty_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_readonly_prepare").await;
            let session_state = test_session_state(&engine);
            let trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 43, 43, 1, 2);

            let mut prepared = trx.prepare();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(!prepared.require_ordered_commit());
            assert!(prepared.readonly());
            let payload = prepared.payload.as_ref().unwrap();
            assert_eq!(payload.sts, 43);
            assert_eq!(payload.log_no, 1);
            assert_eq!(payload.gc_no, 2);
            assert!(payload.row_undo.is_empty());
            assert!(payload.index_undo.is_empty());
            assert!(payload.gc_row_pages.is_empty());
            assert!(payload.old_table_root.is_none());

            prepared.payload.take();
            prepared.session.take();
            prepared.release_transaction_locks();
        });
    }

    #[test]
    fn test_active_trx_prepare_moves_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_prepare").await;
            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 44, 44, 2, 3);
            trx.add_pseudo_redo_log_entry();
            trx.row_undo_mut()
                .push(OwnedRowUndo::new(11, None, 22, RowUndoKind::Delete));
            trx.index_undo_mut().push(IndexUndo {
                table_id: 11,
                row_id: 22,
                kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
            });

            let mut prepared = trx.prepare();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            assert!(!prepared.readonly());
            let payload = prepared.payload.as_ref().unwrap();
            assert_eq!(payload.sts, 44);
            assert_eq!(payload.log_no, 2);
            assert_eq!(payload.gc_no, 3);
            assert_eq!(payload.row_undo.len(), 1);
            assert_eq!(payload.index_undo.len(), 1);
            assert!(payload.gc_row_pages.is_empty());
            assert!(payload.old_table_root.is_none());

            prepared.redo_bin.take();
            prepared.payload.take();
            prepared.session.take();
            prepared.release_transaction_locks();
        });
    }

    #[test]
    fn test_statement_success_merges_statement_effects_into_transaction_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_effect_merge").await;
            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 45, 45, 0, 0);
            trx.exec(async |stmt| {
                let effects = stmt.effects_mut();
                effects.push_row_undo(OwnedRowUndo::new(12, None, 23, RowUndoKind::Delete));
                effects.push_delete_index_undo(12, 23, SelectKey::new(0, vec![]), true);
                effects.insert_row_redo(
                    12,
                    RowRedo {
                        page_id: PageID::new(0),
                        row_id: 23,
                        kind: RowRedoKind::Delete,
                    },
                );
                Ok(())
            })
            .await
            .unwrap();
            assert!(!trx.readonly());
            assert!(trx.require_durability());
            assert!(trx.require_ordered_commit());
            assert_eq!(trx.effects.row_undo.len(), 1);
            assert_eq!(trx.effects.index_undo.len(), 1);
            assert!(!trx.effects.redo.is_empty());

            trx.discard_after_fatal_rollback();
        });
    }

    #[test]
    fn test_statement_error_rolls_back_only_statement_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_error_rollback").await;
            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 49, 49, 0, 0);

            trx.exec(async |stmt| {
                stmt.effects_mut().insert_row_redo(
                    12,
                    RowRedo {
                        page_id: PageID::new(0),
                        row_id: 23,
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
                        12,
                        RowRedo {
                            page_id: PageID::new(0),
                            row_id: 24,
                            kind: RowRedoKind::Delete,
                        },
                    );
                    Err(Report::new(OperationError::NotSupported).into())
                })
                .await;
            let err = res.unwrap_err();

            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
            let table_redo = trx.effects.redo.dml.get(&12).unwrap();
            assert!(table_redo.rows.contains_key(&23));
            assert!(!table_redo.rows.contains_key(&24));

            trx.discard_after_fatal_rollback();
        });
    }

    #[test]
    fn test_statement_locks_release_without_releasing_transaction_locks() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_lock_release").await;
            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let trx_owner = trx.lock_owner().unwrap();
            let trx_resource = LockResource::TableData(91_210);
            assert!(
                trx.try_acquire_transaction_lock(trx_resource, LockMode::IntentExclusive)
                    .unwrap()
            );

            let first_owner = std::cell::Cell::new(None);
            trx.exec(async |stmt| {
                first_owner.set(Some(stmt.lock_owner()));
                stmt.acquire_statement_lock(LockResource::TableMetadata(91_210), LockMode::Shared)
                    .await?;
                Ok(())
            })
            .await
            .unwrap();

            let second_owner = std::cell::Cell::new(None);
            trx.exec(async |stmt| {
                second_owner.set(Some(stmt.lock_owner()));
                assert!(stmt.try_acquire_statement_lock(
                    LockResource::TableMetadata(91_211),
                    LockMode::Shared,
                )?);
                Ok(())
            })
            .await
            .unwrap();

            let first_owner = first_owner.get().unwrap();
            let second_owner = second_owner.get().unwrap();
            assert_eq!(first_owner, LockOwner::Statement(trx.trx_id(), 1));
            assert_eq!(second_owner, LockOwner::Statement(trx.trx_id(), 2));
            assert_eq!(lock_entry_count(&engine, first_owner), 0);
            assert_eq!(lock_entry_count(&engine, second_owner), 0);
            assert_eq!(lock_entry_count(&engine, trx_owner), 1);

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, trx_owner), 0);
        });
    }

    #[test]
    fn test_transaction_lock_cache_skips_covered_requests_and_preserves_errors() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_cache").await;
            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let owner = trx.lock_owner().unwrap();
            let data = LockResource::TableData(91_220);

            assert!(
                trx.try_acquire_transaction_lock(data, LockMode::IntentExclusive)
                    .unwrap()
            );
            assert!(
                trx.cached_transaction_lock_covers(data, LockMode::IntentShared)
                    .unwrap()
            );
            assert!(
                trx.try_acquire_transaction_lock(data, LockMode::IntentShared)
                    .unwrap()
            );
            assert_eq!(lock_entry_count(&engine, owner), 1);

            let err = trx
                .try_acquire_transaction_lock(data, LockMode::Shared)
                .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockConversionNotSupported)
            );
            assert_eq!(lock_entry_count(&engine, owner), 1);

            let metadata = LockResource::TableMetadata(91_221);
            assert!(
                trx.try_acquire_transaction_lock(metadata, LockMode::Shared)
                    .unwrap()
            );
            assert!(
                engine
                    .lock_manager()
                    .try_acquire(metadata, LockMode::Shared, LockOwner::Session(91_221))
                    .unwrap()
            );
            let err = trx
                .try_acquire_transaction_lock(metadata, LockMode::Exclusive)
                .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockUpgradeWouldBlock)
            );
            engine
                .lock_manager()
                .release_owner(LockOwner::Session(91_221));

            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_transaction_locks_release_on_readonly_commit_rollback_and_ordered_commit() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_terminal").await;

            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let owner = trx.lock_owner().unwrap();
            trx.acquire_transaction_lock(LockResource::TableData(91_230), LockMode::IntentShared)
                .await
                .unwrap();
            assert!(trx.readonly());
            assert_eq!(trx.commit().await.unwrap(), 0);
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let owner = trx.lock_owner().unwrap();
            trx.acquire_transaction_lock(
                LockResource::TableData(91_231),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            trx.rollback().await.unwrap();
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let owner = trx.lock_owner().unwrap();
            trx.acquire_transaction_lock(
                LockResource::TableData(91_232),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            trx.add_pseudo_redo_log_entry();
            assert!(trx.commit().await.unwrap() > 0);
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_transaction_locks_release_on_precommit_abort() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_lock_abort").await;
            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 55, 55, 0, 0);
            let owner = trx.lock_owner().unwrap();
            trx.acquire_transaction_lock(
                LockResource::TableData(91_240),
                LockMode::IntentExclusive,
            )
            .await
            .unwrap();
            trx.add_pseudo_redo_log_entry();

            let prepared = trx.prepare();
            let precommit = prepared.fill_cts(91_241);
            precommit.abort();
            assert_eq!(lock_entry_count(&engine, owner), 0);
        });
    }

    #[test]
    fn test_commit_and_rollback_after_fatal_discard_return_error() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_discard_errors").await;

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 50, 50, 0, 0);
            trx.discard_after_fatal_rollback();
            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 51, 51, 0, 0);
            trx.discard_after_fatal_rollback();
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

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 46, 46, 0, 0);
            trx.extend_gc_row_pages(vec![PageID::new(46)]);
            assert!(!trx.require_durability());
            assert!(trx.require_ordered_commit());
            let mut prepared = trx.prepare();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.payload.take();
            prepared.session.take();
            prepared.release_transaction_locks();

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 47, 47, 0, 0);
            trx.index_undo_mut().push(IndexUndo {
                table_id: 47,
                row_id: 1,
                kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
            });
            assert!(!trx.require_durability());
            assert!(trx.require_ordered_commit());
            let mut prepared = trx.prepare();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.payload.take();
            prepared.session.take();
            prepared.release_transaction_locks();

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 48, 48, 0, 0);
            trx.add_pseudo_redo_log_entry();
            assert!(trx.require_durability());
            assert!(trx.require_ordered_commit());
            let mut prepared = trx.prepare();
            assert!(prepared.redo_bin.is_some());
            assert!(prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            prepared.redo_bin.take();
            prepared.payload.take();
            prepared.session.take();
            prepared.release_transaction_locks();
        });
    }

    #[test]
    fn test_old_table_root_moves_to_committed_payload_and_releases_explicitly() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_old_root_payload").await;
            let (old_root_ptr, old_root) = swapped_old_root(&engine, 91_001).await;
            let drop_count_before = old_root_drop_count(old_root_ptr);

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 10, 10, 0, 0);
            trx.retain_old_table_root(old_root).unwrap();
            assert!(!trx.readonly());

            let prepared = trx.prepare();
            assert!(prepared.redo_bin.is_none());
            assert!(!prepared.require_durability());
            assert!(prepared.require_ordered_commit());
            assert!(prepared.payload.as_ref().unwrap().old_table_root.is_some());

            let precommit = prepared.fill_cts(20);
            assert!(!precommit.require_durability());
            assert!(precommit.require_ordered_commit());
            assert!(precommit.payload.as_ref().unwrap().old_table_root.is_some());

            let mut committed = precommit.commit();
            assert_eq!(old_root_drop_count(old_root_ptr), drop_count_before);
            assert!(committed.payload.as_ref().unwrap().old_table_root.is_some());

            committed.release_old_table_root();
            assert_eq!(old_root_drop_count(old_root_ptr), drop_count_before + 1);
        });
    }

    #[test]
    fn test_effects_only_commit_uses_ordered_barrier_without_log_bytes() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_effects_only_ordered_commit").await;
            let (_old_root_ptr, old_root) = swapped_old_root(&engine, 91_006).await;
            let stats_before = engine.trx_sys.trx_sys_stats();

            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.retain_old_table_root(old_root).unwrap();
            assert!(!trx.require_durability());
            assert!(trx.require_ordered_commit());

            let cts = trx.commit().await.unwrap();
            assert!(cts >= MIN_SNAPSHOT_TS);
            assert!(!session.in_trx());

            for _ in 0..100 {
                let stats_after = engine.trx_sys.trx_sys_stats();
                if stats_after.commit_count > stats_before.commit_count {
                    assert_eq!(stats_after.log_bytes, stats_before.log_bytes);
                    assert_eq!(stats_after.sync_count, stats_before.sync_count);
                    assert!(stats_after.trx_count > stats_before.trx_count);
                    return;
                }
                smol::Timer::after(std::time::Duration::from_millis(10)).await;
            }
            panic!("effects-only commit stats were not updated before timeout");
        });
    }

    #[test]
    fn test_old_table_root_retain_rejects_duplicate_with_dedicated_error() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_old_root_duplicate").await;
            let (first_old_root_ptr, first_old_root) = swapped_old_root(&engine, 91_004).await;
            let (second_old_root_ptr, second_old_root) = swapped_old_root(&engine, 91_005).await;
            let first_drop_count_before = old_root_drop_count(first_old_root_ptr);
            let second_drop_count_before = old_root_drop_count(second_old_root_ptr);

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 12, 12, 0, 0);
            trx.retain_old_table_root(first_old_root).unwrap();

            let err = trx.retain_old_table_root(second_old_root).unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::OldTableRootAlreadyRetained)
            );
            assert_eq!(
                old_root_drop_count(first_old_root_ptr),
                first_drop_count_before
            );
            assert_eq!(
                old_root_drop_count(second_old_root_ptr),
                second_drop_count_before + 1
            );

            trx.discard_after_fatal_rollback();
            assert_eq!(
                old_root_drop_count(first_old_root_ptr),
                first_drop_count_before + 1
            );
        });
    }

    #[test]
    fn test_old_table_root_drops_on_active_rollback() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_old_root_active_rollback").await;
            let (old_root_ptr, old_root) = swapped_old_root(&engine, 91_002).await;
            let drop_count_before = old_root_drop_count(old_root_ptr);

            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx.retain_old_table_root(old_root).unwrap();
            trx.rollback().await.unwrap();

            assert_eq!(old_root_drop_count(old_root_ptr), drop_count_before + 1);
        });
    }

    #[test]
    fn test_old_table_root_drops_on_precommit_abort() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_old_root_precommit_abort").await;
            let (old_root_ptr, old_root) = swapped_old_root(&engine, 91_003).await;
            let drop_count_before = old_root_drop_count(old_root_ptr);

            let session_state = test_session_state(&engine);
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 11, 11, 0, 0);
            trx.retain_old_table_root(old_root).unwrap();

            let prepared = trx.prepare();
            let precommit = prepared.fill_cts(21);
            precommit.abort();

            assert_eq!(old_root_drop_count(old_root_ptr), drop_count_before + 1);
        });
    }
}
