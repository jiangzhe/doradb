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
pub mod sys;
pub mod sys_trx;
pub mod undo;
pub mod ver_map;

use crate::buffer::PageID;
use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::TableID;
use crate::engine::EngineRef;
use crate::error::{Error, Result};
use crate::file::table_file::OldRoot;
use crate::row::RowID;
use crate::session::SessionState;
use crate::stmt::Statement;
use crate::trx::log_replay::TrxLog;
use crate::trx::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind};
use crate::trx::undo::{IndexPurgeEntry, IndexUndoLogs, RowUndoHead, RowUndoLogs, UndoStatus};
use crate::value::Val;
use event_listener::{Event, EventListener};
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::mem;
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

/// Immutable transaction identity and runtime handles.
pub(crate) struct TrxContext {
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
        &mut self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.session
            .as_mut()
            .and_then(|session| session.load_active_insert_page(table_id))
    }

    /// Saves the cached active insert page into the attached session.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &mut self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        if let Some(session) = self.session.as_mut() {
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
            return Err(Error::OldTableRootAlreadyRetained);
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

pub struct ActiveTrx {
    ctx: TrxContext,
    effects: TrxEffects,
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

    /// Starts a statement.
    #[inline]
    pub fn start_stmt(self) -> Statement {
        Statement::new(self)
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

    /// Loads the cached active insert page from the attached session.
    #[inline]
    pub(crate) fn load_active_insert_page(
        &mut self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.ctx.load_active_insert_page(table_id)
    }

    /// Saves the cached active insert page into the attached session.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &mut self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.ctx.save_active_insert_page(table_id, page_id, row_id);
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
        self.ctx.rollback_session();
    }

    /// Prepare current transaction for committing.
    #[inline]
    pub fn prepare(mut self) -> PreparedTrx {
        // fast path for readonly transactions
        if !self.require_ordered_commit() {
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
        }
    }

    /// Commit the transaction.
    #[inline]
    pub async fn commit(self) -> Result<TrxID> {
        let engine = self.engine().cloned().unwrap();
        engine.trx_sys.commit(self).await
    }

    /// Rollback the transaction.
    #[inline]
    pub async fn rollback(self) -> Result<()> {
        let engine = self.engine().cloned().unwrap();
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
        self.ctx.assert_cleared();
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
                }
            }
            None => {
                debug_assert!(self.session.is_none());
                PrecommitTrx {
                    cts,
                    redo_bin,
                    payload: None,
                    session: None,
                }
            }
        }
    }

    /// Returns whether the prepared transaction is readonly.
    #[inline]
    pub fn readonly(&self) -> bool {
        !self.require_ordered_commit()
    }
}

impl Drop for PreparedTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.payload.is_none(), "payload should be cleared");
        assert!(self.session.is_none(), "session should be cleared");
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
        match self.payload.take() {
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
        }
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
    }
}

impl Drop for PrecommitTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.payload.is_none(), "payload should be cleared");
        assert!(self.session.is_none(), "session should be cleared");
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

        let old_root_ptr = table_file.active_root() as *const _ as usize;
        let mutable = MutableTableFile::fork(&table_file, engine.table_fs.background_writes());
        let (_table_file, old_root) = mutable.commit(2, false).await.unwrap();
        (old_root_ptr, old_root.unwrap())
    }

    #[test]
    fn test_active_trx_new_splits_context_and_empty_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_context_new").await;
            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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
            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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
        });
    }

    #[test]
    fn test_active_trx_prepare_moves_effect_payload() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_prepare").await;
            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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
        });
    }

    #[test]
    fn test_statement_success_merges_statement_effects_into_transaction_effects() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_effect_merge").await;
            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
            let trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 45, 45, 0, 0);
            let mut stmt = Statement::new(trx);
            stmt.row_undo
                .push(OwnedRowUndo::new(12, None, 23, RowUndoKind::Delete));
            stmt.index_undo.push(IndexUndo {
                table_id: 12,
                row_id: 23,
                kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
            });
            stmt.redo.insert_dml(
                12,
                RowRedo {
                    page_id: PageID::new(0),
                    row_id: 23,
                    kind: RowRedoKind::Delete,
                },
            );

            let mut trx = stmt.succeed();
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
    fn test_transaction_effect_predicates_split_durability_from_ordering() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_trx_effect_predicates").await;

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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
        });
    }

    #[test]
    fn test_old_table_root_moves_to_committed_payload_and_releases_explicitly() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_old_root_payload").await;
            let (old_root_ptr, old_root) = swapped_old_root(&engine, 91_001).await;
            let drop_count_before = old_root_drop_count(old_root_ptr);

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
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

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 12, 12, 0, 0);
            trx.retain_old_table_root(first_old_root).unwrap();

            let err = trx.retain_old_table_root(second_old_root).unwrap_err();
            assert!(matches!(err, Error::OldTableRootAlreadyRetained));
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

            let session_state = Arc::new(SessionState::new(engine.new_ref().unwrap()));
            let mut trx = ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + 11, 11, 0, 0);
            trx.retain_old_table_root(old_root).unwrap();

            let prepared = trx.prepare();
            let precommit = prepared.fill_cts(21);
            precommit.abort();

            assert_eq!(old_root_drop_count(old_root_ptr), drop_count_before + 1);
        });
    }
}
