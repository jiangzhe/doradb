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
//!       are latest. So directly read data and return.
//!    b) otherwise, check if queried RowID exists in the map. if not, same as a).
//!    c) If exists, check the timestamp in entry head. If it's larger than current STS, means
//!       it's invisible, undo change and go to next version in the chain...
//!    d) If less than current STS, return current version.
pub mod group;
pub mod log;
pub mod purge;
pub mod redo;
pub mod row;
pub mod sys;
pub mod undo;

use crate::notify::{Notify, Signal};
use crate::session::{InternalSession, IntoSession, Session};
use crate::stmt::Statement;
use crate::trx::redo::{RedoBin, RedoEntry, RedoKind, RedoLog};
use crate::trx::undo::{IndexPurge, IndexUndoLogs, RowUndoLogs};
use crate::value::Val;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

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
    prepare_signal: Mutex<Option<Signal>>,
}

impl SharedTrxStatus {
    /// Create a new shared transaction status for given transaction id.
    #[inline]
    pub fn new(trx_id: TrxID) -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(trx_id),
            preparing: AtomicBool::new(false),
            prepare_signal: Mutex::new(None),
        }
    }

    /// Create a new transaction status that is globally visible for all
    /// transactions. This is used for transaction rollback.
    #[inline]
    pub fn global_visible() -> Self {
        SharedTrxStatus {
            ts: AtomicU64::new(GLOBAL_VISIBLE_COMMIT_TS),
            preparing: AtomicBool::new(false),
            prepare_signal: Mutex::new(None),
        }
    }

    /// Returns the timestamp of current transaction.
    #[inline]
    pub fn ts(&self) -> TrxID {
        self.ts.load(Ordering::Relaxed)
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
    pub fn prepare_notify(&self) -> Option<Notify> {
        let g = self.prepare_signal.lock();
        g.as_ref().map(|signal| signal.new_notify())
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

pub struct ActiveTrx {
    // Status of the transaction.
    // Every undo log will refer to this object on heap.
    // There are nested pointers to allow atomic update on status when
    // the transaction enters commit phase.
    status: Arc<SharedTrxStatus>,
    // Snapshot timestamp.
    pub sts: TrxID,
    // which log partition it belongs to.
    pub log_no: usize,
    // which GC bucket it belongs to.
    pub gc_no: usize,
    // transaction-level undo logs of row data.
    pub(crate) row_undo: RowUndoLogs,
    // transaction-level index undo operations.
    pub(crate) index_undo: IndexUndoLogs,
    // transaction-level redo logs.
    pub(crate) redo: Vec<RedoEntry>,
    // session of current transaction.
    pub session: Option<Box<InternalSession>>,
}

impl ActiveTrx {
    /// Create a new transaction.
    #[inline]
    pub fn new(
        session: Option<Box<InternalSession>>,
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
    ) -> Self {
        ActiveTrx {
            status: Arc::new(SharedTrxStatus::new(trx_id)),
            sts,
            log_no,
            gc_no,
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: vec![],
            session,
        }
    }

    #[inline]
    pub fn status(&self) -> Arc<SharedTrxStatus> {
        self.status.clone()
    }

    #[inline]
    pub fn is_same_trx(&self, other: &SharedTrxStatus) -> bool {
        std::ptr::addr_eq(self.status.as_ref(), other)
    }

    #[inline]
    pub fn trx_id(&self) -> TrxID {
        self.status.ts()
    }

    /// Starts a statement.
    #[inline]
    pub fn start_stmt(self) -> Statement {
        Statement::new(self)
    }

    /// Returns whether the transaction is readonly.
    #[inline]
    pub fn readonly(&self) -> bool {
        self.redo.is_empty() && self.row_undo.is_empty()
    }

    /// Prepare current transaction for committing.
    #[inline]
    pub fn prepare(mut self) -> PreparedTrx {
        // fast path for readonly transactions
        if self.readonly() {
            // there should be no ref count of transaction status.
            debug_assert!(Arc::strong_count(&self.status) == 1);
            debug_assert!(self.index_undo.is_empty());
            return PreparedTrx {
                status: Arc::clone(&self.status),
                sts: self.sts,
                log_no: self.log_no,
                gc_no: self.gc_no,
                redo_bin: None,
                row_undo: RowUndoLogs::empty(),
                index_undo: IndexUndoLogs::empty(),
                session: self.session.take(),
            };
        }

        // change transaction status
        {
            let mut g = self.status.prepare_signal.lock();
            *g = Some(Signal::default());
            self.status.preparing.store(true, Ordering::SeqCst);
        }
        // use bincode to serialize redo log
        let redo_bin = if self.redo.is_empty() {
            None
        } else {
            // todo: use customized serialization method, and keep CTS placeholder.
            let redo_log = RedoLog {
                cts: INVALID_TRX_ID,
                data: mem::take(&mut self.redo),
            };
            let redo_bin = bincode::serde::encode_to_vec(&redo_log, bincode::config::standard())
                .expect("redo serialization should not fail");
            Some(redo_bin)
        };
        let row_undo = mem::take(&mut self.row_undo);
        let index_undo = mem::take(&mut self.index_undo);
        PreparedTrx {
            status: self.status.clone(),
            sts: self.sts,
            log_no: self.log_no,
            gc_no: self.gc_no,
            redo_bin,
            row_undo,
            index_undo,
            session: self.session.take(),
        }
    }

    /// Add one redo log entry.
    /// This function is only use for test purpose.
    #[inline]
    pub fn add_pseudo_redo_log_entry(&mut self) {
        // self.trx_redo.push(RedoEntry{page_id: 0, row_id: 0, kind: RedoKind::Delete})
        // simulate sysbench record
        // uint64 + int32 + int32 + char(60) + char(120)
        self.redo.push(RedoEntry {
            page_id: 0,
            row_id: 0,
            kind: RedoKind::Insert(vec![
                Val::Byte8(123),
                Val::Byte4(1),
                Val::Byte4(2),
                Val::from(&PSEUDO_SYSBENCH_VAR1[..]),
                Val::from(&PSEUDO_SYSBENCH_VAR2[..]),
            ]),
        })
    }
}

impl Drop for ActiveTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo.is_empty(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_undo.is_empty(), "index undo should be cleared");
    }
}

impl IntoSession for ActiveTrx {
    #[inline]
    fn into_session(mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }

    #[inline]
    fn split_session(&mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }
}

static PSEUDO_SYSBENCH_VAR1: [u8; 60] = [3; 60];
static PSEUDO_SYSBENCH_VAR2: [u8; 120] = [4; 120];

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PreparedTrx {
    status: Arc<SharedTrxStatus>,
    sts: TrxID,
    log_no: usize,
    gc_no: usize,
    redo_bin: Option<RedoBin>,
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    session: Option<Box<InternalSession>>,
}

impl PreparedTrx {
    #[inline]
    pub fn fill_cts(mut self, cts: TrxID) -> PrecommitTrx {
        let redo_bin = if let Some(redo_bin) = self.redo_bin.take() {
            // todo: fill cts into binary log.
            Some(redo_bin)
        } else {
            None
        };
        let row_undo = mem::take(&mut self.row_undo);
        // Once we get a concrete CTS, we won't rollback this transaction.
        // So we convert IndexUndo to IndexGC, and let purge threads to
        // remove unused index entries.
        // let index_undo = mem::take(&mut self.index_undo);
        let index_gc = self.index_undo.commit_for_gc();
        PrecommitTrx {
            status: Arc::clone(&self.status),
            sts: self.sts,
            cts,
            gc_no: self.gc_no,
            redo_bin,
            row_undo,
            index_gc,
            session: self.session.take(),
        }
    }

    /// Returns whether the prepared transaction is readonly.
    #[inline]
    pub fn readonly(&self) -> bool {
        self.redo_bin.is_none() && self.row_undo.is_empty()
    }
}

impl IntoSession for PreparedTrx {
    #[inline]
    fn into_session(mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }

    #[inline]
    fn split_session(&mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }
}

impl Drop for PreparedTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_undo.is_empty(), "index undo should be cleared");
    }
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PrecommitTrx {
    status: Arc<SharedTrxStatus>,
    pub sts: TrxID,
    pub cts: TrxID,
    pub gc_no: usize,
    pub redo_bin: Option<RedoBin>,
    pub row_undo: RowUndoLogs,
    pub index_gc: Vec<IndexPurge>,
    session: Option<Box<InternalSession>>,
}

impl PrecommitTrx {
    #[inline]
    pub fn redo_bin_len(&self) -> usize {
        self.redo_bin.as_ref().map(|bin| bin.len()).unwrap_or(0)
    }

    /// Commit this transaction, the only thing to do is replace ongoing transaction ids
    /// in undo log with cts, and this is atomic operation.
    #[inline]
    pub fn commit(mut self) -> CommittedTrx {
        assert!(self.redo_bin.is_none()); // redo log should be already processed by logger.
                                          // release the prepare notifier in transaction status
        {
            self.status.ts.store(self.cts, Ordering::SeqCst);
            self.status.preparing.store(false, Ordering::SeqCst);
            let mut g = self.status.prepare_signal.lock();
            drop(g.take());
        }
        let row_undo = mem::take(&mut self.row_undo);
        let index_gc = mem::take(&mut self.index_gc);
        CommittedTrx {
            sts: self.sts,
            cts: self.cts,
            gc_no: self.gc_no,
            row_undo,
            index_gc,
            session: self.session.take(),
        }
    }
}

impl Drop for PrecommitTrx {
    #[inline]
    fn drop(&mut self) {
        assert!(self.redo_bin.is_none(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_gc.is_empty(), "index gc should be cleared");
    }
}

impl IntoSession for PrecommitTrx {
    #[inline]
    fn into_session(mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }

    #[inline]
    fn split_session(&mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }
}

pub struct CommittedTrx {
    pub sts: TrxID,
    pub cts: TrxID,
    pub gc_no: usize,
    pub row_undo: RowUndoLogs,
    pub index_gc: Vec<IndexPurge>,
    session: Option<Box<InternalSession>>,
}

impl IntoSession for CommittedTrx {
    #[inline]
    fn into_session(mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }

    #[inline]
    fn split_session(&mut self) -> Session {
        Session::with_internal_session(self.session.take().unwrap())
    }
}
