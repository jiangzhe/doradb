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
pub mod sys_conf;
pub mod sys_trx;
pub mod undo;
pub mod ver_map;

use crate::buffer::page::PageID;
use crate::engine::EngineRef;
use crate::error::Result;
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

pub struct ActiveTrx {
    pub(crate) session: Option<Arc<SessionState>>,
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
    pub(crate) redo: RedoLogs,
    // row pages to be GCed after commit.
    pub(crate) gc_row_pages: Vec<PageID>,
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
            session: Some(session),
            status: Arc::new(SharedTrxStatus::new(trx_id)),
            sts,
            log_no,
            gc_no,
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
            gc_row_pages: Vec::new(),
        }
    }

    /// Returns reference of the storage engine.
    #[inline]
    pub fn engine(&self) -> Option<&EngineRef> {
        self.session.as_ref().map(|s| s.engine())
    }

    #[inline]
    pub fn status(&self) -> Arc<SharedTrxStatus> {
        self.status.clone()
    }

    #[inline]
    pub fn is_same_trx(&self, undo_head: &RowUndoHead) -> bool {
        match &undo_head.next.main.status {
            UndoStatus::Ref(arc) => std::ptr::addr_eq(self.status.as_ref(), arc.as_ref()),
            _ => false,
        }
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
        self.redo.is_empty() && self.row_undo.is_empty() && self.gc_row_pages.is_empty()
    }

    #[inline]
    pub fn extend_gc_row_pages(&mut self, pages: Vec<PageID>) {
        self.gc_row_pages.extend(pages);
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
                sts: Some(self.sts),
                redo_bin: None,
                payload: Some(PreparedTrxPayload {
                    status: Arc::clone(&self.status),
                    sts: self.sts,
                    log_no: self.log_no,
                    gc_no: self.gc_no,
                    row_undo: RowUndoLogs::empty(),
                    index_undo: IndexUndoLogs::empty(),
                    gc_row_pages: Vec::new(),
                }),
                session: self.session.take(),
            };
        }

        // change transaction status
        {
            let mut g = self.status.prepare_ev.lock();
            *g = Some(Event::new());
            self.status.preparing.store(true, Ordering::SeqCst);
        }
        // use bincode to serialize redo log
        let redo_bin = if self.redo.is_empty() && self.gc_row_pages.is_empty() {
            None
        } else {
            Some(TrxLog::new(
                RedoHeader {
                    cts: 0,
                    trx_kind: RedoTrxKind::User,
                },
                mem::take(&mut self.redo),
            ))
        };
        let row_undo = mem::take(&mut self.row_undo);
        let index_undo = mem::take(&mut self.index_undo);
        let gc_row_pages = mem::take(&mut self.gc_row_pages);
        PreparedTrx {
            sts: Some(self.sts),
            redo_bin,
            payload: Some(PreparedTrxPayload {
                status: self.status.clone(),
                sts: self.sts,
                log_no: self.log_no,
                gc_no: self.gc_no,
                row_undo,
                index_undo,
                gc_row_pages,
            }),
            session: self.session.take(),
        }
    }

    /// Commit the transaction.
    #[inline]
    pub async fn commit(self) -> Result<TrxID> {
        let engine = self.engine().cloned().unwrap();
        engine.trx_sys.commit(self, engine.mem_pool).await
    }

    /// Rollback the transaction.
    #[inline]
    pub async fn rollback(self) {
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
        self.redo.insert_dml(
            0,
            RowRedo {
                page_id: 0,
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
        assert!(self.redo.is_empty(), "redo should be cleared");
        assert!(self.row_undo.is_empty(), "row undo should be cleared");
        assert!(self.index_undo.is_empty(), "index undo should be cleared");
        assert!(
            self.gc_row_pages.is_empty(),
            "gc row pages should be cleared"
        );
        assert!(self.session.is_none(), "session should be cleared");
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
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PreparedTrx {
    sts: Option<TrxID>,
    redo_bin: Option<TrxLog>,
    payload: Option<PreparedTrxPayload>,
    session: Option<Arc<SessionState>>,
}

impl PreparedTrx {
    #[inline]
    pub fn engine(&self) -> Option<&EngineRef> {
        self.session.as_ref().map(|s| s.engine())
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
        self.redo_bin.is_none()
            && self
                .payload
                .as_ref()
                .map(|p| p.row_undo.is_empty() && p.gc_row_pages.is_empty())
                .unwrap_or(true)
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
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) fn remove_files(file_pattern: &str) {
        let files = glob::glob(file_pattern);
        if files.is_err() {
            return;
        }
        for f in files.unwrap() {
            if f.is_err() {
                continue;
            }
            let fp = f.unwrap();
            let _ = std::fs::remove_file(&fp);
        }
    }
}
