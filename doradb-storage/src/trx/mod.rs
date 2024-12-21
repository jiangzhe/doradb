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
pub mod redo;
pub mod sys;
pub mod undo;

use crate::buffer::guard::PageGuard;
use crate::buffer::FixedBufferPool;
use crate::latch::LatchFallbackMode;
use crate::row::RowPage;
use crate::trx::redo::{RedoBin, RedoEntry, RedoKind, RedoLog};
use crate::trx::undo::SharedUndoEntry;
use crate::value::Val;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type TrxID = u64;
pub const INVALID_TRX_ID: TrxID = !0;
pub const MIN_SNAPSHOT_TS: TrxID = 1;
pub const MAX_SNAPSHOT_TS: TrxID = 1 << 63;
pub const MAX_COMMIT_TS: TrxID = 1 << 63;
// As active transaction id is always greater than STS, that means
// visibility check can be simplified to "STS is larger".
pub const MIN_ACTIVE_TRX_ID: TrxID = (1 << 63) + 1;

pub struct ActiveTrx {
    pub trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    // which log partition it belongs to.
    pub log_partition_idx: usize,
    // transaction-level undo logs.
    trx_undo: Vec<SharedUndoEntry>,
    // statement-level undo logs.
    pub stmt_undo: Vec<SharedUndoEntry>,
    // transaction-level redo logs.
    trx_redo: Vec<RedoEntry>,
    // statement-level redo logs.
    pub stmt_redo: Vec<RedoEntry>,
}

impl ActiveTrx {
    /// Create a new transaction.
    #[inline]
    pub fn new(trx_id: TrxID, sts: TrxID) -> Self {
        ActiveTrx {
            trx_id: Arc::new(AtomicU64::new(trx_id)),
            sts,
            log_partition_idx: 0,
            trx_undo: vec![],
            stmt_undo: vec![],
            trx_redo: vec![],
            stmt_redo: vec![],
        }
    }

    #[inline]
    pub fn set_log_partition(&mut self, log_partition_idx: usize) {
        self.log_partition_idx = log_partition_idx;
    }

    /// Returns transaction id of current transaction.
    #[inline]
    pub fn trx_id(&self) -> TrxID {
        self.trx_id.load(Ordering::Acquire)
    }

    /// Starts a statement.
    #[inline]
    pub fn start_stmt(&mut self) {
        debug_assert!(self.stmt_undo.is_empty());
        debug_assert!(self.stmt_redo.is_empty());
    }

    /// Ends a statement.
    #[inline]
    pub fn end_stmt(&mut self) {
        self.trx_undo.extend(self.stmt_undo.drain(..));
        self.trx_redo.extend(self.stmt_redo.drain(..));
    }

    /// Rollback a statement.
    #[inline]
    pub fn rollback_stmt(&mut self, buf_pool: &FixedBufferPool) {
        while let Some(undo) = self.stmt_undo.pop() {
            let page_guard: PageGuard<'_, RowPage> = buf_pool
                .get_page(undo.page_id, LatchFallbackMode::Exclusive)
                .expect("get page for undo should not fail");
            let page_guard = page_guard.block_until_exclusive();
            todo!()
        }
    }

    /// Prepare current transaction for committing.
    #[inline]
    pub fn prepare(self) -> PreparedTrx {
        debug_assert!(self.stmt_undo.is_empty());
        debug_assert!(self.stmt_redo.is_empty());
        // use bincode to serialize redo log
        let redo_bin = if self.trx_redo.is_empty() {
            // todo: only for test
            // Some(vec![])
            None
        } else {
            // todo: use customized serialization method, and keep CTS placeholder.
            let redo_log = RedoLog {
                cts: INVALID_TRX_ID,
                data: self.trx_redo,
            };
            let redo_bin = bincode::serde::encode_to_vec(&redo_log, bincode::config::standard())
                .expect("redo serialization should not fail");
            Some(redo_bin)
        };
        PreparedTrx {
            trx_id: self.trx_id,
            sts: self.sts,
            // cts,
            redo_bin,
            undo: self.trx_undo,
        }
    }

    /// Rollback current transaction.
    #[inline]
    pub fn rollback(self) {
        todo!()
    }

    /// Add one redo log entry.
    /// This function is only use for test purpose.
    #[inline]
    pub fn add_pseudo_redo_log_entry(&mut self) {
        // self.trx_redo.push(RedoEntry{page_id: 0, row_id: 0, kind: RedoKind::Delete})
        // simulate sysbench record
        // uint64 + int32 + int32 + char(60) + char(120)
        self.trx_redo.push(RedoEntry {
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

static PSEUDO_SYSBENCH_VAR1: [u8; 60] = [3; 60];
static PSEUDO_SYSBENCH_VAR2: [u8; 120] = [4; 120];

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PreparedTrx {
    trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    pub redo_bin: Option<RedoBin>,
    pub undo: Vec<SharedUndoEntry>,
}

impl PreparedTrx {
    #[inline]
    pub fn fill_cts(self, cts: TrxID) -> PrecommitTrx {
        let mut redo_bin = self.redo_bin;
        if let Some(redo_bin) = redo_bin.as_mut() {
            // todo: fill cts into binary log.
        }
        PrecommitTrx {
            trx_id: self.trx_id,
            sts: self.sts,
            cts,
            redo_bin,
            undo: self.undo,
        }
    }
}

/// PrecommitTrx has been assigned commit timestamp and already prepared redo log binary.
pub struct PrecommitTrx {
    trx_id: Arc<AtomicU64>,
    pub sts: TrxID,
    pub cts: TrxID,
    pub redo_bin: Option<RedoBin>,
    pub undo: Vec<SharedUndoEntry>,
}

impl PrecommitTrx {
    #[inline]
    pub fn redo_bin_len(&self) -> usize {
        self.redo_bin.as_ref().map(|bin| bin.len()).unwrap_or(0)
    }

    /// Commit this transaction, the only thing to do is replace ongoing transaction ids
    /// in undo log with cts, and this is atomic operation.
    #[inline]
    pub fn commit(self) -> CommittedTrx {
        assert!(self.redo_bin.is_none()); // redo log should be already processed by logger.
        self.trx_id.store(self.cts, Ordering::SeqCst);
        CommittedTrx {
            sts: self.sts,
            cts: self.cts,
            undo: self.undo,
        }
    }
}

pub struct CommittedTrx {
    pub sts: TrxID,
    pub cts: TrxID,
    pub undo: Vec<SharedUndoEntry>,
}
