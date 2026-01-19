use crate::file::SparseFile;
use crate::io::{IocbRawPtr, pwrite};
use crate::notify::EventNotifyOnDrop;
use crate::serde::Ser;
use crate::session::SessionState;
use crate::trx::log::SyncGroup;
use crate::trx::log_replay::LogBuf;
use crate::trx::{PrecommitTrx, TrxID};
use event_listener::EventListener;
use parking_lot::{Condvar, Mutex, MutexGuard, WaitTimeoutResult};
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// GroupCommit with mutex and condition variable.
pub(super) struct MutexGroupCommit {
    mu: Mutex<GroupCommit>,
    cv: Condvar,
}

impl MutexGroupCommit {
    /// Create a new group commit with mutex and condition variable.
    #[inline]
    pub fn new(group_commit: GroupCommit) -> Self {
        MutexGroupCommit {
            mu: Mutex::new(group_commit),
            cv: Condvar::new(),
        }
    }

    /// Acquire lock.
    /// Return lock guard of group commit.
    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, GroupCommit> {
        self.mu.lock()
    }

    /// Notify one waiter.
    #[inline]
    pub fn notify_one(&self) -> bool {
        self.cv.notify_one()
    }

    /// Wait on conditional variable with timeout.
    #[inline]
    pub fn wait_for(
        &self,
        g: &mut MutexGuard<GroupCommit>,
        timeout: Duration,
    ) -> WaitTimeoutResult {
        self.cv.wait_for(g, timeout)
    }
}

/// GroupCommit is optimization to group multiple transactions
/// and perform single IO to speed up overall commit performance.
pub(super) struct GroupCommit {
    // Commit group queue, there can be multiple groups in commit phase.
    // Each of them submit IO request to AIO manager and then wait for
    // pwrite & fsync done.
    pub(super) queue: VecDeque<Commit>,
    // Current log file.
    pub(super) log_file: Option<SparseFile>,
}

pub(super) enum Commit {
    Group(CommitGroup),
    // switch from old log file to new log file.
    Switch(SparseFile),
    Shutdown,
}

/// CommitGroup groups multiple transactions with only
/// one logical log IO and at most one fsync() call.
/// It is controlled by two parameters:
/// 1. Maximum IO size, e.g. 16KB.
/// 2. Timeout to wait for next transaction to join.
pub(super) struct CommitGroup {
    pub(super) trx_list: Vec<PrecommitTrx>,
    pub(super) max_cts: TrxID,
    pub(super) fd: RawFd,
    pub(super) offset: usize,
    pub(super) log_buf: LogBuf,
    pub(super) sync_ev: EventNotifyOnDrop,
}

impl CommitGroup {
    #[inline]
    pub(super) fn can_join(&self, trx: &PrecommitTrx) -> bool {
        if let Some(redo_bin) = trx.redo_bin.as_ref() {
            return self.log_buf.capable_for(redo_bin.ser_len());
        }
        true
    }

    #[inline]
    pub(super) fn join(
        &mut self,
        mut trx: PrecommitTrx,
        wait_sync: bool,
    ) -> (Option<Arc<SessionState>>, Option<EventListener>) {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.redo_bin.take() {
            self.log_buf.ser(&redo_bin);
        }
        self.max_cts = trx.cts;
        let session = trx.take_session();
        self.trx_list.push(trx);
        let listener = wait_sync.then(|| self.sync_ev.listen());
        (session, listener)
    }

    #[inline]
    pub(super) fn split(self) -> (IocbRawPtr, SyncGroup) {
        // confirm data length in buffer header.
        let buf = self.log_buf.finish();
        // we always write a complete page instead of partial data.
        let log_bytes = buf.capacity();
        let aio = pwrite(self.max_cts, self.fd, self.offset, buf);
        let iocb_ptr = aio.iocb().load(Ordering::Relaxed);
        let sync_group = SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
            aio,
            sync_ev: self.sync_ev,
            finished: false,
        };
        (iocb_ptr, sync_group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::EventNotifyOnDrop;
    use crate::trx::log_replay::TrxLog;
    use crate::trx::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML};
    use crate::value::Val;
    use std::collections::BTreeMap;

    fn redo_bin(cts: TrxID) -> TrxLog {
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::System,
            },
            RedoLogs::default(),
        )
    }

    fn redo_bin_large(cts: TrxID) -> TrxLog {
        let mut rows = BTreeMap::new();
        // 3000-bytes string.
        let s: String = std::iter::repeat_n('a', 3000).collect();
        rows.insert(
            1u64,
            RowRedo {
                page_id: 5,
                row_id: 100,
                kind: RowRedoKind::Insert(vec![Val::from(1u32), Val::from(&s[..])]),
            },
        );
        let mut dml = BTreeMap::new();
        dml.insert(5u64, TableDML { rows });
        TrxLog::new(
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::User,
            },
            RedoLogs { ddl: None, dml },
        )
    }

    fn precommit(cts: TrxID) -> PrecommitTrx {
        PrecommitTrx {
            cts,
            redo_bin: Some(redo_bin(cts)),
            payload: None,
            session: None,
        }
    }

    fn precommit_large(cts: TrxID) -> PrecommitTrx {
        PrecommitTrx {
            cts,
            redo_bin: Some(redo_bin_large(cts)),
            payload: None,
            session: None,
        }
    }

    fn clear_redo(trx: &mut PrecommitTrx) {
        trx.redo_bin.take();
    }

    #[test]
    fn test_commit_group_join_without_sync_listener() {
        let mut log_buf = LogBuf::new(64);
        log_buf.ser(&redo_bin(1));
        let sync_ev = EventNotifyOnDrop::new();
        let mut group = CommitGroup {
            trx_list: vec![precommit(1)],
            max_cts: 1,
            fd: 0,
            offset: 0,
            log_buf,
            sync_ev,
        };

        let (session, listener) = group.join(precommit(2), false);
        assert!(session.is_none());
        assert!(listener.is_none());
        assert_eq!(group.trx_list.len(), 2);
        assert_eq!(group.max_cts, 2);
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_can_join_respects_capacity() {
        let mut log_buf = LogBuf::new(64);
        log_buf.ser(&redo_bin(100));
        let sync_ev = EventNotifyOnDrop::new();
        let mut group = CommitGroup {
            trx_list: vec![precommit(1)],
            max_cts: 1,
            fd: 0,
            offset: 0,
            log_buf,
            sync_ev,
        };

        let candidate1 = precommit_large(2);
        assert!(group.can_join(&candidate1));
        let _ = group.join(candidate1, false);
        let mut candidate2 = precommit_large(3);
        assert!(!group.can_join(&candidate2));
        clear_redo(&mut candidate2);
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }
}
