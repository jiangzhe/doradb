use crate::file::SparseFile;
use crate::io::{pwrite, DirectBuf, IocbRawPtr};
use crate::notify::EventNotifyOnDrop;
use crate::serde::{Ser, SerdeCtx};
use crate::session::{IntoSession, Session};
use crate::trx::log::SyncGroup;
use crate::trx::{PrecommitTrx, TrxID};
use event_listener::EventListener;
use parking_lot::{Condvar, Mutex, MutexGuard, WaitTimeoutResult};
use std::collections::VecDeque;
use std::os::fd::RawFd;
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
    pub(super) log_buf: DirectBuf,
    pub(super) sync_ev: EventNotifyOnDrop,
    pub(super) serde_ctx: SerdeCtx,
}

impl CommitGroup {
    #[inline]
    pub(super) fn can_join(&self, trx: &PrecommitTrx) -> bool {
        if let Some(redo_bin) = trx.redo_bin.as_ref() {
            return redo_bin.ser_len(&self.serde_ctx) <= self.log_buf.remaining_capacity();
        }
        true
    }

    #[inline]
    pub(super) fn join(&mut self, mut trx: PrecommitTrx) -> (Option<Session>, EventListener) {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.redo_bin.take() {
            self.log_buf.extend_ser(&redo_bin, &self.serde_ctx);
        }
        self.max_cts = trx.cts;
        let session = trx.split_session();
        self.trx_list.push(trx);
        (session, self.sync_ev.listen())
    }

    #[inline]
    pub(super) fn split(self) -> (IocbRawPtr, SyncGroup) {
        // we always write a complete page instead of partial data.
        let log_bytes = self.log_buf.capacity();
        let aio = pwrite(self.max_cts, self.fd, self.offset, self.log_buf);
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
