use crate::buffer::BufferPool;
use crate::io::{pwrite, Buf, IocbRawPtr, SparseFile};
use crate::notify::{Notify, Signal};
use crate::serde::{Ser, SerdeCtx};
use crate::session::{IntoSession, Session};
use crate::trx::log::SyncGroup;
use crate::trx::{PrecommitTrx, TrxID};
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::sync::atomic::Ordering;

/// GroupCommit is optimization to group multiple transactions
/// and perform single IO to speed up overall commit performance.
pub(super) struct GroupCommit<P: BufferPool> {
    // Commit group queue, there can be multiple groups in commit phase.
    // Each of them submit IO request to AIO manager and then wait for
    // pwrite & fsync done.
    pub(super) queue: VecDeque<Commit<P>>,
    // Current log file.
    pub(super) log_file: Option<SparseFile>,
}

pub(super) enum Commit<P: BufferPool> {
    Group(CommitGroup<P>),
    // switch from old log file to new log file.
    Switch(SparseFile),
    Shutdown,
}

/// CommitGroup groups multiple transactions with only
/// one logical log IO and at most one fsync() call.
/// It is controlled by two parameters:
/// 1. Maximum IO size, e.g. 16KB.
/// 2. Timeout to wait for next transaction to join.
pub(super) struct CommitGroup<P: BufferPool> {
    pub(super) trx_list: Vec<PrecommitTrx<P>>,
    pub(super) max_cts: TrxID,
    pub(super) fd: RawFd,
    pub(super) offset: usize,
    pub(super) log_buf: Buf,
    pub(super) sync_signal: Signal,
    pub(super) serde_ctx: SerdeCtx,
}

impl<P: BufferPool> CommitGroup<P> {
    #[inline]
    pub(super) fn can_join(&self, trx: &PrecommitTrx<P>) -> bool {
        if let Some(redo_bin) = trx.redo_bin.as_ref() {
            return redo_bin.ser_len(&self.serde_ctx) <= self.log_buf.remaining_capacity();
        }
        true
    }

    #[inline]
    pub(super) fn join(&mut self, mut trx: PrecommitTrx<P>) -> (Option<Session<P>>, Notify) {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.redo_bin.take() {
            self.log_buf.extend_ser(&redo_bin, &self.serde_ctx);
        }
        self.max_cts = trx.cts;
        let session = trx.split_session();
        self.trx_list.push(trx);
        (session, self.sync_signal.new_notify(false))
    }

    #[inline]
    pub(super) fn split(self) -> (IocbRawPtr, SyncGroup<P>) {
        // we always write a complete page instead of partial data.
        let log_bytes = self.log_buf.capacity();
        let aio = pwrite(self.max_cts, self.fd, self.offset, self.log_buf);
        let iocb_ptr = aio.iocb().load(Ordering::Relaxed);
        let sync_group = SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
            aio,
            sync_signal: self.sync_signal,
            finished: false,
        };
        (iocb_ptr, sync_group)
    }
}
