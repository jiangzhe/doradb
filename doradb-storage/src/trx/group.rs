use crate::id::TrxID;
use crate::io::Completion;
use crate::log::buf::LogBuf;
use crate::log::{LogWriteSubmission, RedoGroupWriteMeta, RedoLogFile, SyncGroup};
use crate::serde::Ser;
use crate::trx::{FailedPrecommitReason, PrecommitTrx};
use parking_lot::{Condvar, Mutex, MutexGuard, WaitTimeoutResult};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

/// GroupCommit with mutex and condition variable.
pub(crate) struct MutexGroupCommit {
    mu: Mutex<GroupCommit>,
    cv: Condvar,
}

impl MutexGroupCommit {
    /// Create a new group commit with mutex and condition variable.
    #[inline]
    pub(crate) fn new(group_commit: GroupCommit) -> Self {
        MutexGroupCommit {
            mu: Mutex::new(group_commit),
            cv: Condvar::new(),
        }
    }

    /// Acquire lock.
    /// Return lock guard of group commit.
    #[inline]
    pub(crate) fn lock(&self) -> MutexGuard<'_, GroupCommit> {
        self.mu.lock()
    }

    /// Notify one waiter.
    #[inline]
    pub(crate) fn notify_one(&self) -> bool {
        self.cv.notify_one()
    }

    /// Wait on conditional variable with timeout.
    #[inline]
    pub(crate) fn wait_for(
        &self,
        g: &mut MutexGuard<GroupCommit>,
        timeout: Duration,
    ) -> WaitTimeoutResult {
        self.cv.wait_for(g, timeout)
    }
}

/// GroupCommit is optimization to group multiple transactions
/// and perform single IO to speed up overall commit performance.
pub(crate) struct GroupCommit {
    /// Commit group queue, there can be multiple groups in commit phase.
    ///
    /// Each group submits one redo write through the log thread's backend-neutral driver and then
    /// waits for write completion plus the configured sync step.
    pub(crate) queue: VecDeque<Commit>,
    /// Closed admission reason used to reject new precommit handoffs.
    ///
    /// Shutdown messages only wake the worker; this flag is the source of truth for admission.
    pub(crate) closed: Option<FailedPrecommitReason>,
    /// Current redo log file used for new durability groups.
    pub(crate) log_file: Option<RedoLogFile>,
}

impl GroupCommit {
    /// Close group-commit admission with the first terminal reason.
    #[inline]
    pub(crate) fn close(&mut self, reason: FailedPrecommitReason) {
        if self.closed.is_none() {
            self.closed = Some(reason);
        }
    }
}

/// Queue item consumed by the log thread's group-commit loop.
pub(crate) enum Commit {
    /// Boundary marker that carries a switched-out redo file and its header write.
    LogFileBoundary {
        /// Redo file whose groups must finish before the file can be sealed.
        ended_log_file: Option<RedoLogFile>,
        /// Pending header write for the newly opened log file.
        header_write: LogWriteSubmission,
    },
    /// Transaction group waiting for redo write and optional sync.
    Group(CommitGroup),
    /// Shutdown marker for the log loop.
    Shutdown,
}

/// Shared ordered-completion waiter for a commit group.
pub(crate) type CommitWaiter = Arc<Completion<()>>;
/// Optional commit waiter returned to transactions that must observe ordered completion.
pub(crate) type CommitJoin = Option<CommitWaiter>;

/// Serialized redo buffer and target file allocation for a durability group.
pub(crate) struct CommitGroupLog {
    /// Target redo file allocation and CTS range metadata.
    pub(crate) write_meta: RedoGroupWriteMeta,
    /// Serialized redo bytes accumulated for this group.
    pub(crate) log_buf: LogBuf,
}

/// CommitGroup groups multiple transactions with only
/// one logical log IO and at most one fsync() call.
/// It is controlled by two parameters:
/// 1. Log block size, e.g. 16KB.
/// 2. Timeout to wait for next transaction to join.
pub(crate) struct CommitGroup {
    /// Transactions accepted into this ordered group.
    pub(crate) trx_list: Vec<PrecommitTrx>,
    /// Maximum CTS assigned to transactions in this group.
    pub(crate) max_cts: TrxID,
    /// Serialized redo state for durability-required groups.
    pub(crate) log: Option<CommitGroupLog>,
    /// Completion signaled after this group reaches its ordered terminal result.
    pub(crate) completion: Arc<Completion<()>>,
}

impl CommitGroup {
    /// Returns whether this group has redo bytes that require persistence.
    #[inline]
    pub(crate) fn require_durability(&self) -> bool {
        self.log.is_some()
    }

    /// Returns whether the transaction can join this existing group.
    #[inline]
    pub(crate) fn can_join(&self, trx: &PrecommitTrx) -> bool {
        if !trx.require_durability() {
            return true;
        }
        if !self.require_durability() {
            return false;
        }
        self.log.as_ref().is_some_and(|log| {
            log.log_buf
                .capable_for(trx.redo_bin.as_ref().unwrap().ser_len())
        })
    }

    /// Add a transaction to this group and return the waiter requested by the caller.
    #[inline]
    pub(crate) fn join(&mut self, mut trx: PrecommitTrx, wait_sync: bool) -> CommitJoin {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.take_log() {
            let log = self
                .log
                .as_mut()
                .expect("durability transaction cannot join a no-log group");
            log.log_buf.append_trx_log(&redo_bin);
            let (min_redo_cts, max_redo_cts) = log
                .log_buf
                .redo_cts_range()
                .expect("redo-bearing group must track a CTS range");
            log.write_meta.min_redo_cts = min_redo_cts;
            log.write_meta.max_redo_cts = max_redo_cts;
        }
        self.max_cts = trx.cts;
        // Session completion ownership stays with the queued PrecommitTrx. The
        // waiter only observes ordered completion, so dropping the user commit
        // future after joining this group cannot strand the owning session.
        self.trx_list.push(trx);
        wait_sync.then(|| Arc::clone(&self.completion))
    }

    /// Convert this commit group into a sync group for redo completion.
    #[inline]
    pub(crate) fn into_sync_group(self) -> SyncGroup {
        let (log_bytes, log_fd, write_meta, write, finished) = match self.log {
            Some(log) => {
                // Confirm data length in buffer header.
                let write_meta = log.write_meta;
                let buf = log.log_buf.finish();
                // We always write a complete page instead of partial data.
                let log_bytes = buf.capacity();
                let log_fd = write_meta.fd;
                (
                    log_bytes,
                    Some(log_fd),
                    Some(write_meta),
                    Some(LogWriteSubmission::new(
                        self.max_cts,
                        log_fd,
                        write_meta.offset,
                        buf,
                    )),
                    false,
                )
            }
            None => (0, None, None, None, true),
        };
        SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
            log_fd,
            write_meta,
            write,
            returned_buf: None,
            completion: self.completion,
            finished,
            failure_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_page_id;
    use crate::id::{RowID, TableID};
    use crate::io::Completion;
    use crate::log::buf::TrxLog;
    use crate::log::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML};
    use crate::value::Val;
    use std::collections::BTreeMap;
    use std::iter::repeat_n;

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
        let s: String = repeat_n('a', 3000).collect();
        rows.insert(
            RowID::new(1u64),
            RowRedo {
                page_id: test_page_id(5),
                row_id: RowID::new(100),
                kind: RowRedoKind::Insert(vec![Val::from(1u32), Val::from(&s[..])]),
            },
        );
        let mut dml = BTreeMap::new();
        dml.insert(TableID::new(5u64), TableDML { rows });
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
            attachment: None,
            lock_manager: None,
            lock_state: None,
        }
    }

    fn precommit_large(cts: TrxID) -> PrecommitTrx {
        PrecommitTrx {
            cts,
            redo_bin: Some(redo_bin_large(cts)),
            payload: None,
            attachment: None,
            lock_manager: None,
            lock_state: None,
        }
    }

    fn precommit_no_log(cts: TrxID) -> PrecommitTrx {
        PrecommitTrx {
            cts,
            redo_bin: None,
            payload: None,
            attachment: None,
            lock_manager: None,
            lock_state: None,
        }
    }

    fn log_group(cts: TrxID, log_buf: LogBuf) -> CommitGroup {
        CommitGroup {
            trx_list: vec![precommit(cts)],
            max_cts: cts,
            log: Some(CommitGroupLog {
                write_meta: RedoGroupWriteMeta {
                    file_seq: 0,
                    fd: 0,
                    offset: 0,
                    end_offset: log_buf.capacity(),
                    min_redo_cts: cts,
                    max_redo_cts: cts,
                },
                log_buf,
            }),
            completion: Arc::new(Completion::new()),
        }
    }

    fn no_log_group(cts: TrxID) -> CommitGroup {
        CommitGroup {
            trx_list: vec![precommit_no_log(cts)],
            max_cts: cts,
            log: None,
            completion: Arc::new(Completion::new()),
        }
    }

    fn clear_redo(trx: &mut PrecommitTrx) {
        trx.redo_bin.take();
    }

    #[test]
    fn test_commit_group_join_without_sync_listener() {
        let mut log_buf = LogBuf::new(64);
        log_buf.append_trx_log(&redo_bin(TrxID::new(1)));
        let mut group = log_group(TrxID::new(1), log_buf);

        let listener = group.join(precommit(TrxID::new(2)), false);
        assert!(listener.is_none());
        assert_eq!(group.trx_list.len(), 2);
        assert_eq!(group.max_cts, TrxID::new(2));
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_can_join_respects_capacity() {
        let mut log_buf = LogBuf::new(64);
        log_buf.append_trx_log(&redo_bin(TrxID::new(100)));
        let mut group = log_group(TrxID::new(1), log_buf);

        let candidate1 = precommit_large(TrxID::new(2));
        assert!(group.can_join(&candidate1));
        let _ = group.join(candidate1, false);
        let mut candidate2 = precommit_large(TrxID::new(3));
        assert!(!group.can_join(&candidate2));
        clear_redo(&mut candidate2);
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_no_log_join_rules() {
        let mut no_log_group = no_log_group(TrxID::new(1));
        assert!(no_log_group.can_join(&precommit_no_log(TrxID::new(2))));
        let mut durability_candidate = precommit(TrxID::new(3));
        assert!(!no_log_group.can_join(&durability_candidate));
        clear_redo(&mut durability_candidate);

        let listener = no_log_group.join(precommit_no_log(TrxID::new(2)), true);
        assert!(listener.is_some());
        assert_eq!(no_log_group.trx_list.len(), 2);
        assert_eq!(no_log_group.max_cts, TrxID::new(2));

        let sync_group = no_log_group.into_sync_group();
        assert_eq!(sync_group.log_bytes, 0);
        assert!(sync_group.write.is_none());
        assert!(sync_group.finished);
    }

    #[test]
    fn test_commit_group_log_group_accepts_no_log_transaction() {
        let mut log_buf = LogBuf::new(64);
        log_buf.append_trx_log(&redo_bin(TrxID::new(10)));
        let mut group = log_group(TrxID::new(10), log_buf);

        assert!(group.require_durability());
        assert!(group.can_join(&precommit_no_log(TrxID::new(11))));
        let _ = group.join(precommit_no_log(TrxID::new(11)), false);
        assert_eq!(group.trx_list.len(), 2);
        assert_eq!(group.max_cts, TrxID::new(11));

        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }
}
