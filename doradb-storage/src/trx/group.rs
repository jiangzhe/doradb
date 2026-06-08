use crate::file::SparseFile;
use crate::id::TrxID;
use crate::io::Completion;
use crate::serde::Ser;
use crate::trx::log::{LogWriteSubmission, SyncGroup};
use crate::trx::log_replay::LogBuf;
use crate::trx::{FailedPrecommitReason, PrecommitTrx};
use parking_lot::{Condvar, Mutex, MutexGuard, WaitTimeoutResult};
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

/// GroupCommit with mutex and condition variable.
pub(super) struct MutexGroupCommit {
    mu: Mutex<GroupCommit>,
    cv: Condvar,
}

impl MutexGroupCommit {
    /// Create a new group commit with mutex and condition variable.
    #[inline]
    pub(super) fn new(group_commit: GroupCommit) -> Self {
        MutexGroupCommit {
            mu: Mutex::new(group_commit),
            cv: Condvar::new(),
        }
    }

    /// Acquire lock.
    /// Return lock guard of group commit.
    #[inline]
    pub(super) fn lock(&self) -> MutexGuard<'_, GroupCommit> {
        self.mu.lock()
    }

    /// Notify one waiter.
    #[inline]
    pub(super) fn notify_one(&self) -> bool {
        self.cv.notify_one()
    }

    /// Wait on conditional variable with timeout.
    #[inline]
    pub(super) fn wait_for(
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
    // Each of them submits one redo write into the backend-neutral worker and
    // then waits for write completion plus the configured sync step.
    pub(super) queue: VecDeque<Commit>,
    // Closed admission reason. Shutdown messages only wake the worker; this
    // flag is the source of truth for rejecting new precommit handoffs.
    pub(super) closed: Option<FailedPrecommitReason>,
    // Current log file.
    pub(super) log_file: Option<SparseFile>,
}

impl GroupCommit {
    #[inline]
    pub(super) fn close(&mut self, reason: FailedPrecommitReason) {
        if self.closed.is_none() {
            self.closed = Some(reason);
        }
    }
}

pub(super) enum Commit {
    Group(CommitGroup),
    // switch from old log file to new log file.
    Switch(SparseFile),
    Shutdown,
}

pub(super) type CommitWaiter = Arc<Completion<()>>;
pub(super) type CommitJoin = Option<CommitWaiter>;

/// CommitGroup groups multiple transactions with only
/// one logical log IO and at most one fsync() call.
/// It is controlled by two parameters:
/// 1. Maximum IO size, e.g. 16KB.
/// 2. Timeout to wait for next transaction to join.
pub(super) struct CommitGroup {
    pub(super) trx_list: Vec<PrecommitTrx>,
    pub(super) max_cts: TrxID,
    pub(super) log: Option<CommitGroupLog>,
    pub(super) completion: Arc<Completion<()>>,
}

/// Serialized redo buffer and target file allocation for a durability group.
pub(super) struct CommitGroupLog {
    pub(super) fd: RawFd,
    pub(super) offset: usize,
    pub(super) log_buf: LogBuf,
}

impl CommitGroup {
    #[inline]
    pub(super) fn require_durability(&self) -> bool {
        self.log.is_some()
    }

    #[inline]
    pub(super) fn can_join(&self, trx: &PrecommitTrx) -> bool {
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

    #[inline]
    pub(super) fn join(&mut self, mut trx: PrecommitTrx, wait_sync: bool) -> CommitJoin {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.take_log() {
            self.log
                .as_mut()
                .expect("durability transaction cannot join a no-log group")
                .log_buf
                .ser(&redo_bin);
        }
        self.max_cts = trx.cts;
        // Session completion ownership stays with the queued PrecommitTrx. The
        // waiter only observes ordered completion, so dropping the user commit
        // future after joining this group cannot strand the owning session.
        self.trx_list.push(trx);
        wait_sync.then(|| Arc::clone(&self.completion))
    }

    #[inline]
    pub(super) fn into_sync_group(self) -> SyncGroup {
        let (log_bytes, write, finished) = match self.log {
            Some(log) => {
                // Confirm data length in buffer header.
                let buf = log.log_buf.finish();
                // We always write a complete page instead of partial data.
                let log_bytes = buf.capacity();
                (
                    log_bytes,
                    Some(LogWriteSubmission::new(
                        self.max_cts,
                        log.fd,
                        log.offset,
                        buf,
                    )),
                    false,
                )
            }
            None => (0, None, true),
        };
        SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
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
                fd: 0,
                offset: 0,
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
        log_buf.ser(&redo_bin(TrxID::new(1)));
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
        log_buf.ser(&redo_bin(TrxID::new(100)));
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
        log_buf.ser(&redo_bin(TrxID::new(10)));
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
