use crate::id::TrxID;
use crate::io::{Completion, DirectBuf};
use crate::log::block_group::LogBlockGroup;
use crate::log::{
    LogWriteSubmission, RedoGroupWriteAlloc, RedoGroupWriteMeta, RedoLogFile, SyncGroup,
};
use crate::trx::{FailedPrecommitReason, PrecommitTrx};
use either::Either;
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
/// and publish them through one ordered redo scheduler entry.
pub(crate) struct GroupCommit {
    /// Commit group queue, there can be multiple groups in commit phase.
    ///
    /// Redo-bearing groups become writer-owned physical requests in the log
    /// thread; request ids, not commit timestamps, identify those IO requests.
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
    /// Target redo file allocation.
    pub(crate) alloc: RedoGroupWriteAlloc,
    /// Logical redo group accumulated for fixed-block serialization.
    pub(crate) group: LogBlockGroup,
}

/// CommitGroup groups multiple transactions into one logical ordered commit unit.
///
/// The log thread may map that logical unit to one or more physical requests.
/// Join admission is decided immediately at enqueue time: no-log transactions
/// may join any group, and redo-bearing transactions may join only redo groups
/// whose serialized buffer has enough remaining capacity.
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
    /// Try to add a transaction to this group and return the requested waiter on success.
    #[inline]
    pub(crate) fn try_join(
        &mut self,
        mut trx: PrecommitTrx,
        wait_sync: bool,
    ) -> Either<CommitJoin, PrecommitTrx> {
        debug_assert!(self.max_cts < trx.cts);
        if let Some(redo_bin) = trx.take_log() {
            let Some(log) = self.log.as_mut() else {
                trx.redo_bin = Some(redo_bin);
                return Either::Right(trx);
            };
            if let Some(rejected) = log.group.append_trx_log(redo_bin) {
                trx.redo_bin = Some(rejected);
                return Either::Right(trx);
            }
        }
        self.max_cts = trx.cts;
        // Session completion ownership stays with the queued PrecommitTrx. The
        // waiter only observes ordered completion, so dropping the user commit
        // future after joining this group cannot strand the owning session.
        self.trx_list.push(trx);
        Either::Left(wait_sync.then(|| Arc::clone(&self.completion)))
    }

    /// Convert this commit group into a sync group for redo publication.
    #[inline]
    pub(crate) fn into_sync_group<F>(self, take_blocks: F) -> SyncGroup
    where
        F: FnOnce(usize) -> Vec<DirectBuf>,
    {
        let (log_bytes, log_fd, write_meta, writes) = match self.log {
            Some(log) => {
                let alloc = log.alloc;
                let (min_redo_cts, max_redo_cts) = log.group.redo_cts_range();
                let write_meta = RedoGroupWriteMeta {
                    file_seq: alloc.file_seq,
                    fd: alloc.fd,
                    offset: alloc.offset,
                    end_offset: alloc.end_offset,
                    min_redo_cts,
                    max_redo_cts,
                };
                let blocks = log
                    .group
                    .finish_with(take_blocks)
                    .expect("accepted redo group must materialize fixed blocks");
                let mut writes = VecDeque::with_capacity(blocks.len());
                let mut offset = alloc.offset;
                let log_fd = alloc.fd;
                for (group_write_idx, buf) in blocks.into_iter().enumerate() {
                    let len = buf.capacity();
                    writes.push_back(LogWriteSubmission::group(
                        log_fd,
                        offset,
                        buf,
                        group_write_idx,
                    ));
                    offset += len;
                }
                debug_assert_eq!(offset, alloc.end_offset);
                let log_bytes = offset - alloc.offset;
                (log_bytes, Some(log_fd), Some(write_meta), writes)
            }
            None => (0, None, None, VecDeque::new()),
        };
        SyncGroup {
            trx_list: self.trx_list,
            max_cts: self.max_cts,
            log_bytes,
            log_fd,
            write_meta,
            writes,
            returned_bufs: Vec::new(),
            completion: self.completion,
            outstanding_requests: 0,
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
    use crate::log::block_group::TrxLog;
    use crate::log::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind, TableDML};
    use crate::value::Val;
    use either::Either::{Left, Right};
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
        redo_bin_with_string_len(cts, 3000)
    }

    fn redo_bin_oversized(cts: TrxID) -> TrxLog {
        redo_bin_with_string_len(cts, 8000)
    }

    fn redo_bin_with_string_len(cts: TrxID, string_len: usize) -> TrxLog {
        let mut rows = BTreeMap::new();
        let s: String = repeat_n('a', string_len).collect();
        rows.insert(
            RowID::new(1u64),
            RowRedo {
                row_id: RowID::new(100),
                kind: RowRedoKind::Insert(
                    test_page_id(5),
                    vec![Val::from(1u32), Val::from(&s[..])],
                ),
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

    const TEST_LOG_BLOCK_SIZE: usize = 4096;

    fn log_group(cts: TrxID, log_group: LogBlockGroup) -> CommitGroup {
        let physical_len = log_group.physical_len();
        let mut trx = precommit(cts);
        clear_redo(&mut trx);
        CommitGroup {
            trx_list: vec![trx],
            max_cts: cts,
            log: Some(CommitGroupLog {
                alloc: RedoGroupWriteAlloc {
                    file_seq: 0,
                    fd: 0,
                    offset: 0,
                    end_offset: physical_len,
                },
                group: log_group,
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
        let log_block_group =
            LogBlockGroup::new(TEST_LOG_BLOCK_SIZE, redo_bin(TrxID::new(1))).unwrap();
        let mut group = log_group(TrxID::new(1), log_block_group);

        assert!(matches!(
            group.try_join(precommit(TrxID::new(2)), false),
            Left(None)
        ));
        assert_eq!(group.trx_list.len(), 2);
        assert_eq!(group.max_cts, TrxID::new(2));
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_try_join_respects_capacity() {
        let log_block_group =
            LogBlockGroup::new(TEST_LOG_BLOCK_SIZE, redo_bin(TrxID::new(100))).unwrap();
        let mut group = log_group(TrxID::new(1), log_block_group);

        let candidate1 = precommit_large(TrxID::new(2));
        assert!(matches!(group.try_join(candidate1, false), Left(None)));
        let trx_count = group.trx_list.len();
        let max_cts = group.max_cts;
        let mut rejected = match group.try_join(precommit_large(TrxID::new(3)), false) {
            Left(_) => panic!("oversized redo trx must be rejected"),
            Right(rejected) => rejected,
        };
        assert_eq!(rejected.cts, TrxID::new(3));
        assert!(rejected.redo_bin.is_some());
        assert_eq!(group.trx_list.len(), trx_count);
        assert_eq!(group.max_cts, max_cts);
        clear_redo(&mut rejected);
        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_no_log_join_rules() {
        let mut no_log_group = no_log_group(TrxID::new(1));
        assert!(matches!(
            no_log_group.try_join(precommit_no_log(TrxID::new(2)), true),
            Left(Some(_))
        ));
        assert_eq!(no_log_group.trx_list.len(), 2);
        assert_eq!(no_log_group.max_cts, TrxID::new(2));
        let mut rejected = match no_log_group.try_join(precommit(TrxID::new(3)), false) {
            Left(_) => panic!("redo trx must not join a no-log group"),
            Right(rejected) => rejected,
        };
        assert_eq!(rejected.cts, TrxID::new(3));
        assert!(rejected.redo_bin.is_some());
        assert_eq!(no_log_group.trx_list.len(), 2);
        assert_eq!(no_log_group.max_cts, TrxID::new(2));
        clear_redo(&mut rejected);

        let sync_group =
            no_log_group.into_sync_group(|_| panic!("no-log group must not request redo buffers"));
        assert_eq!(sync_group.log_bytes, 0);
        assert!(sync_group.writes.is_empty());
        assert_eq!(sync_group.outstanding_requests, 0);
    }

    #[test]
    fn test_commit_group_log_group_accepts_no_log_transaction() {
        let log_block_group =
            LogBlockGroup::new(TEST_LOG_BLOCK_SIZE, redo_bin(TrxID::new(10))).unwrap();
        let mut group = log_group(TrxID::new(10), log_block_group);

        assert!(group.log.is_some());
        assert!(matches!(
            group.try_join(precommit_no_log(TrxID::new(11)), false),
            Left(None)
        ));
        assert_eq!(group.trx_list.len(), 2);
        assert_eq!(group.max_cts, TrxID::new(11));

        for trx in &mut group.trx_list {
            clear_redo(trx);
        }
    }

    #[test]
    fn test_commit_group_oversized_transaction_becomes_multi_block_group() {
        let oversized = redo_bin_oversized(TrxID::new(20));
        let log_block_group = LogBlockGroup::new(TEST_LOG_BLOCK_SIZE, oversized).unwrap();
        assert!(log_block_group.is_multi_block());
        let mut group = log_group(TrxID::new(20), log_block_group);

        let mut rejected = match group.try_join(precommit(TrxID::new(21)), false) {
            Left(_) => panic!("redo trx must not join a multi-block group"),
            Right(rejected) => rejected,
        };
        clear_redo(&mut rejected);
        assert!(matches!(
            group.try_join(precommit_no_log(TrxID::new(22)), false),
            Left(None)
        ));

        let sync_group = group.into_sync_group(|count| {
            (0..count)
                .map(|_| DirectBuf::zeroed(TEST_LOG_BLOCK_SIZE))
                .collect()
        });
        assert!(sync_group.writes.len() > 1);
        assert_eq!(
            sync_group.log_bytes,
            sync_group.writes.len() * TEST_LOG_BLOCK_SIZE
        );
        assert_eq!(sync_group.max_cts, TrxID::new(22));
    }
}
