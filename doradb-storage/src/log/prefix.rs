use super::{
    LogRequestKind, LogWriteKind, LogWriteSubmission, ReadyGroupPrefix, RedoLogFile, SyncGroup,
};
use crate::error::FatalError;
use crate::io::Completion;
use error_stack::Report;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

/// Writer-assigned identity for one logical redo publication prefix entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LogPrefixId(u64);

impl LogPrefixId {
    #[inline]
    pub(super) fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    fn raw(self) -> u64 {
        self.0
    }
}

pub(super) struct LogPrefixTracker {
    front_id: LogPrefixId,
    next_id: LogPrefixId,
    pub(super) entries: VecDeque<LogPrefixEntry>,
}

impl LogPrefixTracker {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            front_id: LogPrefixId::new(0),
            next_id: LogPrefixId::new(0),
            entries: VecDeque::new(),
        }
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    fn alloc_id(&mut self) -> LogPrefixId {
        let id = self.next_id;
        self.next_id = LogPrefixId::new(
            self.next_id
                .raw()
                .checked_add(1)
                .expect("redo prefix entry id overflow"),
        );
        if self.entries.is_empty() {
            self.front_id = id;
        }
        id
    }

    #[inline]
    pub(super) fn push_header(&mut self, write: LogWriteSubmission) {
        let completion = write
            .header_completion()
            .expect("redo header prefix entry must carry header completion");
        let id = self.alloc_id();
        self.entries.push_back(LogPrefixEntry {
            id,
            kind: LogPrefixKind::Header {
                write: Some(write),
                completion: Some(completion),
                ready: false,
                failure: None,
                failure_report: None,
            },
        });
    }

    #[inline]
    pub(super) fn push_group(&mut self, group: SyncGroup) {
        let id = self.alloc_id();
        self.entries.push_back(LogPrefixEntry {
            id,
            kind: LogPrefixKind::Group { group },
        });
    }

    #[inline]
    pub(super) fn push_front_sync(
        &mut self,
        ready_prefix: ReadyGroupPrefix,
        sync: LogWriteSubmission,
    ) {
        let id = ready_prefix
            .sync_barrier_id
            .expect("redo sync barrier must cover a drained prefix entry");
        let next_raw = id
            .raw()
            .checked_add(1)
            .expect("redo prefix entry id overflow");
        if let Some(front) = self.entries.front() {
            debug_assert_eq!(front.id.raw(), next_raw);
        } else {
            debug_assert_eq!(self.next_id.raw(), next_raw);
        }
        self.front_id = id;
        self.entries.push_front(LogPrefixEntry {
            id,
            kind: LogPrefixKind::Sync {
                ready_prefix,
                sync: Some(sync),
                ready: false,
                failure: None,
                started_at: None,
                sync_nanos: 0,
            },
        });
    }

    #[inline]
    pub(super) fn push_seal(&mut self, log_file: RedoLogFile) {
        let id = self.alloc_id();
        self.entries.push_back(LogPrefixEntry {
            id,
            kind: LogPrefixKind::Seal {
                log_file: Some(log_file),
                write: None,
                sync: None,
                ready: false,
                failure: None,
            },
        });
    }

    #[inline]
    pub(super) fn pop_front(&mut self) -> Option<LogPrefixEntry> {
        let entry = self.entries.pop_front()?;
        self.front_id = self.entries.front().map_or(self.next_id, |entry| entry.id);
        Some(entry)
    }

    #[inline]
    pub(super) fn entry_mut(&mut self, prefix_id: LogPrefixId) -> Option<&mut LogPrefixEntry> {
        let idx = usize::try_from(prefix_id.raw().checked_sub(self.front_id.raw())?).ok()?;
        let entry = self.entries.get_mut(idx)?;
        (entry.id == prefix_id).then_some(entry)
    }

    #[inline]
    pub(super) fn entry_id(&self, idx: usize) -> LogPrefixId {
        self.entries
            .get(idx)
            .expect("redo prefix index must be in range")
            .id
    }

    #[inline]
    pub(super) fn shrink_if_sparse(&mut self, floor: usize) {
        let floor = floor.max(1);
        let len = self.entries.len();
        let cap = self.entries.capacity();
        let floor_retain = floor.saturating_mul(4);
        let shrink_threshold = floor.saturating_mul(16);
        let target = len.max(floor_retain);
        if cap > shrink_threshold && cap > target.saturating_mul(4) {
            self.entries.shrink_to(target);
        }
    }

    #[inline]
    pub(super) fn driver_owned_len(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| match &entry.kind {
                LogPrefixKind::Header { write, ready, .. } => {
                    usize::from(write.is_none() && !ready)
                }
                LogPrefixKind::Group { group } => group.outstanding_requests,
                LogPrefixKind::Sync { sync, ready, .. } => usize::from(sync.is_none() && !ready),
                LogPrefixKind::Seal {
                    write, sync, ready, ..
                } => usize::from(write.is_none() && sync.is_none() && !ready),
            })
            .sum()
    }

    #[inline]
    pub(super) fn drain_ready_group_prefix(&mut self) -> ReadyGroupPrefix {
        let mut ready = ReadyGroupPrefix::default();

        while let Some(front) = self.entries.front() {
            let LogPrefixKind::Group { group } = &front.kind else {
                break;
            };
            if !group.ready() {
                break;
            }
            // Keep each durable publication batch on one physical redo file.
            // Rotation normally inserts seal/header barriers, but this guard
            // preserves the invariant even if adjacent ready groups differ.
            if ready.failure_reason.is_none()
                && group.failure_reason.is_none()
                && group.log_bytes > 0
            {
                let group_fd = group
                    .log_fd
                    .expect("redo-bearing sync group must carry its log file fd");
                if let Some(prefix_fd) = ready.log_fd {
                    if prefix_fd != group_fd {
                        break;
                    }
                } else {
                    ready.log_fd = Some(group_fd);
                }
            }

            let entry = self
                .pop_front()
                .expect("front ready redo group entry must exist");
            ready.sync_barrier_id = Some(entry.id);
            let LogPrefixKind::Group { group } = entry.kind else {
                unreachable!("front group entry kind was checked")
            };
            if let Some(reason) = ready.failure_reason.or(group.failure_reason) {
                ready.failure_reason = Some(reason);
                // Failed groups and all later ready groups are removed from the
                // prefix, but they are not counted as durable commits.
                ready.failed.push(group);
                continue;
            }
            ready.trx_count += group.trx_list.len();
            ready.commit_count += 1;
            ready.log_bytes += group.log_bytes;
            ready.written.push(group);
        }

        ready
    }

    #[inline]
    pub(super) fn entry_has_pending_submission(&self, idx: usize) -> bool {
        let entry = self
            .entries
            .get(idx)
            .expect("redo prefix index must be in range");
        match &entry.kind {
            LogPrefixKind::Header { write, .. } => write.is_some(),
            LogPrefixKind::Group { group } => !group.writes.is_empty(),
            LogPrefixKind::Sync { sync, .. } => sync.is_some(),
            LogPrefixKind::Seal { write, sync, .. } => write.is_some() || sync.is_some(),
        }
    }

    #[inline]
    pub(super) fn take_submission(&mut self, idx: usize) -> Option<LogWriteSubmission> {
        let entry = self
            .entries
            .get_mut(idx)
            .expect("redo prefix index must be in range");
        match &mut entry.kind {
            LogPrefixKind::Header { write, .. } => write.take(),
            LogPrefixKind::Group { group } => group.take_submission(),
            LogPrefixKind::Sync { sync, .. } => sync.take(),
            LogPrefixKind::Seal { write, sync, .. } => write.take().or_else(|| sync.take()),
        }
    }

    #[inline]
    pub(super) fn restore_submission(&mut self, idx: usize, submission: LogWriteSubmission) {
        let entry = self
            .entries
            .get_mut(idx)
            .expect("redo prefix index must be in range");
        match &mut entry.kind {
            LogPrefixKind::Header { write, .. } => {
                debug_assert!(write.is_none());
                *write = Some(submission);
            }
            LogPrefixKind::Group { group } => group.restore_submission(submission),
            LogPrefixKind::Sync { sync, .. } => {
                debug_assert!(sync.is_none());
                *sync = Some(submission);
            }
            LogPrefixKind::Seal { write, sync, .. } => match &submission.kind {
                LogWriteKind::SealWrite { .. } => {
                    debug_assert!(write.is_none());
                    *write = Some(submission);
                }
                LogWriteKind::SealSync { .. } => {
                    debug_assert!(sync.is_none());
                    *sync = Some(submission);
                }
                LogWriteKind::Group { .. }
                | LogWriteKind::Header { .. }
                | LogWriteKind::CommitSync
                | LogWriteKind::StandaloneSync => {
                    panic!("restored non-seal submission into seal prefix entry")
                }
            },
        }
    }

    #[inline]
    pub(super) fn mark_submission_driver_owned(&mut self, idx: usize, kind: LogRequestKind) {
        let entry = self
            .entries
            .get_mut(idx)
            .expect("redo prefix index must be in range");
        match &mut entry.kind {
            LogPrefixKind::Header { .. } => {
                debug_assert_eq!(kind, LogRequestKind::Header);
            }
            LogPrefixKind::Group { group } => {
                debug_assert_eq!(kind, LogRequestKind::Group);
                group.mark_request_submitted();
            }
            LogPrefixKind::Sync { started_at, .. } => {
                debug_assert_eq!(kind, LogRequestKind::CommitSync);
                *started_at = Some(Instant::now());
            }
            LogPrefixKind::Seal { .. } => {
                debug_assert!(matches!(
                    kind,
                    LogRequestKind::SealWrite | LogRequestKind::SealSync
                ));
            }
        }
    }
}

pub(super) struct LogPrefixEntry {
    pub(super) id: LogPrefixId,
    pub(super) kind: LogPrefixKind,
}

pub(super) enum LogPrefixKind {
    /// New-file header write barrier inserted after rotation.
    ///
    /// Later groups cannot be published until the header write is known to
    /// have completed successfully or failed the prefix.
    Header {
        /// Pending header write before it is submitted to the shared driver.
        write: Option<LogWriteSubmission>,
        /// Waiter that must be completed when the header barrier leaves the prefix.
        completion: Option<Arc<Completion<()>>>,
        /// Whether the header write has completed and the barrier can be popped.
        ready: bool,
        /// Fatal write failure reported when the barrier is completed.
        failure: Option<FatalError>,
        /// Source-preserving fatal report retained for the header completion waiter.
        failure_report: Option<Report<FatalError>>,
    },
    /// Commit group waiting for its redo write and ordered publication.
    Group { group: SyncGroup },
    /// Backend sync barrier for one ready redo-bearing group prefix.
    ///
    /// The drained groups are owned by this entry until the backend sync
    /// completes. Later writes may be submitted while this entry is inflight,
    /// but transaction publication cannot pass the barrier.
    Sync {
        /// Ready groups covered by this sync operation.
        ready_prefix: ReadyGroupPrefix,
        /// Pending sync before driver submission.
        sync: Option<LogWriteSubmission>,
        /// Whether the sync completion has been observed.
        ready: bool,
        /// Fatal sync failure reported when the barrier completes.
        failure: Option<FatalError>,
        /// Monotonic timestamp captured when the sync is submitted.
        started_at: Option<Instant>,
        /// Async sync latency measured from submission to completion.
        sync_nanos: usize,
    },
    /// Mandatory rotated-file seal barrier.
    ///
    /// The seal write is prepared once this entry reaches the ordered prefix
    /// front, after all older file-local groups have recorded seal metadata.
    /// Later header/data writes may still be submitted while this entry is
    /// waiting for its write and configured sync, but publication remains
    /// blocked until `ready` is true.
    Seal {
        /// Rotated-out file waiting for seal metadata accumulation.
        log_file: Option<RedoLogFile>,
        /// Pending inactive-slot seal write before driver submission.
        write: Option<LogWriteSubmission>,
        /// Pending configured seal sync after the inactive-slot write succeeds.
        sync: Option<LogWriteSubmission>,
        /// Whether the seal write and configured sync have completed.
        ready: bool,
        /// Fatal write or sync failure reported when the barrier completes.
        failure: Option<FatalError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::TrxID;
    use crate::io::Completion;
    use crate::log::LogSync;
    use crate::trx::FailedPrecommitReason;
    use crate::trx::PrecommitTrx;
    use std::os::fd::RawFd;

    fn sync_group_for_order_test(cts: TrxID, ready: bool, log_bytes: usize) -> SyncGroup {
        sync_group_for_order_test_with_log_fd(cts, ready, log_bytes, (log_bytes > 0).then_some(0))
    }

    fn sync_group_for_order_test_with_log_fd(
        cts: TrxID,
        ready: bool,
        log_bytes: usize,
        log_fd: Option<RawFd>,
    ) -> SyncGroup {
        SyncGroup {
            trx_list: vec![PrecommitTrx {
                cts,
                redo_bin: None,
                payload: None,
                attachment: None,
                lock_manager: None,
                lock_state: None,
            }],
            max_cts: cts,
            log_bytes,
            log_fd,
            write_meta: None,
            writes: VecDeque::new(),
            returned_bufs: Vec::new(),
            completion: Arc::new(Completion::new()),
            outstanding_requests: usize::from(!ready),
            failure_reason: None,
        }
    }

    #[test]
    fn test_prefix_tracker_preserves_order_with_no_log_groups() {
        let mut tracker = LogPrefixTracker::new();
        tracker.push_group(sync_group_for_order_test(TrxID::new(10), false, 4096));
        tracker.push_group(sync_group_for_order_test(TrxID::new(11), true, 0));

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.trx_count, 0);
        assert_eq!(ready.commit_count, 0);
        assert_eq!(ready.log_bytes, 0);
        assert_eq!(ready.failure_reason, None);
        assert!(ready.written.is_empty());
        assert!(ready.failed.is_empty());
        assert_eq!(tracker.len(), 2);

        let LogPrefixKind::Group { group } = &mut tracker.entries.front_mut().unwrap().kind else {
            panic!("expected front group")
        };
        group.outstanding_requests = 0;

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.trx_count, 2);
        assert_eq!(ready.commit_count, 2);
        assert_eq!(ready.log_bytes, 4096);
        assert_eq!(ready.failure_reason, None);
        assert_eq!(ready.written.len(), 2);
        assert_eq!(ready.written[0].max_cts, TrxID::new(10));
        assert_eq!(ready.written[1].max_cts, TrxID::new(11));
        assert!(ready.failed.is_empty());
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_prefix_tracker_releases_no_log_prefix_without_later_log() {
        let mut tracker = LogPrefixTracker::new();
        tracker.push_group(sync_group_for_order_test(TrxID::new(20), true, 0));
        tracker.push_group(sync_group_for_order_test(TrxID::new(21), false, 4096));

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.trx_count, 1);
        assert_eq!(ready.commit_count, 1);
        assert_eq!(ready.log_bytes, 0);
        assert_eq!(ready.failure_reason, None);
        assert_eq!(ready.written.len(), 1);
        assert_eq!(ready.written[0].max_cts, TrxID::new(20));
        assert!(ready.failed.is_empty());
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_prefix_tracker_stops_at_failed_redo_boundary() {
        let mut tracker = LogPrefixTracker::new();
        let reason = FailedPrecommitReason::Fatal(FatalError::RedoWrite);
        let mut failed = sync_group_for_order_test(TrxID::new(31), true, 2048);
        failed.failure_reason = Some(reason);

        tracker.push_group(sync_group_for_order_test(TrxID::new(30), true, 1024));
        tracker.push_group(failed);
        tracker.push_group(sync_group_for_order_test(TrxID::new(32), true, 4096));

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.trx_count, 1);
        assert_eq!(ready.commit_count, 1);
        assert_eq!(ready.log_bytes, 1024);
        assert_eq!(ready.failure_reason, Some(reason));
        assert!(tracker.is_empty());
        assert_eq!(ready.written.len(), 1);
        assert_eq!(ready.written[0].max_cts, TrxID::new(30));
        assert_eq!(ready.failed.len(), 2);
        assert_eq!(ready.failed[0].max_cts, TrxID::new(31));
        assert_eq!(ready.failed[1].max_cts, TrxID::new(32));
    }

    #[test]
    fn test_prefix_tracker_keeps_unfinished_groups_after_failed_boundary() {
        let mut tracker = LogPrefixTracker::new();
        let reason = FailedPrecommitReason::Fatal(FatalError::RedoWrite);
        let mut failed = sync_group_for_order_test(TrxID::new(40), true, 1024);
        failed.failure_reason = Some(reason);

        tracker.push_group(failed);
        tracker.push_group(sync_group_for_order_test(TrxID::new(41), false, 2048));

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.trx_count, 0);
        assert_eq!(ready.commit_count, 0);
        assert_eq!(ready.log_bytes, 0);
        assert_eq!(ready.failure_reason, Some(reason));
        assert!(ready.written.is_empty());
        assert_eq!(ready.failed.len(), 1);
        assert_eq!(ready.failed[0].max_cts, TrxID::new(40));
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_prefix_tracker_front_sync_preserves_o1_id_lookup() {
        let mut tracker = LogPrefixTracker::new();
        tracker.push_group(sync_group_for_order_test(TrxID::new(50), true, 4096));
        tracker.push_group(sync_group_for_order_test(TrxID::new(51), false, 4096));
        let sync_id = tracker.entries[0].id;
        let later_id = tracker.entries[1].id;

        let ready = tracker.drain_ready_group_prefix();
        assert_eq!(ready.sync_barrier_id, Some(sync_id));
        assert_eq!(tracker.front_id, later_id);

        let sync = LogWriteSubmission::commit_sync(0, LogSync::Fsync);
        tracker.push_front_sync(ready, sync);

        assert_eq!(tracker.front_id, sync_id);
        assert_eq!(tracker.entries[0].id, sync_id);
        assert_eq!(tracker.entries[1].id, later_id);
        let entry = tracker
            .entry_mut(later_id)
            .expect("later prefix id must resolve through direct id index");
        let LogPrefixKind::Group { group } = &entry.kind else {
            panic!("expected retained group entry")
        };
        assert_eq!(group.max_cts, TrxID::new(51));
    }

    #[test]
    fn test_prefix_tracker_sparse_shrink_preserves_id_lookup() {
        let mut tracker = LogPrefixTracker::new();
        for raw in 0..128 {
            tracker.push_group(sync_group_for_order_test(TrxID::new(1000 + raw), true, 0));
        }

        let popped_id = tracker.entries[10].id;
        let live_id = tracker.entries[120].id;
        for _ in 0..120 {
            tracker.pop_front().unwrap();
        }

        assert_eq!(tracker.front_id, live_id);
        assert_eq!(tracker.next_id, LogPrefixId::new(128));
        assert!(tracker.entry_mut(popped_id).is_none());

        let cap_before = tracker.entries.capacity();
        assert!(cap_before > tracker.len());
        tracker.shrink_if_sparse(1);
        assert!(tracker.entries.capacity() <= cap_before);
        assert_eq!(tracker.front_id, live_id);
        assert_eq!(tracker.next_id, LogPrefixId::new(128));

        let entry = tracker
            .entry_mut(live_id)
            .expect("live prefix id must still resolve after shrink");
        let LogPrefixKind::Group { group } = &entry.kind else {
            panic!("expected retained group entry")
        };
        assert_eq!(group.max_cts, TrxID::new(1120));

        tracker.push_group(sync_group_for_order_test(TrxID::new(2000), true, 0));
        let pushed_id = tracker.entries.back().unwrap().id;
        assert_eq!(pushed_id, LogPrefixId::new(128));
        assert_eq!(tracker.next_id, LogPrefixId::new(129));
        assert!(tracker.entry_mut(pushed_id).is_some());
    }
}
