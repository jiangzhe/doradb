use super::{LogWriteSubmission, RedoLogFile, SyncGroup};
use crate::error::FatalError;
use crate::io::Completion;
use std::collections::VecDeque;
use std::sync::Arc;

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
    pub(super) fn push_seal_dispatch(&mut self, log_file: RedoLogFile) {
        let id = self.alloc_id();
        self.entries.push_back(LogPrefixEntry {
            id,
            kind: LogPrefixKind::SealDispatch {
                log_file: Some(log_file),
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
    pub(super) fn entry_mut(&mut self, entry_id: LogPrefixId) -> Option<&mut LogPrefixEntry> {
        let idx = entry_id.raw().checked_sub(self.front_id.raw())? as usize;
        let entry = self.entries.get_mut(idx)?;
        (entry.id == entry_id).then_some(entry)
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
                LogPrefixKind::SealDispatch { .. } => 0,
            })
            .sum()
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
    },
    /// Commit group waiting for its redo write and ordered publication.
    Group { group: SyncGroup },
    /// Marker that dispatches asynchronous sealing for a rotated-out file.
    ///
    /// The seal itself does not block publication of the next file's header;
    /// the marker only preserves prefix order for handing the old file to the sealer.
    SealDispatch { log_file: Option<RedoLogFile> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::TrxID;
    use crate::io::Completion;
    use crate::trx::PrecommitTrx;

    fn sync_group_for_order_test(cts: TrxID, ready: bool, log_bytes: usize) -> SyncGroup {
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
            log_fd: None,
            write_meta: None,
            write: None,
            returned_bufs: Vec::new(),
            completion: Arc::new(Completion::new()),
            outstanding_requests: usize::from(!ready),
            failure_reason: None,
        }
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
