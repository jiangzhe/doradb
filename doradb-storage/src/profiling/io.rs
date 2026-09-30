use crate::file::fs::StorageLaneId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Monotonic shared-storage and backend IO statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageIoStats {
    /// Shared storage backend submit/wait activity.
    pub backend: IoBackendStats,
    /// Number of admitted table-file or readonly-cache read requests.
    pub table_read_requests: usize,
    /// Number of admitted evictable-pool page-in read requests.
    pub pool_read_requests: usize,
    /// Number of admitted shared background-write requests.
    pub background_write_requests: usize,
    /// Number of scheduler turns consumed by the table-read lane.
    pub table_read_turns: usize,
    /// Number of scheduler turns consumed by the pool-read lane.
    pub pool_read_turns: usize,
    /// Number of scheduler turns consumed by the background-write lane.
    pub background_write_turns: usize,
}

/// Monotonic storage-backend submit/wait activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoBackendStats {
    /// Number of backend kernel-entry calls spent submitting work or waiting.
    pub submit_and_wait_calls: usize,
    /// Number of operations accepted by the backend submit path.
    pub submitted_ops: usize,
    /// Total nanoseconds spent in backend submit-or-wait calls.
    pub submit_and_wait_nanos: usize,
    /// Number of completions observed by the backend wait path.
    pub wait_completions: usize,
}

/// Snapshot of backend-owned submit/wait activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BackendStats {
    /// Number of backend kernel-entry calls spent submitting work or waiting.
    ///
    /// Counts io_uring submit calls and blocking submit-and-wait calls.
    pub(crate) submit_and_wait_calls: usize,
    /// Number of operations accepted by the backend submit path.
    pub(crate) submitted_ops: usize,
    /// Total nanoseconds spent in backend submit-or-wait calls.
    ///
    /// This is a non-overlapping total: io_uring fused `submit_and_wait()`
    /// time is counted once.
    pub(crate) submit_and_wait_nanos: usize,
    /// Number of completions observed by the backend wait path.
    pub(crate) wait_completions: usize,
}

#[derive(Default)]
struct BackendStatsCounters {
    submit_and_wait_calls: AtomicUsize,
    submitted_ops: AtomicUsize,
    submit_and_wait_nanos: AtomicUsize,
    wait_completions: AtomicUsize,
}

/// Shared handle used to collect backend submit and wait statistics.
#[derive(Clone, Default)]
pub(crate) struct BackendStatsHandle(Arc<BackendStatsCounters>);

impl BackendStatsHandle {
    /// Returns a point-in-time snapshot of backend activity counters.
    #[inline]
    pub(crate) fn snapshot(&self) -> BackendStats {
        BackendStats {
            submit_and_wait_calls: self.0.submit_and_wait_calls.load(Ordering::Relaxed),
            submitted_ops: self.0.submitted_ops.load(Ordering::Relaxed),
            submit_and_wait_nanos: self.0.submit_and_wait_nanos.load(Ordering::Relaxed),
            wait_completions: self.0.wait_completions.load(Ordering::Relaxed),
        }
    }

    /// Records submit-or-wait calls and their elapsed time in nanoseconds.
    #[inline]
    pub(crate) fn record_submit_and_wait(&self, submit_and_wait_calls: usize, nanos: usize) {
        if submit_and_wait_calls != 0 {
            self.0
                .submit_and_wait_calls
                .fetch_add(submit_and_wait_calls, Ordering::Relaxed);
        }
        if nanos != 0 {
            self.0
                .submit_and_wait_nanos
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    /// Records operations accepted by the backend submit path.
    #[inline]
    pub(crate) fn record_submitted_ops(&self, submitted_ops: usize) {
        if submitted_ops != 0 {
            self.0
                .submitted_ops
                .fetch_add(submitted_ops, Ordering::Relaxed);
        }
    }

    /// Records completions returned by the backend wait path.
    #[inline]
    pub(crate) fn record_wait_completions(&self, wait_completions: usize) {
        if wait_completions != 0 {
            self.0
                .wait_completions
                .fetch_add(wait_completions, Ordering::Relaxed);
        }
    }

    /// Returns the allocation identity of the shared stats counters.
    #[cfg(test)]
    #[inline]
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}

/// Snapshot of shared-storage service ingress and scheduler activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StorageServiceStats {
    /// Number of admitted table-file or readonly-cache read requests.
    pub(crate) table_read_requests: usize,
    /// Number of admitted evictable-pool page-in read requests.
    pub(crate) pool_read_requests: usize,
    /// Number of admitted shared background-write requests.
    pub(crate) background_write_requests: usize,
    /// Number of scheduler turns consumed by the table-read lane.
    pub(crate) table_read_turns: usize,
    /// Number of scheduler turns consumed by the pool-read lane.
    pub(crate) pool_read_turns: usize,
    /// Number of scheduler turns consumed by the background-write lane.
    pub(crate) background_write_turns: usize,
}

impl StorageServiceStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn delta_since(self, earlier: StorageServiceStats) -> StorageServiceStats {
        StorageServiceStats {
            table_read_requests: self
                .table_read_requests
                .saturating_sub(earlier.table_read_requests),
            pool_read_requests: self
                .pool_read_requests
                .saturating_sub(earlier.pool_read_requests),
            background_write_requests: self
                .background_write_requests
                .saturating_sub(earlier.background_write_requests),
            table_read_turns: self
                .table_read_turns
                .saturating_sub(earlier.table_read_turns),
            pool_read_turns: self.pool_read_turns.saturating_sub(earlier.pool_read_turns),
            background_write_turns: self
                .background_write_turns
                .saturating_sub(earlier.background_write_turns),
        }
    }
}

#[derive(Default)]
struct StorageServiceStatsCounters {
    table_read_requests: AtomicUsize,
    pool_read_requests: AtomicUsize,
    background_write_requests: AtomicUsize,
    table_read_turns: AtomicUsize,
    pool_read_turns: AtomicUsize,
    background_write_turns: AtomicUsize,
}

/// Shared recorder for storage-lane admission and scheduler turns.
#[derive(Clone, Default)]
pub(crate) struct StorageServiceStatsHandle(Arc<StorageServiceStatsCounters>);

impl StorageServiceStatsHandle {
    /// Read independently sampled storage-lane counters.
    #[inline]
    pub(crate) fn snapshot(&self) -> StorageServiceStats {
        StorageServiceStats {
            table_read_requests: self.0.table_read_requests.load(Ordering::Relaxed),
            pool_read_requests: self.0.pool_read_requests.load(Ordering::Relaxed),
            background_write_requests: self.0.background_write_requests.load(Ordering::Relaxed),
            table_read_turns: self.0.table_read_turns.load(Ordering::Relaxed),
            pool_read_turns: self.0.pool_read_turns.load(Ordering::Relaxed),
            background_write_turns: self.0.background_write_turns.load(Ordering::Relaxed),
        }
    }

    /// Record a successfully admitted request for its lane.
    #[inline]
    pub(crate) fn record_request(&self, lane_id: StorageLaneId) {
        match lane_id {
            StorageLaneId::TableReads => {
                self.0.table_read_requests.fetch_add(1, Ordering::Relaxed);
            }
            StorageLaneId::PoolReads => {
                self.0.pool_read_requests.fetch_add(1, Ordering::Relaxed);
            }
            StorageLaneId::BackgroundWrites => {
                self.0
                    .background_write_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record one scheduler turn consumed by the lane.
    #[inline]
    pub(crate) fn record_turn(&self, lane_id: StorageLaneId) {
        match lane_id {
            StorageLaneId::TableReads => {
                self.0.table_read_turns.fetch_add(1, Ordering::Relaxed);
            }
            StorageLaneId::PoolReads => {
                self.0.pool_read_turns.fetch_add(1, Ordering::Relaxed);
            }
            StorageLaneId::BackgroundWrites => {
                self.0
                    .background_write_turns
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Convert internal storage-service counters into a public snapshot.
#[inline]
pub(crate) fn storage_io_stats_snapshot(
    backend: BackendStats,
    storage: StorageServiceStats,
) -> StorageIoStats {
    StorageIoStats {
        backend: io_backend_stats_snapshot(backend),
        table_read_requests: storage.table_read_requests,
        pool_read_requests: storage.pool_read_requests,
        background_write_requests: storage.background_write_requests,
        table_read_turns: storage.table_read_turns,
        pool_read_turns: storage.pool_read_turns,
        background_write_turns: storage.background_write_turns,
    }
}

#[inline]
fn io_backend_stats_snapshot(stats: BackendStats) -> IoBackendStats {
    IoBackendStats {
        submit_and_wait_calls: stats.submit_and_wait_calls,
        submitted_ops: stats.submitted_ops,
        submit_and_wait_nanos: stats.submit_and_wait_nanos,
        wait_completions: stats.wait_completions,
    }
}
