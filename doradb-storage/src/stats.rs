//! Public storage-engine runtime statistics.

use crate::buffer::BufferPoolStats as InternalBufferPoolStats;
use crate::file::fs::StorageServiceStats as InternalStorageServiceStats;
use crate::io::IOBackendStats as InternalIoBackendStats;
use crate::trx::sys::TrxSysStats as InternalTrxSysStats;

/// Monotonic transaction-system, redo, and purge statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionSystemStats {
    /// Number of transactions durably or logically committed.
    pub commit_count: usize,
    /// Number of transactions processed by the log thread.
    pub trx_count: usize,
    /// Total redo log bytes written.
    pub log_bytes: usize,
    /// Number of log sync operations.
    pub sync_count: usize,
    /// Nanoseconds spent syncing redo.
    pub sync_nanos: usize,
    /// Number of redo file seal failures observed.
    pub seal_failure_count: usize,
    /// Number of backend submit-or-wait calls observed by the log thread.
    pub io_submit_and_wait_count: usize,
    /// Total non-overlapping nanoseconds spent in backend submit-or-wait calls.
    pub io_submit_and_wait_nanos: usize,
    /// Number of committed transactions processed by purge.
    pub purge_trx_count: usize,
    /// Number of row undo entries processed by purge.
    pub purge_row_count: usize,
    /// Number of index entries processed by purge.
    pub purge_index_count: usize,
}

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

/// Snapshot of all engine buffer-pool runtime counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    /// Metadata buffer-pool counters.
    pub meta: BufferPoolRuntimeStats,
    /// In-memory row-page buffer-pool counters.
    pub mem: BufferPoolRuntimeStats,
    /// Secondary-index buffer-pool counters.
    pub index: BufferPoolRuntimeStats,
    /// Readonly disk-cache buffer-pool counters.
    pub disk: BufferPoolRuntimeStats,
}

/// Snapshot of one buffer pool's capacity, allocation, and lifecycle counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolRuntimeStats {
    /// Maximum number of pages this pool can allocate or cache.
    pub capacity: usize,
    /// Number of pages currently allocated or mapped.
    pub allocated: usize,
    /// Monotonic access and IO lifecycle counters.
    pub counters: BufferPoolCounters,
}

/// Monotonic buffer-pool access and IO lifecycle counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolCounters {
    /// Number of resident-page accesses satisfied without a miss load.
    pub cache_hits: usize,
    /// Number of logical accesses that missed the resident set.
    pub cache_misses: usize,
    /// Number of miss accesses that joined an existing inflight load.
    pub miss_joins: usize,
    /// Number of read operations queued by the pool.
    pub queued_reads: usize,
    /// Number of read operations accepted into the backend running state.
    pub running_reads: usize,
    /// Number of read operations that reached a terminal state.
    pub completed_reads: usize,
    /// Number of read operations that completed with an error.
    pub read_errors: usize,
    /// Number of write operations queued by the pool.
    pub queued_writes: usize,
    /// Number of write operations accepted into the backend running state.
    pub running_writes: usize,
    /// Number of write operations that reached a terminal state.
    pub completed_writes: usize,
    /// Number of write operations that completed with an error.
    pub write_errors: usize,
}

#[inline]
pub(crate) fn transaction_system_stats_snapshot(
    stats: InternalTrxSysStats,
) -> TransactionSystemStats {
    TransactionSystemStats {
        commit_count: stats.commit_count,
        trx_count: stats.trx_count,
        log_bytes: stats.log_bytes,
        sync_count: stats.sync_count,
        sync_nanos: stats.sync_nanos,
        seal_failure_count: stats.seal_failure_count,
        io_submit_and_wait_count: stats.io_submit_and_wait_count,
        io_submit_and_wait_nanos: stats.io_submit_and_wait_nanos,
        purge_trx_count: stats.purge_trx_count,
        purge_row_count: stats.purge_row_count,
        purge_index_count: stats.purge_index_count,
    }
}

#[inline]
pub(crate) fn storage_io_stats_snapshot(
    backend: InternalIoBackendStats,
    storage: InternalStorageServiceStats,
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
pub(crate) fn buffer_pool_runtime_stats_snapshot(
    capacity: usize,
    allocated: usize,
    counters: InternalBufferPoolStats,
) -> BufferPoolRuntimeStats {
    BufferPoolRuntimeStats {
        capacity,
        allocated,
        counters: buffer_pool_counters_snapshot(counters),
    }
}

#[inline]
fn io_backend_stats_snapshot(stats: InternalIoBackendStats) -> IoBackendStats {
    IoBackendStats {
        submit_and_wait_calls: stats.submit_and_wait_calls,
        submitted_ops: stats.submitted_ops,
        submit_and_wait_nanos: stats.submit_and_wait_nanos,
        wait_completions: stats.wait_completions,
    }
}

#[inline]
fn buffer_pool_counters_snapshot(stats: InternalBufferPoolStats) -> BufferPoolCounters {
    BufferPoolCounters {
        cache_hits: stats.cache_hits,
        cache_misses: stats.cache_misses,
        miss_joins: stats.miss_joins,
        queued_reads: stats.queued_reads,
        running_reads: stats.running_reads,
        completed_reads: stats.completed_reads,
        read_errors: stats.read_errors,
        queued_writes: stats.queued_writes,
        running_writes: stats.running_writes,
        completed_writes: stats.completed_writes,
        write_errors: stats.write_errors,
    }
}
