//! Public storage-engine runtime statistics.

use crate::file::fs::StorageServiceStats as InternalStorageServiceStats;
use crate::io::BackendStats as InternalIoBackendStats;
use crate::trx::sys::TrxSysStats as InternalTrxSysStats;

/// Cumulative logical-lock work and current physical representation statistics.
///
/// Monotonic counters describe completed structural work. Current values are
/// point-in-time observations and peak values are monotonic high-water marks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogicalLockStats {
    /// Requests satisfied by an existing exact claim without manager access.
    pub owner_local_exact_covered_hits: u64,
    /// Fresh exact claims published under an unchanged physical family mode.
    pub owner_local_covered_publications: u64,
    /// Exact conversions that preserved the physical family mode.
    pub owner_local_mode_preserving_conversions: u64,
    /// Exact releases that preserved the physical family mode.
    pub owner_local_mode_preserving_releases: u64,
    /// Shared resource-state transitions.
    pub resource_transitions: u64,
    /// Fixed compatibility mode slots examined by shared transitions.
    pub mode_slots_examined: u64,
    /// Immediately accepted first-family physical acquisitions.
    pub immediate_physical_acquisitions: u64,
    /// Successfully strengthened physical family modes.
    pub physical_upgrades: u64,
    /// Requests appended to intrusive FIFO queues.
    pub enqueued_waiters: u64,
    /// Intrusive FIFO append, detach, or unlink mutations.
    pub queue_link_mutations: u64,
    /// Queued cancellations that removed the FIFO head.
    pub cancelled_head_waiters: u64,
    /// Queued cancellations that removed a middle entry.
    pub cancelled_middle_waiters: u64,
    /// Queued cancellations that removed the FIFO tail.
    pub cancelled_tail_waiters: u64,
    /// Provisional physical holders accepted by their notified observer.
    pub provisional_observations: u64,
    /// Waiters promoted into provisional physical holders.
    pub promoted_waiters: u64,
    /// Exact claims visited by indexed scope close.
    pub scope_close_claims_visited: u64,
    /// Scope-close claims that changed physical family state.
    pub scope_close_physical_changes: u64,
    /// Success-only completion objects allocated for blocked requests.
    pub completion_allocations: u64,
    /// Waiter slab vector growth events.
    pub waiter_slab_growths: u64,
    /// Waiter slab vacant-slot reuse events.
    pub waiter_slab_reuses: u64,
    /// Physical resources currently retained by the manager.
    pub current_physical_resources: u64,
    /// Maximum simultaneously retained physical resources.
    pub peak_physical_resources: u64,
    /// Physical family entries currently retained by the manager.
    pub current_physical_families: u64,
    /// Maximum simultaneously retained physical family entries.
    pub peak_physical_families: u64,
    /// FIFO-linked waiters currently retained.
    pub current_linked_waiters: u64,
    /// Maximum simultaneously FIFO-linked waiters.
    pub peak_linked_waiters: u64,
    /// Waiter nodes in queued, provisional, or released state.
    pub current_live_waiter_nodes: u64,
    /// Maximum simultaneously live waiter nodes.
    pub peak_live_waiter_nodes: u64,
}

/// Snapshot of the engine-owned mandatory runtime's fixed task classes.
///
/// Count and duration fields are monotonic diagnostics. Active counts are
/// independently sampled current state, so concurrent snapshots do not promise
/// equations between submitted, started, completed, and active work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MandatoryRuntimeStats {
    /// Accepted caller DDL and maintenance task statistics.
    pub operation: MandatoryTaskStats,
    /// Engine-internal transaction-cleanup task statistics.
    pub transaction_cleanup: MandatoryTaskStats,
}

/// Snapshot of one mandatory runtime task class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MandatoryTaskStats {
    /// Number of tasks successfully accepted and detached for execution.
    pub submitted_count: usize,
    /// Number of tasks that received their first executor poll.
    pub started_count: usize,
    /// Number of tasks that published terminal supervisor handling.
    pub completed_count: usize,
    /// Number of accepted caller tasks that returned an ordinary error.
    pub error_count: usize,
    /// Number of tasks whose supervised execution panicked.
    pub panic_count: usize,
    /// Number of caller observers dropped without consuming their result.
    pub detached_observer_count: usize,
    /// Current tasks retained by the authoritative class admission accounting.
    pub active_count: usize,
    /// Total successful caller-admission wait time in nanoseconds.
    pub admission_wait_nanos: usize,
    /// Total accepted-to-first-poll queue time in nanoseconds.
    pub queue_wait_nanos: usize,
    /// Total first-poll-to-terminal-publication execution time in nanoseconds.
    pub execution_nanos: usize,
}

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

impl BufferPoolCounters {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "internal buffer pool stats"))]
    pub(crate) fn delta_since(self, earlier: Self) -> Self {
        Self {
            cache_hits: self.cache_hits.saturating_sub(earlier.cache_hits),
            cache_misses: self.cache_misses.saturating_sub(earlier.cache_misses),
            miss_joins: self.miss_joins.saturating_sub(earlier.miss_joins),
            queued_reads: self.queued_reads.saturating_sub(earlier.queued_reads),
            running_reads: self.running_reads.saturating_sub(earlier.running_reads),
            completed_reads: self.completed_reads.saturating_sub(earlier.completed_reads),
            read_errors: self.read_errors.saturating_sub(earlier.read_errors),
            queued_writes: self.queued_writes.saturating_sub(earlier.queued_writes),
            running_writes: self.running_writes.saturating_sub(earlier.running_writes),
            completed_writes: self
                .completed_writes
                .saturating_sub(earlier.completed_writes),
            write_errors: self.write_errors.saturating_sub(earlier.write_errors),
        }
    }
}

/// Convert internal transaction-system counters into a public snapshot.
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

/// Convert internal storage-service counters into a public snapshot.
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

/// Convert internal buffer-pool counters into a public runtime snapshot.
#[inline]
pub(crate) fn buffer_pool_runtime_stats_snapshot(
    capacity: usize,
    allocated: usize,
    counters: BufferPoolCounters,
) -> BufferPoolRuntimeStats {
    BufferPoolRuntimeStats {
        capacity,
        allocated,
        counters,
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
