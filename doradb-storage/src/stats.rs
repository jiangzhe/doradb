//! Public storage-engine runtime statistics.

use std::time::Duration;

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
    /// Scope-close claims that removed their family's last physical entry.
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
    /// Waiter nodes in queued or provisional state.
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

/// Immutable diagnostics for one successful engine bootstrap.
///
/// The outer intervals partition bootstrap wall time. Redo metrics are nested
/// attribution within transaction bootstrap, not additional elapsed time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Internal startup envelope through ready owner assembly.
    pub bootstrap_elapsed: Duration,
    /// Configuration, root setup, and components preceding the catalog.
    pub engine_setup_elapsed: Duration,
    /// Complete catalog construction, including checkpoint loading.
    pub catalog_bootstrap_elapsed: Duration,
    /// Complete transaction-system component construction.
    pub transaction_bootstrap_elapsed: Duration,
    /// Remaining workers, header durability, layout handling, and owner assembly.
    pub runtime_startup_elapsed: Duration,
    /// Intervals nested within transaction-system construction.
    pub phases: RecoveryPhaseTimings,
    /// Observed replay and reconstruction work.
    pub work: RecoveryWorkCounts,
    /// Consumer-side redo stream attribution.
    pub redo: RecoveryRedoMetrics,
    /// At least one diagnostic overflow or invalid subtraction occurred.
    pub saturated: bool,
}

impl RecoveryReport {
    /// Completes derived intervals and counts without changing recovery success.
    pub(crate) fn finish_transaction(&mut self, elapsed: Duration) {
        self.transaction_bootstrap_elapsed = elapsed;
        let mut accounted = Duration::ZERO;
        recovery_add_duration(
            &mut accounted,
            self.phases.preparation_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.user_table_bootstrap_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.redo_planning_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.redo_replay_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.validation_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.absent_file_cleanup_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.hot_index_rebuild_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.redo_repair_planning_elapsed,
            &mut self.saturated,
        );
        recovery_add_duration(
            &mut accounted,
            self.phases.redo_finalize_elapsed,
            &mut self.saturated,
        );
        self.phases.other_elapsed = recovery_sub_duration(elapsed, accounted, &mut self.saturated);
        let work = &mut self.work;
        for count in [
            work.hot_inserts,
            work.hot_updates,
            work.hot_deletes,
            work.cold_deletes,
        ] {
            recovery_add_count(&mut work.user_row_ops_applied, count, &mut self.saturated);
        }
        work.catalog_row_ops_skipped = recovery_sub_count(
            work.catalog_row_ops_seen,
            work.catalog_row_ops_applied,
            &mut self.saturated,
        );
        work.user_row_ops_skipped = recovery_sub_count(
            work.user_row_ops_seen,
            work.user_row_ops_applied,
            &mut self.saturated,
        );
        let redo = &mut self.redo;
        let mut nested = redo.receive_wait_elapsed;
        recovery_add_duration(&mut nested, redo.group_decode_elapsed, &mut self.saturated);
        recovery_add_duration(
            &mut nested,
            redo.reader_shutdown_elapsed,
            &mut self.saturated,
        );
        redo.stream_other_elapsed =
            recovery_sub_duration(redo.stream_refill_elapsed, nested, &mut self.saturated);
        redo.apply_and_dispatch_elapsed = recovery_sub_duration(
            self.phases.redo_replay_elapsed,
            redo.stream_refill_elapsed,
            &mut self.saturated,
        );
    }
}

/// Elapsed intervals within transaction-system bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryPhaseTimings {
    /// Recovery resources, redo discovery, and coordinator construction.
    pub preparation_elapsed: Duration,
    /// Checkpointed user tables, cleanup, and replay-bound seeding.
    pub user_table_bootstrap_elapsed: Duration,
    /// Replay-suffix planning and read-ahead launch.
    pub redo_planning_elapsed: Duration,
    /// Redo stream consumption, application, and termination.
    pub redo_replay_elapsed: Duration,
    /// Catalog, descriptor, table-root, and index lifecycle validation.
    pub validation_elapsed: Duration,
    /// Post-replay provisional-file cleanup.
    pub absent_file_cleanup_elapsed: Duration,
    /// Replay-sidecar consumption and final hot-index reconstruction.
    pub hot_index_rebuild_elapsed: Duration,
    /// Accepted-prefix repair and startup-file policy selection; excludes later repair IO.
    pub redo_repair_planning_elapsed: Duration,
    /// Construction of writable redo startup resources.
    pub redo_finalize_elapsed: Duration,
    /// Remaining transaction-component construction time.
    pub other_elapsed: Duration,
}

/// Integral work observed during recovery; excluded segments have no invented row counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryWorkCounts {
    /// Redo segment filenames discovered at startup.
    pub redo_segments_discovered: u64,
    /// Segments selected for body replay.
    pub redo_segments_selected: u64,
    /// Decoded catalog RowRedo entries, before filtering.
    pub catalog_row_ops_seen: u64,
    /// Successfully applied catalog RowRedo entries, including DDL payloads.
    pub catalog_row_ops_applied: u64,
    /// Decoded catalog RowRedo entries excluded by replay boundaries.
    pub catalog_row_ops_skipped: u64,
    /// Decoded user RowRedo entries, before filtering.
    pub user_row_ops_seen: u64,
    /// Successfully applied user RowRedo entries.
    pub user_row_ops_applied: u64,
    /// Decoded user RowRedo entries excluded by replay boundaries.
    pub user_row_ops_skipped: u64,
    /// Successfully replayed hot inserts.
    pub hot_inserts: u64,
    /// Successfully replayed hot updates.
    pub hot_updates: u64,
    /// Successfully replayed hot deletes.
    pub hot_deletes: u64,
    /// Successfully replayed cold deletes.
    pub cold_deletes: u64,
    /// User tables loaded from the catalog checkpoint.
    pub checkpoint_user_tables: u64,
    /// Successfully allocated replay pages, including pages later dropped.
    pub hot_pages_reconstructed: u64,
    /// Final hot pages visited by index reconstruction.
    pub index_rebuild_pages: u64,
    /// Successful insertions across all active hot indexes.
    pub index_entries_inserted: u64,
}

/// Nested consumer-side stream measurements; worker execution overlaps replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryRedoMetrics {
    /// Complete consumer refill calls, including waits, decoding, and reader termination.
    pub stream_refill_elapsed: Duration,
    /// Channel receive time, including scheduling and immediate receive overhead.
    pub receive_wait_elapsed: Duration,
    /// Transaction-frame deserialization, timed once per validated group.
    pub group_decode_elapsed: Duration,
    /// Reader stop and join time inside stream termination.
    pub reader_shutdown_elapsed: Duration,
    /// Refill time excluding receives, decoding, and reader shutdown.
    pub stream_other_elapsed: Duration,
    /// Replay time excluding refill; includes application, dispatch, and filtering.
    pub apply_and_dispatch_elapsed: Duration,
    /// Complete validated groups decoded.
    pub groups_decoded: u64,
    /// Decoded transactions, including those later filtered.
    pub transactions_decoded: u64,
    /// Data blocks received by the consumer, including terminal/tail blocks.
    pub data_blocks_consumed: u64,
    /// Full buffer bytes consumed, excluding metadata and unused read-ahead.
    pub consumed_bytes: u64,
    /// Logical payload bytes in complete validated groups.
    pub validated_payload_bytes: u64,
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

/// Accumulates a diagnostic count and flags overflow.
pub(crate) fn recovery_add_count(value: &mut u64, increment: u64, saturated: &mut bool) {
    *value = value.checked_add(increment).unwrap_or_else(|| {
        *saturated = true;
        u64::MAX
    });
}

/// Accumulates diagnostic elapsed time and flags overflow.
pub(crate) fn recovery_add_duration(
    value: &mut Duration,
    increment: Duration,
    saturated: &mut bool,
) {
    *value = value.checked_add(increment).unwrap_or_else(|| {
        *saturated = true;
        Duration::MAX
    });
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

fn recovery_sub_count(value: u64, decrement: u64, saturated: &mut bool) -> u64 {
    value.checked_sub(decrement).unwrap_or_else(|| {
        *saturated = true;
        0
    })
}

fn recovery_sub_duration(value: Duration, decrement: Duration, saturated: &mut bool) -> Duration {
    value.checked_sub(decrement).unwrap_or_else(|| {
        *saturated = true;
        Duration::ZERO
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_diagnostic_arithmetic_saturates_without_panicking() {
        let mut saturated = false;
        let mut count = u64::MAX;
        recovery_add_count(&mut count, 1, &mut saturated);
        assert_eq!(count, u64::MAX);
        assert!(saturated);
        saturated = false;
        let mut duration = Duration::MAX;
        recovery_add_duration(&mut duration, Duration::from_nanos(1), &mut saturated);
        assert_eq!(duration, Duration::MAX);
        assert!(saturated);
        saturated = false;
        assert_eq!(recovery_sub_count(0, 1, &mut saturated), 0);
        assert!(saturated);
        saturated = false;
        assert_eq!(
            recovery_sub_duration(Duration::ZERO, Duration::from_nanos(1), &mut saturated),
            Duration::ZERO
        );
        assert!(saturated);
        let mut report = RecoveryReport::default();
        report.phases.redo_replay_elapsed = Duration::from_nanos(1);
        report.finish_transaction(Duration::ZERO);
        assert!(report.saturated);
        assert_eq!(report.phases.other_elapsed, Duration::ZERO);
    }
}
