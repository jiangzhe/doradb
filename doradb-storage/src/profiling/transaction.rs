use crate::log::RedoLog;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Aggregated transaction-system and redo worker statistics.
#[derive(Default)]
pub(crate) struct TrxSysStats {
    /// Number of transactions durably or logically committed.
    pub(crate) commit_count: usize,
    /// Number of transactions processed by the log thread.
    pub(crate) trx_count: usize,
    /// Total redo log bytes written.
    pub(crate) log_bytes: usize,
    /// Number of log sync operations.
    pub(crate) sync_count: usize,
    /// Nanoseconds spent syncing redo.
    pub(crate) sync_nanos: usize,
    /// Number of redo file seal failures observed.
    pub(crate) seal_failure_count: usize,
    /// Number of backend submit-or-wait calls observed by the log thread.
    ///
    /// On `libaio`, one logical IO commonly contributes separate submit and
    /// wait syscalls, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub(crate) io_submit_and_wait_count: usize,
    /// Total non-overlapping nanoseconds spent in backend submit-or-wait calls.
    pub(crate) io_submit_and_wait_nanos: usize,
    /// Number of committed transactions processed by purge.
    pub(crate) purge_trx_count: usize,
    /// Number of row undo entries processed by purge.
    pub(crate) purge_row_count: usize,
    /// Number of index entries processed by purge.
    pub(crate) purge_index_count: usize,
}

impl TrxSysStats {
    /// Read redo, backend I/O, and purge counters from their component owners.
    pub(crate) fn capture(redo_log: &RedoLog) -> Self {
        let mut stats = TrxSysStats::default();
        stats.trx_count += redo_log.stats.trx_count.load(Ordering::Relaxed);
        stats.commit_count += redo_log.stats.commit_count.load(Ordering::Relaxed);
        stats.log_bytes += redo_log.stats.log_bytes.load(Ordering::Relaxed);
        stats.sync_count += redo_log.stats.sync_count.load(Ordering::Relaxed);
        stats.sync_nanos += redo_log.stats.sync_nanos.load(Ordering::Relaxed);
        stats.seal_failure_count += redo_log.stats.seal_failure_count.load(Ordering::Relaxed);
        let io_stats = redo_log.io_backend_stats();
        stats.io_submit_and_wait_count += io_stats.submit_and_wait_calls;
        stats.io_submit_and_wait_nanos += io_stats.submit_and_wait_nanos;
        stats.purge_trx_count += redo_log.stats.purge_trx_count.load(Ordering::Relaxed);
        stats.purge_row_count += redo_log.stats.purge_row_count.load(Ordering::Relaxed);
        stats.purge_index_count += redo_log.stats.purge_index_count.load(Ordering::Relaxed);
        stats
    }
}

/// Atomic counters maintained by the redo log writer.
#[derive(Default)]
pub(crate) struct RedoLogStats {
    /// Number of commit groups completed.
    pub(crate) commit_count: AtomicUsize,
    /// Number of transactions completed through redo.
    pub(crate) trx_count: AtomicUsize,
    /// Total redo bytes written.
    pub(crate) log_bytes: AtomicUsize,
    /// Number of redo file sync calls.
    pub(crate) sync_count: AtomicUsize,
    /// Total nanoseconds spent in redo file sync calls.
    pub(crate) sync_nanos: AtomicUsize,
    /// Number of best-effort redo file seal failures.
    pub(crate) seal_failure_count: AtomicUsize,
    /// Number of transactions handed to purge.
    pub(crate) purge_trx_count: AtomicUsize,
    /// Number of row versions purged.
    pub(crate) purge_row_count: AtomicUsize,
    /// Number of index entries purged.
    pub(crate) purge_index_count: AtomicUsize,
}

impl RedoLogStats {
    /// Accumulate one successfully published redo prefix.
    #[inline]
    pub(crate) fn record(
        &self,
        trx_count: usize,
        commit_count: usize,
        log_bytes: usize,
        sync_count: usize,
        sync_nanos: usize,
    ) {
        self.trx_count.fetch_add(trx_count, Ordering::Relaxed);
        self.commit_count.fetch_add(commit_count, Ordering::Relaxed);
        self.log_bytes.fetch_add(log_bytes, Ordering::Relaxed);
        self.sync_count.fetch_add(sync_count, Ordering::Relaxed);
        self.sync_nanos.fetch_add(sync_nanos, Ordering::Relaxed);
    }
}

/// Convert internal transaction-system counters into a public snapshot.
#[inline]
pub(crate) fn transaction_system_stats_snapshot(stats: TrxSysStats) -> TransactionSystemStats {
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
