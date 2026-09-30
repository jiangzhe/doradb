use crate::runtime::mandatory::MandatoryTaskResult;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Shared observation counters for one mandatory task class.
#[derive(Default)]
pub(crate) struct MandatoryTaskCounters {
    submitted_count: AtomicUsize,
    started_count: AtomicUsize,
    completed_count: AtomicUsize,
    error_count: AtomicUsize,
    panic_count: AtomicUsize,
    /// Number of caller observers dropped without consuming their result.
    pub(crate) detached_observer_count: AtomicUsize,
    admission_wait_nanos: AtomicUsize,
    queue_wait_nanos: AtomicUsize,
    execution_nanos: AtomicUsize,
}

impl MandatoryTaskCounters {
    /// Record accepted work and its caller-admission delay.
    #[inline]
    pub(crate) fn record_submitted(&self, admission_wait_nanos: usize) {
        self.submitted_count.fetch_add(1, Ordering::Relaxed);
        self.admission_wait_nanos
            .fetch_add(admission_wait_nanos, Ordering::Relaxed);
    }

    /// Record the first poll and accepted-to-first-poll delay.
    #[inline]
    pub(crate) fn record_started(&self, queue_wait_nanos: usize) {
        self.started_count.fetch_add(1, Ordering::Relaxed);
        self.queue_wait_nanos
            .fetch_add(queue_wait_nanos, Ordering::Relaxed);
    }

    /// Record terminal handling before publishing the observer result.
    #[inline]
    pub(crate) fn record_completed(&self, result: MandatoryTaskResult, execution_nanos: usize) {
        match result {
            MandatoryTaskResult::Ok => {}
            MandatoryTaskResult::Error => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
            }
            MandatoryTaskResult::Panic => {
                self.panic_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.execution_nanos
            .fetch_add(execution_nanos, Ordering::Relaxed);
        self.completed_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an observer dropped before consuming its result.
    #[inline]
    pub(crate) fn record_observer_detached(&self) {
        self.detached_observer_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Combine diagnostic counters with the authoritative active count.
    #[inline]
    pub(crate) fn snapshot(&self, active_count: usize) -> MandatoryTaskStats {
        MandatoryTaskStats {
            submitted_count: self.submitted_count.load(Ordering::Relaxed),
            started_count: self.started_count.load(Ordering::Relaxed),
            completed_count: self.completed_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            panic_count: self.panic_count.load(Ordering::Relaxed),
            detached_observer_count: self.detached_observer_count.load(Ordering::Relaxed),
            active_count,
            admission_wait_nanos: self.admission_wait_nanos.load(Ordering::Relaxed),
            queue_wait_nanos: self.queue_wait_nanos.load(Ordering::Relaxed),
            execution_nanos: self.execution_nanos.load(Ordering::Relaxed),
        }
    }
}
