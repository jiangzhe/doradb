use crate::buffer::SharedEvictionDomainId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

#[derive(Default)]
struct BufferPoolStatsCounters {
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    miss_joins: AtomicUsize,
    queued_reads: AtomicUsize,
    running_reads: AtomicUsize,
    completed_reads: AtomicUsize,
    read_errors: AtomicUsize,
    queued_writes: AtomicUsize,
    running_writes: AtomicUsize,
    completed_writes: AtomicUsize,
    write_errors: AtomicUsize,
}

/// Cloneable writer handle for buffer-pool stats counters.
#[derive(Clone, Default)]
pub(crate) struct BufferPoolStatsHandle(Arc<BufferPoolStatsCounters>);

impl BufferPoolStatsHandle {
    /// Returns one point-in-time snapshot of all counters.
    #[inline]
    pub(crate) fn snapshot(&self) -> BufferPoolCounters {
        BufferPoolCounters {
            cache_hits: self.0.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.0.cache_misses.load(Ordering::Relaxed),
            miss_joins: self.0.miss_joins.load(Ordering::Relaxed),
            queued_reads: self.0.queued_reads.load(Ordering::Relaxed),
            running_reads: self.0.running_reads.load(Ordering::Relaxed),
            completed_reads: self.0.completed_reads.load(Ordering::Relaxed),
            read_errors: self.0.read_errors.load(Ordering::Relaxed),
            queued_writes: self.0.queued_writes.load(Ordering::Relaxed),
            running_writes: self.0.running_writes.load(Ordering::Relaxed),
            completed_writes: self.0.completed_writes.load(Ordering::Relaxed),
            write_errors: self.0.write_errors.load(Ordering::Relaxed),
        }
    }

    /// Records one cache hit.
    #[inline]
    pub(crate) fn record_cache_hit(&self) {
        self.0.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one cache miss.
    #[inline]
    pub(crate) fn record_cache_miss(&self) {
        self.0.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one miss that joined an existing inflight load.
    #[inline]
    pub(crate) fn record_miss_join(&self) {
        self.0.miss_joins.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds queued read operations to the counter set.
    #[inline]
    pub(crate) fn add_queued_reads(&self, count: usize) {
        if count != 0 {
            self.0.queued_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds running read operations to the counter set.
    #[inline]
    pub(crate) fn add_running_reads(&self, count: usize) {
        if count != 0 {
            self.0.running_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds completed read operations to the counter set.
    #[inline]
    pub(crate) fn add_completed_reads(&self, count: usize) {
        if count != 0 {
            self.0.completed_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds failed read operations to the counter set.
    #[inline]
    pub(crate) fn add_read_errors(&self, count: usize) {
        if count != 0 {
            self.0.read_errors.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds queued write operations to the counter set.
    #[inline]
    pub(crate) fn add_queued_writes(&self, count: usize) {
        if count != 0 {
            self.0.queued_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds running write operations to the counter set.
    #[inline]
    pub(crate) fn add_running_writes(&self, count: usize) {
        if count != 0 {
            self.0.running_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds completed write operations to the counter set.
    #[inline]
    pub(crate) fn add_completed_writes(&self, count: usize) {
        if count != 0 {
            self.0.completed_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Adds failed write operations to the counter set.
    #[inline]
    pub(crate) fn add_write_errors(&self, count: usize) {
        if count != 0 {
            self.0.write_errors.fetch_add(count, Ordering::Relaxed);
        }
    }
}

/// Snapshot of shared-evictor wake and domain-execution activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SharedPoolEvictorStats {
    /// Number of wakeups observed after the evictor blocked for work.
    pub(crate) wake_count: usize,
    /// Number of times the evictor blocked waiting for work.
    pub(crate) wait_count: usize,
    /// Number of readonly-domain runs completed by the shared evictor.
    pub(crate) readonly_runs: usize,
    /// Number of mem-pool-domain runs completed by the shared evictor.
    pub(crate) mem_runs: usize,
    /// Number of index-pool-domain runs completed by the shared evictor.
    pub(crate) index_runs: usize,
}

impl SharedPoolEvictorStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "internal buffer pool stats"))]
    pub(crate) fn delta_since(self, earlier: SharedPoolEvictorStats) -> SharedPoolEvictorStats {
        SharedPoolEvictorStats {
            wake_count: self.wake_count.saturating_sub(earlier.wake_count),
            wait_count: self.wait_count.saturating_sub(earlier.wait_count),
            readonly_runs: self.readonly_runs.saturating_sub(earlier.readonly_runs),
            mem_runs: self.mem_runs.saturating_sub(earlier.mem_runs),
            index_runs: self.index_runs.saturating_sub(earlier.index_runs),
        }
    }
}

#[derive(Default)]
struct SharedPoolEvictorStatsCounters {
    wake_count: AtomicUsize,
    wait_count: AtomicUsize,
    readonly_runs: AtomicUsize,
    mem_runs: AtomicUsize,
    index_runs: AtomicUsize,
}

/// Cloneable writer handle for shared-evictor stats counters.
#[derive(Clone, Default)]
pub(crate) struct SharedPoolEvictorStatsHandle(Arc<SharedPoolEvictorStatsCounters>);

impl SharedPoolEvictorStatsHandle {
    /// Returns one point-in-time snapshot of all counters.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "internal buffer pool stats"))]
    pub(crate) fn snapshot(&self) -> SharedPoolEvictorStats {
        SharedPoolEvictorStats {
            wake_count: self.0.wake_count.load(Ordering::Relaxed),
            wait_count: self.0.wait_count.load(Ordering::Relaxed),
            readonly_runs: self.0.readonly_runs.load(Ordering::Relaxed),
            mem_runs: self.0.mem_runs.load(Ordering::Relaxed),
            index_runs: self.0.index_runs.load(Ordering::Relaxed),
        }
    }

    /// Record the evictor parking for work.
    #[inline]
    pub(crate) fn record_wait(&self) {
        self.0.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an evictor wake after blocking.
    #[inline]
    pub(crate) fn record_wake(&self) {
        self.0.wake_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed run for its owning eviction domain.
    #[inline]
    pub(crate) fn record_domain_run(&self, id: SharedEvictionDomainId) {
        match id {
            SharedEvictionDomainId::Readonly => {
                self.0.readonly_runs.fetch_add(1, Ordering::Relaxed);
            }
            SharedEvictionDomainId::Mem => {
                self.0.mem_runs.fetch_add(1, Ordering::Relaxed);
            }
            SharedEvictionDomainId::Index => {
                self.0.index_runs.fetch_add(1, Ordering::Relaxed);
            }
        }
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
