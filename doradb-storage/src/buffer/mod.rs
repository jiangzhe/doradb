mod arena;
mod evict;
mod evictor;
mod fixed;
pub mod frame;
pub mod guard;
mod identity;
mod load;
pub mod page;
mod pool_guard;
mod readonly;
mod util;

#[cfg(test)]
pub(crate) use self::readonly::tests::{global_readonly_pool_scope, table_readonly_pool};
pub use evict::EvictableBufferPool;
pub(crate) use evict::{IndexPoolWorkers, MemPoolWorkers};
pub use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub use fixed::FixedBufferPool;
pub use identity::PoolRole;
pub(crate) use identity::{PoolIdentity, RowPoolRole};
pub use pool_guard::{PoolGuard, PoolGuards, PoolGuardsBuilder};
pub(crate) use readonly::DiskPoolWorkers;
pub(crate) use readonly::ReadSubmission;
pub use readonly::{GlobalReadonlyBufferPool, PersistedBlockKey, ReadonlyBufferPool};

/// Physical file identity used by persisted-block mappings and cache invalidation.
pub type PersistedFileID = u64;

use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, PageID, VersionedPageID};
use crate::component::{
    Component, ComponentRegistry, DiskPoolConfig, IndexPool, IndexPoolConfig, MemPool, MetaPool,
    MetaPoolConfig, ShelfScope,
};
use crate::conf::EvictableBufferPoolConfig;
use crate::error::Validation;
use crate::error::{PersistedFileKind, Result};
use crate::io::Completion;
use crate::latch::LatchFallbackMode;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Shared terminal-status cell for one page-sized buffer-pool IO operation.
///
/// Readonly pools use this cell to fan out one deduplicated miss load to many
/// waiters. Evictable pools use the same cell for read reloads and for readers
/// waiting behind writeback of the same page.
pub(crate) type PageIOCompletion = Completion<Result<PageID>>;

/// Validation callback for one persisted readonly-cache page image.
pub(crate) type ReadonlyPageValidator = fn(&[u8], PersistedFileKind, PageID) -> Result<()>;

/// Snapshot of buffer-pool access and IO lifecycle counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
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

impl BufferPoolStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    pub fn delta_since(self, earlier: BufferPoolStats) -> BufferPoolStats {
        BufferPoolStats {
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

#[derive(Clone, Default)]
pub(crate) struct BufferPoolStatsHandle(Arc<BufferPoolStatsCounters>);

impl BufferPoolStatsHandle {
    #[inline]
    pub(crate) fn snapshot(&self) -> BufferPoolStats {
        BufferPoolStats {
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

    #[inline]
    pub(crate) fn record_cache_hit(&self) {
        self.0.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_cache_miss(&self) {
        self.0.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_miss_join(&self) {
        self.0.miss_joins.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_queued_reads(&self, count: usize) {
        if count != 0 {
            self.0.queued_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_running_reads(&self, count: usize) {
        if count != 0 {
            self.0.running_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_completed_reads(&self, count: usize) {
        if count != 0 {
            self.0.completed_reads.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_read_errors(&self, count: usize) {
        if count != 0 {
            self.0.read_errors.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_queued_writes(&self, count: usize) {
        if count != 0 {
            self.0.queued_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_running_writes(&self, count: usize) {
        if count != 0 {
            self.0.running_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_completed_writes(&self, count: usize) {
        if count != 0 {
            self.0.completed_writes.fetch_add(count, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_write_errors(&self, count: usize) {
        if count != 0 {
            self.0.write_errors.fetch_add(count, Ordering::Relaxed);
        }
    }
}

/// Abstraction of buffer pool.
pub trait BufferPool: Send + Sync {
    /// Returns the maximum number of pages that can be allocated.
    fn capacity(&self) -> usize;

    /// Returns the number of allocated pages.
    fn allocated(&self) -> usize;

    /// Returns a cloneable keepalive guard for this pool.
    fn pool_guard(&self) -> PoolGuard;

    /// Allocate a new page.
    ///
    /// Caller must pass the guard from the same pool instance the method is
    /// invoked on. Mismatched pool identities panic.
    fn allocate_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
    ) -> impl Future<Output = PageExclusiveGuard<T>> + Send;

    /// Allocate a new page at given id(offset);
    fn allocate_page_at<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send;

    /// Get page.
    fn get_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Result<FacadePageGuard<T>>> + Send;

    /// Get page by versioned page identity.
    /// Returns None if page is unavailable or version mismatches.
    fn get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Result<Option<FacadePageGuard<T>>>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferPage>(&self, g: PageExclusiveGuard<T>);

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Result<Validation<FacadePageGuard<T>>>> + Send;
}

impl Component for MetaPool {
    type Config = MetaPoolConfig;
    type Owned = FixedBufferPool;
    type Access = Self;

    const NAME: &'static str = "meta_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(FixedBufferPool::with_capacity(
            PoolRole::Meta,
            config.bytes,
        )?)
    }

    #[inline]
    fn access(owner: &crate::quiescent::QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for IndexPool {
    type Config = IndexPoolConfig;
    type Owned = EvictableBufferPool;
    type Access = Self;

    const NAME: &'static str = "index_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let (pool, pending) = EvictableBufferPoolConfig::default()
            .role(PoolRole::Index)
            .max_mem_size(config.bytes)
            .max_file_size(config.max_file_size)
            .data_swap_file(config.swap_file)
            .build_index()?;
        registry.register::<Self>(pool)?;
        shelf.put::<IndexPoolWorkers>(pending)?;
        IndexPoolWorkers::build((), registry, shelf.scope::<IndexPoolWorkers>()).await
    }

    #[inline]
    fn access(owner: &crate::quiescent::QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for MemPool {
    type Config = EvictableBufferPoolConfig;
    type Owned = EvictableBufferPool;
    type Access = Self;

    const NAME: &'static str = "mem_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let (pool, pending) = config.role(PoolRole::Mem).build()?;
        registry.register::<Self>(pool)?;
        shelf.put::<MemPoolWorkers>(pending)?;
        MemPoolWorkers::build((), registry, shelf.scope::<MemPoolWorkers>()).await
    }

    #[inline]
    fn access(owner: &crate::quiescent::QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for crate::DiskPool {
    type Config = DiskPoolConfig;
    type Owned = GlobalReadonlyBufferPool;
    type Access = Self;

    const NAME: &'static str = "disk_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(GlobalReadonlyBufferPool::with_capacity(
            PoolRole::Disk,
            config.bytes,
        )?)?;
        DiskPoolWorkers::build((), registry, shelf.scope::<DiskPoolWorkers>()).await
    }

    #[inline]
    fn access(owner: &crate::quiescent::QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}
