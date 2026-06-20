mod arena;
mod evict;
mod evictor;
mod fixed;
pub(crate) mod frame;
pub(crate) mod guard;
mod identity;
mod load;
pub(crate) mod page;
mod pool_guard;
mod readonly;
mod util;

#[cfg(test)]
pub(crate) use self::evict::tests::{
    dispatch_dirty_pages_for_test as test_dispatch_dirty_pages, frame_kind as test_frame_kind,
    io_backend_stats_handle_identity as test_io_backend_stats_handle_identity,
    persist_and_evict_page_for_test as test_persist_and_evict_page,
};
#[cfg(test)]
pub(crate) use self::readonly::tests::{global_readonly_pool_scope, table_readonly_pool};
#[cfg(test)]
pub(crate) use self::tests::test_page_id;
pub(crate) use evict::EvictableBufferPool;
pub(crate) use evict::{
    EvictReadSubmission, EvictSubmission, EvictablePoolStateMachine, PoolRequest,
    build_pool_with_swap_file_field,
};
pub(crate) use evictor::SharedPoolEvictorWorkers;
pub(crate) use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub(crate) use fixed::FixedBufferPool;
pub(crate) use identity::PoolRole;
pub(crate) use identity::{PoolIdentity, RowPoolRole};
pub(crate) use pool_guard::{PoolGuard, PoolGuards};
pub(crate) use readonly::{ReadSubmission, ReadonlyWriteLease, begin_write_barrier};
pub(crate) use readonly::{ReadonlyBlockGuard, ReadonlyBufferPool};

use crate::DiskPool;
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, VersionedPageID};
use crate::component::{
    Component, ComponentRegistry, DiskPoolConfig, IndexPool, IndexPoolConfig, MemPool, MetaPool,
    MetaPoolConfig, ShelfScope,
};
use crate::conf::EvictableBufferPoolConfig;
use crate::error::Validation;
use crate::error::{FileKind, Result};
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::id::{BlockID, PageID};
use crate::io::Completion;
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentBox;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Sentinel page id used when no real page id is available.
pub(crate) const INVALID_PAGE_ID: PageID = PageID::new(u64::MAX);

/// Shared terminal-status cell for one page-sized buffer-pool IO operation.
///
/// Readonly pools use this cell to fan out one deduplicated miss load to many
/// waiters. Evictable pools use the same cell for read reloads and for readers
/// waiting behind writeback of the same page.
pub(crate) type PageIOCompletion = Completion<PageID>;

/// Validation callback for one persisted readonly-cache block image.
pub(crate) type ReadonlyBlockValidator = fn(&[u8], FileKind, BlockID) -> Result<()>;

/// Snapshot of buffer-pool access and IO lifecycle counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BufferPoolStats {
    /// Number of resident-page accesses satisfied without a miss load.
    pub(crate) cache_hits: usize,
    /// Number of logical accesses that missed the resident set.
    pub(crate) cache_misses: usize,
    /// Number of miss accesses that joined an existing inflight load.
    pub(crate) miss_joins: usize,
    /// Number of read operations queued by the pool.
    pub(crate) queued_reads: usize,
    /// Number of read operations accepted into the backend running state.
    pub(crate) running_reads: usize,
    /// Number of read operations that reached a terminal state.
    pub(crate) completed_reads: usize,
    /// Number of read operations that completed with an error.
    pub(crate) read_errors: usize,
    /// Number of write operations queued by the pool.
    pub(crate) queued_writes: usize,
    /// Number of write operations accepted into the backend running state.
    pub(crate) running_writes: usize,
    /// Number of write operations that reached a terminal state.
    pub(crate) completed_writes: usize,
    /// Number of write operations that completed with an error.
    pub(crate) write_errors: usize,
}

impl BufferPoolStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "internal buffer pool stats"))]
    pub(crate) fn delta_since(self, earlier: BufferPoolStats) -> BufferPoolStats {
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

/// Cloneable writer handle for buffer-pool stats counters.
#[derive(Clone, Default)]
pub(crate) struct BufferPoolStatsHandle(Arc<BufferPoolStatsCounters>);

impl BufferPoolStatsHandle {
    /// Returns one point-in-time snapshot of all counters.
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

/// Abstraction of buffer pool.
pub(crate) trait BufferPool: Send + Sync {
    /// Returns the maximum number of pages that can be allocated.
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    fn capacity(&self) -> usize;

    /// Returns the number of allocated pages.
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
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
    ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send;

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
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
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
        let fs = registry.dependency::<FileSystem>()?;
        let (pool, storage) = EvictableBufferPoolConfig::default()
            .role(PoolRole::Index)
            .max_mem_size(config.bytes)
            .max_file_size(config.max_file_size)
            .data_swap_file(config.swap_file)
            .build_index_for_engine(fs)?;
        registry.register::<Self>(pool)?;
        shelf.put::<FileSystemWorkers>(storage)
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
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
        let fs = registry.dependency::<FileSystem>()?;
        let (pool, storage) = config.role(PoolRole::Mem).build_for_engine(fs)?;
        registry.register::<Self>(pool)?;
        shelf.put::<FileSystemWorkers>(storage)
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

impl Component for DiskPool {
    type Config = DiskPoolConfig;
    type Owned = ReadonlyBufferPool;
    type Access = Self;

    const NAME: &'static str = "disk_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let fs = registry.dependency::<FileSystem>()?;
        registry.register::<Self>(ReadonlyBufferPool::with_capacity(
            PoolRole::Disk,
            config.bytes,
            fs,
        )?)
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        Self::from(owner.guard())
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

#[cfg(test)]
pub(crate) mod tests {
    use super::PageID;
    use crate::compression::BitPackable;
    use crate::serde::{Deser, Ser};
    use std::mem;

    #[inline]
    pub(crate) fn test_page_id(value: i32) -> PageID {
        PageID::new(u64::try_from(value).expect("test PageID must be non-negative"))
    }

    #[test]
    fn test_page_id_accessors_and_conversions() {
        let page_id = PageID::new(42);
        assert_eq!(page_id.as_u64(), 42);
        assert_eq!(page_id.as_usize(), 42);
        assert_eq!(PageID::from(42u64), page_id);
        assert_eq!(PageID::from(42u32), page_id);
        assert_eq!(PageID::from(42usize), page_id);
        assert_eq!(u64::from(page_id), 42);
        assert_eq!(usize::from(page_id), 42);
    }

    #[test]
    fn test_page_id_bytes_roundtrip() {
        let page_id = PageID::new(0x0123_4567_89ab_cdef);
        let bytes = page_id.to_le_bytes();
        assert_eq!(bytes, 0x0123_4567_89ab_cdefu64.to_le_bytes());
        assert_eq!(PageID::from_le_bytes(bytes), page_id);
    }

    #[test]
    fn test_page_id_arithmetic() {
        let page_id = PageID::new(10);
        assert_eq!(page_id + 5, PageID::new(15));
        assert_eq!(page_id - 3, PageID::new(7));
        assert_eq!(20u64 - page_id, PageID::new(10));

        let mut next = page_id;
        next += 7;
        assert_eq!(next, PageID::new(17));
        next -= 5;
        assert_eq!(next, PageID::new(12));
    }

    #[test]
    fn test_page_id_partial_eq_signed_and_unsigned() {
        let page_id = PageID::new(7);
        assert_eq!(page_id, 7u64);
        assert_eq!(7u64, page_id);
        assert_eq!(page_id, 7i32);
        assert_eq!(7i32, page_id);
        assert_ne!(page_id, -1i32);
        assert_ne!(-1i32, page_id);
    }

    #[test]
    fn test_page_id_display() {
        assert_eq!(format!("{}", PageID::new(99)), "99");
    }

    #[test]
    fn test_page_id_serde_roundtrip() {
        let page_id = PageID::new(1234);
        assert_eq!(page_id.ser_len(), mem::size_of::<u64>());

        let mut out = vec![0; page_id.ser_len()];
        let end = page_id.ser(&mut out[..], 0);
        assert_eq!(end, out.len());

        let (end, deser) = PageID::deser(&out[..], 0).unwrap();
        assert_eq!(end, out.len());
        assert_eq!(deser, page_id);
    }

    #[test]
    fn test_page_id_bit_packable_contract() {
        let min = PageID::new(10);
        let value = PageID::new(42);
        assert_eq!(PageID::ZERO, PageID::new(0));
        assert_eq!(value.sub_to_u64(min), 32);
        assert_eq!(value.sub_to_u32(min), 32);
        assert_eq!(value.sub_to_u16(min), 32);
        assert_eq!(value.sub_to_u8(min), 32);
        assert_eq!(min.add_from_u32(5), PageID::new(15));
        assert_eq!(min.add_from_u16(6), PageID::new(16));
        assert_eq!(min.add_from_u8(7), PageID::new(17));

        let wrap_min = PageID::new(u64::MAX - 2);
        let wrap_value = PageID::new(1);
        assert_eq!(wrap_value.sub_to_u64(wrap_min), 4);
        assert_eq!(wrap_min.add_from_u32(5), PageID::new(2));
    }

    #[test]
    fn test_test_page_id_accepts_non_negative_values() {
        assert_eq!(test_page_id(0), PageID::new(0));
        assert_eq!(test_page_id(17), PageID::new(17));
    }

    #[test]
    #[should_panic(expected = "test PageID must be non-negative")]
    fn test_test_page_id_panics_on_negative_values() {
        let _ = test_page_id(-1);
    }
}
