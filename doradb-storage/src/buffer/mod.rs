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
pub(crate) use self::evict::tests::{
    dispatch_dirty_pages_for_test as test_dispatch_dirty_pages, frame_kind as test_frame_kind,
    io_backend_stats_handle_identity as test_io_backend_stats_handle_identity,
    persist_and_evict_page_for_test as test_persist_and_evict_page, raw_fd as test_raw_fd,
};
#[cfg(test)]
pub(crate) use self::readonly::tests::{global_readonly_pool_scope, table_readonly_pool};
#[cfg(test)]
pub(crate) use self::tests::test_page_id;
pub use evict::EvictableBufferPool;
pub(crate) use evict::{
    EvictReadSubmission, EvictSubmission, EvictablePoolStateMachine, PoolRequest,
    build_pool_with_swap_file_field,
};
pub(crate) use evictor::SharedPoolEvictorWorkers;
pub use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub use fixed::FixedBufferPool;
pub use identity::PoolRole;
pub(crate) use identity::{PoolIdentity, RowPoolRole};
pub use pool_guard::{PoolGuard, PoolGuards, PoolGuardsBuilder};
pub(crate) use readonly::ReadSubmission;
pub use readonly::{BlockKey, GlobalReadonlyBufferPool, ReadonlyBlockGuard, ReadonlyBufferPool};

/// Physical file identity used by persisted-block mappings and cache invalidation.
pub type PersistedFileID = u64;

use crate::DiskPool;
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, VersionedPageID};
use crate::component::{
    Component, ComponentRegistry, DiskPoolConfig, IndexPool, IndexPoolConfig, MemPool, MetaPool,
    MetaPoolConfig, ShelfScope,
};
use crate::compression::BitPackable;
use crate::conf::EvictableBufferPoolConfig;
use crate::error::Validation;
use crate::error::{FileKind, Result};
use crate::file::BlockID;
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::io::Completion;
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentBox;
use crate::serde::{Deser, Ser, Serde};
use bytemuck::{Pod, Zeroable};
use std::fmt;
use std::future::Future;
use std::mem;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runtime buffer-managed page identity.
///
/// This id is reserved for mutable or cached pages owned by the buffer layer.
/// Persisted fixed-size file units use [`BlockID`] instead.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Zeroable, Pod)]
pub struct PageID(u64);

impl PageID {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn to_le_bytes(self) -> [u8; mem::size_of::<u64>()] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub const fn from_le_bytes(bytes: [u8; mem::size_of::<u64>()]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

impl From<u64> for PageID {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<u32> for PageID {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

impl From<usize> for PageID {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl From<PageID> for u64 {
    #[inline]
    fn from(value: PageID) -> Self {
        value.0
    }
}

impl From<PageID> for usize {
    #[inline]
    fn from(value: PageID) -> Self {
        value.as_usize()
    }
}

impl Add<u64> for PageID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign<u64> for PageID {
    #[inline]
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for PageID {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<u64> for PageID {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl Sub<PageID> for u64 {
    type Output = PageID;

    #[inline]
    fn sub(self, rhs: PageID) -> Self::Output {
        PageID(self - rhs.0)
    }
}

impl PartialEq<u64> for PageID {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<i32> for PageID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<PageID> for u64 {
    #[inline]
    fn eq(&self, other: &PageID) -> bool {
        *self == other.0
    }
}

impl PartialEq<PageID> for i32 {
    #[inline]
    fn eq(&self, other: &PageID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl fmt::Display for PageID {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Ser<'_> for PageID {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.0)
    }
}

impl Deser for PageID {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input
            .deser_u64(start_idx)
            .map(|(idx, raw)| (idx, Self(raw)))
    }
}

impl BitPackable for PageID {
    const ZERO: Self = Self(0);

    #[inline]
    fn sub_to_u64(self, min: Self) -> u64 {
        self.0.wrapping_sub(min.0)
    }

    #[inline]
    fn sub_to_u32(self, min: Self) -> u32 {
        self.0.wrapping_sub(min.0) as u32
    }

    #[inline]
    fn add_from_u32(self, delta: u32) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u16(self, min: Self) -> u16 {
        self.0.wrapping_sub(min.0) as u16
    }

    #[inline]
    fn add_from_u16(self, delta: u16) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u8(self, min: Self) -> u8 {
        self.0.wrapping_sub(min.0) as u8
    }

    #[inline]
    fn add_from_u8(self, delta: u8) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }
}

pub const INVALID_PAGE_ID: PageID = PageID::new(u64::MAX);

/// Shared terminal-status cell for one page-sized buffer-pool IO operation.
///
/// Readonly pools use this cell to fan out one deduplicated miss load to many
/// waiters. Evictable pools use the same cell for read reloads and for readers
/// waiting behind writeback of the same page.
pub(crate) type PageIOCompletion = Completion<Result<PageID>>;

/// Validation callback for one persisted readonly-cache block image.
pub(crate) type ReadonlyBlockValidator = fn(&[u8], FileKind, BlockID) -> Result<()>;

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
    type Owned = GlobalReadonlyBufferPool;
    type Access = Self;

    const NAME: &'static str = "disk_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(GlobalReadonlyBufferPool::with_capacity(
            PoolRole::Disk,
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
