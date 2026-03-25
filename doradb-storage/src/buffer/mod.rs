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

/// Shared terminal-status cell for one page-sized buffer-pool IO operation.
///
/// Readonly pools use this cell to fan out one deduplicated miss load to many
/// waiters. Evictable pools use the same cell for read reloads and for readers
/// waiting behind writeback of the same page.
pub(crate) type PageIOCompletion = Completion<Result<PageID>>;

/// Validation callback for one persisted readonly-cache page image.
pub(crate) type ReadonlyPageValidator = fn(&[u8], PersistedFileKind, PageID) -> Result<()>;

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
