mod arena;
mod evict;
mod evictor;
mod fixed;
pub mod frame;
pub mod guard;
mod identity;
pub mod page;
mod pool_guard;
mod readonly;
mod util;

#[cfg(test)]
pub(crate) use self::readonly::tests::{global_readonly_pool_scope, table_readonly_pool};
pub(crate) use evict::MemPoolWorkers;
pub use evict::{EvictableBufferPool, EvictableBufferPoolConfig};
pub use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub use fixed::FixedBufferPool;
pub use identity::PoolRole;
pub(crate) use identity::{PoolIdentity, RowPoolRole};
pub use pool_guard::{PoolGuard, PoolGuards, PoolGuardsBuilder};
pub(crate) use readonly::DiskPoolWorkers;
pub use readonly::{
    GlobalReadonlyBufferPool, ReadonlyBufferPool, ReadonlyCacheKey, ReadonlyPageSource,
};

/// Physical file identity used by the shared readonly cache.
pub type ReadonlyFileID = u64;

use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, PageID, VersionedPageID};
use crate::component::{
    Component, ComponentRegistry, DiskPoolConfig, IndexPool, IndexPoolConfig, MemPool, MetaPool,
    MetaPoolConfig, ShelfScope,
};
use crate::error::Result;
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use std::future::Future;

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
    ) -> impl Future<Output = FacadePageGuard<T>> + Send;

    /// Get page by versioned page identity.
    /// Returns None if page is unavailable or version mismatches.
    fn try_get_page_versioned<T: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Option<FacadePageGuard<T>>> + Send;

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
    ) -> impl Future<Output = Validation<FacadePageGuard<T>>> + Send;
}

impl<T: BufferPool + ?Sized> BufferPool for &T {
    #[inline]
    fn capacity(&self) -> usize {
        T::capacity(*self)
    }

    #[inline]
    fn allocated(&self) -> usize {
        T::allocated(*self)
    }

    #[inline]
    fn pool_guard(&self) -> PoolGuard {
        T::pool_guard(*self)
    }

    #[inline]
    fn allocate_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
    ) -> impl Future<Output = PageExclusiveGuard<U>> + Send {
        T::allocate_page(*self, guard)
    }

    #[inline]
    fn allocate_page_at<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<U>>> + Send {
        T::allocate_page_at(*self, guard, page_id)
    }

    #[inline]
    fn get_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = FacadePageGuard<U>> + Send {
        T::get_page(*self, guard, page_id, mode)
    }

    #[inline]
    fn try_get_page_versioned<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Option<FacadePageGuard<U>>> + Send {
        T::try_get_page_versioned(*self, guard, id, mode)
    }

    #[inline]
    fn deallocate_page<U: BufferPage>(&self, g: PageExclusiveGuard<U>) {
        T::deallocate_page(*self, g)
    }

    #[inline]
    fn get_child_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<U>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<FacadePageGuard<U>>> + Send {
        T::get_child_page(*self, guard, p_guard, page_id, mode)
    }
}

impl<T: BufferPool> BufferPool for QuiescentGuard<T> {
    #[inline]
    fn capacity(&self) -> usize {
        T::capacity(&**self)
    }

    #[inline]
    fn allocated(&self) -> usize {
        T::allocated(&**self)
    }

    #[inline]
    fn pool_guard(&self) -> PoolGuard {
        T::pool_guard(&**self)
    }

    #[inline]
    fn allocate_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
    ) -> impl Future<Output = PageExclusiveGuard<U>> + Send {
        T::allocate_page(&**self, guard)
    }

    #[inline]
    fn allocate_page_at<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<U>>> + Send {
        T::allocate_page_at(&**self, guard, page_id)
    }

    #[inline]
    fn get_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = FacadePageGuard<U>> + Send {
        T::get_page(&**self, guard, page_id, mode)
    }

    #[inline]
    fn try_get_page_versioned<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Option<FacadePageGuard<U>>> + Send {
        T::try_get_page_versioned(&**self, guard, id, mode)
    }

    #[inline]
    fn deallocate_page<U: BufferPage>(&self, g: PageExclusiveGuard<U>) {
        T::deallocate_page(&**self, g)
    }

    #[inline]
    fn get_child_page<U: BufferPage>(
        &self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<U>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<FacadePageGuard<U>>> + Send {
        T::get_child_page(&**self, guard, p_guard, page_id, mode)
    }
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
    type Owned = FixedBufferPool;
    type Access = Self;

    const NAME: &'static str = "index_pool";

    #[inline]
    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(FixedBufferPool::with_capacity(
            PoolRole::Index,
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
