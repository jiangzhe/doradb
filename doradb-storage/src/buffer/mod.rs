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

pub use evict::{EvictableBufferPool, EvictableBufferPoolConfig};
pub use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub use fixed::FixedBufferPool;
pub use identity::PoolRole;
pub(crate) use identity::{PoolIdentity, RowPoolRole};
pub use pool_guard::{PoolGuard, PoolGuards, PoolGuardsBuilder};
pub use readonly::{
    GlobalReadonlyBufferPool, ReadonlyBufferPool, ReadonlyCacheKey, ReadonlyPageSource,
};

/// Physical file identity used by the shared readonly cache.
pub type ReadonlyFileID = u64;

use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, PageID, VersionedPageID};
use crate::error::Result;
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use std::future::Future;

/// Abstraction of buffer pool.
/// The implementation should be a static pointer providing
/// pooling functionality.
pub trait BufferPool: Send + Sync + StaticLifetime + 'static {
    /// Returns the maximum number of pages that can be allocated.
    fn capacity(&self) -> usize;

    /// Returns the number of allocated pages.
    fn allocated(&self) -> usize;

    /// Returns a cloneable keepalive guard for this pool.
    fn guard(&self) -> PoolGuard;

    /// Allocate a new page.
    ///
    /// Caller must pass the guard from the same pool instance the method is
    /// invoked on. Mismatched pool identities panic.
    fn allocate_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
    ) -> impl Future<Output = PageExclusiveGuard<T>> + Send;

    /// Allocate a new page at given id(offset);
    fn allocate_page_at<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send;

    /// Get page.
    fn get_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = FacadePageGuard<T>> + Send;

    /// Get page by versioned page identity.
    /// Returns None if page is unavailable or version mismatches.
    fn try_get_page_versioned<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Option<FacadePageGuard<T>>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferPage>(&'static self, g: PageExclusiveGuard<T>);

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<FacadePageGuard<T>>> + Send;
}
