mod evict;
mod fixed;
pub mod frame;
pub mod guard;
pub mod page;
mod util;

pub use evict::{EvictableBufferPool, EvictableBufferPoolConfig};
pub use fixed::FixedBufferPool;

use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, PageID};
use crate::error::Result;
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};

/// Abstraction of buffer pool.
/// The implementation should be a static pointer providing
/// pooling functionality.
pub trait BufferPool: Send + Sync + UnwindSafe + RefUnwindSafe + StaticLifetime + 'static {
    /// Returns the maximum number of pages that can be allocated.
    fn capacity(&self) -> usize;

    /// Returns the number of allocated pages.
    fn allocated(&self) -> usize;

    /// Allocate a new page.
    fn allocate_page<T: BufferPage>(
        &'static self,
    ) -> impl Future<Output = PageExclusiveGuard<T>> + Send;

    /// Allocate a new page at given id(offset);
    fn allocate_page_at<T: BufferPage>(
        &'static self,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send;

    /// Get page.
    fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = FacadePageGuard<T>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferPage>(&'static self, g: PageExclusiveGuard<T>);

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T>(
        &'static self,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<FacadePageGuard<T>>> + Send;
}
