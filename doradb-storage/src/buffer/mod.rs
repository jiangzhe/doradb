mod evict;
mod fixed;
pub mod frame;
pub mod guard;
pub mod page;
pub mod ptr;
mod util;

pub use evict::{EvictableBufferPool, EvictableBufferPoolConfig};
pub use fixed::FixedBufferPool;

use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::{BufferPage, Page, PageID};
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};

/// Abstraction of buffer pool.
/// The implementation should be a static pointer providing
/// pooling functionality.
pub trait BufferPool: Send + Sync + UnwindSafe + RefUnwindSafe + 'static {
    /// Allocate a new page.
    fn allocate_page<T: BufferPage>(
        &'static self,
    ) -> impl Future<Output = PageExclusiveGuard<T>> + Send;

    /// Get page.
    fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = PageGuard<T>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferPage>(&'static self, g: PageExclusiveGuard<T>);

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T>(
        &'static self,
        p_guard: &PageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<PageGuard<T>>> + Send;
}

pub enum BufferRequest {
    Read(PageExclusiveGuard<Page>),
    BatchWrite(Vec<PageExclusiveGuard<Page>>),
    Shutdown,
}
