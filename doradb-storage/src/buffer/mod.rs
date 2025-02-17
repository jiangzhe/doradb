mod fixed;
pub mod frame;
pub mod guard;
pub mod page;
pub mod ptr;

pub use fixed::FixedBufferPool;

use crate::buffer::frame::BufferFrameAware;
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::PageID;
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use crate::trx::undo::UndoMap;
use std::future::Future;

/// Abstraction of buffer pool.
/// The implementation should be a static pointer providing
/// pooling functionality.
pub trait BufferPool: Send + Copy {
    /// Allocate a new page.
    fn allocate_page<T: BufferFrameAware>(
        self,
    ) -> impl Future<Output = PageExclusiveGuard<'static, T>> + Send;

    /// Get page.
    fn get_page<T: BufferFrameAware>(
        self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = PageGuard<'static, T>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferFrameAware>(
        self,
        g: PageExclusiveGuard<'static, T>,
    ) -> impl Future<Output = ()> + Send;

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T>(
        self,
        p_guard: &PageGuard<'static, T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<PageGuard<'static, T>>> + Send;

    // load undo map for a data page with MVCC capability.
    fn load_orphan_undo_map(self, page_id: PageID) -> Option<UndoMap>;

    // save undo map of a data page with MVCC capability.
    fn save_orphan_undo_map(self, page_id: PageID, undo_map: UndoMap);
}
