use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::PageExclusiveGuard;
use crate::io::{IOSubmission, Operation};
use crate::notify::EventNotifyOnDrop;
use std::mem;
use std::sync::Arc;

pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];
pub type PageID = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedPageID {
    pub page_id: PageID,
    pub generation: u64,
}

pub const INVALID_PAGE_ID: PageID = !0;

/// BufferPage is a trait for types that can be treated
/// as a fixed-length page, and maintained by buffer pool.
/// Note the page-like type should contain only plain data which
/// can be copied to disk and loaded to memory.
/// Any tree-like or pointer-based data structure should not
/// exist.
/// Additionally, this type should not impl Drop because we
/// don't expect to drop it when it is swapped to disk.
pub trait BufferPage: Sized + Send + Sync + 'static {
    /// zero the page.
    /// Default implementation is to zero all bytes.
    #[inline]
    fn zero(&mut self) {
        let bytes = mem::size_of::<Self>();
        // SAFETY: `self` points to a live page-sized plain-data value, and
        // zeroing `size_of::<Self>()` bytes stays within that allocation.
        unsafe {
            let ptr = self as *mut Self as *mut u8;
            ptr.write_bytes(0, bytes);
        }
    }

    /// Initialize frame before the first use of this page.
    fn init_frame(frame: &mut BufferFrame) {
        debug_assert_eq!(frame.kind(), FrameKind::Uninitialized);
        frame.set_kind(FrameKind::Hot);
    }

    /// Deinitialize frame before the return of page to buffer pool.
    fn deinit_frame(frame: &mut BufferFrame) {
        frame.set_kind(FrameKind::Uninitialized);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOKind {
    Read,
    Write,
    // Because we gather write requests into batch.
    // There can be short period the read requests runs
    // concurrently while write requests are not submmitted.
    // We let read wait for write, and let write overwrite
    // this value.
    ReadWaitForWrite,
}

/// One evictable-pool writeback submission owned by the generic IO worker.
///
/// Read-miss loads now use the shared generic read-load core in `buffer/load.rs`.
pub struct PageIO {
    pub(crate) key: PageID,
    pub(crate) operation: Operation,
    pub(crate) page_guard: PageExclusiveGuard<Page>,
    // Batch-level completion token cloned into each write submission.
    // The last drop notifies the evictor waiting for the whole write batch.
    pub(crate) batch_done: Option<Arc<EventNotifyOnDrop>>,
}

impl PageIO {
    /// Returns the evictable page id targeted by this submission.
    #[inline]
    pub fn page_id(&self) -> PageID {
        self.page_guard.page_id()
    }
}

impl IOSubmission for PageIO {
    type Key = PageID;

    #[inline]
    fn key(&self) -> &Self::Key {
        &self.key
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

/// Convenient for IO thread to process, no matter
/// what kind this page belongs to.
impl BufferPage for Page {}
