use crate::buffer::guard::PageExclusiveGuard;
use std::mem;

pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];
pub type PageID = u64;
pub const INVALID_PAGE_ID: PageID = !0;

/// BufferPage is a trait for types that can be treated
/// as a fixed-length page, and maintained by buffer pool.
/// Note the page-like type should contain only plain data which
/// can be copied to disk and loaded to memory.
/// Any tree-like or pointer-based data structure should not
/// exist.
/// Additionally, this type should not impl Drop because we
/// don't expect to drop it when it is swapped to disk.
pub trait BufferPage: Sized + 'static {
    /// zero the page.
    /// Default implementation is to zero all bytes.
    #[inline]
    fn zero(&mut self) {
        let bytes = mem::size_of::<Self>();
        unsafe {
            let ptr = self as *mut Self as *mut u8;
            ptr.write_bytes(0, bytes);
        }
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

pub struct PageIO {
    pub page_guard: PageExclusiveGuard<Page>,
    pub kind: IOKind,
}

impl BufferPage for Page {}
