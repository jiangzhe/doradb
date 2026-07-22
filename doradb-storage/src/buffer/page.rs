use crate::buffer::guard::PageExclusiveGuard;
use crate::file::BlockKey;
use crate::id::PageID;
use crate::io::{IOSubmission, Operation};
use crate::notify::EventNotifyOnDrop;
use std::mem;
use std::sync::Arc;
use zerocopy::{FromBytes, IntoBytes, KnownLayout};

pub(crate) use super::INVALID_PAGE_ID;

/// Number of bytes in every buffer-pool page image.
pub(crate) const PAGE_SIZE: usize = 64 * 1024;

const _: () = assert!(
    { mem::size_of::<Page>() == PAGE_SIZE },
    "raw page byte array must be exactly PAGE_SIZE"
);

const _: () = assert_buffer_page::<Page>();

/// Raw byte page image used by low-level IO paths.
pub(crate) type Page = [u8; PAGE_SIZE];

// SAFETY: `[u8; PAGE_SIZE]` is exactly one page, has no drop glue, and every
// byte pattern is valid. It is used only as raw bytes under the buffer-pool
// latch and IO ownership protocols.
unsafe impl BufferPage for Page {}

/// Page id plus frame generation captured by a guard or lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedPageID {
    /// Logical frame/page identifier.
    pub page_id: PageID,
    /// Frame reuse generation observed with the id.
    pub generation: u64,
}

/// A page image type that can be stored in the buffer-pool arena.
///
/// # Safety
///
/// Implementors must be exactly [`PAGE_SIZE`] bytes, must not require drop, and
/// every byte pattern including all-zero bytes must be a valid value. The type
/// layout must remain stable for native in-process page images, and all
/// interior mutation must be safe under the buffer-pool latch protocol.
pub(crate) unsafe trait BufferPage:
    FromBytes + IntoBytes + KnownLayout + Sized + Send + Sync + 'static
{
}

/// IO direction or dependency state for one page operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IOKind {
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
pub(crate) struct PageIO {
    /// Backing file/block identity for this IO.
    pub(crate) block_key: BlockKey,
    /// Backend operation descriptor for this page IO.
    pub(crate) operation: Operation,
    /// Exclusive guard retaining the page memory until IO completion.
    pub(crate) page_guard: PageExclusiveGuard<Page>,
    /// Batch-level completion token cloned into each write submission.
    ///
    /// The last drop notifies the evictor waiting for the whole write batch.
    pub(crate) batch_done: Option<Arc<EventNotifyOnDrop>>,
}

impl PageIO {
    /// Returns the evictable page id targeted by this submission.
    #[inline]
    pub(crate) fn page_id(&self) -> PageID {
        self.page_guard.page_id()
    }
}

impl IOSubmission for PageIO {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

/// Compile-time contract assertion for buffer-pool page images.
#[inline]
pub(crate) const fn assert_buffer_page<T: BufferPage>() {
    assert!(mem::size_of::<T>() == PAGE_SIZE);
    assert!(PAGE_SIZE.is_multiple_of(mem::align_of::<T>()));
    assert!(!mem::needs_drop::<T>());
}
