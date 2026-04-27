use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::PageExclusiveGuard;
use crate::error::{Error, Result};
use crate::file::BlockKey;
use crate::io::{IOSubmission, Operation};
use crate::notify::EventNotifyOnDrop;
use std::mem;
use std::sync::Arc;
use zerocopy::{FromBytes, IntoBytes, KnownLayout};

pub use super::{INVALID_PAGE_ID, PageID};

pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];

const _: () = assert!(
    { mem::size_of::<Page>() == PAGE_SIZE },
    "raw page byte array must be exactly PAGE_SIZE"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionedPageID {
    pub page_id: PageID,
    pub generation: u64,
}

/// Logical identity of bytes stored in one buffer frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BufferPageKind {
    /// The frame has no initialized logical page.
    Uninitialized = 0,
    /// Raw page bytes used only by low-level IO and raw-page tests.
    RawBytes = 1,
    /// In-memory row-store page.
    RowPage = 2,
    /// Secondary-index B-tree node.
    BTreeNode = 3,
    /// Row-page index node.
    RowPageIndexNode = 4,
}

impl BufferPageKind {
    /// Returns a stable human-readable kind name for diagnostics.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            BufferPageKind::Uninitialized => "uninitialized",
            BufferPageKind::RawBytes => "raw page bytes",
            BufferPageKind::RowPage => "row page",
            BufferPageKind::BTreeNode => "B-tree node",
            BufferPageKind::RowPageIndexNode => "row-page index node",
        }
    }
}

impl From<u8> for BufferPageKind {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            0 => BufferPageKind::Uninitialized,
            1 => BufferPageKind::RawBytes,
            2 => BufferPageKind::RowPage,
            3 => BufferPageKind::BTreeNode,
            4 => BufferPageKind::RowPageIndexNode,
            _ => unreachable!("invalid buffer page kind"),
        }
    }
}

/// Seals [`BufferPage`] implementations to page image types audited in this crate.
pub(crate) mod sealed {
    /// Private supertrait for crate-owned buffer page image implementations.
    pub trait Sealed {}
}

/// A page image type that can be stored in the buffer-pool arena.
///
/// # Safety
///
/// Implementors must be exactly [`PAGE_SIZE`] bytes, must not require drop, and
/// every byte pattern including all-zero bytes must be a valid value. The type
/// layout must remain stable for native in-process page images, and all
/// interior mutation must be safe under the buffer-pool latch protocol.
pub unsafe trait BufferPage:
    sealed::Sealed + FromBytes + IntoBytes + KnownLayout + Sized + Send + Sync + 'static
{
    /// Logical kind recorded in the owning buffer frame.
    const KIND: BufferPageKind;

    /// Initialize frame before the first use of this page.
    fn init_frame(frame: &mut BufferFrame) {
        debug_assert_eq!(frame.kind(), FrameKind::Uninitialized);
        frame.set_page_kind(Self::KIND);
        frame.set_kind(FrameKind::Hot);
    }

    /// Deinitialize frame before the return of page to buffer pool.
    fn deinit_frame(frame: &mut BufferFrame) {
        frame.set_page_kind(BufferPageKind::Uninitialized);
        frame.set_kind(FrameKind::Uninitialized);
    }
}

/// Compile-time contract assertion for buffer-pool page images.
#[inline]
pub(crate) const fn assert_buffer_page<T: BufferPage>() {
    assert!(mem::size_of::<T>() == PAGE_SIZE);
    assert!(PAGE_SIZE.is_multiple_of(mem::align_of::<T>()));
    assert!(!mem::needs_drop::<T>());
}

/// Returns an internal error if `frame` does not contain the requested page kind.
#[inline]
pub(crate) fn validate_frame_page_kind<T: BufferPage>(frame: &BufferFrame) -> Result<()> {
    let actual = frame.page_kind();
    if actual == T::KIND {
        Ok(())
    } else {
        Err(Error::buffer_page_kind_mismatch(
            T::KIND.as_str(),
            actual.as_str(),
        ))
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
    pub(crate) block_key: BlockKey,
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
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

/// Convenient for IO thread to process, no matter
/// what kind this page belongs to.
impl sealed::Sealed for Page {}

// SAFETY: `[u8; PAGE_SIZE]` is exactly one page, has no drop glue, and every
// byte pattern is valid. It is used only as raw bytes under the buffer-pool
// latch and IO ownership protocols.
unsafe impl BufferPage for Page {
    const KIND: BufferPageKind = BufferPageKind::RawBytes;
}

const _: () = assert_buffer_page::<Page>();
