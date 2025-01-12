use crate::buffer::page::{Page, PageID};
use crate::buffer::BufferPool;
use crate::latch::HybridLatch;
use crate::trx::undo::UndoMap;

const _: () = assert!(
    { std::mem::size_of::<BufferFrame>() % 64 == 0 },
    "Size of BufferFrame must be multiply of 64"
);

const _: () = assert!(
    { std::mem::align_of::<BufferFrame>() % 64 == 0 },
    "Align of BufferFrame must be multiply of 64"
);

#[repr(C, align(512))]
pub struct BufferFrame {
    pub header: FrameHeader,
    pub page: Page,
}

pub struct FrameHeader {
    pub page_id: PageID,
    pub next_free: PageID,
    /// Undo Map is only maintained by RowPage.
    /// Once a RowPage is eliminated, the UndoMap is retained by BufferPool
    /// and when the page is reloaded, UndoMap is reattached to page.
    pub undo_map: Option<UndoMap>,
    pub latch: HybridLatch, // lock proctects free list and page.
}

/// BufferFrameAware defines callbacks on lifecycle of buffer frame
/// for initialization and de-initialization.
pub trait BufferFrameAware {
    /// This callback is called when a page is just loaded into BufferFrame.
    fn on_alloc<P: BufferPool>(_pool: &P, _fh: &mut FrameHeader) {}

    /// This callback is called when a page is cleaned and return to BufferPool.
    fn on_dealloc<P: BufferPool>(_pool: &P, _fh: &mut FrameHeader) {}

    /// This callback is called after a page is initialized.
    fn after_init<P: BufferPool>(&mut self, _pool: &P, _fh: &mut FrameHeader) {}
}
