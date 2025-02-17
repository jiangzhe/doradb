use crate::buffer::page::{Page, PageID, INVALID_PAGE_ID};
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

/// BufferFrame is the header of a page. It contains some
/// metadata of the page and anything that is not suitable
/// storing in the page, e.g. undo map of row page.
#[repr(C, align(128))]
pub struct BufferFrame {
    /* header part */
    pub latch: HybridLatch, // lock proctects free list and page.
    pub page_id: PageID,
    pub next_free: PageID,
    /// Undo Map is only maintained by RowPage.
    /// Once a RowPage is eliminated, the UndoMap is retained by BufferPool
    /// and when the page is reloaded, UndoMap is reattached to page.
    pub undo_map: Option<UndoMap>,
    pub page: *mut Page,
}

impl Default for BufferFrame {
    #[inline]
    fn default() -> Self {
        BufferFrame {
            latch: HybridLatch::new(),
            page_id: 0,
            next_free: INVALID_PAGE_ID,
            undo_map: None,
            page: std::ptr::null_mut(),
        }
    }
}

unsafe impl Sync for BufferFrame {}

/// BufferFrameAware defines callbacks on lifecycle of buffer frame
/// for initialization and de-initialization.
pub trait BufferFrameAware: Sized + 'static {
    /// This callback is called when a page is just loaded into BufferFrame.
    fn on_alloc<P: BufferPool>(pool: P, frame: &mut BufferFrame);

    /// This callback is called when a page is cleaned and return to BufferPool.
    fn on_dealloc<P: BufferPool>(pool: P, frame: &mut BufferFrame);

    /// This callback is called after a page is initialized.
    fn after_init<P: BufferPool>(pool: P, frame: &mut BufferFrame);

    /// Caller must guarantee Self has same size as Page.
    #[inline]
    unsafe fn get(frame: &BufferFrame) -> &Self {
        &*(frame.page as *const Self)
    }
}
