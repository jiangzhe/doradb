use crate::buffer::page::{Page, PageID, INVALID_PAGE_ID};
use crate::latch::HybridLatch;
use crate::trx::undo::UndoMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

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
    frame_kind: AtomicU8,
    dirty: AtomicBool,
    /// Context of this buffer frame. It can store additinal contextual information
    /// about the page, e.g. undo map of row page.
    pub ctx: Option<Box<FrameContext>>,
    pub page: *mut Page,
}

impl BufferFrame {
    #[inline]
    pub fn kind(&self) -> FrameKind {
        let value = self.frame_kind.load(Ordering::Acquire);
        FrameKind::from(value)
    }

    #[inline]
    pub fn set_kind(&self, kind: FrameKind) {
        self.frame_kind.store(kind as u8, Ordering::Release);
    }

    #[inline]
    pub fn compare_exchange_kind(&self, old_kind: FrameKind, new_kind: FrameKind) -> FrameKind {
        match self.frame_kind.compare_exchange(
            old_kind as u8,
            new_kind as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(val) => FrameKind::from(val),
            Err(val) => FrameKind::from(val),
        }
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }
}

impl Default for BufferFrame {
    #[inline]
    fn default() -> Self {
        BufferFrame {
            latch: HybridLatch::new(),
            page_id: 0,
            next_free: INVALID_PAGE_ID,
            frame_kind: AtomicU8::new(FrameKind::Uninitialized as u8),
            // by default the page is dirty because no copy on disk.
            dirty: AtomicBool::new(true),
            ctx: None,
            page: std::ptr::null_mut(),
        }
    }
}

unsafe impl Send for BufferFrame {}

unsafe impl Sync for BufferFrame {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// Uninitialized means this page is only allocated, but not used for any purpose.
    Uninitialized = 0,
    /// Fixed means this page must be fixed in memory. Evict thread will ignore such page.
    Fixed = 1,
    /// HOT means this page is in memory.
    Hot = 2,
    /// COOL means this page is selected as candidate to be spilled to disk.
    Cool = 3,
    /// EVICTING means this page is being evicted.
    Evicting = 4,
    /// EVICTING means this page is spilled to disk.
    Evicted = 5,
}

impl From<u8> for FrameKind {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            0 => FrameKind::Uninitialized,
            1 => FrameKind::Fixed,
            2 => FrameKind::Hot,
            3 => FrameKind::Cool,
            4 => FrameKind::Evicting,
            5 => FrameKind::Evicted,
            _ => unreachable!("invalid frame kind"),
        }
    }
}

pub enum FrameContext {
    UndoMap(UndoMap),
}

impl FrameContext {
    #[inline]
    pub fn undo(&self) -> &UndoMap {
        match self {
            FrameContext::UndoMap(undo) => undo,
        }
    }
}
