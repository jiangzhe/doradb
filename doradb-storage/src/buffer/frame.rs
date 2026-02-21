use crate::buffer::BufferPage;
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{INVALID_PAGE_ID, Page, PageID, VersionedPageID};
use crate::catalog::{TableID, TableMetadata};
use crate::latch::HybridLatch;
use crate::ptr::UnsafePtr;
use crate::trx::TrxID;
use crate::trx::recover::RecoverMap;
use crate::trx::ver_map::RowVersionMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const _: () = assert!(
    { std::mem::size_of::<BufferFrame>().is_multiple_of(64) },
    "Size of BufferFrame must be multiply of 64"
);

const _: () = assert!(
    { std::mem::align_of::<BufferFrame>().is_multiple_of(64) },
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
    generation: AtomicU64,
    dirty: AtomicBool,
    readonly_table_id: AtomicU64,
    readonly_block_id: AtomicU64,
    has_readonly_key: AtomicBool,
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
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    #[inline]
    pub fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    #[inline]
    pub fn versioned_page_id(&self) -> VersionedPageID {
        VersionedPageID {
            page_id: self.page_id,
            generation: self.generation(),
        }
    }

    #[inline]
    pub fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }

    /// Returns readonly cache key stored in this frame, if present.
    #[inline]
    pub fn readonly_key(&self) -> Option<(TableID, PageID)> {
        if !self.has_readonly_key.load(Ordering::Acquire) {
            return None;
        }
        let table_id = self.readonly_table_id.load(Ordering::Acquire);
        let block_id = self.readonly_block_id.load(Ordering::Acquire);
        Some((table_id, block_id))
    }

    /// Updates readonly cache key metadata for this frame.
    #[inline]
    pub fn set_readonly_key(&self, table_id: TableID, block_id: PageID) {
        self.readonly_table_id.store(table_id, Ordering::Release);
        self.readonly_block_id.store(block_id, Ordering::Release);
        self.has_readonly_key.store(true, Ordering::Release);
    }

    /// Clears readonly cache key metadata for this frame.
    #[inline]
    pub fn clear_readonly_key(&self) {
        self.has_readonly_key.store(false, Ordering::Release);
        self.readonly_table_id.store(0, Ordering::Release);
        self.readonly_block_id
            .store(INVALID_PAGE_ID, Ordering::Release);
    }

    #[inline]
    pub fn init_undo_map(&mut self, metadata: Arc<TableMetadata>, max_size: usize) {
        self.ctx = Some(Box::new(FrameContext::RowVerMap(RowVersionMap::new(
            metadata, max_size,
        ))));
    }

    #[inline]
    pub fn init_recover_map(&mut self, create_cts: TrxID) {
        self.ctx = Some(Box::new(FrameContext::RecoverMap(RecoverMap::new(
            create_cts,
        ))));
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
            generation: AtomicU64::new(0),
            // by default the page is dirty because no copy on disk.
            dirty: AtomicBool::new(true),
            readonly_table_id: AtomicU64::new(0),
            readonly_block_id: AtomicU64::new(INVALID_PAGE_ID),
            has_readonly_key: AtomicBool::new(false),
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
    RowVerMap(RowVersionMap),
    RecoverMap(RecoverMap),
}

impl FrameContext {
    #[inline]
    pub fn row_ver(&self) -> Option<&RowVersionMap> {
        match self {
            FrameContext::RowVerMap(ver) => Some(ver),
            FrameContext::RecoverMap(_) => None,
        }
    }

    #[inline]
    pub fn recover(&self) -> Option<&RecoverMap> {
        match self {
            FrameContext::RecoverMap(rec) => Some(rec),
            FrameContext::RowVerMap(_) => None,
        }
    }

    #[inline]
    pub fn recover_mut(&mut self) -> Option<&mut RecoverMap> {
        match self {
            FrameContext::RecoverMap(rec) => Some(rec),
            FrameContext::RowVerMap(_) => None,
        }
    }
}

pub(super) struct BufferFrames(pub(super) *mut BufferFrame);

impl BufferFrames {
    #[inline]
    pub(super) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        // SAFETY: frame memory is allocated as a contiguous mmap region and indexed by page id.
        unsafe { UnsafePtr(self.0.add(page_id as usize)) }
    }

    #[inline]
    pub(super) fn frame(&self, page_id: PageID) -> &BufferFrame {
        Self::frame_ref(self.frame_ptr(page_id))
    }

    #[inline]
    pub(super) fn frame_kind(&self, page_id: PageID) -> FrameKind {
        Self::frame_ref(self.frame_ptr(page_id)).kind()
    }

    #[inline]
    pub(super) fn init_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
    ) -> PageExclusiveGuard<T> {
        let bf = self.frame_ptr(page_id);
        let mut guard = {
            let g = Self::frame_ref(bf.clone()).latch.try_exclusive().unwrap();
            Self::frame_ref(bf.clone()).bump_generation();
            FacadePageGuard::<T>::new(bf.clone(), g).must_exclusive()
        };
        Self::with_frame_mut(bf, &mut guard, |frame| {
            frame.page_id = page_id;
            frame.ctx = None;
            T::init_frame(frame);
            frame.next_free = INVALID_PAGE_ID;
            frame.set_dirty(true);
            frame.clear_readonly_key();
        });
        guard.page_mut().zero();
        guard
    }

    #[inline]
    pub(super) fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        Self::frame_ref(self.frame_ptr(page_id)).compare_exchange_kind(old_kind, new_kind)
    }

    #[inline]
    pub(super) fn try_lock_page_exclusive(
        &self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        let bf = self.frame_ptr(page_id);
        let frame = Self::frame_ref(bf.clone());
        frame
            .latch
            .try_exclusive()
            .map(|g| FacadePageGuard::new(bf, g).must_exclusive())
    }

    #[inline]
    pub(super) fn frame_ref(ptr: UnsafePtr<BufferFrame>) -> &'static BufferFrame {
        debug_assert!(!ptr.0.is_null());
        // SAFETY: `ptr` comes from `frame_ptr` and points into the pool-owned frame array.
        unsafe { &*ptr.0 }
    }

    #[inline]
    pub(super) fn with_frame_mut<T: 'static, R>(
        bf: UnsafePtr<BufferFrame>,
        guard: &mut PageExclusiveGuard<T>,
        f: impl FnOnce(&mut BufferFrame) -> R,
    ) -> R {
        let frame = guard.bf_mut();
        debug_assert_eq!(frame as *mut BufferFrame, bf.0);
        f(frame)
    }
}
