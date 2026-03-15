use crate::buffer::ReadonlyFileID;
use crate::buffer::page::{INVALID_PAGE_ID, Page, PageID, VersionedPageID};
use crate::catalog::TableMetadata;
use crate::latch::HybridLatch;
use crate::trx::TrxID;
use crate::trx::recover::RecoverMap;
use crate::trx::ver_map::RowVersionMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const BUFFER_FRAME_SIZE_BYTES: usize = 128;

const _: () = assert!(
    { std::mem::size_of::<BufferFrame>() == BUFFER_FRAME_SIZE_BYTES },
    "Size of BufferFrame must be exactly 128 bytes"
);

const _: () = assert!(
    { std::mem::align_of::<BufferFrame>() == BUFFER_FRAME_SIZE_BYTES },
    "Align of BufferFrame must be exactly 128 bytes"
);

/// BufferFrame is the header of a page. It contains some
/// metadata of the page and anything that is not suitable
/// storing in the page, e.g. undo map of row page.
///
/// Layout contract:
/// - `BufferFrame` is intentionally kept at exactly 128 bytes.
/// - field order is part of that contract; adding or reordering fields may
///   change the size and reduce effective buffer-pool capacity.
/// - update the layout deliberately and keep the const asserts below green.
#[repr(C, align(128))]
pub struct BufferFrame {
    /* header part */
    pub latch: HybridLatch, // lock proctects free list and page.
    pub page_id: PageID,
    generation: AtomicU64,
    frame_kind: AtomicU8,
    dirty: AtomicBool,
    has_readonly_key: AtomicBool,
    readonly_file_id: AtomicU64,
    readonly_block_id: AtomicU64,
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
    pub fn readonly_key(&self) -> Option<(ReadonlyFileID, PageID)> {
        if !self.has_readonly_key.load(Ordering::Acquire) {
            return None;
        }
        let file_id = self.readonly_file_id.load(Ordering::Acquire);
        let block_id = self.readonly_block_id.load(Ordering::Acquire);
        Some((file_id, block_id))
    }

    /// Updates readonly cache key metadata for this frame.
    #[inline]
    pub fn set_readonly_key(&self, file_id: ReadonlyFileID, block_id: PageID) {
        self.readonly_file_id.store(file_id, Ordering::Release);
        self.readonly_block_id.store(block_id, Ordering::Release);
        self.has_readonly_key.store(true, Ordering::Release);
    }

    /// Clears readonly cache key metadata for this frame.
    #[inline]
    pub fn clear_readonly_key(&self) {
        self.has_readonly_key.store(false, Ordering::Release);
        self.readonly_file_id.store(0, Ordering::Release);
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
            frame_kind: AtomicU8::new(FrameKind::Uninitialized as u8),
            generation: AtomicU64::new(0),
            // by default the page is dirty because no copy on disk.
            dirty: AtomicBool::new(true),
            readonly_file_id: AtomicU64::new(0),
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
