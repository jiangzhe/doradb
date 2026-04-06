use crate::buffer::page::{Page, PageID, VersionedPageID};
use crate::catalog::TableMetadata;
use crate::file::FileID;
use crate::file::cow_file::{BlockID, INVALID_BLOCK_ID};
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
    has_persisted_block_key: AtomicBool,
    persisted_file_id: AtomicU64,
    persisted_block_id: AtomicU64,
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

    /// Returns the persisted-block identity stored in this frame, if present.
    #[inline]
    pub fn persisted_block_key(&self) -> Option<(FileID, BlockID)> {
        if !self.has_persisted_block_key.load(Ordering::Acquire) {
            return None;
        }
        let file_id = FileID::from(self.persisted_file_id.load(Ordering::Acquire));
        let block_id = self.persisted_block_id.load(Ordering::Acquire);
        Some((file_id, BlockID::from(block_id)))
    }

    /// Updates persisted-block metadata for this frame.
    #[inline]
    pub fn set_persisted_block_key(&self, file_id: FileID, block_id: BlockID) {
        self.persisted_file_id
            .store(file_id.into(), Ordering::Release);
        self.persisted_block_id
            .store(block_id.into(), Ordering::Release);
        self.has_persisted_block_key.store(true, Ordering::Release);
    }

    /// Clears persisted-block metadata for this frame.
    #[inline]
    pub fn clear_persisted_block_key(&self) {
        self.has_persisted_block_key.store(false, Ordering::Release);
        self.persisted_file_id.store(0, Ordering::Release);
        self.persisted_block_id
            .store(INVALID_BLOCK_ID.into(), Ordering::Release);
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
            page_id: PageID::new(0),
            frame_kind: AtomicU8::new(FrameKind::Uninitialized as u8),
            generation: AtomicU64::new(0),
            // by default the page is dirty because no copy on disk.
            dirty: AtomicBool::new(true),
            persisted_file_id: AtomicU64::new(0),
            persisted_block_id: AtomicU64::new(INVALID_BLOCK_ID.into()),
            has_persisted_block_key: AtomicBool::new(false),
            ctx: None,
            page: std::ptr::null_mut(),
        }
    }
}

// SAFETY: `BufferFrame` mutation is externally synchronized by the frame latch,
// and its raw page pointer always targets stable arena-owned page memory.
unsafe impl Send for BufferFrame {}

// SAFETY: sharing `&BufferFrame` across threads is safe because interior
// mutation goes through atomics or latch-protected metadata.
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
    /// EVICTED means this page is spilled to disk and must be reloaded before use.
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
