use crate::buffer::page::{BufferPageKind, Page};
use crate::catalog::TableColumnLayout;
use crate::file::cow_file::INVALID_BLOCK_ID;
use crate::id::{BlockID, FileID, PageID, TrxID};
use crate::latch::HybridLatch;
use crate::recovery::RowRecoveryMap;
use crate::trx::ver_map::RowVersionMap;
use std::ptr::null_mut;
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
pub(crate) struct BufferFrame {
    /* header part */
    pub(super) latch: HybridLatch, // lock proctects free list and page.
    pub(super) page_id: PageID,
    generation: AtomicU64,
    frame_kind: AtomicU8,
    page_kind: AtomicU8,
    dirty: AtomicBool,
    has_persisted_block_key: AtomicBool,
    persisted_file_id: AtomicU64,
    persisted_block_id: AtomicU64,
    /// Context of this buffer frame. It can store additinal contextual information
    /// about the page, e.g. undo map of row page.
    pub(crate) ctx: Option<Box<FrameContext>>,
    pub(super) page: *mut Page,
}

impl BufferFrame {
    /// Returns the physical residency state recorded for this frame.
    #[inline]
    pub(crate) fn kind(&self) -> FrameKind {
        let value = self.frame_kind.load(Ordering::Acquire);
        FrameKind::from(value)
    }

    /// Stores the physical residency state for this frame.
    #[inline]
    pub(crate) fn set_kind(&self, kind: FrameKind) {
        self.frame_kind.store(kind as u8, Ordering::Release);
    }

    /// Returns the logical page image kind stored in this frame.
    #[inline]
    pub(crate) fn page_kind(&self) -> BufferPageKind {
        let value = self.page_kind.load(Ordering::Acquire);
        BufferPageKind::from(value)
    }

    /// Stores the logical page image kind for this frame.
    #[inline]
    pub(crate) fn set_page_kind(&self, kind: BufferPageKind) {
        self.page_kind.store(kind as u8, Ordering::Release);
    }

    /// Attempts to swap the physical residency state and returns the observed value.
    #[inline]
    pub(crate) fn compare_exchange_kind(
        &self,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
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

    /// Returns whether this frame contains unflushed page changes.
    #[inline]
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Returns the current reuse generation for this frame slot.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Advances the frame reuse generation and returns the new value.
    #[inline]
    pub(crate) fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Updates the dirty flag for this frame.
    #[inline]
    pub(crate) fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }

    /// Returns the persisted-block identity stored in this frame, if present.
    #[inline]
    pub(crate) fn persisted_block_key(&self) -> Option<(FileID, BlockID)> {
        if !self.has_persisted_block_key.load(Ordering::Acquire) {
            return None;
        }
        let file_id = FileID::from(self.persisted_file_id.load(Ordering::Acquire));
        let block_id = self.persisted_block_id.load(Ordering::Acquire);
        Some((file_id, BlockID::from(block_id)))
    }

    /// Updates persisted-block metadata for this frame.
    #[inline]
    pub(crate) fn set_persisted_block_key(&self, file_id: FileID, block_id: BlockID) {
        self.persisted_file_id
            .store(file_id.into(), Ordering::Release);
        self.persisted_block_id
            .store(block_id.into(), Ordering::Release);
        self.has_persisted_block_key.store(true, Ordering::Release);
    }

    /// Clears persisted-block metadata for this frame.
    #[inline]
    pub(crate) fn clear_persisted_block_key(&self) {
        self.has_persisted_block_key.store(false, Ordering::Release);
        self.persisted_file_id.store(0, Ordering::Release);
        self.persisted_block_id
            .store(INVALID_BLOCK_ID.into(), Ordering::Release);
    }

    /// Installs row-version context metadata for a row page.
    #[inline]
    pub(crate) fn init_undo_map(&mut self, column_layout: Arc<TableColumnLayout>, max_size: usize) {
        self.ctx = Some(Box::new(FrameContext::RowVerMap(RowVersionMap::new(
            column_layout,
            max_size,
        ))));
    }

    /// Installs row-recovery context metadata for a recovering row page.
    #[inline]
    pub(crate) fn init_recover_map(&mut self, create_cts: TrxID) {
        self.ctx = Some(Box::new(FrameContext::RowRecoveryMap(RowRecoveryMap::new(
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
            page_kind: AtomicU8::new(BufferPageKind::Uninitialized as u8),
            generation: AtomicU64::new(0),
            // by default the page is dirty because no copy on disk.
            dirty: AtomicBool::new(true),
            persisted_file_id: AtomicU64::new(0),
            persisted_block_id: AtomicU64::new(INVALID_BLOCK_ID.into()),
            has_persisted_block_key: AtomicBool::new(false),
            ctx: None,
            page: null_mut(),
        }
    }
}

// SAFETY: `BufferFrame` mutation is externally synchronized by the frame latch,
// and its raw page pointer always targets stable arena-owned page memory.
unsafe impl Send for BufferFrame {}

// SAFETY: sharing `&BufferFrame` across threads is safe because interior
// mutation goes through atomics or latch-protected metadata.
unsafe impl Sync for BufferFrame {}

/// Physical residency state of a buffer frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FrameKind {
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

/// Optional page-specific context stored beside a frame header.
pub(crate) enum FrameContext {
    RowVerMap(RowVersionMap),
    RowRecoveryMap(RowRecoveryMap),
}

impl FrameContext {
    /// Returns the row-version map required by normal runtime operations.
    ///
    /// Recovery-time callers must use [`FrameContext::recover`] or match the
    /// context variant directly until the page is refreshed after redo replay.
    #[inline]
    pub(crate) fn expect_vmap(&self) -> &RowVersionMap {
        match self {
            FrameContext::RowVerMap(ver) => ver,
            FrameContext::RowRecoveryMap(_) => {
                panic!("row-version map required after recovery")
            }
        }
    }

    /// Returns the recovery map when this context stores one.
    #[inline]
    pub(crate) fn recover(&self) -> Option<&RowRecoveryMap> {
        match self {
            FrameContext::RowRecoveryMap(rec) => Some(rec),
            FrameContext::RowVerMap(_) => None,
        }
    }

    /// Returns the mutable recovery map when this context stores one.
    #[inline]
    pub(crate) fn recover_mut(&mut self) -> Option<&mut RowRecoveryMap> {
        match self {
            FrameContext::RowRecoveryMap(rec) => Some(rec),
            FrameContext::RowVerMap(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FrameContext;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::id::TrxID;
    use crate::recovery::RowRecoveryMap;
    use crate::trx::ver_map::RowVersionMap;
    use crate::value::ValKind;
    use std::sync::Arc;

    #[test]
    fn test_expect_vmap_returns_version_map() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I64,
                ColumnAttributes::empty(),
            )],
            Vec::new(),
        )
        .expect("valid table metadata");
        let ctx = FrameContext::RowVerMap(RowVersionMap::new(Arc::clone(&metadata.col), 1));

        assert!(Arc::ptr_eq(&ctx.expect_vmap().column_layout, &metadata.col));
    }

    #[test]
    #[should_panic(expected = "row-version map required after recovery")]
    fn test_expect_vmap_rejects_recovery_map() {
        let ctx = FrameContext::RowRecoveryMap(RowRecoveryMap::new(TrxID::new(1)));
        let _ = ctx.expect_vmap();
    }
}
