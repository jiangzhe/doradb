use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::BufferPage;
use crate::buffer::page::{INVALID_PAGE_ID, Page, PageID};
use crate::error::{Error, Result};
use crate::ptr::UnsafePtr;
use libc::{
    MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE, c_void, madvise, mmap, munmap,
};

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

#[inline]
pub(super) unsafe fn mmap_allocate(total_bytes: usize) -> Result<*mut u8> {
    unsafe {
        let memory_chunk = mmap(
            std::ptr::null_mut(),
            total_bytes,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0,
        );
        if memory_chunk == MAP_FAILED {
            return Err(Error::InsufficientMemory(total_bytes));
        }
        madvise(memory_chunk, total_bytes, MADV_HUGEPAGE);
        madvise(memory_chunk, total_bytes, MADV_DONTFORK);
        Ok(memory_chunk as *mut u8)
    }
}

#[inline]
pub(super) unsafe fn mmap_deallocate(ptr: *mut u8, total_bytes: usize) {
    unsafe {
        munmap(ptr as *mut c_void, total_bytes);
    }
}

#[inline]
pub(super) unsafe fn madvise_dontneed(ptr: *mut u8, len: usize) -> bool {
    unsafe { madvise(ptr as *mut c_void, len, MADV_DONTNEED) == 0 }
}

#[allow(dead_code)]
#[inline]
pub(super) unsafe fn madvise_remove(ptr: *mut u8, len: usize) -> bool {
    unsafe { madvise(ptr as *mut c_void, len, MADV_REMOVE) == 0 }
}
