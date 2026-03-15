use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, Page, PageID};
use crate::buffer::util::{deallocate_frame_and_page_arrays, initialize_frame_and_page_arrays};
use crate::error::Result;
use crate::ptr::UnsafePtr;
use crate::quiescent::{QuiescentDrain, QuiescentToken, SyncQuiescentToken};

pub(crate) struct QuiescentArena {
    frames: *mut BufferFrame,
    pages: *mut Page,
    capacity: usize,
    drain: QuiescentDrain,
}

impl QuiescentArena {
    #[inline]
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        // SAFETY: the arena owner drops initialized frames and unmaps both
        // regions exactly once in `Drop`, after outstanding leases drain.
        let (frames, pages) = unsafe { initialize_frame_and_page_arrays(capacity)? };
        Ok(Self {
            frames,
            pages,
            capacity,
            drain: QuiescentDrain::new(),
        })
    }

    #[inline]
    pub(crate) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        debug_assert!((page_id as usize) < self.capacity);
        // SAFETY: frame memory is one contiguous mmap region indexed by page id.
        unsafe { UnsafePtr(self.frames.add(page_id as usize)) }
    }

    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        let ptr = self.frame_ptr(page_id);
        // SAFETY: `ptr` indexes the stable frame mmap owned by this arena.
        unsafe { &*ptr.0 }
    }

    #[inline]
    pub(crate) fn init_page<T: BufferPage>(&self, page_id: PageID) -> PageExclusiveGuard<T> {
        self.lease().init_page(page_id)
    }

    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.lease().try_lock_page_exclusive(page_id)
    }

    #[inline]
    pub(crate) fn lease(&self) -> ArenaLease {
        ArenaLease {
            _keepalive: ArenaKeepalive::Direct(self.drain.token()),
            frames: self.frames,
            pages: self.pages,
            capacity: self.capacity,
        }
    }

    #[inline]
    pub(crate) fn lease_source(&self) -> ArenaLeaseSource {
        ArenaLeaseSource {
            keepalive: self.drain.token().into_local(),
            frames: self.frames,
            pages: self.pages,
            capacity: self.capacity,
        }
    }
}

impl Drop for QuiescentArena {
    #[inline]
    fn drop(&mut self) {
        self.drain.wait_for_zero();
        // SAFETY: all outstanding arena leases have drained before teardown
        // reaches here, so no raw frame/page access can outlive this reclaim.
        unsafe {
            for frame_id in 0..self.capacity {
                std::ptr::drop_in_place(self.frames.add(frame_id));
            }
            deallocate_frame_and_page_arrays(self.frames, self.pages, self.capacity);
        }
    }
}

pub(crate) struct ArenaLease {
    _keepalive: ArenaKeepalive,
    frames: *mut BufferFrame,
    #[allow(dead_code)]
    pages: *mut Page,
    capacity: usize,
}

enum ArenaKeepalive {
    Direct(QuiescentToken),
    Shared(SyncQuiescentToken),
}

#[derive(Clone)]
pub(crate) struct ArenaLeaseSource {
    keepalive: SyncQuiescentToken,
    frames: *mut BufferFrame,
    pages: *mut Page,
    capacity: usize,
}

impl ArenaLease {
    #[inline]
    fn hold(&self) {
        match &self._keepalive {
            ArenaKeepalive::Direct(token) => {
                let _ = token;
            }
            ArenaKeepalive::Shared(token) => {
                let _ = token;
            }
        }
    }

    #[inline]
    pub(crate) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        self.hold();
        debug_assert!((page_id as usize) < self.capacity);
        // SAFETY: frame memory is one contiguous mmap region indexed by page id.
        unsafe { UnsafePtr(self.frames.add(page_id as usize)) }
    }

    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        let ptr = self.frame_ptr(page_id);
        // SAFETY: `ptr` indexes the stable frame mmap guarded by this arena lease.
        unsafe { &*ptr.0 }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn page_ptr(&self, page_id: PageID) -> UnsafePtr<Page> {
        self.hold();
        debug_assert!((page_id as usize) < self.capacity);
        // SAFETY: page memory is one contiguous mmap region indexed by page id.
        unsafe { UnsafePtr(self.pages.add(page_id as usize)) }
    }

    #[inline]
    pub(crate) fn frame_ref(&self, bf: UnsafePtr<BufferFrame>) -> &BufferFrame {
        self.hold();
        debug_assert!(self.contains_frame_ptr(bf.clone()));
        // SAFETY: `bf` points into the arena-owned frame mmap, and the lease's
        // keepalive guarantees that mapping remains valid for this borrow.
        unsafe { &*bf.0 }
    }

    #[inline]
    pub(crate) fn contains_frame_ptr(&self, bf: UnsafePtr<BufferFrame>) -> bool {
        self.hold();
        let start = self.frames as usize;
        let end = start + std::mem::size_of::<BufferFrame>() * self.capacity;
        let ptr = bf.0 as usize;
        ptr >= start && ptr < end
    }

    #[inline]
    pub(crate) fn init_page<T: BufferPage>(self, page_id: PageID) -> PageExclusiveGuard<T> {
        let bf = self.frame_ptr(page_id);
        let mut guard = {
            let frame = self.frame(page_id);
            let g = frame.latch.try_exclusive_raw().unwrap();
            frame.bump_generation();
            FacadePageGuard::<T>::new(self, bf.clone(), g).must_exclusive()
        };
        {
            let frame = guard.bf_mut();
            debug_assert_eq!(frame as *mut BufferFrame, bf.0);
            frame.page_id = page_id;
            frame.ctx = None;
            T::init_frame(frame);
            frame.set_dirty(true);
            frame.clear_readonly_key();
        }
        guard.page_mut().zero();
        guard
    }

    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        let bf = self.frame_ptr(page_id);
        let g = {
            let frame = self.frame(page_id);
            frame.latch.try_exclusive_raw()
        };
        g.map(|g| FacadePageGuard::new(self, bf, g).must_exclusive())
    }
}

impl ArenaLeaseSource {
    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        debug_assert!((page_id as usize) < self.capacity);
        // SAFETY: `self.frames` points to the stable arena-backed frame mmap.
        unsafe { &*self.frames.add(page_id as usize) }
    }

    #[inline]
    pub(crate) fn frame_kind(&self, page_id: PageID) -> FrameKind {
        self.frame(page_id).kind()
    }

    #[inline]
    pub(crate) fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        self.frame(page_id)
            .compare_exchange_kind(old_kind, new_kind)
    }

    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.lease().try_lock_page_exclusive(page_id)
    }

    #[inline]
    pub(crate) fn lease(&self) -> ArenaLease {
        ArenaLease {
            _keepalive: ArenaKeepalive::Shared(self.keepalive.clone()),
            frames: self.frames,
            pages: self.pages,
            capacity: self.capacity,
        }
    }
}

unsafe impl Send for ArenaLease {}
unsafe impl Sync for ArenaLease {}
unsafe impl Send for ArenaLeaseSource {}
unsafe impl Sync for ArenaLeaseSource {}
