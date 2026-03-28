use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard, PageLatchGuard};
use crate::buffer::page::{BufferPage, Page, PageID};
use crate::buffer::util::{deallocate_frame_and_page_arrays, initialize_frame_and_page_arrays};
use crate::buffer::{PoolGuard, PoolIdentity};
use crate::error::Result;
use crate::ptr::UnsafePtr;
use crate::quiescent::{QuiescentBox, QuiescentGuard};

#[derive(Clone)]
pub(crate) struct ArenaGuard {
    inner: UnsafePtr<ArenaInner>,
    keepalive: PoolGuard,
}

impl ArenaGuard {
    #[inline]
    fn inner(&self) -> &ArenaInner {
        // SAFETY: `ArenaGuard` is created only from a stable `QuiescentArena`
        // address and retains one pool keepalive for the full guard lifetime.
        unsafe { &*self.inner.0 }
    }

    #[inline]
    pub(crate) fn frame_kind(&self, page_id: PageID) -> FrameKind {
        self.inner().frame_kind(page_id)
    }

    #[inline]
    pub(crate) fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        self.inner()
            .compare_exchange_frame_kind(page_id, old_kind, new_kind)
    }

    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        self.inner()
            .try_lock_page_exclusive_with(&self.keepalive, page_id)
    }
}

// SAFETY: `ArenaGuard` only shares a stable raw pointer to `ArenaInner` plus
// one retained sync keepalive guard. Callers still need external latch/pool
// synchronization before dereferencing frame/page memory across threads.
unsafe impl Send for ArenaGuard {}

// SAFETY: see `Send` above.
unsafe impl Sync for ArenaGuard {}

/// One-shot owner of the mmap-backed frame/page arrays.
///
/// The frame/page mappings are installed exactly once during construction and
/// must never be reallocated, remapped, or replaced for the full lifetime of
/// this owner. That stable-address invariant is what allows raw frame/page
/// pointers to remain valid while a paired quiescent guard is retained.
pub(crate) struct ArenaInner {
    frames: *mut BufferFrame,
    pages: *mut Page,
    capacity: usize,
}

impl ArenaInner {
    #[inline]
    fn new(capacity: usize) -> Result<Self> {
        // SAFETY: `ArenaInner::drop` destroys initialized frames and unmaps
        // both regions exactly once after the leading keepalive box drains.
        let (frames, pages) = unsafe { initialize_frame_and_page_arrays(capacity)? };
        Ok(Self {
            frames,
            pages,
            capacity,
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
    #[allow(dead_code)]
    pub(crate) fn page_ptr(&self, page_id: PageID) -> UnsafePtr<Page> {
        debug_assert!((page_id as usize) < self.capacity);
        // SAFETY: page memory is one contiguous mmap region indexed by page id.
        unsafe { UnsafePtr(self.pages.add(page_id as usize)) }
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
    pub(crate) fn try_lock_page_exclusive_with(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        let bf = self.frame_ptr(page_id);
        let g = self.frame(page_id).latch.try_exclusive_core();
        g.map(|g| {
            FacadePageGuard::new(PageLatchGuard::new(keepalive.clone(), g), bf).must_exclusive()
        })
    }
}

impl Drop for ArenaInner {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `QuiescentArena` declares `keepalive` before `state`, so the
        // keepalive owner waits for all guards before `ArenaInner::drop`
        // destroys frames and unmaps the backing regions.
        unsafe {
            for frame_id in 0..self.capacity {
                std::ptr::drop_in_place(self.frames.add(frame_id));
            }
            deallocate_frame_and_page_arrays(self.frames, self.pages, self.capacity);
        }
    }
}

pub(crate) struct QuiescentArena {
    // Field order is part of the safety contract. `keepalive` must drop before
    // `state` so owner teardown waits for all outstanding quiescent guards
    // before the frame/page mappings are reclaimed.
    keepalive: QuiescentBox<()>,
    identity: PoolIdentity,
    state: ArenaInner,
}

impl QuiescentArena {
    #[inline]
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        let keepalive = QuiescentBox::new(());
        let identity = keepalive.owner_identity();
        Ok(Self {
            identity,
            keepalive,
            state: ArenaInner::new(capacity)?,
        })
    }

    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.identity
    }

    #[inline]
    fn quiescent_guard(&self) -> QuiescentGuard<()> {
        self.keepalive.guard()
    }

    #[inline]
    pub(crate) fn guard(&self) -> PoolGuard {
        PoolGuard::new(self.identity, self.quiescent_guard().into_sync())
    }

    #[inline]
    pub(crate) fn arena_guard(&self, guard: PoolGuard) -> ArenaGuard {
        guard.assert_matches(self.identity, "arena guard");
        let inner = UnsafePtr(std::ptr::from_ref(&self.state).cast_mut());
        ArenaGuard {
            inner,
            keepalive: guard,
        }
    }

    #[inline]
    pub(crate) fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        self.state.frame_ptr(page_id)
    }

    #[inline]
    pub(crate) fn frame(&self, page_id: PageID) -> &BufferFrame {
        self.state.frame(page_id)
    }

    #[inline]
    pub(crate) fn init_page<T: BufferPage>(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> PageExclusiveGuard<T> {
        keepalive.assert_matches(self.identity, "arena init page");
        let keepalive = keepalive.clone();
        let bf = self.frame_ptr(page_id);
        let mut guard = {
            let frame = self.frame(page_id);
            let g = frame.latch.try_exclusive_core().unwrap();
            frame.bump_generation();
            FacadePageGuard::<T>::new(PageLatchGuard::new(keepalive, g), bf.clone())
                .must_exclusive()
        };
        {
            let frame = guard.bf_mut();
            debug_assert_eq!(frame as *mut BufferFrame, bf.0);
            frame.page_id = page_id;
            frame.ctx = None;
            T::init_frame(frame);
            frame.set_dirty(true);
            frame.clear_persisted_block_key();
        }
        guard.page_mut().zero();
        guard
    }

    #[inline]
    pub(crate) fn try_lock_page_exclusive(
        &self,
        keepalive: &PoolGuard,
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<Page>> {
        keepalive.assert_matches(self.identity, "arena try_lock_page_exclusive");
        self.state.try_lock_page_exclusive_with(keepalive, page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_arena_guard_panics_on_foreign_guard() {
        let arena1 = Box::leak(Box::new(QuiescentArena::new(1).unwrap()));
        let arena2 = Box::leak(Box::new(QuiescentArena::new(1).unwrap()));
        let foreign_guard = arena2.guard();
        let _ = arena1.arena_guard(foreign_guard);
    }
}
