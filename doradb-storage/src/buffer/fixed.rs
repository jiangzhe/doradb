use crate::buffer::frame::BufferFrame;
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::{BufferPage, Page, PageID, INVALID_PAGE_ID};
use crate::buffer::util::{init_bf_exclusive_guard, mmap_allocate, mmap_deallocate};
use crate::buffer::BufferPool;
use crate::error::Validation::Valid;
use crate::error::{Result, Validation};
use crate::latch::LatchFallbackMode;
use crate::latch::Mutex;
use crate::lifetime::StaticLifetime;
use crate::ptr::UnsafePtr;
use std::mem;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};

pub const SAFETY_PAGES: usize = 10;

/// A simple buffer pool with fixed size pre-allocated using mmap() and
/// does not support swap/evict.
pub struct FixedBufferPool {
    frames: *mut BufferFrame,
    pages: *mut Page,
    size: usize,
    allocated: AtomicU64,
    free_list: Mutex<PageID>,
}

impl FixedBufferPool {
    /// Create a buffer pool with given capacity.
    ///
    /// Pool size if total available bytes of this buffer pool.
    /// We will determine the number of pages accordingly.
    /// We separate pages and frames so that pages are always aligned
    /// to the unit of direct IO and can be flushed via libaio.
    #[inline]
    pub fn with_capacity(pool_size: usize) -> Result<Self> {
        let size = pool_size / (mem::size_of::<BufferFrame>() + mem::size_of::<Page>());
        let frame_total_bytes = mem::size_of::<BufferFrame>() * (size + SAFETY_PAGES);
        let page_total_bytes = mem::size_of::<Page>() * (size + SAFETY_PAGES);
        // let dram_total_size = mem::size_of::<BufferFrame>() * (size + SAFETY_PAGES);
        let frames = unsafe { mmap_allocate(frame_total_bytes)? } as *mut BufferFrame;
        let pages = unsafe {
            match mmap_allocate(page_total_bytes) {
                Ok(ptr) => ptr,
                Err(e) => {
                    // cleanup previous allocated memory
                    mmap_deallocate(frames as *mut u8, frame_total_bytes);
                    return Err(e);
                }
            }
        } as *mut Page;
        Ok(FixedBufferPool {
            frames,
            pages,
            size,
            allocated: AtomicU64::new(0),
            free_list: Mutex::new(INVALID_PAGE_ID),
        })
    }

    /// Create a buffer pool with given capacity, leak it to heap
    /// and return the static reference.
    #[inline]
    pub fn with_capacity_static(pool_size: usize) -> Result<&'static Self> {
        let pool = Self::with_capacity(pool_size)?;
        Ok(StaticLifetime::new_static(pool))
    }

    /// Returns the maximum page number of this pool.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    fn try_get_page_from_free_list<T: BufferPage>(&'static self) -> Option<PageExclusiveGuard<T>> {
        unsafe {
            let bf = {
                let mut page_id = self.free_list.lock();
                if *page_id == INVALID_PAGE_ID {
                    return None;
                }
                let bf = self.get_frame(*page_id);
                *page_id = (*bf.0).next_free;
                bf
            };
            (*bf.0).next_free = INVALID_PAGE_ID;
            let g = init_bf_exclusive_guard(bf);
            Some(g)
        }
    }

    #[inline]
    async fn get_page_internal<T: 'static>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> PageGuard<T> {
        unsafe {
            let bf = self.get_frame(page_id);
            let g = (*bf.0).latch.optimistic_fallback(mode).await;
            PageGuard::new(bf, g)
        }
    }

    #[inline]
    unsafe fn get_new_frame(&'static self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        let bf_ptr = self.frames.offset(page_id as isize);
        std::ptr::write(bf_ptr, BufferFrame::default());
        // assign page pointer.
        (*bf_ptr).page = self.pages.offset(page_id as isize);
        UnsafePtr(bf_ptr)
    }

    #[inline]
    unsafe fn get_frame(&'static self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        let bf_ptr = self.frames.offset(page_id as isize);
        UnsafePtr(bf_ptr)
    }
}

impl BufferPool for FixedBufferPool {
    // allocate a new page with exclusive lock.
    #[inline]
    async fn allocate_page<T: BufferPage>(&'static self) -> PageExclusiveGuard<T> {
        // try get from free list.
        if let Some(g) = self.try_get_page_from_free_list() {
            return g;
        }
        // try get from page pool.
        let page_id = self.allocated.fetch_add(1, Ordering::AcqRel);
        if page_id as usize >= self.size {
            panic!("buffer pool full");
        }
        unsafe {
            let bf = self.get_new_frame(page_id);
            (*bf.0).page_id = page_id;
            (*bf.0).next_free = INVALID_PAGE_ID;
            let g = init_bf_exclusive_guard(bf);
            g
        }
    }

    /// Returns the page guard with given page id.
    /// Caller should make sure page id is valid.
    #[inline]
    async fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> PageGuard<T> {
        debug_assert!(
            page_id < self.allocated.load(Ordering::Relaxed),
            "page id out of bound"
        );
        self.get_page_internal(page_id, mode).await
    }

    /// Deallocate page.
    #[inline]
    fn deallocate_page<T: BufferPage>(&'static self, mut g: PageExclusiveGuard<T>) {
        let mut page_id = self.free_list.lock();
        g.bf_mut().next_free = *page_id;
        *page_id = g.page_id();
    }

    /// Get child page by page id provided by parent page.
    /// The parent page guard should be provided because other thread may change page
    /// id concurrently, and the input page id may not be valid through the function
    /// call. So version must be validated before returning the buffer frame.
    #[inline]
    async fn get_child_page<T>(
        &'static self,
        p_guard: &PageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<PageGuard<T>> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            panic!("page id out of bound");
        }
        let g = self.get_page_internal(page_id, mode).await;
        // apply lock coupling.
        // the validation make sure parent page does not change until child
        // page is acquired.
        p_guard.validate().and_then(|_| Valid(g))
    }
}

impl Drop for FixedBufferPool {
    fn drop(&mut self) {
        unsafe {
            let frame_total_bytes = mem::size_of::<BufferFrame>() * (self.size + SAFETY_PAGES);
            mmap_deallocate(self.frames as *mut u8, frame_total_bytes);
            let page_total_bytes = mem::size_of::<Page>() * (self.size + SAFETY_PAGES);
            mmap_deallocate(self.pages as *mut u8, page_total_bytes);
        }
    }
}

unsafe impl Send for FixedBufferPool {}

unsafe impl Sync for FixedBufferPool {}

unsafe impl StaticLifetime for FixedBufferPool {}

impl UnwindSafe for FixedBufferPool {}

impl RefUnwindSafe for FixedBufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::PageOptimisticGuard;
    use crate::index::BlockNode;

    #[test]
    fn test_fixed_buffer_pool() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
            {
                let g: PageExclusiveGuard<BlockNode> = pool.allocate_page().await;
                assert_eq!(g.page_id(), 0);
            }
            {
                let g: PageExclusiveGuard<BlockNode> = pool.allocate_page().await;
                assert_eq!(g.page_id(), 1);
                pool.deallocate_page(g);
                let g: PageExclusiveGuard<BlockNode> = pool.allocate_page().await;
                assert_eq!(g.page_id(), 1);
            }
            {
                let g: PageOptimisticGuard<BlockNode> =
                    pool.get_page(0, LatchFallbackMode::Spin).await.downgrade();
                assert_eq!(unsafe { g.page_id() }, 0);
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }
}
