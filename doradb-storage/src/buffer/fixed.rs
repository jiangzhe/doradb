use crate::buffer::frame::BufferFrame;
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, Page, PageID};
use crate::buffer::util::{init_bf_exclusive_guard, mmap_allocate, mmap_deallocate, AllocMap};
use crate::buffer::BufferPool;
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use crate::ptr::UnsafePtr;
use std::mem;
use std::panic::{RefUnwindSafe, UnwindSafe};

pub const SAFETY_PAGES: usize = 10;

/// A simple buffer pool with fixed size pre-allocated using mmap() and
/// does not support swap/evict.
pub struct FixedBufferPool {
    frames: *mut BufferFrame,
    pages: *mut Page,
    size: usize,
    // free_list: Mutex<PageID>,
    alloc_map: AllocMap,
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
            alloc_map: AllocMap::new(size),
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
    async fn get_page_internal<T: 'static>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        unsafe {
            let bf = self.get_frame(page_id);
            let g = (*bf.0).latch.optimistic_fallback(mode).await;
            FacadePageGuard::new(bf, g)
        }
    }

    /// Since all pages are kept in memory, we can use spin mode to eliminate
    /// the cost of async/await calls.
    #[inline]
    pub fn get_page_spin<T: BufferPage>(&'static self, page_id: PageID) -> FacadePageGuard<T> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        self.get_page_spin_internal(page_id)
    }

    #[inline]
    fn get_page_spin_internal<T: 'static>(&'static self, page_id: PageID) -> FacadePageGuard<T> {
        unsafe {
            let bf = self.get_frame(page_id);
            let g = (*bf.0).latch.optimistic_spin();
            FacadePageGuard::new(bf, g)
        }
    }

    #[inline]
    unsafe fn get_new_frame(&'static self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        unsafe {
            let bf_ptr = self.frames.offset(page_id as isize);
            std::ptr::write(bf_ptr, BufferFrame::default());
            // assign page pointer.
            (*bf_ptr).page = self.pages.offset(page_id as isize);
            UnsafePtr(bf_ptr)
        }
    }

    #[inline]
    unsafe fn get_frame(&'static self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        unsafe {
            let bf_ptr = self.frames.offset(page_id as isize);
            UnsafePtr(bf_ptr)
        }
    }

    #[inline]
    unsafe fn allocate_internal<T: BufferPage>(
        &'static self,
        page_id: PageID,
    ) -> PageExclusiveGuard<T> {
        unsafe {
            let bf = self.get_new_frame(page_id as PageID);
            (*bf.0).page_id = page_id as PageID;
            let mut g = init_bf_exclusive_guard::<T>(bf);
            T::init_frame(g.bf_mut());
            g.page_mut().zero();
            g
        }
    }
}

impl BufferPool for FixedBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.size
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.alloc_map.allocated()
    }

    // allocate a new page with exclusive lock.
    #[inline]
    async fn allocate_page<T: BufferPage>(&'static self) -> PageExclusiveGuard<T> {
        match self.alloc_map.try_allocate() {
            Some(page_id) => unsafe { self.allocate_internal(page_id as PageID) },
            None => {
                panic!("buffer pool full");
            }
        }
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &'static self,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        if self.alloc_map.allocate_at(page_id as usize) {
            Ok(unsafe { self.allocate_internal(page_id as PageID) })
        } else {
            Err(Error::BufferPageAlreadyAllocated)
        }
    }

    /// Returns the page guard with given page id.
    /// Caller should make sure page id is valid.
    #[inline]
    async fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        self.get_page_internal(page_id, mode).await
    }

    /// Deallocate page.
    #[inline]
    fn deallocate_page<T: BufferPage>(&'static self, mut g: PageExclusiveGuard<T>) {
        let page_id = g.page_id();
        g.page_mut().zero();
        T::deinit_frame(g.bf_mut());
        let res = self.alloc_map.deallocate(page_id as usize);
        debug_assert!(res);
    }

    #[inline]
    fn evict_page<T: BufferPage>(&'static self, _g: PageExclusiveGuard<T>) {
        panic!("FixedBufferPool does not support page eviction")
    }

    /// Get child page by page id provided by parent page.
    /// The parent page guard should be provided because other thread may change page
    /// id concurrently, and the input page id may not be valid through the function
    /// call. So version must be validated before returning the buffer frame.
    #[inline]
    async fn get_child_page<T>(
        &'static self,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<T>> {
        debug_assert!(
            self.alloc_map.is_allocated(page_id as usize),
            "page not allocated"
        );
        let g = self.get_page_internal::<T>(page_id, mode).await;
        // apply lock coupling.
        // the validation make sure parent page does not change until child
        // page is acquired.
        if p_guard.validate_bool() {
            return Valid(g);
        }
        if g.is_exclusive() {
            unsafe { g.rollback_exclusive_version_change() };
        }
        Validation::Invalid
    }
}

impl Drop for FixedBufferPool {
    fn drop(&mut self) {
        unsafe {
            // We should drop all active frames before deallocating memory.
            // Because there might be some user-defined context objects stored
            // in the frame.
            for allocated_range in self.alloc_map.allocated_ranges() {
                for page_id in allocated_range {
                    let frame_ptr = self.frames.add(page_id);
                    std::ptr::drop_in_place(frame_ptr);
                }
            }
            // Deallocate memory of frames.
            let frame_total_bytes = mem::size_of::<BufferFrame>() * (self.size + SAFETY_PAGES);
            mmap_deallocate(self.frames as *mut u8, frame_total_bytes);
            // Deallocate memory of pages.
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
    use crate::index::BlockNode;

    #[test]
    fn test_fixed_buffer_pool() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
            {
                let g = pool.allocate_page::<BlockNode>().await;
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<BlockNode>().await;
                assert_eq!(g.page_id(), 1);
                pool.deallocate_page(g);
                let g = pool.allocate_page::<BlockNode>().await;
                assert_eq!(g.page_id(), 1);
            }
            {
                let g = pool
                    .get_page::<BlockNode>(0, LatchFallbackMode::Spin)
                    .await
                    .downgrade();
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<BlockNode>().await;
                let page_id = g.page_id();
                drop(g);
                let g = pool.get_page_spin::<BlockNode>(page_id);
                assert!(g.page_id() == page_id);
                drop(g);

                let p = pool.allocate_page::<BlockNode>().await;
                let p = p.downgrade().facade();
                let c = pool
                    .get_child_page::<BlockNode>(&p, page_id, LatchFallbackMode::Shared)
                    .await;
                let c = c.unwrap();
                drop(c);
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }
}
