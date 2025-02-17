use crate::buffer::frame::{BufferFrame, BufferFrameAware};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::{Page, PageID, INVALID_PAGE_ID};
use crate::buffer::BufferPool;
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use crate::trx::undo::UndoMap;
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_HUGEPAGE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem;
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
    orphan_undo_maps: Mutex<HashMap<PageID, UndoMap>>,
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
                    munmap(frames as *mut c_void, frame_total_bytes);
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
            orphan_undo_maps: Mutex::new(HashMap::new()),
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

    /// Drop static buffer pool.
    ///
    /// # Safety
    ///
    /// Caller must ensure no further use on the deallocated pool.
    pub unsafe fn drop_static(this: &'static Self) {
        drop(Box::from_raw(this as *const Self as *mut Self));
    }

    #[inline]
    fn try_get_page_from_free_list<T: BufferFrameAware>(
        &'static self,
    ) -> Option<PageExclusiveGuard<T>> {
        unsafe {
            let bf = {
                let mut page_id = self.free_list.lock();
                if *page_id == INVALID_PAGE_ID {
                    return None;
                }
                let bf = self.get_frame(*page_id);
                *page_id = (*bf.get()).next_free;
                bf
            };
            (*bf.get()).next_free = INVALID_PAGE_ID;
            T::on_alloc(self, &mut *bf.get());
            let g = init_bf_exclusive_guard(bf);
            Some(g)
        }
    }

    #[inline]
    fn get_page_internal<T>(&self, page_id: PageID, mode: LatchFallbackMode) -> PageGuard<T> {
        unsafe {
            let bf = self.get_frame(page_id);
            let g = (*bf.get()).latch.optimistic_fallback(mode);
            PageGuard::new(bf, g)
        }
    }

    #[inline]
    unsafe fn get_new_frame(&self, page_id: PageID) -> &UnsafeCell<BufferFrame> {
        let bf_ptr = self.frames.offset(page_id as isize);
        std::ptr::write(bf_ptr, BufferFrame::default());
        // assign page pointer.
        (*bf_ptr).page = self.pages.offset(page_id as isize);
        &*(bf_ptr as *mut UnsafeCell<BufferFrame>)
    }

    #[inline]
    unsafe fn get_frame(&self, page_id: PageID) -> &'static UnsafeCell<BufferFrame> {
        let bf_ptr = self.frames.offset(page_id as isize);
        &*(bf_ptr as *mut UnsafeCell<BufferFrame>)
    }
}

impl BufferPool for &'static FixedBufferPool {
    // allocate a new page with exclusive lock.
    #[inline]
    fn allocate_page<T: BufferFrameAware>(self) -> PageExclusiveGuard<'static, T> {
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
            (*bf.get()).page_id = page_id;
            (*bf.get()).next_free = INVALID_PAGE_ID;
            T::on_alloc(self, &mut *bf.get());
            let g = init_bf_exclusive_guard(bf);
            g
        }
    }

    /// Returns the page guard with given page id.
    /// Caller should make sure page id is valid.
    #[inline]
    fn get_page<T>(self, page_id: PageID, mode: LatchFallbackMode) -> PageGuard<'static, T> {
        debug_assert!(
            page_id < self.allocated.load(Ordering::Relaxed),
            "page id out of bound"
        );
        self.get_page_internal(page_id, mode)
    }

    /// Deallocate page.
    #[inline]
    fn deallocate_page<T: BufferFrameAware>(self, mut g: PageExclusiveGuard<'static, T>) {
        T::on_dealloc(self, &mut g.bf_mut());
        let mut page_id = self.free_list.lock();
        g.bf_mut().next_free = *page_id;
        *page_id = g.page_id();
    }

    /// Get child page by page id provided by parent page.
    /// The parent page guard should be provided because other thread may change page
    /// id concurrently, and the input page id may not be valid through the function
    /// call. So version must be validated before returning the buffer frame.
    #[inline]
    fn get_child_page<T>(
        self,
        p_guard: &PageGuard<'static, T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<PageGuard<'static, T>> {
        if page_id >= self.allocated.load(Ordering::Relaxed) {
            panic!("page id out of bound");
        }
        let g = self.get_page_internal(page_id, mode);
        // apply lock coupling.
        // the validation make sure parent page does not change until child
        // page is acquired.
        p_guard.validate().and_then(|_| Valid(g))
    }

    #[inline]
    fn load_orphan_undo_map(self, page_id: PageID) -> Option<UndoMap> {
        let mut g = self.orphan_undo_maps.lock();
        g.remove(&page_id)
    }

    #[inline]
    fn save_orphan_undo_map(self, page_id: PageID, undo_map: UndoMap) {
        debug_assert!(undo_map.occupied() > 0);
        let mut g = self.orphan_undo_maps.lock();
        let res = g.insert(page_id, undo_map);
        debug_assert!(res.is_none());
    }
}

impl Drop for FixedBufferPool {
    fn drop(&mut self) {
        unsafe {
            let frame_total_bytes = mem::size_of::<BufferFrame>() * (self.size + SAFETY_PAGES);
            munmap(self.frames as *mut c_void, frame_total_bytes);
            let page_total_bytes = mem::size_of::<Page>() * (self.size + SAFETY_PAGES);
            munmap(self.pages as *mut c_void, page_total_bytes);
        }
    }
}

unsafe impl Sync for FixedBufferPool {}

unsafe impl StaticLifetime for FixedBufferPool {}

#[inline]
fn init_bf_exclusive_guard<T: BufferFrameAware>(
    bf: &UnsafeCell<BufferFrame>,
) -> PageExclusiveGuard<'_, T> {
    unsafe {
        let g = (*bf.get()).latch.exclusive();
        PageGuard::new(bf, g).block_until_exclusive()
    }
}

#[inline]
unsafe fn mmap_allocate(total_bytes: usize) -> Result<*mut u8> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::guard::PageOptimisticGuard;
    use crate::index::BlockNode;

    #[test]
    fn test_fixed_buffer_pool() {
        let pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page();
            assert_eq!(g.page_id(), 0);
        }
        {
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page();
            assert_eq!(g.page_id(), 1);
            pool.deallocate_page(g);
            let g: PageExclusiveGuard<'_, BlockNode> = pool.allocate_page();
            assert_eq!(g.page_id(), 1);
        }
        {
            let g: PageOptimisticGuard<'_, BlockNode> =
                pool.get_page(0, LatchFallbackMode::Spin).downgrade();
            assert_eq!(unsafe { g.page_id() }, 0);
        }
        unsafe {
            FixedBufferPool::drop_static(pool);
        }
    }
}
