use crate::bitmap::{new_bitmap, Bitmap};
use crate::buffer::frame::BufferFrame;
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::BufferPage;
use crate::error::{Error, Result};
use crate::ptr::UnsafePtr;
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE,
    MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

#[inline]
pub(super) fn init_bf_exclusive_guard<T: BufferPage>(
    bf: UnsafePtr<BufferFrame>,
) -> PageExclusiveGuard<T> {
    unsafe {
        let g = (*bf.0).latch.try_exclusive().unwrap();
        FacadePageGuard::new(bf, g).must_exclusive()
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

pub struct FreeBitmap {
    free_unit_idx: usize,
    bitmap: Box<[u64]>,
}

/// AllocMap is an allocation controller backed by bitmap.
pub struct AllocMap {
    inner: Mutex<FreeBitmap>,
    len: usize,
    allocated: AtomicUsize,
}

impl AllocMap {
    /// Create a new AllocMap.
    #[inline]
    pub fn new(len: usize) -> Self {
        AllocMap {
            inner: Mutex::new(FreeBitmap {
                free_unit_idx: 0,
                bitmap: new_bitmap(len),
            }),
            len,
            allocated: AtomicUsize::new(0),
        }
    }

    /// Returns number of maximum allocations.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns number of allocated objects.
    #[inline]
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Try to allocate a new object, returns index of object.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn try_allocate(&self) -> Option<usize> {
        let unit_end_idx = (self.len + 63) / 64;
        let mut g = self.inner.lock();
        let unit_start_idx = g.free_unit_idx;
        if let Some(idx) = g.bitmap.bitmap_set_first(unit_start_idx, unit_end_idx) {
            if idx < self.len {
                if g.bitmap.bitmap_unit(idx / 64) == 0 {
                    g.free_unit_idx += 1;
                    if g.free_unit_idx == unit_end_idx {
                        g.free_unit_idx = 0;
                    }
                }
                self.allocated.fetch_add(1, Ordering::Relaxed);
                return Some(idx);
            }
        }
        if let Some(idx) = g.bitmap.bitmap_set_first(0, unit_start_idx) {
            if idx < self.len {
                if g.bitmap.bitmap_unit(idx / 64) == 0 {
                    g.free_unit_idx += 1;
                    if g.free_unit_idx == unit_end_idx {
                        g.free_unit_idx = 0;
                    }
                }
                self.allocated.fetch_add(1, Ordering::Relaxed);
                return Some(idx);
            }
        }
        None
    }

    /// Deallocate a object with its index.
    #[inline]
    pub fn deallocate(&self, idx: usize) -> bool {
        debug_assert!(idx < self.len);
        let unit_idx = idx / 64;

        let mut g = self.inner.lock();
        if g.bitmap.bitmap_unset(idx) {
            if g.free_unit_idx > unit_idx {
                g.free_unit_idx = unit_idx;
            }
            self.allocated.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Allocate a new object at given index.
    #[inline]
    pub fn allocate_at(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let mut g = self.inner.lock();
        if g.bitmap.bitmap_set(idx) {
            // Do not update free_unit_idx.
            self.allocated.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Returns whether the object at given position is allocated.
    #[inline]
    pub fn is_allocated(&self, idx: usize) -> bool {
        let g = self.inner.lock();
        g.bitmap.bitmap_get(idx)
    }

    /// Returns allocated ranges.
    #[inline]
    pub fn allocated_ranges(&self) -> Vec<Range<usize>> {
        let mut res = vec![];
        let g = self.inner.lock();
        let mut idx = 0usize;
        for (flag, count) in g.bitmap.bitmap_range_iter(self.len) {
            if flag {
                res.push(idx..idx + count);
            }
            idx += count;
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_alloc_map_concurrent() {
        let bitmap = Arc::new(AllocMap::new(128));
        let mut handles = vec![];

        for i in 0..64 {
            let bitmap = Arc::clone(&bitmap);
            handles.push(thread::spawn(move || {
                assert!(bitmap.allocate_at(i * 2));
                assert!(bitmap.is_allocated(i * 2));
                assert!(bitmap.deallocate(i * 2));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
