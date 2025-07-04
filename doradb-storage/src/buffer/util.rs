use crate::buffer::frame::BufferFrame;
use crate::buffer::guard::{PageExclusiveGuard, FacadePageGuard};
use crate::buffer::page::BufferPage;
use crate::error::{Error, Result};
use crate::ptr::UnsafePtr;
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE,
    MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, PROT_READ, PROT_WRITE,
};
use parking_lot::Mutex;

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

#[inline]
pub(super) unsafe fn mmap_deallocate(ptr: *mut u8, total_bytes: usize) {
    munmap(ptr as *mut c_void, total_bytes);
}

#[inline]
pub(super) unsafe fn madvise_dontneed(ptr: *mut u8, len: usize) -> bool {
    madvise(ptr as *mut c_void, len, MADV_DONTNEED) == 0
}

#[allow(dead_code)]
#[inline]
pub(super) unsafe fn madvise_remove(ptr: *mut u8, len: usize) -> bool {
    madvise(ptr as *mut c_void, len, MADV_REMOVE) == 0
}

pub struct Bitmap(Box<[u64]>);

impl Bitmap {
    /// Create a new bitmap.
    #[inline]
    pub fn new(nbr_of_bits: usize) -> Self {
        let len = (nbr_of_bits + 63) / 64;
        let data = vec![0; len];
        Bitmap(data.into_boxed_slice())
    }

    /// Returns bit at given position.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        let v = self.0[unit_idx];
        v & (1 << bit_idx) == (1 << bit_idx)
    }

    /// Returns unit at given position.
    #[inline]
    pub fn get_unit(&self, unit_idx: usize) -> u64 {
        self.0[unit_idx]
    }

    /// Set bit to true at given position.
    #[inline]
    pub fn set(&mut self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        if self.0[unit_idx] & (1 << bit_idx) != 0 {
            return false;
        }
        self.0[unit_idx] |= 1 << bit_idx;
        true
    }

    /// Unset given bit to be false.
    #[inline]
    pub fn unset(&mut self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        if self.0[unit_idx] & (1 << bit_idx) == 0 {
            return false;
        }
        self.0[unit_idx] &= !(1 << bit_idx);
        true
    }

    /// Set the first zero bit to true within given range.
    #[inline]
    pub fn set_first(&mut self, unit_start_idx: usize, unit_end_idx: usize) -> Option<usize> {
        let mut unit_idx = unit_start_idx;
        for v in &mut self.0[unit_start_idx..unit_end_idx] {
            let bit_idx = (*v).trailing_ones();
            if bit_idx < 64 {
                *v |= 1 << bit_idx;
                return Some(unit_idx * 64 + bit_idx as usize);
            }
            unit_idx += 1;
        }
        None
    }
}

pub struct FreeBitmap {
    free_unit_idx: usize,
    bitmap: Bitmap,
}

pub struct AllocMap {
    inner: Mutex<FreeBitmap>,
    len: usize,
}

impl AllocMap {
    /// Create a new AllocMap.
    #[inline]
    pub fn new(len: usize) -> Self {
        AllocMap {
            inner: Mutex::new(FreeBitmap {
                free_unit_idx: 0,
                bitmap: Bitmap::new(len),
            }),
            len,
        }
    }

    /// Returns number of maximum allocations.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Allocate a new object, returns the global index of object.
    #[inline]
    pub fn allocate(&self) -> Option<usize> {
        let unit_end_idx = (self.len + 63) / 64;
        let mut g = self.inner.lock();
        let unit_start_idx = g.free_unit_idx;
        if let Some(idx) = g.bitmap.set_first(unit_start_idx, unit_end_idx) {
            if idx < self.len {
                if g.bitmap.get_unit(idx / 64) == 0 {
                    g.free_unit_idx += 1;
                    if g.free_unit_idx == unit_end_idx {
                        g.free_unit_idx = 0;
                    }
                }
                return Some(idx);
            }
        }
        if let Some(idx) = g.bitmap.set_first(0, unit_start_idx) {
            if idx < self.len {
                if g.bitmap.get_unit(idx / 64) == 0 {
                    g.free_unit_idx += 1;
                    if g.free_unit_idx == unit_end_idx {
                        g.free_unit_idx = 0;
                    }
                }
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
        if !g.bitmap.get(idx) {
            return false;
        }
        if g.free_unit_idx > unit_idx {
            g.free_unit_idx = unit_idx;
        }
        g.bitmap.unset(idx)
    }

    /// Allocate a new object at given index.
    #[inline]
    pub fn allocate_at(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let mut g = self.inner.lock();
        if g.bitmap.set(idx) {
            // Do not update free_unit_idx.
            return true;
        }
        false
    }

    #[inline]
    pub fn is_allocated(&self, idx: usize) -> bool {
        let g = self.inner.lock();
        g.bitmap.get(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_bitmap_basic() {
        let bitmap = Bitmap::new(64);
        assert!(!bitmap.get(0));
        assert!(!bitmap.get(63));
    }

    #[test]
    fn test_bitmap_set_unset() {
        let mut bitmap = Bitmap::new(64);

        // Test set
        assert!(bitmap.set(0));
        assert!(bitmap.get(0));
        assert!(!bitmap.set(0)); // Can't set again

        // Test release
        assert!(bitmap.unset(0));
        assert!(!bitmap.get(0));
        assert!(!bitmap.unset(0)); // Can't unset again
    }

    #[test]
    fn test_bitmap_boundary() {
        let mut bitmap = Bitmap::new(64);

        // Test boundary bits
        assert!(bitmap.set(0));
        assert!(bitmap.set(63));

        assert!(bitmap.get(0));
        assert!(bitmap.get(63));

        assert!(bitmap.unset(0));
        assert!(bitmap.unset(63));
    }

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
