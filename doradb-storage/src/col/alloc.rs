use crate::col::repr::ByteRepr;
use std::mem::size_of;
use std::ptr::NonNull;

/// Used internally to make sure all memory allocations
/// are aligned to 16 bytes.
#[repr(C, align(16))]
#[derive(Clone, Copy)]
struct U128(u128);

/// align length to 16 bytes.
#[inline]
pub const fn align_u128(v: usize) -> usize {
    (v + 15) & !15
}

/// This method allocate memory area with alignment of 16 bytes.
/// The unit of input capacity is byte.
/// If it's not multiple of 16, it will be rounded up to.
///
/// The memory is leaked, and raw pointer is returned.
/// Caller should always call free_aligned to release the memory.
#[allow(clippy::manual_div_ceil)]
#[inline]
pub(crate) fn alloc_aligned(cap: usize) -> (*mut u8, usize) {
    let cap = usize::max(16, cap);
    let cap_u128 = (cap + 15) / 16;
    let vec: Vec<U128> = Vec::with_capacity(cap_u128);
    let cap = vec.capacity() * 16;
    let array = Vec::leak(vec);
    (array.as_mut_ptr() as *mut u8, cap)
}

#[allow(clippy::manual_div_ceil)]
#[inline]
pub(crate) fn alloc_aligned_zeroes(cap: usize) -> (*mut u8, usize) {
    let cap = usize::max(16, cap);
    let cap_u128 = (cap + 15) / 16;
    let vec: Vec<U128> = vec![U128(0); cap_u128];
    let cap = vec.capacity() * 16;
    let array = Vec::leak(vec);
    (array.as_mut_ptr() as *mut u8, cap)
}

#[allow(clippy::manual_div_ceil)]
#[inline]
pub(crate) fn alloc_aligned_ones(cap: usize) -> (*mut u8, usize) {
    let cap = usize::max(16, cap);
    let cap_u128 = (cap + 15) / 16;
    let vec: Vec<U128> = vec![U128(!0); cap_u128];
    let cap = vec.capacity() * 16;
    let array = Vec::leak(vec);
    (array.as_mut_ptr() as *mut u8, cap)
}

/// Release memory allocated by [`alloc_aligned`].
///
/// # Safety
///
/// Caller must guarantee the input pointer and size is identical to
/// the returned value of `alloc_aligned`, and only call this method
/// once. After this call, the memory must not be used.
#[inline]
pub(crate) unsafe fn free_aligned(ptr: *mut u8, cap: usize) {
    unsafe {
        assert!(cap % 16 == 0);
        let cap_u128 = cap / 16;
        let _ = Vec::<U128>::from_raw_parts(ptr as *mut U128, cap_u128, cap_u128);
    }
}

/// RawArray is a safe abstraction of an aligned byte array.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RawArray {
    ptr: NonNull<u8>,
    cap_u8: usize,
}

unsafe impl Send for RawArray {}
unsafe impl Sync for RawArray {}

impl Clone for RawArray {
    #[inline]
    fn clone(&self) -> Self {
        let mut res = RawArray::with_capacity(self.cap_u8);
        res.as_slice_mut().copy_from_slice(self.as_slice());
        res
    }
}

impl Drop for RawArray {
    #[inline]
    fn drop(&mut self) {
        // # SAFETY
        //
        // The pointer and capacity are guaranteed to be always valid.
        unsafe { free_aligned(self.ptr.as_mut(), self.cap_u8) }
    }
}

impl RawArray {
    /// Create a new raw array with given capacity.
    /// Note: capacity is always algined to multiple of 16.
    #[inline]
    pub fn with_capacity(cap_u8: usize) -> Self {
        let (ptr, cap_u8) = alloc_aligned(cap_u8);
        RawArray {
            ptr: NonNull::new(ptr).unwrap(),
            cap_u8,
        }
    }

    /// Create a new raw array with given capacity.
    /// All bits are set to zero before returned.
    #[inline]
    pub fn zeroes(cap_u8: usize) -> Self {
        let (ptr, cap_u8) = alloc_aligned_zeroes(cap_u8);
        RawArray {
            ptr: NonNull::new(ptr).unwrap(),
            cap_u8,
        }
    }

    /// Create a new raw array with given capacity.
    /// All bits are set to one before returned.
    #[inline]
    pub fn ones(cap_u8: usize) -> Self {
        let (ptr, cap_u8) = alloc_aligned_ones(cap_u8);
        RawArray {
            ptr: NonNull::new(ptr).unwrap(),
            cap_u8,
        }
    }

    /// Returns byte capacity.
    #[inline]
    pub fn cap_u8(&self) -> usize {
        self.cap_u8
    }

    /// Returns immutable byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // # SAFETY
        //
        // RawArray is not allowed to realloc, so capacity is always valid.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const u8, self.cap_u8) }
    }

    /// Returns mutable byte slice.
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        // # SAFETY
        //
        // RawArray is not allowed to realloc, so capacity is always valid.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_mut(), self.cap_u8) }
    }

    /// Cast underlying byte slice to immutable slice of given type T.
    ///
    /// # Safety
    ///
    /// 1. Input length must be within bound.
    /// 2. ZST is not allowed here.
    /// 3. Input type T must has aligned no more than 16.
    #[inline]
    pub unsafe fn cast_slice<T: ByteRepr>(&self, len: usize) -> &[T] {
        unsafe {
            debug_assert!(self.cap_u8 >= len * size_of::<T>());
            std::slice::from_raw_parts(self.ptr.as_ptr() as *const T, len)
        }
    }

    /// Cast underlying byte slice to mutable slice of given type T.
    ///
    /// # Safety
    ///
    /// 1. Input length must be within bound.
    /// 2. ZST is not allowed here.
    /// 3. Input type T must has aligned no more than 16.
    #[inline]
    pub unsafe fn cast_slice_mut<T: ByteRepr>(&mut self, len: usize) -> &mut [T] {
        unsafe {
            debug_assert!(self.cap_u8 >= len * size_of::<T>());
            std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut T, len)
        }
    }

    /// Reserve at least given number of bytes.
    #[inline]
    pub fn reserve(&mut self, cap_u8: usize) {
        if cap_u8 <= self.cap_u8 {
            return;
        }
        let mut new_cap = self.cap_u8 * 2;
        while new_cap < cap_u8 {
            new_cap *= 2;
        }
        let (new_ptr, new_cap) = alloc_aligned(new_cap);
        unsafe {
            let new_slice = std::slice::from_raw_parts_mut(new_ptr, new_cap);
            new_slice[..self.cap_u8].copy_from_slice(self.as_slice());
            free_aligned(self.ptr.as_ptr(), self.cap_u8);
        }
        self.ptr = NonNull::new(new_ptr).unwrap();
        self.cap_u8 = new_cap;
    }
}

impl Default for RawArray {
    #[inline]
    fn default() -> Self {
        RawArray::with_capacity(64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_aligned() {
        let (ptr, cap) = alloc_aligned(1);
        assert!(!ptr.is_null());
        assert_eq!(16, cap);
        unsafe { free_aligned(ptr, cap) }

        let (ptr, cap) = alloc_aligned(16);
        assert!(!ptr.is_null());
        assert_eq!(16, cap);
        unsafe { free_aligned(ptr, cap) }
    }

    #[test]
    fn test_raw_array() {
        let mut arr = RawArray::with_capacity(64);
        unsafe {
            for v in arr.as_slice_mut() {
                *v = 1;
            }
            for v in arr.as_slice() {
                assert_eq!(1u8, *v);
            }
            for (i, v) in arr.cast_slice_mut::<i64>(4).iter_mut().enumerate() {
                *v = i as i64;
            }
            assert_eq!(&[0, 1, 2, 3], arr.cast_slice::<i64>(4))
        }
    }
}
