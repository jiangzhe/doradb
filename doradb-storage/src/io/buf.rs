use crate::io::STORAGE_SECTOR_SIZE;
use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::ptr::NonNull;
use std::slice;

const DIRECT_BUF_MAX_LEN: usize = if usize::BITS < u32::BITS {
    usize::MAX
} else {
    u32::MAX as usize
};

/// IOBuf represents one aligned direct-I/O buffer shared by the supported
/// storage backends.
pub(crate) trait IOBuf: Send + 'static {
    /// Returns reference to underlying byte slice.
    fn as_bytes(&self) -> &[u8];

    /// Returns mutable reference to underlying byte slice.
    fn as_bytes_mut(&mut self) -> &mut [u8];
}

/// DirectBuf is buffer used for Linux DirectIO.
/// DirectIO requires buffer pointer, length and
/// file offset be aligned to logical sector size
/// of underlying storage device, e.g. 4096B.
pub(crate) struct DirectBuf {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

// SAFETY: `DirectBuf` owns one allocation and only exposes shared or unique
// access through Rust references. Moving that owned allocation between threads
// does not create aliases beyond those enforced by the safe API.
unsafe impl Send for DirectBuf {}

impl DirectBuf {
    /// Create a new buffer for DirectIO with all data initialized to zero.
    #[inline]
    pub(crate) fn zeroed(len: usize) -> Self {
        let cap = Self::aligned_capacity(len);
        let layout = match Layout::from_size_align(cap, STORAGE_SECTOR_SIZE) {
            Ok(layout) => layout,
            Err(_) => {
                panic!("direct buffer layout is invalid: cap={cap}, align={STORAGE_SECTOR_SIZE}")
            }
        };
        // SAFETY: `layout` was constructed from a checked non-zero capacity
        // and the exact layout is stored with the pointer for `Drop`.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
        DirectBuf { ptr, len, layout }
    }

    #[inline]
    fn aligned_capacity(len: usize) -> usize {
        assert!(
            len <= DIRECT_BUF_MAX_LEN,
            "direct buffer length {len} exceeds maximum {DIRECT_BUF_MAX_LEN}"
        );
        let len = len.max(STORAGE_SECTOR_SIZE);
        let rounded = len
            .checked_add(STORAGE_SECTOR_SIZE - 1)
            .unwrap_or_else(|| panic!("direct buffer capacity overflow: len={len}"));
        let sector_count = rounded / STORAGE_SECTOR_SIZE;
        let cap = sector_count
            .checked_mul(STORAGE_SECTOR_SIZE)
            .unwrap_or_else(|| panic!("direct buffer capacity overflow: len={len}"));
        assert!(
            cap != 0 && cap <= isize::MAX as usize,
            "direct buffer capacity is not a valid allocation size: len={len}, cap={cap}"
        );
        cap
    }

    #[inline]
    fn full_slice(&self) -> &[u8] {
        // SAFETY: `ptr` was allocated for `layout.size()` initialized bytes and
        // remains valid for the lifetime of `self`.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.capacity()) }
    }

    #[inline]
    fn full_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` was allocated for `layout.size()` initialized bytes and
        // `&mut self` guarantees unique access to the allocation.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity()) }
    }

    /// Returns capacity of this buffer.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.layout.size()
    }

    /// Returns length of data.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reserved direct buffer logical length")
    )]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns remaining capacity of this buffer.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved remaining_capacity"))]
    pub(crate) fn remaining_capacity(&self) -> usize {
        self.capacity() - self.len
    }

    /// Returns reference of data slice.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved data"))]
    pub(crate) fn data(&self) -> &[u8] {
        &self.full_slice()[..self.len]
    }

    /// Returns mutable reference of data slice.
    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        let len = self.len;
        &mut self.full_slice_mut()[..len]
    }

    /// Clear all data and set length to zero.
    #[inline]
    pub(crate) fn reset(&mut self) {
        self.full_slice_mut().fill(0);
        self.len = 0;
    }

    /// Truncate length to given number.
    ///
    /// If the provided length exceeds capacity, it will be clamped to capacity.
    #[inline]
    pub(crate) fn truncate(&mut self, len: usize) {
        if len <= self.capacity() {
            self.len = len;
        } else {
            self.len = self.capacity();
        }
    }
}

impl From<&[u8]> for DirectBuf {
    #[inline]
    fn from(value: &[u8]) -> Self {
        let mut buf = DirectBuf::zeroed(value.len());
        buf.data_mut().copy_from_slice(value);
        buf
    }
}

impl Drop for DirectBuf {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated by `alloc_zeroed` with exactly
        // `self.layout` and is deallocated exactly once here.
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

impl IOBuf for DirectBuf {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.full_slice()
    }

    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        self.full_slice_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::STORAGE_SECTOR_SIZE;

    #[track_caller]
    fn assert_buffer_layout(buf: &DirectBuf, len: usize, capacity: usize) {
        assert_eq!(buf.len(), len);
        assert_eq!(buf.data().len(), len);
        assert_eq!(buf.capacity(), capacity);
        assert_eq!(buf.as_bytes().len(), capacity);
        assert_eq!(buf.remaining_capacity(), capacity - len);
        assert_eq!(buf.as_bytes().as_ptr() as usize % STORAGE_SECTOR_SIZE, 0);
        assert_eq!(capacity % STORAGE_SECTOR_SIZE, 0);
    }

    /// Purpose: Round direct-buffer capacity at empty and sector-boundary lengths.
    /// Expected: Capacity is the smallest covering sector multiple, with a nonempty allocation.
    #[test]
    fn test_direct_buf_aligned_capacity_boundaries() {
        for (len, capacity) in [
            (0, STORAGE_SECTOR_SIZE),
            (1, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE - 1, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE + 1, STORAGE_SECTOR_SIZE * 2),
        ] {
            assert_eq!(DirectBuf::aligned_capacity(len), capacity, "len={len}");
        }
    }

    /// Purpose: Accept the maximum supported logical length on a wide pointer target.
    /// Expected: Sector rounding preserves the required capacity without truncation.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_direct_buf_aligned_capacity_accepts_u32_max_len() {
        assert_eq!(
            DirectBuf::aligned_capacity(u32::MAX as usize),
            (u32::MAX as usize) + 1
        );
    }

    /// Purpose: Reject direct-buffer lengths beyond the supported representation.
    /// Expected: Construction panics with a length-limit diagnostic before allocation.
    #[cfg(target_pointer_width = "64")]
    #[test]
    #[should_panic(expected = "exceeds maximum")]
    fn test_direct_buf_rejects_len_above_u32_max() {
        let _buf = DirectBuf::zeroed((u32::MAX as usize) + 1);
    }

    /// Purpose: Reject capacities beyond the allocation-layout limit on a narrow pointer target.
    /// Expected: Capacity calculation panics with an allocation-size diagnostic.
    #[cfg(target_pointer_width = "32")]
    #[test]
    #[should_panic(expected = "not a valid allocation size")]
    fn test_direct_buf_aligned_capacity_rejects_32_bit_layout_overflow() {
        let _cap = DirectBuf::aligned_capacity((isize::MAX as usize) + 1);
    }

    /// Purpose: Detect sector rounding overflow on a narrow pointer target.
    /// Expected: Capacity calculation panics instead of wrapping to a smaller allocation.
    #[cfg(target_pointer_width = "32")]
    #[test]
    #[should_panic(expected = "capacity overflow")]
    fn test_direct_buf_aligned_capacity_rejects_32_bit_rounding_overflow() {
        let _cap = DirectBuf::aligned_capacity(usize::MAX);
    }

    /// Purpose: Align direct-buffer allocations for empty and nonempty logical data.
    /// Expected: Allocation pointers and capacity satisfy sector alignment while preserving length.
    #[test]
    fn test_direct_buf_allocation_alignment() {
        for len in [1024, 0] {
            let buf = DirectBuf::zeroed(len);
            assert_buffer_layout(&buf, len, STORAGE_SECTOR_SIZE);
        }
    }

    /// Purpose: Initialize direct buffers whose logical lengths require sector padding.
    /// Expected: The allocation is zeroed and capacity accounts for both data and padding.
    #[test]
    fn test_direct_buf_zeroed() {
        for (len, capacity) in [
            (512, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE + 1, STORAGE_SECTOR_SIZE * 2),
        ] {
            let buf = DirectBuf::zeroed(len);
            assert_buffer_layout(&buf, len, capacity);
            assert!(buf.as_bytes().iter().all(|&b| b == 0), "len={len}");
        }
    }

    /// Purpose: Allocate direct buffers for exact sector multiples.
    /// Expected: Logical length fills the allocation without unused capacity.
    #[test]
    fn test_direct_buf_page() {
        for len in [STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE * 4] {
            let buf = DirectBuf::zeroed(len);
            assert_buffer_layout(&buf, len, len);
        }
    }

    /// Purpose: Preserve appended data and clear a direct buffer for reuse.
    /// Expected: Growth retains existing bytes and reset clears the allocation and logical length.
    #[test]
    fn test_direct_buf_data_operations() {
        let mut buf = DirectBuf::zeroed(0);
        let data = [1, 2, 3, 4, 5];

        buf.truncate(data.len());
        buf.data_mut().copy_from_slice(&data);
        assert_eq!(buf.data(), &data);

        let more_data = [6, 7, 8];
        let old_len = buf.len();
        buf.truncate(old_len + more_data.len());
        buf.data_mut()[old_len..].copy_from_slice(&more_data);
        assert_eq!(buf.data(), &[1, 2, 3, 4, 5, 6, 7, 8]);

        let len = buf.len();
        buf.as_bytes_mut()[len..].fill(0xff);
        buf.reset();
        assert_eq!(buf.len(), 0);
        assert!(buf.as_bytes().iter().all(|&b| b == 0));
    }

    /// Purpose: Bound direct-buffer logical length when truncation requests exceed capacity.
    /// Expected: The logical length stops at the allocated capacity.
    #[test]
    fn test_direct_buf_truncate_clamps_to_capacity() {
        let mut buf = DirectBuf::zeroed(32);
        let capacity = buf.capacity();

        buf.truncate(capacity + 1);
        assert_eq!(buf.len(), capacity);
    }

    /// Purpose: Exercise repeated allocation and release across direct-buffer size boundaries.
    /// Expected: Each allocation retains sector-aligned storage and releases without panicking.
    #[test]
    fn test_direct_buf_repeated_alloc_drop_preserves_layout() {
        for (len, capacity) in [
            (0, STORAGE_SECTOR_SIZE),
            (1, STORAGE_SECTOR_SIZE),
            (32, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE),
            (STORAGE_SECTOR_SIZE + 1, STORAGE_SECTOR_SIZE * 2),
        ] {
            for _ in 0..64 {
                let buf = DirectBuf::zeroed(len);
                assert_buffer_layout(&buf, len, capacity);
            }
        }
    }

    /// Purpose: Construct a direct buffer from an existing byte slice.
    /// Expected: The logical data and length match the source slice.
    #[test]
    fn test_direct_buf_from_slice() {
        let data = [10, 20, 30, 40];
        let buf = DirectBuf::from(&data[..]);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.data(), &data);
    }
}
