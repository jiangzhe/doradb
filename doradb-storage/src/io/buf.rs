use crate::io::{STORAGE_SECTOR_SIZE, align_to_sector_size};
use std::alloc::{Layout, alloc, alloc_zeroed};

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
    data: Box<[u8]>,
    len: usize,
}

impl DirectBuf {
    /// Create a new buffer for DirectIO with uninitialized data.
    #[inline]
    pub(crate) fn uninit(len: usize) -> Self {
        debug_assert!(len <= u32::MAX as usize);
        let cap = align_to_sector_size(len);
        // SAFETY: `cap` is sector-aligned by `align_to_sector_size`, which
        // satisfies `DirectBuf::new`'s allocation/layout contract.
        unsafe { Self::new(len, cap, false) }
    }

    /// Create a new buffer for DirectIO with all data initialized to zero.
    #[inline]
    pub(crate) fn zeroed(len: usize) -> Self {
        debug_assert!(len <= u32::MAX as usize);
        let cap = align_to_sector_size(len);
        // SAFETY: `cap` is sector-aligned by `align_to_sector_size`, which
        // satisfies `DirectBuf::new`'s allocation/layout contract.
        unsafe { Self::new(len, cap, true) }
    }

    #[inline]
    unsafe fn new(len: usize, cap: usize, zero: bool) -> Self {
        // SAFETY: callers provide a sector-aligned `cap`; the allocation uses
        // the same layout later transferred into `Vec::from_raw_parts`, so the
        // boxed slice owns exactly the bytes allocated here.
        unsafe {
            let layout = Layout::from_size_align_unchecked(cap, STORAGE_SECTOR_SIZE);
            let ptr = if zero {
                alloc_zeroed(layout)
            } else {
                alloc(layout)
            };
            let vec = Vec::from_raw_parts(ptr, cap, cap);
            DirectBuf {
                data: vec.into_boxed_slice(),
                len,
            }
        }
    }

    /// Returns capacity of this buffer.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Returns length of data.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns remaining capacity of this buffer.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn remaining_capacity(&self) -> usize {
        self.data.len() - self.len
    }

    /// Returns reference of data slice.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn data(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Returns mutable reference of data slice.
    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[..self.len]
    }

    /// Clear all data and set length to zero.
    #[inline]
    pub(crate) fn reset(&mut self) {
        self.data.fill(0);
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
        let mut buf = DirectBuf::uninit(value.len());
        buf.data_mut().copy_from_slice(value);
        buf
    }
}

impl IOBuf for DirectBuf {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::STORAGE_SECTOR_SIZE;

    #[test]
    fn test_direct_buf_uninit() {
        let buf = DirectBuf::uninit(1024);
        assert_eq!(buf.len(), 1024);
        assert!(buf.capacity() >= 1024);
        assert!(buf.capacity().is_multiple_of(STORAGE_SECTOR_SIZE));

        let buf = DirectBuf::uninit(0);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_direct_buf_zeroed() {
        let buf = DirectBuf::zeroed(512);
        assert_eq!(buf.len(), 512);
        assert!(buf.capacity() >= 512);
        assert_eq!(buf.capacity() % STORAGE_SECTOR_SIZE, 0);
        assert_eq!(buf.remaining_capacity(), buf.capacity() - buf.len());
        assert!(buf.data().iter().all(|&b| b == 0));

        let buf = DirectBuf::zeroed(STORAGE_SECTOR_SIZE + 1);
        assert_eq!(buf.len(), STORAGE_SECTOR_SIZE + 1);
        assert!(buf.capacity() > STORAGE_SECTOR_SIZE);
        assert_eq!(buf.capacity() % STORAGE_SECTOR_SIZE, 0);
        assert_eq!(buf.remaining_capacity(), buf.capacity() - buf.len());
    }

    #[test]
    fn test_direct_buf_page() {
        let buf = DirectBuf::zeroed(STORAGE_SECTOR_SIZE);
        assert_eq!(buf.len(), STORAGE_SECTOR_SIZE);
        assert_eq!(buf.capacity(), STORAGE_SECTOR_SIZE);
        assert_eq!(buf.remaining_capacity(), 0);

        let buf = DirectBuf::uninit(STORAGE_SECTOR_SIZE * 4);
        assert_eq!(buf.len(), STORAGE_SECTOR_SIZE * 4);
    }

    #[test]
    fn test_direct_buf_data_operations() {
        let mut buf = DirectBuf::uninit(0);
        let data = [1, 2, 3, 4, 5];

        buf.truncate(data.len());
        buf.data_mut().copy_from_slice(&data);
        assert_eq!(buf.data(), &data);

        let more_data = [6, 7, 8];
        let old_len = buf.len();
        buf.truncate(old_len + more_data.len());
        buf.data_mut()[old_len..].copy_from_slice(&more_data);
        assert_eq!(&buf.data()[data.len()..], &more_data);

        buf.reset();
        assert_eq!(buf.len(), 0);
        assert!(buf.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_direct_buf_truncate_clamps_to_capacity() {
        let mut buf = DirectBuf::uninit(32);
        let capacity = buf.capacity();

        buf.truncate(capacity + 1);
        assert_eq!(buf.len(), capacity);
    }

    #[test]
    fn test_direct_buf_from_slice() {
        let data = [10, 20, 30, 40];
        let buf = DirectBuf::from(&data[..]);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.data(), &data);
    }
}
