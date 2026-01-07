use crate::io::{STORAGE_SECTOR_SIZE, align_to_sector_size};
use crate::serde::{Ser, SerdeCtx};
use std::alloc::{Layout, alloc, alloc_zeroed};

/// AIOBuf represents AIO buffer used for Linux direct IO(currently via libaio).
pub trait AIOBuf: Send + 'static {
    /// Returns reference to underlying byte slice.
    fn as_bytes(&self) -> &[u8];

    /// Returns mutable reference to underlying byte slice.
    fn as_bytes_mut(&mut self) -> &mut [u8];
}

/// DirectBuf is buffer used for Linux DirectIO.
/// DirectIO requires buffer pointer, length and
/// file offset be aligned to logical sector size
/// of underlying storage device, e.g. 4096B.
pub struct DirectBuf {
    data: Box<[u8]>,
    len: usize,
}

impl DirectBuf {
    /// Create a new buffer for DirectIO with uninitialized data.
    #[inline]
    pub fn uninit(len: usize) -> Self {
        debug_assert!(len <= u32::MAX as usize);
        let cap = align_to_sector_size(len);
        unsafe { Self::new(len, cap, false) }
    }

    /// Create a new buffer for DirectIO with all data initialized to zero.
    #[inline]
    pub fn zeroed(len: usize) -> Self {
        debug_assert!(len <= u32::MAX as usize);
        let cap = align_to_sector_size(len);
        unsafe { Self::new(len, cap, true) }
    }

    #[inline]
    unsafe fn new(len: usize, cap: usize, zero: bool) -> Self {
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

    /// Create a new buffer with given data.
    #[inline]
    pub fn with_data(data: &[u8]) -> Self {
        let mut buf = Self::zeroed(data.len());
        buf.data_mut().copy_from_slice(data);
        buf
    }

    /// Returns capacity of this buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Returns length of data.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns remaining capacity of this buffer.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.data.len() - self.len
    }

    /// Returns reference of data slice.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Returns mutable reference of data slice.
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[..self.len]
    }

    /// Extend slice to end of data.
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        debug_assert!(self.len + data.len() <= self.capacity());
        let offset = self.len;
        let new_len = offset + data.len();
        self.data[offset..new_len].copy_from_slice(data);
        self.len = new_len;
    }

    /// Serialize data to end of data.
    #[inline]
    pub fn extend_ser<'a, T: Ser<'a>>(&mut self, data: &T, ctx: &SerdeCtx) {
        let ser_len = data.ser_len(ctx);
        let offset = self.len;
        debug_assert!(offset + ser_len <= self.capacity());
        let new_len = offset + ser_len;
        let res_len = data.ser(ctx, &mut self.data[..new_len], offset);
        self.len = new_len;
        debug_assert!(res_len == new_len);
    }

    /// Clear all data and set length to zero.
    #[inline]
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.len = 0;
    }

    /// Truncate length to given number.
    ///
    /// If the provided length exceeds capacity, it will be clamped to capacity.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
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

impl AIOBuf for DirectBuf {
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
    use crate::io::{MIN_PAGE_SIZE, STORAGE_SECTOR_SIZE};

    #[test]
    fn test_direct_buf_uninit() {
        let buf = DirectBuf::uninit(1024);
        assert_eq!(buf.len(), 1024);
        assert!(buf.capacity() >= 1024);
        assert!(buf.capacity() % STORAGE_SECTOR_SIZE == 0);

        let buf = DirectBuf::uninit(0);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_direct_buf_zeroed() {
        let buf = DirectBuf::zeroed(512);
        assert_eq!(buf.len(), 512);
        assert!(buf.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_direct_buf_page() {
        let buf = DirectBuf::zeroed(MIN_PAGE_SIZE);
        assert_eq!(buf.len(), MIN_PAGE_SIZE);
        assert_eq!(buf.capacity(), MIN_PAGE_SIZE);
        assert_eq!(buf.remaining_capacity(), 0);

        let buf = DirectBuf::uninit(MIN_PAGE_SIZE * 4);
        assert_eq!(buf.len(), MIN_PAGE_SIZE * 4);
    }

    #[test]
    fn test_direct_buf_with_data() {
        let data = [1, 2, 3, 4, 5];
        let buf = DirectBuf::with_data(&data);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.data(), &data);
    }

    #[test]
    fn test_direct_buf_data_operations() {
        let mut buf = DirectBuf::uninit(0);
        let data = [1, 2, 3, 4, 5];

        buf.extend_from_slice(&data);
        assert_eq!(buf.data(), &data);

        let more_data = [6, 7, 8];
        buf.extend_from_slice(&more_data);
        assert_eq!(buf.len(), data.len() + more_data.len());
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
