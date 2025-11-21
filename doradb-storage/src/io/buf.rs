use crate::io::free_list::FreeElem;
use crate::io::{align_to_sector_size, MIN_PAGE_SIZE, STORAGE_SECTOR_SIZE};
use crate::serde::{Ser, SerdeCtx};
use std::alloc::{alloc, alloc_zeroed, Layout};
use std::mem;
use std::ops::{Deref, DerefMut};

/// Buf represents IO buffer used for linux direct IO(libaio).
/// It has two kinds:
/// a) Reuseable buffer, which can be reused and has certain capacity.
/// b) Direct buffer, which is allocated on-the-fly and has flexible capacity.
pub enum Buf {
    Reuse(CacheBuf),
    Direct(DirectBuf),
}

impl Buf {
    #[inline]
    pub fn direct(self) -> Option<DirectBuf> {
        match self {
            Buf::Direct(d) => Some(d),
            Buf::Reuse(_) => None,
        }
    }

    #[inline]
    pub fn reuse(self) -> Option<CacheBuf> {
        match self {
            Buf::Reuse(c) => Some(c),
            Buf::Direct(_) => None,
        }
    }
}

impl Deref for Buf {
    type Target = DirectBuf;
    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Buf::Reuse(buf) => buf,
            Buf::Direct(buf) => buf,
        }
    }
}

impl DerefMut for Buf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Buf::Reuse(buf) => buf,
            Buf::Direct(buf) => buf,
        }
    }
}

/// DirectBuf is buffer used for Linux DirectIO.
/// DirectIO requires buffer pointer, length and
/// file offset be aligned to logical sector size
/// of underlying storage device, e.g. 512B.
/// buffer layout:
/// | 4-byte length field | n-byte payload |
pub struct DirectBuf {
    data: Box<[u8]>,
}

impl DirectBuf {
    pub const LEN_BYTES: usize = mem::size_of::<u32>();

    /// Create a new buffer for DirectIO with uninitialized data.
    #[inline]
    pub fn uninit(data_len: usize) -> Self {
        debug_assert!(data_len <= (u32::MAX - 4u32) as usize);
        let size = align_to_sector_size(data_len + mem::size_of::<u32>());
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, STORAGE_SECTOR_SIZE);
            let ptr = alloc(layout);
            let vec = Vec::from_raw_parts(ptr, size, size);
            let mut buf = DirectBuf {
                data: vec.into_boxed_slice(),
            };
            buf.set_data_len(data_len);
            buf
        }
    }

    /// Create a new buffer for DirectIO with all data initialized to zero.
    #[inline]
    pub fn zeroed(data_len: usize) -> Self {
        debug_assert!(data_len <= (u32::MAX - 4u32) as usize);
        let size = align_to_sector_size(data_len + mem::size_of::<u32>());
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, STORAGE_SECTOR_SIZE);
            let ptr = alloc_zeroed(layout);
            let vec = Vec::from_raw_parts(ptr, size, size);
            let mut buf = DirectBuf {
                data: vec.into_boxed_slice(),
            };
            buf.set_data_len(data_len);
            buf
        }
    }

    /// Create a new page.
    /// The input page size should be multiple of sector size(e.g. n*4KB).
    /// Note: The actual data size can be written is a little smaller than
    /// the page size.
    #[inline]
    pub fn page(page_size: usize) -> Self {
        debug_assert!(page_size >= MIN_PAGE_SIZE && page_size.is_multiple_of(MIN_PAGE_SIZE));
        Self::uninit(page_size - Self::LEN_BYTES)
    }

    /// Create a new page with all zeroed bytes.
    #[inline]
    pub fn page_zeroed(page_size: usize) -> Self {
        debug_assert!(page_size >= MIN_PAGE_SIZE && page_size.is_multiple_of(MIN_PAGE_SIZE));
        Self::zeroed(page_size - Self::LEN_BYTES)
    }

    #[inline]
    fn len_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[..mem::size_of::<u32>()]
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [u8] {
        let len = self.data_len();
        &mut self.data[mem::size_of::<u32>()..mem::size_of::<u32>() + len]
    }

    /// Create a new buffer with given data.
    #[inline]
    pub fn with_data(data: &[u8]) -> Self {
        let mut buf = Self::uninit(data.len());
        buf.set_data_len(0);
        buf.extend_from_slice(data);
        buf
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.data.len() - self.raw_len()
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        Self::data_len_from(&self.data)
    }

    #[inline]
    pub fn set_data_len(&mut self, len: usize) {
        debug_assert!(len + mem::size_of::<u32>() <= self.data.len());
        let len_bytes = (len as u32).to_le_bytes();
        self.len_bytes_mut().copy_from_slice(&len_bytes);
    }

    #[inline]
    pub fn raw_len(&self) -> usize {
        self.data_len() + Self::LEN_BYTES
    }

    #[inline]
    pub fn raw_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data[Self::LEN_BYTES..self.raw_len()]
    }

    #[inline]
    pub fn raw_data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn raw_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    #[inline]
    pub fn set_raw_len(&mut self, len: usize) {
        debug_assert!(len >= mem::size_of::<u32>() && len <= u32::MAX as usize);
        self.set_data_len(len - mem::size_of::<u32>());
    }

    /// Extend slice to end of data.
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        debug_assert!(self.raw_len() + data.len() <= self.capacity());
        let offset = self.raw_len();
        let new_len = offset + data.len();
        self.data[offset..new_len].copy_from_slice(data);
        self.set_raw_len(new_len);
    }

    /// Serialize data to end of data.
    #[inline]
    pub fn extend_ser<'a, T: Ser<'a>>(&mut self, data: &T, ctx: &SerdeCtx) {
        let ser_len = data.ser_len(ctx);
        let offset = self.raw_len();
        debug_assert!(offset + ser_len <= self.capacity());
        let new_len = offset + ser_len;
        self.set_raw_len(new_len);
        let res_len = data.ser(ctx, &mut self.data[..new_len], offset);
        debug_assert!(res_len == new_len);
    }

    /// Clear all data and set length to zero.
    #[inline]
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.set_data_len(0);
    }

    /// Extract data length from a direct buffer.
    #[inline]
    pub fn data_len_from(buf: &[u8]) -> usize {
        debug_assert!(buf.len() >= mem::size_of::<u32>());
        let len_bytes: [u8; 4] = buf[..mem::size_of::<u32>()].try_into().unwrap();
        u32::from_le_bytes(len_bytes) as usize
    }

    /// Extract raw length from a direct buffer.
    /// It includes length number at the beginning
    /// (4 bytes).
    #[inline]
    pub fn raw_len_from(buf: &[u8]) -> usize {
        Self::data_len_from(buf) + mem::size_of::<u32>()
    }

    /// Returns the actual data in buffer.
    #[inline]
    pub fn data_from(buf: &[u8]) -> &[u8] {
        let data_len = Self::data_len_from(buf);
        &buf[mem::size_of::<u32>()..mem::size_of::<u32>() + data_len]
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

pub type CacheBuf = Box<FreeElem<DirectBuf>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{MIN_PAGE_SIZE, STORAGE_SECTOR_SIZE};

    #[test]
    fn test_buf() {
        let d = DirectBuf::page_zeroed(4096);
        let buf = Buf::Direct(d);
        assert!(buf.direct().is_some());
        let d = DirectBuf::page_zeroed(4096);
        let buf = Buf::Direct(d);
        assert!(buf.reuse().is_none());
        let c = Box::new(FreeElem::new(DirectBuf::page_zeroed(4096)));
        let buf = Buf::Reuse(c);
        assert!(buf.direct().is_none());
        let c = Box::new(FreeElem::new(DirectBuf::page_zeroed(4096)));
        let buf = Buf::Reuse(c);
        assert!(buf.reuse().is_some());
    }

    #[test]
    fn test_direct_buf_uninit() {
        let buf = DirectBuf::uninit(1024);
        assert_eq!(buf.data_len(), 1024);
        assert!(buf.capacity() >= 1024 + DirectBuf::LEN_BYTES);
        assert!(buf.capacity() % STORAGE_SECTOR_SIZE == 0);

        let buf = DirectBuf::uninit(0);
        assert_eq!(buf.data_len(), 0);
        assert!(buf.capacity() >= DirectBuf::LEN_BYTES);
    }

    #[test]
    fn test_direct_buf_zeroed() {
        let buf = DirectBuf::zeroed(512);
        assert_eq!(buf.data_len(), 512);
        assert!(buf.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_direct_buf_page() {
        let buf = DirectBuf::page(MIN_PAGE_SIZE);
        assert_eq!(buf.raw_len(), MIN_PAGE_SIZE);
        assert_eq!(buf.capacity(), MIN_PAGE_SIZE);
        assert_eq!(buf.remaining_capacity(), 0);

        let buf = DirectBuf::page(MIN_PAGE_SIZE * 4);
        assert_eq!(buf.raw_len(), MIN_PAGE_SIZE * 4);
    }

    #[test]
    fn test_direct_buf_page_zeroed() {
        let buf = DirectBuf::page_zeroed(MIN_PAGE_SIZE);
        assert_eq!(buf.raw_len(), MIN_PAGE_SIZE);
        assert_eq!(buf.data_len(), MIN_PAGE_SIZE - DirectBuf::LEN_BYTES);
        assert!(buf.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_direct_buf_with_data() {
        let data = [1, 2, 3, 4, 5];
        let buf = DirectBuf::with_data(&data);
        assert_eq!(buf.data_len(), data.len());
        assert_eq!(buf.data(), &data);
    }

    #[test]
    fn test_direct_buf_len_operations() {
        let mut buf = DirectBuf::uninit(100);
        assert_eq!(buf.data_len(), 100);
        assert_eq!(buf.raw_len(), 100 + DirectBuf::LEN_BYTES);

        buf.set_data_len(50);
        assert_eq!(buf.data_len(), 50);
        assert_eq!(buf.raw_len(), 50 + DirectBuf::LEN_BYTES);

        buf.set_raw_len(75);
        assert_eq!(buf.raw_len(), 75);
        assert_eq!(buf.data_len(), 75 - DirectBuf::LEN_BYTES);
    }

    #[test]
    fn test_direct_buf_data_operations() {
        let mut buf = DirectBuf::uninit(0);
        let data = [1, 2, 3, 4, 5];

        buf.extend_from_slice(&data);
        assert_eq!(buf.data(), &data);

        let more_data = [6, 7, 8];
        buf.extend_from_slice(&more_data);
        assert_eq!(buf.data_len(), data.len() + more_data.len());
        assert_eq!(&buf.data()[data.len()..], &more_data);

        buf.reset();
        assert_eq!(buf.data_len(), 0);
        assert!(buf.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_direct_buf_from_slice() {
        let data = [10, 20, 30, 40];
        let buf = DirectBuf::from(&data[..]);
        assert_eq!(buf.data_len(), data.len());
        assert_eq!(buf.data(), &data);
    }

    #[test]
    fn test_direct_buf_pointer_operations() {
        let buf = DirectBuf::uninit(64);
        assert!(!buf.raw_ptr().is_null());

        let mut buf = DirectBuf::uninit(64);
        assert!(!buf.raw_ptr_mut().is_null());
    }

    #[test]
    #[should_panic]
    fn test_direct_buf_invalid_length() {
        let _ = DirectBuf::uninit(u32::MAX as usize + 1);
    }

    #[test]
    #[should_panic]
    fn test_direct_buf_invalid_page_size() {
        let _ = DirectBuf::page(MIN_PAGE_SIZE - 1);
    }
}
