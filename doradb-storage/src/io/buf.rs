use crate::io::free_list::FreeElem;
use crate::io::{align_to_sector_size, MIN_PAGE_SIZE, STORAGE_SECTOR_SIZE};
use std::alloc::{alloc, Layout};
use std::ops::{Deref, DerefMut};

/// Buf represents IO buffer used for linux direct IO(libaio).
/// It has two kinds:
/// a) Reuseable buffer, which can be reused and has certain capacity.
/// b) Direct buffer, which is allocated on-the-fly and has flexible capacity.
pub enum Buf {
    Reuse(Box<FreeElem<PageBuf>>),
    Direct(DirectBuf),
}

impl Buf {
    /// Returns remaining capacity of this buffer.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        match self {
            Buf::Reuse(buf) => buf.page_len() - buf.data_len(),
            Buf::Direct(buf) => buf.capacity() - buf.len(),
        }
    }

    /// Copy data from given slice.
    #[inline]
    pub fn clone_from_slice(&mut self, data: &[u8]) {
        match self {
            Buf::Reuse(buf) => {
                let offset = buf.data_len();
                buf.set_len(offset + data.len());
                buf.clone_from_slice(offset, data);
            }
            Buf::Direct(buf) => {
                let offset = buf.len();
                let new_len = offset + data.len();
                buf.set_len(new_len);
                buf[offset..new_len].clone_from_slice(data);
            }
        }
    }

    /// Returns pointer to the start of the buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Buf::Reuse(buf) => buf.as_mut_ptr(),
            Buf::Direct(buf) => buf.as_mut_ptr(),
        }
    }

    /// Returns aligned length of the buffer.
    #[inline]
    pub fn aligned_len(&self) -> usize {
        match self {
            Buf::Reuse(buf) => buf.data_len_aligned(),
            Buf::Direct(buf) => buf.capacity(),
        }
    }
}

/// DirectBuf is buffer used for Linux DirectIO.
/// DirectIO requires buffer pointer, length and
/// file offset be aligned to logical sector size
/// of underlying storage device, e.g. 512B.
pub struct DirectBuf {
    data: Box<[u8]>,
    len: usize,
}

impl DirectBuf {
    /// Create a new buffer for DirectIO with uninitialized data.
    #[inline]
    pub fn uninit(len: usize) -> Self {
        let size = align_to_sector_size(len);
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, STORAGE_SECTOR_SIZE);
            let ptr = alloc(layout);
            let vec = Vec::from_raw_parts(ptr, size, size);
            DirectBuf {
                data: vec.into_boxed_slice(),
                len,
            }
        }
    }

    /// Create a new buffer for DirectIO with all data initialized to zero.
    #[inline]
    pub fn zeroed(len: usize) -> Self {
        let mut buf = Self::uninit(len);
        unsafe {
            std::ptr::write_bytes(buf.as_mut_ptr(), 0, len);
        }
        buf
    }

    /// Create a new buffer with given data.
    #[inline]
    pub fn with_data(data: &[u8]) -> Self {
        let size = align_to_sector_size(data.len());
        let mut buf = Self::uninit(size);
        buf[..data.len()].copy_from_slice(data);
        buf
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity());
        self.len = len;
    }
}

impl From<&[u8]> for DirectBuf {
    #[inline]
    fn from(value: &[u8]) -> Self {
        let size = align_to_sector_size(value.len());
        let mut buf = DirectBuf::uninit(size);
        buf[..value.len()].copy_from_slice(value);
        buf
    }
}

impl Deref for DirectBuf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data[..self.len]
    }
}

impl DerefMut for DirectBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[..self.len]
    }
}

pub type CacheBuf = Box<FreeElem<PageBuf>>;

/// PageBuf represents a buffer with page size.
/// The start of buffer is aligned to storage sector size(512B).
pub struct PageBuf {
    // pager buffer, the size should be equal to page
    page: Box<[u8]>,
    // actual data length, can be arbitrary number less
    // or equal to page size.
    data_len: usize,
    // aligned length, should align to storage sector
    // size.
    aligned_len: usize,
}

impl PageBuf {
    /// Create a new page buffer with given size.
    #[inline]
    pub fn uninit(page_size: usize) -> Self {
        assert!(page_size >= MIN_PAGE_SIZE && page_size % MIN_PAGE_SIZE == 0);
        unsafe {
            let layout = Layout::from_size_align_unchecked(page_size, STORAGE_SECTOR_SIZE);
            let ptr = alloc(layout);
            let vec = Vec::from_raw_parts(ptr, page_size, page_size);
            PageBuf {
                page: vec.into_boxed_slice(),
                data_len: 0,
                aligned_len: 0,
            }
        }
    }

    /// Create a zeroed page buffer with given size.
    #[inline]
    pub fn zeroed(page_size: usize) -> Self {
        let mut buf = Self::uninit(page_size);
        unsafe {
            std::ptr::write_bytes(buf.page.as_mut_ptr(), 0, page_size);
        }
        buf
    }

    #[inline]
    pub fn page(&self) -> &[u8] {
        &self.page
    }

    #[inline]
    pub fn page_len(&self) -> usize {
        self.page.len()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.page[..self.data_len]
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    #[inline]
    pub fn data_aligned(&self) -> &[u8] {
        &self.page[..self.aligned_len]
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.page.as_mut_ptr()
    }

    #[inline]
    pub fn data_len_aligned(&self) -> usize {
        self.aligned_len
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.page_len());
        self.data_len = len;
        self.aligned_len = align_to_sector_size(len);
    }

    #[inline]
    pub fn clone_from_slice(&mut self, offset: usize, data: &[u8]) {
        debug_assert!(offset + data.len() <= self.data_len);
        self.page[offset..offset + data.len()].clone_from_slice(data);
    }
}
