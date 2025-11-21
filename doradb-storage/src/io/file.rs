use crate::io::buf::Buf;
use crate::io::libaio_abi::io_iocb_cmd;
use crate::io::{align_to_sector_size, AIOError, AIOKey, AIOResult, UnsafeAIO, AIO};
use libc::{
    close, fdatasync, fstat, fsync, ftruncate, open, stat, O_CREAT, O_DIRECT, O_RDWR, O_TRUNC,
};
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use parking_lot::RawMutex;
use scopeguard::defer;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};

/// SparseFile is file with metadata describing empty blocks
/// instead of writing them.
/// The logical size of sparse file can be very large, but the
/// real allocated blocks can be only a few.
pub struct SparseFile {
    fd: RawFd,
    offset: AtomicUsize,
    max_len: AtomicUsize,
    // protect file size change only.
    size_lock: RawMutex,
}

impl AsRawFd for SparseFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl SparseFile {
    /// Create a sparse file with given maximum length.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub fn create(file_path: impl AsRef<str>, max_len: usize) -> AIOResult<SparseFile> {
        unsafe {
            let c_string = CString::new(file_path.as_ref()).map_err(|_| AIOError::OpenFileError)?;
            let fd = open(
                c_string.as_ptr(),
                O_CREAT | O_RDWR | O_TRUNC | O_DIRECT,
                0o644,
            );
            if fd < 0 {
                return Err(AIOError::OpenFileError);
            }
            let ret = ftruncate(fd, max_len as i64);
            if ret < 0 {
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(AIOError::OpenFileError);
            }
            Ok(SparseFile::new(fd, 0, max_len))
        }
    }

    /// Open an existing sparse file with given maximum length.
    #[inline]
    pub fn open(file_path: impl AsRef<str>, max_len: usize) -> AIOResult<SparseFile> {
        unsafe {
            let c_string = CString::new(file_path.as_ref()).map_err(|_| AIOError::OpenFileError)?;
            let fd = open(c_string.as_ptr(), O_RDWR | O_DIRECT, 0o644);
            if fd < 0 {
                return Err(AIOError::OpenFileError);
            }
            let ret = ftruncate(fd, max_len as i64);
            if ret < 0 {
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(AIOError::OpenFileError);
            }
            Ok(SparseFile::new(fd, 0, max_len))
        }
    }

    /// Create a new sparse file.
    #[inline]
    fn new(fd: RawFd, offset: usize, max_len: usize) -> Self {
        SparseFile {
            fd,
            offset: AtomicUsize::new(offset),
            max_len: AtomicUsize::new(max_len),
            size_lock: RawMutex::INIT,
        }
    }

    /// Allocate enough space for data of given length to persist
    /// at end of the file.
    #[inline]
    pub fn alloc(&self, len: usize) -> Result<(usize, usize), AIOError> {
        let size = align_to_sector_size(len);
        loop {
            let offset = self.offset.load(Ordering::Relaxed);
            let new_offset = offset + size;
            if new_offset > self.max_len.load(Ordering::Relaxed) {
                return Err(AIOError::OutOfRange);
            }
            if self
                .offset
                .compare_exchange_weak(offset, new_offset, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Ok((offset, new_offset));
            }
        }
    }

    /// Returns a pread IO request.
    /// User should make sure key is unique.
    #[inline]
    pub fn pread_direct(&self, key: AIOKey, offset: usize, buf: Buf) -> AIO {
        pread_direct(key, self.fd, offset, buf)
    }

    /// Returns a pread IO request.
    ///
    /// # Safety
    ///
    /// Caller must guarantee the pointer is valid during
    /// syscall, and pointer is correctly aligned.
    #[inline]
    pub unsafe fn pread_unchecked(
        &self,
        key: AIOKey,
        offset: usize,
        ptr: *mut u8,
        len: usize,
    ) -> UnsafeAIO {
        unsafe { pread_unchecked(key, self.fd, offset, ptr, len) }
    }

    /// Returns a pwrite IO request.
    /// User should make sure key is unique.
    #[inline]
    pub fn pwrite_direct(&self, key: AIOKey, offset: usize, buf: Buf) -> AIO {
        pwrite_direct(key, self.fd, offset, buf)
    }

    /// Returns a pwrite IO request.
    ///
    /// # Safety
    ///
    /// Caller must guarantee the pointer is valid during
    /// syscall, and pointer is correctly aligned.
    #[inline]
    pub unsafe fn pwrite_unchecked(
        &self,
        key: AIOKey,
        offset: usize,
        ptr: *mut u8,
        len: usize,
    ) -> UnsafeAIO {
        unsafe { pwrite_unchecked(key, self.fd, offset, ptr, len) }
    }

    /// Returns the file syncer.
    #[inline]
    pub fn syncer(&self) -> FileSyncer {
        FileSyncer(self.fd)
    }

    /// Grow the file to given size.
    #[inline]
    pub fn extend_to(&self, max_len: usize) -> std::io::Result<()> {
        self.size_lock.lock();
        defer! {
            unsafe { self.size_lock.unlock(); }
        }
        let curr_len = self.max_len.load(Ordering::Acquire);
        if max_len <= curr_len {
            return Ok(());
        }
        let retcode = unsafe { ftruncate(self.fd, max_len as i64) };
        if retcode == 0 {
            return Ok(());
        }
        debug_assert!(retcode == -1);
        Err(std::io::Error::last_os_error())
    }

    /// Get the logical size and allocated size of this file.
    #[inline]
    pub fn size(&self) -> std::io::Result<(usize, usize)> {
        unsafe {
            let mut s = MaybeUninit::<stat>::zeroed();
            let retcode = fstat(self.fd, s.as_mut_ptr());
            if retcode == 0 {
                let res = s.assume_init_ref();
                let logical_size = res.st_size as usize;
                let allocated_size = (res.st_blocks * 512) as usize;
                return Ok((logical_size, allocated_size));
            }
            debug_assert!(retcode == -1);
            Err(std::io::Error::last_os_error())
        }
    }
}

impl Drop for SparseFile {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            close(self.fd);
        }
    }
}

/// FileSyncer is a simple wrapper to provide functionality
/// of fsync() and fdatasync().
pub struct FileSyncer(RawFd);

impl FileSyncer {
    #[inline]
    pub fn fsync(&self) {
        unsafe {
            fsync(self.0);
        }
    }

    #[inline]
    pub fn fdatasync(&self) {
        unsafe {
            fdatasync(self.0);
        }
    }
}

#[inline]
pub fn pread_direct(key: AIOKey, fd: RawFd, offset: usize, buf: Buf) -> AIO {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    AIO::new(
        key,
        fd,
        offset,
        buf,
        PRIORITY,
        FLAGS,
        io_iocb_cmd::IO_CMD_PREAD,
    )
}

/// pread.
///
/// # Safety
///
/// Caller must guarantee the pointer is valid during
/// syscall, and pointer is correctly aligned.
#[inline]
pub unsafe fn pread_unchecked(
    key: AIOKey,
    fd: RawFd,
    offset: usize,
    ptr: *mut u8,
    len: usize,
) -> UnsafeAIO {
    unsafe {
        const PRIORITY: u16 = 0;
        const FLAGS: u32 = 0;
        UnsafeAIO::new(
            key,
            fd,
            offset,
            ptr,
            len,
            PRIORITY,
            FLAGS,
            io_iocb_cmd::IO_CMD_PREAD,
        )
    }
}

#[inline]
pub fn pwrite_direct(key: AIOKey, fd: RawFd, offset: usize, buf: Buf) -> AIO {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    AIO::new(
        key,
        fd,
        offset,
        buf,
        PRIORITY,
        FLAGS,
        io_iocb_cmd::IO_CMD_PWRITE,
    )
}

/// pwrite.
///
/// # Safety
///
/// Caller must guarantee the pointer is valid during
/// syscall, and pointer is correctly aligned.
#[inline]
pub unsafe fn pwrite_unchecked(
    key: AIOKey,
    fd: RawFd,
    offset: usize,
    ptr: *mut u8,
    len: usize,
) -> UnsafeAIO {
    unsafe {
        const PRIORITY: u16 = 0;
        const FLAGS: u32 = 0;
        UnsafeAIO::new(
            key,
            fd,
            offset,
            ptr,
            len,
            PRIORITY,
            FLAGS,
            io_iocb_cmd::IO_CMD_PWRITE,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_file_open_and_create() {
        let res = SparseFile::open("sparsefile1.bin", 1024 * 1024);
        assert!(res.is_err());
        let file = SparseFile::create("sparsefile1.bin", 1024 * 1024).unwrap();
        drop(file);
        let file = SparseFile::open("sparsefile1.bin", 1024 * 1024).unwrap();
        drop(file);
        let _ = std::fs::remove_file("sparsefile1.bin");
    }
}
