mod buf;
mod free_list;
mod libaio_abi;

use libc::{
    c_long, close, fdatasync, fsync, ftruncate, open, EAGAIN, EINTR, O_CREAT, O_DIRECT, O_RDWR,
    O_TRUNC,
};
use std::ffi::CString;
use std::ops::Deref;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use thiserror::Error;

pub use buf::*;
pub use free_list::*;
pub use libaio_abi::*;

pub const MIN_PAGE_SIZE: usize = 4096;
pub const STORAGE_SECTOR_SIZE: usize = 4096;

/// Align given input length to storage sector size.
#[allow(clippy::manual_div_ceil)]
#[inline]
pub fn align_to_sector_size(len: usize) -> usize {
    (len + STORAGE_SECTOR_SIZE - 1) / STORAGE_SECTOR_SIZE * STORAGE_SECTOR_SIZE
}

const DEFAULT_AIO_MAX_EVENTS: usize = 32;

#[derive(Debug, Clone, Error)]
pub enum AIOError {
    #[error("AIO setup error")]
    SetupError,
    #[error("AIO open file error")]
    OpenFileError,
    #[error("AIO out of range")]
    OutOfRange,
}

pub struct AIOContext(io_context_t);

unsafe impl Sync for AIOContext {}
unsafe impl Send for AIOContext {}

impl Deref for AIOContext {
    type Target = io_context_t;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AIOContext {
    #[inline]
    fn new(maxevents: u32) -> Result<Self, AIOError> {
        let mut ctx = std::ptr::null_mut();
        unsafe {
            match io_setup(maxevents as i32, &mut ctx) {
                0 => Ok(AIOContext(ctx)),
                _ => Err(AIOError::SetupError),
            }
        }
    }
}

impl Drop for AIOContext {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            assert_eq!(io_destroy(self.0), 0);
        }
    }
}

pub type IocbPtr = AtomicPtr<iocb>;
pub type IocbRawPtr = *mut iocb;

/// AIO backed by an owned aligned buffer.
pub struct AIO {
    iocb: IocbPtr,
    // this is essential because libaio requires the pointer
    // to buffer keep valid during async processing.
    buf: Option<Buf>,
    pub key: AIOKey,
}

impl AIO {
    #[inline]
    pub fn new(
        key: AIOKey,
        fd: RawFd,
        offset: usize,
        mut buf: Buf,
        priority: u16,
        flags: u32,
        opcode: io_iocb_cmd,
    ) -> Self {
        let iocb = unsafe { iocb::alloc() };
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.buf = buf.raw_ptr_mut();
        iocb.count = buf.capacity() as u64;
        iocb.offset = offset as u64;
        iocb.flags = flags;
        iocb.data = key; // store and send back via io_event
        AIO {
            key,
            buf: Some(buf),
            iocb: AtomicPtr::new(iocb),
        }
    }

    #[inline]
    pub fn iocb(&self) -> &IocbPtr {
        &self.iocb
    }

    #[inline]
    pub fn take_buf(&mut self) -> Option<Buf> {
        self.buf.take()
    }

    #[inline]
    pub fn buf(&self) -> Option<&Buf> {
        self.buf.as_ref()
    }
}

impl Drop for AIO {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.iocb.load(Ordering::Relaxed)));
        }
    }
}

/// AIO backed by a raw pointer.
/// The raw pointer must be aigned to page and
/// has outlive the async IO call.
///
/// This struct is mainly used for page IO in a buffer pool,
/// Which pre-assign many pages through mmap() call and do
/// not release any of them. So the IO request is guaranteed
/// to be safe.
pub struct UnsafeAIO {
    iocb: IocbPtr,
    pub key: AIOKey,
}

impl UnsafeAIO {
    /// Create a new unsafe IO.
    ///
    /// # Safety
    ///
    /// Caller must guarantee pointer is valid during
    /// syscall and correctly aligned.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub unsafe fn new(
        key: AIOKey,
        fd: RawFd,
        offset: usize,
        ptr: *mut u8,
        len: usize,
        priority: u16,
        flags: u32,
        opcode: io_iocb_cmd,
    ) -> Self {
        let iocb = unsafe { iocb::alloc() };
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.buf = ptr;
        iocb.count = len as u64;
        iocb.offset = offset as u64;
        iocb.flags = flags;
        iocb.data = key; // store and send back via io_event
        UnsafeAIO {
            iocb: AtomicPtr::new(iocb),
            key,
        }
    }

    #[inline]
    pub fn iocb(&self) -> &IocbPtr {
        &self.iocb
    }
}

/// AIOKey represents the unique key of any AIO request.
pub type AIOKey = u64;

/// AIOManager controls all AIO operations.
pub struct AIOManager {
    ctx: AIOContext,
    fd_count: AtomicU64,
    max_events: usize,
}

impl AIOManager {
    /// Create a sparse file with given maximum length.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub fn create_sparse_file(
        &self,
        file_path: impl AsRef<str>,
        max_len: usize,
    ) -> Result<SparseFile, AIOError> {
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
            self.register_fd(fd);
            Ok(SparseFile {
                fd,
                offset: AtomicUsize::new(0),
                max_len,
            })
        }
    }

    /// Open an existing sparse file with given maximum length.
    #[inline]
    pub fn open_sparse_file(
        &self,
        file_path: impl AsRef<str>,
        max_len: usize,
    ) -> Result<SparseFile, AIOError> {
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
            self.register_fd(fd);
            Ok(SparseFile {
                fd,
                offset: AtomicUsize::new(0),
                max_len,
            })
        }
    }

    #[inline]
    pub fn drop_sparse_file(&self, file: SparseFile) {
        self.deregister_fd(file.as_raw_fd());
    }

    #[inline]
    pub fn register_fd(&self, _fd: RawFd) {
        self.fd_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn deregister_fd(&self, _fd: RawFd) {
        self.fd_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn max_events(&self) -> usize {
        self.max_events
    }

    #[inline]
    pub fn events(&self) -> Box<[io_event]> {
        vec![io_event::default(); self.max_events].into_boxed_slice()
    }

    #[inline]
    pub fn submit(&self, reqs: &mut Vec<*mut iocb>) -> usize {
        if reqs.is_empty() {
            return 0;
        }
        let batch_size = reqs.len();
        let ret = unsafe { io_submit(*self.ctx, batch_size as c_long, reqs.as_mut_ptr()) };
        // See https://man7.org/linux/man-pages/man2/io_submit.2.html
        // for the details of return value of io_submit().
        // if success, non-negative value indicates how many IO submitted.
        // if error, negative value of error code.
        if ret < 0 && -ret != EAGAIN {
            panic!("io_submit returns error code {ret}: batch_size={batch_size}");
        }
        reqs.clear();
        ret.max(0) as usize
    }

    #[inline]
    pub fn submit_limit(&self, reqs: &mut Vec<*mut iocb>, limit: usize) -> usize {
        if reqs.is_empty() || limit == 0 {
            return 0;
        }
        let batch_size = limit.min(reqs.len());
        let ret = unsafe { io_submit(*self.ctx, batch_size as c_long, reqs.as_mut_ptr()) };
        // See https://man7.org/linux/man-pages/man2/io_submit.2.html
        // for the details of return value of io_submit().
        // if success, non-negative value indicates how many IO submitted.
        // if error, negative value of error code.
        // if ret < 0 && -ret != EAGAIN {
        if ret < 0 {
            panic!("io_submit returns error code {ret}: batch_size={batch_size}");
        }
        let submit_count = ret as usize;
        reqs.drain(..submit_count);
        submit_count
    }

    /// Wait until given number of IO finishes, and execute callback for each.
    /// Returns number of finished events.
    #[inline]
    pub fn wait_at_least<F>(&self, events: &mut [io_event], min_nr: usize, mut callback: F) -> usize
    where
        F: FnMut(AIOKey, Result<usize, std::io::Error>),
    {
        let max_nwait = events.len();
        let count = loop {
            let ret = unsafe {
                io_getevents(
                    *self.ctx,
                    min_nr as c_long,
                    max_nwait as c_long,
                    events.as_mut_ptr(),
                    std::ptr::null_mut(),
                )
            };
            if ret < 0 {
                let errcode = -ret;
                if errcode == EINTR {
                    // retry if interrupt
                    continue;
                }
                panic!("io_getevents returns error code {errcode}");
            }
            break ret as usize;
        };
        assert!(
            count != 0,
            "io_getevents with min_nr=1 and timeout=None should not return 0"
        );
        for ev in &events[..count] {
            let key = ev.data;
            let res = if ev.res >= 0 {
                Ok(ev.res as usize)
            } else {
                let err = std::io::Error::from_raw_os_error(-ev.res as i32);
                Err(err)
            };
            callback(key, res);
        }
        count
    }
}

impl Drop for AIOManager {
    #[inline]
    fn drop(&mut self) {
        // check if there are any remained file descriptors remain opened.
        let fd_count = self.fd_count.load(Ordering::Relaxed);
        if fd_count != 0 {
            panic!("{fd_count} files remain opened when shutdown AIOManager");
        }
    }
}

unsafe impl Send for AIOManager {}
unsafe impl Sync for AIOManager {}

pub struct AIOManagerConfig {
    max_events: usize,
}

impl AIOManagerConfig {
    #[inline]
    pub fn max_events(mut self, max_events: usize) -> Self {
        self.max_events = max_events;
        self
    }

    #[inline]
    pub fn build(self) -> Result<AIOManager, AIOError> {
        let ctx = AIOContext::new(self.max_events as u32)?;
        Ok(AIOManager {
            ctx,
            fd_count: AtomicU64::new(0),
            max_events: self.max_events,
        })
    }

    #[inline]
    pub fn build_static(self) -> Result<&'static AIOManager, AIOError> {
        let aio_mgr = self.build()?;
        Ok(Box::leak(Box::new(aio_mgr)))
    }
}

impl Default for AIOManagerConfig {
    #[inline]
    fn default() -> Self {
        AIOManagerConfig {
            max_events: DEFAULT_AIO_MAX_EVENTS,
        }
    }
}

pub struct SparseFile {
    fd: RawFd,
    offset: AtomicUsize,
    max_len: usize,
}

impl AsRawFd for SparseFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl SparseFile {
    /// Allocate enough space for data of given length to persist
    /// at end of the file.
    #[inline]
    pub fn alloc(&self, len: usize) -> Result<(usize, usize), AIOError> {
        let size = align_to_sector_size(len);
        loop {
            let offset = self.offset.load(Ordering::Relaxed);
            let new_offset = offset + size;
            if new_offset > self.max_len {
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
    pub fn pread_direct(&self, key: AIOKey, offset: usize, len: usize) -> AIO {
        pread_direct(key, self.fd, offset, len)
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
    pub fn pwrite_direct(&self, key: AIOKey, offset: usize, buf: DirectBuf) -> AIO {
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
pub fn pread_direct(key: AIOKey, fd: RawFd, offset: usize, len: usize) -> AIO {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    let buf = DirectBuf::uninit(len);
    AIO::new(
        key,
        fd,
        offset,
        Buf::Direct(buf),
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
pub fn pwrite_direct(key: AIOKey, fd: RawFd, offset: usize, buf: DirectBuf) -> AIO {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    AIO::new(
        key,
        fd,
        offset,
        Buf::Direct(buf),
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

/// mlock.
///
/// # Safety
///
/// Wrapper of mlock.
#[cfg(feature = "mlock")]
#[inline]
pub unsafe fn mlock(ptr: *mut u8, len: usize) -> bool {
    let res = libc::mlock(ptr as *const c_void, len);
    res == 0
}

/// mlock.
///
/// # Safety
///
/// Wrapper of mlock.
#[cfg(not(feature = "mlock"))]
#[inline]
pub unsafe fn mlock(_ptr: *mut u8, _len: usize) -> bool {
    true
}

/// munlock.
///
/// # Safety
///
/// Wrapper of munlock.
#[cfg(feature = "mlock")]
#[inline]
pub unsafe fn munlock(ptr: *mut u8, len: usize) -> bool {
    let res = libc::munlock(ptr as *const c_void, len);
    res == 0
}

/// munlock.
///
/// # Safety
///
/// Wrapper of munlock.
#[cfg(not(feature = "mlock"))]
#[inline]
pub unsafe fn munlock(_ptr: *mut u8, _len: usize) -> bool {
    true
}

#[inline]
pub fn pread(key: AIOKey, fd: RawFd, offset: usize, buf: Buf) -> AIO {
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

#[inline]
pub fn pwrite(key: AIOKey, fd: RawFd, offset: usize, buf: Buf) -> AIO {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aio_file_ops() {
        let aio_mgr = AIOManagerConfig::default().max_events(16).build().unwrap();
        let file = aio_mgr.create_sparse_file("test.txt", 1024 * 1024).unwrap();
        let buf = DirectBuf::with_data(b"hello, world");
        let (offset, _) = file.alloc(buf.capacity()).unwrap();
        let aio = file.pwrite_direct(100, offset, buf);
        let mut reqs = vec![aio.iocb.load(Ordering::Relaxed)];
        let limit = reqs.len();
        let submit_count = aio_mgr.submit_limit(&mut reqs, limit);
        println!("submit_count={}", submit_count);
        let mut events = aio_mgr.events();
        aio_mgr.wait_at_least(&mut events, 1, |key, res| {
            println!("key={}, res={:?}", key, res);
        });
        aio_mgr.drop_sparse_file(file);
        // for test, we just remove this file
        let _ = std::fs::remove_file("test.txt");
    }
}
