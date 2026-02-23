pub mod meta_page;
pub mod super_page;
pub mod table_file;
pub mod table_fs;

use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::io_iocb_cmd;
use crate::io::{
    AIO, AIOBuf, AIOContext, AIOError, AIOEventListener, AIOKey, AIOKind, AIOResult, AIOStats,
    IOQueue, STORAGE_SECTOR_SIZE, UnsafeAIO, align_to_sector_size,
};
use crate::notify::EventNotifyOnDrop;
use crate::ptr::UnsafePtr;
use event_listener::{EventListener, Listener};
use libc::{
    O_CREAT, O_DIRECT, O_EXCL, O_RDWR, O_TRUNC, close, fdatasync, fstat, fsync, ftruncate, open,
    stat,
};
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use parking_lot::{Mutex, RawMutex};
use scopeguard::defer;
use std::collections::HashMap;
use std::ffi::CString;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::unix::io::{AsRawFd, RawFd};
use std::result::Result as StdResult;
use std::sync::Arc;
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
    /// Create a sparse file and truncate if file already exists.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub fn create_or_trunc(file_path: impl AsRef<str>, max_size: usize) -> AIOResult<SparseFile> {
        // SAFETY: libc calls (`open`, `ftruncate`, `close`) are used with validated C string
        // arguments and checked return codes before constructing `SparseFile`.
        unsafe {
            let c_string =
                CString::new(file_path.as_ref()).map_err(|_| AIOError::CreateFileError)?;
            let fd = open(
                c_string.as_ptr(),
                O_CREAT | O_RDWR | O_TRUNC | O_DIRECT,
                0o644,
            );
            if fd < 0 {
                return Err(AIOError::CreateFileError);
            }
            let ret = ftruncate(fd, max_size as i64);
            if ret < 0 {
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(AIOError::TruncFileError);
            }
            Ok(SparseFile::new(fd, 0, max_size))
        }
    }

    /// Create a sparse file and fail if file already exists.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub fn create_or_fail(file_path: impl AsRef<str>, max_size: usize) -> AIOResult<SparseFile> {
        unsafe {
            let c_string =
                CString::new(file_path.as_ref()).map_err(|_| AIOError::CreateFileError)?;
            let fd = open(
                c_string.as_ptr(),
                O_CREAT | O_RDWR | O_EXCL | O_DIRECT,
                0o644,
            );
            if fd < 0 {
                return Err(AIOError::CreateFileError);
            }
            let ret = ftruncate(fd, max_size as i64);
            if ret < 0 {
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(AIOError::TruncFileError);
            }
            Ok(SparseFile::new(fd, 0, max_size))
        }
    }

    /// Open an existing sparse file with given maximum length.
    #[inline]
    pub fn open(file_path: impl AsRef<str>) -> AIOResult<SparseFile> {
        unsafe {
            let c_string = CString::new(file_path.as_ref()).map_err(|_| AIOError::OpenFileError)?;
            let fd = open(c_string.as_ptr(), O_RDWR | O_DIRECT, 0o644);
            if fd < 0 {
                return Err(AIOError::OpenFileError);
            }
            let (logical_size, _) = match sparse_file_size(fd) {
                Ok((l, a)) => (l, a),
                Err(_) => return Err(AIOError::StatError),
            };
            Ok(SparseFile::new(fd, 0, logical_size))
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
    pub fn alloc(&self, len: usize) -> StdResult<(usize, usize), AIOError> {
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
    pub fn pread_direct<T: AIOBuf>(&self, key: AIOKey, offset: usize, buf: T) -> AIO<T> {
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
    pub fn pwrite_direct<T: AIOBuf>(&self, key: AIOKey, offset: usize, buf: T) -> AIO<T> {
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
        sparse_file_size(self.fd)
    }
}

impl Drop for SparseFile {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self.fd` is owned by this `SparseFile` instance and closed exactly once here.
        unsafe {
            close(self.fd);
        }
    }
}

#[inline]
pub fn sparse_file_size(fd: RawFd) -> std::io::Result<(usize, usize)> {
    // SAFETY: `fstat` fully initializes `stat` on success; `assume_init_ref` is used only
    // when return code is 0.
    unsafe {
        let mut s = MaybeUninit::<stat>::zeroed();
        let retcode = fstat(fd, s.as_mut_ptr());
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

pub enum FileIOState {
    RequestBuf(DirectBuf),
    RunningBuf(AIO<DirectBuf>),
    RequestStaticRead { ptr: UnsafePtr<u8>, len: usize },
    RunningStaticRead { aio: UnsafeAIO, len: usize },
    Result(FileIOResult),
    Invalid,
}

impl FileIOState {
    #[inline]
    pub fn take_buf(&mut self) -> Option<DirectBuf> {
        let state = std::mem::replace(self, FileIOState::Invalid);
        match state {
            FileIOState::RequestBuf(buf) => Some(buf),
            FileIOState::RunningBuf(mut aio) => aio.take_buf(),
            FileIOState::Result(FileIOResult::ReadOk(buf)) => Some(buf),
            FileIOState::Result(_) | FileIOState::Invalid => None,
            FileIOState::RequestStaticRead { .. } | FileIOState::RunningStaticRead { .. } => None,
        }
    }

    #[inline]
    pub fn aio_key(&self) -> Option<AIOKey> {
        match self {
            FileIOState::RunningBuf(aio) => Some(aio.key),
            FileIOState::RunningStaticRead { aio, .. } => Some(aio.key),
            _ => None,
        }
    }
}

impl Default for FileIOState {
    #[inline]
    fn default() -> Self {
        FileIOState::Invalid
    }
}

pub enum FileIOResult {
    ReadOk(DirectBuf),
    ReadStaticOk,
    WriteOk,
    Err(std::io::Error),
}

pub struct FileIO {
    kind: AIOKind,
    fd: RawFd,
    offset: usize,
    state: Arc<Mutex<FileIOState>>,
    ev: EventNotifyOnDrop,
    recycle: bool,
}

impl FileIO {
    #[inline]
    pub fn prepare(
        kind: AIOKind,
        fd: RawFd,
        offset: usize,
        buf: DirectBuf,
        recycle: bool,
    ) -> (Self, FileIOPromise) {
        let ev = EventNotifyOnDrop::new();
        let listener = ev.listen();
        let state = Arc::new(Mutex::new(FileIOState::RequestBuf(buf)));
        let fio = FileIO {
            kind,
            fd,
            offset,
            state: state.clone(),
            ev,
            recycle,
        };
        let promise = FileIOPromise { state, listener };
        (fio, promise)
    }

    /// Prepares an async read that targets caller-owned memory.
    ///
    /// # Safety
    ///
    /// The caller must keep `ptr..ptr+len` valid and sector-aligned until
    /// the returned promise is resolved.
    #[inline]
    pub unsafe fn prepare_static_read(
        fd: RawFd,
        offset: usize,
        ptr: UnsafePtr<u8>,
        len: usize,
    ) -> (Self, FileIOPromise) {
        let ev = EventNotifyOnDrop::new();
        let listener = ev.listen();
        let state = Arc::new(Mutex::new(FileIOState::RequestStaticRead { ptr, len }));
        let fio = FileIO {
            kind: AIOKind::Read,
            fd,
            offset,
            state: state.clone(),
            ev,
            recycle: false,
        };
        let promise = FileIOPromise { state, listener };
        (fio, promise)
    }

    #[inline]
    pub fn take_buf(self) -> Option<DirectBuf> {
        let mut g = self.state.lock();
        g.take_buf()
    }
}

pub struct FileIOPromise {
    state: Arc<Mutex<FileIOState>>,
    listener: EventListener,
}

impl FileIOPromise {
    #[inline]
    pub fn wait(self) -> FileIOResult {
        self.listener.wait();
        let mut g = self.state.lock();
        match std::mem::take(&mut *g) {
            FileIOState::Result(res) => res,
            FileIOState::Invalid
            | FileIOState::RequestBuf(_)
            | FileIOState::RunningBuf(_)
            | FileIOState::RequestStaticRead { .. }
            | FileIOState::RunningStaticRead { .. } => {
                panic!("invalid state");
            }
        }
    }

    #[inline]
    pub async fn wait_async(self) -> FileIOResult {
        self.listener.await;
        let mut g = self.state.lock();
        match std::mem::take(&mut *g) {
            FileIOState::Result(res) => res,
            FileIOState::Invalid
            | FileIOState::RequestBuf(_)
            | FileIOState::RunningBuf(_)
            | FileIOState::RequestStaticRead { .. }
            | FileIOState::RunningStaticRead { .. } => {
                panic!("invalid state");
            }
        }
    }
}

pub struct FileIOListener {
    inflight_io: HashMap<AIOKey, FileIO>,
    key: AIOKey,
    stats: AIOStats,
    buf_list: FixedSizeBufferFreeList,
}

impl FileIOListener {
    #[inline]
    pub fn new(buf_list: FixedSizeBufferFreeList) -> FileIOListener {
        FileIOListener {
            inflight_io: HashMap::new(),
            key: 0,
            stats: AIOStats::default(),
            buf_list,
        }
    }
}

impl AIOEventListener for FileIOListener {
    type Request = FileIO;
    type Submission = FileIO;

    #[inline]
    fn on_request(&mut self, req: FileIO, queue: &mut IOQueue<FileIO>) {
        let key = self.key;
        self.key += 1;
        let mut g = req.state.lock();
        let state = std::mem::replace(&mut *g, FileIOState::Invalid);
        let iocb = match state {
            FileIOState::RequestBuf(buf) => {
                let aio = match req.kind {
                    AIOKind::Read => pread_direct(key, req.fd, req.offset, buf),
                    AIOKind::Write => pwrite_direct(key, req.fd, req.offset, buf),
                };
                let iocb = aio.iocb_raw();
                *g = FileIOState::RunningBuf(aio);
                iocb
            }
            FileIOState::RequestStaticRead { ptr, len } => {
                debug_assert!(matches!(req.kind, AIOKind::Read));
                // SAFETY: caller of `prepare_static_read` guarantees pointer validity and
                // alignment for the whole async IO operation.
                let aio = unsafe { pread_unchecked(key, req.fd, req.offset, ptr.0, len) };
                let iocb = aio.iocb_raw();
                *g = FileIOState::RunningStaticRead { aio, len };
                iocb
            }
            FileIOState::Result(_)
            | FileIOState::Invalid
            | FileIOState::RunningBuf(_)
            | FileIOState::RunningStaticRead { .. } => {
                panic!("invalid file io request state")
            }
        };
        drop(g);
        queue.push(iocb, req);
    }

    #[inline]
    fn on_submit(&mut self, sub: FileIO) {
        let aio_key = {
            let g = sub.state.lock();
            g.aio_key().unwrap()
        };
        let res = self.inflight_io.insert(aio_key, sub);
        debug_assert!(res.is_none());
    }

    #[inline]
    fn on_complete(&mut self, key: AIOKey, res: std::io::Result<usize>) -> AIOKind {
        let sub = self.inflight_io.remove(&key).unwrap();
        let mut g = sub.state.lock();
        let state = std::mem::replace(&mut *g, FileIOState::Invalid);
        match state {
            FileIOState::RunningBuf(mut aio) => {
                let mut buf = aio.take_buf().unwrap();
                match res {
                    Ok(len) => {
                        debug_assert!(len == buf.capacity());
                        match sub.kind {
                            AIOKind::Read => {
                                buf.truncate(len);
                                *g = FileIOState::Result(FileIOResult::ReadOk(buf));
                            }
                            AIOKind::Write => {
                                // recycle write buffer.
                                if sub.recycle {
                                    self.buf_list.recycle(buf);
                                }
                                *g = FileIOState::Result(FileIOResult::WriteOk);
                            }
                        }
                    }
                    Err(err) => {
                        self.buf_list.recycle(buf);
                        *g = FileIOState::Result(FileIOResult::Err(err));
                    }
                }
            }
            FileIOState::RunningStaticRead { aio: _aio, len } => match res {
                Ok(read_len) if read_len == len => {
                    *g = FileIOState::Result(FileIOResult::ReadStaticOk);
                }
                Ok(read_len) => {
                    *g = FileIOState::Result(FileIOResult::Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "static read completed with short length: expected={}, actual={}",
                            len, read_len
                        ),
                    )));
                }
                Err(err) => {
                    *g = FileIOState::Result(FileIOResult::Err(err));
                }
            },
            FileIOState::RequestBuf(_)
            | FileIOState::RequestStaticRead { .. }
            | FileIOState::Result(_)
            | FileIOState::Invalid => {
                panic!("invalid file io completion state");
            }
        }
        drop(g);
        drop(sub.ev); // notify waiter.
        sub.kind
    }

    #[inline]
    fn on_stats(&mut self, stats: &AIOStats) {
        self.stats.merge(stats);
    }

    #[inline]
    fn end_loop(self, _ctx: &AIOContext) {
        // do nothing
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

/// Fixed size buffer free list hold a given number of
/// buffer pages.
/// It's used to reuse pages in heavy IO environment.
#[derive(Clone)]
pub struct FixedSizeBufferFreeList(Arc<FreeList<DirectBuf>>);

impl FixedSizeBufferFreeList {
    /// Create a new buffer free list with given number of
    /// pre-allocated buffer pages.
    #[inline]
    pub fn new(page_size: usize, init_pages: usize, max_pages: usize) -> Self {
        debug_assert!(page_size.is_multiple_of(STORAGE_SECTOR_SIZE));
        let free_list: FreeList<_> = FreeList::new(init_pages, max_pages, move || {
            let mut buf = DirectBuf::zeroed(page_size);
            buf.truncate(0);
            buf
        });
        FixedSizeBufferFreeList(Arc::new(free_list))
    }

    /// Recycle the buffer for future use.
    #[inline]
    pub fn recycle(&self, mut buf: DirectBuf) {
        buf.reset();
        buf.truncate(0);
        self.push(buf);
    }
}

impl Deref for FixedSizeBufferFreeList {
    type Target = FreeList<DirectBuf>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[inline]
pub fn pread_direct<T: AIOBuf>(key: AIOKey, fd: RawFd, offset: usize, buf: T) -> AIO<T> {
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
pub fn pwrite_direct<T: AIOBuf>(key: AIOKey, fd: RawFd, offset: usize, buf: T) -> AIO<T> {
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
        let res = SparseFile::open("sparsefile1.bin");
        assert!(res.is_err());
        let file = SparseFile::create_or_trunc("sparsefile1.bin", 1024 * 1024).unwrap();
        drop(file);
        let file = SparseFile::open("sparsefile1.bin").unwrap();
        drop(file);
        let _ = std::fs::remove_file("sparsefile1.bin");
    }
}
