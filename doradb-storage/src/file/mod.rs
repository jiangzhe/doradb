pub mod cow_file;
pub mod meta_page;
pub mod multi_table_file;
pub mod page_integrity;
pub mod super_page;
pub mod table_file;
pub mod table_fs;

#[cfg(test)]
pub(crate) use self::table_fs::tests::{build_test_fs, build_test_fs_in};

use crate::buffer::{PersistedBlockKey, ReadSubmission};
use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::{
    AIOClient, AIOError, AIOKind, AIOResult, AIOStats, Completion, IOQueue, IOStateMachine,
    IOSubmission, Operation, STORAGE_SECTOR_SIZE, align_to_sector_size,
};
use crate::{error::Error, error::Result};
use libc::{
    O_CREAT, O_DIRECT, O_EXCL, O_RDWR, O_TRUNC, close, fdatasync, fstat, fsync, ftruncate, open,
    stat,
};
use parking_lot::RawMutex;
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use scopeguard::defer;
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

/// Worker-owned write submission for the table-filesystem IO worker.
///
/// The submission keeps the persisted-block identity, the backend-agnostic IO
/// operation, and the waiter notification state together until completion.
pub struct WriteSubmission {
    key: PersistedBlockKey,
    operation: Operation,
    completion: Arc<Completion<Result<()>>>,
}

impl WriteSubmission {
    #[inline]
    fn prepare(
        key: PersistedBlockKey,
        fd: RawFd,
        offset: usize,
        buf: DirectBuf,
    ) -> (Self, impl std::future::Future<Output = Result<()>> + Send) {
        let completion = Arc::new(Completion::new());
        let waiter = Arc::clone(&completion);
        let fio = WriteSubmission {
            key,
            operation: Operation::pwrite_owned(fd, offset, buf),
            completion,
        };
        (fio, async move { waiter.wait_result().await })
    }
}

impl IOSubmission for WriteSubmission {
    type Key = PersistedBlockKey;

    #[inline]
    fn key(&self) -> &Self::Key {
        &self.key
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

pub(crate) enum TableFsRequest {
    Write(WriteSubmission),
    Read(ReadSubmission),
}

pub(crate) enum TableFsSubmission {
    Write(WriteSubmission),
    Read(ReadSubmission),
}

impl IOSubmission for TableFsSubmission {
    type Key = PersistedBlockKey;

    #[inline]
    fn key(&self) -> &Self::Key {
        match self {
            TableFsSubmission::Write(sub) => sub.key(),
            TableFsSubmission::Read(sub) => sub.key(),
        }
    }

    #[inline]
    fn operation(&mut self) -> &mut Operation {
        match self {
            TableFsSubmission::Write(sub) => sub.operation(),
            TableFsSubmission::Read(sub) => sub.operation(),
        }
    }
}

/// Write one direct buffer via async direct IO.
///
/// `key` identifies the persisted block targeted by this write for higher-layer
/// invalidation and inflight bookkeeping.
///
#[inline]
pub(crate) async fn write_direct(
    key: PersistedBlockKey,
    fd: RawFd,
    offset: usize,
    buf: DirectBuf,
    io_client: &AIOClient<TableFsRequest>,
) -> Result<()> {
    let (submission, result) = WriteSubmission::prepare(key, fd, offset, buf);
    if let Err(err) = io_client
        .send_async(TableFsRequest::Write(submission))
        .await
    {
        let TableFsRequest::Write(_submission) = err.into_inner() else {
            unreachable!("write_direct received unexpected readonly-load send error");
        };
        return Err(Error::SendError);
    }
    result.await
}

pub(crate) struct TableFsStateMachine {
    stats: AIOStats,
}

impl TableFsStateMachine {
    /// Creates one state machine for the shared table-filesystem IO worker.
    #[inline]
    pub fn new() -> TableFsStateMachine {
        TableFsStateMachine {
            stats: AIOStats::default(),
        }
    }
}

impl IOStateMachine for TableFsStateMachine {
    type Request = TableFsRequest;
    type Key = PersistedBlockKey;
    type Submission = TableFsSubmission;

    #[inline]
    fn prepare_request(
        &mut self,
        req: TableFsRequest,
        max_new: usize,
        queue: &mut IOQueue<TableFsSubmission>,
    ) -> Option<TableFsRequest> {
        if max_new == 0 {
            return Some(req);
        }
        match req {
            TableFsRequest::Write(req) => queue.push(TableFsSubmission::Write(req)),
            TableFsRequest::Read(req) => queue.push(TableFsSubmission::Read(req)),
        }
        None
    }

    #[inline]
    fn on_submit(&mut self, _sub: &TableFsSubmission) {
        // Submission ownership is retained by the worker inflight table.
    }

    #[inline]
    fn on_complete(&mut self, sub: TableFsSubmission, res: std::io::Result<usize>) -> AIOKind {
        match sub {
            TableFsSubmission::Write(mut sub) => {
                let buf = sub
                    .operation
                    .take_buf()
                    .expect("write operation must still own its direct buffer");
                match res {
                    Ok(len) => {
                        debug_assert!(len == buf.capacity());
                        drop(buf);
                        sub.completion.complete(Ok(()));
                    }
                    Err(err) => {
                        drop(buf);
                        sub.completion.complete(Err(err.into()));
                    }
                }
                AIOKind::Write
            }
            TableFsSubmission::Read(sub) => sub.complete(res),
        }
    }

    #[inline]
    fn on_stats(&mut self, stats: &AIOStats) {
        self.stats.merge(stats);
    }

    #[inline]
    fn end_loop(self) {
        // do nothing
    }
}

/// FileSyncer is a simple wrapper to provide functionality
/// of fsync() and fdatasync().
pub struct FileSyncer(RawFd);

impl FileSyncer {
    #[inline]
    pub fn fsync(&self) -> Result<()> {
        let ret = unsafe { fsync(self.0) };
        if ret == 0 {
            return Ok(());
        }
        debug_assert!(ret == -1);
        Err(std::io::Error::last_os_error().into())
    }

    #[inline]
    pub fn fdatasync(&self) -> Result<()> {
        let ret = unsafe { fdatasync(self.0) };
        if ret == 0 {
            return Ok(());
        }
        debug_assert!(ret == -1);
        Err(std::io::Error::last_os_error().into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sparse_file_open_and_create() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("sparsefile1.bin");
        let file_path = file_path.to_string_lossy().into_owned();
        let res = SparseFile::open(&file_path);
        assert!(res.is_err());
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024).unwrap();
        drop(file);
        let file = SparseFile::open(&file_path).unwrap();
        drop(file);
    }
}
