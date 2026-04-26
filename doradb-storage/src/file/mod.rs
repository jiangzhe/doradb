pub mod block_integrity;
pub mod cow_file;
pub mod fs;
pub mod meta_block;
pub mod multi_table_file;
pub mod super_block;
pub mod table_file;

use self::fs::BackgroundWriteRequest;
#[cfg(test)]
pub(crate) use self::fs::tests::{build_test_fs, build_test_fs_in};
#[cfg(test)]
pub(crate) use self::tests::{
    FileSyncOp, FileSyncTestHook, set_file_sync_test_hook, test_block_id, test_file_id,
};

use crate::buffer::ReadSubmission;
use crate::catalog::USER_OBJ_ID_START;
use crate::compression::BitPackable;
use crate::error::{
    CompletionErrorKind, CompletionResult, ConfigError, Error, IoError, ResourceError, Result,
    StorageOp,
};
use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::{
    Completion, IOClient, IOKind, IOQueue, IOSubmission, Operation, STORAGE_SECTOR_SIZE,
    StdIoResult, align_to_sector_size,
};
use crate::serde::{Deser, Ser, Serde};
use bytemuck::{Pod, Zeroable};
use error_stack::Report;
use libc::{
    O_CREAT, O_DIRECT, O_EXCL, O_RDWR, O_TRUNC, close, fdatasync, fstat, fsync, ftruncate, open,
    stat,
};
use parking_lot::RawMutex;
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use scopeguard::defer;
use std::ffi::CString;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Physical file identity used by persisted-block mappings and shared-storage routing.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Zeroable, Pod)]
pub struct FileID(u64);

impl FileID {
    pub const MAX: Self = Self(u64::MAX);

    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn to_le_bytes(self) -> [u8; std::mem::size_of::<u64>()] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub const fn from_le_bytes(bytes: [u8; std::mem::size_of::<u64>()]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

impl From<u64> for FileID {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<u32> for FileID {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

impl From<usize> for FileID {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl From<FileID> for u64 {
    #[inline]
    fn from(value: FileID) -> Self {
        value.0
    }
}

impl From<FileID> for usize {
    #[inline]
    fn from(value: FileID) -> Self {
        value.as_usize()
    }
}

impl PartialEq<u64> for FileID {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<i32> for FileID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<FileID> for u64 {
    #[inline]
    fn eq(&self, other: &FileID) -> bool {
        *self == other.0
    }
}

impl PartialEq<FileID> for i32 {
    #[inline]
    fn eq(&self, other: &FileID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl fmt::Display for FileID {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Ser<'_> for FileID {
    #[inline]
    fn ser_len(&self) -> usize {
        std::mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.0)
    }
}

impl Deser for FileID {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input
            .deser_u64(start_idx)
            .map(|(idx, raw)| (idx, Self(raw)))
    }
}

impl BitPackable for FileID {
    const ZERO: Self = Self(0);

    #[inline]
    fn sub_to_u64(self, min: Self) -> u64 {
        self.0.wrapping_sub(min.0)
    }

    #[inline]
    fn sub_to_u32(self, min: Self) -> u32 {
        self.0.wrapping_sub(min.0) as u32
    }

    #[inline]
    fn add_from_u32(self, delta: u32) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u16(self, min: Self) -> u16 {
        self.0.wrapping_sub(min.0) as u16
    }

    #[inline]
    fn add_from_u16(self, delta: u16) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u8(self, min: Self) -> u8 {
        self.0.wrapping_sub(min.0) as u8
    }

    #[inline]
    fn add_from_u8(self, delta: u8) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }
}

/// Sentinel persisted-file identity used by files that never participate in
/// persisted-block cache identity or shared-storage worker routing.
pub const UNTRACKED_FILE_ID: FileID = FileID::MAX;

/// Reserved persisted-file identity of the shared index-pool swap file.
pub const INDEX_POOL_SWAP_FILE_ID: FileID = FileID::new(USER_OBJ_ID_START - 3);

/// Reserved persisted-file identity of the shared mem-pool swap file.
pub const MEM_POOL_SWAP_FILE_ID: FileID = FileID::new(USER_OBJ_ID_START - 2);

/// Reserved persisted-file identity of `catalog.mtb`.
pub const CATALOG_MTB_FILE_ID: FileID = FileID::new(USER_OBJ_ID_START - 1);

/// Persisted fixed-size file block identity.
///
/// This id is reserved for physical blocks stored in copy-on-write files and
/// readonly-cache lookups. Runtime buffer-managed pages use
/// `crate::buffer::PageID` instead.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Zeroable, Pod)]
pub struct BlockID(u64);

impl BlockID {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn to_le_bytes(self) -> [u8; std::mem::size_of::<u64>()] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub const fn from_le_bytes(bytes: [u8; std::mem::size_of::<u64>()]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

impl From<u64> for BlockID {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<u32> for BlockID {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

impl From<usize> for BlockID {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl From<BlockID> for u64 {
    #[inline]
    fn from(value: BlockID) -> Self {
        value.0
    }
}

impl From<BlockID> for usize {
    #[inline]
    fn from(value: BlockID) -> Self {
        value.as_usize()
    }
}

impl Add<u64> for BlockID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign<u64> for BlockID {
    #[inline]
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for BlockID {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<u64> for BlockID {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl fmt::Display for BlockID {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq<i32> for BlockID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<BlockID> for i32 {
    #[inline]
    fn eq(&self, other: &BlockID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl Ser<'_> for BlockID {
    #[inline]
    fn ser_len(&self) -> usize {
        std::mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.0)
    }
}

impl Deser for BlockID {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input
            .deser_u64(start_idx)
            .map(|(idx, raw)| (idx, Self(raw)))
    }
}

impl BitPackable for BlockID {
    const ZERO: Self = Self(0);

    #[inline]
    fn sub_to_u64(self, min: Self) -> u64 {
        self.0.wrapping_sub(min.0)
    }

    #[inline]
    fn sub_to_u32(self, min: Self) -> u32 {
        self.0.wrapping_sub(min.0) as u32
    }

    #[inline]
    fn add_from_u32(self, delta: u32) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u16(self, min: Self) -> u16 {
        self.0.wrapping_sub(min.0) as u16
    }

    #[inline]
    fn add_from_u16(self, delta: u16) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }

    #[inline]
    fn sub_to_u8(self, min: Self) -> u8 {
        self.0.wrapping_sub(min.0) as u8
    }

    #[inline]
    fn add_from_u8(self, delta: u8) -> Self {
        Self(self.0.wrapping_add(delta as u64))
    }
}

/// Physical persisted-block identity for cache lookup and shared-storage IO.
///
/// This intentionally excludes root version so unchanged physical blocks keep
/// the same identity across root swaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    /// Persisted file identity owning the block.
    pub file_id: FileID,
    /// Physical page/block id in the backing file.
    pub block_id: BlockID,
}

impl BlockKey {
    /// Builds a key from file id and physical block id.
    #[inline]
    pub fn new(file_id: FileID, block_id: BlockID) -> Self {
        BlockKey { file_id, block_id }
    }
}

/// SparseFile is file with metadata describing empty blocks
/// instead of writing them.
/// The logical size of sparse file can be very large, but the
/// real allocated blocks can be only a few.
pub struct SparseFile {
    file_id: FileID,
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
    pub fn create_or_trunc(
        file_path: impl AsRef<str>,
        max_size: usize,
        file_id: FileID,
    ) -> Result<SparseFile> {
        // SAFETY: libc calls (`open`, `ftruncate`, `close`) are used with validated C string
        // arguments and checked return codes before constructing `SparseFile`.
        unsafe {
            let c_string = c_string_from_path(file_path.as_ref())?;
            let fd = open(
                c_string.as_ptr(),
                O_CREAT | O_RDWR | O_TRUNC | O_DIRECT,
                0o644,
            );
            if fd < 0 {
                return Err(IoError::report_with_op(
                    StorageOp::FileCreate,
                    io::Error::last_os_error(),
                )
                .into());
            }
            let ret = ftruncate(fd, max_size as i64);
            if ret < 0 {
                let err = io::Error::last_os_error();
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(IoError::report_with_op(StorageOp::FileResize, err).into());
            }
            Ok(SparseFile::new(fd, 0, max_size, file_id))
        }
    }

    /// Create a sparse file and fail if file already exists.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub fn create_or_fail(
        file_path: impl AsRef<str>,
        max_size: usize,
        file_id: FileID,
    ) -> Result<SparseFile> {
        // SAFETY: libc calls (`open`, `ftruncate`, `close`) are issued with a
        // validated C string and checked return codes before constructing
        // `SparseFile`.
        unsafe {
            let c_string = c_string_from_path(file_path.as_ref())?;
            let fd = open(
                c_string.as_ptr(),
                O_CREAT | O_RDWR | O_EXCL | O_DIRECT,
                0o644,
            );
            if fd < 0 {
                return Err(IoError::report_with_op(
                    StorageOp::FileCreate,
                    io::Error::last_os_error(),
                )
                .into());
            }
            let ret = ftruncate(fd, max_size as i64);
            if ret < 0 {
                let err = io::Error::last_os_error();
                let _ = close(fd); // close file descriptor if truncate fail.
                return Err(IoError::report_with_op(StorageOp::FileResize, err).into());
            }
            Ok(SparseFile::new(fd, 0, max_size, file_id))
        }
    }

    /// Open an existing sparse file with given maximum length.
    #[inline]
    pub fn open(file_path: impl AsRef<str>, file_id: FileID) -> Result<SparseFile> {
        // SAFETY: `open` is called with a validated C string, and the returned
        // fd is checked before it is used or wrapped.
        unsafe {
            let c_string = c_string_from_path(file_path.as_ref())?;
            let fd = open(c_string.as_ptr(), O_RDWR | O_DIRECT, 0o644);
            if fd < 0 {
                return Err(IoError::report_with_op(
                    StorageOp::FileOpen,
                    io::Error::last_os_error(),
                )
                .into());
            }
            let (logical_size, _) = match sparse_file_size(fd) {
                Ok((l, a)) => (l, a),
                Err(err) => {
                    let _ = close(fd);
                    return Err(IoError::report_with_op(StorageOp::FileStat, err).into());
                }
            };
            Ok(SparseFile::new(fd, 0, logical_size, file_id))
        }
    }

    /// Create a new sparse file.
    #[inline]
    fn new(fd: RawFd, offset: usize, max_len: usize, file_id: FileID) -> Self {
        SparseFile {
            file_id,
            fd,
            offset: AtomicUsize::new(offset),
            max_len: AtomicUsize::new(max_len),
            size_lock: RawMutex::INIT,
        }
    }

    /// Returns the immutable persisted-file identity attached to this file handle.
    #[inline]
    pub fn file_id(&self) -> FileID {
        self.file_id
    }

    /// Allocate enough space for data of given length to persist
    /// at end of the file.
    #[inline]
    pub fn alloc(&self, len: usize) -> Result<(usize, usize)> {
        let size = align_to_sector_size(len);
        loop {
            let offset = self.offset.load(Ordering::Relaxed);
            let new_offset = offset + size;
            let max_len = self.max_len.load(Ordering::Relaxed);
            if new_offset > max_len {
                return Err(Report::new(ResourceError::StorageFileCapacityExceeded)
                    .attach(format!(
                        "sparse file allocation: requested_len={len}, aligned_len={size}, current_offset={offset}, new_offset={new_offset}, max_len={max_len}"
                    ))
                    .into());
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
    pub fn extend_to(&self, max_len: usize) -> io::Result<()> {
        self.size_lock.lock();
        defer! {
            // SAFETY: this path holds `size_lock`, so the matching unlock is
            // paired with the successful lock above.
            unsafe { self.size_lock.unlock(); }
        }
        let curr_len = self.max_len.load(Ordering::Acquire);
        if max_len <= curr_len {
            return Ok(());
        }
        // SAFETY: `self.fd` is a live owned file descriptor and `max_len` is
        // the requested logical file length for this sparse file.
        let retcode = unsafe { ftruncate(self.fd, max_len as i64) };
        if retcode == 0 {
            return Ok(());
        }
        debug_assert!(retcode == -1);
        Err(io::Error::last_os_error())
    }

    /// Get the logical size and allocated size of this file.
    #[inline]
    pub fn size(&self) -> io::Result<(usize, usize)> {
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
pub fn sparse_file_size(fd: RawFd) -> io::Result<(usize, usize)> {
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
        Err(io::Error::last_os_error())
    }
}

#[inline]
fn c_string_from_path(file_path: &str) -> Result<CString> {
    CString::new(file_path).map_err(|_| {
        Report::new(ConfigError::PathMustNotContainNul)
            .attach(format!("path={file_path}"))
            .into()
    })
}

/// Worker-owned table-write request before the backend queue admits it.
///
/// The request keeps the file owner alive while it is deferred or queued on the
/// shared background-write lane. `TableFsStateMachine` materializes the backend
/// `Operation` only when this request is admitted to the IO queue.
pub struct WriteSubmission {
    key: BlockKey,
    file: Arc<SparseFile>,
    offset: usize,
    buf: DirectBuf,
    completion: Arc<Completion<CompletionResult<()>>>,
}

impl WriteSubmission {
    #[inline]
    fn prepare(
        key: BlockKey,
        file: Arc<SparseFile>,
        offset: usize,
        buf: DirectBuf,
    ) -> (Self, impl Future<Output = Result<()>> + Send) {
        let completion = Arc::new(Completion::new());
        let waiter = Arc::clone(&completion);
        let fio = WriteSubmission {
            key,
            file,
            offset,
            buf,
            completion,
        };
        (fio, async move {
            waiter.wait_result().await.map_err(|report| {
                Error::from_completion_report(
                    report,
                    format!("wait for table file background write: key={key:?}, offset={offset}"),
                )
            })
        })
    }

    #[inline]
    fn into_prepared(self) -> PreparedWriteSubmission {
        let WriteSubmission {
            key,
            file,
            offset,
            buf,
            completion,
        } = self;
        let operation = Operation::pwrite_owned(file.as_raw_fd(), offset, buf);
        PreparedWriteSubmission {
            key,
            _file: file,
            operation,
            completion,
        }
    }
}

pub(crate) struct PreparedWriteSubmission {
    key: BlockKey,
    _file: Arc<SparseFile>,
    operation: Operation,
    completion: Arc<Completion<CompletionResult<()>>>,
}

impl IOSubmission for PreparedWriteSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

pub(crate) enum TableFsSubmission {
    Write(PreparedWriteSubmission),
    Read(ReadSubmission),
}

impl IOSubmission for TableFsSubmission {
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
    key: BlockKey,
    file: Arc<SparseFile>,
    offset: usize,
    buf: DirectBuf,
    background_writes: &IOClient<BackgroundWriteRequest>,
) -> Result<()> {
    let (submission, result) = WriteSubmission::prepare(key, file, offset, buf);
    if let Err(err) = background_writes
        .send_async(BackgroundWriteRequest::Table(submission))
        .await
    {
        let BackgroundWriteRequest::Table(_submission) = err.into_inner() else {
            unreachable!("write_direct received unexpected background-write send error");
        };
        return Err(IoError::report_send("send background table write request").into());
    }
    result.await
}

pub(crate) struct TableFsStateMachine;

impl TableFsStateMachine {
    /// Creates one state machine for the shared table-filesystem IO worker.
    #[inline]
    pub fn new() -> TableFsStateMachine {
        TableFsStateMachine
    }

    #[inline]
    pub(crate) fn prepare_read_request(
        &mut self,
        req: ReadSubmission,
        max_new: usize,
        queue: &mut IOQueue<TableFsSubmission>,
    ) -> Option<ReadSubmission> {
        if max_new == 0 {
            return Some(req);
        }
        queue.push(TableFsSubmission::Read(req));
        None
    }

    #[inline]
    pub(crate) fn prepare_write_request(
        &mut self,
        req: WriteSubmission,
        max_new: usize,
        queue: &mut IOQueue<TableFsSubmission>,
    ) -> Option<WriteSubmission> {
        if max_new == 0 {
            return Some(req);
        }
        queue.push(TableFsSubmission::Write(req.into_prepared()));
        None
    }

    #[inline]
    pub(crate) fn on_submit(&mut self, sub: &TableFsSubmission) {
        match sub {
            TableFsSubmission::Write(sub) => {
                let _ = sub.key;
            }
            TableFsSubmission::Read(sub) => sub.record_running(),
        }
    }

    #[inline]
    pub(crate) fn on_complete(
        &mut self,
        sub: TableFsSubmission,
        res: StdIoResult<usize>,
    ) -> IOKind {
        match sub {
            TableFsSubmission::Write(mut sub) => {
                let expected_len = sub.operation.len();
                let buf = sub
                    .operation
                    .take_buf()
                    .expect("write operation must still own its direct buffer");
                debug_assert_eq!(expected_len, buf.capacity());
                match res {
                    Ok(len) => {
                        drop(buf);
                        let result = if len == expected_len {
                            Ok(())
                        } else {
                            Err(CompletionErrorKind::report_unexpected_eof(
                                len,
                                expected_len,
                                format!("complete table file write: key={:?}", sub.key),
                            ))
                        };
                        sub.completion.complete(result);
                    }
                    Err(err) => {
                        drop(buf);
                        sub.completion.complete(Err(CompletionErrorKind::report_io(
                            err,
                            format!("complete table file write: key={:?}", sub.key),
                        )));
                    }
                }
                IOKind::Write
            }
            TableFsSubmission::Read(sub) => sub.complete(res),
        }
    }
}

/// FileSyncer is a simple wrapper to provide functionality
/// of fsync() and fdatasync().
pub struct FileSyncer(RawFd);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileSyncKind {
    Fsync,
    Fdatasync,
}

impl FileSyncer {
    #[inline]
    pub fn fsync(&self) -> Result<()> {
        self.sync(FileSyncKind::Fsync)
    }

    #[inline]
    pub fn fdatasync(&self) -> Result<()> {
        self.sync(FileSyncKind::Fdatasync)
    }

    #[inline]
    fn sync(&self, kind: FileSyncKind) -> Result<()> {
        #[cfg(test)]
        {
            let op = tests::FileSyncOp::new(self.0, kind);
            if let Some(hook) = tests::current_file_sync_test_hook() {
                let mut override_res = None;
                hook.on_sync(op, &mut override_res);
                if let Some(res) = override_res {
                    return res;
                }
            }
        }

        // SAFETY: `self.0` is an owned live file descriptor for the lifetime of
        // this `FileSyncer`, and the libc sync calls do not retain borrowed
        // memory beyond the syscall.
        let ret = unsafe {
            match kind {
                FileSyncKind::Fsync => fsync(self.0),
                FileSyncKind::Fdatasync => fdatasync(self.0),
            }
        };
        if ret == 0 {
            return Ok(());
        }
        debug_assert!(ret == -1);
        Err(IoError::report(io::Error::last_os_error()).into())
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
    use crate::catalog::table::TableMetadata;
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::compression::BitPackable;
    use crate::file::fs::tests::{TestFileSystem, build_test_fs};
    use crate::file::table_file::TableFile;
    use crate::serde::{Deser, Ser};
    use crate::value::ValKind;
    use std::io::ErrorKind as IoErrorKind;
    use std::mem;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    #[inline]
    pub(crate) fn test_file_id(value: i32) -> FileID {
        FileID::new(u64::try_from(value).expect("test FileID must be non-negative"))
    }

    #[inline]
    pub(crate) fn test_block_id(value: i32) -> BlockID {
        BlockID::new(u64::try_from(value).expect("test BlockID must be non-negative"))
    }

    fn build_test_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx_pk",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ))
    }

    async fn committed_test_table_file() -> (TempDir, TestFileSystem, Arc<TableFile>) {
        let (temp_dir, fs) = build_test_fs();
        let mutable = fs
            .create_table_file(801, build_test_metadata(), false)
            .unwrap();
        let (table_file, old_root) = mutable.commit(1, false).await.unwrap();
        drop(old_root);
        (temp_dir, fs, table_file)
    }

    fn prepare_table_write_submission(
        table_file: Arc<TableFile>,
        block_id: BlockID,
    ) -> (WriteSubmission, impl Future<Output = Result<()>> + Send) {
        WriteSubmission::prepare(
            BlockKey::new(table_file.sparse_file().file_id(), block_id),
            Arc::clone(table_file.sparse_file()),
            usize::from(block_id) * STORAGE_SECTOR_SIZE,
            DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
        )
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct FileSyncOp {
        fd: RawFd,
        kind: FileSyncKind,
    }

    impl FileSyncOp {
        #[inline]
        pub(super) fn new(fd: RawFd, kind: FileSyncKind) -> Self {
            Self { fd, kind }
        }

        #[inline]
        pub(crate) fn fd(&self) -> RawFd {
            self.fd
        }

        #[inline]
        pub(crate) fn kind(&self) -> FileSyncKind {
            self.kind
        }
    }

    pub(crate) trait FileSyncTestHook: Send + Sync {
        fn on_sync(&self, _op: FileSyncOp, _override_res: &mut Option<Result<()>>) {}
    }

    type FileSyncHook = Arc<dyn FileSyncTestHook>;

    static FILE_SYNC_TEST_HOOK: Mutex<Option<FileSyncHook>> = Mutex::new(None);

    #[inline]
    pub(super) fn current_file_sync_test_hook() -> Option<FileSyncHook> {
        FILE_SYNC_TEST_HOOK.lock().unwrap().clone()
    }

    #[inline]
    pub(crate) fn set_file_sync_test_hook(hook: Option<FileSyncHook>) -> Option<FileSyncHook> {
        let mut guard = FILE_SYNC_TEST_HOOK.lock().unwrap();
        std::mem::replace(&mut *guard, hook)
    }

    #[test]
    fn test_block_id_accessors_and_conversions() {
        let block_id = BlockID::new(42);
        assert_eq!(block_id.as_u64(), 42);
        assert_eq!(block_id.as_usize(), 42);
        assert_eq!(BlockID::from(42u64), block_id);
        assert_eq!(BlockID::from(42u32), block_id);
        assert_eq!(BlockID::from(42usize), block_id);
        assert_eq!(u64::from(block_id), 42);
        assert_eq!(usize::from(block_id), 42);
    }

    #[test]
    fn test_block_id_bytes_roundtrip() {
        let block_id = BlockID::new(0xfedc_ba98_7654_3210);
        let bytes = block_id.to_le_bytes();
        assert_eq!(bytes, 0xfedc_ba98_7654_3210u64.to_le_bytes());
        assert_eq!(BlockID::from_le_bytes(bytes), block_id);
    }

    #[test]
    fn test_block_id_arithmetic() {
        let block_id = BlockID::new(10);
        assert_eq!(block_id + 5, BlockID::new(15));
        assert_eq!(block_id - 3, BlockID::new(7));

        let mut next = block_id;
        next += 7;
        assert_eq!(next, BlockID::new(17));
        next -= 5;
        assert_eq!(next, BlockID::new(12));
    }

    #[test]
    fn test_block_id_partial_eq_signed() {
        let block_id = BlockID::new(7);
        assert_eq!(block_id, 7i32);
        assert_eq!(7i32, block_id);
        assert_ne!(block_id, -1i32);
        assert_ne!(-1i32, block_id);
    }

    #[test]
    fn test_block_id_display() {
        assert_eq!(format!("{}", BlockID::new(99)), "99");
    }

    #[test]
    fn test_block_id_serde_roundtrip() {
        let block_id = BlockID::new(1234);
        assert_eq!(block_id.ser_len(), mem::size_of::<u64>());

        let mut out = vec![0; block_id.ser_len()];
        let end = block_id.ser(&mut out[..], 0);
        assert_eq!(end, out.len());

        let (end, deser) = BlockID::deser(&out[..], 0).unwrap();
        assert_eq!(end, out.len());
        assert_eq!(deser, block_id);
    }

    #[test]
    fn test_block_id_bit_packable_contract() {
        let min = BlockID::new(10);
        let value = BlockID::new(42);
        assert_eq!(BlockID::ZERO, BlockID::new(0));
        assert_eq!(value.sub_to_u64(min), 32);
        assert_eq!(value.sub_to_u32(min), 32);
        assert_eq!(value.sub_to_u16(min), 32);
        assert_eq!(value.sub_to_u8(min), 32);
        assert_eq!(min.add_from_u32(5), BlockID::new(15));
        assert_eq!(min.add_from_u16(6), BlockID::new(16));
        assert_eq!(min.add_from_u8(7), BlockID::new(17));

        let wrap_min = BlockID::new(u64::MAX - 2);
        let wrap_value = BlockID::new(1);
        assert_eq!(wrap_value.sub_to_u64(wrap_min), 4);
        assert_eq!(wrap_min.add_from_u32(5), BlockID::new(2));
    }

    #[test]
    fn test_file_id_accessors_and_conversions() {
        let file_id = FileID::new(42);
        assert_eq!(file_id.as_u64(), 42);
        assert_eq!(file_id.as_usize(), 42);
        assert_eq!(FileID::from(42u64), file_id);
        assert_eq!(FileID::from(42u32), file_id);
        assert_eq!(FileID::from(42usize), file_id);
        assert_eq!(u64::from(file_id), 42);
        assert_eq!(usize::from(file_id), 42);
    }

    #[test]
    fn test_file_id_bytes_roundtrip() {
        let file_id = FileID::new(0x0123_4567_89ab_cdef);
        let bytes = file_id.to_le_bytes();
        assert_eq!(bytes, 0x0123_4567_89ab_cdefu64.to_le_bytes());
        assert_eq!(FileID::from_le_bytes(bytes), file_id);
    }

    #[test]
    fn test_file_id_partial_eq_signed_and_unsigned() {
        let file_id = FileID::new(7);
        assert_eq!(file_id, 7u64);
        assert_eq!(7u64, file_id);
        assert_eq!(file_id, 7i32);
        assert_eq!(7i32, file_id);
        assert_ne!(file_id, -1i32);
        assert_ne!(-1i32, file_id);
    }

    #[test]
    fn test_file_id_display() {
        assert_eq!(format!("{}", FileID::new(99)), "99");
    }

    #[test]
    fn test_file_id_serde_roundtrip() {
        let file_id = FileID::new(1234);
        assert_eq!(file_id.ser_len(), mem::size_of::<u64>());

        let mut out = vec![0; file_id.ser_len()];
        let end = file_id.ser(&mut out[..], 0);
        assert_eq!(end, out.len());

        let (end, deser) = FileID::deser(&out[..], 0).unwrap();
        assert_eq!(end, out.len());
        assert_eq!(deser, file_id);
    }

    #[test]
    fn test_file_id_bit_packable_contract() {
        let min = FileID::new(10);
        let value = FileID::new(42);
        assert_eq!(FileID::ZERO, FileID::new(0));
        assert_eq!(value.sub_to_u64(min), 32);
        assert_eq!(value.sub_to_u32(min), 32);
        assert_eq!(value.sub_to_u16(min), 32);
        assert_eq!(value.sub_to_u8(min), 32);
        assert_eq!(min.add_from_u32(5), FileID::new(15));
        assert_eq!(min.add_from_u16(6), FileID::new(16));
        assert_eq!(min.add_from_u8(7), FileID::new(17));

        let wrap_min = FileID::new(u64::MAX - 2);
        let wrap_value = FileID::new(1);
        assert_eq!(wrap_value.sub_to_u64(wrap_min), 4);
        assert_eq!(wrap_min.add_from_u32(5), FileID::new(2));
    }

    #[test]
    fn test_test_file_id_accepts_non_negative_values() {
        assert_eq!(test_file_id(0), FileID::new(0));
        assert_eq!(test_file_id(17), FileID::new(17));
    }

    #[test]
    #[should_panic(expected = "test FileID must be non-negative")]
    fn test_test_file_id_panics_on_negative_values() {
        let _ = test_file_id(-1);
    }

    #[test]
    fn test_test_block_id_accepts_non_negative_values() {
        assert_eq!(test_block_id(0), BlockID::new(0));
        assert_eq!(test_block_id(17), BlockID::new(17));
    }

    #[test]
    #[should_panic(expected = "test BlockID must be non-negative")]
    fn test_test_block_id_panics_on_negative_values() {
        let _ = test_block_id(-1);
    }

    #[test]
    fn test_sparse_file_open_and_create() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("sparsefile1.bin");
        let file_path = file_path.to_string_lossy().into_owned();
        let err = match SparseFile::open(&file_path, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected open to fail before create"),
            Err(err) => err,
        };
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::NotFound)
        );
        assert!(format!("{err:?}").contains("op=file open"));
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024, UNTRACKED_FILE_ID).unwrap();
        drop(file);
        let file = SparseFile::open(&file_path, UNTRACKED_FILE_ID).unwrap();
        drop(file);
    }

    #[test]
    fn test_sparse_file_create_missing_parent_maps_create_failed() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir
            .path()
            .join("missing")
            .join("nested")
            .join("create.bin");
        let file_path = file_path.to_string_lossy().into_owned();
        let err = match SparseFile::create_or_trunc(&file_path, 4096, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected create to fail for missing parent"),
            Err(err) => err,
        };
        assert_eq!(
            err.report()
                .downcast_ref::<IoError>()
                .copied()
                .map(IoError::kind),
            Some(IoErrorKind::NotFound)
        );
        assert!(format!("{err:?}").contains("op=file create"));
    }

    #[test]
    fn test_sparse_file_invalid_path_maps_config_error() {
        let invalid_path = "bad\0path";
        let err = match SparseFile::open(invalid_path, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected invalid path open failure"),
            Err(err) => err,
        };
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report()
                .downcast_ref::<crate::error::ConfigError>()
                .copied(),
            Some(crate::error::ConfigError::PathMustNotContainNul)
        );
        let err = match SparseFile::create_or_trunc(invalid_path, 4096, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected invalid path create failure"),
            Err(err) => err,
        };
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report()
                .downcast_ref::<crate::error::ConfigError>()
                .copied(),
            Some(crate::error::ConfigError::PathMustNotContainNul)
        );
    }

    #[test]
    fn test_sparse_file_alloc_exhaustion_maps_capacity_exceeded() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("capacity.bin");
        let file_path = file_path.to_string_lossy().into_owned();
        let file = SparseFile::create_or_trunc(&file_path, STORAGE_SECTOR_SIZE, UNTRACKED_FILE_ID)
            .unwrap();
        assert_eq!(
            file.alloc(STORAGE_SECTOR_SIZE).unwrap(),
            (0, STORAGE_SECTOR_SIZE)
        );
        assert!(file.alloc(STORAGE_SECTOR_SIZE).as_ref().is_err_and(
            |err| err.resource_error() == Some(ResourceError::StorageFileCapacityExceeded)
        ));
    }

    #[test]
    fn test_table_fs_write_completion_succeeds_on_full_write() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) =
                prepare_table_write_submission(Arc::clone(&table_file), BlockID::new(1));
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_write_request(submission, 1, &mut queue)
                    .is_none()
            );

            let Some(TableFsSubmission::Write(submission)) = queue.pop_front() else {
                panic!("expected one prepared table write submission");
            };
            let expected_len = submission.operation.len();
            let kind =
                state_machine.on_complete(TableFsSubmission::Write(submission), Ok(expected_len));

            assert_eq!(kind, IOKind::Write);
            assert!(waiter.await.is_ok());
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_fs_write_completion_rejects_short_write() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) =
                prepare_table_write_submission(Arc::clone(&table_file), BlockID::new(1));
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_write_request(submission, 1, &mut queue)
                    .is_none()
            );

            let Some(TableFsSubmission::Write(submission)) = queue.pop_front() else {
                panic!("expected one prepared table write submission");
            };
            let expected_len = submission.operation.len();
            assert!(expected_len > 0);

            let kind = state_machine
                .on_complete(TableFsSubmission::Write(submission), Ok(expected_len - 1));

            assert_eq!(kind, IOKind::Write);
            assert!(waiter.await.as_ref().is_err_and(|err| {
                err.completion_error() == Some(CompletionErrorKind::Io(IoErrorKind::UnexpectedEof))
                    && format!("{err:?}").contains("propagate from other threads")
                    && format!("{err:?}").contains("wait for table file background write")
            }));
            drop(table_file);
            drop(fs);
        });
    }
}
