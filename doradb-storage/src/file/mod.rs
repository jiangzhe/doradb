pub mod block_integrity;
pub mod cow_file;
pub mod meta_block;
pub mod multi_table_file;
pub mod super_block;
pub mod table_file;
pub mod table_fs;

#[cfg(test)]
pub(crate) use self::table_fs::tests::{build_test_fs, build_test_fs_in};
#[cfg(test)]
pub(crate) use self::tests::{
    FileSyncOp, FileSyncTestHook, set_file_sync_test_hook, test_block_id,
};

use crate::buffer::{BlockKey, ReadSubmission};
use crate::compression::BitPackable;
use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::{
    AIOClient, AIOError, AIOKind, AIOResult, Completion, IOQueue, IOStateMachine, IOSubmission,
    Operation, STORAGE_SECTOR_SIZE, align_to_sector_size,
};
use crate::serde::{Deser, Ser, Serde};
use crate::{error::Error, error::Result};
use bytemuck::{Pod, Zeroable};
use libc::{
    O_CREAT, O_DIRECT, O_EXCL, O_RDWR, O_TRUNC, close, fdatasync, fstat, fsync, ftruncate, open,
    stat,
};
use parking_lot::RawMutex;
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use scopeguard::defer;
use std::ffi::CString;
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::os::unix::io::{AsRawFd, RawFd};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
        // SAFETY: libc calls (`open`, `ftruncate`, `close`) are issued with a
        // validated C string and checked return codes before constructing
        // `SparseFile`.
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
        // SAFETY: `open` is called with a validated C string, and the returned
        // fd is checked before it is used or wrapped.
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
    key: BlockKey,
    operation: Operation,
    completion: Arc<Completion<Result<()>>>,
}

impl WriteSubmission {
    #[inline]
    fn prepare(
        key: BlockKey,
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
    type Key = BlockKey;

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
    type Key = BlockKey;

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
    key: BlockKey,
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

pub(crate) struct TableFsStateMachine;

impl TableFsStateMachine {
    /// Creates one state machine for the shared table-filesystem IO worker.
    #[inline]
    pub fn new() -> TableFsStateMachine {
        TableFsStateMachine
    }
}

impl IOStateMachine for TableFsStateMachine {
    type Request = TableFsRequest;
    type Key = BlockKey;
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
    fn on_submit(&mut self, sub: &TableFsSubmission) {
        if let TableFsSubmission::Read(sub) = sub {
            sub.record_running();
        }
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
    fn end_loop(self) {
        // do nothing
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
    use crate::compression::BitPackable;
    use crate::serde::{Deser, Ser};
    use std::mem;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    #[inline]
    pub(crate) fn test_block_id(value: i32) -> BlockID {
        BlockID::new(u64::try_from(value).expect("test BlockID must be non-negative"))
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
        let res = SparseFile::open(&file_path);
        assert!(res.is_err());
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024).unwrap();
        drop(file);
        let file = SparseFile::open(&file_path).unwrap();
        drop(file);
    }
}
