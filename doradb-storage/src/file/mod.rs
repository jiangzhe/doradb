pub(crate) mod block_integrity;
pub(crate) mod cow_file;
pub(crate) mod fs;
pub(crate) mod meta_block;
pub(crate) mod multi_table_file;
pub(crate) mod super_block;
pub(crate) mod table_file;

use self::fs::BackgroundWriteRequest;
#[cfg(test)]
pub(crate) use self::fs::tests::{build_test_fs, build_test_fs_in};
#[cfg(test)]
pub(crate) use self::tests::{test_block_id, test_file_id};
use crate::id::{BlockID, FileID};

use crate::buffer::{ReadSubmission, ReadonlyWriteLease};
use crate::error::{
    CompletionErrorKind, CompletionResult, IoError, IoResult, ResourceError, ResourceResult,
};
use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::{
    Completion, IOClient, IOKind, IOQueue, IOSubmission, Operation, STORAGE_SECTOR_SIZE,
    StdIoResult, align_to_sector_size,
};
use error_stack::Report;
use libc::{O_CREAT, O_DIRECT, O_EXCL, O_RDWR, O_TRUNC, close, fstat, ftruncate, open, stat};
use parking_lot::RawMutex;
use parking_lot::lock_api::RawMutex as RawMutexAPI;
use scopeguard::defer;
use std::ffi::{CStr, CString};
use std::io::{Error as StdIoError, ErrorKind as IoErrorKind};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Sentinel persisted-file identity used by files that never participate in
/// persisted-block cache identity or shared-storage worker routing.
pub(crate) const UNTRACKED_FILE_ID: FileID = FileID::MAX;

/// Reserved persisted-file identity of the shared index-pool swap file.
pub(crate) const INDEX_POOL_SWAP_FILE_ID: FileID = FileID::new(u64::MAX - 3);

/// Reserved persisted-file identity of the shared mem-pool swap file.
pub(crate) const MEM_POOL_SWAP_FILE_ID: FileID = FileID::new(u64::MAX - 2);

/// Reserved persisted-file identity of `catalog.mtb`.
pub(crate) const CATALOG_MTB_FILE_ID: FileID = FileID::new(u64::MAX - 1);

/// Physical persisted-block identity for cache lookup and shared-storage IO.
///
/// This intentionally excludes root version so unchanged physical blocks keep
/// the same identity across root swaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockKey {
    /// Persisted file identity owning the block.
    pub(crate) file_id: FileID,
    /// Physical page/block id in the backing file.
    pub(crate) block_id: BlockID,
}

impl BlockKey {
    /// Builds a key from file id and physical block id.
    #[inline]
    pub(crate) fn new(file_id: FileID, block_id: BlockID) -> Self {
        BlockKey { file_id, block_id }
    }
}

/// SparseFile is file with metadata describing empty blocks
/// instead of writing them.
/// The logical size of sparse file can be very large, but the
/// real allocated blocks can be only a few.
pub(crate) struct SparseFile {
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
    pub(crate) fn create_or_trunc(
        file_path: impl AsRef<str>,
        max_size: usize,
        file_id: FileID,
    ) -> IoResult<SparseFile> {
        let c_string = c_string_from_path(file_path.as_ref())?;
        let fd = create_sparse_file(&c_string, O_CREAT | O_RDWR | O_TRUNC | O_DIRECT, max_size)?;
        Ok(SparseFile::new(fd, 0, max_size, file_id))
    }

    /// Create a sparse file and fail if file already exists.
    /// Note that space is allocated only when data is written to this file.
    #[inline]
    pub(crate) fn create_or_fail(
        file_path: impl AsRef<str>,
        max_size: usize,
        file_id: FileID,
    ) -> IoResult<SparseFile> {
        let c_string = c_string_from_path(file_path.as_ref())?;
        let fd = create_sparse_file(&c_string, O_CREAT | O_RDWR | O_EXCL | O_DIRECT, max_size)?;
        Ok(SparseFile::new(fd, 0, max_size, file_id))
    }

    /// Open an existing sparse file with given maximum length.
    #[inline]
    pub(crate) fn open(file_path: impl AsRef<str>, file_id: FileID) -> IoResult<SparseFile> {
        let c_string = c_string_from_path(file_path.as_ref())?;
        let fd = open_sparse_file(&c_string)?;
        let (logical_size, _) = match sparse_file_size(fd) {
            Ok((l, a)) => (l, a),
            Err(err) => {
                // SAFETY: the typed open supplier returned one live owned fd,
                // and this error path has not transferred it to `SparseFile`.
                let _ = unsafe { close(fd) };
                return Err(err);
            }
        };
        Ok(SparseFile::new(fd, 0, logical_size, file_id))
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
    pub(crate) fn file_id(&self) -> FileID {
        self.file_id
    }

    /// Allocate enough space for data of given length to persist
    /// at end of the file.
    #[inline]
    pub(crate) fn alloc(&self, len: usize) -> ResourceResult<(usize, usize)> {
        let size = align_to_sector_size(len);
        loop {
            let offset = self.offset.load(Ordering::Relaxed);
            let new_offset = offset + size;
            let max_len = self.max_len.load(Ordering::Relaxed);
            if new_offset > max_len {
                return Err(Report::new(ResourceError::StorageFileCapacityExceeded).attach(
                    format!(
                        "sparse file allocation: requested_len={len}, aligned_len={size}, current_offset={offset}, new_offset={new_offset}, max_len={max_len}"
                    ),
                ));
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

    /// Grow the file to given size.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    #[cfg_attr(
        all(feature = "iouring", test),
        expect(dead_code, reason = "pending dead-code audit")
    )]
    pub(crate) fn extend_to(&self, max_len: usize) -> IoResult<()> {
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
        let err = StdIoError::last_os_error();
        Err(Report::new(IoError::from(err.kind())).attach(format!("op=file_resize, {err}")))
    }

    /// Get the logical size and allocated size of this file.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    #[cfg_attr(
        all(feature = "iouring", test),
        expect(dead_code, reason = "pending dead-code audit")
    )]
    pub(crate) fn size(&self) -> IoResult<(usize, usize)> {
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

/// Worker-owned table-write request before the backend queue admits it.
///
/// The request keeps the file owner alive while it is deferred or queued on the
/// shared background-write lane. `TableFsStateMachine` materializes the backend
/// `Operation` only when this request is admitted to the IO queue.
pub(crate) struct WriteSubmission {
    key: BlockKey,
    file: Arc<SparseFile>,
    offset: usize,
    buf: DirectBuf,
    write_lease: Option<ReadonlyWriteLease>,
    completion: Arc<Completion<()>>,
}

impl WriteSubmission {
    #[inline]
    fn prepare(
        key: BlockKey,
        file: Arc<SparseFile>,
        offset: usize,
        buf: DirectBuf,
        write_lease: Option<ReadonlyWriteLease>,
    ) -> (Self, Arc<Completion<()>>) {
        let completion = Arc::new(Completion::new());
        let fio = WriteSubmission {
            key,
            file,
            offset,
            buf,
            write_lease,
            completion: Arc::clone(&completion),
        };
        (fio, completion)
    }

    #[inline]
    fn into_prepared(self) -> PreparedWriteSubmission {
        let WriteSubmission {
            key,
            file,
            offset,
            buf,
            write_lease,
            completion,
        } = self;
        let operation = Operation::pwrite_owned(file.as_raw_fd(), offset, buf);
        PreparedWriteSubmission {
            key,
            _file: file,
            operation,
            write_lease,
            completion,
        }
    }

    /// Fail a table write before backend submission accepted it.
    #[inline]
    pub(crate) fn fail(mut self, err: &Report<IoError>) {
        drop(self.buf);
        drop(self.write_lease.take());
        let report =
            IoError::report_backend(err, format!("submit table file write: key={:?}", self.key));
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table write completion transport",
        )));
    }
}

/// Backend-prepared write submission retained until completion.
pub(crate) struct PreparedWriteSubmission {
    key: BlockKey,
    _file: Arc<SparseFile>,
    operation: Operation,
    write_lease: Option<ReadonlyWriteLease>,
    completion: Arc<Completion<()>>,
}

impl PreparedWriteSubmission {
    #[inline]
    fn release_write_lease(&mut self) {
        drop(self.write_lease.take());
    }

    /// Fail a prepared table write before backend submission accepted it.
    #[inline]
    fn fail_backend_not_accepted(mut self, err: &Report<IoError>) {
        drop(
            self.operation
                .take_buf()
                .expect("prepared table write must still own its direct buffer"),
        );
        self.release_write_lease();
        let report =
            IoError::report_backend(err, format!("submit table file write: key={:?}", self.key));
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table write completion transport",
        )));
    }

    /// Fail an accepted table write while retaining kernel-facing memory.
    #[inline]
    fn fail_backend_submitted(&mut self, err: &Report<IoError>) {
        self.release_write_lease();
        let report = IoError::report_backend(
            err,
            format!("complete submitted table file write: key={:?}", self.key),
        );
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table write completion transport",
        )));
    }
}

impl IOSubmission for PreparedWriteSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

/// Worker-owned table-file sync request before the backend queue admits it.
///
/// The request keeps the file owner alive while it is deferred or queued on the
/// shared background-write lane. `TableFsStateMachine` materializes the backend
/// `Operation::fsync` only when this request is admitted to the IO queue.
pub(crate) struct SyncSubmission {
    file: Arc<SparseFile>,
    completion: Arc<Completion<()>>,
}

impl SyncSubmission {
    #[inline]
    fn prepare_fsync(file: Arc<SparseFile>) -> (Self, Arc<Completion<()>>) {
        let completion = Arc::new(Completion::new());
        let fio = SyncSubmission {
            file,
            completion: Arc::clone(&completion),
        };
        (fio, completion)
    }

    #[inline]
    fn into_prepared(self) -> PreparedSyncSubmission {
        let SyncSubmission { file, completion } = self;
        let operation = Operation::fsync(file.as_raw_fd());
        PreparedSyncSubmission {
            _file: file,
            operation,
            completion,
        }
    }

    /// Fail a table sync before backend submission accepted it.
    #[inline]
    pub(crate) fn fail(self, err: &Report<IoError>) {
        let report = IoError::report_backend(err, "submit table file fsync");
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table fsync completion transport",
        )));
    }
}

/// Backend-prepared sync submission retained until completion.
pub(crate) struct PreparedSyncSubmission {
    _file: Arc<SparseFile>,
    operation: Operation,
    completion: Arc<Completion<()>>,
}

impl PreparedSyncSubmission {
    /// Fail a prepared table sync before backend submission accepted it.
    #[inline]
    fn fail_backend_not_accepted(self, err: &Report<IoError>) {
        let report = IoError::report_backend(err, "submit table file fsync");
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table fsync completion transport",
        )));
    }

    /// Fail an accepted table sync.
    #[inline]
    fn fail_backend_submitted(&mut self, err: &Report<IoError>) {
        let report = IoError::report_backend(err, "complete submitted table file fsync");
        self.completion.complete(Err(CompletionErrorKind::from_io(
            report,
            "table fsync completion transport",
        )));
    }
}

impl IOSubmission for PreparedSyncSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        &mut self.operation
    }
}

/// Submission variants handled by the shared table-file IO worker.
pub(crate) enum TableFsSubmission {
    Write(PreparedWriteSubmission),
    Sync(PreparedSyncSubmission),
    Read(ReadSubmission),
}

impl IOSubmission for TableFsSubmission {
    #[inline]
    fn operation(&mut self) -> &mut Operation {
        match self {
            TableFsSubmission::Write(sub) => sub.operation(),
            TableFsSubmission::Sync(sub) => sub.operation(),
            TableFsSubmission::Read(sub) => sub.operation(),
        }
    }
}

/// State machine for preparing and completing table-file IO submissions.
pub(crate) struct TableFsStateMachine;

impl TableFsStateMachine {
    /// Creates one state machine for the shared table-filesystem IO worker.
    #[inline]
    pub(crate) fn new() -> TableFsStateMachine {
        TableFsStateMachine
    }

    /// Queue or defer one table-read request depending on available backend capacity.
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

    /// Queue or defer one table-write request depending on available backend capacity.
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

    /// Queue or defer one table-sync request depending on available backend capacity.
    #[inline]
    pub(crate) fn prepare_sync_request(
        &mut self,
        req: SyncSubmission,
        max_new: usize,
        queue: &mut IOQueue<TableFsSubmission>,
    ) -> Option<SyncSubmission> {
        if max_new == 0 {
            return Some(req);
        }
        queue.push(TableFsSubmission::Sync(req.into_prepared()));
        None
    }

    /// Records side effects when one table-file submission is accepted by the backend.
    #[inline]
    pub(crate) fn on_submit(&mut self, sub: &TableFsSubmission) {
        match sub {
            TableFsSubmission::Write(sub) => {
                let _ = sub.key;
            }
            TableFsSubmission::Sync(_) => {}
            TableFsSubmission::Read(sub) => sub.record_running(),
        }
    }

    /// Fail one prepared submission before backend submission accepted it.
    #[inline]
    pub(crate) fn fail_submission_with_backend_error(
        &mut self,
        sub: TableFsSubmission,
        err: &Report<IoError>,
    ) -> IOKind {
        match sub {
            TableFsSubmission::Write(sub) => {
                sub.fail_backend_not_accepted(err);
                IOKind::Write
            }
            TableFsSubmission::Sync(sub) => {
                sub.fail_backend_not_accepted(err);
                IOKind::Fsync
            }
            TableFsSubmission::Read(sub) => {
                let report = IoError::report_backend(err, "submit readonly table read");
                sub.fail(CompletionErrorKind::from_io(
                    report,
                    "readonly table read completion transport",
                ));
                IOKind::Read
            }
        }
    }

    /// Fail one already-submitted table-file operation without releasing
    /// kernel-facing memory.
    #[inline]
    pub(crate) fn fail_submitted_with_backend_error(
        &mut self,
        sub: &mut TableFsSubmission,
        err: &Report<IoError>,
    ) -> IOKind {
        match sub {
            TableFsSubmission::Write(sub) => {
                sub.fail_backend_submitted(err);
                IOKind::Write
            }
            TableFsSubmission::Sync(sub) => {
                sub.fail_backend_submitted(err);
                IOKind::Fsync
            }
            TableFsSubmission::Read(sub) => {
                let report = IoError::report_backend(err, "complete submitted readonly table read");
                sub.fail_backend_submitted(CompletionErrorKind::from_io(
                    report,
                    "readonly table read completion transport",
                ));
                IOKind::Read
            }
        }
    }

    /// Completes one table-file submission and returns its IO kind.
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
                        sub.release_write_lease();
                        let result = if len == expected_len {
                            Ok(())
                        } else {
                            Err(CompletionErrorKind::from_io(
                                Report::new(IoError::from(IoErrorKind::UnexpectedEof)).attach(
                                    format!(
                                        "unexpected eof: actual_bytes={len}, expected_bytes={expected_len}"
                                    ),
                                ),
                                format!("complete table file write: key={:?}", sub.key),
                            ))
                        };
                        sub.completion.complete(result);
                    }
                    Err(err) => {
                        drop(buf);
                        sub.release_write_lease();
                        sub.completion.complete(Err(CompletionErrorKind::from_io(
                            Report::new(IoError::from(err.kind())).attach(format!("{err}")),
                            format!("complete table file write: key={:?}", sub.key),
                        )));
                    }
                }
                IOKind::Write
            }
            TableFsSubmission::Sync(sub) => {
                match res {
                    Ok(0) => sub.completion.complete(Ok(())),
                    Ok(result) => sub.completion.complete(Err(CompletionErrorKind::from_io(
                        Report::new(IoError::from(IoErrorKind::Other)).attach(format!(
                            "unexpected io completion result: actual_result={result}, expected_result=0"
                        )),
                        "complete table file fsync",
                    ))),
                    Err(err) => sub.completion.complete(Err(CompletionErrorKind::from_io(
                        Report::new(IoError::from(err.kind())).attach(format!("{err}")),
                        "complete table file fsync",
                    ))),
                }
                IOKind::Fsync
            }
            TableFsSubmission::Read(sub) => sub.complete(res),
        }
    }
}

/// Fixed size buffer free list hold a given number of
/// buffer pages.
/// It's used to reuse pages in heavy IO environment.
#[derive(Clone)]
pub(crate) struct FixedSizeBufferFreeList(Arc<FreeList<DirectBuf>>);

impl FixedSizeBufferFreeList {
    /// Create a new buffer free list with given number of
    /// pre-allocated buffer pages.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    #[cfg_attr(
        all(feature = "iouring", test),
        expect(dead_code, reason = "pending dead-code audit")
    )]
    pub(crate) fn new(page_size: usize, init_pages: usize, max_pages: usize) -> Self {
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
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    #[cfg_attr(test, expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn recycle(&self, mut buf: DirectBuf) {
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

/// Return the logical size and allocated size for one sparse-file descriptor.
#[inline]
pub(crate) fn sparse_file_size(fd: RawFd) -> IoResult<(usize, usize)> {
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
        let err = StdIoError::last_os_error();
        Err(Report::new(IoError::from(err.kind())).attach(format!("op=file_stat, {err}")))
    }
}

/// Write one direct buffer via async direct IO.
///
/// `key` identifies the persisted block targeted by this write for higher-layer
/// invalidation and inflight bookkeeping.
#[inline]
pub(crate) async fn write_direct(
    key: BlockKey,
    file: Arc<SparseFile>,
    offset: usize,
    buf: DirectBuf,
    background_writes: &IOClient<BackgroundWriteRequest>,
) -> CompletionResult<()> {
    write_direct_with_lease(key, file, offset, buf, background_writes, None).await
}

/// Write one direct buffer while carrying an optional write barrier lease in
/// the backend-owned submission.
#[inline]
pub(crate) async fn write_direct_with_lease(
    key: BlockKey,
    file: Arc<SparseFile>,
    offset: usize,
    buf: DirectBuf,
    background_writes: &IOClient<BackgroundWriteRequest>,
    write_lease: Option<ReadonlyWriteLease>,
) -> CompletionResult<()> {
    let (submission, completion) = WriteSubmission::prepare(key, file, offset, buf, write_lease);
    if let Err(err) = background_writes
        .send_async(BackgroundWriteRequest::Table(submission))
        .await
    {
        let BackgroundWriteRequest::Table(_submission) = err.into_inner() else {
            unreachable!("write_direct received unexpected background-write send error");
        };
        return Err(CompletionErrorKind::from_send(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("background table write request channel closed"),
            "send background table write request",
        ));
    }
    completion.wait_result().await.map_err(|report| {
        report.attach(format!(
            "wait for table file background write: key={key:?}, offset={offset}"
        ))
    })
}

/// Flush one sparse file with a backend-submitted fsync request.
#[inline]
pub(crate) async fn fsync_direct(
    file: Arc<SparseFile>,
    background_writes: &IOClient<BackgroundWriteRequest>,
) -> CompletionResult<()> {
    let file_id = file.file_id();
    let (submission, completion) = SyncSubmission::prepare_fsync(file);
    if let Err(err) = background_writes
        .send_async(BackgroundWriteRequest::TableSync(submission))
        .await
    {
        let BackgroundWriteRequest::TableSync(_submission) = err.into_inner() else {
            unreachable!("fsync_direct received unexpected background-write send error");
        };
        return Err(CompletionErrorKind::from_send(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("background table fsync request channel closed"),
            "send background table fsync request",
        ));
    }
    completion.wait_result().await.map_err(|report| {
        report.attach(format!(
            "wait for table file background fsync: file_id={file_id}"
        ))
    })
}

/// Create and size one sparse-file descriptor from an already validated path.
#[inline]
fn create_sparse_file(file_path: &CStr, flags: i32, max_size: usize) -> IoResult<RawFd> {
    // SAFETY: `file_path` is a valid NUL-terminated C string, return codes are
    // checked, and a descriptor is closed locally if sizing fails.
    unsafe {
        let fd = open(file_path.as_ptr(), flags, 0o644);
        if fd < 0 {
            let err = StdIoError::last_os_error();
            return Err(
                Report::new(IoError::from(err.kind())).attach(format!("op=file_create, {err}"))
            );
        }
        if ftruncate(fd, max_size as i64) < 0 {
            let err = StdIoError::last_os_error();
            let _ = close(fd);
            return Err(
                Report::new(IoError::from(err.kind())).attach(format!("op=file_resize, {err}"))
            );
        }
        Ok(fd)
    }
}

/// Open one sparse-file descriptor from an already validated path.
#[inline]
fn open_sparse_file(file_path: &CStr) -> IoResult<RawFd> {
    // SAFETY: `file_path` is a valid NUL-terminated C string and the returned
    // descriptor is published only after the return code is checked.
    let fd = unsafe { open(file_path.as_ptr(), O_RDWR | O_DIRECT, 0o644) };
    if fd < 0 {
        let err = StdIoError::last_os_error();
        return Err(Report::new(IoError::from(err.kind())).attach(format!("op=file_open, {err}")));
    }
    Ok(fd)
}

#[inline]
fn c_string_from_path(file_path: &str) -> IoResult<CString> {
    CString::new(file_path).map_err(|err| {
        Report::new(err)
            .change_context(IoError::from(IoErrorKind::InvalidFilename))
            .attach(format!("path={file_path}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table::TableMetadata;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, USER_TABLE_ID_START,
    };
    use crate::compression::BitPackable;
    use crate::error::Error;
    use crate::file::fs::tests::{TestFileSystem, build_test_fs};
    use crate::file::table_file::TableFile;
    use crate::id::TrxID;
    use crate::io::{
        IOBackendErrorPhase, IOBackendFailure, IOBackendOperationKind, IOBackendQueueState,
        attach_backend_operation_kind,
    };
    use crate::serde::{Deser, Ser};
    use crate::value::ValKind;
    use std::io::{Error as StdIoError, ErrorKind as IoErrorKind};
    use std::mem;
    use std::sync::Arc;
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
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .expect("valid table metadata"),
        )
    }

    async fn committed_test_table_file() -> (TempDir, TestFileSystem, Arc<TableFile>) {
        let (temp_dir, fs) = build_test_fs();
        let mutable = fs
            .create_table_file(USER_TABLE_ID_START + 801, build_test_metadata(), false)
            .unwrap();
        let (table_file, old_root) = mutable.commit(TrxID::new(1), false).await.unwrap();
        drop(old_root);
        (temp_dir, fs, table_file)
    }

    fn prepare_table_write_submission(
        table_file: Arc<TableFile>,
        block_id: BlockID,
    ) -> (WriteSubmission, Arc<Completion<()>>) {
        WriteSubmission::prepare(
            BlockKey::new(table_file.sparse_file().file_id(), block_id),
            Arc::clone(table_file.sparse_file()),
            usize::from(block_id) * STORAGE_SECTOR_SIZE,
            DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
            None,
        )
    }

    fn prepare_table_sync_submission(
        table_file: &Arc<TableFile>,
    ) -> (SyncSubmission, Arc<Completion<()>>) {
        SyncSubmission::prepare_fsync(Arc::clone(table_file.sparse_file()))
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
        assert_eq!(file_id.as_usize(), 42);
        assert_eq!(FileID::from(42u64), file_id);
        assert_eq!(FileID::from(42u32), file_id);
        assert_eq!(FileID::from(42usize), file_id);
        assert_eq!(u64::from(file_id), 42);
        assert_eq!(usize::from(file_id), 42);
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
        assert_eq!(err.current_context().kind(), IoErrorKind::NotFound);
        assert!(format!("{err:?}").contains("op=file_open"));
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
        assert_eq!(err.current_context().kind(), IoErrorKind::NotFound);
        assert!(format!("{err:?}").contains("op=file_create"));
    }

    #[test]
    fn test_sparse_file_invalid_path_maps_invalid_filename_io_error() {
        let invalid_path = "bad\0path";
        let err = match SparseFile::open(invalid_path, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected invalid path open failure"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), IoErrorKind::InvalidFilename);
        assert!(err.downcast_ref::<std::ffi::NulError>().is_some());
        assert!(format!("{err:?}").contains("path=bad\0path"));

        let err = match SparseFile::create_or_trunc(invalid_path, 4096, UNTRACKED_FILE_ID) {
            Ok(_) => panic!("expected invalid path create failure"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), IoErrorKind::InvalidFilename);
        assert!(err.downcast_ref::<std::ffi::NulError>().is_some());
        assert!(format!("{err:?}").contains("path=bad\0path"));
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
        assert!(file.alloc(STORAGE_SECTOR_SIZE).as_ref().is_err_and(|err| {
            *err.current_context() == ResourceError::StorageFileCapacityExceeded
        }));
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
            assert!(waiter.wait_result().await.is_ok());
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
            let wait_result = waiter.wait_result().await.map_err(|report| {
                Error::from_completion_report(report, "wait for table file background write")
            });
            assert!(wait_result.as_ref().is_err_and(|err| {
                err.report().downcast_ref::<CompletionErrorKind>().copied()
                    == Some(CompletionErrorKind::Io(IoErrorKind::UnexpectedEof))
                    && format!("{err:?}").contains("propagate from other threads")
                    && format!("{err:?}").contains("wait for table file background write")
            }));
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_fs_sync_submission_prepares_fsync_and_retains_file() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let sparse_file = Arc::clone(table_file.sparse_file());
            let fd = sparse_file.as_raw_fd();
            let initial_count = Arc::strong_count(&sparse_file);
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) = prepare_table_sync_submission(&table_file);
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_sync_request(submission, 1, &mut queue)
                    .is_none()
            );
            assert_eq!(Arc::strong_count(&sparse_file), initial_count + 1);

            let Some(TableFsSubmission::Sync(mut submission)) = queue.pop_front() else {
                panic!("expected one prepared table fsync submission");
            };
            assert_eq!(submission.operation.kind(), IOKind::Fsync);
            assert_eq!(submission.operation.fd(), fd);
            assert_eq!(submission.operation.offset(), 0);
            assert_eq!(submission.operation.len(), 0);
            assert!(submission.operation.buf().is_none());
            assert!(submission.operation.take_buf().is_none());
            assert_eq!(submission._file.as_raw_fd(), fd);

            let kind = state_machine.on_complete(TableFsSubmission::Sync(submission), Ok(0));

            assert_eq!(kind, IOKind::Fsync);
            assert!(waiter.wait_result().await.is_ok());
            assert_eq!(Arc::strong_count(&sparse_file), initial_count);
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_fs_sync_completion_rejects_nonzero_success() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) = prepare_table_sync_submission(&table_file);
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_sync_request(submission, 1, &mut queue)
                    .is_none()
            );

            let Some(TableFsSubmission::Sync(submission)) = queue.pop_front() else {
                panic!("expected one prepared table fsync submission");
            };
            let kind = state_machine.on_complete(TableFsSubmission::Sync(submission), Ok(1));

            assert_eq!(kind, IOKind::Fsync);
            let wait_result = waiter.wait_result().await.map_err(|report| {
                Error::from_completion_report(report, "wait for table file background fsync")
            });
            let err = wait_result.expect_err("nonzero fsync completion should fail");
            assert_eq!(
                err.report().downcast_ref::<CompletionErrorKind>().copied(),
                Some(CompletionErrorKind::Io(IoErrorKind::Other))
            );
            assert!(
                format!("{err:?}").contains("wait for table file background fsync"),
                "unexpected error: {err:?}"
            );
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_fs_sync_completion_reports_io_failure() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) = prepare_table_sync_submission(&table_file);
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_sync_request(submission, 1, &mut queue)
                    .is_none()
            );

            let Some(TableFsSubmission::Sync(submission)) = queue.pop_front() else {
                panic!("expected one prepared table fsync submission");
            };
            let kind = state_machine.on_complete(
                TableFsSubmission::Sync(submission),
                Err(StdIoError::from_raw_os_error(libc::EIO)),
            );

            assert_eq!(kind, IOKind::Fsync);
            let wait_result = waiter.wait_result().await.map_err(|report| {
                Error::from_completion_report(report, "wait for table file background fsync")
            });
            let err = wait_result.expect_err("backend fsync completion should fail");
            assert!(
                matches!(
                    err.report().downcast_ref::<CompletionErrorKind>().copied(),
                    Some(CompletionErrorKind::Io(_))
                ),
                "unexpected error: {err:?}"
            );
            assert!(
                format!("{err:?}").contains("wait for table file background fsync"),
                "unexpected error: {err:?}"
            );
            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_fs_pre_submit_sync_failure_preserves_backend_context() {
        smol::block_on(async {
            let (_temp_dir, fs, table_file) = committed_test_table_file().await;
            let mut state_machine = TableFsStateMachine::new();
            let (submission, waiter) = prepare_table_sync_submission(&table_file);
            let mut queue = IOQueue::with_capacity(1);
            assert!(
                state_machine
                    .prepare_sync_request(submission, 1, &mut queue)
                    .is_none()
            );

            let Some(submission) = queue.pop_front() else {
                panic!("expected one prepared table fsync submission");
            };
            let backend_report = IOBackendFailure::report(
                "test_backend",
                IOBackendErrorPhase::Submit,
                StdIoError::from_raw_os_error(libc::EIO),
                2,
                IOBackendQueueState::submit(1, 1),
            );
            let backend_report = attach_backend_operation_kind(backend_report, Some(IOKind::Fsync));

            let kind =
                state_machine.fail_submission_with_backend_error(submission, &backend_report);

            assert_eq!(kind, IOKind::Fsync);
            let completion = waiter
                .wait_result()
                .await
                .expect_err("pre-submit fsync failure should complete with backend error");
            let failure = completion
                .downcast_ref::<IOBackendFailure>()
                .expect("completion error must preserve backend failure attachment");
            assert_eq!(failure.backend(), "test_backend");
            assert_eq!(failure.phase(), IOBackendErrorPhase::Submit);
            assert_eq!(failure.raw_errno(), Some(libc::EIO));
            assert_eq!(failure.call_count(), 2);
            assert!(
                completion
                    .downcast_ref::<IOBackendOperationKind>()
                    .is_some(),
                "completion error must preserve operation-kind attachment"
            );
            drop(table_file);
            drop(fs);
        });
    }
}
