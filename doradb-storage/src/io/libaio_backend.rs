use super::{
    BackendToken, IOBackend, IOBackendStats, IOBackendStatsHandle, IOBuf, IOClient, IOKey, IOKind,
    IOWorkerBuilder, Operation, StdIoResult, build_io_worker, io_context_t, io_destroy, io_event,
    io_getevents, io_iocb_cmd, io_setup, io_submit, iocb,
};
use crate::error::{ConfigError, IoError, Result, StorageOp};
use error_stack::Report;
use libc::{EAGAIN, EINTR, c_long};
use std::collections::VecDeque;
use std::io;
use std::os::unix::io::RawFd;
use std::time::Instant;

const DEFAULT_IO_MAX_EVENTS: usize = 32;

/// Concrete libaio context used by the current storage-engine backend.
///
/// This type still exposes the legacy raw submit/wait helpers for redo-log
/// code, and also implements the generic [`IOBackend`] contract used by
/// [`super::IOWorker`].
pub struct LibaioBackend {
    ctx: io_context_t,
    max_events: usize,
    stats: IOBackendStatsHandle,
}

// SAFETY: shared references only pass the opaque kernel context to libaio
// submit/wait syscalls and access atomics-backed stats; mutable backend state
// lives in the kernel or behind atomics, with no thread-affine Rust data.
unsafe impl Sync for LibaioBackend {}
// SAFETY: `LibaioBackend` stores only the opaque kernel-owned `io_context_t`,
// a plain `usize`, and atomics-backed stats state, so moving it between
// threads does not invalidate any Rust-side aliasing or ownership invariants.
unsafe impl Send for LibaioBackend {}

pub type IocbRawPtr = *mut iocb;

/// Raw libaio request backed by an owned aligned buffer.
pub struct IORequest<T> {
    iocb: Box<iocb>,
    // libaio keeps the raw buffer pointer until completion, so the owned
    // buffer must stay attached to the request object for the full inflight
    // lifetime.
    buf: Option<T>,
    pub key: IOKey,
}

impl<T: IOBuf> IORequest<T> {
    #[inline]
    pub fn new(
        key: IOKey,
        fd: RawFd,
        offset: usize,
        mut buf: T,
        priority: u16,
        flags: u32,
        opcode: io_iocb_cmd,
    ) -> Self {
        let mut iocb = iocb::boxed();
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.buf = buf.as_bytes_mut().as_mut_ptr();
        iocb.count = buf.as_bytes().len() as u64;
        iocb.offset = offset as u64;
        iocb.flags = flags;
        iocb.data = key;
        IORequest {
            key,
            buf: Some(buf),
            iocb,
        }
    }

    #[inline]
    pub fn iocb_raw(&self) -> IocbRawPtr {
        self.iocb.as_mut_ptr()
    }

    #[inline]
    pub fn take_buf(&mut self) -> Option<T> {
        self.buf.take()
    }

    #[inline]
    pub fn buf(&self) -> Option<&T> {
        self.buf.as_ref()
    }
}

/// Raw libaio request backed by a borrowed page-aligned pointer.
///
/// The pointer must outlive the kernel request. This compatibility wrapper is
/// still used for buffer-pool page memory that remains allocated for the whole
/// async submission lifetime.
pub struct UnsafeIORequest {
    iocb: Box<iocb>,
    pub key: IOKey,
}

impl UnsafeIORequest {
    /// Creates one libaio request from a borrowed aligned pointer.
    ///
    /// # Safety
    ///
    /// Caller must guarantee the pointer remains valid until the kernel
    /// reports completion and that the pointer plus length satisfy direct-I/O
    /// alignment rules.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub unsafe fn new(
        key: IOKey,
        fd: RawFd,
        offset: usize,
        ptr: *mut u8,
        len: usize,
        priority: u16,
        flags: u32,
        opcode: io_iocb_cmd,
    ) -> Self {
        let mut iocb = iocb::boxed();
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.buf = ptr;
        iocb.count = len as u64;
        iocb.offset = offset as u64;
        iocb.flags = flags;
        iocb.data = key;
        UnsafeIORequest { iocb, key }
    }

    #[inline]
    pub fn iocb_raw(&self) -> IocbRawPtr {
        self.iocb.as_mut_ptr()
    }
}

impl LibaioBackend {
    /// Create a new libaio context with max events(io depth).
    #[inline]
    pub fn new(max_events: usize) -> Result<Self> {
        if max_events == 0 || max_events > i32::MAX as usize {
            return Err(Report::new(ConfigError::InvalidIoDepth)
                .attach(format!("max_events={max_events}"))
                .into());
        }
        let mut ctx = std::ptr::null_mut();
        // SAFETY: `ctx` points to writable storage for the kernel-owned libaio
        // context handle, and the return code is checked before constructing
        // the Rust backend wrapper.
        unsafe {
            match io_setup(max_events as i32, &mut ctx) {
                0 => Ok(LibaioBackend {
                    ctx,
                    max_events,
                    stats: IOBackendStatsHandle::default(),
                }),
                ret => {
                    let err = io::Error::from_raw_os_error(-ret);
                    Err(IoError::report_with_op(StorageOp::BackendSetup, err).into())
                }
            }
        }
    }

    /// Create a default libaio context.
    #[inline]
    pub fn try_default() -> Result<Self> {
        LibaioBackend::new(DEFAULT_IO_MAX_EVENTS)
    }

    /// Returns maximum events.
    #[inline]
    pub fn max_events(&self) -> usize {
        self.max_events
    }

    /// Returns one snapshot of backend-owned submit/wait activity.
    #[inline]
    pub fn stats(&self) -> IOBackendStats {
        self.stats.snapshot()
    }

    #[inline]
    pub(crate) fn stats_handle(&self) -> IOBackendStatsHandle {
        self.stats.clone()
    }

    /// Create a heap-allocated event array for IO submit and wait.
    #[inline]
    pub fn events(&self) -> Box<[io_event]> {
        vec![io_event::default(); self.max_events].into_boxed_slice()
    }

    /// Submit IO requests with limit.
    /// Submit count will be returned, and caller need to take
    /// care of cleaning the input slice.
    #[inline]
    pub fn submit_limit(&self, reqs: &[*mut iocb], limit: usize) -> usize {
        if reqs.is_empty() || limit == 0 {
            return 0;
        }
        let batch_size = limit.min(reqs.len());
        let ret = io_submit_impl(self.ctx, batch_size as c_long, reqs.as_ptr() as *mut _);
        if ret < 0 {
            let errcode = -ret;
            if errcode == EAGAIN {
                return 0;
            }
            panic!(
                "io_submit returns error code {errcode}: batch_size={batch_size} limit={limit} reqs_len={}",
                reqs.len()
            );
        }
        ret as usize
    }

    /// Wait until given number of IO finishes, and execute callback for each.
    /// Returns number of finished events.
    #[inline]
    pub fn wait_at_least<F>(
        &self,
        events: &mut [io_event],
        min_nr: usize,
        callback: F,
    ) -> (usize, usize)
    where
        F: FnMut(IOKey, StdIoResult<usize>) -> IOKind,
    {
        let (_, read_count, write_count) =
            self.wait_at_least_with_attempts(events, min_nr, callback);
        (read_count, write_count)
    }

    #[inline]
    fn wait_at_least_with_attempts<F>(
        &self,
        events: &mut [io_event],
        min_nr: usize,
        mut callback: F,
    ) -> (usize, usize, usize)
    where
        F: FnMut(IOKey, StdIoResult<usize>) -> IOKind,
    {
        let max_nwait = events.len();
        let mut wait_calls = 0;
        let count = loop {
            wait_calls += 1;
            let ret = io_getevents_impl(
                self.ctx,
                min_nr as c_long,
                max_nwait as c_long,
                events.as_mut_ptr(),
            );
            if ret < 0 {
                let errcode = -ret;
                if errcode == EINTR {
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
        let mut read_count = 0;
        let mut write_count = 0;
        for ev in &mut events[..count] {
            let key = ev.data;
            let res = if ev.res >= 0 {
                Ok(ev.res as usize)
            } else {
                let err = io::Error::from_raw_os_error(-ev.res as i32);
                Err(err)
            };
            match callback(key, res) {
                IOKind::Read => read_count += 1,
                IOKind::Write => write_count += 1,
            }
        }
        (wait_calls, read_count, write_count)
    }

    /// Build an IO worker builder.
    #[inline]
    pub fn io_worker<T>(self) -> (IOWorkerBuilder<T>, IOClient<T>) {
        build_io_worker(self)
    }
}

#[inline]
pub fn pread<T: IOBuf>(key: IOKey, fd: RawFd, offset: usize, buf: T) -> IORequest<T> {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    IORequest::new(
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
pub fn pwrite<T: IOBuf>(key: IOKey, fd: RawFd, offset: usize, buf: T) -> IORequest<T> {
    const PRIORITY: u16 = 0;
    const FLAGS: u32 = 0;
    IORequest::new(
        key,
        fd,
        offset,
        buf,
        PRIORITY,
        FLAGS,
        io_iocb_cmd::IO_CMD_PWRITE,
    )
}

#[inline]
fn io_submit_impl(ctx: io_context_t, nr: c_long, ios: *mut *mut iocb) -> i32 {
    #[cfg(test)]
    {
        if let Some(hook) = tests::current_io_submit_hook() {
            return hook(ctx, nr, ios);
        }
    }
    // SAFETY: the caller forwards the live libaio context, request count, and
    // pointer array exactly as required by `io_submit`.
    unsafe { io_submit(ctx, nr, ios) }
}

#[inline]
fn io_getevents_impl(ctx: io_context_t, min_nr: c_long, nr: c_long, events: *mut io_event) -> i32 {
    #[cfg(test)]
    {
        if let Some(hook) = tests::current_io_getevents_hook() {
            return hook(ctx, min_nr, nr, events);
        }
    }
    // SAFETY: the caller provides a live libaio context plus an output buffer
    // large enough for `nr` events, and uses the null timeout for blocking wait.
    unsafe { io_getevents(ctx, min_nr, nr, events, std::ptr::null_mut()) }
}

impl Drop for LibaioBackend {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self.ctx` is the live libaio context originally created by
        // `io_setup`, and drop is the unique point that destroys it.
        unsafe {
            assert_eq!(io_destroy(self.ctx), 0);
        }
    }
}

/// Backend-owned libaio staging buffer for one worker.
///
/// This stores the pointer-array layout required by `io_submit` while keeping
/// that ABI detail out of the generic worker contract.
pub struct LibaioSubmitBatch {
    staged: VecDeque<*mut iocb>,
    prefix: Vec<*mut iocb>,
}

// SAFETY: the batch only stores raw pointers to prepared `iocb` objects owned
// by the worker's inflight table. The whole batch is moved together with that
// worker onto a single IO thread.
unsafe impl Send for LibaioSubmitBatch {}

impl IOBackend for LibaioBackend {
    type Prepared = Box<iocb>;
    type SubmitBatch = LibaioSubmitBatch;
    type Events = Box<[io_event]>;

    #[inline]
    fn max_events(&self) -> usize {
        self.max_events()
    }

    #[inline]
    fn new_submit_batch(&self) -> Self::SubmitBatch {
        LibaioSubmitBatch {
            staged: VecDeque::with_capacity(self.max_events()),
            prefix: Vec::with_capacity(self.max_events()),
        }
    }

    #[inline]
    fn new_events(&self) -> Self::Events {
        self.events()
    }

    #[inline]
    fn prepare(&mut self, token: BackendToken, operation: &mut Operation) -> Self::Prepared {
        let mut iocb = iocb::boxed();
        iocb.aio_fildes = operation.fd() as u32;
        iocb.aio_lio_opcode = match operation.kind() {
            IOKind::Read => io_iocb_cmd::IO_CMD_PREAD as u16,
            IOKind::Write => io_iocb_cmd::IO_CMD_PWRITE as u16,
        };
        iocb.aio_reqprio = 0;
        iocb.buf = operation.as_mut_ptr();
        iocb.count = operation.len() as u64;
        iocb.offset = operation.offset() as u64;
        iocb.flags = 0;
        iocb.data = token.raw();
        iocb
    }

    #[inline]
    fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
        batch.staged.push_back(prepared.as_mut_ptr());
    }

    #[inline]
    fn submit_batch(&mut self, batch: &mut Self::SubmitBatch, limit: usize) -> usize {
        if batch.staged.is_empty() || limit == 0 {
            return 0;
        }
        let start = Instant::now();
        batch.prefix.clear();
        batch
            .prefix
            .extend(batch.staged.iter().take(limit).copied());
        let submit_count = self.submit_limit(&batch.prefix, limit);
        self.stats
            .record_submit_and_wait(1, start.elapsed().as_nanos() as usize);
        self.stats.record_submitted_ops(submit_count);
        if submit_count != 0 {
            batch.staged.drain(0..submit_count);
        }
        submit_count
    }

    #[inline]
    fn wait_at_least(
        &mut self,
        events: &mut Self::Events,
        min_nr: usize,
    ) -> Vec<(BackendToken, StdIoResult<usize>)> {
        let start = Instant::now();
        let mut completed = Vec::new();
        let (wait_calls, _read_count, _write_count) =
            self.wait_at_least_with_attempts(events, min_nr, |token, res| {
                completed.push((BackendToken::from_raw(token), res));
                IOKind::Read
            });
        self.stats
            .record_submit_and_wait(wait_calls, start.elapsed().as_nanos() as usize);
        self.stats.record_wait_completions(completed.len());
        completed
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::file::{FixedSizeBufferFreeList, SparseFile, UNTRACKED_FILE_ID};
    use crate::io::{DirectBuf, IOQueue, IOStateMachine, IOSubmission};
    use libc::EAGAIN;
    use std::os::fd::AsRawFd;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    pub(crate) type IoSubmitHook = fn(io_context_t, c_long, *mut *mut iocb) -> i32;
    pub(crate) type IoGeteventsHook = fn(io_context_t, c_long, c_long, *mut io_event) -> i32;

    static IO_SUBMIT_HOOK: Mutex<Option<IoSubmitHook>> = Mutex::new(None);
    static IO_GETEVENTS_HOOK: Mutex<Option<IoGeteventsHook>> = Mutex::new(None);
    static IO_GETEVENTS_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    pub(super) fn current_io_submit_hook() -> Option<IoSubmitHook> {
        *IO_SUBMIT_HOOK.lock().unwrap()
    }

    #[inline]
    pub(crate) fn set_io_submit_hook(hook: Option<IoSubmitHook>) -> Option<IoSubmitHook> {
        let mut guard = IO_SUBMIT_HOOK.lock().unwrap();
        std::mem::replace(&mut *guard, hook)
    }

    #[inline]
    pub(super) fn current_io_getevents_hook() -> Option<IoGeteventsHook> {
        *IO_GETEVENTS_HOOK.lock().unwrap()
    }

    #[inline]
    pub(crate) fn set_io_getevents_hook(hook: Option<IoGeteventsHook>) -> Option<IoGeteventsHook> {
        let mut guard = IO_GETEVENTS_HOOK.lock().unwrap();
        std::mem::replace(&mut *guard, hook)
    }

    #[test]
    fn test_aio_file_extend() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("aio_file2.txt");
        let file_path = file_path.to_string_lossy().into_owned();
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024, UNTRACKED_FILE_ID).unwrap();
        let (logical_size, allocated_size) = file.size().unwrap();
        println!("file created, logical size={logical_size}, allocated size={allocated_size}");
        assert_eq!(logical_size, 1024 * 1024);
        assert_eq!(allocated_size, 0);
        file.extend_to(1024 * 1024 * 2).unwrap();
        let (logical_size, allocated_size) = file.size().unwrap();
        println!("file grown, logical size={logical_size}, allocated_size={allocated_size}");
        assert_eq!(logical_size, 2 * 1024 * 1024);
        assert_eq!(allocated_size, 0);
        drop(file);
    }

    #[test]
    fn test_io_worker() {
        let ctx = LibaioBackend::new(16).unwrap();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("aio_file3.txt");
        let file_path = file_path.to_string_lossy().into_owned();
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024, UNTRACKED_FILE_ID).unwrap();

        let buf_free_list = FixedSizeBufferFreeList::new(4096, 4, 4);
        let listener = SimpleListener { file };
        let (worker, client) = ctx.io_worker();
        let handle = worker.bind(listener).start_thread();

        let mut buf = buf_free_list.pop(false);
        buf.reset();
        buf.extend_from_slice(b"hello, world");

        client
            .send(Request {
                kind: IOKind::Write,
                offset: 0,
                buf,
            })
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut elem2 = buf_free_list.pop(false);
        elem2.reset();
        let _ = client.send(Request {
            kind: IOKind::Read,
            offset: 0,
            buf: elem2,
        });
        std::thread::sleep(std::time::Duration::from_millis(100));

        client.shutdown();
        handle.join().unwrap();
    }

    #[test]
    fn test_submit_limit_eagain_no_panic() {
        let ctx = LibaioBackend::try_default().unwrap();
        let previous = set_io_submit_hook(Some(|_, _, _| -EAGAIN));
        let iocb = iocb::boxed();
        let reqs = vec![iocb.as_mut_ptr()];
        let submit_count = ctx.submit_limit(&reqs, 1);
        set_io_submit_hook(previous);
        assert_eq!(submit_count, 0);
    }

    #[test]
    fn test_libaio_backend_rejects_zero_depth_as_config_error() {
        let err = match LibaioBackend::new(0) {
            Ok(_) => panic!("expected invalid io depth"),
            Err(err) => err,
        };
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report()
                .downcast_ref::<crate::error::ConfigError>()
                .copied(),
            Some(crate::error::ConfigError::InvalidIoDepth)
        );
    }

    #[test]
    fn test_wait_at_least_stats_count_eintr_retries() {
        fn eintr_then_one_completion(
            _ctx: io_context_t,
            min_nr: c_long,
            nr: c_long,
            events: *mut io_event,
        ) -> i32 {
            assert_eq!(min_nr, 1);
            assert!(nr >= 1);
            let call = IO_GETEVENTS_CALLS.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return -EINTR;
            }
            assert_eq!(call, 1);
            // SAFETY: the hook only writes one completion into the caller-provided
            // event buffer after asserting there is room for at least one entry.
            unsafe {
                (*events).data = BackendToken::new(7, 3).raw();
                (*events).obj = std::ptr::null_mut();
                (*events).res = 4096;
                (*events).res2 = 0;
            }
            1
        }

        let mut backend = LibaioBackend::try_default().unwrap();
        let mut events = backend.events();
        let token = BackendToken::new(7, 3);

        IO_GETEVENTS_CALLS.store(0, Ordering::SeqCst);
        let previous = set_io_getevents_hook(Some(eintr_then_one_completion));
        let baseline = backend.stats();
        let completions = <LibaioBackend as IOBackend>::wait_at_least(&mut backend, &mut events, 1);
        set_io_getevents_hook(previous);

        assert_eq!(IO_GETEVENTS_CALLS.load(Ordering::SeqCst), 2);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, token);
        match &completions[0].1 {
            Ok(len) => assert_eq!(*len, 4096),
            Err(err) => panic!("expected successful completion, got error: {err}"),
        }

        let delta = backend.stats().delta_since(baseline);
        assert_eq!(delta.submit_and_wait_calls, 2);
        assert_eq!(delta.wait_completions, 1);
    }

    struct Request {
        kind: IOKind,
        offset: usize,
        buf: DirectBuf,
    }

    struct Submission {
        kind: IOKind,
        operation: Operation,
    }

    impl IOSubmission for Submission {
        fn operation(&mut self) -> &mut Operation {
            &mut self.operation
        }
    }

    struct SimpleListener {
        file: SparseFile,
    }

    impl IOStateMachine for SimpleListener {
        type Request = Request;
        type Submission = Submission;

        fn prepare_request(
            &mut self,
            req: Request,
            max_new: usize,
            queue: &mut IOQueue<Submission>,
        ) -> Option<Request> {
            if max_new == 0 {
                return Some(req);
            }
            let operation = match req.kind {
                IOKind::Read => Operation::pread_owned(self.file.as_raw_fd(), req.offset, req.buf),
                IOKind::Write => {
                    Operation::pwrite_owned(self.file.as_raw_fd(), req.offset, req.buf)
                }
            };
            queue.push(Submission {
                kind: req.kind,
                operation,
            });
            None
        }

        fn on_submit(&mut self, _sub: &Submission) {}

        fn on_complete(&mut self, sub: Submission, res: StdIoResult<usize>) -> IOKind {
            match res {
                Ok(len) => {
                    match sub.kind {
                        IOKind::Read => {
                            println!("read {} bytes", len);
                        }
                        IOKind::Write => {
                            println!("write {} bytes", len);
                        }
                    }
                    let buf = sub.operation.buf().unwrap();
                    let n = buf.data().len().min(20);
                    println!("leading {} bytes: {:?}", n, &buf.data()[..n]);
                    sub.kind
                }
                Err(err) => {
                    panic!("{:?}", err);
                }
            }
        }

        fn end_loop(self) {
            drop(self.file);
        }
    }
}
