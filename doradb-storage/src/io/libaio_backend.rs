use super::libaio_abi::{
    io_context_t, io_destroy, io_event, io_getevents, io_iocb_cmd, io_setup, io_submit, iocb,
};
use super::{
    Backend, BackendError, BackendResult, BackendStatsHandle, BackendToken, IOKind, Operation,
    StdIoResult, SubmitAttempt, SubmitRetry, SubmitRetryReason, SubmittedIoCleanup,
};
use crate::error::{IoError, IoResult};
use error_stack::Report;
use libc::{EAGAIN, EINTR, EINVAL, c_long};
use std::collections::VecDeque;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::num::NonZeroUsize;
use std::ptr::null_mut;
use std::time::Instant;

/// Canonical name used in libaio backend diagnostics.
pub(crate) const BACKEND_NAME: &str = "libaio";

type LibaioWaitResult = (usize, Vec<(BackendToken, StdIoResult<usize>)>);

/// Concrete libaio context used by the current storage-engine backend.
///
/// This type implements the generic [`Backend`] contract used by the storage
/// IO schedulers.
pub(crate) struct LibaioBackend {
    ctx: io_context_t,
    io_depth: usize,
    stats: BackendStatsHandle,
}

impl LibaioBackend {
    /// Returns a cloneable handle to backend-owned submit/wait statistics.
    #[inline]
    pub(crate) fn stats_handle(&self) -> BackendStatsHandle {
        self.stats.clone()
    }

    /// Submit IO requests with limit.
    /// Submit count will be returned, and caller need to take
    /// care of cleaning the input slice.
    #[inline]
    fn submit_limit(&self, reqs: &[*mut iocb], limit: usize) -> BackendResult<SubmitAttempt> {
        if reqs.is_empty() || limit == 0 {
            return Ok(SubmitAttempt::Noop);
        }
        let batch_size = limit.min(reqs.len());
        let ret = io_submit_impl(self.ctx, batch_size as c_long, reqs.as_ptr() as *mut _);
        if ret < 0 {
            let errcode = -ret;
            if errcode == EAGAIN {
                let reason = SubmitRetryReason::from_raw_errno(errcode)
                    .expect("EAGAIN must convert to submit retry reason");
                return Ok(SubmitAttempt::Retry(SubmitRetry::new(reason, 1)));
            }
            let err = StdIoError::from_raw_os_error(errcode);
            let err = if errcode == EINVAL {
                BackendError::submit_with_note(
                    BACKEND_NAME,
                    err,
                    1,
                    reqs.len(),
                    batch_size,
                    Some(
                        "native libaio IO_CMD_FSYNC/IO_CMD_FDSYNC may be unsupported by this kernel",
                    ),
                )
            } else {
                BackendError::submit(BACKEND_NAME, err, 1, reqs.len(), batch_size)
            };
            return Err(err);
        }
        if ret == 0 {
            // A non-empty submit prefix reached io_submit, but the kernel
            // accepted no iocbs. Keep the staged batch intact and enter the
            // bounded retry/progress-failure path.
            return Ok(SubmitAttempt::Retry(SubmitRetry::new(
                SubmitRetryReason::NoProgress,
                1,
            )));
        }
        let submitted =
            NonZeroUsize::new(ret as usize).expect("positive io_submit return must be non-zero");
        Ok(SubmitAttempt::Submitted(submitted))
    }

    /// Wait until at least the requested number of IO operations finish.
    #[inline]
    fn wait_at_least_with_attempts(
        &self,
        events: &mut [io_event],
        min_nr: usize,
    ) -> BackendResult<LibaioWaitResult> {
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
                let err = StdIoError::from_raw_os_error(errcode);
                return Err(BackendError::wait(BACKEND_NAME, err, wait_calls));
            }
            break ret as usize;
        };
        assert!(
            count != 0,
            "io_getevents with min_nr=1 and timeout=None should not return 0"
        );
        let mut completed = Vec::with_capacity(count);
        for ev in &mut events[..count] {
            let res = if ev.res >= 0 {
                Ok(ev.res as usize)
            } else {
                let err = StdIoError::from_raw_os_error(-ev.res as i32);
                Err(err)
            };
            completed.push((BackendToken::from_raw(ev.data), res));
        }
        Ok((wait_calls, completed))
    }
}

// SAFETY: shared references only pass the opaque kernel context to libaio
// submit/wait syscalls and access atomics-backed stats; mutable backend state
// lives in the kernel or behind atomics, with no thread-affine Rust data.
unsafe impl Sync for LibaioBackend {}
// SAFETY: `LibaioBackend` stores only the opaque kernel-owned `io_context_t`,
// a plain `usize`, and atomics-backed stats state, so moving it between
// threads does not invalidate any Rust-side aliasing or ownership invariants.
unsafe impl Send for LibaioBackend {}

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

impl Backend for LibaioBackend {
    type Prepared = Box<iocb>;
    type SubmitBatch = LibaioSubmitBatch;
    type Events = Box<[io_event]>;

    #[inline]
    fn setup(io_depth: usize) -> IoResult<Self> {
        if io_depth == 0 {
            return Err(
                Report::new(IoError::from(StdIoErrorKind::InvalidInput)).attach(format!(
                    "backend={BACKEND_NAME}, io_depth=0, reason=io depth must be non-zero"
                )),
            );
        }
        let kernel_io_depth = i32::try_from(io_depth).map_err(|_| {
            Report::new(IoError::from(StdIoErrorKind::InvalidInput)).attach(format!(
                "backend={BACKEND_NAME}, io_depth={io_depth}, reason=io depth exceeds i32"
            ))
        })?;
        let mut ctx = null_mut();
        // SAFETY: `ctx` points to writable storage for the kernel-owned libaio
        // context handle, and the return code is checked before constructing
        // the Rust backend wrapper.
        unsafe {
            match io_setup(kernel_io_depth, &mut ctx) {
                0 => Ok(Self {
                    ctx,
                    io_depth,
                    stats: BackendStatsHandle::default(),
                }),
                ret => {
                    let err = StdIoError::from_raw_os_error(-ret);
                    Err(Report::new(IoError::from(err.kind()))
                        .attach(format!("op=backend_setup, {err}")))
                }
            }
        }
    }

    #[inline]
    fn io_depth(&self) -> usize {
        self.io_depth
    }

    #[inline]
    fn new_submit_batch(&self) -> Self::SubmitBatch {
        LibaioSubmitBatch {
            staged: VecDeque::with_capacity(self.io_depth),
            prefix: Vec::with_capacity(self.io_depth),
        }
    }

    #[inline]
    fn new_events(&self) -> Self::Events {
        vec![io_event::default(); self.io_depth].into_boxed_slice()
    }

    #[inline]
    fn prepare(&mut self, token: BackendToken, operation: &mut Operation) -> Self::Prepared {
        let mut iocb = iocb::boxed();
        iocb.aio_fildes = operation.fd() as u32;
        iocb.aio_lio_opcode = match operation.kind() {
            IOKind::Read => io_iocb_cmd::IO_CMD_PREAD as u16,
            IOKind::Write => io_iocb_cmd::IO_CMD_PWRITE as u16,
            // Native libaio file-sync opcodes require Linux 4.18+; older
            // kernels may reject them from `io_submit` with EINVAL.
            IOKind::Fsync => io_iocb_cmd::IO_CMD_FSYNC as u16,
            IOKind::Fdatasync => io_iocb_cmd::IO_CMD_FDSYNC as u16,
        };
        iocb.aio_reqprio = 0;
        match operation.kind() {
            IOKind::Read | IOKind::Write => {
                iocb.buf = operation.as_mut_ptr();
                iocb.count = operation.len() as u64;
                iocb.offset = operation.offset() as u64;
            }
            IOKind::Fsync | IOKind::Fdatasync => {
                iocb.buf = null_mut();
                iocb.count = 0;
                iocb.offset = 0;
            }
        }
        iocb.flags = 0;
        iocb.data = token.raw();
        iocb
    }

    #[inline]
    fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
        batch.staged.push_back(prepared.as_mut_ptr());
    }

    #[inline]
    fn submit_batch(
        &mut self,
        batch: &mut Self::SubmitBatch,
        limit: usize,
    ) -> BackendResult<SubmitAttempt> {
        if batch.staged.is_empty() || limit == 0 {
            return Ok(SubmitAttempt::Noop);
        }
        let start = Instant::now();
        batch.prefix.clear();
        batch
            .prefix
            .extend(batch.staged.iter().take(limit).copied());
        let submit_result = self.submit_limit(&batch.prefix, limit);
        self.stats
            .record_submit_and_wait(1, start.elapsed().as_nanos() as usize);
        let submit_result = submit_result.inspect(|attempt| {
            if let SubmitAttempt::Submitted(submit_count) = attempt {
                self.stats.record_submitted_ops(submit_count.get());
            }
        })?;
        if let SubmitAttempt::Submitted(submit_count) = submit_result {
            batch.staged.drain(0..submit_count.get());
        }
        Ok(submit_result)
    }

    #[inline]
    fn wait_at_least(
        &mut self,
        events: &mut Self::Events,
        min_nr: usize,
    ) -> BackendResult<Vec<(BackendToken, StdIoResult<usize>)>> {
        let start = Instant::now();
        let (_wait_calls, completed) = self
            .wait_at_least_with_attempts(events, min_nr)
            .inspect(|(wait_calls, completed)| {
                self.stats
                    .record_submit_and_wait(*wait_calls, start.elapsed().as_nanos() as usize);
                self.stats.record_wait_completions(completed.len());
            })
            .inspect_err(|err| {
                self.stats
                    .record_submit_and_wait(err.call_count(), start.elapsed().as_nanos() as usize);
            })?;
        Ok(completed)
    }

    #[inline]
    fn cleanup_submitted_io(&mut self, _submitted: usize) -> SubmittedIoCleanup {
        SubmittedIoCleanup::DropAfterBackend
    }
}

/// Backend-owned libaio staging buffer for one worker.
///
/// This stores the pointer-array layout required by `io_submit` while keeping
/// that ABI detail out of the scheduler contract.
pub(crate) struct LibaioSubmitBatch {
    staged: VecDeque<*mut iocb>,
    prefix: Vec<*mut iocb>,
}

// SAFETY: the batch only stores raw pointers to prepared `iocb` objects owned
// by the worker's inflight table. The whole batch is moved together with that
// worker onto a single IO thread.
unsafe impl Send for LibaioSubmitBatch {}

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
    unsafe { io_getevents(ctx, min_nr, nr, events, null_mut()) }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::file::{FixedSizeBufferFreeList, SparseFile, UNTRACKED_FILE_ID};
    use crate::io::{IOBuf, IOSubmission, SubmissionDriver};
    use libc::EAGAIN;
    use std::mem::replace;
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
        replace(&mut *guard, hook)
    }

    #[inline]
    pub(super) fn current_io_getevents_hook() -> Option<IoGeteventsHook> {
        *IO_GETEVENTS_HOOK.lock().unwrap()
    }

    #[inline]
    pub(crate) fn set_io_getevents_hook(hook: Option<IoGeteventsHook>) -> Option<IoGeteventsHook> {
        let mut guard = IO_GETEVENTS_HOOK.lock().unwrap();
        replace(&mut *guard, hook)
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
    fn test_submission_driver_with_libaio_backend() {
        let ctx = LibaioBackend::setup(16).unwrap();
        assert_eq!(ctx.io_depth(), 16);
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("aio_file3.txt");
        let file_path = file_path.to_string_lossy().into_owned();
        let file = SparseFile::create_or_trunc(&file_path, 1024 * 1024, UNTRACKED_FILE_ID).unwrap();

        let buf_free_list = FixedSizeBufferFreeList::new(4096, 4, 4);
        let mut driver = SubmissionDriver::new(ctx);

        let mut buf = buf_free_list.pop();
        buf.reset();
        let data = b"hello, world";
        buf.truncate(data.len());
        buf.data_mut().copy_from_slice(data);

        assert!(
            driver
                .push(Submission {
                    kind: IOKind::Write,
                    operation: Operation::pwrite_owned(file.as_raw_fd(), 0, buf),
                })
                .is_ok()
        );
        assert_eq!(
            driver.submit_ready().unwrap(),
            SubmitAttempt::Submitted(NonZeroUsize::new(1).unwrap())
        );
        let completed = driver.wait_at_least_one().unwrap();
        assert_eq!(completed.submission.kind, IOKind::Write);
        assert_eq!(completed.result.unwrap(), 4096);

        let mut read_buf = buf_free_list.pop();
        read_buf.reset();
        assert!(
            driver
                .push(Submission {
                    kind: IOKind::Read,
                    operation: Operation::pread_owned(file.as_raw_fd(), 0, read_buf),
                })
                .is_ok()
        );
        assert_eq!(
            driver.submit_ready().unwrap(),
            SubmitAttempt::Submitted(NonZeroUsize::new(1).unwrap())
        );
        let completed = driver.wait_at_least_one().unwrap();
        assert_eq!(completed.submission.kind, IOKind::Read);
        assert_eq!(completed.result.unwrap(), 4096);
        let buf = completed.submission.operation.buf().unwrap();
        assert_eq!(&buf.as_bytes()[..data.len()], data);
    }

    #[test]
    fn test_submit_limit_eagain_no_panic() {
        let ctx = LibaioBackend::setup(32).unwrap();
        let previous = set_io_submit_hook(Some(|_, _, _| -EAGAIN));
        let iocb = iocb::boxed();
        let reqs = vec![iocb.as_mut_ptr()];
        let submit_result = ctx.submit_limit(&reqs, 1).unwrap();
        set_io_submit_hook(previous);
        let SubmitAttempt::Retry(retry) = submit_result else {
            panic!("expected submit retry");
        };
        assert_eq!(retry.reason(), SubmitRetryReason::Eagain);
        assert_eq!(retry.raw_errno(), Some(EAGAIN));
        assert_eq!(retry.call_count(), 1);
    }

    #[test]
    fn test_submit_limit_zero_submit_reports_no_progress_retry() {
        let ctx = LibaioBackend::setup(32).unwrap();
        let previous = set_io_submit_hook(Some(|_, _, _| 0));
        let iocb = iocb::boxed();
        let reqs = vec![iocb.as_mut_ptr()];
        let submit_result = ctx.submit_limit(&reqs, 1).unwrap();
        set_io_submit_hook(previous);
        let SubmitAttempt::Retry(retry) = submit_result else {
            panic!("expected submit retry");
        };
        assert_eq!(retry.reason(), SubmitRetryReason::NoProgress);
        assert_eq!(retry.raw_errno(), None);
        assert_eq!(retry.call_count(), 1);
    }

    #[test]
    fn test_libaio_backend_rejects_zero_depth_as_invalid_input() {
        let err = match LibaioBackend::setup(0) {
            Ok(_) => panic!("expected invalid io depth"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), StdIoErrorKind::InvalidInput);
        let report = format!("{err:?}");
        assert!(report.contains("backend=libaio"), "{report}");
        assert!(report.contains("io_depth=0"), "{report}");
    }

    #[test]
    fn test_libaio_backend_rejects_i32_overflow_as_invalid_input() {
        let io_depth = (i32::MAX as usize) + 1;
        let err = match LibaioBackend::setup(io_depth) {
            Ok(_) => panic!("expected invalid io depth"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), StdIoErrorKind::InvalidInput);
        let report = format!("{err:?}");
        assert!(report.contains("backend=libaio"), "{report}");
        assert!(report.contains(&format!("io_depth={io_depth}")), "{report}");
    }

    #[test]
    fn test_libaio_prepare_sync_operations_use_native_opcodes() {
        let mut backend = LibaioBackend::setup(4).unwrap();

        let mut fsync = Operation::fsync(17);
        let fsync_iocb =
            <LibaioBackend as Backend>::prepare(&mut backend, BackendToken::new(1, 2), &mut fsync);
        assert_eq!(fsync_iocb.aio_lio_opcode, io_iocb_cmd::IO_CMD_FSYNC as u16);
        assert_eq!(fsync_iocb.aio_fildes, 17);
        assert_eq!(fsync_iocb.buf, null_mut());
        assert_eq!(fsync_iocb.count, 0);
        assert_eq!(fsync_iocb.offset, 0);

        let mut fdatasync = Operation::fdatasync(19);
        let fdatasync_iocb = <LibaioBackend as Backend>::prepare(
            &mut backend,
            BackendToken::new(1, 3),
            &mut fdatasync,
        );
        assert_eq!(
            fdatasync_iocb.aio_lio_opcode,
            io_iocb_cmd::IO_CMD_FDSYNC as u16
        );
        assert_eq!(fdatasync_iocb.aio_fildes, 19);
        assert_eq!(fdatasync_iocb.buf, null_mut());
        assert_eq!(fdatasync_iocb.count, 0);
        assert_eq!(fdatasync_iocb.offset, 0);
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
                (*events).obj = null_mut();
                (*events).res = 4096;
                (*events).res2 = 0;
            }
            1
        }

        let mut backend = LibaioBackend::setup(32).unwrap();
        let mut events = backend.new_events();
        let token = BackendToken::new(7, 3);

        IO_GETEVENTS_CALLS.store(0, Ordering::SeqCst);
        let previous = set_io_getevents_hook(Some(eintr_then_one_completion));
        let baseline = backend.stats_handle().snapshot();
        let completions =
            <LibaioBackend as Backend>::wait_at_least(&mut backend, &mut events, 1).unwrap();
        set_io_getevents_hook(previous);

        assert_eq!(IO_GETEVENTS_CALLS.load(Ordering::SeqCst), 2);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, token);
        match &completions[0].1 {
            Ok(len) => assert_eq!(*len, 4096),
            Err(err) => panic!("expected successful completion, got error: {err}"),
        }

        let delta = backend.stats_handle().snapshot().delta_since(baseline);
        assert_eq!(delta.submit_and_wait_calls, 2);
        assert_eq!(delta.wait_completions, 1);
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
}
