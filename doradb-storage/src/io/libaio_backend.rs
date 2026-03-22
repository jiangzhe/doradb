use super::{
    AIOClient, AIOError, AIOKey, AIOKind, AIOResult, BackendToken, IOBackend, IOWorkerBuilder,
    Operation, io_context_t, io_destroy, io_event, io_getevents, io_iocb_cmd, io_setup, io_submit,
    iocb,
};
use flume::bounded;
use libc::{EAGAIN, EINTR, c_long};
use std::collections::VecDeque;
use std::result::Result as StdResult;

const DEFAULT_AIO_MAX_EVENTS: usize = 32;

/// Concrete libaio context used by the current storage-engine backend.
///
/// This type still exposes the legacy raw submit/wait helpers for redo-log
/// code, and also implements the generic [`IOBackend`] contract used by
/// [`super::IOWorker`].
pub struct LibaioContext {
    ctx: io_context_t,
    max_events: usize,
}

unsafe impl Sync for LibaioContext {}
unsafe impl Send for LibaioContext {}

impl LibaioContext {
    /// Create a new AIO context with max events(io depth).
    #[inline]
    pub fn new(max_events: usize) -> AIOResult<Self> {
        debug_assert!(max_events < isize::MAX as usize);
        let mut ctx = std::ptr::null_mut();
        unsafe {
            match io_setup(max_events as i32, &mut ctx) {
                0 => Ok(LibaioContext { ctx, max_events }),
                _ => Err(AIOError::SetupError),
            }
        }
    }

    /// Create a default AIO context.
    #[inline]
    pub fn try_default() -> AIOResult<Self> {
        LibaioContext::new(DEFAULT_AIO_MAX_EVENTS)
    }

    /// Returns maximum events.
    #[inline]
    pub fn max_events(&self) -> usize {
        self.max_events
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
        mut callback: F,
    ) -> (usize, usize)
    where
        F: FnMut(AIOKey, StdResult<usize, std::io::Error>) -> AIOKind,
    {
        let max_nwait = events.len();
        let count = loop {
            let ret = unsafe {
                io_getevents(
                    self.ctx,
                    min_nr as c_long,
                    max_nwait as c_long,
                    events.as_mut_ptr(),
                    std::ptr::null_mut(),
                )
            };
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
        for ev in &events[..count] {
            let key = ev.data;
            let res = if ev.res >= 0 {
                Ok(ev.res as usize)
            } else {
                let err = std::io::Error::from_raw_os_error(-ev.res as i32);
                Err(err)
            };
            match callback(key, res) {
                AIOKind::Read => read_count += 1,
                AIOKind::Write => write_count += 1,
            }
        }
        (read_count, write_count)
    }

    /// Build an IO worker builder.
    #[inline]
    pub fn io_worker<T>(self) -> (IOWorkerBuilder<T>, AIOClient<T>) {
        const DEFAULT_AIO_EVENT_LOOP_BACKLOG: usize = 10;
        let (tx, rx) = bounded(DEFAULT_AIO_EVENT_LOOP_BACKLOG);
        let worker = IOWorkerBuilder { backend: self, rx };
        (worker, AIOClient(tx))
    }
}

#[inline]
fn io_submit_impl(ctx: io_context_t, nr: c_long, ios: *mut *mut iocb) -> i32 {
    #[cfg(test)]
    {
        let hook = *IO_SUBMIT_HOOK.lock().unwrap();
        if let Some(hook) = hook {
            return hook(ctx, nr, ios);
        }
    }
    unsafe { io_submit(ctx, nr, ios) }
}

#[cfg(test)]
type IoSubmitHook = fn(io_context_t, c_long, *mut *mut iocb) -> i32;

#[cfg(test)]
static IO_SUBMIT_HOOK: std::sync::Mutex<Option<IoSubmitHook>> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn set_io_submit_hook(hook: Option<IoSubmitHook>) -> Option<IoSubmitHook> {
    let mut guard = IO_SUBMIT_HOOK.lock().unwrap();
    std::mem::replace(&mut *guard, hook)
}

impl Drop for LibaioContext {
    #[inline]
    fn drop(&mut self) {
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

impl IOBackend for LibaioContext {
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
            AIOKind::Read => io_iocb_cmd::IO_CMD_PREAD as u16,
            AIOKind::Write => io_iocb_cmd::IO_CMD_PWRITE as u16,
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
        batch.prefix.clear();
        batch
            .prefix
            .extend(batch.staged.iter().take(limit).copied());
        let submit_count = self.submit_limit(&batch.prefix, limit);
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
    ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
        let mut completed = Vec::new();
        LibaioContext::wait_at_least(self, events, min_nr, |token, res| {
            completed.push((BackendToken::from_raw(token), res));
            AIOKind::Read
        });
        completed
    }
}
