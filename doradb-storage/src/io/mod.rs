mod buf;
mod libaio_abi;

use crate::lifetime::StaticLifetime;
use crate::thread;
use flume::{Receiver, SendError, Sender, TryRecvError, TrySendError};
use libc::EINTR;
#[cfg(feature = "libaio")]
use libc::{EAGAIN, c_long};
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::result::Result as StdResult;
use std::thread::JoinHandle;
use std::time::Instant;
use thiserror::Error;

pub use buf::*;
pub use libaio_abi::*;

#[cfg(not(feature = "libaio"))]
mod no_libaio {
    use super::*;
    use libc::{c_void, off_t};

    pub(super) struct Completion {
        pub(super) key: AIOKey,
        pub(super) res: std::io::Result<usize>,
    }

    pub(super) unsafe fn blocking_iocb(iocb: IocbRawPtr) -> Completion {
        let iocb = unsafe { &*iocb };
        let fd = iocb.aio_fildes as RawFd;
        let offset = iocb.offset as off_t;
        let len = iocb.count as usize;
        let res = match iocb.aio_lio_opcode {
            code if code == io_iocb_cmd::IO_CMD_PREAD as u16 => unsafe {
                blocking_pread(fd, iocb.buf, len, offset)
            },
            code if code == io_iocb_cmd::IO_CMD_PWRITE as u16 => unsafe {
                blocking_pwrite(fd, iocb.buf, len, offset)
            },
            _ => Err(std::io::Error::other("unsupported iocb opcode")),
        };
        Completion {
            key: iocb.data,
            res,
        }
    }

    pub(super) unsafe fn blocking_pread(
        fd: RawFd,
        buf: *mut u8,
        len: usize,
        offset: off_t,
    ) -> std::io::Result<usize> {
        loop {
            let ret = unsafe { libc::pread(fd, buf as *mut c_void, len, offset) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(EINTR) {
                    continue;
                }
                return Err(err);
            }
            return Ok(ret as usize);
        }
    }

    pub(super) unsafe fn blocking_pwrite(
        fd: RawFd,
        buf: *mut u8,
        len: usize,
        offset: off_t,
    ) -> std::io::Result<usize> {
        loop {
            let ret = unsafe { libc::pwrite(fd, buf as *mut c_void, len, offset) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(EINTR) {
                    continue;
                }
                return Err(err);
            }
            return Ok(ret as usize);
        }
    }
}

#[cfg(not(feature = "libaio"))]
use no_libaio::{Completion, blocking_iocb};

pub const MIN_PAGE_SIZE: usize = 4096;
pub const STORAGE_SECTOR_SIZE: usize = 4096;

/// Align given input length to storage sector size.
#[inline]
pub fn align_to_sector_size(len: usize) -> usize {
    len.max(STORAGE_SECTOR_SIZE).div_ceil(STORAGE_SECTOR_SIZE) * STORAGE_SECTOR_SIZE
}

const DEFAULT_AIO_MAX_EVENTS: usize = 32;

#[derive(Debug, Clone, Error)]
pub enum AIOError {
    #[error("AIO setup error")]
    SetupError,
    #[error("create file error")]
    CreateFileError,
    #[error("open file error")]
    OpenFileError,
    #[error("truncate file error")]
    TruncFileError,
    #[error("AIO out of range")]
    OutOfRange,
    #[error("AIO file stat error")]
    StatError,
}

pub type AIOResult<T> = StdResult<T, AIOError>;

pub struct AIOContext {
    ctx: io_context_t,
    max_events: usize,
    #[cfg(not(feature = "libaio"))]
    completion_tx: Sender<Completion>,
    #[cfg(not(feature = "libaio"))]
    completion_rx: Receiver<Completion>,
}

unsafe impl Sync for AIOContext {}
unsafe impl Send for AIOContext {}

impl AIOContext {
    /// Create a new AIO context with max events(io depth).
    #[inline]
    pub fn new(max_events: usize) -> AIOResult<Self> {
        debug_assert!(max_events < isize::MAX as usize);
        #[cfg(feature = "libaio")]
        let mut ctx = std::ptr::null_mut();
        #[cfg(feature = "libaio")]
        unsafe {
            match io_setup(max_events as i32, &mut ctx) {
                0 => Ok(AIOContext { ctx, max_events }),
                _ => Err(AIOError::SetupError),
            }
        }
        #[cfg(not(feature = "libaio"))]
        {
            let (completion_tx, completion_rx) = flume::unbounded();
            Ok(AIOContext {
                ctx: std::ptr::null_mut(),
                max_events,
                completion_tx,
                completion_rx,
            })
        }
    }

    /// Create a default AIO context.
    #[inline]
    pub fn try_default() -> AIOResult<Self> {
        AIOContext::new(DEFAULT_AIO_MAX_EVENTS)
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
    #[cfg(feature = "libaio")]
    pub fn submit_limit(&self, reqs: &[*mut iocb], limit: usize) -> usize {
        if reqs.is_empty() || limit == 0 {
            return 0;
        }
        let batch_size = limit.min(reqs.len());
        let ret = io_submit_impl(self.ctx, batch_size as c_long, reqs.as_ptr() as *mut _);
        // See https://man7.org/linux/man-pages/man2/io_submit.2.html
        // for the details of return value of io_submit().
        // if success, non-negative value indicates how many IO submitted.
        // if error, negative value of error code.
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

    /// Submit IO requests with limit.
    /// Submit count will be returned, and caller need to take
    /// care of cleaning the input slice.
    #[inline]
    #[cfg(not(feature = "libaio"))]
    pub fn submit_limit(&self, reqs: &[*mut iocb], limit: usize) -> usize {
        if reqs.is_empty() || limit == 0 {
            return 0;
        }
        let batch_size = limit.min(reqs.len());
        for iocb in reqs.iter().take(batch_size).copied() {
            let completion_tx = self.completion_tx.clone();
            let iocb_addr = iocb as usize;
            smol::spawn(async move {
                let completion =
                    smol::unblock(move || unsafe { blocking_iocb(iocb_addr as IocbRawPtr) }).await;
                let _ = completion_tx.send(completion);
            })
            .detach();
        }
        batch_size
    }

    /// Wait until given number of IO finishes, and execute callback for each.
    /// Returns number of finished events.
    #[inline]
    #[cfg(feature = "libaio")]
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

    /// Wait until given number of IO finishes, and execute callback for each.
    /// Returns number of finished events.
    #[inline]
    #[cfg(not(feature = "libaio"))]
    pub fn wait_at_least<F>(
        &self,
        events: &mut [io_event],
        min_nr: usize,
        mut callback: F,
    ) -> (usize, usize)
    where
        F: FnMut(AIOKey, StdResult<usize, std::io::Error>) -> AIOKind,
    {
        let max_nwait = events.len().max(1);
        let min_required = min_nr.min(max_nwait);
        let mut read_count = 0;
        let mut write_count = 0;
        let mut handled = 0;
        let mut handle_completion = |completion: Completion,
                                     read_count: &mut usize,
                                     write_count: &mut usize| {
            match callback(completion.key, completion.res) {
                AIOKind::Read => *read_count += 1,
                AIOKind::Write => *write_count += 1,
            }
        };
        while handled < min_required {
            let completion = self.completion_rx.recv().unwrap();
            handle_completion(completion, &mut read_count, &mut write_count);
            handled += 1;
        }
        while handled < max_nwait {
            match self.completion_rx.try_recv() {
                Ok(completion) => {
                    handle_completion(completion, &mut read_count, &mut write_count);
                    handled += 1;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        (read_count, write_count)
    }

    /// Build a event loop.
    #[inline]
    pub fn event_loop<T>(self) -> (AIOEventLoop<T>, AIOClient<T>) {
        const DEFAULT_AIO_EVENT_LOOP_BACKLOG: usize = 10;
        let (tx, rx) = flume::bounded(DEFAULT_AIO_EVENT_LOOP_BACKLOG);
        let el = AIOEventLoop {
            ctx: self,
            rx,
            submitted: 0,
            shutdown: false,
        };
        (el, AIOClient(tx))
    }
}

#[inline]
#[cfg(feature = "libaio")]
fn io_submit_impl(ctx: io_context_t, nr: c_long, ios: *mut *mut iocb) -> i32 {
    #[cfg(test)]
    {
        let hook = *IO_SUBMIT_HOOK.lock().unwrap();
        if let Some(hook) = hook {
            return hook(ctx, nr, ios);
        }
    }
    // SAFETY: `ctx`, `nr`, and `ios` are forwarded to libaio exactly as prepared
    // by caller-side request construction.
    unsafe { io_submit(ctx, nr, ios) }
}

#[cfg(all(test, feature = "libaio"))]
type IoSubmitHook = fn(io_context_t, c_long, *mut *mut iocb) -> i32;

#[cfg(all(test, feature = "libaio"))]
static IO_SUBMIT_HOOK: std::sync::Mutex<Option<IoSubmitHook>> = std::sync::Mutex::new(None);

#[cfg(all(test, feature = "libaio"))]
fn set_io_submit_hook(hook: Option<IoSubmitHook>) -> Option<IoSubmitHook> {
    let mut guard = IO_SUBMIT_HOOK.lock().unwrap();
    std::mem::replace(&mut *guard, hook)
}

unsafe impl StaticLifetime for AIOContext {}

impl Drop for AIOContext {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "libaio")]
        unsafe {
            assert_eq!(io_destroy(self.ctx), 0);
        }
    }
}

pub type IocbRawPtr = *mut iocb;

/// AIO backed by an owned aligned buffer.
pub struct AIO<T> {
    iocb: Box<iocb>,
    // this is essential because libaio requires the pointer
    // to buffer keep valid during async processing.
    buf: Option<T>,
    pub key: AIOKey,
}

impl<T: AIOBuf> AIO<T> {
    #[inline]
    pub fn new(
        key: AIOKey,
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
        iocb.data = key; // store and send back via io_event
        AIO {
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

/// AIO backed by a raw pointer.
/// The raw pointer must be aigned to page and
/// has outlive the async IO call.
///
/// This struct is mainly used for page IO in a buffer pool,
/// Which pre-assign many pages through mmap() call and do
/// not release any of them. So the IO request is guaranteed
/// to be safe.
pub struct UnsafeAIO {
    iocb: Box<iocb>,
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
        let mut iocb = iocb::boxed();
        iocb.aio_fildes = fd as u32;
        iocb.aio_lio_opcode = opcode as u16;
        iocb.aio_reqprio = priority;
        iocb.buf = ptr;
        iocb.count = len as u64;
        iocb.offset = offset as u64;
        iocb.flags = flags;
        iocb.data = key; // store and send back via io_event
        UnsafeAIO { iocb, key }
    }

    #[inline]
    pub fn iocb_raw(&self) -> IocbRawPtr {
        self.iocb.as_mut_ptr()
    }
}

pub enum AIOKind {
    Read,
    Write,
}

pub enum AIOMessage<T> {
    Shutdown,
    Req(T),
}

impl<T> AIOMessage<T> {
    #[inline]
    pub fn req(self) -> Option<T> {
        match self {
            AIOMessage::Req(r) => Some(r),
            AIOMessage::Shutdown => None,
        }
    }
}

pub struct IOQueue<T> {
    // aligned with AIO interface.
    iocbs: Vec<IocbRawPtr>,
    reqs: VecDeque<T>,
}

impl<T> IOQueue<T> {
    /// Create a new IO queue with given capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        IOQueue {
            iocbs: Vec::with_capacity(capacity),
            reqs: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns length of the queue.
    #[inline]
    pub fn len(&self) -> usize {
        self.iocbs.len()
    }

    /// Returns whether the queue is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn iocbs(&self) -> &[IocbRawPtr] {
        &self.iocbs
    }

    #[inline]
    pub fn drain_to(&mut self, n: usize) -> Vec<(IocbRawPtr, T)> {
        self.iocbs.drain(0..n).zip(self.reqs.drain(0..n)).collect()
    }

    /// Checks consistency of this queue.
    #[inline]
    pub fn consistent(&self) -> bool {
        self.reqs.len() == self.iocbs.len()
    }

    #[inline]
    pub fn push(&mut self, iocb: IocbRawPtr, req: T) {
        self.iocbs.push(iocb);
        self.reqs.push_back(req);
    }
}

/// AIOKey represents the unique key of any AIO request.
pub type AIOKey = u64;

#[derive(Debug, Default)]
pub struct AIOStats {
    pub queuing: usize,
    pub running: usize,
    pub finished_reads: usize,
    pub finished_writes: usize,
    pub io_submit_count: usize,
    pub io_submit_nanos: usize,
    pub io_wait_count: usize,
    pub io_wait_nanos: usize,
}

impl AIOStats {
    #[inline]
    pub fn merge(&mut self, other: &AIOStats) {
        self.queuing += other.queuing;
        self.running += other.running;
        self.finished_reads += other.finished_reads;
        self.finished_writes += other.finished_writes;
        self.io_submit_count += other.io_submit_count;
        self.io_submit_nanos += other.io_submit_nanos;
        self.io_wait_count += other.io_wait_count;
        self.io_wait_nanos += other.io_wait_nanos;
    }
}

/// AIOEventListener defines
/// how to process with IO requests and responses.
pub trait AIOEventListener {
    type Request;
    type Submission;

    /// Called when sending request to AIO event loop.
    /// Implementor should convert the request to submission and put it in the queue.
    fn on_request(&mut self, req: Self::Request, queue: &mut IOQueue<Self::Submission>);

    /// Called when submitted to AIO context.
    fn on_submit(&mut self, sub: Self::Submission);

    /// Called when AIO is completed.
    /// The result contains number of bytes read/write, or the IO error.
    /// It's caller's responsibility to store intermediate result (especially
    /// buffer allocation) and map it via AIOKey in on_complete() function.
    /// In such way, the caller always keeps the buffer in valid state.
    fn on_complete(&mut self, key: AIOKey, res: std::io::Result<usize>) -> AIOKind;

    /// Called when stats is collected.
    fn on_stats(&mut self, stats: &AIOStats);

    /// Called when event loop is ended.
    fn end_loop(self, ctx: &AIOContext);
}

/// Client of AIO event loop, can submit IO request
/// or shutdown the execution loop.``
pub struct AIOClient<T>(Sender<AIOMessage<T>>);

impl<T> AIOClient<T> {
    /// Shutdown the event loop and wait for graceful stop.
    #[inline]
    pub fn shutdown(&self) {
        // Someone might already sent shutdown message via the channel,
        // so we ignore send error.
        let _ = self.0.send(AIOMessage::Shutdown);
    }

    /// Send IO request to AIO executor.
    #[inline]
    pub fn send(&self, req: T) -> StdResult<(), SendError<T>> {
        self.0
            .send(AIOMessage::Req(req))
            .map_err(|e| SendError(e.0.req().unwrap()))
    }

    /// Try send IO request to AIO executor.
    #[inline]
    pub fn try_send(&self, req: T) -> StdResult<(), TrySendError<T>> {
        self.0.try_send(AIOMessage::Req(req)).map_err(|e| match e {
            TrySendError::Full(v) => TrySendError::Full(v.req().unwrap()),
            TrySendError::Disconnected(v) => TrySendError::Disconnected(v.req().unwrap()),
        })
    }

    /// Send IO request to AIO executor in async way.
    #[inline]
    pub async fn send_async(&self, req: T) -> StdResult<(), SendError<T>> {
        self.0
            .send_async(AIOMessage::Req(req))
            .await
            .map_err(|e| SendError(e.0.req().unwrap()))
    }
}

impl<T> Clone for AIOClient<T> {
    #[inline]
    fn clone(&self) -> Self {
        AIOClient(self.0.clone())
    }
}

/// Event loop of AIO.
pub struct AIOEventLoop<T> {
    ctx: AIOContext,
    /// channel of IO requests.
    rx: Receiver<AIOMessage<T>>,
    // Total number of IO submitted.
    submitted: usize,
    // flag to stop the event loop.
    shutdown: bool,
}

impl<T> AIOEventLoop<T> {
    /// Start a separate thread for AIO event loop.
    pub fn start_thread<L>(self, listener: L) -> JoinHandle<()>
    where
        L: AIOEventListener<Request = T> + Send + 'static,
        L::Request: Send + 'static,
    {
        thread::spawn_named("AIOEventLoop", move || self.run(listener))
    }

    #[inline]
    fn io_depth(&self) -> usize {
        self.ctx.max_events()
    }

    #[inline]
    fn fetch_reqs<L: AIOEventListener<Request = T>>(
        &mut self,
        listener: &mut L,
        queue: &mut IOQueue<L::Submission>,
        mut min_reqs: usize,
    ) {
        let mut msg = if min_reqs > 0 {
            // won't fail because we always send Shutdown message before
            // we close sender side.
            self.rx.recv().unwrap()
        } else {
            match self.rx.try_recv() {
                Ok(m) => m,
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => unreachable!(),
            }
        };
        min_reqs = min_reqs.saturating_sub(1);
        loop {
            match msg {
                AIOMessage::Shutdown => {
                    self.shutdown = true;
                    return;
                }
                AIOMessage::Req(req) => {
                    listener.on_request(req, queue);
                }
            }
            // collapse continuous messages
            msg = if min_reqs > 0 {
                // block until next message
                // won't fail
                self.rx.recv().unwrap()
            } else {
                match self.rx.try_recv() {
                    Ok(m) => m,
                    Err(TryRecvError::Empty) => {
                        // no more messages
                        return;
                    }
                    Err(TryRecvError::Disconnected) => unreachable!(),
                }
            };
            min_reqs = min_reqs.saturating_sub(1);
        }
    }

    #[inline]
    #[cfg(feature = "libaio")]
    fn run<L: AIOEventListener<Request = T>>(mut self, mut listener: L) {
        // IO results.
        let mut results = self.ctx.events();
        // IO queue
        let mut queue: IOQueue<L::Submission> = IOQueue::with_capacity(self.io_depth());
        loop {
            debug_assert!(
                queue.consistent(),
                "pending IO number equals to pending request number"
            );
            // We only accept request if shutdown flag is false.
            if !self.shutdown {
                if queue.len() + self.submitted == 0 {
                    // there is no IO operation running.
                    self.fetch_reqs(&mut listener, &mut queue, 1);
                } else if queue.len() < self.io_depth() {
                    self.fetch_reqs(&mut listener, &mut queue, 0);
                } // otherwise, do not fetch
            }
            // Event if shutdown flag is set to true, we still process queued requests.
            let (io_submit_count, io_submit_nanos) = if !queue.is_empty() {
                let start = Instant::now();
                // Try to submit as many IO requests as possible
                debug_assert!(self.io_depth() >= self.submitted);
                let limit = self.io_depth() - self.submitted;
                let submit_count = self.ctx.submit_limit(queue.iocbs(), limit);
                // Add requests to inflight tree.
                for (_iocb, sub) in queue.drain_to(submit_count) {
                    listener.on_submit(sub);
                }
                debug_assert!(queue.consistent());
                self.submitted += submit_count;
                debug_assert!(self.submitted <= self.io_depth());
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // wait for any request to be done.
            // Note: even if we received shutdown message, we should wait all submitted IO finish before quiting.
            // This will prevent kernel from accessing a freed memory via async IO processing.
            let (io_wait_count, io_wait_nanos, finished_reads, finished_writes) = if self.submitted
                != 0
            {
                let start = Instant::now();
                let (read_count, write_count) =
                    self.ctx
                        .wait_at_least(&mut results, 1, |key, res| listener.on_complete(key, res));
                self.submitted -= read_count + write_count;
                (
                    1,
                    start.elapsed().as_nanos() as usize,
                    read_count,
                    write_count,
                )
            } else {
                (0, 0, 0, 0)
            };

            listener.on_stats(&AIOStats {
                queuing: queue.len(),
                running: self.submitted,
                finished_reads,
                finished_writes,
                io_submit_count,
                io_submit_nanos,
                io_wait_count,
                io_wait_nanos,
            });
            // only quit when shutdown flag is set and no submitted tasks.
            // all queued tasks are ignored. (is it safe???)
            if self.shutdown && self.submitted == 0 {
                break;
            }
        }
        listener.end_loop(&self.ctx);
    }

    #[inline]
    #[cfg(not(feature = "libaio"))]
    fn run<L: AIOEventListener<Request = T>>(mut self, mut listener: L) {
        // IO queue
        let mut queue: IOQueue<L::Submission> = IOQueue::with_capacity(self.io_depth());
        let (completion_tx, completion_rx) = flume::unbounded::<Completion>();
        loop {
            debug_assert!(
                queue.consistent(),
                "pending IO number equals to pending request number"
            );
            // We only accept request if shutdown flag is false.
            if !self.shutdown {
                if queue.len() + self.submitted == 0 {
                    // there is no IO operation running.
                    self.fetch_reqs(&mut listener, &mut queue, 1);
                } else if queue.len() < self.io_depth() {
                    self.fetch_reqs(&mut listener, &mut queue, 0);
                } // otherwise, do not fetch
            }
            // Event if shutdown flag is set to true, we still process queued requests.
            let (io_submit_count, io_submit_nanos) = if !queue.is_empty() {
                let start = Instant::now();
                // Try to submit as many IO requests as possible
                debug_assert!(self.io_depth() >= self.submitted);
                let limit = self.io_depth() - self.submitted;
                let submit_count = limit.min(queue.len());
                // Add requests to inflight tree.
                for (iocb, sub) in queue.drain_to(submit_count) {
                    listener.on_submit(sub);
                    let completion_tx = completion_tx.clone();
                    self.submitted += 1;
                    let iocb_addr = iocb as usize;
                    smol::spawn(async move {
                        let completion = smol::unblock(move || unsafe {
                            blocking_iocb(iocb_addr as IocbRawPtr)
                        })
                        .await;
                        let _ = completion_tx.send(completion);
                    })
                    .detach();
                }
                debug_assert!(queue.consistent());
                debug_assert!(self.submitted <= self.io_depth());
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // wait for any request to be done.
            // Note: even if we received shutdown message, we should wait all submitted IO finish before quiting.
            // This will prevent kernel from accessing a freed memory via async IO processing.
            let (io_wait_count, io_wait_nanos, finished_reads, finished_writes) =
                if self.submitted != 0 {
                    let start = Instant::now();
                    let mut read_count = 0;
                    let mut write_count = 0;
                    let mut handle_completion = |completion: Completion| match listener
                        .on_complete(completion.key, completion.res)
                    {
                        AIOKind::Read => read_count += 1,
                        AIOKind::Write => write_count += 1,
                    };
                    let completion = completion_rx.recv().unwrap();
                    handle_completion(completion);
                    while let Ok(completion) = completion_rx.try_recv() {
                        handle_completion(completion);
                    }
                    self.submitted -= read_count + write_count;
                    (
                        1,
                        start.elapsed().as_nanos() as usize,
                        read_count,
                        write_count,
                    )
                } else {
                    (0, 0, 0, 0)
                };

            listener.on_stats(&AIOStats {
                queuing: queue.len(),
                running: self.submitted,
                finished_reads,
                finished_writes,
                io_submit_count,
                io_submit_nanos,
                io_wait_count,
                io_wait_nanos,
            });
            // only quit when shutdown flag is set and no submitted tasks.
            // all queued tasks are ignored. (is it safe???)
            if self.shutdown && self.submitted == 0 {
                break;
            }
        }
        listener.end_loop(&self.ctx);
    }
}

#[inline]
pub fn pread<T: AIOBuf>(key: AIOKey, fd: RawFd, offset: usize, buf: T) -> AIO<T> {
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
pub fn pwrite<T: AIOBuf>(key: AIOKey, fd: RawFd, offset: usize, buf: T) -> AIO<T> {
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
    use crate::file::{FixedSizeBufferFreeList, SparseFile};
    use std::collections::HashMap;

    #[test]
    fn test_aio_file_ops() {
        let ctx = AIOContext::try_default().unwrap();
        let file = SparseFile::create_or_trunc("aio_file1.txt", 1024 * 1024).unwrap();
        let buf = DirectBuf::with_data(b"hello, world");
        let (offset, _) = file.alloc(buf.capacity()).unwrap();
        let aio = file.pwrite_direct(100, offset, buf);
        let mut reqs = vec![aio.iocb_raw()];
        let limit = reqs.len();
        let submit_count = ctx.submit_limit(&mut reqs, limit);
        println!("submit_count={}", submit_count);
        let mut events = ctx.events();
        ctx.wait_at_least(&mut events, 1, |key, res| {
            println!("key={}, res={:?}", key, res);
            AIOKind::Write
        });
        drop(file);
        // for test, we just remove this file
        let _ = std::fs::remove_file("aio_file1.txt");
    }

    #[test]
    fn test_aio_file_extend() {
        let file = SparseFile::create_or_trunc("aio_file2.txt", 1024 * 1024).unwrap();
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
        // for test, we just remove this file
        let _ = std::fs::remove_file("aio_file2.txt");
    }

    #[test]
    fn test_aio_event_loop() {
        let ctx = AIOContext::new(16).unwrap();

        let file = SparseFile::create_or_trunc("aio_file3.txt", 1024 * 1024).unwrap();

        let buf_free_list = FixedSizeBufferFreeList::new(4096, 4, 4);
        let listener = SimpleListener {
            file,
            map: HashMap::new(),
            key: 0,
        };
        let (event_loop, client) = ctx.event_loop();
        let handle = event_loop.start_thread(listener);

        let mut buf = buf_free_list.pop(false);
        buf.reset();
        buf.extend_from_slice(b"hello, world");

        let _ = client
            .send(Request {
                kind: AIOKind::Write,
                offset: 0,
                buf,
            })
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut elem2 = buf_free_list.pop(false);
        elem2.reset();
        let _ = client.send(Request {
            kind: AIOKind::Read,
            offset: 0,
            buf: elem2,
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        // IO operations here

        client.shutdown();
        // wait for executor to quit.
        handle.join().unwrap();

        let _ = std::fs::remove_file("aio_file3.txt");
    }

    #[test]
    #[cfg(feature = "libaio")]
    fn test_submit_limit_eagain_no_panic() {
        let ctx = AIOContext::try_default().unwrap();
        let previous = set_io_submit_hook(Some(|_, _, _| -EAGAIN));
        let iocb = iocb::boxed();
        let reqs = vec![iocb.as_mut_ptr()];
        let submit_count = ctx.submit_limit(&reqs, 1);
        set_io_submit_hook(previous);
        assert_eq!(submit_count, 0);
    }

    struct Request {
        kind: AIOKind,
        offset: usize,
        buf: DirectBuf,
    }
    struct Submission {
        kind: AIOKind,
        aio: AIO<DirectBuf>,
    }
    struct SimpleListener {
        file: SparseFile,
        map: HashMap<AIOKey, Submission>,
        key: AIOKey,
    }
    impl AIOEventListener for SimpleListener {
        type Request = Request;
        type Submission = Submission;

        fn on_request(&mut self, req: Request, queue: &mut IOQueue<Submission>) {
            // auto-generated AIO key.
            let key = self.key;
            self.key += 1;
            let (iocb, aio) = match req.kind {
                AIOKind::Read => {
                    let aio = self.file.pread_direct(key, req.offset, req.buf);
                    let iocb = aio.iocb_raw();
                    (iocb, aio)
                }
                AIOKind::Write => {
                    let aio = self.file.pwrite_direct(key, req.offset, req.buf);
                    let iocb = aio.iocb_raw();
                    (iocb, aio)
                }
            };
            queue.push(
                iocb,
                Submission {
                    kind: req.kind,
                    aio,
                },
            )
        }

        fn on_submit(&mut self, sub: Submission) {
            // here we must hold the IO buffer until syscall returns.
            let res = self.map.insert(sub.aio.key, sub);
            debug_assert!(res.is_none());
        }

        fn on_complete(&mut self, key: AIOKey, res: std::io::Result<usize>) -> AIOKind {
            match res {
                Ok(len) => {
                    let sub = self.map.remove(&key).unwrap();
                    match sub.kind {
                        AIOKind::Read => {
                            println!("read {} bytes", len);
                        }
                        AIOKind::Write => {
                            println!("write {} bytes", len);
                        }
                    }
                    let buf = sub.aio.buf.as_ref().unwrap();
                    let n = buf.data().len().min(20);
                    println!("leading {} bytes: {:?}", n, &buf.data()[..n]);
                    // Here we just drop the buffer.
                    // In actual implementation, we should send the result buffer back
                    // to client.
                    drop(sub.aio);
                    sub.kind
                }
                Err(err) => {
                    panic!("{:?}", err);
                }
            }
        }

        fn on_stats(&mut self, stats: &AIOStats) {
            println!("stats: {:?}", stats);
        }

        fn end_loop(self, _ctx: &AIOContext) {
            drop(self.file);
        }
    }
}
