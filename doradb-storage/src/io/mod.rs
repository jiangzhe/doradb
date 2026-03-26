#[cfg(all(feature = "libaio", feature = "iouring"))]
compile_error!("Enable exactly one storage IO backend feature: `libaio` or `iouring`.");
#[cfg(not(any(feature = "libaio", feature = "iouring")))]
compile_error!("One storage IO backend feature must be enabled: `libaio` or `iouring`.");

mod backend;
mod buf;
mod completion;
#[cfg(feature = "iouring")]
mod iouring_backend;
#[cfg(feature = "libaio")]
mod libaio_abi;
#[cfg(feature = "libaio")]
mod libaio_backend;

use crate::thread;
use flume::{Receiver, SendError, Sender, TryRecvError, TrySendError};
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::result::Result as StdResult;
use std::thread::JoinHandle;
use thiserror::Error;

pub(crate) use backend::IOBackendStatsHandle;
pub use backend::*;
pub use buf::*;
pub use completion::*;
#[cfg(feature = "iouring")]
pub use iouring_backend::IouringBackend;
#[cfg(feature = "libaio")]
pub use libaio_abi::*;
#[cfg(feature = "libaio")]
pub use libaio_backend::{AIO, IocbRawPtr, LibaioBackend, UnsafeAIO, pread, pwrite};

#[cfg(feature = "iouring")]
/// Canonical storage backend selected by cargo features.
pub use iouring_backend::IouringBackend as StorageBackend;
#[cfg(feature = "libaio")]
/// Canonical storage backend selected by cargo features.
pub use libaio_backend::LibaioBackend as StorageBackend;

pub const MIN_PAGE_SIZE: usize = 4096;
pub const STORAGE_SECTOR_SIZE: usize = 4096;

/// Align given input length to storage sector size.
#[inline]
pub fn align_to_sector_size(len: usize) -> usize {
    len.max(STORAGE_SECTOR_SIZE).div_ceil(STORAGE_SECTOR_SIZE) * STORAGE_SECTOR_SIZE
}

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
#[cfg(test)]
pub(crate) use self::tests::{
    StorageBackendOp, StorageBackendTestHook, set_storage_backend_test_hook,
};

/// Buffer ownership model for one backend-agnostic IO operation.
///
/// Higher layers either transfer an owned direct buffer to the worker or
/// provide a borrowed page-aligned pointer whose lifetime they keep valid until
/// completion.
pub enum IOMemory {
    Owned(DirectBuf),
    Borrowed { ptr: *mut u8, len: usize },
}

// SAFETY: borrowed pointers are only used for buffer/page memory that higher
// layers guarantee remains valid until completion, matching the old
// `UnsafeAIO` contract.
unsafe impl Send for IOMemory {}

/// Backend-agnostic description of one submitted kernel IO operation.
///
/// This type is backend-agnostic: it describes one read/write operation and
/// owns or borrows the memory that the backend will bind into its prepared
/// submission shape.
pub struct Operation {
    kind: AIOKind,
    fd: RawFd,
    offset: usize,
    memory: IOMemory,
}

impl Operation {
    /// Build one owned-buffer read operation.
    #[inline]
    pub fn pread_owned(fd: RawFd, offset: usize, buf: DirectBuf) -> Self {
        Operation {
            kind: AIOKind::Read,
            fd,
            offset,
            memory: IOMemory::Owned(buf),
        }
    }

    /// Build one owned-buffer write operation.
    #[inline]
    pub fn pwrite_owned(fd: RawFd, offset: usize, buf: DirectBuf) -> Self {
        Operation {
            kind: AIOKind::Write,
            fd,
            offset,
            memory: IOMemory::Owned(buf),
        }
    }

    /// # Safety
    ///
    /// Caller must guarantee the pointer is valid for the entire lifetime of
    /// the submitted IO and correctly aligned for the storage backend.
    /// The worker may move this completion between threads before submission.
    #[inline]
    pub unsafe fn pread_borrowed(fd: RawFd, offset: usize, ptr: *mut u8, len: usize) -> Self {
        Operation {
            kind: AIOKind::Read,
            fd,
            offset,
            memory: IOMemory::Borrowed { ptr, len },
        }
    }

    /// # Safety
    ///
    /// Caller must guarantee the pointer is valid for the entire lifetime of
    /// the submitted IO and correctly aligned for the storage backend.
    /// The worker may move this completion between threads before submission.
    #[inline]
    pub unsafe fn pwrite_borrowed(fd: RawFd, offset: usize, ptr: *mut u8, len: usize) -> Self {
        Operation {
            kind: AIOKind::Write,
            fd,
            offset,
            memory: IOMemory::Borrowed { ptr, len },
        }
    }

    /// Returns whether this operation is a read or a write.
    #[inline]
    pub fn kind(&self) -> AIOKind {
        self.kind
    }

    /// Returns the raw file descriptor targeted by this operation.
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    /// Returns the byte offset used for this operation.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the byte length of the bound buffer or pointer.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.memory {
            IOMemory::Owned(buf) => buf.capacity(),
            IOMemory::Borrowed { len, .. } => *len,
        }
    }

    /// Returns whether this operation targets an empty buffer.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Takes ownership of the direct buffer if this completion owns one.
    #[inline]
    pub fn take_buf(&mut self) -> Option<DirectBuf> {
        match std::mem::replace(
            &mut self.memory,
            IOMemory::Borrowed {
                ptr: std::ptr::null_mut(),
                len: 0,
            },
        ) {
            IOMemory::Owned(buf) => Some(buf),
            other => {
                self.memory = other;
                None
            }
        }
    }

    /// Returns a shared reference to the owned direct buffer, if present.
    #[inline]
    pub fn buf(&self) -> Option<&DirectBuf> {
        match &self.memory {
            IOMemory::Owned(buf) => Some(buf),
            IOMemory::Borrowed { .. } => None,
        }
    }

    #[inline]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.memory {
            IOMemory::Owned(buf) => buf.as_bytes_mut().as_mut_ptr(),
            IOMemory::Borrowed { ptr, .. } => *ptr,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    reqs: VecDeque<T>,
}

impl<T> IOQueue<T> {
    /// Create a new IO queue with given capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        IOQueue {
            reqs: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns length of the queue.
    #[inline]
    pub fn len(&self) -> usize {
        self.reqs.len()
    }

    /// Returns whether the queue is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn drain_to(&mut self, n: usize) -> Vec<T> {
        let count = n.min(self.reqs.len());
        self.reqs.drain(0..count).collect()
    }

    /// Checks consistency of this queue.
    #[inline]
    pub fn consistent(&self) -> bool {
        true
    }

    #[inline]
    pub fn push(&mut self, req: T) {
        self.reqs.push_back(req);
    }
}

/// One worker-owned submission staged for backend preparation and completion.
///
/// The state-machine key is opaque to the generic worker. Domain-specific
/// inflight dedupe and ordering live above the worker in the state machine and
/// its owning subsystem.
pub trait IOSubmission {
    type Key;

    /// Returns the domain key associated with this submission.
    fn key(&self) -> &Self::Key;
    /// Returns the backend-agnostic IO operation to prepare and submit.
    fn operation(&mut self) -> &mut Operation;
}

/// AIOKey represents the unique key of any AIO request.
pub type AIOKey = u64;

const INVALID_SLOT: u32 = u32::MAX;

enum Entry<T> {
    Occupied(T),
    Vacant(u32),
}

struct Slot<T> {
    generation: u32,
    entry: Entry<T>,
}

struct InflightSlots<T> {
    slots: Vec<Slot<T>>,
    free_head: u32,
}

impl<T> InflightSlots<T> {
    #[inline]
    fn new(capacity: usize) -> Self {
        assert!(capacity <= u32::MAX as usize);
        let mut slots = Vec::with_capacity(capacity);
        for idx in 0..capacity {
            let next = if idx + 1 < capacity {
                (idx + 1) as u32
            } else {
                INVALID_SLOT
            };
            slots.push(Slot {
                generation: 0,
                entry: Entry::Vacant(next),
            });
        }
        InflightSlots {
            slots,
            free_head: if capacity == 0 { INVALID_SLOT } else { 0 },
        }
    }

    #[inline]
    fn has_vacant(&self) -> bool {
        self.free_head != INVALID_SLOT
    }

    #[inline]
    fn reserve(&mut self) -> Option<(BackendToken, u32)> {
        let slot = self.free_head;
        if slot == INVALID_SLOT {
            return None;
        }
        let slot_ref = &self.slots[slot as usize];
        let Entry::Vacant(next) = slot_ref.entry else {
            unreachable!("free list head must be vacant");
        };
        self.free_head = next;
        Some((BackendToken::new(slot_ref.generation, slot), slot))
    }

    #[inline]
    fn occupy_reserved(&mut self, slot: u32, value: T) {
        let slot_ref = &mut self.slots[slot as usize];
        debug_assert!(matches!(slot_ref.entry, Entry::Vacant(_)));
        slot_ref.entry = Entry::Occupied(value);
    }

    #[inline]
    fn get_mut(&mut self, slot: u32) -> &mut T {
        match &mut self.slots[slot as usize].entry {
            Entry::Occupied(value) => value,
            Entry::Vacant(_) => panic!("slot {slot} is not occupied"),
        }
    }

    #[inline]
    fn take(&mut self, token: BackendToken) -> T {
        let slot = token.slot_index();
        let generation = token.generation();
        let slot_ref = self.slots.get_mut(slot as usize).unwrap_or_else(|| {
            panic!(
                "completion references invalid inflight slot: token={}",
                token.raw()
            )
        });
        assert!(
            slot_ref.generation == generation,
            "completion references stale inflight token: token={} slot_generation={}",
            token.raw(),
            slot_ref.generation
        );
        let prev_head = self.free_head;
        let entry = std::mem::replace(&mut slot_ref.entry, Entry::Vacant(prev_head));
        slot_ref.generation = slot_ref.generation.wrapping_add(1);
        self.free_head = slot;
        match entry {
            Entry::Occupied(value) => value,
            Entry::Vacant(_) => panic!(
                "completion references vacant inflight slot: token={}",
                token.raw()
            ),
        }
    }
}

struct InflightEntry<S, P> {
    submission: S,
    _prepared: P,
    submitted: bool,
}

/// IOStateMachine defines how one worker maps requests to submissions and
/// applies completion-side state transitions.
pub trait IOStateMachine {
    type Request;
    /// Opaque domain identity attached to each submission.
    ///
    /// The generic worker does not interpret this key. Subsystems use it for
    /// their own inflight maps, dedupe, and debugging.
    type Key;
    type Submission: IOSubmission<Key = Self::Key>;

    /// Called when receiving a new request from the worker channel.
    ///
    /// `max_new` is the remaining submission headroom for this worker
    /// iteration. Implementations must enqueue at most `max_new`
    /// submissions into `queue`. If the request expands into more
    /// submissions than fit, return the unprocessed remainder so the worker
    /// can resume it after local depth drops.
    fn prepare_request(
        &mut self,
        req: Self::Request,
        max_new: usize,
        queue: &mut IOQueue<Self::Submission>,
    ) -> Option<Self::Request>;

    /// Called after one submission is accepted by the kernel.
    fn on_submit(&mut self, sub: &Self::Submission);

    /// Called when AIO is completed.
    /// The result contains number of bytes read/write, or the IO error.
    fn on_complete(&mut self, sub: Self::Submission, res: std::io::Result<usize>) -> AIOKind;

    /// Called when event loop is ended.
    fn end_loop(self);
}

/// Cloneable sender used to enqueue requests into one [`IOWorker`].
///
/// The client only transports state-machine requests. It does not own any
/// backend-specific submission state.
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

/// Delayed worker construction until the owner can provide the concrete state machine.
///
/// The builder fixes the backend and request channel first, then binds one
/// concrete [`IOStateMachine`] later.
pub struct IOWorkerBuilder<T, B = StorageBackend> {
    backend: B,
    rx: Receiver<AIOMessage<T>>,
}

impl<T, B> IOWorkerBuilder<T, B>
where
    B: IOBackend,
{
    /// Attaches one state machine to this backend-bound worker builder.
    #[inline]
    pub fn bind<S>(self, state_machine: S) -> IOWorker<T, S, B>
    where
        S: IOStateMachine<Request = T>,
    {
        let io_depth = self.backend.max_events();
        let submit_batch = self.backend.new_submit_batch();
        IOWorker {
            backend: self.backend,
            rx: self.rx,
            submitted: 0,
            shutdown: false,
            deferred_req: None,
            staged_slots: VecDeque::new(),
            submit_batch,
            slots: InflightSlots::new(io_depth),
            state_machine,
        }
    }
}

/// Concrete batch-oriented IO worker with one state machine implementation.
///
/// The worker owns:
/// - request receipt from the channel
/// - inflight slot allocation and ABA-safe completion tokens
/// - backend preparation and batch submission
/// - dispatch back into the state machine on submit and completion
///
/// Domain-level dedupe and same-key conflict policy remain inside the state
/// machine and its owning subsystem.
pub struct IOWorker<T, S: IOStateMachine<Request = T>, B: IOBackend = StorageBackend> {
    backend: B,
    rx: Receiver<AIOMessage<T>>,
    submitted: usize,
    shutdown: bool,
    deferred_req: Option<T>,
    staged_slots: VecDeque<u32>,
    submit_batch: B::SubmitBatch,
    slots: InflightSlots<InflightEntry<S::Submission, B::Prepared>>,
    state_machine: S,
}

impl<T, S, B> IOWorker<T, S, B>
where
    S: IOStateMachine<Request = T>,
    S::Request: Send + 'static,
    S::Submission: Send + 'static,
    B: IOBackend,
    B::Prepared: Send + 'static,
    B::SubmitBatch: Send + 'static,
{
    /// Starts a dedicated thread that runs this worker until shutdown and drain.
    pub fn start_thread(self) -> JoinHandle<()>
    where
        S: Send + 'static,
        B: Send + 'static,
    {
        thread::spawn_named("IOWorker", move || self.run())
    }

    #[inline]
    fn io_depth(&self) -> usize {
        self.backend.max_events()
    }

    #[inline]
    fn local_depth(&self, queue: &IOQueue<S::Submission>) -> usize {
        queue.len() + self.staged_slots.len() + self.submitted
    }

    #[inline]
    fn fetch_reqs(&mut self, queue: &mut IOQueue<S::Submission>, mut min_reqs: usize) {
        loop {
            if self.local_depth(queue) >= self.io_depth() {
                return;
            }

            if let Some(req) = self.deferred_req.take() {
                let headroom = self.io_depth() - self.local_depth(queue);
                self.deferred_req = self.state_machine.prepare_request(req, headroom, queue);
                min_reqs = min_reqs.saturating_sub(1);
                if self.deferred_req.is_some() || self.local_depth(queue) >= self.io_depth() {
                    return;
                }
                continue;
            }

            let msg = if min_reqs > 0 {
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

            match msg {
                AIOMessage::Shutdown => {
                    self.shutdown = true;
                    return;
                }
                AIOMessage::Req(req) => {
                    let headroom = self.io_depth() - self.local_depth(queue);
                    self.deferred_req = self.state_machine.prepare_request(req, headroom, queue);
                    if self.deferred_req.is_some() || self.local_depth(queue) >= self.io_depth() {
                        return;
                    }
                }
            }
        }
    }

    #[inline]
    fn prepare_staged(&mut self, queue: &mut IOQueue<S::Submission>) {
        while self.slots.has_vacant() {
            let Some(mut sub) = queue.reqs.pop_front() else {
                break;
            };
            let (token, slot) = self
                .slots
                .reserve()
                .expect("slot reservation should succeed while vacant slots exist");
            let mut prepared = self.backend.prepare(token, sub.operation());
            self.backend
                .push_prepared(&mut self.submit_batch, &mut prepared);
            self.slots.occupy_reserved(
                slot,
                InflightEntry {
                    submission: sub,
                    _prepared: prepared,
                    submitted: false,
                },
            );
            self.staged_slots.push_back(slot);
        }
    }

    fn run(mut self) {
        // IO results.
        let mut results = self.backend.new_events();
        // IO queue
        let mut queue: IOQueue<S::Submission> = IOQueue::with_capacity(self.io_depth());
        loop {
            debug_assert!(
                queue.consistent(),
                "pending IO number equals to pending request number"
            );
            // We only accept new channel requests if shutdown flag is false.
            // Deferred request remainders are already worker-owned, so they
            // must still be expanded and drained after shutdown.
            if self.deferred_req.is_some() {
                self.fetch_reqs(&mut queue, 0);
            } else if !self.shutdown {
                if self.local_depth(&queue) == 0 {
                    // there is no IO operation running.
                    self.fetch_reqs(&mut queue, 1);
                } else if self.local_depth(&queue) < self.io_depth() {
                    self.fetch_reqs(&mut queue, 0);
                } // otherwise, do not fetch
            }
            self.prepare_staged(&mut queue);
            // Event if shutdown flag is set to true, we still process queued requests.
            if !self.staged_slots.is_empty() {
                // Try to submit as many IO requests as possible
                debug_assert!(self.io_depth() >= self.submitted);
                let limit = self.io_depth() - self.submitted;
                let submit_count = self.backend.submit_batch(&mut self.submit_batch, limit);
                #[cfg(test)]
                let hook = tests::current_storage_backend_test_hook();
                for _ in 0..submit_count {
                    let slot = self
                        .staged_slots
                        .pop_front()
                        .expect("submitted slots must have queued staging order");
                    let entry = self.slots.get_mut(slot);
                    debug_assert!(!entry.submitted);
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        let op = entry.submission.operation();
                        hook.on_submit(StorageBackendOp::new(op.kind(), op.fd(), op.offset()));
                    }
                    self.state_machine.on_submit(&entry.submission);
                    entry.submitted = true;
                }
                debug_assert!(queue.consistent());
                self.submitted += submit_count;
                debug_assert!(self.submitted <= self.io_depth());
            }

            // wait for any request to be done.
            // Note: even if we received shutdown message, we should wait all submitted IO finish before quiting.
            // This will prevent kernel from accessing a freed memory via async IO processing.
            if self.submitted != 0 {
                let completions = self.backend.wait_at_least(&mut results, 1);
                let completed_count = completions.len();
                for (token, res) in completions {
                    let entry = self.slots.take(token);
                    debug_assert!(entry.submitted);
                    #[cfg(test)]
                    let (entry, res) = {
                        let mut entry = entry;
                        let mut res = res;
                        if let Some(hook) = tests::current_storage_backend_test_hook() {
                            let op = entry.submission.operation();
                            hook.on_complete(
                                StorageBackendOp::new(op.kind(), op.fd(), op.offset()),
                                &mut res,
                            );
                        }
                        (entry, res)
                    };
                    let _ = self.state_machine.on_complete(entry.submission, res);
                }
                self.submitted -= completed_count;
            }
            // Drain local queues before quitting so backend-staged submissions
            // and worker-owned request state are not dropped silently.
            if self.shutdown
                && self.submitted == 0
                && self.deferred_req.is_none()
                && self.staged_slots.is_empty()
                && queue.is_empty()
            {
                break;
            }
        }
        self.state_machine.end_loop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StorageBackendOp {
        kind: AIOKind,
        fd: RawFd,
        offset: usize,
    }

    impl StorageBackendOp {
        #[inline]
        pub(super) fn new(kind: AIOKind, fd: RawFd, offset: usize) -> Self {
            Self { kind, fd, offset }
        }

        #[inline]
        pub(crate) fn kind(&self) -> AIOKind {
            self.kind
        }

        #[inline]
        pub(crate) fn fd(&self) -> RawFd {
            self.fd
        }

        #[inline]
        pub(crate) fn offset(&self) -> usize {
            self.offset
        }
    }

    pub(crate) trait StorageBackendTestHook: Send + Sync {
        fn on_submit(&self, _op: StorageBackendOp) {}

        fn on_complete(&self, _op: StorageBackendOp, _res: &mut StdResult<usize, std::io::Error>) {}
    }

    type StorageBackendHook = Arc<dyn StorageBackendTestHook>;

    static STORAGE_BACKEND_TEST_HOOK: std::sync::Mutex<Option<StorageBackendHook>> =
        std::sync::Mutex::new(None);

    #[inline]
    pub(super) fn current_storage_backend_test_hook() -> Option<StorageBackendHook> {
        STORAGE_BACKEND_TEST_HOOK.lock().unwrap().clone()
    }

    #[inline]
    pub(crate) fn set_storage_backend_test_hook(
        hook: Option<StorageBackendHook>,
    ) -> Option<StorageBackendHook> {
        let mut guard = STORAGE_BACKEND_TEST_HOOK.lock().unwrap();
        std::mem::replace(&mut *guard, hook)
    }

    #[test]
    fn test_io_queue_drain_to_clamps_to_remaining_len() {
        let mut queue = IOQueue::with_capacity(2);
        queue.push(1u32);
        queue.push(2u32);

        let drained = queue.drain_to(5);
        assert_eq!(drained, vec![1, 2]);
        assert!(queue.is_empty());
    }

    #[derive(Clone, Copy)]
    struct TestBackend {
        max_events: usize,
    }

    impl IOBackend for TestBackend {
        type Prepared = ();
        type SubmitBatch = ();
        type Events = ();

        fn max_events(&self) -> usize {
            self.max_events
        }

        fn new_submit_batch(&self) -> Self::SubmitBatch {}

        fn new_events(&self) -> Self::Events {}

        fn prepare(&mut self, _token: BackendToken, _operation: &mut Operation) -> Self::Prepared {
            unreachable!("test backend does not stage kernel IO")
        }

        fn push_prepared(
            &mut self,
            _batch: &mut Self::SubmitBatch,
            _prepared: &mut Self::Prepared,
        ) {
            unreachable!("test backend does not stage kernel IO")
        }

        fn submit_batch(&mut self, _batch: &mut Self::SubmitBatch, _limit: usize) -> usize {
            unreachable!("test backend does not submit kernel IO")
        }

        fn wait_at_least(
            &mut self,
            _events: &mut Self::Events,
            _min_nr: usize,
        ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
            unreachable!("test backend does not wait for kernel IO")
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ExpandRequest {
        remaining: usize,
    }

    struct ExpandSubmission {
        key: u64,
        operation: Operation,
    }

    impl IOSubmission for ExpandSubmission {
        type Key = u64;

        fn key(&self) -> &Self::Key {
            &self.key
        }

        fn operation(&mut self) -> &mut Operation {
            &mut self.operation
        }
    }

    #[derive(Default)]
    struct ExpandingStateMachine {
        next_key: u64,
    }

    impl IOStateMachine for ExpandingStateMachine {
        type Request = ExpandRequest;
        type Key = u64;
        type Submission = ExpandSubmission;

        fn prepare_request(
            &mut self,
            mut req: ExpandRequest,
            max_new: usize,
            queue: &mut IOQueue<ExpandSubmission>,
        ) -> Option<ExpandRequest> {
            let emit = req.remaining.min(max_new);
            for _ in 0..emit {
                let key = self.next_key;
                self.next_key += 1;
                queue.push(ExpandSubmission {
                    key,
                    operation: Operation::pwrite_owned(
                        -1,
                        0,
                        DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
                    ),
                });
            }
            req.remaining -= emit;
            (req.remaining != 0).then_some(req)
        }

        fn on_submit(&mut self, _sub: &ExpandSubmission) {}

        fn on_complete(&mut self, _sub: ExpandSubmission, _res: std::io::Result<usize>) -> AIOKind {
            unreachable!("fetch-only tests never complete IO")
        }

        fn end_loop(self) {}
    }

    #[test]
    fn test_fetch_reqs_stops_after_request_expands_to_io_depth() {
        let (tx, rx) = flume::unbounded();
        let mut worker = IOWorker {
            backend: TestBackend { max_events: 2 },
            rx,
            submitted: 0,
            shutdown: false,
            deferred_req: None,
            staged_slots: VecDeque::new(),
            submit_batch: (),
            slots: InflightSlots::new(2),
            state_machine: ExpandingStateMachine::default(),
        };
        let mut queue = IOQueue::with_capacity(2);

        tx.send(AIOMessage::Req(ExpandRequest { remaining: 5 }))
            .unwrap();
        worker.fetch_reqs(&mut queue, 1);

        assert_eq!(queue.len(), 2);
        assert_eq!(worker.local_depth(&queue), 2);
        assert_eq!(worker.deferred_req, Some(ExpandRequest { remaining: 3 }));
    }

    #[test]
    fn test_fetch_reqs_counts_submitted_work_against_io_depth() {
        let (tx, rx) = flume::unbounded();
        let mut worker = IOWorker {
            backend: TestBackend { max_events: 2 },
            rx,
            submitted: 1,
            shutdown: false,
            deferred_req: None,
            staged_slots: VecDeque::new(),
            submit_batch: (),
            slots: InflightSlots::new(2),
            state_machine: ExpandingStateMachine::default(),
        };
        let mut queue = IOQueue::with_capacity(2);

        tx.send(AIOMessage::Req(ExpandRequest { remaining: 1 }))
            .unwrap();
        tx.send(AIOMessage::Req(ExpandRequest { remaining: 1 }))
            .unwrap();

        worker.fetch_reqs(&mut queue, 0);
        assert_eq!(queue.len(), 1);
        assert_eq!(worker.local_depth(&queue), 2);
        assert!(worker.deferred_req.is_none());

        worker.submitted = 0;
        worker.fetch_reqs(&mut queue, 0);
        assert_eq!(queue.len(), 2);
        assert_eq!(worker.local_depth(&queue), 2);
    }
}
