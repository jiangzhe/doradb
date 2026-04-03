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
use flume::{Receiver, RecvError, Selector, SendError, Sender, TryRecvError, TrySendError};
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
const DEFAULT_AIO_EVENT_LOOP_BACKLOG: usize = 10;
const UNBOUNDED_LANE_BURST_LIMIT: usize = usize::MAX;

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

/// Worker construction parameters for one logical ingress lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IOLaneConfig {
    backlog: usize,
    burst_limit_ops: usize,
}

impl IOLaneConfig {
    /// Creates one ingress-lane config with bounded queue capacity and a per-turn burst cap.
    #[allow(dead_code)]
    #[inline]
    pub(crate) const fn new(backlog: usize, burst_limit_ops: usize) -> Self {
        IOLaneConfig {
            backlog,
            burst_limit_ops,
        }
    }

    #[inline]
    const fn single_lane(backlog: usize) -> Self {
        IOLaneConfig {
            backlog,
            burst_limit_ops: UNBOUNDED_LANE_BURST_LIMIT,
        }
    }
}

struct SchedulerLaneBuilder<T> {
    rx: Receiver<AIOMessage<T>>,
    burst_limit_ops: usize,
}

struct RequestSchedulerBuilder<T> {
    lanes: Box<[SchedulerLaneBuilder<T>]>,
}

impl<T> RequestSchedulerBuilder<T> {
    #[inline]
    fn single_lane(backlog: usize) -> (Self, AIOClient<T>) {
        let lane_config = IOLaneConfig::single_lane(backlog);
        let (tx, rx) = flume::bounded(backlog);
        let builder = RequestSchedulerBuilder {
            lanes: vec![SchedulerLaneBuilder {
                rx,
                burst_limit_ops: lane_config.burst_limit_ops,
            }]
            .into_boxed_slice(),
        };
        (builder, AIOClient(tx))
    }

    #[inline]
    fn try_new(lane_configs: &[IOLaneConfig]) -> AIOResult<(Self, Vec<AIOClient<T>>)> {
        if lane_configs.is_empty() {
            return Err(AIOError::SetupError);
        }
        let mut lanes = Vec::with_capacity(lane_configs.len());
        let mut clients = Vec::with_capacity(lane_configs.len());
        for lane_config in lane_configs {
            if lane_config.burst_limit_ops == 0 {
                return Err(AIOError::SetupError);
            }
            let (tx, rx) = flume::bounded(lane_config.backlog);
            lanes.push(SchedulerLaneBuilder {
                rx,
                burst_limit_ops: lane_config.burst_limit_ops,
            });
            clients.push(AIOClient(tx));
        }
        Ok((
            RequestSchedulerBuilder {
                lanes: lanes.into_boxed_slice(),
            },
            clients,
        ))
    }

    #[inline]
    fn build(self) -> RequestScheduler<T> {
        let lanes = self
            .lanes
            .into_vec()
            .into_iter()
            .map(|lane| SchedulerLane {
                rx: lane.rx,
                deferred_req: None,
                burst_limit_ops: lane.burst_limit_ops,
                shutdown: false,
            })
            .collect();
        RequestScheduler { lanes, cursor: 0 }
    }
}

struct SchedulerLane<T> {
    rx: Receiver<AIOMessage<T>>,
    deferred_req: Option<T>,
    burst_limit_ops: usize,
    shutdown: bool,
}

enum LaneTurnStart<T> {
    Deferred(usize),
    Message(usize, AIOMessage<T>),
    Disconnected(usize),
}

struct RequestScheduler<T> {
    lanes: Box<[SchedulerLane<T>]>,
    cursor: usize,
}

impl<T> RequestScheduler<T> {
    #[inline]
    fn all_shutdown(&self) -> bool {
        self.lanes.iter().all(|lane| lane.shutdown)
    }

    #[inline]
    fn has_deferred(&self) -> bool {
        self.lanes.iter().any(|lane| lane.deferred_req.is_some())
    }

    #[inline]
    fn next_lane_idx(&self, lane_idx: usize) -> usize {
        if self.lanes.len() == 1 {
            0
        } else {
            (lane_idx + 1) % self.lanes.len()
        }
    }

    #[inline]
    fn local_depth<S>(
        &self,
        queue: &IOQueue<S>,
        staged_slots_len: usize,
        submitted: usize,
    ) -> usize {
        queue.len() + staged_slots_len + submitted
    }

    fn next_ready_turn_start(&mut self) -> Option<LaneTurnStart<T>> {
        for offset in 0..self.lanes.len() {
            let lane_idx = (self.cursor + offset) % self.lanes.len();
            let lane = &mut self.lanes[lane_idx];
            if lane.deferred_req.is_some() {
                return Some(LaneTurnStart::Deferred(lane_idx));
            }
            if lane.shutdown {
                continue;
            }
            match lane.rx.try_recv() {
                Ok(msg) => return Some(LaneTurnStart::Message(lane_idx, msg)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    return Some(LaneTurnStart::Disconnected(lane_idx));
                }
            }
        }
        None
    }

    fn wait_turn_start(&mut self) -> Option<LaneTurnStart<T>> {
        if self.all_shutdown() {
            return None;
        }
        let mut selector = Selector::new();
        let mut active_lanes = 0usize;
        for offset in 0..self.lanes.len() {
            let lane_idx = (self.cursor + offset) % self.lanes.len();
            let lane = &self.lanes[lane_idx];
            if lane.shutdown {
                continue;
            }
            active_lanes += 1;
            selector = selector.recv(&lane.rx, move |res| match res {
                Ok(msg) => LaneTurnStart::Message(lane_idx, msg),
                Err(RecvError::Disconnected) => LaneTurnStart::Disconnected(lane_idx),
            });
        }
        if active_lanes == 0 {
            None
        } else {
            Some(selector.wait())
        }
    }

    fn fetch_reqs<S>(
        &mut self,
        state_machine: &mut S,
        queue: &mut IOQueue<S::Submission>,
        staged_slots_len: usize,
        submitted: usize,
        io_depth: usize,
        mut min_turns: usize,
    ) where
        S: IOStateMachine<Request = T>,
    {
        loop {
            if self.local_depth(queue, staged_slots_len, submitted) >= io_depth {
                return;
            }

            let Some(turn_start) = self.next_ready_turn_start().or_else(|| {
                if min_turns == 0 {
                    None
                } else {
                    self.wait_turn_start()
                }
            }) else {
                return;
            };
            min_turns = min_turns.saturating_sub(1);
            self.process_turn(
                turn_start,
                state_machine,
                queue,
                staged_slots_len,
                submitted,
                io_depth,
            );
        }
    }

    fn process_turn<S>(
        &mut self,
        turn_start: LaneTurnStart<T>,
        state_machine: &mut S,
        queue: &mut IOQueue<S::Submission>,
        staged_slots_len: usize,
        submitted: usize,
        io_depth: usize,
    ) where
        S: IOStateMachine<Request = T>,
    {
        let lane_idx = match turn_start {
            LaneTurnStart::Deferred(lane_idx)
            | LaneTurnStart::Message(lane_idx, _)
            | LaneTurnStart::Disconnected(lane_idx) => lane_idx,
        };
        let headroom = io_depth - self.local_depth(queue, staged_slots_len, submitted);
        debug_assert!(headroom != 0);

        let lane = &mut self.lanes[lane_idx];
        let mut budget = headroom.min(lane.burst_limit_ops);
        let mut first_msg = match turn_start {
            LaneTurnStart::Deferred(_) => None,
            LaneTurnStart::Message(_, msg) => Some(msg),
            LaneTurnStart::Disconnected(_) => {
                lane.shutdown = true;
                None
            }
        };

        while budget != 0 {
            if let Some(req) = lane.deferred_req.take() {
                let queue_len = queue.len();
                lane.deferred_req = state_machine.prepare_request(req, budget, queue);
                let admitted = queue.len() - queue_len;
                debug_assert!(
                    admitted <= budget,
                    "state machine admitted more operations than burst budget"
                );
                budget -= admitted;
                if lane.deferred_req.is_some() {
                    break;
                }
                continue;
            }

            let next_msg = match first_msg.take() {
                Some(msg) => Some(msg),
                None if lane.shutdown => None,
                None => match lane.rx.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        lane.shutdown = true;
                        None
                    }
                },
            };
            let Some(msg) = next_msg else {
                break;
            };
            match msg {
                AIOMessage::Shutdown => {
                    lane.shutdown = true;
                    break;
                }
                AIOMessage::Req(req) => {
                    let queue_len = queue.len();
                    lane.deferred_req = state_machine.prepare_request(req, budget, queue);
                    let admitted = queue.len() - queue_len;
                    debug_assert!(
                        admitted <= budget,
                        "state machine admitted more operations than burst budget"
                    );
                    budget -= admitted;
                    if lane.deferred_req.is_some() {
                        break;
                    }
                }
            }
        }

        self.cursor = self.next_lane_idx(lane_idx);
    }
}

/// Cloneable sender used to enqueue requests into one [`IOWorker`].
///
/// The client only transports state-machine requests. It does not own any
/// backend-specific submission state.
pub struct AIOClient<T>(Sender<AIOMessage<T>>);

impl<T> AIOClient<T> {
    /// Signals that this ingress lane is finished and contributes to worker shutdown.
    ///
    /// A single-lane worker stops after receiving this message. Multi-lane
    /// workers stop once every lane has shut down and all queued, deferred,
    /// staged, and inflight work has drained.
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
    scheduler: RequestSchedulerBuilder<T>,
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
            scheduler: self.scheduler.build(),
            submitted: 0,
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
    scheduler: RequestScheduler<T>,
    submitted: usize,
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
    fn fetch_reqs(&mut self, queue: &mut IOQueue<S::Submission>, min_reqs: usize) {
        let staged_slots_len = self.staged_slots.len();
        let submitted = self.submitted;
        let io_depth = self.io_depth();
        self.scheduler.fetch_reqs(
            &mut self.state_machine,
            queue,
            staged_slots_len,
            submitted,
            io_depth,
            min_reqs,
        );
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
            if self.scheduler.has_deferred() {
                self.fetch_reqs(&mut queue, 0);
            } else if !self.scheduler.all_shutdown() {
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
            if self.scheduler.all_shutdown()
                && self.submitted == 0
                && !self.scheduler.has_deferred()
                && self.staged_slots.is_empty()
                && queue.is_empty()
            {
                break;
            }
        }
        self.state_machine.end_loop();
    }
}

#[inline]
fn build_io_worker<T, B>(backend: B) -> (IOWorkerBuilder<T, B>, AIOClient<T>) {
    let (scheduler, client) = RequestSchedulerBuilder::single_lane(DEFAULT_AIO_EVENT_LOOP_BACKLOG);
    (IOWorkerBuilder { backend, scheduler }, client)
}

type MultiLaneWorkerBuild<T, B> = (IOWorkerBuilder<T, B>, Vec<AIOClient<T>>);

#[inline]
pub(crate) fn build_io_worker_lanes<T, B>(
    backend: B,
    lane_configs: &[IOLaneConfig],
) -> AIOResult<MultiLaneWorkerBuild<T, B>> {
    let (scheduler, clients) = RequestSchedulerBuilder::try_new(lane_configs)?;
    Ok((IOWorkerBuilder { backend, scheduler }, clients))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ExpandRequest {
        lane: usize,
        remaining: usize,
    }

    struct ExpandSubmission {
        key: u64,
        lane: usize,
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
                    lane: req.lane,
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

    fn test_scheduler<T>(
        lane_configs: &[IOLaneConfig],
    ) -> (RequestScheduler<T>, Vec<AIOClient<T>>) {
        let (builder, clients) =
            build_io_worker_lanes(TestBackend { max_events: 4 }, lane_configs).unwrap();
        (builder.scheduler.build(), clients)
    }

    fn test_single_lane_worker(
        max_events: usize,
        submitted: usize,
    ) -> (
        IOWorker<ExpandRequest, ExpandingStateMachine, TestBackend>,
        AIOClient<ExpandRequest>,
    ) {
        let (builder, client) = build_io_worker(TestBackend { max_events });
        let mut worker = builder.bind(ExpandingStateMachine::default());
        worker.submitted = submitted;
        (worker, client)
    }

    fn test_multi_lane_worker(
        max_events: usize,
        submitted: usize,
        lane_configs: &[IOLaneConfig],
    ) -> (
        IOWorker<ExpandRequest, ExpandingStateMachine, TestBackend>,
        Vec<AIOClient<ExpandRequest>>,
    ) {
        let (builder, clients) = build_io_worker_lanes(TestBackend { max_events }, lane_configs)
            .expect("lane configs should be valid");
        let mut worker = builder.bind(ExpandingStateMachine::default());
        worker.submitted = submitted;
        (worker, clients)
    }

    struct ImmediateBackend {
        max_events: usize,
        inflight: VecDeque<BackendToken>,
    }

    impl ImmediateBackend {
        #[inline]
        fn new(max_events: usize) -> Self {
            ImmediateBackend {
                max_events,
                inflight: VecDeque::new(),
            }
        }
    }

    impl IOBackend for ImmediateBackend {
        type Prepared = BackendToken;
        type SubmitBatch = VecDeque<BackendToken>;
        type Events = ();

        fn max_events(&self) -> usize {
            self.max_events
        }

        fn new_submit_batch(&self) -> Self::SubmitBatch {
            VecDeque::with_capacity(self.max_events)
        }

        fn new_events(&self) -> Self::Events {}

        fn prepare(&mut self, token: BackendToken, _operation: &mut Operation) -> Self::Prepared {
            token
        }

        fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
            batch.push_back(*prepared);
        }

        fn submit_batch(&mut self, batch: &mut Self::SubmitBatch, limit: usize) -> usize {
            let submit_count = limit.min(batch.len());
            for _ in 0..submit_count {
                let token = batch
                    .pop_front()
                    .expect("submit batch length must match queued tokens");
                self.inflight.push_back(token);
            }
            submit_count
        }

        fn wait_at_least(
            &mut self,
            _events: &mut Self::Events,
            min_nr: usize,
        ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
            assert!(
                self.inflight.len() >= min_nr,
                "immediate backend requires enough inflight work to satisfy wait"
            );
            let mut completions = Vec::with_capacity(self.inflight.len());
            while let Some(token) = self.inflight.pop_front() {
                completions.push((token, Ok(STORAGE_SECTOR_SIZE)));
            }
            completions
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct RecordedRequest {
        lane: usize,
        tag: &'static str,
        next_op: usize,
        remaining: usize,
    }

    impl RecordedRequest {
        #[inline]
        fn new(lane: usize, tag: &'static str, remaining: usize) -> Self {
            RecordedRequest {
                lane,
                tag,
                next_op: 0,
                remaining,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct RecordedOp {
        lane: usize,
        tag: &'static str,
        op_idx: usize,
    }

    struct RecordedSubmission {
        key: u64,
        op: RecordedOp,
        operation: Operation,
    }

    impl IOSubmission for RecordedSubmission {
        type Key = u64;

        fn key(&self) -> &Self::Key {
            &self.key
        }

        fn operation(&mut self) -> &mut Operation {
            &mut self.operation
        }
    }

    #[derive(Default)]
    struct RecordingLog {
        submits: std::sync::Mutex<Vec<RecordedOp>>,
        completes: std::sync::Mutex<Vec<RecordedOp>>,
        end_count: AtomicUsize,
    }

    struct RecordingStateMachine {
        next_key: u64,
        log: Arc<RecordingLog>,
        submit_tx: Option<flume::Sender<RecordedOp>>,
    }

    type RecordingWorkerParts = (
        IOWorker<RecordedRequest, RecordingStateMachine, ImmediateBackend>,
        Vec<AIOClient<RecordedRequest>>,
        Arc<RecordingLog>,
    );

    impl RecordingStateMachine {
        #[inline]
        fn new(log: Arc<RecordingLog>, submit_tx: Option<flume::Sender<RecordedOp>>) -> Self {
            RecordingStateMachine {
                next_key: 0,
                log,
                submit_tx,
            }
        }
    }

    impl IOStateMachine for RecordingStateMachine {
        type Request = RecordedRequest;
        type Key = u64;
        type Submission = RecordedSubmission;

        fn prepare_request(
            &mut self,
            mut req: RecordedRequest,
            max_new: usize,
            queue: &mut IOQueue<RecordedSubmission>,
        ) -> Option<RecordedRequest> {
            let emit = req.remaining.min(max_new);
            for op_idx in req.next_op..req.next_op + emit {
                let key = self.next_key;
                self.next_key += 1;
                queue.push(RecordedSubmission {
                    key,
                    op: RecordedOp {
                        lane: req.lane,
                        tag: req.tag,
                        op_idx,
                    },
                    operation: Operation::pwrite_owned(
                        -1,
                        0,
                        DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
                    ),
                });
            }
            req.next_op += emit;
            req.remaining -= emit;
            (req.remaining != 0).then_some(req)
        }

        fn on_submit(&mut self, sub: &RecordedSubmission) {
            self.log.submits.lock().unwrap().push(sub.op);
            if let Some(submit_tx) = &self.submit_tx {
                let _ = submit_tx.send(sub.op);
            }
        }

        fn on_complete(
            &mut self,
            sub: RecordedSubmission,
            _res: std::io::Result<usize>,
        ) -> AIOKind {
            self.log.completes.lock().unwrap().push(sub.op);
            AIOKind::Write
        }

        fn end_loop(self) {
            self.log.end_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn recording_worker(
        max_events: usize,
        lane_configs: &[IOLaneConfig],
        submit_tx: Option<flume::Sender<RecordedOp>>,
    ) -> RecordingWorkerParts {
        let log = Arc::new(RecordingLog::default());
        let (builder, clients) =
            build_io_worker_lanes(ImmediateBackend::new(max_events), lane_configs)
                .expect("lane configs should be valid");
        let worker = builder.bind(RecordingStateMachine::new(Arc::clone(&log), submit_tx));
        (worker, clients, log)
    }

    #[test]
    fn test_fetch_reqs_stops_after_request_expands_to_io_depth() {
        let (mut worker, client) = test_single_lane_worker(2, 0);
        let mut queue = IOQueue::with_capacity(2);

        client
            .send(ExpandRequest {
                lane: 0,
                remaining: 5,
            })
            .unwrap();
        worker.fetch_reqs(&mut queue, 1);

        assert_eq!(queue.len(), 2);
        assert_eq!(worker.local_depth(&queue), 2);
        assert_eq!(
            worker.scheduler.lanes[0].deferred_req,
            Some(ExpandRequest {
                lane: 0,
                remaining: 3,
            })
        );
    }

    #[test]
    fn test_fetch_reqs_counts_submitted_work_against_io_depth() {
        let (mut worker, client) = test_single_lane_worker(2, 1);
        let mut queue = IOQueue::with_capacity(2);

        client
            .send(ExpandRequest {
                lane: 0,
                remaining: 1,
            })
            .unwrap();
        client
            .send(ExpandRequest {
                lane: 0,
                remaining: 1,
            })
            .unwrap();

        worker.fetch_reqs(&mut queue, 0);
        assert_eq!(queue.len(), 1);
        assert_eq!(worker.local_depth(&queue), 2);
        assert!(!worker.scheduler.has_deferred());

        worker.submitted = 0;
        worker.fetch_reqs(&mut queue, 0);
        assert_eq!(queue.len(), 2);
        assert_eq!(worker.local_depth(&queue), 2);
    }

    #[test]
    fn test_multi_lane_fetch_reqs_rotates_after_burst_limited_remainder() {
        let lane_configs = [IOLaneConfig::new(4, 1), IOLaneConfig::new(4, 1)];
        let (mut worker, clients) = test_multi_lane_worker(2, 0, &lane_configs);
        let mut queue = IOQueue::with_capacity(2);

        clients[0]
            .send(ExpandRequest {
                lane: 0,
                remaining: 3,
            })
            .unwrap();
        clients[1]
            .send(ExpandRequest {
                lane: 1,
                remaining: 1,
            })
            .unwrap();

        worker.fetch_reqs(&mut queue, 0);

        let queued_lanes: Vec<_> = queue.reqs.iter().map(|sub| sub.lane).collect();
        assert_eq!(queued_lanes, vec![0, 1]);
        assert_eq!(
            worker.scheduler.lanes[0].deferred_req,
            Some(ExpandRequest {
                lane: 0,
                remaining: 2,
            })
        );
        assert!(worker.scheduler.lanes[1].deferred_req.is_none());
    }

    #[test]
    fn test_multi_lane_backpressure_is_lane_local() {
        let lane_configs = [IOLaneConfig::new(1, 1), IOLaneConfig::new(1, 1)];
        let (_scheduler, clients) = test_scheduler::<ExpandRequest>(&lane_configs);

        clients[0]
            .try_send(ExpandRequest {
                lane: 0,
                remaining: 1,
            })
            .unwrap();
        let full = clients[0]
            .try_send(ExpandRequest {
                lane: 0,
                remaining: 1,
            })
            .expect_err("lane 0 backlog should be full");
        assert!(matches!(
            full,
            TrySendError::Full(ExpandRequest { lane: 0, .. })
        ));

        clients[1]
            .try_send(ExpandRequest {
                lane: 1,
                remaining: 1,
            })
            .expect("lane 1 should retain independent enqueue capacity");
    }

    #[test]
    fn test_multi_lane_worker_drains_deferred_and_queued_work_once_on_shutdown() {
        let lane_configs = [IOLaneConfig::new(8, 1), IOLaneConfig::new(8, 1)];
        let (worker, clients, log) = recording_worker(1, &lane_configs, None);

        clients[0]
            .send(RecordedRequest::new(0, "lane0", 3))
            .unwrap();
        clients[1]
            .send(RecordedRequest::new(1, "lane1", 1))
            .unwrap();
        clients[0].shutdown();
        clients[1].shutdown();

        worker.run();

        let submits = log.submits.lock().unwrap().clone();
        let completes = log.completes.lock().unwrap().clone();
        assert_eq!(
            submits,
            vec![
                RecordedOp {
                    lane: 0,
                    tag: "lane0",
                    op_idx: 0,
                },
                RecordedOp {
                    lane: 1,
                    tag: "lane1",
                    op_idx: 0,
                },
                RecordedOp {
                    lane: 0,
                    tag: "lane0",
                    op_idx: 1,
                },
                RecordedOp {
                    lane: 0,
                    tag: "lane0",
                    op_idx: 2,
                },
            ]
        );
        assert_eq!(completes, submits);
        assert_eq!(log.end_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_idle_multi_lane_worker_wakes_on_any_lane_and_resumes_rotation() {
        let lane_configs = [IOLaneConfig::new(8, 1), IOLaneConfig::new(8, 1)];
        let (submit_tx, submit_rx) = flume::unbounded();
        let (worker, clients, log) = recording_worker(1, &lane_configs, Some(submit_tx));

        let handle = std::thread::spawn(move || worker.run());
        std::thread::sleep(Duration::from_millis(20));

        clients[1].send(RecordedRequest::new(1, "wake", 1)).unwrap();
        let first = submit_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("lane 1 work should wake an idle worker");
        assert_eq!(
            first,
            RecordedOp {
                lane: 1,
                tag: "wake",
                op_idx: 0,
            }
        );

        clients[0].send(RecordedRequest::new(0, "next", 1)).unwrap();
        clients[0].shutdown();
        clients[1].shutdown();

        handle.join().unwrap();

        let submits = log.submits.lock().unwrap().clone();
        assert_eq!(
            submits,
            vec![
                RecordedOp {
                    lane: 1,
                    tag: "wake",
                    op_idx: 0,
                },
                RecordedOp {
                    lane: 0,
                    tag: "next",
                    op_idx: 0,
                },
            ]
        );
    }
}
