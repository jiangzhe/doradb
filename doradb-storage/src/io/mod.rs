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

use flume::{Receiver, SendError, Sender};
use std::collections::VecDeque;
use std::mem::replace;
use std::os::unix::io::RawFd;
use std::ptr::null_mut;
use std::result::Result as StdResult;

pub(crate) use backend::*;
pub(crate) use buf::*;
pub(crate) use completion::Completion;

/// Canonical storage backend selected by cargo features.
#[cfg(feature = "iouring")]
pub(crate) use iouring_backend::IouringBackend as StorageBackend;
/// Canonical storage backend selected by cargo features.
#[cfg(feature = "libaio")]
pub(crate) use libaio_backend::LibaioBackend as StorageBackend;

#[cfg(test)]
pub(crate) use self::tests::{
    StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
    current_storage_backend_test_hook, install_storage_backend_test_hook,
};

/// Logical sector size required by storage Direct I/O buffers and offsets.
pub(crate) const STORAGE_SECTOR_SIZE: usize = 4096;

const INVALID_SLOT: u32 = u32::MAX;

/// Buffer ownership model for one backend-agnostic IO operation.
///
/// Higher layers either transfer an owned direct buffer to the worker or
/// provide a borrowed page-aligned pointer whose lifetime they keep valid until
/// completion.
enum IOMemory {
    Owned(DirectBuf),
    Borrowed { ptr: *mut u8, len: usize },
}

// SAFETY: borrowed pointers are only used for buffer/page memory that higher
// layers guarantee remains valid until completion, matching the borrowed
// `Operation` constructor contract.
unsafe impl Send for IOMemory {}

/// Backend-agnostic description of one submitted kernel IO operation.
///
/// This type is backend-agnostic: it describes one read/write operation and
/// owns or borrows the memory that the backend will bind into its prepared
/// submission shape.
pub(crate) struct Operation {
    kind: IOKind,
    fd: RawFd,
    offset: usize,
    memory: IOMemory,
}

impl Operation {
    /// Build one owned-buffer read operation.
    #[inline]
    pub(crate) fn pread_owned(fd: RawFd, offset: usize, buf: DirectBuf) -> Self {
        Operation {
            kind: IOKind::Read,
            fd,
            offset,
            memory: IOMemory::Owned(buf),
        }
    }

    /// Build one owned-buffer write operation.
    #[inline]
    pub(crate) fn pwrite_owned(fd: RawFd, offset: usize, buf: DirectBuf) -> Self {
        Operation {
            kind: IOKind::Write,
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
    pub(crate) unsafe fn pread_borrowed(
        fd: RawFd,
        offset: usize,
        ptr: *mut u8,
        len: usize,
    ) -> Self {
        Operation {
            kind: IOKind::Read,
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
    pub(crate) unsafe fn pwrite_borrowed(
        fd: RawFd,
        offset: usize,
        ptr: *mut u8,
        len: usize,
    ) -> Self {
        Operation {
            kind: IOKind::Write,
            fd,
            offset,
            memory: IOMemory::Borrowed { ptr, len },
        }
    }

    /// Returns whether this operation is a read or a write.
    #[inline]
    pub(crate) fn kind(&self) -> IOKind {
        self.kind
    }

    /// Returns the raw file descriptor targeted by this operation.
    #[inline]
    pub(crate) fn fd(&self) -> RawFd {
        self.fd
    }

    /// Returns the byte offset used for this operation.
    #[inline]
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the byte length of the bound buffer or pointer.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match &self.memory {
            IOMemory::Owned(buf) => buf.capacity(),
            IOMemory::Borrowed { len, .. } => *len,
        }
    }

    /// Takes ownership of the direct buffer if this completion owns one.
    #[inline]
    pub(crate) fn take_buf(&mut self) -> Option<DirectBuf> {
        match replace(
            &mut self.memory,
            IOMemory::Borrowed {
                ptr: null_mut(),
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
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved buf"))]
    pub(crate) fn buf(&self) -> Option<&DirectBuf> {
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

/// Direction of one backend IO operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IOKind {
    Read,
    Write,
}

/// Message delivered from IO clients to storage IO workers.
pub(crate) enum IOMessage<T> {
    Shutdown,
    Req(T),
}

impl<T> IOMessage<T> {
    #[inline]
    fn req(self) -> Option<T> {
        match self {
            IOMessage::Req(r) => Some(r),
            IOMessage::Shutdown => None,
        }
    }
}

/// FIFO queue of backend-agnostic IO submissions prepared by a state machine.
pub(crate) struct IOQueue<T> {
    reqs: VecDeque<T>,
}

impl<T> IOQueue<T> {
    /// Create a new IO queue with given capacity.
    #[inline]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        IOQueue {
            reqs: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns length of the queue.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.reqs.len()
    }

    /// Returns whether the queue is empty.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes and returns up to `n` requests from the front of the queue.
    #[inline]
    pub(crate) fn drain_to(&mut self, n: usize) -> Vec<T> {
        let count = n.min(self.reqs.len());
        self.reqs.drain(0..count).collect()
    }

    /// Checks consistency of this queue.
    #[inline]
    pub(crate) fn consistent(&self) -> bool {
        true
    }

    /// Appends one request to the back of the queue.
    #[inline]
    pub(crate) fn push(&mut self, req: T) {
        self.reqs.push_back(req);
    }

    /// Removes and returns the request at the front of the queue.
    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<T> {
        self.reqs.pop_front()
    }
}

/// One scheduler-owned submission staged for backend preparation and completion.
///
/// The IO scheduler only prepares and submits the backend operation. Any
/// domain identity or higher-level bookkeeping lives in the concrete
/// submission and its owning state machine.
pub(crate) trait IOSubmission {
    /// Returns the backend-agnostic IO operation to prepare and submit.
    fn operation(&mut self) -> &mut Operation;
}

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
        let entry = replace(&mut slot_ref.entry, Entry::Vacant(prev_head));
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

/// One completed backend submission returned by [`SubmissionDriver`].
pub(crate) struct CompletedSubmission<S> {
    /// Original higher-level submission associated with this completion.
    pub(crate) submission: S,
    /// Backend completion result for the submitted operation.
    pub(crate) result: StdIoResult<usize>,
}

/// Backend-neutral direct submission driver.
///
/// The driver owns inflight slots, backend-prepared state, staged submission
/// batches, completion-token validation, and backend submit/wait calls. Callers
/// keep domain scheduling policy outside this type and push backend-facing
/// submissions directly when capacity is available.
pub(crate) struct SubmissionDriver<S, B = StorageBackend>
where
    S: IOSubmission,
    B: IOBackend,
{
    backend: B,
    events: B::Events,
    submit_batch: B::SubmitBatch,
    slots: InflightSlots<InflightEntry<S, B::Prepared>>,
    staged_slots: VecDeque<u32>,
    submitted: usize,
    completed: VecDeque<CompletedSubmission<S>>,
}

impl<S, B> SubmissionDriver<S, B>
where
    S: IOSubmission,
    B: IOBackend,
{
    /// Create a submission driver around one storage backend.
    #[inline]
    pub(crate) fn new(backend: B) -> Self {
        let events = backend.new_events();
        let submit_batch = backend.new_submit_batch();
        let capacity = backend.max_events();
        SubmissionDriver {
            backend,
            events,
            submit_batch,
            slots: InflightSlots::new(capacity),
            staged_slots: VecDeque::with_capacity(capacity),
            submitted: 0,
            completed: VecDeque::new(),
        }
    }

    /// Returns the maximum number of backend submissions this driver can own.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.slots.slots.len()
    }

    /// Returns remaining staging capacity.
    ///
    /// Completed-but-not-yet-returned submissions have already freed their
    /// backend slots, so they do not reduce capacity for new work.
    #[inline]
    pub(crate) fn available_capacity(&self) -> usize {
        self.capacity()
            .saturating_sub(self.staged_slots.len() + self.submitted)
    }

    /// Returns submissions still owned by the driver.
    ///
    /// This includes backend-staged submissions, kernel-submitted submissions,
    /// and completions fetched from the backend but not yet returned to the
    /// caller.
    #[inline]
    pub(crate) fn pending_len(&self) -> usize {
        self.staged_slots.len() + self.submitted_len()
    }

    /// Returns submissions accepted by the backend submit path but not yet
    /// returned to the caller.
    #[inline]
    pub(crate) fn submitted_len(&self) -> usize {
        self.submitted + self.completed.len()
    }

    /// Stages one submission for backend submission.
    ///
    /// Returns the submission unchanged when the driver has no free slot.
    #[inline]
    pub(crate) fn push(&mut self, mut submission: S) -> StdResult<(), S> {
        if self.available_capacity() == 0 {
            return Err(submission);
        }
        let (token, slot) = self
            .slots
            .reserve()
            .expect("slot reservation should succeed when capacity is available");
        let mut prepared = self.backend.prepare(token, submission.operation());
        self.backend
            .push_prepared(&mut self.submit_batch, &mut prepared);
        self.slots.occupy_reserved(
            slot,
            InflightEntry {
                submission,
                _prepared: prepared,
                submitted: false,
            },
        );
        self.staged_slots.push_back(slot);
        Ok(())
    }

    /// Submits staged operations up to backend capacity and returns the count
    /// accepted by the backend.
    #[inline]
    pub(crate) fn submit_ready(&mut self) -> usize {
        if self.staged_slots.is_empty() {
            return 0;
        }
        let limit = self.capacity().saturating_sub(self.submitted);
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
            entry.submitted = true;
        }
        self.submitted += submit_count;
        debug_assert!(self.submitted <= self.capacity());
        submit_count
    }

    /// Waits for at least one accepted submission to complete.
    ///
    /// If a previous backend wait returned several completions, this returns
    /// the next buffered completion without entering the backend wait path.
    #[inline]
    pub(crate) fn wait_at_least_one(&mut self) -> CompletedSubmission<S> {
        if self.completed.is_empty() {
            assert!(
                self.submitted != 0,
                "wait_at_least_one requires at least one backend-submitted operation"
            );
            let completions = self.backend.wait_at_least(&mut self.events, 1);
            let completed_count = completions.len();
            assert!(
                completed_count <= self.submitted,
                "backend returned more completions than submitted operations"
            );
            self.submitted -= completed_count;
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
                self.completed.push_back(CompletedSubmission {
                    submission: entry.submission,
                    result: res,
                });
            }
        }
        self.completed
            .pop_front()
            .expect("backend wait must return at least one completion")
    }

    /// Pops one backend completion that was already fetched by an earlier wait.
    ///
    /// This never enters the backend wait path. It is intended for callers that
    /// want to observe already-available completions without delaying the first
    /// ready item to chase more batching.
    #[inline]
    pub(crate) fn try_pop_completed(&mut self) -> Option<CompletedSubmission<S>> {
        self.completed.pop_front()
    }
}

/// IOStateMachine defines how one scheduler maps requests to submissions and
/// applies completion-side state transitions.
pub(crate) trait IOStateMachine {
    type Request;
    type Submission: IOSubmission;

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

    /// Called when IO is completed.
    /// The result contains number of bytes read/write, or the IO error.
    fn on_complete(&mut self, sub: Self::Submission, res: StdIoResult<usize>) -> IOKind;
}

/// Cloneable sender used to enqueue requests into one IO scheduler.
///
/// The client only transports state-machine requests. It does not own any
/// backend-specific submission state.
pub(crate) struct IOClient<T>(Sender<IOMessage<T>>);

impl<T> IOClient<T> {
    /// Creates one bounded worker ingress channel and its client handle.
    #[inline]
    pub(crate) fn bounded(backlog: usize) -> (Receiver<IOMessage<T>>, Self) {
        let (tx, rx) = flume::bounded(backlog);
        (rx, IOClient(tx))
    }

    /// Signals that this ingress lane is finished and contributes to worker shutdown.
    ///
    /// A single-lane worker stops after receiving this message. Multi-lane
    /// workers stop once every lane has shut down and all queued, deferred,
    /// staged, and inflight work has drained.
    #[inline]
    pub(crate) fn shutdown(&self) {
        // Someone might already sent shutdown message via the channel,
        // so we ignore send error.
        let _ = self.0.send(IOMessage::Shutdown);
    }

    /// Send IO request to the IO worker.
    #[inline]
    pub(crate) fn send(&self, req: T) -> StdResult<(), SendError<T>> {
        self.0
            .send(IOMessage::Req(req))
            .map_err(|e| SendError(e.0.req().unwrap()))
    }

    /// Send IO request to the IO worker in async way.
    #[inline]
    pub(crate) async fn send_async(&self, req: T) -> StdResult<(), SendError<T>> {
        self.0
            .send_async(IOMessage::Req(req))
            .await
            .map_err(|e| SendError(e.0.req().unwrap()))
    }
}

impl<T> Clone for IOClient<T> {
    #[inline]
    fn clone(&self) -> Self {
        IOClient(self.0.clone())
    }
}

/// Align given input length to storage sector size.
#[inline]
pub(crate) fn align_to_sector_size(len: usize) -> usize {
    len.max(STORAGE_SECTOR_SIZE).div_ceil(STORAGE_SECTOR_SIZE) * STORAGE_SECTOR_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::metadata;
    use std::io::{Error as StdIoError, Result as IoResult};
    use std::mem::MaybeUninit;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StorageBackendFileIdentity {
        dev: u64,
        ino: u64,
    }

    impl StorageBackendFileIdentity {
        #[inline]
        pub(crate) fn from_path(path: impl AsRef<Path>) -> IoResult<Self> {
            let md = metadata(path)?;
            Ok(Self {
                dev: md.dev(),
                ino: md.ino(),
            })
        }

        #[inline]
        fn from_fd(fd: RawFd) -> IoResult<Self> {
            // SAFETY: `fstat` initializes the output struct on success and does
            // not take ownership of the borrowed raw fd.
            unsafe {
                let mut stat = MaybeUninit::<libc::stat>::uninit();
                if libc::fstat(fd, stat.as_mut_ptr()) == 0 {
                    let stat = stat.assume_init();
                    Ok(Self {
                        dev: stat.st_dev,
                        ino: stat.st_ino,
                    })
                } else {
                    Err(StdIoError::last_os_error())
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StorageBackendOp {
        kind: IOKind,
        fd: RawFd,
        offset: usize,
    }

    impl StorageBackendOp {
        #[inline]
        pub(crate) fn new(kind: IOKind, fd: RawFd, offset: usize) -> Self {
            Self { kind, fd, offset }
        }

        #[inline]
        pub(crate) fn kind(&self) -> IOKind {
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

        #[inline]
        pub(crate) fn matches_file_identity(&self, expected: StorageBackendFileIdentity) -> bool {
            StorageBackendFileIdentity::from_fd(self.fd).is_ok_and(|actual| actual == expected)
        }
    }

    pub(crate) trait StorageBackendTestHook: Send + Sync {
        fn on_submit(&self, _op: StorageBackendOp) {}

        fn on_complete(&self, _op: StorageBackendOp, _res: &mut StdIoResult<usize>) {}
    }

    type StorageBackendHook = Arc<dyn StorageBackendTestHook>;

    static STORAGE_BACKEND_TEST_HOOK_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    static STORAGE_BACKEND_TEST_HOOK: Mutex<Option<StorageBackendHook>> = Mutex::new(None);

    #[inline]
    fn lock_storage_backend_test_hook_gate() -> MutexGuard<'static, ()> {
        STORAGE_BACKEND_TEST_HOOK_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    #[inline]
    fn lock_storage_backend_test_hook_state() -> MutexGuard<'static, Option<StorageBackendHook>> {
        STORAGE_BACKEND_TEST_HOOK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub(crate) struct InstalledStorageBackendTestHook {
        previous: Option<StorageBackendHook>,
        guard: Option<MutexGuard<'static, ()>>,
    }

    impl Drop for InstalledStorageBackendTestHook {
        #[inline]
        fn drop(&mut self) {
            let _ = set_storage_backend_test_hook(self.previous.take());
            drop(self.guard.take());
        }
    }

    #[inline]
    pub(crate) fn current_storage_backend_test_hook() -> Option<StorageBackendHook> {
        lock_storage_backend_test_hook_state().clone()
    }

    #[inline]
    pub(crate) fn set_storage_backend_test_hook(
        hook: Option<StorageBackendHook>,
    ) -> Option<StorageBackendHook> {
        let mut guard = lock_storage_backend_test_hook_state();
        replace(&mut *guard, hook)
    }

    #[inline]
    fn install_storage_backend_test_hook_locked(
        hook: StorageBackendHook,
        guard: MutexGuard<'static, ()>,
    ) -> InstalledStorageBackendTestHook {
        InstalledStorageBackendTestHook {
            previous: set_storage_backend_test_hook(Some(hook)),
            guard: Some(guard),
        }
    }

    #[inline]
    pub(crate) fn install_storage_backend_test_hook(
        hook: StorageBackendHook,
    ) -> InstalledStorageBackendTestHook {
        let guard = lock_storage_backend_test_hook_gate();
        install_storage_backend_test_hook_locked(hook, guard)
    }

    struct NoopStorageBackendTestHook;

    impl StorageBackendTestHook for NoopStorageBackendTestHook {}

    #[test]
    fn test_install_storage_backend_test_hook_clears_hook_on_drop() {
        let hook: StorageBackendHook = Arc::new(NoopStorageBackendTestHook);
        {
            let _install = install_storage_backend_test_hook(hook.clone());
            let current = current_storage_backend_test_hook().unwrap();
            assert!(Arc::ptr_eq(&current, &hook));
        }
        assert!(current_storage_backend_test_hook().is_none());
    }

    #[test]
    fn test_installed_storage_backend_test_hook_restores_previous_on_drop() {
        let previous: StorageBackendHook = Arc::new(NoopStorageBackendTestHook);
        let next: StorageBackendHook = Arc::new(NoopStorageBackendTestHook);
        let guard = lock_storage_backend_test_hook_gate();
        let replaced = set_storage_backend_test_hook(Some(previous.clone()));
        assert!(replaced.is_none());

        let install = install_storage_backend_test_hook_locked(next.clone(), guard);
        let current = current_storage_backend_test_hook().unwrap();
        assert!(Arc::ptr_eq(&current, &next));
        drop(install);

        let restored = current_storage_backend_test_hook().unwrap();
        assert!(Arc::ptr_eq(&restored, &previous));
        let cleared = set_storage_backend_test_hook(None).unwrap();
        assert!(Arc::ptr_eq(&cleared, &previous));
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

    struct DriverBackend {
        max_events: usize,
        submit_results: VecDeque<usize>,
        inflight: VecDeque<BackendToken>,
        panic_on_wait: bool,
    }

    impl DriverBackend {
        #[inline]
        fn new(max_events: usize) -> Self {
            DriverBackend {
                max_events,
                submit_results: VecDeque::new(),
                inflight: VecDeque::new(),
                panic_on_wait: false,
            }
        }

        #[inline]
        fn with_submit_results(
            max_events: usize,
            submit_results: impl Into<VecDeque<usize>>,
        ) -> Self {
            DriverBackend {
                max_events,
                submit_results: submit_results.into(),
                inflight: VecDeque::new(),
                panic_on_wait: false,
            }
        }

        #[inline]
        fn wait_panics(max_events: usize) -> Self {
            DriverBackend {
                max_events,
                submit_results: VecDeque::new(),
                inflight: VecDeque::new(),
                panic_on_wait: true,
            }
        }
    }

    impl IOBackend for DriverBackend {
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
            let allowed = limit.min(batch.len());
            let submit_count = self
                .submit_results
                .pop_front()
                .unwrap_or(allowed)
                .min(allowed);
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
        ) -> Vec<(BackendToken, StdIoResult<usize>)> {
            assert!(!self.panic_on_wait, "backend wait must not be called");
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
    struct DriverOp {
        id: usize,
    }

    struct DriverSubmission {
        op: DriverOp,
        operation: Operation,
    }

    impl DriverSubmission {
        #[inline]
        fn new(id: usize, fd: RawFd, offset: usize) -> Self {
            DriverSubmission {
                op: DriverOp { id },
                operation: Operation::pwrite_owned(
                    fd,
                    offset,
                    DirectBuf::zeroed(STORAGE_SECTOR_SIZE),
                ),
            }
        }
    }

    impl IOSubmission for DriverSubmission {
        fn operation(&mut self) -> &mut Operation {
            &mut self.operation
        }
    }

    #[derive(Default)]
    struct RecordingHook {
        submits: Mutex<Vec<StorageBackendOp>>,
        completes: Mutex<Vec<StorageBackendOp>>,
    }

    impl StorageBackendTestHook for RecordingHook {
        fn on_submit(&self, op: StorageBackendOp) {
            self.submits.lock().unwrap().push(op);
        }

        fn on_complete(&self, op: StorageBackendOp, _res: &mut StdIoResult<usize>) {
            self.completes.lock().unwrap().push(op);
        }
    }

    #[test]
    fn test_submission_driver_submits_and_completes_with_hooks() {
        let hook = Arc::new(RecordingHook::default());
        let _install = install_storage_backend_test_hook(hook.clone());
        let mut driver = SubmissionDriver::new(DriverBackend::new(2));

        assert_eq!(driver.capacity(), 2);
        assert_eq!(driver.available_capacity(), 2);
        assert!(driver.push(DriverSubmission::new(1, 41, 0)).is_ok());
        assert!(
            driver
                .push(DriverSubmission::new(2, 41, STORAGE_SECTOR_SIZE))
                .is_ok()
        );
        assert_eq!(driver.pending_len(), 2);
        assert_eq!(driver.submitted_len(), 0);
        assert_eq!(driver.available_capacity(), 0);

        assert_eq!(driver.submit_ready(), 2);
        assert_eq!(driver.pending_len(), 2);
        assert_eq!(driver.submitted_len(), 2);

        let first = driver.wait_at_least_one();
        assert_eq!(first.submission.op.id, 1);
        assert_eq!(first.result.unwrap(), STORAGE_SECTOR_SIZE);
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.submitted_len(), 1);

        let second = driver.wait_at_least_one();
        assert_eq!(second.submission.op.id, 2);
        assert_eq!(second.result.unwrap(), STORAGE_SECTOR_SIZE);
        assert_eq!(driver.pending_len(), 0);
        assert_eq!(driver.submitted_len(), 0);
        assert_eq!(driver.available_capacity(), 2);

        let expected_ops = vec![
            StorageBackendOp::new(IOKind::Write, 41, 0),
            StorageBackendOp::new(IOKind::Write, 41, STORAGE_SECTOR_SIZE),
        ];
        assert_eq!(
            hook.submits.lock().unwrap().as_slice(),
            expected_ops.as_slice()
        );
        assert_eq!(
            hook.completes.lock().unwrap().as_slice(),
            expected_ops.as_slice()
        );
    }

    #[test]
    fn test_submission_driver_wait_at_least_one_exposes_buffered_completion_pop() {
        let mut driver = SubmissionDriver::new(DriverBackend::new(2));

        assert!(driver.push(DriverSubmission::new(1, 43, 0)).is_ok());
        assert!(
            driver
                .push(DriverSubmission::new(2, 43, STORAGE_SECTOR_SIZE))
                .is_ok()
        );
        assert_eq!(driver.submit_ready(), 2);

        let first = driver.wait_at_least_one();
        assert_eq!(first.submission.op.id, 1);
        assert_eq!(first.result.unwrap(), STORAGE_SECTOR_SIZE);
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.submitted_len(), 1);

        let second = driver
            .try_pop_completed()
            .expect("second completion should already be buffered");
        assert_eq!(second.submission.op.id, 2);
        assert_eq!(second.result.unwrap(), STORAGE_SECTOR_SIZE);
        assert_eq!(driver.pending_len(), 0);
        assert_eq!(driver.submitted_len(), 0);
    }

    #[test]
    fn test_submission_driver_try_pop_completed_does_not_wait() {
        let mut driver = SubmissionDriver::new(DriverBackend::wait_panics(1));

        assert!(driver.push(DriverSubmission::new(1, 44, 0)).is_ok());
        assert_eq!(driver.submit_ready(), 1);
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.submitted_len(), 1);

        assert!(driver.try_pop_completed().is_none());
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.submitted_len(), 1);
    }

    #[test]
    fn test_submission_driver_keeps_zero_submit_staged_for_retry() {
        let mut driver = SubmissionDriver::new(DriverBackend::with_submit_results(
            2,
            VecDeque::from([0, 1, 1]),
        ));

        assert!(driver.push(DriverSubmission::new(1, 42, 0)).is_ok());
        assert!(
            driver
                .push(DriverSubmission::new(2, 42, STORAGE_SECTOR_SIZE))
                .is_ok()
        );

        assert_eq!(driver.submit_ready(), 0);
        assert_eq!(driver.pending_len(), 2);
        assert_eq!(driver.submitted_len(), 0);
        assert_eq!(driver.available_capacity(), 0);

        assert_eq!(driver.submit_ready(), 1);
        assert_eq!(driver.pending_len(), 2);
        assert_eq!(driver.submitted_len(), 1);
        assert_eq!(driver.available_capacity(), 0);

        let first = driver.wait_at_least_one();
        assert_eq!(first.submission.op.id, 1);
        assert_eq!(driver.pending_len(), 1);
        assert_eq!(driver.submitted_len(), 0);
        assert_eq!(driver.available_capacity(), 1);

        assert_eq!(driver.submit_ready(), 1);
        let second = driver.wait_at_least_one();
        assert_eq!(second.submission.op.id, 2);
        assert_eq!(driver.pending_len(), 0);
        assert_eq!(driver.submitted_len(), 0);
        assert_eq!(driver.available_capacity(), 2);
    }
}
