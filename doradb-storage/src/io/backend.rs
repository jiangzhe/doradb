use std::result::Result as StdResult;

/// Worker-owned completion token stored in backend user-data fields.
///
/// The token packs the inflight-slot generation into the high 32 bits and the
/// slot index into the low 32 bits. The worker validates the generation on
/// completion to reject ABA reuse of an old slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BackendToken(u64);

impl BackendToken {
    /// Builds one token from an inflight-slot generation and slot index.
    #[inline]
    pub const fn new(generation: u32, slot_index: u32) -> Self {
        BackendToken(((generation as u64) << 32) | slot_index as u64)
    }

    /// Wraps one raw backend user-data value as a typed token.
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        BackendToken(raw)
    }

    /// Returns the raw `u64` value stored in the backend user-data field.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Returns the packed inflight-slot generation.
    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Returns the packed inflight-slot index.
    #[inline]
    pub const fn slot_index(self) -> u32 {
        self.0 as u32
    }
}

/// Per-iteration statistics reported by an [`super::IOWorker`].
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
    /// Accumulates another statistics snapshot into `self`.
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

/// Backend-specific submit/wait contract used by [`super::IOWorker`].
///
/// The worker owns scheduling and inflight lifetime. The backend owns:
/// - how one [`super::Operation`] is prepared for the kernel
/// - how prepared submissions are staged into one batch
/// - how completion buffers are allocated and interpreted
///
/// This keeps libaio-specific `*mut *mut iocb` layout and future io_uring
/// submission queue layout out of the generic worker.
pub trait IOBackend {
    type Prepared;
    type SubmitBatch;
    type Events;

    /// Returns maximum concurrent submitted operations supported by this backend.
    fn max_events(&self) -> usize;
    /// Allocates one empty backend-owned submit batch.
    fn new_submit_batch(&self) -> Self::SubmitBatch;
    /// Allocates one backend-owned completion-event buffer.
    fn new_events(&self) -> Self::Events;
    /// Prepares one kernel submission for the given worker token and IO operation.
    fn prepare(&mut self, token: BackendToken, operation: &mut super::Operation) -> Self::Prepared;
    /// Appends one prepared submission to a backend-owned batch.
    fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared);
    /// Submits up to `limit` staged operations from the front of `batch`.
    ///
    /// The backend must retain any unsubmitted suffix in `batch` so the worker
    /// can retry later without rebuilding the batch layout.
    fn submit_batch(&mut self, batch: &mut Self::SubmitBatch, limit: usize) -> usize;
    /// Waits for at least `min_nr` completions and returns worker tokens plus results.
    fn wait_at_least(
        &mut self,
        events: &mut Self::Events,
        min_nr: usize,
    ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)>;
}
