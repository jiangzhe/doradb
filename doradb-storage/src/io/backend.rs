use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Snapshot of backend-owned submit/wait activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IOBackendStats {
    /// Number of backend kernel-entry calls spent submitting work or waiting.
    ///
    /// On `libaio`, one logical IO commonly contributes one submit call and
    /// one wait call, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub submit_and_wait_calls: usize,
    /// Number of operations accepted by the backend submit path.
    pub submitted_ops: usize,
    /// Total nanoseconds spent in backend submit-or-wait calls.
    ///
    /// This is a non-overlapping total. `libaio` contributes separate submit
    /// and wait syscall time, while `io_uring` contributes fused
    /// `submit_and_wait()` time once.
    pub submit_and_wait_nanos: usize,
    /// Number of completions observed by the backend wait path.
    pub wait_completions: usize,
}

impl IOBackendStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    pub fn delta_since(self, earlier: IOBackendStats) -> IOBackendStats {
        IOBackendStats {
            submit_and_wait_calls: self
                .submit_and_wait_calls
                .saturating_sub(earlier.submit_and_wait_calls),
            submitted_ops: self.submitted_ops.saturating_sub(earlier.submitted_ops),
            submit_and_wait_nanos: self
                .submit_and_wait_nanos
                .saturating_sub(earlier.submit_and_wait_nanos),
            wait_completions: self
                .wait_completions
                .saturating_sub(earlier.wait_completions),
        }
    }
}

#[derive(Default)]
struct IOBackendStatsCounters {
    submit_and_wait_calls: AtomicUsize,
    submitted_ops: AtomicUsize,
    submit_and_wait_nanos: AtomicUsize,
    wait_completions: AtomicUsize,
}

#[derive(Clone, Default)]
pub(crate) struct IOBackendStatsHandle(Arc<IOBackendStatsCounters>);

impl IOBackendStatsHandle {
    #[inline]
    pub(crate) fn snapshot(&self) -> IOBackendStats {
        IOBackendStats {
            submit_and_wait_calls: self.0.submit_and_wait_calls.load(Ordering::Relaxed),
            submitted_ops: self.0.submitted_ops.load(Ordering::Relaxed),
            submit_and_wait_nanos: self.0.submit_and_wait_nanos.load(Ordering::Relaxed),
            wait_completions: self.0.wait_completions.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub(crate) fn record_submit_and_wait(&self, submit_and_wait_calls: usize, nanos: usize) {
        if submit_and_wait_calls != 0 {
            self.0
                .submit_and_wait_calls
                .fetch_add(submit_and_wait_calls, Ordering::Relaxed);
        }
        if nanos != 0 {
            self.0
                .submit_and_wait_nanos
                .fetch_add(nanos, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn record_submitted_ops(&self, submitted_ops: usize) {
        if submitted_ops != 0 {
            self.0
                .submitted_ops
                .fetch_add(submitted_ops, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn record_wait_completions(&self, wait_completions: usize) {
        if wait_completions != 0 {
            self.0
                .wait_completions
                .fetch_add(wait_completions, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
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
