use super::BACKEND_NAME;
use crate::error::{IoError, IoResult};
use error_stack::Report;
use libc::{EAGAIN, EBUSY};
use std::fmt;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::num::NonZeroUsize;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Default no-progress window for submit-pressure retry loops.
pub(crate) const DEFAULT_SUBMIT_RETRY_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

/// Standard IO result returned by backend completion paths.
pub(crate) type StdIoResult<T> = StdResult<T, StdIoError>;

/// Runtime backend result carried until a terminal IO reporting boundary.
pub(crate) type BackendResult<T> = StdResult<T, BackendError>;

/// Transient submit pressure reported by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitRetryReason {
    /// Kernel submit returned `EAGAIN`.
    Eagain,
    /// Kernel submit returned `EBUSY`.
    Ebusy,
    /// Submit-side work was available, but the backend staged or accepted no operations.
    ///
    /// This is a non-errno progress failure signal. Callers retry it through
    /// the bounded no-progress path instead of treating it as idle `Noop`.
    NoProgress,
}

impl SubmitRetryReason {
    /// Convert one raw errno into a submit retry reason.
    #[inline]
    pub(crate) const fn from_raw_errno(errno: i32) -> Option<Self> {
        match errno {
            EAGAIN => Some(Self::Eagain),
            EBUSY => Some(Self::Ebusy),
            _ => None,
        }
    }

    /// Return the raw errno represented by this retry reason, if any.
    #[inline]
    pub(crate) const fn raw_errno(self) -> Option<i32> {
        match self {
            Self::Eagain => Some(EAGAIN),
            Self::Ebusy => Some(EBUSY),
            Self::NoProgress => None,
        }
    }
}

impl fmt::Display for SubmitRetryReason {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eagain => f.write_str("EAGAIN"),
            Self::Ebusy => f.write_str("EBUSY"),
            Self::NoProgress => f.write_str("NO_PROGRESS"),
        }
    }
}

/// Backend submit pressure details retained across retry handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubmitRetry {
    reason: SubmitRetryReason,
    call_count: usize,
}

impl SubmitRetry {
    /// Build one submit-pressure retry description.
    #[inline]
    pub(crate) const fn new(reason: SubmitRetryReason, call_count: usize) -> Self {
        Self { reason, call_count }
    }

    /// Submit-pressure reason.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn reason(self) -> SubmitRetryReason {
        self.reason
    }

    /// Raw errno represented by this retry, if any.
    #[inline]
    pub(crate) const fn raw_errno(self) -> Option<i32> {
        self.reason.raw_errno()
    }

    #[inline]
    fn io_error(self) -> StdIoError {
        match self.raw_errno() {
            Some(errno) => StdIoError::from_raw_os_error(errno),
            None => StdIoError::new(StdIoErrorKind::WouldBlock, self.reason.to_string()),
        }
    }

    /// Syscall attempt count from the backend submit call.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn call_count(self) -> usize {
        self.call_count
    }

    #[inline]
    fn progress_error(
        self,
        staged: usize,
        pending: usize,
        attempts: u32,
        elapsed: Duration,
        timeout: Duration,
    ) -> BackendError {
        BackendError::new(
            BACKEND_NAME,
            self.io_error(),
            self.call_count,
            BackendErrorDetail::SubmitRetryExpired {
                staged,
                pending,
                reason: self.reason,
                attempts,
                elapsed,
                timeout,
            },
        )
    }
}

impl fmt::Display for SubmitRetry {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}

/// Outcome of a backend submit attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitAttempt {
    /// The attempt accepted no operations and did not hit submit pressure.
    Noop,
    /// The backend accepted this many staged operations.
    Submitted(NonZeroUsize),
    /// Transient submit pressure; staged operations remain queued for retry.
    Retry(SubmitRetry),
}

/// Bounded backoff used when a ring has staged work but no accepted work to wait on.
#[derive(Debug)]
pub(crate) struct SubmitRetryBackoff {
    attempts: u32,
    started_at: Option<Instant>,
    no_progress_timeout: Duration,
}

impl Default for SubmitRetryBackoff {
    #[inline]
    fn default() -> Self {
        Self::new(DEFAULT_SUBMIT_RETRY_PROGRESS_TIMEOUT)
    }
}

impl SubmitRetryBackoff {
    const MAX_SLEEP_MICROS: u64 = 1024;

    /// Build submit retry backoff with a custom no-progress timeout.
    #[inline]
    pub(crate) const fn new(no_progress_timeout: Duration) -> Self {
        Self {
            attempts: 0,
            started_at: None,
            no_progress_timeout,
        }
    }

    /// Return the configured no-progress timeout.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn no_progress_timeout(&self) -> Duration {
        self.no_progress_timeout
    }

    /// Reset the retry sequence after backend progress.
    #[inline]
    pub(crate) fn reset(&mut self) {
        self.attempts = 0;
        self.started_at = None;
    }

    /// Wait briefly before retrying submit when there is no accepted work.
    #[inline]
    pub(crate) fn backoff(&mut self) {
        let attempts = self.attempts;
        self.attempts = self.attempts.saturating_add(1);
        if attempts == 0 {
            thread::yield_now();
            return;
        }
        let shift = attempts.saturating_sub(1).min(10);
        let micros = (1u64 << shift).min(Self::MAX_SLEEP_MICROS);
        thread::sleep(Duration::from_micros(micros));
    }

    /// Back off one retry step or convert persistent pressure into a progress error.
    #[inline]
    pub(crate) fn backoff_or_progress_error(
        &mut self,
        retry: SubmitRetry,
        staged: usize,
        pending: usize,
    ) -> BackendResult<()> {
        let now = Instant::now();
        let started_at = match self.started_at {
            Some(started_at) => started_at,
            None => {
                self.started_at = Some(now);
                now
            }
        };
        let elapsed = now.saturating_duration_since(started_at);
        if elapsed >= self.no_progress_timeout {
            return Err(retry.progress_error(
                staged,
                pending,
                self.attempts,
                elapsed,
                self.no_progress_timeout,
            ));
        }
        self.backoff();
        Ok(())
    }
}

/// Backend-specific decision after storage has woken submitted waiters on a
/// fatal backend progress failure.
pub(crate) enum SubmittedIoCleanup {
    /// Submitted state can be dropped after the backend owner is dropped.
    DropAfterBackend,
    /// The backend could not prove submitted user memory is no longer in use.
    #[cfg_attr(
        not(feature = "iouring"),
        allow(dead_code, reason = "io_uring-only submitted cleanup disposition")
    )]
    LeakAfterBackend {
        backend: &'static str,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackendErrorDetail {
    Submit {
        staged: usize,
        pending: usize,
        note: Option<&'static str>,
    },
    Wait,
    SubmitRetryExpired {
        staged: usize,
        pending: usize,
        reason: SubmitRetryReason,
        attempts: u32,
        elapsed: Duration,
        timeout: Duration,
    },
}

/// Cloneable runtime backend failure retained until an IO reporting boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendError {
    backend: &'static str,
    io_kind: StdIoErrorKind,
    source: Arc<str>,
    raw_errno: Option<i32>,
    call_count: usize,
    detail: BackendErrorDetail,
}

impl BackendError {
    /// Builds a submit failure from one backend syscall error.
    #[inline]
    pub(crate) fn submit(
        backend: &'static str,
        source: StdIoError,
        call_count: usize,
        staged: usize,
        pending: usize,
    ) -> Self {
        Self::submit_with_note(backend, source, call_count, staged, pending, None)
    }

    /// Builds a submit failure with an optional backend-specific static note.
    #[inline]
    pub(crate) fn submit_with_note(
        backend: &'static str,
        source: StdIoError,
        call_count: usize,
        staged: usize,
        pending: usize,
        note: Option<&'static str>,
    ) -> Self {
        Self::new(
            backend,
            source,
            call_count,
            BackendErrorDetail::Submit {
                staged,
                pending,
                note,
            },
        )
    }

    /// Builds a completion-wait failure from one backend syscall error.
    #[inline]
    pub(crate) fn wait(backend: &'static str, source: StdIoError, call_count: usize) -> Self {
        Self::new(backend, source, call_count, BackendErrorDetail::Wait)
    }

    fn new(
        backend: &'static str,
        source: StdIoError,
        call_count: usize,
        detail: BackendErrorDetail,
    ) -> Self {
        let raw_errno = source.raw_os_error();
        BackendError {
            backend,
            io_kind: source.kind(),
            source: Arc::from(source.to_string()),
            raw_errno,
            call_count,
            detail,
        }
    }

    /// Backend name that produced the failure.
    #[inline]
    pub(crate) fn backend(&self) -> &'static str {
        self.backend
    }

    /// Backend operation that failed.
    #[inline]
    pub(crate) fn op(&self) -> &'static str {
        match &self.detail {
            BackendErrorDetail::Submit { .. } | BackendErrorDetail::SubmitRetryExpired { .. } => {
                "submit"
            }
            BackendErrorDetail::Wait => "wait",
        }
    }

    /// Raw OS errno when the underlying error has one.
    #[inline]
    pub(crate) fn raw_errno(&self) -> Option<i32> {
        self.raw_errno
    }

    /// Syscall attempt count observed before the failure.
    #[inline]
    pub(crate) fn call_count(&self) -> usize {
        self.call_count
    }

    /// Formats the IO-domain root plus structured backend details for logs.
    #[inline]
    pub(crate) fn summary(&self) -> String {
        format!("{}: {self}", IoError::from(self.io_kind))
    }

    /// Converts this runtime failure into its terminal IO-domain report.
    #[inline]
    pub(crate) fn into_report(self) -> Report<IoError> {
        let io_kind = self.io_kind;
        Report::new(IoError::from(io_kind)).attach(self)
    }

    /// Converts a clone into an IO-domain report while retaining this failure.
    #[inline]
    pub(crate) fn to_report(&self) -> Report<IoError> {
        self.clone().into_report()
    }
}

impl fmt::Display for BackendError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "backend={} op={} errno={:?} error={} calls={}",
            self.backend,
            self.op(),
            self.raw_errno,
            self.source,
            self.call_count
        )?;
        match &self.detail {
            BackendErrorDetail::Submit {
                staged,
                pending,
                note,
            } => {
                write!(f, " queue={{staged={staged} pending={pending}}}")?;
                if let Some(note) = note {
                    write!(f, " note={note}")?;
                }
            }
            BackendErrorDetail::Wait => {}
            BackendErrorDetail::SubmitRetryExpired {
                staged,
                pending,
                reason,
                attempts,
                elapsed,
                timeout,
            } => write!(
                f,
                " queue={{staged={staged} pending={pending}}} submit_retry_reason={reason} submit_retry_attempts={attempts} submit_retry_elapsed_ms={} submit_retry_timeout_ms={}",
                elapsed.as_millis(),
                timeout.as_millis()
            )?,
        }
        Ok(())
    }
}

/// Worker-owned completion token stored in backend user-data fields.
///
/// The token packs the inflight-slot generation into the high 32 bits and the
/// slot index into the low 32 bits. The worker validates the generation on
/// completion to reject ABA reuse of an old slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BackendToken(u64);

impl BackendToken {
    /// Builds one token from an inflight-slot generation and slot index.
    #[inline]
    pub(crate) const fn new(generation: u32, slot_index: u32) -> Self {
        BackendToken(((generation as u64) << 32) | slot_index as u64)
    }

    /// Wraps one raw backend user-data value as a typed token.
    #[inline]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        BackendToken(raw)
    }

    /// Returns the raw `u64` value stored in the backend user-data field.
    #[inline]
    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    /// Returns the packed inflight-slot generation.
    #[inline]
    pub(crate) const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Returns the packed inflight-slot index.
    #[inline]
    pub(crate) const fn slot_index(self) -> u32 {
        self.0 as u32
    }
}

/// Snapshot of backend-owned submit/wait activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BackendStats {
    /// Number of backend kernel-entry calls spent submitting work or waiting.
    ///
    /// On `libaio`, one logical IO commonly contributes one submit call and
    /// one wait call, so this count can be roughly doubled compared with
    /// `io_uring` for serialized workloads.
    pub(crate) submit_and_wait_calls: usize,
    /// Number of operations accepted by the backend submit path.
    pub(crate) submitted_ops: usize,
    /// Total nanoseconds spent in backend submit-or-wait calls.
    ///
    /// This is a non-overlapping total. `libaio` contributes separate submit
    /// and wait syscall time, while `io_uring` contributes fused
    /// `submit_and_wait()` time once.
    pub(crate) submit_and_wait_nanos: usize,
    /// Number of completions observed by the backend wait path.
    pub(crate) wait_completions: usize,
}

impl BackendStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(
        any(not(test), feature = "iouring"),
        expect(dead_code, reason = "internal io backend stats")
    )]
    pub(crate) fn delta_since(self, earlier: BackendStats) -> BackendStats {
        BackendStats {
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
struct BackendStatsCounters {
    submit_and_wait_calls: AtomicUsize,
    submitted_ops: AtomicUsize,
    submit_and_wait_nanos: AtomicUsize,
    wait_completions: AtomicUsize,
}

/// Shared handle used to collect backend submit and wait statistics.
#[derive(Clone, Default)]
pub(crate) struct BackendStatsHandle(Arc<BackendStatsCounters>);

impl BackendStatsHandle {
    /// Returns a point-in-time snapshot of backend activity counters.
    #[inline]
    pub(crate) fn snapshot(&self) -> BackendStats {
        BackendStats {
            submit_and_wait_calls: self.0.submit_and_wait_calls.load(Ordering::Relaxed),
            submitted_ops: self.0.submitted_ops.load(Ordering::Relaxed),
            submit_and_wait_nanos: self.0.submit_and_wait_nanos.load(Ordering::Relaxed),
            wait_completions: self.0.wait_completions.load(Ordering::Relaxed),
        }
    }

    /// Records submit-or-wait calls and their elapsed time in nanoseconds.
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

    /// Records operations accepted by the backend submit path.
    #[inline]
    pub(crate) fn record_submitted_ops(&self, submitted_ops: usize) {
        if submitted_ops != 0 {
            self.0
                .submitted_ops
                .fetch_add(submitted_ops, Ordering::Relaxed);
        }
    }

    /// Records completions returned by the backend wait path.
    #[inline]
    pub(crate) fn record_wait_completions(&self, wait_completions: usize) {
        if wait_completions != 0 {
            self.0
                .wait_completions
                .fetch_add(wait_completions, Ordering::Relaxed);
        }
    }

    /// Returns the allocation identity of the shared stats counters.
    #[cfg(test)]
    #[inline]
    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}

/// Backend-specific submit/wait contract used by storage IO schedulers.
///
/// The worker owns scheduling and inflight lifetime. The backend owns:
/// - how one [`super::Operation`] is prepared for the kernel
/// - how prepared submissions are staged into one batch
/// - how completion buffers are allocated and interpreted
///
/// This keeps libaio-specific `*mut *mut iocb` layout and future io_uring
/// submission queue layout out of the IO scheduler.
pub(crate) trait Backend: Sized {
    type Prepared;
    type SubmitBatch;
    type Events;

    /// Set up one backend with the requested concurrent IO depth.
    fn setup(io_depth: usize) -> IoResult<Self>;
    /// Returns the configured concurrent IO depth.
    fn io_depth(&self) -> usize;
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
    fn submit_batch(
        &mut self,
        batch: &mut Self::SubmitBatch,
        limit: usize,
    ) -> BackendResult<SubmitAttempt>;
    /// Waits for at least `min_nr` completions and returns worker tokens plus results.
    fn wait_at_least(
        &mut self,
        events: &mut Self::Events,
        min_nr: usize,
    ) -> BackendResult<Vec<(BackendToken, StdIoResult<usize>)>>;
    /// Best-effort backend cleanup for already-submitted IO after a fatal
    /// progress failure.
    fn cleanup_submitted_io(&mut self, submitted: usize) -> SubmittedIoCleanup;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind as StdIoErrorKind;

    #[test]
    fn test_backend_error_submit_preserves_shared_source_and_converts_at_boundary() {
        let failure = BackendError::submit(
            "test_backend",
            StdIoError::new(StdIoErrorKind::PermissionDenied, "submit denied"),
            2,
            0,
            3,
        );

        assert_eq!(failure.backend(), "test_backend");
        assert_eq!(failure.op(), "submit");
        assert_eq!(failure.call_count(), 2);
        assert_eq!(
            failure.to_string(),
            "backend=test_backend op=submit errno=None error=submit denied calls=2 queue={staged=0 pending=3}"
        );
        let cloned = failure.clone();
        assert!(Arc::ptr_eq(&failure.source, &cloned.source));

        let report = failure.into_report();
        assert!(report.downcast_ref::<BackendError>().is_some());
        assert_eq!(
            report.current_context().kind(),
            StdIoErrorKind::PermissionDenied
        );
    }

    #[test]
    fn test_backend_error_submit_formats_optional_note() {
        let failure = BackendError::submit_with_note(
            "test_backend",
            StdIoError::new(StdIoErrorKind::InvalidInput, "unsupported submit"),
            1,
            2,
            1,
            Some("backend-specific note"),
        );

        assert_eq!(
            failure.to_string(),
            "backend=test_backend op=submit errno=None error=unsupported submit calls=1 queue={staged=2 pending=1} note=backend-specific note"
        );
    }

    #[test]
    fn test_backend_error_wait_formats_common_diagnostics() {
        let failure =
            BackendError::wait("test_backend", StdIoError::from_raw_os_error(libc::EIO), 3);

        assert_eq!(failure.op(), "wait");
        assert_eq!(failure.raw_errno(), Some(libc::EIO));
        assert_eq!(
            failure.to_string(),
            "backend=test_backend op=wait errno=Some(5) error=Input/output error (os error 5) calls=3"
        );
    }

    #[test]
    fn test_submit_retry_expiry_uses_operation_specific_detail() {
        let failure = SubmitRetry::new(SubmitRetryReason::NoProgress, 4).progress_error(
            2,
            3,
            5,
            Duration::from_millis(7),
            Duration::from_millis(6),
        );

        assert_eq!(failure.op(), "submit");
        assert_eq!(failure.raw_errno(), None);
        assert!(matches!(
            failure.detail,
            BackendErrorDetail::SubmitRetryExpired {
                staged: 2,
                pending: 3,
                reason: SubmitRetryReason::NoProgress,
                attempts: 5,
                elapsed,
                timeout,
            } if elapsed == Duration::from_millis(7)
                && timeout == Duration::from_millis(6)
        ));
    }
}
