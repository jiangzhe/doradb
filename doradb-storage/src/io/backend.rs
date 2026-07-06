use crate::error::IoError;
use error_stack::Report;
use libc::{EAGAIN, EBUSY};
use std::fmt;
use std::io::Error as StdIoError;
use std::num::NonZeroUsize;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Standard IO result returned by backend completion paths.
pub(crate) type StdIoResult<T> = StdResult<T, StdIoError>;

/// Result returned by backend submit and wait progress paths.
pub(crate) type BackendResult<T> = StdResult<T, Report<IoError>>;

/// Default no-progress window for submit-pressure retry loops.
pub(crate) const DEFAULT_SUBMIT_RETRY_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

/// Transient submit pressure reported by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitRetryReason {
    /// Kernel submit returned `EAGAIN`.
    Eagain,
    /// Kernel submit returned `EBUSY`.
    Ebusy,
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

    /// Return the raw errno represented by this retry reason.
    #[inline]
    pub(crate) const fn raw_errno(self) -> i32 {
        match self {
            Self::Eagain => EAGAIN,
            Self::Ebusy => EBUSY,
        }
    }
}

impl fmt::Display for SubmitRetryReason {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eagain => f.write_str("EAGAIN"),
            Self::Ebusy => f.write_str("EBUSY"),
        }
    }
}

/// Backend submit pressure details retained across retry handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubmitRetry {
    backend: &'static str,
    reason: SubmitRetryReason,
    call_count: usize,
}

impl SubmitRetry {
    /// Build one submit-pressure retry description.
    #[inline]
    pub(crate) const fn new(
        backend: &'static str,
        reason: SubmitRetryReason,
        call_count: usize,
    ) -> Self {
        Self {
            backend,
            reason,
            call_count,
        }
    }

    /// Backend that reported submit pressure.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn backend(self) -> &'static str {
        self.backend
    }

    /// Submit-pressure reason.
    #[inline]
    #[cfg(test)]
    pub(crate) const fn reason(self) -> SubmitRetryReason {
        self.reason
    }

    /// Raw errno represented by this retry.
    #[inline]
    pub(crate) const fn raw_errno(self) -> i32 {
        self.reason.raw_errno()
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
        queue_state: IOBackendQueueState,
        attempts: u32,
        elapsed: Duration,
        timeout: Duration,
    ) -> Report<IoError> {
        IOBackendFailure::report(
            self.backend,
            IOBackendErrorPhase::Submit,
            StdIoError::from_raw_os_error(self.raw_errno()),
            self.call_count,
            queue_state,
        )
        .attach(format!("submit_retry_reason={}", self.reason))
        .attach(format!("submit_retry_attempts={attempts}"))
        .attach(format!("submit_retry_elapsed_ms={}", elapsed.as_millis()))
        .attach(format!("submit_retry_timeout_ms={}", timeout.as_millis()))
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
        queue_state: IOBackendQueueState,
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
                queue_state,
                self.attempts,
                elapsed,
                self.no_progress_timeout,
            ));
        }
        self.backoff();
        Ok(())
    }
}

/// Kernel-entry phase that produced a backend progress failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IOBackendErrorPhase {
    /// Nonblocking submit path.
    Submit,
    /// Completion wait path.
    Wait,
}

impl fmt::Display for IOBackendErrorPhase {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit => f.write_str("submit"),
            Self::Wait => f.write_str("wait"),
        }
    }
}

/// Queue state observed at one backend progress failure boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IOBackendQueueState {
    staged: Option<usize>,
    pending: Option<usize>,
    submitted: Option<usize>,
    completions: Option<usize>,
}

impl IOBackendQueueState {
    /// Unknown backend queue state.
    #[inline]
    #[cfg_attr(
        not(feature = "libaio"),
        expect(dead_code, reason = "libaio wait path reports unknown queue state")
    )]
    pub(crate) const fn unknown() -> Self {
        Self {
            staged: None,
            pending: None,
            submitted: None,
            completions: None,
        }
    }

    /// Queue state known by submit paths.
    #[inline]
    pub(crate) const fn submit(staged: usize, pending: usize) -> Self {
        Self {
            staged: Some(staged),
            pending: Some(pending),
            submitted: None,
            completions: None,
        }
    }

    /// Queue state known by wait paths when a completion count is available.
    #[inline]
    #[cfg_attr(
        all(not(feature = "iouring"), not(test)),
        expect(
            dead_code,
            reason = "io_uring wait path reports observed completion count"
        )
    )]
    pub(crate) const fn wait_with_completions(completions: usize) -> Self {
        Self {
            staged: None,
            pending: None,
            submitted: None,
            completions: Some(completions),
        }
    }
}

/// Backend failure details attached to backend progress error reports.
///
/// This represents syscall progress failures before a normal per-operation
/// completion exists. Completion-token validation and scheduler consistency
/// violations remain assertion/panic boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IOBackendFailure {
    backend: &'static str,
    phase: IOBackendErrorPhase,
    source: String,
    raw_errno: Option<i32>,
    call_count: usize,
    queue_state: IOBackendQueueState,
    note: Option<&'static str>,
}

impl IOBackendFailure {
    /// Build one backend progress report around the underlying syscall error.
    #[inline]
    pub(crate) fn report(
        backend: &'static str,
        phase: IOBackendErrorPhase,
        source: StdIoError,
        call_count: usize,
        queue_state: IOBackendQueueState,
    ) -> Report<IoError> {
        let raw_errno = source.raw_os_error();
        let kind = source.kind();
        Report::new(IoError::from(kind)).attach(Self {
            backend,
            phase,
            source: source.to_string(),
            raw_errno,
            call_count,
            queue_state,
            note: None,
        })
    }

    /// Attach a static diagnostic note.
    #[inline]
    #[cfg_attr(
        not(feature = "libaio"),
        expect(dead_code, reason = "libaio backend attaches unsupported-sync notes")
    )]
    pub(crate) fn report_with_note(
        backend: &'static str,
        phase: IOBackendErrorPhase,
        source: StdIoError,
        call_count: usize,
        queue_state: IOBackendQueueState,
        note: &'static str,
    ) -> Report<IoError> {
        let raw_errno = source.raw_os_error();
        let kind = source.kind();
        Report::new(IoError::from(kind)).attach(Self {
            backend,
            phase,
            source: source.to_string(),
            raw_errno,
            call_count,
            queue_state,
            note: Some(note),
        })
    }

    /// Backend name that produced the failure.
    #[inline]
    pub(crate) fn backend(&self) -> &'static str {
        self.backend
    }

    /// Kernel-entry phase that failed.
    #[inline]
    pub(crate) fn phase(&self) -> IOBackendErrorPhase {
        self.phase
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
}

impl fmt::Display for IOBackendFailure {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "backend={} phase={} errno={:?} error={} calls={} queue={{staged={} pending={} submitted={} completions={}}}",
            self.backend,
            self.phase,
            self.raw_errno,
            self.source,
            self.call_count,
            OptionalCount(self.queue_state.staged),
            OptionalCount(self.queue_state.pending),
            OptionalCount(self.queue_state.submitted),
            OptionalCount(self.queue_state.completions)
        )?;
        if let Some(note) = self.note {
            write!(f, " note={note}")?;
        }
        Ok(())
    }
}

struct OptionalCount(Option<usize>);

impl fmt::Display for OptionalCount {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => write!(f, "{value}"),
            None => f.write_str("?"),
        }
    }
}

/// Worker-known logical operation kind attached to backend progress reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IOBackendOperationKind {
    kind: super::IOKind,
}

impl fmt::Display for IOBackendOperationKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation_kind={:?}", self.kind)
    }
}

/// Return the backend failure details attached to a progress report.
#[inline]
pub(crate) fn backend_failure(report: &Report<IoError>) -> Option<&IOBackendFailure> {
    report.downcast_ref::<IOBackendFailure>()
}

/// Return the backend syscall attempt count attached to a progress report.
#[inline]
pub(crate) fn backend_call_count(report: &Report<IoError>) -> usize {
    backend_failure(report).map_or(0, IOBackendFailure::call_count)
}

/// Attach the first known logical operation kind affected by a backend failure.
#[inline]
pub(crate) fn attach_backend_operation_kind(
    report: Report<IoError>,
    kind: Option<super::IOKind>,
) -> Report<IoError> {
    match kind {
        Some(kind) => report.attach(IOBackendOperationKind { kind }),
        None => report,
    }
}

/// Return the attached logical operation kind when one has been added.
#[inline]
pub(crate) fn backend_operation_kind(report: &Report<IoError>) -> Option<IOBackendOperationKind> {
    report.downcast_ref::<IOBackendOperationKind>().copied()
}

/// Build a fresh standard IO error carrying backend progress context.
#[inline]
pub(crate) fn backend_report_to_io_error(report: &Report<IoError>) -> StdIoError {
    StdIoError::new(
        report.current_context().kind(),
        backend_report_summary(report),
    )
}

/// Format backend progress context for logs and poison diagnostics.
pub(crate) fn backend_report_summary(report: &Report<IoError>) -> String {
    let mut summary = report.current_context().to_string();
    if let Some(failure) = backend_failure(report) {
        summary.push_str(": ");
        summary.push_str(&failure.to_string());
    }
    if let Some(operation_kind) = backend_operation_kind(report) {
        summary.push_str(": ");
        summary.push_str(&operation_kind.to_string());
    }
    summary
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
pub(crate) struct IOBackendStats {
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

impl IOBackendStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(
        any(not(test), feature = "iouring"),
        expect(dead_code, reason = "internal io backend stats")
    )]
    pub(crate) fn delta_since(self, earlier: IOBackendStats) -> IOBackendStats {
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

/// Shared handle used to collect backend submit and wait statistics.
#[derive(Clone, Default)]
pub(crate) struct IOBackendStatsHandle(Arc<IOBackendStatsCounters>);

impl IOBackendStatsHandle {
    /// Returns a point-in-time snapshot of backend activity counters.
    #[inline]
    pub(crate) fn snapshot(&self) -> IOBackendStats {
        IOBackendStats {
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
pub(crate) trait IOBackend {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind as StdIoErrorKind;

    #[test]
    fn test_backend_failure_formats_known_and_unknown_queue_state() {
        let report = IOBackendFailure::report(
            "test_backend",
            IOBackendErrorPhase::Submit,
            StdIoError::new(StdIoErrorKind::PermissionDenied, "submit denied"),
            2,
            IOBackendQueueState::submit(0, 3),
        );
        let failure = backend_failure(&report).unwrap();

        assert_eq!(failure.backend(), "test_backend");
        assert_eq!(failure.phase(), IOBackendErrorPhase::Submit);
        assert_eq!(backend_call_count(&report), 2);
        assert_eq!(
            failure.to_string(),
            "backend=test_backend phase=submit errno=None error=submit denied calls=2 queue={staged=0 pending=3 submitted=? completions=?}"
        );
    }

    #[test]
    fn test_backend_operation_kind_is_attached_separately() {
        let report = IOBackendFailure::report(
            "test_backend",
            IOBackendErrorPhase::Wait,
            StdIoError::new(StdIoErrorKind::Interrupted, "wait interrupted"),
            4,
            IOBackendQueueState::wait_with_completions(1),
        );
        let report = attach_backend_operation_kind(report, Some(super::super::IOKind::Fsync));
        let operation_kind = backend_operation_kind(&report).unwrap();

        assert_eq!(operation_kind.to_string(), "operation_kind=Fsync");
        assert!(backend_report_summary(&report).contains("operation_kind=Fsync"));
    }
}
