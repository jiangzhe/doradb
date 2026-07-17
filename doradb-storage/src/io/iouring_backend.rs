use super::{
    BackendToken, IOBackend, IOBackendErrorPhase, IOBackendFailure, IOBackendQueueState,
    IOBackendStats, IOBackendStatsHandle, IOKind, Operation, StdIoResult, SubmitAttempt,
    SubmitRetry, SubmitRetryReason, SubmittedIoCleanup, backend_call_count,
};
use crate::error::{IoError, IoResult};
use error_stack::Report;
use io_uring::opcode::{Fsync, Read, Write};
use io_uring::types::{CancelBuilder, FsyncFlags, Timespec};
use io_uring::{IoUring, squeue, types};
use libc::{EAGAIN, EBUSY, EINTR};
use std::collections::VecDeque;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

const IOURING_SYNC_CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

/// Concrete io_uring context used by the storage-engine backend.
pub(crate) struct IouringBackend {
    ring: IoUring,
    io_depth: usize,
    stats: IOBackendStatsHandle,
}

impl IouringBackend {
    /// Returns one snapshot of backend-owned submit/wait activity.
    #[inline]
    #[expect(dead_code, reason = "internal io backend stats")]
    fn stats(&self) -> IOBackendStats {
        self.stats.snapshot()
    }

    /// Returns a cloneable handle to backend-owned submit/wait statistics.
    #[inline]
    pub(crate) fn stats_handle(&self) -> IOBackendStatsHandle {
        self.stats.clone()
    }

    #[inline]
    fn stage_pending_sqes(&mut self, batch: &mut IouringSubmitBatch, limit: usize) -> bool {
        let target = limit.min(batch.staged.len());
        if target == 0 {
            return false;
        }

        while batch.pending_sqes < target {
            let Some(entry) = batch.staged.get(batch.pending_sqes) else {
                break;
            };
            let push_res = {
                let mut sq = self.ring.submission();
                // SAFETY: the SQE is copied into the ring submission queue. The
                // pointed-to IO memory is owned by the worker inflight entry and
                // stays valid until completion is processed.
                unsafe { sq.push(entry) }
            };
            if push_res.is_err() {
                return true;
            }
            batch.pending_sqes += 1;
        }
        false
    }

    #[inline]
    fn take_completions(&mut self) -> Vec<(BackendToken, StdIoResult<usize>)> {
        let mut completed = Vec::new();
        let cq = self.ring.completion();
        for cqe in cq {
            let res = if cqe.result() >= 0 {
                Ok(cqe.result() as usize)
            } else {
                Err(StdIoError::from_raw_os_error(-cqe.result()))
            };
            completed.push((BackendToken::from_raw(cqe.user_data()), res));
        }
        self.stats.record_wait_completions(completed.len());
        completed
    }

    #[inline]
    fn submit_pending_sqes(&mut self, batch: &mut IouringSubmitBatch) -> IoResult<SubmitOutcome> {
        debug_assert!(batch.pending_sqes != 0);

        let mut call_count = 0usize;
        let submitted = loop {
            call_count += 1;
            match self.ring.submit() {
                Ok(submitted) => break submitted,
                Err(err) if err.raw_os_error() == Some(EINTR) => {}
                Err(err) if matches!(err.raw_os_error(), Some(EAGAIN | EBUSY)) => {
                    let reason = SubmitRetryReason::from_raw_errno(
                        err.raw_os_error()
                            .expect("EAGAIN/EBUSY match guarantees raw errno"),
                    )
                    .expect("EAGAIN/EBUSY match must convert to retry reason");
                    return Ok(retry_submit(reason, call_count));
                }
                Err(err) => {
                    return Err(IOBackendFailure::report(
                        "io_uring",
                        IOBackendErrorPhase::Submit,
                        err,
                        call_count,
                        IOBackendQueueState::submit(batch.staged.len(), batch.pending_sqes),
                    ));
                }
            }
        };

        Ok(batch.finish_submit(submitted, call_count))
    }

    fn blocking_submit_and_wait(
        &mut self,
        min_nr: usize,
        observed_completions: usize,
    ) -> IoResult<BlockingWaitOutcome> {
        let mut call_count = 0usize;
        let submitted = loop {
            call_count += 1;
            match self.ring.submit_and_wait(min_nr) {
                Ok(submitted) => break submitted,
                Err(err) if err.raw_os_error() == Some(EINTR) => {}
                Err(err) => {
                    return Err(IOBackendFailure::report(
                        "io_uring",
                        IOBackendErrorPhase::Wait,
                        err,
                        call_count,
                        IOBackendQueueState::wait_with_completions(observed_completions),
                    ));
                }
            }
        };
        Ok(BlockingWaitOutcome {
            submitted,
            call_count,
        })
    }
}

impl IOBackend for IouringBackend {
    type Prepared = squeue::Entry;
    type SubmitBatch = IouringSubmitBatch;
    type Events = ();

    #[inline]
    fn setup(io_depth: usize) -> IoResult<Self> {
        if io_depth == 0 {
            return Err(Report::new(IoError::from(StdIoErrorKind::InvalidInput))
                .attach("backend=io_uring, io_depth=0, reason=io depth must be non-zero"));
        }
        let ring_entries = io_depth.checked_next_power_of_two().ok_or_else(|| {
            Report::new(IoError::from(StdIoErrorKind::InvalidInput)).attach(format!(
                "backend=io_uring, io_depth={io_depth}, reason=next power of two overflow"
            ))
        })?;
        let ring_entries = u32::try_from(ring_entries).map_err(|_| {
            Report::new(IoError::from(StdIoErrorKind::InvalidInput)).attach(format!(
                "backend=io_uring, io_depth={io_depth}, ring_entries={ring_entries}, reason=ring entries exceed u32"
            ))
        })?;
        let ring = IoUring::new(ring_entries).map_err(|err| {
            Report::new(IoError::from(err.kind())).attach(format!("op=backend_setup, {err}"))
        })?;
        Ok(Self {
            ring,
            io_depth,
            stats: IOBackendStatsHandle::default(),
        })
    }

    #[inline]
    fn io_depth(&self) -> usize {
        self.io_depth
    }

    #[inline]
    fn new_submit_batch(&self) -> Self::SubmitBatch {
        IouringSubmitBatch {
            staged: VecDeque::with_capacity(self.io_depth),
            pending_sqes: 0,
        }
    }

    #[inline]
    fn new_events(&self) -> Self::Events {}

    #[inline]
    fn prepare(&mut self, token: BackendToken, operation: &mut Operation) -> Self::Prepared {
        let fd = types::Fd(operation.fd());
        let entry = match operation.kind() {
            IOKind::Read => {
                let ptr = operation.as_mut_ptr();
                let len = operation.len() as _;
                let offset = operation.offset() as u64;
                Read::new(fd, ptr, len).offset(offset).build()
            }
            IOKind::Write => {
                let ptr = operation.as_mut_ptr();
                let len = operation.len() as _;
                let offset = operation.offset() as u64;
                Write::new(fd, ptr, len).offset(offset).build()
            }
            IOKind::Fsync => Fsync::new(fd).build(),
            IOKind::Fdatasync => Fsync::new(fd).flags(FsyncFlags::DATASYNC).build(),
        };
        entry.user_data(token.raw())
    }

    #[inline]
    fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
        batch.staged.push_back(prepared.clone());
    }

    #[inline]
    fn submit_batch(
        &mut self,
        batch: &mut Self::SubmitBatch,
        limit: usize,
    ) -> IoResult<SubmitAttempt> {
        let start = Instant::now();
        let sq_full = self.stage_pending_sqes(batch, limit);

        if batch.pending_sqes == 0 {
            if sq_full {
                // Work was available, but the SQ accepted no staged SQEs.
                // Treat this as submit pressure so bounded retry can fail
                // sustained no-progress instead of spinning on Noop.
                return Ok(SubmitAttempt::Retry(SubmitRetry::new(
                    "io_uring",
                    SubmitRetryReason::NoProgress,
                    0,
                )));
            }
            return Ok(SubmitAttempt::Noop);
        }

        self.submit_pending_sqes(batch)
            .inspect(|outcome| {
                self.stats.record_submit_and_wait(
                    outcome.call_count,
                    start.elapsed().as_nanos() as usize,
                );
                if let SubmitAttempt::Submitted(submitted) = outcome.result {
                    self.stats.record_submitted_ops(submitted.get());
                }
            })
            .inspect_err(|err| {
                self.stats.record_submit_and_wait(
                    backend_call_count(err),
                    start.elapsed().as_nanos() as usize,
                );
            })
            .map(|outcome| outcome.result)
    }

    #[inline]
    fn wait_at_least(
        &mut self,
        _events: &mut Self::Events,
        min_nr: usize,
    ) -> IoResult<Vec<(BackendToken, StdIoResult<usize>)>> {
        {
            let cq = self.ring.completion();
            if cq.len() < min_nr {
                let completions = cq.len();
                drop(cq);
                let start = Instant::now();
                self.blocking_submit_and_wait(min_nr, completions)
                    .inspect(|outcome| {
                        record_blocking_wait_stats(
                            &self.stats,
                            Some(outcome),
                            outcome.call_count,
                            start.elapsed().as_nanos() as usize,
                        );
                    })
                    .inspect_err(|err| {
                        record_blocking_wait_stats(
                            &self.stats,
                            None,
                            backend_call_count(err),
                            start.elapsed().as_nanos() as usize,
                        );
                    })?;
            }
        }
        Ok(self.take_completions())
    }

    #[inline]
    fn cleanup_submitted_io(&mut self, submitted: usize) -> SubmittedIoCleanup {
        if submitted == 0 {
            return SubmittedIoCleanup::DropAfterBackend;
        }
        let timeout = Timespec::from(IOURING_SYNC_CANCEL_TIMEOUT);
        match self
            .ring
            .submitter()
            .register_sync_cancel(Some(timeout), CancelBuilder::any().all())
        {
            Ok(()) => SubmittedIoCleanup::DropAfterBackend,
            Err(err) => SubmittedIoCleanup::LeakAfterBackend {
                backend: "io_uring",
                reason: format!("sync cancel failed for {submitted} submitted ops: {err}"),
            },
        }
    }
}

/// Backend-owned io_uring staging buffer for one worker.
///
/// `staged` keeps prepared SQEs in submission order until the kernel accepts
/// them. `pending_sqes` tracks the prefix already pushed into the ring's SQ but
/// not yet confirmed by a successful `submit()` call.
pub(crate) struct IouringSubmitBatch {
    staged: VecDeque<squeue::Entry>,
    pending_sqes: usize,
}

impl IouringSubmitBatch {
    #[inline]
    fn finish_submit(&mut self, submitted: usize, call_count: usize) -> SubmitOutcome {
        assert!(
            submitted <= self.pending_sqes,
            "io_uring submit reported more accepted SQEs than staged pending entries"
        );
        if submitted != 0 {
            self.staged.drain(0..submitted);
            self.pending_sqes -= submitted;
        }
        let result = match NonZeroUsize::new(submitted) {
            Some(submitted) => SubmitAttempt::Submitted(submitted),
            None => {
                // `submit()` was called with pending SQEs but accepted none.
                // Keep the pending prefix intact and let callers use the
                // bounded retry/progress-failure path.
                SubmitAttempt::Retry(SubmitRetry::new(
                    "io_uring",
                    SubmitRetryReason::NoProgress,
                    call_count,
                ))
            }
        };
        SubmitOutcome { result, call_count }
    }
}

struct SubmitOutcome {
    result: SubmitAttempt,
    call_count: usize,
}

struct BlockingWaitOutcome {
    submitted: usize,
    call_count: usize,
}

#[inline]
fn retry_submit(reason: SubmitRetryReason, call_count: usize) -> SubmitOutcome {
    SubmitOutcome {
        result: SubmitAttempt::Retry(SubmitRetry::new("io_uring", reason, call_count)),
        call_count,
    }
}

#[inline]
fn record_blocking_wait_stats(
    stats: &IOBackendStatsHandle,
    outcome: Option<&BlockingWaitOutcome>,
    call_count: usize,
    elapsed_nanos: usize,
) {
    stats.record_submit_and_wait(call_count, elapsed_nanos);
    if let Some(outcome) = outcome {
        stats.record_submitted_ops(outcome.submitted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::opcode::Nop;

    fn nop_entry() -> squeue::Entry {
        Nop::new().build()
    }

    fn submit_batch_with_pending(staged_len: usize, pending_sqes: usize) -> IouringSubmitBatch {
        let mut staged = VecDeque::with_capacity(staged_len);
        for _ in 0..staged_len {
            staged.push_back(nop_entry());
        }
        IouringSubmitBatch {
            staged,
            pending_sqes,
        }
    }

    #[test]
    fn test_finish_submit_applies_successful_submit_bookkeeping() {
        let mut batch = submit_batch_with_pending(3, 3);
        let outcome = batch.finish_submit(2, 1);
        assert_eq!(
            outcome.result,
            SubmitAttempt::Submitted(NonZeroUsize::new(2).unwrap())
        );
        assert_eq!(outcome.call_count, 1);
        assert_eq!(batch.pending_sqes, 1);
        assert_eq!(batch.staged.len(), 1);
    }

    #[test]
    fn test_finish_submit_returns_no_progress_retry_for_zero_submit() {
        let mut batch = submit_batch_with_pending(2, 2);
        let outcome = batch.finish_submit(0, 1);
        let SubmitAttempt::Retry(retry) = outcome.result else {
            panic!("expected submit retry");
        };
        assert_eq!(retry.backend(), "io_uring");
        assert_eq!(retry.reason(), SubmitRetryReason::NoProgress);
        assert_eq!(retry.raw_errno(), None);
        assert_eq!(retry.call_count(), 1);
        assert_eq!(outcome.call_count, 1);
        assert_eq!(batch.pending_sqes, 2);
        assert_eq!(batch.staged.len(), 2);
    }

    #[test]
    fn test_submit_retry_reason_maps_pressure_errno() {
        assert_eq!(
            SubmitRetryReason::from_raw_errno(EAGAIN),
            Some(SubmitRetryReason::Eagain)
        );
        assert_eq!(
            SubmitRetryReason::from_raw_errno(EBUSY),
            Some(SubmitRetryReason::Ebusy)
        );
        assert_eq!(SubmitRetryReason::from_raw_errno(EINTR), None);
        assert_eq!(SubmitRetryReason::NoProgress.raw_errno(), None);
    }

    #[test]
    fn test_retry_submit_carries_backend_context() {
        let outcome = retry_submit(SubmitRetryReason::Ebusy, 3);
        let SubmitAttempt::Retry(retry) = outcome.result else {
            panic!("expected submit retry");
        };
        assert_eq!(retry.backend(), "io_uring");
        assert_eq!(retry.reason(), SubmitRetryReason::Ebusy);
        assert_eq!(retry.raw_errno(), Some(EBUSY));
        assert_eq!(retry.call_count(), 3);
        assert_eq!(outcome.call_count, 3);
    }

    #[test]
    fn test_submit_batch_reports_no_progress_when_sq_full() {
        let mut backend = IouringBackend::setup(1).unwrap();
        assert_eq!(backend.io_depth(), 1);
        {
            let mut sq = backend.ring.submission();
            loop {
                let entry = nop_entry();
                // SAFETY: this test intentionally fills the SQ with copied NOP
                // SQEs to make the next staged push observe SQ-full.
                if unsafe { sq.push(&entry) }.is_err() {
                    break;
                }
            }
        }
        let mut batch = submit_batch_with_pending(1, 0);

        let submit_result = backend.submit_batch(&mut batch, 1).unwrap();

        let SubmitAttempt::Retry(retry) = submit_result else {
            panic!("expected submit retry");
        };
        assert_eq!(retry.reason(), SubmitRetryReason::NoProgress);
        assert_eq!(retry.raw_errno(), None);
        assert_eq!(retry.call_count(), 0);
        assert_eq!(batch.pending_sqes, 0);
        assert_eq!(batch.staged.len(), 1);
    }

    #[test]
    fn test_record_blocking_wait_stats_counts_combined_fields() {
        let stats = IOBackendStatsHandle::default();
        let outcome = BlockingWaitOutcome {
            submitted: 5,
            call_count: 2,
        };
        record_blocking_wait_stats(&stats, Some(&outcome), outcome.call_count, 17);
        stats.record_wait_completions(3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.submit_and_wait_calls, 2);
        assert_eq!(snapshot.submitted_ops, 5);
        assert_eq!(snapshot.submit_and_wait_nanos, 17);
        assert_eq!(snapshot.wait_completions, 3);
    }

    #[test]
    fn test_iouring_backend_rejects_zero_depth_as_invalid_input() {
        let err = match IouringBackend::setup(0) {
            Ok(_) => panic!("expected invalid io depth"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), StdIoErrorKind::InvalidInput);
        let report = format!("{err:?}");
        assert!(report.contains("backend=io_uring"), "{report}");
        assert!(report.contains("io_depth=0"), "{report}");
    }

    #[test]
    fn test_iouring_backend_rejects_ring_entry_overflow_as_invalid_input() {
        let io_depth = (u32::MAX as usize) + 1;
        let err = match IouringBackend::setup(io_depth) {
            Ok(_) => panic!("expected invalid io depth"),
            Err(err) => err,
        };
        assert_eq!(err.current_context().kind(), StdIoErrorKind::InvalidInput);
        let report = format!("{err:?}");
        assert!(report.contains("backend=io_uring"), "{report}");
        assert!(report.contains(&format!("io_depth={io_depth}")), "{report}");
    }
}
