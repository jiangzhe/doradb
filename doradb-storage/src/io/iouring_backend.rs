use super::{
    AIOClient, AIOError, AIOKind, AIOResult, BackendToken, IOBackend, IOBackendStats,
    IOBackendStatsHandle, IOWorkerBuilder, Operation,
};
use flume::bounded;
use io_uring::{IoUring, opcode, squeue, types};
use libc::{EAGAIN, EBUSY, EINTR};
use std::collections::VecDeque;
use std::result::Result as StdResult;
use std::time::Instant;

const DEFAULT_AIO_MAX_EVENTS: usize = 32;

/// Concrete io_uring context used by the storage-engine backend.
pub struct IouringBackend {
    ring: IoUring,
    max_events: usize,
    stats: IOBackendStatsHandle,
}

impl IouringBackend {
    /// Creates a new io_uring backend with the requested maximum concurrent IO depth.
    #[inline]
    pub fn new(max_events: usize) -> AIOResult<Self> {
        if max_events == 0 {
            return Err(AIOError::SetupError);
        }
        let ring_entries = max_events
            .checked_next_power_of_two()
            .ok_or(AIOError::SetupError)?;
        let ring = IoUring::new(ring_entries as u32).map_err(|_| AIOError::SetupError)?;
        Ok(IouringBackend {
            ring,
            max_events,
            stats: IOBackendStatsHandle::default(),
        })
    }

    /// Creates a default io_uring backend.
    #[inline]
    pub fn try_default() -> AIOResult<Self> {
        Self::new(DEFAULT_AIO_MAX_EVENTS)
    }

    /// Returns maximum concurrent events exposed to the generic worker.
    #[inline]
    pub fn max_events(&self) -> usize {
        self.max_events
    }

    /// Returns one snapshot of backend-owned submit/wait activity.
    #[inline]
    pub fn stats(&self) -> IOBackendStats {
        self.stats.snapshot()
    }

    #[inline]
    pub(crate) fn stats_handle(&self) -> IOBackendStatsHandle {
        self.stats.clone()
    }

    /// Builds an IO worker builder.
    #[inline]
    pub fn io_worker<T>(self) -> (IOWorkerBuilder<T>, AIOClient<T>) {
        const DEFAULT_AIO_EVENT_LOOP_BACKLOG: usize = 10;
        let (tx, rx) = bounded(DEFAULT_AIO_EVENT_LOOP_BACKLOG);
        let worker = IOWorkerBuilder { backend: self, rx };
        (worker, AIOClient(tx))
    }
}

/// Backend-owned io_uring staging buffer for one worker.
///
/// `staged` keeps prepared SQEs in submission order until the kernel accepts
/// them. `pending_sqes` tracks the prefix already pushed into the ring's SQ but
/// not yet confirmed by a successful `submit()` call.
pub struct IouringSubmitBatch {
    staged: VecDeque<squeue::Entry>,
    pending_sqes: usize,
}

struct SubmitOutcome {
    submitted: usize,
    call_count: usize,
}

struct BlockingWaitOutcome {
    submitted: usize,
    call_count: usize,
}

#[inline]
fn finish_submit(
    batch: &mut IouringSubmitBatch,
    submitted: usize,
    call_count: usize,
) -> SubmitOutcome {
    assert!(
        submitted <= batch.pending_sqes,
        "io_uring submit reported more accepted SQEs than staged pending entries"
    );
    if submitted != 0 {
        batch.staged.drain(0..submitted);
        batch.pending_sqes -= submitted;
    }
    SubmitOutcome {
        submitted,
        call_count,
    }
}

#[inline]
fn submit_pending_sqes<Submit, SubmitAndWait>(
    batch: &mut IouringSubmitBatch,
    limit: usize,
    mut submit: Submit,
    mut submit_and_wait: SubmitAndWait,
) -> SubmitOutcome
where
    Submit: FnMut() -> std::io::Result<usize>,
    SubmitAndWait: FnMut(usize) -> std::io::Result<usize>,
{
    debug_assert!(batch.pending_sqes != 0);

    let mut call_count = 0usize;
    let submitted = loop {
        call_count += 1;
        match submit() {
            Ok(submitted) => break submitted,
            Err(err) if err.raw_os_error() == Some(EINTR) => continue,
            Err(err) if matches!(err.raw_os_error(), Some(EAGAIN | EBUSY)) => {
                let submitted = loop {
                    call_count += 1;
                    match submit_and_wait(1) {
                        Ok(submitted) => break submitted,
                        Err(err) if err.raw_os_error() == Some(EINTR) => continue,
                        Err(err) => {
                            panic!(
                                "io_uring blocking submit failed: err={err} pending_sqes={} limit={} staged_len={}",
                                batch.pending_sqes,
                                limit,
                                batch.staged.len()
                            );
                        }
                    }
                };
                break submitted;
            }
            Err(err) => {
                panic!(
                    "io_uring submit failed: err={err} pending_sqes={} limit={} staged_len={}",
                    batch.pending_sqes,
                    limit,
                    batch.staged.len()
                );
            }
        }
    };

    finish_submit(batch, submitted, call_count)
}

fn blocking_submit_and_wait<SubmitAndWait>(
    min_nr: usize,
    mut submit_and_wait: SubmitAndWait,
) -> BlockingWaitOutcome
where
    SubmitAndWait: FnMut(usize) -> std::io::Result<usize>,
{
    let mut call_count = 0usize;
    let submitted = loop {
        call_count += 1;
        match submit_and_wait(min_nr) {
            Ok(submitted) => break submitted,
            Err(err) if err.raw_os_error() == Some(EINTR) => continue,
            Err(err) => panic!("io_uring wait failed: err={err} min_nr={min_nr}"),
        }
    };
    BlockingWaitOutcome {
        submitted,
        call_count,
    }
}

#[inline]
fn record_blocking_wait_stats(
    stats: &IOBackendStatsHandle,
    outcome: BlockingWaitOutcome,
    elapsed_nanos: usize,
) {
    stats.record_submit_and_wait(outcome.call_count, elapsed_nanos);
    stats.record_submitted_ops(outcome.submitted);
}

impl IouringBackend {
    #[inline]
    fn stage_pending_sqes(&mut self, batch: &mut IouringSubmitBatch, limit: usize) {
        let target = limit.min(batch.staged.len());
        if target == 0 {
            return;
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
                break;
            }
            batch.pending_sqes += 1;
        }
    }

    #[inline]
    fn take_completions(&mut self) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
        let mut completed = Vec::new();
        let cq = self.ring.completion();
        for cqe in cq {
            let res = if cqe.result() >= 0 {
                Ok(cqe.result() as usize)
            } else {
                Err(std::io::Error::from_raw_os_error(-cqe.result()))
            };
            completed.push((BackendToken::from_raw(cqe.user_data()), res));
        }
        self.stats.record_wait_completions(completed.len());
        completed
    }
}

impl IOBackend for IouringBackend {
    type Prepared = squeue::Entry;
    type SubmitBatch = IouringSubmitBatch;
    type Events = ();

    #[inline]
    fn max_events(&self) -> usize {
        self.max_events()
    }

    #[inline]
    fn new_submit_batch(&self) -> Self::SubmitBatch {
        IouringSubmitBatch {
            staged: VecDeque::with_capacity(self.max_events()),
            pending_sqes: 0,
        }
    }

    #[inline]
    fn new_events(&self) -> Self::Events {}

    #[inline]
    fn prepare(&mut self, token: BackendToken, operation: &mut Operation) -> Self::Prepared {
        let fd = types::Fd(operation.fd());
        let ptr = operation.as_mut_ptr();
        let len = operation.len() as _;
        let offset = operation.offset() as u64;
        let entry = match operation.kind() {
            AIOKind::Read => opcode::Read::new(fd, ptr, len).offset(offset).build(),
            AIOKind::Write => opcode::Write::new(fd, ptr, len).offset(offset).build(),
        };
        entry.user_data(token.raw())
    }

    #[inline]
    fn push_prepared(&mut self, batch: &mut Self::SubmitBatch, prepared: &mut Self::Prepared) {
        batch.staged.push_back(prepared.clone());
    }

    #[inline]
    fn submit_batch(&mut self, batch: &mut Self::SubmitBatch, limit: usize) -> usize {
        let start = Instant::now();
        self.stage_pending_sqes(batch, limit);

        if batch.pending_sqes == 0 {
            return 0;
        }

        let outcome = submit_pending_sqes(
            batch,
            limit,
            || self.ring.submit(),
            |min_nr| self.ring.submit_and_wait(min_nr),
        );
        self.stats
            .record_submit_and_wait(outcome.call_count, start.elapsed().as_nanos() as usize);
        self.stats.record_submitted_ops(outcome.submitted);
        outcome.submitted
    }

    #[inline]
    fn wait_at_least(
        &mut self,
        _events: &mut Self::Events,
        min_nr: usize,
    ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
        {
            let cq = self.ring.completion();
            if cq.len() < min_nr {
                drop(cq);
                let start = Instant::now();
                let outcome =
                    blocking_submit_and_wait(min_nr, |min_nr| self.ring.submit_and_wait(min_nr));
                record_blocking_wait_stats(
                    &self.stats,
                    outcome,
                    start.elapsed().as_nanos() as usize,
                );
            }
        }
        self.take_completions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io_uring::opcode;

    fn nop_entry() -> squeue::Entry {
        opcode::Nop::new().build()
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
    fn test_submit_pending_sqes_applies_successful_submit_bookkeeping() {
        let mut batch = submit_batch_with_pending(3, 3);
        let outcome = submit_pending_sqes(
            &mut batch,
            3,
            || Ok(2),
            |_| panic!("blocking fallback should not run on successful submit"),
        );
        assert_eq!(outcome.submitted, 2);
        assert_eq!(outcome.call_count, 1);
        assert_eq!(batch.pending_sqes, 1);
        assert_eq!(batch.staged.len(), 1);
    }

    #[test]
    fn test_submit_pending_sqes_falls_back_to_blocking_submit_on_eagain() {
        let mut batch = submit_batch_with_pending(2, 2);
        let mut submit_calls = 0;
        let outcome = submit_pending_sqes(
            &mut batch,
            2,
            || {
                submit_calls += 1;
                Err(std::io::Error::from_raw_os_error(EAGAIN))
            },
            |min_nr| {
                assert_eq!(min_nr, 1);
                Ok(2)
            },
        );
        assert_eq!(outcome.submitted, 2);
        assert_eq!(outcome.call_count, 2);
        assert_eq!(submit_calls, 1);
        assert_eq!(batch.pending_sqes, 0);
        assert!(batch.staged.is_empty());
    }

    #[test]
    fn test_submit_pending_sqes_preserves_unaccepted_suffix_after_blocking_fallback() {
        let mut batch = submit_batch_with_pending(3, 3);
        let outcome = submit_pending_sqes(
            &mut batch,
            3,
            || Err(std::io::Error::from_raw_os_error(EBUSY)),
            |min_nr| {
                assert_eq!(min_nr, 1);
                Ok(1)
            },
        );
        assert_eq!(outcome.submitted, 1);
        assert_eq!(outcome.call_count, 2);
        assert_eq!(batch.pending_sqes, 2);
        assert_eq!(batch.staged.len(), 2);
    }

    #[test]
    fn test_blocking_submit_and_wait_counts_successful_attempt() {
        let mut calls = 0usize;
        let outcome = blocking_submit_and_wait(3, |min_nr| {
            calls += 1;
            assert_eq!(min_nr, 3);
            Ok(2)
        });
        assert_eq!(outcome.submitted, 2);
        assert_eq!(outcome.call_count, 1);
        assert_eq!(calls, 1);
    }

    #[test]
    fn test_blocking_submit_and_wait_retries_on_eintr() {
        let mut calls = 0usize;
        let outcome = blocking_submit_and_wait(4, |min_nr| {
            calls += 1;
            assert_eq!(min_nr, 4);
            if calls == 1 {
                Err(std::io::Error::from_raw_os_error(EINTR))
            } else {
                Ok(3)
            }
        });
        assert_eq!(outcome.submitted, 3);
        assert_eq!(outcome.call_count, 2);
        assert_eq!(calls, 2);
    }

    #[test]
    fn test_record_blocking_wait_stats_counts_combined_fields() {
        let stats = IOBackendStatsHandle::default();
        record_blocking_wait_stats(
            &stats,
            BlockingWaitOutcome {
                submitted: 5,
                call_count: 2,
            },
            17,
        );
        stats.record_wait_completions(3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.submit_and_wait_calls, 2);
        assert_eq!(snapshot.submitted_ops, 5);
        assert_eq!(snapshot.submit_and_wait_nanos, 17);
        assert_eq!(snapshot.wait_completions, 3);
    }
}
