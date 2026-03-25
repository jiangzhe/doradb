use super::{
    AIOClient, AIOError, AIOKind, AIOResult, BackendToken, IOBackend, IOWorkerBuilder, Operation,
};
use flume::bounded;
use io_uring::{IoUring, opcode, squeue, types};
use libc::{EAGAIN, EBUSY, EINTR};
use std::collections::VecDeque;
use std::result::Result as StdResult;

const DEFAULT_AIO_MAX_EVENTS: usize = 32;

/// Concrete io_uring context used by the storage-engine backend.
pub struct IouringBackend {
    ring: IoUring,
    max_events: usize,
}

// SAFETY: the backend owns one io_uring instance and is moved as a whole onto
// the dedicated IO worker thread; it is never used concurrently.
unsafe impl Send for IouringBackend {}

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
        Ok(IouringBackend { ring, max_events })
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

// SAFETY: the batch is owned by one worker thread. SQEs only contain copied
// kernel request descriptors and raw user pointers whose lifetime is owned by
// the worker inflight table.
unsafe impl Send for IouringSubmitBatch {}

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
        let target = limit.min(batch.staged.len());
        if target == 0 {
            return 0;
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

        if batch.pending_sqes == 0 {
            return 0;
        }

        let submitted = loop {
            match self.ring.submit() {
                Ok(submitted) => break submitted,
                Err(err) if err.raw_os_error() == Some(EINTR) => continue,
                Err(err) if matches!(err.raw_os_error(), Some(EAGAIN | EBUSY)) => return 0,
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

        assert!(
            submitted <= batch.pending_sqes,
            "io_uring submit reported more accepted SQEs than staged pending entries"
        );
        if submitted != 0 {
            batch.staged.drain(0..submitted);
            batch.pending_sqes -= submitted;
        }
        submitted
    }

    #[inline]
    fn wait_at_least(
        &mut self,
        _events: &mut Self::Events,
        min_nr: usize,
    ) -> Vec<(BackendToken, StdResult<usize, std::io::Error>)> {
        loop {
            match self.ring.submit_and_wait(min_nr) {
                Ok(_) => break,
                Err(err) if err.raw_os_error() == Some(EINTR) => continue,
                Err(err) => panic!("io_uring wait failed: err={err} min_nr={min_nr}"),
            }
        }

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
        completed
    }
}
