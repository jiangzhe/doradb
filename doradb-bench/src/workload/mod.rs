use crate::error::BenchError;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

mod ddl;
mod insert;
mod lock;
mod maintenance;
mod noop;
mod read;
mod util;

pub(crate) use ddl::{CreateTableExecutor, IndexDdlExecutor, TableDdlExecutor};
pub(crate) use insert::{InsertRandExecutor, InsertSeqExecutor};
pub(crate) use lock::LockTableExecutor;
pub(crate) use maintenance::{CheckpointTableExecutor, FreezeTableExecutor};
pub(crate) use noop::{StmtNoopExecutor, TrxNoopExecutor};
pub(crate) use read::{
    IndexScanExecutor, IndexStreamExecutor, LookupRandExecutor, LookupSeqExecutor,
    TableScanExecutor,
};
pub(crate) use util::build_session_plans;

/// First-error-wins cooperative cancellation shared by one plan run.
pub(crate) struct RunCancellation {
    cancelled: AtomicBool,
    first_error: Mutex<Option<BenchError>>,
}

impl RunCancellation {
    /// Construct an active run state.
    pub(crate) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            first_error: Mutex::new(None),
        }
    }

    /// Return whether a peer has published an invocation-fatal error.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Publish an unexpected error without replacing the first publisher.
    pub(crate) fn fail(&self, error: BenchError) {
        let mut first_error = self.first_error.lock();
        if first_error.is_none() {
            *first_error = Some(error);
            self.cancelled.store(true, Ordering::Release);
        }
    }

    /// Take the primary error after every task has drained.
    pub(crate) fn take_error(&self) -> Option<BenchError> {
        self.first_error.lock().take()
    }
}

impl Default for RunCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic operation assignment for one benchmark session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SessionPlan {
    /// Zero-based session position in this benchmark run.
    pub(crate) session_index: usize,
    /// First logical key or request offset assigned to this session.
    pub(crate) key_start: u64,
    /// Number of operations assigned to this session.
    pub(crate) number: u64,
}
