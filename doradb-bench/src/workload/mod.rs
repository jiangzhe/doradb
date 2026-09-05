use crate::error::BenchError;
use event_listener::Event;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

mod catalog;
mod ddl;
mod insert;
mod lock;
mod maintenance;
mod noop;
mod read;
mod table_scan;
mod update;
mod util;

pub(crate) use catalog::{CatalogCheckpointExecutor, CatalogCheckpointPrepareExecutor};
pub(crate) use ddl::{CreateTableExecutor, IndexDdlExecutor, TableDdlExecutor};
pub(crate) use insert::{InsertRandExecutor, InsertSeqExecutor};
pub(crate) use lock::LockTableExecutor;
pub(crate) use maintenance::{CheckpointTableExecutor, FreezeTableExecutor};
pub(crate) use noop::{StmtNoopExecutor, TrxNoopExecutor};
pub(crate) use read::{
    IndexScanExecutor, IndexStreamExecutor, LookupRandExecutor, LookupSeqExecutor,
};
pub(crate) use table_scan::{
    ParallelTableScanExecutor, ParallelTableScanExecutorConfig, TableScanExecutor,
};
pub(crate) use update::UpdateRandExecutor;
pub(crate) use util::build_session_plans;

/// First-error-wins cooperative cancellation shared by one plan run.
pub(crate) struct RunCancellation {
    cancelled: AtomicBool,
    cancelled_event: Event,
    first_error: Mutex<Option<BenchError>>,
}

impl RunCancellation {
    /// Construct an active run state.
    pub(crate) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            cancelled_event: Event::new(),
            first_error: Mutex::new(None),
        }
    }

    /// Return whether a peer has published an invocation-fatal error.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Wait until a peer publishes an invocation-fatal error.
    pub(crate) async fn wait_for_cancellation(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let listener = self.cancelled_event.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }

    /// Publish an unexpected error without replacing the first publisher.
    pub(crate) fn fail(&self, error: BenchError) {
        let published = {
            let mut first_error = self.first_error.lock();
            if first_error.is_some() {
                false
            } else {
                *first_error = Some(error);
                self.cancelled.store(true, Ordering::Release);
                true
            }
        };
        if published {
            self.cancelled_event.notify(usize::MAX);
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
